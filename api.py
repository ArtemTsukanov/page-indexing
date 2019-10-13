from orm import User, Stat, DoesNotExist
from aiohttp import web
import asyncio
from aioelasticsearch import Elasticsearch
from aio_pika import connect, IncomingMessage, Message, DeliveryMode
import uuid
import json
import time


loop = asyncio.get_event_loop()
es = Elasticsearch()


class AuthMS:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.futures = {}

    async def connect(self):
        self.connection = await connect('amqp://auth:auth@localhost/',
                                        loop=loop)
        self.channel = await self.connection.channel()
        self.callback_queue = await self.channel.declare_queue(exclusive=True)
        await self.callback_queue.consume(self.on_response)

    def on_response(self, message: IncomingMessage):
        future = self.futures.pop(message.correlation_id)
        future.set_result(message.body)

    async def make_request(self, type, data, timeout):
        correlation_id = str(uuid.uuid4())
        future = loop.create_future()
        self.futures[correlation_id] = future
        await self.channel.default_exchange.publish(
            Message(
                json.dumps({'type': type, 'data': data}).encode(),
                content_type='application/json',
                correlation_id=correlation_id,
                reply_to=self.callback_queue.name),
            routing_key='auth')
        try:
            resp = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        return json.loads(resp.decode())

auth_ms = AuthMS()


class CrawlerMS:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self):
        self.connection = await connect('amqp://crawler:crawler@localhost/',
                                        loop=loop)
        self.channel = await self.connection.channel()

    async def make_nowait_request(self, type, data):
        await self.channel.default_exchange.publish(
            Message(
                json.dumps({'type': type, 'data': data}).encode(),
                content_type='application/json',
                delivery_mode=DeliveryMode.PERSISTENT),
            routing_key='crawler')

crawler_ms = CrawlerMS()


async def on_startup(app):
    await auth_ms.connect()
    await crawler_ms.connect()


async def signup(request):
    params = request.rel_url.query
    if 'email' not in params:
        raise web.HTTPBadRequest(body=json.dumps({'status': 'email required',
                                                  'data': {}}))
    if 'password' not in params:
        raise web.HTTPBadRequest(body=json.dumps({'status': 'password ' \
                                                  'required', 'data': {}}))
    if 'name' not in params:
        raise web.HTTPBadRequest(body=json.dumps({'status': 'name required',
                                                  'data': {}}))

    resp = await auth_ms.make_request('signup', timeout=5, data={
                                            'email': params['email'],
                                            'password': params['password'],
                                            'name': params['name']})
    if resp is None:
        raise web.HTTPInternalServerError(body=json.dumps({
                                            'status': 'Timeout exceeded',
                                            'data': {}}))
    return web.json_response(resp)


async def login(request):
    params = request.rel_url.query
    if 'email' not in params:
        raise web.HTTPBadRequest(body=json.dumps({'status': 'email required',
                                                  'data': {}}))
    if 'password' not in params:
        raise web.HTTPBadRequest(body=json.dumps({'status': 'password ' \
                                                  'required', 'data': {}}))

    resp = await auth_ms.make_request('login', timeout=5,
                                      data={'email': params['email'],
                                            'password': params['password']})
    if resp is None:
        raise web.HTTPInternalServerError(body=json.dumps({
                                            'status': 'Timeout exceeded',
                                            'data': {}}))
    return web.json_response(resp)


async def search(request):
    params = request.rel_url.query
    if 'q' not in params:
        raise web.HTTPBadRequest(body=json.dumps({'status': 'q required',
                                                  'data': {}}))
    if 'limit' not in params:
        raise web.HTTPBadRequest(body=json.dumps({'status': 'limit required',
                                                  'data': {}}))
    if 'offset' not in params:
        raise web.HTTPBadRequest(body=json.dumps({'status': 'offset required',
                                                  'data': {}}))
    q = params['q']
    try:
        limit = int(params['limit'])
        offset = int(params['offset'])
    except ValueError as err:
        raise web.HTTPBadRequest(body=json.dumps({'status': str(err),
                                                  'data': {}}))

    if limit < 1 or limit > 100:
        raise web.HTTPBadRequest(body=json.dumps({'status': 'limit shoud ' \
                                    'be between 1 and 100', 'data': {}}))
    if offset < 0:
        offset = 0

    body = {
        'size': limit,
        'from': offset,
        'query': {
            'match_phrase': {
                'content': q
            }
        }
    }
    res = await es.search(index='crawling', doc_type='text', body=body)
    urls = []
    for hit in res['hits']['hits']:
        url = hit['_source']['url']
        urls.append(url)
    return web.json_response({'status': 'ok', 'data': urls})


async def current(request):
    headers = request.headers
    if 'X-Token' not in headers:
        raise web.HTTPForbidden(body=json.dumps({'status': 'forbidden',
                                                 'data': {}}))

    resp = await auth_ms.make_request('validate', timeout=5,
                                      data={'token': headers['X-Token']})
    if resp is None:
        raise web.HTTPInternalServerError(body=json.dumps({
                                            'status': 'Timeout exceeded',
                                            'data': {}}))
    return web.json_response(resp)


async def index(request):
    params = request.rel_url.query
    if 'domain' not in params:
        raise web.HTTPBadRequest(body=json.dumps({'status': 'domain required',
                                                  'data': {}}))
    headers = request.headers
    if 'X-Token' not in headers:
        raise web.HTTPForbidden(body=json.dumps({'status': 'forbidden',
                                                 'data': {}}))

    resp = await auth_ms.make_request('validate', timeout=5,
                                      data={'token': headers['X-Token']})
    if resp is None:
        raise web.HTTPInternalServerError(body=json.dumps({
                                            'status': 'Timeout exceeded',
                                            'data': {}}))
    if resp['status'] != 'ok':
        return web.json_response(resp)

    await crawler_ms.make_nowait_request('crawl', data={
                                            'domain': params['domain'],
                                            'author_id': resp['data']['id']})
    return web.json_response({'status': 'ok', 'data': {
                                    'id': resp['data']['id']}})


async def stat(request):
    headers = request.headers
    if 'X-Token' not in headers:
        raise web.HTTPForbidden(body=json.dumps({'status': 'forbidden',
                                                 'data': {}}))

    resp = await auth_ms.make_request('validate', timeout=5,
                                      data={'token': headers['X-Token']})
    if resp is None:
        raise web.HTTPInternalServerError(body=json.dumps({
                                            'status': 'Timeout exceeded',
                                            'data': {}}))
    if resp['status'] != 'ok':
        return web.json_response(resp)

    data = []
    stats = await Stat.objects.filter(author_id=resp['data']['id'])
    for stat in stats:
        data.append({'domain': stat.domain, 'status': stat.status,
                     'time': stat.time, 'pages': stat.pages_count})

    return web.json_response({'status': 'ok', 'data': data})


def main():
    app = web.Application()
    app.on_startup.append(on_startup)
    app.add_routes([web.post('/signup', signup),
                    web.post('/login', login),
                    web.get('/search', search),
                    web.get('/current', current),
                    web.post('/index', index),
                    web.get('/stat', stat)])
    web.run_app(app)


if __name__ == '__main__':
    main()
