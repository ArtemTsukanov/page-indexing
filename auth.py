from orm import User, Token, DoesNotExist
import asyncio
from functools import partial
from aio_pika import connect, IncomingMessage, Exchange, Message
import uuid
import json
import datetime


def now():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


async def signup(data):
    user = await User.objects.filter(email=data['email'])
    if user:
        return 'User with this email already exists'

    user = await User.objects.filter(name=data['name'])
    if user:
        return 'User with this name already exists'

    user = await User.objects.create(email=data['email'],
        password=data['password'], name=data['name'],
        created_date=now(), last_login_date=now())

    tomorrow = (datetime.datetime.now() +
        datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    token = await Token.objects.create(token=str(uuid.uuid4()),
                                       user_id=user.id,
                                       expire_date=tomorrow)
    return {'token': token.token, 'expire_date': token.expire_date}


async def login(data):
    try:
        user = await User.objects.get(email=data['email'])
    except DoesNotExist:
        return 'Unregistered user'

    await Token.objects.delete(user_id=user.id)

    tomorrow = (datetime.datetime.now() +
        datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    token = await Token.objects.create(token=str(uuid.uuid4()),
                                       user_id=user.id,
                                       expire_date=tomorrow)

    await User.objects.update('last_login_date', id=user.id,
                              last_login_date=now())
    return {'token': token.token, 'expire_date': token.expire_date}


async def validate(data):
    try:
        token = await Token.objects.get(token=data['token'])
    except DoesNotExist:
        return 'Invalid token'

    if token.expire_date < now():
        return 'Token expired'

    user = await User.objects.get(id=token.user_id)
    return {'id': user.id, 'email': user.email, 'name': user.name,
            'created_date': user.created_date,
            'last_login_date': user.last_login_date}



async def on_message(exchange: Exchange, message: IncomingMessage):
    with message.process():
        payload = json.loads(message.body.decode())

        try:
            if payload['type'] == 'signup':
                return_data = await signup(payload['data'])
            elif payload['type'] == 'login':
                return_data = await login(payload['data'])
            elif payload['type'] == 'validate':
                return_data = await validate(payload['data'])
        except ValueError as err:
            response = json.dumps({'status': err, 'data': {}}).encode()
        else:
            if isinstance(return_data, str):
                response = json.dumps({'status': return_data,
                                       'data': {}}).encode()
            else:
                response = json.dumps({'status': 'ok',
                                       'data': return_data}).encode()

        await exchange.publish(
            Message(body=response, content_type='application/json',
                    correlation_id=message.correlation_id),
            routing_key=message.reply_to)


async def main(loop):
    connection = await connect('amqp://auth:auth@localhost/', loop=loop)
    channel = await connection.channel()
    queue = await channel.declare_queue('auth')

    await queue.consume(partial(on_message, channel.default_exchange))


loop = asyncio.get_event_loop()
loop.create_task(main(loop))
loop.run_forever()
