from threading import Thread
from orm import Stat, DoesNotExist
import asyncio
from aio_pika import connect, IncomingMessage
from aiohttp import ClientSession
from aioelasticsearch import Elasticsearch
from collections import deque
from bs4 import BeautifulSoup
import re
import json
import time
import datetime


loop = asyncio.get_event_loop()
crawl_repeat_time = 86399


def now():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class Crawler:
    def __init__(self, max_tasks, max_rps, max_depth):
        self.max_tasks = max_tasks
        self.max_rps = max_rps
        self.max_depth = max_depth
        self.urls = []
        self.stats = {}
        self.q = {}
        self.seen_urls = {}
        self.timer = {}

    async def crawl(self):
        self.session = ClientSession(loop=loop)
        self.es = Elasticsearch()
        workers = [asyncio.Task(self.work()) for _ in range(self.max_tasks)]

    async def add_url(self, url, author_id, https):
        self.urls.append(url)
        q = asyncio.Queue()
        q.put_nowait((url, 0))
        self.q[url] = q
        self.seen_urls[url] = set(url)
        self.timer[url] = deque()
        stat = await Stat.objects.create(domain=url, status='Crawling',
            author_id=author_id, https=1, time=now(), pages_count=0)
        self.stats[url] = stat

    async def work(self):
        while True:
            await asyncio.sleep(0.001)
            for root in self.urls:
                if self.q[root].empty():
                    if root in self.stats and \
                                self.stats[root].pages_count > 10:
                        self.stats[root].status = 'Done'
                        self.stats[root].time = now()
                        await self.stats[root].save('status', 'time')
                    continue

                url, depth = await self.q[root].get()
                if depth == self.max_depth:
                    self.q[root].task_done()
                    continue

                await self.fetch(url, depth, root)
                self.q[root].task_done()

                self.stats[root].pages_count += 1
                if self.stats[root].pages_count % 10 == 0:
                    self.stats[root].time = now()
                    await self.stats[root].save('pages_count', 'time')

    async def fetch(self, url, depth, root):
        await self.is_rps_exceeded(root)
        async with self.session.get(url) as response:
            html = await response.text(encoding='utf-8')
        await self.index_page(url, html)
        links = await self.parse_links(html, root)
        for link in links.difference(self.seen_urls[root]):
            self.q[root].put_nowait((link, depth + 1))
        self.seen_urls[root].update(links)

    async def index_page(self, url, html):
        soup = BeautifulSoup(html, features='html.parser')
        [x.extract() for x in soup.find_all(['title', 'script', 'style',
                                             'meta'])]
        text = re.sub('<[^>]+>', '', str(soup))
        text = re.sub('(\s){2,}', ' ', text)
        content_url = json.dumps({
            'url': url,
            'content': text,
        })
        await self.es.index(index='crawling', doc_type='text', \
                            body=content_url)

    async def parse_links(self, html, root):
        links = set()
        soup = BeautifulSoup(html, features='html.parser')
        for link in soup.find_all('a'):
            try:
                href = link['href']
            except KeyError:
                continue
            if '#' in href:
                href = href.split('#', 1)[0]
            if '../' in href:
                href = href.split('../', 1)[1]
            if root in href:
                links.add(href)
            elif 'https://' in href or 'http://' in href:
                continue
            else:
                links.add(f'{root}/{href}')
        return links

    async def is_rps_exceeded(self, root):
        while True:
            now = time.perf_counter()
            while self.timer[root]:
                if now - self.timer[root][0] > 1:
                    self.timer[root].popleft()
                else:
                    break
            if len(self.timer[root]) < self.max_rps:
                break
            await asyncio.sleep(0.05)
        self.timer[root].append(time.perf_counter())


async def on_message(message: IncomingMessage):
    with message.process():
        payload = json.loads(message.body.decode())
        domain = payload['data']['domain']
        author_id = payload['data']['author_id']

        try:
            stat = await Stat.objects.get(domain=domain)
        except DoesNotExist:
            pass
        else:
            t = (datetime.datetime.now() + datetime.timedelta(
                 seconds=crawl_repeat_time)).strftime('%Y-%m-%d %H:%M:%S')
            if stat.time < t or stat.pages_count == 0 and \
                                stat.status != 'Crawling':
                if stat.author_id == author_id:
                    return None
                else:
                    await Stat.objects.create(domain=domain, time=now(),
                        status=stat.status, author_id=author_id)
                    return None
        if 'https://' in domain:
            https = 1
        elif 'http://' in domain:
            https = 0
        else:
            await Stat.objects.create(domain=domain, time=now(),
                status='Error: protocol should be specified',
                author_id=author_id)
            return None

        await crawler.add_url(url=domain, author_id=author_id, https=https)


async def consumer():
    connection = await connect('amqp://crawler:crawler@localhost/',
                               loop=loop)
    channel = await connection.channel()
    queue = await channel.declare_queue('crawler', durable=True)
    await queue.consume(on_message)


crawler = Crawler(max_tasks=10, max_rps=3, max_depth=3)
loop.run_until_complete(crawler.crawl())
loop.create_task(consumer())
loop.run_forever()
