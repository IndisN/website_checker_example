# coding: utf-8
# took example for async UrlFetcher from here:
# https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html

import time
import logging
import re
from collections import namedtuple
import asyncio
import pickle

from aiohttp import ClientSession

from util.utils import HtmlDataParser, md5_long

UrlData = namedtuple('UrlData', 'url pattern')


class ResponseData:
    def __init__(self, url, status, response_time, doc_body=None, pattern=None):
        self.url = url
        self.status = status
        self.response_time = response_time
        self.doc_body = doc_body
        self.re_pattern = pattern
        self.doc_text = None
        self.re_content = None

    def match_pattern(self):
        """
        >>> response = ResponseData(
        ...     None, None, None,
        ...     doc_body='<head>Some head text</head><script>some script</script><body>Body contains this:   abrakadabra</body>',
        ...     pattern=None,
        ... )
        >>> response.match_pattern()
        >>> response.doc_text is None
        True
        """
        if self.doc_body is not None and self.re_pattern is not None:
            parser = HtmlDataParser()
            parser.feed(self.doc_body)
            parser.close()
            self.doc_text = parser.text()

            match = re.search(self.re_pattern, self.doc_text)
            self.re_content = match.group(0) if match is not None else None

    def to_msg(self):
        return pickle.dumps(dict(
            url_md5=md5_long(self.url.encode('utf-8')),
            status_code=self.status,
            response_time=self.response_time,
            pattern=self.re_pattern,
            match=self.re_content,
        ))


class UrlsFetcher:
    def __init__(self, num_sockets=1000):
        logging.info('init UrlFetcher with %s sockets', num_sockets)
        self.semaphore = asyncio.Semaphore(num_sockets)

    @staticmethod
    async def _fetch(url_data: UrlData, session):
        start_time = time.time()
        async with session.get(url_data.url) as response:
            doc_body = await response.text()
            response_time = time.time() - start_time
            return ResponseData(
                url=url_data.url,
                status=response.status,
                response_time=response_time,
                doc_body=doc_body,
                pattern=url_data.pattern,
            )

    async def _bound_fetch(self, url_data: UrlData, session):
        # Getter function with semaphore.
        async with self.semaphore:
            return await self._fetch(url_data, session)

    async def _run(self, urls_data):
        # Create client session that will ensure we don't open new connection
        # per each request.
        tasks = []

        async with ClientSession() as session:
            for url_data in urls_data:
                # pass Semaphore and session to every GET request
                task = asyncio.ensure_future(self._bound_fetch(url_data, session))
                tasks.append(task)

            return await asyncio.gather(*tasks)

    def process(self, urls_data):
        logging.info('start process for %s urls', len(urls_data))
        loop = asyncio.get_event_loop()

        future = asyncio.ensure_future(self._run(urls_data))
        loop.run_until_complete(future)
        return future.result()
