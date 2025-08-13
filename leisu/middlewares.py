# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

import random
from scrapy.http import Response
from scrapy import signals
from collections import defaultdict


class LeisuSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

PROXY_LIST = [
    "socks5://127.0.0.1:1080",
    "socks5://127.0.0.1:1081",  # 你可以配置多个 Trojan 端口
    "socks5://127.0.0.1:1082",
    "socks5://127.0.0.1:1083",
    "socks5://127.0.0.1:1084",
    "socks5://127.0.0.1:1085"
]

def get_random_proxy():
    return random.choice(PROXY_LIST)

class RandomProxyMiddleware:
    def __init__(self, settings):
        self.proxies = PROXY_LIST
        self.max_retries = 6
        self.url_retry_count = defaultdict(int)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        proxy = random.choice(self.proxies)
        request.meta["proxy"] = proxy
        spider.logger.info(f"Using proxy: {proxy}")


    def process_exception(self, request, exception, spider):
        url = request.url
        self.url_retry_count[url] += 1
        if self.url_retry_count[url] >= self.max_retries:
            spider.logger.error(f"达到最大代理重试次数({self.max_retries}): {url}")
            spider.crawler.stats.inc_value('proxy/failed_urls')
            return None  # 彻底放弃
        """如果代理失效，换一个代理"""
        spider.logger.warning(f"Proxy {request.meta['proxy']} failed, retrying...")
        request.meta["proxy"] = get_random_proxy()
        return request

