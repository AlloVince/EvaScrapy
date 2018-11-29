# -*- coding: utf-8 -*-
import os
import time
import math
from scrapy.http import Response
from scrapy.spiders import CrawlSpider
from scrapy_redis.spiders import RedisCrawlSpider
from evascrapy.items import RawHtmlItem


class BaseSpider(RedisCrawlSpider if os.getenv('APP_DISTRIBUTED') else CrawlSpider):
    deep_start_urls = None
    deep_rules = None
    deep_allowed_domains = None

    def __init__(self, *a, **kw):
        if os.getenv('APP_RUN_DEEP'):
            if hasattr(self, 'deep_start_urls'):
                self.start_urls = self.deep_start_urls or self.start_urls
            if hasattr(self, 'deep_rules'):
                self.rules = self.deep_rules or self.rules
            if hasattr(self, 'deep_allowed_domains'):
                self.allowed_domains = self.deep_allowed_domains or self.allowed_domains
        super(BaseSpider, self).__init__(*a, **kw)

    def spider_idle(self):
        self.close(self, 'RedisCrawlSpider closed by spider idle')

    def handle_item(self, response: Response) -> RawHtmlItem:
        return RawHtmlItem(url=response.url, html=response.text, task=self.settings.get('APP_TASK'),
                           version=self.version, timestamp=math.floor(time.time()))
