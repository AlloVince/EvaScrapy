import logging
import importlib
import inspect
import os
import time
import pytz
from twisted.internet import reactor
from scrapy.spiders import CrawlSpider
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from datetime import datetime
from scrapy_redis.connection import get_redis
from apscheduler.schedulers.twisted import TwistedScheduler

logger = logging.getLogger(__name__)


class ScheduleCrawlerRunner:

    @staticmethod
    def interval_to_app_task(interval='daily'):
        now = datetime.fromtimestamp(
            os.getenv('NOW', time.time()),
            pytz.timezone(os.getenv('APP_TIMEZONE', 'Asia/Chongqing')),
        )
        # %Y-%m-%d %H:%M:%S %z
        # applogger.debug('Interval %s convert to APP_TASK at %s', interval, now)
        formats = {
            'debug': lambda: now.strftime('%Y%m%d_%H%M'),
            'hourly': lambda: now.strftime('%Y%m%d_%H'),
            'daily': lambda: now.strftime('%Y%m%d'),
            'weekly': lambda: now.strftime('%YW%U'),
            'monthly': lambda: now.strftime('%Y%m'),
        }
        return formats[interval]()

    def __init__(self, spider_name: str):
        self.settings = get_project_settings()
        self.crawler = CrawlerRunner(self.settings)
        self.round = 0
        self.spider_name = spider_name

    def get_spider_class(self, spider_name: str):
        spider_module = importlib.import_module('evascrapy.spiders.' + spider_name + '_spider')
        spider_class = None
        for name, spider_member in inspect.getmembers(spider_module):
            if inspect.isclass(spider_member) \
                    and issubclass(spider_member, CrawlSpider) \
                    and hasattr(spider_member, 'name') \
                    and spider_member.name:
                spider_class = spider_member
                break
        return spider_class

    def schedule(self):
        scheduler = TwistedScheduler({
            'apscheduler.timezone': self.settings.get('APP_TIMEZONE')
        })

        # TODO: use random interval
        switch = {
            'debug': lambda: scheduler.add_job(self.run_crawler, 'interval', seconds=3),
            'hourly': lambda: scheduler.add_job(self.run_crawler, 'interval', seconds=3600),
            'daily': lambda: scheduler.add_job(self.run_crawler, 'interval', seconds=86400),
            'weekly': lambda: scheduler.add_job(self.run_crawler, 'interval', seconds=86400 * 7),
            'monthly': lambda: scheduler.add_job(self.run_crawler, 'interval', seconds=86400 * 30),
        }

        switch[self.settings.get('APP_CRAWL_INTERVAL')]()
        scheduler.start()

    def run_crawler(self):
        spider_class = self.get_spider_class(self.spider_name)

        if os.getenv('APP_DISTRIBUTED'):
            redis = get_redis(url=self.crawler.settings.get('REDIS_URL'))

        if len(list(self.crawler.crawlers)) < 1:
            self.crawler.settings.set('APP_TASK',
                                      ScheduleCrawlerRunner.interval_to_app_task(
                                          self.crawler.settings.get('APP_STORAGE_SHUFFLE_INTERVAL')))

            if os.getenv('APP_DISTRIBUTED'):
                if redis.zcount(spider_class.name + ':requests', 0, 100) < 1:
                    for start_url in spider_class.start_urls:
                        redis.sadd(spider_class.name + ':start_urls', start_url)
                else:
                    self.crawler.settings.set('APP_TASK', redis.get(spider_class.name + ':app_task').decode('utf-8'))

            logger.info(
                '[SPIDER.%s.%s.DIS_%s.ROUND_%s] started, APP_CRAWL_INTERVAL: %s, APP_STORAGE_SHUFFLE_INTERVAL: %s',
                spider_class.name,
                self.crawler.settings.get('APP_TASK'),
                os.getenv('APP_DISTRIBUTED'),
                self.round,
                self.crawler.settings.get('APP_CRAWL_INTERVAL'),
                self.crawler.settings.get('APP_STORAGE_SHUFFLE_INTERVAL'))
            self.crawler.crawl(spider_class)
            if os.getenv('APP_DISTRIBUTED'):
                redis.set(spider_class.name + ':app_task', self.crawler.settings.get('APP_TASK'))
            self.round += 1
        else:
            logger.info(
                'NEW ROUND SKIPPED BY [SPIDER.%s.%s.DIS_%s.ROUND_%s]',
                spider_class.name,
                self.crawler.settings.get('APP_TASK'),
                os.getenv('APP_DISTRIBUTED'),
                self.round)

    def start(self):
        reactor.run()
