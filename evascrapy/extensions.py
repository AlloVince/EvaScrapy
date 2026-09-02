import json
import logging
import os
from datetime import datetime

from scrapy import signals
from twisted.internet.task import LoopingCall


logger = logging.getLogger("evascrapy.spider_stats")


class SpiderStatsExtension:
    """Emit small, machine-readable lifecycle and progress records."""

    def __init__(self, crawler):
        self.crawler = crawler
        self.interval = crawler.settings.getint("SPIDER_STATS_INTERVAL", 60)
        self.loop = None
        self.run_id = None

    @classmethod
    def from_crawler(cls, crawler):
        extension = cls(crawler)
        crawler.signals.connect(extension.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(extension.spider_closed, signal=signals.spider_closed)
        return extension

    def spider_opened(self, spider):
        self.run_id = datetime.now().astimezone().isoformat()
        self._emit("spider_opened", spider)
        if self.interval > 0:
            self.loop = LoopingCall(self._emit, "spider_progress", spider)
            self.loop.start(self.interval, now=False)

    def spider_closed(self, spider, reason):
        if self.loop and self.loop.running:
            self.loop.stop()
        self._emit("spider_run_closed", spider, finish_reason=reason)

    def _emit(self, event, spider, finish_reason=None):
        stats = self.crawler.stats.get_stats()
        record = {
            "event": event,
            "spider": spider.name,
            "service": os.getenv("APP_SERVICE_NAME") or os.getenv("HOSTNAME"),
            "run_started_at": self.run_id,
            "task": spider.settings.get("APP_TASK"),
            "at": datetime.now().astimezone().isoformat(),
            "finish_reason": finish_reason or stats.get("finish_reason"),
            "requests": stats.get("downloader/request_count", 0),
            "responses": stats.get("response_received_count", 0),
            "items_scraped": stats.get("item_scraped_count", 0),
            "items_dropped": stats.get("item_dropped_count", 0),
            "download_errors": stats.get("downloader/exception_count", 0),
            "retries": stats.get("retry/count", 0),
            "duplicates": stats.get("dupefilter/filtered", 0),
            "spider_exceptions": stats.get("spider_exceptions/count", 0),
            "response_status": self._status_counts(stats),
            "duration_seconds": stats.get("elapsed_time_seconds"),
        }
        logger.info("SPIDER_STATS %s", json.dumps(record, ensure_ascii=False, separators=(",", ":")))

    @staticmethod
    def _status_counts(stats):
        prefix = "downloader/response_status_count/"
        return {
            key[len(prefix):]: value
            for key, value in stats.items()
            if key.startswith(prefix)
        }
