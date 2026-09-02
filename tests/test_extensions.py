import json

from evascrapy.extensions import SpiderStatsExtension


class FakeStats:
    def __init__(self, values):
        self.values = values

    def get_stats(self):
        return self.values


class FakeSettings:
    def getint(self, name, default):
        return default

    def get(self, name):
        return "task-1" if name == "APP_TASK" else None


class FakeCrawler:
    settings = FakeSettings()
    stats = FakeStats({
        "downloader/request_count": 12,
        "response_received_count": 10,
        "item_scraped_count": 4,
        "downloader/response_status_count/200": 9,
        "downloader/response_status_count/404": 1,
        "finish_reason": "finished",
    })


class FakeSpider:
    name = "example"
    settings = FakeSettings()


def test_status_counts_and_record(caplog):
    extension = SpiderStatsExtension(FakeCrawler())
    with caplog.at_level("INFO", logger="evascrapy.spider_stats"):
        extension._emit("spider_progress", FakeSpider())

    record = json.loads(caplog.records[-1].message.removeprefix("SPIDER_STATS "))
    assert record["event"] == "spider_progress"
    assert record["items_scraped"] == 4
    assert record["response_status"] == {"200": 9, "404": 1}
    assert record["finish_reason"] == "finished"
