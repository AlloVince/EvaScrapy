# runner

## 何时读
改周期调度、APP_TASK 生成、分布式 seed。

## 内容
**职责：** `ScheduleCrawlerRunner`：动态加载 `evascrapy.spiders.{name}_spider` 中带 `name` 的 CrawlSpider 子类；按间隔 `crawl`；Twisted reactor 常驻。

**边界：** 不实现页面解析；一次性 crawl 也可用 scrapy CLI，不必经 runner。

**入口：** `evascrapy/runner.py`；`start.py` 调用 `run_crawler` + `schedule` + `start`。

**APP_TASK：** `interval_to_app_task(APP_STORAGE_SHUFFLE_INTERVAL)` — debug/hourly/daily/weekly/monthly 时间桶；可用 env `NOW` 注入时间戳（测试/回放）。

**调度间隔：** `APP_CRAWL_INTERVAL` 同上枚举 → APScheduler interval 秒数。

**分布式：** 若 `APP_DISTRIBUTED`：队列空则 `sadd` start_urls；否则复用 redis 中 `{name}:app_task`；并 `set` 当前 APP_TASK。

**雷区：** 已有 crawler 在跑则跳过新一轮；`get_spider_class` 取模块内**第一个**符合条件的类；spider 文件名必须 `{spider_name}_spider.py`。

## 相关
- 代码：`evascrapy/runner.py`、`start.py`
- `operations/runtime.md`
