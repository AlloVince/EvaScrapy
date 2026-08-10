# logformatter

## 何时读
改 Scrapy 爬取/抓取/丢弃日志格式。

## 内容
**职责：** 自定义 `LOG_FORMATTER`：crawled/scraped/dropped 消息模板与级别（scraped 为 DEBUG）。

**边界：** 非业务日志；非 metrics。

**入口：** `evascrapy/logformatter.py`；settings `LOG_FORMATTER = 'evascrapy.logformatter.LogFormatter'`

## 相关
- 代码：`evascrapy/logformatter.py`
