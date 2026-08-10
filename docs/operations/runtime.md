# 运行与排障

## 何时读
进程怎么跑、常见失败怎么查。

## 内容

### 进程模型
- Scrapy/Twisted 单进程并发；`CONCURRENT_REQUESTS` 等需启动前设定
- `start.py`：先跑一轮再 APScheduler，reactor.run 阻塞
- 分布式：多进程/多机共享 Redis 队列；空闲退出，需外部或 runner 补 start_urls

### 存储布局
- HTML 等：`{APP_STORAGE_ROOT_PATH}/{spider.name}/{APP_TASK}/` + url md5 分片
- Torrent：`{TORRENT_FILE_PIPELINE_ROOT_PATH}/` + info_hash 分片
- 相同 APP_TASK 再次抓取会覆盖同路径对象

### 排障入口
| 现象 | 方向 |
|---|---|
| 无 pipeline 写入 | APP_STORAGE 与 ITEM_PIPELINES 装配 |
| 分布式不退出/立刻退 | spider_idle 设计为退出；检查 redis 队列 |
| 新一轮不跑 | runner 已有 crawler → SKIPPED 日志 |
| APP_TASK 不对 | STORAGE_SHUFFLE_INTERVAL / 时区 / 分布式 redis app_task |
| Cookie 无效 | COOKIES_GLOBAL 格式 |
| 导入 BaseSpider 基类不对 | APP_DISTRIBUTED 需在 import 前设置 |

### 观测
- 日志：Scrapy + 自定义 LogFormatter；runner 有 SPIDER.*.ROUND_* 日志
- 无内置 Stats API（README TODO）

## 相关
- `runner`、`base_spider`、`config.md`
