# 架构概览

## 何时读
理解系统定位、主路径、目录与运行形态。

## 内容
EvaScrapy：Scrapy 上的**原始数据抓取**基础设施。只抓 raw（HTML/JSON/torrent 等），ETL 在外部项目。

### 能力（代码已实现）
- 单机 `CrawlSpider` / 分布式 `scrapy-redis`（`APP_DISTRIBUTED`）
- 增量 vs 全量：`APP_RUN_DEEP` 切换 spider 的 `deep_*` 配置
- 存储：`file` | `oss` | `s3`（Minio 客户端）
- 通知：Kafka / 阿里云 MNS（可选 pipeline）
- torrent：info_hash 路径；可选 ES 去重
- 周期运行：`start.py` + `ScheduleCrawlerRunner`（APScheduler + Twisted）

### 主数据流
```
env/.env → settings 装配 middleware/pipeline/redis
       → Spider(BaseSpider) 请求与解析
       → Item(QueueBasedItem 族)
       → Pipelines（存储 → 可选 MQ）
       → 外部 ETL 消费路径/消息
```

### 关键结构
| 路径 | 角色 |
|---|---|
| `start.py` | 调度进程入口 |
| `scrapy.cfg` | Scrapy 项目指向 `evascrapy.settings` |
| `evascrapy/settings.py` | 配置与条件装配 |
| `evascrapy/base_spider.py` | 爬虫基类 |
| `evascrapy/items.py` | Item 与序列化/路径 |
| `evascrapy/pipelines.py` | 存储与通知 |
| `evascrapy/middlewares.py` | Global cookies |
| `evascrapy/runner.py` | 周期调度与分布式 seed |
| `evascrapy/spiders/` | 业务爬虫（`{name}_spider.py`） |
| `dl/` | 默认本地存储根（运行产物） |

### 两种运行方式
1. **调度**：`python start.py` — 按 `APP_CRAWL_INTERVAL` 重复 crawl，`APP_TASK` 由 `APP_STORAGE_SHUFFLE_INTERVAL` 生成
2. **一次性 Scrapy CLI**：`scrapy crawl <name>` — 可配 `APP_TASK`、`APP_RUN_DEEP=1` 等

验证于：2026-08-10 首扫

## 相关
- 代码：`evascrapy/`、`start.py`
- 其它：`boundaries.md`、`operations/runtime.md`
