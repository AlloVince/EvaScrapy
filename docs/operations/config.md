# 配置

## 何时读
部署或本地调参；**不写密钥原文**。

## 内容
优先级：环境变量 > `.env` > `settings.py` > scrapy/scrapy-redis 默认。

### 应用
| 键 | 含义 | 默认（settings） |
|---|---|---|
| APP_TASK | 存储分桶名 | None（runner 按 shuffle 间隔生成） |
| APP_RUN_DEEP | 全量 deep_* | False |
| APP_SPIDER | start.py 蜘蛛名 | demo |
| APP_MQ_NOTIFY_KAFKA | Kafka 通知 | False |
| APP_MQ_NOTIFY_MNS | MNS 通知 | False |
| APP_TIMEZONE | 时区 | Asia/Chongqing |
| APP_STORAGE | file\|oss\|s3 | file |
| APP_STORAGE_DEPTH | URL 路径分片深度 | 3 |
| APP_STORAGE_ROOT_PATH | 本地/对象键前缀根 | dl |
| APP_DISTRIBUTED | scrapy-redis | False |
| APP_CRAWL_INTERVAL | 调度间隔枚举 | weekly |
| APP_STORAGE_SHUFFLE_INTERVAL | APP_TASK 时间桶 | monthly |
| APP_RANDOM_UA | 随机 UA | False |
| S3_DUPEFILTER_ENABLED | 是否执行 S3/SeaweedFS 对象存在检查 | False |
| S3_DUPEFILTER_ROOT_PATH | S3 去重对象稳定根路径 | None（启用时必填） |
| S3_DUPEFILTER_DEPTH | S3 去重 MD5 分片深度 | None（回退 APP_STORAGE_DEPTH） |
| COOKIES_GLOBAL | 全局 Cookie 串 | None |
| REDIS_URL | 分布式 Redis | redis://docker.for.mac.host.localhost:6379 |
| LOG_LEVEL | start.py 日志级别 | DEBUG（start 内） |
| NOW | 覆盖 runner 当前时间戳 | 未设则 time.time() |

### Torrent / ES
| 键 | 含义 |
|---|---|
| TORRENT_FILE_PIPELINE_ROOT_PATH | 默认 dl/info_hash |
| TORRENT_FILE_PIPELINE_DEPTH | 默认 3 |
| TORRENT_FILE_ELASTIC_DUPE | 是否 ES 去重 |
| TORRENT_FILE_ELASTIC_DUPE_URL | ES URL（可含 user:pass） |
| TORRENT_FILE_ELASTIC_DUPE_INDICE | index 名 |
| TORRENT_FILE_ELASTIC_DUPE_DOCTYPE | 历史字段，存在于 settings |

`TORRENT_FILE_PIPELINE_ROOT_PATH` 为空时，torrent 对象直接以 `depth` 层 hash 目录写入存储根；例如 depth 3 为 `2e/d3/02/<剩余 hash>.torrent`。非空时仍将该值作为前缀。

### OSS / S3 / Kafka / MNS
见 `settings.py` 中 `OSS_*`、`AWS_S3_*`、`KAFKA_*`、`MNS_*`。凭据仅经环境注入。

启用 S3DupeFilter 还需显式设置：

```ini
DUPEFILTER_CLASS=evascrapy.dupefilters.S3DupeFilter
```

不配置 `DUPEFILTER_CLASS` 时保持 Scrapy 官方 `RFPDupeFilter`，不会执行 S3 去重。

### 与 README 差异（以代码为准）
- README `APP_MQ_NOTIFY` → 代码为 `APP_MQ_NOTIFY_KAFKA` / `APP_MQ_NOTIFY_MNS`
- 存储已支持 `s3`

## 相关
- `evascrapy/settings.py`、`deploy.md`、`runtime.md`
