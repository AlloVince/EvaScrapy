# EvaScrapy

基于 Scrapy 的**原始数据**抓取基础设施。只负责抓取与落盘/通知，ETL 由外部项目完成。

- 单机 / 分布式（scrapy-redis）
- 增量与全量（`APP_RUN_DEEP` + spider `deep_*`）
- 存储：本地文件 / 阿里云 OSS / S3（MinIO 客户端）
- 可选通知：Kafka、阿里云 MNS
- 周期调度：`start.py` + APScheduler
- Docker 运行

## 要求

- Python ≥ 3.14（见 `.python-version` / `pyproject.toml`）
- [uv](https://docs.astral.sh/uv/) 管理依赖

## 快速开始

```bash
uv sync
# 开发依赖（pytest 等）
uv sync --extra dev

# 根目录创建 .env（已 gitignore），按需配置
```

校验：

```bash
uv run pytest tests/
uv run scrapy list
```

## 运行

配置优先级：**环境变量 > `.env` > `settings.py` > Scrapy 默认**。

### 周期调度（推荐常驻）

```bash
uv run python start.py
APP_SPIDER=nyaa uv run python start.py
```

- 按 `APP_CRAWL_INTERVAL` 重复启动 crawl
- 未指定 `APP_TASK` 时，由 `APP_STORAGE_SHUFFLE_INTERVAL` 按时间桶生成

### 一次性抓取

```bash
uv run scrapy crawl <spider_name>
APP_TASK=full APP_RUN_DEEP=1 uv run scrapy crawl <spider_name>
```

### 全量（deep）

设 `APP_RUN_DEEP=1`。Spider 可定义：

- `deep_start_urls`
- `deep_rules`
- `deep_allowed_domains`

运行时优先使用上述字段覆盖普通配置。

### 分布式

基于 scrapy-redis。空闲时会退出（与 scrapy-redis 默认常驻不同），需先写入 start_urls：

```bash
redis-cli sadd <name>:start_urls https://example.com
APP_DISTRIBUTED=1 uv run scrapy crawl <name>

# 清理
redis-cli KEYS "<name>:*" | xargs redis-cli DEL
```

### Docker

```bash
docker run -e APP_TASK=full -e APP_SPIDER=your_spider \
  -v "$(pwd)/your_spider.py:/opt/htdocs/evascrapy/spiders/your_spider.py" \
  -it allovince/evascrapy:latest
```

镜像默认 `CMD python start.py`。业务 spider 可挂载进容器。

## 业务 Spider

- 路径：`evascrapy/spiders/{name}_spider.py`
- 继承 `BaseSpider`，设置 `name`、`version`、`allowed_domains`、`start_urls`、`rules`
- 调度入口用 `APP_SPIDER={name}`（不含 `_spider` 后缀）
- 注意：`.gitignore` 默认忽略 `evascrapy/spiders/*_spider.py`（业务 spider 通常不入库）

仓库内示例：`nyaa_spider.py`。

## Item 与存储

Item 族见 `evascrapy/items.py`，常用：

| 类型 | 说明 |
|---|---|
| `RawHtmlItem` | HTML；文件头为 HTML 注释元数据 + 正文 |
| `RawJsonItem` | JSON 包装 |
| `TorrentFileItem` | 种子；按 info_hash 路径；可选 ES 去重 |

文本类本地路径大致为：

```text
{APP_STORAGE_ROOT_PATH}/{spider.name}/{APP_TASK}/…  # URL md5 分片，深度 APP_STORAGE_DEPTH
```

- 相同 `APP_TASK` 再次抓取会覆盖同路径
- `APP_STORAGE=file|oss|s3` 切换后端

MQ 开启后，pipeline 发送指向存储 URI 的 ETL 命令消息（`etl:{spider}` / `etl:torrent`），供下游消费。

## 主要配置

完整说明见 [`docs/operations/config.md`](docs/operations/config.md)。常用项：

| 键 | 含义 | 默认 |
|---|---|---|
| `APP_TASK` | 存储分桶 | 空则由 runner 按时间生成 |
| `APP_SPIDER` | `start.py` 使用的蜘蛛名 | `demo` |
| `APP_RUN_DEEP` | 启用 deep_* | `false` |
| `APP_STORAGE` | `file` / `oss` / `s3` | `file` |
| `APP_STORAGE_ROOT_PATH` | 存储根 | `dl` |
| `APP_STORAGE_DEPTH` | 路径分片深度 | `3` |
| `APP_DISTRIBUTED` | scrapy-redis | `false` |
| `APP_CRAWL_INTERVAL` | 调度间隔：debug/hourly/daily/weekly/monthly | `weekly` |
| `APP_STORAGE_SHUFFLE_INTERVAL` | APP_TASK 时间桶 | `monthly` |
| `APP_MQ_NOTIFY_KAFKA` | Kafka 通知 | `false` |
| `APP_MQ_NOTIFY_MNS` | MNS 通知 | `false` |
| `APP_TIMEZONE` | 时区 | `Asia/Chongqing` |
| `APP_RANDOM_UA` | 随机 UA | `false` |
| `COOKIES_GLOBAL` | 全局 Cookie 字符串 | 空 |
| `REDIS_URL` | 分布式 Redis | 见 settings |
| `LOG_LEVEL` | `start.py` 日志级别 | `DEBUG` |

OSS / S3 / Kafka / MNS / ES 凭据类变量见 `evascrapy/settings.py`，**勿写入仓库**。

## 目录结构

```text
start.py                 # 调度入口
scrapy.cfg
evascrapy/
  settings.py            # 配置与条件装配 pipeline/middleware
  base_spider.py         # 爬虫基类（deep / 分布式）
  items.py
  pipelines.py           # 存储与 MQ、torrent ES 去重
  middlewares.py
  runner.py              # 周期调度
  spiders/               # 业务爬虫
docs/                    # 工程文档（按需阅读）
tests/
```

## 文档

更细的架构、模块、运维说明：

- 索引：[`docs/index.md`](docs/index.md)
- 架构：`docs/architecture/`
- 开发：`docs/development/`
- 运维：`docs/operations/`

AI/协作约定见根目录 `AGENTS.md`。

## 边界

**做：** 抓取编排、原始 Item、落盘/对象存储、可选 MQ 通知、分布式与周期调度钩子。

**不做：** 业务 ETL/解析入库、代理池、浏览器渲染、Stats API（未实现）。
