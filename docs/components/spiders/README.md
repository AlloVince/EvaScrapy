# spiders

## 何时读
对接外部项目维护的 Spider。

## 内容
**职责：** 说明外部 Spider 接入框架并产出 Item 的通用约定。

**边界：** 具体网站 Spider 的正式版本及站点经验由每次开工时经用户确认的业务项目维护；候选项目来自本项目同级的 `*.crawler` 目录，即使只有一个也必须确认。日常优先修改本项目内、被 `.gitignore` 隔离的本地调试副本；用户发送 `.ai/workflow/end.md` 时才复制改动到已确认的业务项目，同步后必须保留本地副本，不删除或移动本项目内的 Spider 文件。具体站点规则与排障经验不得写入本项目文档。Spider 不直接写存储，经 Item + pipeline。

**开发与同步规则：** 具体网站 Spider 必须以本项目内、被 `.gitignore` 隔离的本地调试副本为唯一开发源，修改、调试和测试只能在本项目内进行，本项目副本始终保持最新。只有用户明确启动或发送 `.ai/workflow/end.md` 时才复制最新 Spider 到已确认的业务项目，未明确启动前禁止同步；同步后必须保留本地副本，不删除或移动本项目内的 Spider 文件。

**约定：**
- 外部项目按运行时要求提供 `evascrapy/spiders/{name}_spider.py`（本项目 `.gitignore` 忽略该模式）
- 类：继承 `BaseSpider`；设 `name`、`version`、`allowed_domains`、`start_urls`、`rules`
- 全量：可选 `deep_start_urls` / `deep_rules` / `deep_allowed_domains`
- runner 通过 `APP_SPIDER={name}` 加载（不含 `_spider` 后缀）

### deep 与非 deep

`deep` 是覆盖性抓取模式，目标是尽可能深入地发现历史数据和站点关联数据，用于首次建库、补历史或重抓。它不等于无限递归，仍必须有明确的 URL、分页和资源边界。

非 deep 是周期增量模式，目标是每轮只读取来源中最新的一段数据，控制请求量和运行成本；通常由 `start.py` 常驻进程按 `APP_CRAWL_INTERVAL` 重复启动。单轮 crawl 结束是正常的，常驻的是调度进程，不应把一次 crawl 的自然结束当成整个服务退出。非 deep 不能因为持久化入口 fingerprint 或旧 JOBDIR 而跳过最新数据。

Spider 通过 `APP_RUN_DEEP` 选择普通字段或对应的 `deep_*` 字段。普通字段必须代表日常增量路径，`deep_*` 字段才扩展覆盖范围；如果两者实际没有差异，应明确保持一致，不要为了形式增加第二套逻辑。

### 简洁性与框架优先

优先使用 Scrapy/CrawlSpider 的原生能力：`start_urls`、`Rule`、`LinkExtractor`、scheduler、dupefilter、`JOBDIR`、retry、cookies、throttle、并发控制和 Item pipeline。只有在原生机制无法表达网站特有语义时，才编写自定义请求编排。自定义代码应集中于站点 URL 白名单、分页/关联关系、页面完整性校验，以及 deep/非 deep 的业务边界；不要手动复制框架已有的链接发现、去重、调度或存储行为。通用能力发现重复需求时，应修改 EvaScrapy 框架，而不是继续堆积到单个业务 Spider。

**依赖：** base_spider、items、scrapy

## 相关
- 代码：`evascrapy/spiders/`
- `base_spider`、`runner`
