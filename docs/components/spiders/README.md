# spiders

## 何时读
对接外部项目维护的 Spider。

## 内容
**职责：** 说明外部 Spider 接入框架并产出 Item 的通用约定。

**边界：** 具体网站 Spider 的正式版本及站点经验由每次开工时经用户确认的业务项目维护；候选项目来自本项目同级的 `*.crawler` 目录，即使只有一个也必须确认。本项目可保留被 `.gitignore` 隔离的本地调试副本；用户发送 `.ai/workflow/end.md` 时复制改动到已确认的业务项目，同步后不删除本地副本。具体站点规则与排障经验不得写入本项目文档。Spider 不直接写存储，经 Item + pipeline。

**约定：**
- 外部项目按运行时要求提供 `evascrapy/spiders/{name}_spider.py`（本项目 `.gitignore` 忽略该模式）
- 类：继承 `BaseSpider`；设 `name`、`version`、`allowed_domains`、`start_urls`、`rules`
- 全量：可选 `deep_start_urls` / `deep_rules` / `deep_allowed_domains`
- runner 通过 `APP_SPIDER={name}` 加载（不含 `_spider` 后缀）

**依赖：** base_spider、items、scrapy

## 相关
- 代码：`evascrapy/spiders/`
- `base_spider`、`runner`
