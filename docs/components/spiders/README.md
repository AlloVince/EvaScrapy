# spiders

## 何时读
新增/修改业务爬虫或示例。

## 内容
**职责：** 站点规则、LinkExtractor、callback，产出 Item。

**边界：** 不直接写存储；经 Item + pipeline。

**约定：**
- 文件：`evascrapy/spiders/{name}_spider.py`（**注意** `.gitignore` 忽略 `evascrapy/spiders/*_spider.py`）
- 类：继承 `BaseSpider`；设 `name`、`version`、`allowed_domains`、`start_urls`、`rules`
- 全量：可选 `deep_start_urls` / `deep_rules` / `deep_allowed_domains`
- runner 通过 `APP_SPIDER={name}` 加载（不含 `_spider` 后缀）

**仓库内示例：** `nyaa_spider.py`（nyaa/sukebei 列表抽 torrent）。是否生产使用待确认。

**依赖：** base_spider、items、scrapy

## 相关
- 代码：`evascrapy/spiders/`
- `base_spider`、`runner`
