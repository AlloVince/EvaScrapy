# base_spider

## 何时读
改爬虫基类、deep 模式、分布式基类选择。

## 内容
**职责：** 统一业务 Spider 基类；deep 配置覆盖；提供 `handle_item` / `handle_torrent`；分布式时空闲关闭。

**边界：** 不含具体站点 LinkExtractor/规则。

**入口：** `evascrapy/base_spider.py` → `BaseSpider`

**行为要点：**
- 类定义时：`APP_DISTRIBUTED` 为真则继承 `RedisCrawlSpider`，否则 `CrawlSpider`（读 **os.getenv**，非 settings 对象）
- `APP_RUN_DEEP`：用 `deep_start_urls` / `deep_rules` / `deep_allowed_domains` 覆盖对应属性（若存在）
- `spider_idle`：直接 `close`（改变 scrapy-redis 默认常驻行为）
- `handle_item` → `RawHtmlItem`；`handle_torrent` → `TorrentFileItem`（Referer 作 from_url）

**依赖：** scrapy、scrapy-redis、items  
**雷区：** 基类在 import 时定死父类；子类应定义 `version`、`name`；deep 字段需在子类预先声明。

## 相关
- 代码：`evascrapy/base_spider.py`
- spiders：`components/spiders/README.md`
