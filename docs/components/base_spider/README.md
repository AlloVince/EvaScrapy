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
- deep 用于尽可能深入的覆盖性抓取；非 deep 用于最新数据的周期增量抓取。`BaseSpider` 只负责字段切换，不替业务 Spider 猜测数量上限、分页策略或关联语义
- Spider 应优先依赖 Scrapy/CrawlSpider 的原生请求发现、去重、调度和 pipeline；只有站点特有边界无法由框架表达时才增加自定义逻辑
- `spider_idle`：直接 `close`（改变 scrapy-redis 默认常驻行为）
- `handle_item` → `RawHtmlItem`；`handle_torrent` → `TorrentFileItem`（Referer 作 from_url）

**依赖：** scrapy、scrapy-redis、items  
**雷区：** 基类在 import 时定死父类；子类应定义 `version`、`name`；deep 字段需在子类预先声明。

## 相关
- 代码：`evascrapy/base_spider.py`
- spiders：`components/spiders/README.md`
