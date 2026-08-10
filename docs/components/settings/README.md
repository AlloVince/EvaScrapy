# settings

## 何时读
改配置项、装配条件、env 覆盖行为。

## 内容
**职责：** 加载 `.env`；定义 APP_* 与第三方凭据默认值；按开关填充 `DOWNLOADER_MIDDLEWARES` / `ITEM_PIPELINES` / scrapy-redis。

**边界：** 不做爬取与存储 IO；密钥只从环境读取，不写死。

**入口：** `evascrapy/settings.py`；`scrapy.cfg` → `default = evascrapy.settings`

**覆盖规则：**
1. 模块内默认常量
2. `load_dotenv(.../../.env)`
3. 遍历 `os.environ`：键全大写且属于本模块 globals / scrapy default_settings / scrapy_redis.defaults → 写入
4. 若干布尔键经 `_as_bool` 规范化

**条件装配（摘要）：**
- `COOKIES_GLOBAL` → 禁用默认 CookiesMiddleware，启用 `GlobalCookiesMiddleware`
- `APP_RANDOM_UA` → `scrapy_fake_useragent` Random UA
- `APP_STORAGE`：`file`→LocalFile，`oss`→AliyunOss，`s3`→AwsS3
- `TORRENT_FILE_ELASTIC_DUPE` → ElasticDupePipeline(100)
- `APP_MQ_NOTIFY_KAFKA` / `APP_MQ_NOTIFY_MNS` → 对应 pipeline(600)
- `APP_DISTRIBUTED` → scrapy-redis Scheduler/DupeFilter 等

**依赖：** python-dotenv、scrapy、scrapy-redis  
**雷区：** 仅覆盖「已知名」的大写 env；生造 env 名不会自动进 settings。布尔必须能被 `_as_bool` 识别。

## 相关
- 代码：`evascrapy/settings.py`
- 配置表：`docs/operations/config.md`
