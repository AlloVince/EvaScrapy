# -*- coding: utf-8 -*-
from dotenv import load_dotenv
import os
# import random
# import string
from scrapy.settings import default_settings
from scrapy_redis import defaults

load_dotenv(dotenv_path=os.path.dirname(os.path.realpath(__file__)) + '/../.env')

# LOG_LEVEL = logging.INFO

# CUSTOM SETTINGS
# APP_TASK = ''.join(random.choice(string.ascii_letters) for m in range(5))
APP_TASK = None
APP_RUN_DEEP = False
APP_SPIDER = 'demo'
APP_MQ_NOTIFY_KAFKA = False
APP_MQ_NOTIFY_MNS = False
APP_TIMEZONE = 'Asia/Chongqing'
APP_STORAGE = 'file'
APP_STORAGE_DEPTH = 3
APP_STORAGE_ROOT_PATH = 'dl'
APP_DISTRIBUTED = False
APP_CRAWL_INTERVAL = 'weekly'
APP_STORAGE_SHUFFLE_INTERVAL = 'monthly'

APP_TORRENT_PIPELINE = False
APP_TORRENT_PIPELINE_ROOT_PATH = 'dl/info_hash'
APP_TORRENT_PIPELINE_DEPTH = 3

OSS_ACCESS_KEY_ID = None
OSS_ACCESS_KEY_SECRET = None
OSS_ENDPOINT = 'http://oss-cn-hangzhou.aliyuncs.com'
OSS_BUCKET = None

MNS_ACCOUNT_ENDPOINT = None
MNS_ACCESSKEY_ID = None
MNS_ACCESSKEY_SECRET = None
MNS_QUEUE_NAME = None

KAFKA_SSL_ENABLE = False
KAFKA_SSL_CERT_PATH = False
KAFKA_SECURITY_PROTOCOL = 'SASL_SSL'
KAFKA_SASL_MECHANISM = 'PLAIN'
KAFKA_SASL_PLAIN_USERNAME = ''
KAFKA_SASL_PLAIN_PASSWORD = ''
KAFKA_SASL_CA_CERT_LOCATION = 'aliyun-kafka-ca-cert'
KAFKA_SERVER_STRING = 'kafka-cn-internet.aliyun.com:8080'
KAFKA_TOPIC = None

AWS_S3_ENDPOINT = None
AWS_S3_ACCESS_KEY = ''
AWS_S3_REGION = None
AWS_S3_ACCESS_SECRET = ''
AWS_S3_ACCESS_SECURE = False
AWS_S3_DEFAULT_BUCKET = ''

# SCRAPY SETTINGS
BOT_NAME = 'evascrapy'

SPIDER_MODULES = ['evascrapy.spiders']
NEWSPIDER_MODULE = 'evascrapy.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'evascrapy (+http://www.yourdomain.com)'

# Obey robots.txt rules
# ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'evascrapy.middlewares.AppstoreSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
}

ITEM_PIPELINES = {
}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }


# Scrapy-Redis Settings
# REDIS_URL = 'redis://localhost:6379'
REDIS_URL = 'redis://docker.for.mac.host.localhost:6379'

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Overwrite all local variables from env
for k, v in dict(os.environ).items():
    if k.isupper() and (k in globals() or k in vars(default_settings) or k in vars(defaults)):
        globals()[k] = os.getenv(k, v)

if APP_STORAGE == 'file':
    ITEM_PIPELINES['evascrapy.pipelines.HtmlFilePipeline'] = 300
elif APP_STORAGE == 'oss':
    ITEM_PIPELINES['evascrapy.pipelines.AliyunOssPipeline'] = 300
elif APP_STORAGE == 's3':
    ITEM_PIPELINES['evascrapy.pipelines.AwsS3Pipeline'] = 300

if APP_TORRENT_PIPELINE:
    ITEM_PIPELINES['evascrapy.pipelines.TorrentFilePipeLine'] = 1
    FILES_STORE = os.getenv('FILES_STORE') or os.path.dirname(
        os.path.realpath(__file__)) + '/../' + APP_TORRENT_PIPELINE_ROOT_PATH

if APP_MQ_NOTIFY_KAFKA:
    ITEM_PIPELINES['evascrapy.pipelines.KafkaPipeline'] = 600

if APP_MQ_NOTIFY_MNS:
    ITEM_PIPELINES['evascrapy.pipelines.AliyunMnsPipeline'] = 600

if APP_DISTRIBUTED:
    SCHEDULER = 'scrapy_redis.scheduler.Scheduler'
    DUPEFILTER_CLASS = 'scrapy_redis.dupefilter.RFPDupeFilter'
    SCHEDULER_PERSIST = False
    REDIS_START_URLS_AS_SET = True
    # ITEM_PIPELINES['scrapy_redis.pipelines.RedisPipeline'] = 300
