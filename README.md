# EvaScrapy

EvaScrapy 是一个基于Scrapy的数据抓取项目， 在Scrapy的基础上扩充了常用的抓取场景，包括

- 支持分布式抓取
- 支持全量抓取和增量更新
- 抓取结果可以存储到本地磁盘或阿里云OSS
- 抓取结果通过消息队列实时通知
- 支持 Docker

EvaScrapy 仅支持抓取原始数据， 数据的ETL通过其他项目完成

## 环境安装

安装 pyenv

```
brew update
brew install pyenv
```

安装 Python3

```
pyenv install 3.6.5
pyenv global 3.6.5
```

安装项目依赖

```
pip install -r requirements.txt
```

## 项目运行

更新监控

```shell
python start.py
```

完整抓取

```shell
APP_TASK=full APP_RUN_DEEP=1 scrapy crawl xxx
```

项目配置可以通过环境变量或`.env`两种方式设置, 配置优先级

环境变量 > `.env` > settings.py > scrapy default settings

## 完整抓取

环境变量 `APP_RUN_DEEP`=1

SpiderClass中定义

- `deep_start_urls`
- `deep_rules`
- `deep_allowed_domains`

在运行时会优先使用`deep_*`作为爬虫配置

## Item设计

仅支持一种Item，Item属性有

- url
- version
- html

Item 存储格式为

```
<!--key1:value1-->
<!--key2:value2-->
<html>
raw html content
</html>
```

## 存储方式

支持 本地文件 / Aliyun OSS 两种存储方式

本地文件存储位置 `dl/{spider_name}/{app_task}`

每次 spider 启动需要指定 `APP_TASK` 来决定存储位置

`APP_TASK` 可以通过环境变量 `APP_TASK` 指定，不指定则根据规则生成

`APP_TASK` 相同，则第二次的抓取会覆盖更新上一次的抓取结果

文件根据 url md5 hash 按 `APP_STORAGE_DEPTH` 层数保存

指定环境变量`APP_STORAGE=oss`可将存储切换为oss

### `APP_TASK` 生成规则

如果未指定`APP_TASK`，会根据当前时间及更新频率自动生成APP_TASK

## 单机运行

Scrapy 基于单进程单线程的Twisted（Reactor模型）实现并发

单机运行前就必须指定并发数 `CONCURRENT_REQUESTS`, 中途无法动态扩容

单机运行一旦退出后无法从断点恢复, 只能通过指定上次的 `APP_TASK` 重新抓取数据到同一目录

## 分布式运行

分布式运行基于 `Scrapy-Redis`， 并更改了退出行为， `Scrapy-Redis` 默认爬虫空闲时不会退出， EvaScrapy在爬虫空闲时会退出，因此需要先向Redis插入数据

```
redis-cli lpush demo:start_urls https://avnpc.com
redis-cli lpush wandoujia:start_urls http://www.wandoujia.com/category/app
redis-cli lpush big5:start_urls http://mirlab.org/jang/books/html/metaCharset.asp
APP_DISTRIBUTED=1 scrapy crawl demo
```

## 配置文件/环境变量

```python
# 抓取任务名称, 影响存储位置
APP_TASK = None

# 默认启动Spider名
APP_SPIDER = 'demo'

# 抓取完成后是否使用消息队列通知
APP_MQ_NOTIFY = False

# 时区
APP_TIMEZONE = 'Asia/Chongqing'

# 抓取结果存储方式 file | oss
APP_STORAGE = 'file'

# 存储深度
APP_STORAGE_DEPTH = 3

# 存储根目录
APP_STORAGE_ROOT_PATH = 'dl'

# 是否使用分布式抓取
APP_DISTRIBUTED = False

# 爬虫重新启动频率
APP_CRAWL_INTERVAL = 'weekly'

# 存储空间更迭频率
APP_STORAGE_SHUFFLE_INTERVAL = 'monthly'
```

## 代理池

TO BE DONE

## TODO:

- Content hash
- Ajax support
- JSON API support
- Stats API

Refers:

- https://www.biaodianfu.com/scrapy-redis.html
