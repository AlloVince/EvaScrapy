# 常用命令

## 何时读
要跑爬虫、调度、测试、清 redis。

## 内容

### 调度进程（增量周期）
```bash
python start.py
# APP_SPIDER 默认 demo；可用 env 覆盖
APP_SPIDER=nyaa python start.py
```

### 一次性 crawl
```bash
scrapy crawl <spider_name>
APP_TASK=full APP_RUN_DEEP=1 scrapy crawl <spider_name>
```

### 分布式
```bash
redis-cli sadd <name>:start_urls <url>
APP_DISTRIBUTED=1 scrapy crawl <name>
# 清理
redis-cli KEYS "<name>:*" | xargs redis-cli DEL
```

### 测试
```bash
pytest tests/
```

### Docker（镜像名以仓库/CI 为准）
```bash
docker run -e "APP_TASK=full" -e "APP_SPIDER=your_spider" \
  -v $(pwd)/your_spider.py:/opt/htdocs/evascrapy/spiders/your_spider.py \
  -it allovince/evascrapy:latest
```

### 其它
- 配置优先级：环境变量 > `.env` > settings.py > scrapy 默认

## 相关
- `setup.md`、`testing.md`、`operations/runtime.md`
