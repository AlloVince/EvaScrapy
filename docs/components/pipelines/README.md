# pipelines

## 何时读
改存储后端、MQ 通知、torrent ES 去重。

## 内容
**职责：** 消费 `QueueBasedItem`；写入存储或发送通知；可选丢弃重复 torrent。请求入队前的 S3 去重见 `components/dupefilters/README.md`。

**边界：** 不解析页面；非 QueueBasedItem 直接透传。

| Pipeline | 开关/条件 | 要点 |
|---|---|---|
| `LocalFilePipeline` | `APP_STORAGE=file` | 按 filepath 建目录；文本/二进制模式 |
| `AliyunOssPipeline` | `oss` | oss2 Bucket.put_object |
| `AwsS3Pipeline` | `s3` | Minio 客户端；带 meta |
| `KafkaPipeline` | `APP_MQ_NOTIFY_KAFKA` | 同步 `future.get()`；可选 SSL/SASL |
| `AliyunMnsPipeline` | `APP_MQ_NOTIFY_MNS` | MNS Queue 发 Message |
| `NatsPipeline` | `APP_MQ_NOTIFY_NATS` | NATS publish；懒连接 + 复用 event loop |
| `ElasticDupePipeline` | `TORRENT_FILE_ELASTIC_DUPE` | 仅 `TorrentFileItem`；exists(info_hash) 则 return None |

**装配优先级（settings）：** ElasticDupe 100 → 存储 300 → MQ 600

**依赖：** oss2、minio、kafka-python、aliyun-mns、nats-py、elasticsearch（按使用）  
**雷区：** LocalFile 用 `from_crawler` 挂 crawler；OSS/S3/Kafka/MNS/NATS 的 `process_item` 签名带 spider。Kafka 发送失败会抛错阻断。NATS 用 `asyncio.new_event_loop()` 建立独立 loop，`process_item` 内 `run_until_complete(publish)` 同步发送，连接成功后复用（同一 loop 不可跨线程；Spider 与 Pipeline 同线程）。ES basic_auth 从 URL userinfo 解析。

## 相关
- 代码：`evascrapy/pipelines.py`
- settings 装配、`operations/config.md`
