# items

## 何时读
改 Item 字段、落盘路径、MQ 消息体。

## 内容
**职责：** Scrapy Item 层次与序列化；URL/hash 分片路径；生成 ETL 命令 JSON（Kafka/MNS）。

**边界：** 不执行 IO；不解析业务字段。

**类型：**
| 类 | 用途 |
|---|---|
| `QueueBasedItem` | 接口：filepath / mns / kafka / meta |
| `RawTextItem` | 文本基类；路径 `APP_STORAGE_ROOT_PATH/{spider}/{APP_TASK}/...` |
| `RawHtmlItem` | HTML + HTML 注释元数据头 |
| `RawJsonItem` | JSON 包装 content |
| `BinaryFileItem` / `TorrentFileItem` | 二进制；torrent 按 info_hash 路径与 sha1 |
| `FilesItem` | 通用 file_urls（Scrapy Files 风格） |

**路径辅助：** `url_to_filepath`、`hash_to_filepath`（md5/hash 按 2 字符分片，`depth` 控制目录层）

**MQ payload：** MNS/Kafka 保持现有兼容格式；NATS 由运行环境的 `NATS_MESSAGE_TEMPLATE` 完整控制。模板渲染得到 JSON 后原样发布，EvaScrapy 不内置业务 Command。

**雷区：** `TorrentFileItem.get_info_hash` 依赖 bencode 解 body；`RawHtmlItem.to_string` 为注释头+html，不是纯 HTML。

## 相关
- 代码：`evascrapy/items.py`
- pipelines：`components/pipelines/README.md`
- 测试：`tests/test_pipelines.py`（测 url_to_filepath）
