# 边界

## 何时读
判断改动是否越界、模块该落在哪。

## 内容

### 系统负责
- 抓取编排（规则、深度模式、分布式队列）
- 原始 Item 建模与落盘/对象存储
- 可选抓取完成通知（Kafka/MNS 消息指向存储 URI）
- 调度循环与 `APP_TASK` 分桶

### 系统不负责
- HTML/业务字段 ETL、清洗、入库建模
- 代理池、Ajax/浏览器渲染（README TODO，未实现）
- 多租户控制面 / Stats API（未实现）

### 模块边界
| 模块 | 做 | 不做 |
|---|---|---|
| settings | 读 env、条件注册组件 | 业务解析 |
| base_spider | 基类、deep 切换、通用 item 辅助 | 具体站点规则 |
| items | 字段、路径、MQ payload | IO |
| pipelines | 写存储、发消息、ES 查重 | 解析页面 |
| middlewares | 全局 Cookie | 业务 header 策略全集 |
| runner | 定时、分布式 seed/APP_TASK | 页面逻辑 |
| spiders | 站点规则与 callback | 存储细节 |
| logformatter | 日志文案 | 业务指标 |

### 依赖方向（宜保持）
`spiders` → `base_spider` / `items` →（经 scrapy）`pipelines` ← `settings` 装配  
`runner` → scrapy project settings + spider 动态 import  
外部：Redis（分布式）、OSS/S3/Kafka/MNS/ES（按开关）

## 相关
- `overview.md`
- 各 `components/*/README.md`
