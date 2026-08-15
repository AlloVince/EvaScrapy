# DupeFilter

## 何时读

配置或实现请求入队前的重复请求过滤。

## 默认行为

EvaScrapy 不配置 `DUPEFILTER_CLASS`，因此使用 Scrapy 官方的 `RFPDupeFilter`。它按请求 fingerprint 过滤当前任务中的重复请求；不会检查 S3，也不会提供跨运行的内容去重。

## S3DupeFilter

`evascrapy.dupefilters.S3DupeFilter` 是可选实现，必须显式配置：

```ini
DUPEFILTER_CLASS=evascrapy.dupefilters.S3DupeFilter
S3_DUPEFILTER_ENABLED=true
S3_DUPEFILTER_ROOT_PATH=dl/fanza/dedupe
S3_DUPEFILTER_DEPTH=3
```

执行顺序：

```text
Scrapy RFPDupeFilter
  → 已见过：过滤
  → 未见过：检查 S3/SeaweedFS 对象
    → 对象存在：过滤
    → 对象不存在：允许下载
```

当：

```ini
S3_DUPEFILTER_ENABLED=false
```

仍然配置了 `S3DupeFilter` 时，只关闭 S3 检查，官方 fingerprint 去重仍然生效。

## 对象 key

请求必须在 `request.meta['detail_url']` 中提供最终详情 URL。S3DupeFilter 使用与 `RawTextItem.to_filepath()` 相同的 `url_to_filepath()` 和 MD5 分片逻辑，但使用独立的稳定根目录。

去重根目录不能包含会变化的 `APP_TASK`，例如：

```text
dl/fanza/dedupe/{md5分片}/{文件}.json
```

`S3_DUPEFILTER_DEPTH` 未配置时回退到 `APP_STORAGE_DEPTH`。根目录和 SeaweedFS bucket 由部署方保证与已有对象一致。

## 注意事项

- `DUPEFILTER_CLASS` 未配置时不会加载 S3DupeFilter，`S3_DUPEFILTER_ENABLED` 单独配置没有效果。
- S3 检查发生在请求下载前，但 `stat_object()` 是同步远程调用，会阻塞调度线程；大规模任务需要评估 SeaweedFS 延迟。
- S3 网络、权限或服务异常会抛错，不应静默当作对象不存在。
- 对象存在只代表已抓取，不代表内容永不过期；刷新策略仍需另行设计。
- 当前实现用进程内缓存记录已确认存在的 key，不承担跨进程锁定。
