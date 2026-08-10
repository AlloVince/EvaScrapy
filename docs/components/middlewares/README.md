# middlewares

## 何时读
改全局 Cookie 注入行为。

## 内容
**职责：** `GlobalCookiesMiddleware`：从 `COOKIES_GLOBAL` 解析 Cookie 字符串，写入每个 request。

**边界：** 不处理 UA（UA 由 settings 挂 scrapy_fake_useragent）；无其它 downloader 中间件。

**入口：** `evascrapy/middlewares.py`；settings 在 `COOKIES_GLOBAL` 真值时 priority 700 启用并禁用默认 CookiesMiddleware。

**雷区：** Cookie 字符串需 `SimpleCookie` 可解析；`dont_merge_cookies` meta 时跳过。

## 相关
- 代码：`evascrapy/middlewares.py`
- `components/settings/README.md`
