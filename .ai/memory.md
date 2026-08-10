# Project Memory

限高：全文建议 ≤150 行。超限先删 Assumed/过时/已升格进 docs 的条目。
只记：代码与 docs 都表达不好、且影响未来开发的信息。
不记：架构复述、API 说明、流水账、临时调试、git 能看到的变更列表。
置信：Confirmed（代码/测试/人确认）| Assumed（待验证，用完升格或删）。

更新：2026-08-10

## 当前焦点
- 进行中：无（bootstrap standard 已完成）
- CI 已迁移至 GitHub Actions（`.github/workflows/ci.yml`），使用 `python-semantic-release`，不创建 GitHub Release
- 旧 CI 文件（`.drone.yml`、`.travis.yml`、`Makefile`）已删除
- `package.json` 已清理 npm semantic-release 依赖
- 下一步：按业务任务开工；首扫遗留待确认见下节

## 雷区与禁忌
- `evascrapy/spiders/*_spider.py` 被 `.gitignore` 忽略；新增业务 spider 默认不进 git，需有意调整 ignore 或改命名约定（Confirmed）
- `BaseSpider` 基类按**进程启动时** `APP_DISTRIBUTED` 环境变量在 import 时选择 `RedisCrawlSpider` vs `CrawlSpider`；改 env 后需重启进程，不能只靠 settings 热切换（Confirmed）
- 配置优先级：环境变量 > `.env` > `settings.py` > scrapy 默认；settings 用 `os.getenv` 覆盖已在 globals 或 scrapy/scrapy-redis 默认名中的大写键（Confirmed）
- README 仍写 Python 3.6 / 旧变量名（如 `APP_MQ_NOTIFY`）；以 `settings.py` 与 `pyproject.toml` 为准（Confirmed）
- `.env` 与密钥不入库；文档与日志禁止粘贴密钥原文（Confirmed）
- **Python 指令必须用 `uv`**：CI 用 `astral-sh/setup-uv` action + `uv pip install --system`，本地用 `uv run` / `uv pip install`，Dockerfile 内可用 `pip`（无 uv）。禁止直接用 `pip install` / `pytest` 等裸命令（Confirmed）

## 调试手册
- 分布式空闲即退：`BaseSpider.spider_idle` 会 close；需先 `redis sadd <name>:start_urls ...` 再 `APP_DISTRIBUTED=1 scrapy crawl <name>`
- 调度跳过新一轮：`runner` 若已有 crawler 在跑则 SKIPPED；查日志 `NEW ROUND SKIPPED`
- 存储路径对不上：检查 `APP_TASK` / `APP_STORAGE_SHUFFLE_INTERVAL` 与 spider.name
- Pipeline 未生效：查 `APP_STORAGE`、`APP_MQ_NOTIFY_*`、`TORRENT_FILE_ELASTIC_DUPE`、`APP_DISTRIBUTED` 是否在 settings 装配分支打开

## 待验证
- `nats-py` 在依赖中但源码未见引用，是否预留或遗留 — Assumed
- 生产是否仍用 `nyaa` 示例 spider，或仅作模板 — Assumed
- 用户偏好「Python 最新 LTS」与仓库 `.python-version`/`pyproject` 的 3.14 是否已对齐为团队标准 — Assumed（本次按用户指示以最新 LTS 为偏好写入 setup）

## 协作偏好（项目级）
- defaults 包名：`defaults`
- Python：最新稳定 LTS（preferences 默认一致）；具体安装版本写在 `docs/development/setup.md`
