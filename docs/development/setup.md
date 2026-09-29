# 环境搭建

## 何时读
新机器/新 session 准备可运行环境。

## 内容

### 运行时
- Python 版本以 `.python-version` 和 `pyproject.toml` 的 `requires-python` 为准
- 工具：pyenv（或等效）+ uv；venv 按需
- 不因本地默认工具链自动改动仓库运行时或依赖约束

### 安装
```bash
uv sync
source .venv/bin/activate
# 开发可选
uv sync --extra dev
```

### 配置
- 复制/创建根目录 `.env`（已 gitignore）
- 至少关注：`APP_SPIDER`、`APP_STORAGE`、`APP_TASK` / 调度相关、分布式时 `REDIS_URL`
- 勿提交密钥；OSS/S3/Kafka/MNS/ES 按需

### 校验
```bash
uv run pytest tests/
# 或
uv run scrapy list
```

### 注意
- 本地 Python 与依赖约束由本页运行时条目所列原生文件决定
- Docker 镜像基座见 `operations/deploy.md`；本地开发以本页为准

## 相关
- `commands.md`、`operations/config.md`
