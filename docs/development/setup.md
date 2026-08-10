# 环境搭建

## 何时读
新机器/新 session 准备可运行环境。

## 内容

### 运行时
- **Python：最新稳定 LTS**（团队偏好；与 `.ai/defaults/preferences.md` 一致）
- 工具：pyenv（或等效）+ venv；可选 uv
- 仓库标记：`.python-version` 与 `pyproject.toml` `requires-python` 以仓库当前值为准；若与「最新 LTS」不一致，**新环境优先 LTS**，并标待确认是否改 pyproject

### 安装
```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
# 开发可选
uv pip install -e ".[dev]"
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
- README 中 pyenv 3.6.5 等为历史文档，勿作当前标准
- Docker 镜像基座偏旧（见 operations/deploy），本地开发以本页为准

## 相关
- `commands.md`、`operations/config.md`
