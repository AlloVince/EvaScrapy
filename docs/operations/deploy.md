# 部署

## 何时读
镜像构建、CI、发布流程。

## 内容

### Docker
- `Dockerfile`：`FROM python:alpine3.7`，WORKDIR `/opt/htdocs/evascrapy`，`CMD python start.py`，EXPOSE 6000
- 镜像名（CI）：`allovince/evascrapy`（staging / latest / tag）
- **待确认：** 基镜像与 requires-python>=3.14 不一致，生产是否已有其它 Dockerfile/流水线

### 挂载业务 Spider
```bash
docker run -e APP_TASK=full -e APP_SPIDER=your_spider \
  -v $(pwd)/your_spider.py:/opt/htdocs/evascrapy/spiders/your_spider.py \
  -it allovince/evascrapy:latest
```

### CI
| 文件 | 作用 |
|---|---|
| `.github/workflows/ci.yml` | main push/PR：单测；main push：semantic-release + Docker 镜像 |
| `pyproject.toml` | `[tool.semantic_release]` 配置语义化发布（不创建 GitHub Release） |

### 发布
- `python-semantic-release` 根据 Conventional Commits 自动 bump 版本、打 tag
- Docker tag：`staging`（每次 main push）、`latest` + `vX.Y.Z`（有版本发布时）
- 不创建 GitHub Release

## 相关
- `Dockerfile`、`.drone.yml`、`config.md`
