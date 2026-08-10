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
| `.drone.yml` | master：单测、推 staging、semantic-release；tag：推 latest+tag |
| `.travis.yml` | 历史：测、semantic-release、推镜像 |
| `package.json` | semantic-release（npmPublish false） |

主用哪条 CI **待确认**。

### 发布
- semantic-release + Docker tag
- 未在代码中看到完整 K8s/编排清单

## 相关
- `Dockerfile`、`.drone.yml`、`config.md`
