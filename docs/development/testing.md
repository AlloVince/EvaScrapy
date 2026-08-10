# 测试

## 何时读
加测、跑测、看 CI 测什么。

## 内容
- 框架：pytest
- 目录：`tests/`
- 现状：`tests/test_pipelines.py` 共 38 用例，覆盖工具函数、item 序列化、所有 pipeline 逻辑（mock 外部 SDK）
- 本地：`uv run pytest tests/`（或激活 venv 后 `pytest tests/`）
- CI：`.github/workflows/ci.yml` 在 main push 用 `uv run pytest tests/`

### 惯例建议
- 改 items 路径/序列化 → 单测纯函数
- 改 pipeline 分支 → 尽量 mock 外部 SDK
- 不删测试装绿；外部服务测试保持可选/可跳过

## 相关
- 代码：`tests/`
- `commands.md`
