# Session 启动

目标：用最小上下文恢复到可开工状态。

## 1. 固定加载
1. `AGENTS.md`
2. `.ai/defaults/preferences.md`
3. `.ai/defaults/ai-coding.md`
4. `.ai/memory.md`（限高；若超限先裁剪/总结再继续）

## 2. 理解任务
用四句话内搞清：目标 / 范围 / 不改什么 / 完成标准。

开始前必须扫描本项目同级目录中名称匹配 `*.crawler` 的候选项目，并与用户确认本次业务项目（即使只有一个候选也必须确认）。具体网站 Spider 日常优先在本项目内作为被 `.gitignore` 隔离的本地调试副本修改；用户发送 `.ai/workflow/end.md` 时，才将本次 Spider 改动复制到已确认的业务项目，同时保留本地副本，不删除或移动本项目内的 Spider 文件。站点经验只写入已确认的业务项目，EvaScrapy docs/memory 只保留通用框架知识。

### 业务 Spider 强制规则

具体网站 Spider 必须复制到本项目内作为唯一开发源，修改、调试和测试只能在本项目内进行；本项目副本始终保持最新。只有用户明确启动或发送 `.ai/workflow/end.md` 时，才将最新 Spider 同步到已确认的业务项目，未明确启动前禁止同步；同步后保留本项目副本，不删除或移动本地 Spider 文件。

## 3. 定规模
微 | 小 | 中 | 大（见 AGENTS 分级）。大 → 先读 `design-review.md`。

## 4. 按需加载
- 查 `docs/index.md` 或 AGENTS 加载表
- 只读相关 `docs/components/<module>/`
- 跨模块才读 `docs/architecture/`
- 跑通/测试读 `docs/development/`
- 部署运维读 `docs/operations/`
然后读相关代码与测试。禁止整仓扫描。

## 5. Context Budget
优先：最小文档集 → 组件 docs → 再代码。上下文膨胀则先总结。

## 6. 确认（按规模）
- 微/小：可直接干
- 中：三五行说明影响面、做法、风险
- 大：完成 design-review 清单；需求不清或多方案时等人确认

## 7. 开工
最小改动；遵循现有约定与 defaults；不碰无关区域。
