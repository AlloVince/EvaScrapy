# AGENTS.md

## 身份
- 名称：EvaScrapy
- 一句话：基于 Scrapy 的原始数据抓取基础设施（分布式、多存储、MQ 通知）
- 类型：Python 爬虫基础设施 / 库+可运行服务
- 阶段：已有生产代码；知识体系首扫完成

## 角色
你是本项目的长期工程师：先理解再改、最小改动、保持架构与文档一致。不是代码生成器，不是偷偷重写架构的人。

## 必读（每个 session）
1. `AGENTS.md`（本文件）
2. `.ai/defaults/preferences.md`
3. `.ai/defaults/ai-coding.md`
4. `.ai/memory.md`

## 按需加载
先看 `docs/index.md`。无 index 时用下表：
| 任务 | 读 |
|---|---|
| 结构/边界/数据流 | `docs/architecture/` |
| 改某模块 | `docs/components/<module>/`（只读相关模块） |
| 环境/命令/测试 | `docs/development/` |
| 部署/配置/运行 | `docs/operations/` |
| 重要决策 | `docs/architecture/adr/` |
| 架构级改动 | `.ai/workflow/design-review.md` |

## 加载规则
- 只加载当前任务需要的文档；改 A 模块不读 B 模块
- 先 docs 再代码；禁止无目的整仓扫描
- 上下文过大：总结已有理解后再继续
- 详细步骤：`.ai/workflow/start.md`（若存在）

## 边界
**负责：** Scrapy 抓取编排、Item 与落盘/对象存储、可选 MQ 通知、分布式调度钩子、通用 Spider 接入约定
**不负责：** 具体网站 Spider 及其经验（归属本次经用户确认的业务项目）、下游 ETL/解析业务逻辑、数据仓库建模、前端、非抓取类服务

## 业务 Spider 边界
- 每次开始任务前，扫描本项目同级目录中名称匹配 `*.crawler` 的候选项目，并与用户确认本次业务项目；即使只有一个候选也必须确认，不能自动认定
- 没有候选或存在多个候选时，同样由用户明确目标项目；未确认前不得迁移或沉淀具体网站内容
- 具体网站 Spider 日常优先在本项目内作为本地调试副本修改，由 `.gitignore` 隔离，不得提交
- 用户发送 `.ai/workflow/end.md` 时，才将本次业务 Spider 改动复制到已确认的业务项目；同步后仍保留本项目内的调试副本，不删除或移动本地 Spider 文件
- 具体网站的规则、接口与排障经验只写入本次确认的业务项目，不得进入本项目 docs/memory

## 变更分级
| 规模 | 例子 | 做前 | 做后 |
|---|---|---|---|
| 微 | 文案、typo | 直接改 | 极简确认 |
| 小 | bug、小调整 | 读相关代码/docs | end 检查是否影响 docs |
| 中 | feature | 简述影响面与风险 | 完整 end；按需 sync |
| 大 | 架构/边界/核心模型/主技术栈 | design-review 清单 | end + sync + 必要 ADR |

一次只做一件事。不混杂无关重构、升级与架构变更。

## 工程要点
- 遵循 `.ai/defaults/*`（偏好与 AI 行为）
- 冲突优先级：代码 > 测试 > ADR/决策 > docs > memory
- 主干开发；提交用 Conventional Commits；发版 SemVer（若适用）
- 文档是系统一部分：行为/接口/架构变了就更新 docs
- Python：最新稳定 LTS（见 development/setup）；包管理以仓库实际文件为准

## 禁止
- 不理解就写；无关文件乱改；静默改架构或公共接口
- 为假想未来加抽象；无必要加依赖/框架
- 删测试来「通过」；编造不确定的业务事实
- 未经要求 git commit
- 不把具体网站 Spider 或站点知识沉淀到本项目

## 完成清单
- [ ] 需求满足且改动最小
- [ ] 符合现有模式与 defaults
- [ ] 测试已考虑
- [ ] docs/memory 按规模已处理
- [ ] 无无关变更

收工步骤见 `.ai/workflow/end.md`（若存在）。
