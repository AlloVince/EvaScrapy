<!-- agent.protocol reference: https://github.com/AlloVince/agent.protocol commit 0f98a089a5f3b176171a9059cc9968cbb5c1dd0f (v0.3.0 source, clean worktree; no release tag verified). -->

# AGENTS.md

## 项目与入口

- 名称与目标：EvaScrapy 是基于 Scrapy 的原始数据抓取基础设施，提供抓取编排、原始 Item、存储、可选 MQ 通知与周期/分布式调度。
- 边界：不实现下游 ETL、业务解析或入库建模，也不承担代理池、浏览器渲染和非抓取服务。模块边界见 `docs/architecture/boundaries.md`。
- 按任务阅读：先读 `docs/index.md`，再按任务读相关架构、组件、开发或运维文档；实现前读相关代码和测试，避免无目的全仓扫描。
- 正式命令：开发、测试和抓取使用 README 与 `docs/development/commands.md` 中的 `uv`/Scrapy 入口；部署和发布参照 `docs/operations/deploy.md` 及 CI，不另建私有脚本或运行路径。
- 已有基础设施：优先复用 `evascrapy/settings.py`、`evascrapy/base_spider.py`、`evascrapy/items.py`、`evascrapy/pipelines.py`、`evascrapy/runner.py` 和 Scrapy/CrawlSpider 原生能力；配置和运行特征分别见 `docs/operations/config.md`、`docs/operations/runtime.md`。
- 所属系统：`/Users/allovince/Developer/yinxing.super`；repo ownership 见其 `../yinxing.super/docs/architecture/repositories.md`，任务路由与发布/部署链路见 `../yinxing.super/docs/architecture/subprojects.md`，跨仓契约见 `../yinxing.super/docs/contracts/README.md`。

按任务加载，不要求阅读整套文档。正式操作前先记录目标仓的 HEAD、branch 和 status，保护已有未提交工作；不 reset、覆盖或擅自 commit、push、release。

## 权威与事实

当前人类指令优先，其次是相关 `owner/` 长期意图和适用的 `AGENTS.md`（更近目录规则更具体）。`owner/` 默认只读，只有当前人类明确要求修改具体文件时才可创建、改写或删除。

实际行为以代码、测试、原生配置、lockfile 与运行结果验证；docs、旧计划、报告和 Git 历史不能替代验证。发现意图、规则与实现冲突时明确区分并报告，不把现有行为自动当成正确目标。

## 项目边界与 Spider

- 通用 crawler runtime、存储、调度与可选 MQ 通知属于本仓；`yinxing.crawler` 只承接已授权的代码同步与镜像打包，NAS 运行配置属于 `nas.ops`。
- 具体网站 Spider 只在本仓的 gitignored `evascrapy/spiders/*_spider.py` 本地调试副本开发和验证；它们默认不得提交。站点规则、接口和排障经验不写入本仓文档。
- 向 `yinxing.crawler` 同步 Spider 前必须取得当前人类的明确授权；其他业务仓不能擅自推断为同步目标。同步后保留本地副本。同步、镜像发布和 NAS 部署是独立动作，未经明确授权不得执行。
- Spider 优先使用 Scrapy/CrawlSpider 的 `Rule`、`LinkExtractor`、scheduler、dupefilter、`JOBDIR`、retry、cookies、throttle 和 Item pipeline。仅将网站特有的 URL、分页、关联、页面校验及 deep/非 deep 边界写入 Spider；通用重复能力回到 EvaScrapy。
- `APP_DISTRIBUTED` 在进程导入 `BaseSpider` 时决定基类，变更后必须重启进程；环境变量、`.env`、`evascrapy/settings.py`、Scrapy 默认的优先级见 `docs/operations/config.md`。

## 复用与正式操作入口

- 新增 CLI、logger、config、storage、queue、HTTP client、validation 或部署能力前，先检查当前仓、已声明的 `.super` 共享能力和既有服务。
- 优先扩展已有层。只有能力不足或复用会破坏明确的依赖、部署、数据边界时才新增，并说明缺口或边界原因；不得留下第二套永久实现。
- 人类与 Agent 对开发、构建、测试、抓取、发布、部署、维护和诊断使用同一正式入口。一次性调查可用临时代码；重复或正式流程必须收敛到已有库与正式入口。

## 人类可读与改动纪律

- 使用清晰命名、直接控制流和显式数据与副作用；遵循项目已有模式，注释解释原因和非显然约束。
- 避免过度抽象、复杂泛型、元编程、隐式魔法和重复实现。不为假想需求增加服务、依赖或框架，不静默改变接口、数据语义或系统边界。
- 不混入无关重构、依赖大升级或格式化。长任务为当前功能做必要重构不受此限制；每个可验证阶段结束前按职责拆解超长函数和超大源文件，整理混杂职责、重复逻辑与废弃实现，保护用户改动并运行相关验证，不机械拆文件。
- 架构、核心模型、技术栈或跨仓接口等大改动前，说明：问题、现有能力缺口、最简方案、影响和验收方式。跨仓 API/Event/Storage、ownership、breaking change 或 Domain/Product 语义没有已批准方案时，先报告并等待人类决定。

## 工程默认线

- Node 默认使用 fnm + pnpm；Python 默认使用 pyenv + uv，venv 按需。新项目或明确升级运行时时，分别使用最新 LTS 或最新稳定版本，无业务要求不做 Node 历史版本兼容；实际运行时、包管理器与构建行为以原生配置和 lockfile 为准。默认规则不授权自动更换工具链、运行时、包管理器、分支或 Git 历史。
- Python 的本地开发、测试和抓取命令使用 `uv`；Dockerfile 内的 `pip` 仅作为镜像构建实现。不得以默认规则改动 `.python-version`、`pyproject.toml` 或 `uv.lock`。
- 发布采用 SemVer，提交采用 Conventional Commits；`main` 是集成主干，一次聚焦一个功能。已有明确项目约束优先。
- 检查适用的低成本生产基线：配置校验、生产模式、可重复构建、错误处理与超时、密钥管理、合理缓存及静态/文本响应压缩。已有 CDN/反代承担时不重复建设；不采用时说明具体原因。
- CLI、批处理和长任务复用既有日志设施，提供阶段/进度、失败原因、最终摘要和退出状态，避免长时间静默。

## 文档准入

`README.md` 与 `owner/` 面向人类；`docs/` 只保存原生代码、测试、schema、配置或 CLI help 难以表达，且能持续减少未来误判、重复探索、上下文或重建成本的稳定知识。

不保存 session 流水账、当前焦点、进度、下一步、一次性 debug 过程或 Git 已表达的变化；进度使用当前对话或已有任务载体，不建立 memory/workflow。设计只有持续指导实现且难从代码恢复时才写普通 docs；高返工、存在真实取舍且理由长期有用时才写 ADR。

## 长任务与交付

- 先明确最终用户成果与可观察验收条件；每轮以当前代码、数据和实际产物自纠偏，旧 plan 不具权威。下一步明确、已获授权且无真实阻塞时继续。
- 中间数据、日志、二进制和多阶段结果按实际规模使用分块、流式读写或现有存储，避免无界整量载入或重写；格式强制的单体输入/交付保持兼容，处理资源仍须有界；不覆盖不可重建输入，及时清理可重建且不再使用的临时产物。
- 阶段结果须可复核。仅在重建有价值时保留必要输入来源、生成命令和校验信息，并复用原生产物机制。
- 不删测试或忽略失败来制造绿灯。按最初验收条件给出真实证据，运行与变更匹配的测试/检查；说明完成内容、验证结果、未验证项和真实阻塞。除非人类明确要求，不自动 commit、push 或 release。
