# Scaling-Agent 设计

目标：让 N 个完全对等的 agent（同一 prompt，只有 ID 不同）在没有中心调度的情况下协作完成一个大任务，
并且 N 从 1 扩到上千时仍然有收益。整体思路来自 Agensh（arXiv:2609.26781），但针对它暴露出的问题
（见第 4 节）用工程上成熟的做法做了加固。

## 1. 总体架构

```
                      ┌──────────────────── 组织基础设施 ────────────────────┐
                      │                                                     │
 launcher ──注册/启动──▶ coordination server (coord)          Gitea          │
 (不分配任务,          │  ├─ shared context board (类型化、只追加) │  共享工作区  │
  只管人数/存活/时钟)  │  ├─ claims (带范围和租约的认领)          │  issue/PR    │
      │               │  ├─ messages (频道 + 私信)              │  main 受保护 │
      │               │  ├─ 每个 worker 的事件队列 (租约/确认)   │              │
      │               │  ├─ merge queue (唯一进入 main 的路径) ──┼──▶ merge-bot │
      │               │  ├─ hotspots / KPIs / trace            │  webhook ───┐│
      │               │  └─ MCP 工具 (streamable HTTP)          │             ││
      │               └────────▲──────────────────────▲─────────┴─────────────┘│
      │                        │ MCP 工具调用 + 长轮询    │ git clone/push, REST     │
      ▼                        │                        │                        
 ┌────────────── sandbox (AGS 常驻沙箱 / K8s Pod / 本地进程) × N ──────────────┐
 │ worker runtime: 长轮询取事件 → 组装 prompt → 驱动 harness 一轮 → ack        │
 │ harness adapter: Claude Code (claude-agent-sdk) / pi / scripted / fake    │
 └───────────────────────────────────────────────────────────────────────────┘
```

| 组件 | 位置 | 职责 |
|---|---|---|
| coord | `src/scaling_agent/coord/` | board、claims、消息、事件投递、合并队列、热点、KPI；对 worker 暴露 MCP 工具 |
| workspace | `src/scaling_agent/workspace/` | Gitea 客户端、webhook 定向路由、合并队列 |
| runtime | `src/scaling_agent/runtime/` | 沙箱内的 worker 事件循环 + harness 适配器 |
| sandbox | `src/scaling_agent/sandbox/` | `ags`（腾讯云常驻沙箱）、`k8s`（Pod 模拟 AGS）、`local`（本机子进程） |
| launcher | `src/scaling_agent/launcher.py` | 错峰启动、反压、存活监控、提醒、收尾 |
| prompts | `src/scaling_agent/prompts/` | 静态 worker prompt + 每轮重发的协议卡 |

## 2. worker 视角的协作循环

1. 找没人做的最有价值的缺口（`claims` / board / main）。
2. `claim(scope, intent)`：scope 是路径、glob 或 `area:<tag>`。服务端当场做重叠检测，冲突方双方都收到通知。
3. 在 `wN/<topic>` 分支上基于最新 main 实现。
4. 按任务的验收标准验证。
5. push 分支，开 PR，调用 `merge_request`。main 受保护，只有合并队列能写。被退回就合并最新 main、重新验证后再提交。
6. 合入后写 `PATCH_SUMMARY`，`release_claim(done)`，回到第 1 步。

随时可以做的：发现了什么就立刻 `board_write`（`FACT` / `OBSERVED` / `FAIL`）；
两人要改同一处时用 `send_dm`；影响别人工作的约定（比如接口）用 `channel_post` 发到频道。

## 3. 事件投递

| 类别 | 例子 | 优先级 | 送达时机 | 语义 |
|---|---|---|---|---|
| 定向事件 | 私信、认领重叠、合并被退回、系统提醒 | HIGH | 本轮中途：附在任何工具调用结果后面（MCP 结果 + PostToolUse 钩子） | 至少一次；中途送达即标记完成 |
| 定向事件 | Gitea 通知（PR/评论/@提及）、main 在你认领的范围内有更新 | LOW | 下一轮开头 | 至少一次（租约 + ack，崩溃会重投） |
| board 条目 | 与你的认领范围重叠 | HIGH | 本轮中途 | 游标，最多一次 |
| board 条目 | 其他 | — | 下一轮开头的摘要 + 快照 | 游标，最多一次（历史随时可 `board_grep`） |
| 频道消息 | 公告 | LOW | 下一轮开头 | 游标 |

只有定向事件和频道消息会唤醒空闲的 worker。board 写入不会唤醒，否则规模一大，每个 worker 都会被不停拉起来读摘要。

## 4. 相对 Agensh 的改进

Agensh 的问题主要来自论文本身和它公开的 1,024 agent 回放数据。

| 问题 | Agensh 的做法 | 本项目的做法 | 代码 |
|---|---|---|---|
| 认领撞车 | CLAIM 是自由文本，靠读到的人自己发现 | 结构化 scope + 租约（活跃即续期，worker 死了自动过期）；写入时由服务端检测重叠，双方收 HIGH 事件；可选 `exclusive` | `coord/store.py: claim` `coord/scope.py` |
| 75% 的 PR 没合入，后期合并跟不上 | 各自 self-merge，冲突自己处理 | 合并队列串行落地，只接受作者本人提交的 PR；用本地裸镜像加 `git merge-tree` 精确判冲突（约 100ms，并列出冲突文件），不依赖 Gitea 异步计算的 mergeable 标志；冲突即时退回作者；队列积压时反压，暂停扩容 | `workspace/merge_queue.py` `workspace/conflicts.py` `launcher.py` |
| main 被"本地验证过"的合并弄坏 | 只做本地验证 | 在合并后的结果上验证（`verify_command`） | `CommandVerifier` |
| 热点文件（`main.py` 被 972 个 agent 改过） | 无 | 统计每个文件的合并/冲突/认领次数；认领时对热点告警；prompt 要求用注册表/插件结构，"加文件不改中心文件" | `store.hotspots` `worker_prompt.md.j2` |
| O(N²) 广播噪声 | 每条 board 新条目推给所有人 | 按认领范围推送相关条目，其余进下一轮摘要（可切回 `all` 做消融实验） | `drain_high(board_mode)` |
| Gitea 通知 | 每个 worker 各自监听通知 | webhook 汇总到 coord 后定向路由：只发给相关的人，以及认领范围被 main 改动的人 | `workspace/routing.py` |
| 私信只能在 MCP 工具返回时送达 | 同左 | 额外加 PostToolUse 钩子（pi 是 `tool_result` 事件），任何工具（Bash/Edit…）调用后都能送达 | `adapters/claude_code.py` `adapters/pi_extension/org.ts` `/api/drain` |
| 长上下文下偏离协议 | 协议只在初始 prompt 里 | 每轮开头重发协议卡；按轮数或上下文占比轮换会话，用 handoff 恢复；关键规则放在基础设施层强制（分支保护、push 守卫、FAIL/overlap 事件） | `runtime/worker.py` |
| 空闲 10 分钟才催；没活干时空转烧 token | idle detector 固定 10 分钟 | 首次空闲 60s 就续跑；之后每连续空转一轮等待翻倍，封顶 10 分钟（`max_continue_after_s`），一有新事件就重置。这个问题是本机 K8s 实测里发现的：6 个做完活的 worker 5 分钟空转了约 1.1 万轮 | `runtime/worker.py` |
| 起步时大家抢同一块 | 固定错峰（30s/3s） | 先让少数创始 worker 搭骨架和接口，再按阶段错峰扩容，并受合并队列反压控制 | `schedule_offsets` |
| 可观测性 | 事后分析轨迹 | 每次工具调用、每轮、每次投递都记 trace；`/api/admin/status` 提供 KPI（认领冲突、合并落地率、队列深度、沉默 worker、热点）；launcher 用 KPI 做反压 | `store.trace` `store.status` |

## 5. 安全边界

worker 按不可信对待，约束尽量放在基础设施层，不靠 prompt：

- 腾讯云 AK/SK 只放在 launcher，永远不进沙箱。本地 provider 只按白名单传递环境变量，每个 worker 有自己独立的 HOME。
- 每个 worker 拿到的凭证：自己的 coord token、自己的 Gitea token，以及模型 API 凭证。git 凭证只写进仓库本地配置，不动全局配置。
- **分支归属**：main 只有 merge-bot 能写；每个 worker 独占 `<wid>/*` 分支命名空间，别人不能 push、强推或删除。合并队列只接受 PR 作者本人提交。
- Gitea 设置 `MAX_CREATION_LIMIT=0`，worker 不能自建仓库或 fork（防止借自己仓库的 webhook 做 SSRF）；webhook 目标只允许 coord。
- 同伴写的文本（私信、频道、claim、scope）渲染进 prompt 前会压成单行，没法伪造 `[event]` 这类系统块；Claude Code 开启 `verbatim_prompts`。
- webhook 做 HMAC 校验并限制请求体大小；admin token 用常量时间比较；coord 的数据库文件权限是 0600。
- K8s 模式下：worker Pod 以 uid 1000 运行、drop ALL capabilities、seccomp RuntimeDefault、不挂载 SA token；NetworkPolicy 规定 worker 只能访问 coord、Gitea、DNS 和外网 443 端口（`up.sh` 的 `MODEL_EGRESS` 可为模型网关另加放行的地址和端口），谁也连不进 worker；launcher 的 Role 只能在本 namespace 管理 pods 和 secrets。
- `verify_command` 会在 coord 所在机器上运行 PR 代码（虽然会降权到 nobody、清空环境变量、在代码运行前删掉带凭证的 remote），**这仍然不是沙箱**。worker 不可信时，应该把验证放到隔离的集成沙箱里跑。

## 6. 部署

### 6.1 本机 K8s（Pod 模拟 AGS）

```bash
deploy/k8s/build.sh        # 离线构建 sa-gitea / sa-coord / sa-launcher / sa-worker 四个镜像
deploy/k8s/up.sh           # 部署 Gitea + coord + RBAC，并用 launcher Job 启动一轮运行
kubectl -n scaling-agent exec deploy/coord -- scaling-agent status
```

默认运行文件 `deploy/k8s/run.scripted.yaml` 用脚本化 worker（不调模型）走完整个协作循环，用来验证基础设施。
要用真模型：照 `deploy/k8s/run.claude.yaml` 填网关和模型，运行 `up.sh` 前 `export ANTHROPIC_AUTH_TOKEN=...`
（或 `ANTHROPIC_API_KEY`，写进 `sa-model` Secret）。网关不在 443 端口或在内网段时，另设 `MODEL_EGRESS=<cidr>:<port>` 放行 worker 出网。
Claude Code 只发 Anthropic Messages 请求，网关要能在 `/v1/messages` 上服务这个模型；只开放 OpenAI 协议的网关见 6.3。

用 kind：`KIND_CLUSTER=<名字> deploy/k8s/build.sh` 会把镜像装进这个集群；`up.sh` 用当前的 kubectl 上下文，
机器上还有别的集群时，建集群用 `kind create cluster --kubeconfig <单独的文件>`，跑 `up.sh` 前把 `KUBECONFIG` 指过去。
同一台机器上第二个 kind 集群起不来、节点里 containerd 报 `failed to create fsnotify watcher: too many open files` 时，
调大 `fs.inotify.max_user_instances`（2026-09-29 这台机器从 128 调到 512 后正常）。
kind 自带的 kindnet 不执行 NetworkPolicy（2026-09-29 实测：带 `app=sa-worker` 标签的 Pod 照样连得上策略外的地址），
worker 的出网限制要换能执行策略的 CNI 才生效。

本机实测（6 个 worker × 3 个功能，每个功能都改同一个热点文件 `REGISTRY.md`，10 分钟一轮）：

| 轮次 | 结果 | 说明 |
|---|---|---|
| 初版 | 约 2.5 分钟合入 18/18；5 分钟内空转 11,641 轮 | 暴露两个问题：做完活的 worker 不停空转；冲突判断只等 9 秒，在大规模下会误判 |
| 修复审查问题后 | 10 分钟合入 16/18，总共 390 轮 | 空闲退避生效；但串行队列要等满 Gitea 的异步检查（最多约 1 分钟）才能退回真冲突，吞吐下降 |
| 加上 `git merge-tree` 冲突检测 | **约 4 分钟合入 18/18**，11 次冲突即时退回并由作者解决 | 退回消息写明冲突文件；main 上没有重复行，也没有冲突标记 |

每轮都验证过：私信和认领重叠通知的中途送达、提醒和关停送达全部 worker（6/6）、launcher 到点清理 Pod、
直接 push main 被拒、推到别人的分支命名空间或删别人的分支被拒、worker 建仓库返回 403、worker 访问不到 kube-apiserver。

在这台开发机上搭 k3s 踩到的坑见 `docs/K8S_LOCAL.md`。

### 6.2 腾讯云 AGS（常驻沙箱）

详见 `docs/AGS.md`。概要：
1. `docker buildx build -f deploy/Dockerfile --target worker-ags --platform linux/amd64 -t <registry>/sa-worker:v1 --push .`
2. 在 VPC 内（或经公网 HTTPS）部署 Gitea 和 coord，要求沙箱能访问到它们；访问不到时用 `scripts/ags_infra.py` 把它们也放进一台 AGS 沙箱。
3. 先跑 `scripts/ags_probe.py`，确认常驻沙箱的超时语义、token 有效期、暂停/恢复后进程是否存活。
4. 在运行文件里设 `provider.kind: ags`、`region: ap-singapore`（控制台 rid=9），然后执行 `scaling-agent launch run.yaml`。

### 6.3 pi harness（OpenAI 兼容网关）

Claude Code 只发 Anthropic Messages 请求。网关只开放 `/v1/responses` 或 `/v1/chat/completions` 时（2026-09-29 实测 sub2api 后面的 gpt-5.6：
`/v1/messages` 回 403，`/v1/responses` 可用，偶发 502 overloaded），换用 [pi](https://pi.dev)（npm `@earendil-works/pi-coding-agent`）。运行文件：

```yaml
harness: pi
model: gpt-5.6-luna
harness_options: {base_url: "http://<gateway>/v1", api: openai-responses, api_key_env: OPENAI_API_KEY, thinking: medium}
forward_env: [OPENAI_API_KEY]
```

`harness_options` 的字段见 `runtime/adapters/pi.py` 的 `PiOptions`，多写、写错的字段直接报错。密钥只经 `forward_env` 进沙箱，
`models.json` 里写的是 `$OPENAI_API_KEY`，不落盘。

| Claude Code 里的做法 | pi 里的做法 |
|---|---|
| MCP 服务器 `org` | pi 没有 MCP 客户端。`pi_extension/org.ts` 启动时对 coord 的 `/mcp` 调 `tools/list`，把每个工具用原名注册成 pi 工具，名字和参数模式都由服务端给 |
| PostToolUse 钩子 | `tool_result` 事件里追加 `/api/drain` 的内容；org 工具的结果本身带更新，跳过 |
| PreToolUse 守卫 | `tool_call` 事件。规则是 `guards.py` 的 TS 移植，`tests/fixtures/push_guard_cases.json` 的用例两边都跑 |
| 一轮结束于 `ResultMessage` | RPC 模式（`pi --mode rpc`）一轮结束于 `agent_settled`；自动重试、压缩期间 `agent_end` 会在同一轮里出现多次 |
| `get_context_usage` | `get_session_stats` 的 `contextUsage.percent` |
| `resume` | 会话 id 由 adapter 先定好，用 `--session-id` 传入。续会话要求 transcript 还在，没有就启动失败，runtime 会开新会话并发 handoff |

- 网关偶发 502：pi 自带指数退避重试（默认 3 次），adapter 调到 `max_retries: 8`；重试用完，这一轮记为失败并带原因。
- pi 在扩展加载失败时照常运行，那样 worker 就没有 org 工具。adapter 起来后检查扩展写的 ready 文件，缺了就报错，由 runtime 退避重试。
- 只加载自己的扩展（`--no-extensions --no-skills --no-prompt-templates --no-context-files`）：同伴能写进仓库的 `AGENTS.md` 之类不会被当成指令。
  沙箱里 `PI_OFFLINE=1`、`PI_SKIP_VERSION_CHECK=1`、`PI_TELEMETRY=0`：不刷新目录、不查版本、不上报，模型请求照常发。
- pi 固定发 `store: false`，单次输出最多 32000 token（pi 自己的上限）。`models.json` 里的 `contextWindow` 是 `context_window` 选项（默认 272000），
  决定压缩和会话轮换的百分比，网关的真实上限不同时要改。
- 镜像：`worker` 阶段从 `node:24-bookworm-slim` 拷 node，从 `deploy/pi/` 拷 pi 的树。`deploy/fetch-pi.sh`（`build.sh` 会调）用宿主机 npm 装
  `@earendil-works/pi-coding-agent@0.87.1`，并去掉 esbuild 里其他平台的二进制（约 285 MB）。直接 `docker build` 前要先跑一次。

### 6.4 在已有项目上继续跑（`seed_dir`、`adopt_checkout`、`Dockerfile.project`）

让 agent 们在一个现成的项目上开发，而不是从空仓库起步，例如 SWE 类题目镜像里 `/app` 的检出。三部分配合：

- **`gitea.seed_dir`**（launcher）：worker 启动前，把这个目录的文件作为一次导入提交（`Import task repository`）快进到 main，由 merge-bot 推送。
  管理员对受保护的 main 强推会被拒（2026-09-29 在 Gitea 上实测 `pre-receive hook declined`），所以先用管理员的 Basic 认证给 merge-bot 签一个临时令牌，
  推完就删。令牌经环境变量交给 git，不出现在命令行。已经导入过就跳过，launcher 重启不会重复导入。
- **`SA_WORKER_ADOPT_CHECKOUT=1` 加 `SA_WORKER_WORKDIR=<镜像里的项目目录>`**（`worker_env`）：worker 第一次启动时，把镜像里已有的检出指向共享仓库并重置到 main。
  未跟踪的文件（比如 `pip install -e` 留下的元数据）保留，项目的环境不变。只做一次（`state_dir/adopted`），runtime 重启不会把 worker 进行中的工作重置掉。
  只对 AGS provider 有效：k8s 和 local provider 自己设 `SA_WORKER_WORKDIR`。
- **`deploy/Dockerfile.project`**：在项目自己的镜像上加 worker 运行时和 pi，不动项目的 Python 环境：运行时装在 `/opt/sa` 的 venv 里，
  用镜像自带的 `python3.11`；pi 装在 `/opt/pi`；只有 `/usr/local/bin/scaling-agent` 和 `pi` 两个软链和 `/usr/bin/envd` 进 PATH。
  基础镜像要有 `python3.11`（带 venv）、git 和 Node ≥ 22.19。

AGS 上用这类镜像要注意：
- 镜像里往往没有 `/workspace`；provider 启动 runtime 时的工作目录用 `/`，命令里自己创建需要的目录。
- 起 runtime 的命令是 `bash -lc`，登录 shell 会重置 `PATH`，项目镜像烘焙的 `PATH` 要从 `/proc/1/environ` 导入：
  `runtime_command: env PATH="$(tr '\0' '\n' </proc/1/environ | sed -n 's/^PATH=//p')" scaling-agent worker run`。
- 沙箱要能访问 coord 和 Gitea（见 6.2）。项目镜像原本的「无网络」设定在这里不成立：agent 能上公网，要防它去找现成实现，只能靠任务说明和事后审查它的命令记录。

### 6.5 实测：DeepSWE 一道题，16 个 agent 对 1 个 agent（2026-09-29）

题目：DeepSWE 1.1 的 `python-statemachine-state-data-scoping`（给状态机加带作用域的数据；参考解 17 个文件、约 490 行；隐藏测试 72 个目标测试加 1286 个原有测试）。
选它是因为前三次不同模型的整套跑分都没解出它，而且需求能拆成 8 到 9 块独立的活。两组用同一个镜像、同一个模型（gpt-5.6-luna，Responses 协议，`thinking: medium`）、
同一份任务说明、都是 90 分钟。16 个的那组第一个 agent 先单独干 5 分钟，之后每 20 秒加 1 个；1 个的那组就是 `workers: 1`。没有配 `verify_command`，合并队列不在合并结果上跑测试。
评分是把 main 相对导入提交的补丁交给题目自带的 verifier（无网络，2 核 8 GiB），reward 为 1 当且仅当 72 个目标测试全过且没有原有测试失败。

| | 1 个 agent | 16 个 agent |
|---|---|---|
| 隐藏测试结果 | 未解出：目标 62/72，原有测试 16 个失败 | 未解出：目标 69/72，原有测试全过 |
| 合入的 PR | 35 | 188（另有 9 次冲突退回、5 次失败、1 次取消） |
| main 上的提交 / 补丁 | 80 个 / 16 个文件 | 388 个 / 178 个文件 |
| 回合数 | 60（失败 0） | 550（失败 0） |
| 模型 token 总量 | 4370 万（未命中缓存的输入 170 万，输出 3.8 万） | 3.6 亿（未命中缓存的输入 1830 万，输出 35.5 万） |

目标测试通过数随时间（每 15 分钟对 main 取一次快照打分，起点为各自开跑）：

| 已用时间 | 16 min | 31 min | 46 min | 61 min | 76 min | 90 min |
|---|---|---|---|---|---|---|
| 1 个 agent | 41 | 37 | 39 | 62 | 62 | 62 |
| 16 个 agent | 45 | 69 | 69 | 69 | 69 | 69 |

- 16 个 agent 在 31 分钟就到了 69/72，之后 60 分钟没有再进步；剩下 3 个测试是同一个问题：题面写「`get_state_data(state)` 返回当前数据字典或 `None`」，
  没有数据的状态它们返回了 `{}`，测试要 `None`。前三次别的模型的跑分也栽在这里，是规格的歧义，不是实现能力。
- 1 个 agent 到 61 分钟才到 62/72，缺历史状态恢复和 SCXML 两块，而且弄坏了 16 个原有测试（历史状态、教程、SCXML 用例）；46 分钟那次快照里，
  原有测试的整轮运行直接被挂住，得到 0/1286。没有 `verify_command` 时，合并队列拦不住这类改动。
- 16 个 agent 的 main 在 16 min 和 31 min 两次快照里也各有 2、3 个原有测试失败，之后由它们自己修好，从 46 分钟起原有测试全过。
- 花的 token 约为 8 倍，换来目标测试多过 7 个、原有测试不退步；两边都没有拿到 reward 1。

局限：每组只跑了一次，只有一道题，结论只能当作一个数据点。模型的随机性没有控制。agent 在 AGS 沙箱里能上公网（题目原本是无网络的），
事后审计了两组所有 bash 命令（收集到最后 8 分钟之前的部分），没有发现去外部地址下载或查看上游实现的迹象；pi 自己没有联网工具。
这次运行用的镜像里 `adopt_checkout` 每次 runtime 启动都会重置检出，pi 的 bash 也没有默认超时，这两点是运行之后审查发现的、之后已改；两组都没有因此出问题（回合失败 0）。

## 7. 已知限制与后续

- 代码经过一轮对抗式审查：4 个方向共报告 48 条，逐条复核后确认 43 条，全部已修复并加了回归测试（`tests/test_regressions.py`）。

- coord 是单进程加 SQLite（WAL）。几百个 worker 的写入量够用；到 1,024 个时建议换 Postgres，并把合并队列拆成独立进程。
- 合并队列目前串行。下一步是批量合并（一次试合 k 个 PR，失败再二分），提高吞吐。
- `CommandVerifier` 跑在 coord 所在机器上（见第 5 节）。应该改成在专用的集成沙箱里跑。在已有项目上跑（6.4）时尤其需要：coord 所在的沙箱没有项目的依赖环境，
  `verify_command` 没法跑项目的测试，6.5 的两组 main 上都出现过原有测试被合入的改动弄坏。
- board 的"相关性"按投递那一刻的认领范围计算。认领范围在两次投递之间变化时，少量条目可能漏推或重复推送；完整历史随时可以用 `board_grep` 查到。
- AGS 实测结果见 `docs/AGS.md`。还没测的是暂停后再恢复（账号的暂停配额被占满），以及常驻实例跑满 24h 以后的表现。
- 脚本化 worker 只验证基础设施，不代表模型能力；真实效果要用 `claude_code` 或 `pi` harness 跑 benchmark 来评估。
