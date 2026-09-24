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
 │ harness adapter: Claude Code (claude-agent-sdk) / scripted / fake           │
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
| 75% 的 PR 没合入，后期合并跟不上 | 各自 self-merge，冲突自己处理 | 合并队列串行落地，可选用合并后的结果做验证；冲突精确退回作者；队列积压时反压，暂停扩容 | `workspace/merge_queue.py` `launcher.py` |
| main 被"本地验证过"的合并弄坏 | 只做本地验证 | 在合并后的结果上验证（`verify_command`） | `CommandVerifier` |
| 热点文件（`main.py` 被 972 个 agent 改过） | 无 | 统计每个文件的合并/冲突/认领次数；认领时对热点告警；prompt 要求用注册表/插件结构，"加文件不改中心文件" | `store.hotspots` `worker_prompt.md.j2` |
| O(N²) 广播噪声 | 每条 board 新条目推给所有人 | 按认领范围推送相关条目，其余进下一轮摘要（可切回 `all` 做消融实验） | `drain_high(board_mode)` |
| Gitea 通知 | 每个 worker 各自监听通知 | webhook 汇总到 coord 后定向路由：只发给相关的人，以及认领范围被 main 改动的人 | `workspace/routing.py` |
| 私信只能在 MCP 工具返回时送达 | 同左 | 额外加 PostToolUse 钩子，任何工具（Bash/Edit…）调用后都能送达 | `adapters/claude_code.py` `/api/drain` |
| 长上下文下偏离协议 | 协议只在初始 prompt 里 | 每轮开头重发协议卡；按轮数或上下文占比轮换会话，用 handoff 恢复；关键规则放在基础设施层强制（分支保护、push 守卫、FAIL/overlap 事件） | `runtime/worker.py` |
| 空闲 10 分钟才催 | idle detector 固定 10 分钟 | `continue_after_s`（默认 60s） | `RuntimeSettings` |
| 起步时大家抢同一块 | 固定错峰（30s/3s） | 先让少数创始 worker 搭骨架和接口，再按阶段错峰扩容，并受合并队列反压控制 | `schedule_offsets` |
| 可观测性 | 事后分析轨迹 | 每次工具调用、每轮、每次投递都记 trace；`/api/admin/status` 提供 KPI（认领冲突、合并落地率、队列深度、沉默 worker、热点）；launcher 用 KPI 做反压 | `store.trace` `store.status` |

## 5. 安全边界

- 腾讯云 AK/SK 只放在 launcher，永远不进沙箱。
- 每个 worker 拿到的凭证：自己的 coord token、自己的 Gitea token（只有仓库 write 权限，main 受保护），以及模型 API 凭证。
- Gitea 的 token 接口只接受 Basic 认证，所以 launcher 用管理员的 Basic 认证给每个 worker 签发 token。
- prompt 里会夹带同伴写的文本（私信、board、PR 标题），因此 Claude Code 开启 `verbatim_prompts`，不做 @path 展开，也不执行斜杠命令。
- K8s 模式下，worker Pod 不挂载 ServiceAccount token；launcher 的 Role 只能在自己的 namespace 里管理 pods 和 secrets。

## 6. 部署

### 6.1 本机 K8s（Pod 模拟 AGS）

```bash
deploy/k8s/build.sh        # 离线构建 sa-gitea / sa-coord / sa-launcher / sa-worker 四个镜像
deploy/k8s/up.sh           # 部署 Gitea + coord + RBAC，并用 launcher Job 启动一轮运行
kubectl -n scaling-agent exec deploy/coord -- scaling-agent status
```

默认运行文件 `deploy/k8s/run.scripted.yaml` 用脚本化 worker（不调模型）走完整个协作循环，用来验证基础设施。
要用真模型：把 `harness` 改成 `claude_code`，运行 `up.sh` 前先 `export ANTHROPIC_API_KEY=...`（会写入 `sa-secrets`）。

本机实测记录（6 个 worker × 3 个功能）：18/18 个功能全部合入 main；8 次冲突被退回后解决；
28 次认领重叠通知、14 条私信；`REGISTRY.md` 被正确识别为热点；直接 push main 被 Gitea 拒绝。

在这台开发机上搭 k3s 踩到的坑见 `docs/K8S_LOCAL.md`。

### 6.2 腾讯云 AGS（常驻沙箱）

详见 `docs/AGS.md`。概要：
1. `docker buildx build -f deploy/Dockerfile --target worker-ags --platform linux/amd64 -t <registry>/sa-worker:v1 --push .`
2. 在 VPC 内（或经公网 HTTPS）部署 Gitea 和 coord，要求沙箱能访问到它们。
3. 先跑 `scripts/ags_probe.py`，确认常驻沙箱的超时语义、token 有效期、暂停/恢复后进程是否存活。
4. 在运行文件里设 `provider.kind: ags`、`region: ap-singapore`（控制台 rid=9），然后执行 `scaling-agent launch run.yaml`。

## 7. 已知限制与后续

- coord 是单进程加 SQLite（WAL）。几百个 worker 的写入量够用；到 1,024 个时建议换 Postgres，并把合并队列拆成独立进程。
- 合并队列目前串行。下一步是批量合并（一次试合 k 个 PR，失败再二分），提高吞吐。
- `CommandVerifier` 跑在 coord 所在机器上。重型工具链应该改成在专用的集成沙箱里跑。
- AGS 的几个行为文档没写清，需要用 probe 脚本实测：常驻沙箱是否真的没有 24h 上限、token 有效期、TrafficToken 用哪个请求头。
- 脚本化 worker 只验证基础设施，不代表模型能力；真实效果要用 `claude_code` harness 跑 benchmark 来评估。
