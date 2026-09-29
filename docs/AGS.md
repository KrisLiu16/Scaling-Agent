# 在腾讯云 AGS 上运行

AGS（Agent Sandbox）以常驻沙箱的形式承载 worker：每个 worker 一个实例。
下面的结论来自 `tencentcloud-sdk-python-ags` 3.1.178 的源码（API 版本 2025-09-20）、官方的 ags-cookbook / ags-cli / ags-go-sdk，以及第三方的实测报告。
标 **待实测** 的项，请先用 `scripts/ags_probe.py` 确认。

## 关键事实

- **区域**：控制台 `rid=9` 对应 **ap-singapore**。控制面 endpoint 国内站是 `ags.tencentcloudapi.com`，国际站是 `ags.intl.tencentcloudapi.com`，
  区域通过请求参数传递。账号在哪个站就用哪个 endpoint（运行文件的 `provider.endpoint`、`ags check --endpoint`、脚本的 `AGS_ENDPOINT`）。
- **常驻（Persistent）是沙箱工具（Tool，即模板）上的属性**，创建后不能修改，而且只有 `ToolType=custom` 这类工具支持。
  常驻实例的 `TimeoutSeconds` / `ExpiresAt` 为 null 时表示没有超时。但直接 `StartSandboxInstance` 时是否还有 24h 上限，**待实测**。
  兜底方案：`UpdateSandboxInstance(Timeout=24h)` 会重新开始计时，launcher 的 keepalive 在实例有超时的情况下会定期调用它。
- **不要用 Deployment（托管部署）**：它的"空闲"判定是"没有进行中的请求或连接"，会把自主干活的 worker 暂停；而且第三方实测显示 ap-singapore 不支持。
- **数据面兼容 E2B**：envd 监听 49983 端口，地址是 `https://49983-{instanceId}.ap-singapore.tencentags.com`，请求头 `X-Access-Token` 带上 `AcquireSandboxInstanceToken` 返回的 token。
  只用 AK/SK 就够，不需要 AGS 的 API key。
- **自定义镜像的要求**：
  - 镜像里必须有 `/usr/bin/envd`（从 `ccr.ccs.tencentyun.com/ags-image/envd:v0.5.14` 拷贝）。
  - 只支持 linux/amd64。
  - AGS 会忽略镜像的 CMD/ENTRYPOINT，所以启动命令要写在 Tool 的 Command/Args 里。
  - 必须配置指向 envd `/health` 的探针，`ReadyTimeoutMs` 不能超过 30000。
  - 从 TCR/CCR 拉镜像需要配置 `RoleArn`。
- **环境变量**：envd 不会把镜像里的 ENV 传给子进程，所以 launcher 通过 envd 启动 runtime 时会逐条传入 env。
- **网络模式**：`PUBLIC` 可以访问公网；`VPC` 需要子网和安全组，默认不能出公网（要出公网得加 NAT），而且只能在创建 Tool 时设置；`SANDBOX` 完全不能出网。
- **暂停**：`PauseSandboxInstance(Memory=true)` 会保留进程和内存；`ResumeSandboxInstance` 的 Timeout 默认只有 5 分钟，调用时务必显式传入。
- **配额**：用 `DescribeQuotaOverview` 查询（SandboxInstances / CPUCores / MemoryGiB）。超出时报 `LimitExceeded.SandboxInstance`，launcher 会跳过该 worker，由后续 supervise 重试。

## 流程

```
launcher (持有 AK/SK)
  ├─ CreateSandboxTool(custom, Persistent=true, Command=envd, Probe=/health:49983)   # 只做一次
  ├─ StartSandboxInstance(ClientToken=hash(run,worker), Metadata=worker_id/run_id)  # 幂等
  ├─ AcquireSandboxInstanceToken → e2b AsyncSandbox(X-Access-Token)
  ├─ commands.run("scaling-agent worker run", background=True, envs=<该 worker 的 env>)
  └─ supervise: commands.list() 检查 runtime 存活，挂了就重启；必要时 UpdateSandboxInstance 续期
```

## 实测结果（2026-09-29，国际站 ap-singapore，镜像在 TCR 企业版）

- 常驻 custom tool 能建。不传 Timeout 启动的实例 `TimeoutSeconds=0`、`ExpiresAt=None`。
- envd 令牌的 `ExpiresAt` 在 2094 年，等于不过期。
- envd 的 exec、文件读写、后台进程都正常（envd v0.5.14，镜像里自带 `/usr/bin/envd`）。
- tool 的 `Resources` 不带 `Storage` 时，沙箱根盘只有约 1 GiB（`df -h /` 为 974M）。`provider.disk` 保持 `20Gi`。
- tool 建好后配置改不了。`ensure_tool` 遇到同名但配置不同的 tool 会直接报错，不会拿它接着用；这时要删掉旧 tool，或者换一个 `tool_name`。
  比较的是 Persistent、镜像、镜像库类型、角色、CPU/内存/磁盘、网络模式（含 VPC 子网和安全组）、端口。镜像只比字符串：
  用可变 tag 时，同一个 tag 推了新内容发现不了，所以镜像要用 `@sha256:` 摘要固定。
  删掉 tool 后用同样的名字和镜像重建，AGS 会拒绝旧的 ClientToken（`FailedOperation.DuplicateRequest: Previous tool no longer exists`），
  `ensure_tool` 会换一个新 token 再建。
- 暂停实例的配额是**整个账号共用**的（`PausedInstances` 上限 10）。满了以后 `PauseSandboxInstance` 返回 `LimitExceeded.PausedInstance`，
  所以暂停后再恢复的那一项没测成。launcher 不会主动暂停实例，这项不影响运行。
- 沙箱能出公网（出口 IP 由 AGS 分配）。但只对白名单开放的主机它连不上：
  开发机的公网 IP 上 22/80/443/3000/8700 等端口从沙箱里全部连不通。这种情况下用下面的「基础设施也放进 AGS」。

## 基础设施也放进 AGS（`scripts/ags_infra.py`）

worker 沙箱访问不到你的主机时，可以把 Gitea 和 coord 放进另一台常驻沙箱，通过 AGS 的端口转发对外提供服务：

- 镜像：`deploy/Dockerfile` 的 `infra-ags` 阶段（coord 加上游镜像里静态链接的 Gitea 二进制，再加 envd）。
- tool 额外声明 3000、8700 端口（`ToolSpec.extra_ports`）。实例用 `AuthMode=PUBLIC` 启动，这样只有 envd 端口要令牌，
  3000、8700 可以直接访问。Gitea（开了 `REQUIRE_SIGNIN_VIEW`）和 coord（bearer token）各自做鉴权，匿名请求返回 403。
- 访问地址是 `https://{port}-{instanceId}.{region}.tencentags.com`。launcher（本机）和 worker（别的沙箱）用同一个地址，
  所以运行文件里 `coord_url` 和 `coord_public_url` 填同一个，`gitea.url` 和 `gitea.public_url` 也填同一个。
- Gitea 的 webhook 走 `127.0.0.1`，只放行 loopback。
- `ags_infra.py up` 可以重复执行：已有存活的实例就接着用，服务已经在跑就不重启，但会用环境里的管理员凭证访问一次 coord 和 Gitea，
  对不上就报错（要先 `down`）。`down` 停掉实例，数据在沙箱本地盘上，停掉就没了；之后再 `up` 会起一台新实例，URL 也是新的
  （同一个 ClientToken 会让 AGS 把那台已停止的旧实例原样返回，`start_instance` 遇到这种情况会换新 token 重起，2026-09-29 实测）。

2026-09-29 实测：4 个 scripted worker 跑 10 分钟，经转发地址 clone、push、调 MCP 都正常，8 个 PR 全部由合并队列合入。

## 待实测清单（`scripts/ags_probe.py` 会逐项回答）

1. 当前区域能否创建 Persistent 的 custom Tool？
2. 不传 Timeout 启动的实例，`TimeoutSeconds` / `ExpiresAt` 是否为 null？
3. token 的有效期；是否返回 TrafficToken。
4. envd 的 exec、文件、后台进程是否正常。
5. 暂停再恢复后，后台进程是否还活着。

在能访问 `*.tencentcloudapi.com` 和 `*.tencentags.com` 的机器上运行即可。`AGS_PROBE_URLS`（逗号分隔）会从沙箱里逐个 curl，
用来确认 worker 能访问到 coord 和 Gitea。
