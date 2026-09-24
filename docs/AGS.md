# 在腾讯云 AGS 上运行

AGS（Agent Sandbox）以常驻沙箱的形式承载 worker：每个 worker 一个实例。
下面的结论来自 `tencentcloud-sdk-python-ags` 3.1.178 的源码（API 版本 2025-09-20）、官方的 ags-cookbook / ags-cli / ags-go-sdk，以及第三方的实测报告。
标 **待实测** 的项，请先用 `scripts/ags_probe.py` 确认。

## 关键事实

- **区域**：控制台 `rid=9` 对应 **ap-singapore**。控制面 endpoint 是 `ags.tencentcloudapi.com`，区域通过请求参数传递。
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

## 待实测清单（`scripts/ags_probe.py` 会逐项回答）

1. 当前区域能否创建 Persistent 的 custom Tool？
2. 不传 Timeout 启动的实例，`TimeoutSeconds` / `ExpiresAt` 是否为 null？
3. token 的有效期；是否返回 TrafficToken。
4. envd 的 exec、文件、后台进程是否正常。
5. 暂停再恢复后，后台进程是否还活着。

在能访问 `*.tencentcloudapi.com` 和 `*.tencentags.com` 的机器上运行即可。当前这个开发容器的网络策略拦截了这两个域名。
