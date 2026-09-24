# 在受限开发机上跑本地 K8s

记录在一台受限的云开发 VM（Firecracker，cgroup v1，出网只能走代理）上把 K8s 搭起来时踩到的坑。
正常的开发机用 kind 或 k3d 直接跑 `deploy/k8s/build.sh && deploy/k8s/up.sh` 就行，不会遇到这些问题。

| 现象 | 原因 | 处理 |
|---|---|---|
| `kind create cluster` 失败：`runc create failed: can't get final child's PID from pipe` | 嵌套容器里无法设置负的 `oom_score_adj` | 改用 `k3s server --docker`（Pod 直接跑在宿主 dockerd 上，只有一层容器） |
| k3s 的 Pod 同样起不来 | kubelet 给每个 pod sandbox 设置 `oom_score_adj=-998`，这台 VM 不允许负值 | 给 dockerd 配一个 runc 包装脚本 `runc-nooom`，把负值改成 0（`/etc/docker/daemon.json` 设为 default-runtime，`kill -HUP` dockerd 热加载） |
| `kubectl logs` 报 EOF | k3s 继承了 `HTTPS_PROXY`，apiserver 访问 kubelet 也走了代理 | 启动 k3s 时把节点 IP、10.42/16、10.43/16、`.svc`、`.cluster.local` 加入 `NO_PROXY` |
| 镜像莫名消失、Pod 被驱逐 | 文件系统报告的可用空间远小于总容量，kubelet 按比例判断"快满了"，触发镜像 GC 和 DiskPressure | `--kubelet-arg=image-gc-high-threshold=100 --kubelet-arg=image-gc-low-threshold=99 "--kubelet-arg=eviction-hard=imagefs.available<1%,nodefs.available<1%"` |
| 构建时 `apt-get` 失败 | Debian 软件源被出网策略拦截 | 基础镜像改用自带 git 和工具链的 `python:3.11-bookworm`，不跑 apt |
| 构建时 pip 报 TLS 错误 | 出网被 TLS 中间人代理，容器里不信任它的 CA | 在宿主机用 `pip wheel` 预先打好 wheelhouse，构建时带 `PIP_NO_INDEX=1` 离线安装 |
| 拉 Docker Hub 镜像 429 | 限流 | 基础镜像只拉一次并打上本地 tag `sa-base:dev`，用经典 builder（`DOCKER_BUILDKIT=0`）构建，它不会去 registry 复查；Gitea 从 GitHub Releases 下载二进制自己打镜像 |
| Gitea 建组织报 422 `name is reserved` | `org` 是 Gitea 的保留名 | 默认 owner 改为 `agents` |

k3s 的启动命令：

```sh
NP="$NO_PROXY,<node-ip>,10.42.0.0/16,10.43.0.0/16,.svc,.cluster.local"
NO_PROXY="$NP" no_proxy="$NP" k3s server --docker --disable traefik --disable metrics-server \
  --write-kubeconfig-mode 644 --kubelet-arg=cgroup-driver=cgroupfs --kubelet-arg=fail-cgroupv1=false \
  --kubelet-arg=image-gc-high-threshold=100 --kubelet-arg=image-gc-low-threshold=99 \
  "--kubelet-arg=eviction-hard=imagefs.available<1%,nodefs.available<1%"
```
