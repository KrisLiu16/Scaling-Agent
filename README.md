# Scaling-Agent

A framework for multi-agent collaboration.

Scaling-Agent 是一个多 Agent 协作框架：N 个完全对等的 agent 用同一个 prompt，没有中心调度器，
通过一层轻量的组织基础设施自组织协作：
共享工作区（Gitea）、消息（频道 + 私信）、共享上下文 board、带范围的认领，以及唯一进入 main 的合并队列。
设计参考 Agensh（arXiv:2609.26781），并针对它的认领撞车、PR 大量被浪费、热点文件、广播噪声、长上下文偏离协议等问题做了加固。
详见 [docs/DESIGN.md](docs/DESIGN.md)。

## 组件

| 进程 | 命令 | 说明 |
|---|---|---|
| coordination server | `scaling-agent coord serve` | board / claims / 消息 / 事件投递 / 合并队列 / KPI，对 worker 暴露 MCP 工具 |
| worker runtime | `scaling-agent worker run` | 跑在每个沙箱里，驱动 Claude Code（或 scripted/fake harness） |
| launcher | `scaling-agent launch run.yaml` | 错峰启动 worker、反压、存活监控、提醒、收尾 |
| status | `scaling-agent status` | 查看组织的 KPI |

沙箱后端：`ags`（腾讯云常驻沙箱，见 [docs/AGS.md](docs/AGS.md)）、`k8s`（用 Pod 模拟）、`local`（本机子进程）。

## 快速开始

```bash
# 单元 + 端到端测试
uv venv && uv pip install -e ".[worker,ags,k8s,dev]" && .venv/bin/pytest

# 本机 K8s 集群：四个镜像 + Gitea + coord + launcher Job，用 scripted worker 验证整套流程
deploy/k8s/build.sh && deploy/k8s/up.sh
kubectl -n scaling-agent exec deploy/coord -- scaling-agent status
```

## 目录

```
src/scaling_agent/
  coord/        board、claims、消息、事件、MCP 工具、HTTP API
  workspace/    Gitea 客户端、webhook 路由、合并队列
  runtime/      worker 事件循环、harness 适配器（claude_code / scripted / fake）
  sandbox/      ags / k8s / local
  prompts/      worker prompt、协议卡
  launcher.py   启动与监督
deploy/         Dockerfile（coord / launcher / worker / worker-ags）、Gitea 镜像、k8s 清单、compose
scripts/        ags_probe.py（验证 AGS 行为）
docs/           设计、AGS、本地 K8s 记录
```
