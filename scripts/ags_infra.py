#!/usr/bin/env python3
"""Run the organization infrastructure (Gitea + coordination server) in one AGS sandbox.

For setups where worker sandboxes cannot reach any host of yours (no public HTTPS, security
groups). The instance is started with AuthMode=PUBLIC, so AGS port forwarding serves Gitea and the
coordination server at https://{port}-{instanceId}.{region}.tencentags.com without a token, while
envd stays behind one. Both services authenticate every request themselves (Gitea requires sign-in,
the coordination server bearer tokens).

    docker build -f deploy/Dockerfile --target infra-ags --platform linux/amd64 -t <registry>/sa-infra:v1 .
    export TENCENTCLOUD_SECRET_ID=... TENCENTCLOUD_SECRET_KEY=...
    export SA_COORD_ADMIN_TOKEN=... SA_GITEA_ADMIN_PASSWORD=...
    export AGS_IMAGE=<registry>/sa-infra:v1 AGS_ROLE_ARN=qcs::cam::uin/<uin>:roleName/<role>
    python scripts/ags_infra.py up      # idempotent; prints the URLs for the run file
    python scripts/ags_infra.py down

Other settings: AGS_REGION (ap-singapore), AGS_ENDPOINT (ags.tencentcloudapi.com; international
site: ags.intl.tencentcloudapi.com), AGS_IMAGE_REGISTRY_TYPE (personal), AGS_TOOL_NAME (sa-infra),
SA_INFRA_NAME (sa-infra: one instance per name), SA_GITEA_ADMIN_USER (root).
"""

from __future__ import annotations

import asyncio
import os
import shlex
import sys

import httpx
from e2b import AsyncSandbox
from e2b.connection_config import ConnectionConfig
from packaging.version import Version

from scaling_agent.sandbox.ags_control import INSTANCE_ALIVE, AgsControlPlane, ToolSpec

GITEA_PORT, COORD_PORT = 3000, 8700
REGION = os.getenv("AGS_REGION", "ap-singapore")
ENDPOINT = os.getenv("AGS_ENDPOINT", "ags.tencentcloudapi.com")
DOMAIN = f"{REGION}.tencentags.com"
NAME = os.getenv("SA_INFRA_NAME", "sa-infra")
TOOL = os.getenv("AGS_TOOL_NAME", "sa-infra")


def public_url(instance_id: str, port: int) -> str:
    return f"https://{port}-{instance_id}.{DOMAIN}"


def control_plane() -> AgsControlPlane:
    return AgsControlPlane(os.environ["TENCENTCLOUD_SECRET_ID"], os.environ["TENCENTCLOUD_SECRET_KEY"], REGION, ENDPOINT)


async def sandbox(cp: AgsControlPlane, instance_id: str) -> AsyncSandbox:
    token, _ = await asyncio.to_thread(cp.acquire_token, instance_id)
    cfg = ConnectionConfig(domain=DOMAIN, request_timeout=60, extra_sandbox_headers={"X-Access-Token": token})
    return AsyncSandbox(
        sandbox_id=instance_id, sandbox_domain=DOMAIN, envd_version=Version("0.5.14"),
        envd_access_token=token, connection_config=cfg,
    )


async def healthy(sbx: AsyncSandbox, url: str) -> bool:
    r = await sbx.commands.run(f"curl -fsS -m 5 -o /dev/null {shlex.quote(url)} && echo ok || true", user="root")
    return r.stdout.strip().endswith("ok")


async def wait_healthy(sbx: AsyncSandbox, url: str, log: str, attempts: int = 60) -> None:
    for _ in range(attempts):
        if await healthy(sbx, url):
            return
        await asyncio.sleep(2)
    tail = await sbx.commands.run(f"tail -n 40 {log} || true", user="root")
    raise SystemExit(f"{url} did not become healthy; last lines of {log}:\n{tail.stdout}")


async def up() -> None:
    admin_token = os.environ["SA_COORD_ADMIN_TOKEN"]
    gitea_user = os.getenv("SA_GITEA_ADMIN_USER", "root")
    gitea_password = os.environ["SA_GITEA_ADMIN_PASSWORD"]
    cp = control_plane()
    tool_id = await asyncio.to_thread(
        cp.ensure_tool,
        ToolSpec(
            name=TOOL,
            image=os.environ["AGS_IMAGE"],
            image_registry_type=os.getenv("AGS_IMAGE_REGISTRY_TYPE", "personal"),
            role_arn=os.getenv("AGS_ROLE_ARN"),
            disk="20Gi",
            extra_ports=[GITEA_PORT, COORD_PORT],
            description="scaling-agent infrastructure: Gitea + coordination server",
        ),
    )
    inst = await asyncio.to_thread(cp.start_instance, tool_id, "infra", NAME, None, None, 600, "PUBLIC")
    iid = inst.InstanceId
    gitea_url, coord_url = public_url(iid, GITEA_PORT), public_url(iid, COORD_PORT)
    sbx = await sandbox(cp, iid)

    if not await healthy(sbx, f"http://127.0.0.1:{GITEA_PORT}/api/healthz"):
        env = {
            "GITEA_ROOT_URL": f"{gitea_url}/",
            "GITEA_ADMIN_USER": gitea_user,
            "GITEA_ADMIN_PASSWORD": gitea_password,
            # Webhooks only go to the coordination server next to it.
            "GITEA_WEBHOOK_ALLOWED_HOSTS": "loopback",
            "GITEA_WORK_DIR": "/data",
            "HOME": "/home/git",
        }
        await sbx.commands.run(
            "exec sa-gitea-entrypoint >> /data/gitea.log 2>&1", background=True, envs=env, cwd="/data", user="git", timeout=0
        )
        await wait_healthy(sbx, f"http://127.0.0.1:{GITEA_PORT}/api/healthz", "/data/gitea.log")

    if not await healthy(sbx, f"http://127.0.0.1:{COORD_PORT}/healthz"):
        env = {
            "SA_COORD_ADMIN_TOKEN": admin_token,
            "SA_COORD_DB_PATH": "/opt/coord/coord.sqlite3",
            "SA_COORD_CONFLICT_MIRROR_DIR": "/opt/coord/coord-mirror.git",
            "SA_COORD_SELF_URL": f"http://127.0.0.1:{COORD_PORT}",
            "SA_COORD_GITEA__URL": f"http://127.0.0.1:{GITEA_PORT}",
            "SA_COORD_GITEA__ADMIN_USER": gitea_user,
            "SA_COORD_GITEA__ADMIN_PASSWORD": gitea_password,
            **{k: v for k, v in os.environ.items() if k.startswith("SA_COORD_") and k != "SA_COORD_ADMIN_TOKEN"},
        }
        await sbx.commands.run(
            "mkdir -p /opt/coord && exec scaling-agent coord serve >> /opt/coord/coord.log 2>&1",
            background=True, envs=env, cwd="/opt", user="root", timeout=0,
        )
        await wait_healthy(sbx, f"http://127.0.0.1:{COORD_PORT}/healthz", "/opt/coord/coord.log")

    # The same URLs serve the launcher (this machine) and the workers (other sandboxes).
    async with httpx.AsyncClient(timeout=30) as http:
        for url in (f"{gitea_url}/api/healthz", f"{coord_url}/healthz"):
            (await http.get(url)).raise_for_status()
    print(f"instance: {iid}")
    print(f"coord_url / coord_public_url: {coord_url}")
    print(f"gitea.url / gitea.public_url: {gitea_url}")


async def down() -> None:
    cp = control_plane()
    tool = await asyncio.to_thread(cp.find_tool, TOOL)
    if tool is None:
        print(f"tool {TOOL} absent")
        return
    inst = await asyncio.to_thread(cp.find_worker_instance, tool.ToolId, "infra", NAME)
    if inst is None or inst.Status not in INSTANCE_ALIVE:
        print(f"no live {NAME} instance")
        return
    await asyncio.to_thread(cp.stop, inst.InstanceId)
    print(f"stopped {inst.InstanceId}")


if __name__ == "__main__":
    commands = {"up": up, "down": down}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        raise SystemExit(f"usage: {sys.argv[0]} {'|'.join(commands)}")
    asyncio.run(commands[sys.argv[1]]())
