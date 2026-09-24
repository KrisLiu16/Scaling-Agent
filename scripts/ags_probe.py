#!/usr/bin/env python3
"""Settle the AGS behaviours the docs leave open. Run once, from a machine that can reach
*.tencentcloudapi.com and *.tencentags.com, before launching a real run.

    pip install -e ".[ags]"
    export TENCENTCLOUD_SECRET_ID=... TENCENTCLOUD_SECRET_KEY=...
    export AGS_IMAGE=sgccr.ccs.tencentyun.com/<ns>/sa-worker:v1 AGS_ROLE_ARN=qcs::cam::uin/<uin>:roleName/<role>
    python scripts/ags_probe.py            # AGS_KEEP=1 keeps the instance running afterwards

Answers:
  1. Is a Persistent=true custom tool accepted in this region?
  2. Started without Timeout, does the instance report TimeoutSeconds=None / ExpiresAt=None (no 24h cap)?
  3. Token lifetime (ExpiresAt) and whether TrafficToken is returned.
  4. envd data plane over X-Access-Token: exec, files, background process.
  5. Does a background process survive Pause(Memory=true) + Resume?
"""

from __future__ import annotations

import asyncio
import json
import os
import time

from e2b import AsyncSandbox
from e2b.connection_config import ConnectionConfig
from packaging.version import Version
from tencentcloud.ags.v20250920 import models

from scaling_agent.sandbox.ags_control import ENVD_PORT, AgsControlPlane, ToolSpec

REGION = os.getenv("AGS_REGION", "ap-singapore")
DOMAIN = f"{REGION}.tencentags.com"


def show(label: str, inst) -> None:
    print(f"{label}: status={inst.Status} persistent={inst.Persistent} "
          f"TimeoutSeconds={inst.TimeoutSeconds} ExpiresAt={inst.ExpiresAt} StopReason={getattr(inst, 'StopReason', None)}")


async def main() -> None:
    cp = AgsControlPlane(os.environ["TENCENTCLOUD_SECRET_ID"], os.environ["TENCENTCLOUD_SECRET_KEY"], REGION)
    print("quota (usage, limit):", json.dumps(cp.quota()))
    name = os.getenv("AGS_TOOL_NAME", "sa-probe")
    tool_id = cp.ensure_tool(
        ToolSpec(
            name=name,
            image=os.environ["AGS_IMAGE"],
            image_registry_type=os.getenv("AGS_IMAGE_REGISTRY_TYPE", "personal"),
            role_arn=os.getenv("AGS_ROLE_ARN"),
            persistent=os.getenv("AGS_PERSISTENT", "1") == "1",
        )
    )
    print("tool", tool_id, "persistent=", cp.get_tool(tool_id).Persistent)

    inst = cp.start_instance(tool_id, "probe", f"probe-{int(time.time())}", timeout=None)
    show("started", inst)
    token, expires_at = cp.acquire_token(inst.InstanceId)
    raw = cp.c.AcquireSandboxInstanceToken(_token_req(inst.InstanceId))
    print("token expires_at=", expires_at, "traffic token returned=", bool(raw.TrafficToken))

    cfg = ConnectionConfig(domain=DOMAIN, request_timeout=60, extra_sandbox_headers={
        "X-Access-Token": token, "E2b-Sandbox-Id": inst.InstanceId, "E2b-Sandbox-Port": str(ENVD_PORT)})
    sbx = AsyncSandbox(sandbox_id=inst.InstanceId, sandbox_domain=DOMAIN, envd_version=Version("0.5.14"),
                       envd_access_token=token, connection_config=cfg)
    r = await sbx.commands.run("uname -a; id; df -h / | tail -1; which git python3", user="root")
    print("exec exit", r.exit_code, "\n", r.stdout)
    await sbx.files.write("/workspace/probe.txt", "hello")
    print("file read:", await sbx.files.read("/workspace/probe.txt"))
    bg = await sbx.commands.run("sleep 3600", background=True, user="root", timeout=0)
    print("background pid", bg.pid)

    pause = models.PauseSandboxInstanceRequest()
    pause.InstanceId, pause.Memory = inst.InstanceId, True
    cp.c.PauseSandboxInstance(pause)
    for _ in range(60):
        if (cur := cp.get_instance(inst.InstanceId)).Status == "PAUSED":
            break
        time.sleep(3)
    show("paused", cur)
    cp.resume(inst.InstanceId, None)
    show("resumed", cp.wait_running(inst.InstanceId))
    pids = [p.pid for p in await sbx.commands.list()]
    print("background process survived pause/resume:", bg.pid in pids)

    if os.getenv("AGS_KEEP", "0") != "1":
        cp.stop(inst.InstanceId)
        print("stopped", inst.InstanceId)


def _token_req(instance_id: str) -> models.AcquireSandboxInstanceTokenRequest:
    req = models.AcquireSandboxInstanceTokenRequest()
    req.InstanceId = instance_id
    return req


if __name__ == "__main__":
    asyncio.run(main())
