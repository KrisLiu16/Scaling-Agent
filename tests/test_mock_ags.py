"""The real AGS provider (Cloud API SDK + e2b SDK) against the local mock AGS and a real envd.

Needs an envd binary (deploy/envd/build.sh builds one from e2b-dev/infra); skipped otherwise.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
from pathlib import Path

import httpx
import pytest
import uvicorn

from scaling_agent.config import ProviderSettings
from scaling_agent.mock_ags.backends import StaticBackend
from scaling_agent.mock_ags.server import MockAgs, build_control_app, build_gateway_app

ENVD = os.environ.get("SA_ENVD_BIN") or str(Path(__file__).resolve().parents[1] / "deploy/envd/envd")
pytestmark = pytest.mark.skipif(not Path(ENVD).exists(), reason="envd binary not built (deploy/envd/build.sh)")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def mock_ags(tmp_path, monkeypatch):
    envd_port, cport, gport = free_port(), free_port(), free_port()
    # Own session: on shutdown envd signals its whole process group, which must not be pytest.
    envd = subprocess.Popen([ENVD, "-isnotfc", "-no-cgroups", "-port", str(envd_port)], cwd=tmp_path, start_new_session=True,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            if httpx.get(f"http://127.0.0.1:{envd_port}/health").status_code == 204:
                break
        except httpx.HTTPError:
            await asyncio.sleep(0.1)
    monkeypatch.setenv("TENCENTCLOUD_SECRET_ID", "AKIDmock")
    monkeypatch.setenv("TENCENTCLOUD_SECRET_KEY", "mock-secret")
    ags = MockAgs(StaticBackend(f"http://127.0.0.1:{envd_port}"), "AKIDmock", "mock-secret", max_instances=2)
    servers = [
        uvicorn.Server(uvicorn.Config(build_control_app(ags), host="127.0.0.1", port=cport, log_level="warning",
                                      timeout_graceful_shutdown=1)),
        uvicorn.Server(uvicorn.Config(build_gateway_app(ags), host="127.0.0.1", port=gport, log_level="warning",
                                      timeout_graceful_shutdown=1)),
    ]
    tasks = [asyncio.create_task(s.serve()) for s in servers]
    while not all(s.started for s in servers):
        await asyncio.sleep(0.05)
    yield ags, cport, gport, tmp_path
    for s in servers:
        s.should_exit = True
    await asyncio.gather(*tasks)
    envd.terminate()
    envd.wait()


def settings(cport: int, gport: int, runtime: str) -> ProviderSettings:
    return ProviderSettings(
        kind="ags", endpoint=f"http://127.0.0.1:{cport}", data_plane_url=f"http://127.0.0.1:{gport}",
        image="sa-worker-ags:dev", image_registry_type="custom", envd_version="0.9.0",
        envd_flags="-isnotfc -no-cgroups", runtime_command=runtime, persistent=True,
    )


async def test_ags_provider_lifecycle_against_mock(mock_ags):
    from scaling_agent.sandbox.ags import AgsProvider

    ags, cport, gport, tmp = mock_ags
    marker = tmp / "runtime-started"
    provider = AgsProvider(settings(cport, gport, f"sh -c 'echo $SA_WORKER_WORKER_ID > {marker}; sleep 300'"), "run1")
    await provider.prepare()
    tool = next(iter(ags.tools.values()))
    assert tool["Persistent"] is True and tool["CustomConfiguration"]["Args"][0].endswith("-isnotfc -no-cgroups")

    env = {"SA_WORKER_WORKER_ID": "w0001", "SA_WORKER_WORKDIR": str(tmp / "ws/repo"), "SA_WORKER_STATE_DIR": str(tmp / "ws/state")}
    handle = await provider.start("w0001", env)
    inst = ags.instances[handle.sandbox_id]
    assert inst.Status == "RUNNING" and inst.TimeoutSeconds is None and handle.meta["pid"]
    for _ in range(50):
        if marker.exists():
            break
        await asyncio.sleep(0.1)
    assert marker.read_text().strip() == "w0001"  # env reached the runtime through envd
    assert await provider.runtime_alive(handle) is True

    again = await provider.start("w0001", env)  # idempotent: same ClientToken -> same instance
    assert again.sandbox_id == handle.sandbox_id and again.meta["pid"] == handle.meta["pid"]  # no 2nd runtime

    address, ags.backend.address = ags.backend.address, "http://127.0.0.1:9"  # envd unreachable
    assert await provider.runtime_alive(handle) is None  # instance still RUNNING: unknown, not "dead"
    ags.backend.address = address

    await ags.backend.stop(handle.sandbox_id)  # the sandbox dies under us
    assert await provider.runtime_alive(handle) is False
    dead = handle.sandbox_id
    await provider.relaunch(handle, env)  # a replacement instance, not the dead one handed back
    assert handle.sandbox_id != dead and ags.instances[handle.sandbox_id].Status == "RUNNING"
    assert await provider.runtime_alive(handle) is True

    await provider.stop(handle)
    assert ags.instances[handle.sandbox_id].Status == "STOPPED"
    assert await provider.runtime_alive(handle) is False


async def test_mock_rejects_bad_signatures_and_tokens(mock_ags):
    from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException

    from scaling_agent.sandbox.ags_control import AgsControlPlane

    ags, cport, gport, _ = mock_ags
    run = asyncio.to_thread  # the SDK is synchronous; the mock serves on this event loop
    bad = AgsControlPlane("AKIDmock", "wrong-secret", "ap-singapore", f"http://127.0.0.1:{cport}")
    with pytest.raises(TencentCloudSDKException) as err:
        await run(bad.quota)
    assert err.value.get_code() == "AuthFailure.SignatureFailure"

    good = AgsControlPlane("AKIDmock", "mock-secret", "ap-singapore", f"http://127.0.0.1:{cport}")
    from scaling_agent.sandbox.ags_control import ToolSpec

    tool_id = await run(good.ensure_tool, ToolSpec(name="t1", image="img", image_registry_type="custom"))
    inst = await run(good.start_instance, tool_id, "w1", "run", "60s")
    assert inst.TimeoutSeconds == 60  # time-limited when a Timeout is given
    async with httpx.AsyncClient() as http:
        r = await http.get(f"http://127.0.0.1:{gport}/health", headers={"E2b-Sandbox-Id": inst.InstanceId, "X-Access-Token": "nope"})
        assert r.status_code == 401
        token, _ = await run(good.acquire_token, inst.InstanceId)
        r = await http.get(f"http://127.0.0.1:{gport}/health", headers={"E2b-Sandbox-Id": inst.InstanceId, "X-Access-Token": token})
        assert r.status_code == 204
    await run(good.start_instance, tool_id, "w2", "run", None)
    with pytest.raises(TencentCloudSDKException) as err:
        await run(good.start_instance, tool_id, "w3", "run", None)  # max_instances=2
    assert err.value.get_code() == "LimitExceeded.SandboxInstance"


def test_envd_binary_is_executable():
    assert shutil.which(ENVD) or os.access(ENVD, os.X_OK)
