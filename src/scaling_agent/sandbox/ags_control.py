"""Tencent Cloud AGS control plane (tencentcloud-sdk-python-ags, API 2025-09-20). Synchronous.

Facts this relies on (sources in docs/AGS.md):
* One `custom` sandbox tool (template) per worker image, created with `Persistent=True` (常驻沙箱).
  Persistence cannot be changed after creation and is only accepted for custom-type tools.
* `StartSandboxInstance` returns the instance nested under `Response.Instance`. Its `Timeout`
  (30s..24h, default 5m) is a hard reclaim deadline; `UpdateSandboxInstance(Timeout=...)` restarts
  the clock and only works on RUNNING instances. `ResumeSandboxInstance.Timeout` defaults to 5m.
* Idempotency: deterministic `ClientToken` (<= 64 chars) on create/start. Instances carry
  `Metadata` (worker_id, run_id); there is no metadata filter, so recovery matches client-side.
* AGS ignores the image CMD/ENTRYPOINT: the tool's Command/Args must start envd (port 49983),
  and a probe on envd `/health` (ReadyTimeoutMs <= 30000) is required for custom tools.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Iterable
from dataclasses import dataclass, field

from tencentcloud.ags.v20250920 import ags_client, models
from tencentcloud.common import credential
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.retry import StandardRetryer

ENVD_PORT = 49983
INSTANCE_FAILED = {"FAILED", "STARTING_FAILED", "STOPPING_FAILED", "STOP_FAILED", "PAUSE_FAILED", "RESUME_FAILED", "FORK_FAILED"}
INSTANCE_ALIVE = {"STARTING", "RUNNING", "PAUSING", "PAUSED"}
IDEMPOTENCY_HITS = ("FailedOperation.DuplicateRequest", "FailedOperation.RequestInProgress")


class AgsError(RuntimeError):
    pass


def client_token(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:64]


@dataclass
class ToolSpec:
    name: str
    image: str
    image_registry_type: str = "personal"
    role_arn: str | None = None
    command: list[str] = field(default_factory=lambda: ["/bin/sh", "-c"])
    args: list[str] = field(default_factory=lambda: [f"exec /usr/bin/envd -port {ENVD_PORT}"])
    env: dict[str, str] = field(default_factory=dict)
    cpu: str = "2"
    memory: str = "4Gi"
    disk: str | None = None
    network_mode: str = "PUBLIC"
    subnet_ids: list[str] = field(default_factory=list)
    security_group_ids: list[str] = field(default_factory=list)
    default_timeout: str | None = None
    persistent: bool = True
    # Ports reachable through AGS port forwarding at https://{port}-{instanceId}.{region}.tencentags.com,
    # besides envd's own.
    extra_ports: list[int] = field(default_factory=list)
    description: str = "scaling-agent worker"


def _env(k: str, v: str) -> models.EnvVar:
    e = models.EnvVar()
    e.Name, e.Value = k, str(v)
    return e


def _meta(k: str, v: str) -> models.MetadataVar:
    m = models.MetadataVar()
    m.Name, m.Value = k, str(v)
    return m


def tool_drift(tool: models.SandboxTool, spec: ToolSpec) -> list[str]:
    """Differences between an existing tool and the spec, as `field: have != want`."""
    cc = tool.CustomConfiguration
    res = cc.Resources if cc else None
    net = tool.NetworkConfiguration
    have = {
        "Persistent": bool(tool.Persistent),
        "Image": cc.Image if cc else None,
        "ImageRegistryType": cc.ImageRegistryType if cc else None,
        "RoleArn": tool.RoleArn or None,
        "CPU": res.CPU if res else None,
        "Memory": res.Memory if res else None,
        "Storage": (res.Storage if res else None) or None,
        "NetworkMode": net.NetworkMode if net else None,
        "Ports": sorted(p.Port for p in (cc.Ports or [])) if cc else [],
    }
    want = {
        "Persistent": spec.persistent,
        "Image": spec.image,
        "ImageRegistryType": spec.image_registry_type,
        "RoleArn": spec.role_arn or None,
        "CPU": spec.cpu,
        "Memory": spec.memory,
        "Storage": spec.disk or None,
        "NetworkMode": spec.network_mode,
        "Ports": sorted({ENVD_PORT, *spec.extra_ports}),
    }
    return [f"{k}: {have[k]!r} != {want[k]!r}" for k in want if have[k] != want[k]]


class AgsControlPlane:
    def __init__(self, secret_id: str, secret_key: str, region: str, endpoint: str = "ags.tencentcloudapi.com") -> None:
        cred = credential.Credential(secret_id, secret_key)
        profile = ClientProfile(httpProfile=HttpProfile(endpoint=endpoint, reqTimeout=30), retryer=StandardRetryer(max_attempts=3))
        self.region = region
        self.c = ags_client.AgsClient(cred, region, profile)

    # ------------------------------------------------------------------- tools

    def find_tool(self, name: str) -> models.SandboxTool | None:
        req = models.DescribeSandboxToolListRequest()
        f = models.Filter()
        f.Name, f.Values = "ToolName", [name]
        req.Filters, req.Limit = [f], 100
        for tool in self.c.DescribeSandboxToolList(req).SandboxToolSet or []:
            if tool.ToolName == name:  # guard in case the filter is ignored
                return tool
        return None

    def get_tool(self, tool_id: str) -> models.SandboxTool | None:
        req = models.DescribeSandboxToolListRequest()
        req.ToolIds = [tool_id]
        tools = self.c.DescribeSandboxToolList(req).SandboxToolSet or []
        return tools[0] if tools else None

    def ensure_tool(self, spec: ToolSpec, wait_s: float = 900) -> str:
        existing = self.find_tool(spec.name)
        if existing is not None:
            drift = tool_drift(existing, spec)
            if drift:
                # Reusing it would silently run workers on another image, disk, or network.
                raise AgsError(
                    f"tool {spec.name} exists with a different configuration ({'; '.join(drift)}); "
                    "delete it or use a different tool_name"
                )
            return self._wait_tool_active(existing.ToolId, wait_s)
        req = self._create_tool_request(spec)
        try:
            tool_id = self.c.CreateSandboxTool(req).ToolId
        except TencentCloudSDKException as e:
            if e.get_code() not in ("InvalidParameterValue.SandboxTool", *IDEMPOTENCY_HITS):
                raise
            time.sleep(2)
            existing = self.find_tool(spec.name)
            if existing is not None:
                tool_id = existing.ToolId
            elif e.get_code() == "FailedOperation.DuplicateRequest":
                # The token belongs to a tool of the same name and image that was deleted since;
                # AGS refuses to reuse it ("Previous tool no longer exists").
                req.ClientToken = client_token("CreateSandboxTool", spec.name, spec.image, str(time.time_ns()))
                tool_id = self.c.CreateSandboxTool(req).ToolId
            else:
                raise
        return self._wait_tool_active(tool_id, wait_s)

    def _create_tool_request(self, spec: ToolSpec) -> models.CreateSandboxToolRequest:
        req = models.CreateSandboxToolRequest()
        req.ToolName, req.ToolType, req.Persistent = spec.name, "custom", spec.persistent
        req.Description = spec.description[:200]
        req.ClientToken = client_token("CreateSandboxTool", spec.name, spec.image)
        if spec.default_timeout:
            req.DefaultTimeout = spec.default_timeout
        if spec.role_arn:
            req.RoleArn = spec.role_arn
        net = models.NetworkConfiguration()
        net.NetworkMode = spec.network_mode
        if spec.network_mode == "VPC":
            vpc = models.VPCConfig()
            vpc.SubnetIds, vpc.SecurityGroupIds = spec.subnet_ids, spec.security_group_ids
            net.VpcConfig = vpc
        req.NetworkConfiguration = net
        cc = models.CustomConfiguration()
        cc.Image, cc.ImageRegistryType = spec.image, spec.image_registry_type
        cc.Command, cc.Args = spec.command, spec.args
        cc.Env = [_env(k, v) for k, v in spec.env.items()]
        cc.Ports = []
        for name, number in [("envd", ENVD_PORT), *((f"port-{p}", p) for p in spec.extra_ports)]:
            port = models.PortConfiguration()
            port.Name, port.Port, port.Protocol = name, number, "TCP"
            cc.Ports.append(port)
        res = models.ResourceConfiguration()
        res.CPU, res.Memory = spec.cpu, spec.memory
        if spec.disk:
            res.Storage = spec.disk
        cc.Resources = res
        http_get = models.HttpGetAction()
        http_get.Path, http_get.Port, http_get.Scheme = "/health", ENVD_PORT, "HTTP"
        probe = models.ProbeConfiguration()
        probe.HttpGet = http_get
        probe.ReadyTimeoutMs, probe.ProbeTimeoutMs, probe.ProbePeriodMs = 30000, 2000, 1000
        probe.SuccessThreshold, probe.FailureThreshold = 1, 60
        cc.Probe = probe
        req.CustomConfiguration = cc
        return req

    def _wait_tool_active(self, tool_id: str, wait_s: float) -> str:
        deadline = time.time() + wait_s
        while time.time() < deadline:
            tool = self.get_tool(tool_id)
            status = tool.Status if tool else None
            if status == "ACTIVE":
                return tool_id
            if status == "FAILED":
                raise AgsError(f"tool {tool_id} FAILED: {tool.StatusReason}")
            time.sleep(5)
        raise AgsError(f"tool {tool_id} not ACTIVE after {wait_s:.0f}s")

    # --------------------------------------------------------------- instances

    def list_instances(
        self, tool_id: str | None = None, statuses: Iterable[str] | None = None, instance_ids: list[str] | None = None
    ) -> list[models.SandboxInstance]:
        out: list[models.SandboxInstance] = []
        token = None
        while True:
            req = models.DescribeSandboxInstanceListRequest()
            if tool_id:
                req.ToolId = tool_id
            if instance_ids:
                req.InstanceIds = instance_ids[:100]
            if statuses:
                f = models.Filter()
                f.Name, f.Values = "Status", sorted(statuses)
                req.Filters = [f]
            req.MaxResults = 100
            if token:
                req.NextToken = token
            resp = self.c.DescribeSandboxInstanceList(req)
            page = resp.InstanceSet or []
            out.extend(page)
            token = resp.NextToken
            if not token or len(page) < 100:
                return out

    def get_instance(self, instance_id: str) -> models.SandboxInstance | None:
        found = self.list_instances(instance_ids=[instance_id])
        return found[0] if found else None

    def find_worker_instance(self, tool_id: str, worker_id: str, run_id: str) -> models.SandboxInstance | None:
        for inst in self.list_instances(tool_id=tool_id, statuses=INSTANCE_ALIVE):
            md = {m.Name: m.Value for m in (inst.Metadata or [])}
            if md.get("worker_id") == worker_id and md.get("run_id") == run_id:
                return inst
        return None

    def start_instance(
        self, tool_id: str, worker_id: str, run_id: str, timeout: str | None, replaces: str | None = None,
        wait_s: float = 600, auth_mode: str = "TOKEN",
    ) -> models.SandboxInstance:
        """Idempotent per (run_id, worker_id, replaced instance); re-attaches to a live instance if any.

        The idempotency token of a replacement is derived from the dead instance's id, so it is
        stable across launcher restarts yet different from the original start.
        `auth_mode`: TOKEN puts every port behind X-Access-Token; PUBLIC keeps only envd behind it.
        """
        existing = self.find_worker_instance(tool_id, worker_id, run_id)
        if existing is not None:
            if existing.Status == "PAUSED":
                self.resume(existing.InstanceId, timeout)
            return self.wait_running(existing.InstanceId, wait_s)
        req = models.StartSandboxInstanceRequest()
        req.ToolId, req.AuthMode = tool_id, auth_mode
        if timeout:
            req.Timeout = timeout
        req.ClientToken = client_token("StartSandboxInstance", run_id, worker_id, replaces or "first")
        req.Metadata = [_meta("worker_id", worker_id), _meta("run_id", run_id)]
        try:
            inst = self.c.StartSandboxInstance(req).Instance
        except TencentCloudSDKException as e:
            if e.get_code() not in IDEMPOTENCY_HITS:
                raise  # e.g. LimitExceeded.SandboxInstance: the launcher backs off
            time.sleep(2)
            inst = self.find_worker_instance(tool_id, worker_id, run_id)
            if inst is None:
                raise
        return self.wait_running(inst.InstanceId, wait_s)

    def wait_running(self, instance_id: str, wait_s: float = 600, interval: float = 3) -> models.SandboxInstance:
        deadline = time.time() + wait_s
        while time.time() < deadline:
            inst = self.get_instance(instance_id)
            status = inst.Status if inst else None
            if status == "RUNNING":
                return inst
            if status in INSTANCE_FAILED or status == "STOPPED":
                raise AgsError(f"instance {instance_id} -> {status} (StopReason={getattr(inst, 'StopReason', None)})")
            time.sleep(interval)
        raise AgsError(f"instance {instance_id} not RUNNING after {wait_s:.0f}s")

    def acquire_token(self, instance_id: str) -> tuple[str, str | None]:
        """(token, expires_at). The token goes in X-Access-Token for every port (AuthMode=TOKEN)."""
        req = models.AcquireSandboxInstanceTokenRequest()
        req.InstanceId = instance_id
        resp = self.c.AcquireSandboxInstanceToken(req)
        return resp.Token, resp.ExpiresAt

    def extend(self, instance_id: str, timeout: str) -> None:
        req = models.UpdateSandboxInstanceRequest()
        req.InstanceId, req.Timeout = instance_id, timeout
        self.c.UpdateSandboxInstance(req)

    def resume(self, instance_id: str, timeout: str | None) -> None:
        req = models.ResumeSandboxInstanceRequest()
        req.InstanceId = instance_id
        if timeout:
            req.Timeout = timeout  # the default is 5 minutes
        self.c.ResumeSandboxInstance(req)

    def stop(self, instance_id: str) -> None:
        req = models.StopSandboxInstanceRequest()
        req.InstanceId = instance_id
        try:
            self.c.StopSandboxInstance(req)
        except TencentCloudSDKException as e:
            if e.get_code() != "ResourceNotFound.SandboxInstance":
                raise

    def quota(self) -> dict[str, tuple[float, float]]:
        resp = self.c.DescribeQuotaOverview(models.DescribeQuotaOverviewRequest())
        quota, usage = resp.AccountQuotaOverview.Quota, resp.AccountQuotaOverview.Usage
        keys = ("SandboxTools", "SandboxInstances", "PausedInstances", "CPUCores", "MemoryGiB")
        return {k: (getattr(usage, k), getattr(quota, k)) for k in keys}
