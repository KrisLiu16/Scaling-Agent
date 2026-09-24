"""Mock AGS: Tencent Cloud API v3 control plane + E2B data-plane gateway."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import re
import secrets
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.routing import Route

from .backends import Backend

log = logging.getLogger(__name__)

PERSISTENT_TYPES = {"custom", "mobile", "android-world", "osworld", "waa"}
ENVD_PORT = 49983
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
              "transfer-encoding", "upgrade", "host", "content-length"}


class ApiError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def iso(ts: float | None) -> str | None:
    return None if ts is None else datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_duration(value: str | None, lo: float = 30, hi: float = 24 * 3600) -> float | None:
    """AGS duration strings: 5m, 300s, 1h (min 30s, max 24h)."""
    if value in (None, ""):
        return None
    m = re.fullmatch(r"(\d+)([smh])", str(value).strip())
    if not m:
        raise ApiError("InvalidParameterValue.Timeout", f"invalid timeout {value!r}; use e.g. 300s, 5m, 1h")
    seconds = int(m.group(1)) * {"s": 1, "m": 60, "h": 3600}[m.group(2)]
    if not lo <= seconds <= hi:
        raise ApiError("InvalidParameterValue.Timeout", f"timeout must be between {lo:.0f}s and {hi:.0f}s")
    return float(seconds)


@dataclass
class Instance:
    InstanceId: str
    ToolId: str
    ToolName: str
    Status: str
    Persistent: bool
    AuthMode: str
    Token: str
    TrafficToken: str
    ClientToken: str | None
    CreateTime: float
    UpdateTime: float
    TimeoutSeconds: float | None = None
    ExpiresAt: float | None = None
    StopReason: str | None = None
    Metadata: list[dict[str, str]] = field(default_factory=list)
    Overrides: dict[str, Any] = field(default_factory=dict)

    def api(self) -> dict[str, Any]:
        return {
            "InstanceId": self.InstanceId, "ToolId": self.ToolId, "ToolName": self.ToolName, "Status": self.Status,
            "Persistent": self.Persistent, "TimeoutSeconds": None if self.TimeoutSeconds is None else int(self.TimeoutSeconds),
            "ExpiresAt": iso(self.ExpiresAt), "StopReason": self.StopReason, "CreateTime": iso(self.CreateTime),
            "UpdateTime": iso(self.UpdateTime), "Metadata": self.Metadata, "AuthMode": self.AuthMode,
            "NetworkMode": "PUBLIC",
        }


class MockAgs:
    def __init__(self, backend: Backend, secret_id: str, secret_key: str, state_file: str | None = None,
                 max_instances: int = 200, verify_signatures: bool = True, max_clock_skew_s: float = 300) -> None:
        self.backend = backend
        self.secret_id = secret_id
        self.secret_key = secret_key
        self.state_file = Path(state_file) if state_file else None
        self.max_instances = max_instances
        self.verify_signatures = verify_signatures
        self.max_clock_skew_s = max_clock_skew_s
        self.tools: dict[str, dict[str, Any]] = {}
        self.instances: dict[str, Instance] = {}
        self._lock = asyncio.Lock()
        self._load()

    # ------------------------------------------------------------ persistence

    def _load(self) -> None:
        if self.state_file and self.state_file.exists():
            data = json.loads(self.state_file.read_text())
            self.tools = data.get("tools", {})
            self.instances = {k: Instance(**v) for k, v in data.get("instances", {}).items()}

    def _save(self) -> None:
        if self.state_file:
            tmp = self.state_file.with_suffix(".tmp")
            tmp.write_text(json.dumps({"tools": self.tools, "instances": {k: asdict(v) for k, v in self.instances.items()}}))
            tmp.replace(self.state_file)

    # ----------------------------------------------------------------- auth

    def verify(self, request: Request, body: bytes) -> None:
        """TC3-HMAC-SHA256, exactly as tencentcloud-sdk signs (SignedHeaders=content-type;host)."""
        if not self.verify_signatures:
            return
        auth = request.headers.get("authorization", "")
        m = re.fullmatch(
            r"TC3-HMAC-SHA256 Credential=([^/]+)/(\d{4}-\d{2}-\d{2})/([^/]+)/tc3_request, "
            r"SignedHeaders=content-type;host, Signature=([0-9a-f]{64})", auth)
        if not m:
            raise ApiError("AuthFailure.SignatureFailure", "missing or malformed TC3 Authorization header")
        secret_id, date, service, signature = m.groups()
        if secret_id != self.secret_id:
            raise ApiError("AuthFailure.SecretIdNotFound", "unknown SecretId")
        timestamp = int(request.headers.get("x-tc-timestamp", "0"))
        if abs(time.time() - timestamp) > self.max_clock_skew_s:
            raise ApiError("AuthFailure.SignatureExpire", "request timestamp too far from server time")
        canonical = "\n".join([
            "POST", "/", "",
            f"content-type:{request.headers.get('content-type', '')}\nhost:{request.headers.get('host', '')}\n",
            "content-type;host",
            hashlib.sha256(body).hexdigest(),
        ])
        to_sign = f"TC3-HMAC-SHA256\n{timestamp}\n{date}/{service}/tc3_request\n{hashlib.sha256(canonical.encode()).hexdigest()}"
        k = hmac.new(("TC3" + self.secret_key).encode(), date.encode(), hashlib.sha256).digest()
        k = hmac.new(k, service.encode(), hashlib.sha256).digest()
        k = hmac.new(k, b"tc3_request", hashlib.sha256).digest()
        expected = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise ApiError("AuthFailure.SignatureFailure", "signature does not match")

    # -------------------------------------------------------------- actions

    async def refresh(self, inst: Instance) -> Instance:
        """Fold the backend's view into the instance (pod started, crashed, disappeared)."""
        if inst.Status in ("STOPPED", "FAILED", "PAUSED"):
            return inst
        st = await self.backend.status(inst.InstanceId)
        if st.status != inst.Status:
            if st.status in ("STOPPED", "FAILED") and inst.Status in ("STARTING", "RUNNING"):
                inst.StopReason = inst.StopReason or ("error" if st.status == "FAILED" else "system")
            inst.Status = st.status
            inst.UpdateTime = time.time()
            self._save()
        return inst

    def _tool(self, params: dict[str, Any]) -> dict[str, Any]:
        tool_id, name = params.get("ToolId"), params.get("ToolName")
        for tool in self.tools.values():
            if (tool_id and tool["ToolId"] == tool_id) or (name and tool["ToolName"] == name):
                return tool
        raise ApiError("ResourceNotFound.SandboxTool", "sandbox tool not found")

    async def CreateSandboxTool(self, p: dict[str, Any]) -> dict[str, Any]:
        name, ttype = p.get("ToolName"), p.get("ToolType")
        if not name or not re.fullmatch(r"[A-Za-z0-9_-]{1,50}", name):
            raise ApiError("InvalidParameterValue.ToolName", "ToolName: 1-50 chars of [A-Za-z0-9_-]")
        if any(t["ToolName"] == name for t in self.tools.values()):
            raise ApiError("InvalidParameterValue.SandboxTool", "沙箱工具名称不可用，可能是已经存在")
        if p.get("Persistent") and ttype not in PERSISTENT_TYPES:
            raise ApiError("InvalidParameterValue",
                           f"persistent mode is only supported for custom, mobile, android-world, osworld and waa tool types, got {ttype}")
        cc = p.get("CustomConfiguration") or {}
        if ttype == "custom":
            if not cc.get("Image"):
                raise ApiError("MissingParameter", "CustomConfiguration.Image is required for custom tools")
            probe = cc.get("Probe")
            if not probe:
                raise ApiError("MissingParameter", "CustomConfiguration.Probe is required for custom tools")
            if int(probe.get("ReadyTimeoutMs") or 0) > 30000:
                raise ApiError("InvalidParameterValue", "Probe.ReadyTimeoutMs must be <= 30000")
            if cc.get("ImageRegistryType") in ("enterprise", "personal") and not p.get("RoleArn"):
                raise ApiError("MissingParameter.RoleArn", "RoleArn is required for TCR/CCR images")
        default_timeout = parse_duration(p.get("DefaultTimeout")) if p.get("DefaultTimeout") else 300.0
        tool_id = f"sdt-{uuid.uuid4().hex[:12]}"
        self.tools[tool_id] = {
            "ToolId": tool_id, "ToolName": name, "ToolType": ttype, "Status": "ACTIVE", "StatusReason": None,
            "Persistent": bool(p.get("Persistent")), "DefaultTimeoutSeconds": int(default_timeout),
            "Description": p.get("Description"), "NetworkConfiguration": p.get("NetworkConfiguration"),
            "RoleArn": p.get("RoleArn"), "CustomConfiguration": cc, "CreateTime": iso(time.time()),
        }
        self._save()
        return {"ToolId": tool_id}

    async def DescribeSandboxToolList(self, p: dict[str, Any]) -> dict[str, Any]:
        tools = list(self.tools.values())
        if p.get("ToolIds"):
            tools = [t for t in tools if t["ToolId"] in p["ToolIds"]]
        for f in p.get("Filters") or []:
            key, values = f.get("Name"), set(f.get("Values") or [])
            tools = [t for t in tools if str(t.get(key)) in values]
        return {"SandboxToolSet": tools, "TotalCount": len(tools)}

    async def DeleteSandboxTool(self, p: dict[str, Any]) -> dict[str, Any]:
        tool = self._tool(p)
        if any(i.ToolId == tool["ToolId"] and i.Status not in ("STOPPED", "FAILED") for i in self.instances.values()):
            raise ApiError("ResourceInUse.SandboxTool", "tool still has instances")
        del self.tools[tool["ToolId"]]
        self._save()
        return {}

    async def StartSandboxInstance(self, p: dict[str, Any]) -> dict[str, Any]:
        tool = self._tool(p)
        token = p.get("ClientToken")
        if token:
            for inst in self.instances.values():
                if inst.ClientToken == token:  # idempotent retry: same logical request, same instance
                    return {"Instance": (await self.refresh(inst)).api()}
        live = [i for i in self.instances.values() if i.Status in ("STARTING", "RUNNING", "PAUSED")]
        if len(live) >= self.max_instances:
            raise ApiError("LimitExceeded.SandboxInstance", "沙箱实例配额超限")
        timeout = parse_duration(p.get("Timeout"))
        if timeout is None and not tool["Persistent"]:
            timeout = float(tool["DefaultTimeoutSeconds"])
        now = time.time()
        inst = Instance(
            InstanceId=f"sbi-{uuid.uuid4().hex[:12]}", ToolId=tool["ToolId"], ToolName=tool["ToolName"],
            Status="STARTING", Persistent=tool["Persistent"], AuthMode=p.get("AuthMode") or "DEFAULT",
            Token=secrets.token_urlsafe(24), TrafficToken=secrets.token_urlsafe(24), ClientToken=token,
            CreateTime=now, UpdateTime=now, TimeoutSeconds=timeout,
            ExpiresAt=None if timeout is None else now + timeout,
            Metadata=p.get("Metadata") or [], Overrides={"CustomConfiguration": p.get("CustomConfiguration") or {}},
        )
        self.instances[inst.InstanceId] = inst
        self._save()
        try:
            await self.backend.start(inst.InstanceId, tool, inst.Overrides)
        except Exception as e:
            inst.Status, inst.StopReason = "FAILED", "error"
            self._save()
            raise ApiError("FailedOperation.ContainerStart", f"sandbox failed to start: {e}") from e
        return {"Instance": inst.api()}

    def _instance(self, instance_id: str | None) -> Instance:
        inst = self.instances.get(instance_id or "")
        if inst is None:
            raise ApiError("ResourceNotFound.SandboxInstance", f"instance {instance_id} not found")
        return inst

    async def DescribeSandboxInstanceList(self, p: dict[str, Any]) -> dict[str, Any]:
        items = list(self.instances.values())
        if p.get("InstanceIds"):
            items = [i for i in items if i.InstanceId in p["InstanceIds"]]
        if p.get("ToolId"):
            items = [i for i in items if i.ToolId == p["ToolId"]]
        items = [await self.refresh(i) for i in items]
        for f in p.get("Filters") or []:
            key, values = f.get("Name"), set(f.get("Values") or [])
            items = [i for i in items if str(getattr(i, key, None)) in values]
        items.sort(key=lambda i: i.CreateTime)
        start = int(p.get("NextToken") or 0)
        size = int(p.get("MaxResults") or p.get("Limit") or 20)
        page = items[start:start + size]
        next_token = str(start + size) if start + size < len(items) else None
        return {"InstanceSet": [i.api() for i in page], "TotalCount": len(items) if p.get("NeedTotalCount") else 0,
                "NextToken": next_token}

    async def AcquireSandboxInstanceToken(self, p: dict[str, Any]) -> dict[str, Any]:
        inst = self._instance(p.get("InstanceId"))
        return {"Token": inst.Token, "TrafficToken": inst.TrafficToken, "ExpiresAt": iso(time.time() + 24 * 3600)}

    async def UpdateSandboxInstance(self, p: dict[str, Any]) -> dict[str, Any]:
        inst = await self.refresh(self._instance(p.get("InstanceId")))
        if inst.Status != "RUNNING":
            raise ApiError("UnsupportedOperation.SandboxInstance", "实例状态不允许修改（只有RUNNING状态的实例可以修改）")
        timeout = parse_duration(p.get("Timeout"))
        if timeout is not None:
            inst.TimeoutSeconds, inst.ExpiresAt = timeout, time.time() + timeout
        if p.get("Metadata"):
            inst.Metadata = p["Metadata"]
        inst.UpdateTime = time.time()
        self._save()
        return {}

    async def PauseSandboxInstance(self, p: dict[str, Any]) -> dict[str, Any]:
        # Simulated: the sandbox keeps running, but the gateway refuses traffic until resumed.
        inst = await self.refresh(self._instance(p.get("InstanceId")))
        if inst.Status != "RUNNING":
            raise ApiError("UnsupportedOperation.SandboxInstance", "only RUNNING instances can be paused")
        inst.Status, inst.UpdateTime = "PAUSED", time.time()
        self._save()
        return {"InstanceStatus": "PAUSED"}

    async def ResumeSandboxInstance(self, p: dict[str, Any]) -> dict[str, Any]:
        inst = self._instance(p.get("InstanceId"))
        if inst.Status != "PAUSED":
            raise ApiError("UnsupportedOperation.SandboxInstance", "only PAUSED instances can be resumed")
        timeout = parse_duration(p.get("Timeout")) or (None if inst.Persistent else 300.0)  # AGS default: 5m
        inst.TimeoutSeconds = timeout
        inst.ExpiresAt = None if timeout is None else time.time() + timeout
        inst.Status, inst.UpdateTime = "STARTING", time.time()
        self._save()
        return {}

    async def StopSandboxInstance(self, p: dict[str, Any]) -> dict[str, Any]:
        inst = self._instance(p.get("InstanceId"))
        await self.backend.stop(inst.InstanceId)
        inst.Status, inst.StopReason, inst.UpdateTime = "STOPPED", "manual", time.time()
        self._save()
        return {}

    async def DescribeQuotaOverview(self, p: dict[str, Any]) -> dict[str, Any]:
        live = [i for i in self.instances.values() if i.Status in ("STARTING", "RUNNING", "PAUSED")]
        paused = [i for i in live if i.Status == "PAUSED"]
        quota = {"SandboxTools": 50, "SandboxInstances": self.max_instances, "PausedInstances": self.max_instances,
                 "CPUCores": 4.0 * self.max_instances, "MemoryGiB": 8.0 * self.max_instances}
        usage = {"SandboxTools": len(self.tools), "SandboxInstances": len(live), "PausedInstances": len(paused),
                 "CPUCores": 0.0, "MemoryGiB": 0.0}
        return {"AccountQuotaOverview": {"Quota": quota, "Usage": usage}, "QuotaGroupSet": [], "TotalCount": 0}

    ACTIONS = {
        "CreateSandboxTool", "DescribeSandboxToolList", "DeleteSandboxTool", "StartSandboxInstance",
        "DescribeSandboxInstanceList", "AcquireSandboxInstanceToken", "UpdateSandboxInstance",
        "PauseSandboxInstance", "ResumeSandboxInstance", "StopSandboxInstance", "DescribeQuotaOverview",
    }

    # --------------------------------------------------------------- reaper

    async def reap(self, stop: asyncio.Event, interval_s: float = 5.0) -> None:
        """Reclaim instances past their deadline, like AGS does for time-limited sandboxes."""
        while not stop.is_set():
            now = time.time()
            for inst in list(self.instances.values()):
                if inst.ExpiresAt and inst.ExpiresAt < now and inst.Status in ("STARTING", "RUNNING", "PAUSED"):
                    log.info("reclaiming %s (timeout)", inst.InstanceId)
                    with contextlib.suppress(Exception):
                        await self.backend.stop(inst.InstanceId)
                    inst.Status, inst.StopReason, inst.UpdateTime = "STOPPED", "timeout", now
                    self._save()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), interval_s)


def build_control_app(ags: MockAgs) -> Starlette:
    async def api(request: Request) -> Response:
        request_id = str(uuid.uuid4())
        body = await request.body()
        action = request.headers.get("x-tc-action", "")
        try:
            ags.verify(request, body)
            if action not in MockAgs.ACTIONS:
                raise ApiError("InvalidAction", f"action {action!r} is not supported by mock AGS")
            params = json.loads(body or b"{}")
            async with ags._lock:
                result = await getattr(ags, action)(params)
        except ApiError as e:
            return JSONResponse({"Response": {"Error": {"Code": e.code, "Message": e.message}, "RequestId": request_id}})
        return JSONResponse({"Response": {**result, "RequestId": request_id}})

    async def healthz(request: Request) -> Response:
        return PlainTextResponse("ok")

    return Starlette(routes=[Route("/", api, methods=["POST"]), Route("/healthz", healthz)])


def build_gateway_app(ags: MockAgs) -> Starlette:
    """E2B data plane: route by E2b-Sandbox-Id (or `{port}-{id}.` host), check the access token, stream."""
    client = httpx.AsyncClient(timeout=httpx.Timeout(None, connect=10))

    async def proxy(request: Request) -> Response:
        instance_id = request.headers.get("e2b-sandbox-id")
        port = request.headers.get("e2b-sandbox-port")
        if not instance_id:
            m = re.match(r"(\d+)-([a-z0-9-]+)\.", request.headers.get("host", ""))
            if m:
                port, instance_id = m.group(1), m.group(2)
        inst = ags.instances.get(instance_id or "")
        if inst is None:
            return PlainTextResponse("invalid sandbox id", status_code=400)
        port = int(port or ENVD_PORT)
        needs_token = inst.AuthMode in ("DEFAULT", "TOKEN") or (inst.AuthMode == "PUBLIC" and port == ENVD_PORT)
        given = request.headers.get("x-access-token", "")
        valid = {inst.Token} | ({inst.TrafficToken} if port != ENVD_PORT else set())
        if needs_token and not any(hmac.compare_digest(given, t) for t in valid):
            return PlainTextResponse("unauthorized", status_code=401)
        if inst.Status == "PAUSED":
            return PlainTextResponse("sandbox is paused", status_code=409)
        st = await ags.backend.status(inst.InstanceId)
        if st.status != "RUNNING" or not st.address:
            return PlainTextResponse(f"sandbox is {st.status.lower()}", status_code=502)
        address = st.address if "://" in st.address else f"http://{st.address}:{port}"
        url = address.rstrip("/") + request.url.path + (f"?{request.url.query}" if request.url.query else "")
        headers = [(k, v) for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP]
        upstream = client.build_request(request.method, url, headers=headers, content=await request.body())
        try:
            resp = await client.send(upstream, stream=True)
        except httpx.HTTPError as e:
            return PlainTextResponse(f"sandbox unreachable: {e}", status_code=502)
        out_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP}
        return StreamingResponse(resp.aiter_raw(), status_code=resp.status_code, headers=out_headers,
                                 background=BackgroundTask(resp.aclose))

    methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
    return Starlette(routes=[Route("/{path:path}", proxy, methods=methods)])


async def serve(ags: MockAgs, host: str, control_port: int, gateway_port: int) -> None:
    import uvicorn

    stop = asyncio.Event()
    servers = [
        uvicorn.Server(uvicorn.Config(build_control_app(ags), host=host, port=control_port, log_level="info")),
        uvicorn.Server(uvicorn.Config(build_gateway_app(ags), host=host, port=gateway_port, log_level="warning")),
    ]
    reaper = asyncio.create_task(ags.reap(stop))
    try:
        await asyncio.gather(*(s.serve() for s in servers))
    finally:
        stop.set()
        await reaper
