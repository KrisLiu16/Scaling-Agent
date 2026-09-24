"""Where mock AGS sandboxes actually run."""

from __future__ import annotations

import abc
import asyncio
import logging
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class BackendStatus:
    status: str  # STARTING | RUNNING | STOPPED | FAILED
    address: str | None = None  # host or IP where the sandbox's ports are reachable
    reason: str | None = None


class Backend(abc.ABC):
    @abc.abstractmethod
    async def start(self, instance_id: str, tool: dict[str, Any], overrides: dict[str, Any]) -> None: ...

    @abc.abstractmethod
    async def status(self, instance_id: str) -> BackendStatus: ...

    @abc.abstractmethod
    async def stop(self, instance_id: str) -> None: ...


class StaticBackend(Backend):
    """Every sandbox is the same already-running envd (tests, laptops)."""

    def __init__(self, address: str) -> None:
        self.address = address
        self.running: set[str] = set()

    async def start(self, instance_id, tool, overrides) -> None:
        self.running.add(instance_id)

    async def status(self, instance_id) -> BackendStatus:
        if instance_id in self.running:
            return BackendStatus("RUNNING", self.address)
        return BackendStatus("STOPPED")

    async def stop(self, instance_id) -> None:
        self.running.discard(instance_id)


def _env_list(items: list[dict[str, Any]] | None) -> dict[str, str]:
    return {e["Name"]: str(e.get("Value", "")) for e in items or [] if e.get("Name")}


class PodBackend(Backend):
    """One Kubernetes pod per sandbox instance, built from the tool's CustomConfiguration."""

    def __init__(self, namespace: str, image_pull_policy: str = "IfNotPresent") -> None:
        from kubernetes import client, config

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.c = client
        self.core = client.CoreV1Api()
        self.namespace = namespace
        self.pull_policy = image_pull_policy

    @staticmethod
    def pod_name(instance_id: str) -> str:
        return f"sbx-{instance_id}".lower()

    def _pod(self, instance_id: str, tool: dict[str, Any], overrides: dict[str, Any]):
        c = self.c
        cc = tool.get("CustomConfiguration") or {}
        env = {**_env_list(cc.get("Env")), **_env_list((overrides.get("CustomConfiguration") or {}).get("Env"))}
        res = cc.get("Resources") or {}
        limits = {k: v for k, v in (("cpu", res.get("CPU")), ("memory", res.get("Memory"))) if v}
        probe = (cc.get("Probe") or {}).get("HttpGet") or {"Path": "/health", "Port": 49983}
        ports = [c.V1ContainerPort(container_port=int(p["Port"]), name=str(p.get("Name", "p"))[:15])
                 for p in cc.get("Ports") or [{"Name": "envd", "Port": 49983}]]
        container = c.V1Container(
            name="sandbox",
            image=cc.get("Image"),
            image_pull_policy=self.pull_policy,
            # Like AGS: the image's CMD/ENTRYPOINT are ignored; the tool's Command/Args start the sandbox.
            command=cc.get("Command") or None,
            args=cc.get("Args") or None,
            env=[c.V1EnvVar(name=k, value=v) for k, v in env.items()],
            ports=ports,
            resources=c.V1ResourceRequirements(requests={"cpu": "100m", "memory": "256Mi"}, limits=limits or None),
            readiness_probe=c.V1Probe(
                http_get=c.V1HTTPGetAction(path=probe.get("Path", "/health"), port=int(probe.get("Port", 49983))),
                period_seconds=1,
                failure_threshold=60,
            ),
            volume_mounts=[c.V1VolumeMount(name="workspace", mount_path="/workspace")],
        )
        labels = {"app": "sa-sandbox", "instance": instance_id.lower(), "tool": str(tool.get("ToolName", ""))[:63]}
        return c.V1Pod(
            metadata=c.V1ObjectMeta(name=self.pod_name(instance_id), labels=labels),
            spec=c.V1PodSpec(
                containers=[container],
                restart_policy="Always",  # a persistent sandbox survives its main process crashing
                automount_service_account_token=False,
                enable_service_links=False,  # like AGS: no cluster service env inside sandboxes
                volumes=[c.V1Volume(name="workspace", empty_dir=c.V1EmptyDirVolumeSource())],
            ),
        )

    async def start(self, instance_id, tool, overrides) -> None:
        from kubernetes.client.rest import ApiException

        try:
            await asyncio.to_thread(self.core.create_namespaced_pod, self.namespace, self._pod(instance_id, tool, overrides))
        except ApiException as e:
            if e.status != 409:
                raise

    async def status(self, instance_id) -> BackendStatus:
        from kubernetes.client.rest import ApiException

        try:
            pod = await asyncio.to_thread(self.core.read_namespaced_pod, self.pod_name(instance_id), self.namespace)
        except ApiException as e:
            if e.status == 404:
                return BackendStatus("STOPPED", reason="pod not found")
            raise
        phase = pod.status.phase
        ready = any(cond.type == "Ready" and cond.status == "True" for cond in pod.status.conditions or [])
        if phase == "Running" and ready:
            return BackendStatus("RUNNING", pod.status.pod_ip)
        if phase in ("Pending", "Running"):
            return BackendStatus("STARTING", pod.status.pod_ip)
        if phase == "Failed":
            return BackendStatus("FAILED", reason=pod.status.reason or "pod failed")
        return BackendStatus("STOPPED", reason=phase)

    async def stop(self, instance_id) -> None:
        from kubernetes.client.rest import ApiException

        try:
            await asyncio.to_thread(self.core.delete_namespaced_pod, self.pod_name(instance_id), self.namespace)
        except ApiException as e:
            if e.status != 404:
                raise
