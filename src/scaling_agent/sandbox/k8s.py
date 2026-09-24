"""Kubernetes provider: a plain Pod per worker stands in for an AGS persistent sandbox.

Mapping to AGS concepts:
  sandbox tool (template)  -> the pod template built here (image, resources, workspace volume)
  persistent instance      -> a Pod with restartPolicy=Always (the kubelet restarts a dead runtime)
  per-instance env         -> a per-worker Secret mounted with envFrom (tokens never in the pod spec)
  envd exec of the runtime -> the container command is the worker runtime itself

Works in-cluster (the launcher runs as a Job with a ServiceAccount) or from a kubeconfig.
"""

from __future__ import annotations

import asyncio
import logging
import re

from ..config import ProviderSettings
from .base import SandboxHandle, SandboxProvider

log = logging.getLogger(__name__)


def _dns_name(value: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", value.lower()).strip("-")[:63]


class K8sProvider(SandboxProvider):
    def __init__(self, settings: ProviderSettings, run_name: str) -> None:
        from kubernetes import client, config

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.core = client.CoreV1Api()
        self.client = client
        self.s = settings
        self.run = _dns_name(run_name)
        self.namespace = settings.k8s_namespace
        if not settings.image:
            raise ValueError("provider.image is required (the worker image)")

    def _names(self, worker_id: str) -> tuple[str, str]:
        base = _dns_name(f"sa-{self.run}-{worker_id}")
        return base, f"{base}-env"

    async def prepare(self) -> None:
        # Namespaced call on purpose: the launcher's Role grants pods/secrets in one namespace only.
        await asyncio.to_thread(self.core.list_namespaced_pod, self.namespace, limit=1)

    def _secret(self, worker_id: str, env: dict[str, str]):
        c = self.client
        _, secret = self._names(worker_id)
        return c.V1Secret(
            metadata=c.V1ObjectMeta(name=secret, labels={"app": "sa-worker", "run": self.run, "worker": _dns_name(worker_id)}),
            string_data=env,
        )

    def _pod(self, worker_id: str):
        c = self.client
        pod, secret = self._names(worker_id)
        labels = {"app": "sa-worker", "run": self.run, "worker": _dns_name(worker_id)}
        container = c.V1Container(
            name="worker",
            image=self.s.image,
            image_pull_policy=self.s.k8s_image_pull_policy,
            command=["/bin/sh", "-c", f"mkdir -p /workspace/home && exec {self.s.runtime_command}"],
            env_from=[c.V1EnvFromSource(secret_ref=c.V1SecretEnvSource(name=secret))],
            env=[
                c.V1EnvVar(name="SA_WORKER_WORKDIR", value="/workspace/repo"),
                c.V1EnvVar(name="SA_WORKER_STATE_DIR", value="/workspace/state"),
                c.V1EnvVar(name="HOME", value="/workspace/home"),
            ],
            security_context=c.V1SecurityContext(
                run_as_non_root=True,
                run_as_user=1000,
                run_as_group=1000,
                allow_privilege_escalation=False,
                capabilities=c.V1Capabilities(drop=["ALL"]),
                seccomp_profile=c.V1SeccompProfile(type="RuntimeDefault"),
            ),
            resources=c.V1ResourceRequirements(
                requests={"cpu": self.s.k8s_cpu_request, "memory": self.s.k8s_memory_request},
                limits={"cpu": self.s.cpu, "memory": self.s.memory},
            ),
            volume_mounts=[c.V1VolumeMount(name="workspace", mount_path="/workspace")],
        )
        return c.V1Pod(
            metadata=c.V1ObjectMeta(name=pod, labels=labels),
            spec=c.V1PodSpec(
                security_context=c.V1PodSecurityContext(fs_group=1000),
                containers=[container],
                restart_policy="Always",
                automount_service_account_token=False,  # a worker has no business with the k8s API
                enable_service_links=False,  # no *_SERVICE_HOST env advertising other services
                volumes=[c.V1Volume(name="workspace", empty_dir=c.V1EmptyDirVolumeSource())],
            ),
        )

    async def _apply_secret(self, worker_id: str, env: dict[str, str]) -> None:
        from kubernetes.client.rest import ApiException

        body = self._secret(worker_id, env)
        try:
            await asyncio.to_thread(self.core.create_namespaced_secret, self.namespace, body)
        except ApiException as e:
            if e.status != 409:
                raise
            await asyncio.to_thread(self.core.replace_namespaced_secret, body.metadata.name, self.namespace, body)

    async def start(self, worker_id: str, env: dict[str, str]) -> SandboxHandle:
        from kubernetes.client.rest import ApiException

        await self._apply_secret(worker_id, env)
        pod = self._pod(worker_id)
        try:
            await asyncio.to_thread(self.core.create_namespaced_pod, self.namespace, pod)
        except ApiException as e:
            if e.status != 409:  # already exists: re-attach, like AGS ClientToken idempotency
                raise
        return SandboxHandle(worker_id=worker_id, sandbox_id=pod.metadata.name, meta={"namespace": self.namespace})

    async def runtime_alive(self, handle: SandboxHandle) -> bool | None:
        """The kubelet already restarts a crashed container in place (restartPolicy=Always), keeping
        the pod's workspace. Only a missing pod, or one that has finished, needs a relaunch."""
        from kubernetes.client.rest import ApiException

        try:
            pod = await asyncio.to_thread(self.core.read_namespaced_pod, handle.sandbox_id, self.namespace)
        except ApiException as e:
            if e.status == 404:
                return False
            return None
        return pod.status.phase not in ("Failed", "Succeeded")

    async def relaunch(self, handle: SandboxHandle, env: dict[str, str]) -> None:
        await self.stop(handle)
        for _ in range(120):
            if not await self._exists(handle.sandbox_id):
                break
            await asyncio.sleep(1)
        else:
            raise RuntimeError(f"{handle.sandbox_id} is still terminating; will retry")
        await self.start(handle.worker_id, env)

    async def _exists(self, name: str) -> bool:
        from kubernetes.client.rest import ApiException

        try:
            await asyncio.to_thread(self.core.read_namespaced_pod, name, self.namespace)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise

    async def stop(self, handle: SandboxHandle) -> None:
        from kubernetes.client.rest import ApiException

        _, secret = self._names(handle.worker_id)
        for fn, name in ((self.core.delete_namespaced_pod, handle.sandbox_id), (self.core.delete_namespaced_secret, secret)):
            try:
                await asyncio.to_thread(fn, name, self.namespace)
            except ApiException as e:
                if e.status != 404:
                    raise
