"""Sandbox providers: Tencent Cloud AGS persistent sandboxes, or local subprocesses for development."""

from __future__ import annotations

from ..config import ProviderSettings
from .base import SandboxHandle, SandboxProvider


def build_provider(settings: ProviderSettings, run_name: str) -> SandboxProvider:
    if settings.kind == "ags":
        from .ags import AgsProvider

        return AgsProvider(settings, run_name)
    if settings.kind == "k8s":
        from .k8s import K8sProvider

        return K8sProvider(settings, run_name)
    if settings.kind == "local":
        from .local import LocalProvider

        return LocalProvider(settings, run_name)
    raise ValueError(f"unknown provider {settings.kind!r}")


__all__ = ["SandboxHandle", "SandboxProvider", "build_provider"]
