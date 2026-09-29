"""Adapters for underlying single-agent harnesses."""

from __future__ import annotations

from .base import DrainFn, HarnessAdapter, TurnOutcome


def build_adapter(kind: str, options: dict | None = None, **kwargs) -> HarnessAdapter:
    if options and kind != "pi":
        raise ValueError(f"harness_options are not supported by harness {kind!r}")
    if kind == "pi":
        from .pi import PiAdapter

        return PiAdapter(options=options, **kwargs)
    if kind == "claude_code":
        from .claude_code import ClaudeCodeAdapter

        return ClaudeCodeAdapter(**kwargs)
    if kind == "scripted":
        from .scripted import ScriptedAdapter

        return ScriptedAdapter(**kwargs)
    if kind == "fake":
        from .fake import FakeAdapter

        return FakeAdapter()  # scripted; ignores harness kwargs
    raise ValueError(f"unknown harness {kind!r}")


__all__ = ["DrainFn", "HarnessAdapter", "TurnOutcome", "build_adapter"]
