"""HTTP client the worker runtime uses to talk to the coordination server."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from ..protocol import Claim, TurnBatch, TurnReport

log = logging.getLogger(__name__)


class CoordClient:
    def __init__(self, base_url: str, token: str, timeout: float = 60.0) -> None:
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"), headers={"Authorization": f"Bearer {token}"}, timeout=timeout
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def _post(self, path: str, body: dict[str, Any], retries: int = 5) -> dict[str, Any]:
        delay = 1.0
        for attempt in range(retries):
            try:
                resp = await self._http.post(path, json=body)
                if resp.status_code < 500:
                    resp.raise_for_status()
                    return resp.json()
                log.warning("coord %s -> %s", path, resp.status_code)
            except httpx.TransportError as e:
                log.warning("coord %s transport error: %s", path, e)
            if attempt < retries - 1:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)  # progressively longer waits, like Agensh's dispatcher
        raise RuntimeError(f"coordination server unreachable: {path}")

    async def wait(self, wait_s: float) -> bool:
        """Long-poll without leasing anything: True as soon as something should start a turn."""
        data = await self._post("/api/turns/wait", {"wait_s": wait_s})
        return bool(data.get("pending"))

    async def next_turn(self, wait_s: float = 0.0) -> tuple[TurnBatch, list[Claim]]:
        data = await self._post("/api/turns/next", {"wait_s": wait_s})
        return TurnBatch.model_validate(data["batch"]), [Claim.model_validate(c) for c in data["claims"]]

    async def ack(self, report: TurnReport) -> bool:
        """Acknowledge a turn. Returns whether the worker wrote to the board during it."""
        data = await self._post("/api/turns/ack", report.model_dump(mode="json"))
        return bool(data.get("wrote_board"))

    async def drain(self) -> str:
        """Pending mid-turn updates, rendered (empty string if none)."""
        data = await self._post("/api/drain", {}, retries=1)
        return data.get("text", "")

    async def handoff(self, skip_if_empty: bool = False) -> str | None:
        resp = await self._http.get("/api/handoff")
        resp.raise_for_status()
        data = resp.json()
        return None if skip_if_empty and data.get("empty") else data["text"]

    async def trace(self, records: list[dict[str, Any]]) -> None:
        if records:
            try:
                await self._post("/api/trace", {"records": records}, retries=2)
            except RuntimeError:
                log.warning("dropped %d trace records", len(records))
