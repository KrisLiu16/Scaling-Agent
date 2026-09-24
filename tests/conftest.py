from __future__ import annotations

import asyncio
import contextlib
import socket
from collections.abc import AsyncIterator

import httpx
import httpx2
import pytest
import uvicorn
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from scaling_agent.config import CoordSettings
from scaling_agent.coord.server import build_app
from scaling_agent.coord.store import Store

ADMIN = "admin-secret"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def store(tmp_path) -> AsyncIterator[Store]:
    s = Store(str(tmp_path / "coord.sqlite3"), claim_ttl_s=600, hot_file_threshold=2)
    await s.open()
    for w in ("w1", "w2", "w3"):
        await s.register_worker(w, token=f"tok-{w}")
    yield s
    await s.close()


class CoordServer:
    def __init__(self, url: str, app) -> None:
        self.url = url
        self.app = app

    @property
    def coord(self):
        return self.app.state.coord

    def admin(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self.url, headers={"Authorization": f"Bearer {ADMIN}"}, timeout=30)

    def worker(self, token: str) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self.url, headers={"Authorization": f"Bearer {token}"}, timeout=30)

    @contextlib.asynccontextmanager
    async def mcp(self, token: str) -> AsyncIterator[Client]:
        http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {token}"}, timeout=30)
        async with Client(streamable_http_client(f"{self.url}/mcp", http_client=http)) as client:
            yield client


@pytest.fixture
async def server(tmp_path) -> AsyncIterator[CoordServer]:
    port = _free_port()
    settings = CoordSettings(
        db_path=str(tmp_path / "server.sqlite3"),
        host="127.0.0.1",
        port=port,
        admin_token=ADMIN,
        long_poll_s=2.0,
        merge_queue_enabled=False,
    )
    app = build_app(settings)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
    srv = uvicorn.Server(config)
    task = asyncio.create_task(srv.serve())
    for _ in range(100):
        if srv.started:
            break
        await asyncio.sleep(0.05)
    yield CoordServer(f"http://127.0.0.1:{port}", app)
    srv.should_exit = True
    await task


def text_of(result) -> str:
    return "\n".join(getattr(c, "text", "") for c in result.content)
