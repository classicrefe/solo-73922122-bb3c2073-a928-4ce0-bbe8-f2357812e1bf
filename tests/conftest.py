"""Shared fixtures: real HTTP servers on ephemeral ports, shared SQLite volume."""

from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
import uvicorn

from app.api import create_app as create_api_app
from app.config import Settings
from app.db import Database
from chain_source.app import ChainState
from chain_source.app import create_app as create_source_app


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class LiveServer:
    def __init__(self, app) -> None:
        self.port = _free_port()
        self.server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=self.port,
                log_level="warning",
                lifespan="on",
            )
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        self._wait_ready()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def _wait_ready(self) -> None:
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                httpx.get(self.base_url + "/health", timeout=0.3)
                return
            except httpx.HTTPError:
                time.sleep(0.03)
        raise RuntimeError(f"server on {self.port} did not start")

    def client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, timeout=10.0)

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(5)


@dataclass
class Stack:
    source_server: LiveServer
    source: ChainState
    db_path: str
    source_url: str
    settings: Settings
    api_servers: list[LiveServer]

    def source_client(self) -> httpx.Client:
        return self.source_server.client()

    def api(self, index: int = 0) -> httpx.Client:
        return self.api_servers[index].client()

    def add_api(self, instance_id: str | None = None, start_follower: bool = False):
        settings = Settings(
            db_path=self.db_path,
            source_url=self.source_url,
            source_timeout=5.0,
            poll_interval=0.1,
            lease_seconds=5.0,
            min_confirmations=3,
            instance_id=instance_id or f"test-{len(self.api_servers)}",
            follower_enabled=start_follower,
            verify_head_after_round=False,
        )
        db = Database(self.db_path)
        db.initialize()
        app = create_api_app(
            settings,
            database=db,
            start_follower=start_follower,
            demo_console=True,
        )
        server = LiveServer(app)
        self.api_servers.append(server)
        return server, app

    def stop(self) -> None:
        for server in self.api_servers:
            server.stop()
        self.source_server.stop()


@pytest.fixture()
def stack(tmp_path: Path):
    state_path = tmp_path / "source" / "chain.json"
    source = ChainState(
        state_path=str(state_path), initial_blocks=0, initial_transfers_per_block=2
    )
    source_server = LiveServer(create_source_app(source))
    db_path = str(tmp_path / "data" / "chain.db")
    stack = Stack(
        source_server=source_server,
        source=source,
        db_path=db_path,
        source_url=source_server.base_url,
        settings=Settings.from_env(),  # placeholder, replaced in add_api
        api_servers=[],
    )
    stack.add_api("instance-a")
    try:
        yield stack
    finally:
        stack.stop()


@pytest.fixture()
def synced(stack: Stack):
    """Source with a 6-block chain synced into a published view."""
    client = stack.source_client()
    client.post(
        "/admin/advance", json={"count": 5, "transfers_per_block": 2}
    ).raise_for_status()
    api = stack.api()
    response = api.post("/admin/sync")
    assert response.status_code == 200, response.text
    assert response.json()["outcome"] == "committed"
    return stack
