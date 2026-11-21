"""Runtime configuration (environment driven)."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class Settings:
    db_path: str
    source_url: str
    source_timeout: float
    poll_interval: float
    lease_seconds: float
    min_confirmations: int
    instance_id: str
    follower_enabled: bool
    verify_head_after_round: bool

    @classmethod
    def from_env(cls, instance_id: str | None = None) -> Settings:
        resolved_instance = (
            instance_id
            or os.getenv("INSTANCE_ID")
            or os.getenv("HOSTNAME")
            or "instance"
        )
        return cls(
            db_path=os.getenv("DB_PATH", "/data/chain.db"),
            source_url=os.getenv("CHAIN_SOURCE_URL", "http://chain-source:9100"),
            source_timeout=_env_float("SOURCE_TIMEOUT_SECONDS", 10.0),
            poll_interval=_env_float("POLL_INTERVAL_SECONDS", 1.0),
            lease_seconds=_env_float("INGEST_LEASE_SECONDS", 15.0),
            # A transfer is served only once it is at least 2 blocks below the
            # published head: confirmations = head_height - height + 1 >= 3.
            min_confirmations=max(3, _env_int("MIN_CONFIRMATIONS", 3)),
            instance_id=resolved_instance,
            follower_enabled=os.getenv("FOLLOWER_ENABLED", "1") != "0",
            verify_head_after_round=os.getenv("VERIFY_HEAD_AFTER_ROUND", "1") != "0",
        )
