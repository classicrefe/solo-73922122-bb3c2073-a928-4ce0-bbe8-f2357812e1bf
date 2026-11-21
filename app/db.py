"""SQLite persistence shared by all API instances.

The database deliberately separates two concepts:

* **Immutable block cache** (`blocks`, `block_transfers`): every block ever
  fetched, keyed by hash, including blocks on branches that were later
  abandoned.  Reaching a common ancestor therefore never requires re-pulling
  known data.
* **Published chain view** (`chain_blocks` + `sync_state`): the canonical
  height -> hash view and the progress/revision that produced it.  Both are
  updated in one transaction so the persisted chain view and progress can never
  disagree.

Address queries read the published view and compute confirmation depth against
the published head.  A reorg or a head rollback therefore withdraws transfers
from query results immediately, even when no additional blocks were fetched.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Hard product rule: only transfers at least two blocks below the published
# head are served, i.e. confirmations >= 3.  Configuration may raise this
# floor but can never lower it, and client query overrides are clamped too.
MIN_CONFIRMATIONS_FLOOR = 3


def utcnow() -> str:
    return datetime.now(UTC).strftime(ISO_FORMAT)


@dataclass(frozen=True)
class TransferRecord:
    txid: str
    from_address: str
    to_address: str
    amount: int


@dataclass(frozen=True)
class BlockRecord:
    hash: str
    height: int
    parent_hash: str
    transfers: tuple[TransferRecord, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "height": self.height,
            "hash": self.hash,
            "parent_hash": self.parent_hash,
            "transfers": [
                {
                    "txid": t.txid,
                    "from_address": t.from_address,
                    "to_address": t.to_address,
                    "amount": t.amount,
                }
                for t in self.transfers
            ],
        }


@dataclass(frozen=True)
class ObservedHead:
    revision: int
    height: int
    hash: str


@dataclass(frozen=True)
class PublishedState:
    published_revision: int
    head_height: int
    head_hash: str
    status: str
    published_at: str | None
    last_round_at: str | None
    last_error: str | None
    last_error_at: str | None
    rounds: int

    @property
    def has_view(self) -> bool:
        return self.head_height >= 0 and bool(self.head_hash)


@dataclass(frozen=True)
class CommitResult:
    published: bool
    outcome: str
    ancestor_height: int | None = None
    previous_head_height: int | None = None
    changed_blocks: int = 0
    removed_blocks: int = 0
    published_revision: int | None = None


@dataclass
class ViewSnapshot:
    state: PublishedState
    min_confirmations: int = MIN_CONFIRMATIONS_FLOOR
    blocks: list[dict[str, Any]] = field(default_factory=list)
    transfers: list[dict[str, Any]] = field(default_factory=list)
    transfer_count: int = 0
    queryable_transfer_count: int = 0
    view_id: str | None = None


class ViewUnavailable(RuntimeError):
    """Raised when no chain view has been published yet."""


SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks (
    hash TEXT PRIMARY KEY,
    height INTEGER NOT NULL,
    parent_hash TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    source_revision INTEGER
);
CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(height);

CREATE TABLE IF NOT EXISTS block_transfers (
    txid TEXT PRIMARY KEY,
    block_hash TEXT NOT NULL REFERENCES blocks(hash),
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    amount INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bt_block ON block_transfers(block_hash);
CREATE INDEX IF NOT EXISTS idx_bt_from ON block_transfers(from_address);
CREATE INDEX IF NOT EXISTS idx_bt_to ON block_transfers(to_address);

CREATE TABLE IF NOT EXISTS chain_blocks (
    height INTEGER PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE REFERENCES blocks(hash),
    parent_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    published_revision INTEGER NOT NULL DEFAULT 0,
    head_height INTEGER NOT NULL DEFAULT -1,
    head_hash TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'empty',
    published_at TEXT,
    last_round_at TEXT,
    last_error TEXT,
    last_error_at TEXT,
    rounds INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO sync_state (id) VALUES (1);

CREATE TABLE IF NOT EXISTS ingest_lease (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    owner TEXT,
    expires_at REAL
);
INSERT OR IGNORE INTO ingest_lease (id, owner, expires_at) VALUES (1, NULL, 0);

CREATE TABLE IF NOT EXISTS ingest_rounds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    observed_revision INTEGER,
    observed_height INTEGER,
    observed_hash TEXT,
    ancestor_height INTEGER,
    fetched_blocks INTEGER NOT NULL DEFAULT 0,
    reused_blocks INTEGER NOT NULL DEFAULT 0,
    changed_blocks INTEGER NOT NULL DEFAULT 0,
    removed_blocks INTEGER NOT NULL DEFAULT 0,
    outcome TEXT NOT NULL,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_rounds_id ON ingest_rounds(id DESC);
"""


class Database:
    def __init__(self, path: str, busy_timeout_ms: int = 10_000) -> None:
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------- connections
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1000.0,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @contextlib.contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    @contextlib.contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            yield conn
        finally:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("ROLLBACK")
            conn.close()

    def initialize(self) -> None:
        """Create schema/interconnect PRAGMAs; safe when called concurrently."""
        last_error: Exception | None = None
        for attempt in range(10):
            conn = self._connect()
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.executescript(SCHEMA)
                return
            except sqlite3.OperationalError as exc:  # lock contention at boot
                last_error = exc
                time.sleep(0.05 * (attempt + 1))
            finally:
                conn.close()
        assert last_error is not None
        raise last_error

    # --------------------------------------------------------------- block cache
    @staticmethod
    def _check_block_rows(conn: sqlite3.Connection, block: BlockRecord) -> None:
        row = conn.execute(
            "SELECT height, parent_hash FROM blocks WHERE hash = ?", (block.hash,)
        ).fetchone()
        if row is None:
            raise RuntimeError(f"block {block.hash} vanished during insert")
        if (
            int(row["height"]) != block.height
            or row["parent_hash"] != block.parent_hash
        ):
            raise RuntimeError(f"conflicting cached block {block.hash}")
        existing = {
            r["txid"]: r
            for r in conn.execute(
                "SELECT txid, block_hash, from_address, to_address, amount "
                "FROM block_transfers WHERE block_hash = ?",
                (block.hash,),
            )
        }
        if len(existing) != len(block.transfers):
            raise RuntimeError(f"conflicting transfer set for block {block.hash}")
        for transfer in block.transfers:
            row = existing.get(transfer.txid)
            if row is None:
                raise RuntimeError(
                    f"conflicting transfer {transfer.txid} for {block.hash}"
                )
            if (
                row["from_address"] != transfer.from_address
                or row["to_address"] != transfer.to_address
                or int(row["amount"]) != transfer.amount
            ):
                raise RuntimeError(f"conflicting transfer {transfer.txid}")

    @staticmethod
    def _insert_block(
        conn: sqlite3.Connection, block: BlockRecord, source_revision: int | None
    ) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO blocks "
            "(hash, height, parent_hash, first_seen_at, source_revision) "
            "VALUES (?, ?, ?, ?, ?)",
            (block.hash, block.height, block.parent_hash, utcnow(), source_revision),
        )
        for transfer in block.transfers:
            if transfer.amount < 0:
                raise ValueError(f"negative amount in {transfer.txid}")
            conn.execute(
                "INSERT OR IGNORE INTO block_transfers "
                "(txid, block_hash, from_address, to_address, amount) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    transfer.txid,
                    block.hash,
                    transfer.from_address,
                    transfer.to_address,
                    transfer.amount,
                ),
            )
        Database._check_block_rows(conn, block)

    def cache_block(self, block: BlockRecord, source_revision: int) -> None:
        """Persist an immutable block prior to publishing it.

        This does not alter the published view; it only makes a partly fetched
        branch resumable after a restart.
        """
        with self._write() as conn:
            self._insert_block(conn, block, source_revision)

    def get_block(self, block_hash: str) -> BlockRecord | None:
        with self._read() as conn:
            row = conn.execute(
                "SELECT hash, height, parent_hash FROM blocks WHERE hash = ?",
                (block_hash,),
            ).fetchone()
            if row is None:
                return None
            transfers = tuple(
                TransferRecord(
                    txid=r["txid"],
                    from_address=r["from_address"],
                    to_address=r["to_address"],
                    amount=int(r["amount"]),
                )
                for r in conn.execute(
                    "SELECT txid, from_address, to_address, amount "
                    "FROM block_transfers WHERE block_hash = ? ORDER BY txid",
                    (block_hash,),
                )
            )
            return BlockRecord(
                hash=row["hash"],
                height=int(row["height"]),
                parent_hash=row["parent_hash"],
                transfers=transfers,
            )

    def canonical_at(self, height: int) -> BlockRecord | None:
        """Only identity is needed while walking parents; transfers are omitted."""
        with self._read() as conn:
            row = conn.execute(
                "SELECT hash, height, parent_hash FROM chain_blocks WHERE height = ?",
                (height,),
            ).fetchone()
            if row is None:
                return None
            return BlockRecord(
                hash=row["hash"],
                height=int(row["height"]),
                parent_hash=row["parent_hash"],
            )

    # --------------------------------------------------------------- state/read
    @staticmethod
    def _state_row(conn: sqlite3.Connection) -> PublishedState:
        row = conn.execute("SELECT * FROM sync_state WHERE id = 1").fetchone()
        if row is None:
            raise RuntimeError("sync_state row missing")
        return PublishedState(
            published_revision=int(row["published_revision"]),
            head_height=int(row["head_height"]),
            head_hash=row["head_hash"],
            status=row["status"],
            published_at=row["published_at"],
            last_round_at=row["last_round_at"],
            last_error=row["last_error"],
            last_error_at=row["last_error_at"],
            rounds=int(row["rounds"]),
        )

    def get_state(self) -> PublishedState:
        with self._read() as conn:
            return self._state_row(conn)

    # ------------------------------------------------------------------- commit
    def commit_view(
        self,
        observed: ObservedHead,
        suffix_desc: list[BlockRecord],
        ancestor_height: int | None,
        ancestor_hash: str | None,
    ) -> CommitResult:
        """Atomically publish a chain view for ``observed``.

        ``suffix_desc`` contains only changed-suffix blocks, ordered from the
        observed head downwards.  The caller guarantees the list links back to
        the common ancestor.  The revision guard rejects late rounds.
        """
        if ancestor_height is None and (not suffix_desc or suffix_desc[-1].height != 0):
            raise ValueError("full-chain publish must end at genesis")
        with self._write() as conn:
            state = self._state_row(conn)
            if state.published_revision >= observed.revision:
                return CommitResult(
                    published=False,
                    outcome="stale",
                    ancestor_height=ancestor_height,
                    previous_head_height=state.head_height if state.has_view else None,
                    changed_blocks=len(suffix_desc),
                    published_revision=state.published_revision,
                )
            # A same-height revision bump carries no suffix and no rollback:
            # confirm the head is already the published head and advance only
            # the revision.  A lower ancestor (head rollback) must still take
            # the deletion path below so withdrawn blocks leave the view.
            same_head = (
                not suffix_desc
                and ancestor_height == observed.height
                and ancestor_hash == observed.hash
                and state.head_height == observed.height
            )
            if same_head:
                conn.execute(
                    "UPDATE sync_state SET "
                    "published_revision = ?, status = 'ok', "
                    "last_round_at = ?, last_error = NULL, last_error_at = NULL, "
                    "rounds = rounds + 1 WHERE id = 1",
                    (observed.revision, utcnow()),
                )
                return CommitResult(
                    published=True,
                    outcome="committed",
                    ancestor_height=ancestor_height,
                    previous_head_height=state.head_height,
                    changed_blocks=0,
                    removed_blocks=0,
                    published_revision=observed.revision,
                )

            if ancestor_height is not None:
                row = conn.execute(
                    "SELECT hash FROM chain_blocks WHERE height = ?",
                    (ancestor_height,),
                ).fetchone()
                if row is None or row["hash"] != ancestor_hash:
                    raise RuntimeError("common ancestor changed before commit")

            for block in suffix_desc:
                self._insert_block(conn, block, observed.revision)

            if ancestor_height is None:
                conn.execute("DELETE FROM chain_blocks")
            else:
                conn.execute(
                    "DELETE FROM chain_blocks WHERE height > ?", (ancestor_height,)
                )

            for block in reversed(suffix_desc):
                conn.execute(
                    "INSERT INTO chain_blocks (height, hash, parent_hash) "
                    "VALUES (?, ?, ?)",
                    (block.height, block.hash, block.parent_hash),
                )

            head_row = conn.execute(
                "SELECT hash FROM chain_blocks WHERE height = ?",
                (observed.height,),
            ).fetchone()
            if head_row is None or head_row["hash"] != observed.hash:
                raise RuntimeError("published view head does not match observation")
            if (
                ancestor_height is not None
                and suffix_desc
                and suffix_desc[-1].parent_hash != ancestor_hash
            ):
                raise RuntimeError("suffix does not descend from common ancestor")

            now = utcnow()
            conn.execute(
                "UPDATE sync_state SET "
                "published_revision = ?, head_height = ?, head_hash = ?, "
                "status = 'ok', published_at = ?, last_round_at = ?, "
                "last_error = NULL, last_error_at = NULL, rounds = rounds + 1 "
                "WHERE id = 1",
                (
                    observed.revision,
                    observed.height,
                    observed.hash,
                    now,
                    now,
                ),
            )
            previous_height = state.head_height if state.has_view else None
            removed = 0
            if previous_height is not None:
                removed = max(
                    0,
                    previous_height
                    - (ancestor_height if ancestor_height is not None else -1),
                )
            return CommitResult(
                published=True,
                outcome="committed",
                ancestor_height=ancestor_height,
                previous_head_height=previous_height,
                changed_blocks=len(suffix_desc),
                removed_blocks=removed,
                published_revision=observed.revision,
            )

    # -------------------------------------------------------------- failures/log
    def record_failure(self, error: str) -> None:
        """Record a failed round without touching the published view.

        Status becomes ``degraded`` when a view is still being served (so the
        head did not move) and stays ``empty`` before the first publish.
        """
        with self._write() as conn:
            state = self._state_row(conn)
            conn.execute(
                "UPDATE sync_state SET status = ?, last_error = ?, "
                "last_error_at = ? WHERE id = 1",
                (
                    "degraded" if state.has_view else "empty",
                    error[:2000],
                    utcnow(),
                ),
            )

    def log_round(
        self,
        *,
        instance_id: str,
        started_at: str,
        observed: ObservedHead | None,
        ancestor_height: int | None,
        fetched_blocks: int,
        reused_blocks: int,
        changed_blocks: int,
        removed_blocks: int,
        outcome: str,
        error: str | None = None,
    ) -> None:
        with self._write() as conn:
            conn.execute(
                "INSERT INTO ingest_rounds "
                "(instance_id, started_at, finished_at, observed_revision, "
                "observed_height, observed_hash, ancestor_height, fetched_blocks, "
                "reused_blocks, changed_blocks, removed_blocks, outcome, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    instance_id,
                    started_at,
                    utcnow(),
                    observed.revision if observed else None,
                    observed.height if observed else None,
                    observed.hash if observed else None,
                    ancestor_height,
                    fetched_blocks,
                    reused_blocks,
                    changed_blocks,
                    removed_blocks,
                    outcome,
                    error[:2000] if error else None,
                ),
            )
            # Keep the audit table bounded on long-running demo deployments.
            conn.execute(
                "DELETE FROM ingest_rounds WHERE id <= "
                "(SELECT MAX(id) - 500 FROM ingest_rounds)"
            )

    # -------------------------------------------------------------------- lease
    def acquire_lease(self, owner: str, ttl_seconds: float) -> bool:
        now = time.time()
        with self._write() as conn:
            row = conn.execute(
                "SELECT owner, expires_at FROM ingest_lease WHERE id = 1"
            ).fetchone()
            if (
                row is None
                or row["owner"] in (None, owner)
                or float(row["expires_at"] or 0) <= now
            ):
                conn.execute(
                    "UPDATE ingest_lease SET owner = ?, expires_at = ? WHERE id = 1",
                    (owner, now + ttl_seconds),
                )
                return True
            return False

    def renew_lease(self, owner: str, ttl_seconds: float) -> bool:
        now = time.time()
        with self._write() as conn:
            cursor = conn.execute(
                "UPDATE ingest_lease SET expires_at = ? WHERE id = 1 AND owner = ?",
                (now + ttl_seconds, owner),
            )
            return cursor.rowcount == 1

    def release_lease(self, owner: str) -> None:
        with self._write() as conn:
            conn.execute(
                "UPDATE ingest_lease SET owner = NULL, expires_at = 0 "
                "WHERE id = 1 AND owner = ?",
                (owner,),
            )

    def lease_info(self) -> dict[str, Any]:
        with self._read() as conn:
            row = conn.execute(
                "SELECT owner, expires_at FROM ingest_lease WHERE id = 1"
            ).fetchone()
            now = time.time()
            return {
                "owner": row["owner"] if row else None,
                "expires_at": row["expires_at"] if row else None,
                "active": bool(
                    row and row["owner"] and float(row["expires_at"] or 0) > now
                ),
            }

    # -------------------------------------------------------------- view reads
    @staticmethod
    def _confirmations(state: PublishedState, block_height: int) -> int:
        return state.head_height - block_height + 1

    def snapshot(
        self,
        address: str | None = None,
        min_confirmations: int = MIN_CONFIRMATIONS_FLOOR,
    ) -> ViewSnapshot:
        if min_confirmations < MIN_CONFIRMATIONS_FLOOR:
            raise ValueError(f"min_confirmations must be >= {MIN_CONFIRMATIONS_FLOOR}")
        with self._read() as conn:
            state = self._state_row(conn)
            if not state.has_view:
                raise ViewUnavailable("no chain view has been published yet")

            block_rows = list(
                conn.execute(
                    "SELECT cb.height, cb.hash, cb.parent_hash, "
                    "(SELECT COUNT(*) FROM block_transfers bt "
                    " WHERE bt.block_hash = cb.hash) AS tx_count "
                    "FROM chain_blocks cb ORDER BY cb.height"
                )
            )
            blocks = [
                {
                    "height": int(r["height"]),
                    "hash": r["hash"],
                    "parent_hash": r["parent_hash"],
                    "tx_count": int(r["tx_count"]),
                }
                for r in block_rows
            ]
            # The chain view deliberately exposes block identity and transfer
            # counts only; unconfirmed transfer contents are never leaked here.
            # Address answers are the only place transfer details appear.
            view_id = hashlib.sha256(
                json.dumps(
                    [[b["height"], b["hash"]] for b in blocks],
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()

            # The view's transfer count is restricted to blocks still canonical.
            canonical_transfer_count = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM block_transfers bt "
                    "JOIN chain_blocks cb ON cb.hash = bt.block_hash"
                ).fetchone()["n"]
            )
            queryable_count = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM block_transfers bt "
                    "JOIN chain_blocks cb ON cb.hash = bt.block_hash "
                    "WHERE (? - cb.height + 1) >= ?",
                    (state.head_height, min_confirmations),
                ).fetchone()["n"]
            )

            transfers: list[dict[str, Any]] = []
            if address is not None:
                rows = conn.execute(
                    "SELECT bt.txid, bt.from_address, bt.to_address, bt.amount, "
                    "cb.height AS block_height, cb.hash AS block_hash "
                    "FROM block_transfers bt "
                    "JOIN chain_blocks cb ON cb.hash = bt.block_hash "
                    "WHERE (bt.from_address = ? OR bt.to_address = ?) "
                    "AND (? - cb.height + 1) >= ? "
                    "ORDER BY cb.height ASC, bt.txid ASC",
                    (address, address, state.head_height, min_confirmations),
                )
                transfers = [
                    {
                        "txid": r["txid"],
                        "from_address": r["from_address"],
                        "to_address": r["to_address"],
                        "amount": int(r["amount"]),
                        "block_height": int(r["block_height"]),
                        "block_hash": r["block_hash"],
                        "confirmations": self._confirmations(
                            state, int(r["block_height"])
                        ),
                    }
                    for r in rows
                ]

            return ViewSnapshot(
                state=state,
                min_confirmations=min_confirmations,
                blocks=blocks,
                transfers=transfers,
                transfer_count=canonical_transfer_count,
                queryable_transfer_count=queryable_count,
                view_id=view_id,
            )

    def recent_rounds(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM ingest_rounds ORDER BY id DESC LIMIT ?", (limit,)
            )
            return [dict(r) for r in rows]
