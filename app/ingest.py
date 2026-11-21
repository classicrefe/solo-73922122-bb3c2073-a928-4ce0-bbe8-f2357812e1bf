"""Chain follower: fixed-revision rounds with common-ancestor suffix updates.

Round contract
--------------
1. Read ``/head`` **once**; the resulting ``revision``/``height``/``hash`` is
   fixed for the whole round.  Every later request in the round uses only
   hashes, never a fresh head.
2. Walk the published view head and the observed head down along
   ``parent_hash`` until the same hash is reached: the common ancestor.  Blocks
   already in the immutable cache are reused, so known prefix blocks are never
   fetched again, and a restart mid-walk resumes without re-fetching.  The walk
   costs O(distance to the fork), not O(chain length), for a normal advance.
3. Fetch only changed-suffix blocks that are not already in the immutable
   cache.  Abandoned-branch blocks stay cached and are reused if a branch
   reappears.
4. Publish the height -> hash view and the observed revision in one SQLite
   transaction.  That transaction refuses to overwrite a view whose published
   revision is already ``>=`` this round's revision, so a late slow round can
   never clobber a newer one, even from a second instance.

Nothing is published unless every step above succeeded.
"""

from __future__ import annotations

import itertools
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from .db import (
    BlockRecord,
    CommitResult,
    Database,
    ObservedHead,
    utcnow,
)
from .source_client import ChainSourceClient, SourceError

log = logging.getLogger("ingest")


@dataclass
class RoundReport:
    round_id: str
    instance_id: str
    outcome: str
    observed: ObservedHead | None = None
    ancestor_height: int | None = None
    ancestor_hash: str | None = None
    fetched_blocks: int = 0
    reused_blocks: int = 0
    changed_blocks: int = 0
    removed_blocks: int = 0
    published: bool = False
    error: str | None = None
    duration_ms: int = 0
    head_moved_during_round: bool | None = None
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "round_id": self.round_id,
            "instance_id": self.instance_id,
            "outcome": self.outcome,
            "observed_head": {
                "revision": self.observed.revision,
                "height": self.observed.height,
                "hash": self.observed.hash,
            }
            if self.observed
            else None,
            "common_ancestor": {
                "height": self.ancestor_height,
                "hash": self.ancestor_hash,
            }
            if self.ancestor_height is not None
            else None,
            "fetched_blocks": self.fetched_blocks,
            "reused_blocks": self.reused_blocks,
            "changed_blocks": self.changed_blocks,
            "removed_blocks": self.removed_blocks,
            "published": self.published,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "head_moved_during_round": self.head_moved_during_round,
            "notes": self.notes,
        }


class Ingestor:
    def __init__(
        self,
        db: Database,
        source: ChainSourceClient,
        instance_id: str | None = None,
        poll_interval: float = 1.0,
        lease_seconds: float = 15.0,
        verify_head_after_round: bool = True,
    ) -> None:
        self.db = db
        self.source = source
        self.instance_id = instance_id or f"ingestor-{uuid.uuid4().hex[:8]}"
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.verify_head_after_round = verify_head_after_round
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._round_lock = threading.Lock()
        self.state: dict[str, Any] = {
            "instance_id": self.instance_id,
            "running": False,
            "leader": False,
            "current_round_id": None,
            "rounds_attempted": 0,
            "rounds_committed": 0,
            "rounds_stale": 0,
            "rounds_failed": 0,
            "last_round": None,
            "last_success_at": None,
            "last_error": None,
            "started_at": None,
        }

    # ----------------------------------------------------------------- control
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self.state["running"] = True
        self.state["started_at"] = utcnow()
        self._thread = threading.Thread(
            target=self._run_loop, name=f"ingestor-{self.instance_id}", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout)
        self.state["running"] = False
        try:
            self.db.release_lease(self.instance_id)
        except Exception:
            log.exception("failed to release ingest lease")

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                leader = self.db.acquire_lease(self.instance_id, self.lease_seconds)
                self.state["leader"] = leader
                if leader:
                    report = self.run_round(blocking=False)
                    if report is not None and report.outcome == "failed":
                        # Back off briefly on failure; the published view stays.
                        self._stop.wait(max(self.poll_interval, 1.0))
                    else:
                        self._stop.wait(self.poll_interval)
                else:
                    self._stop.wait(self.poll_interval)
            except Exception as exc:  # keep the follower alive
                log.exception("ingest loop error")
                self.state["last_error"] = f"{type(exc).__name__}: {exc}"
                self._stop.wait(max(self.poll_interval, 1.0))
        try:
            self.db.release_lease(self.instance_id)
        except Exception:
            log.exception("failed to release ingest lease")
        self.state["running"] = False
        self.state["leader"] = False

    # ------------------------------------------------------------------- round
    def run_round(self, blocking: bool = True) -> RoundReport | None:
        """Run one fixed-revision round.

        With ``blocking=False`` returns ``None`` if a round is already running
        in this process (used by the background loop).  The manual API endpoint
        blocks so its response always describes the round it triggered.
        """
        acquired = self._round_lock.acquire(blocking=blocking)
        if not acquired:
            return None
        try:
            return self._run_round_locked()
        finally:
            self._round_lock.release()

    def _run_round_locked(self) -> RoundReport:
        round_id = uuid.uuid4().hex[:12]
        started = time.monotonic()
        started_at = utcnow()
        self.state["current_round_id"] = round_id
        self.state["rounds_attempted"] += 1
        report = RoundReport(
            round_id=round_id, instance_id=self.instance_id, outcome="failed"
        )
        observed: ObservedHead | None = None
        try:
            self.db.renew_lease(self.instance_id, self.lease_seconds)
            observed = self.source.get_head()
            report.observed = observed
            result = self._sync_to(observed, report)
            report.published = result.published
            report.outcome = result.outcome
            report.ancestor_height = result.ancestor_height
            report.changed_blocks = result.changed_blocks
            report.removed_blocks = result.removed_blocks
            if self.verify_head_after_round:
                # Diagnostics only; `observed` remains the round's basis.
                try:
                    after = self.source.get_head()
                    report.head_moved_during_round = after.revision != observed.revision
                    if report.head_moved_during_round:
                        report.notes.append(
                            f"source head advanced to revision {after.revision} "
                            f"(height {after.height}) while publishing revision "
                            f"{observed.revision}; a later round will follow it"
                        )
                except SourceError as exc:
                    report.notes.append(f"post-round head check failed: {exc}")
            if result.published:
                self.state["rounds_committed"] += 1
                self.state["last_success_at"] = utcnow()
                self.state["last_error"] = None
            else:
                self.state["rounds_stale"] += 1
                report.notes.append(
                    "a revision at least as new as this round was already "
                    "published; nothing was overwritten"
                )
        except SourceError as exc:
            report.error = str(exc)
            report.outcome = "failed"
            self.state["rounds_failed"] += 1
            self.state["last_error"] = str(exc)
            self._record_failure(str(exc), observed)
        except Exception as exc:
            report.error = f"{type(exc).__name__}: {exc}"
            report.outcome = "failed"
            self.state["rounds_failed"] += 1
            self.state["last_error"] = report.error
            log.exception("unexpected ingest failure")
            self._record_failure(report.error, observed)
        finally:
            report.duration_ms = int((time.monotonic() - started) * 1000)
            if report.outcome != "failed":
                self._log_round(report, started_at)
            self.state["last_round"] = report.as_dict()
            self.state["current_round_id"] = None
        return report

    def _record_failure(self, error: str, observed: ObservedHead | None) -> None:
        try:
            self.db.record_failure(error[:2000])
        except Exception:
            log.exception("could not record failure state")
        try:
            self.db.log_round(
                instance_id=self.instance_id,
                started_at=utcnow(),
                observed=observed,
                ancestor_height=None,
                fetched_blocks=0,
                reused_blocks=0,
                changed_blocks=0,
                removed_blocks=0,
                outcome="failed",
                error=error,
            )
        except Exception:
            log.exception("could not record failed round")

    def _log_round(self, report: RoundReport, started_at: str) -> None:
        try:
            self.db.log_round(
                instance_id=report.instance_id,
                started_at=started_at,
                observed=report.observed,
                ancestor_height=report.ancestor_height,
                fetched_blocks=report.fetched_blocks,
                reused_blocks=report.reused_blocks,
                changed_blocks=report.changed_blocks,
                removed_blocks=report.removed_blocks,
                outcome=report.outcome,
                error=report.error,
            )
        except Exception:
            log.exception("could not log ingest round")

    # --------------------------------------------------------------------- sync
    def _sync_to(self, observed: ObservedHead, report: RoundReport) -> CommitResult:
        published = self.db.get_state()
        has_view = published.has_view

        # Revision guard (also re-checked atomically inside commit_view).
        if has_view and published.published_revision >= observed.revision:
            return CommitResult(
                published=False,
                outcome="stale",
                previous_head_height=published.head_height,
                published_revision=published.published_revision,
            )

        if not has_view:
            ancestor_height: int | None = None
            ancestor_hash: str | None = None
            suffix_desc = self._collect_suffix_to_genesis(observed, report)
        else:
            ancestor_height, ancestor_hash, suffix_desc = self._find_ancestor(
                observed, published, report
            )
        report.ancestor_hash = ancestor_hash

        if ancestor_height is not None:
            for child, parent in itertools.pairwise(suffix_desc):
                if child.parent_hash != parent.hash:
                    raise SourceError(
                        f"suffix broken between {child.hash} and {parent.hash}"
                    )
            if suffix_desc:
                root = suffix_desc[-1]
                if (
                    root.height - 1 == ancestor_height
                    and root.parent_hash != ancestor_hash
                ):
                    raise SourceError("changed suffix does not link to ancestor")
            elif observed.height != ancestor_height or observed.hash != ancestor_hash:
                raise SourceError("empty suffix but head differs from ancestor")
        else:
            if suffix_desc[-1].height != 0:
                raise SourceError("full publish does not reach genesis")

        return self.db.commit_view(
            observed, suffix_desc, ancestor_height, ancestor_hash
        )

    def _collect_suffix_to_genesis(
        self, observed: ObservedHead, report: RoundReport
    ) -> list[BlockRecord]:
        suffix: list[BlockRecord] = []
        cursor_hash = observed.hash
        cursor_height = observed.height
        while cursor_height >= 0:
            block = self._load_block(cursor_hash, cursor_height, observed, report)
            suffix.append(block)
            if block.height == 0:
                break
            cursor_hash = block.parent_hash
            cursor_height -= 1
            if cursor_height < 0:
                raise SourceError("walked past genesis")
        return suffix

    def _find_ancestor(
        self,
        observed: ObservedHead,
        published: Any,
        report: RoundReport,
    ) -> tuple[int | None, str | None, list[BlockRecord]]:
        """Return (ancestor_height, ancestor_hash, new_suffix_desc).

        Walks the two heads down in lockstep.  The published side is read from
        the already-published canonical view (identity only); the new side is
        cache-first and fetches only genuinely unknown blocks.
        """
        if observed.hash == published.head_hash:
            if observed.height != published.head_height:
                raise SourceError(
                    f"head {observed.hash} is published at height "
                    f"{published.head_height} but source reports {observed.height}"
                )
            # Same view, newer revision: nothing to fetch, only progress moves.
            return published.head_height, published.head_hash, []

        new_height = observed.height
        new_hash = observed.hash
        old_height = published.head_height
        old_hash = published.head_hash
        suffix: list[BlockRecord] = []

        # Lower the published side to the observed height (rollback case); this
        # only reads the persisted canonical view.
        while old_height > new_height:
            eldest = self.db.canonical_at(old_height)
            parent = self.db.canonical_at(old_height - 1)
            if eldest is None or parent is None or eldest.parent_hash != parent.hash:
                raise SourceError("published canonical view is not contiguous")
            old_hash = parent.hash
            old_height -= 1

        # Lower the observed side to the published height (normal advance).
        while new_height > old_height:
            block = self._load_block(new_hash, new_height, observed, report)
            suffix.append(block)
            new_hash = block.parent_hash
            new_height -= 1

        # Now compare at equal heights, walking down until hashes agree.
        while new_height >= 0:
            if new_hash == old_hash:
                return new_height, new_hash, suffix
            block = self._load_block(new_hash, new_height, observed, report)
            suffix.append(block)
            new_hash = block.parent_hash
            new_height -= 1
            if new_height < 0:
                # Different genesis: no shared ancestor, publish a fresh view.
                return None, None, suffix
            eldest = self.db.canonical_at(old_height)
            parent = self.db.canonical_at(old_height - 1)
            if eldest is None or parent is None or eldest.parent_hash != parent.hash:
                raise SourceError("published canonical view is not contiguous")
            old_hash = parent.hash
            old_height -= 1

        return None, None, suffix

    def _load_block(
        self,
        block_hash: str,
        expected_height: int,
        observed: ObservedHead,
        report: RoundReport,
    ) -> BlockRecord:
        block = self.db.get_block(block_hash)
        if block is not None:
            report.reused_blocks += 1
        else:
            # Keep the ingest lease alive across long fetches.
            self.db.renew_lease(self.instance_id, self.lease_seconds)
            block = self.source.get_block(block_hash)
            report.fetched_blocks += 1
            # Cache immediately so a failure later in the round does not throw
            # away known-valid work; a restart resumes from here.
            self.db.cache_block(block, observed.revision)
        if block.height != expected_height:
            raise SourceError(
                f"block {block_hash} has height {block.height}, expected "
                f"{expected_height}"
            )
        return block
