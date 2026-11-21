"""Ordering, concurrency and no-refetch guarantees."""

from __future__ import annotations

import threading
import time

from app.db import BlockRecord, TransferRecord
from app.ingest import Ingestor
from app.source_client import ChainSourceClient
from tests.conftest import Stack


class FixedHeadClient(ChainSourceClient):
    """A source client whose first head read is captured, then returns `pinned`.

    Blocks are still fetched for real, which is exactly the shape of a slow
    round: the head was seen at revision N, the source moved on, and the round
    finishes late.
    """

    def __init__(self, real: ChainSourceClient, pinned):
        self.real = real
        self.pinned = pinned
        self.served_pinned = False
        self.base_url = real.base_url

    def get_head(self):
        if not self.served_pinned:
            self.served_pinned = True
            return self.pinned
        return self.real.get_head()

    def get_block(self, block_hash):
        return self.real.get_block(block_hash)

    def close(self):
        return None


def _block_counts(stack: Stack) -> dict[str, int]:
    return dict(
        stack.source_client().get("/admin/stats").json()["block_requests_by_hash"]
    )


def test_ordinary_advance_never_refetches_known_prefix(synced: Stack):
    stack = synced
    before = _block_counts(stack)
    stack.source_client().post("/admin/advance", json={"count": 3})
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "committed"
    assert report["changed_blocks"] == 3
    assert report["fetched_blocks"] == 3
    after = _block_counts(stack)
    for block_hash in before:
        assert after.get(block_hash, 0) == before[block_hash]
    # The previous head is reused for ancestor detection, not refetched.
    prev_head = stack.source_client().get("/admin/canonical").json()["blocks"][5]
    assert after.get(prev_head["hash"], 0) == before.get(prev_head["hash"], 0)


def test_reorg_back_to_abandoned_branch_reuses_cached_blocks(synced: Stack):
    stack = synced
    source = stack.source
    old_canonical = dict(source.canonical)
    stack.source_client().post(
        "/admin/fork", json={"from_height": 3, "count": 2, "transfers_per_block": 2}
    )
    assert stack.api().post("/admin/sync").json()["outcome"] == "committed"
    counts = _block_counts(stack)

    # Re-org back onto the original blocks by re-seeding the branch from the
    # cached chain: fork from height 3 with the original hashes restored.
    with source.lock:
        source.canonical = dict(old_canonical)
        source.head_hash = old_canonical[max(old_canonical)]
        source.revision += 1
        source._save()
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "committed"
    assert report["fetched_blocks"] == 0, "cached abandoned blocks were refetched"
    after = _block_counts(stack)
    for block_hash, count in counts.items():
        assert after.get(block_hash, 0) == count


def test_genuinely_late_round_is_rejected_after_newer_publish(synced: Stack):
    stack = synced
    stale = stack.source_client().get("/head").json()
    pinned = type(
        "H",
        (),
        {
            "revision": stale["revision"],
            "height": stale["height"],
            "hash": stale["hash"],
        },
    )()
    wrapper = FixedHeadClient(
        stack.api_servers[0].server.config.app.state.ingestor.source, pinned
    )
    late_ingestor = Ingestor(
        stack.api_servers[0].server.config.app.state.db,
        wrapper,
        instance_id="late-instance",
        verify_head_after_round=False,
    )

    # Newer revision gets published first, from the real source.
    stack.source_client().post("/admin/advance", json={"count": 3})
    assert stack.api().post("/admin/sync").json()["outcome"] == "committed"
    fresh = stack.api().get("/transfers", params={"address": "addr-alice"}).json()

    # The late round now tries to publish revision=stale on a stale head.
    report = late_ingestor.run_round()
    assert report.outcome == "stale"
    assert report.published is False
    after = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert after["chain_view"] == fresh["chain_view"]
    assert after["head_basis"] == fresh["head_basis"]


def test_concurrent_instances_cannot_regress_view(synced: Stack):
    stack = synced
    stack.add_api("instance-b")
    stopped = threading.Event()

    def hammer(index: int):
        while not stopped.is_set():
            stack.api(index).post("/admin/sync")

    threads = [threading.Thread(target=hammer, args=(i,)) for i in (0, 1)]
    for thread in threads:
        thread.start()
    try:
        for i in range(4):
            stack.source_client().post("/admin/advance", json={"count": 2})
            time.sleep(0.15)
        # Let both loops settle on the final head.
        time.sleep(0.5)
        last_expected = stack.source_client().get("/head").json()
        deadline = time.time() + 10
        while time.time() < deadline:
            views = [
                stack.api(i).get("/transfers", params={"address": "addr-alice"}).json()
                for i in (0, 1)
            ]
            if all(
                v["head_basis"]["revision"] == last_expected["revision"] for v in views
            ):
                break
            time.sleep(0.1)
        else:
            raise AssertionError("instances did not converge on the newest revision")
        assert views[0]["chain_view"] == views[1]["chain_view"]
        assert views[0]["head_basis"]["height"] == last_expected["height"]
        revisions = [v["head_basis"]["revision"] for v in views]
        assert revisions[0] == revisions[1] == last_expected["revision"]
    finally:
        stopped.set()
        for thread in threads:
            thread.join(15)


def test_head_rollback_then_recovery_keeps_monotonic_revision(synced: Stack):
    stack = synced
    first = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    stack.source_client().post("/admin/rollback", json={"to_height": 3})
    rolled = stack.api().post("/admin/sync").json()
    assert rolled["outcome"] == "committed"
    rolled_view = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert rolled_view["head_basis"]["revision"] > first["head_basis"]["revision"]
    stack.source_client().post("/admin/advance", json={"count": 4})
    recovered = stack.api().post("/admin/sync").json()
    assert recovered["outcome"] == "committed"
    final = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert final["head_basis"]["height"] == 7
    assert final["head_basis"]["revision"] > rolled_view["head_basis"]["revision"]
    assert {b["height"] for b in final["chain_view"]["blocks"]} == set(range(8))


def test_no_publish_when_head_unchanged_revision_equal(synced: Stack):
    stack = synced
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "stale"
    assert report["published"] is False


def test_restart_during_partial_round_does_not_lose_or_refetch(synced: Stack):
    """A cached-but-unpublished suffix is reused after a crash/restart.

    We reproduce the crash artifact directly: the new head block was fetched
    and cached, but the round never published (process died before commit).
    """
    stack = synced
    stack.source_client().post("/admin/advance", json={"count": 3})
    head = stack.source_client().get("/head").json()
    head_block = stack.source_client().get(f"/blocks/{head['hash']}").json()
    db = stack.api_servers[0].server.config.app.state.db
    db.cache_block(
        BlockRecord(
            hash=head_block["hash"],
            height=head_block["height"],
            parent_hash=head_block["parent_hash"],
            transfers=tuple(
                TransferRecord(
                    txid=t["txid"],
                    from_address=t["from_address"],
                    to_address=t["to_address"],
                    amount=t["amount"],
                )
                for t in head_block["transfers"]
            ),
        ),
        head["revision"],
    )
    # The published view did NOT advance: cache is ahead of the view.
    before = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert before["head_basis"]["height"] == 5

    stack.api_servers[0].stop()
    stack.api_servers.clear()
    stack.add_api("instance-after-crash")
    counts_before = _block_counts(stack)
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "committed"
    assert report["changed_blocks"] == 3
    # The cached head block is reused; the other two are fetched.
    assert report["reused_blocks"] >= 1
    assert report["fetched_blocks"] == 2
    counts_after = _block_counts(stack)
    assert counts_after[head["hash"]] == counts_before[head["hash"]]
    view = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert view["head_basis"]["height"] == 8
    assert [b["height"] for b in view["chain_view"]["blocks"]] == list(range(9))
