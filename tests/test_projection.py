"""Materialized query projection: reuse, atomic switching, retention.

The projection (projection_blocks / projection_addresses / projection_meta)
is rebuilt inside the publish transaction, so it must always agree with
``chain_blocks`` + ``sync_state`` at the published revision — across advances,
reorgs, rollbacks, same-head revision bumps, failed publishes, restarts and
two instances sharing one database file.
"""

from __future__ import annotations

import dataclasses
import threading
import time

from tests.conftest import Stack


def _db(stack: Stack, index: int = 0):
    return stack.api_servers[index].server.config.app.state.db


def _sync(stack: Stack, index: int = 0) -> dict:
    response = stack.api(index).post("/admin/sync")
    assert response.status_code == 200, response.text
    return response.json()


def _transfers(
    stack: Stack, address: str, confirmations: int = 3, index: int = 0
) -> dict:
    response = stack.api(index).get(
        "/transfers",
        params={"address": address, "min_confirmations": confirmations},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _projection(stack: Stack, index: int = 0) -> dict:
    response = stack.api(index).get("/chain/projection")
    assert response.status_code == 200, response.text
    return response.json()


def _snapshot_dict(snapshot) -> dict:
    data = dataclasses.asdict(snapshot)
    data.pop("source")  # the serving path may differ; the content must not
    return data


def _legacy_snapshot(db, address: str | None, min_confirmations: int):
    with db._read() as conn:
        state = db._state_row(conn)
        return db._snapshot_legacy(conn, state, address, min_confirmations)


# --------------------------------------------------------------------- equality
def test_projection_serves_identical_results_as_legacy(synced: Stack):
    db = _db(synced)
    addresses = ("addr-alice", "addr-bob", "addr-carol", "addr-dave", "addr-nobody")
    for address in addresses:
        for min_conf in (3, 4, 6, 1000):
            projected = db.snapshot(address=address, min_confirmations=min_conf)
            assert projected.source == "projection"
            legacy = _legacy_snapshot(db, address, min_conf)
            assert _snapshot_dict(projected) == _snapshot_dict(legacy), (
                f"projection/legacy mismatch for {address} @ {min_conf}"
            )
    # The address-less chain view snapshot matches too.
    projected = db.snapshot()
    assert _snapshot_dict(projected) == _snapshot_dict(_legacy_snapshot(db, None, 3))


def test_same_revision_queries_reuse_precomputed_projection(synced: Stack):
    db = _db(synced)
    info_before = db.projection_info()
    assert info_before["in_sync"] is True

    first = _transfers(synced, "addr-alice")
    second = _transfers(synced, "addr-alice")
    third = _transfers(synced, "addr-bob")
    assert first["chain_view"] == second["chain_view"] == third["chain_view"]
    assert first["head_basis"] == second["head_basis"] == third["head_basis"]
    # /chain/blocks is served from the same precomputed projection.
    blocks = synced.api().get("/chain/blocks").json()
    assert blocks["chain_view"] == first["chain_view"]

    info_after = db.projection_info()
    # No rebuild and no new revision: queries only read precomputed rows.
    assert info_after["projection"] == info_before["projection"]
    assert info_after["projection"]["revision"] == first["head_basis"]["revision"]


# --------------------------------------------------------------- state changes
def test_projection_switches_atomically_on_reorg(synced: Stack):
    stack = synced
    db = _db(stack)
    before = db.projection_info()["projection"]
    old_hashes = {
        b["hash"] for b in _transfers(stack, "addr-alice")["chain_view"]["blocks"]
    }

    stack.source_client().post(
        "/admin/fork",
        json={"from_height": 2, "count": 4, "transfers_per_block": 2, "salt": "proj"},
    )
    report = _sync(stack)
    assert report["outcome"] == "committed"

    info = db.projection_info()
    state = db.get_state()
    assert info["in_sync"] is True
    assert info["projection"]["revision"] == state.published_revision
    assert info["projection"]["revision"] > before["revision"]
    assert info["projection"]["head_height"] == 6

    # The abandoned suffix left the projection's address index as well.
    new_hashes = {
        b["hash"] for b in _transfers(stack, "addr-alice")["chain_view"]["blocks"]
    }
    abandoned = old_hashes - new_hashes
    assert abandoned
    with db._read() as conn:
        rows = conn.execute(
            "SELECT DISTINCT block_hash FROM projection_addresses"
        ).fetchall()
    assert not ({r["block_hash"] for r in rows} & abandoned)

    # The switched projection equals a from-scratch computation.
    snap = db.snapshot(address="addr-dave")
    assert snap.source == "projection"
    assert _snapshot_dict(snap) == _snapshot_dict(_legacy_snapshot(db, "addr-dave", 3))


def test_projection_switches_on_rollback(synced: Stack):
    stack = synced
    db = _db(stack)
    stack.source_client().post("/admin/rollback", json={"to_height": 3})
    assert _sync(stack)["outcome"] == "committed"

    info = db.projection_info()
    assert info["in_sync"] is True
    assert info["projection"]["head_height"] == 3
    assert info["projection"]["blocks"] == 4

    # Stored confirmations were recomputed against the rolled-back head, so
    # height-3 transfers now carry 1 confirmation and are filtered out.
    with db._read() as conn:
        rows = conn.execute(
            "SELECT block_height, confirmations FROM projection_addresses"
        ).fetchall()
    assert rows
    for row in rows:
        assert row["confirmations"] == 3 - row["block_height"] + 1
    snap = db.snapshot(address="addr-alice")
    assert snap.transfers
    assert all(t["confirmations"] >= 3 for t in snap.transfers)
    assert _snapshot_dict(snap) == _snapshot_dict(_legacy_snapshot(db, "addr-alice", 3))


def test_same_head_revision_bump_switches_projection_revision(synced: Stack):
    stack = synced
    db = _db(stack)
    before = db.projection_info()["projection"]
    with stack.source.lock:
        stack.source.revision += 1
        stack.source._save()
    report = _sync(stack)
    assert report["outcome"] == "committed"
    assert report["changed_blocks"] == 0

    info = db.projection_info()
    assert info["in_sync"] is True
    assert info["projection"]["revision"] == before["revision"] + 1
    # Content untouched: same view, same precomputed rows, no rebuild.
    assert info["projection"]["view_id"] == before["view_id"]
    assert info["projection"]["blocks"] == before["blocks"]
    assert info["projection"]["built_at"] == before["built_at"]
    payload = _transfers(stack, "addr-alice")
    assert payload["head_basis"]["revision"] == before["revision"] + 1


def test_failed_publish_retains_previous_projection(synced: Stack):
    stack = synced
    db = _db(stack)
    before = db.projection_info()["projection"]
    stack.source_client().post(
        "/admin/advance", json={"count": 3, "transfers_per_block": 2}
    )
    stack.source_client().post("/admin/faults", json={"block_failures": 5})

    report = _sync(stack)
    assert report["outcome"] == "failed"

    after = db.projection_info()
    assert after["in_sync"] is True
    assert after["projection"] == before  # retained byte-for-byte
    snap = db.snapshot(address="addr-alice")
    assert snap.source == "projection"
    assert snap.state.published_revision == before["revision"]

    # Recovery publishes the pending blocks and switches the projection on.
    stack.source_client().post("/admin/faults", json={})
    assert _sync(stack)["outcome"] == "committed"
    recovered = db.projection_info()
    assert recovered["in_sync"] is True
    assert recovered["projection"]["revision"] > before["revision"]
    assert recovered["projection"]["head_height"] == 8


# ------------------------------------------------------------------ robustness
def test_missing_projection_falls_back_then_reconciles(synced: Stack):
    stack = synced
    db = _db(stack)
    expected = _transfers(stack, "addr-alice")

    # Simulate a partially restored database: projection tables are empty.
    with db._write() as conn:
        conn.execute("DELETE FROM projection_addresses")
        conn.execute("DELETE FROM projection_blocks")
        conn.execute("DELETE FROM projection_meta")

    snap = db.snapshot(address="addr-alice")
    assert snap.source == "legacy"
    assert snap.view_id == expected["chain_view"]["view_id"]
    assert db.projection_info()["in_sync"] is False

    # The startup reconcile rebuilds the projection for the published view.
    assert db.reconcile_projection() is True
    restored = db.snapshot(address="addr-alice")
    assert restored.source == "projection"
    assert restored.view_id == expected["chain_view"]["view_id"]
    assert _snapshot_dict(restored) == _snapshot_dict(db.snapshot(address="addr-alice"))
    assert db.projection_info()["in_sync"] is True


def test_projection_endpoints_in_docs_and_status(stack: Stack):
    api = stack.api()
    schema = api.get("/openapi.json").json()
    assert "/chain/projection" in schema["paths"]
    assert api.get("/docs").status_code == 200

    # Before the first publish there is no projection yet.
    info = api.get("/chain/projection").json()
    assert info == {
        "serving": False,
        "published_revision": 0,
        "in_sync": False,
        "projection": None,
    }
    status = api.get("/chain/status").json()
    assert status["projection"]["in_sync"] is False


def test_two_instances_share_one_projection(synced: Stack):
    stack = synced
    stack.add_api("instance-b")
    stack.source_client().post(
        "/admin/advance", json={"count": 2, "transfers_per_block": 2}
    )
    assert _sync(stack, index=0)["outcome"] == "committed"

    info_a = _projection(stack, index=0)
    info_b = _projection(stack, index=1)
    assert info_a == info_b
    assert info_b["in_sync"] is True
    # Both instances serve identical answers built from the shared projection.
    assert _transfers(stack, "addr-alice", index=0) == _transfers(
        stack, "addr-alice", index=1
    )


def test_projection_never_out_of_sync_during_concurrent_commits(synced: Stack):
    stack = synced
    stack.add_api("instance-b")
    stop = threading.Event()
    violations: list[dict] = []

    def watcher() -> None:
        while not stop.is_set():
            info = _projection(stack, index=1)
            if not info["in_sync"]:
                violations.append(info)
            time.sleep(0.005)

    def syncer(index: int) -> None:
        while not stop.is_set():
            stack.api(index).post("/admin/sync")
            time.sleep(0.01)

    threads = [threading.Thread(target=watcher, daemon=True)] + [
        threading.Thread(target=syncer, args=(i,), daemon=True) for i in (0, 1)
    ]
    for thread in threads:
        thread.start()
    try:
        for _ in range(6):
            stack.source_client().post(
                "/admin/advance", json={"count": 1, "transfers_per_block": 2}
            )
            time.sleep(0.1)
        time.sleep(0.5)
    finally:
        stop.set()
        for thread in threads:
            thread.join(15)

    assert not violations, f"mixed-revision reads observed: {violations[:3]}"
    final = _projection(stack, index=1)
    source_head = stack.source_client().get("/head").json()
    assert final["in_sync"] is True
    assert final["projection"]["revision"] == source_head["revision"]


# -------------------------------------------------------------------- long chain
def test_long_chain_queries_served_from_projection(stack: Stack):
    # 120 blocks x 6 transfers each on top of genesis: a long chain whose
    # view would otherwise be recomputed on every query.
    stack.source_client().post(
        "/admin/advance", json={"count": 120, "transfers_per_block": 6}
    )
    report = _sync(stack)
    assert report["outcome"] == "committed"
    assert report["changed_blocks"] == 121  # genesis + 120

    db = _db(stack)
    info = db.projection_info()
    assert info["in_sync"] is True
    assert info["projection"]["blocks"] == 121
    assert info["projection"]["transfers"] == 120 * 6

    payload = _transfers(stack, "addr-alice")
    assert payload["counts"]["blocks_in_view"] == 121
    assert payload["counts"]["confirmed_transfers_in_view"] == 720
    # min_confirmations=3 -> blocks 0..118 are queryable -> 118 * 6 transfers.
    assert payload["counts"]["queryable_transfers_in_view"] == 118 * 6

    # Projection answers equal the legacy computation on the long chain.
    for address in ("addr-alice", "addr-bob", "addr-carol", "addr-dave"):
        snap = db.snapshot(address=address, min_confirmations=3)
        assert snap.source == "projection"
        assert _snapshot_dict(snap) == _snapshot_dict(_legacy_snapshot(db, address, 3))

    # A stricter override is only a filter over the precomputed rows.
    strict = _transfers(stack, "addr-alice", confirmations=30)
    assert strict["transfers"]
    assert all(t["confirmations"] >= 30 for t in strict["transfers"])
    assert strict["counts"]["queryable_transfers_in_view"] == (120 - 30 + 1) * 6

    # Same-revision queries are repeatable and trigger no rebuild.
    again = _transfers(stack, "addr-alice")
    assert again["chain_view"] == payload["chain_view"]
    assert (
        db.projection_info()["projection"]["built_at"] == info["projection"]["built_at"]
    )

    # A reorg deep into the long chain switches the projection atomically.
    stack.source_client().post(
        "/admin/fork",
        json={
            "from_height": 100,
            "count": 25,
            "transfers_per_block": 6,
            "salt": "deep",
        },
    )
    assert _sync(stack)["outcome"] == "committed"
    reorged = db.projection_info()
    assert reorged["in_sync"] is True
    assert reorged["projection"]["head_height"] == 125
    assert reorged["projection"]["blocks"] == 126
    snap = db.snapshot(address="addr-alice")
    assert _snapshot_dict(snap) == _snapshot_dict(_legacy_snapshot(db, "addr-alice", 3))
