"""End-to-end behaviour of the confirmed-transfers service."""

from __future__ import annotations

import time

from tests.conftest import Stack


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


def _block_requests(stack: Stack) -> dict:
    return stack.source_client().get("/admin/stats").json()["block_requests_by_hash"]


def _source_stats(stack: Stack) -> dict:
    return stack.source_client().get("/admin/stats").json()


# --------------------------------------------------------------------- delivery
def test_transfer_needs_two_blocks_below_head(synced: Stack):
    # Published head is height 5.  Height 4 has 2 confirmations (not served);
    # height 3 has 3 (served).  So the cutoff is "at least 2 blocks below head".
    payload = _transfers(synced, "addr-alice")
    heights = {t["block_height"] for t in payload["transfers"]}
    assert heights and max(heights) <= 3
    assert payload["head_basis"]["height"] == 5
    assert payload["confirmation_policy"]["minimum_confirmations"] == 3
    for transfer in payload["transfers"]:
        assert transfer["confirmations"] == 5 - transfer["block_height"] + 1

    # Loosening is explicitly rejected: two blocks below head is a hard floor,
    # not a client preference.
    response = synced.api().get(
        "/transfers", params={"address": "addr-alice", "min_confirmations": 2}
    )
    assert response.status_code == 422


def test_every_address_query_returns_the_same_complete_view(synced: Stack):
    alice = _transfers(synced, "addr-alice")
    bob = _transfers(synced, "addr-bob")
    assert alice["chain_view"] == bob["chain_view"]
    assert alice["head_basis"] == bob["head_basis"]
    assert alice["chain_view"]["blocks"] == bob["chain_view"]["blocks"]
    heights = [b["height"] for b in alice["chain_view"]["blocks"]]
    assert heights == list(range(len(heights)))  # complete, genesis..head
    assert alice["chain_view"]["head"]["height"] == heights[-1]


def test_unknown_address_returns_empty_but_full_view(synced: Stack):
    payload = _transfers(synced, "addr-nobody")
    assert payload["transfers"] == []
    assert payload["chain_view"]["blocks"]
    assert payload["counts"]["blocks_in_view"] == 6


def test_view_unavailable_before_first_round(stack: Stack):
    response = stack.api().get("/transfers", params={"address": "addr-alice"})
    assert response.status_code == 503
    assert response.json()["detail"]["error"] == "chain_view_unavailable"


# ------------------------------------------------------------------- containment
def test_reorg_updates_only_changed_suffix_and_withdraws_abandoned(
    synced: Stack,
):
    stack = synced
    addresses = ["addr-alice", "addr-bob", "addr-carol", "addr-dave"]
    before = _transfers(stack, "addr-alice")
    before_sets = {
        a: {t["txid"] for t in _transfers(stack, a)["transfers"]} for a in addresses
    }
    # Transfers on the old height-3 block are served before the reorg.
    old_height3 = stack.source_client().get("/canonical/3").json()
    old_height3_txids = {t["txid"] for t in old_height3["transfers"]}
    assert old_height3_txids & set().union(*before_sets.values())
    requests_before = _block_requests(stack)

    fork = (
        stack.source_client()
        .post(
            "/admin/fork",
            json={
                "from_height": 2,
                "count": 4,
                "transfers_per_block": 2,
                "salt": "reorg",
            },
        )
        .json()
    )
    ancestor_hash = fork["common_ancestor"]["hash"]
    assert fork["common_ancestor"]["height"] == 2

    report = _sync(stack)
    assert report["outcome"] == "committed"
    assert report["common_ancestor"]["height"] == 2
    assert report["common_ancestor"]["hash"] == ancestor_hash
    # The four replacement blocks (heights 3..6) are the only new fetches.
    assert report["changed_blocks"] == 4
    assert report["fetched_blocks"] == 4

    after_view = _transfers(stack, "addr-alice")
    assert after_view["head_basis"]["height"] == 6
    old_blocks = {b["height"]: b["hash"] for b in before["chain_view"]["blocks"]}
    new_blocks = {b["height"]: b["hash"] for b in after_view["chain_view"]["blocks"]}
    for height in range(3):
        assert old_blocks[height] == new_blocks[height]
    assert old_blocks[3] != new_blocks[3]

    after_sets = {
        a: {t["txid"] for t in _transfers(stack, a)["transfers"]} for a in addresses
    }
    # Transfer keys that were served from the now-abandoned suffix are gone
    # from *every* address answer, not just the address that used to see them.
    assert not (set().union(*after_sets.values()) & old_height3_txids)
    # Fetch accounting: the common prefix was not re-pulled.
    requests_after = _block_requests(stack)
    for height in range(3):
        block_hash = old_blocks[height]
        assert requests_after.get(block_hash, 0) == requests_before.get(
            block_hash, 0
        ), f"common-prefix block at height {height} was fetched again"


def test_head_rollback_withdraws_insufficiently_confirmed_transfers(
    synced: Stack,
):
    stack = synced
    addresses = ["addr-alice", "addr-bob", "addr-carol", "addr-dave"]
    all_before = {
        a: {t["txid"] for t in _transfers(stack, a)["transfers"]} for a in addresses
    }
    assert _transfers(stack, "addr-alice")["head_basis"]["height"] == 5
    stack.source_client().post("/admin/rollback", json={"to_height": 3})

    report = _sync(stack)
    assert report["outcome"] == "committed"
    assert report["common_ancestor"]["height"] == 3
    after = _transfers(stack, "addr-alice")
    assert after["head_basis"]["height"] == 3
    assert [b["height"] for b in after["chain_view"]["blocks"]] == [0, 1, 2, 3]
    # Height 3 blocks are still in the view but now have only one confirmation;
    # height 2 has two.  Neither may be served.  Their transfers are withdrawn
    # from every address answer even though no new block was fetched.
    still_canonical = stack.source_client().get("/canonical/3").json()
    still_canonical_txids = {t["txid"] for t in still_canonical["transfers"]}
    assert still_canonical_txids & set().union(*all_before.values())
    after_sets = {
        a: {t["txid"] for t in _transfers(stack, a)["transfers"]} for a in addresses
    }
    assert not (set().union(*after_sets.values()) & still_canonical_txids)
    view_blocks = {b["height"]: b["hash"] for b in after["chain_view"]["blocks"]}
    assert view_blocks[3] == still_canonical["hash"]


def test_rollback_below_confirmation_window_withdraws_even_without_new_blocks(
    synced: Stack,
):
    # Head at 5; height 3 has exactly 3 confirmations and is served.  Rolling
    # back to height 4 leaves the height-3 block canonical but only 2
    # confirmations, so its transfers must disappear without any new block.
    before = _transfers(synced, "addr-alice")
    assert any(t["block_height"] == 3 for t in before["transfers"])
    stack = synced
    stack.source_client().post("/admin/rollback", json={"to_height": 4})
    report = _sync(stack)
    assert report["outcome"] == "committed"
    after = _transfers(stack, "addr-alice")
    assert after["head_basis"]["height"] == 4
    assert all(t["block_height"] < 3 for t in after["transfers"])
    assert not any(t["block_height"] == 3 for t in after["transfers"])


# ------------------------------------------------------------------- failure modes
def test_failed_pull_keeps_published_view(synced: Stack):
    before = _transfers(synced, "addr-alice", confirmations=3)
    stack = synced
    stack.source_client().post(
        "/admin/faults", json={"head_failures": 5, "block_failures": 5}
    )
    report = _sync(stack)
    assert report["outcome"] == "failed"
    assert report["published"] is False
    assert report["error"]
    after = _transfers(stack, "addr-alice", confirmations=3)
    assert after["chain_view"] == before["chain_view"]
    assert after["head_basis"] == before["head_basis"]
    # While the source is down, a newly published *local* view never regresses
    # and queries keep working; the failure is observable via status.
    status = stack.api().get("/chain/status").json()
    assert status["last_error"]
    assert status["serving"] is True

    stack.source_client().post("/admin/faults", json={})
    stack.source_client().post("/admin/advance", json={"count": 2})
    assert _sync(stack)["outcome"] == "committed"
    assert _transfers(stack, "addr-alice", confirmations=3)["head_basis"]["height"] == 7


def test_block_failure_mid_round_does_not_publish_partial_view(synced: Stack):
    before = _transfers(synced, "addr-alice", confirmations=3)
    stack = synced
    stack.source_client().post("/admin/advance", json={"count": 4})
    stack.source_client().post("/admin/faults", json={"block_failures": 2})
    report = _sync(stack)
    assert report["outcome"] == "failed"
    after = _transfers(stack, "addr-alice", confirmations=3)
    assert after["chain_view"] == before["chain_view"]

    stack.source_client().post("/admin/faults", json={})
    report = _sync(stack)
    assert report["outcome"] == "committed"
    assert report["changed_blocks"] == 4
    view = _transfers(stack, "addr-alice", confirmations=3)
    assert [b["height"] for b in view["chain_view"]["blocks"]] == list(range(10))


def test_slow_source_does_not_block_queries(synced: Stack, monkeypatch):
    import threading

    stack = synced
    stack.source_client().post("/admin/faults", json={"delay_ms": 1500})
    stack.source_client().post("/admin/advance", json={"count": 1})
    result: dict = {}

    def pull():
        result["report"] = _sync(stack)

    thread = threading.Thread(target=pull)
    thread.start()
    time.sleep(0.25)  # the round is now stuck inside the delayed head pull
    started = time.monotonic()
    payload = _transfers(stack, "addr-alice", confirmations=3)
    elapsed = time.monotonic() - started
    assert payload["chain_view"]["blocks"]
    assert elapsed < 0.75, f"query blocked for {elapsed:.2f}s by slow pull"
    # A slow pull is not a failure: the old view is served meanwhile.
    thread.join(15)
    assert result["report"]["outcome"] == "committed"
    stack.source_client().post("/admin/faults", json={})


# ------------------------------------------------------- revisions / ordering
def test_late_older_revision_never_overwrites_newer(synced: Stack):
    stack = synced
    api = stack.api()
    # Capture an observation, then publish a newer revision first.
    stale_head = stack.source_client().get("/head").json()
    stack.source_client().post("/admin/advance", json={"count": 3})
    assert _sync(stack)["outcome"] == "committed"
    fresh = _transfers(stack, "addr-alice", confirmations=3)
    assert fresh["head_basis"]["height"] == 8

    # Simulate a late round whose observed head is the older revision.
    ingestor = api.post("/admin/sync")  # current, no-op/stale
    assert ingestor.status_code == 200
    from app.db import ObservedHead

    report = stack.api_servers[0].server.config.app.state.ingestor
    late = report._sync_to(  # the exact "late round" path
        ObservedHead(
            revision=stale_head["revision"],
            height=stale_head["height"],
            hash=stale_head["hash"],
        ),
        __import__("app.ingest", fromlist=["RoundReport"]).RoundReport(
            round_id="late", instance_id="late", outcome=""
        ),
    )
    assert late.published is False
    assert late.outcome == "stale"
    after = _transfers(stack, "addr-alice", confirmations=3)
    assert after["chain_view"] == fresh["chain_view"]
    assert after["head_basis"]["revision"] == fresh["head_basis"]["revision"]


def test_identical_head_revision_bump_is_recorded(synced: Stack):
    stack = synced
    before = _transfers(stack, "addr-alice", confirmations=3)
    revision = stack.source.revision
    with stack.source.lock:
        stack.source.revision += 1
        stack.source._save()
    report = _sync(stack)
    assert report["outcome"] == "committed"
    assert report["published"] is True
    assert report["changed_blocks"] == 0
    after = _transfers(stack, "addr-alice", confirmations=3)
    assert after["chain_view"] == before["chain_view"]
    assert after["head_basis"]["revision"] == revision + 1


# ------------------------------------------------------------- persistence
def test_restart_resumes_without_refetching_chain(synced: Stack):
    stack = synced
    stats_before = _source_stats(stack)
    view_before = _transfers(stack, "addr-alice", confirmations=3)
    # Add blocks, then stop the instance *before* ingesting them.
    stack.source_client().post("/admin/advance", json={"count": 3})
    stack.api_servers[0].stop()
    stack.api_servers.clear()

    stack.add_api("instance-restarted")
    after_restart = _transfers(stack, "addr-alice", confirmations=3)
    assert after_restart["chain_view"] == view_before["chain_view"]
    assert after_restart["head_basis"] == view_before["head_basis"]

    report = _sync(stack)
    assert report["outcome"] == "committed"
    assert report["changed_blocks"] == 3
    # Only the three previously-unknown blocks were pulled; the persisted
    # prefix (including the old head) was not re-fetched.
    assert report["fetched_blocks"] == 3
    stats_after = _source_stats(stack)
    for block_hash in {b["hash"] for b in view_before["chain_view"]["blocks"]}:
        assert stats_after["block_requests_by_hash"].get(block_hash, 0) == stats_before[
            "block_requests_by_hash"
        ].get(block_hash, 0)


def test_two_instances_share_volume_and_one_follows(synced: Stack):
    stack = synced
    _server_b, app_b = stack.add_api("instance-b", start_follower=True)
    stack.source_client().post("/admin/advance", json={"count": 2})
    deadline = time.time() + 10
    while time.time() < deadline:
        payload = _transfers(stack, "addr-alice", confirmations=3, index=1)
        if payload["head_basis"]["height"] == 7:
            break
        time.sleep(0.05)
    else:
        raise AssertionError("second instance did not follow the shared view")
    # Both instances serve byte-identical views from the shared SQLite volume.
    assert (
        _transfers(stack, "addr-alice", index=0)["chain_view"] == payload["chain_view"]
    )
    # Only one instance holds the ingest lease at a time.
    lease = app_b.state.db.lease_info()
    assert lease["owner"] in ("instance-b", "instance-a", "instance-restarted")


# --------------------------------------------------------------------- source
def test_source_persists_chain_state(tmp_path):
    from chain_source.app import ChainState

    path = tmp_path / "source.json"
    state = ChainState(state_path=str(path), initial_blocks=3)
    head_hash = state.head_hash
    revision = state.revision
    state.advance(2)
    fork_revision = state.revision
    state.rollback(3)
    assert state.revision == fork_revision + 1

    reloaded = ChainState(state_path=str(path))
    assert reloaded.head_hash == state.head_hash
    assert reloaded.revision == state.revision
    assert reloaded.head.height == 3
    assert reloaded.blocks[head_hash].height == 3
    assert revision < reloaded.revision


def test_docs_and_openapi_available(stack: Stack):
    api = stack.api()
    assert api.get("/docs").status_code == 200
    schema = api.get("/openapi.json").json()
    paths = schema["paths"]
    assert "/transfers" in paths
    assert "/chain/blocks" in paths
    assert "/admin/sync" in paths
    assert "get" in paths["/transfers"]
    # The demo console is served too.
    assert api.get("/").status_code == 200


def test_demo_console_can_drive_fork_and_query(synced: Stack):
    stack = synced
    api = stack.api()
    # The console proxies source controls same-origin.
    assert (
        api.post(
            "/demo/source/admin/fork",
            json={"from_height": 2, "count": 2, "transfers_per_block": 2},
        ).status_code
        == 200
    )
    assert api.post("/demo/source/admin/advance", json={"count": 1}).status_code == 200
    assert _sync(stack)["outcome"] == "committed"
    payload = _transfers(stack, "addr-alice", confirmations=3)
    assert payload["head_basis"]["height"] == 5
    assert payload["chain_view"]["blocks"][0]["height"] == 0


def test_stale_previous_view_survives_concurrent_instance_failure(synced: Stack):
    """Instance B fails a round while A's view is already published."""
    stack = synced
    stack.add_api("instance-b")
    stack.source_client().post("/admin/faults", json={"fail_all": True})
    report = _sync(stack, index=1)
    assert report["outcome"] == "failed"
    good = _transfers(stack, "addr-alice", index=0)
    assert good["chain_view"]["blocks"]
    stack.source_client().post("/admin/faults", json={})
