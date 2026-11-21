"""FastAPI application: chain-view delivery and browser demo console."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from .config import Settings
from .db import (
    MIN_CONFIRMATIONS_FLOOR,
    Database,
    ViewSnapshot,
    ViewUnavailable,
)
from .ingest import Ingestor
from .source_client import ChainSourceClient

log = logging.getLogger("api")


class PreviewRequest(BaseModel):
    address: str
    min_confirmations: int | None = Field(
        default=None, ge=MIN_CONFIRMATIONS_FLOOR, le=1000
    )


CONFIRMATION_RULE = "confirmations = published_head_height - block_height + 1"


def _policy(min_confirmations: int) -> dict[str, Any]:
    return {
        "minimum_confirmations": min_confirmations,
        "rule": CONFIRMATION_RULE,
        "transfers_served_when": "confirmations >= minimum_confirmations",
        "note": "at least 2 blocks below the published head are served",
    }


def _view_payload(
    snapshot: ViewSnapshot, requested_address: str | None
) -> dict[str, Any]:
    """One complete, self-consistent chain view for every query."""
    return {
        "chain_view": {
            "view_id": snapshot.view_id,
            "head": {
                "height": snapshot.state.head_height,
                "hash": snapshot.state.head_hash,
            },
            "blocks": snapshot.blocks,
        },
        "head_basis": {
            # The revision is the head's justification: the published view is
            # exactly what the source advertised at this revision.
            "revision": snapshot.state.published_revision,
            "height": snapshot.state.head_height,
            "hash": snapshot.state.head_hash,
            "published_at": snapshot.state.published_at,
            "last_round_at": snapshot.state.last_round_at,
        },
        "confirmation_policy": _policy(snapshot.min_confirmations),
        "address": requested_address,
        "transfers": snapshot.transfers,
        "counts": {
            "blocks_in_view": len(snapshot.blocks),
            "confirmed_transfers_in_view": snapshot.transfer_count,
            "queryable_transfers_in_view": snapshot.queryable_transfer_count,
            "returned_transfers": len(snapshot.transfers),
        },
    }


def create_app(
    settings: Settings | None = None,
    *,
    database: Database | None = None,
    source: ChainSourceClient | None = None,
    start_follower: bool | None = None,
    demo_console: bool = True,
) -> FastAPI:
    settings = settings or Settings.from_env()
    db = database or Database(settings.db_path)
    db.initialize()
    source_client = source or ChainSourceClient(
        settings.source_url, settings.source_timeout
    )
    ingestor = Ingestor(
        db,
        source_client,
        instance_id=settings.instance_id,
        poll_interval=settings.poll_interval,
        lease_seconds=settings.lease_seconds,
        verify_head_after_round=settings.verify_head_after_round,
    )
    follower_enabled = (
        settings.follower_enabled if start_follower is None else start_follower
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if follower_enabled:
            ingestor.start()
        try:
            yield
        finally:
            ingestor.stop()
            if source is None:
                source_client.close()

    app = FastAPI(
        title="Confirmed Transfers API",
        version="1.0.0",
        description=(
            "Chain-view delivery for address behaviour analysis. Every address "
            "query returns the same complete published chain view plus the head "
            "revision it is based on. Only transfers at least two blocks below "
            "the published head are served (minimum confirmations is "
            f"{settings.min_confirmations}). Reorgs and head rollbacks withdraw "
            "abandoned or insufficiently confirmed transfers, and a late round "
            "can never overwrite a newer published revision. Progress and chain "
            "view are persisted in SQLite, so a restart resumes following."
        ),
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.db = db
    app.state.ingestor = ingestor
    app.state.source_client = source_client

    # ------------------------------------------------------------------- health
    @app.get("/health", tags=["ops"])
    def health() -> dict[str, Any]:
        # Deliberately cheap and read-only: Docker healthchecks fire every 5s
        # on both replicas and must never contend with ingestion.
        state = db.get_state()
        return {
            "status": "ok",
            "instance_id": settings.instance_id,
            "follower": ingestor.state,
            "chain_view": {
                "published_revision": state.published_revision,
                "head_height": state.head_height,
                "head_hash": state.head_hash,
                "status": state.status,
                "serving": state.has_view,
                "last_error": state.last_error,
                "last_error_at": state.last_error_at,
            },
            "lease": db.lease_info(),
        }

    @app.get("/chain/status", tags=["chain"])
    def chain_status() -> dict[str, Any]:
        state = db.get_state()
        return {
            "published_revision": state.published_revision,
            "head": {
                "height": state.head_height,
                "hash": state.head_hash,
            }
            if state.has_view
            else None,
            "status": state.status,
            "serving": state.has_view,
            "published_at": state.published_at,
            "last_round_at": state.last_round_at,
            "rounds": state.rounds,
            "last_error": state.last_error,
            "last_error_at": state.last_error_at,
            "min_confirmations": settings.min_confirmations,
            "follower": ingestor.state,
            "lease": db.lease_info(),
        }

    @app.get("/chain/blocks", tags=["chain"])
    def chain_blocks() -> dict[str, Any]:
        snapshot = _snapshot_or_503()
        return {
            "chain_view": {
                "view_id": snapshot.view_id,
                "head": {
                    "height": snapshot.state.head_height,
                    "hash": snapshot.state.head_hash,
                },
                "blocks": snapshot.blocks,
            },
            "head_basis": {
                "revision": snapshot.state.published_revision,
                "height": snapshot.state.head_height,
                "hash": snapshot.state.head_hash,
                "published_at": snapshot.state.published_at,
            },
            "confirmation_policy": _policy(snapshot.min_confirmations),
        }

    @app.get("/transfers", tags=["transfers"])
    def transfers_by_address(
        address: str = Query(..., min_length=1, max_length=256),
        min_confirmations: int | None = Query(
            default=None,
            ge=MIN_CONFIRMATIONS_FLOOR,
            le=1000,
            description=(
                "Override the server default; never below "
                f"{MIN_CONFIRMATIONS_FLOOR} (two blocks below head)"
            ),
        ),
    ) -> dict[str, Any]:
        snapshot = _snapshot_or_503(
            min_confirmations=min_confirmations, address=address
        )
        payload = _view_payload(snapshot, address)
        if min_confirmations is not None:
            payload["confirmation_policy"]["override_used"] = True
        return payload

    @app.post("/transfers/preview", tags=["transfers"], include_in_schema=False)
    def preview(body: PreviewRequest) -> dict[str, Any]:
        snapshot = _snapshot_or_503(
            min_confirmations=body.min_confirmations, address=body.address
        )
        return _view_payload(snapshot, body.address)

    def _snapshot_or_503(
        min_confirmations: int | None = None, address: str | None = None
    ) -> ViewSnapshot:
        effective = (
            settings.min_confirmations
            if min_confirmations is None
            else min_confirmations
        )
        try:
            return db.snapshot(address=address, min_confirmations=effective)
        except ViewUnavailable as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "chain_view_unavailable",
                    "message": str(exc),
                    "hint": "wait for the follower to publish its first round",
                },
            ) from exc

    # -------------------------------------------------------------------- admin
    @app.post("/admin/sync", tags=["admin"])
    def sync_once() -> dict[str, Any]:
        """Run exactly one round now; useful for deterministic demos/tests."""
        report = ingestor.run_round(blocking=True)
        if report is None:  # practically unreachable: blocking=True
            raise HTTPException(status_code=409, detail="a round is already running")
        return report.as_dict()

    @app.get("/admin/rounds", tags=["admin"])
    def rounds(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, Any]:
        return {"rounds": db.recent_rounds(limit)}

    if demo_console:

        @app.get("/", response_class=HTMLResponse, include_in_schema=False)
        def demo_console() -> HTMLResponse:
            return HTMLResponse(DEMO_HTML)

        @app.api_route(
            "/demo/source{path:path}",
            methods=["GET", "POST"],
            include_in_schema=False,
        )
        async def demo_source_proxy(path: str, request: Request) -> Any:
            """Same-origin passthrough to the chain source for the demo page.

            Async I/O only: it must never occupy a worker thread that a query
            or the follower needs.
            """
            import httpx

            url = f"{settings.source_url.rstrip('/')}{path}"
            body = await request.body()
            headers = {
                k: v
                for k, v in request.headers.items()
                if k.lower() in ("content-type", "accept")
            }
            try:
                async with httpx.AsyncClient(timeout=settings.source_timeout) as client:
                    response = await client.request(
                        request.method,
                        url,
                        content=body or None,
                        headers=headers,
                        params=request.query_params,
                    )
            except httpx.HTTPError as exc:
                raise HTTPException(
                    status_code=502, detail=f"source unreachable: {exc}"
                )
            try:
                payload = response.json()
            except ValueError:
                payload = {"detail": response.text[:500]}
            return JSONResponse(status_code=response.status_code, content=payload)

    return app


DEMO_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Confirmed transfers — demo console</title>
<style>
  :root { color-scheme: dark; }
  body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; margin: 0;
         background:#0e1116; color:#d6dde6; }
  header { padding:14px 20px; border-bottom:1px solid #263040; display:flex;
           gap:16px; align-items:baseline; flex-wrap:wrap; }
  h1 { font-size:16px; margin:0; letter-spacing:.04em; }
  main { display:grid; grid-template-columns: 360px 1fr; gap:16px; padding:16px 20px; }
  .card { border:1px solid #263040; border-radius:8px; padding:12px 14px;
          background:#141a22; }
  .card h2 { font-size:12px; margin:0 0 10px; color:#8ea3bb; letter-spacing:.08em;
             text-transform:uppercase; }
  label { display:block; font-size:11px; color:#8ea3bb; margin:8px 0 3px; }
  input, select { width:100%; box-sizing:border-box; background:#0e1116;
          color:#d6dde6; border:1px solid #2c3a4b; border-radius:5px; padding:6px; }
  button { margin-top:10px; margin-right:6px; background:#1f6feb; color:#fff;
           border:0; border-radius:5px; padding:7px 11px; cursor:pointer; }
  button.ghost { background:#263040; }
  button:disabled { opacity:.5; cursor:wait; }
  pre { white-space:pre-wrap; word-break:break-word; margin:6px 0 0;
        font-size:11.5px; line-height:1.45; max-height:320px; overflow:auto; }
  .pill { font-size:11px; padding:2px 7px; border-radius:999px;
          border:1px solid #2c3a4b; }
  .ok { background:#12341f; border-color:#2b6b3f; }
  .bad { background:#3a1417; border-color:#7a2a30; }
  .withdrawn { color:#7a899c; text-decoration:line-through; }
  .grid2 { display:grid; grid-template-columns:1fr 1fr; gap:10px; }
  table { width:100%; border-collapse:collapse; font-size:11.5px; }
  td, th { text-align:left; padding:4px 6px; border-bottom:1px solid #202a37; }
  th { color:#8ea3bb; font-weight:500; }
  .hint { color:#7a899c; font-size:11px; margin-top:6px; }
  @media (max-width: 900px) { main { grid-template-columns:1fr; } }
</style>
</head>
<body>
<header>
  <h1>confirmed transfers · demo console</h1>
  <span id="status" class="pill">loading…</span>
  <span class="hint">same API as the other replica; both share /data — open :8001 for instance B</span>
</header>
<main>
  <section>
    <div class="card">
      <h2>follow chain</h2>
      <button onclick="sync()" id="syncBtn">run one round</button>
      <button class="ghost" onclick="refresh()">refresh</button>
      <pre id="round">—</pre>
    </div>
    <div class="card" style="margin-top:12px">
      <h2>address query</h2>
      <label>address</label>
      <input id="addr" value="addr-alice">
      <label>minimum confirmations</label>
      <input id="conf" type="number" value="3" min="1">
      <button onclick="query()">query transfers</button>
      <div class="hint">Keys withdrawn by a reorg/rollback stay visible struck
        through (as seen by this browser) so the change is observable.</div>
      <pre id="query">—</pre>
    </div>
    <div class="card" style="margin-top:12px">
      <h2>persisted view</h2>
      <pre id="view">—</pre>
    </div>
  </section>
  <section>
    <div class="card">
      <h2>transfers in view</h2>
      <div id="txs">query an address…</div>
    </div>
    <div class="card" style="margin-top:12px">
      <h2>recent rounds</h2>
      <div id="rounds">—</div>
    </div>
    <div class="card" style="margin-top:12px">
      <h2>chain source controls</h2>
      <div class="grid2">
        <div>
          <label>advance blocks</label>
          <input id="advCount" type="number" value="3" min="1">
          <button class="ghost" onclick="advance()">advance</button>
        </div>
        <div>
          <label>fork from height (suffix replaced)</label>
          <input id="forkFrom" type="number" value="3" min="0">
          <button class="ghost" onclick="fork()">fork</button>
        </div>
      </div>
      <div class="grid2">
        <div>
          <label>rollback to height</label>
          <input id="rollbackTo" type="number" value="2" min="0">
          <button class="ghost" onclick="rollback()">rollback head</button>
        </div>
        <div>
          <label>simulate failure (next N head / block pulls)</label>
          <input id="failures" type="number" value="3" min="1">
          <button class="ghost" onclick="fail()">inject failures</button>
        </div>
      </div>
      <label>slow pull: delay every source response (ms)</label>
      <input id="delay" type="number" value="0" min="0" step="250">
      <button class="ghost" onclick="slow()">apply delay</button>
      <p class="hint">Advanced controls call the chain source directly through
        this console's /demo proxy.</p>
    </div>
  </section>
</main>
<script>
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let lastQuery = null;

async function api(path, opts) {
  const res = await fetch(path, opts);
  const body = await res.json().catch(() => ({detail: res.statusText}));
  if (!res.ok) throw new Error(JSON.stringify(body.detail ?? body));
  return body;
}
async function source(path, opts) {
  return api('/demo/source' + path, opts);
}
function busy(btn, on) { if (btn) btn.disabled = on; }

async function refresh() {
  try {
    const h = await api('/health');
    const v = h.chain_view;
    const pill = $('status');
    pill.textContent = v.serving
      ? `serving rev ${v.published_revision} · head ${v.head_height}`
      : 'no view yet';
    pill.className = 'pill ' + (v.serving ? 'ok' : 'bad');
    $('view').textContent = JSON.stringify(v, null, 2);
    const rounds = await api('/admin/rounds?limit=6');
    $('rounds').innerHTML = '<table><tr><th>round</th><th>inst</th><th>outcome</th>' +
      '<th>obs rev/h</th><th>anc</th><th>fetched/reused</th><th>ms</th></tr>' +
      rounds.rounds.map(r => `<tr><td>${esc(r.id)}</td><td>${esc(r.instance_id.slice(0,12))}</td>` +
        `<td>${esc(r.outcome)}</td><td>${r.observed_revision ?? '—'}/${r.observed_height ?? '—'}</td>` +
        `<td>${r.ancestor_height ?? '—'}</td><td>${r.fetched_blocks}/${r.reused_blocks}</td>` +
        `<td>${r.finished_at?.slice(11,19) ?? ''}</td></tr>`).join('') + '</table>';
  } catch (e) {
    $('status').textContent = 'error'; $('status').className = 'pill bad';
    $('view').textContent = String(e);
  }
}

async function sync() {
  busy($('syncBtn'), true);
  try { $('round').textContent = JSON.stringify(await api('/admin/sync', {method:'POST'}), null, 2); }
  catch (e) { $('round').textContent = String(e); }
  finally { busy($('syncBtn'), false); refresh(); }
}

async function query() {
  const address = $('addr').value;
  const conf = $('conf').value;
  try {
    const body = await api(`/transfers?address=${encodeURIComponent(address)}&min_confirmations=${encodeURIComponent(conf)}`);
    lastQuery = body;
    $('query').textContent = JSON.stringify({
      head_basis: body.head_basis, counts: body.counts,
      policy: body.confirmation_policy.minimum_confirmations,
    }, null, 2);
    renderTxs(body.transfers);
  } catch (e) { $('query').textContent = String(e); renderTxs([]); }
  refresh();
}

// Withdrawal is tracked in the browser purely for the demo: keys present in
// this address's previous answer but absent now are struck through with the
// reason, so a reorg/rollback is visible without polling the source.
const lastRows = new Map();
function renderTxs(rows) {
  const addr = $('addr').value;
  const previous = lastRows.get(addr) || [];
  const now = new Set(rows.map(t => t.txid));
  const gone = previous.filter(t => !now.has(t.txid));
  lastRows.set(addr, rows);
  if (!rows.length && !gone.length) {
    $('txs').textContent = 'no confirmed transfers';
    return;
  }
  $('txs').innerHTML = '<table><tr><th>height</th><th>txid</th><th>from</th>' +
    '<th>to</th><th>amount</th><th>confs</th></tr>' + rows.map(t =>
      `<tr><td>${t.block_height}</td><td title="${esc(t.txid)}">${esc(t.txid.slice(0,12))}…</td>` +
      `<td>${esc(t.from_address)}</td><td>${esc(t.to_address)}</td>` +
      `<td>${t.amount}</td><td>${t.confirmations}</td></tr>`).join('') +
    gone.map(t => `<tr class="withdrawn"><td>${t.block_height}</td>` +
      `<td title="withdrawn: reorg or head rollback">${esc(t.txid.slice(0,12))}…</td>` +
      `<td colspan="3">withdrawn — abandoned by reorg or below confirmation floor</td>` +
      `<td>0</td></tr>`).join('') + '</table>';
}

async function advance() {
  await source('/admin/advance', {method:'POST', headers:{'content-type':'application/json'},
    body: JSON.stringify({count: +$('advCount').value, transfers_per_block: 2})});
  await sync();
}
async function fork() {
  await source('/admin/fork', {method:'POST', headers:{'content-type':'application/json'},
    body: JSON.stringify({from_height: +$('forkFrom').value, count: 3, transfers_per_block: 2})});
  await sync(); if (lastQuery) query();
}
async function rollback() {
  await source('/admin/rollback', {method:'POST', headers:{'content-type':'application/json'},
    body: JSON.stringify({to_height: +$('rollbackTo').value})});
  await sync(); if (lastQuery) query();
}
async function fail() {
  const n = +$('failures').value;
  await source('/admin/faults', {method:'POST', headers:{'content-type':'application/json'},
    body: JSON.stringify({head_failures: n, block_failures: n})});
  await sync();
}
async function slow() {
  await source('/admin/faults', {method:'POST', headers:{'content-type':'application/json'},
    body: JSON.stringify({delay_ms: +$('delay').value})});
  await sync();
}
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""
