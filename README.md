# Confirmed transfers — chain-following delivery API

Delivers **confirmed** address transfers to address-behaviour analysis, keeping
the published chain view consistent with the chain source: reorgs and head
rollbacks withdraw abandoned and insufficiently confirmed transfers, and a slow
or late ingestion round can never overwrite a newer published view.  A
**materialized query projection** (block list, per-address index, counts) is
precomputed and switched atomically with every published revision, so
long-chain queries reuse precomputed results instead of recomputing the view
from SQLite on every request.

Two FastAPI + SQLite instances share one volume, plus a controllable local HTTP
chain source. Delivered as launchable Docker Compose.

## Quick start

```bash
docker compose up --build
```

| Service        | URL                          | Purpose                        |
| -------------- | ---------------------------- | ------------------------------ |
| API instance A | http://localhost:8000/docs   | Swagger: query + force a round |
| API instance A | http://localhost:8000/       | browser demo console           |
| API instance B | http://localhost:8001/docs   | second instance, same volume   |
| Chain source   | http://localhost:9100/docs   | advance / fork / rollback / faults |

Both API instances mount the same named volume `api-data` at `/data`; the
source persists to `source-data`. Restarting any container resumes from the
persisted chain view and progress — the chain is never re-collected.

Demo (UI or curl):

```bash
# force one complete ingestion round (also happens automatically each second)
curl -X POST http://localhost:8000/admin/sync

# query confirmed transfers for an address
curl 'http://localhost:8000/transfers?address=addr-alice'

# cause a reorg: replace heights 4..6 of the source chain
curl -X POST http://localhost:9100/admin/fork \
  -H 'content-type: application/json' \
  -d '{"from_height":4,"count":3,"transfers_per_block":2,"salt":"demo"}'
curl -X POST http://localhost:8000/admin/sync
curl 'http://localhost:8000/transfers?address=addr-alice'   # moved head + withdrawn keys

# roll the head back (withdraws even without new blocks)
curl -X POST http://localhost:9100/admin/rollback \
  -H 'content-type: application/json' -d '{"to_height":3}'
curl -X POST http://localhost:8000/admin/sync

# simulate source failures: the published view stays served
curl -X POST http://localhost:9100/admin/faults \
  -H 'content-type: application/json' -d '{"head_failures":2,"block_failures":2}'
curl -X POST http://localhost:8000/admin/sync
curl -X POST http://localhost:9100/admin/faults \
  -H 'content-type: application/json' -d '{}'
```

Local (no Docker), Python 3.11+:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q                      # 36 end-to-end tests

# terminals 1-3
SOURCE_STATE_PATH=/tmp/chain-source.json INITIAL_BLOCKS=6 \
  uvicorn chain_source.main:app --port 9100
DB_PATH=/tmp/chain.db CHAIN_SOURCE_URL=http://127.0.0.1:9100 INSTANCE_ID=api-a \
  uvicorn app.main:app --port 8000
DB_PATH=/tmp/chain.db CHAIN_SOURCE_URL=http://127.0.0.1:9100 INSTANCE_ID=api-b \
  uvicorn app.main:app --port 8001
```

## Delivery contract

* **Only confirmed transfers are served.** Confirmations are computed against
  the **published** head: `confirmations = head_height - block_height + 1`.
  The default minimum is 3, i.e. a transfer is served only when it is **at least
  two blocks below the published head**. The floor is enforced in config, the
  DB query, the Pydantic model and the HTTP query parameter — clients can raise
  it but never lower it (`?min_confirmations=2` → `422`).
* **Every address query returns the same complete chain view and its basis.**
  `GET /transfers?address=…` returns `chain_view` (view id, head, every block
  from genesis through head with hash/parent/tx_count), `head_basis` (the
  **published revision** the view is based on, head height/hash, timestamps),
  the confirmation policy, the address's confirmed transfers, and counts. Two
  different addresses in the same revision get byte-identical chain views.
* **Reorgs and rollbacks withdraw.** Abandoned-suffix blocks leave the
  `chain_blocks` view; and because confirmations are recomputed from the
  published head on every query, a rollback withdraws transfers that are still
  canonical but now too shallow **even if no block was fetched**. Withdrawal is
  global: a txid gone from the view is gone from every address answer.
* **Late rounds cannot overwrite newer revisions.** The observed head is fixed
  once per round. `commit_view` runs in one SQLite `BEGIN IMMEDIATE`
  transaction that compares the target revision against `published_revision`
  and refuses (`stale`) when a revision at least as new is already published.
* **Long-chain queries reuse a materialized projection.** Every publish
  transaction also rebuilds `projection_blocks` (per-block transfer counts
  with a running total), `projection_addresses` (per-address transfers with
  precomputed confirmations) and `projection_meta` (view id, totals, build
  time). Queries for the same revision then read precomputed rows: the counts
  are O(1) lookups and address answers are an index scan filtered by
  `min_confirmations` — no per-query recomputation of the chain view, the
  counts or the address result.
* **The projection switches atomically per revision.** It is written in the
  same transaction as `chain_blocks` and `sync_state`, so advance, reorg,
  rollback and same-head revision bumps switch it together with the view, a
  failed publish leaves the previous projection untouched, and two instances
  sharing the database never read a mixed revision (each query checks the
  projection's revision against `sync_state` inside one read transaction; a
  missing or stale projection falls back to computing from `chain_blocks`
  until the next publish). `/chain/projection` reports the projection's
  revision, size, build time and `in_sync` flag.
* **Failures keep the published view.** Blocks are fetched into an immutable
  cache first; the published view and progress only change in the final commit.
  Any head/block/validation error leaves the previous view intact and the
  error recorded on `/chain/status` and `/admin/rounds`.
* **No refetch of the known prefix.** The follower walks the two heads down
  `parent_hash` to the common ancestor, cache first. Only changed-suffix blocks
  are fetched; abandoned branches stay cached and are reused if a branch
  returns. Blocks pulled before a crash are reused after restart.
* **Slow pulls do not block queries.** Normal API handlers are synchronous
  functions (threadpool), and the follower runs in its own thread on its own
  SQLite connections. A delayed source only delays the next commit.
* **Persisted view + progress, restart-safe.** `sync_state` (published revision,
  head, status, errors) and `chain_blocks` are written together in the same
  transaction. On restart the follower continues from the persisted head.
* **Two instances, one view.** Both replicas may run the follower. A SQLite
  lease (renewed during long rounds) elects one active follower at a time, and
  the revision guard makes concurrent rounds safe regardless of the lease.

## API

| Method | Path                       | Description |
| ------ | -------------------------- | ----------- |
| GET    | `/transfers?address=…`     | Confirmed transfers + full chain view + head basis |
| GET    | `/chain/blocks`            | Full published chain view + head basis |
| GET    | `/chain/projection`        | Materialized projection status: revision, size, build time, `in_sync` |
| GET    | `/chain/status`            | Progress: revision, head, errors, lease, follower state, projection |
| GET    | `/health`                  | Ops health for Compose |
| POST   | `/admin/sync`              | Run exactly one round synchronously and report it |
| GET    | `/admin/rounds`            | Recent round audit log |
| GET    | `/docs`                    | Swagger UI |
| GET    | `/`                        | Browser demo console |

The demo console has buttons for a round, an address query, advance, fork,
rollback, failure injection and slow-source delay. It calls source controls
through the same-origin `/demo/source/*` passthrough.

### Chain source

Blocks are immutable and fetched by hash; `/head` carries a monotonically
increasing `revision`. Control endpoints:

| Method | Path                | Description |
| ------ | ------------------- | ----------- |
| GET    | `/head`             | `{revision, height, hash}` |
| GET    | `/blocks/{hash}`    | `{height, hash, parent_hash, transfers[]}` |
| GET    | `/canonical/{height}` | Canonical block at height |
| POST   | `/admin/advance`    | Append `count` blocks |
| POST   | `/admin/fork`       | Replace the canonical suffix above `from_height` |
| POST   | `/admin/rollback`   | Move the head down to `to_height` |
| POST   | `/admin/faults`     | `delay_ms`, `head_failures`, `block_failures`, `fail_all` |
| GET    | `/admin/stats`      | Per-block request counts (proves no refetching) |
| GET    | `/docs`             | Swagger UI |

A transfer is `{txid, from_address, to_address, amount}` with an integer
`amount`; `from`/`to` aliases are also emitted.

## Persistence schema

* `blocks` / `block_transfers` — immutable by-hash cache of every block ever
  fetched, including abandoned branches.
* `chain_blocks` — the published canonical height→hash view.
* `sync_state` — published revision, head, status, last round/error, round count.
* `projection_blocks` — materialized published view: one row per block with
  `tx_count` and `cum_tx_count` (running total) for O(1) count lookups.
* `projection_addresses` — per-address transfer index for the published
  revision, with confirmations precomputed at publish time; queries only
  filter `confirmations >= min_confirmations`.
* `projection_meta` — the projection's revision, view id, head, totals and
  build time; its revision must match `sync_state.published_revision`.
* `ingest_lease` — one active follower across instances.
* `ingest_rounds` — bounded audit trail (last 500 rounds).

The three projection tables are written only inside the publish transaction
(or the startup reconcile), so they always describe exactly one published
revision.

## Long-chain demo (Compose + /docs)

```bash
docker compose up --build
bash scripts/demo-long-chain.sh      # or drive the same calls from /docs
```

The script (curl + jq; override ports via `API_A`/`API_B`/`SOURCE`) walks
through the projection lifecycle, and every step is also clickable in Swagger
UI at http://localhost:8000/docs (source controls at
http://localhost:9100/docs):

1. **Build a long chain** — `POST /admin/advance` on the source with e.g.
   `{"count": 300, "transfers_per_block": 8}`, then `POST /admin/sync` on the
   API. `GET /chain/projection` shows the precomputed projection
   (`blocks`, `addresses`, `transfers`, `build_ms`) with `in_sync: true`.
2. **Repeated queries reuse it** — repeated `GET /transfers?address=…` calls
   return byte-identical `chain_view`s, the projection's `built_at` does not
   change, and `?min_confirmations=100` is just a stricter filter over the
   precomputed rows.
3. **Reorg** — `POST /admin/fork` + sync: the projection switches atomically
   to the new revision/head on both instances.
4. **Rollback** — `POST /admin/rollback` + sync: the projection switches to
   the lower head; confirmations are recomputed at publish time, so
   now-too-shallow transfers leave every address answer.
5. **Failed publish** — `POST /admin/faults` + advance + sync fails; the
   projection is retained unchanged (`built_at` identical, `in_sync` still
   true) and queries keep serving the last published revision.
6. **Recovery** — clear faults + sync: the projection switches to the newest
   revision; instance B (`:8001`) reports the identical projection.

## Verified behaviour (tests)

`pytest` runs 36 end-to-end tests against real HTTP servers on ephemeral
ports and a real SQLite file. They cover: the two-block confirmation cutoff and
that it cannot be lowered; every address seeing the identical complete view;
reorg suffix-only update with no common-prefix refetch (asserted from
`/admin/stats` per-hash counters); global withdrawal of abandoned txids; head
rollback withdrawing both abandoned and now-too-shallow transfers; failures
leaving the published view intact; slow pulls not delaying queries; genuinely
late rounds rejected after a newer publish; two instances converging from a
shared volume; restart resuming without refetching; crash-cached blocks reused;
same-head revision bumps; source state persistence; and `/docs`/console
availability. Projection-specific tests cover: projection answers identical to
the from-scratch computation for every address and confirmation override;
same-revision queries reusing the precomputed projection without a rebuild;
atomic projection switches on reorg, rollback and same-head revision bumps;
the projection retained unchanged through failed publishes; fallback to the
legacy computation when the projection is missing, with startup reconcile
rebuilding it; two instances sharing one projection; `in_sync` never violated
during concurrent commits from two instances; and a 120-block long chain
served from the projection including a deep reorg.

## Known limits

* SQLite with a shared WAL is intended for replicas on the **same host**
  (one Compose volume). It is not a multi-node database; use a real DB server
  for cross-host replicas.
* One global lease means only one follower is actively pulling at a time; the
  other replica serves reads and takes over after the lease expires
  (`INGEST_LEASE_SECONDS`, default 15 s).
* The chain source is a deterministic simulation, not a Bitcoin/EVM node; its
  "blocks" and txids are synthetic and its revision is local to the process
  state file.
* Confirmation depth counts blocks on top of the transaction's block inclusive
  of the head, per the documented formula; there is no separate finality rule.
* The demo console is intentionally unauthenticated and bound to the host's
  published ports; keep it off untrusted networks.
* Docker was unavailable in the build sandbox, so the Compose definition and
  Dockerfiles were validated by YAML inspection and by running both ASGI apps
  under Uvicorn with the exact pinned requirements, not by an actual
  `docker compose up`. See "Verification performed" below.

## Verification performed

| Check | Result |
| --- | --- |
| `pytest` (36 end-to-end tests, real HTTP servers + real SQLite file) | pass |
| Both ASGI apps under Uvicorn (`chain_source.main:app`, `app.main:app`), two API instances on one SQLite file | started, served `/health`, `/docs`, `/`, queries; identical `/chain/projection` and `chain_view` on both instances |
| Long-chain live run (300 blocks × 8 transfers) | projection built once at publish (34 ms); 20 repeated queries returned one identical view with `built_at` unchanged; `min_confirmations=100` override filtered precomputed rows |
| Reorg / rollback / fault injection over live HTTP | projection switched atomically per published revision; retained unchanged through failed publishes (`in_sync` stayed true); switched to newest revision after recovery |
| Cold start + restart | no projection before first publish; first commit built it atomically; restart served the persisted projection without rebuild |
| `scripts/demo-long-chain.sh` against a live 3-service stack | all steps passed, including dual-instance projection equality |
| `docker compose up --build` | **not executed** — no Docker daemon in this environment; the Compose definition and Dockerfiles are unchanged and were validated by YAML inspection plus the Uvicorn runs above |

Not implemented / out of scope: authentication and rate limiting on the API and
demo console, a real chain node adapter, cross-host replica coordination,
metrics/tracing, and pruning of the immutable block cache.
