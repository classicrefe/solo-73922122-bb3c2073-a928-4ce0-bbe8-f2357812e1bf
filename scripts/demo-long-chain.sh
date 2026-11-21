#!/usr/bin/env bash
# Long-chain materialized-projection demo against the Docker Compose stack.
#
#   docker compose up --build
#   bash scripts/demo-long-chain.sh
#
# Drives the same endpoints that Swagger UI exposes at http://localhost:8000/docs
# (API instance A), http://localhost:8001/docs (instance B) and
# http://localhost:9100/docs (chain source): build a long chain, watch the
# materialized projection switch atomically per published revision on reorg /
# rollback / same-head revision bumps, stay retained through failed publishes,
# and serve repeated long-chain queries from precomputed rows.
#
# Requires: curl, jq, python3 (only for JSON pretty-printing).
set -euo pipefail

API_A=${API_A:-http://localhost:8000}
API_B=${API_B:-http://localhost:8001}
SOURCE=${SOURCE:-http://localhost:9100}
BLOCKS=${BLOCKS:-300}            # blocks to append for the long chain
TPB=${TPB:-8}                    # transfers per block
QUERIES=${QUERIES:-20}           # repeated same-revision queries to time

step() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
proj() {  # $1 = api base url
  curl -s "$1/chain/projection" | jq -c \
    '{in_sync, rev: .projection.revision, head: .projection.head_height,
      blocks: .projection.blocks, addresses: .projection.addresses,
      transfers: .projection.transfers, build_ms: .projection.build_ms}'
}

step "wait for the stack (compose healthchecks)"
for url in "$SOURCE/health" "$API_A/health" "$API_B/health"; do
  for _ in $(seq 1 60); do
    curl -sf "$url" > /dev/null 2>&1 && break
    sleep 1
  done
  curl -sf "$url" > /dev/null || { echo "not healthy: $url" >&2; exit 1; }
done
echo "stack is healthy — Swagger UIs: $API_A/docs · $API_B/docs · $SOURCE/docs"

step "advance the source to a long chain: $BLOCKS blocks x $TPB transfers"
remaining=$BLOCKS
while [ "$remaining" -gt 0 ]; do
  chunk=$(( remaining > 200 ? 200 : remaining ))
  curl -s -X POST "$SOURCE/admin/advance" -H 'content-type: application/json' \
    -d "{\"count\": $chunk, \"transfers_per_block\": $TPB}" \
    | jq -c '{head: .head.height, created: (.created | length)}'
  remaining=$(( remaining - chunk ))
done

step "publish one round on instance A"
curl -s -X POST "$API_A/admin/sync" | jq -c \
  '{outcome, rev: .observed_head.revision, head: .observed_head.height,
    changed_blocks, duration_ms}'

step "projection after publish (A and B share the database)"
proj "$API_A"
proj "$API_B"

step "$QUERIES repeated long-chain queries reuse the precomputed projection"
built_before=$(curl -s "$API_A/chain/projection" | jq -r '.projection.built_at')
start=$(date +%s%N)
for _ in $(seq 1 "$QUERIES"); do
  curl -s "$API_A/transfers?address=addr-alice" | jq -r '.chain_view.view_id'
done | sort -u | wc -l | xargs -I{} echo "distinct view_ids across $QUERIES queries: {}"
end=$(date +%s%N)
echo "total: $(( (end - start) / 1000000 ))ms for $QUERIES queries on the long chain"
built_after=$(curl -s "$API_A/chain/projection" | jq -r '.projection.built_at')
[ "$built_before" = "$built_after" ] \
  && echo "projection built_at unchanged: queries reused precomputed rows" \
  || { echo "projection changed during queries!"; exit 1; }
curl -s "$API_A/transfers?address=addr-alice&min_confirmations=100" | jq -c \
  '{override_min_conf: .confirmation_policy.minimum_confirmations,
    returned: .counts.returned_transfers,
    queryable: .counts.queryable_transfers_in_view}'

step "reorg: replace the top 20 blocks with 25 new ones"
head=$(curl -s "$SOURCE/head" | jq -r '.height')
curl -s -X POST "$SOURCE/admin/fork" -H 'content-type: application/json' \
  -d "{\"from_height\": $((head - 20)), \"count\": 25, \"transfers_per_block\": $TPB, \"salt\": \"long-demo\"}" \
  | jq -c '{ancestor: .common_ancestor.height, head: .head.height}'
curl -s -X POST "$API_A/admin/sync" | jq -c '{outcome, changed_blocks, removed_blocks}'
proj "$API_A"; proj "$API_B"

step "rollback the head by 30 blocks (no new blocks fetched)"
head=$(curl -s "$SOURCE/head" | jq -r '.height')
curl -s -X POST "$SOURCE/admin/rollback" -H 'content-type: application/json' \
  -d "{\"to_height\": $((head - 30))}" | jq -c '{head: .height}'
curl -s -X POST "$API_A/admin/sync" | jq -c '{outcome, changed_blocks, removed_blocks}'
proj "$API_A"

step "failed publish: source faults leave the projection retained"
retained_before=$(curl -s "$API_A/chain/projection" | jq -r '.projection.built_at')
curl -s -X POST "$SOURCE/admin/faults" -H 'content-type: application/json' \
  -d '{"head_failures": 4, "block_failures": 4}' > /dev/null
curl -s -X POST "$SOURCE/admin/advance" -H 'content-type: application/json' \
  -d '{"count": 5, "transfers_per_block": 4}' | jq -c '{head: .head.height}'
curl -s -X POST "$API_A/admin/sync" | jq -c '{outcome, error: (.error // "" | .[0:70])}'
proj "$API_A"
retained_after=$(curl -s "$API_A/chain/projection" | jq -r '.projection.built_at')
[ "$retained_before" = "$retained_after" ] \
  && echo "projection retained unchanged through the failed publish" \
  || { echo "projection moved during a failed publish!"; exit 1; }
curl -s "$API_A/chain/status" | jq -c '{status, last_error: (.last_error // "" | .[0:60])}'

step "recover: faults cleared, projection switches to the newest revision"
curl -s -X POST "$SOURCE/admin/faults" -H 'content-type: application/json' -d '{}' > /dev/null
curl -s -X POST "$API_A/admin/sync" | jq -c '{outcome, changed_blocks}'
proj "$API_A"; proj "$API_B"

step "done"
echo "Explore interactively: $API_A/docs (try /transfers, /chain/blocks,"
echo "/chain/projection, /admin/sync) and $SOURCE/docs (advance/fork/rollback/faults)."
