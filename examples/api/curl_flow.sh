#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
API_KEY="${API_KEY:-}"
ENV_ID="${ENV_ID:-dev}"
DOMAIN="${DOMAIN:-openehr}"
STRATEGY_ID="${STRATEGY_ID:-openehr.rps_dual}"

HDRS=(-H "Content-Type: application/json")
if [[ -n "$API_KEY" ]]; then
  HDRS+=(-H "X-API-Key: $API_KEY")
fi

echo "== list strategies =="
curl -sS "$BASE_URL/strategies" "${HDRS[@]}" | sed -e 's/{/{\n/g' | head -n 30

echo "== activate env =="
curl -sS -X POST "$BASE_URL/environments/$ENV_ID/activate" "${HDRS[@]}" -d @- <<JSON
{
  "strategy_id": "$STRATEGY_ID",
  "domain": "$DOMAIN",
  "version": "latest",
  "config": {
    "database": "openehr_db"
  },
  "bindings_ref": "env:$ENV_ID"
}
JSON

echo

echo "== compile query =="
curl -sS -X POST "$BASE_URL/environments/$ENV_ID/compile_query" "${HDRS[@]}" -d @- <<JSON
{
  "domain": "$DOMAIN",
  "query": "SELECT c FROM EHR e CONTAINS COMPOSITION c LIMIT 10"
}
JSON

echo

echo "== execute query =="
curl -sS -X POST "$BASE_URL/environments/$ENV_ID/query" "${HDRS[@]}" -d @- <<JSON
{
  "domain": "$DOMAIN",
  "query": "SELECT c FROM EHR e CONTAINS COMPOSITION c LIMIT 10"
}
JSON

echo

echo "== trigger strategy op =="
curl -sS -X POST "$BASE_URL/environments/$ENV_ID/activations/$DOMAIN/ops/ensure_dictionaries" "${HDRS[@]}" -d '{}'

echo

echo "done"
