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

echo "== capabilities =="
curl -sS "$BASE_URL/environments/$ENV_ID/capabilities" "${HDRS[@]}"

echo

echo "== run strategy op (universal endpoint) =="
curl -sS -X POST "$BASE_URL/environments/$ENV_ID/run" "${HDRS[@]}" -d @- <<JSON
{
  "domain": "$DOMAIN",
  "operation": "ensure_dictionaries",
  "payload": {}
}
JSON

echo

echo "== compile query (universal endpoint) =="
curl -sS -X POST "$BASE_URL/environments/$ENV_ID/run" "${HDRS[@]}" -d @- <<JSON
{
  "domain": "$DOMAIN",
  "operation": "compile_query",
  "payload": {
    "domain": "$DOMAIN",
    "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c LIMIT 10"
  }
}
JSON

echo

echo "== execute query (universal endpoint) =="
curl -sS -X POST "$BASE_URL/environments/$ENV_ID/run" "${HDRS[@]}" -d @- <<JSON
{
  "domain": "$DOMAIN",
  "operation": "query",
  "payload": {
    "domain": "$DOMAIN",
    "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c LIMIT 10"
  }
}
JSON

echo

echo "done"
