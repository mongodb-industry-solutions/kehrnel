#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

RUNTIME_URL="${RUNTIME_URL:-http://localhost:8000}"
ENV_ID="${ENV_ID:-dev}"
DOMAIN="${DOMAIN:-openehr}"
STRATEGY_ID="${STRATEGY_ID:-openehr.rps_dual}"
API_KEY="${API_KEY:-${KEHRNEL_API_KEY:-}}"
BINDINGS_REF="${BINDINGS_REF:-}"

WORKDIR="${WORKDIR:-.kehrnel/workflow-smoke}"
TEMPLATE_OPT="${TEMPLATE_OPT:-samples/templates/T-IGR-TUMOUR-SUMMARY.opt}"
SOURCE_DB="${SOURCE_DB:-hc_openEHRCDR}"
SOURCE_COLLECTION="${SOURCE_COLLECTION:-samples}"
PATIENT_COUNT="${PATIENT_COUNT:-50}"
REQUIRE_RUNTIME="${REQUIRE_RUNTIME:-1}"
GENERATE_RANDOM="${GENERATE_RANDOM:-0}"
VALIDATION_STRICT="${VALIDATION_STRICT:-0}"

run() {
  echo
  echo "==> $*"
  "$@"
}

if ! command -v kehrnel >/dev/null 2>&1; then
  echo "error: 'kehrnel' command not found. Install with: python -m pip install -e '.[cli,api]'" >&2
  exit 127
fi

mkdir -p "${WORKDIR}"

SETUP_CMD=(
  kehrnel setup
  --non-interactive
  --skip-health
  --runtime-url "${RUNTIME_URL}"
  --env "${ENV_ID}"
  --domain "${DOMAIN}"
  --strategy "${STRATEGY_ID}"
)
if [[ -n "${API_KEY}" ]]; then
  SETUP_CMD+=(--api-key "${API_KEY}")
fi
run "${SETUP_CMD[@]}"
run kehrnel core health

echo
echo "### Local template workflow ###"
run kehrnel common map-skeleton -- "${TEMPLATE_OPT}" -o "${WORKDIR}/mapping.skeleton.yaml" --macros
if [[ "${GENERATE_RANDOM}" == "1" ]]; then
  run kehrnel common generate -- -t "${TEMPLATE_OPT}" -o "${WORKDIR}/composition.json" --random
else
  run kehrnel common generate -- -t "${TEMPLATE_OPT}" -o "${WORKDIR}/composition.json"
fi
echo
echo "==> kehrnel common validate -- -c ${WORKDIR}/composition.json -t ${TEMPLATE_OPT} --stats --json"
if ! kehrnel common validate -- -c "${WORKDIR}/composition.json" -t "${TEMPLATE_OPT}" --stats --json > "${WORKDIR}/validate.report.json"; then
  if [[ "${VALIDATION_STRICT}" == "1" ]]; then
    echo "error: validation failed and VALIDATION_STRICT=1 is set." >&2
    exit 3
  fi
  echo "validation returned issues; continuing smoke workflow (set VALIDATION_STRICT=1 to fail)." >&2
fi
run kehrnel common transform -- flatten "${WORKDIR}/composition.json" -o "${WORKDIR}/flattened.json"

# Configure resource profiles to validate universal source/sink model.
if [[ -n "${MONGODB_URI:-}" ]]; then
  run kehrnel resource add src --type mongo --uri "${MONGODB_URI}" --db "${SOURCE_DB}" --collection "${SOURCE_COLLECTION}"
  run kehrnel resource add dst --type mongo --uri "${MONGODB_URI}" --db "${SOURCE_DB}" --collection "compositions_rps"
else
  run kehrnel resource add src --type file --path "${WORKDIR}"
  run kehrnel resource add dst --type file --path "${WORKDIR}/out"
fi
run kehrnel resource use --source src --sink dst
run kehrnel op list --domain "${DOMAIN}"
run kehrnel op capabilities --env "${ENV_ID}" --json

if [[ -z "${BINDINGS_REF}" ]]; then
  if [[ "${REQUIRE_RUNTIME}" == "1" ]]; then
    echo "error: BINDINGS_REF is required for runtime activation in this smoke workflow." >&2
    echo "hint: export BINDINGS_REF=env://DB_BINDINGS (or your resolver-specific value)." >&2
    exit 2
  fi
  echo
  echo "Skipping runtime activation and runtime ops because BINDINGS_REF is not set."
  exit 0
fi

echo
echo "### Runtime workflow ###"
run kehrnel core env activate --env "${ENV_ID}" --domain "${DOMAIN}" --strategy "${STRATEGY_ID}" --bindings-ref "${BINDINGS_REF}"
run kehrnel run ensure_dictionaries --env "${ENV_ID}" --domain "${DOMAIN}"

cat > "${WORKDIR}/synthetic.payload.json" <<EOF
{
  "patient_count": ${PATIENT_COUNT},
  "generation_mode": "from_source",
  "source_database": "${SOURCE_DB}",
  "source_collection": "${SOURCE_COLLECTION}",
  "source_min_per_patient": 1,
  "source_max_per_patient": 2
}
EOF

run kehrnel run synthetic_generate_batch --env "${ENV_ID}" --domain "${DOMAIN}" --payload "${WORKDIR}/synthetic.payload.json" --dry-run

cat > "${WORKDIR}/query.payload.json" <<EOF
{
  "domain": "${DOMAIN}",
  "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c LIMIT 5"
}
EOF

run kehrnel run compile_query --env "${ENV_ID}" --domain "${DOMAIN}" --payload "${WORKDIR}/query.payload.json"
run kehrnel run query --env "${ENV_ID}" --domain "${DOMAIN}" --payload "${WORKDIR}/query.payload.json"

echo
echo "Workflow completed. Artifacts written to: ${WORKDIR}"
