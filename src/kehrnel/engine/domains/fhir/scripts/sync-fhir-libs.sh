#!/usr/bin/env bash
# Sync vendored FHIR libraries into ../libs/ (no submodules).
# Usage: ./src/kehrnel/engine/domains/fhir/scripts/sync-fhir-libs.sh [SOURCE_ROOT]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LIBS="$(cd "$SCRIPT_DIR/../libs" && pwd)"
KEHRNEL_ROOT="$(cd "$SCRIPT_DIR/../../../../../../" && pwd)"
SOURCE_ROOT="${1:-$(cd "$KEHRNEL_ROOT/../../.." && pwd)}"

EXCLUDE=(--exclude .git --exclude .venv --exclude venv --exclude __pycache__
  --exclude .pytest_cache --exclude .mypy_cache --exclude dist --exclude build
  --exclude .eggs --exclude node_modules --exclude htmlcov --exclude .tox)

sync_one() {
  local name="$1"
  local src="$SOURCE_ROOT/$name"
  local dest="$LIBS/$name"
  if [[ ! -d "$src" ]]; then
    echo "Source not found: $src" >&2
    exit 1
  fi
  rm -rf "$dest"
  mkdir -p "$dest"
  if command -v rsync >/dev/null 2>&1; then
    rsync -a "${EXCLUDE[@]}" "$src/" "$dest/"
  else
    robocopy "$src" "$dest" /E /MT:8 /XD .git .venv venv __pycache__ .pytest_cache dist build .eggs node_modules /NFL /NDL /NJH /NJS || true
  fi
  echo "Synced $name -> $dest"
}

sync_one fhir-data-generation
sync_one fhir-search-to-mql
echo "Done. From kehrnel root, reinstall:"
echo "  pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql"
echo "  pip install -e \".[fhir]\""
