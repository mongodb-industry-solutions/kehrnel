#!/usr/bin/env bash
set -euo pipefail

# Example CLI flow for pack + data operations.
# Adjust paths to your environment.

PACK_DIR="${PACK_DIR:-src/kehrnel/engine/strategies/openehr/rps_dual}"

echo "== validate strategy pack =="
kehrnel-validate-pack "$PACK_DIR"

echo "== list bundles (if configured) =="
kehrnel-list-bundles || true

echo "== transform (example command discovery) =="
kehrnel-transform --help >/dev/null

echo "== ingest (example command discovery) =="
kehrnel-ingest --help >/dev/null

echo "CLI skeleton complete. Plug your project-specific file paths and options."
