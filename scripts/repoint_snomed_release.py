"""Repoint the snomedct activation at a different SNOMED release id.

WHY THIS EXISTS (and why it is not an HTTP call)
------------------------------------------------
Reads resolve the release from the ACTIVATION CONFIG (`release.id`), so ingesting
a new release is not enough on its own - the concepts sit in the collection
unused until the activation points at them.

Kehrnel has no config-patch route. The documented way to change one config key is
to re-activate the same strategy with a merged config via
`POST /environments/{env}/activate`, but on THIS deployment that is impossible:

  * the route requires `bindings_ref` whenever auth is enabled
    (api/core/admin/routes.py, BINDINGS_REF_REQUIRED), and
  * this activation has `bindings_ref: None` - it was created with plaintext
    bindings - so re-activating would both 400 and, if forced through, discard
    the Mongo binding that makes the strategy work.

So the only safe change is a surgical edit of the activation record in the
registry file, which is exactly what this does: it rewrites `config.release`
and nothing else, recomputes `config_hash` the same way the runtime does
(sha256 of json.dumps(config, sort_keys=True) - see
StrategyRuntime._config_hash), and writes a timestamped backup first.

Kehrnel reads the registry at startup, so RESTART Kehrnel afterwards.

SYNTHETIC CONTENT: hdl-synthetic-2 is a synthetic SNOMED-shaped hierarchy, NOT an
official SNOMED CT release, and no SNOMED CT licence is implied or required for it.

USAGE
    python scripts/repoint_snomed_release.py --show
    python scripts/repoint_snomed_release.py --release-id hdl-synthetic-2
    python scripts/repoint_snomed_release.py --release-id hdl-demo-synthetic-1   # revert
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
REGISTRY = REPO / ".kehrnel_registry.json"

LABELS = {
    "hdl-synthetic-2": "Synthetic SNOMED-shaped demonstration hierarchy (NOT an official SNOMED CT release)",
    "hdl-demo-synthetic-1": "HDL demo synthetic concept set (NOT licensed SNOMED CT)",
}


def config_hash(config: dict) -> str:
    """Mirror StrategyRuntime._config_hash exactly."""
    try:
        blob = json.dumps(config or {}, sort_keys=True).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()
    except Exception:
        return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--registry", type=Path, default=REGISTRY)
    ap.add_argument("--env-id", default="env-1768371326510")
    ap.add_argument("--domain", default="snomedct")
    ap.add_argument("--release-id")
    ap.add_argument("--label")
    ap.add_argument("--show", action="store_true", help="Print the current release and exit.")
    args = ap.parse_args()

    if not args.registry.exists():
        print(f"registry not found: {args.registry}")
        return 1

    data = json.loads(args.registry.read_text(encoding="utf-8"))
    env_acts = (data.get("activations") or {}).get(args.env_id)
    if not env_acts:
        print(f"no activations for env {args.env_id}")
        return 1
    act = env_acts.get(args.domain)
    if not act:
        print(f"no {args.domain} activation for env {args.env_id}")
        return 1

    cfg = act.setdefault("config", {})
    current = (cfg.get("release") or {}).get("id")
    print(f"registry       : {args.registry}")
    print(f"env / domain   : {args.env_id} / {args.domain}")
    print(f"strategy       : {act.get('strategy_id')} {act.get('version')}")
    print(f"database       : {cfg.get('database')}")
    print(f"current release: {current}")

    if args.show or not args.release_id:
        if not args.release_id and not args.show:
            print("\nNothing to do: pass --release-id to change it, or --show to inspect.")
        return 0

    if current == args.release_id:
        print(f"\nAlready pointing at {args.release_id}; nothing to change.")
        return 0

    # Back up before touching anything.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = args.registry.with_suffix(args.registry.suffix + f".bak-{stamp}")
    shutil.copy2(args.registry, backup)
    print(f"\nbackup written : {backup.name}")

    label = args.label or LABELS.get(args.release_id) or f"Release {args.release_id}"
    cfg["release"] = {"id": args.release_id, "label": label}

    # Recompute the hash the runtime would derive from this config, so the stored
    # value stays consistent with the stored config rather than describing the old one.
    act["config_hash"] = config_hash(cfg)
    act["updated_at"] = datetime.now(timezone.utc).isoformat()

    args.registry.write_text(json.dumps(data, indent=2), encoding="utf-8")

    print(f"new release    : {args.release_id}")
    print(f"new label      : {label}")
    print(f"new config_hash: {act['config_hash']}")
    print("\nRESTART Kehrnel for this to take effect (the registry is read at startup).")
    print("Then verify:  snomed_readiness -> releaseId should be " + args.release_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
