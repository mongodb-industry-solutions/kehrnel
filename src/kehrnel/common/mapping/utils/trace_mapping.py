# src/mapper/utils/trace_mapping.py
"""
Trace a YAML/JSON mapping against a source document and show what would be
inserted into a composition.

CLI usage
---------
    python -m openEHRMapper.utils.trace_mapping \\
           --mapping  src/mapper/mappings/tumour_mapping.yaml \\
           --source   src/mapper/in/fiche_tumour.xml \\
           --output   trace.csv          # “-” or omit → pretty table on stdout
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml
from tabulate import tabulate

# our handlers (extend with CSVHandler etc. later)
from ..handlers.xml_handler import XMLHandler


# ─────────────────────────────────────────────────────────────────────────────
# public helper used by core.OpenEHRGenerator.trace()
# ─────────────────────────────────────────────────────────────────────────────
def build_trace_table(
    mapping: Dict[str, Any],
    source_path: Path,
    handlers,
) -> List[Dict[str, str]]:
    """
    Return a list of {json_path, rule, value, ok} rows.
    """
    handler = next((h for h in handlers if h.can_handle(source_path)), None)
    if handler is None:
        raise ValueError(f"No handler registered for '{source_path.suffix}'")

    src_root = handler.load_source(source_path)

    rows: List[Dict[str, str]] = []
    for json_path, rule in mapping.items():
        if json_path.startswith("_"):        # skip meta / preprocessing keys
            continue
        try:
            val = handler.extract_value(src_root, rule)
            rows.append(
                {
                    "json_path": json_path,
                    "rule": json.dumps(rule, ensure_ascii=False)
                    if isinstance(rule, dict)
                    else str(rule),
                    "value": "" if val in (None, []) else val,
                    "ok": "✔︎" if val not in (None, "", []) else "✖︎",
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "json_path": json_path,
                    "rule": str(rule),
                    "value": f"ERROR: {exc}",
                    "ok": "✖︎",
                }
            )

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# command-line interface (largely your original code, just namespaced)
# ─────────────────────────────────────────────────────────────────────────────
def _load_mapping(path: Path) -> Dict:
    with path.open("rt", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _cli():
    p = argparse.ArgumentParser()
    p.add_argument("--mapping", required=True, type=Path)
    p.add_argument("--source", required=True, type=Path)
    p.add_argument(
        "--output",
        type=Path,
        default=Path("-"),
        help="CSV file to write (use '-' for stdout)",
    )
    args = p.parse_args()

    mapping = _load_mapping(args.mapping)
    # for the standalone script we hard-code the handler list
    handlers = [XMLHandler()]

    try:
        rows = build_trace_table(mapping, args.source, handlers)
    except Exception as exc:
        sys.exit(f"❌ {exc}")

    if args.output.name == "-":
        print(tabulate(rows, headers="keys", tablefmt="github"))
    else:
        with args.output.open("wt", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
        print(f"📝 Trace written to {args.output}")


if __name__ == "__main__":
    _cli()