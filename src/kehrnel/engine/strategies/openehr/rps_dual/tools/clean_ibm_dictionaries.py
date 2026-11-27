"""Clean IBM RPS-dual dictionary exports and emit Kehrnel-friendly seeds.

This helper is aimed at IBM-style exports shaped like:

- Archetype codes: [{ "_id": "25", "archetypeId": "openEHR-EHR-CLUSTER.admin_salut.v0" }, ...]
- Coded values:    [{ "_id": "dct", "value": "DV_CODED_TEXT" }, ...]

The archetype export may contain corrupted rows where SQL / command injection
payloads were appended to an otherwise valid archetype id. We recover the
canonical archetype id prefix, choose the best code per archetype, and emit:

- a cleaned IBM-style list export
- a Kehrnel `_codes` single-document seed
- a Kehrnel `_shortcuts` single-document seed merged with bundled key shortcuts

Example:

    .venv/bin/python src/kehrnel/engine/strategies/openehr/rps_dual/tools/clean_ibm_dictionaries.py \
      --archetypes /Users/me/Downloads/cdr-informational-ibm.ibm-ArchetypeCodes.json \
      --coded-values /Users/me/Downloads/cdr-informational-ibm.ibm-CodedValues.json \
      --out-dir /tmp/ibm-clean
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ARCHETYPE_PREFIX_RE = re.compile(
    r"^(openEHR-EHR-[A-Z_]+\.[A-Za-z0-9_\-]+\.(?:v[0-9]+))"
)


@dataclass(frozen=True)
class ArchetypeCandidate:
    code: str
    raw_archetype_id: str
    canonical_archetype_id: str
    exact_match: bool


def _load_json_array(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{path} must contain a JSON array of objects")
    rows = [row for row in payload if isinstance(row, dict)]
    if len(rows) != len(payload):
        raise ValueError(f"{path} contains non-object items")
    return rows


def _extract_canonical_archetype_id(raw: str) -> str | None:
    if not isinstance(raw, str):
        return None
    match = ARCHETYPE_PREFIX_RE.match(raw.strip())
    if not match:
        return None
    return match.group(1)


def _numeric_code(code: str) -> int:
    try:
        return int(str(code))
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"Expected numeric archetype code, got {code!r}") from exc


def _candidate_sort_key(candidate: ArchetypeCandidate) -> Tuple[int, int, int, str]:
    return (
        0 if candidate.exact_match else 1,
        len(candidate.code),
        _numeric_code(candidate.code),
        candidate.code,
    )


def _build_archetype_candidates(rows: Iterable[Dict[str, Any]]) -> Tuple[List[ArchetypeCandidate], List[Dict[str, Any]]]:
    candidates: List[ArchetypeCandidate] = []
    invalid_rows: List[Dict[str, Any]] = []

    for row in rows:
        raw_code = row.get("_id")
        raw_archetype_id = row.get("archetypeId")
        if raw_code is None or raw_archetype_id is None:
            invalid_rows.append(row)
            continue

        code = str(raw_code).strip()
        archetype_id = str(raw_archetype_id).strip()
        canonical = _extract_canonical_archetype_id(archetype_id)
        if not code or not code.isdigit() or canonical is None:
            invalid_rows.append(row)
            continue

        candidates.append(
            ArchetypeCandidate(
                code=code,
                raw_archetype_id=archetype_id,
                canonical_archetype_id=canonical,
                exact_match=(archetype_id == canonical),
            )
        )

    return candidates, invalid_rows


def clean_archetype_codes(rows: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    candidates, invalid_rows = _build_archetype_candidates(rows)
    grouped: Dict[str, List[ArchetypeCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate.canonical_archetype_id].append(candidate)

    cleaned_rows: List[Dict[str, str]] = []
    recovered_from_prefix = 0
    candidate_count_by_archetype: Dict[str, int] = {}

    for archetype_id in sorted(grouped):
        bucket = sorted(grouped[archetype_id], key=_candidate_sort_key)
        chosen = bucket[0]
        candidate_count_by_archetype[archetype_id] = len(bucket)
        if not chosen.exact_match:
            recovered_from_prefix += 1
        cleaned_rows.append({"_id": chosen.code, "archetypeId": archetype_id})

    code_to_archetype: Dict[str, str] = {}
    code_collisions: Dict[str, List[str]] = defaultdict(list)
    for row in cleaned_rows:
        code = row["_id"]
        archetype_id = row["archetypeId"]
        existing = code_to_archetype.get(code)
        if existing is None:
            code_to_archetype[code] = archetype_id
            continue
        if existing != archetype_id:
            code_collisions[code].extend([existing, archetype_id])

    deduped_collisions = {
        code: sorted(set(archetypes))
        for code, archetypes in code_collisions.items()
    }

    report = {
        "input_rows": len(list(rows)) if isinstance(rows, list) else None,
        "candidate_rows": len(candidates),
        "invalid_rows": len(invalid_rows),
        "unique_archetypes": len(cleaned_rows),
        "recovered_from_prefix_only": recovered_from_prefix,
        "multi_candidate_archetypes": sum(1 for count in candidate_count_by_archetype.values() if count > 1),
        "code_collisions": deduped_collisions,
    }
    return cleaned_rows, report


def _build_nested_codes_doc(cleaned_rows: Iterable[Dict[str, str]], *, doc_id: str = "ar_code") -> Dict[str, Any]:
    nested: Dict[str, Any] = {"_id": doc_id, "at": {}}
    max_code = 0

    for row in cleaned_rows:
        code = str(row["_id"])
        archetype_id = row["archetypeId"]
        parts = archetype_id.split(".")
        if len(parts) != 3:
            raise ValueError(f"Unexpected archetype id format: {archetype_id}")
        rm_type, name, version = parts
        nested.setdefault(rm_type, {}).setdefault(name, {})[version] = code
        max_code = max(max_code, _numeric_code(code))

    nested["_max"] = max_code
    return nested


def _load_bundled_shortcut_keys() -> Dict[str, str]:
    here = Path(__file__).resolve()
    shortcuts_path = here.parents[1] / "bundles" / "shortcuts" / "shortcuts.json"
    payload = json.loads(shortcuts_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected shortcuts payload in {shortcuts_path}")
    keys = payload.get("keys")
    if not isinstance(keys, dict):
        raise ValueError(f"Expected shortcuts.keys object in {shortcuts_path}")
    return {str(k): str(v) for k, v in keys.items()}


def build_shortcuts_doc(
    coded_value_rows: Iterable[Dict[str, Any]],
    *,
    doc_id: str = "shortcuts",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    keys = _load_bundled_shortcut_keys()
    values: Dict[str, str] = {}
    duplicate_codes: Dict[str, List[str]] = defaultdict(list)
    duplicate_values: Dict[str, List[str]] = defaultdict(list)

    for row in coded_value_rows:
        raw_code = row.get("_id")
        raw_value = row.get("value")
        if raw_code is None or raw_value is None:
            continue
        code = str(raw_code).strip()
        value = str(raw_value).strip()
        if not code or not value:
            continue

        existing_code = values.get(value)
        if existing_code is not None and existing_code != code:
            duplicate_values[value].extend([existing_code, code])
            continue

        for known_value, known_code in values.items():
            if known_code == code and known_value != value:
                duplicate_codes[code].extend([known_value, value])

        values[value] = code

    report = {
        "input_rows": len(list(coded_value_rows)) if isinstance(coded_value_rows, list) else None,
        "unique_values": len(values),
        "duplicate_codes": {code: sorted(set(items)) for code, items in duplicate_codes.items()},
        "duplicate_values": {value: sorted(set(items)) for value, items in duplicate_values.items()},
    }
    return {"_id": doc_id, "keys": keys, "values": values}, report


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archetypes", required=True, help="Path to IBM archetype-code export JSON")
    parser.add_argument("--coded-values", required=True, help="Path to IBM coded-values export JSON")
    parser.add_argument("--out-dir", required=True, help="Directory where cleaned outputs will be written")
    parser.add_argument("--codes-doc-id", default="ar_code", help="Doc id for the emitted Kehrnel codes seed")
    parser.add_argument("--shortcuts-doc-id", default="shortcuts", help="Doc id for the emitted Kehrnel shortcuts seed")
    return parser


def main(argv: List[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    archetypes_path = Path(args.archetypes).expanduser().resolve()
    coded_values_path = Path(args.coded_values).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()

    archetype_rows = _load_json_array(archetypes_path)
    coded_value_rows = _load_json_array(coded_values_path)

    cleaned_archetypes, archetype_report = clean_archetype_codes(archetype_rows)
    if archetype_report["code_collisions"]:
        print("Refusing to emit ambiguous codes: multiple archetypes resolved to the same code.", file=sys.stderr)
        print(json.dumps(archetype_report["code_collisions"], indent=2), file=sys.stderr)
        return 2

    codes_doc = _build_nested_codes_doc(cleaned_archetypes, doc_id=args.codes_doc_id)
    shortcuts_doc, shortcuts_report = build_shortcuts_doc(
        coded_value_rows,
        doc_id=args.shortcuts_doc_id,
    )

    _write_json(out_dir / "ibm_archetype_codes.cleaned.list.json", cleaned_archetypes)
    _write_json(out_dir / "kehrnel_codes.seed.json", codes_doc)
    _write_json(out_dir / "kehrnel_shortcuts.seed.json", shortcuts_doc)
    _write_json(
        out_dir / "report.json",
        {
            "archetypes": archetype_report,
            "coded_values": shortcuts_report,
            "outputs": {
                "cleaned_archetypes": str(out_dir / "ibm_archetype_codes.cleaned.list.json"),
                "codes_seed": str(out_dir / "kehrnel_codes.seed.json"),
                "shortcuts_seed": str(out_dir / "kehrnel_shortcuts.seed.json"),
            },
        },
    )

    print(f"Wrote cleaned outputs to {out_dir}")
    print(f"  archetypes: {out_dir / 'ibm_archetype_codes.cleaned.list.json'}")
    print(f"  codes seed: {out_dir / 'kehrnel_codes.seed.json'}")
    print(f"  shortcuts:  {out_dir / 'kehrnel_shortcuts.seed.json'}")
    print("Summary:")
    print(f"  archetype input rows: {len(archetype_rows)}")
    print(f"  archetype unique ids: {archetype_report['unique_archetypes']}")
    print(f"  archetype invalid rows: {archetype_report['invalid_rows']}")
    print(f"  archetype multi-candidate ids: {archetype_report['multi_candidate_archetypes']}")
    print(f"  coded values rows: {len(coded_value_rows)}")
    print(f"  coded values unique values: {shortcuts_report['unique_values']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
