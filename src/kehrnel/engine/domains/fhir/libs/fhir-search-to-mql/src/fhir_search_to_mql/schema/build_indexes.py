"""
Build local indexes under schema/indexes/ for fast resource-config work.

Inputs (repo ``schema/``, outside ``src/``):
  schema/fhir.schema.v5.json | v6.json
  schema/hl7.fhir.r5.search/package/
  schema/hl7.fhir.r6.search/package/

Outputs:
  schema/indexes/resources.r5.json
  schema/indexes/search-parameters.r5.json
  schema/indexes/search-parameters-shipped.r5.json

    python -m fhir_search_to_mql.schema.build_indexes
    python -m fhir_search_to_mql.schema.build_indexes --version R6
"""

from __future__ import annotations

import argparse
import json

import yaml

from fhir_search_to_mql.schema.paths import configs_dir, indexes_dir, schema_data_root
from fhir_search_to_mql.schema.schema_lib import build_resources_index, resources_index_path
from fhir_search_to_mql.schema.search_package_loader import (
    build_search_parameters_index,
    search_parameters_index_path,
    shipped_search_parameters_path,
)


def build_shipped_index(version: str = "R5") -> dict:
    resources: dict = {}
    for path in sorted(configs_dir().glob("*.yaml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        resource = data.get("resource") or path.stem
        params = data.get("search_parameters") or {}
        rows = []
        for code, meta in sorted(params.items()):
            if not isinstance(meta, dict):
                continue
            rows.append({
                "code": code,
                "type": meta.get("type"),
                "description": (meta.get("description") or "")[:160],
            })
        resources[resource] = rows
    return {
        "fhir_version": version.upper(),
        "source": "src/fhir_search_to_mql/configs/*.yaml",
        "resource_count": len(resources),
        "resources": resources,
    }


def _write(path, payload: dict) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path.stat().st_size


def build(version: str = "R5", *, skip_search: bool = False) -> None:
    v = version.upper()
    print(f"Building indexes for {v} (data: {schema_data_root()})...")

    res_path = resources_index_path(v)
    res_payload = build_resources_index(v)
    res_size = _write(res_path, res_payload)
    print(f"  resources: {res_payload['resource_count']} types -> {res_path} ({res_size // 1024} KB)")

    if not skip_search:
        try:
            sp_path = search_parameters_index_path(v)
            sp_payload = build_search_parameters_index(v)
            sp_size = _write(sp_path, sp_payload)
            total_sp = sum(len(x) for x in sp_payload["resources"].values())
            print(
                f"  search-parameters: {sp_payload['resource_count']} resources, "
                f"{total_sp} param rows -> {sp_path} ({sp_size // 1024} KB)"
            )
        except FileNotFoundError as e:
            print(f"  search-parameters: SKIP ({e})")

    shipped_path = shipped_search_parameters_path(v)
    shipped = build_shipped_index(v)
    shipped_size = _write(shipped_path, shipped)
    print(
        f"  shipped: {shipped['resource_count']} configs -> {shipped_path} "
        f"({shipped_size // 1024} KB)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FHIR schema/search indexes")
    parser.add_argument("--version", action="append", dest="versions")
    parser.add_argument("--schema-only", action="store_true")
    args = parser.parse_args()
    versions = [v.upper() for v in (args.versions or ["R5"])]
    indexes_dir().mkdir(parents=True, exist_ok=True)
    for v in versions:
        build(v, skip_search=args.schema_only)
    print("Done.")


if __name__ == "__main__":
    main()
