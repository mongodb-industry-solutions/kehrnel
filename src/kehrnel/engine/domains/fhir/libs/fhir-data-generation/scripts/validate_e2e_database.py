#!/usr/bin/env python3
"""
Validate FHIR resources stored in an E2E MongoDB database.

Checks per document:
  - resourceType matches collection name; required id/meta
  - enricher fields (MQL 84 shipped set)
  - schema field types on populated paths
  - registered terminology on Coding elements
  - Reference targets exist in the database
  - canonical resources: string publisher, valid versionAlgorithm[x]
  - no empty Coding objects; no duplicate versionAlgorithm choices

Usage:
  python scripts/validate_e2e_database.py --db fhir_e2e_gen_ind_full84
  python scripts/validate_e2e_database.py --uri mongodb://localhost:27017/ --db fhir_e2e_gen_hc01 --json report.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Repo imports
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fhir_gen.codes.validation import validate_resource_codings
from fhir_gen.config import settings
from fhir_gen.generators.canonical_resource import (
    _STRING_PUBLISHER_RESOURCES,
    _plausible_publisher_name,
    _valid_coding,
)
from fhir_gen.resolvers.dependency import MQL_SHIPPED_RESOURCES
from fhir_gen.schema.field_catalog import field_catalog
from fhir_gen.schema.field_validation import validate_resource_fields
from fhir_gen.schema.parser import FHIRSchemaParser
from tests.mql_resource_checks import MQL_ENRICHED_FIELDS, iter_reference_strings

_MONGO_META = frozenset({
    "_id",
    "_search",
    "_stored_at",
    "_compartments",
    "_fhir_resource_type",
})

_CLINICAL_MARKERS = (
    "patient",
    "clinical",
    "medication",
    "assessment",
    "vital",
    "care team",
    "transition-of-care",
)


def _is_clinical_narrative(text: str) -> bool:
    lowered = text.lower()
    return any(m in lowered for m in _CLINICAL_MARKERS) or len(text) > 80


def _iter_codings(node: Any, path: str = "") -> list[tuple[str, dict]]:
    found: list[tuple[str, dict]] = []
    if isinstance(node, dict):
        if "coding" in node and isinstance(node.get("coding"), list):
            for i, c in enumerate(node["coding"]):
                if isinstance(c, dict):
                    found.append((f"{path}.coding[{i}]" if path else f"coding[{i}]", c))
        elif set(node.keys()) >= {"system", "code"} or (
            "code" in node and ("system" in node or "display" in node)
        ):
            if path and not path.endswith("meta"):
                found.append((path, node))
        for k, v in node.items():
            if k.startswith("_") and k != "_id":
                continue
            child = f"{path}.{k}" if path else k
            found.extend(_iter_codings(v, child))
    elif isinstance(node, list):
        for i, item in enumerate(node):
            found.extend(_iter_codings(item, f"{path}[{i}]" if path else f"[{i}]"))
    return found


@dataclass
class DocResult:
    resource_type: str
    doc_id: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class DbReport:
    database: str
    collections: dict[str, dict[str, Any]] = field(default_factory=dict)
    total_docs: int = 0
    total_errors: int = 0
    total_warnings: int = 0

    def add(self, coll: str, result: DocResult) -> None:
        bucket = self.collections.setdefault(
            coll,
            {"documents": 0, "errors": 0, "warnings": 0, "samples": []},
        )
        bucket["documents"] += 1
        self.total_docs += 1
        if result.errors:
            bucket["errors"] += len(result.errors)
            self.total_errors += len(result.errors)
            if len(bucket["samples"]) < 3:
                bucket["samples"].append({
                    "id": result.doc_id,
                    "errors": result.errors[:12],
                })
        if result.warnings:
            bucket["warnings"] += len(result.warnings)
            self.total_warnings += len(result.warnings)


def build_id_index(db) -> dict[str, set[str]]:
    index: dict[str, set[str]] = defaultdict(set)
    for name in db.list_collection_names():
        if name.startswith("system."):
            continue
        for doc in db[name].find({}, {"id": 1}):
            rid = doc.get("id")
            if rid:
                index[name].add(str(rid))
    return dict(index)


def validate_document(
    doc: dict[str, Any],
    *,
    collection: str,
    id_index: dict[str, set[str]],
    parser: FHIRSchemaParser,
    field_specs: dict[str, list],
) -> DocResult:
    rtype = doc.get("resourceType") or collection
    doc_id = str(doc.get("id", "?"))
    result = DocResult(resource_type=rtype, doc_id=doc_id)

    if doc.get("resourceType") != collection:
        result.errors.append(
            f"resourceType={doc.get('resourceType')!r} != collection {collection!r}"
        )
    if not doc.get("id"):
        result.errors.append("missing id")
    if not doc.get("meta"):
        result.warnings.append("missing meta")

    if rtype in MQL_ENRICHED_FIELDS:
        for fname in MQL_ENRICHED_FIELDS[rtype]:
            if fname not in doc:
                result.errors.append(f"missing enriched field {fname!r}")

    # References
    for ref in iter_reference_strings(doc):
        if ref.startswith("#") or ref.startswith("urn:"):
            continue
        if "/" not in ref:
            result.errors.append(f"malformed reference {ref!r}")
            continue
        target_type, target_id = ref.split("/", 1)
        if target_id not in id_index.get(target_type, set()):
            result.errors.append(f"unresolved reference {ref!r}")

    # Terminology
    coding_errors = validate_resource_codings(doc, strict_registered=True)
    for err in coding_errors[:5]:
        result.errors.append(f"coding: {err}")
    if len(coding_errors) > 5:
        result.errors.append(f"coding: (+{len(coding_errors) - 5} more)")

    # Empty / invalid Coding nodes
    for path, coding in _iter_codings(doc):
        if not coding:
            result.errors.append(f"empty Coding at {path}")
        elif not _valid_coding(coding) and coding.keys():
            result.errors.append(f"invalid Coding at {path}: {coding!r}")

    # Canonical metadata
    if rtype in _STRING_PUBLISHER_RESOURCES:
        pub = doc.get("publisher")
        if pub is None:
            result.warnings.append("missing publisher")
        elif isinstance(pub, dict):
            result.errors.append("publisher must be string, not Reference object")
        elif isinstance(pub, str) and not _plausible_publisher_name(pub):
            result.errors.append(f"publisher looks like clinical narrative: {pub[:60]!r}...")

    vac = doc.get("versionAlgorithmCoding")
    vas = doc.get("versionAlgorithmString")
    if vac is not None or vas is not None:
        if isinstance(vac, dict) and not _valid_coding(vac):
            if vac == {}:
                result.errors.append("versionAlgorithmCoding is empty object")
            else:
                result.errors.append(f"versionAlgorithmCoding invalid: {vac!r}")
        if isinstance(vas, str) and _is_clinical_narrative(vas):
            result.errors.append(
                f"versionAlgorithmString is clinical text: {vas[:60]!r}..."
            )
        if vac and vas and _valid_coding(vac) if isinstance(vac, dict) else False:
            result.warnings.append("both versionAlgorithmCoding and versionAlgorithmString set")

    # Schema field types (populated fields only)
    specs = field_specs.get(rtype, [])
    if specs:
        field_errors = validate_resource_fields(
            doc,
            specs,
            parser,
            check_required=False,
            max_required_depth=2,
        )
        for err in field_errors[:6]:
            result.errors.append(f"schema: {err}")
        if len(field_errors) > 6:
            result.errors.append(f"schema: (+{len(field_errors) - 6} more)")

    return result


def validate_database(uri: str, db_name: str) -> DbReport:
    from pymongo import MongoClient

    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    db = client[db_name]
    report = DbReport(database=db_name)

    schema_path = settings.resolved_schema_path
    parser = FHIRSchemaParser(schema_path)
    specs = field_catalog(str(schema_path))

    id_index = build_id_index(db)
    collections = sorted(
        n for n in db.list_collection_names() if not n.startswith("system.")
    )

    for coll in collections:
        if coll not in MQL_SHIPPED_RESOURCES:
            report.collections[coll] = {
                "documents": 0,
                "errors": 0,
                "warnings": 0,
                "samples": [],
                "note": "not in MQL_SHIPPED_RESOURCES",
            }
        for doc in db[coll].find({}):
            res = validate_document(
                doc,
                collection=coll,
                id_index=id_index,
                parser=parser,
                field_specs=specs,
            )
            report.add(coll, res)

    client.close()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate E2E MongoDB FHIR data")
    parser.add_argument("--uri", default="mongodb://localhost:27017/")
    parser.add_argument("--db", default="fhir_e2e_gen_ind_full84")
    parser.add_argument("--json", dest="json_path", help="Write full report JSON")
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit code 1 if any errors",
    )
    args = parser.parse_args()

    report = validate_database(args.uri, args.db)

    failing = {
        coll: data
        for coll, data in report.collections.items()
        if data.get("errors", 0) > 0
    }

    print(f"Database: {report.database}")
    print(f"Documents: {report.total_docs}")
    print(f"Errors: {report.total_errors}")
    print(f"Warnings: {report.total_warnings}")
    print(f"Collections with errors: {len(failing)} / {len(report.collections)}")
    print()

    for coll in sorted(failing, key=lambda c: -failing[c]["errors"]):
        data = failing[coll]
        print(f"  {coll}: {data['errors']} error(s) in {data['documents']} doc(s)")
        for sample in data.get("samples", []):
            print(f"    id={sample['id']}")
            for err in sample["errors"][:5]:
                print(f"      - {err}")

    if args.json_path:
        Path(args.json_path).write_text(
            json.dumps(
                {
                    "database": report.database,
                    "total_docs": report.total_docs,
                    "total_errors": report.total_errors,
                    "total_warnings": report.total_warnings,
                    "collections": report.collections,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"\nWrote {args.json_path}")

    return 1 if args.fail_on_error and report.total_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
