#!/usr/bin/env python3
"""
Kehrnel FHIR integration spike: fhir-gen generate → MongoDB → fhir-mql denorm → search.

Standalone script for ``fhir.clinical_cdr`` (not imported by the strategy runtime).
Verifies vendored fhir-gen and fhir-mql ([fhir] extra).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

from kehrnel.engine.strategies.fhir.clinical_cdr._paths import FHIR_GEN_ROOT, PACK_ROOT

# fhir_gen Settings loads package-root `.env` only (not kehrnel repo root).
os.chdir(FHIR_GEN_ROOT)

from fhir_gen import ResourceGenerator
from fhir_gen.persistence import FHIRMongoStore
from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from pymongo import MongoClient


def _patient_family_name(patient: dict[str, Any]) -> str | None:
    for name in patient.get("name") or []:
        if isinstance(name, dict):
            family = name.get("family")
            if family:
                return str(family)
    return None


def _execute_mql(
    db, collection_name: str, mql: dict[str, Any], limit: int
) -> list[dict[str, Any]]:
    """Minimal search execution (matches fhir-mql CLI envelope handling)."""
    if isinstance(mql, dict) and "_multi_step" in mql:
        composed = dict(mql.get("_query") or {})
        and_clauses: list[dict[str, Any]] = []
        for step in mql["_multi_step"]:
            step_coll = db[step.get("collection") or collection_name]
            field = step.get("project_field", "_id")
            ids = list(
                step_coll.find(
                    step.get("query") or {},
                    {field: 1, "_id": 0},
                )
            )
            id_values = [d.get(field) for d in ids if d.get(field) is not None]
            target_field = step.get("target_field") or field
            if id_values:
                and_clauses.append({target_field: {"$in": id_values}})
            else:
                and_clauses.append({"_id": {"$in": []}})
        if and_clauses:
            mql = (
                {"$and": [composed] + and_clauses}
                if composed
                else {"$and": and_clauses}
            )
        else:
            mql = composed

    cursor = db[collection_name].find(mql)
    if limit > 0:
        cursor = cursor.limit(limit)
    return list(cursor)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="FHIR gen + mql MongoDB spike for fhir.clinical_cdr"
    )
    parser.add_argument(
        "--uri", default="mongodb://localhost:27017/", help="MongoDB URI"
    )
    parser.add_argument("--db", default="fhir_kehrnel_spike", help="Database name")
    parser.add_argument("--seed", type=int, default=1, help="Generator seed")
    args = parser.parse_args()

    print(
        f"[spike] pack={PACK_ROOT.name} MongoDB {args.uri} db={args.db} seed={args.seed}"
    )

    gen = ResourceGenerator(seed=args.seed)
    patients = gen.generate("Patient", count=2)
    observations = gen.generate("Observation", count=5)
    docs = patients + observations
    print(f"[spike] generated Patient={len(patients)} Observation={len(observations)}")

    store = FHIRMongoStore(uri=args.uri, db_name=args.db)
    store.save_many(docs)
    patient_coll = store.collection_name("Patient")
    print(f"[spike] saved {len(docs)} documents (Patient collection: {patient_coll})")

    family = _patient_family_name(patients[0])
    if not family:
        print(
            "[spike] ERROR: could not read family name from first Patient",
            file=sys.stderr,
        )
        return 1
    print(f"[spike] search target family: {family!r}")

    client = MongoClient(args.uri)
    db = client[args.db]
    collection = db[patient_coll]

    denorm = ResourceDenormalizer()
    denorm.denormalize_from_mongodb(
        collection, query={}, batch_size=50, update_in_place=True
    )
    print("[spike] denormalized Patient in place (_search fields)")

    converter = FHIRSearchConverter()
    query_string = f"name={family}"
    mql = converter.convert("Patient", query_string=query_string)
    results = _execute_mql(db, patient_coll, mql, limit=10)
    print(f"[spike] MQL: {mql}")
    print(f"[spike] search {query_string!r} matched {len(results)} document(s)")
    if results:
        first = results[0]
        print(
            f"[spike] first match id={first.get('id')} resourceType={first.get('resourceType')}"
        )
    else:
        print(
            "[spike] WARNING: zero matches (check denorm / search param config)",
            file=sys.stderr,
        )
        client.close()
        return 1

    client.close()
    print("[spike] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
