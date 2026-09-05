#!/usr/bin/env python3
"""Export a curated CDISC example as a verified solution-evidence package.

This developer utility exercises the same strategy operations as a deployed
Kehrnel instance while keeping its transient repository and artifacts local.
It is intended for solution-library development and integration tests, not as
an alternative ingestion implementation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any, Dict

from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.cdisc.sdr.strategy import CDISCSDRStrategy
from kehrnel.persistence.artifacts.filesystem import FileSystemArtifactStore


ROOT = Path(__file__).resolve().parents[1]
PACK = ROOT / "src" / "kehrnel" / "engine" / "strategies" / "cdisc" / "sdr"


class TransientStorage:
    """Minimal strategy storage adapter for a single local export run."""

    def __init__(self) -> None:
        self.data: Dict[str, Dict[str, Dict[str, Any]]] = {}

    async def replace_many(self, collection: str, documents) -> None:
        target = self.data.setdefault(collection, {})
        for document in documents:
            target[document["_id"]] = document

    async def find_one(
        self,
        collection: str,
        predicate: Dict[str, Any],
        projection: Dict[str, Any] | None = None,
    ) -> Dict[str, Any] | None:
        for document in self.data.get(collection, {}).values():
            if self._matches(document, predicate):
                return document
        return None

    async def aggregate(
        self,
        collection: str,
        pipeline: list[Dict[str, Any]],
        allow_disk_use: bool = True,
    ) -> list[Dict[str, Any]]:
        del allow_disk_use
        documents = list(self.data.get(collection, {}).values())
        for stage in pipeline:
            if "$match" in stage:
                documents = [
                    document
                    for document in documents
                    if self._matches(document, stage["$match"])
                ]
            elif "$sort" in stage:
                for path, direction in reversed(list(stage["$sort"].items())):
                    documents.sort(
                        key=lambda document: self._sortable(self._value_at(document, path)),
                        reverse=direction < 0,
                    )
            elif "$skip" in stage:
                documents = documents[stage["$skip"] :]
            elif "$limit" in stage:
                documents = documents[: stage["$limit"]]
            else:
                raise ValueError(f"Transient storage does not support stage: {stage}")
        return documents

    @classmethod
    def _value_at(cls, document: Dict[str, Any], path: str) -> Any:
        value: Any = document
        for part in path.split("."):
            if isinstance(value, list):
                value = [item.get(part) for item in value if isinstance(item, dict)]
            elif isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    @staticmethod
    def _sortable(value: Any) -> tuple[bool, str]:
        return value is None, "" if value is None else str(value)

    @classmethod
    def _matches(cls, document: Dict[str, Any], expression: Dict[str, Any]) -> bool:
        if "$and" in expression:
            return all(cls._matches(document, item) for item in expression["$and"])
        for path, expected in expression.items():
            actual = cls._value_at(document, path)
            if isinstance(expected, dict):
                if "$in" in expected and actual not in expected["$in"]:
                    return False
            elif isinstance(actual, list):
                if expected not in actual:
                    return False
            elif actual != expected:
                return False
        return True


async def export_example(example_id: str, snapshot_id: str, output: Path) -> dict:
    storage = TransientStorage()
    with tempfile.TemporaryDirectory(prefix="kehrnel-cdisc-example-") as temporary:
        artifact_store = FileSystemArtifactStore(Path(temporary) / "artifacts")
        manifest = load_strategy("cdisc.sdr", PACK)
        context = StrategyContext(
            environment_id="local-example-export",
            config={
                **manifest.default_config,
                "tenant_id": "local-example-export",
                "validation": {"require_before_publish": True},
            },
            adapters={"storage": storage, "artifact_store": artifact_store},
            manifest=manifest,
            meta={},
        )
        strategy = CDISCSDRStrategy()
        ingested = await strategy.run_op(
            context,
            "cdisc_ingest_example",
            {
                "exampleId": example_id,
                "snapshotId": snapshot_id,
                "acknowledgeTerms": True,
                "validate": True,
                "publish": True,
            },
        )
        if not ingested["ok"] or ingested["publication"]["state"] != "published":
            raise RuntimeError("Curated example did not validate and publish")
        exported = await strategy.run_op(
            context,
            "cdisc_export_solution_evidence",
            {"studyId": ingested["studyId"], "snapshotId": snapshot_id},
        )
        content = await artifact_store.get(exported["artifact"]["objectKey"])
        package = json.loads(content)
        if package["manifest"]["packageId"] != exported["packageId"]:
            raise RuntimeError("Export response and package identity do not match")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(content)
        return {
            "exampleId": example_id,
            "studyId": ingested["studyId"],
            "snapshotId": snapshot_id,
            "packageId": exported["packageId"],
            "counts": exported["counts"],
            "output": str(output.resolve()),
            "bytes": len(content),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("example_id", help="Curated example id from cdisc_list_examples")
    parser.add_argument("output", type=Path, help="Output solution-evidence JSON path")
    parser.add_argument("--snapshot-id", default="solution-library-v1")
    parser.add_argument(
        "--acknowledge-terms",
        action="store_true",
        help="Required acknowledgement of the source terms linked by the curated catalog",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.acknowledge_terms:
        raise SystemExit("--acknowledge-terms is required")
    result = asyncio.run(export_example(args.example_id, args.snapshot_id, args.output))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
