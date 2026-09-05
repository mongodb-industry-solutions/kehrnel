"""Checksum-pinned example catalog and controlled example ingestion."""

from __future__ import annotations

import asyncio
import hashlib
import urllib.request
from pathlib import Path
from typing import Any, Dict

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext

from ..common import ensure_not_cancelled, load_json, report_progress


CATALOG_PATH = Path(__file__).resolve().parent / "catalog.json"
MAX_EXAMPLE_FILE_BYTES = 10_000_000


class ExampleDataService:
    def __init__(self, artifacts, ingestion, validation, projections):
        self.artifacts = artifacts
        self.ingestion = ingestion
        self.validation = validation
        self.projections = projections

    @staticmethod
    def _catalog() -> Dict[str, Any]:
        return load_json(CATALOG_PATH)

    @classmethod
    def _example(cls, example_id: str) -> Dict[str, Any]:
        for example in cls._catalog()["examples"]:
            if example["id"] == example_id:
                return example
        raise KehrnelError(
            code="CDISC_EXAMPLE_NOT_FOUND",
            status=404,
            message=f"Unknown curated CDISC example: {example_id}",
        )

    async def list(self, _ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        profile = str(payload.get("profile") or "").strip().lower()
        examples = self._catalog()["examples"]
        if profile:
            examples = [item for item in examples if item["profile"] == profile]
        return {"ok": True, "catalogVersion": self._catalog()["catalogVersion"], "items": examples}

    async def _fetch(self, ctx: StrategyContext, url: str) -> bytes:
        fetcher = (ctx.adapters or {}).get("example_fetcher")
        if fetcher is not None:
            content = await fetcher.fetch(url)
        else:
            def download() -> bytes:
                request = urllib.request.Request(url, headers={"User-Agent": "kehrnel-cdisc-examples/1"})
                with urllib.request.urlopen(request, timeout=60) as response:
                    length = response.headers.get("Content-Length")
                    if length and int(length) > MAX_EXAMPLE_FILE_BYTES:
                        raise ValueError("example file exceeds the 10 MB safety limit")
                    return response.read(MAX_EXAMPLE_FILE_BYTES + 1)

            try:
                content = await asyncio.to_thread(download)
            except Exception as exc:
                raise KehrnelError(
                    code="CDISC_EXAMPLE_FETCH_FAILED", status=502, message=str(exc)
                ) from exc
        if len(content) > MAX_EXAMPLE_FILE_BYTES:
            raise KehrnelError(
                code="CDISC_EXAMPLE_TOO_LARGE",
                status=413,
                message="Curated example file exceeds the 10 MB safety limit.",
            )
        return content

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        if payload.get("acknowledgeTerms") is not True:
            raise KehrnelError(
                code="CDISC_EXAMPLE_TERMS_REQUIRED",
                status=400,
                message="acknowledgeTerms must be true before fetching external example data.",
            )
        if bool(payload.get("publish", False)) and not bool(payload.get("validate", True)):
            raise KehrnelError(
                code="CDISC_EXAMPLE_PUBLICATION_REQUIRES_VALIDATION",
                status=409,
                message="A curated example must be validated before it can be published.",
            )
        example = self._example(str(payload.get("exampleId") or "").strip())
        snapshot_id = str(payload.get("snapshotId") or "example-v1").strip()
        if not snapshot_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="snapshotId cannot be blank")
        await report_progress(ctx, progress=2, phase="fetching-example")
        stored: Dict[str, Dict[str, Any]] = {}
        for index, item in enumerate(example["files"], start=1):
            ensure_not_cancelled(ctx)
            content = await self._fetch(ctx, item["url"])
            digest = hashlib.sha256(content).hexdigest()
            if digest != item["sha256"]:
                raise KehrnelError(
                    code="CDISC_EXAMPLE_CHECKSUM_MISMATCH",
                    status=502,
                    message=f"Checksum mismatch for curated example file {item['name']}.",
                    details={"expectedSha256": item["sha256"], "actualSha256": digest},
                )
            stored[item["name"]] = await self.artifacts.store(
                ctx,
                content=content,
                media_type=item["mediaType"],
                source_name=item["name"],
                kind=f"example-{item['role']}",
                expected_sha256=item["sha256"],
                metadata={
                    "exampleId": example["id"],
                    "sourceRepository": example["source"]["repository"],
                    "sourceRevision": example["source"]["revision"],
                    "sourceUrl": item["url"],
                },
            )
            await report_progress(
                ctx,
                progress=2 + round(28 * index / len(example["files"])),
                phase="fetching-example",
                stats={"filesCompleted": index, "filesTotal": len(example["files"])},
            )

        define = next(
            (stored[item["name"]]["artifact"]["artifactId"] for item in example["files"] if item["role"] == "define-xml"),
            None,
        )
        datasets = [item for item in example["files"] if item["role"] == "dataset"]
        ingested = []
        for index, item in enumerate(datasets, start=1):
            ingested.append(await self.ingestion.ingest_xpt(ctx, {
                "xptArtifactId": stored[item["name"]]["artifact"]["artifactId"],
                "defineArtifactId": define,
                "studyOID": example.get("studyId"),
                "packageId": example["packageId"],
                "snapshotId": snapshot_id,
                "standardsPackageId": example["id"],
                "profile": example["profile"],
                "standard": example["standard"],
                "publicationState": "staged",
            }))
            await report_progress(
                ctx,
                progress=30 + round(40 * index / len(datasets)),
                phase="ingesting-example",
                stats={"datasetsCompleted": index, "datasetsTotal": len(datasets)},
            )

        validation = None
        if bool(payload.get("validate", True)):
            validation = await self.validation.validate_snapshot(ctx, {
                "studyId": example["studyId"], "snapshotId": snapshot_id,
            })
        publication = None
        if bool(payload.get("publish", False)):
            if validation is not None and not validation["ok"]:
                publication = {"state": "blocked", "reason": "validation_failed"}
            else:
                publication = await self.ingestion.publish(ctx, {
                    "studyId": example["studyId"], "snapshotId": snapshot_id,
                })
                publication["projections"] = await self.projections.rebuild(ctx, {
                    "studyId": example["studyId"], "snapshotId": snapshot_id,
                })
        await report_progress(ctx, progress=100, phase="completed")
        return {
            "ok": validation is None or validation["ok"],
            "exampleId": example["id"],
            "profile": example["profile"],
            "studyId": example["studyId"],
            "snapshotId": snapshot_id,
            "artifacts": [value["artifact"] for value in stored.values()],
            "ingested": ingested,
            "validation": validation,
            "publication": publication,
            "source": example["source"],
        }
