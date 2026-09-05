"""Kernel-facing facade for the modular CDISC Study Data Repository."""
from __future__ import annotations

from typing import Any, Dict

import jsonschema

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.plugin import StrategyPlugin
from kehrnel.engine.core.types import ApplyPlan, ApplyResult, QueryPlan, QueryResult, StrategyContext, TransformResult

from .artifacts import ArtifactService
from .analysis import AnalysisService
from .assistant import AssistantService
from .common import (
    DEFAULTS_PATH,
    MANIFEST_PATH,
    SCHEMA_PATH,
    collections,
    config,
    deep_merge,
    ensure_not_cancelled,
    load_json,
    report_progress,
)
from .export import ExportService
from .examples import ExampleDataService
from .ingest import IngestionService
from .lineage import LineageService
from .packages import PackageService
from .query import QueryService
from .repository import RepositoryService
from .projections import ProjectionService
from .synthetic import SyntheticStudyGenerator
from .validation import ValidationService

MANIFEST = StrategyManifest(**load_json(MANIFEST_PATH))


class CDISCSDRStrategy(StrategyPlugin):
    """Thin orchestrator over focused ingestion, query, export, and generation services."""

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        self.manifest = manifest
        self.manifest.default_config = load_json(DEFAULTS_PATH)
        self.manifest.config_schema = load_json(SCHEMA_PATH)
        self.artifacts = ArtifactService()
        self.ingestion = IngestionService(self.artifacts)
        self.query = QueryService()
        self.analysis = AnalysisService()
        self.repository = RepositoryService()
        self.projections = ProjectionService()
        self.exports = ExportService(self.artifacts)
        self.validation = ValidationService()
        self.examples = ExampleDataService(
            self.artifacts, self.ingestion, self.validation, self.projections
        )
        self.synthetic = SyntheticStudyGenerator()
        self.lineage = LineageService()
        self.assistant = AssistantService(
            analysis=self.analysis,
            query=self.query,
            repository=self.repository,
            validation=self.validation,
            lineage=self.lineage,
        )
        self.packages = PackageService(
            self.artifacts, self.ingestion, self.exports, self.validation, self.projections
        )

    async def validate_config(self, ctx: StrategyContext | Dict[str, Any]) -> None:
        raw = ctx.config if isinstance(ctx, StrategyContext) else ctx
        cfg = deep_merge(load_json(DEFAULTS_PATH), raw or {})
        configured_collections = collections(cfg)
        duplicate_collections = sorted({
            name for name in configured_collections.values()
            if list(configured_collections.values()).count(name) > 1
        })
        if duplicate_collections:
            raise KehrnelError(
                code="CONFIG_INVALID",
                status=400,
                message="CDISC collection names must be distinct.",
                details={"duplicateCollections": duplicate_collections},
            )
        if not str(cfg.get("tenant_id") or "").strip():
            raise KehrnelError(code="CONFIG_INVALID", status=400, message="tenant_id is required")
        query_cfg = cfg.get("query") or {}
        if int(query_cfg.get("default_limit", 100)) > int(query_cfg.get("max_limit", 1000)):
            raise KehrnelError(code="CONFIG_INVALID", status=400, message="query.default_limit cannot exceed query.max_limit")
        invalid = [
            path for path in query_cfg.get("extra_allowed_paths", [])
            if not isinstance(path, str)
            or not path.startswith(("facets.", "semantic."))
            or any(part.startswith("$") for part in path.split("."))
        ]
        if invalid:
            raise KehrnelError(code="CONFIG_INVALID", status=400, message="Invalid query extension paths.", details={"invalidPaths": invalid})
        semantic_cfg = cfg.get("semantic") or {}
        if semantic_cfg.get("embed_on_rebuild") and not semantic_cfg.get("enabled"):
            raise KehrnelError(
                code="CONFIG_INVALID",
                status=400,
                message="semantic.embed_on_rebuild requires semantic.enabled.",
            )
        if not ((cfg.get("validation") or {}).get("blocking_severities") or []):
            raise KehrnelError(
                code="CONFIG_INVALID",
                status=400,
                message="validation.blocking_severities must contain at least one severity.",
            )

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        cfg = config(ctx)
        coll = collections(cfg)
        semantic = cfg.get("semantic") or {}
        specs = [
            ("records", [("tenantId", 1), ("snapshotRef", 1), ("datasetId", 1), ("rowOrdinal", 1)], "idx_cdisc_dataset_order", False),
            ("records", [("tenantId", 1), ("studyId", 1), ("facets.subjectId", 1), ("domain", 1), ("facets.studyDay", 1)], "idx_cdisc_study_subject", False),
            ("records", [("tenantId", 1), ("profile", 1), ("domain", 1), ("facets.testCode", 1)], "idx_cdisc_profile_domain_test", False),
            ("entities", [("tenantId", 1), ("snapshotRef", 1), ("entityType", 1), ("entityId", 1)], "idx_cdisc_entity", True),
            ("materializations", [("tenantId", 1), ("snapshotRef", 1), ("kind", 1), ("groupId", 1)], "idx_cdisc_materialization", False),
            ("datasets", [("tenantId", 1), ("studyId", 1), ("snapshotId", 1), ("domain", 1)], "idx_cdisc_dataset_identity", True),
            ("snapshots", [("tenantId", 1), ("studyId", 1), ("snapshotId", 1), ("state", 1)], "idx_cdisc_snapshot_state", False),
            ("artifacts", [("tenantId", 1), ("digest.value", 1)], "idx_cdisc_artifact_digest", True),
            ("validation_runs", [("tenantId", 1), ("snapshotRef", 1), ("completedAt", -1)], "idx_cdisc_validation_run", False),
            ("validation_findings", [("tenantId", 1), ("snapshotRef", 1), ("severity", 1)], "idx_cdisc_finding", False),
            ("validation_waivers", [("tenantId", 1), ("ruleId", 1), ("expiresAt", 1)], "idx_cdisc_waiver", False),
            ("transformations", [("tenantId", 1), ("inputRefs", 1), ("completedAt", -1)], "idx_cdisc_transformation_input", False),
        ]
        return ApplyPlan(artifacts={
            "collections": list(dict.fromkeys(coll.values())),
            "indexes": [
                {"collection": coll[key], "keys": keys, "options": {"name": name, **({"unique": True} if unique else {})}}
                for key, keys, name, unique in specs
            ],
            "search_indexes": [{
                "collection": coll["records"],
                "name": semantic.get("lexical_index", "cdisc_semantic_text"),
                "definition": {"mappings": {"dynamic": False, "fields": {"semantic": {"type": "document", "fields": {"text": {"type": "string"}}}}}},
            }] if semantic.get("enabled", True) else [],
            "vector_indexes": [{
                "collection": coll["records"],
                "name": semantic.get("vector_index", "cdisc_semantic_vector"),
                "definition": {"fields": [
                    {"type": "vector", "path": "semantic.vector", "numDimensions": int(semantic.get("embedding_dimensions", 1536)), "similarity": "cosine"},
                    {"type": "filter", "path": "tenantId"}, {"type": "filter", "path": "snapshotRef"},
                    {"type": "filter", "path": "studyId"}, {"type": "filter", "path": "profile"}, {"type": "filter", "path": "domain"},
                ]},
            }] if semantic.get("enabled", True) and semantic.get("embed_on_rebuild", False) else [],
        })

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        admin = (ctx.adapters or {}).get("index_admin")
        if admin is None:
            return ApplyResult(warnings=["index_admin adapter not available"])
        artifacts = plan.artifacts if isinstance(plan, ApplyPlan) else (plan or {}).get("artifacts", {})
        created, warnings = [], []
        for collection in artifacts.get("collections", []):
            await admin.ensure_collection(collection)
            created.append(collection)
        for index in artifacts.get("indexes", []):
            result = await admin.ensure_indexes(index["collection"], [{"keys": index["keys"], "options": index["options"]}])
            warnings.extend(result.get("warnings", []))
        search = (ctx.adapters or {}).get("atlas_search")
        if search is None and (artifacts.get("search_indexes") or artifacts.get("vector_indexes")):
            warnings.append("atlas_search adapter not available; semantic indexes were not applied")
        elif search is not None:
            for index in artifacts.get("search_indexes", []):
                result = await search.ensure_search_index(index["collection"], index["name"], index["definition"])
                created.extend(result.get("created", []))
                warnings.extend(result.get("warnings", []))
            for index in artifacts.get("vector_indexes", []):
                result = await search.ensure_vector_index(index["collection"], index["name"], index["definition"])
                created.extend(result.get("created", []))
                warnings.extend(result.get("warnings", []))
        return ApplyResult(created=created, warnings=warnings)

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        return await self.ingestion.transform(ctx, payload)

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = await self.ingestion.ingest(ctx, payload)
        if (
            result.get("publicationState") == "published"
            and bool((config(ctx).get("projections") or {}).get("rebuild_on_publish", True))
        ):
            result["projections"] = await self.projections.rebuild(
                ctx, {"studyId": result["studyId"], "snapshotId": result["snapshotId"]}
            )
        return result

    async def reverse_transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        return await self.exports.transform(ctx, payload)

    async def compile_query(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        return await self.query.compile(ctx, domain, query)

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        return await self.query.execute(ctx, plan)

    async def generate(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        ensure_not_cancelled(ctx)
        if bool(payload.get("publish", False)) and (
            not bool(payload.get("persist", False)) or not bool(payload.get("validate", True))
        ):
            raise KehrnelError(
                code="CDISC_SYNTHETIC_PUBLICATION_REQUIRES_VALIDATION",
                status=409,
                message="Synthetic publication requires persistence and validation.",
            )
        await report_progress(ctx, progress=5, phase="generating")
        recipe_payload = payload.get("recipe")
        if not isinstance(recipe_payload, dict):
            recipe_payload = {
                key: payload[key]
                for key in ("studyId", "profile", "subjects", "seed", "domains", "anomalyRate")
                if key in payload
            }
        generated = self.synthetic.generate(recipe_payload)
        profile = str(generated["recipe"].get("profile") or "sdtm")
        standard_defaults = {
            "sdtm": {"family": "SDTM", "implementationGuide": "SDTMIG", "implementationGuideVersion": "3.4"},
            "send": {"family": "SEND", "implementationGuide": "SENDIG", "implementationGuideVersion": "3.1.1"},
            "adam": {"family": "ADaM", "implementationGuide": "ADaMIG", "implementationGuideVersion": "1.3"},
            "tig": {"family": "TIG", "implementationGuide": "TIG", "implementationGuideVersion": "1.0"},
        }
        standard = payload.get("standard") or standard_defaults[profile]
        generated["modelSource"]["standard"] = standard
        generated["modelSource"]["standardsPackageId"] = payload.get("standardsPackageId")
        await report_progress(
            ctx,
            progress=25,
            phase="generated",
            stats={"datasets": len(generated["datasets"]), "profile": generated["recipe"]["profile"]},
        )
        if not bool(payload.get("persist", False)):
            await report_progress(ctx, progress=100, phase="completed")
            return {"ok": True, **generated}
        recipe = generated["recipe"]
        package_id = str(payload.get("packageId") or f"synthetic:{generated['recipeDigest'][-16:]}")
        snapshot_id = str(payload.get("snapshotId") or "synthetic-v1")
        ingested = []
        documents = list(generated["datasets"].values())
        for index, document in enumerate(documents, start=1):
            ensure_not_cancelled(ctx)
            ingested.append(await self.ingestion.ingest(ctx, {
                "datasetJSON": document, "packageId": package_id, "snapshotId": snapshot_id,
                "standardsPackageId": payload.get("standardsPackageId") or package_id,
                "profile": profile, "standard": standard, "publicationState": "staged",
            }))
            await report_progress(
                ctx,
                progress=25 + round(45 * index / len(documents)),
                phase="ingesting",
                stats={"datasetsCompleted": index, "datasetsTotal": len(documents)},
            )
        ensure_not_cancelled(ctx)
        validation = await self.validation.validate_snapshot(
            ctx, {"studyId": recipe["studyId"], "snapshotId": snapshot_id}
        ) if bool(payload.get("validate", True)) else None
        await report_progress(ctx, progress=85, phase="validated" if validation is not None else "validation-skipped")
        publication = None
        if bool(payload.get("publish", False)):
            if validation is not None and not validation["ok"]:
                publication = {"state": "blocked", "reason": "validation_failed"}
            else:
                publication = await self._publish_snapshot(
                    ctx, {"studyId": recipe["studyId"], "snapshotId": snapshot_id}
                )
        await report_progress(ctx, progress=100, phase="completed")
        response = {
            "ok": validation is None or validation["ok"], "synthetic": True,
            "recipe": recipe, "recipeDigest": generated["recipeDigest"],
            "generatorVersion": generated["generatorVersion"],
            "modelSource": generated["modelSource"], "watermark": generated["watermark"],
            "expectedAnomalies": generated["expectedAnomalies"],
            "expectedSignals": generated["expectedSignals"],
            "ingested": ingested, "validation": validation, "publication": publication,
        }
        if payload.get("includeDocuments"):
            response["datasets"] = generated["datasets"]
        return response

    async def _publish_snapshot(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        published = await self.ingestion.publish(ctx, payload)
        if bool((config(ctx).get("projections") or {}).get("rebuild_on_publish", True)):
            published["projections"] = await self.projections.rebuild(ctx, payload)
        return published

    async def _ingest_xpt(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = await self.ingestion.ingest_xpt(ctx, payload)
        if (
            result.get("publicationState") == "published"
            and bool((config(ctx).get("projections") or {}).get("rebuild_on_publish", True))
        ):
            result["projections"] = await self.projections.rebuild(
                ctx, {"studyId": result["studyId"], "snapshotId": result["snapshotId"]}
            )
        return result

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        handlers = {
            "cdisc_store_artifact": self.artifacts.store_base64,
            "cdisc_initiate_upload": self.artifacts.initiate_upload,
            "cdisc_finalize_upload": self.artifacts.finalize_upload,
            "cdisc_prepare_download": self.artifacts.prepare_download,
            "cdisc_register_external_artifact": self.artifacts.register_external,
            "cdisc_replay_artifact": self.artifacts.replay_base64,
            "cdisc_inspect_dataset_json": lambda _ctx, value: self.ingestion.inspect_dataset_json(value),
            "cdisc_ingest_dataset_json": self.ingest,
            "cdisc_inspect_define_xml": self.ingestion.inspect_define,
            "cdisc_ingest_xpt": self._ingest_xpt,
            "cdisc_publish_snapshot": self._publish_snapshot,
            "cdisc_rebuild_projections": self.projections.rebuild,
            "cdisc_browse_projections": self.projections.browse,
            "cdisc_validate_snapshot": self.validation.validate_snapshot,
            "cdisc_get_validation_run": self.validation.get_run,
            "cdisc_list_studies": self.repository.list_studies,
            "cdisc_list_snapshots": self.repository.list_snapshots,
            "cdisc_snapshot_summary": self.repository.snapshot_summary,
            "cdisc_list_datasets": self.repository.list_datasets,
            "cdisc_list_validation_runs": self.repository.list_validation_runs,
            "cdisc_list_standards": self.repository.list_standards,
            "cdisc_list_artifacts": self.repository.list_artifacts,
            "cdisc_list_examples": self.examples.list,
            "cdisc_ingest_example": self.examples.ingest,
            "cdisc_register_validation_waiver": self.validation.register_waiver,
            "cdisc_export_dataset_json": self.exports.export,
            "cdisc_export_xpt": self.exports.export_xpt,
            "cdisc_ingest_package": self.packages.ingest_package,
            "cdisc_export_package": self.packages.export_package,
            "cdisc_export_solution_evidence": self.packages.export_solution_evidence,
            "cdisc_register_standards": self.ingestion.register_standards,
            "cdisc_get_standards_package": self.ingestion.get_standards,
            "cdisc_generate_synthetic_study": self.generate,
            "cdisc_semantic_search": self.query.search,
            "cdisc_run_analysis": self.analysis.run,
            "cdisc_ask_assistant": self.assistant.ask,
            "cdisc_get_lineage": self.lineage.inspect,
            "cdisc_record_transformation": self.lineage.record,
            "cdisc_supersede_snapshot": self.lineage.supersede,
        }
        handler = handlers.get(op)
        if handler is None:
            raise KehrnelError(code="OP_NOT_SUPPORTED", status=400, message=f"Unsupported CDISC op: {op}")
        result = await handler(ctx, payload)
        contract = next((item.output_schema for item in self.manifest.ops if item.name == op), None)
        if contract:
            try:
                jsonschema.Draft7Validator(contract).validate(result)
            except jsonschema.ValidationError as exc:
                raise KehrnelError(
                    code="CDISC_OUTPUT_CONTRACT_VIOLATION",
                    status=500,
                    message=f"Operation {op} returned an invalid response.",
                    details={"path": list(exc.absolute_path), "reason": exc.message},
                ) from exc
        return result
