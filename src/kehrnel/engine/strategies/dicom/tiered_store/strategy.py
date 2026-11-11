from __future__ import annotations

import asyncio
import jsonschema

from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.plugin import StrategyPlugin
from kehrnel.engine.core.types import ApplyPlan, ApplyResult, TransformResult

from . import catalog, headers, model, playground, query, synthetic
from .common import ROOT, check_cancel, config, fail, storage, version, write
from .query import compile_query as compile_request


class DICOMTieredStoreStrategy(StrategyPlugin):
    def __init__(self, manifest=None):
        self.manifest = manifest or load_strategy("dicom.tiered_store", ROOT)

    async def validate_config(self, ctx):
        config(ctx)

    async def plan(self, ctx):
        cfg = config(ctx)
        prefix = [("_kehrnel.tenantId", 1), ("_kehrnel.centerId", 1)]
        definitions = [
            ("studies", "idx_dicom_study_patient_day", prefix + [("_kehrnel.patientId", 1), ("_kehrnel.day", 1), ("_id", 1)]),
            ("studies", "idx_dicom_study_day", prefix + [("_kehrnel.day", 1), ("_id", 1)]),
            ("studies", "idx_dicom_study_description_day", prefix + [("_kehrnel.description", 1), ("_kehrnel.day", 1), ("_id", 1)]),
            ("studies", "idx_dicom_study_modality_day", prefix + [("_kehrnel.seriesFacets.modality", 1), ("_kehrnel.day", 1), ("_id", 1)]),
            ("series", "idx_dicom_series_study", prefix + [("_kehrnel.studyUid", 1), ("_kehrnel.day", 1), ("_id", 1)]),
            ("series", "idx_dicom_series_modality_day", prefix + [("_kehrnel.modality", 1), ("_kehrnel.day", 1), ("_id", 1)]),
            ("series", "idx_dicom_series_patient_day", prefix + [("_kehrnel.patientId", 1), ("_kehrnel.day", 1), ("_id", 1)]),
            ("series", "idx_dicom_series_description_day", prefix + [("_kehrnel.description", 1), ("_kehrnel.day", 1), ("_id", 1)]),
            ("instances", "idx_dicom_instance_series_order", prefix + [("_kehrnel.seriesUid", 1), ("_kehrnel.orderMissing", 1), ("_kehrnel.instanceNumber", 1), ("_id", 1)]),
        ]
        if cfg["projections"]["enabled"]:
            definitions.extend([
                ("research", "idx_dicom_research_scope", prefix + [("projectionVersion", 1), ("_kehrnel.modality", 1), ("_kehrnel.day", 1), ("_id", 1)]),
                ("research", "idx_dicom_research_strings", prefix + [("projectionVersion", 1), ("attrs.p", 1), ("attrs.s", 1)]),
                ("research", "idx_dicom_research_numbers", prefix + [("projectionVersion", 1), ("attrs.p", 1), ("attrs.n", 1)]),
            ])
        indexes = {}
        for kind, name, keys in definitions:
            indexes.setdefault(cfg["collections"][kind], []).append({"keys": keys, "options": {"name": name}})
        # Metadata aliases exist only on promoted study summaries. Partial
        # indexes avoid a missing-field entry for every legacy study.
        for name, suffix in [("idx_dicom_metadata_day", [("studyDate", 1), ("_id", 1)]),
                             ("idx_dicom_metadata_description", [("descriptionNorm", 1), ("_id", 1)])]:
            indexes[cfg["collections"]["studies"]].append({
                "keys": [("tenantId", 1), ("centerId", 1)] + suffix,
                "options": {"name": name, "partialFilterExpression": {"tenantId": {"$exists": True}, "centerId": {"$exists": True}}}})
        collection_names = [cfg["collections"][x] for x in ("studies", "series", "instances")]
        rule_collection=cfg["collections"]["studies"]+"_metadata_rules"
        collection_names.append(rule_collection)
        indexes[rule_collection]=[{"keys":[("tenantId",1),("centerId",1),("createdAt",-1),("_id",1)],"options":{"name":"idx_dicom_metadata_rules_scope"}}]
        search = []
        if cfg["projections"]["enabled"]:
            collection_names.append(cfg["collections"]["research"])
            if cfg["projections"]["search_enabled"]:
                fields = {
                    "projectionVersion": {"type": "token"},
                    "_kehrnel": {"type": "document", "fields": {
                        "tenantId": {"type": "token"}, "centerId": {"type": "token"},
                        "modality": {"type": "token"}, "day": {"type": "token"},
                        "studyUid": {"type": "token"}, "seriesUid": {"type": "token"}}},
                    "attrs": {"type": "embeddedDocuments", "fields": {
                        "p": {"type": "token"}, "s": {"type": "token"}, "n": {"type": "number"}}},
                }
                search.append({"collection": cfg["collections"]["research"], "name": cfg["projections"]["search_index"],
                               "definition": {"mappings": {"dynamic": False, "fields": fields}}})
        return ApplyPlan(artifacts={"collections": collection_names, "indexes": indexes,
                                   "search": search, "sharding": {"automatic": False,
                                       "reason": "Shard routing, zones, and UID uniqueness require deployment-specific validation."},
                                   "projectionVersion": version(cfg)})

    async def apply(self, ctx, plan):
        # Recompute artifacts: an untrusted apply payload cannot create arbitrary
        # collections or indexes outside the activation's configuration.
        expected = await self.plan(ctx)
        from .common import digest
        incoming = expected.artifacts if plan is None else plan.get("artifacts") if isinstance(plan, dict) else plan.artifacts
        if digest(incoming) != digest(expected.artifacts):
            fail("DICOM_APPLY_PLAN_STALE", "Apply the current activation plan.", 409)
        plan = expected
        admin = (ctx.adapters or {}).get("index_admin")
        if admin is None:
            fail("INDEX_ADMIN_REQUIRED", "Index administration is required to apply this plan.", 503)
        result = ApplyResult()
        for name in plan.artifacts["collections"]:
            await admin.ensure_collection(name)
            result.created.append(name)
        for name, specs in plan.artifacts["indexes"].items():
            response = await admin.ensure_indexes(name, specs)
            if response.get("warnings"):
                fail("DICOM_INDEX_APPLY_FAILED", "Required DICOM indexes could not be applied.", 503)
            result.created.extend(response.get("created", []))
        for item in plan.artifacts["search"]:
            adapter = (ctx.adapters or {}).get("atlas_search")
            if adapter is None:
                fail("ATLAS_SEARCH_REQUIRED", "Optional search indexing requires an Atlas Search adapter.", 503)
            response = await adapter.ensure_search_index(item["collection"], item["name"], item["definition"])
            if response.get("warnings"):
                fail("DICOM_SEARCH_INDEX_APPLY_FAILED", "The optional search index could not be applied.", 503)
            result.created.append(item["name"])
        return result

    async def transform(self, ctx, payload):
        cfg = config(ctx)
        if payload.get("resource") == "study":
            base, children = model.study(ctx, cfg, payload["document"])
            return TransformResult(base=base, meta={"series": children, "sourcePreserved": True})
        if payload.get("resource") == "instance":
            document = payload["document"]
            series_uid = model.uid(model.scalar(document.get("header", {}), "0020000E"))
            parent = await storage(ctx).find_one(cfg["collections"]["series"], {
                **query.scoped(ctx, cfg), "_id": series_uid})
            base = model.instance(ctx, cfg, document, parent)
            return TransformResult(base=base, search=model.project(cfg, base, document),
                                   meta={"sourcePreserved": True})
        fail("DICOM_RESOURCE_INVALID", "Transform a study or an instance.")

    async def reverse_transform(self, ctx, payload):
        cfg = config(ctx)
        document = payload["document"]
        metadata = document.get("_kehrnel", {})
        if metadata.get("tenantId") != cfg["tenant_id"] or metadata.get("centerId") not in query.scoped(ctx, cfg)["_kehrnel.centerId"]["$in"]:
            fail("DICOM_CENTER_FORBIDDEN", "The document is outside the authorized scope.", 403)
        if metadata.get("headerRef"):
            document = await storage(ctx).find_one(cfg["collections"]["instances"], {
                **query.scoped(ctx, cfg), "_id": document["_id"]})
            if document is None:
                fail("DICOM_METADATA_REFERENCE_INVALID", "Resolve an imported, authorized instance metadata reference.", 404)
        return await headers.original(ctx, cfg, document)

    async def ingest(self, ctx, payload):
        cfg = config(ctx)
        studies, instances = payload.get("studies", []), payload.get("instances", [])
        if not isinstance(studies, list) or not isinstance(instances, list) or len(studies) + len(instances) > cfg["ingest"]["max_documents"]:
            fail("DICOM_INGEST_BOUND_EXCEEDED", "Use bounded study/instance import batches.")
        from bson import BSON
        if len(BSON.encode(payload)) > cfg["ingest"]["max_bytes"]:
            fail("DICOM_INGEST_BOUND_EXCEEDED", "The import batch exceeds the configured byte bound.")
        transformed_studies, parents, catalog_series = [], {}, []
        for raw in studies:
            check_cancel(ctx)
            doc, children = model.study(ctx, cfg, raw)
            if doc["_id"] in {x["_id"] for x in transformed_studies}:
                fail("DICOM_STUDY_DUPLICATE", "A batch cannot contain duplicate study UIDs.")
            transformed_studies.append(doc)
            for child in children:
                if child["_id"] in parents:
                    fail("DICOM_HIERARCHY_CONFLICT", "A series UID belongs to multiple studies in this batch.")
                parents[child["_id"]] = child
                catalog_series.append(child)
        transformed_instances, projections, seen = [], [], set()
        for raw in instances:
            check_cancel(ctx)
            series_uid = model.uid(model.scalar(raw.get("header", {}), "0020000E"))
            if series_uid not in parents:
                found = await storage(ctx).find_one(cfg["collections"]["series"], {
                    **query.scoped(ctx, cfg), "_id": series_uid})
                if found is None:
                    fail("DICOM_SERIES_NOT_FOUND", "Import the study/series catalog before its instances.", 409)
                parents[series_uid] = found
            doc = model.instance(ctx, cfg, raw, parents[series_uid])
            if doc["_id"] in seen:
                fail("DICOM_INSTANCE_DUPLICATE", "A batch cannot contain duplicate SOP UIDs.")
            seen.add(doc["_id"])
            transformed_instances.append(doc)
            projections.append(model.project(cfg, doc, raw) if cfg["projections"]["refresh_on_ingest"] else None)
        # All source shapes are validated and all bundles are durable before
        # metadata locators become visible. Retries reuse content-addressed keys.
        retained = await headers.bundle(ctx, cfg, transformed_instances)
        semaphore = asyncio.Semaphore(cfg["ingest"]["write_concurrency"])

        async def persist(kind, document):
            async with semaphore:
                check_cancel(ctx)
                await write(ctx, cfg["collections"][kind], document, preserve_runtime=kind == "series")

        for kind, docs in (("series", catalog_series), ("studies", transformed_studies),
                           ("instances", retained)):
            responses = await asyncio.gather(*(persist(kind, doc) for doc in docs), return_exceptions=True)
            for response in responses:
                if isinstance(response, BaseException):
                    raise response
        # Reconcile removed series on an authoritative study re-import.
        for study_doc in transformed_studies:
            wanted = [x["si"] for x in study_doc.get("se", [])]
            await storage(ctx).delete_many(cfg["collections"]["series"], {
                **query.scoped(ctx, cfg), "_kehrnel.studyUid": study_doc["_id"],
                "_id": {"$nin": wanted}})
        if cfg["projections"]["enabled"] and cfg["projections"]["refresh_on_ingest"]:
            for doc, projection in zip(transformed_instances, projections):
                if projection:
                    await persist("research", projection)
                else:
                    await storage(ctx).delete_many(cfg["collections"]["research"], {
                        **query.scoped(ctx, cfg), "_id": doc["_id"]})
        elif cfg["projections"]["enabled"] and transformed_instances:
            # One bounded invalidation per batch. Exact replays keep the old
            # valid projection; changed sources wait for the background build.
            await storage(ctx).delete_many(cfg["collections"]["research"], {
                **query.scoped(ctx, cfg), "$or": [
                    {"_id": doc["_id"], "_kehrnel.source_revision": {"$ne": doc["_kehrnel"]["source_revision"]}}
                    for doc in transformed_instances]})
        return {"ok": True, "studies": len(transformed_studies),
                "series": sum(len(x.get("se", [])) for x in transformed_studies),
                "instances": len(retained), "projected": sum(x is not None for x in projections),
                "headerMode": cfg["headers"]["mode"], "projectionVersion": version(cfg),
                "affectedSeries": sorted(parents), "summaryRefreshRequired": bool(parents),
                "researchRefreshOnIngest": cfg["projections"]["refresh_on_ingest"],
                "atomic": False, "retry": "idempotent source UID upserts; source revisions preserved"}

    async def compile_query(self, ctx, domain, query):
        if domain != "dicom":
            fail("DICOM_DOMAIN_INVALID", "Use the dicom activation domain.")
        return compile_request(ctx, query)

    async def execute_query(self, ctx, plan):
        return await query.execute_query(ctx, plan)

    async def generate(self, ctx, payload):
        result = synthetic.generate(payload)
        if payload.get("persist"):
            cfg = config(ctx)
            summary = await self.ingest(ctx, {"studies": result["studies"]})
            summary["instances"] = 0
            size = cfg["ingest"]["max_documents"]
            for offset in range(0, len(result["instances"]), size):
                batch = await self.ingest(ctx, {"instances": result["instances"][offset:offset + size]})
                summary["instances"] += batch["instances"]
            result["ingested"] = summary
        return result

    async def rebuild(self, ctx, payload):
        cfg = config(ctx)
        if not cfg["projections"]["enabled"]:
            fail("DICOM_PROJECTION_DISABLED", "Activate a selective projection before rebuilding it.", 409)
        parent = await storage(ctx).find_one(cfg["collections"]["series"], {
            **query.scoped(ctx, cfg), "_id": payload["seriesUid"]})
        if parent is None:
            fail("DICOM_SERIES_NOT_FOUND", "The series is absent from the authorized catalog.", 404)
        request = {"version": "dicom-query/v1", "resource": "instances",
                   "filter": {"seriesUid": payload["seriesUid"]}, "limit": payload.get("limit", 100)}
        if payload.get("cursor"):
            request["cursor"] = payload["cursor"]
        page = await query.execute_query(ctx, query.compile_query(ctx, request))
        written = 0
        # Cache only within this bounded rebuild page. Full archive caching and
        # long-running job orchestration belong to the artifact/runtime layer.
        cache = {}
        for row in page.rows:
            check_cancel(ctx)
            doc = await storage(ctx).find_one(cfg["collections"]["instances"], {
                **query.scoped(ctx, cfg), "_id": row["_id"]})
            if doc is None:
                fail("DICOM_SOURCE_CHANGED", "Instance catalog changed during projection rebuild; retry.", 409)
            raw = await headers.original(ctx, cfg, doc, cache)
            projected = model.project(cfg, doc, raw, parent)
            if projected:
                await write(ctx, cfg["collections"]["research"], projected)
                written += 1
            else:
                await storage(ctx).delete_many(cfg["collections"]["research"], {**query.scoped(ctx, cfg), "_id": doc["_id"]})
        return {"ok": True, "processed": len(page.rows), "projected": written,
                "projectionVersion": version(cfg), "page": page.explain["page"],
                "coverage": "partial", "publishedCorpus": False}

    async def refresh_summary(self, ctx, payload):
        from datetime import datetime, timezone
        cfg = config(ctx)
        parent = await storage(ctx).find_one(cfg["collections"]["series"], {
            **query.scoped(ctx, cfg), "_id": payload["seriesUid"]})
        if parent is None:
            fail("DICOM_SERIES_NOT_FOUND", "The series is absent from the authorized catalog.", 404)
        owner = parent["_kehrnel"]
        match = {"_kehrnel.tenantId": owner["tenantId"], "_kehrnel.centerId": owner["centerId"],
                 "_kehrnel.seriesUid": parent["_id"]}
        counted = await storage(ctx).aggregate(cfg["collections"]["instances"],
            [{"$match": match}, {"$count": "n"}], allow_disk_use=False,
            hint="idx_dicom_instance_series_order")
        summary = {"importedInstanceDocuments": counted[0]["n"] if counted else 0,
                   "sourceReportedCount": parent["source"].get("ni"),
                   "sourceRevision": owner["source_revision"],
                   "asOf": datetime.now(timezone.utc), "metadataCoverage": "partial_or_unknown"}
        result = await storage(ctx).update_one(cfg["collections"]["series"], {
            "_id": parent["_id"], "_kehrnel.tenantId": owner["tenantId"],
            "_kehrnel.centerId": owner["centerId"], "_kehrnel.source_revision": owner["source_revision"]},
            {"$set": {"_kehrnel.metadataSummary": summary}}, upsert=False)
        if getattr(result, "matched_count", 0) != 1:
            fail("DICOM_SOURCE_CHANGED", "The source catalog changed during summary refresh; retry.", 409)
        return {"ok": True, "summary": summary, "consistency": "asynchronous; count reflects the refresh read"}

    async def capabilities(self, ctx):
        cfg = config(ctx)
        return {"ok": True, "strategyId": self.manifest.id, "maturity": "preview",
                "queryContract": "dicom-query/v1", "operations": [x.name for x in self.manifest.ops],
                "headerMode": cfg["headers"]["mode"], "projectionVersion": version(cfg),
                "researchEnabled": cfg["projections"]["enabled"],
                "strategyVersion": self.manifest.version,
                "queryCatalogOperation": "dicom_query_catalog",
                "headerBatchOperation": "dicom_get_headers",
                "ruleCatalogOperation": "dicom_rule_catalog",
                "rulePreviewOperation": "dicom_rule_preview",
                "receptionOperation": "dicom_rule_receive",
                "rulePublicationScope": "bounded_batch; not an archive-wide corpus",
                "researchPredicateLimit": 16,
                "searchIndexPlanningEnabled": cfg["projections"]["enabled"] and cfg["projections"]["search_enabled"],
                "adaptersBound": sorted((ctx.adapters or {}).keys()),
                "unsupportedInteractions": ["DICOM DIMSE", "DICOMweb QIDO/WADO/STOW conformance",
                    "pixel streaming", "geometric frame ordering", "condition inference",
                    "complete corpus cohort publication", "automatic sharding",
                    "Online Archive/Federation/Infinite execution", "Atlas Search execution through the legacy scalar-rule query contract",
                    "nested sequence predicate correlation"],
                "metadataCatalogOperation":"dicom_metadata_catalog", "metadataPreviewOperation":"dicom_metadata_preview", "metadataQueryOperation":"dicom_metadata_query", "metadataSearchExecution":"conditional on bound Atlas adapter and READY typed mapping", "metadataContract":"dicom-metadata-preview/v2", "semanticSearch":"design only", "scaleValidated": False}

    async def run_op(self, ctx, op, payload):
        contract = next((x for x in self.manifest.ops if x.name == op), None)
        if contract is None:
            fail("OP_NOT_SUPPORTED", "Unsupported DICOM strategy operation.")
        try:
            jsonschema.Draft7Validator(contract.input_schema).validate(payload)
        except jsonschema.ValidationError:
            fail("DICOM_OPERATION_INVALID", "The operation does not satisfy its published input contract.")
        if op.startswith("dicom_metadata_"):
            from . import metadata_runtime
            return await metadata_runtime.dispatch(ctx, op, payload)
        if op.startswith("dicom_sample_"):
            from . import sample_runtime
            return await sample_runtime.dispatch(ctx, op, payload)
        if op.startswith("dicom_rule_"):
            return await playground.dispatch(ctx, op, payload)
        if op == "dicom_query":
            result = await self.execute_query(ctx, await self.compile_query(ctx, "dicom", payload))
            return {"ok": True, "rows": result.rows, "explain": result.explain}
        handlers = {"dicom_ingest": self.ingest, "dicom_get_header": headers.get_header,
                    "dicom_rebuild_projections": self.rebuild, "dicom_generate_synthetic": self.generate,
                    "dicom_refresh_series_summary": self.refresh_summary,
                    "dicom_capabilities": lambda context, _: self.capabilities(context),
                    "dicom_query_catalog": lambda context, _: _query_catalog(context, self.manifest),
                    "dicom_get_headers": headers.get_headers}
        result = await handlers[op](ctx, payload)
        return result


async def _query_catalog(ctx, manifest):
    return catalog.describe(ctx, manifest)
