"""SDK/API boundary. Partition workers use Converter directly, without HTTP overhead."""

import asyncio
from pathlib import Path
import jsonschema

from kehrnel.engine.core.plugin import StrategyPlugin
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.types import ApplyPlan, ApplyResult, TransformResult
from kehrnel.engine.domains.openehr.analytics.converter import Converter, SourceFields
from kehrnel.engine.domains.openehr.analytics.decoder import IBMDecoder, IBMFields
from kehrnel.engine.domains.openehr.analytics.rules import RuleSet
from kehrnel.engine.domains.openehr.analytics.store import MongoProjectionStore
from kehrnel.engine.domains.openehr.analytics.source import digest


class OpenEHRAnalyticsStrategy(StrategyPlugin):
    def __init__(self, manifest=None):
        self.manifest = manifest or load_strategy(
            "openehr.analytics", Path(__file__).resolve().parent
        )
        self._cached_converter = None

    def converter(self, ctx):
        # Cache the decoder per environment/configuration, never across tenants.
        revision = (getattr(ctx, "environment_id", None), digest(ctx.config))
        cached = self._cached_converter
        if cached and cached[0] == revision:
            return cached[1]
        from kehrnel.engine.domains.openehr.analytics.execution import converter_for, source_connection
        cfg = ctx.config
        jsonschema.validate(cfg, self.manifest.config_schema)
        source = cfg['source']
        if source['format'] == 'ibm' and not (source.get('codes') and source.get('shortcuts')):
            with source_connection(cfg, ctx.bindings) as collection:
                converter = converter_for(cfg, collection)
        else:
            converter = converter_for(cfg)
        # One immutable assignment prevents a competing environment from seeing
        # a cache key paired with another environment's decoder.
        self._cached_converter = (revision, converter)
        return converter

    def store(self, ctx):
        cfg = ctx.config
        adapter = (ctx.adapters or {}).get("storage")
        if adapter is None:
            raise ValueError("MongoDB storage adapter is required")
        db = adapter.db
        # Motor explicitly wraps a thread-safe PyMongo database. Run the shared
        # synchronous transaction implementation in a worker thread.
        from pymongo.database import Database

        if not isinstance(db, Database):
            # Standalone mongo-only installations do not require Motor.
            try:
                from motor.motor_asyncio import AsyncIOMotorDatabase
                if isinstance(db, AsyncIOMotorDatabase):
                    db = db.delegate
            except ImportError:
                pass

        if not isinstance(db, Database) or db.name != cfg["database"]:
            raise ValueError(
                "Bind this strategy to its configured analytics MongoDB database"
            )
        return MongoProjectionStore(
            db,
            source_namespace=cfg["source_namespace"],
            prefix=cfg["collection_prefix"],
            rollup_partitions=cfg["rollup_partitions"],
            max_bytes=cfg["max_transaction_bytes"],
            max_facts=cfg["max_transaction_facts"],
        )

    async def validate_config(self, ctx):
        from kehrnel.engine.domains.openehr.analytics.setup import validate_setup
        # The SDK activation hook passes a dict; engine operations pass a context.
        config = ctx if isinstance(ctx, dict) else ctx.config
        validate_setup(config, self.manifest.config_schema)
        RuleSet.compile(config["rules"])

    async def plan(self, ctx):
        rules = RuleSet.compile(ctx.config["rules"])
        cfg = ctx.config
        store = MongoProjectionStore(
            None,
            source_namespace=cfg["source_namespace"],
            prefix=cfg["collection_prefix"],
            rollup_partitions=cfg["rollup_partitions"],
        )
        return ApplyPlan(
            artifacts={
                "database": cfg["database"],
                "collections": store.names,
                "indexes": store.plan(),
                "rules_revision": rules.revision,
                "source_namespace": cfg["source_namespace"],
                "rollup_partitions": cfg["rollup_partitions"],
                "transactions_required": True,
                "source_writes": False,
            }
        )

    async def apply(self, ctx, plan):
        expected = await self.plan(ctx)
        incoming = plan.get("artifacts") if isinstance(plan, dict) else plan.artifacts
        if digest(incoming) != digest(expected.artifacts):
            raise ValueError("Apply the current analytics plan")
        store = self.store(ctx)
        await asyncio.to_thread(
            store.prepare, RuleSet.compile(ctx.config["rules"]).by_revision.values()
        )
        return ApplyResult(
            created=list(store.names.values()),
            warnings=[
                "Atlas Search provisioning and sharding are deployment-specific; this plan creates native indexes."
            ],
        )

    async def transform(self, ctx, payload):
        converter = await asyncio.to_thread(self.converter, ctx)
        projection = await asyncio.to_thread(
            converter.convert,
            payload["document"],
            accepted_at=payload["accepted_at"],
            deleted=payload.get("deleted", False),
        )
        return TransformResult(
            base=projection.documents(),
            meta={"sourcePreserved": True, "status": projection.status},
        )

    async def ingest(self, ctx, payload):
        converter = await asyncio.to_thread(self.converter, ctx)
        projection = await asyncio.to_thread(
            converter.convert,
            payload["document"],
            accepted_at=payload["accepted_at"],
            deleted=payload.get("deleted", False),
        )
        result = await asyncio.to_thread(self.store(ctx).write, projection)
        return {"ok": True, **result}

    async def run_op(self, ctx, op, payload):
        definition = next((x for x in self.manifest.ops if x.name == op), None)
        if definition is None:
            raise ValueError("Unknown analytics operation")
        jsonschema.validate(payload, definition.input_schema)
        if op == "analytics_source_from_strategy":
            from kehrnel.engine.domains.openehr.analytics.source_environment import source_from_strategy
            return {"ok": True, "source": source_from_strategy(**payload), "database_writes": False}
        if op == "analytics_validate_setup":
            await self.validate_config(ctx)
            return {"ok": True, "configuration": ctx.config, "database_writes": False}
        if op == "analytics_prepare":
            result = await self.apply(ctx, await self.plan(ctx))
            return {"ok": True, "collections": result.created}
        if op in ("analytics_test_source", "analytics_source_sample", "analytics_process_documents", "analytics_backfill"):
            from kehrnel.engine.domains.openehr.analytics import execution
            await self.validate_config(ctx)
            try:
                if op == "analytics_test_source":
                    return await asyncio.to_thread(execution.test_source, ctx.config, ctx.bindings)
                store = self.store(ctx) if payload.get("write") or op == "analytics_backfill" else None
                if op == "analytics_process_documents":
                    return await asyncio.to_thread(execution.process_documents, ctx.config, payload["documents"], accepted_at=payload["accepted_at"], store=store)
                if op == "analytics_source_sample":
                    return await asyncio.to_thread(execution.source_sample, ctx.config, ctx.bindings, template=payload["template"], limit=payload["limit"], accepted_at=payload["accepted_at"], store=store)
                return await asyncio.to_thread(execution.backfill, ctx.config, ctx.bindings, store, payload["job"], payload["max_batches"])
            except Exception as exc:
                from pymongo.errors import PyMongoError
                if isinstance(exc, PyMongoError):
                    raise ValueError("Analytics database operation failed; check the configured connection, access and indexes") from None
                raise
        if op == "analytics_business_catalog":
            from kehrnel.engine.domains.openehr.analytics.business_catalog import catalog_page
            return await asyncio.to_thread(catalog_page, ctx.config.get("rules", []), **payload)
        if op == "analytics_source_inspection_plan":
            from kehrnel.engine.domains.openehr.analytics.inspection import inspection_plan
            fields = SourceFields(**ctx.config.get("source", {}).get("fields", {}))
            return inspection_plan(fields, **payload)
        if op == "analytics_source_coverage":
            from kehrnel.engine.domains.openehr.analytics.inspection import source_coverage
            fields = SourceFields(**ctx.config.get("source", {}).get("fields", {}))
            return await asyncio.to_thread(source_coverage, payload["headers"], payload["opts"],
                                          ctx.config.get("rules", []), as_of=payload["as_of"], fields=fields)
        if op in ("analytics_workbench_plan", "analytics_workbench"):
            from kehrnel.engine.domains.openehr.analytics.workbench import workbench_plan, read_workbench
            rules = ctx.config.get("rules", [])
            if op == "analytics_workbench_plan":
                return await asyncio.to_thread(workbench_plan, rules, **payload)
            store = self.store(ctx)
            return await asyncio.to_thread(read_workbench, store.collection(payload["dataset"]), rules, **payload)
        if op == "analytics_default_mapping":
            from kehrnel.engine.domains.openehr.analytics.authoring import proposal_for

            proposal = await asyncio.to_thread(
                proposal_for, payload, ctx.config.get("rules", [])
            )
            return {"ok": True, "proposal": proposal}
        if op in ("analytics_validate_draft", "analytics_preview_draft", "analytics_review_draft"):
            from copy import deepcopy
            from types import SimpleNamespace
            from kehrnel.engine.domains.openehr.analytics.authoring import compile_draft

            result = await asyncio.to_thread(
                compile_draft, payload, ctx.config.get("rules", [])
            )
            if op in ("analytics_preview_draft", "analytics_review_draft"):
                cfg = deepcopy(ctx.config)
                rule = result["rule"]
                cfg["rules"] = [
                    r
                    for r in cfg.get("rules", [])
                    if r.get("revision") != rule["revision"]
                ] + [rule]
                # A private converter prevents draft preview from changing the active cache.
                previewer = OpenEHRAnalyticsStrategy(self.manifest)
                projection = await asyncio.to_thread(
                    previewer.converter(SimpleNamespace(config=cfg)).convert,
                    payload["document"],
                    accepted_at=payload["accepted_at"],
                    rule_revision=rule["revision"],
                )
                result["preview"] = projection.documents()
                from kehrnel.engine.domains.openehr.analytics.review import review_requirements, acknowledge
                requirements = review_requirements(result, projection, cfg)
                result["review_requirements"] = requirements
                if op == "analytics_review_draft":
                    if projection.status != "projected":
                        raise ValueError("A successful draft preview is required for review")
                    result["review_receipt"] = acknowledge(requirements, payload["acknowledgements"])
            return result
        if op == "analytics_validate_configuration":
            from types import SimpleNamespace

            candidate = OpenEHRAnalyticsStrategy(self.manifest)
            await candidate.validate_config(SimpleNamespace(config=payload["config"]))
            plan = await candidate.plan(SimpleNamespace(config=payload["config"]))
            return {
                "ok": True,
                "config_digest": digest(payload["config"]),
                "plan": plan.artifacts,
            }
        if op == "analytics_preview":
            result = await self.transform(ctx, payload)
            return {"ok": True, **result.base}
        if op == "analytics_ingest":
            return await self.ingest(ctx, payload)
        if op == "analytics_rules":
            rules = RuleSet.compile(ctx.config["rules"])
            return {
                "ok": True,
                "revision": rules.revision,
                "rules": [r.document() for r in rules.by_revision.values()],
            }
        if op == "analytics_reprocessing_plan":
            from dataclasses import asdict
            from kehrnel.engine.domains.openehr.analytics.jobs import Backfill
            from kehrnel.engine.domains.openehr.analytics.projection import utc

            converter = Converter(RuleSet.compile(ctx.config["rules"]), source_namespace=ctx.config["source_namespace"], fields=SourceFields(**ctx.config["source"].get("fields", {})))
            job = Backfill(
                job_id=payload["job_id"],
                template=payload["template"],
                rule_revision=payload["rule_revision"],
                accepted_at=utc(payload["accepted_at"]),
                source_namespace=converter.namespace,
                committed_from=utc(payload["committed_from"]),
                committed_until=utc(payload["committed_until"]),
                batch_size=payload.get("batch_size", 64),
            )
            converter.rules.select(job.template, job.accepted_at, job.rule_revision)
            return {
                "ok": True,
                "job": asdict(job),
                "job_revision": job.revision,
                "source_filter": job.query(converter.fields),
                "source_writes": False,
                "execution": "external_worker",
                "submitted": False,
                "required_source_index": {
                    converter.fields.template: 1,
                    converter.fields.composition_id: 1,
                },
                "notes": [
                    "This plan does not start a scan.",
                    "Bounds filter source commit time, not inferred clinical time.",
                    "Run this immutable job with run_backfill; retain its source and rule revisions.",
                ],
            }
        if op == "analytics_jobs":
            store = self.store(ctx)
            query = {}
            if payload.get("after"):
                query["_id"] = {"$gt": payload["after"]}
            if payload.get("status"):
                query["status"] = payload["status"]
            if payload.get("template"):
                query["spec.template"] = payload["template"]

            def read_jobs():
                return list(
                    store.collection("jobs")
                    .find(query)
                    .sort("_id", 1)
                    .limit(payload.get("limit", 50))
                    .max_time_ms(10000)
                )

            return {"ok": True, "rows": await asyncio.to_thread(read_jobs)}
        if op == "analytics_coverage":
            store = self.store(ctx)
            query = {"status": "needs_rules"}
            if payload.get("after"):
                query["_id"] = {"$gt": payload["after"]}
            if payload.get("template"):
                query["template"] = payload["template"]

            def read():
                return list(
                    store.collection("projection_state")
                    .find(query)
                    .sort("_id", 1)
                    .limit(payload.get("limit", 50))
                    .max_time_ms(10000)
                )

            return {"ok": True, "rows": await asyncio.to_thread(read)}
        raise ValueError("Unknown analytics operation")
