"""Native rule, reception and frozen-batch playground operations."""
from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path

from kehrnel.engine.domains.dicom.sample_data import DICOMSampleError, describe_sample, load_sample

from . import headers, rules
from .common import ROOT, bounded_bson, centers, config, digest, fail, storage

PROFILES = json.loads((ROOT / "rule-profiles.json").read_text())
# Source assets belong to the domain. Pin their version/digest independently of
# this strategy's projection contract and package digest.
DEMO_SAMPLE_REF = {
    "id": "dicom.synthetic-imaging", "version": "1.0.0",
    "digest": "sha256:069af817ab7c9c475d6c61afe6cdebc41b3ea9e4111194636ec848ecd8639f89",
}


def _sample_metadata():
    try:
        return describe_sample(DEMO_SAMPLE_REF["id"], DEMO_SAMPLE_REF["version"],
                               expected_digest=DEMO_SAMPLE_REF["digest"])
    except DICOMSampleError:
        fail("DICOM_SAMPLE_INVALID", "The pinned domain demo samples are unavailable or invalid.", 503)


def catalog(ctx, _):
    from kehrnel.engine.core.outcome_catalog import load_bundled_industry_outcome_catalog
    architecture = next(item for item in load_bundled_industry_outcome_catalog().reference_architectures
                        if item.artifact_ref.id == 'architecture.healthcare.dicom-metadata')
    cfg = config(ctx)
    return {"ok": True, "contract": "dicom-rule-catalog/v1", "profileSchema": rules.PROFILE_SCHEMA,
            "profiles": copy.deepcopy(PROFILES["profiles"]), "placementGuide": copy.deepcopy(PROFILES["placementGuide"]),
            "sourceContract": "customer study se[] + instance header/ContentIndex",
            "sampleDatasets": [_sample_metadata()],
            "collectionNames": rules.collection_names(cfg), "authorizedCenters": centers(ctx,cfg),
            "supportedAggregations": ["distinct", "common", "range"],
            "groupSemantics": "same_instance_scalar_tuple", "naturalLanguage": "bounded explicit field comparisons",
            "limits": {"batchDocuments":cfg["ingest"]["max_documents"],"fields":32,"groups":8,"embeddedSummaryBytes":262144},
            "referenceArchitecture": architecture.to_wire(),
            "runtimeWorkflow": [
                {"id":"intake","label":"Reception inbox","operation":"dicom_rule_receive","status":"bounded",
                 "detail":"Immutable source batch and pinned rules; no projection work at reception."},
                {"id":"inventory","label":"Inventory and original metadata","operation":"dicom_rule_process","status":"bounded",
                 "detail":"Unique SOPs, study/series routing and retained original headers; inline or artifact bundles."},
                {"id":"rules","label":"Consolidation under versioned rules","operation":"dicom_rule_process","status":"bounded",
                 "detail":"Consolidate earlier receptions; calculate distinct/common/range and scalar tuple groups."},
                {"id":"studies","label":"Studies with embedded series summaries","operation":"dicom_rule_preview","status":"bounded",
                 "detail":"Frozen study snapshots with bounded summaries inside se[]. Compatibility viewer catalogue remains separate."},
                {"id":"research","label":"Selected series research projections","operation":"dicom_rule_process","status":"bounded",
                 "detail":"Typed observations, same-image groups, coverage and immutable generation publication."},
                {"id":"query","label":"Rule-aware queries and regular indexes","operation":"dicom_rule_query","status":"bounded",
                 "detail":"Native MongoDB execution on a ready generation; bounded explicit natural-language comparisons."},
                {"id":"search","label":"Atlas Search mapping and query plans","operation":"dicom_rule_apply_indexes","status":"plan_and_request",
                 "detail":"Generate mappings and request indexes; readiness polling and Search execution are not implemented."},
                {"id":"archive","label":"Selective historical rebuild","operation":"dicom_rule_plan_backfill","status":"plan_only",
                 "detail":"Modality/SOP Class/date selection plan; resumable archive worker is future work."},
                {"id":"clinical","label":"CDR and clinical cohort composition","status":"planned",
                 "detail":"Issuer-aware identity and provenance-linked clinical evidence; no inferred diagnosis from imaging tags."},
            ],
            "unsupported": ["Frame/sequence item correlated query compilation", "Archive-wide backfill worker",
                            "Automatic index readiness polling", "General clinical natural-language inference"]}


def demo_source(preset="mixed"):
    if preset not in {"mixed", "ct", "mr", "mg", "legacy"}:
        fail("DICOM_DEMO_INVALID", "Select an advertised synthetic preset.")
    try:
        return load_sample(DEMO_SAMPLE_REF["id"], DEMO_SAMPLE_REF["version"],
                           variant=preset, expected_digest=DEMO_SAMPLE_REF["digest"])
    except DICOMSampleError:
        fail("DICOM_SAMPLE_INVALID", "The pinned domain demo samples are unavailable or invalid.", 503)


def demo(ctx, payload):
    source = demo_source(payload.get("preset","mixed"))
    profile = payload.get("profile") or PROFILES["profiles"][0]
    result = rules.preview(ctx,{**source,"profile":profile})
    response = {"ok":True,"synthetic":True,"containsPixels":False,"source":source,"profile":profile,"preview":result,
                "sampleDataset":{"domain":"dicom", **DEMO_SAMPLE_REF, "variant":payload.get("preset","mixed")}}
    if payload.get("query"):
        plan = rules.compile_query(ctx,{**payload["query"],"profile":profile,"generationId":result["generationId"]})
        group_name = payload["query"].get("group")
        def compare(value, op, literal):
            return {"=":lambda:value==literal,">":lambda:value>literal,">=":lambda:value>=literal,
                    "<":lambda:value<literal,"<=":lambda:value<=literal}[op]()
        def predicate_matches(values, clause):
            return any(compare(value,clause["op"],clause["value"]) for value in values)
        matches=[]
        for row in result["collections"]["research"]:
            if plan["modality"] and row["_kehrnel"]["modality"] != plan["modality"]:
                continue
            if group_name:
                selected = [g for g in row["groups"] if g["p"] == group_name]
                passed=any(all(predicate_matches(list(g["v"].get(c["field"],{}).values()),c) for c in plan["predicates"]) for g in selected)
            else:
                field_names = {c["field"] for c in plan["predicates"]}
                passed = all(any(all(compare(a.get("n",a.get("s")),c["op"],c["value"]) for c in plan["predicates"] if c["field"]==field)
                                 for a in row["attrs"] if a["p"]==field) for field in field_names)
            if passed: matches.append(row)
        response["queryResult"]={**plan,"rows":sorted(matches,key=lambda r:r["_id"])[:payload["query"].get("limit",50)],
                                 "executed":False,"engine":"bounded_native_preview","publishedCorpus":False}
    return response


def scope(ctx, cfg):
    return {"tenantId":cfg["tenant_id"],"centerIds":{"$in":centers(ctx,cfg)}}


def same_inventory_source(previous, incoming):
    fields = ('source_revision', 'tenantId', 'centerId', 'studyUid', 'seriesUid')
    return all(previous.get('_kehrnel', {}).get(key) == incoming['_kehrnel'].get(key) for key in fields)


async def receive(ctx,payload):
    profile = payload["profile"]; profile_version = rules.validate(profile)
    cfg,bases,parents,accepted,source_gen,replayed = rules.prepare(ctx,payload)
    batch_id = digest([cfg["tenant_id"],source_gen,profile_version])
    names = rules.collection_names(cfg)
    row = {"_id":batch_id,"tenantId":cfg["tenant_id"],"centerIds":sorted({p["_kehrnel"]["centerId"] for p in parents.values()}),
           "sourceGeneration":source_gen,"profileVersion":profile_version,"profile":copy.deepcopy(profile),
           "source":{"studies":copy.deepcopy(payload["studies"]),"instances":[accepted[k]["raw"] for k in sorted(accepted)]},
           "state":"pending","receivedAt":datetime.now(timezone.utc),"scope":"bounded_batch"}
    bounded_bson(row)
    await storage(ctx).update_one(names["inbox"],{"_id":batch_id,"tenantId":cfg["tenant_id"]},{"$setOnInsert":row},upsert=True)
    return {"ok":True,"batchId":batch_id,"profileVersion":profile_version,"uniqueSops":len(accepted),
            "replayedReceptions":replayed,"state":"accepted_or_already_present",
            "deduplicationScope":"this immutable batch; cross-batch SOP conflicts are checked during processing"}


async def process(ctx,payload):
    cfg=config(ctx); names=rules.collection_names(cfg); db=storage(ctx)
    batch=await db.find_one(names["inbox"],{"_id":payload["batchId"],**scope(ctx,cfg)})
    if not batch or not set(batch["centerIds"]) <= set(centers(ctx,cfg)):
        fail("DICOM_BATCH_NOT_FOUND","The reception batch is outside the authorized scope.",404)
    if batch["state"] == "processed":
        return {"ok":True,"batchId":batch["_id"],"generationId":batch["generationId"],"state":"processed","reused":True}
    # An expiring claim supports recovery if a process stops between writes.
    from datetime import timedelta
    import uuid
    now=datetime.now(timezone.utc)
    claim_token=uuid.uuid4().hex
    claim=await db.update_one(names["inbox"],{"_id":batch["_id"],"tenantId":cfg["tenant_id"],
        "$or":[{"state":{"$in":["pending","failed"]}},{"state":"processing","leaseUntil":{"$lt":now}}]},
        {"$set":{"state":"processing","leaseUntil":now+timedelta(minutes=10),"claimToken":claim_token}},upsert=False)
    if getattr(claim,"matched_count",0) != 1:
        fail("DICOM_BATCH_BUSY","This batch is being processed; retry after its lease.",409)
    try:
        _,_,parents,accepted,_,_=rules.prepare(ctx,{**batch["source"],"profile":batch["profile"]})
        # Check permanent inventory fingerprints, not a temporary reception list.
        for sop,record in accepted.items():
            previous=await db.find_one(names["inventory"],{"_id":sop})
            if previous and not same_inventory_source(previous, record['doc']):
                fail("DICOM_RECEPTION_CONFLICT","An existing SOP has different source or ownership; resolve before rebuilding.",409)
        retained=await headers.bundle(ctx,cfg,[record["doc"] for record in accepted.values()])
        for doc in retained:
            # Immutable source: competing inserts cannot replace an accepted SOP.
            await db.update_one(names["inventory"],{"_id":doc["_id"]}, {"$setOnInsert":doc},upsert=True)
            previous=await db.find_one(names["inventory"],{"_id":doc["_id"]})
            if not same_inventory_source(previous, doc):
                fail("DICOM_RECEPTION_CONFLICT","A concurrent reception changed SOP ownership/content.",409)
        # Consolidate previously received unique instances of affected series,
        # never just the latest arrival batch. Large series require the separate
        # partitioned worker rather than silently truncating these statistics.
        consolidated={}
        cache={}
        maximum=cfg["ingest"]["max_documents"]-len(batch["source"]["studies"])
        for series_uid,parent in parents.items():
            existing=await db.aggregate(names["inventory"],[{"$match":{
                "_kehrnel.tenantId":cfg["tenant_id"],"_kehrnel.centerId":parent["_kehrnel"]["centerId"],
                "_kehrnel.seriesUid":series_uid}}, {"$sort":{"_id":1}}, {"$limit":maximum+1}],allow_disk_use=False)
            for document in existing:
                consolidated[document["_id"]]=await headers.original(ctx,cfg,document,cache)
            if len(consolidated)>maximum:
                fail("DICOM_CONSOLIDATION_BOUND_EXCEEDED","Series consolidation exceeds the playground bound; use the partitioned archive worker.",409)
        result=rules.preview(ctx,{"studies":batch["source"]["studies"],"instances":list(consolidated.values()),"profile":batch["profile"]})
        # Rows are immutable and hidden from queries until the generation
        # manifest is inserted last. Partial writes are safe to retry.
        for kind in ("studies","research"):
            for row in result["collections"][kind]:
                await db.update_one(names[kind],{"_id":row["_id"]},{"$setOnInsert":row},upsert=True)
        profile_row={"_id":digest([cfg["tenant_id"],result["profileVersion"]]),"tenantId":cfg["tenant_id"],"profile":batch["profile"]}
        await db.update_one(names["profiles"],{"_id":profile_row["_id"],"tenantId":cfg["tenant_id"]},{"$setOnInsert":profile_row},upsert=True)
        manifest={"_id":result["generationId"],"tenantId":cfg["tenant_id"],"centerIds":batch["centerIds"],
                  "profile":batch["profile"],"profileVersion":result["profileVersion"],"counts":result["counts"],
                  "scope":"bounded_batch","publishedCorpus":False,"createdAt":datetime.now(timezone.utc),"state":"ready"}
        current=await db.find_one(names["inbox"],{"_id":batch["_id"],"tenantId":cfg["tenant_id"],"claimToken":claim_token,"state":"processing","leaseUntil":{"$gt":datetime.now(timezone.utc)}})
        if not current:
            fail("DICOM_BATCH_LEASE_LOST","This processor lost its lease; retry through the current owner.",409)
        await db.update_one(names["generations"],{"_id":manifest["_id"],"tenantId":cfg["tenant_id"]},{"$setOnInsert":manifest},upsert=True)
        await db.update_one(names["inbox"],{"_id":batch["_id"],"tenantId":cfg["tenant_id"],"claimToken":claim_token},
                            {"$set":{"state":"processed","generationId":manifest["_id"]},"$unset":{"leaseUntil":"","claimToken":""}},upsert=False)
        return {**result,"persisted":True,"state":"processed","batchId":batch["_id"]}
    except Exception:
        await db.update_one(names["inbox"],{"_id":batch["_id"],"tenantId":cfg["tenant_id"],"claimToken":claim_token},
                            {"$set":{"state":"failed"},"$unset":{"leaseUntil":"","claimToken":""}},upsert=False)
        raise


async def execute(ctx,payload):
    if payload.get("allowPartial") is not True:
        fail("DICOM_PROJECTION_COVERAGE_UNKNOWN","Explicitly allow the bounded generation; it is not a full archive cohort.")
    cfg=config(ctx); names=rules.collection_names(cfg)
    manifest=await storage(ctx).find_one(names["generations"],{"_id":payload["generationId"],**scope(ctx,cfg),"state":"ready"})
    if not manifest or not set(manifest["centerIds"]) <= set(centers(ctx,cfg)):
        fail("DICOM_GENERATION_NOT_FOUND","Query an authorized ready generation.",404)
    if rules.validate(payload["profile"]) != manifest["profileVersion"]:
        fail("DICOM_PROFILE_STALE","Use the profile associated with the selected generation.",409)
    plan=rules.compile_query(ctx,payload)
    rows=await storage(ctx).aggregate(plan["collection"],plan["pipeline"],allow_disk_use=False)
    return {**plan,"rows":rows,"executed":True,"publishedCorpus":False,"pagination":"bounded first page; narrow the selection for more results"}


async def apply_indexes(ctx,payload):
    cfg=config(ctx); profile=payload["profile"]; rules.validate(profile)
    plan=rules.index_plan(cfg,profile); names=rules.collection_names(cfg)
    admin=(ctx.adapters or {}).get("index_admin")
    if admin is None:
        fail("INDEX_ADMIN_REQUIRED","Use an index administration binding.",503)
    for name in names.values():
        await admin.ensure_collection(name)
    specs={names["samples"]:[{"keys":[("tenantId",1),("dataset.id",1),("dataset.version",1),("state",1),("_id",1)],"options":{"name":"idx_dicom_sample_dataset"}}], names["research"]:plan["regular"], names["inventory"]:[{"keys":[("_kehrnel.tenantId",1),("_kehrnel.centerId",1),("_kehrnel.seriesUid",1),("_id",1)],"options":{"name":"idx_dicom_rule_inventory_series"}}], names["inbox"]:[{"keys":[("tenantId",1),("state",1),("leaseUntil",1)],"options":{"name":"idx_dicom_reception_state"}}],
           names["studies"]:[{"keys":[("_kehrnel.tenantId",1),("_kehrnel.centerId",1),("generationId",1),("_kehrnel.day",1)],"options":{"name":"idx_dicom_rule_study_generation"}}]}
    for collection,indexes in specs.items():
        response=await admin.ensure_indexes(collection,indexes)
        if response.get("warnings"):
            fail("DICOM_INDEX_APPLY_FAILED","Required playground indexes could not be applied.",503)
    search_state="not_requested"
    if payload.get("includeSearch"):
        adapter=(ctx.adapters or {}).get("atlas_search")
        if not adapter:
            fail("ATLAS_SEARCH_REQUIRED","Search index creation requires an Atlas Search binding.",503)
        response=await adapter.ensure_search_index(plan["collection"],plan["search"]["name"],plan["search"]["definition"])
        if response.get("warnings"):
            fail("DICOM_SEARCH_INDEX_APPLY_FAILED","The search index could not be requested.",503)
        search_state="requested; readiness and search execution not verified"
    return {"ok":True,"indexPlan":plan,"regularIndexes":"applied","searchState":search_state}


def backfill_plan(ctx,payload):
    cfg=config(ctx); profile=payload["profile"]; rules.validate(profile)
    selection=payload["selection"]
    from datetime import date
    try:
        start,end=date.fromisoformat(selection["fromDay"]),date.fromisoformat(selection["toDay"])
        if start>end: raise ValueError()
    except (ValueError,TypeError,KeyError):
        fail("DICOM_DATE_INVALID","Provide an inclusive valid acquisition-day interval.")
    # A selective study match reduces candidates; extraction still needs each
    # candidate's relevant instance/frame metadata from the inventory service.
    element={"centerId":{"$in":centers(ctx,cfg)}}
    if profile.get("modalities"): element["modality"]={"$in":profile["modalities"]}
    match={"_kehrnel.tenantId":cfg["tenant_id"],"_kehrnel.centerId":{"$in":centers(ctx,cfg)},
           "_kehrnel.day":{"$gte":start.isoformat(),"$lte":end.isoformat()},"_kehrnel.seriesFacets":{"$elemMatch":element}}
    return {"ok":True,"profileVersion":rules.validate(profile),"sourcePipeline":[{"$match":match},{"$sort":{"_kehrnel.day":1,"_id":1}}],
            "scope":"plan_only","workerAvailable":False,"sopClasses":profile.get("sopClasses",[]),
            "requiredNextSteps":["Estimate candidate studies/series and metadata bytes without a full scan",
              "Resolve matching SOP Classes through series inventory; legacy classes can be unknown",
              "Partition by tenant/centre/day and stable UID ranges; checkpoint and throttle each partition",
              "Capture a source watermark and replay additions/corrections before publishing a new generation"],
            "duration":"depends on selected bytes and measured extraction throughput; not promised as one day"}


async def dispatch(ctx,op,payload):
    functions={"dicom_rule_catalog":catalog,"dicom_rule_demo":demo,"dicom_rule_preview":rules.preview,
               "dicom_rule_compile_query":rules.compile_query,"dicom_rule_plan_backfill":backfill_plan}
    if op in functions:
        return functions[op](ctx,payload)
    functions={"dicom_rule_receive":receive,"dicom_rule_process":process,"dicom_rule_query":execute,
               "dicom_rule_apply_indexes":apply_indexes}
    return await functions[op](ctx,payload)
