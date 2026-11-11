"""Bounded publication tests against an explicitly disposable local MongoDB."""
import copy
import os
import uuid
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.dicom.tiered_store import playground, rules
from kehrnel.persistence.artifacts.filesystem import FileSystemArtifactStore
from kehrnel.persistence.mongodb.storage import MongoStorageAdapter
from kehrnel.persistence.mongodb.index_admin import MongoIndexAdminAdapter


@pytest_asyncio.fixture
async def ctx(tmp_path):
    uri=os.environ.get("KEHRNEL_DICOM_TEST_MONGODB_URI")
    if not uri: pytest.skip("Set a disposable localhost MongoDB URI.")
    assert urlparse(uri).hostname in {"127.0.0.1","localhost","::1"}
    client=AsyncIOMotorClient(uri,serverSelectionTimeoutMS=5000)
    await client.admin.command("ping")
    name="dicom_rules_"+uuid.uuid4().hex; db=client[name]
    context=StrategyContext(environment_id="rules",config={"database":name},adapters={
        "storage":MongoStorageAdapter(db),"index_admin":MongoIndexAdminAdapter(db),
        "artifact":FileSystemArtifactStore(tmp_path/"headers")})
    try: yield context
    finally:
        await client.drop_database(name); client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode",["inline","bundle"])
async def test_progressive_reception_consolidates_unique_sops_and_freezes_old_generations(ctx,mode):
    ctx.config["headers"]={"mode":mode}
    source=playground.demo_source(); profile=playground.PROFILES["profiles"][0]
    await playground.apply_indexes(ctx,{"profile":profile})
    first={"studies":source["studies"],"instances":source["instances"][:3],"profile":profile}
    received=await playground.receive(ctx,first)
    partial=await playground.process(ctx,{"batchId":received["batchId"]})
    assert partial["counts"]["uniqueSops"]==3
    assert partial["collections"]["research"][0]["counts"]["state"]=="partial_or_unknown"
    second={"studies":source["studies"],"instances":source["instances"][3:],"profile":profile}
    another=await playground.receive(ctx,second)
    full=await playground.process(ctx,{"batchId":another["batchId"]})
    assert full["counts"]["uniqueSops"]==12
    assert full["generationId"]!=partial["generationId"]
    assert full["collections"]["research"][0]["counts"]["receivedUniqueSops"]==6
    assert not full["publishedCorpus"]
    again=await playground.receive(ctx,first)
    assert again["batchId"]==received["batchId"]
    replay=await playground.process(ctx,{"batchId":again["batchId"]})
    assert replay["reused"] and replay["generationId"]==partial["generationId"]
    names=rules.collection_names(rules.config(ctx))
    assert await ctx.adapters["storage"].db[names["inventory"]].count_documents({})==12
    if mode=="bundle":
        instance=await ctx.adapters["storage"].find_one(names["inventory"],{"_id":source["instances"][0]["_id"]})
        assert "header" not in instance and "ContentIndex" not in instance
        assert instance["_kehrnel"]["headerRef"]
    query={"profile":profile,"generationId":full["generationId"],"allowPartial":True,
           "text":"Find CT series where rows = 1024 and photometric = MONOCHROME2"}
    assert len((await playground.execute(ctx,query))["rows"])==1
    assert (await playground.execute(ctx,{**query,"group":"rendering"}))["rows"]==[]
    assert (await playground.execute(ctx,{**query,"text":"Find CT series where rows > 600 and rows < 800"}))["rows"]==[]
    explanation=await ctx.adapters["storage"].db.command({"explain":{"aggregate":names["research"],
        "pipeline":rules.compile_query(ctx,{**query,"text":"Find CT series where manufacturer = SYNTHETIC_VENDOR_B"})["pipeline"],"cursor":{}},"verbosity":"executionStats"})
    assert "idx_dicom_rule_strings" in str(explanation)


@pytest.mark.asyncio
async def test_conflicting_sop_reception_does_not_replace_inventory(ctx):
    source=playground.demo_source(); profile=playground.PROFILES["profiles"][0]
    first=await playground.receive(ctx,{**source,"profile":profile})
    await playground.process(ctx,{"batchId":first["batchId"]})
    changed=copy.deepcopy(source); changed["instances"][0]["sz"]=100
    second=await playground.receive(ctx,{**changed,"profile":profile})
    with pytest.raises(KehrnelError) as error:
        await playground.process(ctx,{"batchId":second["batchId"]})
    assert error.value.code=="DICOM_RECEPTION_CONFLICT"
    names=rules.collection_names(rules.config(ctx))
    row=await ctx.adapters["storage"].find_one(names["inventory"],{"_id":source["instances"][0]["_id"]})
    assert row["sz"]==0


@pytest.mark.asyncio
async def test_unchanged_raw_sop_cannot_silently_move_to_another_center(ctx):
    source=playground.demo_source(); profile=playground.PROFILES['profiles'][0]
    received=await playground.receive(ctx,{**source,'profile':profile})
    await playground.process(ctx,{'batchId':received['batchId']})
    changed=copy.deepcopy(source)
    changed['studies'][0]['dm']='DEMO_B'
    changed['studies'][0]['se'][0]['ci']='DEMO_B'
    next_batch=await playground.receive(ctx,{**changed,'profile':profile})
    with pytest.raises(KehrnelError) as error:
        await playground.process(ctx,{'batchId':next_batch['batchId']})
    assert error.value.code=='DICOM_RECEPTION_CONFLICT'


@pytest.mark.asyncio
async def test_rule_change_creates_new_generation_and_rejects_stale_queries(ctx):
    source=playground.demo_source(); profile=playground.PROFILES["profiles"][0]
    first=await playground.receive(ctx,{**source,"profile":profile}); old=await playground.process(ctx,{"batchId":first["batchId"]})
    edited=copy.deepcopy(profile); edited["fields"][0]["aggregate"]="common"
    second=await playground.receive(ctx,{**source,"profile":edited}); new=await playground.process(ctx,{"batchId":second["batchId"]})
    assert old["generationId"]!=new["generationId"]
    query={"profile":edited,"generationId":old["generationId"],"allowPartial":True,"text":"Find series where manufacturer = SYNTHETIC_VENDOR_B"}
    with pytest.raises(KehrnelError) as error:
        await playground.execute(ctx,query)
    assert error.value.code=="DICOM_PROFILE_STALE"


@pytest.mark.asyncio
async def test_failed_publication_is_hidden_and_can_be_retried(ctx,monkeypatch):
    source=playground.demo_source(); profile=playground.PROFILES["profiles"][0]
    received=await playground.receive(ctx,{**source,"profile":profile})
    db=ctx.adapters["storage"]; original=db.update_one
    names=rules.collection_names(rules.config(ctx))
    async def broken(collection,*args,**kwargs):
        if collection==names["generations"]: raise RuntimeError("Simulated publication failure")
        return await original(collection,*args,**kwargs)
    monkeypatch.setattr(db,"update_one",broken)
    with pytest.raises(RuntimeError): await playground.process(ctx,{"batchId":received["batchId"]})
    assert await db.db[names["generations"]].count_documents({})==0
    monkeypatch.setattr(db,"update_one",original)
    published=await playground.process(ctx,{"batchId":received["batchId"]})
    assert published["state"]=="processed"
    assert await db.db[names["inventory"]].count_documents({})==12
