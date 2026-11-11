"""Hosted routing must never turn a tenant binding into a catalog copy/write."""
import copy
from types import SimpleNamespace
import pytest
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.snomedct.mongodb import source_policy as policy
from kehrnel.engine.strategies.snomedct.mongodb.strategy import SNOMEDCTMongoDBStrategy, _config, _db, _aggregate, _search_pipeline, _hybrid_search_pipeline
from kehrnel.engine.domains.snomedct.retrieval import build_vector_search_pipeline
from kehrnel.engine.core.runtime import StrategyRuntime

PACK = 'src/kehrnel/engine/strategies/snomedct/mongodb'

def context():
    manifest = load_strategy('snomedct.mongodb', PACK)
    config = copy.deepcopy(manifest.default_config)
    config['database'] = 'tenant_a_snomedct'
    return StrategyContext(environment_id='tenant-a', config=config, manifest=manifest, adapters={'storage': SimpleNamespace(db=FakeDB())}, bindings={'db': {'uri': 'mongodb://tenant-host', 'database': 'tenant_a_snomedct'}})

class FakeCollection:
    def __init__(self): self.calls=[]
    def aggregate(self, pipeline, **kwargs):
        self.calls.append((pipeline, kwargs))
        async def rows():
            yield {'conceptId': '73211009', 'term': 'Diabetes mellitus'}
        return rows()
    async def insert_one(self, value): self.calls.append(value)

class FakeDB:
    def __init__(self): self.collections={}
    def __getitem__(self, name): return self.collections.setdefault(name, FakeCollection())

@pytest.mark.parametrize('operation', sorted(policy.HOSTED_FORBIDDEN_OPS))
@pytest.mark.asyncio
async def test_hosted_writes_are_denied_before_any_io(operation, monkeypatch):
    ctx=context()
    monkeypatch.delenv('SNOMED_AUTHORITY_URI', raising=False)
    strategy=SNOMEDCTMongoDBStrategy(ctx.manifest)
    for call in [lambda: strategy.run_op(ctx, operation, {'license_acknowledged': True}), lambda: getattr(strategy, operation)(ctx, {'license_acknowledged': True})]:
        with pytest.raises(KehrnelError) as exc: await call()
        assert exc.value.code == 'SNOMED_HOSTED_READ_ONLY'
    assert not ctx.adapters['storage'].db.collections

@pytest.mark.asyncio
async def test_catalog_reads_and_workspace_writes_route_to_distinct_databases(monkeypatch):
    ctx=context(); catalog=FakeDB()
    monkeypatch.setattr(policy, 'catalog_database', lambda cfg: policy.CatalogDatabase(catalog, ['snomed-irbd', 'snomed-term-search']))
    # A tenant's general aggregate adapter must not receive the reference query.
    async def wrong(*args, **kwargs): raise AssertionError('reference query used tenant aggregate adapter')
    ctx.adapters['storage'].aggregate=wrong
    cfg=_config(ctx)
    collection, pipeline=_search_pipeline(cfg, {'q':'diab', 'language':'en', 'limit':10})
    rows=await _aggregate(ctx, collection, pipeline, hint='legacy_index')
    assert rows[0]['conceptId']=='73211009'
    assert catalog[collection].calls[0][1].get('hint') is None
    assert not ctx.adapters['storage'].db.collections
    await _db(ctx)['snomed_grounding_reviews'].insert_one({'reviewId':'own-review'})
    assert ctx.adapters['storage'].db['snomed_grounding_reviews'].calls == [{'reviewId':'own-review'}]
    with pytest.raises(KehrnelError): await _db(ctx)['snomed-irbd'].insert_one({'conceptId':'forged'})

@pytest.mark.parametrize('pipeline', [
    [{'$merge':'snomed-irbd'}], [{'$facet':{'x':[{'$out':'snomed-irbd'}]}}],
    [{'$lookup':{'from':'private_users','as':'x'}}], [{'$unionWith':'private_users'}],
    [{'$graphLookup':{'from':'private_users'}}],
    [{'$unionWith':{'db':'another_tenant','coll':'snomed-irbd'}}],
])
def test_reference_pipelines_cannot_write_or_join_outside_authority(pipeline):
    with pytest.raises(KehrnelError): policy.safe_pipeline(pipeline, {'snomed-irbd','snomed-term-search'})

def test_client_configuration_cannot_replace_authority_or_release():
    ctx=context();ctx.config['source'].update(database='private', uri_env='ATTACKER_URI', concepts='private')
    ctx.config['release']['id']='forged'
    ctx.config['collections']['concepts']='private'
    cfg=_config(ctx)
    assert cfg['source']=={'mode':'hosted_reference','id':'healthcaredatalab'}
    assert cfg['release']['id']=='20260601'
    assert cfg['collections']['concepts']=='snomed-irbd'
    assert 'uri_env' not in policy.evidence(cfg)

@pytest.mark.parametrize('mode', ['hosted_reference','customer_owned'])
def test_authority_cannot_become_workspace_or_ingestion_destination(mode, monkeypatch):
    ctx=context();ctx.config['database']='terminology';ctx.config['source']['mode']=mode
    ctx.bindings['db']['uri']='mongodb+srv://tenant:secret@shared.example'
    monkeypatch.setenv('SNOMED_AUTHORITY_URI','mongodb+srv://operator:secret@shared.example')
    with pytest.raises(KehrnelError) as exc: StrategyRuntime._bind_strategy_database(ctx.manifest,ctx.config,ctx.bindings)
    assert exc.value.code=='SNOMED_REFERENCE_PROTECTED'

def test_legacy_activation_remains_customer_owned():
    ctx=context();ctx.config['source'].pop('mode')
    assert _config(ctx)['source']['mode']=='customer_owned'

@pytest.mark.asyncio
async def test_missing_reference_credential_fails_without_using_tenant_catalog(monkeypatch):
    ctx=context();monkeypatch.delenv('SNOMED_AUTHORITY_URI', raising=False)
    with pytest.raises(KehrnelError) as exc: _db(ctx)
    assert exc.value.code=='SNOMED_REFERENCE_NOT_CONNECTED'
    assert not ctx.adapters['storage'].db.collections

def test_typing_and_filtered_search_use_indexed_pre_filters_without_global_sort():
    cfg=_config(context())
    for query in [{'q':'di','language':'es'}, {'q':'diab','language':'en','ancestor_id':'73211009','area_tag':'disorder','semantic_tag_key':'disorder'}]:
        _, pipeline=_search_pipeline(cfg,query)
        assert pipeline[0]['$search']['index']=='snomed_text_idx'
        assert not any('$sort' in stage or '$match' in stage for stage in pipeline)
        filters=pipeline[0]['$search']['compound']['filter']
        assert {'equals':{'path':'languageCode','value':query['language']}} in filters
    _, pipeline, explain=_hybrid_search_pipeline(cfg,{'q':'diab','ancestor_id':'73211009'})
    assert {'equals':{'path':'ancestorIds','value':'73211009'}} in pipeline[0]['$search']['compound']['filter']
    assert explain['hint'] is None

def test_auto_embedding_sends_text_in_query_instead_of_query_vector():
    stage=build_vector_search_pipeline(query='heart failure',limit=10,index_name='snomed_voyage_idx',path='embedText')[0]['$vectorSearch']
    assert stage['query']=='heart failure'
    assert 'queryVector' not in stage


def test_expansion_limit_handles_full_release_without_changing_search_limit():
    from kehrnel.engine.strategies.snomedct.mongodb.strategy import _limit, _release_id
    cfg=_config(context())
    assert _limit(cfg, {'mode':'ecl','limit':128})==128
    assert _limit(cfg, {'q':'diabetes','limit':128})==100
    with pytest.raises(KehrnelError) as exc: _release_id(cfg, {'release_id':'forged'})
    assert exc.value.code=='SNOMED_RELEASE_MISMATCH'


def test_authority_evidence_excludes_credentials_and_requires_pinned_hosted_identity():
    from pydantic import ValidationError
    from kehrnel.contracts.terminology_authority import TerminologyAuthorityEvidence
    source=policy.evidence(_config(context()))
    assert TerminologyAuthorityEvidence.model_validate(source).model_dump(by_alias=True)==source
    for key in ['uri', 'uri_env', 'password', 'database']:
        with pytest.raises(ValidationError): TerminologyAuthorityEvidence.model_validate({**source,key:'private'})
    with pytest.raises(ValidationError): TerminologyAuthorityEvidence.model_validate({**source,'readOnly':False})


@pytest.mark.asyncio
async def test_hosted_capabilities_do_not_advertise_release_files_or_ingestion():
    ctx=context();strategy=SNOMEDCTMongoDBStrategy(ctx.manifest)
    result=await strategy.run_op(ctx,'snomed_capabilities',{'include_readiness':False})
    assert result['source']['readOnly'] is True
    assert result['releaseLifecycle']['stagingDirectory'] is None
    assert not set(result['domainOperations']) & policy.HOSTED_FORBIDDEN_OPS
    assert not set(result['releaseLifecycle']['operations']) & policy.HOSTED_FORBIDDEN_OPS


@pytest.mark.asyncio
async def test_projection_operator_requires_separate_credentials_before_writing(monkeypatch):
    import runpy
    module=runpy.run_path('scripts/snomed/build_projection.py')
    monkeypatch.setenv('SNOMED_AUTHORITY_URI','mongodb://shared.example')
    monkeypatch.delenv('SNOMED_PROJECTION_OPERATOR_URI',raising=False)
    with pytest.raises(ValueError):
        await module['run'](SimpleNamespace(source_id='healthcaredatalab',projection_id='new-release',concept_limit=0,sample_size=10,languages=['en'],apply=True))
