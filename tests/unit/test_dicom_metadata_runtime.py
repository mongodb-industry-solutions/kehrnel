import copy
import json
from types import SimpleNamespace
from pathlib import Path
import pytest
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.dicom.metadata import bson_bytes
from kehrnel.engine.domains.dicom.metadata_explorer import MetadataExplorer, native_filter
from kehrnel.engine.strategies.dicom.tiered_store import metadata_runtime as runtime

@pytest.fixture
def ctx():return StrategyContext(environment_id="metadata-test",config={"tenant_id":"test","allowed_centers":["A","B"]})

def test_public_sample_is_pinned_and_balanced():
    e=MetadataExplorer()
    assert len(e.rows)==199 and len(e.fields)==159
    assert len({r['header']['00080016']['Value'][0] for r in e.rows})==25
    assert len({r['header']['00080060']['Value'][0] for r in e.rows})==18

def test_tampered_domain_asset_fails_closed(tmp_path):
    import shutil
    e=MetadataExplorer();root=Path(__import__('kehrnel.engine.domains.dicom.metadata_explorer',fromlist=['']).__file__).with_name('samples')/'idc-metadata/1.0.0'
    shutil.copytree(root,tmp_path/'source');(tmp_path/'source/metadata_registry.json').write_text('{}')
    with pytest.raises(ValueError,match='checksum'):MetadataExplorer(root=tmp_path/'source')

def test_selected_series_cannot_silently_switch():
    with pytest.raises(ValueError,match='selected series'):MetadataExplorer().preview({'seriesUid':'1.2.3.4.5','scope':{'modalities':['CT']}})

def test_byte_budget_includes_final_detail_flags():
    e=MetadataExplorer()
    p=e.preview({'scope':{'modalities':['SR']},'fields':[], 'limits':{'maxSummaryBytes':4096}})
    assert bson_bytes(p['summary'])<=4096
    assert p['summary']['detail']['overflow']

def test_both_bounds_bind_to_one_native_vm_member():
    p=native_filter({'range':{'path':'se.summary.acquisitionGroups.values.pixelSpacing','gte':1,'lte':3}})
    assert p['$or'][1]['se.summary.acquisitionGroups.values.pixelSpacing']=={'$elemMatch':{'$gte':1,'$lte':3}}
    assert '$not' in p['$or'][0]['$and'][0]['se.summary.acquisitionGroups.values.pixelSpacing']

@pytest.mark.asyncio
async def test_query_scope_and_primary_collection(ctx):
    p=await runtime.query(ctx,{'kind':'searchCombined','modality':'CT','minimum':0,'maximum':2,'centerIds':['A','B']})
    assert p['collection']=='dicom_studies' and not p['extensionLookup']
    assert p['pipeline'][0]['$search']['compound']['filter'][:2]==[{'equals':{'path':'tenantId','value':'test'}},{'in':{'path':'centerId','value':['A','B']}}]
    assert not p['executed']
    with pytest.raises(KehrnelError) as error:await runtime.query(ctx,{'kind':'searchCombined','centerIds':['C']})
    assert error.value.code=='DICOM_CENTER_FORBIDDEN'

@pytest.mark.asyncio
async def test_native_and_text_contracts(ctx):
    p=await runtime.query(ctx,{'kind':'searchCombined','manufacturer':'A','minimum':1,'maximum':3,'engine':'native'})
    assert '$match' in p['pipeline'][0] and '$elemMatch' in p['pipelineNative']
    with pytest.raises(KehrnelError):await runtime.query(ctx,{'kind':'fuzzy','engine':'native'})

@pytest.mark.asyncio
async def test_scope_preserved_on_custom_headers_and_duplicate_rejected(ctx):
    h=copy.deepcopy(runtime.explorer().rows[0]['header']);p=await runtime.preview(ctx,{'instances':[{'header':h}],'centerId':'B','fields':['manufacturer']})
    assert p['document']['_kehrnel']['centerId']=='B'
    changed=copy.deepcopy(h);changed['00080070']={'vr':'LO','Value':['OTHER']}
    with pytest.raises(KehrnelError) as error:await runtime.preview(ctx,{'instances':[{'header':h},{'header':changed}]})
    assert error.value.code=='DICOM_RECEPTION_CONFLICT'

class Store:
    def __init__(self,existing=None,matched=1):self.existing=copy.deepcopy(existing);self.matched=matched;self.writes=[];self.pipeline=None
    async def find_one(self,name,query):return copy.deepcopy(self.existing)
    async def update_one(self,name,query,update,upsert=False):
        self.writes.append((name,query,update,upsert));return SimpleNamespace(matched_count=self.matched)
    async def aggregate(self,name,pipeline,**kwargs):self.pipeline=pipeline;return []

@pytest.mark.asyncio
async def test_publication_requires_explicit_series_snapshot(ctx):
    with pytest.raises(KehrnelError) as error:await runtime.publish(ctx,{'sourceMode':'complete_selected_series_snapshot'})
    assert error.value.code=='DICOM_SOURCE_SERIES_REQUIRED'

@pytest.mark.asyncio
async def test_publication_preserves_counts_other_series_and_operational_revision(ctx):
    e=runtime.explorer();uid=next(iter(e.by_series));p={'seriesUid':uid,'fields':['manufacturer'],'sourceMode':'complete_selected_series_snapshot'}
    incoming=(await runtime.preview(ctx,p))['document'];existing={**incoming,'se':[{'si':uid,'ni':500,'sz':1234,'_kehrnel':{'original':'keep'}},{'si':'1.2.3.9','ni':5,'mo':'MR'}]}
    existing['_kehrnel']['source_revision']='operational';existing['_kehrnel']['modalities']=['CT','MR']
    db=Store(existing);ctx.adapters={'storage':db};r=await runtime.publish(ctx,p)
    assert r['persisted'] and r['document']['se'][0]['ni']==500 and r['document']['se'][0]['sz']==1234
    assert r['document']['se'][1]==existing['se'][1]
    flt=db.writes[-1][1];assert flt['se']==existing['se'] and flt['_kehrnel']==existing['_kehrnel']
    assert r['document']['_kehrnel']['source_revision']=='operational'

@pytest.mark.asyncio
async def test_concurrent_publish_fails(ctx):
    uid=next(iter(runtime.explorer().by_series));p={'seriesUid':uid,'sourceMode':'complete_selected_series_snapshot'}
    existing=(await runtime.preview(ctx,p))['document'];ctx.adapters={'storage':Store(existing,matched=0)}
    with pytest.raises(KehrnelError) as error:await runtime.publish(ctx,p)
    assert error.value.code=='DICOM_SUMMARY_REVISION_CONFLICT'

@pytest.mark.asyncio
async def test_search_readiness_blocks_execution(ctx):
    class Search:
        async def list_search_indexes(self,name):return [{'name':'dicom_studies_metadata','status':'BUILDING','queryable':True}]
    ctx.adapters={'storage':Store(),'atlas_search':Search()}
    with pytest.raises(KehrnelError) as error:await runtime.query(ctx,{'kind':'fuzzy','execute':True,'allowPartial':True})
    assert error.value.code=='DICOM_SEARCH_NOT_READY'

@pytest.mark.asyncio
async def test_retained_rebuild_is_scoped_and_reports_missing_metadata(ctx):
    db=Store();ctx.adapters={'storage':db}
    with pytest.raises(KehrnelError) as error:await runtime.rebuild_series(ctx,{'seriesUid':'1.2.3','catalog':'rule_inventory','centerId':'B'})
    assert error.value.code=='DICOM_METADATA_SOURCE_NOT_AVAILABLE'
    assert db.pipeline[0]['$match']=={'_kehrnel.tenantId':'test','_kehrnel.centerId':'B','_kehrnel.seriesUid':'1.2.3'}
    assert db.pipeline[-1]['$limit']==501

@pytest.mark.asyncio
async def test_index_promotion_retains_older_fields(ctx):
    e=runtime.explorer()
    class Search:
        async def list_search_indexes(self,name):return [{'name':'dicom_studies_metadata','latestDefinition':e.search_mapping(['rows'])}]
        async def ensure_search_index(self,col,name,definition):self.definition=definition;return {}
    a=Search();ctx.adapters={'atlas_search':a};await runtime.apply_search(ctx,{'fields':['manufacturer']})
    paths=e.mapping_paths(a.definition)
    assert 'se.summary.acquisitionGroups.values.rows' in paths and 'se.summary.acquisitionGroups.values.manufacturer' in paths


def test_source_rule_bounds_cannot_cross_vm_values():
    e=MetadataExplorer();row=copy.deepcopy(e.rows[0]);row['header']['00280030']={'vr':'DS','Value':['1','5']};row['header']['00080016']={'vr':'UI','Value':['1.2.840.10008.5.1.4.1.1.2']};row['source'].pop('sourceHeaderSha256',None)
    p=MetadataExplorer(records=[row]).preview({'when':[{'field':'pixelSpacing','op':'gte','value':2},{'field':'pixelSpacing','op':'lte','value':4}]})
    assert p['affectedSeries']==0


@pytest.mark.asyncio
async def test_saved_rule_history_is_scoped_and_bounded(ctx):
    db=Store();ctx.adapters={'storage':db};r=await runtime.saved_rules(ctx,{'centerId':'B','ruleId':'version-id'})
    assert r['collection']=='dicom_studies_metadata_rules'
    assert db.pipeline[0]['$match']=={'tenantId':'test','centerId':'B','_id':'version-id'}
    assert db.pipeline[-1]['$limit']==25
