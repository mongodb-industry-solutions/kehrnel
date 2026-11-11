"""Class-aware metadata operations shared by the SDK, HTTP and HDL surfaces.

The publisher is a bounded, optimistic study-summary update. It is not a
partitioned archive scheduler, a pixel receiver or an exactly-once ingest engine.
"""
from __future__ import annotations
import copy, json
from functools import lru_cache
from datetime import datetime, timezone
from kehrnel.engine.domains.dicom.metadata_explorer import MetadataExplorer
from kehrnel.engine.domains.dicom.metadata import compact, first, native, hash_value, bson_bytes
from .common import config, centers, storage, fail, bounded_bson, digest

@lru_cache(maxsize=1)
def explorer():
    return MetadataExplorer()

def owner(ctx, payload):
    cfg=config(ctx);center=payload.get('centerId') or centers(ctx,cfg)[0]
    if center not in centers(ctx,cfg):fail('DICOM_CENTER_FORBIDDEN','Choose a center authorized by this activation.',403)
    return cfg,center

def selected_explorer(ctx,p):
    cfg,center=owner(ctx,p)
    if 'instances' not in p:return explorer(),cfg,center
    raw=p['instances'];bounded_bson({'instances':raw},cfg['ingest']['max_bytes'])
    if not 0<len(raw)<=cfg['ingest']['max_documents']:fail('DICOM_INGEST_BOUND_EXCEEDED','Use a bounded header batch.')
    rows=[];seen={}
    for item in raw:
        h=item.get('header')
        if not isinstance(h,dict):fail('DICOM_HEADER_REQUIRED','Each source instance needs a DICOM JSON header.')
        from .model import uid
        sop=uid(first(h,'00080018'));uid(first(h,'0020000D'));uid(first(h,'0020000E'))
        if item.get('_id',sop)!=sop:fail('DICOM_HIERARCHY_CONFLICT','Source identity disagrees with its SOP UID.')
        source=compact({'collectionId':'connected-source','datasetKind':'customer-provided metadata','sourceHeaderSha256':hash_value(h)})
        # The extractor's canonical hash intentionally differs from persistence envelope digests.
        import hashlib
        source['sourceHeaderSha256']=hashlib.sha256(json.dumps(h,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        if sop in seen and seen[sop]!=source['sourceHeaderSha256']:fail('DICOM_RECEPTION_CONFLICT','One batch has conflicting content for a SOP.')
        if sop in seen:continue
        seen[sop]=source['sourceHeaderSha256'];rows.append({'header':copy.deepcopy(h),'source':source})
    return MetadataExplorer(records=rows),cfg,center

def scope_pipeline(pipeline, cfg, center, collection):
    # Replace only fixed engine-owned scope and demo collection/index identifiers.
    value=copy.deepcopy(pipeline)
    def visit(v):
        if isinstance(v,list):
            for item in v:visit(item)
        elif isinstance(v,dict):
            for key,item in list(v.items()):
                if key=='tenantId' and item=='ctti_demo':v[key]=cfg['tenant_id']
                if key=='centerId' and item=='IDC_PUBLIC':v[key]={'$in':center} if isinstance(center,list) else center
                if key=='equals' and isinstance(item,dict):
                    if item.get('path')=='tenantId':item['value']=cfg['tenant_id']
                    if item.get('path')=='centerId':
                        if isinstance(center,list):v['in']={'path':'centerId','value':center};del v[key]
                        else:item['value']=center
                if key=='index' and item=='dicom_evidence':v[key]=collection+'_metadata'
                if key=='from' and item=='evidence_extensions':v[key]=cfg['collections']['research']+'_metadata'
                visit(item)
    visit(value);return value

def rescope_preview(result,cfg,center):
    result=copy.deepcopy(result)
    if 'document' not in result:return result
    d=result['document'];d['tenantId']=cfg['tenant_id'];d['centerId']=center
    d['_id']=d['studyUid'] if result['config']['destination']=='shared' else digest([cfg['tenant_id'],center,d['studyUid'],d['seriesUid'],d['ruleVersion']])
    d['_kehrnel']={'tenantId':cfg['tenant_id'],'centerId':center,'studyUid':d['studyUid'],'storage_schema_version':'1.0.0','metadata_contract_version':result['summary']['schemaVersion'],'metadataRevision':result['summary']['sourceRevision'],'ruleVersion':d['ruleVersion']}
    if result['config']['destination']=='shared':
        d['dm']=center
        if d.get('studyDate') is not None:d['dt']=d['studyDate'];d['_kehrnel']['studyDate']=d['studyDate'];d['_kehrnel']['day']=d['studyDate'].strftime('%Y-%m-%d')
        d['_kehrnel']['modalities']=sorted({s['mo'] for s in d['se'] if s.get('mo')})
    ref={'collection':cfg['collections']['studies']+'_metadata_rules','id':digest([cfg['tenant_id'],center,d['ruleVersion']])}
    if result['config']['destination']=='shared':
        for series in d['se']:series['ruleVersion']=d['ruleVersion'];series['ruleRef']=ref
    else:d['ruleRef']=ref
    result['ruleRef']=ref
    result['summaryBsonBytes']=bson_bytes(result['summary'])
    result['documentNative']=native(d)
    result['documentBsonBytes']=bson_bytes(d)
    collection=cfg['collections']['studies'];result['collection']=collection if result['config']['destination']=='shared' else cfg['collections']['research']+'_metadata'
    result['queryNative']=native(scope_pipeline(explorer().search_parameter_query(result['summary']),cfg,center,collection))
    result['indexPlan'].update({'collection':collection,'index':collection+'_metadata','productionCollection':collection})
    result['persisted']=False;return result

async def catalog(ctx,p):
    cfg=config(ctx);e=explorer();r=e.state()
    return {'ok':True,'contract':'dicom-metadata-catalog/v2','strategyId':'dicom.tiered_store','registry':r['registry'],'sourceCatalog':r['sourceCatalog'],'report':r['report'],'sourceManifest':r['sourceManifest'],'authorizedCenters':centers(ctx,cfg),'collection':cfg['collections']['studies'],'ruleCollection':cfg['collections']['studies']+'_metadata_rules','summaryPath':'se[].summary','source':'pinned public metadata; pixel images remain in the showcase image release','publication':'bounded optimistic update; not a full corpus scheduler','operations':['dicom_metadata_preview','dicom_metadata_rules','dicom_metadata_source','dicom_metadata_publish','dicom_metadata_rebuild_series','dicom_metadata_search_plan','dicom_metadata_apply_search','dicom_metadata_query'],'semanticSearch':'design only; no embedding model or vector index','narrative':{'public':'Question → real source facts → study summary → query → architectural reason','developer':'Activate → inspect source → preview rules → receive/consolidate → publish summary → inspect indexes → query','sdk':'One class-aware rule and query contract through SDK, CLI and HTTP'}}

async def preview(ctx,p):
    e,cfg,center=selected_explorer(ctx,p)
    try:result=e.preview(p)
    except (ValueError,KeyError,TypeError) as exc:fail('DICOM_METADATA_INVALID',str(exc))
    return {'ok':True,'contract':'dicom-metadata-preview/v2',**rescope_preview(result,cfg,center)}

async def source(ctx,p):
    owner(ctx,p)
    try:return {'ok':True,**explorer().source(p['sopUid'],p.get('text',''))}
    except ValueError as exc:fail('DICOM_SAMPLE_INVALID',str(exc))

async def search_plan(ctx,p):
    cfg,center=owner(ctx,p);mapping=explorer().search_mapping(p.get('fields'))
    indexes=[];adapter=(ctx.adapters or {}).get('atlas_search')
    if adapter is not None:indexes=await adapter.list_search_indexes(cfg['collections']['studies'])
    index=next((i for i in indexes if i.get('name')==cfg['collections']['studies']+'_metadata'),{})
    plan=explorer().mapping_plan(mapping,index.get('latestDefinition',{}) if adapter is not None else None)
    plan.update({'collection':cfg['collections']['studies'],'index':cfg['collections']['studies']+'_metadata','indexReady':index.get('status')=='READY' and bool(index.get('queryable')),'discovery':'adapter query; empty can mean unavailable as well as absent' if adapter else 'no atlas_search binding'})
    return {'ok':True,'plan':plan,'mappingNative':native(mapping),'status':indexes}

async def apply_search(ctx,p):
    cfg,center=owner(ctx,p);adapter=(ctx.adapters or {}).get('atlas_search')
    if adapter is None:fail('DICOM_SEARCH_ADAPTER_REQUIRED','Bind Atlas Search to request a metadata index.',503)
    existing=await adapter.list_search_indexes(cfg['collections']['studies'])
    index=next((i for i in existing if i.get('name')==cfg['collections']['studies']+'_metadata'),{})
    paths=explorer().mapping_paths(index.get('latestDefinition',{}));prefix='se.summary.acquisitionGroups.values.'
    prior={path[len(prefix):] for path in paths if path.startswith(prefix) and path[len(prefix):] in explorer().fields}
    keys=sorted(prior|set(p.get('fields',explorer().default_search_keys())))
    mapping=explorer().search_mapping(keys)
    result=await adapter.ensure_search_index(cfg['collections']['studies'],cfg['collections']['studies']+'_metadata',mapping)
    return {'ok':not bool(result.get('warnings')),'request':result,'mappingNative':native(mapping),'readiness':'asynchronous; compare again before query execution','semanticSearch':'no vector index requested'}

async def publish(ctx,p):
    if not p.get('seriesUid'):fail('DICOM_SOURCE_SERIES_REQUIRED','Select the exact series to publish; previewing all candidates does not publish them.')
    if p.get('sourceMode')!='complete_selected_series_snapshot':fail('DICOM_SOURCE_SNAPSHOT_REQUIRED','Publication needs an accepted snapshot of each selected series; use reception/consolidation for arrival deltas.')
    result=await preview(ctx,p)
    if 'document' not in result:fail('DICOM_METADATA_EMPTY','No source matches this rule.')
    cfg,center=owner(ctx,p);db=storage(ctx);incoming=result['document'];name=result['collection']
    rule={'_id':result['ruleRef']['id'],'tenantId':cfg['tenant_id'],'centerId':center,'ruleVersion':incoming['ruleVersion'],'config':result['config'],'createdAt':datetime.now(timezone.utc),'schemaVersion':'dicom-metadata-rule/v2'}
    await db.update_one(result['ruleRef']['collection'],{'_id':rule['_id']},{'$setOnInsert':rule},upsert=True)
    existing=await db.find_one(name,{'_id':incoming['_id']})
    if existing and (existing.get('_kehrnel',{}).get('tenantId'),existing.get('_kehrnel',{}).get('centerId'))!=(cfg['tenant_id'],center):fail('DICOM_RECEPTION_CONFLICT','The stored study is owned by another scope.',409)
    if result['config']['destination']=='shared':
        by_series={s['si']:copy.deepcopy(s) for s in (existing or {}).get('se',[])}
        for s in incoming['se']:
            previous=by_series.get(s['si'],{})
            # Summary scope may cover only matching instances. It cannot replace operational counts.
            update={k:v for k,v in s.items() if k not in {'ni','sz'}}
            by_series[s['si']]={**previous,**update}
        incoming.pop('dt',None);incoming.pop('description',None);incoming.pop('studyDate',None)
        # Parent demographics and date corrections belong to operational ingestion.
        if not existing:
            e,_,_=selected_explorer(ctx,p);h=e.by_series[p['seriesUid']][0]['header']
            patient=first(h,'00100020')
            if patient:incoming['pi']=patient;incoming['_kehrnel']['patientId']=patient
            if result['document'].get('studyDate') is not None:incoming['dt']=result['document']['studyDate'];incoming['studyDate']=result['document']['studyDate']
            if result['document'].get('description'):incoming['description']=result['document']['description'];incoming['descriptionNorm']=incoming['description'].casefold()
        incoming['se']=list(by_series.values())
        if existing:
            derived={k:v for k,v in incoming['_kehrnel'].items() if k in {'metadata_contract_version','metadataRevision','ruleVersion'}}
            incoming['_kehrnel']={**existing.get('_kehrnel',{}),**derived}
        else:incoming['_kehrnel']['modalities']=sorted({s['mo'] for s in incoming['se'] if s.get('mo')})
    revision=digest([incoming.get('se'),incoming.get('ruleVersion')]);incoming['_kehrnel']['metadataRevision']=revision
    bounded_bson(incoming)
    if existing:
        old=existing.get('_kehrnel',{}).get('metadataRevision');flt={'_id':incoming['_id'],'_kehrnel.tenantId':cfg['tenant_id'],'_kehrnel.centerId':center,'_kehrnel.metadataRevision':old if old is not None else {'$exists':False}}
        flt['_kehrnel']=existing.get('_kehrnel',{})
        if result['config']['destination']=='shared':flt['se']=existing.get('se',[])
        update={k:v for k,v in incoming.items() if k!='_id'}
        written=await db.update_one(name,flt,{'$set':update},upsert=False)
        if getattr(written,'matched_count',0)!=1:fail('DICOM_SUMMARY_REVISION_CONFLICT','The study changed during publication; preview and retry.',409)
    else:
        await db.update_one(name,{'_id':incoming['_id']},{'$setOnInsert':incoming},upsert=True)
        stored=await db.find_one(name,{'_id':incoming['_id']})
        if not stored or stored.get('_kehrnel',{}).get('metadataRevision')!=revision:fail('DICOM_SUMMARY_REVISION_CONFLICT','A competing publisher wrote another revision.',409)
    return {**result,'document':incoming,'documentNative':native(incoming),'documentBsonBytes':bson_bytes(incoming),'persisted':True,'metadataRevision':revision,'publication':'bounded series update; series remain open','receival':'This operation derives accepted metadata; binary/receipt ingestion stays a separate operation.'}

async def query(ctx,p):
    cfg,center=owner(ctx,p);e=explorer()
    if p.get('centerIds'):
        if not set(p['centerIds'])<=set(centers(ctx,cfg)):fail('DICOM_CENTER_FORBIDDEN','Query centers must be authorized.',403)
        center=p['centerIds']
    try:plan=e.query_plan(p)
    except (ValueError,KeyError,TypeError) as exc:fail('DICOM_METADATA_QUERY_INVALID',str(exc))
    pipeline=scope_pipeline(plan['pipeline'],cfg,center,cfg['collections']['studies']);result={'ok':True,**plan,'collection':cfg['collections']['studies'],'pipeline':pipeline,'pipelineNative':'db.'+cfg['collections']['studies']+'.aggregate('+native(pipeline)+')','executed':False}
    if not p.get('execute'):return result
    if not p.get('allowPartial'):fail('DICOM_PROJECTION_COVERAGE_UNKNOWN','Explicitly acknowledge bounded summary coverage.',409)
    db=storage(ctx)
    if plan['search']:
        adapter=(ctx.adapters or {}).get('atlas_search')
        if adapter is None:fail('DICOM_SEARCH_ADAPTER_REQUIRED','Search execution requires Atlas Search.',503)
        indexes=await adapter.list_search_indexes(cfg['collections']['studies']);index=next((i for i in indexes if i.get('name')==cfg['collections']['studies']+'_metadata'),{})
        if index.get('status')!='READY' or not index.get('queryable'):fail('DICOM_SEARCH_NOT_READY','Wait for a ready metadata index.',409)
        paths=explorer().mapping_paths(index.get('latestDefinition',{}))
        def check(v):
            if isinstance(v,list):
                for i in v:check(i)
            elif isinstance(v,dict):
                if 'path' in v:
                    for path in v['path'] if isinstance(v['path'],list) else [v['path']]:
                        if path not in paths:fail('DICOM_SEARCH_FIELD_MISSING','Promote '+path+' before executing.',409)
                for i in v.values():check(i)
        check(pipeline)
        # Atlas Search requires a compatible local read concern. No native-query concern is changed.
        from pymongo.read_concern import ReadConcern
        cursor=db.db[cfg['collections']['studies']].with_options(read_concern=ReadConcern('local')).aggregate(pipeline,maxTimeMS=15000,allowDiskUse=False)
        rows=[row async for row in cursor]
    else:rows=await db.aggregate(cfg['collections']['studies'],pipeline,allow_disk_use=False)
    return {**result,'rows':rows,'executed':True,'resultGrain':plan['resultGrain'],'coverage':'bounded summary data; Search freshness is asynchronous'}

async def rebuild_series(ctx,p):
    """Select immutable catalog locators once, then hydrate a bounded accepted snapshot."""
    from . import headers, rules
    cfg,center=owner(ctx,p);db=storage(ctx)
    name=rules.collection_names(cfg)['inventory'] if p.get('catalog','instances')=='rule_inventory' else cfg['collections']['instances']
    pipeline=[{'$match':{'_kehrnel.tenantId':cfg['tenant_id'],'_kehrnel.centerId':center,'_kehrnel.seriesUid':p['seriesUid']}},{'$sort':{'_id':1}},{'$limit':cfg['ingest']['max_documents']+1}]
    locators=await db.aggregate(name,pipeline,allow_disk_use=False)
    if not locators:fail('DICOM_METADATA_SOURCE_NOT_AVAILABLE','This series has no retained metadata in the selected catalog.',404)
    if len(locators)>cfg['ingest']['max_documents']:fail('DICOM_CONSOLIDATION_BOUND_EXCEEDED','Use the partitioned worker for larger series; no partial rebuild is published.',409)
    cache={};source=[]
    for doc in locators:
        source.append(await headers.original(ctx,cfg,doc,cache))
    request={k:v for k,v in p.items() if k not in {'catalog','publish'}}
    request['instances']=source
    result=await publish(ctx,{**request,'sourceMode':'complete_selected_series_snapshot'}) if p.get('publish') else await preview(ctx,request)
    return {**result,'sourceCatalog':name,'sourcePipelineNative':'db.'+name+'.aggregate('+native(pipeline)+')','reads':{'catalogLocators':len(locators),'headers':len(source),'objectAccess':'stable key and ordinal; bytes verified before extraction'},'snapshot':'the locators returned by one bounded catalog query; series remain open; later arrivals require another revision','fullCorpusRebuilt':False}

async def saved_rules(ctx,p):
    cfg,center=owner(ctx,p);name=cfg['collections']['studies']+'_metadata_rules'
    match={'tenantId':cfg['tenant_id'],'centerId':center}
    if p.get('ruleId'):match['_id']=p['ruleId']
    rows=await storage(ctx).aggregate(name,[{'$match':match},{'$sort':{'createdAt':-1,'_id':1}},{'$limit':25}],allow_disk_use=False)
    return {'ok':True,'collection':name,'rules':rows,'limit':25,'immutable':True,'meaning':'Summary ruleRef resolves its exact scope, predicates, fields and budgets.'}

async def dispatch(ctx,op,p):
    return await {'dicom_metadata_catalog':catalog,'dicom_metadata_rules':saved_rules,'dicom_metadata_preview':preview,'dicom_metadata_source':source,'dicom_metadata_publish':publish,'dicom_metadata_search_plan':search_plan,'dicom_metadata_apply_search':apply_search,'dicom_metadata_query':query,'dicom_metadata_rebuild_series':rebuild_series}[op](ctx,p)
