from pathlib import Path
import sys,json,hashlib,asyncio
from collections import defaultdict
import argparse,os
os.environ['KEHRNEL_SKIP_DOTENV']='true'
parser=argparse.ArgumentParser(description='Build the frozen metadata story from pinned Kehrnel domain samples.')
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
main=Path(__file__).resolve().parents[2];k=main
sys.path.insert(0,str(main/'src'))
import kehrnel.engine.domains.dicom as domain
import kehrnel.engine.strategies.dicom.tiered_store as strategy
domain.__path__.insert(0,str(k/'src/kehrnel/engine/domains/dicom'));strategy.__path__.insert(0,str(k/'src/kehrnel/engine/strategies/dicom/tiered_store'))
from kehrnel.engine.domains.dicom.metadata_explorer import MetadataExplorer,native_filter
from kehrnel.engine.domains.dicom.metadata import native,bson_bytes,first
from kehrnel.engine.domains.dicom.sample_data import load_sample
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.dicom.tiered_store.metadata_runtime import preview, scope_pipeline
from bson import json_util
source=[];parents={};groups=defaultdict(list)
image_fields=MetadataExplorer().default_search_keys()+['photometricInterpretation','samplesPerPixel','bitsAllocated','bitsStored','pixelSpacing','imageType','protocolName','laterality','bodyPartExamined','patientWeight','studyDescription','seriesDescription']
for variant in ['ct','mr','mg','linked-ct-pet']:
 data=load_sample('dicom.public-imaging','1.0.0',variant=variant)
 for study in data['studies']:parents[study['_id']]=study
 for r in data['instances']:
  h=r['header'];row={'header':h,'source':{'collectionId':'dicom.public-imaging','datasetKind':'licensed public metadata','sourceHeaderSha256':hashlib.sha256(json.dumps(h,sort_keys=True,separators=(',',':')).encode()).hexdigest()}}
  groups[first(h,'0020000E')].append(row)
rows={};cases=[];contexts=StrategyContext(environment_id='frozen-dicom-showcase',config={'tenant_id':'public_demo','allowed_centers':[p['dm'] for p in parents.values()]})
for uid,records in groups.items():
 study=parents[first(records[0]['header'],'0020000D')]
 result=asyncio.run(preview(contexts,{'instances':[{'header':r['header']} for r in records],'centerId':study['dm'],'fields':image_fields}));d=result['document'];rows[uid]={'summary':d['se'][0]['summary'],'documentNative':result['documentNative'],'summaryBsonBytes':result['summaryBsonBytes'],'fieldCount':len(result['summary'].get('coverage',{})),'schemaVersion':result['summary']['schemaVersion'],'sourceRevision':result['summary']['sourceRevision']}
 cases.append(d)
merged={}
for d in cases:
 if d['studyUid'] in merged:merged[d['studyUid']]['se'].extend(d['se'])
 else:merged[d['studyUid']]=d
cases=list(merged.values())
for uid,row in rows.items():row['documentNative']=native(next(d for d in cases if any(s['si']==uid for s in d['se'])))
e=MetadataExplorer();report=e.state()['report'];previews={}
for modality in ['CT','MR','MG','PT','US','SM','SR','SEG']:
 p=e.preview({'scope':{'modalities':[modality]}})
 previews[modality]={'modality':modality,'matchedInstances':p['matchedInstances'],'affectedSeries':p['affectedSeries'],'summaryBsonBytes':p['summaryBsonBytes'],'documentNative':p['documentNative'],'configNative':p['configNative'],'candidateSearchMappingNative':p['candidateSearchMappingNative'],'summary':p['summary'],'source':{k:v for k,v in e.source(p['sourceSops'][0]).items() if k not in ['headerNative']},'calculations':p['calculations']}
# Frozen answers are deterministic predicate evaluation on source-derived paired groups.
# They are not labeled as live Atlas Search execution.
def matching_studies(docs,modality='all',manufacturer='all'):
 return [d for d in docs if any((modality=='all' or s['mo']==modality) and (manufacturer=='all' or s['summary']['detail']['acquisitionFilteringComplete'] and any(g.get('values',{}).get('manufacturer')==manufacturer for g in s['summary'].get('acquisitionGroups',[]))) for s in d['se'])]
image_questions={}
for mo in ['all','CT','MR','MG','PT']:
 for man in ['all','SIEMENS','TOSHIBA']:
  matched=matching_studies(cases,mo,man)
  params={'kind':'searchCombined',**({'modality':mo} if mo!='all' else {}),**({'manufacturer':man} if man!='all' else {})}
  plan=e.query_plan(params);plan['pipeline']=scope_pipeline(plan['pipeline'],{'tenant_id':'public_demo','collections':{'studies':'studies','research':'research'}},sorted(set(p['dm'] for p in parents.values())),'studies');image_questions[mo+'|'+man]={'studyUids':[d['studyUid'] for d in matched],'seriesUids':[s['si'] for d in matched for s in d['se'] if (mo=='all' or s['mo']==mo) and any(man=='all' or g['values'].get('manufacturer')==man for g in s['summary']['acquisitionGroups'])],'pipelineNative':'db.studies.aggregate('+native(plan['pipeline'])+')','serving':'frozen_source_predicate_evaluation; Search plan compiled by Kehrnel','atlasExecuted':False}
q=[]
for id,title,area,p in [('thin-ct','Find CT series with observed slice thickness between 0 and 2 mm','quality',{'kind':'searchCombined','modality':'CT','minimum':0,'maximum':2}),('devices','Find SIEMENS CT acquisitions with thickness between 0 and 10 mm','devices',{'kind':'searchCombined','modality':'CT','manufacturer':'SIEMENS','minimum':0,'maximum':10}),('dates','Find CT acquisitions in a date interval with the same parameter context','cohorts',{'kind':'searchCombined','modality':'CT','minimum':0,'maximum':10,'fromDay':'1940-01-01','toDay':'2030-01-01'}),('codes','Find a coded measurement: system, code and value in the same fact','cohorts',{'kind':'searchCodes'}),('annotation','Find volume in annotation concepts and text','cohorts',{'kind':'semanticText','text':'volume'}),('fuzzy','Find a study description with a spelling variation','quality',{'kind':'fuzzy','text':'chset'})]:
 plan=e.query_plan(p);q.append({'id':id,'title':title,'area':area,'params':p,'pipelineNative':'db.studies.aggregate('+native(plan['pipeline'])+')','executed':False,'explanation':'Same series, acquisition group or fact; compiled Search plan over study summaries.'})
output={'id':'dicom.consolidated-metadata-story','version':'1.0.0','producer':'Kehrnel dicom.tiered_store 0.5.0 metadata contract v2','producerSha256':{name:hashlib.sha256((k/'src/kehrnel/engine/domains/dicom'/name).read_bytes()).hexdigest() for name in ['metadata.py','metadata_explorer.py']},'imageRuleFields':image_fields,'builtOn':'2026-11-03','serving':'immutable, source-derived metadata and compiled query plans; no live Atlas claim','imageCaseSummaries':rows,'imageQuestions':image_questions,'corpus':{'records':report['records'],'series':report['series'],'sopClasses':report['sopClasses'],'modalities':report['modalities'],'candidateConceptCount':report['candidateConceptCount'],'limitations':report['limitations']},'previews':previews,'questions':q,'sourceCatalog':e.state()['sourceCatalog'],'registry':e.registry,'modelDecisions':[{'title':'Embed useful facts in the study','why':'Viewer and recurring research read one study shape with bounded series summaries.'},{'title':'Keep acquisition values paired','why':'Independent arrays can combine values from different SOPs or frames.'},{'title':'Share SR image-reference contexts','why':'A Structured Report can repeat the same target list across many measurements; preserve the binding once and reference it.'},{'title':'Retain immutable original metadata','why':'A new rule can rebuild a scoped projection without reopening every DICOM binary.'},{'title':'Index typed facts as well as text','why':'Token, code, date and quantity filters can be combined through nested Atlas Search mappings.'},{'title':'Treat research rows as extensions','why':'Exceptional project-specific fields join to the exact study/series; they are not the default research source.'}]}
p=args.output;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json_util.dumps(output,indent=2));print('Generated',len(rows),'image-linked summaries and',len(previews),'class previews; frozen artifact',p.stat().st_size,'bytes.')
