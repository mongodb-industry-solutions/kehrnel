"""Frozen public-data exploration, not a production migration scheduler."""
import json
from pathlib import Path
from collections import defaultdict
from .metadata import *

class MetadataExplorer:
 def __init__(self,root=None,records=None):
  root=Path(root) if root else Path(__file__).with_name('samples')/'idc-metadata/1.0.0'
  pin=json.loads((root/'checksums.json').read_text())
  for name, expected in pin['files'].items():
   if name not in {'metadata_registry.json','report.json','tag_labels.json','instances.json','manifest.json','selection.json'} or hashlib.sha256((root/name).read_bytes()).hexdigest()!=expected:raise ValueError('Domain sample asset checksum mismatch.')
  self.registry=json.loads((root/'metadata_registry.json').read_text());self.report=json.loads((root/'report.json').read_text())
  self.rows=json.loads((root/'instances.json').read_text()) if records is None else records;self.by_series=defaultdict(list)
  self.labels=json.loads((root/'tag_labels.json').read_text())
  for r in self.rows:
   checksum=hashlib.sha256(json.dumps(r['header'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
   if r.get('source',{}).get('sourceHeaderSha256',checksum)!=checksum:raise ValueError('Header checksum mismatch.')
   self.by_series[first(r['header'],'0020000E')].append(r)
  self.fields={f['key']:f for f in self.registry['fields']}
 def state(self):
  class_names={c['sopClassUid']:c['name'] for c in self.report['perClass']}
  catalog=[compact({'sopUid':first(r['header'],'00080018'),'studyUid':first(r['header'],'0020000D'),'seriesUid':first(r['header'],'0020000E'),'modality':first(r['header'],'00080060'),'sopClassUid':first(r['header'],'00080016'),'className':class_names.get(first(r['header'],'00080016')),'seriesDescription':first(r['header'],'0008103E'),'collectionId':r['source']['collectionId'],'datasetKind':r['source']['datasetKind']}) for r in self.rows]
  return {'registry':self.registry,'tagLabels':self.labels,'sourceCatalog':catalog,'report':{k:v for k,v in self.report.items() if k not in {'instances','sourceManifest'}},'sourceManifest':{k:v for k,v in self.report['sourceManifest'].items() if k not in {'series'}},'mode':'Frozen public sample preview; live ingestion remains in the main demonstrator.'}
 def source(self,sop,query=''):
  row=next((r for r in self.rows if first(r['header'],'00080018')==sop),None)
  if row is None:raise ValueError('Unknown public sample SOP.')
  displayed=[];total=0
  for path,tag,e in walk(row['header']):
   label=self.labels.get(tag,{'name':'Private or unknown element','keyword':'PrivateElement'})
   if query and query.casefold() not in (path+' '+label['name']+' '+label['keyword']).casefold():continue
   total+=1
   if len(displayed)>=250:continue
   val=e.get('Value',[])
   shown=(f'{len(val)} sequence item(s)' if e.get('vr')=='SQ' else val[:8])
   if 'InlineBinary' in e:shown='Binary metadata retained in source'
   unit=next((f.get('unit') for f in self.registry['fields'] if f['tag']==tag),None)
   displayed.append(compact({'path':path,'tag':tag,'name':label['name'],'keyword':label['keyword'],'vr':e.get('vr'),'values':shown,'unit':unit,'valueMultiplicity':len(val)}))
  return {'source':row['source'],'elements':displayed,'matchingElements':total,'displayLimit':250,'headerNative':native(row['header']),'headerJsonBytes':len(canonical(row['header'])),'headerBsonBytes':bson_bytes(row['header'])}
 def preview(self,p):
  keys=p.get('fields');clauses=p.get('when',[]);selected=p.get('seriesUid');scope=p.get('scope',{})
  if keys is not None and (not isinstance(keys,list) or not all(k in self.fields for k in keys)):raise ValueError('Choose fields from the checked metadata registry.')
  if not isinstance(clauses,list) or len(clauses)>8:raise ValueError('Use up to eight source-context conditions.')
  for c in clauses:
   if c.get('field') not in self.fields or c.get('op') not in {'present','equals','gte','lte'}:raise ValueError('Unsupported field condition.')
   if c['op'] in {'gte','lte'} and self.fields[c['field']]['type']!='number':raise ValueError('Numeric conditions require a numeric field.')
  def within(r):
   h=r['header']
   if scope.get('modalities') and first(h,'00080060') not in scope['modalities']:return False
   if scope.get('sopClasses') and first(h,'00080016') not in scope['sopClasses']:return False
   if scope.get('collection') and r['source']['collectionId']!=scope['collection']:return False
   date=first(h,'00080020','')
   if scope.get('fromDay') or scope.get('toDay'):
    try:day=datetime.strptime(date,'%Y%m%d').strftime('%Y-%m-%d')
    except (ValueError,TypeError):return False
    if scope.get('fromDay') and day<scope['fromDay']:return False
    if scope.get('toDay') and day>=scope['toDay']:return False
   return True
  def applies(r):
   h=r['header'];fam=family(first(h,'00080016',''))
   grouped=defaultdict(list)
   for c in clauses:grouped[c['field']].append(c)
   for key,conditions in grouped.items():
    f=self.fields[key]
    if not applicable(f,fam):return False
    element=h.get(f['tag'])
    if element is None:
     shared=next(iter(items(h,'52009229')),{});matches=[e for _,t,e in walk(shared) if t==f['tag']]
     if len(matches)==1:element=matches[0]
    try:v=typed(element,f)
    except ValueError:return False
    if v is None:return False
    vals=v if isinstance(v,list) else [v]
    def matches_all(x):
     for c in conditions:
      if c['op']=='present':continue
      w=c.get('value')
      if c['op']=='equals' and x!=w:return False
      if c['op'] in {'gte','lte'}:
       try:w=float(w)
       except (TypeError,ValueError):raise ValueError('Enter a numeric condition value.')
       if not math.isfinite(w):raise ValueError('Condition values must be finite.')
       if not isinstance(x,(int,float)) or not (x>=w if c['op']=='gte' else x<=w):return False
     return True
    if not any(matches_all(x) for x in vals):return False
   return True
  candidates={s:[r for r in rows if within(r)] for s,rows in self.by_series.items()};candidates={s:rows for s,rows in candidates.items() if rows}
  affected={s:[r for r in rows if applies(r)] for s,rows in candidates.items()};affected={s:rows for s,rows in affected.items() if rows}
  if selected is not None and selected not in affected:raise ValueError('The selected series does not match this source scope and rule.')
  if selected is None:selected=next(iter(affected),None)
  if selected is None:return {'candidateSeries':len(candidates),'affectedSeries':0,'matchedInstances':0,'message':'No source records match this scope and source-level condition.','config':p}
  rows=affected[selected];limits=p.get('limits',{})
  if not isinstance(limits,dict):raise ValueError('Limits must be an object.')
  for k,v in limits.items():
   if k not in self.registry['limits'] or not isinstance(v,int) or isinstance(v,bool) or v<1:raise ValueError('Choose valid positive projection limits.')
   if v>({'maxGroups':256,'maxFacts':2048,'maxSummaryBytes':262144,'maxExampleSources':10}[k]):raise ValueError('Preview limit exceeds its teaching budget.')
  doc=document_for_series(rows,self.registry,keys,p.get('includeFacts',True),limits);summary=doc['se'][0]['summary'];destination=p.get('destination','shared')
  if destination not in {'shared','extension'}:raise ValueError('Choose shared summary or scoped extension.')
  persisted=doc
  if destination=='extension':
   persisted={'_id':hash_value(['ctti_demo','IDC_PUBLIC',doc['studyUid'],selected,summary['profileHash']]),'tenantId':'ctti_demo','centerId':'IDC_PUBLIC','studyUid':doc['studyUid'],'seriesUid':selected,'profileId':'research-extension-preview','generationId':summary['profileHash'][:24],'schemaVersion':self.registry['version'],'summary':summary}
  calculated=[]
  for key,c in summary.get('coverage',{}).items():
   observed={hash_value(g['values'][key]):g['values'][key] for g in summary.get('acquisitionGroups',[]) if key in g.get('values',{})}
   values=list(observed.values());numeric=[v for v in values if isinstance(v,(int,float))]
   calculated.append(compact({'field':key,'coverage':c,'observedValues':values[:12],'observedValueCountInRetainedGroups':len(values),'range':{'min':min(numeric),'max':max(numeric)} if numeric else None,'meaning':'Values remain paired inside acquisitionGroups. A displayed range is descriptive, not evidence of intermediate values.'}))
  config=compact({'version':self.registry['version'],'destination':destination,'scope':scope,'when':clauses,'fields':keys if keys is not None else [f['key'] for f in self.registry['fields'] if f['default']],'includeFacts':p.get('includeFacts',True),'limits':{**self.registry['limits'],**limits},'conditionContext':'instance and unambiguous shared functional group only; repeated per-frame conditions require explicit context rules.','conditionValueSemantics':'Scalar equality or any value in the source VM array. Ordered-vector component constraints require an explicit tuple rule.'})
  if keys==[]:config['fields']=[] # An explicit zero-field projection differs from omitted optional data.
  mapping=self.search_mapping(config['fields']);config['search']={'collection':'studies','path':'se.summary','definitionHash':hash_value(mapping),'operators':['equals','in','range','text','embeddedDocument'],'correlation':'same series, then same acquisition group or fact'}
  if p.get('semanticDesign'):
   config['semanticSearch']={'status':'designOnly','sourceCandidates':['se.de','se.summary.facts.text','se.summary.facts.concept.meaning'],'unit':'one series narrative or fact context','requires':['chosen embedding model and version','matching vector dimensions','separate vector index and measured relevance'],'exactFiltersRemainAuthoritative':True}
  persisted['ruleVersion']=hash_value(config)
  return {'candidateSeries':len(candidates),'affectedSeries':len(affected),'matchedInstances':sum(map(len,affected.values())),'selectedSeries':selected,'affected':[{'seriesUid':s,'modality':first(v[0]['header'],'00080060'),'sopClassUid':first(v[0]['header'],'00080016'),'collectionId':v[0]['source']['collectionId'],'matchingInstances':len(v)} for s,v in affected.items()],'config':config,'configNative':native(config),'candidateSearchMappingNative':native(mapping),'documentNative':native(persisted),'document':persisted,'documentBsonBytes':bson_bytes(persisted),'summaryBsonBytes':bson_bytes(summary),'summary':summary,'calculations':calculated,'sourceSops':[first(r['header'],'00080018') for r in rows],'trace':['Read candidate scoped series from the frozen public catalog.','Read extracted headers for scoped SOPs; verify canonical source checksums.','Validate source-context predicates without flattening repeated sequences.','Resolve class profile and root/shared/per-frame inheritance.','Group complete parameter tuples and extract context-preserving semantic facts.','Apply explicit cardinality and byte budgets; record incomplete filtering if overflowed.','Generate token, number, date, text and embedded Search mappings from the selected rule fields.','Return a preview only; the live publication pointer and demo Search mapping are unchanged.'],'queryNative':native(self.search_parameter_query(summary)),'nativeQueryNative':native(self.query_for(summary)),'indexPlan':self.mapping_plan(mapping)}
 def query_for(self,summary):
  group=next(iter(summary.get('acquisitionGroups',[])),{})
  values=group.get('values',{});pred={}
  for k in ['manufacturer','manufacturerModelName','sliceThickness','echoTime','rows','columns']:
   if k in values and len(pred)<2:pred['values.'+k]=values[k]
  if not pred:pred={'sopClassUid':group.get('sopClassUid','')}
  return [{'$match':{'tenantId':'ctti_demo','centerId':'IDC_PUBLIC','se':{'$elemMatch':{'summary.acquisitionGroups':{'$elemMatch':pred},'summary.detail.acquisitionFilteringComplete':True}}}},{'$limit':100}]
 def search_mapping(self,keys=None):
  keys=self.default_search_keys() if keys is None else keys
  if not isinstance(keys,list) or len(keys)>len(self.fields) or any(k not in self.fields for k in keys):raise ValueError('Search fields must come from the checked registry.')
  text_keys={'studyDescription','seriesDescription','protocolName','imageComments'}
  fields={k:({'type':'number'} if self.fields[k]['type']=='number' else [{'type':'token'},{'type':'string'}] if k in text_keys else {'type':'token'}) for k in keys}
  facts={'type':'embeddedDocuments','fields':{'kind':{'type':'token'},'text':{'type':'string'},'label':[{'type':'token'},{'type':'string'}],'number':{'type':'number'},'concept':{'type':'document','fields':{'meaning':{'type':'string'},'code':{'type':'token'},'system':{'type':'token'}}},'valueCode':{'type':'document','fields':{'code':{'type':'token'},'system':{'type':'token'},'meaning':{'type':'string'}}},'unit':{'type':'document','fields':{'code':{'type':'token'},'system':{'type':'token'}}}}}
  return {'mappings':{'dynamic':False,'fields':{'tenantId':{'type':'token'},'centerId':{'type':'token'},'studyUid':{'type':'token'},'studyDate':{'type':'date'},'description':[{'type':'token'},{'type':'string'}],'se':{'type':'embeddedDocuments','fields':{'si':{'type':'token'},'mo':{'type':'token'},'de':[{'type':'token'},{'type':'string'}],'summary':{'type':'document','fields':{'families':{'type':'token'},'detail':{'type':'document','fields':{'acquisitionFilteringComplete':{'type':'boolean'},'factFilteringComplete':{'type':'boolean'}}},'acquisitionGroups':{'type':'embeddedDocuments','fields':{'sopClassUid':{'type':'token'},'family':{'type':'token'},'context':{'type':'token'},'values':{'type':'document','fields':fields}}},'facts':facts}}}}}}}
 def default_search_keys(self):
  return ['manufacturer','manufacturerModelName','sliceThickness','magneticFieldStrength','echoTime','rows','columns','convolutionKernel','softwareVersions','lossyImageCompression']
 @staticmethod
 def mapping_paths(mapping):
  out={}
  def visit(fields,prefix=''):
   for key,value in fields.items():
    path=prefix+key;items=value if isinstance(value,list) else [value]
    out[path]=sorted(v['type'] for v in items)
    for item in items:
     if 'fields' in item:visit(item['fields'],path+'.')
  visit(mapping.get('mappings',{}).get('fields',{}));return out
 def mapping_plan(self,mapping,live=None):
  desired=self.mapping_paths(mapping);actual=self.mapping_paths(live or {})
  missing=[{'path':path,'types':types} for path,types in desired.items() if not set(types).issubset(actual.get(path,[]))]
  return {'collection':'evidence_studies','productionCollection':'studies','index':'dicom_evidence','definitionHash':hash_value(mapping),'mappedPaths':len(desired),'requiredPaths':desired,'liveComparison':'checked' if live is not None else 'not checked; compare against Atlas','missingOrIncompatible':missing if live is not None else [],'sufficient':not missing if live is not None else None,'notes':['Token equality, date/quantity ranges and text can be combined in compound.filter/must.','embeddedDocument preserves series and parameter/fact correlation.','Numeric VM arrays support observed-value predicates; ordered vector components need an explicit component rule.','Each embedded element increases Search indexing work; measure realistic cardinalities.','Projection completeness and Search freshness are separate requirements.']}
 def search_parameter_query(self,summary):
  values=next(iter(summary.get('acquisitionGroups',[])),{}).get('values',{});clauses=[]
  for k in ['manufacturer','manufacturerModelName','sliceThickness','echoTime','rows','columns']:
   if k in values and not isinstance(values[k],list) and len(clauses)<2:clauses.append({'equals':{'path':'se.summary.acquisitionGroups.values.'+k,'value':values[k]}})
  if not clauses:clauses=[{'exists':{'path':'se.summary.acquisitionGroups.sopClassUid'}}]
  return [self.search_stage({'embeddedDocument':{'path':'se','operator':{'compound':{'filter':[{'equals':{'path':'se.summary.detail.acquisitionFilteringComplete','value':True}},{'embeddedDocument':{'path':'se.summary.acquisitionGroups','operator':{'compound':{'filter':clauses}}}}]}}}}),{'$limit':100}]
 @staticmethod
 def search_stage(operator):
  return {'$search':{'index':'dicom_evidence','compound':{'filter':[{'equals':{'path':'tenantId','value':'ctti_demo'}},{'equals':{'path':'centerId','value':'IDC_PUBLIC'}},operator]}}}
 def query_plan(self,p):
  kind=p.get('kind','searchCombined')
  summary=self.preview({'seriesUid':p.get('seriesUid')})['summary'] if kind in {'correlated','searchParameters','searchCodes'} else {}
  if kind=='correlated':pipeline=self.query_for(summary)
  elif kind=='searchParameters':pipeline=self.search_parameter_query(summary)
  elif kind=='searchCombined':
   filters=[];series_filters=[];group_filters=[]
   for key,bound in [('fromDay','gte'),('toDay','lt')]:
    if p.get(key):
     try:day=datetime.strptime(p[key],'%Y-%m-%d').replace(tzinfo=timezone.utc)
     except (TypeError,ValueError):raise ValueError('Use an ISO date for the Search date range.')
     filters.append({'range':{'path':'studyDate',bound:day}})
   if p.get('fromDay') and p.get('toDay') and p['fromDay']>=p['toDay']:raise ValueError('The exclusive end date must follow the start date.')
   if p.get('modality'):series_filters.append({'equals':{'path':'se.mo','value':str(p['modality'])}})
   if p.get('sopClassUid'):group_filters.append({'equals':{'path':'se.summary.acquisitionGroups.sopClassUid','value':str(p['sopClassUid'])}})
   if p.get('manufacturer'):group_filters.append({'equals':{'path':'se.summary.acquisitionGroups.values.manufacturer','value':str(p['manufacturer'])}})
   key=p.get('quantityField','sliceThickness');limits={}
   for attr,bound in [('minimum','gte'),('maximum','lte')]:
    if p.get(attr) not in (None,''):
     try:value=float(p[attr])
     except (TypeError,ValueError):raise ValueError('Quantity bounds must be numeric.')
     if not math.isfinite(value):raise ValueError('Quantity bounds must be finite.')
     limits[bound]=value
   if limits:
    if key not in self.fields or self.fields[key]['type']!='number':raise ValueError('Choose a declared numeric quantity field.')
    if limits.get('gte',-math.inf)>limits.get('lte',math.inf):raise ValueError('Minimum exceeds maximum.')
    group_filters.append({'range':{'path':'se.summary.acquisitionGroups.values.'+key,**limits}})
   if group_filters:series_filters += [{'equals':{'path':'se.summary.detail.acquisitionFilteringComplete','value':True}},{'embeddedDocument':{'path':'se.summary.acquisitionGroups','operator':{'compound':{'filter':group_filters}}}}]
   if p.get('text'):series_filters.append({'text':{'path':'se.de','query':str(p['text'])[:120]}})
   if series_filters:filters.append({'embeddedDocument':{'path':'se','operator':{'compound':{'filter':series_filters}}}})
   pipeline=[{'$search':{'index':'dicom_evidence','compound':{'filter':[{'equals':{'path':'tenantId','value':'ctti_demo'}},{'equals':{'path':'centerId','value':'IDC_PUBLIC'}},*filters]}}},{'$limit':100}]
  elif kind=='prefix':
   text=str(p.get('text','chest'))[:120].casefold()
   pipeline=[{'$match':{'tenantId':'ctti_demo','centerId':'IDC_PUBLIC','descriptionNorm':{'$regex':'^'+re.escape(text)}}},{'$limit':100}]
  elif kind=='fuzzy':
   text=str(p.get('text','chest'))[:120]
   pipeline=[{'$search':{'index':'dicom_evidence','compound':{'filter':[{'equals':{'path':'tenantId','value':'ctti_demo'}},{'equals':{'path':'centerId','value':'IDC_PUBLIC'}}],'must':[{'text':{'path':'description','query':text,'fuzzy':{'maxEdits':1,'prefixLength':1}}}]}}},{'$limit':100}]
  elif kind=='semanticText':
   text=str(p.get('text','volume'))[:120]
   pipeline=[self.search_stage({'embeddedDocument':{'path':'se','operator':{'embeddedDocument':{'path':'se.summary.facts','operator':{'text':{'path':['se.summary.facts.concept.meaning','se.summary.facts.text','se.summary.facts.label'],'query':text}}}}}}),{'$limit':100}]
  elif kind=='searchCodes':
   fact=next((f for f in summary.get('facts',[]) if f.get('concept',{}).get('code') and f.get('concept',{}).get('system')),None)
   if fact is None:
    source=self.preview({'scope':{'modalities':['SR']},'fields':[]})['summary'];fact=next(f for f in source['facts'] if f.get('concept',{}).get('code') and f.get('concept',{}).get('system'))
   concept={'system':p['codeSystem'],'code':p['code']} if p.get('codeSystem') and p.get('code') else fact['concept'];clauses=[{'equals':{'path':'se.summary.facts.concept.'+k,'value':concept[k]}} for k in ['system','code']]
   if p.get('codeSystem') and p.get('code'):
    bounds={bound:float(p[attr]) for attr,bound in [('minimum','gte'),('maximum','lte')] if p.get(attr) is not None}
    if bounds:clauses.append({'range':{'path':'se.summary.facts.number',**bounds}})
   elif fact.get('number') is not None:clauses.append({'range':{'path':'se.summary.facts.number','gte':fact['number'],'lte':fact['number']}})
   pipeline=[self.search_stage({'embeddedDocument':{'path':'se','operator':{'embeddedDocument':{'path':'se.summary.facts','operator':{'compound':{'filter':clauses}}}}}}),{'$limit':100}]
  elif kind=='extension':
   generation=p.get('generationId') or hash_value({'registryVersion':self.registry['version'],'fields':['deviceSerialNumber'],'includeFacts':False,'limits':self.registry['limits']})[:24]
   pipeline=[{'$match':{'tenantId':'ctti_demo','centerId':'IDC_PUBLIC'}},{'$unwind':'$se'},{'$lookup':{'from':'evidence_extensions','let':{'t':'$tenantId','c':'$centerId','st':'$studyUid','sr':'$se.si'},'pipeline':[{'$match':{'profileId':p.get('profileId','research-extension-preview'),'generationId':generation,'$expr':{'$and':[{'$eq':['$tenantId','$$t']},{'$eq':['$centerId','$$c']},{'$eq':['$studyUid','$$st']},{'$eq':['$seriesUid','$$sr']}]}}}],'as':'extension'}},{'$match':{'extension.0':{'$exists':True}}},{'$limit':100}]
  else:raise ValueError('Choose a supported study-summary query or the exceptional extension join.')
  if p.get('engine')=='native' and '$search' in pipeline[0]:
   pipeline=[{'$match':native_filter(pipeline[0]['$search']['compound'])},*pipeline[1:]]
  return {'pipeline':pipeline,'resultGrain':'study-series' if kind=='extension' else 'studies','valueSemantics':'Ranges bind both bounds to one scalar or one member of a source VM array; ordered vector components require explicit rules.','extensionLookup':kind=='extension','search':pipeline[0].get('$search') is not None}

def native_filter(operator,prefix=''):
 """Compile only exact typed Search filters into a native comparison plan."""
 if 'compound' in operator:return native_filter(operator['compound'],prefix)
 if any(key in operator for key in ['must','mustNot','should']):raise ValueError('Text/fuzzy compound clauses require Atlas Search.')
 if 'filter' in operator:return {'$and':[native_filter(c,prefix) for c in operator['filter']]}
 for op in ['equals','range','exists']:
  if op in operator:
   a=operator[op];path=a['path'];path=path[len(prefix):] if path.startswith(prefix) else path
   if op=='equals':return {path:a['value']}
   if op=='exists':return {path:{'$exists':True}}
   bounds={'$'+k:v for k,v in a.items() if k in ['gt','gte','lt','lte']}
   return {'$or':[{'$and':[{path:{'$not':{'$type':'array'}}},{path:bounds}]},{path:{'$elemMatch':bounds}}]}
 if 'embeddedDocument' in operator:
  a=operator['embeddedDocument'];path=a['path'];relative=path[len(prefix):] if path.startswith(prefix) else path
  return {relative:{'$elemMatch':native_filter(a['operator'],path+'.')}}
 raise ValueError('This text/fuzzy recipe needs Atlas Search; native comparison supports typed filters only.')
