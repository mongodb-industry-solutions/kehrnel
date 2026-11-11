"""Bounded research-summary preview with explicit instance/frame/item context.

This module does not decide clinical truth or silently flatten DICOM sequences.
It accepts faithful DICOM JSON and a checked registry. No Kehrnel dependency.
"""
from __future__ import annotations
import hashlib, json, math, re
from collections import Counter, defaultdict
from datetime import datetime, timezone

CLASS_ROOT='1.2.840.10008.5.1.4.1.1.'
CLASS_FAMILIES={'2':'CT','2.1':'CT','4':'MR','4.1':'MR','4.4':'MR','128':'PT','130':'PT','1.2':'MG','1.2.1':'MG','13.1.3':'MG','6.1':'US','3.1':'US','6.2':'US','77.1.6':'SM','66.4':'SEG','66.7':'SEG','91.1':'ANN','481.2':'RTDOSE','481.3':'RTSTRUCT','481.5':'RTPLAN','20':'NM','67':'RWV','30':'PARAMETRIC','66.1':'REG','1.1':'DX','1.1.1':'DX','1':'CR','12.1':'XA','7':'SC','7.1':'SC','7.2':'SC','7.3':'SC','7.4':'SC'}
IMAGE_FAMILIES={'CT','MR','PT','MG','US','SM','NM','DX','CR','SC','XA','PARAMETRIC','RTDOSE','SEG'}

def compact(v):
 if isinstance(v,dict):return {k:compact(x) for k,x in v.items() if x is not None and x!='' and x!=[] and x!={}}
 if isinstance(v,list):return [compact(x) for x in v if x is not None and x!='' and x!=[] and x!={}]
 return v
def persisted_types(v,key=''):
 if isinstance(v,dict):return {k:persisted_types(x,k) for k,x in v.items()}
 if isinstance(v,list):return [persisted_types(x) for x in v]
 if isinstance(v,int) and not isinstance(v,bool) and key in {'acceptedSopCount','frameContextsProcessed','ni','contextCount','sopCount','frameCount','pointCount','annotationCount'}:
  try:
   from bson.int64 import Int64
   return Int64(v)
  except ImportError:pass
 return v
def canonical(v):
 return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,default=lambda x:x.isoformat() if isinstance(x,datetime) else str(x)).encode()
def hash_value(v):return hashlib.sha256(canonical(v)).hexdigest()
def first(h,tag,default=None):
 vals=h.get(tag,{}).get('Value',[])
 return vals[0] if isinstance(vals,list) and vals else default
def items(h,tag):return [v for v in h.get(tag,{}).get('Value',[]) if isinstance(v,dict)]
def walk(h,path=''):
 for tag,element in h.items():
  if not isinstance(element,dict):continue
  p=path+'/'+tag;yield p,tag,element
  if element.get('vr')=='SQ':
   for i,item in enumerate(items(h,tag)):yield from walk(item,p+f'[{i}]')
def family(sop):
 suffix=sop.removeprefix(CLASS_ROOT)
 if suffix.startswith('88.'):return 'SR'
 if suffix.startswith('11.'):return 'PR'
 return CLASS_FAMILIES.get(suffix,'OTHER')
def applicable(field,fam):return 'ALL' in field['appliesTo'] or fam in field['appliesTo'] or 'IMAGE' in field['appliesTo'] and fam in IMAGE_FAMILIES
def typed(element,field):
 if not element or not element.get('Value'):return None
 if element.get('vr')!=field['vr']:raise ValueError('VR mismatch')
 result=[]
 for x in element['Value']:
  if x is None or x=='':continue
  if isinstance(x,(dict,list,bool)):raise ValueError('Scalar or vector value required')
  if field['type']=='number':
   n=float(x)
   if not math.isfinite(n):raise ValueError('Non-finite number')
   if field['vr'] in {'IS','US','SS','UL','SL','SV','UV'} and not n.is_integer():raise ValueError('Integer VR with fractional value')
   result.append(int(n) if field['vr'] in {'IS','US','SS','UL','SL','SV','UV'} else n)
  else:result.append(str(x))
 return result[0] if len(result)==1 else result or None
def code(h,tag):
 c=next(iter(items(h,tag)),{})
 return compact({'system':first(c,'00080102'),'code':first(c,'00080100',first(c,'00080119',first(c,'00080120'))),'meaning':first(c,'00080104'),'version':first(c,'00080103')})
def references(h):
 out=[]
 for p,t,e in walk(h):
  if t=='00081155':
   parent=h
   # Preserve sibling frame/class values by reading the actual sequence item.
   for seg in p.strip('/').split('/')[:-1]:
    m=re.fullmatch(r'([0-9A-F]{8})\[(\d+)\]',seg)
    if m:parent=items(parent,m[1])[int(m[2])]
   out.append(compact({'sopUid':e.get('Value',[None])[0],'frames':parent.get('00081160',{}).get('Value'),'sopClassUid':first(parent,'00081150'),'path':p}))
 return out
def source(h,path,**extra):return compact({'sopUid':first(h,'00080018'),'path':path,**extra})

def annotation_facts(h):
 """SR leaf facts retain their nearest measurement/container context and targets."""
 sop=first(h,'00080018');fam=family(first(h,'00080016',''));facts=[]
 if fam=='SR':
  def visit(node,path,container,targets,target_path):
   vt=first(node,'0040A040');children=items(node,'0040A730')
   if vt=='CONTAINER':
    container=path
    local=[]
    for i,c in enumerate(children):
     if first(c,'0040A040') in {'IMAGE','COMPOSITE'}:local+=[{**ref,'path':path.rstrip('/')+f'/0040A730[{i}]'+ref['path']} for ref in references(c)]
    if local:targets=local;target_path=path
   else:
    local=[]
    for i,child in enumerate(children):
     if first(child,'0040A040') in {'IMAGE','COMPOSITE'}:local+=[{**ref,'path':path.rstrip('/')+f'/0040A730[{i}]'+ref['path']} for ref in references(child)]
    if local:targets=local;target_path=path
   fact={'kind':'sr','valueType':vt,'concept':code(node,'0040A043'),'contextId':hash_value([sop,container])[:20],'source':source(h,path),'targets':targets,'targetSource':source(h,target_path)}
   if vt=='TEXT':fact['text']=first(node,'0040A160')
   elif vt=='CODE':fact['valueCode']=code(node,'0040A168')
   elif vt=='NUM':
    measured=next(iter(items(node,'0040A300')),{})
    raw=first(measured,'0040A30A',first(measured,'0040A161'))
    try:
     number=float(raw)
     if math.isfinite(number):fact.update({'number':number,'unit':code(measured,'004008EA')})
    except (TypeError,ValueError):fact['invalidNumericValue']=True
   elif vt in {'SCOORD','SCOORD3D'}:
    fact.update({'graphicType':first(node,'00700023'),'geometryRetainedInHeader':True})
   if any(k in fact for k in ['text','valueCode','number','graphicType']):facts.append(compact(fact))
   for i,c in enumerate(children):visit(c,path.rstrip('/')+f'/0040A730[{i}]',container,targets,target_path)
  visit(h,'/', '/', [],'/')
 elif fam=='SEG':
  per_segment=defaultdict(lambda:{'frames':0,'targets':{}})
  for i,frame in enumerate(items(h,'52009230')):
   segment=first(next(iter(items(frame,'0062000A')),{}),'0062000B')
   if segment is None:continue
   item=per_segment[str(segment)];item['frames']+=1
   for target in references(frame):item['targets'][hash_value(target)]=target
  for i,seg in enumerate(items(h,'00620002')):
   number=first(seg,'00620004');membership=per_segment[str(number)];targets=list(membership['targets'].values())
   facts.append(compact({'kind':'segment','segmentNumber':number,'label':first(seg,'00620005'),'description':first(seg,'00620006'),'category':code(seg,'00620003'),'type':code(seg,'0062000F'),'algorithmType':first(seg,'00620008'),'algorithmName':first(seg,'00620009'),'frameCount':membership['frames'],'targetCount':len(targets),'targetExamples':targets[:3],'targetMembershipComplete':len(targets)<=3,'source':source(h,f'/00620002[{i}]')}))
 elif fam=='PR':
  for i,ann in enumerate(items(h,'00700001')):
   targets=references(ann)
   for j,text in enumerate(items(ann,'00700008')):
    facts.append(compact({'kind':'presentationText','text':first(text,'00700006'),'targets':targets,'source':source(h,f'/00700001[{i}]/00700008[{j}]')}))
 elif fam=='RTSTRUCT':
  contours={str(first(c,'30060084')):items(c,'30060040') for c in items(h,'30060039')}
  for i,roi in enumerate(items(h,'30060020')):
   number=first(roi,'30060022');cs=contours.get(str(number),[])
   facts.append(compact({'kind':'roi','roiNumber':number,'label':first(roi,'30060026'),'description':first(roi,'30060028'),'contourCount':len(cs),'pointCount':sum(int(first(c,'30060046',0)) for c in cs),'geometryRetainedInHeader':True,'source':source(h,f'/30060020[{i}]')}))
 elif fam=='ANN':
  for i,g in enumerate(items(h,'006A0002')):
   facts.append(compact({'kind':'annotationGroup','label':first(g,'006A0005'),'description':first(g,'006A0006'),'annotationCount':first(g,'006A000C'),'category':code(g,'006A0009'),'geometryRetainedInHeader':True,'source':source(h,f'/006A0002[{i}]')}))
 return facts

def sequence_groups(h,fam):
 """Known semantic item groups; never mix values from different SQ items."""
 specs=[]
 if fam in {'PT','NM'}:specs.append(('radiopharmaceutical','00540016',{'name':'00180031','startTime':'00181072','startDateTime':'00181078','totalDoseBq':'00181074','halfLifeSeconds':'00181075','positronFraction':'00181076'}))
 if fam=='US':specs.append(('ultrasoundRegion','00186011',{'minX':'00186018','minY':'0018601A','maxX':'0018601C','maxY':'0018601E','deltaX':'0018602C','deltaY':'0018602E','unitsXCode':'00186024','unitsYCode':'00186026'}))
 if fam=='SM':
  specs.append(('opticalPath','00480105',{'identifier':'00480106','description':'00480107','objectivePower':'00480112','objectiveNumericalAperture':'00480113'}))
  specs.append(('specimen','00400560',{'identifier':'00400551','uid':'00400554','shortDescription':'00400600','description':'00400602'}))
 output=[]
 for kind,tag,mapping in specs:
  for i,node in enumerate(items(h,tag)):
   values={}
   for k,t in mapping.items():
    v=first(node,t)
    if v is None:continue
    if node[t].get('vr') in {'DS','IS','FD','FL','US','SS','UL','SL'}:
     try:
      v=float(v)
      if not math.isfinite(v):continue
     except (ValueError,TypeError):continue
    values[k]=v
   output.append(compact({'kind':kind,'values':values,'source':source(h,f'/{tag}[{i}]')}))
 return output

def project_series(records,registry,selected=None,include_facts=True,limits=None):
 limits={**registry['limits'],**(limits or {})};fields=[x for x in registry['fields'] if x['key'] in selected] if selected is not None else [x for x in registry['fields'] if x['default']]
 source_families={family(first(r['header'],'00080016','')) for r in records}
 fields=[f for f in fields if any(applicable(f,fam) for fam in source_families)]
 groups={};facts=[];coverage=defaultdict(Counter);families=set();frames_processed=0;invalid=[]
 def extract(node,fam,nested=False):
  result={}
  for f in fields:
   if not applicable(f,fam):continue
   element=node.get(f['tag'])
   if nested and element is None:
    matches=[e for _,t,e in walk(node) if t==f['tag']]
    if len(matches)==1:element=matches[0]
    elif len(matches)>1:continue # Ambiguous repetition needs an explicit path/context rule.
   try:
    value=typed(element,f)
    if value is not None:result[f['key']]=value
   except ValueError:pass
  return result
 for r in records:
  h=r['header'];sop=first(h,'00080018');fam=family(first(h,'00080016',''));families.add(fam)
  vals=extract(h,fam);shared=next(iter(items(h,'52009229')),{});vals.update(extract(shared,fam,True))
  for f in fields:
   if not applicable(f,fam):coverage[f['key']]['notApplicableInstances']+=1;continue
   if f['key'] in vals:coverage[f['key']]['presentInstances']+=1
   else:
    if h.get(f['tag'],{}).get('Value'):
     try:typed(h[f['tag']],f)
     except ValueError:coverage[f['key']]['invalidInstances']+=1;invalid.append({'sopUid':sop,'field':f['key'],'reason':'Value or VR does not match configured type'})
    else:coverage[f['key']]['missingAtInstanceOrSharedLevel']+=1
  frames=items(h,'52009230')
  contexts=[(None,vals)] if not frames else [(i+1,{**vals,**extract(fr,fam,True)}) for i,fr in enumerate(frames)]
  for frame,values in contexts:
   frames_processed+=int(frame is not None)
   if frame is not None:
    for f in fields:
     if applicable(f,fam):coverage[f['key']]['presentFrameContexts' if f['key'] in values else 'missingFrameContexts']+=1
   # SOP class is part of the tuple, so derived producers never merge with image devices.
   key=hash_value({'family':fam,'sopClassUid':first(h,'00080016'),'values':values})
   g=groups.setdefault(key,{'id':key[:20],'kind':'acquisition','sopClassUid':first(h,'00080016'),'family':fam,'values':values,'contextCount':0,'sourceExamples':[],'sourceSops':set(),'context':'frame' if frame else 'instance'})
   g['contextCount']+=1;g['sourceSops'].add(sop)
   if len(g['sourceExamples'])<limits['maxExampleSources']:g['sourceExamples'].append(compact({'sopUid':sop,'frame':frame}))
  if include_facts:facts+=sequence_groups(h,fam)+annotation_facts(h)
 group_values=[]
 for g in groups.values():
  sops=g.pop('sourceSops');g['sopCount']=len(sops);g['sourceMembershipComplete']=len(sops)<=limits['maxExampleSources'] and g['contextCount']<=limits['maxExampleSources'];group_values.append(compact(g))
 group_values.sort(key=lambda x:(-x['contextCount'],x['id']))
 # Facts remain source-context records; no independent concept/value arrays.
 contexts={}
 for fact in facts:
  if 'targets' not in fact:continue
  refs=fact.pop('targets');context_id=hash_value([fact.get('contextId'),refs])[:20]
  contexts.setdefault(context_id,compact({'id':context_id,'measurementContextId':fact.get('contextId'),'kind':'srTargets','targetCount':len(refs),'targetExamples':refs[:limits['maxExampleSources']],'targetMembershipComplete':len(refs)<=limits['maxExampleSources'],'source':fact.pop('targetSource',fact.get('source')),'bindingSemantics':'Direct IMAGE children or inherited container references; template-aware relationship validation is pending.'}))
  fact['targetContextId']=context_id
 all_group_count=len(group_values);all_fact_count=len(facts);full_hash=hash_value({'groups':group_values,'facts':facts,'contexts':contexts})
 source_revision=hash_value(sorted([[first(r['header'],'00080018'),r.get('source',{}).get('sourceHeaderSha256') or hash_value(r['header'])] for r in records]))
 result={'schemaVersion':registry['version'],'profileHash':hash_value({'registryVersion':registry['version'],'fields':[f['key'] for f in fields],'includeFacts':include_facts,'limits':limits}),'sourceRevision':source_revision,'acceptedSopCount':len(records),'frameContextsProcessed':frames_processed,'families':sorted(families),'acquisitionGroups':group_values[:limits['maxGroups']],'facts':facts[:limits['maxFacts']],'coverage':dict(coverage),'quality':{'invalidValueCount':len(invalid),'examples':invalid[:5]},'detail':{'totalGroups':all_group_count,'totalFacts':all_fact_count,'projectedContentHash':full_hash,'sourceMembership':'Immutable header records by scoped SOP; examples are witnesses, not a full membership index.'}}
 def attach_contexts():
  referenced={f['targetContextId'] for f in result['facts'] if 'targetContextId' in f}
  result['contexts']=[contexts[c] for c in sorted(referenced)]
 attach_contexts()
 def size(v):
  try:
   from bson import BSON
   return len(BSON.encode(persisted_types(v)))
  except ImportError:return len(canonical(v))
 def update_detail():
  target_complete=all(c['targetMembershipComplete'] for c in result['contexts']) and all(f.get('targetMembershipComplete',True) for f in result['facts'])
  result['detail'].update({'retainedGroups':len(result['acquisitionGroups']),'retainedFacts':len(result['facts']),'totalContexts':len(contexts),'retainedContexts':len(result['contexts']),'overflow':all_group_count>len(result['acquisitionGroups']) or all_fact_count>len(result['facts']),'acquisitionFilteringComplete':all_group_count==len(result['acquisitionGroups']),'factValuesFilteringComplete':all_fact_count==len(result['facts']),'targetFilteringComplete':target_complete,'factFilteringComplete':all_fact_count==len(result['facts']) and target_complete,'exactFilteringComplete':all_group_count==len(result['acquisitionGroups']) and all_fact_count==len(result['facts']) and target_complete})
 update_detail()
 while size(compact(result))>limits['maxSummaryBytes'] and (result['facts'] or result['acquisitionGroups']):
  if result['facts']:result['facts'].pop()
  else:result['acquisitionGroups'].pop()
  attach_contexts();update_detail()
 result=compact(result)
 if size(result)>limits['maxSummaryBytes']:raise ValueError('Budget below fixed coverage/provenance overhead; increase budget or reduce fields.')
 return persisted_types(result)

def document_for_series(records,registry,selected=None,include_facts=True,limits=None):
 h=records[0]['header'];study=first(h,'0020000D');series=first(h,'0020000E');summary=project_series(records,registry,selected,include_facts,limits)
 date=None;raw=first(h,'00080020')
 try:date=datetime.strptime(raw,'%Y%m%d').replace(tzinfo=timezone.utc)
 except (TypeError,ValueError):pass
 se=compact({'si':series,'mo':first(h,'00080060'),'de':first(h,'0008103E'),'ni':len(records),'state':'open','summary':summary})
 return persisted_types(compact({'_id':hash_value(['ctti_demo','IDC_PUBLIC',study]),'tenantId':'ctti_demo','centerId':'IDC_PUBLIC','studyUid':study,'studyDate':date,'description':first(h,'00081030'),'se':[se],'schemaVersion':registry['version'],'sourceScope':'sampled public metadata','dateQuality':'validSourceDate' if date else 'missingOrInvalidSourceDate'}))

def bson_bytes(value):
 try:
  from bson import BSON
  return len(BSON.encode(value))
 except ImportError:return None

def native(value,depth=0,key=''):
 if isinstance(value,datetime):return 'ISODate('+json.dumps(value.isoformat().replace('+00:00','Z'))+')'
 if value is None:return 'null'
 if isinstance(value,bool):return str(value).lower()
 if isinstance(value,str):return json.dumps(value,ensure_ascii=False)
 if isinstance(value,(int,float)):
  return 'NumberLong('+json.dumps(str(value))+')' if isinstance(value,int) and key in {'acceptedSopCount','frameContextsProcessed','ni','contextCount','sopCount','frameCount','pointCount','annotationCount'} else str(value)
 if isinstance(value,list):
  short='['+', '.join(native(v,depth+1) for v in value)+']'
  if len(short)+depth*2<110 and '\n' not in short:return short
  return '[\n'+',\n'.join('  '*(depth+1)+native(v,depth+1) for v in value)+'\n'+'  '*depth+']'
 if isinstance(value,dict):
  pairs=[(k if re.fullmatch(r'[A-Za-z_$][\w$]*',k) else json.dumps(k))+': '+native(v,depth+1,k) for k,v in value.items()]
  short='{ '+', '.join(pairs)+' }'
  if len(short)+depth*2<110 and '\n' not in short:return short
  return '{\n'+',\n'.join('  '*(depth+1)+p for p in pairs)+'\n'+'  '*depth+'}'
 raise TypeError(type(value).__name__)
