import unittest
from kehrnel.engine.domains.dicom.metadata import *

def tag(vr,*v):return {'vr':vr,'Value':list(v)}
def record(uid,**kw):
 h={'00080018':tag('UI',uid),'00080016':tag('UI',CLASS_ROOT+'2'),'00080060':tag('CS','CT'),'0020000D':tag('UI','1.2.3'),'0020000E':tag('UI','1.2.3.1'),'00080020':tag('DA','20260101')}
 h.update(kw);return {'header':h}
REG={'version':'test','fields':[{'key':'manufacturer','tag':'00080070','vr':'LO','type':'string','appliesTo':['ALL'],'default':True},{'key':'sliceThickness','tag':'00180050','vr':'DS','type':'number','appliesTo':['CT'],'default':True}],'limits':{'maxGroups':64,'maxFacts':128,'maxSummaryBytes':98304,'maxExampleSources':3}}
class Tests(unittest.TestCase):
 def test_correlated_groups_prevent_cross_instance_match(self):
  rows=[record('1',**{'00080070':tag('LO','A'),'00180050':tag('DS','1')}),record('2',**{'00080070':tag('LO','B'),'00180050':tag('DS','5')})]
  s=project_series(rows,REG)
  self.assertFalse(any(g['values'].get('manufacturer')=='A' and g['values'].get('sliceThickness')==5 for g in s['acquisitionGroups']))
  self.assertEqual(len(s['acquisitionGroups']),2)
 def test_shared_and_per_frame_override(self):
  r=record('1',**{'00080016':tag('UI',CLASS_ROOT+'2.1'),'52009229':tag('SQ',{'00289110':tag('SQ',{'00180050':tag('DS','2')})}),'52009230':tag('SQ',{'00289110':tag('SQ',{'00180050':tag('DS','1')})},{})})
  s=project_series([r],REG)
  self.assertEqual({g['values']['sliceThickness'] for g in s['acquisitionGroups']},{1,2})
  self.assertEqual(s['frameContextsProcessed'],2)
 def test_invalid_numeric_is_quality_not_zero(self):
  s=project_series([record('1',**{'00180050':tag('DS','not-a-number')})],REG)
  self.assertNotIn('sliceThickness',s['acquisitionGroups'][0]['values'] if 'values' in s['acquisitionGroups'][0] else {})
  self.assertEqual(s['quality']['invalidValueCount'],1)
 def test_sop_class_controls_applicability(self):
  r=record('1',**{'00080016':tag('UI',CLASS_ROOT+'128'),'00080060':tag('CS','CT'),'00180050':tag('DS','4')})
  s=project_series([r],REG)
  self.assertEqual(s['families'],['PT']);self.assertNotIn('sliceThickness',s.get('coverage',{}))
 def test_overflow_is_explicit(self):
  s=project_series([record(str(i),**{'00080070':tag('LO',str(i))}) for i in range(4)],REG,limits={'maxGroups':1})
  self.assertEqual(s['detail']['totalGroups'],4);self.assertEqual(s['detail']['retainedGroups'],1);self.assertFalse(s['detail']['exactFilteringComplete'])
 def test_invalid_source_date_is_omitted(self):
  d=document_for_series([record('1',**{'00080020':tag('DA','00000101')})],REG)
  self.assertNotIn('studyDate',d);self.assertEqual(d['dateQuality'],'missingOrInvalidSourceDate')
 def test_sr_numeric_context_and_targets(self):
  c={'00080100':tag('SH','G-D7FE'),'00080102':tag('SH','SRT'),'00080104':tag('LO','Volume')}
  leaf={'0040A040':tag('CS','NUM'),'0040A043':tag('SQ',c),'0040A300':tag('SQ',{'0040A30A':tag('DS','3.5'),'004008EA':tag('SQ',{'00080100':tag('SH','cm3'),'00080102':tag('SH','UCUM')})})}
  image={'0040A040':tag('CS','IMAGE'),'00081199':tag('SQ',{'00081155':tag('UI','9.8.7'),'00081160':tag('IS',4)})}
  r=record('1',**{'00080016':tag('UI',CLASS_ROOT+'88.33'),'0040A040':tag('CS','CONTAINER'),'0040A730':tag('SQ',leaf,image)})
  facts=annotation_facts(r['header']);self.assertEqual(facts[0]['number'],3.5);self.assertEqual(facts[0]['unit']['code'],'cm3');self.assertEqual(facts[0]['targets'][0]['sopUid'],'9.8.7');self.assertEqual(facts[0]['targets'][0]['frames'],[4])
 def test_sr_target_contexts_are_shared_and_bounded(self):
  measurements=[{'0040A040':tag('CS','NUM'),'0040A300':tag('SQ',{'0040A30A':tag('DS',str(n))})} for n in [1,2]]
  images=[{'0040A040':tag('CS','IMAGE'),'00081199':tag('SQ',{'00081155':tag('UI',f'9.8.{n}')})} for n in range(10)]
  r=record('1',**{'00080016':tag('UI',CLASS_ROOT+'88.33'),'0040A040':tag('CS','CONTAINER'),'0040A730':tag('SQ',*(measurements+images))})
  s=project_series([r],REG)
  self.assertEqual(len(s['contexts']),1);self.assertEqual(s['contexts'][0]['targetCount'],10)
  self.assertEqual(len(s['contexts'][0]['targetExamples']),3)
  self.assertEqual(s['facts'][0]['targetContextId'],s['facts'][1]['targetContextId'])
  self.assertNotIn('targets',s['facts'][0])
  self.assertTrue(s['detail']['factValuesFilteringComplete']);self.assertFalse(s['detail']['targetFilteringComplete'])
 def test_empty_outputs_omitted_zero_retained(self):
  self.assertEqual(compact({'empty':'','zero':0,'false':False,'list':[]}),{'zero':0,'false':False})

