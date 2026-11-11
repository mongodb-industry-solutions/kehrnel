from types import SimpleNamespace
from urllib.parse import urlencode
import pytest
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.domains.snomedct.implicit_valuesets import implicit_snomed_valueset
from kehrnel.engine.strategies.snomedct.mongodb.strategy import SNOMEDCTMongoDBStrategy
from kehrnel.engine.strategies.fhir.resource_store.scripts.valueset_expansion import expand_implicit_valuesets

URL='http://snomed.info/sct?fhir_vs=ecl/<<84114007'
@pytest.mark.parametrize('url,expression',[(URL,'<<84114007'),('http://snomed.info/sct?fhir_vs=ecl%2F%3C%3C84114007','<<84114007'),('http://snomed.info/sct?fhir_vs=refset/447562003','^ 447562003')])
def test_implicit_urls_parse_without_a_saved_registry_entry(url,expression):
    assert implicit_snomed_valueset(url)['expression']==expression

def test_version_and_named_urls_are_distinct():
    value=implicit_snomed_valueset('http://snomed.info/sct/900000000000207008/version/20260601?fhir_vs=ecl/84114007')
    assert value['version'].endswith('/version/20260601')
    assert implicit_snomed_valueset('https://example.org/ValueSet/heart-failure') is None

@pytest.mark.parametrize('url',['http://snomed.info/sct?fhir_vs=all','http://snomed.info/sct?fhir_vs=ecl/','http://snomed.info/sct?fhir_vs=refset/not-a-code','http://snomed.info/sct?fhir_vs=ecl/1&fhir_vs=ecl/2'])
def test_invalid_implicit_selectors_fail_before_database_access(url):
    with pytest.raises(KehrnelError):implicit_snomed_valueset(url)

class Storage:
    def __init__(self):self.calls=[]
    async def aggregate(self,collection,pipeline):
        self.calls.append((collection,pipeline))
        return [{'conceptId':'84114007','fsn':'Heart failure (disorder)'},{'conceptId':'42343007','fsn':'Congestive heart failure (disorder)'}]
    @property
    def db(self):raise AssertionError('Implicit expansion must not consult a saved ValueSet')

def context():
    manifest=load_strategy('snomedct.mongodb','src/kehrnel/engine/strategies/snomedct/mongodb')
    storage=Storage();return StrategyContext(environment_id='tenant-a',manifest=manifest,config={**manifest.default_config,'source':{'mode':'customer_owned'}},adapters={'storage':storage}),storage

@pytest.mark.asyncio
async def test_native_expansion_includes_focus_and_descendants_without_registration():
    ctx,storage=context();result=await SNOMEDCTMongoDBStrategy(ctx.manifest).snomed_expand_value_set(ctx,{'url':URL,'limit':257})
    assert [r['code'] for r in result['expansion']['contains']]==['84114007','42343007']
    assert result['complete'] is True and result['releaseId']=='20260601'
    assert result['explain']['ast']['operator']=='<<'
    assert result['explain']['ast']['conceptId']=='84114007'

@pytest.mark.asyncio
@pytest.mark.parametrize('query',[urlencode({'code:below':'http://snomed.info/sct|84114007'}),urlencode({'code:in':URL})])
async def test_fhir_filters_use_the_same_native_expansion(query):
    ctx,storage=context();strategy=SNOMEDCTMongoDBStrategy(ctx.manifest)
    class Adapter:
        async def execute(self,op,payload):
            assert op=='expand';return await strategy.snomed_expand_value_set(ctx,{**payload,'limit':payload['count']})
    ctx.adapters['terminology']=Adapter()
    rewritten,code_filter,evidence=await expand_implicit_valuesets(ctx,query,None)
    assert not rewritten and code_filter['codes']==['42343007','84114007']
    assert evidence['complete'] and evidence['releaseId']=='20260601'

@pytest.mark.asyncio
async def test_partial_native_pages_never_become_an_incomplete_fhir_filter():
    ctx,_=context()
    class Adapter:
        async def execute(self,*args):return {'concepts':[{'conceptId':'84114007'}],'page':{'hasMore':True},'complete':False}
    ctx.adapters['terminology']=Adapter()
    with pytest.raises(KehrnelError) as exc:await expand_implicit_valuesets(ctx,urlencode({'code:in':URL}),None)
    assert exc.value.code=='FHIR_VALUESET_EXPANSION_TOO_LARGE'

@pytest.mark.asyncio
async def test_implicit_membership_does_not_require_registering_a_value_set(monkeypatch):
    ctx,storage=context();strategy=SNOMEDCTMongoDBStrategy(ctx.manifest)
    async def valid(*args):return {'ok':True,'valid':True,'code':'84114007'}
    monkeypatch.setattr(strategy,'snomed_validate_code',valid)
    result=await strategy.snomed_validate_value_set(ctx,{'url':URL,'code':'84114007'})
    assert result['valid'] and result['inValueSet']


def test_fhir_native_expansion_keeps_displays_and_does_not_invent_a_total():
    from kehrnel.api.domains.fhir.routes import _native_fhir_valueset
    members = [{"system": "http://snomed.info/sct", "code": "84114007", "display": "Heart failure"}]
    result = {"expansion": {"contains": members, "offset": 25}, "complete": False}
    value_set = _native_fhir_valueset(result, {"url": URL})
    assert value_set["expansion"]["contains"] == members
    assert value_set["expansion"]["offset"] == 25
    assert "total" not in value_set["expansion"]
    assert value_set["expansion"]["parameter"][0]["valueBoolean"] is True
    result["complete"] = True
    result["expansion"]["offset"] = 0
    assert _native_fhir_valueset(result, {"url": URL})["expansion"]["total"] == 1

@pytest.mark.asyncio
async def test_implicit_url_selects_snomed_when_other_system_providers_exist():
    from test_terminology_service import _Runtime
    from kehrnel.engine.terminology.service import TerminologyService
    runtime = _Runtime(native=True, terminology={"providers": [{
        "id": "loinc", "type": "fhir-terminology", "base_url": "https://terminology.example/fhir",
        "systems": ["http://loinc.org"],
    }]})
    result = await TerminologyService(runtime, "tenant-a").execute("expand", {"url": URL, "count": 257})
    assert result["provider"]["id"] == "snomed-local"
    assert runtime.calls[0][2]["payload"]["system"] == "http://snomed.info/sct"


def test_value_set_members_use_canonical_concept_descriptions():
    from kehrnel.engine.strategies.snomedct.mongodb.strategy import _valueset_contains
    rows = [{"conceptId": "84114007", "descriptions": [
        {"term": "Historical label", "languageCode": "en", "active": False},
        {"term": "Heart failure (disorder)", "languageCode": "en", "active": True},
        {"term": "Insuficiencia cardíaca", "languageCode": "es", "active": True},
    ]}]
    assert _valueset_contains(rows, {"language": "en"})[0]["display"] == "Heart failure (disorder)"
    assert _valueset_contains(rows, {"displayLanguage": "es"})[0]["display"] == "Insuficiencia cardíaca"
