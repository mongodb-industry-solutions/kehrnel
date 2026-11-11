"""Private design-reference integrity, whole-record filtering and authority.

Synthetic self-hashed fixtures test structure/security only. They are neither
operator qualification, clinical semantics nor live target evidence.
"""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from cryptography.hazmat.primitives.asymmetric import rsa
import httpx
import jsonschema
import pytest

from kehrnel.api.common.delegated_principal import load_authority_configuration
from kehrnel.api.core import design_catalogs
from kehrnel.contracts.architecture_foundation import (
    ArchitectureResolutionInputCatalog, SemanticExecutionBindingV1, TargetCapabilityProfile,
)
from kehrnel.engine.core.architecture_catalog import (
    ArchitectureInputCatalogError, architecture_input_record_content_digest,
    load_architecture_resolution_context, semantic_binding_content_digest, target_capability_content_digest,
)
from kehrnel.engine.core.source_profiles import load_source_profile_catalog
from kehrnel.engine.core.strategy_access import StrategyAccessPolicy
from tests.contract.test_delegated_principal import assertion, claims, signer
from tests.contract.test_strategy_retrieval_authorization import (
    DELEGATOR, KEY, OTHER_KEY, OWNER, OTHER, fingerprint, make_app,
)

PATH = "/v1/architecture-resolution-inputs"
SRC = Path(__file__).resolve().parents[2] / "src"
SCHEMAS = SRC / "kehrnel/contracts/schemas/v1"


def fixture_ref(kind, name):
    return {"kind": kind, "id": name, "version": "1", "digest": "sha256:" + hashlib.sha256((kind + ':' + name).encode()).hexdigest()}


def record(package, owner=OWNER, name="synthetic-a", bindings=True):
    target = {"contract": "target-capability-profile/v1", "artifactRef": fixture_ref("target-capability-profile", name),
              "deploymentId": name, "mongodbVersion": "unobserved", "topology": {"deploymentModel": "synthetic-design-reference"},
              "capabilities": [{"capabilityId": "mongo.pipeline", "availability": "unknown"}]}
    target["artifactRef"]["digest"] = target_capability_content_digest(target)
    authority = fixture_ref("openehr-template", name + '.authority')
    component = fixture_ref("implementation-component", name + '.planner')
    binding = {"contract": "semantic-execution-binding/v1", "artifactRef": fixture_ref("semantic-execution-binding", name),
               "semanticAuthorityRef": authority,
               "strategyRef": {"kind": "strategy", "id": package["id"], "version": package["version"], "digest": package["packageDigest"]},
               "pathBindings": [{"semanticPath": "/uid/value", "physicalPath": "comp_id", "valueType": "synthetic-design-metadata"}],
               "consumers": [{"consumerId": "planner", "kind": "query_planner", "componentRef": component}]}
    binding["artifactRef"]["digest"] = semantic_binding_content_digest(binding)
    raw = {"artifactRef": fixture_ref("architecture-input-record", name), "owner": owner,
           "requiredPackageRefs": [package], "sourceProfileRefs": [load_source_profile_catalog().profiles[0].artifact_ref.to_wire()],
           "evidenceBasis": "design_reference", "evidenceRefs": [authority, component],
           "targetCapabilityProfiles": [target], "semanticExecutionBindings": [binding] if bindings else []}
    raw["artifactRef"]["digest"] = architecture_input_record_content_digest(raw)
    return raw


def write_context(path, records):
    path.write_text(json.dumps({"contract": "architecture-resolution-context/v1", "records": records}))
    return path


def inputs_app(tmp_path, monkeypatch, **kwargs):
    app, manifests, refs, access, ceilings = make_app(tmp_path, monkeypatch, **kwargs)
    path = write_context(tmp_path / "operator-inputs.json", [record(refs[0])])
    monkeypatch.setenv("KEHRNEL_ARCHITECTURE_INPUTS_FILE", str(path))
    return app, manifests, refs, access, ceilings, path


def assert_private(response):
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["vary"] == "X-API-Key, X-HDL-Principal-Assertion"


def assert_unavailable(response):
    assert response.status_code == 503, response.text
    assert response.json() == {"error": {"code": "ARCHITECTURE_INPUT_CATALOG_UNAVAILABLE", "message": "Architecture inputs unavailable", "details": {}}}
    assert_private(response)


def test_normalized_models_schemas_and_digest_rules(tmp_path):
    package = next(iter(design_catalogs.PROFILE_PACKAGE_REQUIREMENTS.values()))[0].as_dict()
    raw = record(package)
    original = deepcopy(raw)
    models = load_architecture_resolution_context(write_context(tmp_path / 'inputs.json', [raw]))
    item = models[0]
    for model, name in [(item.inputs, 'architecture-resolution-input-catalog'), (item.inputs.target_capability_profiles[0], 'target-capability-profile'), (item.inputs.semantic_execution_bindings[0], 'semantic-execution-binding')]:
        jsonschema.validate(model.to_wire(), json.loads((SCHEMAS / (name + '.schema.json')).read_text()))
        assert type(model).model_validate(model.to_wire()) == model
    target = raw['targetCapabilityProfiles'][0]
    target['topology']['shardCount'] = None
    assert target_capability_content_digest(target) == original['targetCapabilityProfiles'][0]['artifactRef']['digest']
    target['artifactRef']['id'] = 'new-id'
    assert target_capability_content_digest(target) == original['targetCapabilityProfiles'][0]['artifactRef']['digest']
    assert architecture_input_record_content_digest(raw) != original['artifactRef']['digest']
    binding = raw['semanticExecutionBindings'][0]
    binding['pathBindings'][0]['unitPolicy'] = None
    assert semantic_binding_content_digest(binding) == original['semanticExecutionBindings'][0]['artifactRef']['digest']
    assert item.inputs.semantic_execution_bindings[0].covers_interaction('arbitrary-id', write=False)
    # Reusability in authority scope does not qualify arbitrary workload semantics.
    assert item.inputs.target_capability_profiles[0].capabilities[0].availability.value == 'unknown'


@pytest.mark.parametrize('case', ['target-kind','binding-kind','no-target','duplicate-target','duplicate-binding','duplicate-capability','duplicate-region','duplicate-path','duplicate-consumer','duplicate-record','target-tamper','binding-tamper','record-tamper','profile-version','profile-digest','package-version','authority-evidence','component-evidence','unknown-field','evidence-basis','empty-evidence','duplicate-package'])
def test_context_integrity_fails_closed(tmp_path, case):
    package = next(iter(design_catalogs.PROFILE_PACKAGE_REQUIREMENTS.values()))[0].as_dict()
    raw = record(package); target = raw['targetCapabilityProfiles'][0]; binding = raw['semanticExecutionBindings'][0]
    records = [raw]
    if case == 'target-kind': target['artifactRef']['kind'] = 'strategy'
    elif case == 'binding-kind': binding['artifactRef']['kind'] = 'strategy'
    elif case == 'no-target': raw['targetCapabilityProfiles'] = []
    elif case == 'duplicate-target': raw['targetCapabilityProfiles'] *= 2
    elif case == 'duplicate-binding': raw['semanticExecutionBindings'] *= 2
    elif case == 'duplicate-capability': target['capabilities'] *= 2
    elif case == 'duplicate-region': target['topology']['regions'] = ['x','x']
    elif case == 'duplicate-path': binding['pathBindings'] *= 2
    elif case == 'duplicate-consumer': binding['consumers'] *= 2
    elif case == 'duplicate-record': records *= 2
    elif case == 'target-tamper': target['capabilities'][0]['availability'] = 'available'
    elif case == 'binding-tamper': binding['pathBindings'][0]['physicalPath'] = 'invented'
    elif case == 'record-tamper': raw['owner'] = OTHER
    elif case == 'profile-version': raw['sourceProfileRefs'][0]['version'] = 'stale'
    elif case == 'profile-digest': raw['sourceProfileRefs'][0]['digest'] = fixture_ref('source-profile','changed')['digest']
    elif case == 'package-version': raw['requiredPackageRefs'][0]['version'] = 'stale'
    elif case == 'authority-evidence': raw['evidenceRefs'] = raw['evidenceRefs'][1:]
    elif case == 'component-evidence': raw['evidenceRefs'] = raw['evidenceRefs'][:1]
    elif case == 'unknown-field': raw['reviewed'] = True
    elif case == 'evidence-basis': raw['evidenceBasis'] = 'observed_runtime'
    elif case == 'empty-evidence': raw['evidenceRefs'] = []
    elif case == 'duplicate-package': raw['requiredPackageRefs'] *= 2
    # Rehash envelope when model-valid: inner definition/link checks must stand alone.
    if case not in {'record-tamper','unknown-field','evidence-basis','empty-evidence','duplicate-package','target-kind','binding-kind','no-target','duplicate-target','duplicate-binding','duplicate-capability','duplicate-region','duplicate-path','duplicate-consumer'}:
        raw['artifactRef']['digest'] = architecture_input_record_content_digest(raw)
    with pytest.raises(ArchitectureInputCatalogError, match='^Architecture inputs unavailable$'):
        load_architecture_resolution_context(write_context(tmp_path / 'inputs.json', records))


@pytest.mark.parametrize('reverse', [False, True])
def test_conflicting_immutable_target_rejected_without_order_selection(tmp_path, reverse):
    package = next(iter(design_catalogs.PROFILE_PACKAGE_REQUIREMENTS.values()))[0].as_dict()
    a, b = record(package), record(package, name='synthetic-b')
    t = b['targetCapabilityProfiles'][0]
    t['artifactRef'].update(a['targetCapabilityProfiles'][0]['artifactRef'])
    t['artifactRef']['digest'] = target_capability_content_digest(t)
    b['artifactRef']['digest'] = architecture_input_record_content_digest(b)
    with pytest.raises(ArchitectureInputCatalogError):
        load_architecture_resolution_context(write_context(tmp_path/'inputs.json', [b,a] if reverse else [a,b]))


@pytest.mark.parametrize('case', ['unset','missing','invalid-json','invalid-utf8','undigestible-float','undigestible-text','wrong-envelope','package-root','symlink-file','symlink-directory'])
@pytest.mark.asyncio
async def test_operator_file_errors_private_and_bounded(tmp_path, monkeypatch, case):
    app, _, _, _, _, path = inputs_app(tmp_path, monkeypatch)
    if case == 'unset': monkeypatch.delenv('KEHRNEL_ARCHITECTURE_INPUTS_FILE')
    elif case == 'missing': monkeypatch.setenv('KEHRNEL_ARCHITECTURE_INPUTS_FILE', str(tmp_path/'absent'))
    elif case == 'invalid-json': path.write_text('{')
    elif case == 'invalid-utf8': path.write_bytes(b'\xff\xfe')
    elif case in {'undigestible-float','undigestible-text'}:
        raw = json.loads(path.read_text()); target = raw['records'][0]['targetCapabilityProfiles'][0]
        if case == 'undigestible-float': target['capabilities'][0]['constraints'] = {'unsupported': 0.5}
        else: target['mongodbVersion'] = '\ud800'
        if case == "undigestible-float": TargetCapabilityProfile.model_validate(target)
        path.write_text(json.dumps(raw))
    elif case == 'wrong-envelope': path.write_text('{"contract":"wrong","records":[]}')
    elif case == 'package-root': app.state.architecture_input_package_roots += (str(tmp_path),)
    elif case == 'symlink-file':
        link = tmp_path/'link.json'; link.symlink_to(path); monkeypatch.setenv('KEHRNEL_ARCHITECTURE_INPUTS_FILE', str(link))
    elif case == 'symlink-directory':
        link = tmp_path/'link'; link.symlink_to(tmp_path, target_is_directory=True); monkeypatch.setenv('KEHRNEL_ARCHITECTURE_INPUTS_FILE', str(link/path.name))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test', headers={'x-api-key': KEY}) as client:
        assert_unavailable(await client.get(PATH, params={'path': str(path)}))


@pytest.mark.asyncio
async def test_http_unknown_target_empty_binding_no_side_effect(tmp_path, monkeypatch):
    app, _, refs, _, _, path = inputs_app(tmp_path, monkeypatch)
    raw = record(refs[0], bindings=False); write_context(path, [raw])
    def forbidden(*args, **kwargs): raise AssertionError('Catalog read must not compile/activate/execute')
    for method in ('activate','dispatch'): monkeypatch.setattr(app.state.strategy_runtime, method, forbidden)
    before = app.state.strategy_runtime.registry.path.read_bytes()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test', headers={'x-api-key': KEY}) as client:
        response = await client.get(PATH); assert response.status_code == 200, response.text; assert_private(response)
        model = ArchitectureResolutionInputCatalog.model_validate(response.json())
        assert model.target_capability_profiles[0].capabilities[0].availability.value == 'unknown'
        assert model.semantic_execution_bindings == []
        assert 'owner' not in response.text and 'evidence' not in response.text and 'packageDigest' not in response.text
        assert (await client.post('/v1/architecture-resolutions/compile', json={})).status_code == 404
    assert app.state.strategy_runtime.registry.path.read_bytes() == before
    assert not app.state.strategy_runtime.registry.activations


@pytest.mark.asyncio
async def test_two_owners_shared_key_whole_record_filtering_and_revocation(tmp_path, monkeypatch):
    app, _, refs, access, _, path = inputs_app(tmp_path, monkeypatch, publication={'visibility':'public','approved':True})
    first, second = record(refs[0]), record(refs[0], OTHER, name='private-other')
    write_context(path,[first,second]); private = signer(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test', headers={'x-api-key': DELEGATOR}) as client:
        for owner, expected, excluded in [(OWNER,'synthetic-a','private-other'),(OTHER,'private-other','synthetic-a'),(OWNER,'synthetic-a','private-other')]:
            response = await client.get(PATH, headers={'x-hdl-principal-assertion': assertion(private, claims(refs[0], owner))})
            assert response.status_code == 200, response.text; assert_private(response)
            assert expected in response.text and excluded not in response.text
            assert len(response.json()['targetCapabilityProfiles']) == len(response.json()['semanticExecutionBindings']) == 1
        access[0].pop('publication'); access[0]['owner'] = OTHER
        app.state.strategy_access_policy = StrategyAccessPolicy(access)
        assert_unavailable(await client.get(PATH, headers={'x-hdl-principal-assertion': assertion(private, claims(refs[0]))}))


@pytest.mark.parametrize('case',['cross-owner','unadmitted','registry-drift','missing-package','dependency','no-read','no-ref','public-no-ref','empty-records','wrong-profile-association','partial-record'])
@pytest.mark.asyncio
async def test_whole_record_prerequisites_rechecked(tmp_path, monkeypatch, case):
    app, _, refs, access, _, path = inputs_app(tmp_path, monkeypatch)
    key = KEY; registry = app.state.strategy_runtime.registry
    if case == 'cross-owner': key = OTHER_KEY
    elif case == 'unadmitted': app.state.strategy_access_policy = StrategyAccessPolicy()
    elif case == 'registry-drift': registry.manifest_identities[refs[0]['id']] = {**refs[0], 'version':'changed'}
    elif case == 'missing-package': registry.manifests.pop(refs[0]['id'])
    elif case == 'dependency':
        access[0]['dependencies'] = [refs[1]]; app.state.strategy_access_policy = StrategyAccessPolicy(access)
    elif case == 'no-read': app.state.api_key_authorities[fingerprint(KEY)]['actions'] = ['strategy:compile']
    elif case in {'no-ref','public-no-ref'}:
        app.state.api_key_authorities[fingerprint(KEY)]['strategyRefs'] = []
        if case == 'public-no-ref':
            access[0]['publication'] = {'visibility':'public','approved':True}; app.state.strategy_access_policy = StrategyAccessPolicy(access)
    elif case == 'empty-records': write_context(path, [])
    elif case == 'wrong-profile-association': write_context(path,[record(refs[1])])
    elif case == 'partial-record':
        raw = record(refs[0]); raw['requiredPackageRefs'].append(refs[1]); raw['artifactRef']['digest'] = architecture_input_record_content_digest(raw); write_context(path,[raw])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test', headers={'x-api-key': key}) as client:
        assert_unavailable(await client.get(PATH))


@pytest.mark.parametrize('case',['missing-key','invalid-key','misconfigured','missing-assertion','forged','expired','audience','scope','ceiling','empty-read','validate-only','compile-only'])
@pytest.mark.asyncio
async def test_auth_action_denials_before_loading(tmp_path, monkeypatch, case):
    app, _, refs, _, ceilings, _ = inputs_app(tmp_path, monkeypatch)
    ceilings[fingerprint(DELEGATOR)]['actions'].append('blueprint:validate')
    monkeypatch.setenv('KEHRNEL_API_KEY_AUTHORITIES', json.dumps(ceilings)); app.state.api_key_authorities = load_authority_configuration()
    private = signer(monkeypatch); value = claims(deepcopy(refs[0])); headers={'x-api-key': DELEGATOR}; status=401
    if case == 'missing-key': headers={}
    elif case == 'invalid-key': headers={'x-api-key':'invalid'}
    elif case == 'misconfigured': monkeypatch.delenv('KEHRNEL_API_KEYS'); status=503
    elif case == 'missing-assertion': status=403
    elif case == 'forged': private = rsa.generate_private_key(public_exponent=65537,key_size=2048)
    elif case == 'expired':
        now=int(time.time()); value.update(iat=now-100,nbf=now-100,exp=now-40)
    elif case == 'audience': value['aud']='other-instance'
    elif case == 'scope': value['scope']=value['workspace']={'kind':'team','id':'unadmitted'}
    elif case == 'ceiling': app.state.api_key_authorities[fingerprint(DELEGATOR)]['actions']=['strategy:compile']; status=403
    elif case == 'empty-read': value['strategyRefs']=[]
    elif case == 'validate-only': value.update(actions=['blueprint:validate'],strategyRefs=[]); status=403
    elif case == 'compile-only': value['actions']=['strategy:compile']; status=403
    if case not in {'missing-key','invalid-key','misconfigured','missing-assertion'}: headers['x-hdl-principal-assertion']=assertion(private,value)
    def forbidden(*args,**kwargs): raise AssertionError('Denied request loaded private metadata')
    monkeypatch.setattr(design_catalogs,'load_architecture_resolution_context',forbidden)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://test') as client:
        response=await client.get(PATH,headers=headers); assert response.status_code==status,response.text; assert_private(response)


def test_sdk_own_package_root_and_import_independence(tmp_path):
    code = '''
import sys
from pathlib import Path
from kehrnel.engine.core.architecture_catalog import ArchitectureInputCatalogError, load_architecture_resolution_context
for path in (None, Path(__import__('kehrnel').__file__).parent/'engine/source_profiles/catalog.json'):
    try: load_architecture_resolution_context(path)
    except ArchitectureInputCatalogError: pass
    else: raise AssertionError('SDK package/default inputs accepted')
assert not any(n.startswith(('kehrnel.api','kehrnel.engine.strategies','fastapi','jwt')) for n in sys.modules)
assert not any(n in sys.modules for n in ('kehrnel.engine.core.architecture_plan_compiler','kehrnel.engine.core.implementation_package','kehrnel.contracts.schema_registry'))
'''
    env={k:v for k,v in os.environ.items() if not k.startswith(('KEHRNEL_HDL','KEHRNEL_API_KEY'))}; env.update(PYTHONPATH=str(SRC),PYTHONDONTWRITEBYTECODE='1')
    result=subprocess.run([sys.executable,'-c',code],cwd=tmp_path,env=env,capture_output=True,text=True)
    assert result.returncode==0,result.stderr


@pytest.mark.asyncio
async def test_absent_inputs_preserve_structural_blueprint_save(tmp_path, monkeypatch):
    app, _, _, _, ceilings, path=inputs_app(tmp_path,monkeypatch)
    write_context(path,[]); ceilings[fingerprint(DELEGATOR)]['actions'].append('blueprint:validate')
    monkeypatch.setenv('KEHRNEL_API_KEY_AUTHORITIES',json.dumps(ceilings)); app.state.api_key_authorities=load_authority_configuration()
    private=signer(monkeypatch); value=claims(None,actions=['blueprint:validate']); value['strategyRefs']=[]
    body=json.loads((SRC.parent/'tests/fixtures/architecture/v1/blueprint-hdl-golden.json').read_text())
    pin=load_source_profile_catalog().profiles[0].artifact_ref.to_wire();body['participants'][0]['sourceProfileRef']=pin
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://test') as client:
        assert_unavailable(await client.get(PATH,headers={'x-api-key':KEY}))
        response=await client.post('/v1/solution-blueprints/canonicalize',json=body,headers={'x-api-key':DELEGATOR,'x-hdl-principal-assertion':assertion(private,value)})
        assert response.status_code==200,response.text; receipt=response.json()
        assert receipt['canonicalBlueprint']['participants'][0]['sourceProfileRef']==pin
        assert receipt['validationScope']=='structural_only'
        assert all(receipt[k]=='not_evaluated' for k in ('catalogReadiness','compilerReadiness','activationReadiness'))
