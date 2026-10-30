from copy import deepcopy
import json
from pathlib import Path
import shutil

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from kehrnel.api.app import create_app, _load_manifests
from kehrnel.contracts.deployment_configuration import DeploymentCompilationRequest
from kehrnel.contracts.package_digest import compute_package_digest
from kehrnel.engine.core.deployment_configuration import compile_deployment_configuration
from kehrnel.engine.core.deployment_identity import package_identity
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.registry import FileActivationRegistry

ROOT = Path(__file__).resolve().parents[2] / 'src/kehrnel/engine/strategies/openehr'
TEMPLATE = {'templateId': 'Boundary.Example', 'fields': [{'path': '/context/start_time/value', 'rmType': 'DV_DATE_TIME'}]}

def request(**kwargs):
    return DeploymentCompilationRequest(config={'database': 'isolated'}, templates=[TEMPLATE], **kwargs)

def prepare_external_package(root, profile, monkeypatch, tag):
    shutil.copytree(ROOT/profile, root, ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
    source=root/'manifest.json'
    data=json.loads(source.read_text())
    module, class_name=data['entrypoint'].split(':')
    # A real external entrypoint must live inside its pack; retain the native engine as a dependency.
    entrypoint='boundary_pack_'+tag
    (root/(entrypoint+'.py')).write_text(f'from {module} import {class_name}\n')
    data['entrypoint']=entrypoint+':'+class_name
    source.write_text(json.dumps(data))
    monkeypatch.syspath_prepend(str(root))

def test_compiler_requires_admin_auth_before_reading_or_compiling(tmp_path, monkeypatch):
    monkeypatch.setenv('KEHRNEL_AUTH_ENABLED', 'true')
    monkeypatch.setenv('KEHRNEL_API_KEYS', 'reader-key,admin-key')
    monkeypatch.setenv('KEHRNEL_ADMIN_API_KEYS', 'admin-key')
    with TestClient(create_app(str(tmp_path / 'registry.json'))) as client:
        url='/strategies/openehr.rps_dual/deployment-config/compile'
        for headers, status in [({},401), ({'x-api-key':'invalid'},401), ({'x-api-key':'reader-key'},403), ({'x-api-key':'admin-key'},200)]:
            response=client.post(url, json=request().model_dump(), headers=headers)
            assert response.status_code == status, response.text
            if status == 200:
                assert response.json()['input'] == request().model_dump(mode='json')
        assert not client.app.state.strategy_runtime.registry.activations

@pytest.mark.asyncio
@pytest.mark.parametrize('profile', ['rps_dual', 'rps_dual_ibm'])
async def test_loaded_package_controls_digest_and_dictionary_after_registry_reload(tmp_path, monkeypatch, profile):
    root=tmp_path / profile
    prepare_external_package(root, profile, monkeypatch, profile)
    seed=root/'bundles/dictionaries/_codes.json'
    contents=json.loads(seed.read_text())
    dictionary=next(doc for doc in contents if doc.get('_id') == 'ar_code') if isinstance(contents,list) else contents
    dictionary['_pack_origin']='isolated-package'
    seed.write_text(json.dumps(contents))
    manifest=load_strategy('openehr.'+profile, root)
    registry=FileActivationRegistry(tmp_path/'registry.json')
    registry.register_manifest(manifest)
    loaded=FileActivationRegistry(tmp_path/'registry.json').get_manifest(manifest.id)
    assert loaded.pack_root == str(root.resolve())
    assert 'pack_root' not in loaded.model_dump()
    identity=package_identity(loaded)
    assert identity['packageDigest'] == compute_package_digest(root)
    assert identity['packageDigest'] != compute_package_digest(ROOT/profile)
    result=await compile_deployment_configuration(loaded, request(packageDigest=identity['packageDigest']))
    assert result['dictionaries']['codes']['snapshot']['_pack_origin'] == 'isolated-package'
    with pytest.raises(KehrnelError) as error:
        await compile_deployment_configuration(loaded, request(packageDigest=compute_package_digest(ROOT/profile)))
    assert error.value.code == 'DEPLOYMENT_PACKAGE_MISMATCH'

def test_discovery_preserves_the_real_package_root(tmp_path, monkeypatch):
    from kehrnel.api import app as app_module
    root=tmp_path/'native'
    prepare_external_package(root, 'rps_dual', monkeypatch, 'discovery')
    monkeypatch.setattr(app_module,'_strategy_paths',lambda:[tmp_path])
    manifests, _, _ = _load_manifests()
    assert len(manifests) == 1
    assert manifests[0].pack_root == str(root.resolve())

@pytest.mark.parametrize('pins', [{'version':''},{'version':'  '},{'packageDigest':''},{'packageDigest':'sha256:invalid'}])
def test_explicit_empty_or_malformed_pins_are_rejected(pins):
    with pytest.raises(ValidationError): request(**pins)

@pytest.mark.asyncio
@pytest.mark.parametrize('fields', [
    [{'path':'/context/value','rmType':'DV_TEXT','extract':'value.value'}, {'path':'/context/value','rmType':'DV_COUNT','extract':'value.value'}],
    [{'path':'/context/value','extract':'value'}],
    [{'path':'/context/value','rmType':'CLUSTER','extract':'value.value'}],
])
async def test_strict_analytics_rejects_duplicate_types_and_unresolved_terminals(fields):
    with pytest.raises(KehrnelError) as error:
        await compile_deployment_configuration(load_strategy('openehr.rps_dual',ROOT/'rps_dual'), DeploymentCompilationRequest(config={'database':'isolated'}, templates=[{'templateId':'Invalid','fields':fields}]))
    assert error.value.code == 'DEPLOYMENT_ANALYTICS_INVALID'

@pytest.mark.asyncio
@pytest.mark.parametrize('config', [{'serviceKey':'secret'}, {'transform':{'token':'secret'}}, {'transform':{'envelope':{'base':{'source':'https://user:password@example.test/config'}}}}, {'transform':{'privateKey':'secret'}}, {'transform': {'envelope': {'base': {'auth_token': 'secret'}}}}, {'transform': {'envelope': {'base': {'db_uri': 'secret'}}}}, {'transform': {'envelope': {'base': {'x-api-key': 'secret'}}}}, {'transform': {'envelope': {'base': {'authTokenValue': 'secret'}}}}])
async def test_credentials_cannot_enter_compiler_inputs_or_echo(config):
    with pytest.raises(KehrnelError) as error:
        await compile_deployment_configuration(load_strategy('openehr.rps_dual',ROOT/'rps_dual'), DeploymentCompilationRequest(config={'database':'isolated',**config},templates=[TEMPLATE]))
    assert error.value.code == 'DEPLOYMENT_SECRET_FORBIDDEN'
