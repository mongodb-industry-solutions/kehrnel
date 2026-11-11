from urllib.parse import urlparse

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from kehrnel.api.app import create_app
import kehrnel.cli.unified as unified


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "registry.json"))
    client = TestClient(app)
    response = client.post("/v1/environments/dicom-preview/activate", json={
        "domain": "dicom", "strategy_id": "dicom.tiered_store", "version": "0.5.0",
        "config": {"database": "dicom_preview_contract"}, "bindings": {},
        "allow_plaintext_bindings": True,
    })
    assert response.status_code == 200, response.text
    return client


def test_http_discovery_activation_and_query_compilation_share_the_pack(client):
    catalog = client.get("/v1/strategies")
    assert catalog.status_code == 200
    assert "dicom.tiered_store" in catalog.text
    response = client.post("/v1/environments/dicom-preview/compile_query", json={
        "domain": "dicom", "query": {"version": "dicom-query/v1", "resource": "instances",
                                  "filter": {"seriesUid": "2.25.2001"}, "limit": 5},
    })
    assert response.status_code == 200, response.text
    assert "_kehrnel.instanceNumber" in response.text
    assert "header.0020000E.Value" not in response.text


def test_http_universal_operation_uses_manifest_contract_and_native_generator(client):
    response = client.post("/v1/environments/dicom-preview/run", json={
        "operation": "dicom_generate_synthetic", "domain": "dicom", "strategy_id": "dicom.tiered_store",
        "payload": {"instances": 3},
    })
    assert response.status_code == 200, response.text
    assert response.json()["result"]["synthetic"] is True
    assert len(response.json()["result"]["instances"]) == 6
    invalid = client.post("/v1/environments/dicom-preview/run", json={
        "operation": "dicom_generate_synthetic", "domain": "dicom",
        "payload": {"instances": 10000},
    })
    assert invalid.status_code >= 400


def test_cli_executes_dicom_operation_through_the_real_http_dispatcher(client, monkeypatch):
    def http_json(method, url, api_key=None, payload=None):
        response = client.request(method, urlparse(url).path, json=payload)
        return response.status_code, response.json()
    monkeypatch.setattr(unified, "_http_json", http_json)
    monkeypatch.setattr(unified, "_resolve_api_key", lambda value=None: None)
    result = CliRunner().invoke(unified.app, [
        "run", "dicom_generate_synthetic", "--env", "dicom-preview", "--domain", "dicom",
        "--strategy", "dicom.tiered_store", "--runtime-url", "http://testserver",
        "--set", "instances=3",
    ])
    assert result.exit_code == 0, result.stdout
    assert "dicom_generate_synthetic" in result.stdout


def test_http_query_catalog_exposes_only_active_research_capabilities(client):
    response = client.post('/v1/environments/dicom-preview/run', json={
        'operation': 'dicom_query_catalog', 'domain': 'dicom', 'payload': {}})
    assert response.status_code == 200, response.text
    result = response.json()['result']
    assert result['queryContract'] == 'dicom-query/v1'
    assert result['strategyVersion'] == '0.5.0'
    assert not next(row for row in result['resources'] if row['id'] == 'research')['enabled']
    assert result['headerInputSchema']['properties']['sopUids']['maxItems'] == 100


def test_cli_query_catalog_uses_the_same_http_operation(client, monkeypatch):
    def http_json(method, url, api_key=None, payload=None):
        response = client.request(method, urlparse(url).path, json=payload)
        return response.status_code, response.json()
    monkeypatch.setattr(unified, '_http_json', http_json)
    monkeypatch.setattr(unified, '_resolve_api_key', lambda value=None: None)
    result = CliRunner().invoke(unified.app, [
        'run', 'dicom_query_catalog', '--env', 'dicom-preview', '--domain', 'dicom',
        '--runtime-url', 'http://testserver'])
    assert result.exit_code == 0, result.stdout
    assert 'dicom-query-catalog/v1' in result.stdout


def test_http_header_batch_rejects_oversized_input_before_storage_access(client):
    response = client.post('/v1/environments/dicom-preview/run', json={
        'operation': 'dicom_get_headers', 'domain': 'dicom', 'payload': {
            'seriesUid': '2.25.2001', 'sopUids': ['2.25.' + str(i) for i in range(101)]}})
    assert response.status_code >= 400


def test_http_rule_discovery_and_correlated_demo_use_native_contract(client, monkeypatch):
    def operation(name, payload):
        response = client.post('/v1/environments/dicom-preview/run', json={
            'operation': name, 'domain': 'dicom', 'payload': payload})
        assert response.status_code == 200, response.text
        return response.json()['result']
    catalog = operation('dicom_rule_catalog', {})
    assert len(catalog['profiles']) == 11
    query = {'text': 'Find CT series where rows = 1024 and photometric = MONOCHROME2'}
    broad = operation('dicom_rule_demo', {'query': query})
    precise = operation('dicom_rule_demo', {'query': {**query, 'group': 'rendering'}})
    assert len(broad['queryResult']['rows']) == 1
    assert precise['queryResult']['rows'] == []
    assert not broad['preview']['persisted']
    def http_json(method, url, api_key=None, payload=None):
        response = client.request(method, urlparse(url).path, json=payload)
        return response.status_code, response.json()
    monkeypatch.setattr(unified, '_http_json', http_json)
    monkeypatch.setattr(unified, '_resolve_api_key', lambda value=None: None)
    cli = CliRunner().invoke(unified.app, ['run', 'dicom_rule_catalog', '--env', 'dicom-preview',
        '--domain', 'dicom', '--runtime-url', 'http://testserver'])
    assert cli.exit_code == 0, cli.stdout
    assert 'dicom-rule-profile/v1' in cli.stdout


def test_http_and_cli_execute_the_example_journey_against_real_mongo(tmp_path, monkeypatch):
    import json
    import os
    import uuid
    from pymongo import MongoClient
    from kehrnel.engine.strategies.dicom.tiered_store.catalog import EXAMPLES
    uri = os.environ.get('KEHRNEL_DICOM_TEST_MONGODB_URI')
    if not uri:
        pytest.skip('Set a disposable localhost MongoDB URI to exercise HTTP query execution.')
    assert urlparse(uri).hostname in {'localhost', '127.0.0.1', '::1'}
    database = 'kehrnel_dicom_http_' + uuid.uuid4().hex
    owner = MongoClient(uri, serverSelectionTimeoutMS=5000)
    owner.admin.command('ping')
    try:
        with TestClient(create_app(str(tmp_path / 'http-registry.json'))) as http:
            prefix = '/v1/environments/dicom-http-demo'
            activated = http.post(prefix + '/activate', json={
                'domain': 'dicom', 'strategy_id': 'dicom.tiered_store', 'version': '0.5.0',
                'config': {'database': database, 'projections': {'enabled': True}},
                'bindings': {'db': {'provider': 'mongodb', 'uri': uri, 'database': database}},
                'allow_plaintext_bindings': True})
            assert activated.status_code == 200, activated.text
            applied = http.post(prefix + '/apply', json={'domain': 'dicom'})
            assert applied.status_code == 200, applied.text
            def operation(name, payload):
                response = http.post(prefix + '/run', json={'domain': 'dicom', 'operation': name, 'payload': payload})
                assert response.status_code == 200, response.text
                return response.json()['result']
            rule_demo = operation('dicom_rule_demo', {})
            operation('dicom_rule_apply_indexes', {'profile': rule_demo['profile']})
            reception = operation('dicom_rule_receive', {**rule_demo['source'], 'profile': rule_demo['profile']})
            generation = operation('dicom_rule_process', {'batchId': reception['batchId']})
            cohort = operation('dicom_rule_query', {'profile': rule_demo['profile'],
                'generationId': generation['generationId'], 'allowPartial': True,
                'text': 'Find CT series where manufacturer = SYNTHETIC_VENDOR_B'})
            assert len(cohort['rows']) == 1 and cohort['executed']
            operation('dicom_generate_synthetic', EXAMPLES['dataset']['payload'])
            cursor = None
            while True:
                rebuilt = operation('dicom_rebuild_projections', {
                    **EXAMPLES['researchBuildExample']['payload'], **({'cursor': cursor} if cursor else {})})
                cursor = rebuilt['page']['nextCursor']
                if not cursor:
                    break
            for example in EXAMPLES['queries']:
                response = http.post(prefix + '/query', json={'domain': 'dicom', 'query': example['query']})
                assert response.status_code == 200, response.text
                assert len(response.json()['result']['rows']) == example['expectedRows']
            headers = operation('dicom_get_headers', EXAMPLES['headerExample']['payload'])
            assert headers['page']['available'] == 2
            def http_json(method, url, api_key=None, payload=None):
                response = http.request(method, urlparse(url).path, json=payload)
                return response.status_code, response.json()
            monkeypatch.setattr(unified, '_http_json', http_json)
            monkeypatch.setattr(unified, '_resolve_api_key', lambda value=None: None)
            request_path = tmp_path / 'research-query.json'
            request_path.write_text(json.dumps(EXAMPLES['queries'][-1]['query']))
            cli = CliRunner().invoke(unified.app, [
                'run', 'dicom_query', '--env', 'dicom-http-demo', '--domain', 'dicom',
                '--runtime-url', 'http://testserver', '--payload', str(request_path)])
            assert cli.exit_code == 0, cli.stdout
            assert 'same_instance; same_attribute_value_per_tag' in cli.stdout
    finally:
        # Only the exact database created by this test is removed.
        owner.drop_database(database)
        owner.close()


def test_http_metadata_contract_scopes_and_compiles_primary_study_search(client):
    def run(name,payload):return client.post('/v1/environments/dicom-preview/run',json={'operation':name,'domain':'dicom','payload':payload})
    response=run('dicom_metadata_catalog',{})
    assert response.status_code==200,response.text
    assert len(response.json()['result']['registry']['fields'])==159
    response=run('dicom_metadata_query',{'kind':'searchCombined','modality':'CT','manufacturer':'SIEMENS','maximum':2})
    assert response.status_code==200,response.text
    result=response.json()['result']
    assert result['collection']=='dicom_studies' and not result['executed'] and not result['extensionLookup']
    assert 'embeddedDocument' in result['pipelineNative']
    assert run('dicom_metadata_publish',{'sourceMode':'complete_selected_series_snapshot'}).status_code>=400
    assert run('dicom_metadata_query',{'kind':'searchCombined','centerIds':['FORBIDDEN']}).status_code>=400
