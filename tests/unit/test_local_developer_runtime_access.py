import json
from types import SimpleNamespace
import pytest
from starlette.requests import Request
from kehrnel.api.core.admin.routes import _local_developer_runtime_allowed, _require_environment_runtime_access, run_env_op, list_env_endpoints
from kehrnel.engine.core.errors import KehrnelError

@pytest.fixture
def local(monkeypatch):
    values={'KEHRNEL_AUTH_ENABLED':'true','KEHRNEL_LOCAL_DEVELOPER_RUNTIME':'true','CORE_MONGODB_URL':'mongodb://127.0.0.1:27917','CORE_DATABASE_NAME':'hdl_dev_core','KEHRNEL_API_KEYS':'developer','KEHRNEL_ADMIN_API_KEYS':'operator','KEHRNEL_API_KEY_ENV_SCOPES':json.dumps({'developer':['env-hdl-developer'],'operator':['env-hdl-developer']})}
    for k,v in values.items():monkeypatch.setenv(k,v)
    return values

def request(key='developer',runtime=None):
    return Request({'type':'http','headers':[(b'x-api-key',key.encode())],'app':SimpleNamespace(state=SimpleNamespace(strategy_runtime=runtime)),'scheme':'http','server':('127.0.0.1',8187),'path':'/','query_string':b''})

def test_local_key_is_scoped_and_not_admin(local):
    _require_environment_runtime_access(request(),'env-hdl-developer')
    for key,env in [('unknown','env-hdl-developer'),('developer','other-env')]:
        with pytest.raises(KehrnelError):_require_environment_runtime_access(request(key),env)

@pytest.mark.parametrize('key,value',[('KEHRNEL_LOCAL_DEVELOPER_RUNTIME','false'),('CORE_MONGODB_URL','mongodb+srv://atlas.example'),('CORE_MONGODB_URL','mongodb://user:password@127.0.0.1'),('CORE_MONGODB_URL','mongodb://127.0.0.1,external.example'),('CORE_MONGODB_URL','mongodb://127.0.0.1/?replicaSet=remote'),('CORE_DATABASE_NAME','hdl_core'),('KEHRNEL_API_KEY_ENV_SCOPES','{"developer":"*"}')])
def test_nonlocal_or_unscoped_requires_admin(local,monkeypatch,key,value):
    monkeypatch.setenv(key,value)
    assert not _local_developer_runtime_allowed(request(),'env-hdl-developer')
    with pytest.raises(KehrnelError):_require_environment_runtime_access(request(),'env-hdl-developer')

@pytest.mark.asyncio
async def test_operator_operations_cannot_use_run_as_bypass(local):
    class Runtime:
        async def dispatch(self,*args):raise AssertionError('Must not dispatch')
    for op in ['plan','apply','snomed_ingest_release','unrecognized_extension']:
        res=await run_env_op('env-hdl-developer',request(runtime=Runtime()),{'operation':op,'domain':'snomedct'})
        assert res.status_code==403

@pytest.mark.asyncio
async def test_local_transform_dispatches_and_endpoint_discovery_works(local):
    class Runtime:
        registry=SimpleNamespace(list_activations=lambda env:{})
        async def dispatch(self,env,op,payload):return {'env':env,'op':op}
    rt=Runtime()
    result=await run_env_op('env-hdl-developer',request(runtime=rt),{'operation':'transform','domain':'context','payload':{}})
    assert result['ok'] and result['result']['op']=='transform'
    endpoints=await list_env_endpoints('env-hdl-developer',request(runtime=rt))
    assert endpoints['env_id']=='env-hdl-developer'
