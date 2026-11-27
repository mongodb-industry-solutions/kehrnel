import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
import pytest
from fastapi import HTTPException
from kehrnel.api.domains.openehr.composition import dependencies
from kehrnel.api.strategies.openehr.rps_dual.synthetic import context as factory, service
from kehrnel.api.strategies.openehr.rps_dual.synthetic import routes as normal_routes
from kehrnel.api.strategies.openehr.rps_dual_ibm.synthetic import routes as ibm_routes
from kehrnel.api.strategies.openehr.rps_dual.synthetic.models import SyntheticDataRequest
from kehrnel.engine.strategies.openehr.rps_dual_ibm.strategy import RPSDualIBMStrategy


def request(env, shared):
    cfg = deepcopy(RPSDualIBMStrategy().defaults)
    cfg['collections']['compositions']['name'] = env + '_flat'
    cfg['collections']['search']['name'] = env + '_search'
    scoped = {'env_id': env, 'db': object(), 'database_name': env,
              'activation': SimpleNamespace(strategy_id='openehr.rps_dual_ibm', config=cfg),
              'synced': {k: env + '_' + k for k in ('canonical_collection', 'flat_collection', 'search_collection', 'contributions_collection', 'ehr_collection')}}
    return SimpleNamespace(context=scoped, app=SimpleNamespace(state=shared))


@pytest.mark.asyncio
@pytest.mark.parametrize('routes', [ibm_routes, normal_routes])
async def test_concurrent_synthetic_requests_keep_database_codec_and_collections(monkeypatch, routes):
    shared = SimpleNamespace(flattener=object(), target_db=object(), config={'target': {'search_compositions_merge': True}})
    requests = [request(env, shared) for env in ('one', 'two')]
    async def resolve(req, **kw):
        await asyncio.sleep(0)
        return req.context
    async def build(self, ctx):
        await asyncio.sleep(0)
        return SimpleNamespace(env_id=ctx.environment_id, db=ctx.adapters['storage'].db)
    for module in (factory, dependencies, routes): monkeypatch.setattr(module, 'resolve_active_openehr_context', resolve)
    monkeypatch.setattr(RPSDualIBMStrategy, '_build_flattener_for_context', build)
    calls = []
    async def generate(**kw):
        calls.append(kw)
        return []
    monkeypatch.setattr(routes, 'generate_synthetic_data', generate)
    async def run(req):
        flattener = await routes.get_flattener(req)
        await routes.generate_synthetic_data_endpoint(req, SyntheticDataRequest(count=1, base_composition={'_type': 'COMPOSITION'}), db=shared.target_db, flattener=flattener)
    await asyncio.gather(*(run(req) for req in requests))
    assert len(calls) == 2
    for call in calls:
        req = next(r for r in requests if r.context['env_id'] == call['flattener'].env_id)
        assert call['db'] is req.context['db'] is call['flattener'].db
        assert call['config'].database == req.context['env_id']
        assert call['config'].compositions == req.context['synced']['canonical_collection']
        assert call['config'].flatten_compositions == req.context['synced']['flat_collection']
        assert call['merge_search_docs'] is False


@pytest.mark.asyncio
async def test_ibm_generation_does_not_return_driver_exception_text(monkeypatch):
    req = request('one', SimpleNamespace())
    async def resolve(*a, **kw): return req.context
    monkeypatch.setattr(ibm_routes, 'resolve_active_openehr_context', resolve)
    monkeypatch.setattr(dependencies, 'resolve_active_openehr_context', resolve)
    async def fail(**kw): raise RuntimeError('mongodb://synthetic:secret@example.test')
    monkeypatch.setattr(ibm_routes, 'generate_synthetic_data', fail)
    with pytest.raises(HTTPException) as exc:
        await ibm_routes.generate_synthetic_data_endpoint(req, SyntheticDataRequest(count=1, base_composition={'_type':'COMPOSITION'}), db=req.context['db'], flattener=object())
    assert exc.value.status_code == 500
    assert 'mongodb' not in exc.value.detail and 'secret' not in exc.value.detail


@pytest.mark.asyncio
async def test_scoped_configuration_failure_does_not_fall_back(monkeypatch):
    async def denied(*a, **kw): raise HTTPException(status_code=403, detail='Denied')
    monkeypatch.setattr(dependencies, 'resolve_active_openehr_context', denied)
    with pytest.raises(HTTPException) as exc: await dependencies.get_composition_config(SimpleNamespace())
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_generation_service_passes_scoped_config_to_composition_writer(monkeypatch):
    scoped_config, database, codec = object(), object(), object()
    async def ehr(**kw): return SimpleNamespace(ehr_id=SimpleNamespace(value='synthetic-ehr'))
    async def add(**kw):
        assert kw['config'] is scoped_config and kw['db'] is database and kw['flattener'] is codec
        return SimpleNamespace(uid='synthetic-composition', time_created=datetime(2026, 1, 1, tzinfo=timezone.utc))
    monkeypatch.setattr(service, 'create_ehr', ehr)
    monkeypatch.setattr(service, 'add_composition', add)
    monkeypatch.setattr(service.SyntheticDataGenerator, 'generate_synthetic_composition', lambda self: {'_type':'COMPOSITION', 'archetype_details': {'template_id': {'value':'synthetic'}}})
    records = await service.generate_synthetic_data(database, {'_type':'COMPOSITION'}, 1, codec, scoped_config)
    assert records[0]['composition_uid'] == 'synthetic-composition'
