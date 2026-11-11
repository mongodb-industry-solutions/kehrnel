"""Validation-only delegation does not confer strategy or runtime authority."""
import json
import time
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric import rsa
import httpx
import pytest

from kehrnel.api.common.delegated_principal import load_authority_configuration
from tests.contract.test_delegated_principal import assertion, claims, signer
from tests.contract.test_strategy_retrieval_authorization import DELEGATOR, KEY, OWNER, make_app, fingerprint

PATH = "/v1/solution-blueprints/canonicalize"
GOLDEN = Path(__file__).parents[1] / "fixtures/architecture/v1/blueprint-hdl-golden.json"


@pytest.fixture
def validation_app(tmp_path, monkeypatch):
    app, _, refs, _, ceilings = make_app(tmp_path, monkeypatch)
    ceilings[fingerprint(DELEGATOR)]["actions"].append("blueprint:validate")
    monkeypatch.setenv("KEHRNEL_API_KEY_AUTHORITIES", json.dumps(ceilings))
    app.state.api_key_authorities = load_authority_configuration()
    return app, refs, signer(monkeypatch)


def validation_claims():
    value = claims(None, actions=["blueprint:validate"])
    value["strategyRefs"] = []
    return value


def blueprint():
    return json.loads(GOLDEN.read_text())


@pytest.mark.asyncio
async def test_shared_validation_with_empty_refs_has_no_artifact_or_runtime_access(validation_app, monkeypatch):
    app, _, private = validation_app
    registry = app.state.strategy_runtime.registry
    def forbidden(*args, **kwargs):
        raise AssertionError("Validation-only requests must not access named packages or activate anything")
    monkeypatch.setattr(registry, "get_manifest", forbidden)
    monkeypatch.setattr(app.state.strategy_runtime, "activate", forbidden)
    payload = blueprint()
    payload["preferredFoundationRefs"] = [{"kind": "strategy", "id": "private.unknown", "version": "1.0.0", "digest": "sha256:" + "f" * 64}]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.post(PATH, json=payload, headers={"x-api-key": DELEGATOR, "x-hdl-principal-assertion": assertion(private, validation_claims())})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["canonicalBlueprint"]["preferredFoundationRefs"] == payload["preferredFoundationRefs"]
    assert body["validationScope"] == "structural_only"
    assert all(body[field] == "not_evaluated" for field in ["catalogReadiness", "compilerReadiness", "activationReadiness"])
    assert not registry.activations


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["missing", "forged", "expired", "audience", "scope", "workspace", "action", "ceiling"])
async def test_validation_delegation_fails_closed(validation_app, case):
    app, refs, private = validation_app
    value = validation_claims()
    if case == "forged": private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    elif case == "expired":
        now = int(time.time()); value.update(iat=now-120, nbf=now-120, exp=now-60)
    elif case == "audience": value["aud"] = "other-instance"
    elif case == "scope": value["scope"] = value["workspace"] = {"kind": "team", "id": "not-admitted"}
    elif case == "workspace": value["workspace"] = {"kind": "individual", "id": "other"}
    elif case == "action": value = claims(refs[0], actions=["strategy:compile"])
    elif case == "ceiling": app.state.api_key_authorities[fingerprint(DELEGATOR)]["actions"].remove("blueprint:validate")
    headers = {"x-api-key": DELEGATOR}
    if case != "missing": headers["x-hdl-principal-assertion"] = assertion(private, value)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.post(PATH, json=blueprint(), headers=headers)
    assert response.status_code == (403 if case in {"missing", "action", "ceiling"} else 401)


@pytest.mark.asyncio
@pytest.mark.parametrize("actions", [["strategy:read"], ["strategy:compile"], ["runtime:query"], ["blueprint:validate", "strategy:read"], ["blueprint:validate", "strategy:compile"], ["blueprint:validate", "runtime:query"]])
async def test_empty_references_remain_invalid_for_any_package_or_runtime_action(validation_app, actions):
    app, _, private = validation_app
    value = validation_claims(); value["actions"] = actions
    value["environment"] = {"id": "hdl-a", "runtimeId": "runtime-a"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.post(PATH, json=blueprint(), headers={"x-api-key": DELEGATOR, "x-hdl-principal-assertion": assertion(private, value)})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_validation_only_authority_cannot_retrieve_compile_query_or_write(validation_app):
    app, refs, private = validation_app
    headers = {"x-api-key": DELEGATOR, "x-hdl-principal-assertion": assertion(private, validation_claims())}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers=headers) as client:
        for path in [f"/v1/strategies/{refs[0]['id']}", f"/v1/strategies/{refs[0]['id']}/spec", f"/v1/strategies/{refs[0]['id']}/assets/README.md"]:
            assert (await client.get(path)).status_code == 404
        assert (await client.post(f"/v1/strategies/{refs[0]['id']}/deployment-config/compile", json={"config": {}})).status_code == 404
        for path in ["/v1/environments/runtime-a/query", "/v1/environments/runtime-a/compile_query", "/v1/environments/runtime-a/activate", "/v1/architecture-resolutions/compile"]:
            assert (await client.post(path, json={"domain": "openehr"})).status_code == 403
        assert (await client.post("/solution-blueprints/canonicalize", json=blueprint())).status_code == 404
    assert not app.state.strategy_runtime.registry.activations


@pytest.mark.asyncio
async def test_standalone_api_key_validation_preserves_existing_auth_and_missing_key_denial(validation_app):
    app, _, _ = validation_app
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        assert (await client.post(PATH, json=blueprint())).status_code == 401
        assert (await client.post(PATH, json=blueprint(), headers={"x-api-key": "wrong-key"})).status_code == 401
        response = await client.post(PATH, json=blueprint(), headers={"x-api-key": KEY})
        assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_validation_only_ceiling_needs_no_package_inventory(validation_app):
    app, _, private = validation_app
    ceiling = app.state.api_key_authorities[fingerprint(DELEGATOR)]
    ceiling.update(actions=["blueprint:validate"], strategyRefs=[], environmentIds=[])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.post(PATH, json=blueprint(), headers={"x-api-key": DELEGATOR, "x-hdl-principal-assertion": assertion(private, validation_claims())})
    assert response.status_code == 200, response.text
