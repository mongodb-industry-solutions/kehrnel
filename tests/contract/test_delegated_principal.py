from copy import deepcopy
import json
import time

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import httpx
import jwt
import pytest

from kehrnel.engine.core.activation import EnvironmentActivation
from kehrnel.engine.core.strategy_access import Authority, Owner
from tests.contract.test_strategy_retrieval_authorization import make_app, KEY, DELEGATOR, OWNER, OTHER


def signer(monkeypatch):
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = private.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    monkeypatch.setenv("KEHRNEL_HDL_ISSUER_KEYS", json.dumps({"synthetic-key": public}))
    monkeypatch.setenv("KEHRNEL_HDL_ISSUER", "https://synthetic.hdl.test")
    monkeypatch.setenv("KEHRNEL_HDL_AUDIENCE", "synthetic-kehrnel")
    return private


def claims(ref, owner=OWNER, actions=None, environment=False):
    now = int(time.time())
    value = {"iss": "https://synthetic.hdl.test", "aud": "synthetic-kehrnel", "sub": "synthetic-actor",
             "iat": now, "nbf": now, "exp": now + 60, "jti": "synthetic-request", "v": 1,
             "scope": owner, "workspace": owner, "actions": actions or ["strategy:read"], "strategyRefs": [ref]}
    if environment:
        value["environment"] = {"id": "hdl-a", "runtimeId": "runtime-a"}
    return value


def assertion(private, value, headers=None):
    return jwt.encode(value, private, algorithm="RS256", headers=headers or {"kid": "synthetic-key", "typ": "hdl-delegated-principal+jwt"})


@pytest.mark.asyncio
async def test_two_owners_one_service_key_are_request_local(tmp_path, monkeypatch):
    app, _, refs, _, _ = make_app(tmp_path, monkeypatch)
    private = signer(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": DELEGATOR}) as client:
        for owner, ref in [(OWNER, refs[0]), (OTHER, refs[1]), (OWNER, refs[0])]:
            response = await client.get("/strategies", headers={"x-hdl-principal-assertion": assertion(private, claims(ref, owner))})
            assert [m["id"] for m in response.json()["strategies"]] == [ref["id"]]
        assert (await client.get("/strategies")).json() == {"strategies": []}
        assert not hasattr(app.state, "strategy_authority")


INVALID_CASES = ["forged", "expired", "future", "long", "issuer", "audience", "algorithm", "kid", "typ", "header_url", "missing", "unknown", "scope", "workspace", "actions", "duplicates", "ref_duplicates", "malformed_digest", "unresolved", "unadmitted", "version", "float_date", "boolean_date", "nbf", "oversized", "too_many_refs", "environment", "query_without_environment"]

@pytest.mark.asyncio
@pytest.mark.parametrize("case", INVALID_CASES)
async def test_invalid_assertions_fail_closed_before_retrieval(tmp_path, monkeypatch, case):
    app, _, refs, _, _ = make_app(tmp_path, monkeypatch)
    private = signer(monkeypatch)
    value = claims(deepcopy(refs[0])); headers = {"kid": "synthetic-key", "typ": "hdl-delegated-principal+jwt"}
    now = int(time.time())
    if case == "forged": private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    elif case == "expired": value.update(iat=now-100, nbf=now-100, exp=now-40)
    elif case == "future": value.update(iat=now+20, nbf=now+20, exp=now+80)
    elif case == "long": value["exp"] += 1
    elif case == "issuer": value["iss"] = "https://attacker.test"
    elif case == "audience": value["aud"] = "other-instance"
    elif case == "kid": headers["kid"] = "unknown"
    elif case == "typ": headers["typ"] = "JWT"
    elif case == "header_url": headers["jku"] = "https://attacker.test/keys"
    elif case == "missing": del value["workspace"]
    elif case == "unknown": value["publish"] = True
    elif case == "scope": value["scope"] = value["workspace"] = {"kind": "team", "id": "unadmitted-team"}
    elif case == "workspace": value["workspace"] = OTHER
    elif case == "actions": value["actions"] = ["runtime:write"]
    elif case == "duplicates": value["actions"] *= 2
    elif case == "ref_duplicates": value["strategyRefs"] *= 2
    elif case == "malformed_digest": value["strategyRefs"][0]["packageDigest"] = "sha256:bad"
    elif case == "unresolved": value["strategyRefs"][0]["packageDigest"] = "sha256:" + "0"*64
    elif case == "unadmitted": app.state.strategy_access_policy = type(app.state.strategy_access_policy)()
    elif case == "version": value["v"] = True
    elif case == "float_date": value["iat"] = float(value["iat"])
    elif case == "boolean_date": value["iat"] = True
    elif case == "nbf": value["nbf"] -= 1
    elif case == "oversized": value["jti"] = "x"*9000
    elif case == "too_many_refs": value["strategyRefs"] *= 65
    elif case == "environment": value["environment"] = {"id": "hdl-a", "runtimeId": "runtime-a", "owner": OWNER}
    elif case == "query_without_environment": value["actions"] = ["runtime:query"]
    token = jwt.encode(value, "synthetic-secret", algorithm="HS256", headers=headers) if case == "algorithm" else assertion(private, value, headers)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.get("/strategies", headers={"x-api-key": DELEGATOR, "x-hdl-principal-assertion": token})
        assert response.status_code == 401, (case, response.text)
        assert response.json()["error"]["code"] == "INVALID_PRINCIPAL_ASSERTION"
        assert token not in response.text


@pytest.mark.asyncio
async def test_non_delegator_assertion_and_missing_assertion_never_grant_private_access(tmp_path, monkeypatch):
    app, _, refs, _, _ = make_app(tmp_path, monkeypatch); private = signer(monkeypatch)
    token = assertion(private, claims(refs[0]))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        assert (await client.get("/strategies", headers={"x-api-key": KEY, "x-hdl-principal-assertion": token})).status_code == 401
        assert (await client.get("/strategies/openehr.rps_dual", headers={"x-api-key": DELEGATOR})).status_code == 404
        assert (await client.get("/strategies", headers={"x-hdl-principal-assertion": token})).status_code == 401
        # Ordinary installation keys keep their explicit package scope.
        assert (await client.get("/strategies/openehr.rps_dual", headers={"x-api-key": KEY})).status_code == 200


@pytest.mark.asyncio
async def test_key_ceiling_intersection_and_no_general_delegated_writes(tmp_path, monkeypatch):
    app, _, refs, _, _ = make_app(tmp_path, monkeypatch); private = signer(monkeypatch)
    ceiling = next(v for v in app.state.api_key_authorities.values() if v["kind"] == "hdl-delegator")
    ceiling["actions"] = ["strategy:read"]; ceiling["strategyRefs"] = [refs[0]]
    value = claims(refs[0], actions=["strategy:read", "strategy:compile"])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": DELEGATOR, "x-hdl-principal-assertion": assertion(private, value)}) as client:
        assert (await client.get("/strategies/openehr.rps_dual")).status_code == 200
        assert (await client.post("/strategies/openehr.rps_dual/deployment-config/compile", json={"config": {}})).status_code == 404
        for path in ("/strategies/load", "/v1/strategies/load", "/environments/runtime-a/activate", "/ops", "/api/domains/openehr/query/aql"):
            assert (await client.post(path, json={})).status_code == 403
        assert (await client.get("/strategies/installed")).status_code == 403
        value = claims(refs[1], OTHER)
        assert (await client.get("/strategies", headers={"x-hdl-principal-assertion": assertion(private, value)})).json() == {"strategies": []}


@pytest.mark.asyncio
async def test_runtime_query_requires_own_action_exact_binding_and_no_activation(tmp_path, monkeypatch):
    app, manifests, refs, _, _ = make_app(tmp_path, monkeypatch); private = signer(monkeypatch)
    runtime = app.state.strategy_runtime
    calls = []
    async def dispatch(env_id, op, payload): calls.append((env_id, op)); return {"rows": []}
    async def forbidden(*args, **kwargs): raise AssertionError("No implicit activation")
    monkeypatch.setattr(runtime, "dispatch", dispatch); monkeypatch.setattr(runtime, "activate", forbidden)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": DELEGATOR}) as client:
        for action in ("strategy:read", "strategy:compile"):
            token = assertion(private, claims(refs[0], actions=[action], environment=True))
            # Without a runtime:query grant, compilation/reading cannot execute clinical queries.
            response = await client.post("/environments/runtime-a/query", json={"domain": "openehr"}, headers={"x-hdl-principal-assertion": token})
            assert response.status_code == 403
            assert not calls
        token = assertion(private, claims(refs[0], actions=["runtime:query"], environment=True))
        response = await client.post("/environments/runtime-a/query", json={"domain": "openehr"}, headers={"x-hdl-principal-assertion": token})
        assert response.status_code == 409 and response.json()["error"]["code"] == "PREPARATION_REQUIRED"
        assert not runtime.registry.activations and not calls
        runtime.registry.activations["runtime-a"] = {"openehr": EnvironmentActivation("runtime-a", "openehr", manifests[0].id, manifests[0].version)}
        assert (await client.post("/environments/runtime-b/query", json={"domain": "openehr"}, headers={"x-hdl-principal-assertion": token})).status_code == 403
        assert not calls
        assert (await client.post("/environments/runtime-a/query", json={"domain": "openehr"}, headers={"x-hdl-principal-assertion": token})).status_code == 200
        assert calls == [("runtime-a", "query")]
        runtime.registry.activations["runtime-a"]["openehr"].version = "other-version"
        assert (await client.post("/environments/runtime-a/query", json={"domain": "openehr"}, headers={"x-hdl-principal-assertion": token})).status_code == 409
        assert len(calls) == 1


@pytest.mark.asyncio
async def test_installation_run_query_cannot_bypass_separate_query_grant(tmp_path, monkeypatch):
    app, manifests, _, _, _ = make_app(tmp_path, monkeypatch)
    runtime = app.state.strategy_runtime
    runtime.registry.activations["runtime-a"] = {"openehr": EnvironmentActivation("runtime-a", "openehr", manifests[0].id, manifests[0].version)}
    calls = []
    async def dispatch(*args): calls.append(args); return {}
    monkeypatch.setattr(runtime, "dispatch", dispatch)
    ceiling = next(v for v in app.state.api_key_authorities.values() if v["kind"] == "installation" and v["scopes"] == [OWNER])
    ceiling["actions"] = ["strategy:read", "strategy:compile"]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": KEY}) as client:
        for prefix in ("", "/v1"):
            response = await client.post(prefix + "/environments/runtime-a/run", json={"operation": "query", "domain": "openehr"})
            assert response.status_code == 403
            assert not calls
        assert (await client.post("/environments/runtime-a/compile_query", json={"domain": "openehr"})).status_code == 200
        assert len(calls) == 1
