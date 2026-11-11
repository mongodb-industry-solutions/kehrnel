from copy import deepcopy
import hashlib
import json
from pathlib import Path

import httpx
import pytest

from kehrnel.api import app as app_module
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.strategy_access import Authority, Owner, PackageRef, StrategyAccessPolicy
from kehrnel.engine.core.strategy_identity import package_reference
from tests.contract.test_strategy_registration_integrity import pack

ROOT = Path(__file__).resolve().parents[2] / "src/kehrnel/engine/strategies/openehr"
OWNER = {"kind": "team", "id": "team-a"}
OTHER = {"kind": "team", "id": "team-b"}
KEY = "synthetic-installation-key"
DELEGATOR = "synthetic-hdl-delegator-key"
OTHER_KEY = "synthetic-other-key"


def fingerprint(key):
    return hashlib.sha256(key.encode()).hexdigest()[:20]


def make_app(tmp_path, monkeypatch, publication=None, dependencies=None):
    manifests = [load_strategy("openehr." + name, ROOT / name) for name in ("rps_dual", "rps_dual_ibm")]
    refs = [package_reference(m) for m in manifests]
    records = [{"ref": refs[0], "owner": OWNER, "assets": ["README.md"], "dependencies": dependencies or []},
               {"ref": refs[1], "owner": OTHER, "assets": []}]
    if publication:
        records[0]["publication"] = publication
    sidecar = tmp_path / "access.json"
    sidecar.write_text(json.dumps({"contract": "strategy-access/v1", "records": records}))
    ceilings = {fingerprint(KEY): {"kind": "installation", "scopes": [OWNER], "actions": ["strategy:read", "strategy:compile", "runtime:query"], "strategyRefs": refs, "environmentIds": ["runtime-a"]},
                fingerprint(OTHER_KEY): {"kind": "installation", "scopes": [OTHER], "actions": ["strategy:read"], "strategyRefs": refs, "environmentIds": ["runtime-b"]},
                fingerprint(DELEGATOR): {"kind": "hdl-delegator", "scopes": [OWNER, OTHER], "actions": ["strategy:read", "strategy:compile", "runtime:query"], "strategyRefs": refs, "environmentIds": ["runtime-a", "runtime-b"]}}
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "true")
    monkeypatch.setenv("KEHRNEL_API_KEYS", ",".join([KEY, OTHER_KEY, DELEGATOR]))
    monkeypatch.setenv("KEHRNEL_ADMIN_API_KEYS", ",".join([KEY, DELEGATOR]))
    monkeypatch.setenv("KEHRNEL_API_KEY_ENV_SCOPES", json.dumps({KEY: ["runtime-a"], OTHER_KEY: ["runtime-b"], DELEGATOR: ["runtime-a", "runtime-b"]}))
    monkeypatch.setenv("KEHRNEL_API_KEY_AUTHORITIES", json.dumps(ceilings))
    monkeypatch.setenv("KEHRNEL_STRATEGY_ACCESS_FILE", str(sidecar))
    monkeypatch.setenv("KEHRNEL_RATE_LIMIT", "0")
    monkeypatch.delenv("CORE_MONGODB_URL", raising=False)
    monkeypatch.setattr(app_module, "_load_manifests", lambda: (manifests, [], {m.id: m.pack_root for m in manifests}))
    app = app_module.create_app(str(tmp_path / "registry.json"), str(tmp_path / "bundles"))
    return app, manifests, refs, records, ceilings


@pytest.mark.asyncio
async def test_known_id_cross_owner_unknown_parity_and_aliases(tmp_path, monkeypatch):
    app, _, _, _, _ = make_app(tmp_path, monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        for prefix in ("", "/v1"):
            for suffix in ("", "/spec", "/assets/README.md", "/endpoints"):
                private = await client.get(prefix + "/strategies/openehr.rps_dual" + suffix, headers={"x-api-key": OTHER_KEY})
                unknown = await client.get(prefix + "/strategies/openehr.unknown" + suffix, headers={"x-api-key": OTHER_KEY})
                assert private.status_code == unknown.status_code == 404
                assert private.json() == unknown.json()
            response = await client.get(prefix + "/strategies", headers={"x-api-key": KEY})
            assert [m["id"] for m in response.json()["strategies"]] == ["openehr.rps_dual"]
        assert (await client.get("/strategies")).json() == {"strategies": []}
        assert (await client.get("/api/persistence-strategies/openehr.rps_dual", headers={"x-api-key": OTHER_KEY})).status_code == 404


@pytest.mark.asyncio
async def test_entitled_manifest_spec_declared_asset_and_compile(tmp_path, monkeypatch):
    app, _, refs, _, _ = make_app(tmp_path, monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": KEY}) as client:
        detail = await client.get("/strategies/openehr.rps_dual")
        assert detail.status_code == 200 and detail.json()["packageDigest"] == refs[0]["packageDigest"]
        assert "pack_root" not in detail.text
        for path in ("/spec", "/assets/README.md", "/endpoints"):
            assert (await client.get("/strategies/openehr.rps_dual" + path)).status_code == 200
        for path in ("strategy.py", "manifest.json", "spec.json", "../strategy.py", "%252e%252e/strategy.py"):
            assert (await client.get("/strategies/openehr.rps_dual/assets/" + path)).status_code != 200
        response = await client.post("/strategies/openehr.rps_dual/deployment-config/compile", json={"config": {"database": "synthetic"}, "templates": [{"templateId": "Synthetic", "fields": [{"path": "/context/start_time/value", "rmType": "DV_DATE_TIME"}]}]})
        assert response.status_code == 200, response.text
        assert response.json()["readiness"]["runtime"] == "not_observed"
        assert not app.state.strategy_runtime.registry.activations


@pytest.mark.asyncio
async def test_docs_openapi_components_and_inventory_are_separate(tmp_path, monkeypatch):
    app, _, _, _, _ = make_app(tmp_path, monkeypatch)
    # Add a private schema reachable only through the inaccessible IBM strategy.
    schema = app.openapi()
    schema["paths"]["/api/strategies/openehr/rps_dual_ibm/private"] = {"get": {"responses": {"200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/PrivateOnly"}}}}}}}
    schema.setdefault("components", {}).setdefault("schemas", {})["PrivateOnly"] = {"type": "string", "description": "private marker"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        for path in ("/docs/strategies/openehr/rps_dual", "/redoc/strategies/openehr/rps_dual", "/openapi/strategies/openehr/rps_dual.json", "/openapi/strategies/openehr.rps_dual.json"):
            assert (await client.get(path, headers={"x-api-key": OTHER_KEY})).status_code == 404
            assert (await client.get(path, headers={"x-api-key": KEY})).status_code == 200
        anonymous = await client.get("/openapi.json")
        assert not any("/api/strategies/" in path or "/api/domains/" in path for path in anonymous.json()["paths"])
        entitled = await client.get("/openapi.json", headers={"x-api-key": KEY})
        assert "PrivateOnly" not in entitled.json()["components"].get("schemas", {})
        assert "rps_dual_ibm/private" not in entitled.text
        assert entitled.headers["cache-control"] == "private, no-store"
        assert (await client.get("/strategies/installed", headers={"x-api-key": KEY})).status_code == 403
        assert (await client.get("/strategies/diagnostics", headers={"x-api-key": KEY})).status_code == 403
        monkeypatch.setenv("KEHRNEL_INVENTORY_API_KEY_FINGERPRINTS", fingerprint(KEY))
        assert len((await client.get("/strategies/installed", headers={"x-api-key": KEY})).json()["strategies"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("publication,public", [({"visibility": "public", "approved": True}, True), ({"visibility": "public", "approved": "true"}, False), ({"visibility": "internal", "approved": True}, False)])
async def test_explicit_publication_and_no_missing_sidecar_grant(tmp_path, monkeypatch, publication, public):
    app, _, _, _, _ = make_app(tmp_path, monkeypatch, publication)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.get("/strategies/openehr.rps_dual")
        assert response.status_code == (200 if public else 404)
        app.state.strategy_access_policy = StrategyAccessPolicy()
        assert (await client.get("/strategies", headers={"x-api-key": KEY})).json() == {"strategies": []}


def test_dependency_closure_cycles_and_asset_symlinks(tmp_path):
    from kehrnel.engine.core.registry import FileActivationRegistry
    registry = FileActivationRegistry(tmp_path / "registry.json")
    first = pack(tmp_path / "first")
    second = pack(tmp_path / "second")
    second.id = "synthetic.dependency"
    data = json.loads((Path(second.pack_root) / "manifest.json").read_text()); data["id"] = second.id
    (Path(second.pack_root) / "manifest.json").write_text(json.dumps(data))
    registry.register_manifest(first); registry.register_manifest(second)
    refs = [package_reference(m) for m in (first, second)]
    records = [{"ref": refs[0], "owner": OWNER, "publication": {"visibility": "public", "approved": True}, "dependencies": [refs[1]], "assets": ["code.py"]}, {"ref": refs[1], "owner": OWNER}]
    policy = StrategyAccessPolicy(records)
    authority = Authority(Owner.parse(OWNER), frozenset(PackageRef.parse(r) for r in refs), frozenset({"strategy:read"}))
    assert not policy.allowed(registry, PackageRef.parse(refs[0]))
    assert policy.allowed(registry, PackageRef.parse(refs[0]), authority)
    records[1]["dependencies"] = [refs[0]]
    assert not StrategyAccessPolicy(records).allowed(registry, PackageRef.parse(refs[0]), authority)
    # A declared symlink still cannot be served, and changing package bytes also revokes its old admission.
    (Path(first.pack_root) / "alias.txt").symlink_to(Path(first.pack_root) / "code.py")
    with pytest.raises(KehrnelError):
        policy.asset(registry, first.id, "alias.txt", authority)


def test_unregistered_changed_content_and_undeclared_dependency_fail_closed(tmp_path):
    from kehrnel.engine.core.registry import FileActivationRegistry
    registry = FileActivationRegistry(tmp_path / "registry.json")
    manifest = pack(tmp_path / "pack"); registry.register_manifest(manifest)
    old_ref = package_reference(manifest)
    (Path(manifest.pack_root) / "code.py").write_text("new content")
    new_ref = package_reference(manifest)
    policy = StrategyAccessPolicy([{"ref": new_ref, "publication": {"visibility": "public", "approved": True}}])
    assert old_ref != new_ref
    assert not policy.allowed(registry, PackageRef.parse(new_ref))
    # A package-declared dependency must match the admitted exact closure.
    second = pack(tmp_path / "second")
    second.id = "synthetic.with-dependency"
    data = json.loads((Path(second.pack_root) / "manifest.json").read_text()); data.update(id=second.id, dependencies=[old_ref])
    (Path(second.pack_root) / "manifest.json").write_text(json.dumps(data))
    second = second.model_copy(update={"dependencies": [old_ref]}); registry.register_manifest(second)
    ref = package_reference(second)
    policy = StrategyAccessPolicy([{"ref": ref, "publication": {"visibility": "public", "approved": True}}])
    assert not policy.allowed(registry, PackageRef.parse(ref))


@pytest.mark.asyncio
async def test_load_conflict_preserves_registry_and_sidecar_before_admission(tmp_path, monkeypatch):
    app, manifests, _, _, _ = make_app(tmp_path, monkeypatch)
    before = app.state.strategy_runtime.registry.path.read_bytes()
    sidecar = tmp_path / "access.json"; access_before = sidecar.read_bytes()
    root = tmp_path / "candidate"; candidate = pack(root)
    data = json.loads((root / "manifest.json").read_text()); data["id"] = manifests[0].id
    (root / "manifest.json").write_text(json.dumps(data)); candidate.id = manifests[0].id
    from kehrnel.api.core.admin import routes
    monkeypatch.setattr(routes, "load_strategy", lambda *a: candidate)
    app.state.allowed_strategy_paths = [tmp_path]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": KEY}) as client:
        response = await client.post("/strategies/load", json={"path": str(root), "owner": OTHER, "publication": {"approved": True}})
        assert response.status_code == 409
        assert app.state.strategy_runtime.registry.path.read_bytes() == before
        assert sidecar.read_bytes() == access_before


def test_loader_contains_spec_and_does_not_import_candidate_packages(tmp_path, monkeypatch):
    import sys
    from kehrnel.engine.core.pack_loader import contained_package_file, validate_installed_pack
    root = tmp_path / "candidate"; root.mkdir()
    (root / "__init__.py").write_text("raise AssertionError('candidate code must not run')")
    (root / "strategy.py").write_text("class Strategy: pass")
    data = {"id": "synthetic.discover", "name": "Synthetic", "version": "1", "domain": "synthetic", "entrypoint": "candidate.strategy:Strategy", "ops": []}
    (root / "manifest.json").write_text(json.dumps(data))
    monkeypatch.syspath_prepend(str(tmp_path))
    assert validate_installed_pack(data, root) == []
    assert "candidate" not in sys.modules
    for relative in ("../outside.json", "/tmp/outside.json", "sub/../outside.json", "%2e%2e/file", "x\\file"):
        with pytest.raises(KehrnelError): contained_package_file(root, relative)
    outside = tmp_path / "outside.json"; outside.write_text("{}")
    (root / "spec.json").symlink_to(outside)
    with pytest.raises(KehrnelError): contained_package_file(root, "spec.json")


@pytest.mark.asyncio
async def test_bound_operation_refuses_cross_owner_package_before_dispatch(tmp_path, monkeypatch):
    from kehrnel.engine.core.activation import EnvironmentActivation
    app, manifests, _, _, _ = make_app(tmp_path, monkeypatch)
    runtime = app.state.strategy_runtime
    runtime.registry.activations["runtime-a"] = {"openehr": EnvironmentActivation("runtime-a", "openehr", manifests[1].id, manifests[1].version)}
    calls = []
    async def dispatch(*args): calls.append(args); return {}
    monkeypatch.setattr(runtime, "dispatch", dispatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": KEY}) as client:
        for path, body in [("/ops", {"environment": "runtime-a", "domain": "openehr", "op": "synthetic"}),
                           ("/environments/runtime-a/run", {"operation": "synthetic", "domain": "openehr"}),
                           ("/environments/runtime-a/activations/openehr/ops/synthetic", {}),
                           ("/environments/runtime-a/plan", {"domain": "openehr"})]:
            assert (await client.post(path, json=body)).status_code == 404
            assert not calls
        response = await client.get("/environments/runtime-a/capabilities")
        assert response.json()["domains"] == []
        assert (await client.get("/environments/runtime-a")).json()["activations"] == {}


@pytest.mark.asyncio
async def test_registration_cannot_replace_operator_owner_or_publication(tmp_path, monkeypatch):
    app, manifests, _, _, _ = make_app(tmp_path, monkeypatch)
    from kehrnel.api.core.admin import routes
    monkeypatch.setattr(routes, "load_strategy", lambda *a: manifests[0])
    app.state.allowed_strategy_paths = [Path(manifests[0].pack_root)]
    before = app.state.strategy_runtime.registry.path.read_bytes()
    sidecar_before = (tmp_path / "access.json").read_bytes()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": KEY}) as client:
        response = await client.post("/strategies/load", json={"path": manifests[0].pack_root, "owner": OTHER, "publication": {"visibility": "public", "approved": True}})
        assert response.status_code == 403
        assert response.json()["error"]["code"] == "ACCESS_RECORDS_OPERATOR_OWNED"
    assert app.state.strategy_runtime.registry.path.read_bytes() == before
    assert (tmp_path / "access.json").read_bytes() == sidecar_before
