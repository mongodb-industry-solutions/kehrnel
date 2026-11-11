"""Catalog integrity, exact package admission and request-local read authority."""
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from cryptography.hazmat.primitives.asymmetric import rsa
import httpx
import jsonschema
import pytest

from kehrnel.api.common.delegated_principal import load_authority_configuration
from kehrnel.api.core import design_catalogs
from kehrnel.contracts.source_architecture import SourceProfile, SourceProfileCatalog
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.source_profiles import (
    SourceProfileCatalogError,
    load_source_profile_catalog, source_profile_content_digest, source_profile_index,
)
from kehrnel.engine.core.strategy_access import Authority, Owner, PackageRef, StrategyAccessPolicy
from kehrnel.engine.core.strategy_identity import package_reference
from tests.contract.test_delegated_principal import assertion, claims, signer
from tests.contract.test_strategy_retrieval_authorization import (
    DELEGATOR, KEY, OTHER_KEY, OWNER, OTHER, fingerprint, make_app,
)

PATH = "/v1/source-profiles"
SRC = Path(__file__).resolve().parents[2] / "src"
SCHEMAS = SRC / "kehrnel/contracts/schemas/v1"


def catalog():
    return load_source_profile_catalog()


def assert_private(response):
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["vary"] == "X-API-Key, X-HDL-Principal-Assertion"


def assert_unavailable(response):
    assert response.status_code == 503, response.text
    assert response.json() == {"error": {"code": "SOURCE_PROFILE_CATALOG_UNAVAILABLE", "message": "Source profile catalog unavailable", "details": {}}}
    assert_private(response)


def test_bundled_profile_schema_and_normalized_digest():
    value = catalog()
    wire = value.to_wire()
    jsonschema.validate(wire, json.loads((SCHEMAS / "source-profile-catalog.schema.json").read_text()))
    assert len(value.profiles) == 1
    profile = value.profiles[0]
    jsonschema.validate(profile.to_wire(), json.loads((SCHEMAS / "source-profile.schema.json").read_text()))
    assert profile.artifact_ref.digest == source_profile_content_digest(profile)
    # Defaults and explicit nulls normalize identically, without mutating input.
    raw = profile.to_wire(); raw["productVersion"] = None
    del raw["structures"][0]["fields"][0]["repeated"]
    before = deepcopy(raw)
    assert source_profile_content_digest(raw) == profile.artifact_ref.digest
    assert raw == before
    assert source_profile_index(value)[(profile.artifact_ref.id, profile.artifact_ref.version, profile.artifact_ref.digest)] == profile
    assert profile.structures[0].fields[0].data_type == "string"
    assert profile.semantic_bindings[0].concept_id == "openehr.composition.uid"
    assert profile.capability_maturity.value == "specified"
    assert profile.category_ids == ["healthcare.ehr", "healthcare.interoperability"]
    assert profile.mapping_routes[0].to_wire() == {"targetModel": "openehr", "mode": "direct", "readiness": "proposed", "strategyIds": ["openehr.rps_dual"]}
    assert "ibm" not in json.dumps(wire).lower()
    assert not any(name in json.dumps(wire).lower() for name in ("fhir", "dicom", "cdisc", "production_validated"))


@pytest.mark.parametrize("case", ["empty", "duplicate", "kind", "tamper", "malformed", "unknown-field"])
def test_catalog_integrity_failures(tmp_path, case):
    raw = catalog().to_wire()
    if case == "empty": raw["profiles"] = []
    elif case == "duplicate": raw["profiles"] *= 2
    elif case == "kind": raw["profiles"][0]["artifactRef"]["kind"] = "strategy"
    elif case == "tamper": raw["profiles"][0]["productName"] = "changed body"
    elif case == "unknown-field": raw["profiles"][0]["owner"] = OTHER
    path = tmp_path / "catalog.json"
    path.write_text("not JSON" if case == "malformed" else json.dumps(raw))
    with pytest.raises(SourceProfileCatalogError): load_source_profile_catalog(path)


def test_changed_profile_requires_new_full_pin(tmp_path):
    value = catalog(); original = value.profiles[0]
    raw = original.to_wire()
    raw["artifactRef"]["version"] = "0.2.0"
    raw["productName"] = "reviewed new descriptor"
    raw["artifactRef"]["digest"] = source_profile_content_digest(raw)
    assert raw["artifactRef"]["digest"] != original.artifact_ref.digest
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps({"contract": "source-profile-catalog/v1", "profiles": [raw]}))
    changed = load_source_profile_catalog(path)
    class AllAllowed:
        def allowed(self, *args): return True
    # A genuinely self-hashed unknown version is not the server-reviewed pin.
    with pytest.raises(KehrnelError) as error:
        design_catalogs.authorized_source_profile_catalog(changed, registry=None, policy=AllAllowed(), authority=Authority())
    assert error.value.code == "SOURCE_PROFILE_CATALOG_UNAVAILABLE"


@pytest.mark.asyncio
async def test_actual_http_wire_and_no_runtime_effects(tmp_path, monkeypatch):
    app, _, refs, _, _ = make_app(tmp_path, monkeypatch)
    def forbidden(*args, **kwargs): raise AssertionError("Catalog GET must not execute or prepare")
    for name in ("activate", "dispatch"):
        monkeypatch.setattr(app.state.strategy_runtime, name, forbidden)
    before = app.state.strategy_runtime.registry.path.read_bytes()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": KEY}) as client:
        response = await client.get(PATH)
        assert response.status_code == 200, response.text
        assert_private(response)
        assert response.json() == catalog().to_wire()
        profile = SourceProfile.model_validate(response.json()["profiles"][0])
        assert source_profile_content_digest(profile) == profile.artifact_ref.digest
        installed = (await client.get("/v1/strategies")).json()["strategies"]
        assert installed[0]["packageDigest"] == refs[0]["packageDigest"]
        unavailable = await client.get("/v1/architecture-resolution-inputs")
        assert unavailable.status_code == 503
        assert unavailable.json() == {"error": {"code": "ARCHITECTURE_INPUT_CATALOG_UNAVAILABLE", "message": "Architecture inputs unavailable", "details": {}}}
        assert_private(unavailable)
    assert not app.state.strategy_runtime.registry.activations
    assert app.state.strategy_runtime.registry.path.read_bytes() == before


@pytest.mark.asyncio
async def test_same_service_key_two_owners_and_policy_revocation(tmp_path, monkeypatch):
    app, _, refs, records, _ = make_app(tmp_path, monkeypatch)
    private = signer(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": DELEGATOR}) as client:
        for owner, ref, visible in [(OWNER, refs[0], True), (OTHER, refs[0], False), (OWNER, refs[0], True), (OTHER, refs[1], False)]:
            response = await client.get(PATH, headers={"x-hdl-principal-assertion": assertion(private, claims(ref, owner))})
            if visible: assert response.status_code == 200; assert_private(response)
            else: assert_unavailable(response)
        # Revoke owner eligibility while retaining admission/current identity.
        records[0]["owner"] = OTHER
        app.state.strategy_access_policy = StrategyAccessPolicy(records)
        assert_unavailable(await client.get(PATH, headers={"x-hdl-principal-assertion": assertion(private, claims(refs[0]))}))
    assert not hasattr(app.state, "strategy_authority")


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["cross-owner", "unadmitted", "registry-drift", "missing", "dependency", "no-read", "no-ref"])
async def test_catalog_prerequisite_absence_is_bounded(tmp_path, monkeypatch, case):
    app, _, refs, records, _ = make_app(tmp_path, monkeypatch)
    registry = app.state.strategy_runtime.registry
    key = KEY
    if case == "cross-owner": key = OTHER_KEY
    elif case == "unadmitted": app.state.strategy_access_policy = StrategyAccessPolicy()
    elif case == "registry-drift": registry.manifest_identities[refs[0]["id"]] = {**refs[0], "packageDigest": "sha256:" + "0" * 64}
    elif case == "missing": registry.manifests.pop(refs[0]["id"])
    elif case == "dependency":
        records[0]["dependencies"] = [refs[1]]
        app.state.strategy_access_policy = StrategyAccessPolicy(records)
    elif case == "no-read": app.state.api_key_authorities[fingerprint(KEY)]["actions"] = ["strategy:compile"]
    elif case == "no-ref": app.state.api_key_authorities[fingerprint(KEY)]["strategyRefs"] = []
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": key}) as client:
        assert_unavailable(await client.get(PATH))
        if case in ("unadmitted", "no-ref"):
            assert (await client.get("/v1/strategies")).json() == {"strategies": []}


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["version", "content"])
async def test_admitted_same_id_new_package_cannot_replace_required_pin(tmp_path, monkeypatch, change):
    app, manifests, refs, records, _ = make_app(tmp_path, monkeypatch)
    copied = tmp_path / "replacement"
    shutil.copytree(manifests[0].pack_root, copied)
    if change == "version":
        path = copied / "manifest.json"; raw = json.loads(path.read_text())
        raw["version"] = "0.2.1"; path.write_text(json.dumps(raw))
    else: (copied / "README.md").write_text("changed reviewed package bytes\n")
    # Simulate an already-installed current registry identity without importing code.
    changed = manifests[0].model_copy(update={"pack_root": str(copied), "version": "0.2.1" if change == "version" else manifests[0].version})
    new_ref = package_reference(changed)
    assert new_ref != refs[0]
    registry = app.state.strategy_runtime.registry
    registry.manifests[changed.id] = changed; registry.manifest_identities[changed.id] = new_ref
    records[0]["ref"] = new_ref
    app.state.strategy_access_policy = StrategyAccessPolicy(records)
    app.state.api_key_authorities[fingerprint(KEY)]["strategyRefs"] = [new_ref]
    authority = Authority(Owner.parse(OWNER), frozenset({PackageRef.parse(new_ref)}), frozenset({"strategy:read"}))
    assert app.state.strategy_access_policy.allowed(registry, PackageRef.parse(new_ref), authority)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": KEY}) as client:
        assert_unavailable(await client.get(PATH))


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["missing-key", "missing-assertion", "forged", "expired", "audience", "scope", "ceiling", "empty-read", "validate-only", "compile-only"])
async def test_delegated_catalog_read_action_separation(tmp_path, monkeypatch, case):
    app, _, refs, _, ceilings = make_app(tmp_path, monkeypatch)
    ceilings[fingerprint(DELEGATOR)]["actions"].append("blueprint:validate")
    monkeypatch.setenv("KEHRNEL_API_KEY_AUTHORITIES", json.dumps(ceilings))
    app.state.api_key_authorities = load_authority_configuration()
    private = signer(monkeypatch); value = claims(deepcopy(refs[0]))
    headers = {"x-api-key": DELEGATOR}
    status = 401
    if case == "missing-key": headers = {}
    elif case == "missing-assertion": status = 403
    elif case == "forged": private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    elif case == "expired":
        now = int(time.time()); value.update(iat=now-100, nbf=now-100, exp=now-40)
    elif case == "audience": value["aud"] = "other-instance"
    elif case == "scope": value["scope"] = value["workspace"] = {"kind": "team", "id": "unadmitted"}
    elif case == "ceiling": app.state.api_key_authorities[fingerprint(DELEGATOR)]["actions"] = ["strategy:compile"]; status = 403
    elif case == "empty-read": value["strategyRefs"] = []
    elif case == "validate-only": value.update(actions=["blueprint:validate"], strategyRefs=[]); status = 403
    elif case == "compile-only": value["actions"] = ["strategy:compile"]; status = 403
    if case not in ("missing-key", "missing-assertion"):
        headers["x-hdl-principal-assertion"] = assertion(private, value)
    def forbidden(*args, **kwargs): raise AssertionError("Denied requests must not load catalog metadata")
    monkeypatch.setattr(design_catalogs, "load_source_profile_catalog", forbidden)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.get(PATH, headers=headers)
        assert response.status_code == status, response.text
        assert_private(response)


@pytest.mark.asyncio
async def test_malformed_packaged_catalog_does_not_leak_paths(tmp_path, monkeypatch):
    app, _, _, _, _ = make_app(tmp_path, monkeypatch)
    def broken(): raise SourceProfileCatalogError("private filesystem path and hidden profile marker")
    monkeypatch.setattr(design_catalogs, "load_source_profile_catalog", broken)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": KEY}) as client:
        assert_unavailable(await client.get(PATH))


@pytest.mark.asyncio
async def test_catalog_absence_keeps_structural_save_and_authored_pin(tmp_path, monkeypatch):
    app, _, _, _, ceilings = make_app(tmp_path, monkeypatch)
    ceilings[fingerprint(DELEGATOR)]["actions"].append("blueprint:validate")
    monkeypatch.setenv("KEHRNEL_API_KEY_AUTHORITIES", json.dumps(ceilings))
    app.state.api_key_authorities = load_authority_configuration()
    app.state.strategy_access_policy = StrategyAccessPolicy()
    private = signer(monkeypatch)
    body = json.loads((SRC.parent / "tests/fixtures/architecture/v1/blueprint-hdl-golden.json").read_text())
    pin = catalog().profiles[0].artifact_ref.to_wire()
    body["participants"][0]["sourceProfileRef"] = pin
    value = claims(None, actions=["blueprint:validate"]); value["strategyRefs"] = []
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        assert_unavailable(await client.get(PATH, headers={"x-api-key": KEY}))
        response = await client.post("/v1/solution-blueprints/canonicalize", json=body, headers={"x-api-key": DELEGATOR, "x-hdl-principal-assertion": assertion(private, value)})
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["canonicalBlueprint"]["participants"][0]["sourceProfileRef"] == pin
        assert result["validationScope"] == "structural_only"
        assert all(result[k] == "not_evaluated" for k in ("catalogReadiness", "compilerReadiness", "activationReadiness"))
    assert not app.state.strategy_runtime.registry.activations


def test_pure_loading_and_boundary_import_without_recovered_graph(tmp_path):
    code = '''
import sys
from kehrnel.engine.core.source_profiles import load_source_profile_catalog
value = load_source_profile_catalog()
assert len(value.profiles) == 1
assert not any(name.startswith(('kehrnel.api', 'kehrnel.engine.strategies')) for name in sys.modules)
import kehrnel.api.core.design_catalogs
assert not any(name.startswith(('kehrnel.engine.strategies', 'kehrnel.engine.core.implementation_package')) for name in sys.modules)
assert 'kehrnel.engine.core.architecture_plan_compiler' not in sys.modules
assert 'kehrnel.engine.core.strategy_portfolio' not in sys.modules
assert 'kehrnel.contracts.schema_registry' not in sys.modules
'''
    env = {k: v for k, v in os.environ.items() if not k.startswith(('KEHRNEL_HDL', 'KEHRNEL_API_KEY'))}
    env.update(PYTHONPATH=str(SRC), PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run([sys.executable, "-c", code], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["invalid-utf8", "undigestible-text"])
async def test_malformed_text_has_bounded_catalog_error(tmp_path, monkeypatch, case):
    app, _, _, _, _ = make_app(tmp_path, monkeypatch)
    path = tmp_path / "malformed-catalog.json"
    if case == "invalid-utf8":
        path.write_bytes(b"\xff\xfe")
    else:
        raw = catalog().to_wire()
        raw["profiles"][0]["productName"] = "\ud800"
        # The contract permits this string, but normalized wire cannot encode it.
        SourceProfile.model_validate(raw["profiles"][0])
        path.write_text(json.dumps(raw))
    with pytest.raises(SourceProfileCatalogError):
        load_source_profile_catalog(path)
    monkeypatch.setattr(design_catalogs, "load_source_profile_catalog", lambda: load_source_profile_catalog(path))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test", headers={"x-api-key": KEY}) as client:
        assert_unavailable(await client.get(PATH))
