from pathlib import Path

import pytest

from kehrnel.engine.core.integrations.hdl import bindings_resolver
from kehrnel.engine.core.integrations.hdl.bindings_resolver import _artifact_binding, _resolve_database_name


def test_hdl_artifact_binding_is_optional(monkeypatch):
    monkeypatch.delenv("KEHRNEL_HDL_ARTIFACT_PROVIDER", raising=False)

    assert _artifact_binding("env-1") is None


def test_hdl_filesystem_artifacts_are_scoped_by_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("KEHRNEL_HDL_ARTIFACT_PROVIDER", "filesystem")
    monkeypatch.setenv("KEHRNEL_HDL_ARTIFACT_ROOT", str(tmp_path))

    binding = _artifact_binding("env/tenant 1")

    assert binding == {
        "provider": "filesystem",
        "root": str(Path(tmp_path) / "env-tenant-1"),
    }


def test_hdl_s3_artifacts_are_scoped_and_support_service_credentials(monkeypatch):
    monkeypatch.setenv("KEHRNEL_HDL_ARTIFACT_PROVIDER", "s3")
    monkeypatch.setenv("KEHRNEL_HDL_ARTIFACT_S3_BUCKET", "clinical-artifacts")
    monkeypatch.setenv("KEHRNEL_HDL_ARTIFACT_S3_PREFIX", "kehrnel/production")
    monkeypatch.setenv("KEHRNEL_HDL_ARTIFACT_S3_REGION", "eu-west-1")
    monkeypatch.setenv("KEHRNEL_HDL_ARTIFACT_S3_ACCESS_KEY_ID", "access")
    monkeypatch.setenv("KEHRNEL_HDL_ARTIFACT_S3_SECRET_ACCESS_KEY", "secret")

    binding = _artifact_binding("env-1")

    assert binding == {
        "provider": "s3",
        "bucket": "clinical-artifacts",
        "prefix": "kehrnel/production/env-1",
        "region": "eu-west-1",
        "access_key_id": "access",
        "secret_access_key": "secret",
    }


@pytest.mark.parametrize("provider", ["filesystem", "s3", "azure"])
def test_hdl_artifact_binding_rejects_incomplete_or_unknown_configuration(monkeypatch, provider):
    monkeypatch.setenv("KEHRNEL_HDL_ARTIFACT_PROVIDER", provider)
    monkeypatch.delenv("KEHRNEL_HDL_ARTIFACT_ROOT", raising=False)
    monkeypatch.delenv("KEHRNEL_HDL_ARTIFACT_S3_BUCKET", raising=False)

    with pytest.raises(ValueError):
        _artifact_binding("env-1")


def test_strategy_activation_database_wins_over_uri_and_environment_database():
    assert _resolve_database_name(
        explicit_db=None,
        uri="mongodb://localhost:27017/tenant_core",
        context={"activation_config": {"database": "tenant_fhir"}},
        env_id="env-1",
    ) == "tenant_fhir"


def test_strategy_activation_database_cannot_reuse_environment_core_database():
    with pytest.raises(ValueError, match="must be different from the environment core database"):
        _resolve_database_name(
            explicit_db=None,
            uri="mongodb+srv://user:secret@example.mongodb.net/hdl-team?retryWrites=true",
            context={"activation_config": {"database": "hdl-team"}},
            env_id="env-1",
        )


def test_package_only_fhir_catalog_can_render_before_database_reactivation():
    assert _resolve_database_name(
        explicit_db=None,
        uri="mongodb://localhost:27017/hdl-team",
        context={
            "activation_config": {"database": "hdl-team"},
            "payload": {"op": "fhir_resource_catalog"},
        },
        env_id="env-1",
    ) == "hdl-team"


def test_fhir_binding_rejects_database_from_hdl_environment_record(monkeypatch):
    class _Secrets:
        @staticmethod
        def find_one(query):
            return {"sealedUri": {"opaque": True}}

    class _Db:
        def __getitem__(self, name):
            assert name == "environment_secrets"
            return _Secrets()

    class _Store:
        db = _Db()

    monkeypatch.setattr(bindings_resolver, "_core_store", lambda: _Store())
    monkeypatch.setattr(bindings_resolver, "_decrypt_sealed_uri", lambda value: "mongodb://localhost:27017")
    monkeypatch.setattr(bindings_resolver, "_environment_database_names", lambda store, env_id: {"hdl-team"})

    with pytest.raises(ValueError, match="must be different from the HDL environment database"):
        bindings_resolver.resolve_hdl_bindings(
            bindings_ref="hdl:env:env-1",
            env_id="env-1",
            domain="fhir",
            strategy_id="fhir.clinical_cdr",
            op="op",
            context={
                "activation_config": {"database": "hdl-team"},
                "payload": {"op": "fhir_search"},
            },
        )


def test_binding_reference_database_cannot_override_reviewed_strategy_database():
    with pytest.raises(ValueError, match="does not match reviewed strategy database"):
        _resolve_database_name(
            explicit_db="tenant_core",
            uri="mongodb://localhost:27017/tenant_core",
            context={"activation_config": {"database": "tenant_openehr"}},
            env_id="env-1",
        )


def test_strategy_database_is_required_by_hdl_resolver():
    assert _resolve_database_name(
        explicit_db=None,
        uri="mongodb://localhost:27017/tenant_core",
        context={"activation_config": {}},
        env_id="env-1",
    ) is None
