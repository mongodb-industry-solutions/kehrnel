from __future__ import annotations

import json
import tarfile
from pathlib import Path

import pytest

from kehrnel.engine.domains.fhir.implementation_guides import (
    ImplementationGuideError,
    compile_configured_implementation_guides,
    inspect_configured_implementation_guides,
    inspect_implementation_guide,
    resolve_active_profiles,
    stage_implementation_guide,
)


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _package(tmp_path: Path) -> Path:
    root = tmp_path / "customer-ig" / "package"
    _write_json(
        root / "package.json",
        {
            "name": "example.fhir.ig",
            "version": "1.0.0",
            "canonical": "https://example.test/fhir",
            "fhirVersions": ["5.0.0"],
            "dependencies": {"hl7.fhir.r5.core": "5.0.0"},
        },
    )
    _write_json(
        root / "StructureDefinition-example-patient.json",
        {
            "resourceType": "StructureDefinition",
            "id": "example-patient",
            "url": "https://example.test/fhir/StructureDefinition/example-patient",
            "type": "Patient",
            "kind": "resource",
            "derivation": "constraint",
            "baseDefinition": "http://hl7.org/fhir/StructureDefinition/Patient",
            "differential": {"element": []},
        },
    )
    _write_json(
        root / "SearchParameter-patient-city.json",
        {
            "resourceType": "SearchParameter",
            "id": "patient-city",
            "url": "https://example.test/fhir/SearchParameter/patient-city",
            "code": "city",
            "type": "string",
            "base": ["Patient"],
            "expression": "Patient.address.city",
        },
    )
    _write_json(
        root / "SearchParameter-patient-complex.json",
        {
            "resourceType": "SearchParameter",
            "id": "patient-complex",
            "url": "https://example.test/fhir/SearchParameter/patient-complex",
            "code": "complex",
            "type": "string",
            "base": ["Patient"],
            "expression": "Patient.name.where(use='official').family",
        },
    )
    _write_json(
        root / "Patient-example.json",
        {"resourceType": "Patient", "id": "example"},
    )
    return root.parent


def test_no_ig_is_valid_fhir_core_mode() -> None:
    assert inspect_configured_implementation_guides({}) == []
    assert (
        inspect_configured_implementation_guides(
            {"implementation_guides": {"packages": []}}
        )
        == []
    )


def test_compiler_inventories_profiles_examples_and_review_boundaries(
    tmp_path: Path,
) -> None:
    source = _package(tmp_path)

    result = inspect_implementation_guide(source)

    assert result["package"]["name"] == "example.fhir.ig"
    assert result["package"]["fhir_versions"] == ["5.0.0"]
    assert "source" not in result["package"]
    assert result["inventory"]["resource_count"] == 4
    assert len(result["inventory"]["profiles"]) == 1
    assert result["inventory"]["examples"] == [
        {"file": "Patient-example.json", "resource_type": "Patient", "id": "example"}
    ]
    statuses = {
        item["code"]: item["compilation_status"]
        for item in result["inventory"]["search_parameters"]
    }
    assert statuses == {
        "city": "candidate",
        "complex": "reviewed-override-required",
    }
    assert result["evidence"]["profile_validation_enabled"] is False
    assert result["evidence"]["manual_override_required"] == [
        "https://example.test/fhir/SearchParameter/patient-complex"
    ]


def test_compiler_verifies_checksum_and_writes_immutable_artifacts(
    tmp_path: Path,
) -> None:
    source = _package(tmp_path)
    inspected = inspect_implementation_guide(source)
    compiled_root = tmp_path / "compiled"
    config = {
        "implementation_guides": {
            "compiled_root": str(compiled_root),
            "packages": [
                {"source": str(source), "sha256": inspected["package"]["sha256"]}
            ],
        }
    }

    first = compile_configured_implementation_guides(config)
    second = compile_configured_implementation_guides(config)

    assert first == second
    destination = Path(first[0]["output"]).parent
    assert {path.name for path in destination.iterdir()} == {
        "catalog.json",
        "compiled-package.json",
        "package.lock.json",
        "search-plan.json",
    }

    with pytest.raises(ImplementationGuideError, match="checksum mismatch"):
        inspect_implementation_guide(source, expected_sha256="0" * 64)


def test_profile_selection_is_optional_and_resolved_from_enabled_packages(
    tmp_path: Path,
) -> None:
    source = _package(tmp_path)
    profile_url = "https://example.test/fhir/StructureDefinition/example-patient"
    config = {
        "implementation_guides": {
            "compiled_root": str(tmp_path / "compiled"),
            "packages": [{"source": str(source)}],
            "active_profiles": [profile_url],
        }
    }

    selected = resolve_active_profiles(config)
    assert [item["url"] for item in selected] == [profile_url]
    assert selected[0]["type"] == "Patient"

    config["implementation_guides"]["active_profiles"] = []
    assert resolve_active_profiles(config) == []

    config["implementation_guides"]["active_profiles"] = [
        "https://missing.test/Profile"
    ]
    with pytest.raises(ImplementationGuideError, match="not present"):
        resolve_active_profiles(config)


def test_disabled_packages_are_ignored_without_compiled_root(tmp_path: Path) -> None:
    source = _package(tmp_path)
    config = {
        "implementation_guides": {
            "packages": [{"source": str(source), "enabled": False}],
            "active_profiles": [],
        }
    }
    assert inspect_configured_implementation_guides(config) == []


def test_profile_selection_rejects_ambiguous_canonical_across_packages(
    tmp_path: Path,
) -> None:
    first = _package(tmp_path / "one")
    second = _package(tmp_path / "two")
    profile_url = "https://example.test/fhir/StructureDefinition/example-patient"
    config = {
        "schema_version": "R5",
        "implementation_guides": {
            "compiled_root": str(tmp_path / "compiled"),
            "packages": [{"source": str(first)}, {"source": str(second)}],
            "active_profiles": [profile_url],
        },
    }

    with pytest.raises(ImplementationGuideError, match="same profile canonical URL"):
        resolve_active_profiles(config)


def test_uploaded_package_is_staged_but_not_activated(tmp_path: Path) -> None:
    source = _package(tmp_path / "source")
    archive = tmp_path / "example.fhir.ig.tgz"
    with tarfile.open(archive, "w:gz") as handle:
        handle.add(source / "package", arcname="package")
    data = archive.read_bytes()

    staged = stage_implementation_guide(
        data,
        filename=archive.name,
        environment_id="customer/dev",
        staging_root=tmp_path / "staged",
        expected_release="R5",
        max_upload_bytes=len(data) + 1,
        max_environment_bytes=len(data) + 1,
    )

    assert staged["package"]["name"] == "example.fhir.ig"
    assert staged["inventory"]["profile_count"] == 1
    assert Path(staged["source"]).is_file()
    assert Path(staged["source"]).parent.name == "customer-dev"
    assert staged["activation_entry"] == {
        "enabled": True,
        "source": staged["source"],
        "sha256": staged["sha256"],
    }
    assert staged["activated"] is False
    assert staged["profiles_selected"] is False
    assert staged["profile_validation_enabled"] is False

    with pytest.raises(ImplementationGuideError, match="does not declare compatibility"):
        stage_implementation_guide(
            data,
            filename=archive.name,
            environment_id="customer/dev",
            staging_root=tmp_path / "other-staged",
            expected_release="R4",
            max_upload_bytes=len(data) + 1,
            max_environment_bytes=len(data) + 1,
        )
