"""Tests for HL7 search_package loader."""

from fhir_search_to_mql.schema.search_package_loader import (
    package_dir_for,
    search_parameters_for_resource,
)


def test_r5_package_exists():
    assert package_dir_for("R5").is_dir()


def test_condition_has_resource_specific_params():
    rows = search_parameters_for_resource("Condition", "R5", include_resource_common=False)
    codes = {r.code for r in rows}
    assert "code" in codes
    assert "patient" in codes
    assert "clinical-status" in codes
    assert len(rows) >= 20


def test_condition_includes_common_id_lastupdated():
    rows = search_parameters_for_resource("Condition", "R5")
    codes = {r.code for r in rows}
    assert "_id" in codes
    assert "_lastUpdated" in codes


def test_condition_patient_has_target():
    rows = search_parameters_for_resource("Condition", "R5")
    patient = next(r for r in rows if r.code == "patient")
    assert patient.type == "reference"
    assert "Patient" in patient.target
    assert "Patient" in patient.expression or "subject" in patient.expression
