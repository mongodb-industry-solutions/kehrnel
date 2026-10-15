from pydantic import ValidationError
import pytest

from kehrnel.contracts import (
    ActionAvailability,
    ActionAvailabilityState,
    ArtifactRef,
    CapabilityClaim,
    CapabilityMaturity,
    ExecutionState,
    ObservationKind,
    PackageLifecycle,
    ProblemDetails,
    PublicationStatus,
    RuntimeAvailability,
    SupportStatus,
    Visibility,
)


def _values(enum_type):
    return {item.value for item in enum_type}


def test_evidence_dimensions_are_independent_and_exact():
    assert _values(PackageLifecycle) == {
        "draft", "experimental", "preview", "generally_available", "deprecated", "retired"
    }
    assert _values(CapabilityMaturity) == {
        "not_assessed", "specified", "simulated", "compilable",
        "pilot_validated", "production_validated",
    }
    assert _values(SupportStatus) == {
        "unsupported", "evaluation_only", "supported", "deprecated"
    }
    assert _values(RuntimeAvailability) == {
        "available", "degraded", "unavailable", "unknown"
    }
    assert _values(ExecutionState) == {
        "planned", "running", "executed", "failed", "cancelled"
    }
    assert _values(ObservationKind) == {
        "measured", "simulated", "estimated", "illustrative"
    }
    assert _values(Visibility) == {"private", "tenant", "organization", "public"}
    assert _values(PublicationStatus) == {"draft", "in_review", "published", "withdrawn"}


def test_capability_claim_and_action_use_camel_case_wire_contract():
    ref = ArtifactRef(kind="evidence", id="proof", version="1", digest="sha256:" + "1" * 64)
    claim = CapabilityClaim(
        capability_id="fhir.search",
        capability_maturity="pilot_validated",
        support_status="supported",
        runtime_availability="available",
        evidence_refs=[ref],
    )
    action = ActionAvailability(
        action_id="ask-clinical-question",
        state=ActionAvailabilityState.ENABLED,
        required_capability_ids=["fhir.search"],
        contributing_capability_ids=["fhir.search"],
        target_ref=ref,
    )

    assert claim.to_wire()["capabilityId"] == "fhir.search"
    assert claim.to_wire()["evidenceRefs"][0]["digest"] == ref.digest
    assert action.to_wire()["requiredCapabilityIds"] == ["fhir.search"]
    assert action.to_wire()["state"] == "enabled"


def test_problem_details_requires_correlation_and_json_pointer_paths():
    problem = ProblemDetails(
        title="Invalid artifact reference",
        status=422,
        code="ARTIFACT_REF_INVALID",
        correlation_id="corr-123",
        field_errors=[{"path": "/artifactRef/digest", "code": "pattern", "message": "invalid"}],
    )
    wire = problem.to_wire()
    assert wire["correlationId"] == "corr-123"
    assert wire["fieldErrors"][0]["path"] == "/artifactRef/digest"

    with pytest.raises(ValidationError):
        ProblemDetails(title="Bad", status=422, code="BAD", correlation_id="")
    with pytest.raises(ValidationError):
        ProblemDetails(
            title="Bad",
            status=422,
            code="BAD",
            correlation_id="corr",
            field_errors=[{"path": "not-a-pointer", "code": "bad", "message": "bad"}],
        )
