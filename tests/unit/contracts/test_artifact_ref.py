from pydantic import ValidationError
import pytest

from kehrnel.contracts import ArtifactRef


DIGEST = "sha256:" + "a" * 64


def test_artifact_ref_has_exact_camel_case_wire_shape_and_is_frozen():
    ref = ArtifactRef(kind="strategy", id="fhir.resource_store", version="0.13.1", digest=DIGEST)

    assert ref.to_wire() == {
        "kind": "strategy",
        "id": "fhir.resource_store",
        "version": "0.13.1",
        "digest": DIGEST,
    }
    with pytest.raises(ValidationError):
        ref.version = "changed"


@pytest.mark.parametrize(
    "digest",
    ["", "a" * 64, "sha256:ABC", "sha1:" + "a" * 40, "sha256:" + "a" * 63],
)
def test_artifact_ref_rejects_non_canonical_digests(digest):
    with pytest.raises(ValidationError):
        ArtifactRef(kind="strategy", id="x", version="1", digest=digest)


def test_artifact_ref_rejects_unknown_fields_and_surrounding_whitespace():
    with pytest.raises(ValidationError):
        ArtifactRef(kind="strategy", id=" x", version="1", digest=DIGEST)
    with pytest.raises(ValidationError):
        ArtifactRef(kind="strategy", id="x", version="1", digest=DIGEST, uri="hidden")
