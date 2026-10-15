"""Request models for SNOMED CT domain APIs."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SnomedReleaseSourceRequest(BaseModel):
    path: str | None = Field(default=None, description="File inside the configured source.local_dir staging directory.")
    file_name: str | None = Field(default=None, description="File name inside the configured staging directory.")
    limit: int | None = Field(default=None, ge=1, le=10_000_000)


class SnomedReleaseDiffRequest(SnomedReleaseSourceRequest):
    release_id: str | None = None
    release_label: str | None = None
    sample_limit: int = Field(default=20, ge=1, le=100)
    include_descendants: bool | None = None


class SnomedReleaseIngestRequest(SnomedReleaseDiffRequest):
    batch_size: int | None = Field(default=None, ge=1, le=100_000)
    dry_run: bool = False
    license_acknowledged: bool = Field(
        default=False,
        description=(
            "Required for a non-dry-run ingest. Confirms that the tenant is "
            "authorized to use the staged SNOMED CT content."
        ),
    )
    drop_before_ingest: bool | None = None
    rebuild_sidecar: bool | None = None


class SnomedReleaseUploadRequest(BaseModel):
    file_name: str = Field(description="Plain .json file name staged inside the configured release directory.")
    content_base64: str = Field(description="Base64-encoded licensed release JSON (up to 5 MB).")
    sha256: str | None = Field(default=None, description="Optional checksum of the decoded content; verified when present.")
    max_bytes: int | None = Field(default=None, ge=1)


class SnomedReleaseUploadInitiateRequest(BaseModel):
    file_name: str = Field(description="Plain .json file name the upload will stage as.")
    sha256: str = Field(description="Checksum of the file that will be uploaded.")
    size: int = Field(ge=1)
    media_type: str | None = None
    expires_in: int | None = Field(default=900, ge=60, le=3600)


class SnomedReleaseUploadFinalizeRequest(BaseModel):
    object_key: str = Field(description="Object key returned by the initiate step.")
    file_name: str | None = None
    sha256: str | None = None
    size: int | None = Field(default=None, ge=1)


class SnomedSidecarRebuildRequest(BaseModel):
    release_id: str | None = None
    languages: list[str] | None = None
    limit: int | None = Field(default=None, ge=1, le=10_000_000)
    batch_size: int | None = Field(default=None, ge=1, le=100_000)
    drop_before_rebuild: bool = False
    dry_run: bool = False


class SnomedEnsureIndexesRequest(BaseModel):
    dry_run: bool = False


class SnomedSearchRequest(BaseModel):
    q: str = Field(..., description="Search text.")
    language: str = Field(default="es", description="Description language code.")
    release_id: str | None = Field(default=None, description="SNOMED CT release id.")
    ecl: str | None = Field(default=None, description="Optional ECL scope for the search.")
    area_concept_id: str | None = Field(default=None, description="Optional area-concept scope for the search.")
    force_scope_refresh: bool = Field(default=False, description="Bypass the scope cache for this request.")
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=100)


class SnomedValidateCodeRequest(BaseModel):
    code: str = Field(..., min_length=1, description="SNOMED CT concept id.")
    system: str = Field(default="http://snomed.info/sct", description="Canonical SNOMED CT code-system URI.")
    version: str | None = Field(default=None, description="Optional SNOMED CT edition/version URI or release id.")
    release_id: str | None = Field(default=None, description="Native release id override.")
    display: str | None = Field(default=None, description="Optional display to validate against active descriptions.")
    language: str | None = Field(default=None, description="Optional language restriction for display validation.")


class SnomedSubsumesRequest(BaseModel):
    code_a: str = Field(..., min_length=1, description="Candidate ancestor concept id.")
    code_b: str = Field(..., min_length=1, description="Candidate descendant concept id.")
    system: str = Field(default="http://snomed.info/sct", description="Canonical SNOMED CT code-system URI.")
    version: str | None = Field(default=None, description="Optional SNOMED CT edition/version URI or release id.")
    release_id: str | None = Field(default=None, description="Native release id override.")


class SnomedHybridSearchRequest(BaseModel):
    q: str | None = Field(default=None, description="Optional lexical search text.")
    mode: str = Field(default="lexical", description="Retrieval mode: lexical, vector, or hybrid.")
    language: str = Field(default="es", description="Description language code.")
    release_id: str | None = Field(default=None, description="SNOMED CT release id.")
    ancestor_id: str | None = Field(default=None, description="Restrict matches to concepts under this ancestor.")
    area_tag: str | None = Field(default=None, description="Restrict matches to a high-level semantic area tag.")
    semantic_tag: str | None = Field(default=None, description="Restrict matches by semantic tag label.")
    semantic_tag_key: str | None = Field(default=None, description="Restrict matches by normalized semantic tag key.")
    ecl: str | None = Field(default=None, description="Optional ECL scope.")
    area_concept_id: str | None = Field(default=None, description="Optional area-concept scope.")
    force_scope_refresh: bool = Field(default=False, description="Bypass the scope cache for this request.")
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=100)


class SnomedConceptExpansionRequest(BaseModel):
    concept_id: str | None = Field(default=None, description="Focus concept id.")
    release_id: str | None = Field(default=None, description="SNOMED CT release id.")
    include_self: bool = Field(default=False, description="Include the focus concept in the returned set.")
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=500)


class SnomedEclRequest(BaseModel):
    expression: str = Field(..., description="Basic ECL expression.")
    release_id: str | None = None
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=500)


class SnomedValueSetExpandRequest(BaseModel):
    url: str | None = Field(default=None, description="Registered ValueSet canonical URL.")
    value_set_version: str | None = Field(default=None, description="Registered ValueSet version.")
    expression: str | None = Field(default=None, description="ECL expression to expand.")
    concept_id: str | None = Field(default=None, description="Optional focus concept id used as << concept_id when expression is omitted.")
    release_id: str | None = None
    include_self: bool = Field(default=True)
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=500)


class SnomedValueSetRegistrationRequest(BaseModel):
    url: str = Field(..., min_length=1, description="Stable ValueSet canonical URL.")
    version: str = Field(..., min_length=1, description="Immutable ValueSet definition version.")
    name: str | None = None
    status: str = Field(default="active")
    release_id: str | None = None
    expression: str | None = Field(default=None, description="ECL-backed definition.")
    concepts: list[str] | None = Field(default=None, description="Explicit concept-id definition.")


class SnomedValueSetValidateRequest(SnomedValidateCodeRequest):
    url: str = Field(..., min_length=1, description="Registered ValueSet canonical URL.")
    value_set_version: str | None = None


class SnomedRelationshipSearchRequest(BaseModel):
    type_id: str | None = Field(default=None, description="Relationship type concept id.")
    destination_id: str | None = Field(default=None, description="Relationship destination concept id.")
    release_id: str | None = None
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=500)


class SnomedSemanticFacetsRequest(BaseModel):
    q: str | None = Field(default=None, description="Optional lexical term filter.")
    language: str = Field(default="es", description="Description language code.")
    release_id: str | None = None
    ancestor_id: str | None = Field(default=None, description="Optional hierarchy scope.")


class SnomedGroundRequest(BaseModel):
    mentions: list[str] | None = Field(default=None, description="Extracted mention texts.")
    text: str | None = Field(default=None, description="Optional simple text input.")
    language: str = Field(default="es")
    release_id: str | None = None
    limit_per_mention: int = Field(default=5, ge=1, le=25)


class SnomedGroundingReviewRequest(BaseModel):
    source_ref: str = Field(..., min_length=1, description="Opaque caller-owned source reference.")
    review_id: str | None = Field(default=None, description="Optional idempotency key.")
    text: str | None = Field(default=None, description="Source text; hashed and discarded unless activation policy allows storage.")
    language: str = Field(default="es")
    release_id: str | None = None
    reviewer: str | None = None
    codings: list[dict[str, Any]] = Field(..., min_length=1, max_length=250)


class SnomedGroundedCorpusRequest(BaseModel):
    concept_id: str = Field(..., min_length=1)
    include_non_present: bool = False
    include_family: bool = False
    include_text: bool = False
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=100)


class SnomedRetrievalBenchmarkRequest(BaseModel):
    cases: list[dict[str, Any]] = Field(default_factory=list, max_length=100)
    gold_note_ids: list[str] | None = Field(
        default=None,
        description="Grade against stored gold notes instead of (or as well as) inline cases.",
    )
    language: str = Field(default="es")
    release_id: str | None = None
    top_k: int = Field(default=10, ge=1, le=100)


class SnomedScopeRequest(BaseModel):
    ecl: str | None = Field(default=None, description="ECL expression narrowing the scope.")
    area_concept_id: str | None = Field(default=None, description="Focus concept whose descendants bound the scope.")
    release_id: str | None = None
    force_scope_refresh: bool = False


class SnomedNlpMapRequest(BaseModel):
    text: str = Field(..., min_length=1, description="Clinical note text.")
    language: str = Field(default="es")
    release_id: str | None = None
    ecl: str | None = Field(default=None, description="Optional ECL scope for candidate retrieval.")
    area_concept_id: str | None = Field(default=None, description="Optional area-concept scope.")
    force_scope_refresh: bool = False
    target_context: str | None = Field(default=None, description="For example Condition.code or Procedure.code.")
    extractor: str | None = Field(default=None, description="deterministic to skip LLM extraction.")
    max_phrases: int = Field(default=20, ge=1, le=40)
    per_phrase_limit: int = Field(default=5, ge=1, le=25)
    extract_only: bool = Field(default=False, description="Return the evidence graph without retrieving candidates.")
    ground_mentions: list[dict[str, Any]] | None = Field(
        default=None,
        description="Retrieval-only mode: ground mentions the caller already extracted.",
    )


class SnomedLlmGroundRequest(BaseModel):
    mentions: list[dict[str, Any]] = Field(..., min_length=1, max_length=25)
    language: str = Field(default="en")


class SnomedGoldNoteFact(BaseModel):
    phrase: str = Field(..., min_length=1)
    assertion: str = Field(default="present")
    subject: str = Field(default="patient")
    section: str = Field(default="other")
    conceptId: str | None = None
    term: str | None = None


class SnomedGoldNoteRequest(BaseModel):
    id: str = Field(..., min_length=1)
    title: str | None = None
    languageCode: str = Field(default="en")
    text: str = Field(..., min_length=1)
    facts: list[SnomedGoldNoteFact] = Field(..., min_length=1)


class SnomedUsageEventRequest(BaseModel):
    event_type: str = Field(..., min_length=1)
    tenant_id: str | None = None
    actor_id: str | None = None
    release_id: str | None = None
    language: str | None = None
    concept_id: str | None = None
    concept_ids: list[str] | None = None
    query: str | None = None
    result_count: int | None = None
    duration_ms: float | None = None
    metadata: dict[str, Any] | None = None
