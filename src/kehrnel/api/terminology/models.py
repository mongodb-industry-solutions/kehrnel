"""Normalized terminology request models."""

from __future__ import annotations

from pydantic import BaseModel, Field


class TerminologyRequest(BaseModel):
    provider_id: str | None = Field(default=None, description="Optional explicit configured provider id.")
    system: str = Field(..., min_length=1, description="Canonical code-system URI.")
    version: str | None = None


class TerminologyLookupRequest(TerminologyRequest):
    code: str = Field(..., min_length=1)
    language: str | None = None


class TerminologyValidateCodeRequest(TerminologyLookupRequest):
    display: str | None = None
    url: str | None = Field(default=None, description="Optional ValueSet canonical URL for membership validation.")
    value_set_version: str | None = Field(default=None, description="Optional version of the named ValueSet definition.")


class TerminologyCoding(BaseModel):
    provider_id: str | None = Field(default=None, description="Optional explicit configured provider id.")
    system: str = Field(..., min_length=1, description="Canonical code-system URI.")
    version: str | None = None
    code: str = Field(..., min_length=1)
    display: str | None = None
    url: str | None = Field(default=None, description="Optional ValueSet canonical URL for membership validation.")
    value_set_version: str | None = Field(default=None, description="Optional version of the named ValueSet definition.")
    path: str | None = Field(default=None, description="Caller-owned location retained in the result.")


class TerminologyBatchValidateRequest(BaseModel):
    codings: list[TerminologyCoding] = Field(..., min_length=1, max_length=100)


class TerminologySubsumesRequest(TerminologyRequest):
    code_a: str = Field(..., min_length=1)
    code_b: str = Field(..., min_length=1)


class TerminologyExpandRequest(BaseModel):
    provider_id: str | None = None
    system: str | None = Field(default=None, description="Code system used for deterministic provider routing.")
    version: str | None = None
    url: str | None = Field(default=None, description="ValueSet canonical URL.")
    value_set_version: str | None = Field(default=None, description="ValueSet definition version.")
    expression: str | None = Field(default=None, description="Native ECL expression when supported.")
    concept_id: str | None = None
    filter: str | None = None
    offset: int | None = Field(default=None, ge=0)
    count: int | None = Field(default=None, ge=1, le=1000)
    include_designations: bool | None = None
    active_only: bool | None = None


class TerminologyTranslateRequest(BaseModel):
    provider_id: str | None = Field(default=None, description="Optional explicit configured provider id.")
    url: str | None = Field(default=None, description="ConceptMap canonical URL.")
    concept_map_version: str | None = None
    system: str | None = Field(default=None, description="Source code-system URI.")
    version: str | None = None
    code: str = Field(..., min_length=1)
    target: str | None = Field(default=None, description="Target ValueSet canonical URL.")
    target_system: str | None = Field(default=None, description="Target code-system URI.")
    reverse: bool | None = None


class TerminologySearchRequest(TerminologyRequest):
    q: str = Field(..., min_length=1)
    language: str | None = None
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=100)
