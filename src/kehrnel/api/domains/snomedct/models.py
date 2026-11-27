"""Request models for SNOMED CT domain APIs."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SnomedSearchRequest(BaseModel):
    q: str = Field(..., description="Search text.")
    language: str = Field(default="es", description="Description language code.")
    release_id: str | None = Field(default=None, description="SNOMED CT release id.")
    limit: int = Field(default=20, ge=1, le=100)


class SnomedHybridSearchRequest(BaseModel):
    q: str | None = Field(default=None, description="Optional lexical search text.")
    language: str = Field(default="es", description="Description language code.")
    release_id: str | None = Field(default=None, description="SNOMED CT release id.")
    ancestor_id: str | None = Field(default=None, description="Restrict matches to concepts under this ancestor.")
    area_tag: str | None = Field(default=None, description="Restrict matches to a high-level semantic area tag.")
    semantic_tag: str | None = Field(default=None, description="Restrict matches by semantic tag label.")
    semantic_tag_key: str | None = Field(default=None, description="Restrict matches by normalized semantic tag key.")
    limit: int = Field(default=20, ge=1, le=100)


class SnomedConceptExpansionRequest(BaseModel):
    concept_id: str | None = Field(default=None, description="Focus concept id.")
    release_id: str | None = Field(default=None, description="SNOMED CT release id.")
    include_self: bool = Field(default=False, description="Include the focus concept in the returned set.")
    limit: int = Field(default=50, ge=1, le=500)


class SnomedEclRequest(BaseModel):
    expression: str = Field(..., description="Basic ECL expression.")
    release_id: str | None = None
    limit: int = Field(default=50, ge=1, le=500)


class SnomedValueSetExpandRequest(BaseModel):
    expression: str | None = Field(default=None, description="ECL expression to expand.")
    concept_id: str | None = Field(default=None, description="Optional focus concept id used as << concept_id when expression is omitted.")
    release_id: str | None = None
    include_self: bool = Field(default=True)
    limit: int = Field(default=50, ge=1, le=500)


class SnomedRelationshipSearchRequest(BaseModel):
    type_id: str | None = Field(default=None, description="Relationship type concept id.")
    destination_id: str | None = Field(default=None, description="Relationship destination concept id.")
    release_id: str | None = None
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
