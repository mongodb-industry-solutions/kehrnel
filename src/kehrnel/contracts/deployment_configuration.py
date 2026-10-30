"""Inputs for activation-free, portable strategy configuration compilation."""
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DeploymentTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    templateId: str = Field(min_length=1, max_length=256)
    fields: list[dict[str, Any]] = Field(default_factory=list, max_length=10000)
    provenance: dict[str, str] = Field(default_factory=dict)

    @field_validator("templateId")
    @classmethod
    def nonempty_id(cls, value):
        if not value.strip():
            raise ValueError("templateId cannot be blank")
        return value.strip()

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, fields):
        for field in fields:
            if not isinstance(field.get("path"), str) or not field["path"].startswith("/"):
                raise ValueError("Every analytics field requires an absolute openEHR path")
            for key in ("extract", "valuePath", "rmType"):
                if key in field and field[key] is not None and not isinstance(field[key], str):
                    raise ValueError(f"{key} must be a string")
        return fields


class DeploymentCompilationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: str | None = Field(default=None, min_length=1, max_length=128)
    packageDigest: str | None = Field(default=None, pattern=r"^sha256:[a-f0-9]{64}$")
    config: dict[str, Any] = Field(default_factory=dict)
    templates: list[DeploymentTemplate] = Field(default_factory=list, max_length=2000)
    dictionaries: dict[str, Any] = Field(default_factory=dict)


    @field_validator("version")
    @classmethod
    def nonblank_version(cls, value):
        if value is not None and not value.strip():
            raise ValueError("version must be nonblank when supplied")
        return value
