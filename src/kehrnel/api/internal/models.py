from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class RequestContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requestId: str
    environmentId: str
    principal: str | None = None
    dryRun: bool = False
    includeDebug: bool = False


class CompositionGetRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    context: RequestContext
    ehrId: str
    compositionUid: str
    version: int | None = Field(default=None, ge=1)
    view: Literal["metadata", "canonical"] = "canonical"


class CompositionGetResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ehrId: str
    compositionUid: str
    version: int
    templateId: str | None = None
    deleted: bool
    timeCommitted: datetime
    representation: dict[str, Any] | None = None


class CompositionCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    context: RequestContext
    ehrId: str
    templateId: str
    ifNoneExist: str | None = None
    idempotencyKey: str | None = None
    returnMode: Literal["metadata", "canonical"] = "metadata"
    composition: dict[str, Any]


class CompositionCreateResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ehrId: str
    compositionUid: str
    version: int
    contributionId: str
    timeCommitted: datetime
    representation: dict[str, Any] | None = None


class AqlExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    context: RequestContext
    aql: str
    ehrId: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    fetch: int | None = Field(default=None, ge=0)
    offset: int | None = Field(default=None, ge=0)
    timeoutMs: int | None = Field(default=None, ge=1)
    includeExecutedAql: bool = False
    includeExplain: bool = False
    featureMode: str = "parity"


class AqlExecuteResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    columns: dict[str, str | None]
    rows: list[dict[str, Any]]
    meta: dict[str, Any]
    debug: dict[str, Any] | None = None
