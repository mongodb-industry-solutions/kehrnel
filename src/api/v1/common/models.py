from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Literal, Any, Dict

from src.app.core.models import AuditDetails

class HierObjectID(BaseModel):
    value: str

class EhrIdModel(HierObjectID):
    pass

class SystemIdModel(HierObjectID):
    pass

class ObjectVersionID(BaseModel):
    value: str
    type: str = Field(alias="_type", default="OBJECT_VERSION_ID")

    class Config:
        populate_by_name = True

class PartySelf(BaseModel):
    type: Literal["PARTY_SELF"] = Field("PARTY_SELF", alias="_type")
    external_ref: Optional[Dict[str, Any]] = None

    class Config:
        populate_by_name = True

class SubjectModel(BaseModel):
    id: HierObjectID
    namespace: str

class DvDateTime(BaseModel):
    value: datetime

class ObjectRef(BaseModel):
    id: HierObjectID
    namespace: str = "local"
    type: str

class ErrorResponse(BaseModel):
    detail: str

class RevisionHistoryItem(BaseModel):
    version_id: ObjectVersionID = Field(..., alias="versionId")
    audit: AuditDetails

    class Config:
        populate_by_name = True

class RevisionHistory(BaseModel):
    items: List[RevisionHistoryItem]

class OriginalVersionResponse(BaseModel):
    uid: ObjectVersionID
    preceding_version_uid: Optional[ObjectVersionID] = Field(None, alias="precedingVersionUid")
    data: Dict[str, Any]
    commit_audit: AuditDetails = Field(..., alias="commitAudit")
    contribution: ObjectRef
    type: Literal["ORIGINAL_VERSION"] = Field("ORIGINAL_VERSION", alias="_type")

    class Config:
        populate_by_name = True

class DvText(BaseModel):
    value: str
    type: str = Field("DV_TEXT", alias="_type", frozen=True)

    class Config:
        populate_by_name = True