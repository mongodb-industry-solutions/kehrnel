# ehr/models.py

from pydantic import BaseModel, Field, validator, RootModel
from datetime import datetime
from typing import Optional, List, Literal, Any, Dict, Union
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


# Based on OpenEHR RM PartySelf
class PartySelf(BaseModel):
    type: Literal["PARTY_SELF"] = Field("PARTY_SELF", alias="_type")
    external_ref: Optional[Dict[str, Any]] = None # Simplified for now

    class Config:
        populate_by_name = True


class SubjectModel(BaseModel):
    id: HierObjectID
    namespace: str


# Based on OpenEHR RM EHR_STATUS
class EHRStatus(BaseModel):
    uid: ObjectVersionID
    type: Literal["EHR_STATUS"] = Field("EHR_STATUS", alias="_type")
    archetype_node_id: str = "openEHR-EHR-EHR_STATUS.generic.v1"
    name: Dict[str, str] = {"value": "EHR status"}
    subject: Union[PartySelf, SubjectModel]
    is_modifiable: bool = True
    is_queryable: bool = True

    class Config:
        populate_by_name = True


# Model for the EHR_STATUS provided in a request body
class EHRStatusCreate(BaseModel):
    type: Literal["EHR_STATUS"] = Field(..., alias="_type")
    subject: PartySelf
    is_modifiable: bool = True
    is_queryable: bool = True
    
    class Config:
        populate_by_name = True


class DvDateTime(BaseModel):
    value: datetime


class ObjectRef(BaseModel):
    id: HierObjectID
    namespace: str = "local"
    type: str


class EHRCreationResponse(BaseModel):
    ehr_id: EhrIdModel
    ehr_status: ObjectRef
    system_id: SystemIdModel
    time_created: DvDateTime
    ehr_access: ObjectRef
    

class EHR(BaseModel):
    ehr_id: EhrIdModel = Field(..., alias="_id")
    system_id: SystemIdModel
    time_created: DvDateTime
    ehr_status: EHRStatus
    ehr_access: ObjectRef
    contributions: List[ObjectRef] = []
    compositions: List[ObjectRef] = []
    directory: Optional[ObjectRef] = None

    class Config:
        populate_by_name = True

class ErrorResponse(BaseModel):
    detail: str


class CompositionCreate(RootModel[Dict[str, Any]]):
    """
    A model that accepts a raw dictionary as its payload, which is expected
    to be a valid openEHR canonical COMPOSITION object.
    """

    @validator("root")
    def check_composition_structure(cls, v):
        # Basic validation to ensure we're getting a composition-like object.
        # With RootModel, 'v' is the entire dictionary.
        if "_type" not in v or v["_type"] != "COMPOSITION":
            raise ValueError("Request body must be a valid openEHR COMPOSITION with _type: 'COMPOSITION'")
        if "archetype_details" not in v or "template_id" not in v["archetype_details"]:
            raise ValueError("COMPOSITION must have archetype_details with a template_id")
        return v
    
    # Helper property to easily access the template_id
    @property
    def template_id(self) -> str:
        return self.root["archetype_details"]["template_id"]["value"]
    
     # Helper property to get the full dictionary content
    @property
    def content(self) -> Dict[str, Any]:
        return self.root

# Full Composition model for database and response. This model is correct and does not need changes.
class Composition(BaseModel):
    uid: str = Field(..., alias="_id")
    time_created: datetime
    # The 'data' field will hold the entire canonical JSON object
    data: Dict[str, Any]

    class Config:
        populate_by_name = True

class RevisionHistoryItem(BaseModel):
    version_id: ObjectVersionID = Field(..., alias="versionId")
    audit: AuditDetails

    class Config:
        populate_by_name = True

# The top-level response model for the revision history endpoint
class RevisionHistory(BaseModel):
    items: List[RevisionHistoryItem]