# src/kehrnel/api/compatibility/v1/composition/models.py

from pydantic import BaseModel, Field, RootModel, model_validator
from datetime import datetime
from typing import Optional, List, Literal, Any, Dict

from kehrnel.api.bridge.app.core.models import AuditDetails
from kehrnel.api.common.models import HierObjectID, ObjectRef, DvDateTime

class CompositionCreate(RootModel[Dict[str, Any]]):
    """
    A model that accepts a raw dictionary as its payload, which is expected
    to be a valid openEHR canonical COMPOSITION object.
    """
    @model_validator(mode="after")
    def check_composition_structure(self):
        v = self.root
        if "_type" not in v or v["_type"] != "COMPOSITION":
            raise ValueError("Request body must be a valid openEHR COMPOSITION with _type: 'COMPOSITION'")
        if "archetype_details" not in v or "template_id" not in v["archetype_details"]:
            raise ValueError("COMPOSITION must have archetype_details with a template_id")
        return self
    
    @property
    def template_id(self) -> str:
        return self.root["archetype_details"]["template_id"]["value"]
    
    @property
    def content(self) -> Dict[str, Any]:
        return self.root
    

class Composition(BaseModel):
    uid: str = Field(..., alias="_id")
    time_created: datetime
    data: Dict[str, Any]

    class Config:
        populate_by_name = True


class CompositionSummary(BaseModel):
    uid: str
    name: str = ""
    templateId: str = ""


class BulkCompositionDeleteRequest(BaseModel):
    uids: List[str] = Field(default_factory=list)


class BulkCompositionDeleteFailure(BaseModel):
    uid: str
    message: str


class BulkCompositionDeleteResult(BaseModel):
    ehr_id: str
    deletedCount: int
    deletedUids: List[str] = Field(default_factory=list)
    auditUids: List[str] = Field(default_factory=list)
    failed: List[BulkCompositionDeleteFailure] = Field(default_factory=list)
    committedAt: Optional[datetime] = None


class BulkCompositionCreateItem(BaseModel):
    composition: Dict[str, Any]


class BulkCompositionCreateRequest(BaseModel):
    items: List[BulkCompositionCreateItem] = Field(default_factory=list)


class BulkCompositionCreateSuccess(BaseModel):
    index: int
    uid: str
    name: str = ""
    templateId: str = ""


class BulkCompositionCreateFailure(BaseModel):
    index: int
    message: str


class BulkCompositionCreateResult(BaseModel):
    ehr_id: str
    createdCount: int
    created: List[BulkCompositionCreateSuccess] = Field(default_factory=list)
    failed: List[BulkCompositionCreateFailure] = Field(default_factory=list)
    committedAt: Optional[datetime] = None


class VersionedComposition(BaseModel):
    uid: HierObjectID
    owner_id: ObjectRef = Field(..., alias="ownerId")
    time_created: DvDateTime = Field(..., alias="timeCreated")
    type: Literal["VERSIONED_COMPOSITION"] = Field("VERSIONED_COMPOSITION", alias="_type")

    class Config:
        populate_by_name = True
