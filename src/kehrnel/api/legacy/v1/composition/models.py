# src/kehrnel/api/legacy/v1/composition/models.py

from pydantic import BaseModel, Field, validator, RootModel
from datetime import datetime
from typing import Optional, List, Literal, Any, Dict

from kehrnel.api.legacy.app.core.models import AuditDetails
from kehrnel.api.legacy.v1.common.models import HierObjectID, ObjectRef, DvDateTime

class CompositionCreate(RootModel[Dict[str, Any]]):
    """
    A model that accepts a raw dictionary as its payload, which is expected
    to be a valid openEHR canonical COMPOSITION object.
    """
    @validator("root")
    def check_composition_structure(cls, v):
        if "_type" not in v or v["_type"] != "COMPOSITION":
            raise ValueError("Request body must be a valid openEHR COMPOSITION with _type: 'COMPOSITION'")
        if "archetype_details" not in v or "template_id" not in v["archetype_details"]:
            raise ValueError("COMPOSITION must have archetype_details with a template_id")
        return v
    
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


class VersionedComposition(BaseModel):
    uid: HierObjectID
    owner_id: ObjectRef = Field(..., alias="ownerId")
    time_created: DvDateTime = Field(..., alias="timeCreated")
    type: Literal["VERSIONED_COMPOSITION"] = Field("VERSIONED_COMPOSITION", alias="_type")

    class Config:
        populate_by_name = True