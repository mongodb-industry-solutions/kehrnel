from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import Optional, List, Literal, Any, Dict

# Based on OpenEHR RM PartySelf
class PartySelf(BaseModel):
    id: str = Field(..., description="Identifier of the subject of care")
    namespace: str = Field("local", description="Namespace of the identifier")

# Based on OpenEHR RM EHR_STATUS
class EHRStatus(BaseModel):
    uid: Optional[str] = None
    type: Literal["EHR_STATUS"] = Field("EHR_STATUS", alias="_type")
    subject: PartySelf
    is_modifiable: bool = True
    is_queryable: bool = True

class EHRCreationResponse(BaseModel):
    ehr_id: str
    ehr_status: EHRStatus
    system_id: str
    time_created: datetime = Field(..., description="Timestamp when the EHR was created")

class EHR(BaseModel):
    # The ehr_id is now aliased to be the primary key _id
    ehr_id: str = Field(..., alias="_id")
    system_id: str
    time_created: datetime
    ehr_status: EHRStatus
    contributions: List[str] = []
    compositions: List[str] = []
    directory_id: str | None = None

    class Config:
        populate_by_name = True

class ErrorResponse(BaseModel):
    detail: str


class CompositionCreate(BaseModel):
    root: Dict[str, Any]
    
    @validator("root")
    def check_composition_structure(cls, v):
        # Basic validator to ensure we're getting a composition-like object
        if "_type" not in v or v["_type"] != "COMPOSITION":
            raise ValueError("Request body must be a valid openEHR composition with _type: 'COMPOSITION'")
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

class Composition(BaseModel):
    uid: str = Field(..., alias="_id")
    time_created: datetime
    # The 'data' field will hold the entire canonical JSON object
    data: Dict[str, Any]

    class Config:
        populate_by_name = True