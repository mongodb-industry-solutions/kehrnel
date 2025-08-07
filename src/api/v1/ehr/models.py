from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Literal

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
    template_id: str
    data: dict 

    class Config:
        json_schema_extra = {
            "example": {
                "template_id": "T-IGR-BIOLOGY",
                "data": {
                    "/content[openEHR-EHR-SECTION.adhoc.v1]/items[openEHR-EHR-OBSERVATION.lab_test-hba1c.v1]/data[at0001]/events[at0002]/data[at0003]/items[at0078.2]": "7.5 %"
                }
            }
        }

class Composition(CompositionCreate):
    uid: str = Field(..., alias = "_id")
    type: Literal["COMPOSITION"] = Field("COMPOSITION", alias="_type")
    time_created: datetime

    class Config:
        populate_by_name = True