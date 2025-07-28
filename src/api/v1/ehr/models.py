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