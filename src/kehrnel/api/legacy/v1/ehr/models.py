# ehr/models.py

from pydantic import BaseModel, Field
from typing import Optional, List
from kehrnel.api.legacy.v1.ehr_status.models import EHRStatus

from kehrnel.api.legacy.v1.common.models import (
    EhrIdModel,
    SystemIdModel,
    DvDateTime,
    ObjectRef
)


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
