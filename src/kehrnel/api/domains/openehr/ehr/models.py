# ehr/models.py

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field
from kehrnel.api.domains.openehr.ehr_status.models import EHRStatus

from kehrnel.api.common.models import (
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
    model_config = ConfigDict(populate_by_name=True)
