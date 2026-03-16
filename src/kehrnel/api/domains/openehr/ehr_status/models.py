from pydantic import BaseModel, Field
from typing import Literal, Union, Dict
from kehrnel.api.common.models import (
    ObjectVersionID,
    PartySelf,
    SubjectModel,
    HierObjectID,
    ObjectRef,
    DvDateTime,
)


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


class EHRStatusCreate(BaseModel):
    type: Literal["EHR_STATUS"] = Field(..., alias="_type")
    subject: PartySelf
    is_modifiable: bool = True
    is_queryable: bool = True
    
    class Config:
        populate_by_name = True


class VersionedEHRStatus(BaseModel):
    uid: HierObjectID
    owner_id: ObjectRef = Field(..., alias="ownerId")
    time_created: DvDateTime = Field(..., alias="timeCreated")
    type: Literal["VERSIONED_EHR_STATUS"] = Field("VERSIONED_EHR_STATUS", alias="_type")

    class Config:
        populate_by_name = True