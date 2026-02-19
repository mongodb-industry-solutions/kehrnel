# src/kehrnel/api/composition/models.py

from datetime import datetime
from typing import Any, Dict, Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator
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
    model_config = ConfigDict(populate_by_name=True)


class VersionedComposition(BaseModel):
    uid: HierObjectID
    owner_id: ObjectRef = Field(..., alias="ownerId")
    time_created: DvDateTime = Field(..., alias="timeCreated")
    type: Literal["VERSIONED_COMPOSITION"] = Field("VERSIONED_COMPOSITION", alias="_type")
    model_config = ConfigDict(populate_by_name=True)
