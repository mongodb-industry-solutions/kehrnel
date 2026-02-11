# src/kehrnel/api/legacy/v1/directory/models.py

from pydantic import BaseModel, Field
from typing import List, Optional

from kehrnel.api.legacy.v1.common.models import (
    ObjectVersionID,
    ObjectRef,
    DvText
)

class Folder(BaseModel):
    type: str = Field(alias="_type", default="FOLDER", frozen=True)
    archetype_node_id: str
    name: DvText
    uid: Optional[ObjectVersionID] = None
    items: List[ObjectRef] = []
    folders: List['Folder'] = []
    
    class Config:
        populate_by_name = True

Folder.model_rebuild()

class FolderCreate(BaseModel):
    type: str = Field(alias="_type", default="FOLDER", frozen=True)
    archetype_node_id: str = "openEHR-EHR-FOLDER.directory.v1"
    name: DvText = Field(default_factory=lambda: DvText(value="Directory"))
    items: List[ObjectRef] = []
    folders: List['FolderCreate'] = []

    class Config:
        populate_by_name = True


Folder.model_rebuild()
FolderCreate.model_rebuild()