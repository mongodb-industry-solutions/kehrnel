# src/kehrnel/api/directory/models.py

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from kehrnel.api.common.models import (
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
    model_config = ConfigDict(populate_by_name=True)

Folder.model_rebuild()

class FolderCreate(BaseModel):
    type: str = Field(alias="_type", default="FOLDER", frozen=True)
    archetype_node_id: str = "openEHR-EHR-FOLDER.directory.v1"
    name: DvText = Field(default_factory=lambda: DvText(value="Directory"))
    items: List[ObjectRef] = []
    folders: List['FolderCreate'] = []
    model_config = ConfigDict(populate_by_name=True)


Folder.model_rebuild()
FolderCreate.model_rebuild()
