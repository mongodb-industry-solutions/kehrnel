import uuid
from datetime import datetime
from typing import List, Any

from pydantic import BaseModel, ConfigDict, Field

class AuditDetails(BaseModel):
    system_id: str
    committer_name: str
    time_committed: datetime = Field(default_factory=datetime.utcnow)
    change_type: str
    description: str | None = None

class Contribution(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), alias="_id")
    ehr_id: str
    versions: List[Any]
    audit: AuditDetails
    model_config = ConfigDict(populate_by_name=True)
