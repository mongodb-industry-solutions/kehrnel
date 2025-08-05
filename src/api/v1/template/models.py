from pydantic import BaseModel, Field
from datetime import datetime, timezone
from enum import Enum

# Enum for supported template formats
class TemplateFormat(str, Enum):
    ADL_1_4 = "adl1.4"
    ADL_2 = "adl2"

class Template(BaseModel):
    """Pydantic model for storing an operation EHR template in MongoDB"""
    template_id: str = Field(..., alias="_id", description="Unique identifier for the template, this is extracted from the OPT file")

    template_format: TemplateFormat = Field(..., description = "The definition language for the template, this is extracted from the OPT file")

    content: str = Field(..., description="The raw XML content of the template (.opt)")
    created_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Timestamp when the template was created")

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "_id": "T-IGR-PMSI-EXTRACT",
                "template_format": "adl1.4",
                "content": "<?xml version='1.0' encoding='UTF-8'?><template>...</template>",
                "created_timestamp": "2023-10-01T12:00:00Z"
            }
        }