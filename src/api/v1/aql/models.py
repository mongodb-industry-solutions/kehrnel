# src/api/v1/aql/models.py

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

class StoredQuery(BaseModel):
    """
    Represents a stored AQL query in the database.
    """
    name: str = Field(..., alias="_id", description="The unique name/identifier for the stored query, like 'com.example.org::my_query/v1'.")
    query: str = Field(..., description="The AQL query string.")
    created_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Timestamp of when the query was stored.")

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "_id": "com.example.org::my_query/v1",
                "query": "SELECT o/data[at0002]/events[at0003]/data[at0001]/items[at0004]/value/magnitude AS Systolic FROM EHR e CONTAINS COMPOSITION c CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v2]",
                "created_timestamp": "2023-11-20T10:00:00Z"
            }
        }

class QueryResultColumn(BaseModel):
    """
    Defines the name and path of a single column in an AQL query result set.
    """
    name: str
    path: str

class MetaData(BaseModel):
    """
    Metadata associated with AQL query results.
    """
    href: str = Field(..., description="Reference to the executed query endpoint.")
    type: str = Field(default="RESULTSET", description="Type of the result.")
    schema_version: str = Field(default="1.0.4", description="Version of the openEHR REST API.")
    created: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Timestamp when the result was created.")
    generator: str = Field(default="PythonEHRBase/1.0.0", description="Software that generated this result.")
    executed_aql: str = Field(..., description="The AQL query that was actually executed.")

class StoredQueryDefinition(BaseModel):
    """
    Definition of a stored query for creation/update operations.
    """
    query_text: str = Field(..., description="The AQL query string", alias="q")
    type: str = Field(default="AQL", description="Type of query language")

class StoredQuerySummary(BaseModel):
    """A summary view of a stored query."""
    name: str = Field(..., alias="_id")
    created_timestamp: datetime

    class Config:
        populate_by_name = True

class QueryResponse(BaseModel):
    """
    Standard response model for an executed AQL query.
    """
    meta: MetaData
    q: str = Field(..., description="The original AQL query string sent by the client.")
    columns: List[QueryResultColumn]
    rows: List[Dict[str, Any]]