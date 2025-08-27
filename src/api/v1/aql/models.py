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

class QueryRequest(BaseModel):
    """
    Represents the request body for executing an AQL query.
    """
    query: str = Field(..., description="The AQL query string to be executed.", alias="q")
    ehr_id: Optional[str] = Field(None, description="If specified, the query will be executed only on this single EHR.")
    offset: Optional[int] = Field(None, ge=0, description="Number of rows to skip in the result set.")
    fetch: Optional[int] = Field(None, gt=0, description="Maximum number of rows to return.")
    query_parameters: Optional[Dict[str, Any]] = Field(None, description="Parameters to substitute in the AQL query.")

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "q": "SELECT e/ehr_id/value FROM EHR e WHERE e/ehr_id/value = $ehr_id_param",
                "query_parameters": {
                    "ehr_id_param": "a1b2c3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6"
                },
                "offset": 0,
                "fetch": 10
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

class QueryResponse(BaseModel):
    """
    Complete response for AQL query execution, including metadata.
    """
    meta: MetaData = Field(..., description="Metadata about the query execution.")
    name: Optional[str] = Field(None, description="Name of the stored query, if applicable.")
    query: str = Field(..., description="The original AQL query that was executed.", alias="q")
    columns: List[QueryResultColumn] = Field(..., description="Column definitions for the result set.")
    rows: List[List[Any]] = Field(..., description="The actual result data.")

    class Config:
        populate_by_name = True

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