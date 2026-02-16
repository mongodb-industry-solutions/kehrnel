# src/kehrnel/api/compatibility/v1/synthetic/models.py

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime


class SyntheticDataRequest(BaseModel):
    """Request model for generating synthetic data."""
    
    count: int = Field(
        ..., 
        ge=1, 
        le=100, 
        description="Number of synthetic records to generate (max 100)"
    )
    base_composition: Optional[Dict[str, Any]] = Field(
        None,
        description="Base composition template. If not provided, will use the default vaccination composition."
    )
    
    @validator('count')
    def validate_count(cls, v):
        if v < 1:
            raise ValueError('Count must be at least 1')
        if v > 100:
            raise ValueError('Count cannot exceed 100 for performance reasons')
        return v


class SyntheticDataRecord(BaseModel):
    """Model representing a single created synthetic record."""
    
    record_number: int = Field(..., description="Sequential number of this record")
    ehr_id: Optional[str] = Field(None, description="ID of the created EHR")
    subject_id: Optional[str] = Field(None, description="Subject ID of the synthetic patient")
    composition_uid: Optional[str] = Field(None, description="UID of the created composition")
    time_created: Optional[str] = Field(None, description="Timestamp when the record was created")
    error: Optional[str] = Field(None, description="Error message if creation failed")


class SyntheticDataResponse(BaseModel):
    """Response model for synthetic data generation."""
    
    total_requested: int = Field(..., description="Total number of records requested")
    total_created: int = Field(..., description="Total number of records successfully created")
    total_errors: int = Field(..., description="Total number of records that failed to create")
    generation_time_seconds: float = Field(..., description="Time taken to generate all records")
    records: List[SyntheticDataRecord] = Field(..., description="List of created records")
    
    @validator('records', pre=True)
    def sort_records(cls, v):
        """Sort records by record_number to ensure consistent ordering."""
        if isinstance(v, list):
            # Handle both dict and SyntheticDataRecord objects
            return sorted(v, key=lambda x: x.get('record_number', 0) if isinstance(x, dict) else getattr(x, 'record_number', 0))
        return v


class SyntheticDataStats(BaseModel):
    """Statistics about the synthetic data generation process."""
    
    success_rate: float = Field(..., description="Percentage of successful creations")
    average_time_per_record: float = Field(..., description="Average time per record in seconds")
    total_ehrs_created: int = Field(..., description="Total EHRs created")
    total_compositions_created: int = Field(..., description="Total compositions created")