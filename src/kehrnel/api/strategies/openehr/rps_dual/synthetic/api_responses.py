# src/kehrnel/api/compatibility/v1/synthetic/api_responses.py

from fastapi import status
from kehrnel.api.strategies.openehr.rps_dual.synthetic.models import SyntheticDataResponse, SyntheticDataStats
from kehrnel.api.common.models import ErrorResponse


generate_synthetic_data_responses = {
    status.HTTP_201_CREATED: {
        "description": "Synthetic data generated successfully",
        "model": SyntheticDataResponse,
        "content": {
            "application/json": {
                "example": {
                    "total_requested": 5,
                    "total_created": 5,
                    "total_errors": 0,
                    "generation_time_seconds": 2.456,
                    "records": [
                        {
                            "record_number": 1,
                            "ehr_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                            "subject_id": "synthetic-patient-f1e2d3c4-b5a6-9876-5432-abcdef123456",
                            "composition_uid": "comp123::my-openehr-server::1",
                            "time_created": "2024-01-01T10:30:45.123Z",
                            "error": None
                        },
                        {
                            "record_number": 2,
                            "ehr_id": "b2c3d4e5-f6a7-8901-bcde-f23456789012",
                            "subject_id": "synthetic-patient-g2f3e4d5-c6b7-0987-6543-bcdef234567a",
                            "composition_uid": "comp456::my-openehr-server::1",
                            "time_created": "2024-01-01T10:30:46.234Z",
                            "error": None
                        }
                    ]
                }
            }
        }
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "Invalid request parameters",
        "model": ErrorResponse,
        "content": {
            "application/json": {
                "example": {
                    "detail": "Count must be between 1 and 100"
                }
            }
        }
    },
    status.HTTP_422_UNPROCESSABLE_ENTITY: {
        "description": "Invalid composition template provided",
        "model": ErrorResponse,
        "content": {
            "application/json": {
                "example": {
                    "detail": "Composition could not be processed: Invalid archetype structure"
                }
            }
        }
    },
    status.HTTP_500_INTERNAL_SERVER_ERROR: {
        "description": "Server error during synthetic data generation",
        "model": ErrorResponse,
        "content": {
            "application/json": {
                "example": {
                    "detail": "Failed to generate synthetic data: Database connection error"
                }
            }
        }
    }
}


get_synthetic_stats_responses = {
    status.HTTP_200_OK: {
        "description": "Synthetic data statistics retrieved successfully",
        "model": SyntheticDataStats,
        "content": {
            "application/json": {
                "example": {
                    "success_rate": 95.0,
                    "average_time_per_record": 0.75,
                    "total_ehrs_created": 150,
                    "total_compositions_created": 150
                }
            }
        }
    }
}