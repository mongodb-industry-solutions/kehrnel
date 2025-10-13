from fastapi import status
from src.app.core.models import Contribution
from src.api.v1.contribution.models import ErrorResponse

get_contribution_responses = {
    status.HTTP_200_OK: {
        "description": "Contribution found and returned successfully",
        "model": Contribution,
        "content": {
            "application/json": {
                "example": {
                    "_id": "c1d2e3f4-a5b6-c7d8-e9f0-a1b2c3d4e5f6",
                    "ehr_id": "a1b2c3d4-e4f5-a6b7-c8d9-e0f1a2b3c4d5",
                    "versions": [
                        {
                            "_type": "EHR_STATUS",
                            "uid": {
                                "value": "f1g2h3i4::my-openehr-server::1",
                                "_type": "OBJECT_VERSION_ID"
                            }
                        }
                    ],
                    "audit": {
                        "system_id": "my-openehr-server",
                        "committer_name": "System",
                        "time_committed": "2024-01-01T10:00:00.000Z",
                        "change_type": "creation"
                    }
                }
            }
        },
        "headers": {
            "ETag": {
                "description": "The ETag of the contribution (its UID)",
                "schema": {
                    "type": "string"
                }
            },
            "Location": {
                "description": "The path to the retrieved contribution resource.",
                "schema": {
                    "type": "string"
                }
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The specified EHR or the Contribution within it was not found.",
        "model": ErrorResponse
    }
}


