# src/kehrnel/api/legacy/v1/synthetic/routes.py

import json
import time
from fastapi import APIRouter, Depends, status, Body, Request, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Dict, Any

from kehrnel.api.legacy.v1.synthetic.service import generate_synthetic_data
from kehrnel.api.legacy.v1.synthetic.models import (
    SyntheticDataRequest,
    SyntheticDataResponse,
    SyntheticDataRecord,
    SyntheticDataStats
)
from kehrnel.api.legacy.v1.synthetic.api_responses import (
    generate_synthetic_data_responses,
    get_synthetic_stats_responses
)
from kehrnel.legacy.transform.flattener_g import CompositionFlattener
from kehrnel.api.legacy.app.core.database import get_mongodb_ehr_db


router = APIRouter(
    prefix="/synthetic",
    tags=["Synthetic Data"]
)


def get_flattener(request: Request) -> CompositionFlattener:
    """
    Dependency to retrieve the globally initialized CompositionFlattener
    """
    return request.app.state.flattener


def get_default_vaccination_composition() -> Dict[str, Any]:
    """
    Default vaccination composition template based on the provided vacc_composition.json
    """
    return {
        "_type": "COMPOSITION",
        "name": {
            "_type": "DV_TEXT",
            "value": "HC3 Immunization List"
        },
        "archetype_details": {
            "archetype_id": {
                "value": "openEHR-EHR-COMPOSITION.vaccination_list.v0"
            },
            "template_id": {
                "value": "HC3 Immunization List v0.5"
            },
            "rm_version": "1.0.4"
        },
        "feeder_audit": {
            "_type": "FEEDER_AUDIT",
            "originating_system_item_ids": [
                {
                    "_type": "DV_IDENTIFIER",
                    "id": "76800561202195058471136828980959641600",
                    "type": "cpc"
                },
                {
                    "_type": "DV_IDENTIFIER",
                    "id": "290699037",
                    "type": "cpi"
                },
                {
                    "_type": "DV_IDENTIFIER",
                    "id": "231542544",
                    "type": "center_origin"
                }
            ],
            "feeder_system_item_ids": [
                {
                    "_type": "DV_IDENTIFIER",
                    "id": "76800561202195058471136828980959641600",
                    "type": "cpc"
                }
            ],
            "originating_system_audit": {
                "_type": "FEEDER_AUDIT_DETAILS",
                "system_id": "salutms.cat"
            }
        },
        "language": {
            "_type": "CODE_PHRASE",
            "terminology_id": {
                "_type": "TERMINOLOGY_ID",
                "value": "ISO_639-1"
            },
            "code_string": "en"
        },
        "territory": {
            "_type": "CODE_PHRASE",
            "terminology_id": {
                "_type": "TERMINOLOGY_ID",
                "value": "ISO_3166-1"
            },
            "code_string": "ES"
        },
        "category": {
            "_type": "DV_CODED_TEXT",
            "value": "event",
            "defining_code": {
                "_type": "CODE_PHRASE",
                "terminology_id": {
                    "_type": "TERMINOLOGY_ID",
                    "value": "openehr"
                },
                "code_string": "433"
            }
        },
        "composer": {
            "_type": "PARTY_IDENTIFIED",
            "identifiers": [
                {
                    "_type": "DV_IDENTIFIER",
                    "id": "HC3"
                }
            ]
        },
        "context": {
            "_type": "EVENT_CONTEXT",
            "start_time": {
                "_type": "DV_DATE_TIME",
                "value": "2023-12-21T03:42:44.219+01:00"
            },
            "setting": {
                "_type": "DV_CODED_TEXT",
                "value": "other care",
                "defining_code": {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {
                        "_type": "TERMINOLOGY_ID",
                        "value": "openehr"
                    },
                    "code_string": "238"
                }
            },
            "other_context": {
                "_type": "ITEM_TREE",
                "name": {
                    "_type": "DV_TEXT",
                    "value": "Tree"
                },
                "items": [
                    {
                        "_type": "CLUSTER",
                        "name": {
                            "_type": "DV_TEXT",
                            "value": "XDS Metadata"
                        },
                        "archetype_details": {
                            "archetype_id": {
                                "value": "openEHR-EHR-CLUSTER.xds_metadata.v0"
                            },
                            "template_id": {
                                "value": "HC3 Immunization List v0.5"
                            },
                            "rm_version": "1.0.4"
                        },
                        "items": [
                            {
                                "_type": "ELEMENT",
                                "name": {
                                    "_type": "DV_TEXT",
                                    "value": "Document type"
                                },
                                "value": {
                                    "_type": "DV_CODED_TEXT",
                                    "value": "-",
                                    "defining_code": {
                                        "_type": "CODE_PHRASE",
                                        "terminology_id": {
                                            "_type": "TERMINOLOGY_ID",
                                            "value": "2.16.840.1.113883.6.1"
                                        },
                                        "code_string": "VAC"
                                    }
                                },
                                "archetype_node_id": "at0003"
                            },
                            {
                                "_type": "ELEMENT",
                                "name": {
                                    "_type": "DV_TEXT",
                                    "value": "Confidentiality code"
                                },
                                "value": {
                                    "_type": "DV_CODED_TEXT",
                                    "value": "-",
                                    "defining_code": {
                                        "_type": "CODE_PHRASE",
                                        "terminology_id": {
                                            "_type": "TERMINOLOGY_ID",
                                            "value": "2.16.840.1.113883.5.25"
                                        },
                                        "code_string": "N"
                                    }
                                },
                                "archetype_node_id": "at0004"
                            }
                        ],
                        "archetype_node_id": "openEHR-EHR-CLUSTER.xds_metadata.v0"
                    },
                    {
                        "_type": "CLUSTER",
                        "name": {
                            "_type": "DV_TEXT",
                            "value": "Admin Salut"
                        },
                        "archetype_details": {
                            "archetype_id": {
                                "value": "openEHR-EHR-CLUSTER.admin_salut.v0"
                            },
                            "template_id": {
                                "value": "HC3 Immunization List v0.5"
                            },
                            "rm_version": "1.0.4"
                        },
                        "items": [
                            {
                                "_type": "ELEMENT",
                                "name": {
                                    "_type": "DV_TEXT",
                                    "value": "Document authorisation date"
                                },
                                "value": {
                                    "_type": "DV_DATE_TIME",
                                    "value": "2016-10-20T14:02:52+02:00"
                                },
                                "archetype_node_id": "at0001"
                            },
                            {
                                "_type": "ELEMENT",
                                "name": {
                                    "_type": "DV_TEXT",
                                    "value": "Document creation date"
                                },
                                "value": {
                                    "_type": "DV_DATE_TIME",
                                    "value": "2016-10-20T14:02:52+02:00"
                                },
                                "archetype_node_id": "at0002"
                            },
                            {
                                "_type": "ELEMENT",
                                "name": {
                                    "_type": "DV_TEXT",
                                    "value": "Document publication date"
                                },
                                "value": {
                                    "_type": "DV_DATE_TIME",
                                    "value": "2016-10-20T14:08:05+02:00"
                                },
                                "archetype_node_id": "at0023"
                            },
                            {
                                "_type": "CLUSTER",
                                "name": {
                                    "_type": "DV_TEXT",
                                    "value": "Publishing institution"
                                },
                                "items": [
                                    {
                                        "_type": "ELEMENT",
                                        "name": {
                                            "_type": "DV_TEXT",
                                            "value": "Publishing centre"
                                        },
                                        "value": {
                                            "_type": "DV_CODED_TEXT",
                                            "value": "-",
                                            "defining_code": {
                                                "_type": "CODE_PHRASE",
                                                "terminology_id": {
                                                    "_type": "TERMINOLOGY_ID",
                                                    "value": "2.16.840.1.113883.2.19.10.1"
                                                },
                                                "code_string": "E08665478"
                                            }
                                        },
                                        "archetype_node_id": "at0014"
                                    },
                                    {
                                        "_type": "ELEMENT",
                                        "name": {
                                            "_type": "DV_TEXT",
                                            "value": "Publishing UP"
                                        },
                                        "value": {
                                            "_type": "DV_CODED_TEXT",
                                            "value": "-",
                                            "defining_code": {
                                                "_type": "CODE_PHRASE",
                                                "terminology_id": {
                                                    "_type": "TERMINOLOGY_ID",
                                                    "value": "-"
                                                },
                                                "code_string": "04547"
                                            }
                                        },
                                        "archetype_node_id": "at0016"
                                    }
                                ],
                                "archetype_node_id": "at0007"
                            },
                            {
                                "_type": "CLUSTER",
                                "name": {
                                    "_type": "DV_TEXT",
                                    "value": "Custodial institution"
                                },
                                "items": [
                                    {
                                        "_type": "ELEMENT",
                                        "name": {
                                            "_type": "DV_TEXT",
                                            "value": "Custodial centre"
                                        },
                                        "value": {
                                            "_type": "DV_CODED_TEXT",
                                            "value": "-",
                                            "defining_code": {
                                                "_type": "CODE_PHRASE",
                                                "terminology_id": {
                                                    "_type": "TERMINOLOGY_ID",
                                                    "value": "2.16.840.1.113883.4.292.10.4"
                                                },
                                                "code_string": "E08665478"
                                            }
                                        },
                                        "archetype_node_id": "at0017"
                                    }
                                ],
                                "archetype_node_id": "at0010"
                            }
                        ],
                        "archetype_node_id": "openEHR-EHR-CLUSTER.admin_salut.v0"
                    }
                ],
                "archetype_node_id": "at0004"
            }
        },
        "content": [
            {
                "_type": "SECTION",
                "name": {
                    "_type": "DV_TEXT",
                    "value": "Immunization list"
                },
                "archetype_details": {
                    "archetype_id": {
                        "value": "openEHR-EHR-SECTION.immunisation_list.v0"
                    },
                    "template_id": {
                        "value": "HC3 Immunization List v0.5"
                    },
                    "rm_version": "1.0.4"
                },
                "items": [
                    {
                        "_type": "ACTION",
                        "name": {
                            "_type": "DV_TEXT",
                            "value": "Immunization management"
                        },
                        "archetype_details": {
                            "archetype_id": {
                                "value": "openEHR-EHR-ACTION.medication.v1"
                            },
                            "template_id": {
                                "value": "HC3 Immunization List v0.5"
                            },
                            "rm_version": "1.0.4"
                        },
                        "language": {
                            "_type": "CODE_PHRASE",
                            "terminology_id": {
                                "_type": "TERMINOLOGY_ID",
                                "value": "ISO_639-1"
                            },
                            "code_string": "en"
                        },
                        "encoding": {
                            "_type": "CODE_PHRASE",
                            "terminology_id": {
                                "_type": "TERMINOLOGY_ID",
                                "value": "IANA_character-sets"
                            },
                            "code_string": "UTF-8"
                        },
                        "subject": {
                            "_type": "PARTY_SELF"
                        },
                        "provider": {
                            "_type": "PARTY_IDENTIFIED",
                            "identifiers": [
                                {
                                    "_type": "DV_IDENTIFIER",
                                    "id": "A",
                                    "type": "BCAE0AA4-92B6-11DC-AF5F-27E855D89593"
                                }
                            ]
                        },
                        "other_participations": [
                            {
                                "_type": "PARTICIPATION",
                                "function": {
                                    "_type": "DV_TEXT",
                                    "value": "Performer"
                                },
                                "performer": {
                                    "_type": "PARTY_IDENTIFIED",
                                    "name": "TERESA",
                                    "identifiers": [
                                        {
                                            "_type": "DV_IDENTIFIER",
                                            "id": "P..L",
                                            "type": "1.3.6.1.4.1.5734.1.2"
                                        },
                                        {
                                            "_type": "DV_IDENTIFIER",
                                            "id": "M..L",
                                            "type": "1.3.6.1.4.1.5734.1.3"
                                        },
                                        {
                                            "_type": "DV_IDENTIFIER",
                                            "id": "01817273",
                                            "type": "2.16.840.1.113883.4.292.10.2"
                                        },
                                        {
                                            "_type": "DV_IDENTIFIER",
                                            "id": "MD",
                                            "type": "83D02C4E-92B6-11DC-9BB7-10E755D89593"
                                        }
                                    ]
                                }
                            }
                        ],
                        "time": {
                            "_type": "DV_DATE_TIME",
                            "value": "2006-11-13T00:00:00+01:00"
                        },
                        "description": {
                            "_type": "ITEM_TREE",
                            "name": {
                                "_type": "DV_TEXT",
                                "value": "Tree"
                            },
                            "items": [
                                {
                                    "_type": "ELEMENT",
                                    "name": {
                                        "_type": "DV_TEXT",
                                        "value": "Immunization item"
                                    },
                                    "value": {
                                        "_type": "DV_CODED_TEXT",
                                        "value": "Meningocòccica C conjugada",
                                        "defining_code": {
                                            "_type": "CODE_PHRASE",
                                            "terminology_id": {
                                                "_type": "TERMINOLOGY_ID",
                                                "value": "E0FC202C-9D9B-11DC-BA79-9FB156D89593"
                                            },
                                            "code_string": "MCC"
                                        }
                                    },
                                    "archetype_node_id": "at0020"
                                },
                                {
                                    "_type": "CLUSTER",
                                    "name": {
                                        "_type": "DV_TEXT",
                                        "value": "Immunization details"
                                    },
                                    "archetype_details": {
                                        "archetype_id": {
                                            "value": "openEHR-EHR-CLUSTER.medication.v2"
                                        },
                                        "template_id": {
                                            "value": "HC3 Immunization List v0.5"
                                        },
                                        "rm_version": "1.0.4"
                                    },
                                    "items": [
                                        {
                                            "_type": "ELEMENT",
                                            "name": {
                                                "_type": "DV_TEXT",
                                                "value": "Name"
                                            },
                                            "value": {
                                                "_type": "DV_CODED_TEXT",
                                                "value": "650785",
                                                "defining_code": {
                                                    "_type": "CODE_PHRASE",
                                                    "terminology_id": {
                                                        "_type": "TERMINOLOGY_ID",
                                                        "value": "2.16.840.1.113883.4.292.10.5"
                                                    },
                                                    "code_string": "650785"
                                                }
                                            },
                                            "archetype_node_id": "at0132"
                                        },
                                        {
                                            "_type": "CLUSTER",
                                            "name": {
                                                "_type": "DV_TEXT",
                                                "value": "Constituent details"
                                            },
                                            "archetype_details": {
                                                "archetype_id": {
                                                    "value": "openEHR-EHR-CLUSTER.medication.v2"
                                                },
                                                "template_id": {
                                                    "value": "HC3 Immunization List v0.5"
                                                },
                                                "rm_version": "1.0.4"
                                            },
                                            "items": [
                                                {
                                                    "_type": "ELEMENT",
                                                    "name": {
                                                        "_type": "DV_TEXT",
                                                        "value": "Name"
                                                    },
                                                    "value": {
                                                        "_type": "DV_CODED_TEXT",
                                                        "value": "Meningocòccica C conjugada",
                                                        "defining_code": {
                                                            "_type": "CODE_PHRASE",
                                                            "terminology_id": {
                                                                "_type": "TERMINOLOGY_ID",
                                                                "value": "E0FC202C-9D9B-11DC-BA79-9FB156D89593"
                                                            },
                                                            "code_string": "MCC"
                                                        }
                                                    },
                                                    "archetype_node_id": "at0132"
                                                }
                                            ],
                                            "archetype_node_id": "openEHR-EHR-CLUSTER.medication.v2"
                                        }
                                    ],
                                    "archetype_node_id": "openEHR-EHR-CLUSTER.medication.v2"
                                }
                            ],
                            "archetype_node_id": "at0017"
                        },
                        "ism_transition": {
                            "_type": "ISM_TRANSITION",
                            "current_state": {
                                "_type": "DV_CODED_TEXT",
                                "value": "active",
                                "defining_code": {
                                    "_type": "CODE_PHRASE",
                                    "terminology_id": {
                                        "_type": "TERMINOLOGY_ID",
                                        "value": "openehr"
                                    },
                                    "code_string": "245"
                                }
                            }
                        },
                        "archetype_node_id": "openEHR-EHR-ACTION.medication.v1"
                    }
                ],
                "archetype_node_id": "openEHR-EHR-SECTION.immunisation_list.v0"
            }
        ],
        "archetype_node_id": "openEHR-EHR-COMPOSITION.vaccination_list.v0"
    }


@router.post(
    "/generate",
    response_model=SyntheticDataResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Generate Synthetic EHR Data",
    description="Generate synthetic EHR data with compositions based on a template. Each record creates an EHR with a subject and attaches a randomized composition based on the provided template.",
    responses=generate_synthetic_data_responses
)
async def generate_synthetic_data_endpoint(
    request: Request,
    synthetic_request: SyntheticDataRequest = Body(
        ...,
        description="Configuration for synthetic data generation",
        examples={
            "simple": {
                "summary": "Generate 5 records with default vaccination template",
                "description": "Uses the built-in vaccination composition template",
                "value": {
                    "count": 5
                }
            },
            "custom": {
                "summary": "Generate 10 records with custom composition",
                "description": "Provide your own composition template",
                "value": {
                    "count": 10,
                    "base_composition": {
                        "_type": "COMPOSITION",
                        "name": {"_type": "DV_TEXT", "value": "Custom Template"},
                        "archetype_details": {
                            "archetype_id": {"value": "openEHR-EHR-COMPOSITION.custom.v1"},
                            "template_id": {"value": "Custom Template v1.0"}
                        }
                    }
                }
            }
        }
    ),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db),
    flattener: CompositionFlattener = Depends(get_flattener)
):
    """
    Generate synthetic EHR data with compositions.
    
    This endpoint creates synthetic clinical data for testing and development purposes.
    For each requested record, it will:
    
    1. Create a new EHR with a synthetic patient subject
    2. Generate a randomized composition based on the provided template
    3. Link the composition to the EHR
    4. Store both canonical and flattened versions of the composition
    
    **Features:**
    - Randomizes dates, identifiers, and clinical data
    - Creates realistic variations of the base template
    - Supports custom composition templates
    - Provides detailed response with creation statistics
    - Handles partial failures gracefully
    
    **Default Template:**
    If no base_composition is provided, uses the HC3 Immunization List template
    which includes vaccination data with randomized:
    - Patient identifiers
    - Vaccination dates
    - Vaccine types (Meningococcal C, Hepatitis B, Tetanus-Diphtheria, etc.)
    - Healthcare provider information
    - Document metadata
    """
    
    start_time = time.time()
    
    # Use default vaccination composition if none provided
    base_composition = synthetic_request.base_composition
    if not base_composition:
        base_composition = get_default_vaccination_composition()
    
    # Get merge_search_docs configuration
    target_search = request.app.state.config.get("target", {})
    merge_search = target_search.get("search_compositions_merge", False)

    # Use runtime target DB if available
    db = getattr(request.app.state, "target_db", db)
    
    try:
        # Generate the synthetic data
        created_records = await generate_synthetic_data(
            db=db,
            base_composition=base_composition,
            count=synthetic_request.count,
            flattener=flattener,
            merge_search_docs=merge_search
        )
        
        end_time = time.time()
        generation_time = end_time - start_time
        
        # Convert to response models
        record_models = [SyntheticDataRecord(**record) for record in created_records]
        
        # Calculate statistics
        successful_records = [r for r in record_models if r.error is None]
        failed_records = [r for r in record_models if r.error is not None]
        
        response = SyntheticDataResponse(
            total_requested=synthetic_request.count,
            total_created=len(successful_records),
            total_errors=len(failed_records),
            generation_time_seconds=round(generation_time, 3),
            records=record_models
        )
        
        return response
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate synthetic data: {str(e)}"
        )


@router.get(
    "/stats",
    response_model=SyntheticDataStats,
    status_code=status.HTTP_200_OK,
    summary="Get Synthetic Data Statistics",
    description="Get statistics about the synthetic data generation process from the last generation run.",
    responses=get_synthetic_stats_responses
)
async def get_synthetic_data_stats():
    """
    Get statistics about synthetic data generation.
    
    Note: This is a placeholder endpoint. In a production system, you might want to:
    - Store statistics in the database
    - Track historical generation runs
    - Provide more detailed analytics
    """
    # This is a placeholder implementation
    # In a real system, you'd retrieve actual statistics from storage
    return SyntheticDataStats(
        success_rate=95.0,
        average_time_per_record=0.75,
        total_ehrs_created=0,
        total_compositions_created=0
    )
