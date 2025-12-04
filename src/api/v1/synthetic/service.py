# src/api/v1/synthetic/service.py

import uuid
import json
import random
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Dict, Any, List, Optional
from fastapi import HTTPException, status

from src.api.v1.ehr.service import create_ehr
from src.api.v1.composition.service import add_composition
from src.api.v1.composition.models import CompositionCreate
from src.api.v1.ehr_status.models import EHRStatusCreate
from src.transform.flattener_g import CompositionFlattener


class SyntheticDataGenerator:
    """
    Service for generating synthetic data based on a base composition template.
    """
    
    def __init__(self):
        self.base_composition_template: Optional[Dict[str, Any]] = None
        
    def load_base_composition(self, composition_data: Dict[str, Any]) -> None:
        """Load a base composition template for synthetic data generation."""
        self.base_composition_template = composition_data
    
    def _generate_random_identifier(self, prefix: str = "") -> str:
        """Generate a random identifier with optional prefix."""
        return f"{prefix}{random.randint(100000000, 999999999)}"
    
    def _generate_random_date(self, start_days_ago: int = 365, end_days_ago: int = 0) -> str:
        """Generate a random datetime within the specified range."""
        start_date = datetime.now(timezone.utc) - timedelta(days=start_days_ago)
        end_date = datetime.now(timezone.utc) - timedelta(days=end_days_ago)
        
        random_timestamp = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        return random_timestamp.isoformat()
    
    def _randomize_feeder_audit_ids(self, composition: Dict[str, Any]) -> Dict[str, Any]:
        """Randomize feeder audit identifiers in the composition."""
        if "feeder_audit" in composition:
            feeder_audit = composition["feeder_audit"]
            
            # Randomize originating_system_item_ids
            if "originating_system_item_ids" in feeder_audit:
                for item in feeder_audit["originating_system_item_ids"]:
                    if item.get("type") == "cpc":
                        item["id"] = self._generate_random_identifier()
                    elif item.get("type") == "cpi":
                        item["id"] = self._generate_random_identifier()
                    elif item.get("type") == "center_origin":
                        item["id"] = self._generate_random_identifier()
            
            # Randomize feeder_system_item_ids
            if "feeder_system_item_ids" in feeder_audit:
                for item in feeder_audit["feeder_system_item_ids"]:
                    if item.get("type") == "cpc":
                        item["id"] = self._generate_random_identifier()
        
        return composition
    
    def _randomize_composition_dates(self, composition: Dict[str, Any]) -> Dict[str, Any]:
        """Randomize various dates in the composition."""
        # Randomize context start_time
        if "context" in composition and "start_time" in composition["context"]:
            composition["context"]["start_time"]["value"] = self._generate_random_date(365, 30)
        
        # Randomize other_context dates (Admin Salut dates)
        if "context" in composition and "other_context" in composition["context"]:
            items = composition["context"]["other_context"].get("items", [])
            for item in items:
                if item.get("archetype_node_id") == "openEHR-EHR-CLUSTER.admin_salut.v0":
                    admin_items = item.get("items", [])
                    for admin_item in admin_items:
                        if admin_item.get("archetype_node_id") in ["at0001", "at0002", "at0023"]:
                            # Document dates
                            admin_item["value"]["value"] = self._generate_random_date(365, 30)
        
        # Randomize action time in content
        if "content" in composition:
            for content_item in composition["content"]:
                if content_item.get("_type") == "SECTION":
                    section_items = content_item.get("items", [])
                    for section_item in section_items:
                        if section_item.get("_type") == "ACTION":
                            # Randomize action time
                            if "time" in section_item:
                                section_item["time"]["value"] = self._generate_random_date(1825, 365)  # between 1 and 5 years ago
        
        return composition
    
    def _randomize_performer_data(self, composition: Dict[str, Any]) -> Dict[str, Any]:
        """Randomize performer/provider data in the composition."""
        # List of random names and IDs for variation
        performer_names = ["MARIA", "CARLOS", "ANA", "JOSE", "LAURA", "DAVID", "ELENA", "MIGUEL"]
        
        # Randomize provider identifiers
        if "content" in composition:
            for content_item in composition["content"]:
                if content_item.get("_type") == "SECTION":
                    section_items = content_item.get("items", [])
                    for section_item in section_items:
                        if section_item.get("_type") == "ACTION":
                            # Randomize provider
                            if "provider" in section_item:
                                provider_ids = section_item["provider"].get("identifiers", [])
                                for provider_id in provider_ids:
                                    provider_id["id"] = self._generate_random_identifier("P")
                            
                            # Randomize other participations
                            if "other_participations" in section_item:
                                for participation in section_item["other_participations"]:
                                    performer = participation.get("performer", {})
                                    if "name" in performer:
                                        performer["name"] = random.choice(performer_names)
                                    
                                    performer_ids = performer.get("identifiers", [])
                                    for performer_id in performer_ids:
                                        if performer_id.get("type") == "1.3.6.1.4.1.5734.1.2":
                                            performer_id["id"] = f"P{random.randint(10, 99)}L"
                                        elif performer_id.get("type") == "1.3.6.1.4.1.5734.1.3":
                                            performer_id["id"] = f"M{random.randint(10, 99)}L"
                                        elif performer_id.get("type") == "2.16.840.1.113883.4.292.10.2":
                                            performer_id["id"] = self._generate_random_identifier()
        
        return composition
    
    def _randomize_vaccine_data(self, composition: Dict[str, Any]) -> Dict[str, Any]:
        """Randomize vaccine-specific data."""
        # Different vaccine types and codes for variation
        vaccine_types = [
            {"name": "Meningocòccica C conjugada", "code": "MCC"},
            {"name": "Hepatitis B", "code": "HBV"},
            {"name": "Tètanus-Diftèria", "code": "TD"},
            {"name": "Grip", "code": "FLU"},
            {"name": "Pneumocòccica conjugada", "code": "PCV"}
        ]
        
        selected_vaccine = random.choice(vaccine_types)
        
        if "content" in composition:
            for content_item in composition["content"]:
                if content_item.get("_type") == "SECTION":
                    section_items = content_item.get("items", [])
                    for section_item in section_items:
                        if section_item.get("_type") == "ACTION":
                            # Update immunization item
                            description = section_item.get("description", {})
                            items = description.get("items", [])
                            for item in items:
                                if item.get("archetype_node_id") == "at0020":
                                    # Update vaccine name and code
                                    item["value"]["value"] = selected_vaccine["name"]
                                    item["value"]["defining_code"]["code_string"] = selected_vaccine["code"]
                                
                                # Update medication details
                                if item.get("archetype_node_id") == "openEHR-EHR-CLUSTER.medication.v2":
                                    cluster_items = item.get("items", [])
                                    for cluster_item in cluster_items:
                                        if cluster_item.get("archetype_node_id") == "at0132":
                                            # Update medication code
                                            cluster_item["value"]["defining_code"]["code_string"] = self._generate_random_identifier()
                                        
                                        # Update constituent details
                                        if cluster_item.get("archetype_node_id") == "openEHR-EHR-CLUSTER.medication.v2":
                                            const_items = cluster_item.get("items", [])
                                            for const_item in const_items:
                                                if const_item.get("archetype_node_id") == "at0132":
                                                    const_item["value"]["value"] = selected_vaccine["name"]
                                                    const_item["value"]["defining_code"]["code_string"] = selected_vaccine["code"]
        
        return composition
    
    def generate_synthetic_composition(self) -> Dict[str, Any]:
        """
        Generate a synthetic composition based on the loaded template.
        
        Returns:
            A dictionary representing a synthetic composition.
        """
        if not self.base_composition_template:
            raise ValueError("No base composition template loaded")
        
        # Deep copy the template to avoid modifying the original
        synthetic_composition = json.loads(json.dumps(self.base_composition_template))
        
        # Apply randomizations
        synthetic_composition = self._randomize_feeder_audit_ids(synthetic_composition)
        synthetic_composition = self._randomize_composition_dates(synthetic_composition)
        synthetic_composition = self._randomize_performer_data(synthetic_composition)
        synthetic_composition = self._randomize_vaccine_data(synthetic_composition)
        
        # Generate new UID for the composition (this will be overridden by the service)
        synthetic_composition["uid"] = {
            "_type": "OBJECT_VERSION_ID",
            "value": f"{uuid.uuid4()}::synthetic-server::1"
        }
        
        return synthetic_composition


async def generate_synthetic_data(
    db: AsyncIOMotorDatabase,
    base_composition: Dict[str, Any],
    count: int,
    config,
    flattener: CompositionFlattener,
    merge_search_docs: bool = False
) -> List[Dict[str, Any]]:
    """
    Generate synthetic EHR data with compositions based on a template.
    
    Args:
        db: Database connection
        base_composition: Base composition template to use for generation
        count: Number of synthetic records to generate
        config: CompositionCollectionNames configuration
        flattener: Composition flattener instance
        merge_search_docs: Whether to merge search documents
    
    Returns:
        List of created records with EHR and composition information
    
    Raises:
        HTTPException: If generation fails
    """
    
    generator = SyntheticDataGenerator()
    generator.load_base_composition(base_composition)
    
    created_records = []
    
    for i in range(count):
        try:
            # Step 1: Create a synthetic EHR with a subject
            subject_id = f"synthetic-patient-{uuid.uuid4()}"
            
            ehr_status = EHRStatusCreate(
                _type="EHR_STATUS",
                subject={
                    "_type": "PARTY_SELF",
                    "external_ref": {
                        "id": {"value": subject_id},
                        "namespace": "synthetic.data.namespace",
                        "type": "PERSON"
                    }
                },
                is_modifiable=True,
                is_queryable=True
            )
            
            # Create the EHR
            ehr_response = await create_ehr(
                db=db,
                initial_status=ehr_status,
                committer_name="SyntheticDataGenerator"
            )
            
            # Step 2: Generate a synthetic composition
            synthetic_composition_data = generator.generate_synthetic_composition()
            
            # Create CompositionCreate object
            composition_create = CompositionCreate(root=synthetic_composition_data)
            
            # Step 3: Add the composition to the EHR
            new_composition = await add_composition(
                ehr_id=ehr_response.ehr_id.value,
                composition_create=composition_create,
                db=db,
                config=config,
                flattener=flattener,
                merge_search_docs=merge_search_docs,
                committer_name="SyntheticDataGenerator"
            )
            
            created_record = {
                "record_number": i + 1,
                "ehr_id": ehr_response.ehr_id.value,
                "subject_id": subject_id,
                "composition_uid": new_composition.uid,
                "time_created": new_composition.time_created.isoformat()
            }
            
            created_records.append(created_record)
            
        except Exception as e:
            # If we encounter an error, we should still return what we've created so far
            # and include error information
            error_record = {
                "record_number": i + 1,
                "error": f"Failed to create record {i+1}: {str(e)}",
                "ehr_id": None,
                "subject_id": None,
                "composition_uid": None,
                "time_created": None
            }
            created_records.append(error_record)
            
            # Log the error but continue with next record
            print(f"Error creating synthetic record {i+1}: {e}")
    
    return created_records