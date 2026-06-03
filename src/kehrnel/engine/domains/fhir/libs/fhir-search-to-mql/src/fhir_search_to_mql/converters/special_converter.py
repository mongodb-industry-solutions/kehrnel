"""
Special parameters converter.

Handles FHIR special search parameters like _id, _lastUpdated, _tag, _has, etc.
These parameters have special meaning and behavior in FHIR searches.
"""

from typing import Dict, Any, Optional, List

from fhir_search_to_mql.converters.base_converter import BaseConverter
from fhir_search_to_mql.converters.multi_step_query import MultiStepQuery, create_simple_multi_step_query
from fhir_search_to_mql.core.exceptions import ConversionError


class SpecialConverter(BaseConverter):
    """
    Convert FHIR special parameters to MongoDB queries.
    
    Special parameters:
    - _id: Resource ID
    - _lastUpdated: Last modification time
    - _tag: Resource tag
    - _profile: Resource profile
    - _security: Security label
    - _has: Reverse chaining
    - _text: Full-text search in narrative
    - _content: Full-text search in entire resource
    
    Examples:
        # _id with single value
        converter.convert_id("123")
        → {"_id": "123"}
        
        # _id with multiple values
        converter.convert_id("123,456,789")
        → {"_id": {"$in": ["123", "456", "789"]}}
        
        # _lastUpdated with date prefix
        converter.convert_last_updated("ge2024-01-01")
        → {"meta.lastUpdated": {"$gte": datetime(2024, 1, 1)}}
        
        # _tag
        converter.convert_tag("http://terminology.org|code")
        → {"meta.tag": {"$elemMatch": {"system": "...", "code": "..."}}}
        
        # _has (reverse chaining)
        converter.convert_has("Observation:subject:code=8480-6")
        → MultiStepQuery(...)
        
        # _text (full-text search)
        converter.convert_text("diabetes")
        → {"$text": {"$search": "diabetes"}}
    """
    
    def convert(
        self,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        This method is not used for special parameters.
        Each special parameter has its own conversion method.
        
        Raises:
            NotImplementedError: Always, use specific convert_* methods
        """
        raise NotImplementedError(
            "Use specific conversion methods for special parameters: "
            "convert_id, convert_last_updated, convert_tag, etc."
        )
    
    @staticmethod
    def convert_id(value: str) -> Dict[str, Any]:
        """
        Convert _id parameter.
        
        Supports multiple IDs separated by commas.
        
        Args:
            value: ID or comma-separated list of IDs
            
        Returns:
            MongoDB query
            
        Examples:
            convert_id("123") → {"_id": "123"}
            convert_id("123,456") → {"_id": {"$in": ["123", "456"]}}
        """
        # Split by comma for multiple IDs
        ids = [id.strip() for id in value.split(',') if id.strip()]
        
        if len(ids) == 0:
            raise ConversionError("No valid IDs provided")
        elif len(ids) == 1:
            return {"_id": ids[0]}
        else:
            return {"_id": {"$in": ids}}
    
    @staticmethod
    def convert_last_updated(value: str, prefix: Optional[str] = None) -> Dict[str, Any]:
        """
        Convert _lastUpdated parameter.
        
        Uses date converter logic for meta.lastUpdated field.
        
        Args:
            value: Date/datetime value
            prefix: Optional date prefix (ge, le, etc.)
            
        Returns:
            MongoDB query
            
        Examples:
            convert_last_updated("2024-01-01") → Range query
            convert_last_updated("2024-01-01", "ge") → Greater than or equal
        """
        from fhir_search_to_mql.converters.date_converter import DateConverter
        
        # Create date converter with meta.lastUpdated field
        config = {
            'type': 'date',
            'fields': [{'field': 'meta.lastUpdated', 'type': 'date'}]
        }
        
        converter = DateConverter(config)
        return converter.convert(value, modifier=None, prefix=prefix)
    
    @staticmethod
    def convert_tag(value: str, modifier: Optional[str] = None) -> Dict[str, Any]:
        """
        Convert _tag parameter.
        
        Tags are stored in meta.tag array as Coding objects.
        Uses token converter logic with $elemMatch for array.
        
        Args:
            value: Tag value (system|code or code)
            modifier: Optional modifier (:not, :missing)
            
        Returns:
            MongoDB query
            
        Examples:
            convert_tag("http://terminology.org|tag1")
            → {"meta.tag": {"$elemMatch": {"system": "...", "code": "..."}}}
        """
        if modifier == 'missing':
            is_missing = value.lower() == 'true'
            if is_missing:
                return {"$or": [
                    {"meta.tag": {"$exists": False}},
                    {"meta.tag": []},
                    {"meta.tag": None}
                ]}
            else:
                return {
                    "meta.tag": {
                        "$exists": True,
                        "$ne": [],
                        "$ne": None
                    }
                }
        
        # Parse tag value (system|code format)
        if '|' in value:
            parts = value.split('|', 1)
            system = parts[0]
            code = parts[1] if len(parts) > 1 else ''
            
            if system and code:
                query = {
                    "meta.tag": {
                        "$elemMatch": {
                            "system": system,
                            "code": code
                        }
                    }
                }
            elif code:
                query = {
                    "meta.tag": {
                        "$elemMatch": {
                            "code": code
                        }
                    }
                }
            else:
                # System only
                query = {
                    "meta.tag": {
                        "$elemMatch": {
                            "system": system
                        }
                    }
                }
        else:
            # Code only
            query = {
                "meta.tag": {
                    "$elemMatch": {
                        "code": value
                    }
                }
            }
        
        # Apply :not modifier if present
        if modifier == 'not':
            return {"$nor": [query]}
        
        return query
    
    @staticmethod
    def convert_profile(value: str) -> Dict[str, Any]:
        """
        Convert _profile parameter.
        
        Profiles are stored in meta.profile array as URIs.
        
        Args:
            value: Profile URI
            
        Returns:
            MongoDB query
            
        Examples:
            convert_profile("http://hl7.org/fhir/StructureDefinition/Patient")
            → {"meta.profile": "..."}
        """
        # Support multiple profiles separated by comma
        profiles = [p.strip() for p in value.split(',') if p.strip()]
        
        if len(profiles) == 1:
            return {"meta.profile": profiles[0]}
        else:
            return {"meta.profile": {"$in": profiles}}
    
    @staticmethod
    def convert_security(value: str, modifier: Optional[str] = None) -> Dict[str, Any]:
        """
        Convert _security parameter.
        
        Security labels are stored in meta.security array as Coding objects.
        Uses same logic as _tag.
        
        Args:
            value: Security label value (system|code or code)
            modifier: Optional modifier (:not, :missing)
            
        Returns:
            MongoDB query
        """
        # Reuse tag conversion logic, just change field
        tag_query = SpecialConverter.convert_tag(value, modifier)
        
        # Replace meta.tag with meta.security
        security_query = {}
        for key, val in tag_query.items():
            if key == "meta.tag":
                security_query["meta.security"] = val
            elif key in ["$or", "$nor"]:
                # Update nested queries
                new_conditions = []
                for condition in val:
                    if "meta.tag" in condition:
                        new_conditions.append({"meta.security": condition["meta.tag"]})
                    else:
                        new_conditions.append(condition)
                security_query[key] = new_conditions
            else:
                security_query[key] = val
        
        return security_query
    
    @staticmethod
    def convert_has(value: str, base_resource_type: str) -> MultiStepQuery:
        """
        Convert _has parameter (reverse chaining).
        
        Format: _has:ResourceType:referenceParam:searchParam=value
        Example: _has:Observation:subject:code=8480-6
        
        This finds resources that are referenced by other resources matching criteria.
        
        Steps:
        1. Find Observation resources where code=8480-6
        2. Extract subject references from those Observations
        3. Query base resource (e.g., Patient) with those IDs
        
        Args:
            value: Reverse chain specification
            base_resource_type: Base resource type being queried
            
        Returns:
            MultiStepQuery object
            
        Raises:
            ConversionError: If format is invalid
        """
        # Parse _has format: ResourceType:referenceParam:searchParam=value
        parts = value.split(':')
        
        if len(parts) < 3:
            raise ConversionError(
                f"Invalid _has format. Expected 'ResourceType:referenceParam:searchParam=value', got '{value}'"
            )
        
        target_resource_type = parts[0]
        reference_param = parts[1]
        
        # Find searchParam=value part (may contain colons in value)
        search_part = ':'.join(parts[2:])
        
        if '=' not in search_part:
            raise ConversionError(
                f"Invalid _has format. Expected 'searchParam=value' in '{search_part}'"
            )
        
        search_param, search_value = search_part.split('=', 1)
        
        # Build query for target resource
        # This is simplified - in practice, would use appropriate converter
        target_query = {f"{search_param}": search_value}
        
        # Create multi-step query
        multi_step = MultiStepQuery(
            description=f"Reverse chain: Find {base_resource_type} referenced by {target_resource_type} where {search_param}={search_value}"
        )
        
        # Step 1: Query target resource
        multi_step.add_step(
            resource_type=target_resource_type,
            query=target_query,
            extract_field=f"{reference_param}.reference",
            description=f"Find {target_resource_type} where {search_param}={search_value}"
        )
        
        # Step 2: Build final query with extracted IDs
        multi_step.set_final_query_builder(
            lambda ids: {"_id": {"$in": ids}} if ids else {"_id": None}
        )
        
        return multi_step
    
    @staticmethod
    def convert_text(value: str) -> Dict[str, Any]:
        """
        Convert _text parameter for full-text search in narrative.
        
        Searches the text.div field using MongoDB text index.
        
        Args:
            value: Search text
            
        Returns:
            MongoDB query with $text operator
            
        Note:
            Requires text index on text.div field:
            db.collection.createIndex({"text.div": "text"})
        """
        return {
            "$text": {
                "$search": value,
                "$caseSensitive": False
            }
        }
    
    @staticmethod
    def convert_content(value: str) -> Dict[str, Any]:
        """
        Convert _content parameter for full-text search in entire resource.
        
        Searches the entire resource content using MongoDB text index.
        
        Args:
            value: Search text
            
        Returns:
            MongoDB query with $text operator
            
        Note:
            Requires text index on searchable fields or $** wildcard index:
            db.collection.createIndex({"$**": "text"})
        """
        return {
            "$text": {
                "$search": value,
                "$caseSensitive": False
            }
        }
