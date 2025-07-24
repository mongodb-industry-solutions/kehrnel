#!/usr/bin/env python3
"""
JSON/FHIR Source Handler for OpenEHR Generator
Handles JSON and FHIR resource processing with JSONPath support
"""
from pathlib import Path
from typing import Any, Dict, List, Union, Optional
import json
import jsonpath_ng
from jsonpath_ng import parse

from openehr_generator_core import SourceHandler

class JSONFHIRHandler(SourceHandler):
    """Handler for JSON and FHIR documents"""
    
    def __init__(self):
        self.data = None
        self.is_fhir = False
    
    def can_handle(self, source_path: Path) -> bool:
        """Check if this handler can process the source file"""
        return source_path.suffix.lower() in ['.json', '.fhir']
    
    def load_source(self, source_path: Path) -> Dict:
        """Load the JSON source file"""
        with open(source_path, 'r', encoding='utf-8') as f:
            self.data = json.load(f)
            
        # Check if it's a FHIR resource
        if isinstance(self.data, dict):
            resource_type = self.data.get('resourceType')
            if resource_type:
                self.is_fhir = True
                
        return self.data
    
    def extract_value(self, source_data: Any, extraction_rule: Any) -> Any:
        """Extract a value from JSON using the extraction rule"""
        if self.data is None:
            return None
            
        if isinstance(extraction_rule, str):
            if extraction_rule.startswith("jsonpath:"):
                jsonpath = extraction_rule[9:].strip()
                return self._evaluate_jsonpath(jsonpath)
                
            elif extraction_rule.startswith("fhirpath:"):
                # For FHIR-specific paths
                fhirpath = extraction_rule[9:].strip()
                return self._evaluate_fhirpath(fhirpath)
                
            elif extraction_rule.startswith("constant:"):
                value = extraction_rule[9:].strip()
                if value.lower() == "true":
                    return True
                elif value.lower() == "false":
                    return False
                elif value == "":
                    return None
                return value
                
        elif isinstance(extraction_rule, dict):
            # Complex rules
            if "jsonpath" in extraction_rule:
                base_value = self._evaluate_jsonpath(extraction_rule["jsonpath"])
                
                # Apply mapping if present
                if "map" in extraction_rule and base_value is not None:
                    mapping = extraction_rule["map"]
                    mapped_value = mapping.get(str(base_value).strip().lower())
                    return mapped_value if mapped_value is not None else base_value
                    
                # Apply FHIR code system mapping
                if "fhir_system" in extraction_rule and base_value is not None:
                    return self._map_fhir_code(base_value, extraction_rule["fhir_system"])
                    
                return base_value
                
            # Handle FHIR-specific extractions
            if "fhir_reference" in extraction_rule:
                return self._resolve_fhir_reference(extraction_rule["fhir_reference"])
                
        return None
    
    def count_elements(self, source_data: Any, xpath_or_path: str) -> int:
        """Count elements matching the JSONPath"""
        if self.data is None:
            return 0
            
        if xpath_or_path.startswith("$"):
            # JSONPath
            jsonpath_expr = parse(xpath_or_path)
            matches = jsonpath_expr.find(self.data)
            return len(matches)
        elif self.is_fhir and xpath_or_path.startswith("entry"):
            # Special handling for FHIR Bundle entries
            if isinstance(self.data, dict) and self.data.get('resourceType') == 'Bundle':
                entries = self.data.get('entry', [])
                return len(entries)
                
        return 0
    
    def _evaluate_jsonpath(self, jsonpath: str) -> Any:
        """Evaluate a JSONPath expression"""
        try:
            jsonpath_expr = parse(jsonpath)
            matches = jsonpath_expr.find(self.data)
            
            if not matches:
                return None
            elif len(matches) == 1:
                return matches[0].value
            else:
                # Multiple matches - return list
                return [match.value for match in matches]
                
        except Exception as e:
            print(f"JSONPath evaluation error: {e}")
            return None
    
    def _evaluate_fhirpath(self, fhirpath: str) -> Any:
        """
        Evaluate a FHIRPath expression
        This is a simplified implementation - full FHIRPath would require a dedicated library
        """
        # Convert simple FHIRPath to JSONPath
        # Examples:
        # "Patient.name[0].given[0]" -> "$.name[0].given[0]"
        # "Bundle.entry.resource.where(resourceType='Patient')" -> more complex
        
        if self.is_fhir and self.data:
            # Remove resource type prefix if it matches
            resource_type = self.data.get('resourceType', '')
            if fhirpath.startswith(f"{resource_type}."):
                fhirpath = fhirpath[len(resource_type)+1:]
                
            # Convert to JSONPath
            jsonpath = "$." + fhirpath.replace(".", ".").replace("[", "[")
            return self._evaluate_jsonpath(jsonpath)
            
        return None
    
    def _map_fhir_code(self, code: str, system: str) -> str:
        """Map FHIR codes to OpenEHR terminology"""
        # This would contain mappings from FHIR code systems to OpenEHR
        # Example mappings
        fhir_to_openehr_map = {
            "http://loinc.org": {
                "8867-4": "at0004",  # Heart rate
                "8480-6": "at0005",  # Systolic BP
                # ... more mappings
            },
            "http://snomed.info/sct": {
                "386661006": "at0008",  # Fever
                # ... more mappings
            }
        }
        
        system_map = fhir_to_openehr_map.get(system, {})
        return system_map.get(code, code)
    
    def _resolve_fhir_reference(self, reference: str) -> Any:
        """Resolve a FHIR reference within the document"""
        # Simple reference resolution for contained resources
        if reference.startswith("#") and isinstance(self.data, dict):
            contained = self.data.get('contained', [])
            ref_id = reference[1:]  # Remove #
            
            for resource in contained:
                if resource.get('id') == ref_id:
                    return resource
                    
        return reference
    
    def preprocess_mapping(self, mapping: Dict, source_data: Any) -> Dict:
        """
        Preprocess mapping to handle FHIR arrays (e.g., Bundle entries)
        """
        if "_preprocessing" not in mapping:
            return mapping
            
        processed = dict(mapping)
        del processed["_preprocessing"]
        
        for directive in mapping["_preprocessing"]:
            if directive["type"] == "multiply_by_bundle_entries":
                # Handle FHIR Bundle entries
                if (isinstance(self.data, dict) and 
                    self.data.get('resourceType') == 'Bundle'):
                    
                    entries = self.data.get('entry', [])
                    entry_count = len(entries)
                    
                    # Filter by resource type if specified
                    resource_type_filter = directive.get('resource_type')
                    if resource_type_filter:
                        filtered_indices = []
                        for i, entry in enumerate(entries):
                            resource = entry.get('resource', {})
                            if resource.get('resourceType') == resource_type_filter:
                                filtered_indices.append(i)
                        entry_count = len(filtered_indices)
                    
                    # Expand mappings
                    expanded = {}
                    for key, rule in processed.items():
                        if "{entry}" in key:
                            for i in range(entry_count):
                                actual_index = filtered_indices[i] if resource_type_filter else i
                                new_key = key.replace("{entry}", str(i))
                                new_rule = self._replace_entry_placeholders(rule, actual_index)
                                expanded[new_key] = new_rule
                        else:
                            expanded[key] = rule
                            
                    return expanded
                    
            elif directive["type"] == "multiply_by_array":
                # Handle generic JSON arrays
                array_path = directive["array_path"]
                array = self._evaluate_jsonpath(array_path)
                
                if isinstance(array, list):
                    expanded = {}
                    for key, rule in processed.items():
                        if "{index}" in key:
                            for i in range(len(array)):
                                new_key = key.replace("{index}", str(i))
                                new_rule = self._replace_index_placeholders(rule, i)
                                expanded[new_key] = new_rule
                        else:
                            expanded[key] = rule
                            
                    return expanded
                    
        return processed
    
    def _replace_entry_placeholders(self, value: Any, entry_idx: int) -> Any:
        """Replace entry placeholders in values"""
        if isinstance(value, str):
            result = value.replace("{entry}", str(entry_idx))
            # Also replace in JSONPath expressions
            if "$.entry[{entry}]" in result:
                result = result.replace("$.entry[{entry}]", f"$.entry[{entry_idx}]")
            return result
        elif isinstance(value, dict):
            return {k: self._replace_entry_placeholders(v, entry_idx) for k, v in value.items()}
        else:
            return value
    
    def _replace_index_placeholders(self, value: Any, index: int) -> Any:
        """Replace generic index placeholders"""
        if isinstance(value, str):
            return value.replace("{index}", str(index))
        elif isinstance(value, dict):
            return {k: self._replace_index_placeholders(v, index) for k, v in value.items()}
        else:
            return value