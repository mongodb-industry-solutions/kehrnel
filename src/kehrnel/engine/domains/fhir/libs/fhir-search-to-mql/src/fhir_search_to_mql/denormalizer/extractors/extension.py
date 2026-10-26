"""
Extension extractor for FHIR extensions.

Extracts extension information for custom FHIR elements.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class ExtensionExtractor(FieldExtractor):
    """Extract Extension FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Extension structure.
        
        Args:
            value: Extension or list of Extension structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted extension fields
        """
        result = {}
        extensions = self._ensure_list(value)
        
        if not extensions:
            return result
        
        urls = []
        string_values = []
        integer_values = []
        boolean_values = []
        code_values = []
        uri_values = []
        extension_map = {}  # Map URL to values
        
        for ext in extensions:
            if not isinstance(ext, dict):
                continue
            
            url = ext.get('url')
            if url:
                urls.append(url)
            
            # Extract value based on type
            if 'valueString' in ext:
                value_str = ext['valueString']
                string_values.append(value_str)
                if url:
                    if url not in extension_map:
                        extension_map[url] = []
                    extension_map[url].append(value_str)
            
            elif 'valueInteger' in ext:
                value_int = ext['valueInteger']
                integer_values.append(value_int)
                if url:
                    if url not in extension_map:
                        extension_map[url] = []
                    extension_map[url].append(value_int)
            
            elif 'valueBoolean' in ext:
                value_bool = ext['valueBoolean']
                boolean_values.append(value_bool)
                if url:
                    if url not in extension_map:
                        extension_map[url] = []
                    extension_map[url].append(value_bool)
            
            elif 'valueCode' in ext:
                value_code = ext['valueCode']
                code_values.append(value_code)
                if url:
                    if url not in extension_map:
                        extension_map[url] = []
                    extension_map[url].append(value_code)
            
            elif 'valueUri' in ext:
                value_uri = ext['valueUri']
                uri_values.append(value_uri)
                if url:
                    if url not in extension_map:
                        extension_map[url] = []
                    extension_map[url].append(value_uri)
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                extension_url = mapping.get('extension_url')  # Specific extension URL to filter
                
                if not target_field:
                    continue
                
                # If specific extension URL is provided, extract values for that URL only
                if extension_url and extension_url in extension_map:
                    result[target_field] = extension_map[extension_url]
                elif 'url' in source_path:
                    if urls:
                        result[target_field] = urls
                elif 'valueString' in source_path:
                    if string_values:
                        result[target_field] = string_values
                elif 'valueInteger' in source_path:
                    if integer_values:
                        result[target_field] = integer_values
                elif 'valueBoolean' in source_path:
                    if boolean_values:
                        result[target_field] = boolean_values
                elif 'valueCode' in source_path:
                    if code_values:
                        result[target_field] = code_values
                elif 'valueUri' in source_path:
                    if uri_values:
                        result[target_field] = uri_values
        else:
            # Default extraction without mappings
            if urls:
                result['extensionUrls'] = urls
            if string_values:
                result['extensionStringValues'] = string_values
            if integer_values:
                result['extensionIntegerValues'] = integer_values
            if boolean_values:
                result['extensionBooleanValues'] = boolean_values
            if code_values:
                result['extensionCodeValues'] = code_values
            
            # Include extension map for URL-based lookups
            if extension_map:
                result['extensionsByUrl'] = extension_map
        
        return result
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
