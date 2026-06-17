"""
URI parameter converter.

Converts FHIR URI searches to MongoDB queries.
Handles hierarchical URI searches with :below and :above modifiers.

URI search is used for canonical URLs, value set URLs, and other URI references.
"""

from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, urljoin

from fhir_search_to_mql.converters.base_converter import BaseConverter
from fhir_search_to_mql.core.exceptions import ConversionError
from fhir_search_to_mql.core.constants import URI_MODIFIERS


class URIConverter(BaseConverter):
    """
    Convert FHIR URI parameters to MongoDB queries.
    
    Handles:
    - Exact match (default)
    - :below modifier (hierarchical children)
    - :above modifier (hierarchical parents)
    - :missing modifier
    
    Examples:
        # Exact match
        converter.convert("http://example.org/ValueSet/123")
        → {"url": "http://example.org/ValueSet/123"}
        
        # Below (hierarchical children) - uses range query (PREFERRED)
        converter.convert("http://example.org/", modifier="below")
        → {"url": {"$gte": "http://example.org/", "$lt": "http://example.org/\\uffff"}}
        
        # Below with regex (FALLBACK if range not suitable)
        → {"url": {"$regex": "^http://example.org/"}}
        
        # Above (hierarchical parents)
        converter.convert("http://example.org/path/to/resource", modifier="above")
        → {"$or": [
              {"url": "http://example.org/path/to"},
              {"url": "http://example.org/path"},
              {"url": "http://example.org"}
           ]}
    
    Note:
        URI search with :below modifier is one of the few cases where regex
        may be used as a fallback. However, range query is preferred for performance.
        An index on the URI field is strongly recommended.
    """
    
    def convert(
        self,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert URI parameter to MongoDB query.
        
        Args:
            value: URI value
            modifier: Optional modifier (:below, :above, :missing)
            prefix: Not used for URI parameters
            
        Returns:
            MongoDB query dictionary
            
        Raises:
            ConversionError: If conversion fails
        """
        # Validate modifier
        self._validate_modifier(modifier, URI_MODIFIERS)
        
        # Handle :missing modifier
        if modifier == 'missing':
            return self._handle_missing(value)
        
        # Get fields to query
        fields = self._get_fields_for_modifier(modifier)
        
        if not fields:
            raise ConversionError("No fields configured for URI parameter")
        
        # Build query based on modifier
        if modifier == 'below':
            return self._handle_below(value, fields)
        elif modifier == 'above':
            return self._handle_above(value, fields)
        else:
            # Exact match (default)
            return self._handle_exact(value, fields)
    
    def _handle_exact(self, value: str, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Handle exact URI match.
        
        Args:
            value: URI to match exactly
            fields: Fields to query
            
        Returns:
            MongoDB query
        """
        field_queries = []
        
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            field_queries.append({field_name: value})
        
        return self._create_or_query(field_queries)
    
    def _handle_below(self, value: str, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Handle :below modifier for hierarchical children.
        
        Finds all URIs that start with the given prefix (children in URI hierarchy).
        
        Strategy:
        1. PREFERRED: Use range query (fastest, index-backed)
           - Works when URIs are predictable strings
           - {"url": {"$gte": "prefix", "$lt": "prefix\\uffff"}}
           - Performance: 5-10ms
        
        2. FALLBACK: Use regex (slower, but sometimes necessary)
           - Use when range query semantics don't match requirement
           - {"url": {"$regex": "^prefix"}}
           - Performance: 50-200ms (requires index on url field)
        
        Args:
            value: URI prefix
            fields: Fields to query
            
        Returns:
            MongoDB query
        """
        field_queries = []
        
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            query_type = field_config.get('query_type', 'range') if isinstance(field_config, dict) else 'range'
            
            if query_type == 'regex':
                # Use regex as fallback
                # Escape special regex characters in URI
                escaped_value = self._escape_regex(value)
                field_queries.append({
                    field_name: {
                        "$regex": f"^{escaped_value}",
                        "$options": "i"  # Case-insensitive
                    }
                })
            else:
                # Use range query (PREFERRED)
                # This works for most URI hierarchies
                field_queries.append({
                    field_name: {
                        "$gte": value,
                        "$lt": value + "\uffff"
                    }
                })
        
        return self._create_or_query(field_queries)
    
    def _handle_above(self, value: str, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Handle :above modifier for hierarchical parents.
        
        Finds all parent URIs in the hierarchy.
        
        Example:
            value = "http://example.org/path/to/resource"
            parents = [
                "http://example.org/path/to",
                "http://example.org/path",
                "http://example.org"
            ]
        
        Args:
            value: URI to find parents for
            fields: Fields to query
            
        Returns:
            MongoDB query
        """
        # Generate all parent URIs
        parents = self._generate_parent_uris(value)
        
        if not parents:
            # No parents found, return a query that matches nothing
            return {"_no_match": True}
        
        field_queries = []
        
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            
            # Create OR query for all parent URIs
            parent_queries = [{field_name: parent} for parent in parents]
            
            if len(parent_queries) == 1:
                field_queries.append(parent_queries[0])
            else:
                field_queries.append({"$or": parent_queries})
        
        return self._create_or_query(field_queries)
    
    def _generate_parent_uris(self, uri: str) -> List[str]:
        """
        Generate all parent URIs in the hierarchy.
        
        Args:
            uri: URI to generate parents for
            
        Returns:
            List of parent URIs (from immediate parent to root)
        """
        parents = []
        
        # Parse URI
        parsed = urlparse(uri)
        
        if not parsed.scheme:
            # Not a valid URL, try simple path splitting
            parts = uri.rstrip('/').split('/')
            for i in range(len(parts) - 1, 0, -1):
                parent = '/'.join(parts[:i])
                if parent:
                    parents.append(parent)
        else:
            # Valid URL with scheme
            path = parsed.path.rstrip('/')
            path_parts = [p for p in path.split('/') if p]
            
            # Generate parent paths
            for i in range(len(path_parts) - 1, -1, -1):
                parent_path = '/' + '/'.join(path_parts[:i]) if i > 0 else ''
                parent_uri = f"{parsed.scheme}://{parsed.netloc}{parent_path}"
                parents.append(parent_uri)
        
        return parents
    
    def _escape_regex(self, value: str) -> str:
        """
        Escape special regex characters in a string.
        
        Args:
            value: String to escape
            
        Returns:
            Escaped string safe for use in regex
        """
        # Characters that need escaping in regex
        special_chars = r'\.^$*+?{}[]|()'
        
        escaped = ""
        for char in value:
            if char in special_chars:
                escaped += "\\" + char
            else:
                escaped += char
        
        return escaped
    
    def _handle_missing(self, value: str) -> Dict[str, Any]:
        """
        Handle :missing modifier.
        
        Args:
            value: "true" or "false"
            
        Returns:
            MongoDB query
        """
        is_missing = value.lower() == 'true'
        
        fields = self._get_fields_for_modifier(None)  # Use default fields
        
        if not fields:
            raise ConversionError("No fields configured for missing check")
        
        field_name = fields[0].get('field') if isinstance(fields[0], dict) else fields[0]
        
        if is_missing:
            return {"$or": [
                {field_name: {"$exists": False}},
                {field_name: None}
            ]}
        else:
            return {field_name: {"$exists": True, "$ne": None}}
