"""
Date parameter converter.

Converts FHIR date/datetime searches to MongoDB queries using prefixes:
- eq (equals)
- ne (not equals)
- ge, gt (greater than or equal/greater than)
- le, lt (less than or equal/less than)
- sa (starts after)
- eb (ends before)
- ap (approximately)
"""

from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from dateutil import parser as date_parser

from fhir_search_to_mql.converters.base_converter import BaseConverter
from fhir_search_to_mql.core.exceptions import ConversionError
from fhir_search_to_mql.core.constants import PREFIXES


class DateConverter(BaseConverter):
    """
    Convert FHIR date parameters to MongoDB queries.
    
    Handles:
    - Dates: 2024-01-01
    - DateTimes: 2024-01-01T10:30:00Z
    - Partial dates: 2024, 2024-01
    - Date ranges with prefixes: ge2024-01-01
    - Period queries (start/end)
    """
    
    # Allowed prefixes for date searches
    ALLOWED_PREFIXES = ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'sa', 'eb', 'ap']
    
    def convert(
        self,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert date parameter to MongoDB query.
        
        Args:
            value: Date/datetime string
            modifier: Optional modifier (:missing)
            prefix: Optional prefix (eq, ne, ge, gt, le, lt, sa, eb, ap)
            
        Returns:
            MongoDB query dictionary
            
        Raises:
            ConversionError: If conversion fails
        """
        # Handle :missing modifier
        if modifier == 'missing':
            return self._handle_missing(value)
        
        # Validate prefix
        self._validate_prefix(prefix, self.ALLOWED_PREFIXES)
        
        # Parse date value
        try:
            date_range = self._parse_date_range(value)
        except Exception as e:
            raise ConversionError(f"Invalid date format '{value}': {str(e)}")
        
        # Get fields to query
        fields = self._get_fields_for_modifier(modifier)
        
        if not fields:
            raise ConversionError("No fields configured for date parameter")
        
        # Build query for each field
        field_queries = []
        
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            field_type = field_config.get('type', 'date') if isinstance(field_config, dict) else 'date'
            
            if field_type == 'period':
                # Period field (has start and end)
                query = self._build_period_query(
                    field_name, 
                    date_range, 
                    prefix or 'eq'
                )
            else:
                # Single date/datetime field
                query = self._build_date_query(
                    field_name, 
                    date_range, 
                    prefix or 'eq'
                )
            
            field_queries.append(query)
        
        # Combine with OR if multiple fields
        return self._create_or_query(field_queries)
    
    def _parse_date_range(self, value: str) -> Dict[str, datetime]:
        """
        Parse a date string and return a range.
        
        FHIR dates have implicit ranges:
        - 2024 -> 2024-01-01 to 2024-12-31T23:59:59
        - 2024-03 -> 2024-03-01 to 2024-03-31T23:59:59
        - 2024-03-15 -> 2024-03-15T00:00:00 to 2024-03-15T23:59:59
        
        Args:
            value: Date string
            
        Returns:
            Dictionary with 'start' and 'end' datetime objects
        """
        # Parse the date
        try:
            parsed_date = date_parser.parse(value)
        except Exception as e:
            raise ConversionError(f"Cannot parse date '{value}': {str(e)}")
        
        # Determine precision and calculate range
        if len(value) == 4:  # Year only
            start = datetime(parsed_date.year, 1, 1)
            end = datetime(parsed_date.year, 12, 31, 23, 59, 59, 999999)
        elif len(value) == 7:  # Year-month
            start = datetime(parsed_date.year, parsed_date.month, 1)
            # Last day of month
            if parsed_date.month == 12:
                end = datetime(parsed_date.year, 12, 31, 23, 59, 59, 999999)
            else:
                next_month = datetime(parsed_date.year, parsed_date.month + 1, 1)
                end = next_month - timedelta(microseconds=1)
        elif 'T' in value:  # DateTime with time
            start = parsed_date
            end = parsed_date
        else:  # Full date without time
            start = datetime(parsed_date.year, parsed_date.month, parsed_date.day)
            end = datetime(parsed_date.year, parsed_date.month, parsed_date.day, 23, 59, 59, 999999)
        
        return {'start': start, 'end': end}
    
    def _build_date_query(
        self, 
        field_name: str, 
        date_range: Dict[str, datetime], 
        prefix: str
    ) -> Dict[str, Any]:
        """
        Build MongoDB query for a single date field.
        
        Args:
            field_name: Field to query
            date_range: Date range dict
            prefix: Comparison prefix
            
        Returns:
            MongoDB query
        """
        start = date_range['start']
        end = date_range['end']
        
        if prefix == 'eq':
            # Date equals (overlaps with range)
            return {
                "$and": [
                    {field_name: {"$gte": start}},
                    {field_name: {"$lte": end}}
                ]
            }
        elif prefix == 'ne':
            # Date not equals
            return {
                "$or": [
                    {field_name: {"$lt": start}},
                    {field_name: {"$gt": end}}
                ]
            }
        elif prefix == 'gt':
            # Greater than (after end of range)
            return {field_name: {"$gt": end}}
        elif prefix == 'ge':
            # Greater than or equal (at or after start)
            return {field_name: {"$gte": start}}
        elif prefix == 'lt':
            # Less than (before start of range)
            return {field_name: {"$lt": start}}
        elif prefix == 'le':
            # Less than or equal (at or before end)
            return {field_name: {"$lte": end}}
        elif prefix == 'sa':
            # Starts after
            return {field_name: {"$gt": end}}
        elif prefix == 'eb':
            # Ends before
            return {field_name: {"$lt": start}}
        elif prefix == 'ap':
            # Approximately (within 10% of value)
            # For dates, use +/- 1 day
            approx_start = start - timedelta(days=1)
            approx_end = end + timedelta(days=1)
            return {
                "$and": [
                    {field_name: {"$gte": approx_start}},
                    {field_name: {"$lte": approx_end}}
                ]
            }
        else:
            return {field_name: {"$gte": start, "$lte": end}}
    
    def _build_period_query(
        self, 
        field_prefix: str, 
        date_range: Dict[str, datetime], 
        prefix: str
    ) -> Dict[str, Any]:
        """
        Build MongoDB query for a Period field (with start and end).
        
        Args:
            field_prefix: Field prefix (e.g., "_search.period")
            date_range: Date range dict
            prefix: Comparison prefix
            
        Returns:
            MongoDB query
        """
        start_field = f"{field_prefix}.start"
        end_field = f"{field_prefix}.end"
        search_start = date_range['start']
        search_end = date_range['end']
        
        if prefix == 'eq':
            # Period overlaps with search range
            return {
                "$or": [
                    # Period start is within range
                    {"$and": [
                        {start_field: {"$gte": search_start}},
                        {start_field: {"$lte": search_end}}
                    ]},
                    # Period end is within range
                    {"$and": [
                        {end_field: {"$gte": search_start}},
                        {end_field: {"$lte": search_end}}
                    ]},
                    # Period encompasses range
                    {"$and": [
                        {start_field: {"$lte": search_start}},
                        {end_field: {"$gte": search_end}}
                    ]}
                ]
            }
        elif prefix in ['gt', 'sa']:
            # Period starts after
            return {start_field: {"$gt": search_end}}
        elif prefix in ['lt', 'eb']:
            # Period ends before
            return {end_field: {"$lt": search_start}}
        elif prefix == 'ge':
            # Period starts at or after
            return {start_field: {"$gte": search_start}}
        elif prefix == 'le':
            # Period ends at or before
            return {end_field: {"$lte": search_end}}
        else:
            # Default to overlap
            return {
                "$and": [
                    {start_field: {"$lte": search_end}},
                    {end_field: {"$gte": search_start}}
                ]
            }
    
    def _handle_missing(self, value: str) -> Dict[str, Any]:
        """Handle :missing modifier."""
        is_missing = value.lower() == 'true'
        
        fields = self._get_fields_for_modifier(None)
        field_name = fields[0].get('field') if isinstance(fields[0], dict) else fields[0]
        
        if is_missing:
            return {"$or": [
                {field_name: {"$exists": False}},
                {field_name: None}
            ]}
        else:
            return {field_name: {"$exists": True, "$ne": None}}
