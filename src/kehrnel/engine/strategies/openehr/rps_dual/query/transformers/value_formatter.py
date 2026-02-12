# src/kehrnel/engine/strategies/openehr/rps_dual/query/transformers/value_formatter.py
import re
import uuid
from datetime import datetime
from typing import Any
from bson import Binary


class ValueFormatter:
    """
    Handles conversion of values from AST format to MongoDB-appropriate formats.
    """

    @staticmethod
    def format_value(value: Any) -> Any:
        """Converts string value from AST to appropriate Python type for MongoDB."""
        if not isinstance(value, str):
            return value
        
        # Handle boolean string representations
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        
        # Handle numeric strings
        # Check for integer
        if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
            return int(value)
        
        # Check for float
        try:
            float_val = float(value)
            # Only return as float if it actually contains a decimal point
            # This prevents integers from being converted to floats
            if '.' in value:
                return float_val
        except ValueError:
            pass
        
        # Check if it's a date string and format it appropriately for MongoDB
        if ValueFormatter._is_iso_date_string(value):
            # Convert to MongoDB ISODate format but keep as string for the aggregation pipeline
            # The MongoDB driver will handle the actual conversion
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt
            except (ValueError, TypeError):
                return value
        
        # It's not a special type, return as string
        return value

    @staticmethod
    def format_ehr_id(ehr_id: str) -> Binary:
        """
        Converts string UUID to proper BSON Binary for MongoDB.
        """
        try:
            uuid_obj = uuid.UUID(ehr_id)
            return Binary.from_uuid(uuid_obj)
        except (ValueError, TypeError):
            # If conversion fails, raise an error since EHR IDs should be valid UUIDs
            raise ValueError(f"Invalid EHR ID format: {ehr_id}")

    @staticmethod
    def _is_iso_date_string(value: str) -> bool:
        """Check if a string looks like an ISO 8601 date."""
        if not isinstance(value, str):
            return False
        # Basic check for ISO format patterns
        iso_patterns = [
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # Basic ISO format
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}',  # With timezone
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',  # With Z timezone
        ]
        return any(re.match(pattern, value) for pattern in iso_patterns)
    
    @staticmethod
    def _is_uuid_string(value: str) -> bool:
        """Check if a string looks like a UUID."""
        if not isinstance(value, str):
            return False
        try:
            uuid.UUID(value)
            return True
        except (ValueError, TypeError):
            return False
