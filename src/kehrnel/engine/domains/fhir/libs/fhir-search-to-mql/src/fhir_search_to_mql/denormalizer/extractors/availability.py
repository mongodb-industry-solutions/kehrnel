"""
Availability extractor for FHIR availability schedules.

Extracts availability information including available times and days.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class AvailabilityExtractor(FieldExtractor):
    """Extract Availability FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Availability structure.
        
        Args:
            value: Availability or list of Availability structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted availability fields
        """
        result = {}
        availabilities = self._ensure_list(value)
        
        if not availabilities:
            return result
        
        days_of_week = []
        all_day = []
        available_start_times = []
        available_end_times = []
        
        for availability in availabilities:
            if not isinstance(availability, dict):
                continue
            
            # Extract days of week
            if 'daysOfWeek' in availability:
                days = availability['daysOfWeek']
                if isinstance(days, list):
                    days_of_week.extend(days)
                else:
                    days_of_week.append(days)
            
            # Extract allDay flag
            if 'allDay' in availability:
                all_day.append(availability['allDay'])
            
            # Extract available start time
            if 'availableStartTime' in availability:
                available_start_times.append(availability['availableStartTime'])
            
            # Extract available end time
            if 'availableEndTime' in availability:
                available_end_times.append(availability['availableEndTime'])
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                
                if not target_field:
                    continue
                
                # Map to appropriate extracted values
                if 'daysOfWeek' in source_path:
                    if days_of_week:
                        result[target_field] = list(set(days_of_week))  # Deduplicate
                elif 'allDay' in source_path:
                    if all_day:
                        result[target_field] = any(all_day)  # True if any are allDay
                elif 'availableStartTime' in source_path or 'startTime' in target_field:
                    if available_start_times:
                        result[target_field] = available_start_times
                elif 'availableEndTime' in source_path or 'endTime' in target_field:
                    if available_end_times:
                        result[target_field] = available_end_times
        else:
            # Default extraction without mappings
            if days_of_week:
                result['availabilityDaysOfWeek'] = list(set(days_of_week))
            if all_day:
                result['availabilityAllDay'] = any(all_day)
            if available_start_times:
                result['availabilityStartTime'] = available_start_times
            if available_end_times:
                result['availabilityEndTime'] = available_end_times
        
        return result
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
