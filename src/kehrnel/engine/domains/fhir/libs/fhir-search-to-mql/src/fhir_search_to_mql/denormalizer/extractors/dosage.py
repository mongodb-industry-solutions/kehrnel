"""
Dosage extractor for medication dosage instructions.

Extracts dosage information including route, timing, and quantity.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class DosageExtractor(FieldExtractor):
    """Extract Dosage FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Dosage structure.
        
        Args:
            value: Dosage or list of Dosage structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted dosage fields
        """
        result = {}
        dosages = self._ensure_list(value)
        
        if not dosages:
            return result
        
        texts = []
        routes = []
        route_codes = []
        methods = []
        method_codes = []
        timing_events = []
        dose_values = []
        dose_units = []
        rate_values = []
        rate_units = []
        
        for dosage in dosages:
            if not isinstance(dosage, dict):
                continue
            
            # Extract text
            if 'text' in dosage:
                texts.append(dosage['text'])
            
            # Extract route
            if 'route' in dosage:
                route = dosage['route']
                if isinstance(route, dict):
                    if 'text' in route:
                        routes.append(route['text'])
                    if 'coding' in route:
                        for coding in route['coding']:
                            if 'code' in coding:
                                route_codes.append(coding['code'])
            
            # Extract method
            if 'method' in dosage:
                method = dosage['method']
                if isinstance(method, dict):
                    if 'text' in method:
                        methods.append(method['text'])
                    if 'coding' in method:
                        for coding in method['coding']:
                            if 'code' in coding:
                                method_codes.append(coding['code'])
            
            # Extract timing
            if 'timing' in dosage:
                timing = dosage['timing']
                if isinstance(timing, dict) and 'event' in timing:
                    events = timing['event'] if isinstance(timing['event'], list) else [timing['event']]
                    timing_events.extend(events)
            
            # Extract dose quantity
            if 'doseQuantity' in dosage:
                dose_qty = dosage['doseQuantity']
                if isinstance(dose_qty, dict):
                    if 'value' in dose_qty:
                        dose_values.append(dose_qty['value'])
                    if 'unit' in dose_qty:
                        dose_units.append(dose_qty['unit'])
            
            # Extract rate quantity
            if 'rateQuantity' in dosage:
                rate_qty = dosage['rateQuantity']
                if isinstance(rate_qty, dict):
                    if 'value' in rate_qty:
                        rate_values.append(rate_qty['value'])
                    if 'unit' in rate_qty:
                        rate_units.append(rate_qty['unit'])
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                
                if not target_field:
                    continue
                
                # Map to appropriate extracted values
                if 'text' in source_path and 'route' not in source_path and 'method' not in source_path:
                    if texts:
                        result[target_field] = texts
                elif 'route.text' in source_path or ('route' in target_field and 'code' not in target_field):
                    if routes:
                        result[target_field] = routes
                elif 'route.coding.code' in source_path or ('route' in target_field and 'code' in target_field):
                    if route_codes:
                        result[target_field] = route_codes
                elif 'method.text' in source_path or ('method' in target_field and 'code' not in target_field):
                    if methods:
                        result[target_field] = methods
                elif 'method.coding.code' in source_path or ('method' in target_field and 'code' in target_field):
                    if method_codes:
                        result[target_field] = method_codes
                elif 'timing.event' in source_path:
                    if timing_events:
                        result[target_field] = timing_events
                elif 'doseQuantity.value' in source_path or ('dose' in target_field and 'value' in target_field):
                    if dose_values:
                        result[target_field] = dose_values
                elif 'doseQuantity.unit' in source_path or ('dose' in target_field and 'unit' in target_field):
                    if dose_units:
                        result[target_field] = dose_units
                elif 'rateQuantity.value' in source_path or ('rate' in target_field and 'value' in target_field):
                    if rate_values:
                        result[target_field] = rate_values
                elif 'rateQuantity.unit' in source_path or ('rate' in target_field and 'unit' in target_field):
                    if rate_units:
                        result[target_field] = rate_units
        else:
            # Default extraction without mappings
            if texts:
                result['dosageText'] = texts
            if routes:
                result['dosageRoute'] = routes
            if route_codes:
                result['dosageRouteCodes'] = route_codes
            if methods:
                result['dosageMethod'] = methods
            if method_codes:
                result['dosageMethodCodes'] = method_codes
            if timing_events:
                result['dosageTimingEvents'] = timing_events
            if dose_values:
                result['dosageDoseValue'] = dose_values
            if dose_units:
                result['dosageDoseUnit'] = dose_units
        
        return result
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
