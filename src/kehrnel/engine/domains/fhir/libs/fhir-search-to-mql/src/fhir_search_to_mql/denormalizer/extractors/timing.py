"""
Timing extractor for FHIR timing schedules.

Extracts timing information for schedules, appointments, and medication
administration. Reusable across resources — anywhere a Timing field is
denormalized (Observation.effectiveTiming, MedicationRequest.dosage,
ServiceRequest.occurrenceTiming, ...).

Supported outputs (selected via ``source_path`` / ``target_field``):
- ``event`` array
- ``repeat.boundsPeriod.start`` / ``.end``
- ``repeat.frequency``, ``repeat.period``, ``repeat.periodUnit``
- ``code`` (Timing.code.coding[*].code)
- **eventBounds** — chronological min/max of ``event[]`` projected as a
  Period ``{start, end}`` dict. This is the value that powers FHIR's
  ``date`` parameter when only a Timing is present on the resource.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class TimingExtractor(FieldExtractor):
    """Extract Timing FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Timing structure.
        
        Args:
            value: Timing or list of Timing structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted timing fields
        """
        result = {}
        timings = self._ensure_list(value)
        
        if not timings:
            return result
        
        events = []
        repeat_bounds_start = []
        repeat_bounds_end = []
        repeat_frequencies = []
        repeat_periods = []
        repeat_period_units = []
        codes = []
        
        for timing in timings:
            if not isinstance(timing, dict):
                continue
            
            # Extract event dates/times
            if 'event' in timing:
                timing_events = timing['event'] if isinstance(timing['event'], list) else [timing['event']]
                events.extend(timing_events)
            
            # Extract repeat information
            if 'repeat' in timing:
                repeat = timing['repeat']
                
                # Bounds (Period or Duration)
                if 'boundsPeriod' in repeat:
                    period = repeat['boundsPeriod']
                    if 'start' in period:
                        repeat_bounds_start.append(period['start'])
                    if 'end' in period:
                        repeat_bounds_end.append(period['end'])
                
                # Frequency
                if 'frequency' in repeat:
                    repeat_frequencies.append(repeat['frequency'])
                
                # Period and period unit
                if 'period' in repeat:
                    repeat_periods.append(repeat['period'])
                if 'periodUnit' in repeat:
                    repeat_period_units.append(repeat['periodUnit'])
            
            # Extract code (event timing code)
            if 'code' in timing:
                code = timing['code']
                if isinstance(code, dict) and 'coding' in code:
                    for coding in code['coding']:
                        if 'code' in coding:
                            codes.append(coding['code'])
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                
                if not target_field:
                    continue
                
                # Bounds-style output: any target whose name ends in
                # ``Bounds`` (e.g. ``effectiveTimingBounds``, ``eventBounds``)
                # gets a synthesised Period {start, end}. We try TWO
                # sources in priority order before giving up:
                #   1. ``event[]`` — chronological min/max of the
                #      explicit event list (ISO-8601 sorts
                #      lexicographically so a plain sort works).
                #   2. ``repeat.boundsPeriod`` — the explicit Period
                #      bound when no event[] is present (the common
                #      shape for an Observation whose effectiveTiming
                #      describes a recurring schedule via boundsPeriod
                #      rather than enumerating each occurrence).
                # Without (2), an Observation with effectiveTiming
                # populated only via repeat.boundsPeriod would fail
                # the `date` search parameter because
                # `_search.effectiveTimingBounds` was never written.
                wants_bounds = (
                    target_field.endswith('Bounds')
                    and ('event' in source_path or not source_path)
                )
                if wants_bounds:
                    bounds: Optional[Dict[str, Any]] = None
                    if events:
                        ordered = sorted(e for e in events if isinstance(e, str) and e)
                        if ordered:
                            bounds = {'start': ordered[0], 'end': ordered[-1]}
                    if bounds is None and (repeat_bounds_start or repeat_bounds_end):
                        bounds = {}
                        if repeat_bounds_start:
                            ordered_starts = sorted(
                                s for s in repeat_bounds_start
                                if isinstance(s, str) and s
                            )
                            if ordered_starts:
                                bounds['start'] = ordered_starts[0]
                        if repeat_bounds_end:
                            ordered_ends = sorted(
                                e for e in repeat_bounds_end
                                if isinstance(e, str) and e
                            )
                            if ordered_ends:
                                bounds['end'] = ordered_ends[-1]
                        if not bounds:
                            bounds = None
                    if bounds:
                        result[target_field] = bounds
                elif 'event' in source_path:
                    if events:
                        result[target_field] = events
                elif 'repeat.boundsPeriod.start' in source_path or 'boundsStart' in target_field:
                    if repeat_bounds_start:
                        result[target_field] = repeat_bounds_start
                elif 'repeat.boundsPeriod.end' in source_path or 'boundsEnd' in target_field:
                    if repeat_bounds_end:
                        result[target_field] = repeat_bounds_end
                elif 'repeat.frequency' in source_path:
                    if repeat_frequencies:
                        result[target_field] = repeat_frequencies
                elif 'repeat.period' in source_path and 'Unit' not in source_path:
                    if repeat_periods:
                        result[target_field] = repeat_periods
                elif 'repeat.periodUnit' in source_path:
                    if repeat_period_units:
                        result[target_field] = repeat_period_units
                elif 'code' in source_path:
                    if codes:
                        result[target_field] = codes
        else:
            # Default extraction without mappings
            if events:
                result['timingEvents'] = events
            if repeat_bounds_start:
                result['timingBoundsStart'] = repeat_bounds_start[0] if len(repeat_bounds_start) == 1 else repeat_bounds_start
            if repeat_bounds_end:
                result['timingBoundsEnd'] = repeat_bounds_end[0] if len(repeat_bounds_end) == 1 else repeat_bounds_end
            if repeat_frequencies:
                result['timingFrequencies'] = repeat_frequencies
            if repeat_periods:
                result['timingPeriods'] = repeat_periods
            if repeat_period_units:
                result['timingPeriodUnits'] = repeat_period_units
            if codes:
                result['timingCodes'] = codes
        
        return result
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
