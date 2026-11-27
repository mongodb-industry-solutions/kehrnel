"""
Period extractor for FHIR period structures (date/time ranges).

Extracts:
- start: Start date/time
- end:   End date/time

Source modes
------------
* ``source: <field>`` — receives the field value (a Period dict or
  a list of Period dicts).
* ``source: $resource`` — receives the entire FHIR resource. Two
  sub-modes:
    1. **Synthetic-period**: ``start`` and ``end`` siblings live at
       the resource root (e.g. ``Appointment.start``/``Appointment.end``);
       we treat the resource itself as a single Period.
    2. **Path-resolved**: each ``field_mapping`` carries an explicit
       ``source_path`` (e.g. ``qualification[*].period``); the
       extractor walks that path against the resource and projects
       the resolved Period dicts. This lets one rule pull periods
       nested inside BackboneElements (Practitioner.qualification.period)
       without requiring the parent BackboneElement to be the
       extractor's input.

Sparse output
-------------
When a target's source value is missing (e.g. an Appointment with
``start`` set but no ``end``), the field is OMITTED from the result
rather than written as ``None``. This matches the pattern already
adopted by ``IdentifierExtractor`` and ``CodeableConceptExtractor``
and prevents downstream "expected string, got NoneType" validation
failures in :class:`ResourceDenormalizer`.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.path_resolver import (
    is_path_expression,
    looks_like_resource,
    resolve_path,
)


class PeriodExtractor(FieldExtractor):
    """Extract Period FHIR structure to searchable fields."""

    def extract(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Extract Period structure.

        Args:
            value: A Period dict, a list of Period dicts, or — when
                the rule was declared with ``source: $resource`` — the
                full FHIR resource (we read its top-level ``start``
                / ``end`` siblings as a synthetic Period, OR walk a
                path expression supplied by ``source_path`` to harvest
                nested Periods like ``qualification[*].period``).
            field_mappings: Per-field mapping configuration.

        Returns:
            Dictionary keyed by ``target_field``. Targets whose
            source is absent are NOT present in the dict (sparse).
        """
        # Resource-rooted + per-mapping path expression. Each mapping
        # may walk a different path (e.g. one rule for `subject.period`,
        # another for `effectivePeriod`) — handle them independently
        # so the cardinality of one path doesn't bleed into another.
        if (
            looks_like_resource(value)
            and field_mappings
            and any(is_path_expression(m.get("source_path")) for m in field_mappings)
        ):
            return self._extract_from_resource(value, field_mappings)

        result: Dict[str, Any] = {}
        periods = self._ensure_list(value)

        if not periods:
            return result

        starts: List[Any] = []
        ends: List[Any] = []

        for period in periods:
            if not isinstance(period, dict):
                continue
            # Period-shaped dicts AND resource-rooted dicts both
            # surface `start` / `end` as top-level keys, so the same
            # iteration logic works for both modes.
            if period.get('start') is not None:
                starts.append(period['start'])
            if period.get('end') is not None:
                ends.append(period['end'])

        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '') or ''
                datatype = mapping.get('datatype', 'string')

                if not target_field:
                    continue

                # ``array[object]`` (or ``object``) datatype on a Period
                # rule means "give me the whole Period object(s)" — the
                # canonical shape that powers FHIR's `period` search
                # parameter (e.g. Appointment.requestedPeriod). Project
                # each input period as a sparse `{start, end}` dict so
                # range queries like `_search.requestedPeriod.start: {$gte: ...}`
                # work after a single $unwind. Without this branch, the
                # historical behavior was to fall through to the
                # endswith('start') / endswith('end') router and write a
                # flat array of start (or end) strings — losing the end
                # boundary entirely and breaking period-overlap queries.
                lowered_dt = datatype.lower()
                if 'object' in lowered_dt:
                    period_objs: List[Dict[str, Any]] = []
                    for period in periods:
                        if not isinstance(period, dict):
                            continue
                        obj: Dict[str, Any] = {}
                        if period.get('start') is not None:
                            obj['start'] = period['start']
                        if period.get('end') is not None:
                            obj['end'] = period['end']
                        if obj:
                            period_objs.append(obj)
                    if 'array' in lowered_dt:
                        if period_objs:
                            result[target_field] = period_objs
                    else:
                        # Scalar object — write a single Period (using
                        # min(start)/max(end) when more than one was
                        # supplied). Keeps the legacy single-Period
                        # contract (`_search.appointmentPeriod`).
                        if len(period_objs) == 1:
                            result[target_field] = period_objs[0]
                        elif len(period_objs) > 1:
                            string_starts = sorted(
                                p['start'] for p in period_objs
                                if isinstance(p.get('start'), str)
                            )
                            string_ends = sorted(
                                p['end'] for p in period_objs
                                if isinstance(p.get('end'), str)
                            )
                            collapsed: Dict[str, Any] = {}
                            if string_starts:
                                collapsed['start'] = string_starts[0]
                            if string_ends:
                                collapsed['end'] = string_ends[-1]
                            if collapsed:
                                result[target_field] = collapsed
                    continue

                # String / scalar datatypes: route by source_path token.
                # Use endswith/equality rather than substring so 'start'
                # doesn't accidentally match a target like
                # 'extended_period.start' the wrong way; downstream
                # schemas can rely on the canonical tokens we set
                # ourselves.
                lowered = source_path.lower()
                if lowered.endswith('end'):
                    extracted = ends
                elif lowered.endswith('start') or lowered == '':
                    extracted = starts
                else:
                    # Default to start (preserves prior behavior for
                    # ambiguous source_paths).
                    extracted = starts

                if 'array' in lowered_dt:
                    if extracted:
                        result[target_field] = extracted
                    # Sparse: omit when nothing extracted.
                else:
                    if len(extracted) > 1:
                        # Singular target with multiple matches —
                        # surface the list rather than silently
                        # collapsing data; the validator will flag
                        # the cardinality mismatch in the config.
                        result[target_field] = extracted
                    elif len(extracted) == 1:
                        result[target_field] = extracted[0]
                    # else: sparse — omit entirely.
        else:
            if starts:
                result['start'] = starts[0] if len(starts) == 1 else starts
            if ends:
                result['end'] = ends[0] if len(ends) == 1 else ends

        return result

    # ------------------------------------------------------------------
    # `$resource` + path-resolved mode
    # ------------------------------------------------------------------
    def _extract_from_resource(
        self,
        resource: Dict[str, Any],
        field_mappings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Resolve each mapping's ``source_path`` against the full
        resource and project the resulting Period dicts.

        Mirrors the contract of :class:`IdentifierExtractor._extract_from_resource`:
        walk the path → flat list of leaf values → keep only Period-shaped
        dicts → project per ``datatype``. ``array[object]`` produces a
        flat ``[{start, end}, ...]`` array — exactly the shape needed
        for FHIR period range queries (the search-parameter side hits
        ``_search.<target>.start`` / ``.end``).
        """
        result: Dict[str, Any] = {}

        for mapping in field_mappings:
            target_field = mapping.get("target_field")
            source_path = (mapping.get("source_path") or "").strip()
            datatype = (mapping.get("datatype") or "string").lower()
            if not target_field or not source_path:
                continue

            # Walk the path. Discard non-dict leaves so a malformed
            # resource (e.g. a string accidentally landing in a Period
            # slot) doesn't crash the extractor.
            periods = [
                v for v in resolve_path(resource, source_path)
                if isinstance(v, dict)
            ]

            # `array[object]` projection — preserve ALL periods as
            # `{start, end}` objects so range queries can use the
            # `start` / `end` sub-fields directly.
            if "object" in datatype:
                period_objs: List[Dict[str, Any]] = []
                for period in periods:
                    obj: Dict[str, Any] = {}
                    if period.get("start") is not None:
                        obj["start"] = period["start"]
                    if period.get("end") is not None:
                        obj["end"] = period["end"]
                    if obj:
                        period_objs.append(obj)
                if "array" in datatype:
                    if period_objs:
                        result[target_field] = period_objs
                else:
                    if len(period_objs) == 1:
                        result[target_field] = period_objs[0]
                    elif len(period_objs) > 1:
                        # Single-object datatype with multiple Periods:
                        # collapse to min(start)/max(end) so the
                        # canonical scalar contract (one Period) is
                        # preserved without losing range coverage.
                        starts_str = sorted(
                            p["start"] for p in period_objs
                            if isinstance(p.get("start"), str)
                        )
                        ends_str = sorted(
                            p["end"] for p in period_objs
                            if isinstance(p.get("end"), str)
                        )
                        collapsed: Dict[str, Any] = {}
                        if starts_str:
                            collapsed["start"] = starts_str[0]
                        if ends_str:
                            collapsed["end"] = ends_str[-1]
                        if collapsed:
                            result[target_field] = collapsed
                continue

            # Scalar / string datatype: route by source_path token suffix.
            starts = [p["start"] for p in periods if p.get("start") is not None]
            ends = [p["end"] for p in periods if p.get("end") is not None]
            lowered_path = source_path.lower()
            if lowered_path.endswith(".end") or lowered_path.endswith("end"):
                extracted = ends
            else:
                extracted = starts

            if "array" in datatype:
                if extracted:
                    result[target_field] = extracted
            elif len(extracted) == 1:
                result[target_field] = extracted[0]
            elif len(extracted) > 1:
                result[target_field] = extracted
            # else: sparse — omit.

        return result
