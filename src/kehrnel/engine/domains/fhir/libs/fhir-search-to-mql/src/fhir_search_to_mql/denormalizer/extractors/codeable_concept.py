"""
CodeableConcept extractor for FHIR code structures.

Extracts:
- codes: Array of all code values
- systems: Array of all system URIs
- systemValues: Array of "system|code" pairs
- text: Text representation

Two invocation modes:

1. **Pre-resolved** (default): the denormalizer hands the extractor a
   CodeableConcept (or array of them) already navigated via a top-level
   field on the resource. ``source_path`` is treated as a hint indicating
   which sub-array of the flattened result the mapping wants.

2. **Resource-rooted** (``source: $resource``): the extractor receives the
   full FHIR resource and uses ``source_path`` as an actual path
   expression (see :mod:`fhir_search_to_mql.denormalizer.path_resolver`)
   evaluated against the resource. This is what powers the cross-cutting
   composite parameters (e.g. Observation's ``combo-code`` aggregating
   ``code`` ∪ ``component[*].code``) without resorting to a
   resource-specific extractor.

The two modes share the same flatten-codings core so behavior, ordering,
and field semantics are identical.
"""

from typing import Any, Dict, List, Optional, Tuple

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.path_resolver import (
    looks_like_resource,
    resolve_path,
)


class CodeableConceptExtractor(FieldExtractor):
    """Extract CodeableConcept FHIR structure to searchable fields."""

    def extract(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract CodeableConcept structure.

        Args:
            value: CodeableConcept, list of CodeableConcept, or full FHIR
                resource (with ``resourceType``). Resource-mode requires
                ``field_mappings`` to declare paths via ``source_path``.
            field_mappings: Field mapping configuration

        Returns:
            Dictionary with extracted code fields
        """
        if looks_like_resource(value) and field_mappings:
            return self._extract_from_resource(value, field_mappings)
        return self._extract_from_concepts(value, field_mappings)

    # ------------------------------------------------------------------
    # Resource-rooted mode: walk source_path against the full resource.
    # ------------------------------------------------------------------
    def _extract_from_resource(
        self,
        resource: Dict[str, Any],
        field_mappings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Re-resolve a (potentially union'd) path per mapping."""
        result: Dict[str, Any] = {}
        for mapping in field_mappings:
            target_field = mapping.get("target_field")
            source_path = mapping.get("source_path") or ""
            if not target_field or not source_path:
                continue

            # Resolve the path; the resolver handles `|` unions and `[*]`.
            # The target of the resolved path may be a CodeableConcept, a
            # Coding, or a string (for `.code` / `.system` short-paths) —
            # `_collect_codings` normalizes all three into a flat list of
            # Coding-like dicts (or strings for primitive paths).
            resolved = resolve_path(resource, source_path)
            codes, systems, system_values, displays, texts = self._collect_codings(resolved)
            self._assign(
                result,
                mapping,
                codes=codes,
                systems=systems,
                system_values=system_values,
                displays=displays,
                texts=texts,
            )
        return result

    @staticmethod
    def _collect_codings(items: List[Any]) -> Tuple[List[str], List[str], List[str], List[str], List[str]]:
        """
        Reduce a heterogeneous list of CodeableConcept / Coding dicts and
        primitive code strings into the standard (codes, systems,
        system|code pairs, displays, texts) tuple used by ``_assign``.
        """
        codes: List[str] = []
        systems: List[str] = []
        system_values: List[str] = []
        displays: List[str] = []
        texts: List[str] = []

        def _consume_coding(coding: Any) -> None:
            if not isinstance(coding, dict):
                if isinstance(coding, str) and coding:
                    codes.append(coding)
                    system_values.append(f"|{coding}")
                return
            code = coding.get("code")
            system = coding.get("system")
            display = coding.get("display")
            if code:
                codes.append(code)
            if system:
                systems.append(system)
            if display:
                displays.append(display)
            if system and code:
                system_values.append(f"{system}|{code}")
            elif code:
                system_values.append(f"|{code}")

        for item in items:
            if isinstance(item, dict) and "coding" in item:
                # CodeableConcept
                codings = item.get("coding") or []
                if not isinstance(codings, list):
                    codings = [codings]
                for c in codings:
                    _consume_coding(c)
                if isinstance(item.get("text"), str):
                    texts.append(item["text"])
            elif isinstance(item, dict) and ("code" in item or "system" in item):
                # Bare Coding
                _consume_coding(item)
            elif isinstance(item, str):
                # Primitive path target like `code.coding[*].code`.
                codes.append(item)
                system_values.append(f"|{item}")

        return codes, systems, system_values, displays, texts

    # ------------------------------------------------------------------
    # Pre-resolved mode: behave exactly like the legacy extractor.
    # ------------------------------------------------------------------
    def _extract_from_concepts(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        concepts = self._ensure_list(value)
        if not concepts:
            return result

        codes: List[str] = []
        systems: List[str] = []
        system_values: List[str] = []
        texts: List[str] = []
        displays: List[str] = []

        for concept in concepts:
            if not isinstance(concept, dict):
                continue
            codings = concept.get("coding", [])
            if not isinstance(codings, list):
                codings = [codings]
            for coding in codings:
                if not isinstance(coding, dict):
                    continue
                code = coding.get("code")
                system = coding.get("system")
                display = coding.get("display")
                if code:
                    codes.append(code)
                if system:
                    systems.append(system)
                if display:
                    displays.append(display)
                if system and code:
                    system_values.append(f"{system}|{code}")
                elif code:
                    system_values.append(f"|{code}")
            if "text" in concept:
                texts.append(concept["text"])

        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get("target_field")
                source_path = mapping.get("source_path", "")
                if not target_field:
                    continue
                # Target-field-first projection: an explicit field name
                # is the most reliable signal of caller intent. Source-
                # path heuristics are kept as fallbacks for legacy rules
                # that don't follow the convention. Without this
                # ordering, hints like ``coding[*]`` accidentally match
                # the ``"code" in source_path`` test (because "coding"
                # CONTAINS "code") and silently mis-route every
                # ``systemCode`` rule to the codes list.
                #
                # The target-field check is done on the lowercased name
                # so both naming conventions work: ``code_systemCode``
                # (camelCase, used by Patient/Observation/Appointment/
                # Organization configs) AND ``system_codes`` (snake-case
                # variant used in some unit-test fixtures) are routed
                # to the same ``system|code`` projection.
                target_lower = target_field.lower()
                if "system" in target_lower and (
                    "code" in target_lower or "value" in target_lower
                ):
                    values = system_values
                elif "system" in target_lower:
                    values = systems
                elif "display" in target_lower:
                    values = displays
                # `text` must beat `code` here even when the target
                # name contains both — e.g. `code_text_lower` is the
                # `:text` modifier projection (display + text), NOT
                # the `code_codes` projection. The check accepts any
                # `_text` segment (suffix `_text`, infix `_text_`,
                # exact `text`) so naming conventions like
                # `code_text`, `code_text_lower`, `value_text`, etc.
                # all route to the texts list.
                elif (
                    target_lower.endswith("_text")
                    or target_lower == "text"
                    or "_text_" in target_lower
                ):
                    values = texts
                elif "code" in target_lower:
                    values = codes
                # Fallback: source-path heuristics for rules that name
                # the target field generically (e.g. Patient's
                # ``language`` mapping for ``communication.language``).
                elif "code" in source_path and "system" not in source_path:
                    values = codes
                elif "system" in source_path and "code" not in source_path:
                    values = systems
                elif "system" in source_path and "code" in source_path:
                    values = system_values
                elif "text" in source_path:
                    values = texts
                elif "display" in source_path:
                    values = displays
                else:
                    values = codes
                self._assign_legacy(result, mapping, values)
        else:
            result["codes"] = codes if codes else []
            result["systems"] = systems if systems else []
            result["systemValues"] = system_values if system_values else []
            if texts:
                result["text"] = texts
            if displays:
                result["displays"] = displays

        return result

    # ------------------------------------------------------------------
    # Field-mapping → result wiring (shared between both modes).
    # ------------------------------------------------------------------
    @staticmethod
    def _assign_legacy(
        result: Dict[str, Any],
        mapping: Dict[str, Any],
        values: List[str],
    ) -> None:
        target_field = mapping["target_field"]
        normalize = mapping.get("normalize")
        if normalize == "lowercase":
            values = [v.lower() if isinstance(v, str) else v for v in values]
        datatype = mapping.get("datatype", "string")
        if "array" in datatype:
            # Sparse for arrays too: writing `[]` was the historical
            # bug — it polluted Appointment._search with
            # `reasonCode_codes: []` whenever an Appointment used
            # the R5 CodeableReference shape (`reason[*].reference`)
            # without a sibling `concept`. Empty arrays also clutter
            # indexes and break `$exists` coverage queries — mirrors
            # the rationale already adopted in ReferenceExtractor.
            if values:
                result[target_field] = values
        else:
            # Sparse output for scalar datatypes: when no values were
            # projected, omit the field entirely (mirrors the resource-
            # rooted ``_assign`` helper). Writing ``None`` would clash
            # with the denormalizer's ``datatype: string`` validation
            # and roll the whole rule back, losing OTHER mappings on
            # the same rule (e.g. an empty ``code.text`` would silently
            # drop the populated ``code_codes`` / ``code_systemCode``).
            if len(values) > 1:
                result[target_field] = values
            elif len(values) == 1:
                result[target_field] = values[0]
            # else: leave target_field absent

    def _assign(
        self,
        result: Dict[str, Any],
        mapping: Dict[str, Any],
        *,
        codes: List[str],
        systems: List[str],
        system_values: List[str],
        displays: List[str],
        texts: List[str],
    ) -> None:
        """Pick the right list for this mapping in resource-rooted mode."""
        target_field = mapping["target_field"]
        source_path = mapping.get("source_path") or ""
        # Heuristics mirror the legacy mapping rules so target field names
        # like ``*_codes`` / ``*_systemCode`` keep their existing meaning.
        if "systemCode" in target_field or (
            "system" in source_path and "code" in source_path
        ):
            values = system_values
        elif target_field.endswith("_systems"):
            values = systems
        elif target_field.endswith("_displays"):
            values = displays
        elif target_field.endswith("_text") or "text" in source_path:
            values = texts
        else:
            values = codes
        # In resource-rooted mode the rule always fires (the denormalizer
        # cannot pre-filter on a top-level source field), so we suppress
        # empty writes to keep the output document sparse and aligned with
        # legacy behavior where missing inputs produced no `_search.*` key.
        if not values:
            return
        self._assign_legacy(result, mapping, values)
