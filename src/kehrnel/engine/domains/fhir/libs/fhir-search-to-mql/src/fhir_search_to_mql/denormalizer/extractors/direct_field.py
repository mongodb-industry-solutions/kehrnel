"""
Direct field extractor for FHIR scalar / polymorphic-scalar fields.

Use this extractor whenever a denormalization rule needs to copy a
literal value (boolean, dateTime, integer, decimal, string) from a
FHIR resource path into a denormalized field WITHOUT re-shaping it.
The canonical motivating case is FHIR's polymorphic ``[x]`` choice
elements where the resource carries one of several
type-suffixed siblings (e.g. ``Patient.deceasedBoolean`` /
``Patient.deceasedDateTime``) and we want to project each variant
into its own searchable bucket.

Source modes
------------
Only resource-rooted (``source: $resource``) is supported. Each
``field_mappings`` entry declares a path expression in
``source_path`` (resolved by :mod:`path_resolver`) and a target
``datatype`` for type coercion. Pre-resolved mode is intentionally
omitted — when the value has already been navigated, the simpler
extractors (``HumanNameExtractor`` etc.) are usually more
appropriate; use this one when the *path* is the polymorphic
discriminator.

Sparse output
-------------
If a path resolves to nothing (or to a single ``None`` value), the
target field is OMITTED from the output. This is consistent with
``PeriodExtractor`` and the resource-rooted branch of
``ReferenceExtractor`` and avoids polluting the denormalized
document with ``null`` values that would clash with the
``ResourceDenormalizer`` datatype validator.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.path_resolver import resolve_path


class DirectFieldExtractor(FieldExtractor):
    """Copy raw scalar values from FHIR resource paths into target fields."""

    # Datatypes for which we accept a value as-is (already in canonical
    # form). Anything else we coerce via the helpers below. Keep the
    # set small and explicit — surprises here corrupt search indexes.
    _PASSTHROUGH_TYPES = {"boolean", "integer", "decimal", "object"}

    def extract(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Resolve each mapping's ``source_path`` against the resource
        and write the (optionally coerced) value to ``target_field``.

        Args:
            value: The full FHIR resource (this extractor is meant to
                be used with ``source: $resource``).
            field_mappings: Per-field mappings — each must declare
                ``source_path``, ``target_field``, and ``datatype``.

        Returns:
            Dictionary of {target_field: coerced_value}; empty when
            no mapping resolved.
        """
        if not field_mappings or not isinstance(value, dict):
            return {}

        result: Dict[str, Any] = {}

        for mapping in field_mappings:
            target_field = mapping.get("target_field")
            source_path = mapping.get("source_path") or ""
            datatype = mapping.get("datatype", "string")
            transform = (mapping.get("transform") or "").strip().lower()
            # ``normalize: lowercase`` / ``uppercase`` is the canonical
            # case-folding hook every other extractor in this package
            # honors (HumanNameExtractor, AddressExtractor,
            # CodeableConceptExtractor, IdentifierExtractor,
            # ContactPointExtractor, ReferenceExtractor, TextExtractor).
            # Applied AFTER `_coerce` so date / boolean / numeric
            # passthrough paths are unaffected — only string-typed
            # output is folded.
            normalize = (mapping.get("normalize") or "").strip().lower() or None

            if not target_field or not source_path:
                continue

            resolved = resolve_path(value, source_path)
            # `resolve_path` returns a list. For polymorphic scalars
            # (the primary use case) the list has 0 or 1 elements;
            # for arrays it can have more. Filter out None / empty so
            # the sparse contract holds.
            cleaned = [r for r in resolved if r is not None and r != ""]
            if not cleaned:
                continue

            # `transform: presence` — write a literal `True` when the
            # path resolved to anything. This is the canonical way to
            # bridge FHIR's polymorphic `deceased[x]` choice into a
            # single `_search.deceased: boolean` token bucket: a
            # `deceasedDateTime` value implies `deceased=true` per
            # the R5 search-parameter spec, but the dateTime string
            # itself can't be coerced to bool. Don't compete with
            # other mappings on the same target_field — the LATER
            # write wins, which is fine because the rule's whole
            # purpose is "any of these → true".
            if transform == "presence":
                result[target_field] = True
                continue

            if "array" in datatype:
                coerced_list = [
                    self._coerce(item, datatype) for item in cleaned
                ]
                # Drop any items that failed to coerce (returned None).
                coerced_list = [c for c in coerced_list if c is not None]
                coerced_list = self._apply_normalize(coerced_list, normalize)
                if coerced_list:
                    result[target_field] = coerced_list
                continue

            # Scalar target. Take the first resolved value; if more
            # than one resolved, surface the list rather than silently
            # collapsing data — the denormalizer's validator will
            # flag the cardinality mismatch in the YAML.
            if len(cleaned) == 1:
                coerced = self._coerce(cleaned[0], datatype)
                if coerced is not None:
                    folded = self._apply_normalize([coerced], normalize)
                    if folded:
                        result[target_field] = folded[0]
            else:
                coerced_list = [
                    self._coerce(item, datatype) for item in cleaned
                ]
                coerced_list = [c for c in coerced_list if c is not None]
                coerced_list = self._apply_normalize(coerced_list, normalize)
                if coerced_list:
                    result[target_field] = coerced_list

        return result

    @staticmethod
    def _apply_normalize(
        values: List[Any], normalize: Optional[str]
    ) -> List[Any]:
        """Case-fold every string element of ``values`` according to
        ``normalize`` (``lowercase`` / ``uppercase``). Non-string
        elements (e.g. ``datetime`` / ``bool`` / ``int``) are passed
        through unchanged so date / boolean targets aren't corrupted
        when a YAML accidentally carries the normalize flag."""
        if normalize == "lowercase":
            return [v.lower() if isinstance(v, str) else v for v in values]
        if normalize == "uppercase":
            return [v.upper() if isinstance(v, str) else v for v in values]
        return values

    @classmethod
    def _coerce(cls, raw: Any, datatype: str) -> Any:
        """
        Coerce ``raw`` to a canonical form for ``datatype``.

        - ``string`` / ``date`` / ``dateTime`` / ``time`` / ``url`` /
          ``code``: ``str(raw).strip()``, drop empty results.
        - ``boolean``: pass through Python booleans; coerce common
          string representations ("true"/"false"); drop ambiguous.
        - ``integer``: ``int(raw)`` with a safety net.
        - ``decimal``: ``float(raw)`` with a safety net.
        - ``object`` / arrays-of-object: pass through.
        - everything else: pass through (datatype is informational).

        Special-case: when ``raw`` is already a Python ``date`` /
        ``datetime`` instance and the declared datatype is one of the
        FHIR date kinds (``date`` / ``dateTime`` / ``instant`` / ``time``),
        we pass it through UNCHANGED. The MQL date converter emits BSON
        ``datetime`` for range comparisons; if we stringified the
        denormalized value here, BSON's type-aware comparison would
        treat string-vs-datetime as never equal and date queries would
        silently miss every match. Production pipelines that ingest
        FHIR JSON with ISO-8601 string dates should pre-coerce them to
        ``datetime`` at ingest time; the extractor's job is to NOT
        lossily downgrade what the caller already provided.
        """
        # Strip "array[...]" prefix — the caller already split arrays.
        normalized = datatype.split("[", 1)[-1].rstrip("]").strip().lower()

        if normalized in cls._PASSTHROUGH_TYPES:
            if normalized == "boolean":
                if isinstance(raw, bool):
                    return raw
                if isinstance(raw, str):
                    s = raw.strip().lower()
                    if s == "true":
                        return True
                    if s == "false":
                        return False
                    return None
                return None
            if normalized == "integer":
                try:
                    return int(raw)
                except (TypeError, ValueError):
                    return None
            if normalized == "decimal":
                try:
                    return float(raw)
                except (TypeError, ValueError):
                    return None
            return raw

        # Date-kind types: keep `datetime` / `date` instances as-is so
        # they round-trip as BSON `datetime` and remain comparable to
        # converter-emitted range operands.
        if normalized in ("date", "datetime", "instant", "time"):
            from datetime import date as _date, datetime as _datetime
            if isinstance(raw, (_date, _datetime)):
                return raw

        # Default: stringify and trim. Covers `string`, `date`,
        # `dateTime`, `time`, `code`, `url`, `uri`, `markdown`, `id`.
        if normalized == "string" and isinstance(raw, dict):
            display = raw.get("display")
            if isinstance(display, str) and display.strip():
                return display.strip()
            reference = raw.get("reference")
            if isinstance(reference, str) and reference.strip():
                return reference.split("/")[-1].strip()
            return None
        s = str(raw).strip()
        return s if s else None
