"""
Generic text extractor for free-text and markdown FHIR fields.

Walks one or more paths against the supplied value (typically the full
FHIR resource via ``source: $resource``) and writes a normalized,
optionally-lowercased string into the target field. Useful for the
``value-markdown`` family of search parameters and any other resource
that needs a single denormalized text blob to power range/prefix queries.

The extractor is data-type-agnostic: any path that resolves to a string
contributes to the output. Non-string leaves are coerced via ``str()``.

Configuration keys honored on each ``field_mappings`` entry:
- ``source_path``: path expression (supports ``|`` union, ``[*]`` arrays,
  dot navigation — see :mod:`fhir_search_to_mql.denormalizer.path_resolver`).
- ``target_field``: name of the denormalized field to write.
- ``separator``: string used to join multi-value results (default: " ").
- ``normalize``: ``"lowercase"``, ``"uppercase"``, or absent (default).
  Mirrors the no-normalization default of every other extractor in
  :mod:`fhir_search_to_mql.denormalizer.extractors`.
- ``datatype``: ``"string"`` (default) — single-string output. If
  ``array`` is present in the datatype the extractor writes the list
  of resolved strings (no joining); normalization is applied per
  element so ``array[string]`` + ``normalize: lowercase`` yields a
  lowercased list — symmetric with the single-string path and matches
  how :class:`HumanNameExtractor` and :class:`AddressExtractor` behave.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.path_resolver import resolve_path


class TextExtractor(FieldExtractor):
    """Resolve text-bearing paths and write joined / normalized output."""

    def extract(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if not field_mappings:
            return {}

        result: Dict[str, Any] = {}
        for mapping in field_mappings:
            target_field = mapping.get("target_field")
            source_path = mapping.get("source_path") or ""
            if not target_field or not source_path:
                continue

            resolved = resolve_path(value, source_path)
            strings: List[str] = []
            for item in resolved:
                if isinstance(item, str):
                    s = item.strip()
                elif item is None:
                    continue
                else:
                    s = str(item).strip()
                if s:
                    strings.append(s)

            if not strings:
                continue

            datatype = mapping.get("datatype", "string")
            normalize = mapping.get("normalize")

            if "array" in datatype:
                # Caller wants the list — useful for indexing every
                # individual text fragment as its own searchable token.
                # Normalize per element so a `normalize: lowercase` rule
                # produces a lowercased array (consistent with how
                # HumanNameExtractor / AddressExtractor lower their
                # array outputs).
                if normalize == "lowercase":
                    strings = [s.lower() for s in strings]
                elif normalize == "uppercase":
                    strings = [s.upper() for s in strings]
                result[target_field] = strings
                continue

            separator = mapping.get("separator", " ")
            joined = separator.join(strings)
            if normalize == "lowercase":
                joined = joined.lower()
            elif normalize == "uppercase":
                joined = joined.upper()
            result[target_field] = joined

        return result
