"""
Phonetic extractor for FHIR name-bearing fields.

Generates phonetic encodings (Soundex) for the tokens of a name so that
the FHIR `phonetic` search parameter can be served from a precomputed
array instead of an expensive runtime substring scan.

The extractor accepts two input shapes — both are valid in FHIR R5:

1. **HumanName** — `{family: ..., given: [...]}` (Patient, Practitioner,
   Person, RelatedPerson). The family / given parts are encoded
   separately so the rule's `source_path` can pick "family", "given", or
   the union.

2. **Plain string** (or list of strings) — `Organization.name` (single
   string) and `Organization.alias` (list of strings) are typed as
   `string`, not `HumanName`. Each whitespace-separated token of every
   string is Soundex-encoded just like a name part.

Mixed lists work too: `[{"family": "Smith"}, "Acme Corp", ...]`.

Soundex was selected because:
- It is in the public domain and has no external dependency.
- The algorithm is fixed-length (4 chars) and indexable.
- It matches the spirit of the FHIR `phonetic` modifier ("a portion of
  either family or given name using some kind of phonetic matching
  algorithm") while remaining deterministic and reversible.

The extractor writes a deduplicated array of Soundex codes (e.g.
`["S530", "J525"]`) into the configured target field, typically
`_search.phonetic_codes`.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.path_resolver import (
    looks_like_resource,
    resolve_path,
)


# Standard American Soundex mapping. Vowels (AEIOU), H, W, Y -> not coded.
_SOUNDEX_CODES = {
    "B": "1", "F": "1", "P": "1", "V": "1",
    "C": "2", "G": "2", "J": "2", "K": "2",
    "Q": "2", "S": "2", "X": "2", "Z": "2",
    "D": "3", "T": "3",
    "L": "4",
    "M": "5", "N": "5",
    "R": "6",
}


def soundex(token: str) -> Optional[str]:
    """
    Compute the American Soundex code for a single name token.

    Args:
        token: A single name token (no spaces). Empty / non-alpha returns None.

    Returns:
        4-character Soundex code (e.g. "S530") or None for empty input.
    """
    if not token or not isinstance(token, str):
        return None

    cleaned = "".join(c for c in token.upper() if c.isalpha())
    if not cleaned:
        return None

    first = cleaned[0]
    encoded = [first]
    prev_code = _SOUNDEX_CODES.get(first, "")

    for ch in cleaned[1:]:
        code = _SOUNDEX_CODES.get(ch, "")
        if code == "":
            # H and W are skipped without resetting the previous code so that
            # adjacent equal codes still collapse (per the canonical algorithm).
            if ch in ("H", "W"):
                continue
            prev_code = ""
            continue
        if code != prev_code:
            encoded.append(code)
        prev_code = code
        if len(encoded) == 4:
            break

    encoded.extend(["0"] * (4 - len(encoded)))
    return "".join(encoded[:4])


class PhoneticExtractor(FieldExtractor):
    """
    Extract Soundex codes from a HumanName (or list of HumanName) value.

    Configuration field_mappings entries are honored when present. If absent,
    the extractor writes the deduplicated codes to the conventional
    `phonetic_codes` key for backward compatibility.
    """

    def extract(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Compute phonetic codes for the given HumanName value(s) or
        plain-string name field(s).

        Args:
            value: HumanName dict, list of dicts, plain string, list of
                strings, mixed list, or — when ``source: $resource`` is
                used — the full FHIR resource (with ``resourceType``).
                In that last case ``field_mappings`` MUST supply a
                ``source_path`` so the extractor knows which path(s) to
                resolve.
            field_mappings: Optional explicit mapping; when provided the
                extractor uses each mapping's ``target_field`` to place
                the code array. ``source_path`` selects which name parts
                to encode:
                  - contains "family" → family names only
                  - contains "given" → given names only
                  - otherwise → both family and given names

        Returns:
            Dict mapping target field names to a deduplicated list of
            Soundex codes (or an empty list when no name tokens are
            present).
        """
        if looks_like_resource(value) and field_mappings:
            return self._extract_from_resource(value, field_mappings)
        names = self._ensure_list(value)
        family_tokens: List[str] = []
        given_tokens: List[str] = []

        for name in names:
            if isinstance(name, dict):
                # HumanName shape (Patient/Practitioner/etc.)
                family = name.get("family")
                if isinstance(family, str) and family.strip():
                    family_tokens.extend(family.split())
                given = name.get("given")
                if isinstance(given, list):
                    for g in given:
                        if isinstance(g, str) and g.strip():
                            given_tokens.extend(g.split())
                elif isinstance(given, str) and given.strip():
                    given_tokens.extend(given.split())
            elif isinstance(name, str) and name.strip():
                # Plain-string shape (Organization.name, Organization.alias).
                # No family/given partition is meaningful, so every token
                # contributes to the "given" bucket; the union path below
                # handles them identically to HumanName tokens.
                given_tokens.extend(name.split())

        def _encode(tokens: List[str]) -> List[str]:
            seen: List[str] = []
            for tok in tokens:
                code = soundex(tok)
                if code and code not in seen:
                    seen.append(code)
            return seen

        family_codes = _encode(family_tokens)
        given_codes = _encode(given_tokens)
        all_codes = _encode(family_tokens + given_tokens)

        result: Dict[str, Any] = {}

        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get("target_field")
                if not target_field:
                    continue
                source_path = (mapping.get("source_path") or "").lower()
                if "family" in source_path:
                    result[target_field] = list(family_codes)
                elif "given" in source_path:
                    result[target_field] = list(given_codes)
                else:
                    result[target_field] = list(all_codes)
        else:
            result["phonetic_codes"] = list(all_codes)

        return result

    def _extract_from_resource(
        self,
        resource: Dict[str, Any],
        field_mappings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Resource-rooted extraction: each mapping's ``source_path`` is
        resolved against the full resource and the resulting items
        (HumanName dicts and/or strings) are tokenised through the
        regular pre-resolved logic.

        Empty resolutions are skipped (sparse output) — no
        ``target_field`` is written when the path matches nothing on
        this particular resource.
        """
        result: Dict[str, Any] = {}
        for mapping in field_mappings:
            target_field = mapping.get("target_field")
            source_path = mapping.get("source_path") or ""
            if not target_field or not source_path:
                continue

            resolved = resolve_path(resource, source_path)
            if not resolved:
                continue

            sub = self.extract(resolved, field_mappings=[mapping])
            codes = sub.get(target_field)
            if codes:
                result[target_field] = codes
        return result
