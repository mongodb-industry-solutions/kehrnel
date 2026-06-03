"""
Address extractor for FHIR address structures.

Extracts:
- addressLine: Address lines
- addressCity: City
- addressState: State/province
- addressPostalCode: Postal/ZIP code
- addressCountry: Country
- addressFull: Complete address string
- addressUse: Use codes (home / work / temp / old / billing)
- addressType: Type codes (postal / physical / both)
- addressText: Free-text address representation

Two invocation modes (mirrors :class:`IdentifierExtractor`,
:class:`CodeableConceptExtractor`, :class:`ReferenceExtractor`):

1. **Pre-resolved**: ``value`` is an Address (or list) already navigated
   by the denormalizer via a top-level field on the resource (this is
   how Patient.address is wired).

2. **Resource-rooted** (``source: $resource``): ``value`` is the entire
   FHIR resource and each mapping's ``source_path`` is a path
   expression evaluated by :mod:`path_resolver`. This handles addresses
   nested under intermediate fields — for example
   ``Organization.contact[*].address`` (R5 ExtendedContactDetail) —
   without any resource-specific extractor.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.path_resolver import (
    looks_like_resource,
    resolve_path,
)


class AddressExtractor(FieldExtractor):
    """Extract Address FHIR structure to searchable fields."""

    def extract(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Address structure.

        Args:
            value: Address, list of Address, or full FHIR resource
                (with ``resourceType``). Resource-mode requires
                ``field_mappings`` so the extractor knows which paths
                to walk.
            field_mappings: Field mapping configuration

        Returns:
            Dictionary with extracted address fields
        """
        if looks_like_resource(value) and field_mappings:
            return self._extract_from_resource(value, field_mappings)
        result = {}
        addresses = self._ensure_list(value)
        
        if not addresses:
            return result
        
        lines = []
        cities = []
        states = []
        postal_codes = []
        countries = []
        full_addresses = []
        uses = []
        types = []
        texts = []

        for address in addresses:
            if not isinstance(address, dict):
                continue

            # Extract components
            if 'line' in address:
                addr_lines = address['line'] if isinstance(address['line'], list) else [address['line']]
                lines.extend(addr_lines)

            if 'city' in address:
                cities.append(address['city'])

            if 'state' in address:
                states.append(address['state'])

            if 'postalCode' in address:
                postal_codes.append(address['postalCode'])

            if 'country' in address:
                countries.append(address['country'])

            if 'use' in address:
                uses.append(address['use'])

            if 'type' in address:
                types.append(address['type'])

            if 'text' in address and isinstance(address['text'], str):
                texts.append(address['text'])

            # Construct full address. Prefer the curated `text` form when
            # present (FHIR allows publishers to ship a fully-formatted
            # address string); fall back to a comma-joined component
            # roll-up otherwise.
            if isinstance(address.get('text'), str) and address['text'].strip():
                full_addresses.append(address['text'].strip())
            else:
                parts = []
                if 'line' in address:
                    addr_lines = address['line'] if isinstance(address['line'], list) else [address['line']]
                    parts.extend(addr_lines)
                if 'city' in address:
                    parts.append(address['city'])
                if 'state' in address:
                    parts.append(address['state'])
                if 'postalCode' in address:
                    parts.append(address['postalCode'])
                if 'country' in address:
                    parts.append(address['country'])

                if parts:
                    full_addresses.append(', '.join(parts))
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                normalize = mapping.get('normalize')
                
                if not target_field:
                    continue
                
                # Determine what to extract. Match the most specific
                # path tail first so that `addressUse` does not get
                # mis-routed to the catch-all "full" bucket.
                path_tail = source_path.rsplit('.', 1)[-1].lower()
                target_lower = target_field.lower()

                if 'line' in path_tail:
                    extracted = lines
                elif 'city' in path_tail:
                    extracted = cities
                elif 'state' in path_tail:
                    extracted = states
                elif 'postalcode' in path_tail or 'postal' in path_tail:
                    extracted = postal_codes
                elif 'country' in path_tail:
                    extracted = countries
                elif path_tail == 'use':
                    extracted = uses
                elif path_tail == 'type':
                    extracted = types
                elif path_tail == 'text':
                    extracted = texts
                elif 'full' in target_lower or 'text' in target_lower:
                    extracted = full_addresses
                else:
                    extracted = full_addresses  # Default to full
                
                # Apply normalization
                if normalize == 'lowercase':
                    extracted = [v.lower() if isinstance(v, str) else v for v in extracted]
                
                # Set the field based on datatype
                datatype = mapping.get('datatype', 'string')
                if 'array' in datatype:
                    # Always return array for array datatypes
                    result[target_field] = extracted
                else:
                    # Return single value for non-array datatypes
                    if len(extracted) > 1:
                        result[target_field] = extracted
                    elif len(extracted) == 1:
                        result[target_field] = extracted[0]
                    else:
                        result[target_field] = None
        else:
            # Default extraction
            result['addressLine'] = lines if lines else []
            result['addressCity'] = cities if cities else []
            result['addressState'] = states if states else []
            result['addressPostalCode'] = postal_codes if postal_codes else []
            result['addressCountry'] = countries if countries else []
            result['addressFull'] = full_addresses if full_addresses else []

        return result

    def _extract_from_resource(
        self,
        resource: Dict[str, Any],
        field_mappings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Resource-rooted extraction: each mapping's ``source_path`` is
        evaluated against the full resource and the resulting items are
        funneled through the regular projection logic. The path is
        normalized to its "address-rooted" prefix so that the same
        component-routing logic (city / state / use / etc.) keeps
        working.

        Example: ``contact[*].address.city`` resolves the city values
        AND we synthesize an internal source_path of ``address.city``
        for the inner extractor so its tail-matching picks "city".
        """
        # Group mappings by their address root so that components from
        # the SAME address path are extracted together and the same
        # parallel component arrays can serve all mappings for that
        # root.
        result: Dict[str, Any] = {}

        # Bucket mappings by their address-prefix.
        roots: Dict[str, List[Dict[str, Any]]] = {}
        for mapping in field_mappings:
            target_field = mapping.get("target_field")
            source_path = mapping.get("source_path") or ""
            if not target_field or not source_path:
                continue
            # The "address root" is the path up to and including the
            # node that resolves to one or more Address values. Anything
            # after `.address` is a component sub-path.
            lower = source_path.lower()
            address_marker = ".address"
            idx = lower.rfind(address_marker)
            if idx == -1:
                # Path doesn't reference an `address` segment — treat
                # the whole path as the root (caller knows what they're
                # doing) and leave no component tail.
                root = source_path
                tail = ""
            else:
                root = source_path[: idx + len(address_marker)]
                tail = source_path[idx + len(address_marker):].lstrip(".")
            roots.setdefault(root, []).append({**mapping, "_tail": tail})

        for root, group in roots.items():
            addresses = [
                item for item in resolve_path(resource, root)
                if isinstance(item, dict)
            ]
            if not addresses:
                continue

            # For each mapping in the group, hand it to the pre-resolved
            # path with a synthesized source_path so the existing tail
            # matching picks the right component.
            synthesized: List[Dict[str, Any]] = []
            for mapping in group:
                tail = mapping.get("_tail", "")
                synth_path = f"address.{tail}" if tail else "address"
                synthesized.append({
                    **{k: v for k, v in mapping.items() if k != "_tail"},
                    "source_path": synth_path,
                })
            sub = self.extract(addresses, field_mappings=synthesized)
            for k, v in sub.items():
                if v not in (None, [], ""):
                    result[k] = v
        return result
