"""
ContactPoint extractor for FHIR contact point structures (phone, email, etc).

Source modes
------------
* ``source: <field>`` — receives the field value (a single ContactPoint
  dict or a list). This is the canonical mode for resources that hang
  ``ContactPoint[]`` directly off the resource root, e.g.
  ``Patient.telecom`` / ``Practitioner.telecom``.
* ``source: $resource`` — receives the full FHIR resource. Each
  ``field_mapping`` may carry a navigable ``source_path`` (e.g.
  ``contact[*].telecom[*]``); the extractor walks that path against the
  resource and feeds the resolved ``ContactPoint`` dicts to the same
  per-mapping routing used in pre-resolved mode. This lets nested
  ContactPoints — like PractitionerRole's ``contact[*].telecom`` (an
  ExtendedContactDetail wrapper around ``ContactPoint[]``) — be
  denormalized without flattening at ingest time.

Categorization
--------------
For each mapping we route the resolved ContactPoints to one of:
``phone`` / ``email`` / ``fax`` / ``systems`` / ``uses`` / ``values``
based on hints in ``target_field`` and ``source_path``. The same
heuristic is shared between the pre-resolved and ``$resource`` paths so
the YAML contract is identical regardless of source mode.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.path_resolver import (
    is_path_expression,
    looks_like_resource,
    resolve_path,
)


class ContactPointExtractor(FieldExtractor):
    """Extract ContactPoint FHIR structure to searchable fields."""

    def extract(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract ContactPoint structure.

        Args:
            value: ContactPoint, list of ContactPoints, OR — when the
                rule was declared with ``source: $resource`` — the full
                FHIR resource. In ``$resource`` mode every mapping must
                carry a navigable ``source_path`` pointing to where the
                ContactPoint dicts live (e.g. ``contact[*].telecom[*]``).
            field_mappings: Field mapping configuration

        Returns:
            Dictionary with extracted contact fields.
        """
        # `$resource` + per-mapping path expression. Resolve each path
        # independently so cardinality of one path doesn't leak into
        # another, then run the resolved ContactPoints through the
        # exact same `_project_contacts` logic the pre-resolved branch
        # uses — the two modes share semantics by construction.
        if (
            looks_like_resource(value)
            and field_mappings
            and any(is_path_expression(m.get("source_path")) for m in field_mappings)
        ):
            return self._extract_from_resource(value, field_mappings)

        contacts = self._ensure_list(value)
        return self._project_contacts(contacts, field_mappings)

    # ------------------------------------------------------------------
    # `$resource` + path-resolved mode
    # ------------------------------------------------------------------
    def _extract_from_resource(
        self,
        resource: Dict[str, Any],
        field_mappings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Resolve each mapping's ``source_path`` against the full
        resource and project the resulting ContactPoint dicts.

        Mirrors the contract of :class:`PeriodExtractor._extract_from_resource`:
        walk the path → flat list of leaf values → keep dict-shaped
        leaves AND flatten any list leaves so a path like
        ``contact[*].telecom`` (which leaves the inner ``ContactPoint[]``
        un-flattened by our resolver's single-step semantics) still
        yields individual ContactPoints. Bare strings — produced when
        a path ends in ``.value`` — are dropped because every routing
        branch needs the full ContactPoint dict to inspect ``system``
        and ``use``.
        """
        result: Dict[str, Any] = {}

        for mapping in field_mappings:
            target_field = mapping.get("target_field")
            source_path = (mapping.get("source_path") or "").strip()
            if not target_field or not source_path:
                continue

            leaves = resolve_path(resource, source_path)
            contacts: List[Dict[str, Any]] = []
            for leaf in leaves:
                if isinstance(leaf, list):
                    contacts.extend(c for c in leaf if isinstance(c, dict))
                elif isinstance(leaf, dict):
                    contacts.append(leaf)

            single_mapping = self._project_contacts(contacts, [mapping])
            result.update(single_mapping)

        return result

    # ------------------------------------------------------------------
    # Shared per-mapping projection — preserves the historical contract
    # of pre-resolved mode (including the legacy `[]`-on-empty array
    # semantics) so existing configs/tests behave identically.
    # ------------------------------------------------------------------
    @staticmethod
    def _project_contacts(
        contacts: List[Any],
        field_mappings: Optional[List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        if not contacts:
            return result

        values: List[str] = []
        systems: List[str] = []
        phones: List[str] = []
        emails: List[str] = []
        faxes: List[str] = []
        uses: List[str] = []

        for contact in contacts:
            if not isinstance(contact, dict):
                continue

            contact_value = contact.get('value')
            contact_system = contact.get('system')
            contact_use = contact.get('use')

            if contact_value:
                values.append(contact_value)

                if contact_system == 'phone':
                    phones.append(contact_value)
                elif contact_system == 'email':
                    emails.append(contact_value)
                elif contact_system == 'fax':
                    faxes.append(contact_value)

            if contact_system:
                systems.append(contact_system)

            if contact_use:
                uses.append(contact_use)

        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                normalize = mapping.get('normalize')

                if not target_field:
                    continue

                if 'phone' in target_field.lower() or 'phone' in source_path:
                    extracted = phones
                elif 'email' in target_field.lower() or 'email' in source_path:
                    extracted = emails
                elif 'fax' in target_field.lower() or 'fax' in source_path:
                    extracted = faxes
                elif 'system' in source_path:
                    extracted = systems
                elif 'use' in source_path:
                    extracted = uses
                else:
                    extracted = values

                if normalize == 'lowercase':
                    extracted = [
                        v.lower() if isinstance(v, str) else v for v in extracted
                    ]

                datatype = mapping.get('datatype', 'string')
                if 'array' in datatype:
                    result[target_field] = extracted
                else:
                    if len(extracted) > 1:
                        result[target_field] = extracted
                    elif len(extracted) == 1:
                        result[target_field] = extracted[0]
                    else:
                        result[target_field] = None
        else:
            result['values'] = values if values else []
            result['systems'] = systems if systems else []
            if phones:
                result['phone'] = phones
            if emails:
                result['email'] = emails
            if faxes:
                result['fax'] = faxes

        return result
