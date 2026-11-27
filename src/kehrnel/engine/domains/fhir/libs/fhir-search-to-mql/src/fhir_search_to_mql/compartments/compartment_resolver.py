"""
Compartment Query Resolver.

Resolves FHIR compartment queries to MongoDB query fragments.

Two execution paths (Hybrid Approach from
``analysis_documents/FHIR_TO_MQL_COMPARTMENT.md``):

1. **Precomputed (fast path)** — when a resource config opts in via the
   ``compartments.precomputed`` block (e.g. ``[Patient]``), the resolver
   emits a single-field equality lookup against
   ``_compartments.<CompartmentType>``. This is paired with
   :class:`CompartmentMembershipExtractor` which populates that field at
   denormalization time.

2. **Dynamic (default)** — for compartments not in the precomputed list,
   the resolver walks the ``CompartmentDefinition`` linking parameters
   and unions their per-field queries with ``$or``. This stays
   configuration-driven and avoids any data duplication for the long
   tail of less-frequent compartments (Encounter / Practitioner /
   Device / RelatedPerson).
"""

from typing import Dict, List, Optional
from dataclasses import dataclass

from fhir_search_to_mql.compartments.compartment_loader import CompartmentLoader
from fhir_search_to_mql.core.exceptions import ConversionError, ConfigurationError


@dataclass
class CompartmentQuery:
    """Compartment query specification."""
    
    compartment_type: str  # Patient, Encounter, etc.
    compartment_id: str     # The ID value to match
    resource_type: str      # Resource type to query


class CompartmentResolver:
    """
    Resolve compartment queries to MongoDB query fragments.
    
    Uses CompartmentDefinition files to determine which parameters
    link a resource to a compartment, then generates MongoDB queries
    based on resource configuration.
    """
    
    def __init__(self, definitions_dir: Optional[str] = None):
        """
        Initialize resolver.
        
        Args:
            definitions_dir: Path to compartment definitions directory.
                If None, uses default location.
        """
        self.loader = CompartmentLoader(definitions_dir)
        self.loader.load_all()
    
    def resolve(
        self,
        compartment_type: str,
        compartment_id: str,
        resource_type: str,
        config: Dict
    ) -> Dict:
        """
        Resolve compartment query to MongoDB query fragment.
        
        Args:
            compartment_type: Compartment type (Patient, Encounter, etc.)
            compartment_id: Compartment ID to match
            resource_type: Resource type to query
            config: Resource configuration with parameter definitions
            
        Returns:
            MongoDB query fragment
            
        Raises:
            ConversionError: If compartment resolution fails
            
        Example:
            >>> resolver.resolve(
            ...     compartment_type='Patient',
            ...     compartment_id='pat-123',
            ...     resource_type='Observation',
            ...     config=observation_config
            ... )
            {
                "$or": [
                    {"_search.patientId": "pat-123"},
                    {"_search.performerId": "pat-123"}
                ]
            }
        """
        # Validate compartment type
        compartment = self.loader.get_compartment(compartment_type)
        if not compartment:
            raise ConversionError(
                f"Invalid compartment type '{compartment_type}'. "
                f"Valid types: {', '.join(self.loader.get_all_compartments().keys())}"
            )

        # Check if resource is in compartment
        if not self.loader.is_resource_in_compartment(compartment_type, resource_type):
            raise ConversionError(
                f"Resource type '{resource_type}' is not in "
                f"compartment '{compartment_type}'"
            )

        # Hybrid fast-path: if the resource config opts into precomputed
        # compartment membership for this compartment type, emit a single
        # indexed lookup against `_compartments.<Type>` instead of walking
        # the linking parameters. The denormalizer must be configured to
        # populate `_compartments.<Type>` (see CompartmentMembershipExtractor).
        if self._is_precomputed(compartment_type, config):
            return {f"_compartments.{compartment_type}": compartment_id}

        # Get linking parameters
        params = self.loader.get_linking_parameters(compartment_type, resource_type)
        
        if not params:
            raise ConversionError(
                f"No linking parameters found for {resource_type} "
                f"in {compartment_type} compartment"
            )
        
        # Generate query fragments for each parameter
        query_fragments = []
        
        for param_name in params:
            try:
                fragment = self._generate_parameter_query(
                    param_name,
                    compartment_id,
                    config
                )
                if fragment:
                    query_fragments.append(fragment)
            except Exception as e:
                # Log warning but continue with other parameters
                # Some parameters may not be configured
                pass
        
        if not query_fragments:
            raise ConversionError(
                f"Could not generate any query fragments for {resource_type} "
                f"in {compartment_type} compartment. "
                f"Check resource configuration for parameters: {', '.join(params)}"
            )
        
        # Combine with OR logic
        if len(query_fragments) == 1:
            return query_fragments[0]
        else:
            return {"$or": query_fragments}
    
    @staticmethod
    def _is_precomputed(compartment_type: str, config: Dict) -> bool:
        """
        True if the resource config opts into the precomputed fast-path
        for ``compartment_type``. Recognizes the canonical YAML shape:

            compartments:
              precomputed:
                - Patient
                - Encounter

        Tolerates a couple of common alternates without making the
        contract ambiguous: a flat list at ``compartments_precomputed``
        and the boolean ``compartments.precompute_<Type>: true`` form.
        """
        if not isinstance(config, dict):
            return False
        section = config.get("compartments")
        if isinstance(section, dict):
            precomputed = section.get("precomputed")
            if isinstance(precomputed, list) and compartment_type in precomputed:
                return True
            flag = section.get(f"precompute_{compartment_type}")
            if flag is True:
                return True
        flat = config.get("compartments_precomputed")
        if isinstance(flat, list) and compartment_type in flat:
            return True
        return False

    def _generate_parameter_query(
        self,
        param_name: str,
        compartment_id: str,
        config: Dict
    ) -> Optional[Dict]:
        """
        Generate query fragment for a single parameter.
        
        Args:
            param_name: Parameter name (e.g., 'subject', 'patient')
            compartment_id: Compartment ID to match
            config: Resource configuration
            
        Returns:
            MongoDB query fragment or None if parameter not configured
        """
        # Accept either 'search_parameters' (canonical YAML key) or 'parameters'
        # (the historical/test alias). This keeps both shapes valid.
        params_section = config.get('search_parameters') or config.get('parameters')
        if params_section is None:
            raise ConfigurationError(
                "No search_parameters (or parameters) defined in configuration"
            )

        param_config = params_section.get(param_name)
        if not param_config:
            # Parameter not configured, skip it
            return None
        
        # Get field mappings
        if 'fields' not in param_config:
            raise ConfigurationError(
                f"No fields defined for parameter '{param_name}'"
            )
        
        fields = param_config['fields']

        # Compartment scoping always matches the bare resource id of the
        # compartment root (e.g., "pat-123") against the linking parameter.
        # That means we should ONLY target fields that store the extracted
        # id portion of a reference (`referenceType: id`). Targeting the
        # `type` field or the full `Patient/pat-123` reference produces dead
        # branches: those fields can never equal the bare id and they bloat
        # the final $or, defeating the index plan.
        id_field_queries: List[Dict] = []
        fallback_queries: List[Dict] = []

        for field_def in fields:
            if not isinstance(field_def, dict) or 'field' not in field_def:
                continue

            field_path = field_def['field']
            ref_field_type = field_def.get('referenceType')

            if ref_field_type == 'id':
                id_field_queries.append({field_path: compartment_id})
            elif ref_field_type is None:
                # Untyped fields are kept as a fallback in case the param
                # config omits referenceType (older configs / non-reference
                # params used inside compartment definitions).
                fallback_queries.append({field_path: compartment_id})
            # Skip 'type' and 'full' fields — they cannot equal a bare id.

        field_queries = id_field_queries or fallback_queries

        if not field_queries:
            return None
        elif len(field_queries) == 1:
            return field_queries[0]
        else:
            return {"$or": field_queries}
    
    def combine_with_parameters(
        self,
        compartment_query: Dict,
        parameter_queries: List[Dict]
    ) -> Dict:
        """
        Combine compartment scope with additional parameter queries.
        
        The compartment query defines the scope (which patient/encounter/etc.),
        and parameter queries provide additional filtering within that scope.
        
        Args:
            compartment_query: Compartment query fragment (typically with $or)
            parameter_queries: List of parameter query fragments
            
        Returns:
            Combined MongoDB query with AND logic
            
        Example:
            >>> compartment_query = {"$or": [
            ...     {"_search.patientId": "pat-123"},
            ...     {"_search.performerId": "pat-123"}
            ... ]}
            >>> parameter_queries = [{"code": "8480-6"}]
            >>> resolver.combine_with_parameters(compartment_query, parameter_queries)
            {
                "$and": [
                    {
                        "$or": [
                            {"_search.patientId": "pat-123"},
                            {"_search.performerId": "pat-123"}
                        ]
                    },
                    {"code": "8480-6"}
                ]
            }
        """
        # Filter out empty queries
        all_queries = [compartment_query] + [q for q in parameter_queries if q]
        
        if not all_queries:
            return {}
        
        if len(all_queries) == 1:
            return all_queries[0]
        
        # Check if we can merge queries (all different top-level fields)
        can_merge = True
        all_keys = set()
        
        for query in all_queries:
            if any(key.startswith('$') for key in query.keys()):
                # Contains logical operator, cannot merge
                can_merge = False
                break
            
            # Check for duplicate keys
            if all_keys & query.keys():
                can_merge = False
                break
            
            all_keys.update(query.keys())
        
        if can_merge:
            # Merge into single dict
            merged = {}
            for query in all_queries:
                merged.update(query)
            return merged
        else:
            # Use $and
            return {"$and": all_queries}
    
    def validate_compartment_query(
        self,
        compartment_type: str,
        compartment_id: str,
        resource_type: str
    ) -> tuple[bool, Optional[str]]:
        """
        Validate a compartment query before resolution.
        
        Args:
            compartment_type: Compartment type
            compartment_id: Compartment ID
            resource_type: Resource type
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check compartment type
        if not self.loader.get_compartment(compartment_type):
            return False, f"Invalid compartment type '{compartment_type}'"
        
        # Check ID is present
        if not compartment_id:
            return False, "Compartment ID is required"
        
        # Check resource type is in compartment
        if not self.loader.is_resource_in_compartment(compartment_type, resource_type):
            return False, (
                f"Resource type '{resource_type}' is not in "
                f"compartment '{compartment_type}'"
            )
        
        return True, None
    
    def get_compartment_resources(self, compartment_type: str) -> List[str]:
        """
        Get list of resource types in a compartment.
        
        Args:
            compartment_type: Compartment type
            
        Returns:
            List of resource type codes
        """
        compartment = self.loader.get_compartment(compartment_type)
        if not compartment:
            return []
        
        return list(compartment.resources.keys())
    
    def get_compartment_info(self, compartment_type: str) -> Optional[Dict]:
        """
        Get information about a compartment.
        
        Args:
            compartment_type: Compartment type
            
        Returns:
            Dictionary with compartment information or None if not found
        """
        compartment = self.loader.get_compartment(compartment_type)
        if not compartment:
            return None
        
        return {
            'id': compartment.id,
            'url': compartment.url,
            'name': compartment.name,
            'code': compartment.code,
            'status': compartment.status,
            'description': compartment.description,
            'resource_count': len(compartment.resources),
            'resources': list(compartment.resources.keys())
        }
