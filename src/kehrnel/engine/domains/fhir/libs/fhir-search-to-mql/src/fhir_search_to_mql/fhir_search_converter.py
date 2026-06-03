"""
Main FHIR Search to MQL Converter.

This is the primary interface for converting FHIR search queries to MongoDB queries.
"""

from typing import Dict, Any, Optional, List

from fhir_search_to_mql.core.config_loader import ConfigLoader
from fhir_search_to_mql.core.exceptions import (
    ConversionError,
    UnsupportedParameterError,
    MissingConfigurationError,
)
from fhir_search_to_mql.parser.query_parser import QueryParser
from fhir_search_to_mql.converters.string_converter import StringConverter
from fhir_search_to_mql.converters.token_converter import TokenConverter
from fhir_search_to_mql.converters.date_converter import DateConverter
from fhir_search_to_mql.converters.reference_converter import ReferenceConverter
from fhir_search_to_mql.converters.number_converter import NumberConverter
from fhir_search_to_mql.converters.quantity_converter import QuantityConverter
from fhir_search_to_mql.converters.uri_converter import URIConverter
from fhir_search_to_mql.converters.composite_converter import CompositeConverter
from fhir_search_to_mql.converters.special_converter import SpecialConverter
from fhir_search_to_mql.converters.multi_step_query import MultiStepQuery, is_multi_step_query
from fhir_search_to_mql.builder.mql_builder import MQLBuilder
from fhir_search_to_mql.compartments import CompartmentResolver

# Common ("special") FHIR search parameters dispatched to SpecialConverter rather
# than looked up in the resource config. They apply to every FHIR resource.
_SPECIAL_PARAMS = frozenset({
    "_id",
    "_lastUpdated",
    "_tag",
    "_profile",
    "_security",
    "_source",
    "_text",
    "_content",
    "_has",
    "_filter",
})


class FHIRSearchConverter:
    """
    Main converter for FHIR search queries to MongoDB Query Language (MQL).
    
    Usage:
        converter = FHIRSearchConverter(config_dir="path/to/configs")
        mql_query = converter.convert("Patient", "name=Smith&gender=male")
    """
    
    # Registry of converter classes by parameter type
    CONVERTERS = {
        'string': StringConverter,
        'token': TokenConverter,
        'date': DateConverter,
        'datetime': DateConverter,
        'reference': ReferenceConverter,
        'number': NumberConverter,
        'quantity': QuantityConverter,
        'uri': URIConverter,
        'composite': CompositeConverter,
    }
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        config_dir: Optional[Any] = None,
        compartment_definitions_dir: Optional[str] = None,
    ):
        """
        Initialize the FHIR search converter.

        Args:
            config_path: Path to a specific resource mapping file.
            config_dir: A single directory path or a list of paths
                containing resource mapping YAMLs. When omitted, the
                YAML configs bundled with this package are used (so
                ``FHIRSearchConverter()`` "just works" after a
                ``pip install``).
            compartment_definitions_dir: Path to compartment
                definitions directory. If None, uses the default
                location bundled with the package.

        Raises:
            ConfigurationError: If configuration cannot be loaded.
        """
        # Always initialize a config_loader. If neither path nor dir
        # is supplied, ConfigLoader falls back to the configs bundled
        # in the package (importlib.resources resolution).
        self.config_loader = ConfigLoader(
            config_path=config_path, config_dir=config_dir
        )

        self.query_parser = QueryParser()
        self.mql_builder = MQLBuilder()
        self.compartment_resolver = CompartmentResolver(compartment_definitions_dir)
        self._converter_cache: Dict[str, Any] = {}
    
    def convert(
        self, 
        resource_type: str, 
        query_string: Optional[str] = None, 
        url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert FHIR search query to MongoDB query.
        
        Args:
            resource_type: FHIR resource type (e.g., "Patient", "Observation")
            query_string: FHIR search query string (e.g., "name=Smith&gender=male")
            url: Full URL with query string (alternative to query_string)
            
        Returns:
            MongoDB query dictionary ready for use with pymongo
            
        Raises:
            ConversionError: If conversion fails
            MissingConfigurationError: If no configuration for resource type
            UnsupportedParameterError: If parameter not supported
            
        Example:
            >>> converter = FHIRSearchConverter(config_dir="configs")
            >>> query = converter.convert("Patient", "name=Smith&birthdate=ge1980-01-01")
            >>> # query = {
            >>> #     "$and": [
            >>> #         {"_search.familyName_lower": {"$gte": "smith", "$lt": "smith\\uffff"}},
            >>> #         {"_search.birthDate": {"$gte": datetime(1980, 1, 1)}}
            >>> #     ]
            >>> # }
        """
        # Get configuration for resource type
        try:
            # Check if _load_config method exists (for testing)
            if hasattr(self, '_load_config'):
                config = self._load_config(resource_type)
            elif self.config_loader:
                config = self.config_loader.get_config(resource_type)
            else:
                raise MissingConfigurationError(
                    f"No configuration loader available. Provide config_path or config_dir."
                )
        except MissingConfigurationError as e:
            raise MissingConfigurationError(
                f"No configuration found for resource type '{resource_type}'. "
                f"Create a {resource_type}.yaml file in the config directory."
            )
        
        # Parse query string
        parsed_query = self.query_parser.parse(query_string=query_string, url=url)
        parameters = parsed_query.get('parameters', [])
        
        if not parameters:
            return {}  # Empty query matches all
        
        # Convert each parameter to MongoDB query
        parameter_queries: List[Dict[str, Any]] = []
        multi_step_queries: List[MultiStepQuery] = []

        for param in parameters:
            param_name = param['name']
            param_value = param['value']
            modifier = param.get('modifier')
            prefix = param.get('prefix')

            try:
                if param_name in _SPECIAL_PARAMS:
                    # Common FHIR parameters apply to every resource. Prefer
                    # the resource's explicit YAML declaration (so a config
                    # can pin `_id` to a custom field path), otherwise route
                    # to SpecialConverter for the canonical implementation.
                    try:
                        param_config = self._get_parameter_config(config, param_name)
                    except UnsupportedParameterError:
                        param_config = None

                    if param_config is not None:
                        converter = self._get_converter(param_config)
                        mongo_query = converter.convert(
                            param_value, modifier=modifier, prefix=prefix
                        )
                    else:
                        mongo_query = self._convert_special_param(
                            param_name, param_value, modifier, prefix, resource_type
                        )
                else:
                    param_config = self._get_parameter_config(config, param_name)
                    converter = self._get_converter(param_config)
                    mongo_query = converter.convert(
                        param_value, modifier=modifier, prefix=prefix
                    )

                # Reference :identifier and _has produce MultiStepQuery objects
                # that cannot be folded into a single MQL dict. Surface them
                # via a stable envelope so callers can still execute them.
                if is_multi_step_query(mongo_query):
                    multi_step_queries.append(mongo_query)
                elif mongo_query:
                    parameter_queries.append(mongo_query)

            except UnsupportedParameterError:
                print(f"Warning: Parameter '{param_name}' not supported for {resource_type}")
                continue
            except Exception as e:
                print(f"Warning: Failed to convert parameter '{param_name}={param_value}': {str(e)}")
                continue

        # Build final MongoDB query
        if not parameter_queries and not multi_step_queries:
            return {}

        if parameter_queries:
            mql_query = self.mql_builder.build(parameter_queries, logic='AND')
            self.mql_builder.validate(mql_query)
            mql_query = self.mql_builder.optimize(mql_query)
        else:
            mql_query = {}

        if multi_step_queries:
            # Wrap in an envelope rather than dropping them. Execution
            # layer can detect this and run the multi-step plan first,
            # then AND the resulting id-list query into `mql_query`.
            return {
                "_query": mql_query,
                "_multi_step": [
                    msq.get_execution_plan() for msq in multi_step_queries
                ],
            }

        return mql_query

    def _convert_special_param(
        self,
        param_name: str,
        param_value: str,
        modifier: Optional[str],
        prefix: Optional[str],
        resource_type: str,
    ) -> Any:
        """Dispatch a common FHIR parameter to SpecialConverter."""
        if param_name == "_id":
            return SpecialConverter.convert_id(param_value)
        if param_name == "_lastUpdated":
            return SpecialConverter.convert_last_updated(param_value, prefix=prefix)
        if param_name == "_tag":
            return SpecialConverter.convert_tag(param_value, modifier=modifier)
        if param_name == "_profile":
            return SpecialConverter.convert_profile(param_value)
        if param_name == "_security":
            return SpecialConverter.convert_security(param_value, modifier=modifier)
        if param_name == "_source":
            # _source maps to meta.source (a uri); reuse profile-style logic.
            sources = [s.strip() for s in param_value.split(',') if s.strip()]
            if len(sources) == 1:
                return {"meta.source": sources[0]}
            return {"meta.source": {"$in": sources}}
        if param_name == "_text":
            return SpecialConverter.convert_text(param_value)
        if param_name == "_content":
            return SpecialConverter.convert_content(param_value)
        if param_name == "_has":
            # The query parser extracts `_has:Observation:patient:code=8480-6`
            # as base="_has", modifier="Observation:patient:code", value="8480-6".
            # SpecialConverter.convert_has needs the full
            # "ResourceType:ref:searchParam=value" form, so re-stitch it here.
            chain_spec = (
                f"{modifier}={param_value}" if modifier else param_value
            )
            return SpecialConverter.convert_has(
                chain_spec, base_resource_type=resource_type
            )
        if param_name == "_filter":
            # _filter expression syntax is not implemented; surface a clear
            # warning rather than silently dropping it.
            raise UnsupportedParameterError(
                f"Parameter '_filter' is not yet supported for {resource_type}"
            )
        raise UnsupportedParameterError(
            f"Unhandled special parameter '{param_name}' for {resource_type}"
        )
    
    def convert_with_compartment(
        self, 
        compartment_type: str, 
        compartment_id: str, 
        resource_type: str, 
        query_string: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert FHIR compartment search to MongoDB query.
        
        Args:
            compartment_type: Compartment type (e.g., "Patient", "Encounter")
            compartment_id: Compartment ID
            resource_type: Resource type to search
            query_string: Additional search parameters
            
        Returns:
            MongoDB query with compartment filter
            
        Raises:
            ConversionError: If compartment resolution fails
            MissingConfigurationError: If no configuration for resource type
            
        Example:
            >>> query = converter.convert_with_compartment(
            ...     "Patient", "pat-123", "Observation", "code=8480-6"
            ... )
            >>> # Returns compartment-scoped query:
            >>> # {
            >>> #     "$and": [
            >>> #         {
            >>> #             "$or": [
            >>> #                 {"_search.patientId": "pat-123"},
            >>> #                 {"_search.performerId": "pat-123"}
            >>> #             ]
            >>> #         },
            >>> #         {"_search.codeSystem_code": "8480-6"}
            >>> #     ]
            >>> # }
        """
        # Validate compartment query
        is_valid, error_msg = self.compartment_resolver.validate_compartment_query(
            compartment_type, compartment_id, resource_type
        )
        
        if not is_valid:
            raise ConversionError(f"Invalid compartment query: {error_msg}")
        
        # Get resource configuration
        try:
            config = self.config_loader.get_config(resource_type)
        except MissingConfigurationError as e:
            raise MissingConfigurationError(
                f"No configuration found for resource type '{resource_type}'. "
                f"Create a {resource_type}.yaml file in the config directory."
            )
        
        # Resolve compartment to MongoDB query fragment
        compartment_query = self.compartment_resolver.resolve(
            compartment_type=compartment_type,
            compartment_id=compartment_id,
            resource_type=resource_type,
            config=config
        )
        
        # Convert additional query parameters if present
        parameter_queries = []
        
        if query_string:
            # Parse query string
            parsed_query = self.query_parser.parse(query_string=query_string)
            parameters = parsed_query.get('parameters', [])
            
            # Convert each parameter
            for param in parameters:
                param_name = param['name']
                param_value = param['value']
                modifier = param.get('modifier')
                prefix = param.get('prefix')

                try:
                    if param_name in _SPECIAL_PARAMS:
                        try:
                            param_config = self._get_parameter_config(config, param_name)
                        except UnsupportedParameterError:
                            param_config = None

                        if param_config is not None:
                            converter = self._get_converter(param_config)
                            mongo_query = converter.convert(
                                param_value, modifier=modifier, prefix=prefix
                            )
                        else:
                            mongo_query = self._convert_special_param(
                                param_name, param_value, modifier, prefix, resource_type
                            )
                    else:
                        param_config = self._get_parameter_config(config, param_name)
                        converter = self._get_converter(param_config)
                        mongo_query = converter.convert(
                            param_value, modifier=modifier, prefix=prefix
                        )

                    if is_multi_step_query(mongo_query):
                        # Compartment + multi-step is rare; surface the plan via
                        # the same envelope used by `convert()`.
                        parameter_queries.append(
                            {"_multi_step": mongo_query.get_execution_plan()}
                        )
                    elif mongo_query:
                        parameter_queries.append(mongo_query)

                except UnsupportedParameterError:
                    print(f"Warning: Parameter '{param_name}' not supported for {resource_type}")
                    continue
                except Exception as e:
                    print(f"Warning: Failed to convert parameter '{param_name}={param_value}': {str(e)}")
                    continue
        
        # Combine compartment query with parameter queries
        final_query = self.compartment_resolver.combine_with_parameters(
            compartment_query,
            parameter_queries
        )
        
        # Optimize query
        final_query = self.mql_builder.optimize(final_query)
        
        return final_query
    
    def _get_parameter_config(self, config: Dict[str, Any], param_name: str) -> Dict[str, Any]:
        """
        Get configuration for a search parameter.
        
        Args:
            config: Resource configuration
            param_name: Parameter name
            
        Returns:
            Parameter configuration
            
        Raises:
            UnsupportedParameterError: If parameter not found
        """
        search_params = config.get('search_parameters', {})
        
        if param_name not in search_params:
            raise UnsupportedParameterError(
                f"Parameter '{param_name}' not configured for {config.get('resource')}"
            )
        
        return search_params[param_name]
    
    def _get_converter(self, param_config: Dict[str, Any]):
        """
        Get converter instance for parameter type.
        
        Args:
            param_config: Parameter configuration
            
        Returns:
            Converter instance
            
        Raises:
            UnsupportedParameterError: If converter not available
        """
        param_type = param_config.get('type')
        
        if not param_type:
            raise UnsupportedParameterError("Parameter configuration missing 'type' field")
        
        # Get converter class
        converter_class = self.CONVERTERS.get(param_type)
        
        if not converter_class:
            raise UnsupportedParameterError(
                f"No converter available for parameter type '{param_type}'"
            )
        
        # Create converter instance (cached by config)
        cache_key = f"{param_type}_{id(param_config)}"
        
        if cache_key not in self._converter_cache:
            self._converter_cache[cache_key] = converter_class(param_config)
        
        return self._converter_cache[cache_key]
    
    def get_supported_parameters(self, resource_type: str) -> List[str]:
        """
        Get list of supported search parameters for a resource type.
        
        Args:
            resource_type: FHIR resource type
            
        Returns:
            List of parameter names
        """
        try:
            config = self.config_loader.get_config(resource_type)
            return list(config.get('search_parameters', {}).keys())
        except MissingConfigurationError:
            return []
    
    def get_compartment_resources(self, compartment_type: str) -> List[str]:
        """
        Get list of resource types in a compartment.
        
        Args:
            compartment_type: Compartment type (Patient, Encounter, etc.)
            
        Returns:
            List of resource type codes
        """
        return self.compartment_resolver.get_compartment_resources(compartment_type)
    
    def get_compartment_info(self, compartment_type: str) -> Optional[Dict]:
        """
        Get information about a compartment.
        
        Args:
            compartment_type: Compartment type
            
        Returns:
            Dictionary with compartment information or None if not found
        """
        return self.compartment_resolver.get_compartment_info(compartment_type)
    
    def list_compartments(self) -> List[str]:
        """
        Get list of available compartment types.
        
        Returns:
            List of compartment type codes
        """
        return list(self.compartment_resolver.loader.get_all_compartments().keys())
