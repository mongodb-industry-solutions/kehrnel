"""
Configuration loader for YAML-based FHIR resource mapping files.

This module loads and validates resource configuration files that define:
1. Denormalization rules (what fields to extract and how)
2. Search parameters (how to convert FHIR searches to MQL)
3. Index recommendations

Bundled defaults
----------------
The package ships its own set of FHIR R5 resource configs at
``fhir_search_to_mql/configs/*.yaml`` (Patient, Observation,
Appointment, Organization, Location, …). When a caller constructs a
:class:`ConfigLoader` (or :class:`ResourceDenormalizer` /
:class:`FHIRSearchConverter`) without passing ``config_path`` or
``config_dir``, the loader resolves the bundled directory via
:mod:`importlib.resources` so the library is fully usable as an
installable dependency in any host project — no working-directory
gymnastics required.

Layered overrides
-----------------
``config_dir`` accepts EITHER a single string OR a list of strings.
When multiple paths are supplied, configs are loaded left-to-right and
later directories override earlier ones for the same ``resource``
type. This is the recommended way for a host project to add resources
the library doesn't bundle (e.g. ``Encounter``, ``MedicationRequest``)
or to override a bundled config without forking this package.
"""

import os
import yaml
from typing import Dict, Any, Optional, List, Union
from pathlib import Path

try:
    from importlib.resources import files as _resources_files  # py>=3.9
except ImportError:  # pragma: no cover - py<3.9 fallback (unsupported)
    _resources_files = None  # type: ignore[assignment]

from fhir_search_to_mql.core.exceptions import (
    ConfigurationError,
    MissingConfigurationError,
    ValidationError,
)
from fhir_search_to_mql.core.constants import (
    PARAMETER_TYPES,
    FHIR_VERSIONS,
    DEFAULT_FHIR_VERSION,
)


def _bundled_configs_dir() -> str:
    """
    Resolve the path to the YAML configs that ship with this package.

    Returns the filesystem path to ``fhir_search_to_mql/configs/``,
    which is populated by ``[tool.setuptools.package-data]`` in
    ``pyproject.toml``. Works for both editable installs (``pip
    install -e .``) and wheel installs.
    """
    if _resources_files is None:  # pragma: no cover
        # Fallback: derive from this module's __file__.
        here = Path(__file__).resolve()
        return str(here.parent.parent / "configs")
    return str(_resources_files("fhir_search_to_mql") / "configs")


class ConfigLoader:
    """
    Load and validate FHIR resource mapping configurations from YAML files.

    Supports:

    - Loading a single configuration file (``config_path``)
    - Loading every config from a directory (``config_dir`` as a string)
    - Loading from MULTIPLE directories with override semantics
      (``config_dir`` as a list — later dirs win for duplicate
      ``resource`` types)
    - Defaulting to the configs bundled inside this package when
      neither ``config_path`` nor ``config_dir`` is provided
    - Multi-version FHIR support (R4, R5, R6)
    - Configuration caching
    - Validation of configuration structure
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        config_dir: Optional[Union[str, List[str]]] = None,
    ):
        """
        Initialize configuration loader.

        Args:
            config_path: Path to a single resource mapping file.
            config_dir: A directory path, or a list of directory paths,
                each containing resource mapping files. When
                multiple are given, later directories override earlier
                ones for the same ``resource`` type. If both
                ``config_path`` and ``config_dir`` are None, the
                bundled package configs are loaded.

        Raises:
            ConfigurationError: If a provided directory does not exist
                or contains no YAML files.
        """
        # Resolve config_dir to a list of directory strings (or None).
        dirs: Optional[List[str]] = None
        if config_dir is not None:
            if isinstance(config_dir, (list, tuple)):
                dirs = [str(d) for d in config_dir]
            else:
                dirs = [str(config_dir)]

        # Default: use the configs bundled with the package.
        if not config_path and not dirs:
            dirs = [_bundled_configs_dir()]

        self.config_path = config_path
        self.config_dir = dirs[0] if dirs and len(dirs) == 1 else dirs
        self._config_dirs: Optional[List[str]] = dirs
        self._config_cache: Dict[str, Dict[str, Any]] = {}

        # Load configurations.
        if dirs:
            self._load_all_configs()
        elif config_path:
            self._load_single_config(config_path)
    
    def _load_single_config(self, path: str) -> Dict[str, Any]:
        """Load a single configuration file."""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            # Validate configuration
            self._validate_config(config, path)
            
            # Extract resource type
            resource_type = config.get('resource')
            if not resource_type:
                raise ConfigurationError(f"Configuration missing 'resource' field: {path}")
            
            # Cache the configuration
            self._config_cache[resource_type] = config
            
            return config
            
        except FileNotFoundError:
            raise ConfigurationError(f"Configuration file not found: {path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {path}: {str(e)}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration from {path}: {str(e)}")
    
    def _load_all_configs(self) -> None:
        """
        Load every YAML config from all configured directories.

        When multiple directories are configured (layered overrides),
        they are processed left-to-right so the LAST directory's copy
        of a given resource type wins — letting host projects override
        bundled configs by passing ``[bundled_dir, my_overrides_dir]``.

        Validation errors on individual files are intentionally
        non-fatal — the loader logs a warning and continues so a
        single malformed file in a host project's overrides directory
        doesn't take down the whole library. The public surface
        (:meth:`list_resources`) lets callers detect the empty case.
        """
        dirs = self._config_dirs or []
        if not dirs:
            return

        any_yaml_files = False
        for raw_dir in dirs:
            config_path = Path(raw_dir)
            if not config_path.exists():
                raise ConfigurationError(
                    f"Configuration directory not found: {raw_dir}"
                )
            if not config_path.is_dir():
                raise ConfigurationError(f"Not a directory: {raw_dir}")

            config_files = sorted(
                list(config_path.glob("*.yaml"))
                + list(config_path.glob("*.yml"))
            )
            if config_files:
                any_yaml_files = True
            for config_file in config_files:
                try:
                    self._load_single_config(str(config_file))
                except Exception as e:
                    print(f"Warning: Failed to load {config_file}: {str(e)}")
                    # Continue loading other files

        if not any_yaml_files:
            raise ConfigurationError(
                f"No configuration files found in: {dirs}"
            )
    
    def get_config(self, resource_type: str, fhir_version: Optional[str] = None) -> Dict[str, Any]:
        """
        Get configuration for a specific resource type.
        
        Args:
            resource_type: FHIR resource type (e.g., "Patient", "Observation")
            fhir_version: FHIR version (R4, R5, R6). If None, uses any available.
            
        Returns:
            Configuration dictionary
            
        Raises:
            MissingConfigurationError: If configuration not found
        """
        # Try exact match first
        if resource_type in self._config_cache:
            config = self._config_cache[resource_type]
            
            # Check version if specified
            if fhir_version:
                config_version = config.get('fhir_version', DEFAULT_FHIR_VERSION)
                if config_version != fhir_version:
                    raise MissingConfigurationError(
                        f"Configuration for {resource_type} found but version mismatch: "
                        f"requested {fhir_version}, found {config_version}"
                    )
            
            return config
        
        # Not found
        raise MissingConfigurationError(
            f"No configuration found for resource: {resource_type}"
        )
    
    def has_config(self, resource_type: str) -> bool:
        """Check if configuration exists for a resource type."""
        return resource_type in self._config_cache
    
    def list_resources(self) -> List[str]:
        """List all configured resource types."""
        return list(self._config_cache.keys())
    
    def _validate_config(self, config: Dict[str, Any], path: str) -> None:
        """
        Validate configuration structure.
        
        Args:
            config: Configuration dictionary
            path: File path (for error messages)
            
        Raises:
            ValidationError: If configuration is invalid
        """
        # Required fields
        if 'resource' not in config:
            raise ValidationError(f"Missing required field 'resource' in {path}")
        
        if 'search_parameters' not in config and 'parameters' not in config:
            raise ValidationError(
                f"Missing required field 'search_parameters' in {path}"
            )
        
        # Use search_parameters if present, otherwise fall back to parameters (backward compat)
        params_section = config.get('search_parameters') or config.get('parameters')
        
        # Validate FHIR version if present
        fhir_version = config.get('fhir_version')
        if fhir_version and fhir_version not in FHIR_VERSIONS:
            raise ValidationError(
                f"Invalid FHIR version '{fhir_version}' in {path}. "
                f"Must be one of: {', '.join(FHIR_VERSIONS)}"
            )
        
        # Validate search parameters
        if not isinstance(params_section, dict):
            raise ValidationError(
                f"'search_parameters' must be a dictionary in {path}"
            )
        
        for param_name, param_config in params_section.items():
            self._validate_parameter(param_name, param_config, path)
        
        # Validate denormalization section if present
        if 'denormalization' in config:
            self._validate_denormalization(config['denormalization'], path)
    
    def _validate_parameter(
        self, 
        param_name: str, 
        param_config: Dict[str, Any], 
        path: str
    ) -> None:
        """Validate a single parameter configuration."""
        # Required fields
        if 'type' not in param_config:
            raise ValidationError(
                f"Parameter '{param_name}' missing required field 'type' in {path}"
            )
        
        # Validate parameter type
        param_type = param_config['type']
        if param_type not in PARAMETER_TYPES:
            raise ValidationError(
                f"Invalid parameter type '{param_type}' for '{param_name}' in {path}. "
                f"Must be one of: {', '.join(PARAMETER_TYPES)}"
            )

        # Composite parameters declare sub-parameters via 'components'
        # (handled by CompositeConverter); all other types use 'fields'.
        if param_type == 'composite':
            if 'components' not in param_config:
                raise ValidationError(
                    f"Composite parameter '{param_name}' missing required field "
                    f"'components' in {path}"
                )
            components = param_config['components']
            if not isinstance(components, list) or not components:
                raise ValidationError(
                    f"Composite parameter '{param_name}' 'components' must be a "
                    f"non-empty list in {path}"
                )
            for i, component in enumerate(components):
                if not isinstance(component, dict):
                    raise ValidationError(
                        f"Composite parameter '{param_name}' component[{i}] must be "
                        f"a dictionary in {path}"
                    )
                if 'type' not in component:
                    raise ValidationError(
                        f"Composite parameter '{param_name}' component[{i}] missing "
                        f"required field 'type' in {path}"
                    )
            return

        if 'fields' not in param_config:
            raise ValidationError(
                f"Parameter '{param_name}' missing required field 'fields' in {path}"
            )
        
        # Validate fields structure
        fields = param_config['fields']
        if not isinstance(fields, (list, dict)):
            raise ValidationError(
                f"Parameter '{param_name}' 'fields' must be a list or dict in {path}"
            )
    
    def _validate_denormalization(
        self, 
        denorm_config: Dict[str, Any], 
        path: str
    ) -> None:
        """Validate denormalization configuration."""
        if not isinstance(denorm_config, dict):
            raise ValidationError(
                f"'denormalization' must be a dictionary in {path}"
            )
        
        for field_name, field_config in denorm_config.items():
            if not isinstance(field_config, dict):
                raise ValidationError(
                    f"Denormalization config for '{field_name}' must be a dictionary in {path}"
                )
            
            # Check for required extractor
            if 'extractor' not in field_config:
                raise ValidationError(
                    f"Denormalization config for '{field_name}' missing 'extractor' in {path}"
                )
            
            # Validate field_mappings if present
            if 'field_mappings' in field_config:
                field_mappings = field_config['field_mappings']
                if not isinstance(field_mappings, list):
                    raise ValidationError(
                        f"field_mappings for '{field_name}' must be a list in {path}"
                    )
                
                for mapping in field_mappings:
                    if not isinstance(mapping, dict):
                        raise ValidationError(
                            f"Each field_mapping must be a dictionary in {path}"
                        )
                    
                    # Required fields in mapping
                    if 'target_field' not in mapping:
                        raise ValidationError(
                            f"field_mapping missing 'target_field' in {path}"
                        )
    
    def reload(self) -> None:
        """Reload all configurations from disk."""
        self._config_cache.clear()
        if self._config_dirs:
            self._load_all_configs()
        elif self.config_path:
            self._load_single_config(self.config_path)
    
    def get_denormalization_rules(self, resource_type: str) -> Dict[str, Any]:
        """Get denormalization rules for a resource."""
        config = self.get_config(resource_type)
        return config.get('denormalization', {})
    
    def get_search_parameters(self, resource_type: str) -> Dict[str, Any]:
        """Get search parameters configuration for a resource."""
        config = self.get_config(resource_type)
        # Support both 'search_parameters' and 'parameters' (backward compat)
        return config.get('search_parameters') or config.get('parameters', {})
