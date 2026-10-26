"""
Resource Denormalizer - Main orchestrator for FHIR resource denormalization.

This module coordinates the denormalization process, loading configurations
and applying extractors to transform FHIR resources into optimized MongoDB documents.
"""

from typing import Any, Dict, Optional, List
import json
import sys
from pathlib import Path

from fhir_search_to_mql.core.config_loader import ConfigLoader
from fhir_search_to_mql.core.exceptions import (
    DenormalizationError,
    ConfigurationError,
    MissingConfigurationError,
)
from fhir_search_to_mql.core.constants import DEFAULT_SEARCH_TARGET
from fhir_search_to_mql.temporal import build_date_projections

# Import extractors from extractors package
from fhir_search_to_mql.denormalizer.extractors import (
    HumanNameExtractor,
    CodeableConceptExtractor,
    ReferenceExtractor,
    IdentifierExtractor,
    ContactPointExtractor,
    AddressExtractor,
    QuantityExtractor,
    PeriodExtractor,
    TimingExtractor,
    RangeExtractor,
    RatioExtractor,
    RatioRangeExtractor,
    CodingExtractor,
    ExtensionExtractor,
    MoneyExtractor,
    AgeDurationExtractor,
    DosageExtractor,
    AvailabilityExtractor,
    PhoneticExtractor,
    TextExtractor,
    DirectFieldExtractor,
    CompartmentMembershipExtractor,
)


class ResourceDenormalizer:
    """
    Main denormalizer for FHIR resources.
    
    Loads configuration and applies extractors to denormalize FHIR resources
    for optimized MongoDB querying.
    
    KEY PRINCIPLE: 100% Configuration-Driven
    - Only denormalizes fields explicitly listed in configuration
    - No automatic denormalization based on field types
    - Empty denormalization section = no _search fields generated
    """
    
    # Registry of available extractors (21 total: 18 FHIR data-type extractors
    # + PhoneticExtractor (Soundex for HumanName phonetic search) +
    # TextExtractor (resource-agnostic free-text concat / lowercase) +
    # CompartmentMembershipExtractor (precompute `_compartments.<Type>`
    # fields that power the CompartmentResolver fast-path)).
    #
    # All extractors are data-type-specific or generic capability extractors —
    # NO resource-specific extractors. Cross-cutting denormalizations (e.g.
    # Observation's combo-* / component-* aggregates, Patient compartment
    # precompute) are expressed by setting ``source: $resource`` on the rule
    # and using path expressions in ``source_path`` against the relevant
    # generic extractor.
    EXTRACTORS = {
        'HumanNameExtractor': HumanNameExtractor,
        'CodeableConceptExtractor': CodeableConceptExtractor,
        'ReferenceExtractor': ReferenceExtractor,
        'IdentifierExtractor': IdentifierExtractor,
        'ContactPointExtractor': ContactPointExtractor,
        'AddressExtractor': AddressExtractor,
        'QuantityExtractor': QuantityExtractor,
        'PeriodExtractor': PeriodExtractor,
        'TimingExtractor': TimingExtractor,
        'RangeExtractor': RangeExtractor,
        'RatioExtractor': RatioExtractor,
        'RatioRangeExtractor': RatioRangeExtractor,
        'CodingExtractor': CodingExtractor,
        'ExtensionExtractor': ExtensionExtractor,
        'MoneyExtractor': MoneyExtractor,
        'AgeDurationExtractor': AgeDurationExtractor,
        'DosageExtractor': DosageExtractor,
        'AvailabilityExtractor': AvailabilityExtractor,
        'PhoneticExtractor': PhoneticExtractor,
        'TextExtractor': TextExtractor,
        'DirectFieldExtractor': DirectFieldExtractor,
        'CompartmentMembershipExtractor': CompartmentMembershipExtractor,
    }
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        config_dir: Optional[Any] = None,
    ):
        """
        Initialize the resource denormalizer.

        Args:
            config_path: Path to a specific resource mapping file.
            config_dir: A single directory path or a list of paths
                containing resource mapping YAMLs. When omitted, the
                YAML configs bundled with this package are used (so
                ``ResourceDenormalizer()`` "just works" after a
                ``pip install``).

        Raises:
            ConfigurationError: If configuration cannot be loaded.
        """
        # Always initialize a config_loader. If neither path nor dir
        # is supplied, ConfigLoader falls back to the configs bundled
        # in the package (importlib.resources resolution).
        self.config_loader = ConfigLoader(
            config_path=config_path, config_dir=config_dir
        )
        self._extractor_cache: Dict[str, Any] = {}
    
    def denormalize(
        self,
        resource: Dict[str, Any],
        warnings: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Denormalize a FHIR resource by adding ``_search`` (and any
        other configured top-level buckets such as ``_compartments``).

        Per-field rule failures are intentionally non-fatal: they are
        logged as warnings and the remaining rules continue. This
        keeps a single bad rule (e.g. an extractor typo in a YAML
        config) from blocking an entire bulk re-denormalization run.

        Args:
            resource: FHIR resource dictionary.
            warnings: Optional list to which per-field failure
                messages are appended. Callers (notably
                :class:`MongoDBHandler`) use this to count
                ``field_failures`` and ``documents_with_field_failures``
                in their bulk-operation stats so the final progress
                report reflects every rule that didn't write its
                target — instead of only counting documents that
                crashed outright.

        Returns:
            Resource with denormalization buckets added (original
            resource is NOT mutated).

        Raises:
            DenormalizationError: If denormalization fails outright
                (resource missing ``resourceType``).
            MissingConfigurationError: If no configuration for
                resource type.
        """
        result = resource.copy()

        resource_type = resource.get('resourceType')
        if not resource_type:
            raise DenormalizationError("Resource missing 'resourceType' field")

        if not self.config_loader:
            return result

        try:
            config = self.config_loader.get_config(resource_type)
        except MissingConfigurationError:
            # No configuration → return unchanged. This is a normal
            # outcome for resources we don't denormalize (e.g.
            # `Bundle`), not a failure to record.
            return result

        denorm_rules = config.get('denormalization', {})

        # Buckets for top-level denormalization containers. ``_search`` is
        # the canonical bucket; rules may opt into additional buckets (e.g.
        # ``_compartments`` for compartment membership) by setting
        # ``target: <bucket>`` or ``target: <bucket>.<sub>`` on the rule.
        buckets: Dict[str, Dict[str, Any]] = {DEFAULT_SEARCH_TARGET: {}}

        for field_name, rule in denorm_rules.items():
            try:
                source = rule.get('source', field_name)

                if source == '$resource':
                    field_value = resource
                else:
                    if source not in resource:
                        continue
                    field_value = resource[source]
                    if field_value is None:
                        continue

                extractor_name = rule.get('extractor')
                if not extractor_name:
                    continue

                extractor = self._get_extractor(extractor_name)
                if not extractor:
                    # Unknown extractor → record as a real per-field
                    # failure, not a silent skip. This is what
                    # surfaced the `CustomExtractor` typo in
                    # Appointment.yaml.
                    msg = (
                        f"Failed to denormalize field '{field_name}': "
                        f"unknown extractor '{extractor_name}'"
                    )
                    self._record_warning(msg, warnings)
                    continue

                field_mappings = rule.get('field_mappings', [])

                extracted = extractor.extract(field_value, field_mappings=field_mappings)

                for mapping in field_mappings:
                    target_field = mapping.get('target_field')
                    expected_type = mapping.get('datatype')
                    if target_field in extracted and expected_type:
                        self._validate_datatype(
                            extracted[target_field],
                            expected_type,
                            f"{field_name}.{target_field}"
                        )

                self._merge_into_buckets(buckets, rule, extracted)

            except Exception as e:
                msg = f"Failed to denormalize field '{field_name}': {str(e)}"
                self._record_warning(msg, warnings)
                continue

        # Promote each non-empty bucket onto the result document. ``_search``
        # keeps its existing semantics; other buckets (``_compartments`` etc.)
        # appear as siblings of ``_search`` so they index independently.
        for bucket_name, bucket_value in buckets.items():
            if bucket_value:
                result[bucket_name] = bucket_value

        # Keep canonical FHIR temporal strings untouched and create internal
        # BSON-date intervals after all configured Period/Timing projections
        # have been materialized.
        date_projections = build_date_projections(result, config)
        if date_projections:
            result.setdefault(DEFAULT_SEARCH_TARGET, {})["_dates"] = date_projections

        return result

    @staticmethod
    def _record_warning(
        message: str, warnings: Optional[List[str]]
    ) -> None:
        """
        Emit a per-field warning to stderr AND record it in the
        caller's collector list.

        We intentionally route the human-readable copy through the
        same logger used elsewhere (stderr) so existing operators
        that scrape logs keep working, while bulk callers that pass
        ``warnings=[]`` get a structured count for their stats
        report.
        """
        # stderr (operator visibility) — keep the historical "Warning:" prefix
        # so log scrapers that match on it continue to fire.
        print(f"Warning: {message}", file=sys.stderr)
        if warnings is not None:
            warnings.append(message)

    def _merge_into_buckets(
        self,
        buckets: Dict[str, Dict[str, Any]],
        rule: Dict[str, Any],
        extracted: Dict[str, Any],
    ) -> None:
        """
        Route ``extracted`` into the right top-level bucket based on the
        rule's ``target`` directive.

        - ``target: _search`` (default) → merge into the ``_search`` dict.
        - ``target: _search.<sub>`` → nested under ``_search.<sub>``.
        - ``target: _compartments`` (or any other top-level name) →
          merge into a sibling bucket on the result document.
        - ``target: <bucket>.<sub>`` → nested under that bucket.

        Dotted ``target_field`` names inside ``extracted`` (e.g.
        ``"appointmentPeriod.start"``) are auto-nested into a
        sub-document. This lets configs project Period-shaped data
        into ``_search.<periodField>.<start|end>`` without having to
        invent flat names like ``appointmentPeriodStart`` — and it
        keeps the data queryable as a nested object via
        ``$elemMatch``-style operators if needed.

        Buckets are auto-created on first write so resource configs
        can introduce new top-level groupings without code changes.
        """
        target = rule.get('target', DEFAULT_SEARCH_TARGET)
        bucket_name, _, nested_path = target.partition('.')
        if bucket_name not in buckets:
            buckets[bucket_name] = {}
        bucket = buckets[bucket_name]
        if nested_path:
            self._set_nested(bucket, nested_path, extracted)
        else:
            self._merge_extracted(bucket, extracted)

    def _merge_extracted(
        self, bucket: Dict[str, Any], extracted: Dict[str, Any]
    ) -> None:
        """
        Merge an extractor's output dict into a bucket.

        Keys with dots are nested via :meth:`_set_nested`; flat keys
        are written directly.

        Sparse-output guarantee: values that carry no searchable
        information are silently dropped so ``_search`` (and
        ``_compartments``) never contain empty placeholder fields.
        Skipped cases:
          - ``None`` — extractor explicitly found nothing
          - ``[]``   — extractor traversed the source path but collected
                       zero items (e.g. CodeableConcept with no codings)

        Intentionally NOT skipped: ``0``, ``False``, ``""`` — these are
        legitimate searchable values for numeric, boolean and string
        fields respectively.
        """
        for key, value in extracted.items():
            if value is None:
                continue
            if isinstance(value, list) and len(value) == 0:
                continue
            if '.' in key:
                self._set_nested(bucket, key, value)
            else:
                bucket[key] = value

    def denormalize_with_config(
        self,
        resource: Dict[str, Any],
        config: Dict[str, Any],
        warnings: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Denormalize a FHIR resource using an explicit configuration
        dictionary (bypassing :class:`ConfigLoader`).

        Args:
            resource: FHIR resource dictionary.
            config: Configuration dictionary with denormalization
                rules (the same shape as a parsed YAML config).
            warnings: Optional list to which per-field failure
                messages are appended. See :meth:`denormalize` for
                the rationale — keeps the bulk-stats reporting
                accurate when callers iterate this method directly
                (e.g. tests, ad-hoc scripts).

        Returns:
            Resource with denormalization buckets added (original
            resource is NOT mutated).

        Raises:
            DenormalizationError: If denormalization fails outright.
        """
        result = resource.copy()

        denorm_rules = config.get('denormalization', {})
        if not denorm_rules:
            return result

        # See `denormalize` for the bucket model — same logic applies here.
        buckets: Dict[str, Dict[str, Any]] = {DEFAULT_SEARCH_TARGET: {}}

        for field_name, rule in denorm_rules.items():
            try:
                source = rule.get('source', field_name)

                # Sentinel `$resource` lets an extractor see the full FHIR
                # resource. This is the only clean way to compute combos
                # spanning multiple top-level fields (e.g. Observation's
                # combo-code aggregating `code` + `component[*].code`).
                if source == '$resource':
                    field_value = resource
                else:
                    if source not in resource:
                        continue
                    field_value = resource[source]
                    if field_value is None:
                        continue

                extractor_name = rule.get('extractor')
                if not extractor_name:
                    continue

                extractor = self._get_extractor(extractor_name)
                if not extractor:
                    msg = (
                        f"Failed to denormalize field '{field_name}': "
                        f"unknown extractor '{extractor_name}'"
                    )
                    self._record_warning(msg, warnings)
                    continue

                field_mappings = rule.get('field_mappings', [])

                extracted = extractor.extract(field_value, field_mappings=field_mappings)

                for mapping in field_mappings:
                    target_field = mapping.get('target_field')
                    expected_type = mapping.get('datatype')
                    if target_field in extracted and expected_type:
                        self._validate_datatype(
                            extracted[target_field],
                            expected_type,
                            f"{field_name}.{target_field}"
                        )

                self._merge_into_buckets(buckets, rule, extracted)

            except Exception as e:
                msg = f"Failed to denormalize field '{field_name}': {str(e)}"
                self._record_warning(msg, warnings)
                continue

        for bucket_name, bucket_value in buckets.items():
            if bucket_value:
                result[bucket_name] = bucket_value

        return result

    def denormalize_from_file(self, file_path: str) -> Dict[str, Any]:
        """
        Load a FHIR resource from JSON file and denormalize it.
        
        Args:
            file_path: Path to JSON file containing FHIR resource
            
        Returns:
            Denormalized resource
            
        Raises:
            DenormalizationError: If file cannot be loaded or denormalization fails
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                resource = json.load(f)
            
            return self.denormalize(resource)
            
        except FileNotFoundError:
            raise DenormalizationError(f"File not found: {file_path}")
        except json.JSONDecodeError as e:
            raise DenormalizationError(f"Invalid JSON in {file_path}: {str(e)}")
        except Exception as e:
            raise DenormalizationError(f"Error processing {file_path}: {str(e)}")
    
    def denormalize_from_folder(
        self, 
        folder_path: str, 
        resource_type: Optional[str] = None,
        pattern: str = "*.json",
        recursive: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Denormalize all FHIR resources from a folder.
        
        Args:
            folder_path: Path to folder containing JSON files
            resource_type: Optional filter by resource type
            pattern: File pattern (default: *.json)
            recursive: Search subdirectories (default: False)
            
        Returns:
            List of denormalized resources
            
        Raises:
            DenormalizationError: If folder cannot be accessed
        """
        path = Path(folder_path)
        if not path.exists():
            raise DenormalizationError(f"Folder not found: {folder_path}")
        
        if not path.is_dir():
            raise DenormalizationError(f"Not a directory: {folder_path}")
        
        # Find all matching files
        if recursive:
            files = list(path.rglob(pattern))
        else:
            files = list(path.glob(pattern))
        
        results = []
        for file_path in files:
            try:
                resource = self.denormalize_from_file(str(file_path))
                
                # Filter by resource type if specified
                if resource_type and resource.get('resourceType') != resource_type:
                    continue
                
                results.append(resource)
                
            except Exception as e:
                print(f"Warning: Failed to process {file_path}: {str(e)}")
                continue
        
        return results
    
    def _get_extractor(self, extractor_name: str):
        """Get an extractor instance (cached)."""
        if extractor_name not in self._extractor_cache:
            extractor_class = self.EXTRACTORS.get(extractor_name)
            if not extractor_class:
                raise DenormalizationError(f"Unknown extractor: {extractor_name}")
            self._extractor_cache[extractor_name] = extractor_class()
        
        return self._extractor_cache[extractor_name]
    
    def _validate_datatype(self, value: Any, expected_type: str, field_path: str) -> None:
        """Validate that a value matches the expected datatype."""
        if expected_type == 'string' and not isinstance(value, str):
            raise DenormalizationError(
                f"{field_path}: expected string, got {type(value).__name__}"
            )
        elif expected_type == 'number' and not isinstance(value, (int, float)):
            raise DenormalizationError(
                f"{field_path}: expected number, got {type(value).__name__}"
            )
        elif expected_type == 'boolean' and not isinstance(value, bool):
            raise DenormalizationError(
                f"{field_path}: expected boolean, got {type(value).__name__}"
            )
        elif expected_type == 'array[string]':
            if not isinstance(value, list):
                raise DenormalizationError(
                    f"{field_path}: expected array[string], got {type(value).__name__}"
                )
            if not all(isinstance(x, str) for x in value):
                raise DenormalizationError(
                    f"{field_path}: expected array[string], but array contains non-string values"
                )
        elif expected_type == 'array[number]':
            if not isinstance(value, list):
                raise DenormalizationError(
                    f"{field_path}: expected array[number], got {type(value).__name__}"
                )
            if not all(isinstance(x, (int, float)) for x in value):
                raise DenormalizationError(
                    f"{field_path}: expected array[number], but array contains non-number values"
                )
    
    def _set_nested(self, obj: Dict[str, Any], path: str, value: Any) -> None:
        """Set a value in a nested dictionary using dot notation."""
        parts = path.split('.')
        current = obj
        
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            current = current[part]
        
        current[parts[-1]] = value
    
    def denormalize_from_mongodb(
        self,
        collection,
        query: Optional[Dict[str, Any]] = None,
        batch_size: int = 100,
        update_in_place: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Denormalize FHIR resources from a MongoDB collection.
        
        Args:
            collection: pymongo collection object
            query: MongoDB query filter (default: {} - all documents)
            batch_size: Number of documents to process per batch
            update_in_place: If True, update documents in database; if False, return denormalized docs
            
        Returns:
            List of denormalized resources (empty if update_in_place=True)
            
        Raises:
            DenormalizationError: If MongoDB operation fails
        """
        if query is None:
            query = {}
        
        try:
            # Count total documents for progress tracking
            total = collection.count_documents(query)
            processed = 0
            results = [] if not update_in_place else None
            
            # Process in batches
            cursor = collection.find(query).batch_size(batch_size)
            
            for resource in cursor:
                try:
                    # Denormalize the resource
                    denormalized = self.denormalize(resource)
                    
                    if update_in_place:
                        # Update in database
                        collection.update_one(
                            {'_id': resource['_id']},
                            {'$set': {'_search': denormalized.get('_search', {})}}
                        )
                    else:
                        # Collect results
                        results.append(denormalized)
                    
                    processed += 1
                    
                    # Progress tracking
                    if processed % batch_size == 0:
                        print(f"Progress: {processed}/{total} documents processed")
                
                except Exception as e:
                    print(f"Warning: Failed to denormalize document {resource.get('_id')}: {str(e)}")
                    continue
            
            print(f"Completed: {processed}/{total} documents processed")
            
            return results if not update_in_place else []
            
        except Exception as e:
            raise DenormalizationError(f"MongoDB operation failed: {str(e)}")
    
    def denormalize_field(self, field_path: str, value: Any, resource_type: str) -> Dict[str, Any]:
        """
        Denormalize a specific field value.
        
        Args:
            field_path: Path to the field (e.g., "name", "identifier")
            value: Field value to denormalize
            resource_type: Type of FHIR resource
            
        Returns:
            Dictionary of denormalized fields (empty dict if field is simple/not configured)
            
        Raises:
            DenormalizationError: If denormalization fails
        """
        # Get configuration for resource type
        try:
            config = self.config_loader.get_config(resource_type)
        except MissingConfigurationError:
            # No configuration - field is simple, return empty dict
            return {}
        
        # Get denormalization rules
        denorm_rules = config.get('denormalization', {})
        
        # Check if this field has a denormalization rule
        if field_path not in denorm_rules:
            # Field not in rules - it's a simple field, return empty dict
            return {}
        
        rule = denorm_rules[field_path]
        
        # Get extractor
        extractor_name = rule.get('extractor')
        if not extractor_name:
            return {}
        
        extractor = self._get_extractor(extractor_name)
        
        # Get field mappings
        field_mappings = rule.get('field_mappings', [])
        
        # Extract and return denormalized fields
        try:
            extracted = extractor.extract(value, field_mappings=field_mappings)
            
            # Validate datatypes
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                expected_type = mapping.get('datatype')
                if target_field in extracted and expected_type:
                    self._validate_datatype(
                        extracted[target_field],
                        expected_type,
                        f"{field_path}.{target_field}"
                    )
            
            return extracted
            
        except Exception as e:
            raise DenormalizationError(f"Failed to denormalize field '{field_path}': {str(e)}")
    
    def validate(self, resource: Dict[str, Any]) -> bool:
        """
        Validate a denormalized resource.
        
        Args:
            resource: Denormalized resource to validate
            
        Returns:
            True if valid
            
        Raises:
            ValidationError: If validation fails
        """
        from fhir_search_to_mql.core.exceptions import ValidationError
        
        # Check resourceType exists
        if 'resourceType' not in resource:
            raise ValidationError("Resource missing 'resourceType' field")
        
        resource_type = resource['resourceType']
        
        # Get configuration
        try:
            config = self.config_loader.get_config(resource_type)
        except MissingConfigurationError:
            # No config - can't validate, but not an error
            return True
        
        # Get denormalization rules
        denorm_rules = config.get('denormalization', {})
        
        # If no rules, resource doesn't need _search field
        if not denorm_rules:
            return True
        
        # If rules exist, check if _search field exists
        if '_search' not in resource:
            # Check if any source fields exist that should be denormalized
            for field_name, rule in denorm_rules.items():
                source = rule.get('source', field_name)
                if source in resource and resource[source] is not None:
                    raise ValidationError(
                        f"Resource has field '{source}' but missing '_search' denormalized data"
                    )
            return True
        
        search_fields = resource['_search']
        
        # Validate datatypes of denormalized fields
        for field_name, rule in denorm_rules.items():
            field_mappings = rule.get('field_mappings', [])
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                expected_type = mapping.get('datatype')
                optional = mapping.get('optional', False)
                
                if target_field in search_fields:
                    # Field exists - validate type
                    try:
                        self._validate_datatype(
                            search_fields[target_field],
                            expected_type,
                            f"_search.{target_field}"
                        )
                    except DenormalizationError as e:
                        raise ValidationError(str(e))
                elif not optional:
                    # Required field missing
                    source = rule.get('source', field_name)
                    if source in resource and resource[source] is not None:
                        raise ValidationError(
                            f"Required denormalized field '_search.{target_field}' is missing "
                            f"(source field '{source}' exists)"
                        )
        
        return True
    
    def get_denormalization_rules(self, resource_type: str) -> Dict[str, Any]:
        """Get denormalization rules for a resource type."""
        return self.config_loader.get_denormalization_rules(resource_type)
