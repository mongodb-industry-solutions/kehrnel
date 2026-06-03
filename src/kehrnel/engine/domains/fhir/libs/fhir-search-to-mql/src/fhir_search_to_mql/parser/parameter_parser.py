"""
Parameter parser for individual FHIR search parameters.

Parses parameters with modifiers and prefixes:
- name=Smith (basic)
- name:exact=Smith (modifier)
- birthdate=ge1980-01-01 (prefix)
"""

from typing import Dict, Any, Optional
import re

from fhir_search_to_mql.core.exceptions import ParsingError, ValidationError
from fhir_search_to_mql.core.constants import PREFIXES, SPECIAL_PARAMETERS


class ParameterParser:
    """
    Parse individual FHIR search parameters.
    
    Extracts:
    - Parameter name
    - Modifier (if present)
    - Prefix (if present)
    - Value
    """
    
    # Regex to extract modifier from parameter name
    MODIFIER_PATTERN = re.compile(r'^([^:]+):(.+)$')
    
    # Regex to extract prefix from value
    PREFIX_PATTERN = re.compile(r'^(' + '|'.join(PREFIXES) + r')(.+)$')
    
    def parse_parameter(
        self, 
        param_name: str, 
        value: str,
        param_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Parse a single parameter.
        
        Args:
            param_name: Parameter name (may include modifier)
            value: Parameter value (may include prefix)
            param_type: Optional parameter type (string, token, date, etc.)
                       If not provided, will attempt to infer from parameter name
            
        Returns:
            Dictionary with parsed parameter components including:
            - name: Base parameter name
            - modifier: Modifier if present (e.g., 'exact', 'contains')
            - prefix: Prefix if present (e.g., 'ge', 'lt')
            - value: Parsed value (without prefix)
            - values: List of values (for comma-separated)
            - type: Parameter type
            - raw_name: Original parameter name
            - raw_value: Original value
            
        Raises:
            ParsingError: If parameter cannot be parsed
            ValidationError: If syntax is invalid
        """
        # Validate parameter name
        if not param_name:
            raise ValidationError("Parameter name cannot be empty")
        
        # Extract modifier from parameter name
        modifier = None
        base_name = param_name
        
        modifier_match = self.MODIFIER_PATTERN.match(param_name)
        if modifier_match:
            base_name = modifier_match.group(1)
            modifier = modifier_match.group(2)
        
        # Extract prefix from value — but ONLY for parameter types that
        # actually accept a comparator prefix per FHIR R5 (number, date,
        # quantity). Per spec §3.1.1.5.1 the prefixes
        # `eq/ne/gt/lt/ge/le/sa/eb/ap` only have meaning for ordered
        # types; for string/token/reference/uri the entire value is
        # taken literally. The previous logic unconditionally peeled
        # the prefix and silently broke string queries whose value
        # happened to start with one of those two-letter pairs — e.g.
        # `family=geraldine` was reduced to `"raldine"` (stripped `ge`),
        # `given=sarah` (after lowercasing) to `"rah"` (stripped `sa`),
        # `family=eq-test` to `"-test"`. Routed prefix extraction by
        # type fixes that without changing the date/number/quantity
        # path (which still extracts the prefix as before).
        prefix = None
        actual_value = value
        prefix_match = self.PREFIX_PATTERN.match(value)

        # Speculatively peel the prefix only if the (possibly-inferred)
        # type accepts one. Two-stage decision because callers commonly
        # invoke without a `param_type` (the query parser does so before
        # the YAML config is consulted) — we still want
        # `birthdate=ge1980` to come out with prefix=`ge`.
        #
        # CRITICAL: do NOT pass `prefix` to `_infer_parameter_type` here.
        # That would create a feedback loop because the inference falls
        # through to "number" whenever a prefix is detected and the
        # name isn't in its date allowlist — a string parameter like
        # `family=geraldine` (regex matches `ge`) would be misclassified
        # as a numeric param and have its prefix stripped, leaving
        # `actual_value="raldine"`. Inferring by NAME first preserves
        # the spec contract ("string params don't accept prefixes").
        speculative_type = param_type
        if speculative_type is None:
            speculative_type = self._infer_parameter_type(
                base_name, modifier, None
            )
        if prefix_match and speculative_type in ("date", "number", "quantity"):
            prefix = prefix_match.group(1)
            actual_value = prefix_match.group(2)

        # Handle comma-separated values (OR logic)
        values = []
        if ',' in actual_value:
            values = [v.strip() for v in actual_value.split(',') if v.strip()]
        else:
            values = [actual_value] if actual_value else []

        # Infer parameter type if not provided. Mirrors the logic that
        # speculative-typed prefix extraction used above so we don't
        # lose the inference (or accidentally flip the type when the
        # prefix turned out to be part of a literal string value).
        if param_type is None:
            param_type = speculative_type
        
        return {
            'name': base_name,
            'modifier': modifier,
            'prefix': prefix,
            'value': actual_value,
            'values': values,  # For comma-separated values (OR logic)
            'type': param_type,
            'raw_name': param_name,
            'raw_value': value,
        }
    
    def _infer_parameter_type(
        self, 
        param_name: str, 
        modifier: Optional[str],
        prefix: Optional[str]
    ) -> str:
        """
        Infer parameter type from parameter name, modifier, and prefix.
        
        Args:
            param_name: Parameter name
            modifier: Modifier if present
            prefix: Prefix if present
            
        Returns:
            Inferred parameter type
        """
        # `_lastUpdated` is the only common parameter that is *both*
        # FHIR-special (starts with `_`) AND prefix-eligible (it's a
        # `date` per the FHIR R5 common-parameters table). We classify
        # it as `date` BEFORE the SPECIAL_PARAMETERS shortcut so the
        # caller's prefix-extraction guard correctly peels `ge`/`le`/…
        # prefixes from values like `_lastUpdated=ge2024-01-01`. All
        # other `_*` specials are token/string-shaped and continue to
        # take the special branch.
        if param_name == "_lastUpdated":
            return "date"

        # Special parameters
        if param_name in SPECIAL_PARAMETERS:
            return "special"

        # Name-first inference. The previous implementation only
        # consulted the param-name allowlists AFTER a `prefix` check,
        # which meant a name like `family=geraldine` (regex falsely
        # detects a `ge` prefix) was classified as `number` and had
        # its prefix stripped, mangling the value. Anchoring the
        # type by NAME first matches the FHIR contract: a string
        # parameter is a string regardless of what its value happens
        # to start with.

        # Common string parameters (FHIR R5 `individual-*` shared
        # params + per-resource string params we ship configs for).
        if param_name in (
            "name", "family", "given", "address", "address-city",
            "address-state", "address-country", "address-postalcode",
            "phonetic", "text",
        ):
            return "string"

        # Common token parameters.
        if param_name in (
            "code", "status", "gender", "active", "identifier",
            "deceased", "language", "communication", "email", "phone",
            "telecom", "address-use",
        ):
            return "token"

        # Common reference parameters.
        if param_name in (
            "subject", "patient", "practitioner", "encounter",
            "performer", "requester", "location", "organization",
            "general-practitioner", "based-on", "part-of",
        ):
            return "reference"

        # Common date parameters — recognized by NAME so we don't
        # depend on the (potentially unreliable) `prefix` signal.
        # Keep this list in sync with `type: date` declarations across
        # every shipped YAML config (run
        # `grep "type: date" src/fhir_search_to_mql/configs/*.yaml`
        # to audit). When the converter is invoked through the YAML
        # path the explicit `param_type` overrides this anyway; this
        # branch only matters for ad-hoc calls into the parser
        # without a resource config.
        if param_name in (
            "birthdate", "date", "period", "_lastUpdated",
            "death-date", "qualification-period", "onset-date",
            "abatement-date", "recorded-date",
            "effective-date", "requested-period", "value-date",
            "expiration-date", "manufacture-date",
            "start", "date-start", "end-date",
            "authored", "occurrence",
        ):
            return "date"

        # If has prefix and we haven't classified by name, the
        # parameter is most likely number or quantity. Date params
        # were already caught above so there's no risk of a false
        # date promotion here.
        if prefix:
            return "number"

        # Based on modifier.
        if modifier in ("exact", "contains"):
            return "string"
        if modifier in ("not", "text", "in", "not-in", "of-type"):
            return "token"
        if modifier == "identifier":
            return "reference"

        # Default to string.
        return "string"
    
    def is_special_parameter(self, param_name: str) -> bool:
        """Check if parameter is a special FHIR parameter (_id, _lastUpdated, etc.)."""
        return param_name.startswith('_') or param_name in SPECIAL_PARAMETERS
    
    def validate_syntax(self, param_name: str, value: str) -> None:
        """
        Validate parameter syntax.
        
        Args:
            param_name: Parameter name
            value: Parameter value
            
        Raises:
            ValidationError: If syntax is invalid
        """
        if not param_name:
            raise ValidationError("Parameter name cannot be empty")
        
        if value is None:
            raise ValidationError(f"Parameter '{param_name}' has no value")
        
        # Check for invalid characters in parameter name
        if not re.match(r'^[a-zA-Z0-9_:.-]+$', param_name):
            raise ValidationError(
                f"Invalid characters in parameter name: {param_name}"
            )
    
    def extract_chaining(self, param_name: str) -> Optional[Dict[str, Any]]:
        """
        Extract chaining information from parameter name.
        
        Example: "subject:Patient.name" -> {base: "subject", type: "Patient", chain: "name"}
        
        Args:
            param_name: Parameter name with potential chaining
            
        Returns:
            Chaining info or None if not chained
        """
        # Check for chaining pattern: param:Type.chainedParam
        if ':' in param_name and '.' in param_name:
            parts = param_name.split(':')
            if len(parts) == 2:
                base = parts[0]
                rest = parts[1]
                
                if '.' in rest:
                    type_chain = rest.split('.', 1)
                    return {
                        'base': base,
                        'type': type_chain[0],
                        'chain': type_chain[1],
                    }
        
        return None
