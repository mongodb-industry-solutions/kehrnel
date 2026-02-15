# src/kehrnel/api/compatibility/v1/aql/transformers/format_resolver.py
import re
from typing import Tuple, Dict, Optional
from .archetype_resolver import ArchetypeResolver


class FormatResolver:
    """
    Handles path resolution for both full-path and shortened-path formats.
    """

    def __init__(self, context_map: Dict[str, Dict], ehr_alias: str, composition_alias: str, schema_config: Dict[str, str], archetype_resolver: Optional[ArchetypeResolver] = None):
        self.context_map = context_map
        self.ehr_alias = ehr_alias
        self.composition_alias = composition_alias
        self.schema_config = schema_config
        self.format = schema_config.get('format', 'full')
        self.archetype_resolver = archetype_resolver

    async def translate_aql_path(self, aql_path: str) -> Tuple[str, str]:
        """
        Translates AQL path based on the detected format.
        
        For full format: Returns (p_regex, data_path) for cn array filtering
        For shortened format: Returns (None, direct_path) for direct field access
        """
        if self.format == 'shortened':
            return await self._translate_shortened_path(aql_path)
        else:
            return self._translate_full_path(aql_path)

    async def _translate_shortened_path(self, aql_path: str) -> Tuple[str, str]:
        """
        Translates AQL path for shortened format.
        Shortened format can have either:
        1. cn array with short p paths (like p:'7')
        2. Direct nested structure
        """
        parts = aql_path.split('/')[1:]  # remove variable alias
        variable_alias = aql_path.split('/')[0]
        
        # Handle variable-specific path mapping dynamically
        # Check if any part contains archetype references (AT codes like [at0001] or full archetype IDs like [openEHR-EHR-CLUSTER.name.v0])
        has_archetype_refs = any(re.search(r'\[(?:at\d+|openEHR-[^\]]+)\]', part) for part in parts)
        

        
        # Handle archetype references FIRST, regardless of variable type
        if has_archetype_refs:
            # Check if we can resolve this as a nested archetype path
            if self.archetype_resolver:
                nested_pattern = await self.archetype_resolver.resolve_nested_path_to_p_pattern(
                    variable_alias, parts, self.context_map
                )

                if nested_pattern:
                    # Build data path from remaining non-archetype parts
                    remaining_parts = []
                    for part in parts:
                        # Keep only parts that are NOT archetype references AND not navigation paths
                        # Skip parts like: 
                        # - other_context[at0001], items[openEHR-EHR-CLUSTER.name.v0], items[at0003] (archetype references)
                        # - context (navigation path to archetype references)
                        # But keep regular field names like: value, defining_code, code_string
                        if not re.search(r'\[(?:at\d+|openEHR-[^\]]+)\]', part) and part not in ['context', 'description', 'data', 'state', 'protocol', 'activities', 'events']:
                            remaining_parts.append(part)
                    
                    if remaining_parts:
                        data_path = f"data.{'.'.join(remaining_parts)}"
                    else:
                        data_path = "data"
                    
                    return nested_pattern, data_path
                else:
                    # If we can't resolve the nested pattern, use basic data path
                    remaining_parts = []
                    for part in parts:
                        if not re.match(r"items\[(.+)\]", part):
                            remaining_parts.append(part)
                    
                    if remaining_parts:
                        data_path = f"data.{'.'.join(remaining_parts)}"
                    else:
                        data_path = "data"
                    
                    # Try to get just the base pattern for the variable
                    base_pattern = await self.archetype_resolver.resolve_variable_to_p_pattern(
                        variable_alias, self.context_map
                    )
                    return base_pattern, data_path
            else:

                # No archetype resolver available - return None to indicate direct field access
                remaining_parts = []
                for part in parts:
                    if not re.match(r"items\[(.+)\]", part):
                        remaining_parts.append(part)
                
                if remaining_parts:
                    data_path = f"data.{'.'.join(remaining_parts)}"
                else:
                    data_path = "data"
                
                return None, data_path

        # AFTER archetype handling, check for composition-specific simple paths
        if variable_alias == self.composition_alias:
            # For composition-level paths like c/uid/value, c/name/value
            # In shortened format, some fields are at document root, others in cn array
            if len(parts) >= 1:
                if parts[0] == "uid":
                    # Composition UID is stored as comp_id at document root level
                    return None, "comp_id"  # Direct field access from document root
                elif parts[0] == "name":
                    # Get dynamic composition p-pattern
                    comp_pattern = await self._get_composition_p_pattern()
                    return comp_pattern, "data.name.value"  # Still in cn array at composition root
                elif parts[0] == "archetype_node_id":
                    comp_pattern = await self._get_composition_p_pattern()
                    return comp_pattern, "data.archetype_node_id"  # Still in cn array at composition root
                else:
                    # For other composition-level fields WITHOUT archetype references
                    data_path = "data." + ".".join(parts)
                    comp_pattern = await self._get_composition_p_pattern()
                    return comp_pattern, data_path
            else:
                # Just the composition itself
                comp_pattern = await self._get_composition_p_pattern()
                return comp_pattern, "data"
        else:
            # Generic path handling for all other variables
            if len(parts) > 0:
                if parts[0] == "time":
                    data_path = "data.time.value"
                elif parts[0] == "other_participations":
                    data_path = f"data.{'.'.join(parts)}"
                elif parts[0] == "description":
                    # Handle description paths
                    desc_parts = parts[1:]  # Skip 'description'
                    if desc_parts and desc_parts[0].startswith("["):
                        # Handle description[at0017]/items/...
                        desc_path = self._handle_description_path(desc_parts)
                        data_path = f"data.description.{desc_path}"
                    else:
                        data_path = f"data.description.{'.'.join(desc_parts)}"
                elif parts[0] == "ism_transition":
                    data_path = f"data.{'.'.join(parts)}"
                else:
                    data_path = f"data.{'.'.join(parts)}"
            else:
                data_path = "data"
        
        # For shortened format with cn array, we need to return a p regex pattern
        # This will be used to filter the cn array elements
        if variable_alias == self.composition_alias:
            comp_pattern = await self._get_composition_p_pattern()
            return comp_pattern, data_path  # Already handled above
        else:
            # Use dynamic archetype resolution
            if self.archetype_resolver:
                p_pattern = await self.archetype_resolver.resolve_variable_to_p_pattern(
                    variable_alias, self.context_map
                )

                return p_pattern, data_path
            else:
                # Fallback if no archetype resolver available
                return None, data_path
    
    async def _get_composition_p_pattern(self) -> str:
        """
        Get the p-pattern for composition dynamically or fallback to default.
        """
        if self.archetype_resolver:
            pattern = await self.archetype_resolver.resolve_composition_p_pattern(
                self.composition_alias, self.context_map
            )
            return pattern
        else:
            # Fallback to match any composition when no specific predicate
            return "^\\d+$"
    
    def _handle_description_path(self, desc_parts: list) -> str:
        """
        Handle description path navigation for shortened format.
        """
        if not desc_parts:
            return "items"
            
        result_parts = []
        i = 0
        
        while i < len(desc_parts):
            part = desc_parts[i]
            
            if part.startswith("[") and part.endswith("]"):
                # Skip archetype node references like [at0017]
                i += 1
                continue
            elif part == "items":
                result_parts.append("items")
                # Check if next part is an archetype reference
                if i + 1 < len(desc_parts) and desc_parts[i + 1].startswith("["):
                    archetype_ref = desc_parts[i + 1]
                    # Map archetype references to indices
                    if "[openEHR-EHR-CLUSTER.medication.v2]" in archetype_ref:
                        result_parts.append("1")  # Index for medication cluster
                    elif "[at0132]" in archetype_ref:
                        result_parts.append("0")  # Index for name element
                    elif "[at0150]" in archetype_ref:
                        result_parts.append("1")  # Index for batch element
                    elif "[at0003]" in archetype_ref:
                        result_parts.append("2")  # Index for expiry element
                    else:
                        result_parts.append("0")  # Default index
                    i += 2  # Skip both 'items' and archetype reference
                    continue
                else:
                    i += 1
            else:
                result_parts.append(part)
                i += 1
        
        return ".".join(result_parts)

    def _translate_full_path(self, aql_path: str) -> Tuple[str, str]:
        """
        Original translation logic for full format.
        """
        variable_alias = aql_path.split('/')[0]
        parts = aql_path.split('/')[1:]  # remove variable alias
        
        # Handle composition-level paths specially
        if variable_alias == self.composition_alias:
            # For composition-level paths like c/uid/value, c/name/value
            # We need to find the root composition element in cn array
            # The root composition typically has p matching the template archetype ID
            if len(parts) >= 1:
                if parts[0] == "uid":
                    # Look for root composition in cn array - match any composition p-value
                    return "^\\d+$", "data.uid.value"
                elif parts[0] == "name":
                    return "^\\d+$", "data.name.value"
                elif parts[0] == "archetype_node_id":
                    return "^\\d+$", "data.archetype_node_id"
                else:
                    # For other composition-level fields
                    data_path = "data." + ".".join(parts)
                    return "^\\d+$", data_path
            else:
                # Just the composition itself
                return "^\\d+$", "data"
        
        # Handle other variable paths
        p_parts = []
        data_parts = ["data"]

        for part in parts:
            match = re.match(r"(.+)\[(.+)\]", part)
            if match:
                # This is a node identifier like 'items[at0001]'
                node_code = match.group(2)
                p_parts.append(node_code)
            else:
                # This is part of the path within the data object
                data_parts.append(part)
        
        p_regex = "/".join(reversed(p_parts)) if p_parts else ""
        
        # Handle simple paths like `med_ac/time` which implies `time.value`
        if len(data_parts) == 2 and data_parts[1] in ["time"]:
            data_parts.append('value')
        
        return p_regex, ".".join(data_parts)

    def _build_items_path(self, parts: list) -> str:
        """
        Builds path for items array access in shortened format.
        """
        # This is a simplified approach - in production you'd want more sophisticated mapping
        return f"data.{'.'.join(parts)}"

    def _map_to_content_structure(self, parts: list) -> str:
        """
        Maps archetype aliases to their structure in shortened format.
        Based on the compositionShortenPath.json structure.
        """
        # For the shortened format, we need to navigate the content structure
        if not parts:
            return "data"
        
        first_part = parts[0]
        
        # Based on the JSON structure provided, map paths appropriately
        if first_part == "items":
            # Handle items arrays using generic approach
            return f"data.{'.'.join(parts)}"
        elif first_part == "time":
            return "data.time.value"
        elif first_part == "other_participations":
            return f"data.{'.'.join(parts)}"
        elif first_part == "description":
            return f"data.{'.'.join(parts)}"
        elif first_part == "ism_transition":
            return f"data.{'.'.join(parts)}"
        else:
            # Default mapping
            return f"data.{'.'.join(parts)}"

    def _convert_archetype_refs(self, parts: list) -> list:
        """
        Converts archetype node references to array indices or field names.
        """
        converted = []
        for part in parts:
            match = re.match(r"(.+)\[(.+)\]", part)
            if match:
                # Convert archetype references to indices or field access
                field_name = match.group(1)
                archetype_code = match.group(2)
                
                # For now, use field name and handle archetype filtering later
                converted.append(field_name)
                # You could add logic here to map specific archetype codes to indices
            else:
                converted.append(part)
        
        return converted

    def _handle_items_array(self, parts: list) -> str:
        """
        Handle items array access for shortened format.
        Based on the structure in compositionShortenPath.json
        """
        # For admin_salut cluster navigation
        if len(parts) >= 3:
            # Pattern: items[at0007]/items[at0014]/...
            # This maps to context.other_context.items[].items[]
            remaining_path = []
            item_index = 0
            
            for i, part in enumerate(parts):
                match = re.match(r"items\[(.+)\]", part)
                if match:
                    archetype_code = match.group(1)
                    
                    # Map specific archetype codes to array indices
                    if archetype_code == "at0007":  # Publishing institution
                        remaining_path.append("items")
                        remaining_path.append("3")  # Index 3 based on JSON structure
                    elif archetype_code == "at0014":  # Publishing centre
                        remaining_path.append("items")
                        remaining_path.append("0")  # Index 0 within that cluster
                    elif archetype_code == "at0010":  # Custodial institution  
                        remaining_path.append("items")
                        remaining_path.append("4")  # Index 4 based on JSON structure
                    elif archetype_code == "at0017":  # Custodial centre
                        remaining_path.append("items") 
                        remaining_path.append("0")  # Index 0 within that cluster
                    else:
                        # Default handling
                        remaining_path.append("items")
                        remaining_path.append(str(item_index))
                        item_index += 1
                else:
                    # Non-archetype parts (like 'value', 'defining_code', etc.)
                    remaining_path.append(part)
            
        return f"data.{'.'.join(parts)}"

    def build_full_path_regex(self, variable: str) -> str:
        """
        Builds path regex - only used for full format.
        """
        if self.format == 'shortened':
            return None  # Not used in shortened format
            
        if variable not in self.context_map:
            raise ValueError(f"Unknown variable alias '{variable}' in query.")
        
        # Get the target archetype for this variable
        target_archetype = self.context_map[variable].get('archetype_id')
        if not target_archetype:
            return ".*"
            
        # Get the composition archetype using dynamic alias
        comp_archetype = self.context_map.get(self.composition_alias, {}).get('archetype_id')
        if not comp_archetype:
            return f"^{re.escape(target_archetype)}.*"
            
        # Build the regex pattern: ^target_archetype(?:/[^/]+)*/composition_archetype$
        target_escaped = re.escape(target_archetype)
        comp_escaped = re.escape(comp_archetype)
        
        return f"^{target_escaped}(?:/[^/]+)*/{comp_escaped}$"

    def combine_path_regex(self, base_regex: str, path_prefix: str) -> str:
        """Combines base path regex with specific path prefix - only for full format."""
        if self.format == 'shortened':
            return None
            
        if path_prefix and base_regex and base_regex.startswith("^"):
            # Remove trailing slash from path_prefix to avoid double slashes
            clean_prefix = path_prefix.rstrip('/')
            # Only replace the first ^ to avoid corrupting character classes like [^/]
            return f"^{clean_prefix}/" + base_regex[1:]
        else:
            return base_regex

    def map_ehr_path_to_field(self, field_path: str) -> str:
        """
        Maps EHR-level field paths to MongoDB field names.
        Same for both formats.
        """
        if field_path == "ehr_id/value":
            return "ehr_id"
        # Add more EHR field mappings as needed
        return field_path.replace('/', '.')

    def resolve_path_to_mongo_field(self, path: str) -> str:
        """
        Resolves an AQL path to a MongoDB field reference.
        Handles both formats appropriately.
        """
        if not path:
            return ""
            
        if "/" in path:
            parts = path.split("/")
            alias = parts[0]
            
            if alias == self.ehr_alias:
                # EHR field mapping - same for both formats
                field_path = "/".join(parts[1:])
                mapped_field = self.map_ehr_path_to_field(field_path)
                return f"${mapped_field}"
            elif alias == self.composition_alias:
                # Composition field mapping
                field_path = "/".join(parts[1:])
                
                if self.format == 'shortened':
                    # Direct field access for shortened format
                    return f"${field_path.replace('/', '.')}"
                else:
                    # cn array access for full format
                    return f"$cn.{self.schema_config['data_field']}.{field_path.replace('/', '.')}"
        
        return f"${path}"