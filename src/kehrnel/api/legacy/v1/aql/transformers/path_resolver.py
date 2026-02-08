# src/kehrnel/api/legacy/v1/aql/transformers/path_resolver.py
import re
from typing import Tuple, Dict


class PathResolver:
    """
    Handles AQL path resolution and translation to MongoDB field paths.
    """

    def __init__(self, context_map: Dict[str, Dict], ehr_alias: str, composition_alias: str, schema_config: Dict[str, str]):
        self.context_map = context_map
        self.ehr_alias = ehr_alias
        self.composition_alias = composition_alias
        self.schema_config = schema_config

    def translate_aql_path(self, aql_path: str) -> Tuple[str, str]:
        """
        Splits a full AQL path into a regex for the 'p' field and a dot-notation path for the 'data' field.
        Example: 'admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string'
        -> p_regex: 'at0014/at0007' (path parts only, will be combined with archetype regex later)
        -> data_path: 'data.value.defining_code.code_string'
        """
        parts = aql_path.split('/')[1:]  # remove variable alias
        
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

    def build_full_path_regex(self, variable: str) -> str:
        """Builds the hierarchical regex for an alias using the context map."""
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
        """Combines base path regex with specific path prefix."""
        if path_prefix and base_regex.startswith("^"):
            # Remove trailing slash from path_prefix to avoid double slashes
            clean_prefix = path_prefix.rstrip('/')
            # Only replace the first ^ to avoid corrupting character classes like [^/]
            return f"^{clean_prefix}/" + base_regex[1:]
        else:
            return base_regex

    def map_ehr_path_to_field(self, field_path: str) -> str:
        """
        Maps EHR-level field paths to MongoDB field names.
        """
        if field_path == "ehr_id/value":
            return "ehr_id"
        # Add more EHR field mappings as needed
        return field_path.replace('/', '.')

    def resolve_path_to_mongo_field(self, path: str) -> str:
        """
        Resolves an AQL path to a MongoDB field reference.
        This handles the semi-flattened schema field mapping.
        """
        if not path:
            return ""
            
        # This is similar to existing path resolution logic
        # For now, using a simplified approach
        if "/" in path:
            parts = path.split("/")
            alias = parts[0]
            
            if alias == self.ehr_alias:
                # EHR field mapping
                field_path = "/".join(parts[1:])
                mapped_field = self.map_ehr_path_to_field(field_path)
                return f"${mapped_field}"
            elif alias == self.composition_alias:
                # Composition field mapping via cn array
                field_path = "/".join(parts[1:])
                # For LET expressions, we need to use a different approach since we don't have cn_matched yet
                return f"$cn.{self.schema_config['data_field']}.{field_path.replace('/', '.')}"
        
        return f"${path}"
