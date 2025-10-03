# src/api/v1/aql/transformers/archetype_resolver.py

from typing import Dict, Optional, Tuple, List
from motor.motor_asyncio import AsyncIOMotorDatabase
import re


class ArchetypeResolver:
    """
    Dynamically resolves archetype IDs to p-values by querying the _codes collection.
    Replaces hardcoded p-value patterns with database-driven resolution.
    """
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self._archetype_to_code_cache: Dict[str, int] = {}
        self._at_code_to_int_cache: Dict[str, int] = {}
        self._codes_loaded = False
    
    async def _load_codes_if_needed(self):
        """Load codes from database if not already loaded."""
        if self._codes_loaded:
            return
            
        codes_col = self.db["_codes"]
        doc = await codes_col.find_one({"_id": "ar_code"}) or {}
        
        # Load archetype mappings (positive codes)
        for rm_type, archetypes in doc.items():
            if rm_type in ("_id", "_max", "_min", "at"):
                continue
            if isinstance(archetypes, dict):
                for archetype_name, versions in archetypes.items():
                    if isinstance(versions, dict):
                        for version, code in versions.items():
                            # Build the full archetype ID as it appears in context_map
                            full_archetype_id = f"{rm_type}.{archetype_name}.{version}"
                            self._archetype_to_code_cache[full_archetype_id] = code
        
        # Load AT codes (negative codes)
        at_codes = doc.get("at", {})
        for at_code, code_value in at_codes.items():
            self._at_code_to_int_cache[at_code] = code_value
        
        self._codes_loaded = True
        print(f"Loaded {len(self._archetype_to_code_cache)} archetype codes and {len(self._at_code_to_int_cache)} AT codes")
    
    async def get_archetype_code(self, archetype_id: str) -> Optional[int]:
        """
        Get the numeric code for a full archetype ID.
        
        Args:
            archetype_id: Full archetype ID like "openEHR-EHR-CLUSTER.admin_salut.v0"
        
        Returns:
            The numeric code or None if not found
        """
        await self._load_codes_if_needed()
        return self._archetype_to_code_cache.get(archetype_id)
    
    async def get_at_code(self, at_code: str) -> Optional[int]:
        """
        Get the numeric code for an AT code.
        
        Args:
            at_code: AT code like "at0007", "at0014"
        
        Returns:
            The numeric code or None if not found
        """
        await self._load_codes_if_needed()
        return self._at_code_to_int_cache.get(at_code)
    
    async def resolve_variable_to_p_pattern(self, variable_alias: str, context_map: Dict[str, Dict]) -> Optional[str]:
        """
        Get the numeric code for a full archetype ID and return as regex pattern.
        For shortened format, this creates a pattern that matches the hierarchical p-values.
        
        Args:
            variable_alias: The variable alias like "admin_salut", "med_ac"
            context_map: The context map containing archetype IDs for variables
        
        Returns:
            Regex pattern for the p-value or None if not found
        """
        if variable_alias not in context_map:
            return None
        
        archetype_id = context_map[variable_alias].get('archetype_id')
        if not archetype_id:
            return None
        
        # Get the numeric code for this archetype
        archetype_code = await self.get_archetype_code(archetype_id)
        if archetype_code is None:
            return None
        
        # For shortened format, we need to match the hierarchical structure
        # The pattern should match p-values that start with this archetype code
        # For example: med_ac (code 11) should match "11.10.7" 
        # MongoDB regex needs dots escaped for $regexMatch
        return str(archetype_code)
    
    async def resolve_nested_path_to_p_pattern(self, variable_alias: str, aql_path_parts: List[str], context_map: Dict[str, Dict]) -> Optional[str]:
        """
        Resolves nested archetype paths to their specific p-value patterns.
        
        This handles cases like admin_salut/items[at0007]/items[at0014] where we need
        to build a composite p-value based on the nested archetype structure.
        
        The p-values are inverted in the flattened structure:
        - AQL path: a/description[at0001]/items[at0002]/value/defining_code/code_string
        - P-value: "-2.-1.24.22" (deepest first: at0002, at0001, ACTION, COMPOSITION)
        
        Args:
            variable_alias: The base variable alias
            aql_path_parts: The path parts after the variable alias
            context_map: The context map containing archetype IDs
        
        Returns:
            Regex pattern for the nested p-value or None if not resolvable
        """
        # Get the base archetype code for the variable
        if variable_alias not in context_map:
            return None
        
        archetype_id = context_map[variable_alias].get('archetype_id')
        if not archetype_id:
            return None
        
        base_archetype_code = await self.get_archetype_code(archetype_id)
        if base_archetype_code is None:
            return None
        
        # Build the nested p-value by processing archetype node references
        at_codes = []  # Collect AT codes in the order they appear in AQL path
        
        for part in aql_path_parts:
            # Look for any archetype node reference patterns: items[at0XXX] or description[at0XXX]
            archetype_match = re.match(r"(?:items|description|value|name|protocol|data|state|activities|activity|events|event|items_single|items_multiple)\[(.+)\]", part)
            if archetype_match:
                at_code = archetype_match.group(1)
                at_code_value = await self.get_at_code(at_code)
                if at_code_value is not None:
                    # Collect AT codes in order they appear in the AQL path
                    at_codes.append(str(at_code_value))
                else:
                    # If we can't resolve an AT code, we can't build the full path
                    return None
            elif part in ["value", "defining_code", "code_string", "magnitude", "units", "normal_range", "time", "name"]:
                # These are leaf properties that don't contribute to the p-value
                continue
            else:
                # Unknown part - continue processing but don't add to p-value
                continue
        
        # Build p_parts in the correct inverted hierarchical order
        # The p-value is inverted: [deepest_at_code, parent_at_code, ..., base_archetype, composition_archetype]
        # AQL path: description[at0001]/items[at0002] represents items INSIDE description
        # So at0002 (items) is deeper than at0001 (description)
        # In p-value: -2.-1.24.22 (items first, then description, then ACTION, then COMPOSITION)
        # So we need to reverse the at_codes to get deepest first
        p_parts = list(reversed(at_codes))  # Reverse to get deepest element first
        p_parts.append(str(base_archetype_code))  # Add base archetype code
        
        # Get the composition archetype code and add it at the end
        # Find the composition in the context map
        composition_archetype_code = None
        for alias, info in context_map.items():
            archetype_id = info.get('archetype_id')
            if archetype_id and 'COMPOSITION' in archetype_id:
                composition_archetype_code = await self.get_archetype_code(archetype_id)
                break
        
        if composition_archetype_code is not None:
            p_parts.append(str(composition_archetype_code))
        
        # Build the composite p-value pattern
        if len(p_parts) > 1:
            # For nested paths, the pattern should match the specific hierarchy
            # Example: -2.-1.24.22 (where -2=at0002, -1=at0001, 24=ACTION, 22=COMPOSITION)
            composite_p = ".".join(p_parts)
            # For exact matching in shortened format, we want to match this exact pattern
            # MongoDB regex needs dots escaped for $regexMatch
            escaped_pattern = composite_p.replace(".", "\\.")
            return f"^{escaped_pattern}$"
        else:
            # Fallback to just the base archetype pattern
            return f"^{base_archetype_code}$"
    
    async def resolve_composition_p_pattern(self, composition_alias: str, context_map: Dict[str, Dict]) -> str:
        """
        Resolves the composition p-pattern.
        
        Args:
            composition_alias: The composition variable alias
            context_map: The context map containing archetype IDs
        
        Returns:
            Regex pattern for composition p-value (defaults to "^7$" if not found)
        """
        pattern = await self.resolve_variable_to_p_pattern(composition_alias, context_map)
        # Default to "^7$" which seems to be the standard composition root pattern
        return pattern or "^7$"