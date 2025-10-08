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
        result = self._archetype_to_code_cache.get(archetype_id)
        return result
    
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
        
        This handles cases where archetypes are nested within other archetypes and we need
        to build the complete hierarchical p-value pattern including all intermediate
        structural elements.
        
        The key insight is that nested aliases (like CLUSTER inside EVALUATION) need to
        account for their full containment context, not just their immediate archetype code.
        
        Args:
            variable_alias: The variable alias (e.g., 'loc' for CLUSTER)
            aql_path_parts: The path parts after the variable alias
            context_map: The context map containing archetype IDs and parent relationships
        
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
        
        # Build the nested p-value by processing archetype node references from the path
        at_codes = []  # Collect AT codes in the order they appear in AQL path
        
        for part in aql_path_parts:
            # Look for any archetype node reference patterns
            archetype_match = re.match(r"(?:items|description|value|name|protocol|data|state|activities|activity|events|event|items_single|items_multiple|context|other_context)\[(.+)\]", part)
            if archetype_match:
                reference = archetype_match.group(1)
                
                # Handle AT codes (like at0001)
                if reference.startswith('at'):
                    at_code_value = await self.get_at_code(reference)
                    if at_code_value is not None:
                        at_codes.append(str(at_code_value))
                    else:
                        return None
                        
                # Handle full archetype IDs (like openEHR-EHR-CLUSTER.xds_metadata.v0)
                elif reference.startswith('openEHR-'):
                    archetype_code = await self.get_archetype_code(reference)
                    if archetype_code is not None:
                        at_codes.append(str(archetype_code))
                    else:
                        return None
                else:
                    continue
                    
            elif part in ["value", "defining_code", "code_string", "magnitude", "units", "normal_range", "time", "name"]:
                # These are leaf properties that don't contribute to the p-value
                continue
            else:
                # Unknown part - continue processing but don't add to p-value
                continue
        
        # CRITICAL: Build the complete hierarchical context for nested aliases
        # We need to reconstruct the full containment path by walking up the parent chain
        # and understanding where this alias exists within its parent's structure
        
        hierarchical_parts = []
        
        # Start with the AT codes from the current path (deepest first)
        hierarchical_parts.extend(reversed(at_codes))
        
        # Add the current archetype code
        hierarchical_parts.append(str(base_archetype_code))
        
        # Walk up the containment hierarchy to build the complete context
        current_alias = variable_alias
        while current_alias in context_map:
            parent_alias = context_map[current_alias].get('parent')
            if not parent_alias or parent_alias not in context_map:
                break
                
            parent_archetype_id = context_map[parent_alias].get('archetype_id')
            if not parent_archetype_id:
                current_alias = parent_alias
                continue
            
            # For nested archetypes, we need to find the structural elements that connect
            # the child to the parent. This requires understanding the openEHR archetype
            # containment patterns.
            
            # Common pattern: CLUSTER inside EVALUATION typically goes through a data[at0001] structure
            # More generally, most archetypes have standard structural containers
            
            # Get the parent archetype code
            parent_archetype_code = await self.get_archetype_code(parent_archetype_id)
            if parent_archetype_code is None:
                current_alias = parent_alias
                continue
            
            # Skip composition - handle it separately at the end
            if 'COMPOSITION' in parent_archetype_id:
                current_alias = parent_alias
                break
                
            # Add intermediate structural element based on archetype containment patterns
            # This is the key insight: most openEHR archetypes have standard data containers
            if 'EVALUATION' in parent_archetype_id or 'OBSERVATION' in parent_archetype_id or 'ACTION' in parent_archetype_id:
                # These typically contain items through a data[at0001] structure
                hierarchical_parts.append("-1")  # at0001 is the standard data container
            elif 'SECTION' in parent_archetype_id:
                # Sections typically contain items directly or through specific structures
                # We might need to add logic here for specific section patterns if needed
                pass
            
            # Add the parent archetype code
            hierarchical_parts.append(str(parent_archetype_code))
            
            current_alias = parent_alias
        
        # Add composition archetype code at the end
        composition_archetype_code = None
        for alias, info in context_map.items():
            archetype_id = info.get('archetype_id')
            if archetype_id and 'COMPOSITION' in archetype_id:
                composition_archetype_code = await self.get_archetype_code(archetype_id)
                break
        
        if composition_archetype_code is not None:
            hierarchical_parts.append(str(composition_archetype_code))
        
        # Build the composite p-value pattern
        if len(hierarchical_parts) > 1:
            composite_p = ".".join(hierarchical_parts)
            escaped_pattern = composite_p.replace(".", "\\.")
            return f"^{escaped_pattern}(?:\\.\\d+)*$"
        else:
            # Fallback to just the base archetype pattern
            return f"^{base_archetype_code}(?:\\.\\d+)*$"
    
    async def resolve_composition_p_pattern(self, composition_alias: str, context_map: Dict[str, Dict]) -> str:
        """
        Resolves the composition p-pattern.
        
        Args:
            composition_alias: The composition variable alias
            context_map: The context map containing archetype IDs
        
        Returns:
            Regex pattern for composition p-value (defaults to "^\\d+$" if not found)
        """
        pattern = await self.resolve_variable_to_p_pattern(composition_alias, context_map)
        # Default to match any numeric composition p-value when no specific predicate
        return pattern or "^\\d+$"