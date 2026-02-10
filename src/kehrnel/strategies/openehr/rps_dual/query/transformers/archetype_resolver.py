# src/kehrnel/api/legacy/v1/aql/transformers/archetype_resolver.py

from typing import Dict, Optional, Tuple, List
from motor.motor_asyncio import AsyncIOMotorDatabase
import re
import logging

logger = logging.getLogger(__name__)

class ArchetypeResolver:
    """
    Dynamically resolves archetype IDs to p-values by querying the _codes collection.
    Replaces hardcoded p-value patterns with database-driven resolution and pattern discovery.
    """
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self._archetype_to_code_cache: Dict[str, int] = {}
        self._at_code_to_int_cache: Dict[str, int] = {}
        self._structural_pattern_cache: Dict[str, List[str]] = {}
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
        logger.info(f"Loaded {len(self._archetype_to_code_cache)} archetype codes and {len(self._at_code_to_int_cache)} AT codes")
    
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
        Resolves nested archetype paths to their specific p-value patterns using a truly
        data-driven approach that analyzes actual document patterns in the database.
        
        This method:
        1. Extracts AT codes from the AQL path and converts them to numeric codes using the _codes collection.
        2. Queries the database to discover actual p-value patterns for the given archetype and path.
        3. Dynamically generates a regex pattern that matches the discovered patterns.
        4. Applies fallback logic to return a default regex if no patterns are found.
        
        Args:
            variable_alias: The variable alias (e.g., 'admin_salut' for CLUSTER)
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
        
        # Extract AT codes from the AQL path itself - these represent the hierarchical structure
        at_code_sequence = []
        for part in aql_path_parts:
            # Look for archetype node references in path segments like items[at0007], items[at0014]
            archetype_match = re.match(r"(?:items|description|value|name|protocol|data|state|activities|activity|events|event|items_single|items_multiple|context|other_context)\[(.+)\]", part)
            if archetype_match:
                reference = archetype_match.group(1)
                
                # Handle AT codes (like at0002, at0004, at0007, at0014)
                if reference.startswith('at'):
                    at_code_value = await self.get_at_code(reference)
                    if at_code_value is not None:
                        at_code_sequence.append(str(at_code_value))
                    else:
                        logger.warning(f"Warning: Could not resolve AT code {reference}")
                        return None
                        
                # Handle nested archetype IDs (less common but possible)
                elif reference.startswith('openEHR-'):
                    archetype_code = await self.get_archetype_code(reference)
                    if archetype_code is not None:
                        at_code_sequence.append(str(archetype_code))
                    else:
                        logger.warning(f"Warning: Could not resolve archetype {reference}")
                        return None
        
        # If no AT codes found in path, fall back to base archetype pattern
        if not at_code_sequence:
            return str(base_archetype_code)
        
        # Now query actual documents to find patterns that match our AT code sequence
        try:
            # Use the search collection for pattern discovery
            search_col = self.db["sm_search3"]
            
            # Build the expected pattern structure
            # For admin_salut/items[at0007]/items[at0014], we expect:
            # at0014 (deepest) -> at0007 -> admin_salut archetype -> ... -> composition
            expected_start = f"{at_code_sequence[-1]}.{'.'.join(reversed(at_code_sequence[:-1]))}.{base_archetype_code}" if len(at_code_sequence) > 1 else f"{at_code_sequence[0]}.{base_archetype_code}"
            
            # Query documents that have sn elements with p-values starting with our expected pattern
            #regex_pattern = f"^{expected_start.replace('.', '\\.')}\\."

            escaped = expected_start.replace(".", r"\.")
            regex_pattern = rf"^{escaped}\."
            
            # Find documents with matching patterns
            matching_docs = await search_col.find({
                "sn.p": {"$regex": regex_pattern}
            }).limit(10).to_list(length=10)
            
            if matching_docs:
                # Extract all matching p-values to understand the full pattern
                all_patterns = []
                for doc in matching_docs:
                    for sn_item in doc.get('sn', []):
                        p_value = sn_item.get('p', '')
                        if p_value.startswith(expected_start):
                            all_patterns.append(p_value)
                
                if all_patterns:
                    # Find the most common pattern structure
                    # For our case, we expect patterns like: "-14.-7.9.-4.7"
                    # We want to create a regex that matches this structure
                    
                    # Get the longest common pattern to understand the structure
                    sample_pattern = all_patterns[0]
                    pattern_parts = sample_pattern.split('.')
                    
                    # Build a flexible regex that matches the discovered structure
                    # This allows for variations in the composition/parent structure
                    if len(pattern_parts) >= len(expected_start.split('.')):
                        # Create a pattern that matches the AT code sequence exactly
                        # but is flexible about what comes after
                        escaped_start = expected_start.replace('.', '\\.')
                        return f"^{escaped_start}(?:\\.[-\\d]+)*$"
                    else:
                        # Fallback to exact match if pattern is shorter than expected
                        escaped_pattern = sample_pattern.replace('.', '\\.')
                        return f"^{escaped_pattern}$"
                else:
                    logger.warning(f"Warning: Found documents but no matching p-values for pattern {expected_start}")
            else:
                logger.warning(f"Warning: No documents found with pattern starting with {expected_start}")
                
        except Exception as e:
            logger.warning(f"Error during data-driven pattern discovery: {e}")
        
        # Fallback: if no data-driven pattern found, create a basic pattern
        # This should match the AT code sequence + base archetype
        fallback_pattern = f"{at_code_sequence[-1]}.{'.'.join(reversed(at_code_sequence[:-1]))}.{base_archetype_code}" if len(at_code_sequence) > 1 else f"{at_code_sequence[0]}.{base_archetype_code}"
        escaped_fallback = fallback_pattern.replace('.', '\\.')
        return f"^{escaped_fallback}(?:\\.[-\\d]+)*$"
    
    async def _build_containment_chain(self, variable_alias: str, context_map: Dict[str, Dict]) -> List[Dict]:
        """
        Builds the containment chain from child to composition.
        
        Returns:
            List of dicts with archetype_code, archetype_type, and alias
        """
        chain = []
        current_alias = variable_alias
        
        while current_alias in context_map:
            parent_alias = context_map[current_alias].get('parent')
            if not parent_alias or parent_alias not in context_map:
                break
                
            parent_archetype_id = context_map[parent_alias].get('archetype_id')
            if not parent_archetype_id:
                current_alias = parent_alias
                continue
            
            parent_archetype_code = await self.get_archetype_code(parent_archetype_id)
            if parent_archetype_code is None:
                current_alias = parent_alias
                continue
            
            # Extract archetype type from ID
            archetype_type = self._extract_archetype_type(parent_archetype_id)
            
            chain.append({
                'alias': parent_alias,
                'archetype_code': parent_archetype_code,
                'archetype_type': archetype_type,
                'archetype_id': parent_archetype_id
            })
            
            current_alias = parent_alias
        
        return chain
    
    def _extract_archetype_type(self, archetype_id: str) -> str:
        """Extract archetype type from full archetype ID."""
        # Handle both full IDs and shortened ones
        if 'openEHR-EHR-' in archetype_id:
            return archetype_id.split('.')[0].replace('openEHR-EHR-', '')
        else:
            # For shortened format, extract from the ID structure
            for rm_type in ['COMPOSITION', 'EVALUATION', 'OBSERVATION', 'ACTION', 'SECTION', 'CLUSTER', 'ADMIN_ENTRY']:
                if rm_type.lower() in archetype_id.lower():
                    return rm_type
        return 'UNKNOWN'
    
    async def _discover_structural_elements(self, child_archetype_code: str, parent_archetype_code: str, parent_archetype_type: str) -> List[str]:
        """
        Discovers intermediate structural elements between child and parent archetypes.
        
        This method uses a hybrid approach:
        1. First tries data-driven pattern discovery from actual compositions
        2. Falls back to openEHR structural patterns as a secondary approach
        
        Args:
            child_archetype_code: The child archetype code
            parent_archetype_code: The parent archetype code  
            parent_archetype_type: The parent archetype type (EVALUATION, OBSERVATION, etc.)
        
        Returns:
            List of intermediate structural element codes
        """
        # Try data-driven discovery first (most accurate)
        try:
            discovered_elements = await self._discover_structural_elements_from_data(
                child_archetype_code, parent_archetype_code
            )
            if discovered_elements:
                return discovered_elements
        except Exception as e:
            logger.warning(f"Data-driven discovery failed, falling back to pattern-based: {e}")
        
        # Fallback to pattern-based discovery using openEHR structural knowledge
        structural_elements = []
        
        # Pattern 1: Most RM types have a standard 'data' container (at0001 = -1)
        if parent_archetype_type in ['EVALUATION', 'OBSERVATION', 'ACTION']:
            # These typically have a data[at0001] structure containing items
            structural_elements.append("-1")
        
        # Pattern 2: ADMIN_ENTRY typically has direct item containers or data structures
        elif parent_archetype_type == 'ADMIN_ENTRY':
            # Often has data[at0001] as well
            structural_elements.append("-1")
        
        # Pattern 3: SECTION can contain items directly or through specific structures
        elif parent_archetype_type == 'SECTION':
            # Some sections may have intermediate structures, but often direct containment
            # Could be enhanced with specific section patterns
            pass
        
        # Pattern 4: CLUSTER within CLUSTER typically has direct item containment
        elif parent_archetype_type == 'CLUSTER':
            # Usually direct containment through items[], no intermediate structure needed
            pass
        
        return structural_elements
    
    async def _discover_structural_elements_from_data(self, child_archetype_code: str, parent_archetype_code: str) -> List[str]:
        """
        Advanced pattern discovery that analyzes actual data to find structural elements.
        
        This method queries a sample of compositions to understand the actual p-value patterns
        between specific archetype combinations, making the system truly data-driven.
        
        Args:
            child_archetype_code: The child archetype code
            parent_archetype_code: The parent archetype code
        
        Returns:
            List of discovered intermediate structural element codes
        """
        cache_key = f"{child_archetype_code}->{parent_archetype_code}"
        
        # Check cache first
        if cache_key in self._structural_pattern_cache:
            return self._structural_pattern_cache[cache_key]
        
        try:
            # Query compositions to find patterns between these archetypes
            compositions_col = self.db["compositions"]
            
            # Look for documents that contain both archetypes in their cn array
            pipeline = [
                {
                    "$match": {
                        "cn.data.archetype_node_id": {"$in": [int(child_archetype_code), int(parent_archetype_code)]}
                    }
                },
                {
                    "$project": {
                        "cn": {
                            "$filter": {
                                "input": "$cn",
                                "cond": {
                                    "$in": ["$$this.data.archetype_node_id", [int(child_archetype_code), int(parent_archetype_code)]]
                                }
                            }
                        }
                    }
                },
                {"$limit": 10}
            ]
            
            structural_elements = []
            async for doc in compositions_col.aggregate(pipeline):
                # Analyze p-value patterns to extract intermediate elements
                patterns = self._analyze_p_value_patterns(doc["cn"], child_archetype_code, parent_archetype_code)
                structural_elements.extend(patterns)
            
            # Deduplicate and find the most common pattern
            if structural_elements:
                from collections import Counter
                most_common = Counter(structural_elements).most_common(1)
                result = most_common[0][0] if most_common else []
            else:
                result = []
            
            # Cache the result
            self._structural_pattern_cache[cache_key] = result
            return result
            
        except Exception as e:
            logger.warning(f"Pattern discovery failed for {cache_key}: {e}")
            return []
    
    def _analyze_p_value_patterns(self, cn_array: List[Dict], child_code: str, parent_code: str) -> List[List[str]]:
        """
        Analyzes p-value patterns in a composition to extract structural relationships.
        
        Args:
            cn_array: The cn array from a composition
            child_code: The child archetype code
            parent_code: The parent archetype code
        
        Returns:
            List of intermediate element patterns found
        """
        patterns = []
        
        # Find p-values for child and parent archetypes
        child_p_values = [item["p"] for item in cn_array if item.get("data", {}).get("archetype_node_id") == int(child_code)]
        parent_p_values = [item["p"] for item in cn_array if item.get("data", {}).get("archetype_node_id") == int(parent_code)]
        
        # Analyze the hierarchical relationship
        for child_p in child_p_values:
            for parent_p in parent_p_values:
                if child_p.endswith(parent_p):
                    # Extract intermediate elements
                    prefix = child_p[:-len(parent_p)].rstrip('.')
                    if prefix:
                        # Remove the child code itself and extract intermediate elements
                        parts = prefix.split('.')
                        if parts and parts[0] == child_code:
                            intermediate = parts[1:] if len(parts) > 1 else []
                            if intermediate:
                                patterns.append(intermediate)
        
        return patterns
    
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