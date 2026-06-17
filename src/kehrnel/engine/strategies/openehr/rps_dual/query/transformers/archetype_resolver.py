# src/kehrnel/api/compatibility/v1/aql/transformers/archetype_resolver.py

from typing import Any, Dict, Optional, Tuple, List
from motor.motor_asyncio import AsyncIOMotorDatabase
import re
import logging

logger = logging.getLogger(__name__)

class ArchetypeResolver:
    """
    Dynamically resolves archetype IDs to p-values by querying the _codes collection.
    Replaces hardcoded p-value patterns with database-driven resolution and pattern discovery.
    """
    
    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        *,
        codes_collection: str | None = None,
        codes_doc_id: str | None = None,
        search_collection: str | None = None,
        composition_collection: str | None = None,
        separator: str | None = None,
        atcode_strategy: str | None = None,
    ):
        self.db = db
        self.codes_collection = codes_collection or "_codes"
        self.codes_doc_id = codes_doc_id or "ar_code"
        self.search_collection = search_collection or "sm_search3"
        self.composition_collection = composition_collection or "compositions"
        self.separator = separator or ":"
        self.atcode_strategy = (atcode_strategy or "negative_int").strip().lower()
        self._archetype_to_code_cache: Dict[str, Any] = {}
        self._at_code_to_int_cache: Dict[str, Any] = {}
        self._structural_pattern_cache: Dict[str, List[str]] = {}
        self._codes_loaded = False

    def _escaped_separator(self) -> str:
        return re.escape(self.separator)

    def _not_separator_class(self) -> str:
        return f"[^{re.escape(self.separator)}]"

    def _root_path_regex(self) -> str:
        return rf"^{self._not_separator_class()}+$"

    def _join_tokens(self, *tokens: Any) -> str:
        return self.separator.join(str(token) for token in tokens if token is not None and str(token) != "")

    def _path_suffix_regex(self) -> str:
        escaped_sep = self._escaped_separator()
        not_sep = self._not_separator_class()
        return rf"(?:{escaped_sep}{not_sep}+)*$"

    @staticmethod
    def _candidate_code_values(code: Any) -> List[Any]:
        candidates: List[Any] = []
        for candidate in (code, str(code)):
            if candidate not in candidates:
                candidates.append(candidate)
        try:
            numeric = int(str(code))
        except Exception:
            numeric = None
        if numeric is not None and numeric not in candidates:
            candidates.append(numeric)
        return candidates

    @staticmethod
    def _node_selector_value(item: Dict[str, Any]) -> Any:
        data = item.get("data", {}) if isinstance(item, dict) else {}
        if not isinstance(data, dict):
            return None
        if "archetype_node_id" in data:
            return data.get("archetype_node_id")
        return data.get("ani")
    
    async def _load_codes_if_needed(self):
        """Load codes from database if not already loaded."""
        if self._codes_loaded:
            return
            
        codes_col = self.db[self.codes_collection]
        doc = await codes_col.find_one({"_id": self.codes_doc_id}) or {}

        explicit_items = doc.get("items") or doc.get("codes")
        if isinstance(explicit_items, dict):
            for selector, code in explicit_items.items():
                if not isinstance(selector, str):
                    continue
                if selector.lower().startswith("at"):
                    self._at_code_to_int_cache[selector] = code
                else:
                    self._archetype_to_code_cache[selector] = code
        
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
            self._at_code_to_int_cache[str(at_code).lower()] = code_value

        self._codes_loaded = True
        logger.info(f"Loaded {len(self._archetype_to_code_cache)} archetype codes and {len(self._at_code_to_int_cache)} AT codes")
    
    async def get_archetype_code(self, archetype_id: str) -> Optional[Any]:
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
    
    async def get_at_code(self, at_code: str) -> Optional[Any]:
        """
        Get the numeric code for an AT code.
        
        Args:
            at_code: AT code like "at0007", "at0014"
        
        Returns:
            The numeric code or None if not found
        """
        await self._load_codes_if_needed()
        if not at_code:
            return None

        normalized = str(at_code).lower()
        resolved = self._at_code_to_int_cache.get(at_code)
        if resolved is None:
            resolved = self._at_code_to_int_cache.get(normalized)
        if resolved is not None:
            return resolved

        fallback = self._synthesize_at_code(normalized)
        if fallback is not None:
            self._at_code_to_int_cache[at_code] = fallback
            self._at_code_to_int_cache[normalized] = fallback
        return fallback

    def _synthesize_at_code(self, at_code: str) -> Optional[Any]:
        strategy = self.atcode_strategy or "negative_int"
        if strategy == "negative_int":
            return self._encode_at_negative_int(at_code)
        if strategy == "literal":
            return at_code
        if strategy == "compact_prefix":
            return self._encode_at_compact_prefix(at_code)
        return None

    @staticmethod
    def _extract_at_digits(at_code: str) -> Optional[str]:
        match = re.match(r"^at(\d+)$", str(at_code).strip().lower())
        if not match:
            return None
        return match.group(1)

    def _encode_at_negative_int(self, at_code: str) -> Optional[int]:
        digits = self._extract_at_digits(at_code)
        if digits is None:
            return None
        return -int(digits)

    def _encode_at_compact_prefix(self, at_code: str) -> Optional[str]:
        digits = self._extract_at_digits(at_code)
        if digits is None:
            return None

        if self.separator == "/":
            significant = digits.lstrip("0") or "0"
            if len(significant) == 1:
                prefix = "D"
            elif len(significant) == 2:
                prefix = "C"
            elif len(significant) == 3:
                prefix = "B"
            else:
                prefix = "A"
            return f"{prefix}{significant}"

        selector = f"at{digits}"
        if selector.startswith("at000"):
            return "A" + selector[5:]
        if selector.startswith("at00"):
            return "B" + selector[4:]
        if selector.startswith("at0"):
            return "C" + selector[3:]
        return selector
    
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
            archetype_match = re.match(r"(?:items|description|value|name|protocol|data|state|activities|activity|events|event|items_single|items_multiple|context|other_context|content)\[(.+)\]", part)
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
            search_col = self.db[self.search_collection]
            
            # Build the expected pattern structure
            # For admin_salut/items[at0007]/items[at0014], we expect:
            # at0014 (deepest) -> at0007 -> admin_salut archetype -> ... -> composition
            expected_tokens = [at_code_sequence[-1], *reversed(at_code_sequence[:-1]), base_archetype_code]
            expected_start = self._join_tokens(*expected_tokens)
            
            # Query documents that have sn elements with p-values starting with our expected pattern
            #regex_pattern = f"^{expected_start.replace('.', '\\.')}\\."

            escaped = re.escape(expected_start)
            regex_pattern = rf"^{escaped}(?:{self._escaped_separator()}|$)"
            
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
                    pattern_parts = sample_pattern.split(self.separator)
                    
                    # Build a flexible regex that matches the discovered structure
                    # This allows for variations in the composition/parent structure
                    if len(pattern_parts) >= len(expected_start.split(self.separator)):
                        # Create a pattern that matches the AT code sequence exactly
                        # but is flexible about what comes after
                        escaped_start = re.escape(expected_start)
                        return rf"^{escaped_start}{self._path_suffix_regex()}"
                    else:
                        # Fallback to exact match if pattern is shorter than expected
                        escaped_pattern = re.escape(sample_pattern)
                        return f"^{escaped_pattern}$"
                else:
                    logger.warning(f"Warning: Found documents but no matching p-values for pattern {expected_start}")
            else:
                logger.warning(f"Warning: No documents found with pattern starting with {expected_start}")
                
        except Exception as e:
            logger.warning(f"Error during data-driven pattern discovery: {e}")
        
        # Fallback: if no data-driven pattern found, create a basic pattern
        # This should match the AT code sequence + base archetype
        fallback_pattern = self._join_tokens(at_code_sequence[-1], *reversed(at_code_sequence[:-1]), base_archetype_code)
        escaped_fallback = re.escape(fallback_pattern)
        return rf"^{escaped_fallback}{self._path_suffix_regex()}"
    
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
            compositions_col = self.db[self.composition_collection]
            candidate_values = [
                *self._candidate_code_values(child_archetype_code),
                *self._candidate_code_values(parent_archetype_code),
            ]
            
            # Look for documents that contain both archetypes in their cn array
            pipeline = [
                {
                    "$match": {
                        "$or": [
                            {"cn.data.archetype_node_id": {"$in": candidate_values}},
                            {"cn.data.ani": {"$in": candidate_values}},
                        ]
                    }
                },
                {
                    "$project": {
                        "cn": {
                            "$filter": {
                                "input": "$cn",
                                "cond": {
                                    "$or": [
                                        {"$in": ["$$this.data.archetype_node_id", candidate_values]},
                                        {"$in": ["$$this.data.ani", candidate_values]},
                                    ]
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
        child_candidates = set(self._candidate_code_values(child_code))
        parent_candidates = set(self._candidate_code_values(parent_code))
        
        # Find p-values for child and parent archetypes
        child_p_values = [
            item["p"] for item in cn_array
            if self._node_selector_value(item) in child_candidates
        ]
        parent_p_values = [
            item["p"] for item in cn_array
            if self._node_selector_value(item) in parent_candidates
        ]
        
        # Analyze the hierarchical relationship
        for child_p in child_p_values:
            for parent_p in parent_p_values:
                if child_p.endswith(parent_p):
                    # Extract intermediate elements
                    prefix = child_p[:-len(parent_p)].rstrip(self.separator)
                    if prefix:
                        # Remove the child code itself and extract intermediate elements
                        parts = prefix.split(self.separator)
                        if parts and parts[0] == str(child_code):
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
        return pattern or self._root_path_regex()
