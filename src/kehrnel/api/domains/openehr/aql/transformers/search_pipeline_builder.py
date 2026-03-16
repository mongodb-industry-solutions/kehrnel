# src/kehrnel/api/compatibility/v1/aql/transformers/search_pipeline_builder.py

from typing import Dict, Any, List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from .condition_processor import ConditionProcessor
from .value_formatter import ValueFormatter
from .format_resolver import FormatResolver
from kehrnel.persistence import PersistenceStrategy, get_default_strategy
from kehrnel.api.bridge.app.core.config import settings
import logging
import re

logger = logging.getLogger(__name__)


class SearchPipelineBuilder:
    """
    Builds MongoDB aggregation pipelines that start with $search stages for Atlas Search.
    Designed specifically for the search collection (sm_search3) with 'sn' array structure.
    """

    def __init__(self, ehr_alias: str, composition_alias: str, schema_config: Dict[str, str], 
                 format_resolver: FormatResolver, context_map: Dict[str, Dict], 
                 let_variables: Dict[str, Any] = None,
                 strategy: Optional[PersistenceStrategy] = None,
                 search_index_name: Optional[str] = None):
        self.ehr_alias = ehr_alias
        self.composition_alias = composition_alias
        self.schema_config = schema_config
        self.format_resolver = format_resolver
        self.context_map = context_map
        self.let_variables = let_variables or {}
        self.strategy = strategy or get_default_strategy()
        
        # Use centralized configuration like repository.py does
        self.search_index_name = (
            search_index_name
            or self._resolve_search_index_name()
            or settings.search_config.search_index_name
        )
        self.full_compositions_collection = settings.search_config.flatten_collection
        
        # For search collection, we use 'sn' instead of 'cn'
        self.search_config = self._build_search_schema_config()
        
        self.condition_processor = ConditionProcessor(
            ehr_alias, composition_alias, self.search_config, format_resolver, let_variables
        )
        self.value_formatter = ValueFormatter()

    def _build_search_schema_config(self):
        search_fields = self.strategy.fields.get("search") if self.strategy else None
        return {
            'composition_array': search_fields.nodes if search_fields and search_fields.nodes else 'sn',
            'path_field': search_fields.path if search_fields and search_fields.path else 'p',
            'data_field': search_fields.data if search_fields and search_fields.data else 'data',
        }

    def _resolve_search_index_name(self):
        search_collection = self.strategy.collections.get("search") if self.strategy else None
        return search_collection.atlas_index_name if search_collection else None

    async def build_search_pipeline(self, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Constructs the full MongoDB aggregation pipeline starting with $search.
        """
        pipeline = []

        # 1. Build the $search stage from WHERE clause
        search_stage = await self.build_search_stage(ast)
        if search_stage:
            pipeline.append(search_stage)

        # 2. Build additional $match stages for conditions not handled by $search
        additional_match = await self.build_additional_match_stage(ast)
        if additional_match:
            pipeline.append(additional_match)

        # 3. Build the $lookup stage to get complete composition data
        lookup_stage = self.build_lookup_stage(ast)
        if lookup_stage:
            pipeline.append(lookup_stage)

        # 4. Build the $addFields stage for LET variables
        let_stage = self.build_let_stage()
        if let_stage:
            pipeline.append(let_stage)

        # 5. Build the $project stage from the SELECT clause  
        project_stage = await self.build_project_stage(ast)
        if project_stage:
            pipeline.append(project_stage)

        # 6. Build the $sort stage from ORDER BY clause
        sort_stage = self.build_sort_stage(ast)
        if sort_stage:
            pipeline.append(sort_stage)
        
        # 7. Build the $limit stage from LIMIT clause
        limit_stage = self.build_limit_stage(ast)
        if limit_stage:
            pipeline.append(limit_stage)
        
        return pipeline

    async def build_search_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $search stage from AQL WHERE clause using Atlas Search.
        """
        where_clause = ast.get("where")
        if not where_clause:
            # If no WHERE clause, create a basic $search to match all documents
            return {
                "$search": {
                    "index": self.search_index_name,
                    "exists": {
                        "path": "sn"
                    }
                }
            }

        # Convert WHERE clause to search query
        search_query = await self._convert_where_to_search(where_clause)
        
        if search_query:
            return {
                "$search": {
                    "index": self.search_index_name,
                    **search_query
                }
            }
        else:
            # Fallback to basic search if WHERE clause couldn't be converted
            logger.warning(f"Could not convert WHERE clause to search query, using basic exists search: {where_clause}")
            return {
                "$search": {
                    "index": self.search_index_name,
                    "exists": {
                        "path": "sn"
                    }
                }
            }

    async def _convert_where_to_search(self, where_clause: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Converts an AQL WHERE clause to Atlas Search query syntax.
        Handles both the new AQL parser format and compatibility formats.
        """
        # Handle the actual AQL parser format (flat structure)
        if "path" in where_clause and "operator" in where_clause:
            return await self._handle_direct_condition_search(where_clause)
        
        # Handle logical conditions with multiple sub-conditions
        if "operator" in where_clause and "conditions" in where_clause:
            return await self._handle_logical_conditions_search(where_clause)
        
        # Compatibility format handling (for backward compatibility)
        condition_type = where_clause.get("type")
        
        if condition_type == "comparison":
            return await self._handle_comparison_search(where_clause)
        
        elif condition_type == "logical":
            return await self._handle_logical_search(where_clause)
        
        elif condition_type == "exists":
            return await self._handle_exists_search(where_clause)
        
        elif condition_type == "matches":
            return await self._handle_matches_search(where_clause)
        
        return None

    async def _handle_direct_condition_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles direct condition format from AQL parser: {"path": "...", "operator": "...", "value": "..."}
        """
        aql_path = condition.get("path")
        operator = condition.get("operator")
        value = condition.get("value")
        
        if not aql_path or not operator:
            logger.warning(f"Invalid direct condition format: {condition}")
            return None
        
        # Translate AQL path to search collection path
        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
            # For search operations, we use the data path directly in the sn.data structure
            search_path = f"sn.{data_path}" if data_path else "sn.data"
        except Exception as e:
            logger.warning(f"Could not resolve path {aql_path}: {e}")
            return None
        
        # Handle special operators
        if operator == "EXISTS":
            # Use embedded document exists query for sn paths
            if search_path.startswith("sn."):
                return self._build_embedded_document_exists_query(search_path)
            else:
                return {
                    "exists": {
                        "path": search_path
                    }
                }
        
        if operator == "NOT EXISTS":
            # Use embedded document exists query wrapped in mustNot for sn paths
            if search_path.startswith("sn."):
                exists_query = self._build_embedded_document_exists_query(search_path)
                return {
                    "compound": {
                        "mustNot": [exists_query]
                    }
                }
            else:
                return {
                    "compound": {
                        "mustNot": [{
                            "exists": {
                                "path": search_path
                            }
                        }]
                    }
                }
        
        if operator == "MATCHES":
            # Handle MATCHES with array values
            if isinstance(value, dict):
                # Convert object with numeric keys back to array
                value_array = [value.get(str(i)) for i in range(len(value))]
                # Use regex with alternation for multiple values
                regex_pattern = "|".join(f"({re.escape(str(v))})" for v in value_array if v is not None)
                return {
                    "regex": {
                        "path": search_path,
                        "query": regex_pattern
                    }
                }
            else:
                return {
                    "regex": {
                        "path": search_path,
                        "query": str(value)
                    }
                }
        
        if operator == "LIKE":
            # Convert SQL LIKE to regex
            regex_pattern = str(value).replace('%', '.*').replace('_', '.')
            return {
                "regex": {
                    "path": search_path,
                    "query": regex_pattern
                }
            }
        
        # Handle comparison operators
        if operator == "=":
            if search_path.startswith("sn."):
                return self._build_embedded_document_equals_query(search_path, value)
            else:
                if isinstance(value, str):
                    return {
                        "text": {
                            "query": value,
                            "path": search_path
                        }
                    }
                else:
                    return {
                        "equals": {
                            "path": search_path,
                            "value": value
                        }
                    }

        elif operator in [">=", ">", "<=", "<"]:
            # For date/time values, ensure proper formatting and use embedded document queries
            formatted_value = self._format_date_value_for_search(value)
            
            # Check if this is an embedded document query (sn array)
            if search_path.startswith("sn."):
                return self._build_embedded_document_range_query(search_path, operator, formatted_value)
            else:
                # Non-embedded document range query (fallback)
                range_condition = {}
                if operator == ">=":
                    range_condition["gte"] = formatted_value
                elif operator == ">":
                    range_condition["gt"] = formatted_value
                elif operator == "<=":
                    range_condition["lte"] = formatted_value
                elif operator == "<":
                    range_condition["lt"] = formatted_value
                    
                return {
                    "range": {
                        "path": search_path,
                        **range_condition
                    }
                }

        elif operator in ["!=", "<>"]:
            return {
                "compound": {
                    "mustNot": [{
                        "equals": {
                            "path": search_path,
                            "value": value
                        }
                    }]
                }
            }
        
        logger.warning(f"Unsupported operator in search condition: {operator}")
        return None

    async def _handle_logical_conditions_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles logical conditions with multiple sub-conditions from AQL parser.
        Format: {"operator": "AND/OR", "conditions": {"0": {...}, "1": {...}}}
        """
        operator = condition.get("operator")
        conditions = condition.get("conditions", {})
        
        if not operator or not conditions:
            logger.warning(f"Invalid logical condition format: {condition}")
            return None
        
        # Recursively convert child conditions
        search_conditions = []
        for key, child_condition in conditions.items():
            if isinstance(child_condition, dict):
                child_search = await self._convert_where_to_search(child_condition)
                if child_search:
                    search_conditions.append(child_search)
        
        if not search_conditions:
            logger.warning(f"No valid search conditions found in logical condition")
            return None
            
        if len(search_conditions) == 1:
            return search_conditions[0]
            
        if operator == "AND":
            return {
                "compound": {
                    "must": search_conditions
                }
            }
        elif operator == "OR":
            return {
                "compound": {
                    "should": search_conditions
                }
            }
        
        logger.warning(f"Unsupported logical operator: {operator}")
        return None

    async def _handle_comparison_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles comparison operators for Atlas Search.
        """
        left = condition.get("left", {})
        operator = condition.get("operator")
        right = condition.get("right", {})
        
        # Extract path from left side
        if left.get("type") != "dataMatchPath":
            return None
            
        # Extract AQL path and use translate_aql_path
        aql_path = self._extract_aql_path_from_path_object(left)
        if not aql_path:
            return None
            
        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
            # For search operations, we use the data path directly in the sn.data structure
            search_path = f"sn.{data_path}" if data_path else "sn.data"
        except Exception as e:
            logger.warning(f"Could not resolve path {aql_path}: {e}")
            return None
            
        value = self.value_formatter.format_literal_value(right)
        
        # Map AQL operators to Atlas Search
        if operator == "=":
            if isinstance(value, str):
                return {
                    "text": {
                        "query": value,
                        "path": search_path
                    }
                }
            else:
                return {
                    "equals": {
                        "path": search_path,
                        "value": value
                    }
                }
        
        elif operator in [">=", ">", "<=", "<"]:
            range_condition = {}
            if operator == ">=":
                range_condition["gte"] = value
            elif operator == ">":
                range_condition["gt"] = value
            elif operator == "<=":
                range_condition["lte"] = value
            elif operator == "<":
                range_condition["lt"] = value
                
            return {
                "range": {
                    "path": search_path,
                    **range_condition
                }
            }
        
        elif operator == "!=":
            return {
                "compound": {
                    "mustNot": [{
                        "equals": {
                            "path": search_path,
                            "value": value
                        }
                    }]
                }
            }
        
        return None

    async def _handle_logical_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles logical operators (AND, OR) for Atlas Search.
        """
        operator = condition.get("operator")
        conditions = condition.get("conditions", [])
        
        if not conditions:
            return None
            
        # Recursively convert child conditions
        search_conditions = []
        for child_condition in conditions:
            child_search = await self._convert_where_to_search(child_condition)
            if child_search:
                search_conditions.append(child_search)
        
        if not search_conditions:
            return None
            
        if len(search_conditions) == 1:
            return search_conditions[0]
            
        if operator == "AND":
            return {
                "compound": {
                    "must": search_conditions
                }
            }
        elif operator == "OR":
            return {
                "compound": {
                    "should": search_conditions,
                    "minimumShouldMatch": 1
                }
            }
        
        return None

    async def _handle_exists_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles EXISTS conditions for Atlas Search.
        """
        path = condition.get("path", {})
        
        if path.get("type") != "dataMatchPath":
            return None
            
        # Extract AQL path and use translate_aql_path
        aql_path = self._extract_aql_path_from_path_object(path)
        if not aql_path:
            return None
            
        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
            # For search operations, we use the data path directly in the sn.data structure
            search_path = f"sn.{data_path}" if data_path else "sn.data"
        except Exception as e:
            logger.warning(f"Could not resolve path {aql_path}: {e}")
            return None
        
        return {
            "exists": {
                "path": search_path
            }
        }

    async def _handle_matches_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles MATCHES conditions (regex) for Atlas Search.
        """
        path = condition.get("path", {})
        pattern = condition.get("pattern")
        
        if path.get("type") != "dataMatchPath" or not pattern:
            return None
            
        # Extract AQL path and use translate_aql_path
        aql_path = self._extract_aql_path_from_path_object(path)
        if not aql_path:
            return None
            
        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
            # For search operations, we use the data path directly in the sn.data structure
            search_path = f"sn.{data_path}" if data_path else "sn.data"
        except Exception as e:
            logger.warning(f"Could not resolve path {aql_path}: {e}")
            return None
        
        return {
            "regex": {
                "path": search_path,
                "query": pattern
            }
        }

    async def build_additional_match_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Builds additional $match stage for conditions that cannot be handled by $search.
        For example, EHR-level conditions or complex nested conditions.
        """
        contains_clause = ast.get("contains")
        additional_conditions = {}
        
        # Process CONTAINS clause for structural filtering
        if contains_clause:
            contains_conditions = await self._process_contains_clause(contains_clause)
            if contains_conditions:
                additional_conditions.update(contains_conditions)
        
        return {"$match": additional_conditions} if additional_conditions else None

    async def _process_contains_clause(self, contains_clause: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Processes CONTAINS clause for the search collection structure.
        """
        if not contains_clause:
            return None
            
        rmType = contains_clause.get("rmType")
        predicate = contains_clause.get("predicate")
        
        # Handle COMPOSITION level filtering
        if rmType == "COMPOSITION" and predicate:
            conditions = {}
            
            # Handle archetype_id predicate
            if predicate.get("type") == "archetype_id":
                archetype_id = predicate.get("value")
                if archetype_id and self.format_resolver.archetype_resolver:
                    # Resolve archetype ID to numeric code
                    try:
                        numeric_code = await self.format_resolver.archetype_resolver.resolve_archetype_to_code(archetype_id)
                        if numeric_code is not None:
                            conditions["tid"] = numeric_code
                    except Exception as e:
                        logger.warning(f"Could not resolve archetype {archetype_id}: {e}")
                        # Fallback: no filtering by archetype
                        pass
            
            return conditions
            
        return None

    def build_lookup_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Builds $lookup stage to get complete composition data from flatten_compositions.
        This enables hybrid data access: search index for filtering, full data for projection.
        """
        # Check if we need full composition data based on SELECT clause
        if self._needs_full_composition_data(ast):
            logger.info(f"Adding $lookup stage to get complete composition data from {self.full_compositions_collection}")
            return {
                "$lookup": {
                    "from": self.full_compositions_collection,
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "full_composition"
                }
            }
        else:
            logger.debug("No metadata fields detected - skipping $lookup stage for optimal performance")
        return None

    def _needs_full_composition_data(self, ast: Dict[str, Any]) -> bool:
        """
        Determines if the query needs full composition data based on SELECT clause.
        Returns True if any selected field requires data not available in search collection.
        """
        columns = ast.get("select", {}).get("columns", {})
        if not columns:
            return False
        
        for col_data in columns.values():
            path = col_data.get("path") or col_data.get("value", {})
            if path.get("type") == "dataMatchPath":
                aql_path = self._extract_aql_path_from_path_object(path)
                if aql_path and (self._is_composition_metadata_path(aql_path) or self._is_archetype_specific_path(aql_path)):
                    return True
        return False

    def _is_composition_metadata_path(self, aql_path: str) -> bool:
        """
        Determines if an AQL path refers to composition metadata that might not be
        available in the search collection's reduced sn array.
        """
        if not aql_path:
            return False
            
        # Composition metadata patterns that are often missing from search collection
        metadata_patterns = [
            '/name/value',
            '/name/defining_code',
            '/context/start_time',
            '/context/end_time', 
            '/context/setting',
            '/context/location',
            '/context/health_care_facility',
            '/context/participations',
            '/composer',
            '/category',
            '/territory',
            '/language'
        ]
        
        # Check if path contains any metadata patterns
        for pattern in metadata_patterns:
            if pattern in aql_path:
                return True
                
        # Also check for root composition elements (usually have simple path patterns)
        variable = aql_path.split('/')[0]
        if variable == self.composition_alias:
            # If it's a composition path but not uid/value (which we handle specially)
            if not aql_path.endswith('/uid/value'):
                # Check if it's likely a root composition field
                path_parts = aql_path.split('/')
                if len(path_parts) <= 3:  # e.g., c/name/value
                    return True
        
        return False

    def _is_archetype_specific_path(self, aql_path: str) -> bool:
        """
        Determines if an AQL path contains archetype-specific elements (AT codes)
        that might not be indexed in the search collection.
        
        This handles cases like:
        - med_ac/description[at0017]/items[at0020]/value/defining_code/code_string
        - action_var/activities[at0001]/description[at0002]/items[at0003]/value
        
        Returns True if the path contains AT codes, indicating it's archetype-specific
        and might need full composition data for resolution.
        """
        if not aql_path:
            return False
        
        # Check if path contains AT code references in square brackets
        # AT codes follow pattern: at followed by digits (e.g., at0001, at0017, at0020)
        import re
        at_code_pattern = r'\[at\d+\]'
        
        if re.search(at_code_pattern, aql_path):
            logger.debug(f"Detected archetype-specific path with AT codes: {aql_path}")
            return True
        
        # Additionally check for paths that go through archetype elements
        # These are typically deeper than simple composition metadata
        variable = aql_path.split('/')[0] 
        
        # Skip composition alias paths (already handled by metadata check)
        if variable == self.composition_alias:
            return False
            
        # Check if this is a deep path through archetype structures
        # Archetype paths typically have more segments and contain structural elements
        path_parts = aql_path.split('/')
        if len(path_parts) > 3:  # Deep paths are more likely to be archetype-specific
            # Look for common archetype structural elements
            archetype_elements = [
                'description', 'items', 'activities', 'activity', 'data', 'state', 
                'protocol', 'events', 'event', 'value', 'values', 'items_single',
                'items_multiple', 'other_context'
            ]
            
            # If the path contains archetype structural elements, it's likely archetype-specific
            for part in path_parts[1:]:  # Skip variable name
                # Remove any bracket notation to get the element name
                element_name = re.sub(r'\[.*?\]', '', part)
                if element_name in archetype_elements:
                    logger.debug(f"Detected deep archetype path with structural elements: {aql_path}")
                    return True
        
        return False

    async def _build_hybrid_field_projection(self, aql_path: str, alias: str) -> Dict[str, Any]:
        """
        Builds projection logic that intelligently routes field access between 
        search collection (sn array) and full composition data (cn array from lookup).
        """
        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
            
            # Determine if this is likely composition metadata or archetype-specific
            is_metadata = self._is_composition_metadata_path(aql_path)
            is_archetype_specific = self._is_archetype_specific_path(aql_path)
            
            # Prefer full composition data for metadata or archetype-specific paths
            prefer_full_composition = is_metadata or is_archetype_specific
            
            if prefer_full_composition:
                logger.debug(f"Using full-composition-priority projection for {aql_path} (metadata: {is_metadata}, archetype: {is_archetype_specific})")
            else:
                logger.debug(f"Using search-collection-priority projection for {aql_path}")
            
            if path_regex_pattern is None:
                # Direct field access
                if data_path == "comp_id":
                    return "$comp_id"
                else:
                    return f"${data_path}"
            else:
                # Complex path - use hybrid logic
                if prefer_full_composition:
                    # For metadata or archetype-specific fields, prefer full composition data
                    return {
                        "$let": {
                            "vars": {
                                "full_comp": {"$first": "$full_composition"},
                                "search_element": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$sn",
                                            "as": "item",
                                            "cond": {"$regexMatch": {"input": "$$item.p", "regex": path_regex_pattern}}
                                        }
                                    }
                                }
                            },
                            "in": {
                                "$cond": {
                                    "if": {"$ne": ["$$full_comp", None]},
                                    "then": {
                                        # Try to get from full composition first
                                        "$let": {
                                            "vars": {
                                                "full_element": {
                                                    "$first": {
                                                        "$filter": {
                                                            "input": "$$full_comp.cn",
                                                            "as": "item",
                                                            "cond": {"$regexMatch": {"input": "$$item.p", "regex": path_regex_pattern}}
                                                        }
                                                    }
                                                }
                                            },
                                            "in": {
                                                "$cond": {
                                                    "if": {"$ne": ["$$full_element", None]},
                                                    "then": f"$$full_element.{data_path}",
                                                    "else": {
                                                        # Fallback to search collection
                                                        "$cond": {
                                                            "if": {"$ne": ["$$search_element", None]},
                                                            "then": f"$$search_element.{data_path}",
                                                            "else": None
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "else": {
                                        # No full composition, use search collection
                                        "$cond": {
                                            "if": {"$ne": ["$$search_element", None]},
                                            "then": f"$$search_element.{data_path}",
                                            "else": None
                                        }
                                    }
                                }
                            }
                        }
                    }
                else:
                    # For non-metadata fields, prefer search collection (performance)
                    return {
                        "$let": {
                            "vars": {
                                "search_element": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$sn",
                                            "as": "item",
                                            "cond": {"$regexMatch": {"input": "$$item.p", "regex": path_regex_pattern}}
                                        }
                                    }
                                },
                                "full_comp": {"$first": "$full_composition"}
                            },
                            "in": {
                                "$cond": {
                                    "if": {"$ne": ["$$search_element", None]},
                                    "then": f"$$search_element.{data_path}",
                                    "else": {
                                        # Fallback to full composition if not in search
                                        "$cond": {
                                            "if": {"$ne": ["$$full_comp", None]},
                                            "then": {
                                                "$let": {
                                                    "vars": {
                                                        "full_element": {
                                                            "$first": {
                                                                "$filter": {
                                                                    "input": "$$full_comp.cn",
                                                                    "as": "item",
                                                                    "cond": {"$regexMatch": {"input": "$$item.p", "regex": path_regex_pattern}}
                                                                }
                                                            }
                                                        }
                                                    },
                                                    "in": {
                                                        "$cond": {
                                                            "if": {"$ne": ["$$full_element", None]},
                                                            "then": f"$$full_element.{data_path}",
                                                            "else": None
                                                        }
                                                    }
                                                }
                                            },
                                            "else": None
                                        }
                                    }
                                }
                            }
                        }
                    }
        except Exception as e:
            logger.warning(f"Could not resolve path {aql_path}: {e}")
            return f"${alias}"

    def build_let_stage(self) -> Optional[Dict[str, Any]]:
        """
        Constructs the $addFields stage for LET variables.
        """
        if not self.let_variables:
            return None
        
        add_fields = {}
        
        for var_name, expression in self.let_variables.items():
            # Resolve the expression to MongoDB syntax
            resolved_expr = self._resolve_expression(expression)
            add_fields[var_name] = resolved_expr
        
        return {"$addFields": add_fields}

    async def build_project_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $project stage from the SELECT clause for search collection.
        """
        columns = ast.get("select", {}).get("columns", {})
        if not columns:
            return None

        projection = {"_id": 0}
        
        for col_data in columns.values():
            alias = col_data.get("alias")
            # The path is actually in the 'value' field based on AST structure
            path = col_data.get("path") or col_data.get("value", {})
            
            # Generate alias if not provided
            if not alias:
                if path.get("type") == "dataMatchPath":
                    aql_path = path.get("path")
                    if aql_path:
                        # Generate alias from path: c/uid/value -> c_uid_value
                        alias = aql_path.replace("/", "_")
                    else:
                        continue
                else:
                    continue
            
            if path.get("type") == "dataMatchPath":
                # Extract AQL path from the path object
                aql_path = self._extract_aql_path_from_path_object(path)
                
                if aql_path:
                    variable = aql_path.split('/')[0]
                    
                    # Handle EHR-level paths
                    if variable == self.ehr_alias:
                        if 'ehr_id' in aql_path:
                            projection[alias] = "$ehr_id"
                        else:
                            projection[alias] = f"${alias}"
                    
                    # Handle composition-level paths with hybrid data routing
                    elif variable == self.composition_alias:
                        # Special handling for composition UID - it's stored in document _id
                        if '/uid/value' in aql_path or aql_path.endswith('/uid/value'):
                            projection[alias] = "$_id"
                        else:
                            # Route field to appropriate data source
                            projection[alias] = await self._build_hybrid_field_projection(aql_path, alias)
                    else:
                        # Handle other variable types with hybrid projection
                        projection[alias] = await self._build_hybrid_field_projection(aql_path, alias)
                else:
                    # Fallback to alias if path cannot be extracted
                    projection[alias] = f"${alias}"
            
            elif path.get("type") == "literal":
                projection[alias] = self.value_formatter.format_literal_value(path)
            
            else:
                # Default case
                projection[alias] = f"${alias}"
        
        return {"$project": projection}

    def build_sort_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $sort stage from the ORDER BY clause.
        """
        order_by = ast.get("orderBy", {})
        
        if not order_by or not order_by.get("columns"):
            return None
            
        sort_spec = {}
        columns = order_by.get("columns", {})
        
        for col_data in columns.values():
            alias = col_data.get("alias")
            direction = col_data.get("direction", "ASC")
            
            if alias:
                sort_spec[alias] = 1 if direction.upper() == "ASC" else -1
            
        return {"$sort": sort_spec} if sort_spec else None

    def build_limit_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $limit stage from the LIMIT clause.
        """
        limit_value = ast.get("limit")
        
        if limit_value is None:
            return None
            
        try:
            limit_int = int(limit_value)
            if limit_int > 0:
                return {"$limit": limit_int}
        except (ValueError, TypeError):
            pass
            
        return None

    def _extract_aql_path_from_path_object(self, path_obj: Dict[str, Any]) -> Optional[str]:
        """
        Extracts the full AQL path from a path object.
        Handles both complex structure (variable + segments) and simple structure (direct path).
        """
        if path_obj.get("type") != "dataMatchPath":
            return None
        
        # First check if path is directly available (simplified AST structure)
        direct_path = path_obj.get("path")
        if direct_path:
            return direct_path
            
        # Fallback to complex structure (variable + segments)
        variable = path_obj.get("variable")
        segments = path_obj.get("segments", [])
        
        if not variable:
            return None
            
        # Construct full path: variable/segment1/segment2/...
        path_parts = [variable]
        for segment in segments:
            if isinstance(segment, dict):
                field = segment.get("field")
                if field:
                    path_parts.append(field)
            elif isinstance(segment, str):
                path_parts.append(segment)
                
        return "/".join(path_parts)

    def _resolve_expression(self, expression: Dict[str, Any]) -> Any:
        """
        Resolves an expression to its MongoDB representation for search collection.
        """
        expr_type = expression.get("type")
        
        if expr_type == "literal":
            return self.value_formatter.format_literal_value(expression)
        
        elif expr_type == "dataMatchPath":
            # For search collection context, we need to handle sn array
            # This is a simplified version - full implementation would need
            # to properly resolve paths within the sn array structure
            return f"$sn.data"
        
        # Default: return as is
        return expression

    def _format_date_value_for_search(self, value: Any) -> Any:
        """
        Formats date values for Atlas Search compatibility.
        For embedded document queries, Atlas Search expects actual datetime objects, not string representations.
        """
        if isinstance(value, str):
            # Check if it's already a date string
            if 'T' in value and ('Z' in value or '+' in value or value.endswith('00:00:00')):
                try:
                    from datetime import datetime
                    # Parse and return as actual datetime object for MongoDB
                    parsed_date = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    # Return the datetime object directly - MongoDB will handle ISODate conversion
                    return parsed_date
                except:
                    # If parsing fails, return original value
                    return value
            else:
                # Try to ensure it's a valid date string
                try:
                    from datetime import datetime
                    # Parse and return as actual datetime object
                    parsed_date = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    return parsed_date
                except:
                    # If parsing fails, return original value
                    return value
        else:
            # For non-string values (numbers, etc.), return as-is
            return value

    def _build_embedded_document_range_query(self, search_path: str, operator: str, value: Any) -> Dict[str, Any]:
        """
        Builds an embedded document range query for Atlas Search.
        Converts paths like 'sn.data.time.value' into proper embeddedDocument queries.
        
        For Atlas Search embedded documents, we need:
        - embeddedDocument.path: The context path to the embedded document level
        - operator.range.path: The full absolute path from document root
        
        Generic logic for determining embedded document context:
        - For paths ending in .value: Use parent path as embedded context
        - For direct data fields: Use sn.data as embedded context
        - Handle arbitrary nesting levels (sn.data.a.b.c.value -> sn.data.a.b.c)
        
        Args:
            search_path: The full path like 'sn.data.time.value'
            operator: The comparison operator (>, <, >=, <=)
            value: The value to compare against
            
        Returns:
            Atlas Search embeddedDocument query structure
        """
        if not search_path.startswith("sn."):
            logger.warning(f"Expected sn. prefix in search path: {search_path}")
            return None
        
        # Build the range condition
        range_condition = {}
        if operator == ">=":
            range_condition["gte"] = value
        elif operator == ">":
            range_condition["gt"] = value
        elif operator == "<=":
            range_condition["lte"] = value
        elif operator == "<":
            range_condition["lt"] = value
        
        # Determine the embedded document context intelligently
        # The key insight: the embedded document path should be the parent of the final field
        
        path_parts = search_path.split('.')
        
        if len(path_parts) >= 3 and path_parts[0] == "sn" and path_parts[1] == "data":
            if search_path.endswith('.value') and len(path_parts) >= 4:
                # For paths ending in .value (like sn.data.time.value or sn.data.ism_transition.current_state.value)
                # The embedded document context is everything except the final .value
                embedded_context_parts = path_parts[:-1]  # Remove the final .value part
                embedded_document_path = ".".join(embedded_context_parts)
            elif len(path_parts) == 3:
                # Direct field in data (like sn.data.archetype_node_id)
                embedded_document_path = "sn.data"
            else:
                # For other nested structures, use the parent path
                # This handles cases like sn.data.a.b.c.d where we want sn.data.a.b.c
                embedded_context_parts = path_parts[:-1]  # Remove the final field
                embedded_document_path = ".".join(embedded_context_parts)
            
            return {
                "embeddedDocument": {
                    "path": embedded_document_path,
                    "operator": {
                        "range": {
                            "path": search_path,  # Full absolute path
                            **range_condition
                        }
                    }
                }
            }
        else:
            # Fallback for any other sn.* pattern
            return {
                "embeddedDocument": {
                    "path": "sn",
                    "operator": {
                        "range": {
                            "path": search_path,  # Full absolute path
                            **range_condition
                        }
                    }
                }
            }

    def _build_embedded_document_exists_query(self, search_path: str) -> Dict[str, Any]:
        """
        Builds an embedded document exists query for Atlas Search.
        Uses the same intelligent path resolution logic as range queries.
        """
        if not search_path.startswith("sn."):
            return {
                "exists": {
                    "path": search_path
                }
            }
        
        # Use the same logic as range queries for determining embedded document context
        path_parts = search_path.split('.')
        
        if len(path_parts) >= 3 and path_parts[0] == "sn" and path_parts[1] == "data":
            if search_path.endswith('.value') and len(path_parts) >= 4:
                # For paths ending in .value - embedded context is parent path
                embedded_context_parts = path_parts[:-1]
                embedded_document_path = ".".join(embedded_context_parts)
            elif len(path_parts) == 3:
                # Direct field in data
                embedded_document_path = "sn.data"
            else:
                # For other nested structures, use parent path
                embedded_context_parts = path_parts[:-1]
                embedded_document_path = ".".join(embedded_context_parts)
            
            return {
                "embeddedDocument": {
                    "path": embedded_document_path,
                    "operator": {
                        "exists": {
                            "path": search_path  # Full absolute path
                        }
                    }
                }
            }
        else:
            # Fallback for any other sn.* pattern
            return {
                "embeddedDocument": {
                    "path": "sn",
                    "operator": {
                        "exists": {
                            "path": search_path  # Full absolute path
                        }
                    }
                }
            }

    def _build_embedded_document_equals_query(self, search_path: str, value: Any) -> Dict[str, Any]:
        """
        Builds an embedded document equals query for Atlas Search.
        Uses the same intelligent path resolution logic as range queries.
        """
        if not search_path.startswith("sn."):
            if isinstance(value, str):
                return {
                    "text": {
                        "query": value,
                        "path": search_path
                    }
                }
            else:
                return {
                    "equals": {
                        "path": search_path,
                        "value": value
                    }
                }
        
        # Use the same logic as range queries for determining embedded document context
        path_parts = search_path.split('.')
        
        # Determine the operator query based on value type
        if isinstance(value, str):
            operator_query = {
                "text": {
                    "query": value,
                    "path": search_path  # Full absolute path
                }
            }
        else:
            operator_query = {
                "equals": {
                    "path": search_path,  # Full absolute path
                    "value": value
                }
            }
        
        if len(path_parts) >= 3 and path_parts[0] == "sn" and path_parts[1] == "data":
            if search_path.endswith('.value') and len(path_parts) >= 4:
                # For paths ending in .value - embedded context is parent path
                embedded_context_parts = path_parts[:-1]
                embedded_document_path = ".".join(embedded_context_parts)
            elif len(path_parts) == 3:
                # Direct field in data
                embedded_document_path = "sn.data"
            else:
                # For other nested structures, use parent path
                embedded_context_parts = path_parts[:-1]
                embedded_document_path = ".".join(embedded_context_parts)
            
            return {
                "embeddedDocument": {
                    "path": embedded_document_path,
                    "operator": operator_query
                }
            }
        else:
            # Fallback for any other sn.* pattern
            return {
                "embeddedDocument": {
                    "path": "sn",
                    "operator": operator_query
                }
            }
