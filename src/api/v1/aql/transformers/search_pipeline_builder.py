# src/api/v1/aql/transformers/search_pipeline_builder.py

from typing import Dict, Any, List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from .condition_processor import ConditionProcessor
from .value_formatter import ValueFormatter
from .format_resolver import FormatResolver
import logging

logger = logging.getLogger(__name__)


class SearchPipelineBuilder:
    """
    Builds MongoDB aggregation pipelines that start with $search stages for Atlas Search.
    Designed specifically for the search collection (sm_search3) with 'sn' array structure.
    """

    def __init__(self, ehr_alias: str, composition_alias: str, schema_config: Dict[str, str], 
                 format_resolver: FormatResolver, context_map: Dict[str, Dict], 
                 let_variables: Dict[str, Any] = None, search_index_name: str = "search_compositions_index"):
        self.ehr_alias = ehr_alias
        self.composition_alias = composition_alias
        self.schema_config = schema_config
        self.format_resolver = format_resolver
        self.context_map = context_map
        self.let_variables = let_variables or {}
        self.search_index_name = search_index_name
        
        # For search collection, we use 'sn' instead of 'cn'
        self.search_config = {
            'composition_array': 'sn',  # Search collection uses 'sn' array
            'path_field': 'p',          # Field containing hierarchical path
            'data_field': 'data'        # Field containing RM object data
        }
        
        self.condition_processor = ConditionProcessor(
            ehr_alias, composition_alias, self.search_config, format_resolver, let_variables
        )
        self.value_formatter = ValueFormatter()

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

        # 3. Build the $addFields stage for LET variables
        let_stage = self.build_let_stage()
        if let_stage:
            pipeline.append(let_stage)

        # 4. Build the $project stage from the SELECT clause  
        project_stage = await self.build_project_stage(ast)
        if project_stage:
            pipeline.append(project_stage)

        # 5. Build the $sort stage from ORDER BY clause
        sort_stage = self.build_sort_stage(ast)
        if sort_stage:
            pipeline.append(sort_stage)
        
        # 6. Build the $limit stage from LIMIT clause
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
        
        return None

    async def _convert_where_to_search(self, where_clause: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Converts an AQL WHERE clause to Atlas Search query syntax.
        """
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
                    
                    # Handle composition-level paths
                    elif variable == self.composition_alias:
                        # Special handling for composition UID - it's stored in document _id
                        if '/uid/value' in aql_path or aql_path.endswith('/uid/value'):
                            projection[alias] = "$_id"
                        else:
                            # For other composition paths, use translate_aql_path
                            try:
                                path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
                                
                                if path_regex_pattern is None:
                                    # Direct field access (for cases like composition fields at document root)
                                    if data_path == "comp_id":
                                        projection[alias] = "$comp_id"
                                    else:
                                        projection[alias] = f"${data_path}"
                                else:
                                    # Use sn array filtering logic with dynamic p-patterns for search collection
                                    projection[alias] = {
                                        "$let": {
                                            "vars": {
                                                "target_element": {
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
                                                    "if": {"$ne": ["$$target_element", None]},
                                                    "then": f"$$target_element.{data_path}",
                                                    "else": None  # Return null if no matching element found
                                                }
                                            }
                                        }
                                    }
                            except Exception as e:
                                logger.warning(f"Could not resolve path {aql_path}: {e}")
                                projection[alias] = f"${alias}"
                    else:
                        # Handle other variable types
                        try:
                            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
                            
                            if path_regex_pattern is None:
                                projection[alias] = f"${data_path}" if data_path else f"${alias}"
                            else:
                                # Use sn array filtering logic
                                projection[alias] = {
                                    "$let": {
                                        "vars": {
                                            "target_element": {
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
                                                "if": {"$ne": ["$$target_element", None]},
                                                "then": f"$$target_element.{data_path}",
                                                "else": None
                                            }
                                        }
                                    }
                                }
                        except Exception as e:
                            logger.warning(f"Could not resolve path {aql_path}: {e}")
                            projection[alias] = f"${alias}"
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