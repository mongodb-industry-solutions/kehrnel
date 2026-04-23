# src/kehrnel/api/compatibility/v1/aql/transformers/pipeline_builder.py

from typing import Dict, Any, List, Optional
import re
from .condition_processor import ConditionProcessor
from ..contains_clause import build_shortened_contains_condition, build_shortened_row_fanout_spec
from .value_formatter import ValueFormatter
from .format_resolver import FormatResolver
import uuid
from bson import Binary

TEMPLATE_PATTERNS = {
    # Mapping of archetype IDs to template name patterns for fallback matching
    # Used when archetype resolver cannot find numeric codes in database
    "openEHR-EHR-COMPOSITION.vaccination_list.v0": ["Sample Immunization List", "vaccination", "immunization"],
    "openEHR-EHR-COMPOSITION.tumour.v0": ["T-IGR-TUMOUR-SUMMARY", "tumour", "tumor"],
    "openEHR-EHR-COMPOSITION.encounter.v1": ["encounter", "visit"]
}


class PipelineBuilder:
    """
    Builds individual MongoDB aggregation pipeline stages.
    """

    def __init__(self, ehr_alias: str, composition_alias: str, schema_config: Dict[str, str], 
                 format_resolver: FormatResolver, context_map: Dict[str, Dict], let_variables: Dict[str, Any] = None,
                 version_alias: str | None = None):
        self.ehr_alias = ehr_alias
        self.composition_alias = composition_alias
        self.version_alias = version_alias
        self.schema_config = schema_config
        self.format_resolver = format_resolver
        self.context_map = context_map
        self.let_variables = let_variables or {}
        self.format = schema_config.get('format', 'full')
        
        # Use the provided FormatResolver (which should have ArchetypeResolver configured)
        self.format_resolver = format_resolver
        
        self.condition_processor = ConditionProcessor(
            ehr_alias, composition_alias, schema_config, format_resolver, let_variables, version_alias=version_alias
        )
        self.value_formatter = ValueFormatter()

    def _root_path_regex(self) -> str:
        separator = self.schema_config.get("separator", ":") or ":"
        return rf"^[^{re.escape(separator)}]+$"

    async def build_match_stage(self, ast: Dict[str, Any], ehr_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Constructs the $match stage of the aggregation pipeline."""
        where_clause = ast.get("where")
        contains_clause = ast.get("contains")
        
        # Initialize match conditions even if there's no WHERE clause
        # (we might still need to add EHR ID or other conditions)
        match_conditions = {}

        # Process WHERE clause if it exists
        ehr_conditions = {}
        comp_conditions_structure = None
        
        if where_clause:
            # Process WHERE clause with proper OR/AND support
            processed_where = self.condition_processor.process_where_clause(where_clause)
            
            # Separate EHR-level conditions from composition-level conditions
            ehr_conditions, comp_conditions_structure = self.condition_processor.separate_conditions(processed_where)

        # Add EHR-level conditions
        if ehr_conditions:
            match_conditions.update(ehr_conditions)
        
        # Add external EHR ID if provided and not already in conditions
        ehr_field = self.schema_config.get("ehr_id", "ehr_id")
        if ehr_id and ehr_field not in match_conditions:
            # For shortened format collections, keep EHR ID as string to match document format
            match_conditions[ehr_field] = self.value_formatter.format_id_value(
                ehr_id,
                self.schema_config.get("ehr_id_encoding", "string"),
            )
        
        # Process CONTAINS clause for composition filtering
        if contains_clause:
            contains_conditions = await self._process_contains_clause(contains_clause)
            if contains_conditions:
                comp_array_field = self.schema_config['composition_array']
                if comp_array_field in match_conditions:
                    # Merge with existing composition conditions using $and
                    match_conditions[comp_array_field] = {
                        "$and": [match_conditions[comp_array_field], contains_conditions]
                    }
                else:
                    match_conditions[comp_array_field] = contains_conditions
        
        # Add composition-level conditions with proper OR/AND support
        if comp_conditions_structure:
            comp_array_field = self.schema_config['composition_array']
            comp_match = await self.condition_processor.build_composition_match(comp_conditions_structure)
            if comp_array_field in match_conditions:
                # Merge with existing composition conditions properly
                existing_condition = match_conditions[comp_array_field]
                match_conditions[comp_array_field] = self._merge_composition_conditions(existing_condition, comp_match)
            else:
                match_conditions[comp_array_field] = comp_match
        
        return {"$match": match_conditions} if match_conditions else None

    def _merge_composition_conditions(self, existing: Dict, new: Dict) -> Dict:
        """
        Properly merges two composition-level conditions that might be $elemMatch or $all.
        If both conditions target the same p-value, merge them into a single $elemMatch.
        """
        # Helper function to extract p-value from a condition
        def get_p_value(condition):
            if "$elemMatch" in condition:
                elem_match = condition["$elemMatch"]
                
                # Handle direct p field
                if "p" in elem_match:
                    p_val = elem_match["p"]
                    if isinstance(p_val, str):
                        return p_val
                    elif isinstance(p_val, dict) and "$regex" in p_val:
                        return p_val["$regex"]
                
                # Handle p field inside $and array
                elif "$and" in elem_match:
                    for and_condition in elem_match["$and"]:
                        if "p" in and_condition:
                            p_val = and_condition["p"]
                            if isinstance(p_val, str):
                                return p_val
                            elif isinstance(p_val, dict) and "$regex" in p_val:
                                return p_val["$regex"]
            return None
        
        # Helper function to extract all conditions from $elemMatch 
        def extract_conditions(condition):
            if "$elemMatch" in condition:
                elem_match = condition["$elemMatch"]
                if "$and" in elem_match:
                    # Flatten $and array into individual conditions
                    conditions = {}
                    for and_condition in elem_match["$and"]:
                        conditions.update(and_condition)
                    return conditions
                else:
                    return elem_match
            return {}
        
        # Check if both conditions are $elemMatch targeting the same p-value
        if "$elemMatch" in existing and "$elemMatch" in new:
            existing_p = get_p_value(existing)
            new_p = get_p_value(new)
            
            # If they target the same p-value, merge into single $elemMatch
            if existing_p and new_p and existing_p == new_p:
                # Extract all conditions from both elemMatch
                existing_conditions = extract_conditions(existing)
                new_conditions = extract_conditions(new)
                
                # Merge all conditions
                merged_conditions = {}
                merged_conditions.update(existing_conditions)
                merged_conditions.update(new_conditions)
                
                # Ensure we use the exact p-value match (not regex) when possible
                if existing_p:
                    merged_conditions["p"] = existing_p
                
                return {"$elemMatch": merged_conditions}
            else:
                # Different p-values, use $all
                return {"$all": [existing, new]}
        
        # If existing is $all and new is $elemMatch, add to the $all array
        elif "$all" in existing and "$elemMatch" in new:
            existing_all = existing["$all"]
            return {"$all": existing_all + [new]}
        
        # If existing is $elemMatch and new is $all, add existing to new's $all array
        elif "$elemMatch" in existing and "$all" in new:
            new_all = new["$all"]
            return {"$all": [existing] + new_all}
        
        # If both are $all, combine their arrays
        elif "$all" in existing and "$all" in new:
            return {"$all": existing["$all"] + new["$all"]}
        
        # Default fallback - this shouldn't happen in normal operation
        else:
            return {"$all": [existing, new]}

    def build_let_stage(self) -> Optional[Dict[str, Any]]:
        """
        Constructs the $addFields stage for LET variables.
        This stage adds computed fields to documents based on LET expressions.
        
        Returns:
            Optional[Dict[str, Any]]: MongoDB $addFields stage or None if no LET clause
        """
        if not self.let_variables:
            return None
        
        add_fields = {}
        
        for var_name, expression in self.let_variables.items():
            # Remove $ prefix for field name (MongoDB field names don't use $)
            field_name = var_name.lstrip('$')
            
            # Resolve the expression to MongoDB aggregation expression
            resolved_expression = self._resolve_expression(expression, "let")
            add_fields[field_name] = resolved_expression
        
        return {"$addFields": add_fields}

    def _first_matching_node_value(
        self,
        nodes_expr: Any,
        *,
        path_field: str,
        path_regex_pattern: Any,
        data_path: str,
    ) -> Dict[str, Any]:
        return {
            "$first": {
                "$map": {
                    "input": {
                        "$filter": {
                            "input": nodes_expr,
                            "as": "node",
                            "cond": {
                                "$regexMatch": {
                                    "input": f"$$node.{path_field}",
                                    "regex": path_regex_pattern,
                                }
                            },
                        }
                    },
                    "as": "node",
                    "in": f"$$node.{data_path}",
                }
            }
        }

    async def _get_row_fanout_spec(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self.format != "shortened":
            return None
        return await build_shortened_row_fanout_spec(
            ast,
            ast.get("contains"),
            composition_alias=self.composition_alias,
            archetype_resolver=self.format_resolver.archetype_resolver,
            separator=self.schema_config.get("separator", ":"),
        )

    def _fanout_alias_path_expr(self, leaf_path_expr: Any, alias_code: str) -> Dict[str, Any]:
        separator = self.schema_config.get("separator", ":") or ":"
        return {
            "$let": {
                "vars": {
                    "parts": {"$split": [leaf_path_expr, separator]},
                },
                "in": {
                    "$let": {
                        "vars": {
                            "idx": {"$indexOfArray": ["$$parts", str(alias_code)]},
                        },
                        "in": {
                            "$cond": [
                                {"$gte": ["$$idx", 0]},
                                {
                                    "$reduce": {
                                        "input": {"$slice": ["$$parts", "$$idx", {"$size": "$$parts"}]},
                                        "initialValue": "",
                                        "in": {
                                            "$cond": [
                                                {"$eq": ["$$value", ""]},
                                                "$$this",
                                                {"$concat": ["$$value", separator, "$$this"]},
                                            ]
                                        },
                                    }
                                },
                                None,
                            ]
                        },
                    }
                },
            }
        }

    def _build_fanout_paths_document(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        leaf_expr: Any = "$__fanout_nodes.p"
        target_alias = spec["target_alias"]
        alias_codes = spec.get("alias_codes", {})
        paths: Dict[str, Any] = {}
        for alias in spec.get("aliases", []):
            if alias == target_alias:
                paths[alias] = leaf_expr
            else:
                code = alias_codes.get(alias)
                if code is not None:
                    paths[alias] = self._fanout_alias_path_expr(leaf_expr, str(code))
        return paths

    def _build_fanout_regex_expr(self, alias_path_expr: Any, selector_codes: List[str]) -> Any:
        separator = self.schema_config.get("separator", ":") or ":"
        prefix = separator.join(reversed([str(code) for code in selector_codes]))
        pieces: List[Any] = ["^"]
        if prefix:
            pieces.extend([prefix, separator])
        pieces.extend([alias_path_expr, "$"])
        return {"$concat": pieces}

    async def build_row_fanout_stages(self, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
        spec = await self._get_row_fanout_spec(ast)
        if not spec:
            return []

        composition_array_field = self.schema_config["composition_array"]
        path_field = self.schema_config["path_field"]
        return [
            {
                "$addFields": {
                    "__fanout_nodes": {
                        "$filter": {
                            "input": f"${composition_array_field}",
                            "as": "node",
                            "cond": {
                                "$regexMatch": {
                                    "input": f"$$node.{path_field}",
                                    "regex": spec["target_regex"],
                                }
                            },
                        }
                    }
                }
            },
            {"$unwind": "$__fanout_nodes"},
            {
                "$addFields": {
                    "__fanout_paths": self._build_fanout_paths_document(spec),
                }
            },
        ]

    async def _build_fanout_aware_projection(
        self,
        aql_path: str,
        data_path: str,
        spec: Dict[str, Any],
    ) -> Optional[Any]:
        variable = aql_path.split("/", 1)[0]
        if variable not in set(spec.get("aliases", [])):
            return None

        selector_codes = await self.format_resolver.get_selector_codes(aql_path)
        if variable == spec["target_alias"] and not selector_codes:
            return f"$__fanout_nodes.{data_path}"

        regex_expr = self._build_fanout_regex_expr(f"$__fanout_paths.{variable}", selector_codes)
        return self._first_matching_node_value(
            f"${self.schema_config['composition_array']}",
            path_field=self.schema_config["path_field"],
            path_regex_pattern=regex_expr,
            data_path=data_path,
        )

    async def build_project_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Constructs the $project stage from the SELECT clause."""
        columns = ast.get("select", {}).get("columns", {})
        if not columns:
            return None

        fanout_spec = await self._get_row_fanout_spec(ast)
        projection = {"_id": 0}
        for col_data in columns.values():
            alias = col_data.get("alias")
            value_spec = col_data.get("value", {})
            
            # Check if this is a variable reference
            if value_spec.get("type") == "variable":
                var_name = value_spec.get("name")
                if not alias:
                    alias = var_name.lstrip('$')  # Use variable name as alias
                
                # Reference the field created by the LET stage
                field_name = var_name.lstrip('$')
                projection[alias] = f"${field_name}"
                continue
            
            # Handle regular path-based columns
            aql_path = value_spec.get("path")
            if not aql_path:
                continue
                
            if not alias:  # If no alias, use a more descriptive default based on full path
                path_parts = aql_path.split('/')
                if len(path_parts) >= 2:
                    # Use variable + field name, e.g., "e_ehr_id" or "c_name"
                    alias = f"{path_parts[0]}_{path_parts[1]}"
                else:
                    alias = path_parts[-1]

            variable = aql_path.split('/')[0]

            if variable == self.ehr_alias:  # Use dynamic EHR alias instead of hardcoded 'e'
                if 'ehr_id' in aql_path:
                    projection[alias] = f"${self.schema_config.get('ehr_id', 'ehr_id')}"
                # Don't continue here - let other columns be processed too
            else:
                # Handle different formats - now async
                path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
                
                if path_regex_pattern is None:
                    # Check for special direct field access (like composition UID at document root)
                    if data_path == "comp_id":
                        # Direct field access for composition UID at document root
                        projection[alias] = f"${data_path}"
                    else:
                        # Direct field access (for pure shortened format without cn array)
                        projection[alias] = f"${data_path}"
                else:
                    # Use cn array filtering logic with dynamic p-patterns
                    composition_array_field = self.schema_config['composition_array']
                    path_field = self.schema_config['path_field']

                    fanout_projection = None
                    if fanout_spec:
                        fanout_projection = await self._build_fanout_aware_projection(
                            aql_path,
                            data_path,
                            fanout_spec,
                        )
                    projection[alias] = fanout_projection or self._first_matching_node_value(
                        f"${composition_array_field}",
                        path_field=path_field,
                        path_regex_pattern=path_regex_pattern,
                        data_path=data_path,
                    )
        return {"$project": projection}

    def build_sort_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $sort stage from the ORDER BY clause.
        
        Returns:
            Optional[Dict[str, Any]]: MongoDB $sort stage or None if no ORDER BY clause
        """
        order_by = ast.get("orderBy", {})
        columns = order_by.get("columns") if isinstance(order_by, dict) and isinstance(order_by.get("columns"), dict) else order_by

        # Check if orderBy exists and has columns
        if not isinstance(columns, dict) or not columns:
            return None
            
        sort_spec = {}
        projected_aliases = {
            col.get("value", {}).get("path"): col.get("alias")
            for col in ast.get("select", {}).get("columns", {}).values()
            if isinstance(col, dict) and isinstance(col.get("value"), dict) and col.get("alias")
        }
        
        for col_data in columns.values():
            aql_path = col_data.get("path")
            variable = col_data.get("variable")
            direction = col_data.get("direction", "ASC").upper()
            
            field_name = None
            
            # Handle LET variable references in ORDER BY
            if variable:
                # Use the field name created by the LET stage
                field_name = variable.lstrip('$')
            elif aql_path:
                projected_alias = projected_aliases.get(aql_path)
                if projected_alias:
                    field_name = projected_alias
                else:
                    direct_field = self.format_resolver.resolve_document_field(aql_path)
                    if direct_field:
                        field_name = direct_field
            
            if not field_name and aql_path:
                # Convert AQL path to MongoDB field name using same logic as projection
                path_parts = aql_path.split('/')
                if len(path_parts) >= 2:
                    # Use variable + field name, e.g., "e_ehr_id" or "c_name"
                    field_name = f"{path_parts[0]}_{path_parts[1]}"
                else:
                    field_name = path_parts[-1]
            
            if not field_name:
                continue
                
            # Convert direction to MongoDB sort value
            sort_direction = 1 if direction == "ASC" else -1
            sort_spec[field_name] = sort_direction
            
        return {"$sort": sort_spec} if sort_spec else None

    async def _process_contains_clause(self, contains_clause: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Processes the CONTAINS clause to generate composition filtering conditions.
        
        Args:
            contains_clause: The CONTAINS clause from the AST
            
        Returns:
            MongoDB condition to filter compositions based on structure requirements
        """
        if not contains_clause:
            return None

        if self.format == "shortened":
            shortened_condition = await build_shortened_contains_condition(
                contains_clause,
                self.format_resolver.archetype_resolver,
                path_field=self.schema_config.get("path_field", "p"),
                data_field=self.schema_config.get("data_field", "data"),
                separator=self.schema_config.get("separator", ":"),
            )
            if shortened_condition:
                return shortened_condition

        rmType = contains_clause.get("rmType")
        predicate = contains_clause.get("predicate")
        if rmType != "COMPOSITION" and contains_clause.get("contains"):
            return await self._process_contains_clause(contains_clause.get("contains"))

        # Handle COMPOSITION level filtering
        if rmType == "COMPOSITION" and predicate:
            path = predicate.get("path")
            operator = predicate.get("operator")
            value = predicate.get("value")

            if path == "archetype_node_id" and operator == "=" and value:
                if self.format == "shortened":
                    return {"$elemMatch": {self.schema_config.get("path_field", "p"): {"$regex": self._root_path_regex()}}}
                return {
                    "$elemMatch": {
                        "p": {"$regex": f"^.*{value}.*$"}
                    }
                }

        return None

    def build_limit_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $limit stage from the LIMIT clause.
        
        Returns:
            Optional[Dict[str, Any]]: MongoDB $limit stage or None if no LIMIT clause
        """
        limit_value = ast.get("limit")
        
        # Check if limit exists and is a positive integer
        if limit_value is None:
            return None
            
        try:
            limit_int = int(limit_value)
            if limit_int <= 0:
                raise ValueError(f"LIMIT value must be a positive integer greater than zero, got: {limit_int}")
            return {"$limit": limit_int}
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid LIMIT value: {limit_value}. Must be a positive integer.")

    def build_distinct_stages(self, ast: Dict[str, Any], projected_fields: List[str]) -> List[Dict[str, Any]]:
        """
        Constructs the $group and $replaceRoot stages for DISTINCT queries.
        
        DISTINCT in AQL removes duplicate rows from the result set. In MongoDB,
        this is implemented using:
        1. $group stage: Groups documents by all projected fields (compound _id)
        2. $replaceRoot stage: Flattens the grouped _id back to a normal document
        
        Args:
            ast: The parsed AQL AST
            projected_fields: List of field names that are being projected (output columns)
            
        Returns:
            List[Dict[str, Any]]: List containing $group and $replaceRoot stages,
                                  or empty list if DISTINCT is not requested
        """
        select_clause = ast.get("select", {})
        is_distinct = select_clause.get("distinct", False)
        
        if not is_distinct:
            return []
        
        if not projected_fields:
            return []
        
        # Build the compound _id for $group using all projected fields
        # This ensures we group by all columns to find unique combinations
        group_id = {}
        for field in projected_fields:
            if field != "_id":  # Skip _id as it's handled separately
                # Use the field name as key and reference the projected field value
                group_id[field] = f"${field}"
        
        if not group_id:
            return []
        
        # Build the $group stage
        group_stage = {
            "$group": {
                "_id": group_id
            }
        }
        
        # Build the $replaceRoot stage to flatten the _id back to document fields
        # This converts {_id: {field1: val1, field2: val2}} to {field1: val1, field2: val2}
        replace_root_stage = {
            "$replaceRoot": {
                "newRoot": "$_id"
            }
        }
        
        return [group_stage, replace_root_stage]

    def get_projected_field_names(self, ast: Dict[str, Any]) -> List[str]:
        """
        Extracts the list of field names that will be in the projection output.
        Used for building DISTINCT stages.
        
        Args:
            ast: The parsed AQL AST
            
        Returns:
            List[str]: List of projected field names (aliases or generated names)
        """
        columns = ast.get("select", {}).get("columns", {})
        field_names = []
        
        for col_data in columns.values():
            alias = col_data.get("alias")
            value_spec = col_data.get("value", {})
            
            if alias:
                field_names.append(alias)
            elif value_spec.get("type") == "variable":
                # Variable reference - use variable name as field name
                var_name = value_spec.get("name", "")
                field_names.append(var_name.lstrip('$'))
            elif value_spec.get("path"):
                # Path-based column - generate name from path
                aql_path = value_spec.get("path")
                path_parts = aql_path.split('/')
                if len(path_parts) >= 2:
                    field_names.append(f"{path_parts[0]}_{path_parts[1]}")
                else:
                    field_names.append(path_parts[-1])
        
        return field_names

    def _resolve_expression(self, expression: Dict[str, Any], context: str = "select") -> Any:
        """
        Resolves an expression (from LET or elsewhere) to its MongoDB representation.
        
        Args:
            expression: The expression dictionary from the AST
            context: The context where the expression is being resolved
        
        Returns:
            MongoDB expression or literal value
        """
        expr_type = expression.get("type")
        
        if expr_type == "literal":
            return expression.get("value")
        
        elif expr_type == "dataMatchPath":
            path = expression.get("path")
            if not path:
                return None
                
            # For dataMatchPath in LET context, we need to handle it differently
            # This is a simplified approach - in production, you'd want more sophisticated path resolution
            if "/" in path:
                parts = path.split("/")
                alias = parts[0]
                
                if alias == self.ehr_alias:
                    # EHR field mapping  
                    field_path = "/".join(parts[1:])
                    mapped_field = self.format_resolver.map_ehr_path_to_field(field_path)
                    return f"${mapped_field}"
                elif alias == self.composition_alias:
                    # For composition paths in LET, use array element access
                    field_path = "/".join(parts[1:])
                    data_path = f"{self.schema_config['data_field']}.{field_path.replace('/', '.')}"
                    
                    # Return a more complex expression to find the matching element
                    return {
                        "$let": {
                            "vars": {
                                "target_element": {
                                    "$first": {
                                        "$filter": {
                                            "input": f"${self.schema_config['composition_array']}",
                                            "as": "item",
                                            "cond": {"$regexMatch": {"input": f"$$item.{self.schema_config['path_field']}", "regex": ".*"}}
                                        }
                                    }
                                }
                            },
                            "in": f"$$target_element.{data_path}"
                        }
                    }
            
            return f"${path}"
        
        elif expr_type == "concat":
            # Handle string concatenation using $concat
            operands = expression.get("operands", [])
            resolved_operands = [self._resolve_expression(op, context) for op in operands]
            return {"$concat": resolved_operands}
        
        elif expr_type == "arithmetic":
            # Handle arithmetic operations
            operator = expression.get("operator")
            operands = expression.get("operands", [])
            resolved_operands = [self._resolve_expression(op, context) for op in operands]
            
            if operator == "+":
                return {"$add": resolved_operands}
            elif operator == "-":
                return {"$subtract": resolved_operands}
            elif operator == "*":
                return {"$multiply": resolved_operands}
            elif operator == "/":
                return {"$divide": resolved_operands}
        
        elif expr_type == "conditional":
            # Handle conditional expressions using $cond
            condition = expression.get("condition")
            true_value = self._resolve_expression(expression.get("trueValue"), context)
            false_value = self._resolve_expression(expression.get("falseValue"), context)
            
            # Convert condition to MongoDB expression
            mongo_condition = self._build_condition_expression(condition)
            
            return {
                "$cond": {
                    "if": mongo_condition,
                    "then": true_value,
                    "else": false_value
                }
            }
        
        elif expr_type == "function":
            # Handle function calls
            func_name = expression.get("name")
            if func_name == "CURRENT_YEAR":
                return {"$year": "$$NOW"}
            # Add more functions as needed
        
        # Default: return as is
        return expression

    def _build_condition_expression(self, condition: Dict[str, Any]) -> Dict[str, Any]:
        """
        Builds a MongoDB condition expression from an AST condition.
        """
        from .condition_processor import OPERATOR_MAP
        
        if "path" in condition:
            field = self.format_resolver.resolve_path_to_mongo_field(condition["path"])
            operator = condition.get("operator", "=")
            value = condition.get("value")
            
            if operator == "LIKE":
                # Convert LIKE to regex
                regex_pattern = value.replace("%", ".*")
                return {"$regexMatch": {"input": field, "regex": regex_pattern, "options": "i"}}
            else:
                mongo_op = OPERATOR_MAP.get(operator, "$eq")
                return {mongo_op: [field, value]}
        
        return condition
