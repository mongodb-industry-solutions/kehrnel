# src/kehrnel/api/compatibility/v1/aql/transformers/pipeline_builder.py

from typing import Dict, Any, List, Optional
from .condition_processor import ConditionProcessor
from .value_formatter import ValueFormatter
from .format_resolver import FormatResolver
import uuid
from bson import Binary

TEMPLATE_PATTERNS = {
    # Mapping of archetype IDs to template name patterns for fallback matching
    # Used when archetype resolver cannot find numeric codes in database
    "openEHR-EHR-COMPOSITION.vaccination_list.v0": ["HC3 Immunization List", "vaccination", "immunization"],
    "openEHR-EHR-COMPOSITION.tumour.v0": ["T-IGR-TUMOUR-SUMMARY", "tumour", "tumor"],
    "openEHR-EHR-COMPOSITION.encounter.v1": ["encounter", "visit"]
}


class PipelineBuilder:
    """
    Builds individual MongoDB aggregation pipeline stages.
    """

    def __init__(self, ehr_alias: str, composition_alias: str, schema_config: Dict[str, str], 
                 format_resolver: FormatResolver, context_map: Dict[str, Dict], let_variables: Dict[str, Any] = None):
        self.ehr_alias = ehr_alias
        self.composition_alias = composition_alias
        self.schema_config = schema_config
        self.format_resolver = format_resolver
        self.context_map = context_map
        self.let_variables = let_variables or {}
        self.format = schema_config.get('format', 'full')
        
        # Use the provided FormatResolver (which should have ArchetypeResolver configured)
        self.format_resolver = format_resolver
        
        self.condition_processor = ConditionProcessor(
            ehr_alias, composition_alias, schema_config, format_resolver, let_variables
        )
        self.value_formatter = ValueFormatter()

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
        if ehr_id and 'ehr_id' not in match_conditions:
            # For shortened format collections, keep EHR ID as string to match document format
            match_conditions['ehr_id'] = ehr_id
        
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

    async def build_project_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Constructs the $project stage from the SELECT clause."""
        columns = ast.get("select", {}).get("columns", {})
        if not columns:
            return None

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
                    # Convert UUID binary to string representation
                    projection[alias] = {"$toString": "$ehr_id"}
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
                    
                    # Use MongoDB's $regexMatch which returns boolean for $filter condition
                    projection[alias] = {
                        "$let": {
                            "vars": {
                                "target_element": {
                                    "$first": {
                                        "$filter": {
                                            "input": f"${composition_array_field}",
                                            "as": "item",
                                            "cond": {"$regexMatch": {"input": f"$$item.{path_field}", "regex": path_regex_pattern}}
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
        return {"$project": projection}

    def build_sort_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $sort stage from the ORDER BY clause.
        
        Returns:
            Optional[Dict[str, Any]]: MongoDB $sort stage or None if no ORDER BY clause
        """
        order_by = ast.get("orderBy", {})
        
        # Check if orderBy exists and has columns
        if not order_by or not order_by.get("columns"):
            return None
            
        sort_spec = {}
        columns = order_by.get("columns", {})
        
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
            
        rmType = contains_clause.get("rmType")
        predicate = contains_clause.get("predicate")
        
        # Handle COMPOSITION level filtering
        if rmType == "COMPOSITION" and predicate:
            path = predicate.get("path")
            operator = predicate.get("operator")
            value = predicate.get("value")
            
            if path == "archetype_node_id" and operator == "=" and value:
                # For shortened format, use archetype resolver to get numeric code
                if self.format == 'shortened':
                    # Use archetype resolver to get the numeric code for this archetype
                    archetype_resolver = self.format_resolver.archetype_resolver
                    if archetype_resolver:
                        archetype_code = await archetype_resolver.get_archetype_code(value)
                        if archetype_code is not None:
                            # Match elements with exact p-value for this archetype
                            return {
                                "$elemMatch": {
                                    "$and": [
                                        {"p": str(archetype_code)},  # Exact match for the archetype code
                                        {"data._type": "COMPOSITION"}  # Ensure it's a composition
                                    ]
                                }
                            }
                        else:
                            patterns = TEMPLATE_PATTERNS.get(value, [value])
                            
                            or_conditions = []
                            for pattern in patterns:
                                or_conditions.extend([
                                    {"data.archetype_details.template_id.value": {"$regex": pattern, "$options": "i"}},
                                    {"data.name.value": {"$regex": pattern, "$options": "i"}}
                                ])
                            
                            return {
                                "$elemMatch": {
                                    "$and": [
                                        {"p": {"$regex": "^\\d+$"}},  # Composition root element
                                        {"data._type": "COMPOSITION"},  # Ensure it's a composition
                                        {"$or": or_conditions}
                                    ]
                                }
                            }
                    else:
                        patterns = TEMPLATE_PATTERNS.get(value, [value])
                        
                        or_conditions = []
                        for pattern in patterns:
                            or_conditions.extend([
                                {"data.archetype_details.template_id.value": {"$regex": pattern, "$options": "i"}},
                                {"data.name.value": {"$regex": pattern, "$options": "i"}}
                            ])
                        
                        return {
                            "$elemMatch": {
                                "$and": [
                                    {"p": {"$regex": "^\\d+$"}},  # Composition root element
                                    {"data._type": "COMPOSITION"},  # Ensure it's a composition
                                    {"$or": or_conditions}
                                ]
                            }
                        }
                else:
                    # For full format, use p field matching
                    return {
                        "$elemMatch": {
                            "p": {"$regex": f"^.*{value}.*$"}
                        }
                    }
        
        # Handle nested CONTAINS (children elements)
        contains_children = contains_clause.get("contains")
        if contains_children:
            # For nested archetype filtering, we need to ensure that the composition
            # contains both the parent archetype AND the nested archetype
            nested_rmType = contains_children.get("rmType")
            nested_predicate = contains_children.get("predicate")
            
            if nested_rmType and nested_predicate:
                nested_path = nested_predicate.get("path")
                nested_operator = nested_predicate.get("operator")
                nested_value = nested_predicate.get("value")
                
                if nested_path == "archetype_node_id" and nested_operator == "=" and nested_value:
                    # For shortened format, add the nested archetype as an additional constraint
                    if self.format == 'shortened':
                        archetype_resolver = self.format_resolver.archetype_resolver
                        if archetype_resolver:
                            nested_archetype_code = await archetype_resolver.get_archetype_code(nested_value)
                            if nested_archetype_code is not None:
                                # Return a condition that requires BOTH the composition archetype AND the nested archetype
                                # This modifies the current return to include both conditions
                                composition_array = self.schema_config['composition_array']
                                
                                # Get the composition archetype code from current predicate processing
                                if rmType == "COMPOSITION" and predicate:
                                    comp_path = predicate.get("path")
                                    comp_operator = predicate.get("operator") 
                                    comp_value = predicate.get("value")
                                    
                                    if comp_path == "archetype_node_id" and comp_operator == "=" and comp_value:
                                        comp_archetype_code = await archetype_resolver.get_archetype_code(comp_value)
                                        if comp_archetype_code is not None:
                                            # For nested archetypes, the p-value follows the hierarchy:
                                            # - Composition: "22" 
                                            # - Action within composition: "24.22"
                                            # - Elements within action: "-2.-1.24.22" etc.
                                            
                                            # Build the hierarchical p-pattern for the nested archetype
                                            nested_p_pattern = f"{nested_archetype_code}.{comp_archetype_code}"
                                            
                                            # Return condition requiring the nested archetype to exist
                                            # The composition archetype will be matched by the parent CONTAINS processing
                                            return {
                                                composition_array: {
                                                    "$elemMatch": {
                                                        "p": {"$regex": f"^{nested_p_pattern}$"},
                                                        "data._type": nested_rmType
                                                    }
                                                }
                                            }
            
            # If we can't process the nested contains, continue with just the parent processing
            
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
