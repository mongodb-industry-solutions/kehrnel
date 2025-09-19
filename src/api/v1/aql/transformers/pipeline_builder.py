# src/api/v1/aql/transformers/pipeline_builder.py
from typing import Dict, Any, List, Optional
from .condition_processor import ConditionProcessor
from .value_formatter import ValueFormatter
import uuid
from bson import Binary


class PipelineBuilder:
    """
    Builds individual MongoDB aggregation pipeline stages.
    """

    def __init__(self, ehr_alias: str, composition_alias: str, schema_config: Dict[str, str], 
                 path_resolver, context_map: Dict[str, Dict], let_variables: Dict[str, Any] = None):
        self.ehr_alias = ehr_alias
        self.composition_alias = composition_alias
        self.schema_config = schema_config
        self.path_resolver = path_resolver
        self.context_map = context_map
        self.let_variables = let_variables or {}
        self.condition_processor = ConditionProcessor(
            ehr_alias, composition_alias, schema_config, path_resolver, let_variables
        )
        self.value_formatter = ValueFormatter()

    def build_match_stage(self, ast: Dict[str, Any], ehr_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Constructs the $match stage of the aggregation pipeline."""
        where_clause = ast.get("where")
        
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
            # Convert string UUID to proper BSON Binary for MongoDB
            try:
                uuid_obj = uuid.UUID(ehr_id)
                match_conditions['ehr_id'] = Binary.from_uuid(uuid_obj)
            except (ValueError, TypeError):
                # If conversion fails, use as string (fallback)
                match_conditions['ehr_id'] = ehr_id
        
        # Add composition-level conditions with proper OR/AND support
        if comp_conditions_structure:
            comp_array_field = self.schema_config['composition_array']
            match_conditions[comp_array_field] = self.condition_processor.build_composition_match(comp_conditions_structure)
        
        return {"$match": match_conditions} if match_conditions else None

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

    def build_project_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
                # Process composition/observation columns
                path_regex_part, data_path = self.path_resolver.translate_aql_path(aql_path)
                base_path_regex = self.path_resolver.build_full_path_regex(variable)
                
                # Use configurable field names
                composition_array_field = self.schema_config['composition_array']
                path_field = self.schema_config['path_field']
                
                # Combine path prefix with base regex properly
                full_path_regex = self.path_resolver.combine_path_regex(base_path_regex, path_regex_part)
                
                projection[alias] = {
                    "$let": {
                        "vars": {
                            "target_element": {
                                "$first": {
                                    "$filter": {
                                        "input": f"${composition_array_field}",
                                        "as": "item",
                                        "cond": {"$regexMatch": {"input": f"$$item.{path_field}", "regex": full_path_regex}}
                                    }
                                }
                            }
                        },
                        "in": f"$$target_element.{data_path}"
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
                raise ValueError(f"LIMIT value must be positive, got: {limit_int}")
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
                    mapped_field = self.path_resolver.map_ehr_path_to_field(field_path)
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
            field = self.path_resolver.resolve_path_to_mongo_field(condition["path"])
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
