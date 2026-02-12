# src/api/v1/aql/transformers/condition_processor.py
from typing import Dict, Any, List, Tuple
from .value_formatter import ValueFormatter


# A mapping from AQL operators to MQL operators
OPERATOR_MAP = {
    "=": "$eq",
    "!=": "$ne",
    ">": "$gt",
    "<": "$lt",
    ">=": "$gte",
    "<=": "$lte",
}


class ConditionProcessor:
    """
    Processes WHERE clause conditions and builds MongoDB match conditions.
    Handles OR/AND logic, EHR vs composition level separation, and condition grouping.
    """

    def __init__(self, ehr_alias: str, composition_alias: str, schema_config: Dict[str, str], 
                 format_resolver, let_variables: Dict[str, Any] = None):
        self.ehr_alias = ehr_alias
        self.composition_alias = composition_alias
        self.schema_config = schema_config
        self.format_resolver = format_resolver
        self.let_variables = let_variables or {}
        self.value_formatter = ValueFormatter()
        self.format = schema_config.get('format', 'full')

    def process_where_clause(self, where_node: Dict) -> Dict:
        """
        Recursively processes WHERE clause maintaining OR/AND structure.
        Returns a structured representation that preserves logical operators.
        """
        if not where_node:
            return {}
        
        operator = where_node.get("operator")
        
        if operator in ["AND", "OR"]:
            # Process logical operator with children
            children = []
            conditions_dict = where_node.get("conditions", {})
            
            for child_cond in conditions_dict.values():
                processed_child = self.process_where_clause(child_cond)
                if processed_child:  # Only add non-empty conditions
                    children.append(processed_child)
            
            return {
                "operator": operator,
                "children": children
            } if children else {}
        else:
            # Base condition (leaf node)
            condition = {
                "type": "condition",
                "path": where_node.get("path"),
                "operator": where_node.get("operator"),
                "value": where_node.get("value")
            }
            
            # Handle LET variable references in WHERE clause
            if "variable" in where_node:
                condition["variable"] = where_node.get("variable")
            
            return condition

    def separate_conditions(self, processed_where: Dict) -> Tuple[Dict, Dict]:
        """
        Separates EHR-level conditions from composition-level conditions.
        Returns (ehr_conditions, comp_conditions_structure)
        """
        ehr_conditions = {}
        comp_conditions = []
        
        self._extract_conditions_by_level(processed_where, ehr_conditions, comp_conditions)
        
        # If we have composition conditions, structure them properly
        comp_structure = None
        if comp_conditions:
            if len(comp_conditions) == 1:
                comp_structure = comp_conditions[0]
            else:
                # Multiple top-level composition conditions should be ANDed
                comp_structure = {
                    "operator": "AND",
                    "children": comp_conditions
                }
        
        return ehr_conditions, comp_structure

    def _extract_conditions_by_level(self, node: Dict, ehr_conditions: Dict, comp_conditions: List):
        """
        Recursively extracts conditions and separates them by EHR vs composition level.
        """
        if not node:
            return
        
        if node.get("type") == "condition":
            # Base condition - check if it's EHR or composition level
            path = node.get("path")
            variable_ref = node.get("variable")
            
            # Handle variable references vs path references
            if variable_ref:
                # For variable references, we treat them as composition-level for now
                # In a full implementation, you'd analyze the variable definition
                comp_conditions.append(node)
            elif path:
                variable = path.split('/')[0]
                
                if variable == self.ehr_alias:
                    # EHR-level condition
                    path_parts = path.split('/')[1:]  # Remove EHR alias
                    if len(path_parts) >= 2 and path_parts[0] == 'ehr_id':
                        ehr_field = 'ehr_id'
                        mql_operator = OPERATOR_MAP.get(node["operator"], "$eq")
                        value = self.value_formatter.format_value(node["value"])
                        
                        # For shortened format collections, keep EHR ID as string to match document format
                        # Don't convert to Binary for now as documents store EHR IDs as strings
                        
                        if mql_operator == "$eq":
                            ehr_conditions[ehr_field] = value
                        else:
                            ehr_conditions[ehr_field] = {mql_operator: value}
                else:
                    # Composition-level condition
                    comp_conditions.append(node)
            else:
                # No path or variable - skip this condition
                pass
        
        elif node.get("operator") in ["AND", "OR"]:
            # Logical operator - need to check if all children are same level
            children = node.get("children", [])
            ehr_children = []
            comp_children = []
            
            # Separate children by level
            for child in children:
                temp_ehr = {}
                temp_comp = []
                self._extract_conditions_by_level(child, temp_ehr, temp_comp)
                
                if temp_ehr:
                    ehr_children.append(child)
                if temp_comp:
                    comp_children.extend(temp_comp)
            
            # Add structured conditions
            if ehr_children:
                # For EHR conditions, we need to handle OR/AND at the MongoDB level
                if node["operator"] == "OR":
                    # This is complex - OR conditions at EHR level need $or
                    # For now, raise an error as this needs special handling
                    raise NotImplementedError("OR conditions at EHR level not yet supported")
                else:
                    # AND conditions - process each child
                    for child in ehr_children:
                        self._extract_conditions_by_level(child, ehr_conditions, [])
            
            if comp_children:
                comp_conditions.append({
                    "operator": node["operator"],
                    "children": comp_children
                })

    async def build_composition_match(self, comp_structure: Dict) -> Dict:
        """
        Builds MongoDB match conditions for composition-level queries with OR/AND support.
        """
        if comp_structure.get("type") == "condition":
            # Single condition
            variable = comp_structure["path"].split('/')[0]
            elem_match = await self._create_elem_match_for_single_condition(variable, comp_structure)
            return {"$elemMatch": elem_match}
        
        elif comp_structure.get("operator") == "AND":
            # AND conditions - use $all with multiple $elemMatch
            elem_matches = []
            
            # Group conditions by variable
            variable_conditions = {}
            for child in comp_structure.get("children", []):
                self._group_conditions_by_variable(child, variable_conditions)
            
            # Create $elemMatch for each variable
            for variable, conditions in variable_conditions.items():
                if conditions:
                    # Convert list of conditions to proper structure
                    if len(conditions) == 1:
                        condition_structure = conditions[0]
                    else:
                        condition_structure = {
                            "operator": "AND",
                            "children": conditions
                        }
                    elem_match = await self._create_elem_match_for_variable_group(variable, condition_structure)
                    elem_matches.append({"$elemMatch": elem_match})
            
            return {"$all": elem_matches} if len(elem_matches) > 1 else elem_matches[0] if elem_matches else {}
        
        elif comp_structure.get("operator") == "OR":
            # OR conditions - use $elemMatch with $or
            or_conditions = []
            
            for child in comp_structure.get("children", []):
                child_match = self.build_composition_match(child)
                if "$elemMatch" in child_match:
                    or_conditions.append(child_match["$elemMatch"])
                elif child_match:
                    or_conditions.append(child_match)
            
            return {"$elemMatch": {"$or": or_conditions}} if or_conditions else {}
        
        return {}

    def _group_conditions_by_variable(self, node: Dict, variable_conditions: Dict):
        """
        Groups conditions by their variable alias for proper $elemMatch construction.
        """
        if node.get("type") == "condition":
            # Determine the variable for grouping
            if node.get("variable"):
                # Variable reference - group by the composition alias since variables are resolved at that level
                variable = self.composition_alias
            elif node.get("path"):
                variable = node["path"].split('/')[0]
            else:
                # Skip conditions without path or variable
                return
                
            if variable not in variable_conditions:
                variable_conditions[variable] = []
            variable_conditions[variable].append(node)
        
        elif node.get("operator") in ["AND", "OR"]:
            # For nested operators, we need to handle them as groups
            for child in node.get("children", []):
                self._group_conditions_by_variable(child, variable_conditions)

    async def _create_elem_match_for_variable_group(self, variable: str, conditions_structure: Dict) -> Dict:
        """
        Creates a single $elemMatch object for conditions on a variable, supporting OR/AND logic.
        """
        if conditions_structure.get("type") == "condition":
            # Single condition
            return await self._create_elem_match_for_single_condition(variable, conditions_structure)
        
        elif conditions_structure.get("operator") == "AND":
            # Multiple conditions on same variable - combine with AND logic
            data_conditions = {}
            path_condition = None
            
            # For shortened format, we need to collect the p-pattern from any condition
            p_pattern_for_shortened = None
            
            for condition in conditions_structure.get("children", []):
                if condition.get("type") == "condition":
                    aql_path = condition["path"]
                    p_regex_part, data_path = await self.format_resolver.translate_aql_path(aql_path)
                    
                    # For shortened format, collect the p-pattern from any condition
                    if self.format == 'shortened' and p_regex_part and not p_pattern_for_shortened:
                        p_pattern_for_shortened = p_regex_part
                    
                    mql_operator = OPERATOR_MAP.get(condition["operator"])
                    if not mql_operator:
                        raise NotImplementedError(f"AQL operator '{condition['operator']}' not supported.")
                    
                    value = self.value_formatter.format_value(condition["value"])
                    
                    # For multiple conditions on same field, combine them
                    if data_path in data_conditions:
                        if isinstance(data_conditions[data_path], dict) and not any(op in data_conditions[data_path] for op in ["$eq", "$ne"]):
                            # Existing condition is a range condition, add to it
                            data_conditions[data_path][mql_operator] = value
                        else:
                            # Convert to $and array if we have conflicting operators
                            existing = data_conditions[data_path]
                            data_conditions[data_path] = {"$and": [existing, {mql_operator: value}]}
                    else:
                        if mql_operator == "$eq":
                            data_conditions[data_path] = value
                        else:
                            data_conditions[data_path] = {mql_operator: value}
            
            # Build path condition based on format
            path_field = self.schema_config['path_field']
            
            if self.format == 'shortened':
                # For shortened format, use the p-pattern directly if available
                if p_pattern_for_shortened:
                    path_condition = {"$regex": p_pattern_for_shortened}
                else:
                    # If no specific p-pattern, match any element (fallback)
                    path_condition = {"$exists": True}
            else:
                # For full format, use the original logic
                base_path_regex = self.format_resolver.build_full_path_regex(variable)
                path_prefix = ""
                
                # Update path prefix if we have specific node identifiers from any condition
                for condition in conditions_structure.get("children", []):
                    if condition.get("type") == "condition":
                        aql_path = condition["path"]
                        p_regex_part, _ = await self.format_resolver.translate_aql_path(aql_path)
                        if p_regex_part and not path_prefix:
                            path_prefix = p_regex_part + "/"
                            break
                
                full_path_regex = self.format_resolver.combine_path_regex(base_path_regex, path_prefix)
                if full_path_regex:
                    path_condition = {"$regex": full_path_regex}
                else:
                    # Fallback if no regex pattern available
                    path_condition = {"$exists": True}
            
            return {
                path_field: path_condition,
                **data_conditions
            }
        
        elif conditions_structure.get("operator") == "OR":
            # OR conditions on same variable
            or_conditions = []
            for condition in conditions_structure.get("children", []):
                condition_match = await self._create_elem_match_for_variable_group(variable, condition)
                or_conditions.append(condition_match)
            
            return {"$or": or_conditions} if or_conditions else {}
        
        return {}

    async def _create_elem_match_for_single_condition(self, variable: str, condition: Dict) -> Dict:
        """Creates $elemMatch for a single condition."""
        base_path_regex = self.format_resolver.build_full_path_regex(variable)
        
        # Handle variable references vs path references
        if condition.get("variable"):
            # Variable reference - resolve the variable to its value
            var_name = condition["variable"]
            if var_name in self.let_variables:
                # Use the resolved variable value directly
                value = self._resolve_let_variable(var_name, "where")
                mql_operator = OPERATOR_MAP.get(condition["operator"], "$eq")
                
                # For variable references, we use a generic path match and then check the variable value
                # This is a simplified approach - in production you'd want more sophisticated handling
                path_field = self.schema_config['path_field']
                
                if mql_operator == "$eq":
                    data_condition = value
                else:
                    data_condition = {mql_operator: value}
                
                return {
                    path_field: {"$regex": base_path_regex},
                    # Use a placeholder data path - this would need more sophisticated handling in production
                    f"{self.schema_config['data_field']}.placeholder": data_condition
                }
            else:
                raise ValueError(f"Unknown LET variable: {var_name}")
        
        else:
            # Regular path-based condition
            aql_path = condition["path"]
            if not aql_path:
                raise ValueError("Condition must have either path or variable reference")
                
            p_regex_part, data_path = await self.format_resolver.translate_aql_path(aql_path)
            
            mql_operator = OPERATOR_MAP.get(condition["operator"])
            if not mql_operator:
                raise NotImplementedError(f"AQL operator '{condition['operator']}' not supported.")
            
            value = self.value_formatter.format_value(condition["value"])
            path_field = self.schema_config['path_field']
            
            # For shortened format, we need to handle path patterns differently
            if self.format == 'shortened':
                # For shortened format, use the p_regex_part directly if available
                if p_regex_part:
                    path_condition = {"$regex": p_regex_part}
                else:
                    # If no specific p-pattern, match any element (fallback)
                    path_condition = {"$exists": True}
            else:
                # For full format, combine the regex patterns
                full_path_regex = self.format_resolver.combine_path_regex(base_path_regex, p_regex_part)
                if full_path_regex:
                    path_condition = {"$regex": full_path_regex}
                else:
                    # Fallback if no regex pattern available
                    path_condition = {"$exists": True}
            
            # Build data condition
            if mql_operator == "$eq":
                data_condition = value
            else:
                data_condition = {mql_operator: value}
            
            return {
                path_field: path_condition,
                data_path: data_condition
            }

    def _resolve_let_variable(self, var_name: str, context: str = "where") -> Any:
        """
        Resolves a LET variable to its MongoDB representation.
        Note: This is a simplified implementation - you'd want to integrate with 
        a proper LET variable resolver in production.
        """
        if var_name not in self.let_variables:
            raise ValueError(f"Unknown LET variable: {var_name}")
        
        # For now, return the variable definition as-is
        # In production, you'd resolve expressions, paths, etc.
        return self.let_variables[var_name]
