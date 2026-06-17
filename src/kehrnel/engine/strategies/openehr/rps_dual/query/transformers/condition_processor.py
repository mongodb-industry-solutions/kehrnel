# src/kehrnel/api/compatibility/v1/aql/transformers/condition_processor.py
import re
from typing import Dict, Any, List, Tuple, Set
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
                 format_resolver, let_variables: Dict[str, Any] = None, version_alias: str | None = None):
        self.ehr_alias = ehr_alias
        self.composition_alias = composition_alias
        self.version_alias = version_alias
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
        Separates top-level document conditions from composition-node conditions.
        Returns (top_level_conditions, comp_conditions_structure)
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

    def requires_mixed_level_match(self, processed_where: Dict) -> bool:
        """
        Returns True when the logical tree includes an OR that spans both
        document-level predicates and composition-node predicates.
        """
        if not processed_where:
            return False

        if processed_where.get("type") == "condition":
            return False

        if processed_where.get("operator") == "OR" and len(self._collect_node_levels(processed_where)) > 1:
            return True

        return any(self.requires_mixed_level_match(child) for child in processed_where.get("children", []))

    async def build_mixed_level_match(self, node: Dict, comp_array_field: str) -> Dict[str, Any]:
        """
        Builds a full MongoDB match tree for logical expressions that mix EHR-level
        and composition-level predicates under the same OR branch.
        """
        if not node:
            return {}

        levels = self._collect_node_levels(node)
        if not levels:
            return {}

        if levels == {"ehr"}:
            return self._build_ehr_condition_tree(node)

        if levels == {"comp"}:
            return {comp_array_field: await self.build_composition_match(node)}

        operator = node.get("operator")
        if operator not in {"AND", "OR"}:
            return {}

        child_conditions: List[Dict[str, Any]] = []
        for child in node.get("children", []):
            child_condition = await self.build_mixed_level_match(child, comp_array_field)
            if child_condition:
                child_conditions.append(child_condition)

        if not child_conditions:
            return {}

        if operator == "OR":
            return {"$or": child_conditions}

        merged: Dict[str, Any] = {}
        for child_condition in child_conditions:
            self._merge_mongo_match(merged, child_condition)
        return merged

    def _collect_node_levels(self, node: Dict) -> Set[str]:
        if not node:
            return set()

        if node.get("type") == "condition":
            if node.get("variable"):
                return {"comp"}

            path = node.get("path")
            if not path:
                return set()

            top_level = self._build_top_level_condition(path, node["operator"], node["value"])
            return {"ehr"} if top_level else {"comp"}

        levels: Set[str] = set()
        for child in node.get("children", []):
            levels.update(self._collect_node_levels(child))
        return levels

    def _can_flat_merge_field(self, existing: Any, incoming: Any) -> bool:
        if not isinstance(existing, dict) or not isinstance(incoming, dict):
            return False
        if "$regex" in existing or "$regex" in incoming:
            return False
        if "$not" in existing or "$not" in incoming:
            return False
        return not (set(existing) & set(incoming))

    def _merge_mongo_match(self, target: Dict[str, Any], condition: Dict[str, Any]) -> None:
        if not condition:
            return

        if not target:
            target.update(condition)
            return

        if "$and" in target or "$or" in target or "$and" in condition or "$or" in condition:
            if "$and" in target and len(target) == 1:
                target["$and"].append(condition)
                return
            existing = dict(target)
            target.clear()
            target["$and"] = [existing, condition]
            return

        for field_name, value in condition.items():
            if field_name not in target:
                target[field_name] = value
            elif self._can_flat_merge_field(target[field_name], value):
                target[field_name] = {**target[field_name], **value}
            else:
                existing = dict(target)
                target.clear()
                target["$and"] = [existing, condition]
                return

    def _build_ehr_condition_tree(self, node: Dict) -> Dict[str, Any]:
        if not node:
            return {}

        if node.get("type") == "condition":
            path = node.get("path")
            if not path:
                return {}
            return self._build_top_level_condition(path, node["operator"], node["value"])

        operator = node.get("operator")
        if operator not in ["AND", "OR"]:
            return {}

        child_conditions = []
        for child in node.get("children", []):
            child_condition = self._build_ehr_condition_tree(child)
            if child_condition:
                child_conditions.append(child_condition)

        if not child_conditions:
            return {}

        if operator == "OR":
            return {"$or": child_conditions}

        merged: Dict[str, Any] = {}
        for child_condition in child_conditions:
            self._merge_mongo_match(merged, child_condition)
        return merged

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
                top_level = self._build_top_level_condition(path, node["operator"], node["value"])
                if top_level:
                    for field_name, condition in top_level.items():
                        if field_name not in ehr_conditions:
                            ehr_conditions[field_name] = condition
                        elif isinstance(ehr_conditions[field_name], dict) and isinstance(condition, dict):
                            ehr_conditions[field_name].update(condition)
                        else:
                            ehr_conditions[field_name] = condition
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
                ehr_tree = self._build_ehr_condition_tree({
                    "operator": node["operator"],
                    "children": ehr_children,
                })
                self._merge_mongo_match(ehr_conditions, ehr_tree)
            
            if comp_children:
                comp_conditions.append({
                    "operator": node["operator"],
                    "children": comp_children
                })

    def _build_top_level_condition(self, path: str, operator: str, value: Any) -> Dict[str, Any]:
        field_name = None
        id_encoding = None
        if path in {"ehr_id", f"{self.ehr_alias}/ehr_id/value"}:
            field_name = self.schema_config.get("ehr_id", "ehr_id")
            id_encoding = self.schema_config.get("ehr_id_encoding", "string")
        elif path == f"{self.composition_alias}/uid/value":
            field_name = self.schema_config.get("comp_id", "comp_id")
            id_encoding = self.schema_config.get("composition_id_encoding", "string")
        elif path == f"{self.composition_alias}/archetype_details/template_id/value":
            field_name = self.schema_config.get("template_id", "tid")
        elif self.version_alias and path == f"{self.version_alias}/commit_audit/time_committed/value":
            field_name = (
                self.schema_config.get("time_committed")
                or self.schema_config.get("sort_time")
                or "time_c"
            )

        if not field_name:
            return {}

        formatted_value = self.value_formatter.format_value(value)
        if id_encoding is not None:
            formatted_value = self.value_formatter.format_id_value(formatted_value, id_encoding)
        return {field_name: self._build_data_condition(operator, formatted_value, preformatted=True)}

    def _build_aql_like_pattern(self, value: Any) -> str:
        parts = ["^"]
        for char in str(value):
            if char == "*":
                parts.append(".*")
            elif char == "?":
                parts.append(".")
            else:
                parts.append(re.escape(char))
        parts.append("$")
        return "".join(parts)

    def _build_matches_pattern(self, value: Any) -> str:
        if isinstance(value, dict):
            values = [value.get(str(idx)) for idx in range(len(value))]
            escaped = [re.escape(str(item)) for item in values if item is not None]
            if escaped:
                return rf"^(?:{'|'.join(escaped)})$"
        if isinstance(value, list):
            escaped = [re.escape(str(item)) for item in value if item is not None]
            if escaped:
                return rf"^(?:{'|'.join(escaped)})$"
        return str(value)

    def _build_data_condition(self, operator: str, value: Any, *, preformatted: bool = False) -> Any:
        normalized_operator = str(operator or "").upper()
        formatted_value = value if preformatted else self.value_formatter.format_value(value)

        if normalized_operator == "=":
            return formatted_value
        if normalized_operator in {"!=", "<>"}:
            return {"$ne": formatted_value}
        if normalized_operator in OPERATOR_MAP:
            return {OPERATOR_MAP[normalized_operator]: formatted_value}
        if normalized_operator == "EXISTS":
            return {"$exists": True}
        if normalized_operator == "LIKE":
            return {"$regex": self._build_aql_like_pattern(formatted_value)}
        if normalized_operator == "MATCHES":
            return {"$regex": self._build_matches_pattern(formatted_value)}
        raise NotImplementedError(f"AQL operator '{operator}' not supported.")

    def _can_merge_field_conditions(self, existing: Any, incoming: Any) -> bool:
        if not isinstance(existing, dict) or not isinstance(incoming, dict):
            return False
        if "$regex" in existing or "$regex" in incoming:
            return False
        return not (set(existing) & set(incoming))

    async def build_composition_match(self, comp_structure: Dict) -> Dict:
        """
        Builds MongoDB match conditions for composition-level queries with OR/AND support.
        """
        if comp_structure.get("type") == "condition":
            # Single condition
            variable = comp_structure["path"].split('/')[0]
            elem_match = await self._create_elem_match_for_single_condition(variable, comp_structure)
            return self._wrap_elem_match(elem_match)
        
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
                    elem_matches.append(self._wrap_elem_match(elem_match))
            
            return {"$all": elem_matches} if len(elem_matches) > 1 else elem_matches[0] if elem_matches else {}
        
        elif comp_structure.get("operator") == "OR":
            # OR conditions - use $elemMatch with $or
            or_conditions = []
            
            for child in comp_structure.get("children", []):
                child_match = await self.build_composition_match(child)
                if "$elemMatch" in child_match:
                    or_conditions.append(child_match["$elemMatch"])
                elif child_match:
                    or_conditions.append(child_match)
            
            return {"$elemMatch": {"$or": or_conditions}} if or_conditions else {}
        
        return {}

    def _wrap_elem_match(self, elem_match: Dict) -> Dict:
        if "$notElemMatch" in elem_match:
            return {"$not": {"$elemMatch": elem_match["$notElemMatch"]}}
        return {"$elemMatch": elem_match}

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
                    
                    value = self._build_data_condition(condition["operator"], condition["value"])
                    
                    # For multiple conditions on same field, combine them
                    if data_path in data_conditions:
                        existing = data_conditions[data_path]
                        if self._can_merge_field_conditions(existing, value):
                            data_conditions[data_path] = {**existing, **value}
                        else:
                            raise NotImplementedError(
                                f"Multiple conditions on the same AQL path are not supported for operator combination "
                                f"'{condition['operator']}'."
                            )
                    else:
                        data_conditions[data_path] = value
            
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
                data_condition = self._build_data_condition(condition["operator"], value)
                
                # For variable references, we use a generic path match and then check the variable value
                # This is a simplified approach - in production you'd want more sophisticated handling
                path_field = self.schema_config['path_field']

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
            
            operator = str(condition.get("operator") or "").upper()
            data_condition = (
                None
                if operator == "NOT EXISTS"
                else self._build_data_condition(condition["operator"], condition["value"])
            )
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
            
            if operator == "NOT EXISTS":
                return {"$notElemMatch": {path_field: path_condition}}

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
