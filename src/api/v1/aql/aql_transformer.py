# src/api/v1/aql/aql_transformer.py
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from bson import ObjectId, Binary
import uuid

# A mapping from AQL operators to MQL operators
OPERATOR_MAP = {
    "=": "$eq",
    "!=": "$ne",
    ">": "$gt",
    "<": "$lt",
    ">=": "$gte",
    "<=": "$le",
}

class AQLtoMQLTransformer:
    """
    Transforms an AQL Abstract Syntax Tree (AST) into a MongoDB Aggregation Pipeline.
    
    This transformer is designed for a specific "semi-flattened" openEHR schema where
    a composition document contains a 'cn' array, and each element in 'cn' has:
    - 'p': A string representing the hierarchical path of the node.
    - 'data': The actual openEHR Reference Model object for that node.
    """
    def __init__(self, ast: Dict[str, Any], ehr_id: Optional[str] = None, schema_config: Optional[Dict[str, str]] = None):
        self.ast = ast
        self.ehr_id = ehr_id  # Optional EHR ID to add to match stage
        self.context_map: Dict[str, Dict] = {}
        
        # Schema field configuration (Point 3 preparation)
        self.schema_config = schema_config or {
            'composition_array': 'cn',  # Array containing composition nodes
            'path_field': 'p',          # Field containing hierarchical path
            'data_field': 'data'        # Field containing RM object data
        }
        
        # Dynamic alias detection
        self.ehr_alias = None
        self.composition_alias = None
        
        self._validate_ast()
        self._build_context_map()
        self._detect_key_aliases()

    def build_pipeline(self) -> List[Dict[str, Any]]:
        """
        Constructs the full MongoDB aggregation pipeline from the AST.
        """
        pipeline = []

        # 1. Build the $match stage from WHERE and CONTAINS clauses
        match_stage = self._build_match_stage()
        if match_stage:
            pipeline.append(match_stage)

        # 2. Build the $project stage from the SELECT clause
        project_stage = self._build_project_stage()
        if project_stage:
            pipeline.append(project_stage)

        # 3. Build the $sort stage from ORDER BY clause
        sort_stage = self._build_sort_stage()
        if sort_stage:
            pipeline.append(sort_stage)
        
        return pipeline

    # --- Validation ---
    def _validate_ast(self):
        """
        Validates that the AST contains required structure elements.
        Raises ValueError for invalid or unsupported AST patterns.
        """
        if not isinstance(self.ast, dict):
            raise ValueError("AST must be a dictionary")
        
        # Check for required top-level elements
        if "from" not in self.ast:
            raise ValueError("AST must contain a 'from' clause")
        
        if "select" not in self.ast:
            raise ValueError("AST must contain a 'select' clause")
        
        # Validate FROM clause
        from_clause = self.ast.get("from", {})
        if not from_clause.get("alias"):
            raise ValueError("FROM clause must have an alias")
        
        if from_clause.get("rmType") != "EHR":
            raise ValueError("FROM clause must reference an EHR")
        
        # Validate SELECT clause
        select_clause = self.ast.get("select", {})
        if not select_clause.get("columns"):
            raise ValueError("SELECT clause must contain columns")

    def _detect_key_aliases(self):
        """
        Dynamically detects EHR and COMPOSITION aliases from the AST.
        """
        # Detect EHR alias from FROM clause
        from_clause = self.ast.get("from", {})
        if from_clause.get("rmType") == "EHR":
            self.ehr_alias = from_clause.get("alias")
        
        # Detect COMPOSITION alias from CONTAINS clause
        contains_node = self.ast.get("contains")
        if contains_node and contains_node.get("rmType") == "COMPOSITION":
            self.composition_alias = contains_node.get("alias")
        
        # Validate that we found the required aliases
        if not self.ehr_alias:
            raise ValueError("Could not detect EHR alias from AST")
        
        if not self.composition_alias:
            raise ValueError("Could not detect COMPOSITION alias from AST")

    # --- Context Building ---
    def _build_context_map(self):
        """
        Walks the FROM and CONTAINS clauses of the AST to map variable aliases
        to their archetype IDs and parent relationships. This context is essential
        for reconstructing hierarchical paths for querying.
        """
        from_alias = self.ast.get("from", {}).get("alias")
        if from_alias:
            self.context_map[from_alias] = {"archetype_id": None, "parent": None}
        
        contains_node = self.ast.get("contains")
        if contains_node:
            self._process_contains_node(contains_node, parent_alias=from_alias)

    def _process_contains_node(self, node: Dict, parent_alias: Optional[str]):
        """Recursively processes a CONTAINS node or its children."""
        if not node:
            return

        if node.get("operator") in ["AND", "OR"]:
            for child_node in node.get("children", {}).values():
                self._process_contains_node(child_node, parent_alias)
            return

        alias = node.get("alias")
        # Some nodes like SECTION might not have an alias
        current_alias = alias if alias else f"_{parent_alias}_child" 

        archetype_id = node.get("predicate", {}).get("value")
        self.context_map[current_alias] = {"archetype_id": archetype_id, "parent": parent_alias}
        
        if "contains" in node:
            self._process_contains_node(node["contains"], parent_alias=current_alias)

    # --- Match Stage Builder ---
    def _build_match_stage(self) -> Optional[Dict[str, Any]]:
        """Constructs the $match stage of the aggregation pipeline."""
        where_clause = self.ast.get("where")
        
        # Initialize match conditions even if there's no WHERE clause
        # (we might still need to add EHR ID or other conditions)
        match_conditions = {}

        # Process WHERE clause if it exists
        ehr_conditions = {}
        comp_conditions_structure = None
        
        if where_clause:
            # Process WHERE clause with proper OR/AND support
            processed_where = self._process_where_clause(where_clause)
            
            # Separate EHR-level conditions from composition-level conditions
            ehr_conditions, comp_conditions_structure = self._separate_conditions(processed_where)

        # Add EHR-level conditions
        if ehr_conditions:
            match_conditions.update(ehr_conditions)
        
        # Add external EHR ID if provided and not already in conditions
        if self.ehr_id and 'ehr_id' not in match_conditions:
            # Convert string UUID to proper BSON Binary for MongoDB
            try:
                uuid_obj = uuid.UUID(self.ehr_id)
                match_conditions['ehr_id'] = Binary.from_uuid(uuid_obj)
            except (ValueError, TypeError):
                # If conversion fails, use as string (fallback)
                match_conditions['ehr_id'] = self.ehr_id
        
        # Add composition-level conditions with proper OR/AND support
        if comp_conditions_structure:
            comp_array_field = self.schema_config['composition_array']
            match_conditions[comp_array_field] = self._build_composition_match(comp_conditions_structure)
        
        return {"$match": match_conditions} if match_conditions else None

    def _process_where_clause(self, where_node: Dict) -> Dict:
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
                processed_child = self._process_where_clause(child_cond)
                if processed_child:  # Only add non-empty conditions
                    children.append(processed_child)
            
            return {
                "operator": operator,
                "children": children
            } if children else {}
        else:
            # Base condition (leaf node)
            return {
                "type": "condition",
                "path": where_node.get("path"),
                "operator": where_node.get("operator"),
                "value": where_node.get("value")
            }

    def _separate_conditions(self, processed_where: Dict) -> Tuple[Dict, Dict]:
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
            variable = node["path"].split('/')[0]
            
            if variable == self.ehr_alias:
                # EHR-level condition
                path_parts = node["path"].split('/')[1:]  # Remove EHR alias
                if len(path_parts) >= 2 and path_parts[0] == 'ehr_id':
                    ehr_field = 'ehr_id'
                    mql_operator = OPERATOR_MAP.get(node["operator"], "$eq")
                    value = self._format_value(node["value"])
                    
                    # Convert to BSON Binary UUID if it's an ehr_id field
                    if ehr_field == 'ehr_id' and isinstance(value, str):
                        try:
                            uuid_obj = uuid.UUID(value)
                            value = Binary.from_uuid(uuid_obj)
                        except (ValueError, TypeError):
                            pass  # Keep as string if conversion fails
                    
                    if mql_operator == "$eq":
                        ehr_conditions[ehr_field] = value
                    else:
                        ehr_conditions[ehr_field] = {mql_operator: value}
            else:
                # Composition-level condition
                comp_conditions.append(node)
        
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

    def _build_composition_match(self, comp_structure: Dict) -> Dict:
        """
        Builds MongoDB match conditions for composition-level queries with OR/AND support.
        """
        if comp_structure.get("type") == "condition":
            # Single condition
            variable = comp_structure["path"].split('/')[0]
            elem_match = self._create_elem_match_for_single_condition(variable, comp_structure)
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
                    elem_match = self._create_elem_match_for_variable_group(variable, condition_structure)
                    elem_matches.append({"$elemMatch": elem_match})
            
            return {"$all": elem_matches} if len(elem_matches) > 1 else elem_matches[0] if elem_matches else {}
        
        elif comp_structure.get("operator") == "OR":
            # OR conditions - use $elemMatch with $or
            or_conditions = []
            
            for child in comp_structure.get("children", []):
                child_match = self._build_composition_match(child)
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
            variable = node["path"].split('/')[0]
            if variable not in variable_conditions:
                variable_conditions[variable] = []
            variable_conditions[variable].append(node)
        
        elif node.get("operator") in ["AND", "OR"]:
            # For nested operators, we need to handle them as groups
            for child in node.get("children", []):
                self._group_conditions_by_variable(child, variable_conditions)

    def _create_elem_match_for_variable_group(self, variable: str, conditions_structure: Dict) -> Dict:
        """
        Creates a single $elemMatch object for conditions on a variable, supporting OR/AND logic.
        This replaces the old _create_elem_match_for_variable method.
        """
        if conditions_structure.get("type") == "condition":
            # Single condition
            return self._create_elem_match_for_single_condition(variable, conditions_structure)
        
        elif conditions_structure.get("operator") == "AND":
            # Multiple conditions on same variable - combine with AND logic
            base_path_regex = self._build_full_path_regex(variable)
            data_conditions = {}
            path_prefix = ""
            
            for condition in conditions_structure.get("children", []):
                if condition.get("type") == "condition":
                    aql_path = condition["path"]
                    p_regex_part, data_path = self._translate_aql_path(aql_path)
                    
                    # Update path prefix if we have specific node identifiers
                    if p_regex_part and not path_prefix:
                        path_prefix = p_regex_part + "/"
                    
                    mql_operator = OPERATOR_MAP.get(condition["operator"])
                    if not mql_operator:
                        raise NotImplementedError(f"AQL operator '{condition['operator']}' not supported.")
                    
                    value = self._format_value(condition["value"])
                    
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
            
            # Build final regex pattern
            path_field = self.schema_config['path_field']
            full_path_regex = self._combine_path_regex(base_path_regex, path_prefix)
            
            return {
                path_field: {"$regex": full_path_regex},
                **data_conditions
            }
        
        elif conditions_structure.get("operator") == "OR":
            # OR conditions on same variable
            or_conditions = []
            for condition in conditions_structure.get("children", []):
                condition_match = self._create_elem_match_for_variable_group(variable, condition)
                or_conditions.append(condition_match)
            
            return {"$or": or_conditions} if or_conditions else {}
        
        return {}

    def _create_elem_match_for_single_condition(self, variable: str, condition: Dict) -> Dict:
        """Creates $elemMatch for a single condition."""
        base_path_regex = self._build_full_path_regex(variable)
        aql_path = condition["path"]
        p_regex_part, data_path = self._translate_aql_path(aql_path)
        
        mql_operator = OPERATOR_MAP.get(condition["operator"])
        if not mql_operator:
            raise NotImplementedError(f"AQL operator '{condition['operator']}' not supported.")
        
        value = self._format_value(condition["value"])
        path_field = self.schema_config['path_field']
        full_path_regex = self._combine_path_regex(base_path_regex, p_regex_part)
        
        # Build data condition
        if mql_operator == "$eq":
            data_condition = value
        else:
            data_condition = {mql_operator: value}
        
        return {
            path_field: {"$regex": full_path_regex},
            data_path: data_condition
        }

    def _combine_path_regex(self, base_regex: str, path_prefix: str) -> str:
        """Combines base path regex with specific path prefix."""
        if path_prefix and base_regex.startswith("^"):
            # Remove trailing slash from path_prefix to avoid double slashes
            clean_prefix = path_prefix.rstrip('/')
            # Only replace the first ^ to avoid corrupting character classes like [^/]
            return f"^{clean_prefix}/" + base_regex[1:]
        else:
            return base_regex

    # Backward compatibility method - keeping for legacy calls
    def _create_elem_match_for_variable(self, variable: str, conditions: List[Dict]) -> Dict:
        """
        Legacy method for backward compatibility.
        Converts old-style condition list to new structure.
        """
        if len(conditions) == 1:
            # Single condition
            condition_structure = {
                "type": "condition",
                "path": conditions[0]["path"],
                "operator": conditions[0]["operator"],
                "value": conditions[0]["value"]
            }
        else:
            # Multiple conditions - assume AND
            children = []
            for cond in conditions:
                children.append({
                    "type": "condition",
                    "path": cond["path"],
                    "operator": cond["operator"],
                    "value": cond["value"]
                })
            condition_structure = {
                "operator": "AND",
                "children": children
            }
        
        return self._create_elem_match_for_variable_group(variable, condition_structure)

    # --- Project Stage Builder ---
    def _build_project_stage(self) -> Optional[Dict[str, Any]]:
        """Constructs the $project stage from the SELECT clause."""
        columns = self.ast.get("select", {}).get("columns", {})
        if not columns:
            return None

        projection = {"_id": 0}
        for col_data in columns.values():
            alias = col_data.get("alias")
            aql_path = col_data["value"]["path"]
            if not alias: # If no alias, use a more descriptive default based on full path
                path_parts = aql_path.split('/')
                if len(path_parts) >= 2:
                    # Use variable + field name, e.g., "e_ehr_id" or "c_name"
                    alias = f"{path_parts[0]}_{path_parts[1]}"
                else:
                    alias = path_parts[-1]

            variable = aql_path.split('/')[0]

            if variable == self.ehr_alias: # Use dynamic EHR alias instead of hardcoded 'e'
                if 'ehr_id' in aql_path:
                    # Convert UUID binary to string representation
                    projection[alias] = {"$toString": "$ehr_id"}
                # Don't continue here - let other columns be processed too
            else:
                # Process composition/observation columns
                path_regex_part, data_path = self._translate_aql_path(aql_path)
                base_path_regex = self._build_full_path_regex(variable)
                
                # Use configurable field names
                composition_array_field = self.schema_config['composition_array']
                path_field = self.schema_config['path_field']
                
                # Combine path prefix with base regex properly
                full_path_regex = self._combine_path_regex(base_path_regex, path_regex_part)
                
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

    def _build_sort_stage(self) -> Optional[Dict[str, Any]]:
        """
        Constructs the $sort stage from the ORDER BY clause.
        
        Returns:
            Optional[Dict[str, Any]]: MongoDB $sort stage or None if no ORDER BY clause
            
        Example ORDER BY AST:
        "orderBy": {
            "columns": {
                "0": {"path": "c/name/value", "direction": "ASC"},
                "1": {"path": "c/context/start_time/value", "direction": "DESC"}
            }
        }
        
        Produces MongoDB $sort:
        {"$sort": {"c_name": 1, "c_context_start_time": -1}}
        """
        order_by = self.ast.get("orderBy", {})
        
        # Check if orderBy exists and has columns
        if not order_by or not order_by.get("columns"):
            return None
            
        sort_spec = {}
        columns = order_by.get("columns", {})
        
        for col_data in columns.values():
            aql_path = col_data.get("path")
            direction = col_data.get("direction", "ASC").upper()
            
            if not aql_path:
                continue
                
            # Convert AQL path to MongoDB field name using same logic as projection
            path_parts = aql_path.split('/')
            if len(path_parts) >= 2:
                # Use variable + field name, e.g., "e_ehr_id" or "c_name"
                field_name = f"{path_parts[0]}_{path_parts[1]}"
            else:
                field_name = path_parts[-1]
                
            # Convert direction to MongoDB sort value
            sort_direction = 1 if direction == "ASC" else -1
            sort_spec[field_name] = sort_direction
            
        return {"$sort": sort_spec} if sort_spec else None

    # --- Utility Methods ---
    def _translate_aql_path(self, aql_path: str) -> Tuple[str, str]:
        """
        Splits a full AQL path into a regex for the 'p' field and a dot-notation path for the 'data' field.
        Example: 'admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string'
        -> p_regex: 'at0014/at0007' (path parts only, will be combined with archetype regex later)
        -> data_path: 'data.value.defining_code.code_string'
        """
        parts = aql_path.split('/')[1:] # remove variable alias
        
        p_parts = []
        data_parts = ["data"]

        for part in parts:
            match = re.match(r"(.+)\[(.+)\]", part)
            if match:
                # This is a node identifier like 'items[at0001]'
                node_code = match.group(2)
                p_parts.append(node_code)
            else:
                # This is part of the path within the data object
                data_parts.append(part)
        
        p_regex = "/".join(reversed(p_parts)) if p_parts else ""
        
        # Handle simple paths like `med_ac/time` which implies `time.value`
        if len(data_parts) == 2 and data_parts[1] in ["time"]:
             data_parts.append('value')
        
        return p_regex, ".".join(data_parts)

    def _build_full_path_regex(self, variable: str) -> str:
        """Builds the hierarchical regex for an alias using the context map."""
        if variable not in self.context_map:
            raise ValueError(f"Unknown variable alias '{variable}' in query.")
        
        # Get the target archetype for this variable
        target_archetype = self.context_map[variable].get('archetype_id')
        if not target_archetype:
            return ".*"
            
        # Get the composition archetype using dynamic alias
        comp_archetype = self.context_map.get(self.composition_alias, {}).get('archetype_id')
        if not comp_archetype:
            return f"^{re.escape(target_archetype)}.*"
            
        # Build the regex pattern: ^target_archetype(?:/[^/]+)*/composition_archetype$
        target_escaped = re.escape(target_archetype)
        comp_escaped = re.escape(comp_archetype)
        
        return f"^{target_escaped}(?:/[^/]+)*/{comp_escaped}$"
    
    def _format_value(self, value: Any) -> Any:
        """Converts string value from AST to appropriate Python type for MongoDB."""
        if not isinstance(value, str):
            return value
            
        # Check if it's a date string and format it appropriately for MongoDB
        if self._is_iso_date_string(value):
            # Convert to MongoDB ISODate format but keep as string for the aggregation pipeline
            # The MongoDB driver will handle the actual conversion
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt
            except (ValueError, TypeError):
                return value
        
        # It's not a date, return as is
        return value
    
    def _is_iso_date_string(self, value: str) -> bool:
        """Check if a string looks like an ISO 8601 date."""
        if not isinstance(value, str):
            return False
        # Basic check for ISO format patterns
        iso_patterns = [
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # Basic ISO format
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}',  # With timezone
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',  # With Z timezone
        ]
        return any(re.match(pattern, value) for pattern in iso_patterns)
    
    def _is_uuid_string(self, value: str) -> bool:
        """Check if a string looks like a UUID."""
        if not isinstance(value, str):
            return False
        try:
            uuid.UUID(value)
            return True
        except (ValueError, TypeError):
            return False