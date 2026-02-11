# src/kehrnel/legacy/aql_parser/aql_to_ast.py

"""
Python AQL to AST Parser
A comprehensive AQL parser that converts openEHR AQL queries into AST structures.
Ported from JavaScript implementation to maintain compatibility with existing pipelines.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ParseError(Exception):
    """Exception raised when AQL parsing fails"""
    message: str
    position: Optional[int] = None
    line: Optional[int] = None
    column: Optional[int] = None


class AQLToASTParser:
    """
    Main AQL to AST parser class.
    Converts AQL query strings into structured AST dictionaries.
    """
    
    def __init__(self, aql_query: str):
        self.aql_query = aql_query.strip()
        self.original_query = aql_query
        
    def parse(self) -> Dict[str, Any]:
        """
        Main parsing method that orchestrates the AQL to AST conversion.
        Returns a structured AST dictionary compatible with the existing pipeline.
        """
        try:
            # Clean and normalize the input
            cleaned_aql = self._clean_query(self.aql_query)
            
            # Extract main clauses
            clauses = self._extract_main_clauses(cleaned_aql)
            
            # Parse each clause
            result = {
                "select": self._parse_select_clause(clauses.get("select", "")),
                "from": {},
                "contains": None,
                "where": {},
                "orderBy": {},
                "limit": None,
                "offset": None
            }
            
            # Parse FROM and CONTAINS
            from_result = self._parse_from_and_contains(clauses.get("from", ""))
            result["from"] = from_result["from"]
            result["contains"] = from_result["contains"]
            
            # Parse WHERE
            if clauses.get("where"):
                result["where"] = self._parse_where_clause(clauses["where"])
            
            # Parse ORDER BY
            if clauses.get("orderBy"):
                result["orderBy"] = self._parse_order_by_clause(clauses["orderBy"])
            
            # Parse LIMIT/OFFSET
            if clauses.get("limit"):
                limit_offset = self._parse_limit_offset(clauses["limit"])
                result["limit"] = limit_offset["limit"]
                result["offset"] = limit_offset["offset"]
            
            logger.info(f"Successfully parsed AQL query into AST")
            return result
            
        except Exception as e:
            logger.error(f"Failed to parse AQL query: {str(e)}")
            raise ParseError(f"AQL parsing failed: {str(e)}")
    
    def _clean_query(self, aql: str) -> str:
        """Clean and normalize the AQL query string"""
        # Replace multiple whitespace with single space
        cleaned = re.sub(r'\s+', ' ', aql)
        return cleaned.strip()
    
    def _extract_main_clauses(self, aql: str) -> Dict[str, str]:
        """Extract main AQL clauses (SELECT, FROM, WHERE, etc.)"""
        clauses = {
            "select": "",
            "from": "",
            "where": "",
            "orderBy": "",
            "limit": ""
        }
        
        # Match SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)(?=\s+FROM\s+|$)', aql, re.IGNORECASE | re.DOTALL)
        if select_match:
            clauses["select"] = select_match.group(1).strip()
        
        # Match FROM clause
        from_match = re.search(r'FROM\s+(.*?)(?=\s+WHERE\s+|\s+ORDER\s+BY|\s+LIMIT\s+|$)', aql, re.IGNORECASE | re.DOTALL)
        if from_match:
            clauses["from"] = from_match.group(1).strip()
        
        # Match WHERE clause
        where_match = re.search(r'WHERE\s+(.*?)(?=\s+ORDER\s+BY|\s+LIMIT\s+|$)', aql, re.IGNORECASE | re.DOTALL)
        if where_match:
            clauses["where"] = where_match.group(1).strip()
        
        # Match ORDER BY clause
        order_by_match = re.search(r'ORDER\s+BY\s+(.*?)(?=\s+LIMIT\s+|$)', aql, re.IGNORECASE | re.DOTALL)
        if order_by_match:
            clauses["orderBy"] = order_by_match.group(1).strip()
        
        # Match LIMIT clause
        limit_match = re.search(r'LIMIT\s+(.*?)$', aql, re.IGNORECASE | re.DOTALL)
        if limit_match:
            clauses["limit"] = limit_match.group(1).strip()
        
        return clauses
    
    def _parse_select_clause(self, select_text: str) -> Dict[str, Any]:
        """Parse the SELECT clause"""
        if not select_text:
            return {"distinct": False, "columns": {}}
        
        # Check for DISTINCT
        distinct = bool(re.search(r'DISTINCT\s+', select_text, re.IGNORECASE))
        if distinct:
            select_text = re.sub(r'DISTINCT\s+', '', select_text, flags=re.IGNORECASE)
        
        # Split columns by commas, respecting nested functions and parentheses
        columns = self._split_by_commas(select_text)
        
        result = {
            "distinct": distinct,
            "columns": {}
        }
        
        # Process each column
        for index, column in enumerate(columns):
            # Look for alias with AS keyword
            as_match = re.search(r'\s+AS\s+(\w+)$', column, re.IGNORECASE)
            
            if as_match:
                alias = as_match.group(1)
                path = column[:as_match.start()].strip()
            else:
                alias = None
                path = column.strip()
            
            # Parse column value
            value = self._parse_column_value(path)
            
            # Build column object - only include alias if it exists (matches JavaScript parser)
            column_obj = {"value": value}
            if alias is not None:
                column_obj["alias"] = alias
            
            result["columns"][str(index)] = column_obj
        
        return result
    
    def _parse_column_value(self, text: str) -> Dict[str, Any]:
        """Parse a column value (path, function call, etc.)"""
        text = text.strip()
        
        # Check if it's an aggregate function
        agg_func_match = re.match(r'^(COUNT|MIN|MAX|AVG|SUM)\s*\((.*)\)$', text, re.IGNORECASE)
        if agg_func_match:
            return {
                "type": "aggregateFunctionCall",
                "path": text,
                "function": {
                    "name": agg_func_match.group(1).upper(),
                    "args": agg_func_match.group(2).strip()
                }
            }
        
        # Check if it's a function call
        func_match = re.match(r'^(\w+)\s*\((.*)\)$', text, re.IGNORECASE)
        if func_match and not agg_func_match:
            return {
                "type": "functionCall",
                "path": text,
                "function": {
                    "name": func_match.group(1),
                    "args": func_match.group(2).strip()
                }
            }
        
        # If it's a literal (starts with number or quote)
        if re.match(r"^(['\"]).+\1$", text) or re.match(r"^-?\d+(\.\d+)?$", text):
            value = text
            if re.match(r"^(['\"]).+\1$", text):
                value = text[1:-1]  # Remove quotes
            return {
                "type": "literal",
                "path": text,
                "value": value
            }
        
        # Default to data path
        return {
            "type": "dataMatchPath",
            "path": text
        }
    
    def _parse_from_and_contains(self, from_text: str) -> Dict[str, Any]:
        """Parse FROM clause and extract CONTAINS structures"""
        if not from_text:
            return {"from": {}, "contains": None}
        
        # Split the FROM clause by CONTAINS, respecting parentheses
        parts = self._split_by_top_level_contains(from_text)
        
        # The first part is the main FROM expression
        main_from = self._parse_rm_type(parts[0])
        
        # If there are no CONTAINS clauses, return just the FROM part
        if len(parts) <= 1:
            return {"from": main_from, "contains": None}
        
        # Build the nested CONTAINS structure from the remaining parts
        contains_structure = self._build_nested_contains_structure(parts[1:])
        
        return {"from": main_from, "contains": contains_structure}
    
    def _split_by_top_level_contains(self, text: str) -> List[str]:
        """Split a FROM clause by top-level CONTAINS"""
        parts = []
        remainder = text.strip()
        
        while remainder:
            contains_index = self._find_top_level_keyword(remainder, 'CONTAINS')
            
            if contains_index == -1:
                # No more CONTAINS
                parts.append(remainder)
                break
            
            # Add the part before CONTAINS
            parts.append(remainder[:contains_index].strip())
            
            # Move to after CONTAINS
            remainder = remainder[contains_index + 8:].strip()
        
        return parts
    
    def _build_nested_contains_structure(self, parts: List[str]) -> Optional[Dict[str, Any]]:
        """Build a nested CONTAINS structure from parts"""
        if not parts:
            return None
        
        # Process the first part
        first_part = parts[0]
        
        # Check if this part has a nested CONTAINS within it
        nested_contains_index = self._find_top_level_keyword(first_part, 'CONTAINS')
        
        if nested_contains_index != -1:
            # This part has a nested CONTAINS
            before_contains = first_part[:nested_contains_index].strip()
            after_contains = first_part[nested_contains_index + 8:].strip()
            
            # Parse the part before CONTAINS
            containing_node = self._parse_contains_expression(before_contains)
            
            # Parse the part after CONTAINS
            contained_node = self._parse_contains_expression(after_contains)
            
            # Create the nested structure
            containing_node["contains"] = contained_node
            
            # If there are more top-level CONTAINS parts, process them
            if len(parts) > 1:
                next_contains = self._build_nested_contains_structure(parts[1:])
                if next_contains:
                    containing_node["contains"] = next_contains
            
            return containing_node
        
        # Check for AND/OR expressions
        and_index = self._find_top_level_keyword(first_part, 'AND')
        or_index = self._find_top_level_keyword(first_part, 'OR')
        
        if and_index != -1 or or_index != -1:
            # This is a compound AND/OR expression
            op_index = min([i for i in [and_index, or_index] if i != -1])
            
            operator = first_part[op_index:op_index + 3].upper().strip()
            left_expr = first_part[:op_index].strip()
            right_expr = first_part[op_index + 3:].strip()
            
            # Parse both sides of the AND/OR
            left_node = self._parse_contains_expression(left_expr)
            right_node = self._parse_contains_expression(right_expr)
            
            # Create the AND/OR structure
            result = {
                "operator": operator,
                "children": {
                    "0": left_node,
                    "1": right_node
                }
            }
            
            # If there are more parts, add them
            if len(parts) > 1:
                next_contains = self._build_nested_contains_structure(parts[1:])
                if next_contains:
                    result["contains"] = next_contains
            
            return result
        
        # Simple expression without nested CONTAINS or AND/OR
        node = self._parse_contains_expression(first_part)
        
        # If there are more parts, process them
        if len(parts) > 1:
            next_contains = self._build_nested_contains_structure(parts[1:])
            if next_contains:
                node["contains"] = next_contains
        
        return node
    
    def _parse_contains_expression(self, expr: str) -> Dict[str, Any]:
        """Parse a CONTAINS expression"""
        expr = expr.strip()
        
        # Remove outer parentheses if balanced
        if expr.startswith('(') and expr.endswith(')') and self._is_balanced(expr):
            return self._parse_contains_expression(expr[1:-1].strip())
        
        # Check for AND/OR expressions
        and_index = self._find_top_level_keyword(expr, 'AND')
        or_index = self._find_top_level_keyword(expr, 'OR')
        
        if and_index != -1 or or_index != -1:
            # This is a compound expression
            op_index = min([i for i in [and_index, or_index] if i != -1])
            
            operator = expr[op_index:op_index + 3].upper().strip()
            left_expr = expr[:op_index].strip()
            right_expr = expr[op_index + 3:].strip()
            
            # Check if right side has a CONTAINS
            right_contains_index = self._find_top_level_keyword(right_expr, 'CONTAINS')
            
            if right_contains_index != -1:
                # Right side has CONTAINS, parse recursively
                right_before = right_expr[:right_contains_index].strip()
                right_after = right_expr[right_contains_index + 8:].strip()
                
                right_containing = self._parse_rm_type(right_before)
                right_contained = self._parse_contains_expression(right_after)
                
                right_containing["contains"] = right_contained
                
                return {
                    "operator": operator,
                    "children": {
                        "0": self._parse_rm_type(left_expr),
                        "1": right_containing
                    }
                }
            
            # Regular AND/OR
            return {
                "operator": operator,
                "children": {
                    "0": self._parse_rm_type(left_expr),
                    "1": self._parse_rm_type(right_expr)
                }
            }
        
        # Check for nested CONTAINS
        contains_index = self._find_top_level_keyword(expr, 'CONTAINS')
        
        if contains_index != -1:
            # This has a nested CONTAINS
            before_contains = expr[:contains_index].strip()
            after_contains = expr[contains_index + 8:].strip()
            
            containing_node = self._parse_rm_type(before_contains)
            contained_node = self._parse_contains_expression(after_contains)
            
            containing_node["contains"] = contained_node
            return containing_node
        
        # Simple RM type expression
        return self._parse_rm_type(expr)
    
    def _parse_rm_type(self, text: str) -> Dict[str, Any]:
        """Parse a Resource Model type expression (e.g., "EHR e[ehr_id/value='123']")"""
        text = text.strip()
        
        # Improved regex pattern to handle alias and bracket content correctly
        # Makes alias optional and handles spacing better
        regex = r'^([A-Z]+)(?:\s+([a-zA-Z0-9_]+))?\s*(?:\[(.*?)\])?'
        match = re.match(regex, text)
        
        if not match:
            # Fallback for unmatched text
            return {"rmType": "", "alias": text, "predicate": None}
        
        rm_type = match.group(1)
        alias = match.group(2) or ""
        bracket_content = match.group(3) if match.group(3) else None
        
        predicate = None
        if bracket_content:
            predicate = self._parse_predicate(bracket_content)
        
        return {"rmType": rm_type, "alias": alias, "predicate": predicate}
    
    def _parse_predicate(self, text: str) -> Dict[str, Any]:
        """Parse a predicate expression (content inside square brackets)"""
        text = text.strip()
        
        # If it's an openEHR archetype ID without operator
        if text.startswith('openEHR-') and '=' not in text:
            return {
                "path": "archetype_node_id",
                "operator": "=",
                "value": text
            }
        
        # Match equality pattern with optional quotes
        equals_match = re.match(r"^([^=]+)\s*=\s*['\"]?([^'\"]*?)['\"]?$", text)
        if equals_match:
            return {
                "path": equals_match.group(1).strip(),
                "operator": "=",
                "value": equals_match.group(2).strip()
            }
        
        # Fallback
        return {"path": text, "operator": None, "value": None}
    
    def _parse_where_clause(self, where_text: str) -> Dict[str, Any]:
        """Parse the WHERE clause"""
        if not where_text:
            return {}
        
        # Parse the WHERE conditions into a structured format
        conditions = self._parse_where_conditions(where_text)
        
        return conditions
    
    def _parse_where_conditions(self, text: str) -> Dict[str, Any]:
        """Parse WHERE conditions recursively"""
        text = text.strip()
        
        # Try to find top-level AND or OR
        and_index = self._find_top_level_keyword(text, 'AND')
        or_index = self._find_top_level_keyword(text, 'OR')
        
        if and_index != -1 or or_index != -1:
            # Compound condition with AND/OR
            op_index = min([i for i in [and_index, or_index] if i != -1])
            
            operator = text[op_index:op_index + 3].upper().strip()
            left_expr = text[:op_index].strip()
            right_expr = text[op_index + 3:].strip()
            
            # Parse both sides recursively
            left_condition = self._parse_where_conditions(left_expr)
            right_condition = self._parse_where_conditions(right_expr)
            
            return {
                "operator": operator,
                "conditions": {
                    "0": left_condition,
                    "1": right_condition
                }
            }
        
        # If it's a parenthesized expression
        if text.startswith('(') and text.endswith(')') and self._is_balanced(text):
            return self._parse_where_conditions(text[1:-1].strip())
        
        # Parse a single condition
        return self._parse_single_condition(text)
    
    def _parse_single_condition(self, text: str) -> Dict[str, Any]:
        """Parse a single WHERE condition"""
        text = text.strip()
        
        # EXISTS
        if re.match(r'^EXISTS\s+', text, re.IGNORECASE):
            path = re.sub(r'^EXISTS\s+', '', text, flags=re.IGNORECASE).strip()
            return {
                "path": path,
                "operator": "EXISTS",
                "value": None
            }
        
        # NOT EXISTS
        if re.match(r'^NOT\s+EXISTS\s+', text, re.IGNORECASE):
            path = re.sub(r'^NOT\s+EXISTS\s+', '', text, flags=re.IGNORECASE).strip()
            return {
                "path": path,
                "operator": "NOT EXISTS",
                "value": None
            }
        
        # MATCHES with array
        matches_match = re.search(r'(.*?)\s+MATCHES\s+\{(.*?)\}', text, re.IGNORECASE)
        if matches_match:
            path = matches_match.group(1).strip()
            
            # Parse the comma-separated values, handling quotes
            values_text = matches_match.group(2)
            value_array = self._split_by_commas(values_text)
            value_array = [v.strip().strip('\'"') for v in value_array]
            
            # Convert to object with numeric keys
            value_obj = {str(i): v for i, v in enumerate(value_array)}
            
            return {
                "path": path,
                "operator": "MATCHES",
                "value": value_obj
            }
        
        # LIKE operator
        like_match = re.search(r'(.*?)\s+LIKE\s+(.*)', text, re.IGNORECASE)
        if like_match:
            path = like_match.group(1).strip()
            value = like_match.group(2).strip().strip('\'"')
            
            return {
                "path": path,
                "operator": "LIKE",
                "value": value
            }
        
        # Standard comparison operators (=, !=, >, <, >=, <=)
        comparison_match = re.search(r'(.*?)\s*([=!<>]=?|!=)\s*(.*)', text)
        if comparison_match:
            path = comparison_match.group(1).strip()
            operator = comparison_match.group(2)
            raw_value = comparison_match.group(3).strip()
            
            # Check if the original value was quoted
            is_quoted = (raw_value.startswith("'") and raw_value.endswith("'")) or \
                       (raw_value.startswith('"') and raw_value.endswith('"'))
            
            # Remove quotes if present
            value = raw_value.strip('\'"')
            
            # Only attempt numeric conversion for unquoted values
            # Quoted values should always be preserved as strings to maintain
            # leading zeros and other string semantics (e.g., '01817273', '007')
            if not is_quoted:
                try:
                    if '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                except ValueError:
                    pass  # Keep as string
            
            return {
                "path": path,
                "operator": operator,
                "value": value
            }
        
        # If no pattern matched, return the raw condition
        return {"raw": text}
    
    def _parse_order_by_clause(self, order_by_text: str) -> Dict[str, Any]:
        """Parse the ORDER BY clause"""
        if not order_by_text:
            return {}
        
        # Split by commas, respecting functions and parentheses
        parts = self._split_by_commas(order_by_text)
        result = {}
        
        for index, part in enumerate(parts):
            part = part.strip()
            
            # Check for direction (ASC/DESC)
            direction = "ASC"
            direction_match = re.search(r'\s+(ASC|ASCENDING|DESC|DESCENDING)$', part, re.IGNORECASE)
            
            if direction_match:
                direction_text = direction_match.group(1).upper()
                direction = "ASC" if direction_text.startswith("ASC") else "DESC"
                path = part[:direction_match.start()].strip()
            else:
                path = part
            
            result[str(index)] = {
                "path": path,
                "direction": direction
            }
        
        return result
    
    def _parse_limit_offset(self, limit_text: str) -> Dict[str, Optional[int]]:
        """Parse LIMIT and OFFSET clauses"""
        if not limit_text:
            return {"limit": None, "offset": None}
        
        # Check for OFFSET keyword
        parts = re.split(r'\s+OFFSET\s+', limit_text, flags=re.IGNORECASE)
        
        try:
            limit = int(parts[0]) if parts[0] else None
        except ValueError:
            limit = None
        
        try:
            offset = int(parts[1]) if len(parts) > 1 else None
        except ValueError:
            offset = None
        
        return {"limit": limit, "offset": offset}
    
    # Helper methods
    
    def _find_top_level_keyword(self, text: str, keyword: str) -> int:
        """Find a top-level keyword in text, respecting parentheses depth"""
        depth = 0
        upper_text = text.upper()
        keyword_length = len(keyword)
        
        for i in range(len(text) - keyword_length + 1):
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
            
            if depth == 0 and upper_text[i:i + keyword_length] == keyword:
                # Check word boundaries
                if (i == 0 or not text[i-1].isalnum()) and \
                   (i + keyword_length >= len(text) or not text[i + keyword_length].isalnum()):
                    return i
        
        return -1
    
    def _split_by_commas(self, text: str) -> List[str]:
        """Split a string by commas, respecting nested parentheses and brackets"""
        result = []
        current = ""
        depth = 0
        
        for char in text:
            if char in '([{':
                depth += 1
            elif char in ')]}':
                depth -= 1
            
            if char == ',' and depth == 0:
                if current.strip():
                    result.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            result.append(current.strip())
        
        return result
    
    def _is_balanced(self, text: str) -> bool:
        """Check if a string has balanced parentheses"""
        depth = 0
        
        for char in text:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth < 0:
                    return False
        
        return depth == 0


def parse_aql_to_ast(aql_query: str) -> Dict[str, Any]:
    """
    Main function to parse AQL query string into AST structure.
    
    Args:
        aql_query: The AQL query string to parse
        
    Returns:
        Dict containing the AST structure
        
    Raises:
        ParseError: If parsing fails
    """
    parser = AQLToASTParser(aql_query)
    return parser.parse()


# Validation functions

def validate_ast_structure(ast: Dict[str, Any]) -> bool:
    """
    Validate that the AST has the expected structure for the pipeline.
    
    Args:
        ast: The AST dictionary to validate
        
    Returns:
        True if valid, False otherwise
    """
    required_keys = ["select", "from", "contains", "where", "orderBy", "limit", "offset"]
    
    for key in required_keys:
        if key not in ast:
            logger.error(f"Missing required AST key: {key}")
            return False
    
    # Validate select structure
    if not isinstance(ast["select"], dict) or "columns" not in ast["select"]:
        logger.error("Invalid select structure in AST")
        return False
    
    # Validate from structure
    if not isinstance(ast["from"], dict):
        logger.error("Invalid from structure in AST")
        return False
    
    return True


def get_sample_ast() -> Dict[str, Any]:
    """
    Returns a sample AST structure for testing purposes.
    """
    return {
        "select": {
            "distinct": False,
            "columns": {
                "0": {
                    "value": {
                        "type": "dataMatchPath",
                        "path": "c/context/start_time/value"
                    },
                    "alias": "start_time"
                }
            }
        },
        "from": {
            "rmType": "EHR",
            "alias": "e",
            "predicate": None
        },
        "contains": {
            "rmType": "COMPOSITION",
            "alias": "c",
            "predicate": {
                "path": "archetype_node_id",
                "operator": "=",
                "value": "openEHR-EHR-COMPOSITION.test.v0"
            }
        },
        "where": {
            "path": "e/ehr_id/value",
            "operator": "=",
            "value": "test-ehr-id"
        },
        "orderBy": {
            "0": {
                "path": "c/context/start_time/value",
                "direction": "DESC"
            }
        },
        "limit": 10,
        "offset": None
    }