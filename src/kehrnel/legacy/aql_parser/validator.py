# src/kehrnel/legacy/aql_parser/validator.py

"""
AQL Validation Module
Provides validation functions for AQL queries and AST structures.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from .aql_to_ast import parse_aql_to_ast, ParseError, validate_ast_structure

logger = logging.getLogger(__name__)


class AQLValidator:
    """
    AQL validation class that provides comprehensive validation of AQL queries.
    """
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def validate_aql_query(self, aql_query: str) -> Dict[str, Any]:
        """
        Comprehensive validation of an AQL query string.
        
        Args:
            aql_query: The AQL query string to validate
            
        Returns:
            Dict containing validation results with success, message, errors, and warnings
        """
        self.errors = []
        self.warnings = []
        
        try:
            # Basic syntax checks
            if not self._basic_syntax_check(aql_query):
                return self._build_validation_result(False, "Basic syntax validation failed")
            
            # Try to parse the query
            try:
                ast = parse_aql_to_ast(aql_query)
                
                # Validate the generated AST
                if not validate_ast_structure(ast):
                    self.errors.append("Generated AST structure is invalid")
                    return self._build_validation_result(False, "AST validation failed")
                
                # Additional semantic validation
                self._semantic_validation(ast)
                
                # Determine overall success
                success = len(self.errors) == 0
                message = "Valid AQL query ✅" if success else "AQL validation failed"
                
                return self._build_validation_result(success, message)
                
            except ParseError as e:
                self.errors.append(f"Parsing error: {e.message}")
                return self._build_validation_result(False, "AQL parsing failed")
            
        except Exception as e:
            self.errors.append(f"Unexpected validation error: {str(e)}")
            return self._build_validation_result(False, "Validation process failed")
    
    def _basic_syntax_check(self, aql_query: str) -> bool:
        """Perform basic syntax checks on the AQL query"""
        
        if not aql_query or not aql_query.strip():
            self.errors.append("Query is empty")
            return False
        
        # Check for required SELECT clause
        if not re.search(r'\bSELECT\b', aql_query, re.IGNORECASE):
            self.errors.append("Missing SELECT clause")
            return False
        
        # Check for required FROM clause
        if not re.search(r'\bFROM\b', aql_query, re.IGNORECASE):
            self.errors.append("Missing FROM clause")
            return False
        
        # Check for balanced parentheses
        if not self._check_balanced_parentheses(aql_query):
            self.errors.append("Unbalanced parentheses")
            return False
        
        # Check for balanced square brackets
        if not self._check_balanced_brackets(aql_query):
            self.errors.append("Unbalanced square brackets")
            return False
        
        # Check for common typos in keywords
        self._check_keyword_typos(aql_query)
        
        return True
    
    def _check_balanced_parentheses(self, text: str) -> bool:
        """Check if parentheses are balanced"""
        depth = 0
        for char in text:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth < 0:
                    return False
        return depth == 0
    
    def _check_balanced_brackets(self, text: str) -> bool:
        """Check if square brackets are balanced"""
        depth = 0
        for char in text:
            if char == '[':
                depth += 1
            elif char == ']':
                depth -= 1
                if depth < 0:
                    return False
        return depth == 0
    
    def _check_keyword_typos(self, aql_query: str):
        """Check for common keyword typos and add warnings"""
        
        # Common typos
        typos = {
            r'\bSELCT\b': 'SELECT',
            r'\bFORM\b': 'FROM',
            r'\bWHERE\b(?=\s+[^A-Z])': 'WHERE (check syntax)',
            r'\bCONTAINS\b(?=\s+[^A-Z])': 'CONTAINS (check syntax)',
            r'\bORDER\s+BT\b': 'ORDER BY',
            r'\bLIMTE\b': 'LIMIT'
        }
        
        for pattern, suggestion in typos.items():
            if re.search(pattern, aql_query, re.IGNORECASE):
                self.warnings.append(f"Possible typo detected, did you mean '{suggestion}'?")
    
    def _semantic_validation(self, ast: Dict[str, Any]):
        """Perform semantic validation on the AST"""
        
        # Check SELECT clause
        self._validate_select_clause(ast.get("select", {}))
        
        # Check FROM clause
        self._validate_from_clause(ast.get("from", {}))
        
        # Check WHERE clause
        self._validate_where_clause(ast.get("where", {}))
        
        # Check ORDER BY clause
        self._validate_order_by_clause(ast.get("orderBy", {}))
        
        # Check LIMIT/OFFSET
        self._validate_limit_offset(ast.get("limit"), ast.get("offset"))
    
    def _validate_select_clause(self, select_clause: Dict[str, Any]):
        """Validate SELECT clause semantics"""
        
        if not select_clause.get("columns"):
            self.errors.append("SELECT clause has no columns")
            return
        
        columns = select_clause["columns"]
        if not isinstance(columns, dict) or len(columns) == 0:
            self.errors.append("SELECT clause must have at least one column")
            return
        
        # Check for duplicate aliases
        aliases = []
        for col_data in columns.values():
            if col_data.get("alias"):
                if col_data["alias"] in aliases:
                    self.errors.append(f"Duplicate column alias: {col_data['alias']}")
                aliases.append(col_data["alias"])
        
        # Warn about SELECT *
        for col_data in columns.values():
            if col_data.get("value", {}).get("path") == "*":
                self.warnings.append("SELECT * may return large result sets")
    
    def _validate_from_clause(self, from_clause: Dict[str, Any]):
        """Validate FROM clause semantics"""
        
        if not from_clause.get("rmType"):
            self.errors.append("FROM clause missing resource type")
            return
        
        # Check for valid RM types
        valid_rm_types = ["EHR", "COMPOSITION", "OBSERVATION", "EVALUATION", 
                         "INSTRUCTION", "ACTION", "ADMIN_ENTRY", "SECTION", 
                         "CLUSTER", "ELEMENT"]
        
        if from_clause["rmType"] not in valid_rm_types:
            self.warnings.append(f"Unknown RM type: {from_clause['rmType']}")
    
    def _validate_where_clause(self, where_clause: Dict[str, Any]):
        """Validate WHERE clause semantics"""
        
        if not where_clause:
            return  # WHERE clause is optional
        
        # Check for common issues in WHERE conditions
        self._check_where_conditions_recursive(where_clause)
    
    def _check_where_conditions_recursive(self, condition: Dict[str, Any]):
        """Recursively check WHERE conditions for common issues"""
        
        if isinstance(condition, dict):
            # Check for operator with null value (except EXISTS)
            if condition.get("operator") and condition.get("value") is None:
                if condition["operator"] not in ["EXISTS", "NOT EXISTS"]:
                    self.warnings.append(f"Condition with {condition['operator']} has null value")
            
            # Check for conditions on conditions (recursive)
            if "conditions" in condition:
                for sub_condition in condition["conditions"].values():
                    self._check_where_conditions_recursive(sub_condition)
    
    def _validate_order_by_clause(self, order_by_clause: Dict[str, Any]):
        """Validate ORDER BY clause semantics"""
        
        if not order_by_clause:
            return  # ORDER BY is optional
        
        # Check for valid directions
        for order_item in order_by_clause.values():
            if order_item.get("direction") not in ["ASC", "DESC"]:
                self.warnings.append(f"Unknown sort direction: {order_item.get('direction')}")
    
    def _validate_limit_offset(self, limit: Optional[int], offset: Optional[int]):
        """Validate LIMIT and OFFSET values"""
        
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                self.errors.append("LIMIT must be a positive integer")
        
        if offset is not None:
            if not isinstance(offset, int) or offset < 0:
                self.errors.append("OFFSET must be a non-negative integer")
            
            if limit is None:
                self.warnings.append("OFFSET without LIMIT may not be supported by all systems")
    
    def _build_validation_result(self, success: bool, message: str) -> Dict[str, Any]:
        """Build the validation result dictionary"""
        return {
            "success": success,
            "message": message,
            "errors": self.errors.copy(),
            "warnings": self.warnings.copy()
        }


# Convenience functions

def validate_aql_syntax(aql_query: str) -> Dict[str, Any]:
    """
    Validate AQL syntax and return validation results.
    
    Args:
        aql_query: The AQL query string to validate
        
    Returns:
        Dict containing validation results with 'success', 'message', 'errors', and 'warnings' keys
    """
    validator = AQLValidator()
    return validator.validate_aql_query(aql_query)


def quick_syntax_check(aql_query: str) -> bool:
    """
    Quick syntax check that returns True/False.
    
    Args:
        aql_query: The AQL query string to check
        
    Returns:
        True if syntax appears valid, False otherwise
    """
    result = validate_aql_syntax(aql_query)
    return result["success"]


def get_validation_errors(aql_query: str) -> List[str]:
    """
    Get a list of validation errors for an AQL query.
    
    Args:
        aql_query: The AQL query string to validate
        
    Returns:
        List of error messages
    """
    result = validate_aql_syntax(aql_query)
    return result["errors"]


def get_validation_warnings(aql_query: str) -> List[str]:
    """
    Get a list of validation warnings for an AQL query.
    
    Args:
        aql_query: The AQL query string to validate
        
    Returns:
        List of warning messages
    """
    result = validate_aql_syntax(aql_query)
    return result["warnings"]