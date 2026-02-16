# src/kehrnel/api/compatibility/v1/aql/transformers/ast_validator.py
from typing import Dict, Any


class ASTValidator:
    """
    Validates that the AQL AST contains required structure elements.
    Raises ValueError for invalid or unsupported AST patterns.
    """

    @staticmethod
    def validate_ast(ast: Dict[str, Any]) -> None:
        """
        Validates that the AST contains required structure elements.
        Raises ValueError for invalid or unsupported AST patterns.
        """
        if not isinstance(ast, dict):
            raise ValueError("AST must be a dictionary")
        
        # Check for required top-level elements
        if "from" not in ast:
            raise ValueError("AST must contain a 'from' clause")
        
        if "select" not in ast:
            raise ValueError("AST must contain a 'select' clause")
        
        # Validate FROM clause
        from_clause = ast.get("from", {})
        if not from_clause.get("alias"):
            raise ValueError("FROM clause must have an alias")
        
        if from_clause.get("rmType") != "EHR":
            raise ValueError("FROM clause must reference an EHR")
        
        # Validate SELECT clause
        select_clause = ast.get("select", {})
        if not select_clause.get("columns"):
            raise ValueError("SELECT clause must contain columns")

    @staticmethod
    def detect_key_aliases(ast: Dict[str, Any]) -> tuple[str, str]:
        """
        Dynamically detects EHR and COMPOSITION aliases from the AST.
        Returns tuple of (ehr_alias, composition_alias)
        """
        ehr_alias = None
        composition_alias = None
        
        # Detect EHR alias from FROM clause
        from_clause = ast.get("from", {})
        if from_clause.get("rmType") == "EHR":
            ehr_alias = from_clause.get("alias")
        
        # Detect COMPOSITION alias from CONTAINS clause
        contains_node = ast.get("contains")
        if contains_node and contains_node.get("rmType") == "COMPOSITION":
            composition_alias = contains_node.get("alias")
        
        # Validate that we found the required aliases
        if not ehr_alias:
            raise ValueError("Could not detect EHR alias from AST")
        
        if not composition_alias:
            raise ValueError("Could not detect COMPOSITION alias from AST")
            
        return ehr_alias, composition_alias
