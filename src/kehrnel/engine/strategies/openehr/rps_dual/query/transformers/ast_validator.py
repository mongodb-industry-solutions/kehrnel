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
    def _find_alias(node: Dict[str, Any] | None, rm_type: str) -> str | None:
        if not isinstance(node, dict):
            return None
        if node.get("rmType") == rm_type and node.get("alias"):
            return node.get("alias")

        contains_node = node.get("contains")
        found = ASTValidator._find_alias(contains_node, rm_type)
        if found:
            return found

        children = node.get("children")
        if isinstance(children, dict):
            for child in children.values():
                found = ASTValidator._find_alias(child, rm_type)
                if found:
                    return found
        return None

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
        composition_alias = ASTValidator._find_alias(ast.get("contains"), "COMPOSITION")
        
        # Validate that we found the required aliases
        if not ehr_alias:
            raise ValueError("Could not detect EHR alias from AST")
        
        if not composition_alias:
            raise ValueError("Could not detect COMPOSITION alias from AST")
            
        return ehr_alias, composition_alias

    @staticmethod
    def detect_version_alias(ast: Dict[str, Any]) -> str | None:
        version_clause = ast.get("version")
        if isinstance(version_clause, dict) and version_clause.get("alias"):
            return version_clause.get("alias")
        contains_alias = ASTValidator._find_alias(ast.get("contains"), "VERSION")
        if contains_alias:
            return contains_alias

        def _scan_path(value: Any) -> str | None:
            if isinstance(value, dict):
                path = value.get("path")
                if isinstance(path, str) and path.endswith("/commit_audit/time_committed/value") and "/" in path:
                    return path.split("/", 1)[0]
                inner = value.get("value")
                if isinstance(inner, dict):
                    return _scan_path(inner)
                for child in value.values():
                    found = _scan_path(child)
                    if found:
                        return found
            return None

        for section in (ast.get("select"), ast.get("where"), ast.get("orderBy")):
            found = _scan_path(section)
            if found:
                return found
        return None
