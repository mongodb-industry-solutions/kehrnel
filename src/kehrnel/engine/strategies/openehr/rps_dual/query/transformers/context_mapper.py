# src/kehrnel/api/aql/transformers/context_mapper.py
from typing import Dict, Any, Optional


class ContextMapper:
    """
    Builds context map that walks the FROM and CONTAINS clauses of the AST 
    to map variable aliases to their archetype IDs and parent relationships.
    """

    def __init__(self):
        self.context_map: Dict[str, Dict] = {}

    def build_context_map(self, ast: Dict[str, Any]) -> Dict[str, Dict]:
        """
        Walks the FROM and CONTAINS clauses of the AST to map variable aliases
        to their archetype IDs and parent relationships. This context is essential
        for reconstructing hierarchical paths for querying.
        """
        self.context_map = {}
        
        from_alias = ast.get("from", {}).get("alias")
        if from_alias:
            self.context_map[from_alias] = {"archetype_id": None, "parent": None}
        
        contains_node = ast.get("contains")
        if contains_node:
            self._process_contains_node(contains_node, parent_alias=from_alias)
            
        return self.context_map

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

        predicate = node.get("predicate")
        archetype_id = predicate.get("value") if predicate else None
        self.context_map[current_alias] = {"archetype_id": archetype_id, "parent": parent_alias}
        
        if "contains" in node:
            self._process_contains_node(node["contains"], parent_alias=current_alias)
