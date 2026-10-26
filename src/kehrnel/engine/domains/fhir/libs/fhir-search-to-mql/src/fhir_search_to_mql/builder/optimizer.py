"""
Query optimizer for MongoDB queries.

Optimizes query structure by flattening, removing redundancy, and merging conditions.
"""

from typing import Dict, Any, List, Optional
import copy


class QueryOptimizer:
    """
    Optimize MongoDB query structure.
    
    Performs optimizations:
    - Flatten unnecessary $and/$or operators
    - Remove redundant conditions
    - Merge adjacent conditions
    - Simplify single-element operators
    """
    
    def __init__(self):
        """Initialize the query optimizer."""
        pass
    
    def optimize(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize a MongoDB query.
        
        Args:
            query: MongoDB query to optimize
            
        Returns:
            Optimized query
            
        Examples:
            >>> optimizer = QueryOptimizer()
            >>> optimizer.optimize({"$and": [{"field": "value"}]})
            {"field": "value"}
            
            >>> optimizer.optimize({"$or": [{"field": "value"}]})
            {"field": "value"}
        """
        if not query:
            return query
        
        # Make a copy to avoid modifying original
        optimized = copy.deepcopy(query)
        
        # Apply optimization passes
        optimized = self._flatten_operators(optimized)
        optimized = self._simplify_single_element_operators(optimized)
        optimized = self._remove_redundant_conditions(optimized)
        optimized = self._merge_adjacent_conditions(optimized)
        
        return optimized
    
    def _flatten_operators(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten nested logical operators.
        
        Examples:
            {"$and": [{"$and": [{"a": 1}]}]} → {"$and": [{"a": 1}]}
            {"$and": [{"field1": "v1"}, {"$and": [{"field2": "v2"}]}]} 
                → {"$and": [{"field1": "v1"}, {"field2": "v2"}]}
        
        Args:
            query: Query to flatten
            
        Returns:
            Flattened query
        """
        if not isinstance(query, dict):
            return query
        
        result = {}
        
        for key, value in query.items():
            if key in ['$and', '$or']:
                # Flatten nested same operator
                flattened = []
                for item in value:
                    if isinstance(item, dict) and len(item) == 1 and key in item:
                        # Nested same operator - flatten it
                        flattened.extend(item[key])
                    else:
                        # Recursively flatten the item
                        flattened.append(self._flatten_operators(item))
                result[key] = flattened
            elif isinstance(value, dict):
                # Recursively flatten nested dicts
                result[key] = self._flatten_operators(value)
            elif isinstance(value, list):
                # Recursively flatten list items
                result[key] = [
                    self._flatten_operators(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                result[key] = value
        
        return result
    
    def _simplify_single_element_operators(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Simplify operators with single elements.
        
        Examples:
            {"$and": [{"field": "value"}]} → {"field": "value"}
            {"$or": [{"field": "value"}]} → {"field": "value"}
        
        Args:
            query: Query to simplify
            
        Returns:
            Simplified query
        """
        if not isinstance(query, dict):
            return query
        
        # Check if this is a single-element operator
        if len(query) == 1:
            key = list(query.keys())[0]
            value = query[key]
            
            if key in ['$and', '$or'] and isinstance(value, list) and len(value) == 1:
                # Single element in operator - unwrap it
                return self._simplify_single_element_operators(value[0])
        
        # Recursively simplify nested structures
        result = {}
        for key, value in query.items():
            if isinstance(value, dict):
                result[key] = self._simplify_single_element_operators(value)
            elif isinstance(value, list):
                result[key] = [
                    self._simplify_single_element_operators(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                result[key] = value
        
        return result
    
    def _remove_redundant_conditions(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove redundant or duplicate conditions.
        
        Examples:
            {"$and": [{"a": 1}, {"a": 1}]} → {"$and": [{"a": 1}]}
            {"$or": [{"a": 1}, {"a": 1}]} → {"$or": [{"a": 1}]}
        
        Args:
            query: Query to clean
            
        Returns:
            Query without redundancy
        """
        if not isinstance(query, dict):
            return query
        
        result = {}
        
        for key, value in query.items():
            if key in ['$and', '$or'] and isinstance(value, list):
                # Remove duplicate conditions
                unique_conditions = []
                seen = set()
                
                for condition in value:
                    # Recursively clean nested conditions
                    cleaned = self._remove_redundant_conditions(condition)
                    
                    # Use string representation for comparison
                    # (not perfect but works for most cases)
                    condition_str = str(sorted(cleaned.items()) if isinstance(cleaned, dict) else cleaned)
                    
                    if condition_str not in seen:
                        seen.add(condition_str)
                        unique_conditions.append(cleaned)
                
                result[key] = unique_conditions
            elif isinstance(value, dict):
                result[key] = self._remove_redundant_conditions(value)
            elif isinstance(value, list):
                result[key] = [
                    self._remove_redundant_conditions(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                result[key] = value
        
        return result
    
    def _merge_adjacent_conditions(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge adjacent field conditions when possible.
        
        Example:
            If $and contains only field conditions with no duplicates,
            merge them into a single dict.
            
            {"$and": [{"a": 1}, {"b": 2}, {"c": 3}]} → {"a": 1, "b": 2, "c": 3}
        
        Args:
            query: Query to merge
            
        Returns:
            Query with merged conditions
        """
        if not isinstance(query, dict):
            return query
        
        # Only handle top-level $and for now
        if len(query) == 1 and '$and' in query:
            conditions = query['$and']
            
            # Check if all conditions are simple field matches (no nested operators)
            can_merge = True
            all_fields = []
            
            for condition in conditions:
                if not isinstance(condition, dict):
                    can_merge = False
                    break
                
                # Check for logical operators or complex conditions
                for key in condition.keys():
                    if key.startswith('$'):
                        can_merge = False
                        break
                    all_fields.append(key)
                
                if not can_merge:
                    break
            
            # Check for duplicate fields
            if can_merge and len(all_fields) != len(set(all_fields)):
                can_merge = False
            
            # Merge if possible
            if can_merge and len(conditions) > 0:
                merged = {}
                for condition in conditions:
                    merged.update(condition)
                return merged
        
        # Recursively process nested structures
        result = {}
        for key, value in query.items():
            if isinstance(value, dict):
                result[key] = self._merge_adjacent_conditions(value)
            elif isinstance(value, list):
                result[key] = [
                    self._merge_adjacent_conditions(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                result[key] = value
        
        return result
    
    def estimate_complexity(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Estimate query complexity.
        
        Returns metrics about the query:
        - Number of conditions
        - Nesting depth
        - Number of logical operators
        - Estimated performance category (fast/medium/slow)
        
        Args:
            query: Query to analyze
            
        Returns:
            Dict with complexity metrics
        """
        metrics = {
            'num_conditions': 0,
            'max_depth': 0,
            'num_logical_ops': 0,
            'num_fields': 0,
            'has_or': False,
            'has_nor': False,
            'has_regex': False,
            'performance': 'fast'
        }
        
        self._analyze_query(query, metrics, depth=0)
        
        # Determine performance category
        if metrics['num_conditions'] > 10:
            metrics['performance'] = 'slow'
        elif metrics['num_conditions'] > 5 or metrics['max_depth'] > 3:
            metrics['performance'] = 'medium'
        elif metrics['has_regex']:
            metrics['performance'] = 'slow'
        
        return metrics
    
    def _analyze_query(
        self, 
        query: Any, 
        metrics: Dict[str, Any], 
        depth: int
    ) -> None:
        """
        Recursively analyze query structure.
        
        Args:
            query: Query or query fragment
            metrics: Metrics dict to update
            depth: Current nesting depth
        """
        if depth > metrics['max_depth']:
            metrics['max_depth'] = depth
        
        if not isinstance(query, dict):
            return
        
        for key, value in query.items():
            if key == '$and':
                metrics['num_logical_ops'] += 1
                metrics['num_conditions'] += len(value) if isinstance(value, list) else 1
                if isinstance(value, list):
                    for item in value:
                        self._analyze_query(item, metrics, depth + 1)
            elif key == '$or':
                metrics['num_logical_ops'] += 1
                metrics['has_or'] = True
                metrics['num_conditions'] += len(value) if isinstance(value, list) else 1
                if isinstance(value, list):
                    for item in value:
                        self._analyze_query(item, metrics, depth + 1)
            elif key == '$nor':
                metrics['num_logical_ops'] += 1
                metrics['has_nor'] = True
                metrics['num_conditions'] += len(value) if isinstance(value, list) else 1
                if isinstance(value, list):
                    for item in value:
                        self._analyze_query(item, metrics, depth + 1)
            elif key == '$regex':
                metrics['has_regex'] = True
            elif not key.startswith('$'):
                # Field condition
                metrics['num_fields'] += 1
                metrics['num_conditions'] += 1
                if isinstance(value, dict):
                    self._analyze_query(value, metrics, depth + 1)
