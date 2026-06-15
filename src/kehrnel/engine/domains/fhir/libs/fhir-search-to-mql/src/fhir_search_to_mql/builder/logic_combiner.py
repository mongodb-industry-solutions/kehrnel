"""
Logic combiner for MongoDB queries.

Combines multiple query fragments using AND/OR logic with intelligent merging.
"""

from typing import Dict, Any, List, Optional


class LogicCombiner:
    """
    Combine MongoDB query fragments with AND/OR logic.
    
    Provides intelligent merging strategies that produce optimal query structures.
    """
    
    def __init__(self):
        """Initialize the logic combiner."""
        pass
    
    def combine_and(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine queries with AND logic.
        
        Strategy:
        1. Remove empty queries
        2. If single query, return it directly
        3. If all queries have different top-level fields, merge them
        4. Otherwise, use $and operator
        
        Args:
            queries: List of queries to combine with AND
            
        Returns:
            Combined query
            
        Examples:
            >>> combiner = LogicCombiner()
            >>> combiner.combine_and([{"name": "John"}, {"age": 30}])
            {"name": "John", "age": 30}
            
            >>> combiner.combine_and([{"name": "John"}, {"name": "Jane"}])
            {"$and": [{"name": "John"}, {"name": "Jane"}]}
        """
        # Remove empty queries
        queries = [q for q in queries if q]
        
        if not queries:
            return {}
        
        # Single query
        if len(queries) == 1:
            return queries[0]
        
        # Try to merge if all fields are different
        all_fields = []
        for query in queries:
            # Get top-level keys
            all_fields.extend(query.keys())
        
        # Check for duplicates and logical operators
        has_logical_ops = any(key in ['$and', '$or', '$nor'] for key in all_fields)
        has_duplicates = len(all_fields) != len(set(all_fields))
        
        if not has_logical_ops and not has_duplicates:
            # Safe to merge
            result = {}
            for query in queries:
                result.update(query)
            return result
        
        # Use $and operator
        return {"$and": queries}
    
    def combine_or(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine queries with OR logic.
        
        Strategy:
        1. Remove empty queries
        2. If single query, return it directly
        3. Otherwise, use $or operator
        
        Args:
            queries: List of queries to combine with OR
            
        Returns:
            Combined query with $or
            
        Examples:
            >>> combiner = LogicCombiner()
            >>> combiner.combine_or([{"name": "John"}, {"name": "Jane"}])
            {"$or": [{"name": "John"}, {"name": "Jane"}]}
        """
        # Remove empty queries
        queries = [q for q in queries if q]
        
        if not queries:
            return {}
        
        # Single query
        if len(queries) == 1:
            return queries[0]
        
        # Use $or operator
        return {"$or": queries}
    
    def combine_nor(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine queries with NOR logic.
        
        Args:
            queries: List of queries to combine with NOR
            
        Returns:
            Combined query with $nor
        """
        # Remove empty queries
        queries = [q for q in queries if q]
        
        if not queries:
            return {}
        
        # NOR always needs the operator
        return {"$nor": queries}
    
    def combine_same_parameter(
        self, 
        queries: List[Dict[str, Any]], 
        parameter_name: str
    ) -> Dict[str, Any]:
        """
        Combine multiple queries for the same parameter.
        
        When a parameter appears multiple times (e.g., name=John&name=Jane),
        combine with OR logic.
        
        Args:
            queries: List of queries for the same parameter
            parameter_name: Name of the parameter
            
        Returns:
            Combined query with OR
            
        Example:
            >>> combiner = LogicCombiner()
            >>> combiner.combine_same_parameter(
            ...     [{"name": "John"}, {"name": "Jane"}],
            ...     "name"
            ... )
            {"$or": [{"name": "John"}, {"name": "Jane"}]}
        """
        return self.combine_or(queries)
    
    def combine_different_parameters(
        self, 
        queries: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Combine queries for different parameters.
        
        When different parameters are used, combine with AND logic.
        
        Args:
            queries: List of queries for different parameters
            
        Returns:
            Combined query with AND
            
        Example:
            >>> combiner = LogicCombiner()
            >>> combiner.combine_different_parameters(
            ...     [{"name": "John"}, {"age": 30}]
            ... )
            {"name": "John", "age": 30}
        """
        return self.combine_and(queries)
    
    def merge_adjacent_and(self, queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Merge adjacent AND conditions.
        
        Flattens nested $and operators.
        
        Args:
            queries: List of queries
            
        Returns:
            Flattened list of queries
            
        Example:
            >>> combiner = LogicCombiner()
            >>> combiner.merge_adjacent_and([
            ...     {"$and": [{"a": 1}, {"b": 2}]},
            ...     {"c": 3}
            ... ])
            [{"a": 1}, {"b": 2}, {"c": 3}]
        """
        result = []
        for query in queries:
            if isinstance(query, dict) and len(query) == 1 and '$and' in query:
                # Flatten nested $and
                result.extend(query['$and'])
            else:
                result.append(query)
        return result
    
    def merge_adjacent_or(self, queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Merge adjacent OR conditions.
        
        Flattens nested $or operators.
        
        Args:
            queries: List of queries
            
        Returns:
            Flattened list of queries
            
        Example:
            >>> combiner = LogicCombiner()
            >>> combiner.merge_adjacent_or([
            ...     {"$or": [{"a": 1}, {"b": 2}]},
            ...     {"c": 3}
            ... ])
            [{"a": 1}, {"b": 2}, {"c": 3}]
        """
        result = []
        for query in queries:
            if isinstance(query, dict) and len(query) == 1 and '$or' in query:
                # Flatten nested $or
                result.extend(query['$or'])
            else:
                result.append(query)
        return result
