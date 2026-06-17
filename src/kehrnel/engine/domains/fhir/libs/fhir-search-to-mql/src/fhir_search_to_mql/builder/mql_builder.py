"""
MQL Builder - Constructs MongoDB queries from converted parameters.
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
import time

from fhir_search_to_mql.core.exceptions import ConversionError
from fhir_search_to_mql.builder.logic_combiner import LogicCombiner
from fhir_search_to_mql.builder.optimizer import QueryOptimizer
from fhir_search_to_mql.builder.validator import QueryValidator, ValidationResult
from fhir_search_to_mql.builder.index_recommender import IndexRecommender


@dataclass
class QueryMetadata:
    """
    Metadata about the built query.
    
    Attributes:
        parsed_parameters: List of parsed parameter names
        num_conditions: Number of query conditions
        index_hints: Recommended indexes
        performance_estimate: Estimated performance category
        warnings: List of warnings
        complexity: Query complexity metrics
        build_time_ms: Time taken to build query (milliseconds)
    """
    parsed_parameters: List[str] = field(default_factory=list)
    num_conditions: int = 0
    index_hints: List[str] = field(default_factory=list)
    performance_estimate: str = "fast"
    warnings: List[str] = field(default_factory=list)
    complexity: Dict[str, Any] = field(default_factory=dict)
    build_time_ms: float = 0.0


class MQLBuilder:
    """
    Build MongoDB Query Language (MQL) queries.
    
    Combines individual parameter queries with AND/OR logic.
    Applies optimizations and validates query structure.
    Provides metadata and query explanation capabilities.
    """
    
    def __init__(
        self,
        resource_type: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        enable_optimization: bool = True,
        enable_validation: bool = True
    ):
        """
        Initialize the MQL builder.
        
        Args:
            resource_type: FHIR resource type (e.g., "Patient")
            config: Resource configuration
            enable_optimization: Enable query optimization
            enable_validation: Enable query validation
        """
        self.resource_type = resource_type or "Resource"
        self.config = config or {}
        self.enable_optimization = enable_optimization
        self.enable_validation = enable_validation
        
        # Initialize components
        self.combiner = LogicCombiner()
        self.optimizer = QueryOptimizer()
        self.validator = QueryValidator(config)
        self.index_recommender = IndexRecommender(self.resource_type, config)
    
    def build(
        self, 
        parameter_queries: List[Dict[str, Any]], 
        logic: str = 'AND',
        parameter_names: Optional[List[str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Build final MQL query from parameter queries.
        
        Args:
            parameter_queries: List of MongoDB query dictionaries
            logic: Combination logic ('AND' or 'OR')
            parameter_names: Names of parsed parameters (for metadata)
            dry_run: If True, return explanation instead of query
            
        Returns:
            Combined MongoDB query (or explanation if dry_run=True)
            
        Raises:
            ConversionError: If query building fails
        """
        start_time = time.time()
        
        if not parameter_queries:
            if dry_run:
                return self._build_explanation({}, [], parameter_names or [])
            return {}
        
        # Remove empty queries
        parameter_queries = [q for q in parameter_queries if q]
        
        if not parameter_queries:
            if dry_run:
                return self._build_explanation({}, [], parameter_names or [])
            return {}
        
        # Single query
        if len(parameter_queries) == 1:
            query = parameter_queries[0]
        else:
            # Combine with specified logic
            if logic.upper() == 'AND':
                query = self.combiner.combine_and(parameter_queries)
            elif logic.upper() == 'OR':
                query = self.combiner.combine_or(parameter_queries)
            else:
                raise ConversionError(f"Unknown logic operator: {logic}")
        
        # Optimize query
        if self.enable_optimization:
            query = self.optimizer.optimize(query)
        
        # Validate query
        if self.enable_validation:
            validation = self.validator.validate_query(query, self.resource_type)
            if not validation.is_valid:
                raise ConversionError(f"Query validation failed: {validation.errors}")
        
        # Build metadata
        build_time = (time.time() - start_time) * 1000  # milliseconds
        
        # Dry run mode - return explanation
        if dry_run:
            return self._build_explanation(query, parameter_queries, parameter_names or [], build_time)
        
        return query
    
    def build_with_metadata(
        self,
        parameter_queries: List[Dict[str, Any]],
        logic: str = 'AND',
        parameter_names: Optional[List[str]] = None
    ) -> Tuple[Dict[str, Any], QueryMetadata]:
        """
        Build query and return with metadata.
        
        Args:
            parameter_queries: List of MongoDB query dictionaries
            logic: Combination logic ('AND' or 'OR')
            parameter_names: Names of parsed parameters
            
        Returns:
            Tuple of (query, metadata)
        """
        start_time = time.time()
        
        # Build query
        query = self.build(parameter_queries, logic, parameter_names, dry_run=False)
        
        # Build metadata
        metadata = self._build_metadata(query, parameter_names or [])
        metadata.build_time_ms = (time.time() - start_time) * 1000
        
        return query, metadata
    
    def _build_metadata(
        self,
        query: Dict[str, Any],
        parameter_names: List[str]
    ) -> QueryMetadata:
        """
        Build metadata for query.
        
        Args:
            query: MongoDB query
            parameter_names: List of parsed parameter names
            
        Returns:
            QueryMetadata object
        """
        metadata = QueryMetadata()
        
        # Set parsed parameters
        metadata.parsed_parameters = parameter_names
        
        # Get complexity metrics
        complexity = self.optimizer.estimate_complexity(query)
        metadata.complexity = complexity
        metadata.num_conditions = complexity['num_conditions']
        metadata.performance_estimate = complexity['performance']
        
        # Get validation warnings
        validation = self.validator.validate_query(query, self.resource_type)
        metadata.warnings = validation.warnings
        
        # Get index recommendations
        recommendations = self.index_recommender.analyze(query)
        metadata.index_hints = [
            f"{rec.index_type}: {', '.join(rec.fields)} ({rec.priority.value})"
            for rec in recommendations[:3]  # Top 3 recommendations
        ]
        
        return metadata
    
    def _build_explanation(
        self,
        query: Dict[str, Any],
        parameter_queries: List[Dict[str, Any]],
        parameter_names: List[str],
        build_time: float = 0.0
    ) -> Dict[str, Any]:
        """
        Build query explanation (dry-run mode).
        
        Args:
            query: Final MongoDB query
            parameter_queries: Individual parameter queries
            parameter_names: Parameter names
            build_time: Time taken to build (ms)
            
        Returns:
            Explanation dict
        """
        # Get metadata
        metadata = self._build_metadata(query, parameter_names)
        metadata.build_time_ms = build_time
        
        # Get index recommendations
        recommendations = self.index_recommender.analyze(query)
        
        explanation = {
            'resource_type': self.resource_type,
            'final_query': query,
            'parameter_queries': parameter_queries,
            'parsed_parameters': parameter_names,
            'num_parameters': len(parameter_names),
            'num_conditions': metadata.num_conditions,
            'complexity': metadata.complexity,
            'performance_estimate': metadata.performance_estimate,
            'warnings': metadata.warnings,
            'index_recommendations': [
                {
                    'type': rec.index_type,
                    'fields': rec.fields,
                    'priority': rec.priority.value,
                    'reason': rec.reason,
                    'command': rec.command
                }
                for rec in recommendations
            ],
            'build_time_ms': build_time,
            'optimization_enabled': self.enable_optimization,
            'validation_enabled': self.enable_validation
        }
        
        return explanation
    
    def explain(
        self,
        parameter_queries: List[Dict[str, Any]],
        logic: str = 'AND',
        parameter_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Explain query without executing (dry-run mode).
        
        Returns detailed explanation including:
        - Final query
        - Individual parameter queries
        - Complexity analysis
        - Index recommendations
        - Performance warnings
        
        Args:
            parameter_queries: List of MongoDB query dictionaries
            logic: Combination logic ('AND' or 'OR')
            parameter_names: Names of parsed parameters
            
        Returns:
            Query explanation dict
        """
        return self.build(parameter_queries, logic, parameter_names, dry_run=True)
    
    def _combine_with_and(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine queries with AND logic.
        
        Deprecated: Use LogicCombiner.combine_and() instead.
        
        Args:
            queries: List of queries to combine
            
        Returns:
            Combined query
        """
        return self.combiner.combine_and(queries)
    
    def _combine_with_or(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine queries with OR logic.
        
        Deprecated: Use LogicCombiner.combine_or() instead.
        
        Args:
            queries: List of queries to combine
            
        Returns:
            Combined query with $or
        """
        return self.combiner.combine_or(queries)
    
    def add_compartment_filter(
        self, 
        query: Dict[str, Any], 
        compartment_query: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Add compartment filter to existing query.
        
        Args:
            query: Existing MongoDB query
            compartment_query: Compartment filter query
            
        Returns:
            Combined query with compartment filter
        """
        if not query:
            return compartment_query
        
        if not compartment_query:
            return query
        
        # Combine with AND
        return self.combiner.combine_and([query, compartment_query])
    
    def optimize(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize MongoDB query.
        
        Optimizations:
        - Flatten nested $and/$or where possible
        - Remove redundant conditions
        - Simplify single-element operators
        - Merge adjacent conditions
        
        Args:
            query: MongoDB query to optimize
            
        Returns:
            Optimized query
        """
        return self.optimizer.optimize(query)
    
    def validate(self, query: Dict[str, Any]) -> ValidationResult:
        """
        Validate MongoDB query structure.
        
        Args:
            query: MongoDB query to validate
            
        Returns:
            ValidationResult with errors and warnings
            
        Raises:
            ConversionError: If query is invalid
        """
        # Basic validation
        if not isinstance(query, dict):
            raise ConversionError("Query must be a dictionary")
        
        # Use validator
        result = self.validator.validate_query(query, self.resource_type)
        
        if not result.is_valid:
            raise ConversionError(f"Query validation failed: {', '.join(result.errors)}")
        
        return result
    
    def get_index_recommendations(
        self,
        query: Dict[str, Any],
        format: str = "text"
    ) -> str:
        """
        Get index recommendations for query.
        
        Args:
            query: MongoDB query to analyze
            format: Output format ("text", "markdown")
            
        Returns:
            Formatted index recommendations
        """
        recommendations = self.index_recommender.analyze(query)
        return self.index_recommender.format_recommendations(recommendations, format)
