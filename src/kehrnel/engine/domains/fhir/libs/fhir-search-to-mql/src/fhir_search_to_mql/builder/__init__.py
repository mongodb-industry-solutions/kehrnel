"""
Builder package for constructing MongoDB queries.
"""

from fhir_search_to_mql.builder.mql_builder import MQLBuilder, QueryMetadata
from fhir_search_to_mql.builder.logic_combiner import LogicCombiner
from fhir_search_to_mql.builder.optimizer import QueryOptimizer
from fhir_search_to_mql.builder.validator import QueryValidator, ValidationResult
from fhir_search_to_mql.builder.index_recommender import (
    IndexRecommender,
    IndexRecommendation,
    IndexPriority
)

__all__ = [
    # Main builder
    "MQLBuilder",
    "QueryMetadata",
    # Components
    "LogicCombiner",
    "QueryOptimizer",
    "QueryValidator",
    "ValidationResult",
    "IndexRecommender",
    "IndexRecommendation",
    "IndexPriority",
]
