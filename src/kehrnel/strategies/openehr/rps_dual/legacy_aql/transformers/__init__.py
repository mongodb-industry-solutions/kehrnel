# AQL Transformers Module
"""
Modular AQL to MQL transformation components.

This package provides a clean separation of concerns for transforming 
AQL (Archetype Query Language) ASTs to MongoDB aggregation pipelines:

- ASTValidator: Validates AST structure and extracts key aliases
- ContextMapper: Maps variable aliases to archetype IDs  
- ValueFormatter: Handles value type conversions
- PathResolver: Translates AQL paths to MongoDB field paths
- ConditionProcessor: Processes WHERE clause conditions with OR/AND logic
- PipelineBuilder: Constructs individual MongoDB aggregation stages
- AQLtoMQLTransformer: Main orchestrator class

Usage:
    from .aql_transformer import AQLtoMQLTransformer
    
    transformer = AQLtoMQLTransformer(ast, ehr_id)
    pipeline = transformer.build_pipeline()
"""

from .ast_validator import ASTValidator
from .context_mapper import ContextMapper
from .condition_processor import ConditionProcessor
from .pipeline_builder import PipelineBuilder
from .path_resolver import PathResolver
from .format_resolver import FormatResolver
from .value_formatter import ValueFormatter
from .aql_transformer import AQLtoMQLTransformer

__all__ = [
    'ASTValidator',
    'ContextMapper', 
    'ConditionProcessor',
    'PipelineBuilder',
    'PathResolver',
    'FormatResolver',
    'ValueFormatter',
    'AQLtoMQLTransformer'
]
