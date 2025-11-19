# src/api/v1/aql/transformers/aql_transformer.py
from typing import Any, Dict, List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from .ast_validator import ASTValidator
from .context_mapper import ContextMapper
from .format_resolver import FormatResolver
from .pipeline_builder import PipelineBuilder
from .search_pipeline_builder import SearchPipelineBuilder
from .archetype_resolver import ArchetypeResolver
from src.persistence import PersistenceStrategy, get_default_strategy


class AQLtoMQLTransformer:
    """
    Transforms an AQL Abstract Syntax Tree (AST) into a MongoDB Aggregation Pipeline.
    
    This transformer is designed for a specific "semi-flattened" openEHR schema where
    a composition document contains a 'cn' array, and each element in 'cn' has:
    - 'p': A string representing the hierarchical path of the node.
    - 'data': The actual openEHR Reference Model object for that node.
    
    This class now orchestrates the transformation using specialized components:
    - ASTValidator: Validates AST structure
    - ContextMapper: Maps variable aliases to archetype IDs
    - PathResolver: Handles AQL path translation 
    - PipelineBuilder: Constructs MongoDB aggregation stages
    """

    def __init__(
        self,
        ast: Dict[str, Any],
        ehr_id: Optional[str] = None,
        schema_config: Optional[Dict[str, str]] = None,
        db: Optional[AsyncIOMotorDatabase] = None,
        search_index_name: str = "search_compositions_index",
        strategy: Optional[PersistenceStrategy] = None,
    ):
        self.ast = ast
        self.ehr_id = ehr_id
        self.db = db
        self.search_index_name = search_index_name
        self.strategy = strategy or get_default_strategy()
        
        # Schema field configuration (Point 3 preparation)
        self.schema_config = schema_config or self._build_schema_config_from_strategy(self.strategy)
        
        # LET variable storage
        self.let_variables: Dict[str, Any] = {}
        
        # Initialize components
        self._initialize_components()

    def _initialize_components(self):
        """Initialize all transformer components with proper dependencies."""
        # 1. Validate AST structure
        ASTValidator.validate_ast(self.ast)
        
        # 2. Detect key aliases
        self.ehr_alias, self.composition_alias = ASTValidator.detect_key_aliases(self.ast)
        
        # 3. Build context map
        self.context_mapper = ContextMapper()
        self.context_map = self.context_mapper.build_context_map(self.ast)
        
        # 4. Process LET variables
        self._process_let_variables()
        
        # 5. Initialize archetype resolver if database is available
        self.archetype_resolver = None
        if self.db is not None:
            self.archetype_resolver = ArchetypeResolver(self.db)
        
        # 6. Initialize format resolver with archetype resolver
        self.path_resolver = FormatResolver(
            self.context_map, 
            self.ehr_alias, 
            self.composition_alias, 
            self.schema_config,
            self.archetype_resolver
        )
        
        # 7. Initialize pipeline builders (both standard and search)
        self.pipeline_builder = PipelineBuilder(
            self.ehr_alias,
            self.composition_alias,
            self.schema_config,
            self.path_resolver,
            self.context_map,
            self.let_variables
        )
        
        # 8. Initialize search pipeline builder
        self.search_pipeline_builder = SearchPipelineBuilder(
            self.ehr_alias,
            self.composition_alias,
            self.schema_config,
            self.path_resolver,
            self.context_map,
            self.let_variables,
            strategy=self.strategy,
            search_index_name=self.search_index_name,
        )

    def _process_let_variables(self):
        """
        Processes LET clause variables and stores them for resolution during query building.
        LET variables can contain literals, paths, expressions, or computed values.
        """
        let_clause = self.ast.get("let")
        if not let_clause:
            return
        
        variables = let_clause.get("variables", {})
        for var_id, var_def in variables.items():
            var_name = var_def.get("name")
            var_expression = var_def.get("expression")
            
            if not var_name or not var_expression:
                continue
                
            # Store the variable definition for later resolution
            self.let_variables[var_name] = var_expression

    async def build_pipeline(self) -> List[Dict[str, Any]]:
        """
        Constructs the full MongoDB aggregation pipeline from the AST.
        Uses standard pipeline builder (for flatten_compositions collection).
        """
        pipeline = []

        # 1. Build the $match stage from WHERE and CONTAINS clauses
        match_stage = await self.pipeline_builder.build_match_stage(self.ast, self.ehr_id)
        if match_stage:
            pipeline.append(match_stage)

        # 2. Build the $addFields stage for LET variables (before projection)
        let_stage = self.pipeline_builder.build_let_stage()
        if let_stage:
            pipeline.append(let_stage)

        # 3. Build the $project stage from the SELECT clause
        project_stage = await self.pipeline_builder.build_project_stage(self.ast)
        if project_stage:
            pipeline.append(project_stage)

        # 4. Build the $sort stage from ORDER BY clause
        sort_stage = self.pipeline_builder.build_sort_stage(self.ast)
        if sort_stage:
            pipeline.append(sort_stage)
        
        # 5. Build the $limit stage from LIMIT clause
        limit_stage = self.pipeline_builder.build_limit_stage(self.ast)
        if limit_stage:
            pipeline.append(limit_stage)
        
        return pipeline

    async def build_search_pipeline(self) -> List[Dict[str, Any]]:
        """
        Constructs the full MongoDB aggregation pipeline starting with $search.
        Uses search pipeline builder (for search collection).
        """
        return await self.search_pipeline_builder.build_search_pipeline(self.ast)

    def should_use_search_strategy(self, ehr_id: Optional[str] = None, force_search: bool = False) -> bool:
        """
        Determines whether to use search strategy based on query characteristics.
        
        Args:
            ehr_id: EHR ID if provided in request
            force_search: Force search strategy for testing
            
        Returns:
            True if search strategy should be used, False for standard match strategy
        """
        # Force search strategy if requested (for testing)
        if force_search:
            return True
            
        # Use search strategy when no specific EHR ID is provided
        # This enables cross-EHR queries using Atlas Search
        return ehr_id is None

    # --- Public utility methods for backward compatibility ---
    
    def get_context_map(self) -> Dict[str, Dict]:
        """Returns the context map for external access."""
        return self.context_map
    
    def get_aliases(self) -> tuple[str, str]:
        """Returns the detected EHR and composition aliases."""
        return self.ehr_alias, self.composition_alias
    
    def get_let_variables(self) -> Dict[str, Any]:
        """Returns the processed LET variables."""
        return self.let_variables

    def _build_schema_config_from_strategy(self, strategy: PersistenceStrategy) -> Dict[str, str]:
        composition_fields = strategy.fields.get("composition")
        return {
            'composition_array': composition_fields.nodes if composition_fields and composition_fields.nodes else 'cn',
            'path_field': composition_fields.path if composition_fields and composition_fields.path else 'p',
            'data_field': composition_fields.data if composition_fields and composition_fields.data else 'data',
        }
