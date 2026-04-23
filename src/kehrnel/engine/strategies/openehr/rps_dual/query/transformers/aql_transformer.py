# src/kehrnel/api/compatibility/v1/aql/transformers/aql_transformer.py
from typing import Any, Dict, List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from .ast_validator import ASTValidator
from .context_mapper import ContextMapper
from .format_resolver import FormatResolver
from .pipeline_builder import PipelineBuilder
from .search_pipeline_builder import SearchPipelineBuilder
from .archetype_resolver import ArchetypeResolver
from ..strategy_selector import should_prefer_match_for_cross_patient_ast
from kehrnel.persistence import PersistenceStrategy, get_default_strategy


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
        search_schema_config: Optional[Dict[str, str]] = None,
        db: Optional[AsyncIOMotorDatabase] = None,
        search_index_name: str = "search_compositions_index",
        strategy: Optional[PersistenceStrategy] = None,
        shortcut_map: Optional[Dict[str, str]] = None,
    ):
        self.ast = ast
        self.ehr_id = ehr_id
        self.db = db
        self.search_index_name = search_index_name
        self.strategy = strategy or get_default_strategy()
        self.shortcut_map = shortcut_map or {}
        
        # Schema field configuration (Point 3 preparation)
        self.schema_config = schema_config or self._build_schema_config_from_strategy(self.strategy)
        self.search_schema_config = search_schema_config or self._build_search_schema_config_from_strategy(self.strategy)
        
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
        self.version_alias = ASTValidator.detect_version_alias(self.ast)
        
        # 3. Build context map
        self.context_mapper = ContextMapper()
        self.context_map = self.context_mapper.build_context_map(self.ast)
        
        # 4. Process LET variables
        self._process_let_variables()
        
        # 5. Initialize archetype resolver if database is available
        self.archetype_resolver = None
        if self.db is not None:
            self.archetype_resolver = ArchetypeResolver(
                self.db,
                codes_collection=self.schema_config.get("codes_collection"),
                codes_doc_id=self.schema_config.get("codes_doc_id"),
                search_collection=self.search_schema_config.get("collection"),
                composition_collection=self.schema_config.get("collection"),
                separator=self.schema_config.get("separator"),
                atcode_strategy=self.schema_config.get("atcode_strategy"),
            )
        
        # 6. Initialize format resolver with archetype resolver
        self.path_resolver = FormatResolver(
            self.context_map, 
            self.ehr_alias, 
            self.composition_alias, 
            self.schema_config,
            self.archetype_resolver,
            shortcut_map=self.shortcut_map,
            version_alias=self.version_alias,
        )
        self.search_path_resolver = FormatResolver(
            self.context_map,
            self.ehr_alias,
            self.composition_alias,
            self.search_schema_config,
            self.archetype_resolver,
            shortcut_map=self.shortcut_map,
            version_alias=self.version_alias,
        )
        
        # 7. Initialize pipeline builders (both standard and search)
        self.pipeline_builder = PipelineBuilder(
            self.ehr_alias,
            self.composition_alias,
            self.schema_config,
            self.path_resolver,
            self.context_map,
            self.let_variables,
            version_alias=self.version_alias,
        )
        
        # 8. Initialize search pipeline builder
        self.search_pipeline_builder = SearchPipelineBuilder(
            self.ehr_alias,
            self.composition_alias,
            self.schema_config,
            self.search_path_resolver,
            self.context_map,
            self.let_variables,
            strategy=self.strategy,
            search_index_name=self.search_index_name,
            version_alias=self.version_alias,
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

        # 3. Fan out rows only when the selected leaf alias is repeated.
        fanout_stages = await self.pipeline_builder.build_row_fanout_stages(self.ast)
        if fanout_stages:
            pipeline.extend(fanout_stages)

        # 4. Build the $project stage from the SELECT clause
        project_stage = await self.pipeline_builder.build_project_stage(self.ast)
        if project_stage:
            pipeline.append(project_stage)

        # 5. Build DISTINCT stages ($group + $replaceRoot) if SELECT DISTINCT is used
        # This must come after $project so we can group by the projected field names
        projected_fields = self.pipeline_builder.get_projected_field_names(self.ast)
        distinct_stages = self.pipeline_builder.build_distinct_stages(self.ast, projected_fields)
        if distinct_stages:
            pipeline.extend(distinct_stages)

        # 6. Build the $sort stage from ORDER BY clause
        sort_stage = self.pipeline_builder.build_sort_stage(self.ast)
        if sort_stage:
            pipeline.append(sort_stage)
        
        # 7. Build the $limit stage from LIMIT clause
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
        # unless the query shape is simple enough to stay on the flattened collection.
        if ehr_id is not None:
            return False
        return not should_prefer_match_for_cross_patient_ast(
            self.ast,
            ehr_alias=self.ehr_alias,
            composition_alias=self.composition_alias,
            version_alias=self.version_alias or "v",
        )

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
        atcode_cfg = strategy.coding.atcodes if strategy.coding and strategy.coding.atcodes else {}
        return {
            'composition_array': composition_fields.nodes if composition_fields and composition_fields.nodes else 'cn',
            'path_field': composition_fields.path if composition_fields and composition_fields.path else 'p',
            'data_field': composition_fields.data if composition_fields and composition_fields.data else 'data',
            'ehr_id': composition_fields.ehr_id if composition_fields and composition_fields.ehr_id else 'ehr_id',
            'comp_id': composition_fields.comp_id if composition_fields and composition_fields.comp_id else 'comp_id',
            'template_id': composition_fields.template_id if composition_fields and composition_fields.template_id else 'tid',
            'time_committed': composition_fields.time_committed if composition_fields and composition_fields.time_committed else 'time_c',
            'atcode_strategy': atcode_cfg.get('strategy', 'negative_int') if isinstance(atcode_cfg, dict) else 'negative_int',
        }

    def _build_search_schema_config_from_strategy(self, strategy: PersistenceStrategy) -> Dict[str, str]:
        search_fields = strategy.fields.get("search")
        atcode_cfg = strategy.coding.atcodes if strategy.coding and strategy.coding.atcodes else {}
        return {
            'composition_array': search_fields.nodes if search_fields and search_fields.nodes else 'sn',
            'path_field': search_fields.path if search_fields and search_fields.path else 'p',
            'data_field': search_fields.data if search_fields and search_fields.data else 'data',
            'ehr_id': search_fields.ehr_id if search_fields and search_fields.ehr_id else 'ehr_id',
            'comp_id': search_fields.comp_id if search_fields and search_fields.comp_id else 'comp_id',
            'template_id': search_fields.template_id if search_fields and search_fields.template_id else 'tid',
            'time_committed': search_fields.sort_time if search_fields and search_fields.sort_time else 'sort_time',
            'sort_time': search_fields.sort_time if search_fields and search_fields.sort_time else 'sort_time',
            'atcode_strategy': atcode_cfg.get('strategy', 'negative_int') if isinstance(atcode_cfg, dict) else 'negative_int',
        }
