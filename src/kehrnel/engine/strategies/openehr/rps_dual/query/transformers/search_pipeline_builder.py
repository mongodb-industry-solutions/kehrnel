# src/kehrnel/api/compatibility/v1/aql/transformers/search_pipeline_builder.py

from typing import Dict, Any, List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from .condition_processor import ConditionProcessor
from ..contains_clause import (
    build_shortened_contains_condition,
    build_shortened_row_fanout_spec,
    find_deepest_referenced_alias,
)
from .value_formatter import ValueFormatter
from .format_resolver import FormatResolver
from kehrnel.persistence import PersistenceStrategy, get_default_strategy
from kehrnel.api.bridge.app.core.config import settings
import logging
import re

logger = logging.getLogger(__name__)


class SearchPipelineBuilder:
    """
    Builds MongoDB aggregation pipelines that start with $search stages for Atlas Search.
    Designed specifically for the search collection (sm_search3) with 'sn' array structure.
    """

    def __init__(self, ehr_alias: str, composition_alias: str, schema_config: Dict[str, str], 
                 format_resolver: FormatResolver, context_map: Dict[str, Dict], 
                 let_variables: Dict[str, Any] = None,
                 strategy: Optional[PersistenceStrategy] = None,
                 search_index_name: Optional[str] = None,
                 version_alias: str | None = None):
        self.ehr_alias = ehr_alias
        self.composition_alias = composition_alias
        self.version_alias = version_alias
        self.schema_config = schema_config
        self.format_resolver = format_resolver
        self.context_map = context_map
        self.let_variables = let_variables or {}
        self.strategy = strategy or get_default_strategy()
        
        # Use centralized configuration like repository.py does
        self.search_index_name = (
            search_index_name
            or self._resolve_search_index_name()
            or settings.search_config.search_index_name
        )
        self.full_compositions_collection = self.schema_config.get("collection") or settings.search_config.flatten_collection
        
        # For search collection, we use 'sn' instead of 'cn'
        self.search_config = self._build_search_schema_config()
        
        self.condition_processor = ConditionProcessor(
            ehr_alias,
            composition_alias,
            self.search_config,
            format_resolver,
            let_variables,
            version_alias=version_alias,
        )
        self.value_formatter = ValueFormatter()

    def _build_search_schema_config(self):
        search_fields = self.strategy.fields.get("search") if self.strategy else None
        return {
            'composition_array': search_fields.nodes if search_fields and search_fields.nodes else 'sn',
            'path_field': search_fields.path if search_fields and search_fields.path else 'p',
            'data_field': search_fields.data if search_fields and search_fields.data else 'data',
            'ehr_id': search_fields.ehr_id if search_fields and search_fields.ehr_id else 'ehr_id',
            'comp_id': search_fields.comp_id if search_fields and search_fields.comp_id else 'comp_id',
            'template_id': search_fields.template_id if search_fields and search_fields.template_id else 'tid',
            'sort_time': search_fields.sort_time if search_fields and search_fields.sort_time else 'sort_time',
            'separator': self.schema_config.get("separator", ":"),
        }

    def _root_path_regex(self) -> str:
        separator = self.search_config.get("separator", ":") or ":"
        return rf"^[^{re.escape(separator)}]+$"

    def _resolve_search_index_name(self):
        search_collection = self.strategy.collections.get("search") if self.strategy else None
        return search_collection.atlas_index_name if search_collection else None

    def _first_matching_node_value(
        self,
        nodes_expr: Any,
        *,
        path_field: str,
        path_regex_pattern: Any,
        data_path: str,
    ) -> Dict[str, Any]:
        return {
            "$first": {
                "$map": {
                    "input": {
                        "$filter": {
                            "input": nodes_expr,
                            "as": "node",
                            "cond": {
                                "$regexMatch": {
                                    "input": f"$$node.{path_field}",
                                    "regex": path_regex_pattern,
                                }
                            },
                        }
                    },
                    "as": "node",
                    "in": f"$$node.{data_path}",
                }
            }
        }

    def _full_composition_nodes_expr(self) -> Dict[str, Any]:
        return {
            "$ifNull": [
                {
                    "$getField": {
                        "field": self.schema_config["composition_array"],
                        "input": {"$first": "$full_composition"},
                    }
                },
                [],
            ]
        }

    async def _get_row_fanout_spec(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return await build_shortened_row_fanout_spec(
            ast,
            ast.get("contains"),
            composition_alias=self.composition_alias,
            archetype_resolver=self.format_resolver.archetype_resolver,
            separator=self.search_config.get("separator", ":"),
        )

    def _fanout_alias_path_expr(self, leaf_path_expr: Any, alias_code: str) -> Dict[str, Any]:
        separator = self.search_config.get("separator", ":") or ":"
        return {
            "$let": {
                "vars": {
                    "parts": {"$split": [leaf_path_expr, separator]},
                },
                "in": {
                    "$let": {
                        "vars": {
                            "idx": {"$indexOfArray": ["$$parts", str(alias_code)]},
                        },
                        "in": {
                            "$cond": [
                                {"$gte": ["$$idx", 0]},
                                {
                                    "$reduce": {
                                        "input": {"$slice": ["$$parts", "$$idx", {"$size": "$$parts"}]},
                                        "initialValue": "",
                                        "in": {
                                            "$cond": [
                                                {"$eq": ["$$value", ""]},
                                                "$$this",
                                                {"$concat": ["$$value", separator, "$$this"]},
                                            ]
                                        },
                                    }
                                },
                                None,
                            ]
                        },
                    }
                },
            }
        }

    def _build_fanout_paths_document(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        leaf_expr: Any = "$__fanout_nodes.p"
        target_alias = spec["target_alias"]
        alias_codes = spec.get("alias_codes", {})
        paths: Dict[str, Any] = {}
        for alias in spec.get("aliases", []):
            if alias == target_alias:
                paths[alias] = leaf_expr
            else:
                code = alias_codes.get(alias)
                if code is not None:
                    paths[alias] = self._fanout_alias_path_expr(leaf_expr, str(code))
        return paths

    def _supports_exact_row_correlation(self) -> bool:
        return str(self.schema_config.get("path_instance_mode") or "").strip().lower() == "chain"

    def _normalized_instance_array_expr(self, path_expr: Any, instance_expr: Any) -> Dict[str, Any]:
        separator = self.schema_config.get("separator", ":") or ":"
        return {
            "$let": {
                "vars": {
                    "parts": {"$split": [path_expr, separator]},
                    "instances": instance_expr,
                },
                "in": {
                    "$cond": [
                        {"$isArray": "$$instances"},
                        "$$instances",
                        {
                            "$map": {
                                "input": {"$range": [0, {"$size": "$$parts"}]},
                                "as": "idx",
                                "in": -1,
                            }
                        },
                    ]
                },
            }
        }

    def _fanout_alias_instance_expr(self, leaf_path_expr: Any, leaf_instance_expr: Any, alias_code: str) -> Dict[str, Any]:
        separator = self.schema_config.get("separator", ":") or ":"
        return {
            "$let": {
                "vars": {
                    "parts": {"$split": [leaf_path_expr, separator]},
                    "instances": self._normalized_instance_array_expr(leaf_path_expr, leaf_instance_expr),
                },
                "in": {
                    "$let": {
                        "vars": {
                            "idx": {"$indexOfArray": ["$$parts", str(alias_code)]},
                        },
                        "in": {
                            "$cond": [
                                {"$gte": ["$$idx", 0]},
                                {"$slice": ["$$instances", "$$idx", {"$size": "$$instances"}]},
                                None,
                            ]
                        },
                    }
                },
            }
        }

    def _build_fanout_instances_document(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        instance_field = self.schema_config.get("path_instance_field", "pi")
        leaf_path_expr: Any = "$__fanout_nodes.p"
        leaf_instance_expr: Any = f"$__fanout_nodes.{instance_field}"
        target_alias = spec["target_alias"]
        alias_codes = spec.get("alias_codes", {})
        instances: Dict[str, Any] = {}
        for alias in spec.get("aliases", []):
            if alias == target_alias:
                instances[alias] = self._normalized_instance_array_expr(leaf_path_expr, leaf_instance_expr)
            else:
                code = alias_codes.get(alias)
                if code is not None:
                    instances[alias] = self._fanout_alias_instance_expr(leaf_path_expr, leaf_instance_expr, str(code))
        return instances

    def _build_fanout_regex_expr(self, alias_path_expr: Any, selector_codes: List[str]) -> Any:
        separator = self.search_config.get("separator", ":") or ":"
        prefix = separator.join(reversed([str(code) for code in selector_codes]))
        pieces: List[Any] = ["^"]
        if prefix:
            pieces.extend([prefix, separator])
        pieces.extend([alias_path_expr, "$"])
        return {"$concat": pieces}

    async def build_row_fanout_stages(self, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
        spec = await self._get_row_fanout_spec(ast)
        if not spec:
            return []

        path_field = self.schema_config["path_field"]
        fanout_add_fields: Dict[str, Any] = {
            "__fanout_paths": self._build_fanout_paths_document(spec),
        }
        if self._supports_exact_row_correlation():
            fanout_add_fields["__fanout_instances"] = self._build_fanout_instances_document(spec)
        return [
            {
                "$addFields": {
                    "__fanout_nodes": {
                        "$filter": {
                            "input": self._full_composition_nodes_expr(),
                            "as": "node",
                            "cond": {
                                "$regexMatch": {
                                    "input": f"$$node.{path_field}",
                                    "regex": spec["target_regex"],
                                }
                            },
                        }
                    }
                }
            },
            {"$unwind": "$__fanout_nodes"},
            {"$addFields": fanout_add_fields},
        ]

    def _iter_where_children(self, node: Dict[str, Any]) -> List[Dict[str, Any]]:
        children = node.get("conditions")
        if isinstance(children, dict):
            return [child for child in children.values() if isinstance(child, dict)]
        if isinstance(children, list):
            return [child for child in children if isinstance(child, dict)]
        return []

    def _where_has_row_sensitive_alias(self, node: Dict[str, Any] | None, spec: Dict[str, Any]) -> bool:
        if not isinstance(node, dict) or not node:
            return False

        operator = str(node.get("operator") or "").upper()
        if operator in {"AND", "OR"}:
            return any(self._where_has_row_sensitive_alias(child, spec) for child in self._iter_where_children(node))

        path = node.get("path")
        if not isinstance(path, str) or "/" not in path:
            return False
        alias = path.split("/", 1)[0].strip()
        return bool(alias and alias != self.composition_alias and alias in set(spec.get("full_aliases", [])))

    def _format_condition_value(self, path: str, value: Any) -> Any:
        formatted_value = self.value_formatter.format_value(value)
        if path in {"ehr_id", f"{self.ehr_alias}/ehr_id/value"}:
            return self.value_formatter.format_id_value(
                formatted_value,
                self.schema_config.get("ehr_id_encoding", "string"),
            )
        if path == f"{self.composition_alias}/uid/value":
            return self.value_formatter.format_id_value(
                formatted_value,
                self.schema_config.get("composition_id_encoding", "string"),
            )
        return formatted_value

    def _build_regex_predicate_expr(self, field_expr: Any, pattern: str) -> Dict[str, Any]:
        return {
            "$regexMatch": {
                "input": {"$toString": {"$ifNull": [field_expr, ""]}},
                "regex": pattern,
            }
        }

    def _build_sql_like_pattern(self, value: Any) -> str:
        parts = ["^"]
        for char in str(value):
            if char == "%":
                parts.append(".*")
            elif char == "_":
                parts.append(".")
            else:
                parts.append(re.escape(char))
        parts.append("$")
        return "".join(parts)

    def _build_matches_pattern(self, value: Any) -> str:
        if isinstance(value, dict):
            values = [value.get(str(idx)) for idx in range(len(value))]
            escaped = [re.escape(str(item)) for item in values if item is not None]
            if escaped:
                return rf"^(?:{'|'.join(escaped)})$"
        if isinstance(value, list):
            escaped = [re.escape(str(item)) for item in value if item is not None]
            if escaped:
                return rf"^(?:{'|'.join(escaped)})$"
        return str(value)

    def _build_value_predicate_expr(
        self,
        field_expr: Any,
        operator: str,
        value: Any,
        *,
        preformatted: bool = False,
    ) -> Optional[Dict[str, Any]]:
        op = str(operator or "").upper()
        formatted_value = value if preformatted else self.value_formatter.format_value(value)

        if op == "=":
            return {"$eq": [field_expr, formatted_value]}
        if op in {"!=", "<>"}:
            return {"$ne": [field_expr, formatted_value]}
        if op == ">":
            return {"$gt": [field_expr, formatted_value]}
        if op == "<":
            return {"$lt": [field_expr, formatted_value]}
        if op == ">=":
            return {"$gte": [field_expr, formatted_value]}
        if op == "<=":
            return {"$lte": [field_expr, formatted_value]}
        if op == "LIKE":
            return self._build_regex_predicate_expr(field_expr, self._build_sql_like_pattern(formatted_value))
        if op == "MATCHES":
            return self._build_regex_predicate_expr(field_expr, self._build_matches_pattern(formatted_value))
        return None

    def _combine_exprs(self, operator: str, exprs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not exprs:
            return None
        if len(exprs) == 1:
            return exprs[0]
        return {operator: exprs}

    def _build_direct_field_condition_expr(self, path: str, field_expr: Any, operator: str, value: Any) -> Optional[Dict[str, Any]]:
        op = str(operator or "").upper()
        if op == "EXISTS":
            return {"$ne": [{"$type": field_expr}, "missing"]}
        if op == "NOT EXISTS":
            return {"$eq": [{"$type": field_expr}, "missing"]}
        return self._build_value_predicate_expr(
            field_expr,
            op,
            self._format_condition_value(path, value),
            preformatted=True,
        )

    def _resolve_row_match_direct_field_expr(self, path: str) -> Optional[str]:
        if path in {"ehr_id", f"{self.ehr_alias}/ehr_id/value"}:
            return f"${self.search_config.get('ehr_id', 'ehr_id')}"
        document_field = self.format_resolver.resolve_document_field(path)
        if document_field:
            return f"${document_field}"
        return None

    def _build_candidate_correlation_expr(
        self,
        spec: Dict[str, Any],
        variable_alias: str,
        *,
        candidate_path_expr: Any,
        candidate_instance_expr: Any,
    ) -> Optional[Dict[str, Any]]:
        if not self._supports_exact_row_correlation():
            return None

        alias_index = spec.get("alias_index") or {}
        target_alias = spec.get("target_alias")
        target_index = alias_index.get(target_alias)
        variable_index = alias_index.get(variable_alias)
        if variable_index is None or target_index is None:
            return None

        anchor_alias = variable_alias if variable_index <= target_index else target_alias
        anchor_code = (spec.get("full_alias_codes") or {}).get(anchor_alias)
        if anchor_code is None:
            return None

        return {
            "$and": [
                {
                    "$eq": [
                        self._fanout_alias_path_expr(candidate_path_expr, str(anchor_code)),
                        f"$__fanout_paths.{anchor_alias}",
                    ]
                },
                {
                    "$eq": [
                        self._fanout_alias_instance_expr(candidate_path_expr, candidate_instance_expr, str(anchor_code)),
                        f"$__fanout_instances.{anchor_alias}",
                    ]
                },
            ]
        }

    async def _build_leaf_row_match_expr(
        self,
        condition: Dict[str, Any],
        spec: Dict[str, Any],
        *,
        nodes_expr: Any,
    ) -> Optional[Dict[str, Any]]:
        path = condition.get("path")
        operator = str(condition.get("operator") or "").upper()
        if not isinstance(path, str) or not operator:
            return None

        direct_field_expr = self._resolve_row_match_direct_field_expr(path)
        if direct_field_expr is not None:
            return self._build_direct_field_condition_expr(path, direct_field_expr, operator, condition.get("value"))

        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(path)
        except Exception:
            return None

        if path_regex_pattern is None:
            return self._build_direct_field_condition_expr(path, f"${data_path}", operator, condition.get("value"))

        variable_alias = path.split("/", 1)[0].strip()
        path_field = self.schema_config["path_field"]
        instance_field = self.schema_config.get("path_instance_field", "pi")
        conds: List[Dict[str, Any]] = [
            {
                "$regexMatch": {
                    "input": f"$$node.{path_field}",
                    "regex": path_regex_pattern,
                }
            }
        ]
        correlation_expr = self._build_candidate_correlation_expr(
            spec,
            variable_alias,
            candidate_path_expr=f"$$node.{path_field}",
            candidate_instance_expr=f"$$node.{instance_field}",
        )
        if correlation_expr is not None:
            conds.append(correlation_expr)

        if operator not in {"EXISTS", "NOT EXISTS"}:
            value_expr = self._build_value_predicate_expr(f"$$node.{data_path}", operator, condition.get("value"))
            if value_expr is None:
                return None
            conds.append(value_expr)

        filter_expr = self._combine_exprs("$and", conds)
        if filter_expr is None:
            return None

        match_count_expr = {
            "$size": {
                "$filter": {
                    "input": nodes_expr,
                    "as": "node",
                    "cond": filter_expr,
                }
            }
        }
        if operator == "NOT EXISTS":
            return {"$eq": [match_count_expr, 0]}
        return {"$gt": [match_count_expr, 0]}

    async def _build_row_where_expr(
        self,
        node: Dict[str, Any] | None,
        spec: Dict[str, Any],
        *,
        nodes_expr: Any,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict) or not node:
            return None

        operator = str(node.get("operator") or "").upper()
        if operator in {"AND", "OR"}:
            child_exprs: List[Dict[str, Any]] = []
            children = self._iter_where_children(node)
            for child in children:
                child_expr = await self._build_row_where_expr(child, spec, nodes_expr=nodes_expr)
                if child_expr is None:
                    if operator == "OR":
                        return None
                    continue
                child_exprs.append(child_expr)
            return self._combine_exprs(f"${operator.lower()}", child_exprs)

        return await self._build_leaf_row_match_expr(node, spec, nodes_expr=nodes_expr)

    async def build_row_exact_match_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self._supports_exact_row_correlation():
            return None

        spec = await self._get_row_fanout_spec(ast)
        if not spec or not self._where_has_row_sensitive_alias(ast.get("where"), spec):
            return None

        row_expr = await self._build_row_where_expr(
            ast.get("where"),
            spec,
            nodes_expr=self._full_composition_nodes_expr(),
        )
        if row_expr is None:
            return None
        return {"$match": {"$expr": row_expr}}

    async def _build_fanout_aware_projection(
        self,
        aql_path: str,
        spec: Dict[str, Any],
    ) -> Optional[Any]:
        variable = aql_path.split("/", 1)[0]
        if variable not in set(spec.get("aliases", [])):
            return None

        _path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
        selector_codes = await self.format_resolver.get_selector_codes(aql_path)
        if variable == spec["target_alias"] and not selector_codes:
            return f"$__fanout_nodes.{data_path}"

        regex_expr = self._build_fanout_regex_expr(f"$__fanout_paths.{variable}", selector_codes)
        return self._first_matching_node_value(
            self._full_composition_nodes_expr(),
            path_field=self.schema_config["path_field"],
            path_regex_pattern=regex_expr,
            data_path=data_path,
        )

    async def build_search_pipeline(self, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Constructs the full MongoDB aggregation pipeline starting with $search.
        """
        pipeline = []

        # 1. Build the $search stage from WHERE clause
        search_stage = await self.build_search_stage(ast)
        if search_stage:
            pipeline.append(search_stage)

        # 2. Build additional $match stages for conditions not handled by $search
        additional_match = await self.build_additional_match_stage(ast)
        if additional_match:
            pipeline.append(additional_match)

        # 3. Build the $lookup stage to get complete composition data
        lookup_stage = self.build_lookup_stage(ast)
        if lookup_stage:
            pipeline.append(lookup_stage)

        # 4. Fan out rows only when the deepest selected alias can repeat.
        fanout_stages = await self.build_row_fanout_stages(ast)
        if fanout_stages:
            pipeline.extend(fanout_stages)

        # 5. Re-apply supported WHERE logic at row level when exact fanout correlation is available.
        row_exact_match = await self.build_row_exact_match_stage(ast)
        if row_exact_match:
            pipeline.append(row_exact_match)

        # 6. Build the $addFields stage for LET variables
        let_stage = self.build_let_stage()
        if let_stage:
            pipeline.append(let_stage)

        # 7. Build the $project stage from the SELECT clause  
        project_stage = await self.build_project_stage(ast)
        if project_stage:
            pipeline.append(project_stage)

        # 8. Build DISTINCT stages ($group + $replaceRoot) if SELECT DISTINCT is used
        # This must come after $project so we can group by the projected field names
        projected_fields = self.get_projected_field_names(ast)
        distinct_stages = self.build_distinct_stages(ast, projected_fields)
        if distinct_stages:
            pipeline.extend(distinct_stages)
            logger.info(f"Added DISTINCT stages for fields: {projected_fields}")

        # 9. Build the $sort stage from ORDER BY clause
        sort_stage = self.build_sort_stage(ast)
        if sort_stage:
            pipeline.append(sort_stage)
        
        # 10. Build the $limit stage from LIMIT clause
        limit_stage = self.build_limit_stage(ast)
        if limit_stage:
            pipeline.append(limit_stage)
        
        return pipeline

    async def build_search_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $search stage from AQL WHERE clause using Atlas Search.
        """
        where_clause = ast.get("where")
        if not where_clause:
            # If no WHERE clause, create a basic $search to match all documents
            return {
                "$search": {
                    "index": self.search_index_name,
                    "exists": {
                        "path": "sn"
                    }
                }
            }

        # Convert WHERE clause to search query
        search_query = await self._convert_where_to_search(where_clause)
        
        if search_query:
            return {
                "$search": {
                    "index": self.search_index_name,
                    **search_query
                }
            }
        else:
            # Fallback to basic search if WHERE clause couldn't be converted
            logger.warning(f"Could not convert WHERE clause to search query, using basic exists search: {where_clause}")
            return {
                "$search": {
                    "index": self.search_index_name,
                    "exists": {
                        "path": "sn"
                    }
                }
            }

    async def _convert_where_to_search(self, where_clause: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Converts an AQL WHERE clause to Atlas Search query syntax.
        Handles both the new AQL parser format and compatibility formats.
        """
        # Handle the actual AQL parser format (flat structure)
        if "path" in where_clause and "operator" in where_clause:
            return await self._handle_direct_condition_search(where_clause)
        
        # Handle logical conditions with multiple sub-conditions
        if "operator" in where_clause and "conditions" in where_clause:
            return await self._handle_logical_conditions_search(where_clause)
        
        # Compatibility format handling (for backward compatibility)
        condition_type = where_clause.get("type")
        
        if condition_type == "comparison":
            return await self._handle_comparison_search(where_clause)
        
        elif condition_type == "logical":
            return await self._handle_logical_search(where_clause)
        
        elif condition_type == "exists":
            return await self._handle_exists_search(where_clause)
        
        elif condition_type == "matches":
            return await self._handle_matches_search(where_clause)
        
        return None

    async def _handle_direct_condition_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles direct condition format from AQL parser: {"path": "...", "operator": "...", "value": "..."}
        """
        aql_path = condition.get("path")
        operator = condition.get("operator")
        value = condition.get("value")
        
        if not aql_path or not operator:
            logger.warning(f"Invalid direct condition format: {condition}")
            return None
        
        # Translate AQL path to search collection path
        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
            # Top-level metadata fields stay at document root; node data lives under sn.
            if path_regex_pattern is None:
                search_path = data_path
            else:
                search_path = f"sn.{data_path}" if data_path else "sn.data"
        except Exception as e:
            logger.warning(f"Could not resolve path {aql_path}: {e}")
            return None
        
        # Handle special operators
        if operator == "EXISTS":
            # Use embedded document exists query for sn paths
            if search_path.startswith("sn."):
                return self._build_embedded_document_exists_query(search_path)
            else:
                return {
                    "exists": {
                        "path": search_path
                    }
                }
        
        if operator == "NOT EXISTS":
            # Use embedded document exists query wrapped in mustNot for sn paths
            if search_path.startswith("sn."):
                exists_query = self._build_embedded_document_exists_query(search_path)
                return {
                    "compound": {
                        "mustNot": [exists_query]
                    }
                }
            else:
                return {
                    "compound": {
                        "mustNot": [{
                            "exists": {
                                "path": search_path
                            }
                        }]
                    }
                }
        
        if operator == "MATCHES":
            # Handle MATCHES with array values
            if isinstance(value, dict):
                # Convert object with numeric keys back to array
                value_array = [value.get(str(i)) for i in range(len(value))]
                # Use regex with alternation for multiple values
                regex_pattern = "|".join(f"({re.escape(str(v))})" for v in value_array if v is not None)
                return {
                    "regex": {
                        "path": search_path,
                        "query": regex_pattern
                    }
                }
            else:
                return {
                    "regex": {
                        "path": search_path,
                        "query": str(value)
                    }
                }
        
        if operator == "LIKE":
            # Convert SQL LIKE to regex
            regex_pattern = str(value).replace('%', '.*').replace('_', '.')
            return {
                "regex": {
                    "path": search_path,
                    "query": regex_pattern
                }
            }
        
        # Handle comparison operators
        if operator == "=":
            if search_path.startswith("sn."):
                return self._build_embedded_document_equals_query(search_path, value)
            else:
                exact_metadata_fields = {
                    self.search_config.get("ehr_id"),
                    self.search_config.get("comp_id"),
                    self.search_config.get("template_id"),
                    self.search_config.get("sort_time"),
                }
                if search_path in exact_metadata_fields:
                    coerced_value = value
                    if search_path == self.search_config.get("ehr_id"):
                        coerced_value = self.value_formatter.format_id_value(
                            self.value_formatter.format_value(value),
                            self.schema_config.get("ehr_id_encoding", "string"),
                        )
                    elif search_path == self.search_config.get("comp_id"):
                        coerced_value = self.value_formatter.format_id_value(
                            self.value_formatter.format_value(value),
                            self.schema_config.get("composition_id_encoding", "string"),
                        )
                    elif search_path == self.search_config.get("sort_time"):
                        coerced_value = self.value_formatter.format_value(value)
                    return {
                        "equals": {
                            "path": search_path,
                            "value": coerced_value
                        }
                    }
                if isinstance(value, str):
                    return {
                        "text": {
                            "query": value,
                            "path": search_path
                        }
                    }
                else:
                    return {
                        "equals": {
                            "path": search_path,
                            "value": value
                        }
                    }

        elif operator in [">=", ">", "<=", "<"]:
            # For date/time values, ensure proper formatting and use embedded document queries
            formatted_value = self._format_date_value_for_search(value)
            
            # Check if this is an embedded document query (sn array)
            if search_path.startswith("sn."):
                return self._build_embedded_document_range_query(search_path, operator, formatted_value)
            else:
                # Non-embedded document range query (fallback)
                range_condition = {}
                if operator == ">=":
                    range_condition["gte"] = formatted_value
                elif operator == ">":
                    range_condition["gt"] = formatted_value
                elif operator == "<=":
                    range_condition["lte"] = formatted_value
                elif operator == "<":
                    range_condition["lt"] = formatted_value
                    
                return {
                    "range": {
                        "path": search_path,
                        **range_condition
                    }
                }

        elif operator in ["!=", "<>"]:
            return {
                "compound": {
                    "mustNot": [{
                        "equals": {
                            "path": search_path,
                            "value": value
                        }
                    }]
                }
            }
        
        logger.warning(f"Unsupported operator in search condition: {operator}")
        return None

    async def _handle_logical_conditions_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles logical conditions with multiple sub-conditions from AQL parser.
        Format: {"operator": "AND/OR", "conditions": {"0": {...}, "1": {...}}}
        """
        operator = condition.get("operator")
        conditions = condition.get("conditions", {})
        
        if not operator or not conditions:
            logger.warning(f"Invalid logical condition format: {condition}")
            return None
        
        # Recursively convert child conditions
        search_conditions = []
        for key, child_condition in conditions.items():
            if isinstance(child_condition, dict):
                child_search = await self._convert_where_to_search(child_condition)
                if child_search:
                    search_conditions.append(child_search)
        
        if not search_conditions:
            logger.warning(f"No valid search conditions found in logical condition")
            return None
            
        if len(search_conditions) == 1:
            return search_conditions[0]
            
        if operator == "AND":
            return {
                "compound": {
                    "must": search_conditions
                }
            }
        elif operator == "OR":
            return {
                "compound": {
                    "should": search_conditions
                }
            }
        
        logger.warning(f"Unsupported logical operator: {operator}")
        return None

    async def _handle_comparison_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles comparison operators for Atlas Search.
        """
        left = condition.get("left", {})
        operator = condition.get("operator")
        right = condition.get("right", {})
        
        # Extract path from left side
        if left.get("type") != "dataMatchPath":
            return None
            
        # Extract AQL path and use translate_aql_path
        aql_path = self._extract_aql_path_from_path_object(left)
        if not aql_path:
            return None
            
        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
            # For search operations, we use the data path directly in the sn.data structure
            search_path = f"sn.{data_path}" if data_path else "sn.data"
        except Exception as e:
            logger.warning(f"Could not resolve path {aql_path}: {e}")
            return None
            
        value = self.value_formatter.format_literal_value(right)
        
        # Map AQL operators to Atlas Search
        if operator == "=":
            if isinstance(value, str):
                return {
                    "text": {
                        "query": value,
                        "path": search_path
                    }
                }
            else:
                return {
                    "equals": {
                        "path": search_path,
                        "value": value
                    }
                }
        
        elif operator in [">=", ">", "<=", "<"]:
            range_condition = {}
            if operator == ">=":
                range_condition["gte"] = value
            elif operator == ">":
                range_condition["gt"] = value
            elif operator == "<=":
                range_condition["lte"] = value
            elif operator == "<":
                range_condition["lt"] = value
                
            return {
                "range": {
                    "path": search_path,
                    **range_condition
                }
            }
        
        elif operator == "!=":
            return {
                "compound": {
                    "mustNot": [{
                        "equals": {
                            "path": search_path,
                            "value": value
                        }
                    }]
                }
            }
        
        return None

    async def _handle_logical_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles logical operators (AND, OR) for Atlas Search.
        """
        operator = condition.get("operator")
        conditions = condition.get("conditions", [])
        
        if not conditions:
            return None
            
        # Recursively convert child conditions
        search_conditions = []
        for child_condition in conditions:
            child_search = await self._convert_where_to_search(child_condition)
            if child_search:
                search_conditions.append(child_search)
        
        if not search_conditions:
            return None
            
        if len(search_conditions) == 1:
            return search_conditions[0]
            
        if operator == "AND":
            return {
                "compound": {
                    "must": search_conditions
                }
            }
        elif operator == "OR":
            return {
                "compound": {
                    "should": search_conditions,
                    "minimumShouldMatch": 1
                }
            }
        
        return None

    async def _handle_exists_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles EXISTS conditions for Atlas Search.
        """
        path = condition.get("path", {})
        
        if path.get("type") != "dataMatchPath":
            return None
            
        # Extract AQL path and use translate_aql_path
        aql_path = self._extract_aql_path_from_path_object(path)
        if not aql_path:
            return None
            
        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
            # For search operations, we use the data path directly in the sn.data structure
            search_path = f"sn.{data_path}" if data_path else "sn.data"
        except Exception as e:
            logger.warning(f"Could not resolve path {aql_path}: {e}")
            return None
        
        return {
            "exists": {
                "path": search_path
            }
        }

    async def _handle_matches_search(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handles MATCHES conditions (regex) for Atlas Search.
        """
        path = condition.get("path", {})
        pattern = condition.get("pattern")
        
        if path.get("type") != "dataMatchPath" or not pattern:
            return None
            
        # Extract AQL path and use translate_aql_path
        aql_path = self._extract_aql_path_from_path_object(path)
        if not aql_path:
            return None
            
        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
            # For search operations, we use the data path directly in the sn.data structure
            search_path = f"sn.{data_path}" if data_path else "sn.data"
        except Exception as e:
            logger.warning(f"Could not resolve path {aql_path}: {e}")
            return None
        
        return {
            "regex": {
                "path": search_path,
                "query": pattern
            }
        }

    async def build_additional_match_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Builds additional $match stage for conditions that cannot be handled by $search.
        For example, EHR-level conditions or complex nested conditions.
        """
        contains_clause = ast.get("contains")
        where_clause = ast.get("where")
        additional_conditions = {}
        
        # Process CONTAINS clause for structural filtering
        skip_root_composition_match = self._where_has_template_constraint(where_clause)
        if contains_clause:
            contains_conditions = await self._process_contains_clause(
                contains_clause,
                nested_only=skip_root_composition_match,
            )
            if contains_conditions:
                additional_conditions.update(contains_conditions)
        
        return {"$match": additional_conditions} if additional_conditions else None

    def _where_has_template_constraint(self, where_clause: Dict[str, Any] | None) -> bool:
        if not isinstance(where_clause, dict):
            return False

        path = where_clause.get("path")
        if isinstance(path, str) and path.endswith("/archetype_details/template_id/value"):
            return True

        conditions = where_clause.get("conditions")
        if isinstance(conditions, dict):
            return any(self._where_has_template_constraint(child) for child in conditions.values() if isinstance(child, dict))
        if isinstance(conditions, list):
            return any(self._where_has_template_constraint(child) for child in conditions if isinstance(child, dict))
        return False

    async def _process_contains_clause(
        self,
        contains_clause: Dict[str, Any],
        *,
        nested_only: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Processes CONTAINS clause for the search collection structure.
        """
        if not contains_clause:
            return None

        shortened_condition = await build_shortened_contains_condition(
            contains_clause,
            self.format_resolver.archetype_resolver,
            path_field=self.search_config.get("path_field", "p"),
            data_field=self.search_config.get("data_field", "data"),
            separator=self.search_config.get("separator", ":"),
            nested_only=nested_only,
        )
        if shortened_condition:
            return {
                self.search_config["composition_array"]: shortened_condition
            }

        if nested_only:
            return None

        rmType = contains_clause.get("rmType")
        predicate = contains_clause.get("predicate")
        if rmType != "COMPOSITION" and contains_clause.get("contains"):
            return await self._process_contains_clause(contains_clause.get("contains"))

        # Handle COMPOSITION level filtering
        if rmType == "COMPOSITION" and predicate:
            conditions = {}
            predicate_path = predicate.get("path")
            archetype_id = predicate.get("value")

            if predicate_path == "archetype_node_id" and archetype_id:
                conditions[self.search_config["composition_array"]] = {
                    "$elemMatch": {
                        self.search_config.get("path_field", "p"): {"$regex": self._root_path_regex()}
                    }
                }

            return conditions

        return None

    def build_lookup_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Builds $lookup stage to get complete composition data from flatten_compositions.
        This enables hybrid data access: search index for filtering, full data for projection.
        """
        # Check if we need full composition data based on SELECT clause
        if self._needs_full_composition_data(ast):
            logger.info(f"Adding $lookup stage to get complete composition data from {self.full_compositions_collection}")
            return {
                "$lookup": {
                    "from": self.full_compositions_collection,
                    "localField": "_id",
                    "foreignField": self.schema_config.get("comp_id", "comp_id"),
                    "as": "full_composition"
                }
            }
        else:
            logger.debug("No metadata fields detected - skipping $lookup stage for optimal performance")
        return None

    def _needs_full_composition_data(self, ast: Dict[str, Any]) -> bool:
        """
        Determines if the query needs full composition data based on SELECT clause.
        Returns True if any selected field requires data not available in search collection.
        """
        if find_deepest_referenced_alias(
            ast,
            ast.get("contains"),
            composition_alias=self.composition_alias,
        ):
            return True

        columns = ast.get("select", {}).get("columns", {})
        if not columns:
            return False
        
        for col_data in columns.values():
            path = col_data.get("path") or col_data.get("value", {})
            if path.get("type") == "dataMatchPath":
                aql_path = self._extract_aql_path_from_path_object(path)
                if aql_path and (self._is_composition_metadata_path(aql_path) or self._is_archetype_specific_path(aql_path)):
                    return True
        return False

    def _is_composition_metadata_path(self, aql_path: str) -> bool:
        """
        Determines if an AQL path refers to composition metadata that might not be
        available in the search collection's reduced sn array.
        """
        if not aql_path:
            return False
            
        # Composition metadata patterns that are often missing from search collection
        metadata_patterns = [
            '/name/value',
            '/name/defining_code',
            '/context/start_time',
            '/context/end_time', 
            '/context/setting',
            '/context/location',
            '/context/health_care_facility',
            '/context/participations',
            '/composer',
            '/category',
            '/territory',
            '/language'
        ]
        
        # Check if path contains any metadata patterns
        for pattern in metadata_patterns:
            if pattern in aql_path:
                return True
                
        # Also check for root composition elements (usually have simple path patterns)
        variable = aql_path.split('/')[0]
        if variable == self.composition_alias:
            # If it's a composition path but not uid/value (which we handle specially)
            if not aql_path.endswith('/uid/value'):
                # Check if it's likely a root composition field
                path_parts = aql_path.split('/')
                if len(path_parts) <= 3:  # e.g., c/name/value
                    return True
        
        return False

    def _is_archetype_specific_path(self, aql_path: str) -> bool:
        """
        Determines if an AQL path contains archetype-specific elements (AT codes)
        that might not be indexed in the search collection.
        
        This handles cases like:
        - med_ac/description[at0017]/items[at0020]/value/defining_code/code_string
        - action_var/activities[at0001]/description[at0002]/items[at0003]/value
        
        Returns True if the path contains AT codes, indicating it's archetype-specific
        and might need full composition data for resolution.
        """
        if not aql_path:
            return False
        
        # Check if path contains AT code references in square brackets
        # AT codes follow pattern: at followed by digits (e.g., at0001, at0017, at0020)
        import re
        at_code_pattern = r'\[at\d+\]'
        
        if re.search(at_code_pattern, aql_path):
            logger.debug(f"Detected archetype-specific path with AT codes: {aql_path}")
            return True
        
        # Additionally check for paths that go through archetype elements
        # These are typically deeper than simple composition metadata
        variable = aql_path.split('/')[0] 
        
        # Skip composition alias paths (already handled by metadata check)
        if variable == self.composition_alias:
            return False
            
        # Check if this is a deep path through archetype structures
        # Archetype paths typically have more segments and contain structural elements
        path_parts = aql_path.split('/')
        if len(path_parts) > 3:  # Deep paths are more likely to be archetype-specific
            # Look for common archetype structural elements
            archetype_elements = [
                'description', 'items', 'activities', 'activity', 'data', 'state', 
                'protocol', 'events', 'event', 'value', 'values', 'items_single',
                'items_multiple', 'other_context'
            ]
            
            # If the path contains archetype structural elements, it's likely archetype-specific
            for part in path_parts[1:]:  # Skip variable name
                # Remove any bracket notation to get the element name
                element_name = re.sub(r'\[.*?\]', '', part)
                if element_name in archetype_elements:
                    logger.debug(f"Detected deep archetype path with structural elements: {aql_path}")
                    return True
        
        return False

    async def _build_hybrid_field_projection(self, aql_path: str, alias: str) -> Dict[str, Any]:
        """
        Builds projection logic that intelligently routes field access between 
        search collection (sn array) and full composition data (cn array from lookup).
        """
        try:
            path_regex_pattern, data_path = await self.format_resolver.translate_aql_path(aql_path)
            
            # Determine if this is likely composition metadata or archetype-specific
            is_metadata = self._is_composition_metadata_path(aql_path)
            is_archetype_specific = self._is_archetype_specific_path(aql_path)
            
            # Prefer full composition data for metadata or archetype-specific paths
            prefer_full_composition = is_metadata or is_archetype_specific
            
            if prefer_full_composition:
                logger.debug(f"Using full-composition-priority projection for {aql_path} (metadata: {is_metadata}, archetype: {is_archetype_specific})")
            else:
                logger.debug(f"Using search-collection-priority projection for {aql_path}")
            
            if path_regex_pattern is None:
                # Direct field access
                if data_path == "comp_id":
                    return "$comp_id"
                else:
                    return f"${data_path}"
            else:
                search_value = self._first_matching_node_value(
                    {"$ifNull": [f"${self.search_config['composition_array']}", []]},
                    path_field=self.search_config["path_field"],
                    path_regex_pattern=path_regex_pattern,
                    data_path=data_path,
                )
                full_value = self._first_matching_node_value(
                    self._full_composition_nodes_expr(),
                    path_field=self.schema_config["path_field"],
                    path_regex_pattern=path_regex_pattern,
                    data_path=data_path,
                )

                # Complex path - use hybrid logic
                if prefer_full_composition:
                    return {"$ifNull": [full_value, search_value]}
                else:
                    return {"$ifNull": [search_value, full_value]}
        except Exception as e:
            logger.warning(f"Could not resolve path {aql_path}: {e}")
            return f"${alias}"

    def build_let_stage(self) -> Optional[Dict[str, Any]]:
        """
        Constructs the $addFields stage for LET variables.
        """
        if not self.let_variables:
            return None
        
        add_fields = {}
        
        for var_name, expression in self.let_variables.items():
            # Resolve the expression to MongoDB syntax
            resolved_expr = self._resolve_expression(expression)
            add_fields[var_name] = resolved_expr
        
        return {"$addFields": add_fields}

    async def build_project_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $project stage from the SELECT clause for search collection.
        """
        columns = ast.get("select", {}).get("columns", {})
        if not columns:
            return None

        fanout_spec = await self._get_row_fanout_spec(ast)
        projection = {"_id": 0}
        
        for col_data in columns.values():
            alias = col_data.get("alias")
            # The path is actually in the 'value' field based on AST structure
            path = col_data.get("path") or col_data.get("value", {})
            
            # Generate alias if not provided
            if not alias:
                if path.get("type") == "dataMatchPath":
                    aql_path = path.get("path")
                    if aql_path:
                        # Generate alias from path: c/uid/value -> c_uid_value
                        alias = aql_path.replace("/", "_")
                    else:
                        continue
                else:
                    continue
            
            if path.get("type") == "dataMatchPath":
                # Extract AQL path from the path object
                aql_path = self._extract_aql_path_from_path_object(path)
                
                if aql_path:
                    variable = aql_path.split('/')[0]
                    
                    # Handle EHR-level paths
                    if variable == self.ehr_alias:
                        if 'ehr_id' in aql_path:
                            projection[alias] = "$ehr_id"
                        else:
                            projection[alias] = f"${alias}"
                    
                    # Handle composition-level paths with hybrid data routing
                    elif variable == self.composition_alias:
                        # Special handling for composition UID - it's stored in document _id
                        if '/uid/value' in aql_path or aql_path.endswith('/uid/value'):
                            projection[alias] = "$_id"
                        else:
                            # Route field to appropriate data source
                            projection[alias] = await self._build_hybrid_field_projection(aql_path, alias)
                    else:
                        fanout_projection = None
                        if fanout_spec:
                            fanout_projection = await self._build_fanout_aware_projection(
                                aql_path,
                                fanout_spec,
                            )
                        projection[alias] = fanout_projection or await self._build_hybrid_field_projection(aql_path, alias)
                else:
                    # Fallback to alias if path cannot be extracted
                    projection[alias] = f"${alias}"
            
            elif path.get("type") == "literal":
                projection[alias] = self.value_formatter.format_literal_value(path)
            
            else:
                # Default case
                projection[alias] = f"${alias}"
        
        return {"$project": projection}

    def build_sort_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $sort stage from the ORDER BY clause.
        """
        order_by = ast.get("orderBy", {})
        columns = order_by.get("columns") if isinstance(order_by, dict) and isinstance(order_by.get("columns"), dict) else order_by

        if not isinstance(columns, dict) or not columns:
            return None
            
        sort_spec = {}
        projected_aliases = {
            col.get("value", {}).get("path"): col.get("alias")
            for col in ast.get("select", {}).get("columns", {}).values()
            if isinstance(col, dict) and isinstance(col.get("value"), dict) and col.get("alias")
        }
        
        for col_data in columns.values():
            alias = col_data.get("alias")
            aql_path = col_data.get("path")
            direction = col_data.get("direction", "ASC")
            
            field_name = alias
            if not field_name and aql_path:
                field_name = projected_aliases.get(aql_path) or self.format_resolver.resolve_document_field(aql_path)
            
            if field_name:
                sort_spec[field_name] = 1 if direction.upper() == "ASC" else -1
            
        return {"$sort": sort_spec} if sort_spec else None

    def build_limit_stage(self, ast: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Constructs the $limit stage from the LIMIT clause.
        """
        limit_value = ast.get("limit")
        
        if limit_value is None:
            return None
            
        try:
            limit_int = int(limit_value)
            if limit_int > 0:
                return {"$limit": limit_int}
        except (ValueError, TypeError):
            pass
            
        return None

    def build_distinct_stages(self, ast: Dict[str, Any], projected_fields: List[str]) -> List[Dict[str, Any]]:
        """
        Constructs the $group and $replaceRoot stages for DISTINCT queries.
        
        DISTINCT in AQL removes duplicate rows from the result set. In MongoDB,
        this is implemented using:
        1. $group stage: Groups documents by all projected fields (compound _id)
        2. $replaceRoot stage: Flattens the grouped _id back to a normal document
        
        Args:
            ast: The parsed AQL AST
            projected_fields: List of field names that are being projected (output columns)
            
        Returns:
            List[Dict[str, Any]]: List containing $group and $replaceRoot stages,
                                  or empty list if DISTINCT is not requested
        """
        select_clause = ast.get("select", {})
        is_distinct = select_clause.get("distinct", False)
        
        if not is_distinct:
            return []
        
        if not projected_fields:
            return []
        
        # Build the compound _id for $group using all projected fields
        # This ensures we group by all columns to find unique combinations
        group_id = {}
        for field in projected_fields:
            if field != "_id":  # Skip _id as it's handled separately
                # Use the field name as key and reference the projected field value
                group_id[field] = f"${field}"
        
        if not group_id:
            return []
        
        # Build the $group stage
        group_stage = {
            "$group": {
                "_id": group_id
            }
        }
        
        # Build the $replaceRoot stage to flatten the _id back to document fields
        # This converts {_id: {field1: val1, field2: val2}} to {field1: val1, field2: val2}
        replace_root_stage = {
            "$replaceRoot": {
                "newRoot": "$_id"
            }
        }
        
        return [group_stage, replace_root_stage]

    def get_projected_field_names(self, ast: Dict[str, Any]) -> List[str]:
        """
        Extracts the list of field names that will be in the projection output.
        Used for building DISTINCT stages.
        
        Args:
            ast: The parsed AQL AST
            
        Returns:
            List[str]: List of projected field names (aliases or generated names)
        """
        columns = ast.get("select", {}).get("columns", {})
        field_names = []
        
        for col_data in columns.values():
            alias = col_data.get("alias")
            path = col_data.get("path") or col_data.get("value", {})
            
            if alias:
                field_names.append(alias)
            elif isinstance(path, dict) and path.get("type") == "dataMatchPath":
                # Path-based column - generate name from path
                aql_path = path.get("path", "")
                if aql_path:
                    # Generate alias from path: c/uid/value -> c_uid_value
                    field_names.append(aql_path.replace("/", "_"))
        
        return field_names

    def _extract_aql_path_from_path_object(self, path_obj: Dict[str, Any]) -> Optional[str]:
        """
        Extracts the full AQL path from a path object.
        Handles both complex structure (variable + segments) and simple structure (direct path).
        """
        if path_obj.get("type") != "dataMatchPath":
            return None
        
        # First check if path is directly available (simplified AST structure)
        direct_path = path_obj.get("path")
        if direct_path:
            return direct_path
            
        # Fallback to complex structure (variable + segments)
        variable = path_obj.get("variable")
        segments = path_obj.get("segments", [])
        
        if not variable:
            return None
            
        # Construct full path: variable/segment1/segment2/...
        path_parts = [variable]
        for segment in segments:
            if isinstance(segment, dict):
                field = segment.get("field")
                if field:
                    path_parts.append(field)
            elif isinstance(segment, str):
                path_parts.append(segment)
                
        return "/".join(path_parts)

    def _resolve_expression(self, expression: Dict[str, Any]) -> Any:
        """
        Resolves an expression to its MongoDB representation for search collection.
        """
        expr_type = expression.get("type")
        
        if expr_type == "literal":
            return self.value_formatter.format_literal_value(expression)
        
        elif expr_type == "dataMatchPath":
            # For search collection context, we need to handle sn array
            # This is a simplified version - full implementation would need
            # to properly resolve paths within the sn array structure
            return f"$sn.data"
        
        # Default: return as is
        return expression

    def _format_date_value_for_search(self, value: Any) -> Any:
        """
        Formats date values for Atlas Search compatibility.
        For embedded document queries, Atlas Search expects actual datetime objects, not string representations.
        """
        if isinstance(value, str):
            # Check if it's already a date string
            if 'T' in value and ('Z' in value or '+' in value or value.endswith('00:00:00')):
                try:
                    from datetime import datetime
                    # Parse and return as actual datetime object for MongoDB
                    parsed_date = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    # Return the datetime object directly - MongoDB will handle ISODate conversion
                    return parsed_date
                except:
                    # If parsing fails, return original value
                    return value
            else:
                # Try to ensure it's a valid date string
                try:
                    from datetime import datetime
                    # Parse and return as actual datetime object
                    parsed_date = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    return parsed_date
                except:
                    # If parsing fails, return original value
                    return value
        else:
            # For non-string values (numbers, etc.), return as-is
            return value

    def _build_embedded_document_range_query(self, search_path: str, operator: str, value: Any) -> Dict[str, Any]:
        """
        Builds an embedded document range query for Atlas Search.
        Converts paths like 'sn.data.time.value' into proper embeddedDocument queries.
        
        For Atlas Search embedded documents, we need:
        - embeddedDocument.path: The context path to the embedded document level
        - operator.range.path: The full absolute path from document root
        
        Generic logic for determining embedded document context:
        - For paths ending in .value: Use parent path as embedded context
        - For direct data fields: Use sn.data as embedded context
        - Handle arbitrary nesting levels (sn.data.a.b.c.value -> sn.data.a.b.c)
        
        Args:
            search_path: The full path like 'sn.data.time.value'
            operator: The comparison operator (>, <, >=, <=)
            value: The value to compare against
            
        Returns:
            Atlas Search embeddedDocument query structure
        """
        if not search_path.startswith("sn."):
            logger.warning(f"Expected sn. prefix in search path: {search_path}")
            return None
        
        # Build the range condition
        range_condition = {}
        if operator == ">=":
            range_condition["gte"] = value
        elif operator == ">":
            range_condition["gt"] = value
        elif operator == "<=":
            range_condition["lte"] = value
        elif operator == "<":
            range_condition["lt"] = value
        
        return {
            "embeddedDocument": {
                "path": "sn",
                "operator": {
                    "range": {
                        "path": search_path,
                        **range_condition
                    }
                }
            }
        }

    def _build_embedded_document_exists_query(self, search_path: str) -> Dict[str, Any]:
        """
        Builds an embedded document exists query for Atlas Search.
        Uses the same intelligent path resolution logic as range queries.
        """
        if not search_path.startswith("sn."):
            return {
                "exists": {
                    "path": search_path
                }
            }
        
        return {
            "embeddedDocument": {
                "path": "sn",
                "operator": {
                    "exists": {
                        "path": search_path
                    }
                }
            }
        }

    def _build_embedded_document_equals_query(self, search_path: str, value: Any) -> Dict[str, Any]:
        """
        Builds an embedded document equals query for Atlas Search.
        Uses the same intelligent path resolution logic as range queries.
        """
        exact_token_suffixes = (".cs", ".id", ".units")
        prefer_exact = isinstance(value, str) and search_path.endswith(exact_token_suffixes)

        if not search_path.startswith("sn."):
            if isinstance(value, str) and not prefer_exact:
                return {
                    "text": {
                        "query": value,
                        "path": search_path
                    }
                }
            else:
                return {
                    "equals": {
                        "path": search_path,
                        "value": value
                    }
                }
        
        # Determine the operator query based on value type
        if isinstance(value, str) and not prefer_exact:
            operator_query = {
                "text": {
                    "query": value,
                    "path": search_path  # Full absolute path
                }
            }
        else:
            operator_query = {
                "equals": {
                    "path": search_path,  # Full absolute path
                    "value": value
                }
            }
        
        return {
            "embeddedDocument": {
                "path": "sn",
                "operator": operator_query
            }
        }
