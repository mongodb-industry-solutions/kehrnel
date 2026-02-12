"""Path resolver using strategy config (field names) with slim search support."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from kehrnel.strategies.openehr.rps_dual.services.codes_service import atcode_to_token, archetype_to_token
from kehrnel.strategies.openehr.rps_dual.services.shortcuts_service import canonical_to_slim
from kehrnel.strategies.openehr.rps_dual.ingest.encoding import PathCodec


@dataclass
class ResolvedPath:
    cn_regex: str
    sn_wildcard: str
    cn_data_path: str
    sn_data_path: str
    grouping_key: str


class PathResolver:
    def __init__(self, cfg: Dict, shortcuts: Dict[str, str] | None = None):
        fields = cfg.get("fields", {}) if isinstance(cfg, dict) else {}
        comp = fields.get("composition", {})
        search = fields.get("search", {})
        self.comp = comp
        self.search = search
        self.search_path = search.get("nodes", "sn")
        self.comp_nodes = comp.get("nodes", "cn")
        self.search_nodes = search.get("nodes", "sn")
        self.token_joiner = cfg.get("node_representation", {}).get("path", {}).get("token_joiner", ".")
        self.shortcuts = shortcuts or {}
        path_sep = (cfg.get("paths") or {}).get("separator", ".")
        ar_codes = (cfg.get("dict_cache") or {}).get("codes") or {}
        at_codes = (cfg.get("dict_cache") or {}).get("at") or {}
        self.path_codec = PathCodec(ar_codes=ar_codes, at_codes=at_codes, separator=path_sep, shortcuts=self.shortcuts)

    def resolve(self, path: str, scope: str = "patient") -> str:
        return self._resolve_comp(path) if scope == "patient" else self._resolve_search(path)

    def _resolve_comp(self, path: str) -> str:
        if path == "ehr_id":
            return self.comp.get("ehr_id", "ehr_id")
        if path == "comp_id":
            return self.comp.get("comp_id", "_id")
        if path == "path":
            return self.comp.get("path", "p")
        return self.comp.get("nodes", "cn")

    def _resolve_search(self, path: str) -> str:
        if path == "ehr_id":
            return self.search.get("ehr_id", "ehr_id")
        if path == "comp_id":
            return self.search.get("comp_id", "cid")
        if path == "path":
            return self.search.get("path", "p")
        return self.search.get("nodes", "sn")

    def resolve_full(self, path: str, scope: str = "patient") -> ResolvedPath:
        tokens = self._tokenize_path(path)
        reversed_tokens = list(reversed(tokens))
        cn_regex = self._cn_regex(reversed_tokens)
        sn_wildcard = self._sn_wildcard(reversed_tokens)
        canonical_data = f"data.{'.'.join(tokens[1:])}" if len(tokens) > 1 else "data"
        slim = canonical_to_slim(".".join(tokens[1:]), self.shortcuts)
        sn_data = f"data.{slim}" if slim else "data"
        grouping_key = "/".join(tokens)
        return ResolvedPath(
            cn_regex=cn_regex,
            sn_wildcard=sn_wildcard,
            cn_data_path=canonical_data,
            sn_data_path=sn_data,
            grouping_key=grouping_key,
        )

    def resolve_data_path(self, path: str, scope: str, prefer_search: bool = False) -> str:
        resolved = self.resolve_full(path, scope)
        return resolved.sn_data_path if scope == "cross_patient" and prefer_search else resolved.cn_data_path

    def path_regex(self, aql_path: str) -> str:
        return self.resolve_full(aql_path, "patient").cn_regex

    def _tokenize_path(self, path: str) -> List[str]:
        # Basic tokenization with archetype/atcode token fallback
        parts = [p for p in path.replace("[", ".").replace("]", "").split("/") if p]
        tokens: List[str] = []
        for p in parts:
            if p.startswith("at"):
                tokens.append(str(self.path_codec._selector_to_code(p)))
            elif "-" in p and p.startswith("openehr"):
                tokens.append(str(self.path_codec._selector_to_code(p)))
            else:
                tokens.append(p)
        return tokens

    def _cn_regex(self, reversed_tokens: List[str]) -> str:
        # ^t1(?:\.[^.]+)*\.t2(?:\.[^.]+)*$
        escaped_sep = self.token_joiner.replace(".", "\\.")
        pattern_parts = [f"{t}(?:{escaped_sep}[^${escaped_sep}]+)*" for t in reversed_tokens]
        return "^" + "\\.".join(pattern_parts) + "$"

    def _sn_wildcard(self, reversed_tokens: List[str]) -> str:
        return "*".join(reversed_tokens)
