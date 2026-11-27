"""
Load FHIR SearchParameter definitions from prebuilt indexes or schema/hl7.fhir.*.search/.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from fhir_search_to_mql.schema.paths import indexes_dir, search_package_dir
from fhir_search_to_mql.schema.schema_lib import is_domain_resource

_RESOURCE_COMMON_CODES = frozenset({"_id", "_lastUpdated"})


@dataclass(frozen=True)
class SearchParamRow:
    code: str
    type: str
    expression: str
    base: List[str]
    target: List[str]
    processing_mode: str
    description: str
    id: str
    source: str

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "code": self.code,
            "type": self.type,
            "expression": self.expression,
            "base": self.base,
            "source": self.source,
        }
        if self.target:
            d["target"] = self.target
        if self.processing_mode:
            d["processingMode"] = self.processing_mode
        if self.description:
            d["description"] = self.description[:200]
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SearchParamRow:
        return cls(
            code=str(d.get("code") or ""),
            type=str(d.get("type") or ""),
            expression=str(d.get("expression") or ""),
            base=list(d.get("base") or []),
            target=list(d.get("target") or []),
            processing_mode=str(d.get("processingMode") or ""),
            description=str(d.get("description") or ""),
            id=str(d.get("id") or d.get("code") or ""),
            source=str(d.get("source") or "resource"),
        )


def search_parameters_index_path(version: str) -> Path:
    suffix = "r5" if version.upper().startswith("R5") else "r6"
    return indexes_dir() / f"search-parameters.{suffix}.json"


def shipped_search_parameters_path(version: str = "R5") -> Path:
    suffix = "r5" if version.upper().startswith("R5") else "r6"
    return indexes_dir() / f"search-parameters-shipped.{suffix}.json"


def package_dir_for(version: str) -> Path:
    path = search_package_dir(version)
    if not path.is_dir():
        raise FileNotFoundError(
            f"HL7 search package not found: {path}\n"
            "Expected schema/hl7.fhir.<r5|r6>.search/package/ — run build_indexes."
        )
    return path


@lru_cache(maxsize=4)
def _load_search_index(version: str) -> Optional[Dict[str, Any]]:
    path = search_parameters_index_path(version)
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_sp_file(path: Path) -> Optional[SearchParamRow]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if data.get("resourceType") != "SearchParameter":
        return None
    return SearchParamRow(
        code=str(data.get("code") or ""),
        type=str(data.get("type") or ""),
        expression=str(data.get("expression") or ""),
        base=list(data.get("base") or []),
        target=list(data.get("target") or []),
        processing_mode=str(data.get("processingMode") or ""),
        description=str(data.get("description") or "").replace("\r\n", " ").strip(),
        id=str(data.get("id") or path.stem),
        source="",
    )


@lru_cache(maxsize=2)
def _load_all_rows_from_package(version: str) -> List[SearchParamRow]:
    pkg = package_dir_for(version)
    rows: List[SearchParamRow] = []
    for path in sorted(pkg.glob("SearchParameter-*.json")):
        row = _parse_sp_file(path)
        if row and row.code:
            rows.append(row)
    return rows


def _rows_from_index(
    resource: str,
    version: str,
    *,
    include_resource_common: bool,
    include_domain_resource: bool,
    include_special: bool,
) -> Optional[List[SearchParamRow]]:
    data = _load_search_index(version)
    if not data:
        return None
    raw_list = (data.get("resources") or {}).get(resource)
    if raw_list is None:
        return None

    rows = [SearchParamRow.from_dict(d) for d in raw_list]
    out: List[SearchParamRow] = []
    for row in rows:
        if not include_special and row.type == "special":
            continue
        if row.source == "Resource" and not include_resource_common:
            continue
        if row.source == "DomainResource" and not include_domain_resource:
            continue
        out.append(row)
    return out


def search_parameters_for_resource(
    resource: str,
    version: str = "R5",
    *,
    include_resource_common: bool = True,
    include_domain_resource: bool = True,
    include_special: bool = False,
) -> List[SearchParamRow]:
    resource = resource.strip()
    if not resource:
        raise ValueError("resource name required")

    indexed = _rows_from_index(
        resource,
        version,
        include_resource_common=include_resource_common,
        include_domain_resource=include_domain_resource,
        include_special=include_special,
    )
    if indexed is not None:
        return indexed

    return _search_parameters_live(
        resource,
        version,
        include_resource_common=include_resource_common,
        include_domain_resource=include_domain_resource,
        include_special=include_special,
    )


def _search_parameters_live(
    resource: str,
    version: str,
    *,
    include_resource_common: bool,
    include_domain_resource: bool,
    include_special: bool,
) -> List[SearchParamRow]:
    specific: List[SearchParamRow] = []
    resource_level: List[SearchParamRow] = []
    domain_level: List[SearchParamRow] = []

    for row in _load_all_rows_from_package(version):
        if not include_special and row.type == "special":
            continue
        bases = row.base
        if resource in bases:
            specific.append(SearchParamRow(**{**row.__dict__, "source": "resource"}))
        elif include_resource_common and "Resource" in bases:
            if row.code in _RESOURCE_COMMON_CODES:
                resource_level.append(
                    SearchParamRow(**{**row.__dict__, "source": "Resource"})
                )
        elif (
            include_domain_resource
            and "DomainResource" in bases
            and is_domain_resource(resource, version)
        ):
            if include_special or row.code not in ("_text",):
                domain_level.append(
                    SearchParamRow(**{**row.__dict__, "source": "DomainResource"})
                )

    by_code: Dict[str, SearchParamRow] = {}
    for group in (resource_level, domain_level, specific):
        for row in group:
            by_code[row.code] = row

    def sort_key(r: SearchParamRow) -> tuple:
        order = {"Resource": 0, "DomainResource": 1, "resource": 2}
        return (order.get(r.source, 9), r.code)

    return sorted(by_code.values(), key=sort_key)


def expression_denorm_hint(expression: str) -> str:
    if not expression:
        return ""
    if "." in expression:
        tail = expression.split(".", 1)[1]
        tail = tail.split(".where", 1)[0].split("[", 1)[0]
        return tail
    return expression


def build_search_parameters_index(version: str = "R5") -> Dict[str, Any]:
    by_resource: Dict[str, Dict[str, SearchParamRow]] = {}

    for row in _load_all_rows_from_package(version):
        if row.type == "special":
            continue
        for base in row.base:
            if base in ("Resource", "DomainResource"):
                continue
            bucket = by_resource.setdefault(base, {})
            bucket[row.code] = SearchParamRow(**{**row.__dict__, "source": "resource"})

    common = _search_parameters_live(
        "Patient",
        version,
        include_resource_common=True,
        include_domain_resource=False,
        include_special=False,
    )
    common_codes = {r.code: r for r in common if r.source == "Resource"}

    resources: Dict[str, List[Dict[str, Any]]] = {}
    for res, codes in sorted(by_resource.items()):
        merged = dict(common_codes)
        merged.update(codes)
        resources[res] = [merged[c].to_dict() for c in sorted(merged)]

    suffix = "r5" if version.upper().startswith("R5") else "r6"
    return {
        "fhir_version": version.upper(),
        "source": str(search_package_dir(version)),
        "resource_count": len(resources),
        "resources": resources,
    }
