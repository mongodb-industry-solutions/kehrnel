from __future__ import annotations

from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Dict, List, Optional
from .manifest import StrategyManifest


@dataclass
class QueryPlan:
    engine: str
    plan: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryResult:
    engine_used: str
    rows: List[Dict[str, Any]] = field(default_factory=list)
    explain: Optional[Dict[str, Any]] = None


@dataclass
class ApplyPlan:
    artifacts: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ApplyResult:
    created: List[str] = field(default_factory=list)
    updated: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class TransformResult:
    base: Dict[str, Any] = field(default_factory=dict)
    search: Optional[Dict[str, Any]] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StrategyContext:
    environment_id: str
    config: Dict[str, Any]
    bindings: Optional[Dict[str, Any]] = None
    adapters: Optional[Dict[str, Any]] = None
    manifest: Optional[StrategyManifest] = None
    trace_id: Optional[str] = None
    logger: Any = None
    meta: Optional[Dict[str, Any]] = None
