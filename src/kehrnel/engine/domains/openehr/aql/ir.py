from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

ALLOWED_OPS = {"eq", "=", "ne", "lt", "gt", "lte", "gte", "<=", ">=", "in", "contains"}
ALLOWED_SCOPE = {"patient", "cross_patient", "unknown"}


@dataclass
class SelectExpr:
    path: str
    alias: str
    agg: str = "first"  # first | none

    def __post_init__(self):
        if self.agg:
            self.agg = self.agg.lower()
        if self.agg not in ("first", "none"):
            raise ValueError(f"Unsupported aggregation {self.agg}")


@dataclass
class AqlPredicate:
    path: str
    op: str
    value: Any

    def __post_init__(self):
        if self.op:
            self.op = self.op.lower()
        if self.op not in ALLOWED_OPS:
            raise ValueError(f"Operator {self.op} not supported in AQL IR")


@dataclass
class AqlQueryIR:
    scope: str = "unknown"  # patient | cross_patient | unknown
    predicates: List[AqlPredicate] = field(default_factory=list)
    select: List[SelectExpr] = field(default_factory=list)
    projection: Optional[List[str]] = None
    limit: Optional[int] = None
    sort: Optional[Dict[str, int]] = None
    offset: Optional[int] = None

    def __post_init__(self):
        if self.scope not in ALLOWED_SCOPE:
            raise ValueError(f"Scope {self.scope} not supported")
        normalized_preds: List[AqlPredicate] = []
        for pred in self.predicates:
            if isinstance(pred, dict):
                normalized_preds.append(AqlPredicate(**pred))
            elif isinstance(pred, AqlPredicate):
                normalized_preds.append(pred)
            else:
                raise ValueError(f"Invalid predicate type {type(pred)}")
        self.predicates = normalized_preds
        normalized_select: List[SelectExpr] = []
        for sel in self.select:
            if isinstance(sel, dict):
                normalized_select.append(SelectExpr(**sel))
            elif isinstance(sel, SelectExpr):
                normalized_select.append(sel)
            else:
                raise ValueError(f"Invalid select type {type(sel)}")
        self.select = normalized_select

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
