"""
Shared dataclasses / enums used by generator, validator and API layers.
Nothing in here should import an external dependency.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(slots=True)
class ValidationIssue:
    """
    Represents one validation finding.

    Attributes
    ----------
    path
        JSON-style path that pinpoints the offending element.
    message
        Human-readable explanation of the problem.
    severity
        ERROR / WARNING / INFO
    code
        Stable short identifier (e.g. ``"VAL_RANGE"``) for machines.
    expected, found
        Optional extra context.
    """
    path: str
    message: str
    severity: Severity = Severity.ERROR
    code: Optional[str] = None
    expected: Optional[Any] = None
    found: Optional[Any] = None