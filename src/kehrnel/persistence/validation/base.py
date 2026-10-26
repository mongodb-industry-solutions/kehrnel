"""Protocol for CDISC CORE, Pinnacle 21, or other validation engines."""
from __future__ import annotations

from typing import Any, Dict, List, Protocol


class ValidationEngineAdapter(Protocol):
    async def validate(
        self,
        *,
        snapshot: Dict[str, Any],
        datasets: List[Dict[str, Any]],
        options: Dict[str, Any],
    ) -> Dict[str, Any]: ...
