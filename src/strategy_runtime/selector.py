from __future__ import annotations

from typing import Iterable, Optional

from .models import ActiveStrategy


def pick_by_capability_and_protocol(
    strategies: Iterable[ActiveStrategy],
    capability: str,
    protocol: Optional[str] = None,
) -> Optional[ActiveStrategy]:
    """
    Selection helper to support future policy-based routing (priority, health, load).
    Currently returns the first strategy that matches capability and protocol (if provided).
    """
    for strat in strategies:
        caps = {c.value if hasattr(c, "value") else c for c in strat.manifest.capabilities}
        if capability not in caps:
            continue
        if protocol and protocol not in (strat.manifest.protocols or []):
            continue
        return strat
    return None
