from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy


MANIFEST_PATH = Path(__file__).parent / "manifest.json"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST = StrategyManifest(**load_json(MANIFEST_PATH))


class RPSDualIBMStrategy(RPSDualStrategy):
    """Compatibility wrapper for environments activated as openehr.rps_dual_ibm."""

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        super().__init__(manifest)
