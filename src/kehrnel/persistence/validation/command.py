"""Subprocess bridge for CDISC CORE, Pinnacle 21, and compatible validators."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List


class CommandValidationEngine:
    """Run a validator through a bounded JSON envelope protocol.

    ``argv`` must contain ``{input}`` and may contain ``{output}``. The input
    file contains the snapshot, canonical datasets, and caller options. The
    adapter reads a JSON result from the output file or stdout. A shell is never
    used, so arguments cannot be interpreted as shell syntax.
    """

    def __init__(
        self,
        argv: List[str],
        *,
        timeout_seconds: int = 900,
        max_output_bytes: int = 20_000_000,
        environment: Dict[str, str] | None = None,
    ):
        if not argv or not all(isinstance(item, str) and item for item in argv):
            raise ValueError("validation command argv must be a non-empty string array")
        if not any("{input}" in item for item in argv):
            raise ValueError("validation command argv must include {input}")
        self.argv = list(argv)
        self.timeout_seconds = int(timeout_seconds)
        self.max_output_bytes = int(max_output_bytes)
        self.environment = dict(environment or {})

    async def validate(
        self,
        *,
        snapshot: Dict[str, Any],
        datasets: List[Dict[str, Any]],
        options: Dict[str, Any],
    ) -> Dict[str, Any]:
        envelope = {"protocol": "kehrnel-validation/v1", "snapshot": snapshot, "datasets": datasets, "options": options}

        def execute() -> Dict[str, Any]:
            with tempfile.TemporaryDirectory(prefix="kehrnel-validation-") as directory:
                root = Path(directory)
                input_path = root / "input.json"
                output_path = root / "output.json"
                input_path.write_text(json.dumps(envelope, ensure_ascii=False, default=str), encoding="utf-8")
                argv = [item.replace("{input}", str(input_path)).replace("{output}", str(output_path)) for item in self.argv]
                env = {**os.environ, **self.environment}
                result = subprocess.run(
                    argv,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=self.timeout_seconds,
                    check=False,
                    env=env,
                )
                if result.returncode != 0:
                    detail = result.stderr[:4096].decode("utf-8", errors="replace")
                    raise RuntimeError(f"validator exited with status {result.returncode}: {detail}")
                content = output_path.read_bytes() if output_path.is_file() else result.stdout
                if len(content) > self.max_output_bytes:
                    raise RuntimeError("validator output exceeds configured limit")
                parsed = json.loads(content.decode("utf-8"))
                if not isinstance(parsed, dict) or not isinstance(parsed.get("findings", []), list):
                    raise RuntimeError("validator output must be an object containing a findings array")
                return parsed

        return await asyncio.to_thread(execute)
