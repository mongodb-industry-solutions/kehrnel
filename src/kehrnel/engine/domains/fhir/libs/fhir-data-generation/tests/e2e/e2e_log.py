"""Simple E2E status lines and heartbeats for long-running steps."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Generator

# Set by ``run_cli_e2e.py`` (``--quiet`` disables).
_STATUS: bool = True

# Heartbeat interval for steps that run longer than this (seconds).
HEARTBEAT_INTERVAL = 30.0


def set_status_logging(enabled: bool) -> None:
    global _STATUS
    _STATUS = enabled


def set_verbose(enabled: bool) -> None:
    """Alias for :func:`set_status_logging` (used by ``run_cli_e2e``)."""
    set_status_logging(enabled)


def status_enabled() -> bool:
    return _STATUS


def is_verbose() -> bool:
    """Alias for :func:`status_enabled` (search runner progress)."""
    return _STATUS


def log_status(msg: str) -> None:
    if _STATUS:
        print(msg, flush=True)


def log_scenario(phase: str, scenario_id: str, title: str) -> None:
    log_status(f"\n[{phase}] {scenario_id}: {title}")


@contextmanager
def status_phase(
    label: str,
    *,
    heartbeat: bool = True,
) -> Generator[dict[str, Any], None, None]:
    """Print ``label...`` then optional ``still running`` heartbeats until done."""
    progress: dict[str, Any] = {"detail": ""}
    if not _STATUS:
        yield progress
        return

    log_status(f"  {label}...")
    stop = threading.Event()
    t0 = time.perf_counter()

    def _heartbeat() -> None:
        while not stop.wait(HEARTBEAT_INTERVAL):
            elapsed = time.perf_counter() - t0
            detail = progress.get("detail")
            suffix = f" ({detail})" if detail else ""
            log_status(f"  {label} still running{suffix} — {elapsed:.0f}s")

    thread: threading.Thread | None = None
    if heartbeat:
        thread = threading.Thread(target=_heartbeat, daemon=True)
        thread.start()
    try:
        yield progress
    finally:
        stop.set()
        if thread is not None:
            thread.join(timeout=0.5)
        elapsed = time.perf_counter() - t0
        log_status(f"  {label} done ({elapsed:.1f}s)")
