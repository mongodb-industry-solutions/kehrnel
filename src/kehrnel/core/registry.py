"""Backward-compatible wrapper for ``kehrnel.engine.core.registry``."""

from kehrnel.engine.core.registry import ActivationRegistry, FileActivationRegistry
from kehrnel.engine.core.environment import EnvironmentRecord

__all__ = ["ActivationRegistry", "FileActivationRegistry", "EnvironmentRecord"]
