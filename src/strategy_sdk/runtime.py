from __future__ import annotations

from typing import Any, Dict
import inspect

from .plugin import StrategyBindings, StrategyContext, StrategyPlugin
from .manifest import StrategyManifest


class StrategyRuntimeError(RuntimeError):
    pass


class StrategyHandle:
    """
    Thin wrapper that owns a plugin instance and mediates lifecycle calls.
    Kernel code can compose multiple handles and route requests by capability.
    """

    def __init__(self, manifest: StrategyManifest, plugin: StrategyPlugin):
        self.manifest = manifest
        self.plugin = plugin
        self.initialized = False

    async def activate(self, config: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext):
        """
        Activate plugin, honoring async validate/initialize implementations.
        """
        if hasattr(self.plugin, "validate_config"):
            res = self.plugin.validate_config(config)
            if inspect.isawaitable(res):
                await res
        if hasattr(self.plugin, "initialize"):
            res = self.plugin.initialize(config=config, bindings=bindings, context=context)
            if inspect.isawaitable(res):
                await res
        self.initialized = True

    def shutdown(self):
        if hasattr(self.plugin, "shutdown"):
            self.plugin.shutdown()

    # Generic dispatcher — callers can also access plugin directly.
    def dispatch(self, capability: str, *args, **kwargs):
        if not self.initialized:
            raise StrategyRuntimeError(f"Strategy {self.manifest.id} not initialized")
        fn = getattr(self.plugin, capability, None)
        if fn is None:
            raise StrategyRuntimeError(f"Capability '{capability}' not implemented by {self.manifest.id}")
        return fn(*args, **kwargs)
