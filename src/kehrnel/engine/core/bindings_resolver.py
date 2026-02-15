"""Bindings reference resolver loader.

Allows resolving secure bindings from an external secret system (e.g., HDL backend)
using a pluggable callable configured via environment variable.
"""
from __future__ import annotations

import importlib
import inspect
import os
from typing import Any, Awaitable, Callable, Optional


BindingsResolver = Callable[..., dict[str, Any] | None | Awaitable[dict[str, Any] | None]]


def load_bindings_resolver_from_env() -> Optional[BindingsResolver]:
    """
    Load resolver from `KEHRNEL_BINDINGS_RESOLVER`.

    Expected value: `module.submodule:function_name`
    Function signature is flexible, but Kehrnel calls it with keyword args:
      - bindings_ref
      - env_id
      - domain
      - strategy_id
      - op
      - context
    """
    spec = (os.getenv("KEHRNEL_BINDINGS_RESOLVER") or "").strip()
    # Auto-default for HDL deployments when resolver is not explicitly set.
    # This keeps a single secure path (bindings_ref -> resolver) without
    # requiring manual resolver wiring every time.
    if not spec:
        has_hdl_env = all(
            (os.getenv(name) or "").strip()
            for name in ("ENV_SECRETS_KEY", "CORE_MONGODB_URL", "CORE_DATABASE_NAME")
        )
        if has_hdl_env:
            spec = "kehrnel.engine.core.integrations.hdl.bindings_resolver:resolve_hdl_bindings"
    if not spec:
        return None
    if ":" not in spec:
        raise ValueError("KEHRNEL_BINDINGS_RESOLVER must be 'module:function'")
    mod_name, fn_name = spec.split(":", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, fn_name)
    if not callable(fn):
        raise TypeError(f"Resolver {spec} is not callable")
    return fn


async def resolve_bindings(
    resolver: Optional[BindingsResolver],
    *,
    bindings_ref: str,
    env_id: str,
    domain: str | None,
    strategy_id: str | None,
    op: str | None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if resolver is None:
        return None
    result = resolver(
        bindings_ref=bindings_ref,
        env_id=env_id,
        domain=domain,
        strategy_id=strategy_id,
        op=op,
        context=context or {},
    )
    if inspect.isawaitable(result):
        result = await result
    if result is not None and not isinstance(result, dict):
        raise TypeError("Bindings resolver must return dict or None")
    return result
