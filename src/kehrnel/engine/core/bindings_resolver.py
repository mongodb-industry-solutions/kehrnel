"""Bindings reference resolver loader.

Allows resolving secure bindings from an external secret system (e.g., HDL backend)
using a pluggable callable configured via environment variable.

Built-in schemes (no configuration required):

  env://VARNAME
    Reads the named environment variable and interprets it as either:
    - A JSON object:  {"db": {"provider": "mongodb", "uri": "...", "database": "..."}}
    - A plain URI string: mongodb+srv://...  (database inferred from URI path or
      CORE_DATABASE_NAME / KEHRNEL_DATABASE env vars)

    Examples:
      --bindings-ref env://MONGODB_URI
      --bindings-ref env://DB_BINDINGS
"""
from __future__ import annotations

import importlib
import inspect
import json
import os
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlparse


BindingsResolver = Callable[..., dict[str, Any] | None | Awaitable[dict[str, Any] | None]]

_ENV_SCHEME = "env://"


def _resolve_env_var_bindings(*, bindings_ref: str, **_kwargs: Any) -> dict[str, Any] | None:
    """
    Built-in resolver for the ``env://VARNAME`` scheme.

    Reads the named environment variable and returns a bindings dict.
    The variable may contain:
    - A full bindings JSON object (pass-through).
    - A plain MongoDB URI string; database is inferred from the URI path,
      then from CORE_DATABASE_NAME, then from KEHRNEL_DATABASE.
    """
    if not bindings_ref.startswith(_ENV_SCHEME):
        return None
    var_name = bindings_ref[len(_ENV_SCHEME):]
    if not var_name:
        raise ValueError("env:// bindings_ref must specify a variable name, e.g. env://MONGODB_URI")
    value = (os.getenv(var_name) or "").strip()
    if not value:
        raise ValueError(
            f"env:// bindings_ref references ${var_name} but the variable is not set or empty"
        )
    # Try JSON first — lets users store a full bindings object in one env var.
    if value.startswith("{"):
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError(f"${var_name} must be a JSON object or a MongoDB URI string")
        return parsed
    # Treat as a plain URI.
    uri = value
    db_name = _db_from_uri(uri) or _fallback_db_name()
    if not db_name:
        raise ValueError(
            f"Could not determine database name from ${var_name} URI. "
            "Set CORE_DATABASE_NAME or KEHRNEL_DATABASE, or include the database in the URI path."
        )
    return {"db": {"provider": "mongodb", "uri": uri, "database": db_name}}


def _db_from_uri(uri: str) -> str | None:
    try:
        path = (urlparse(uri).path or "").lstrip("/").split("?")[0].split("/")[0]
        return path or None
    except Exception:
        return None


def _fallback_db_name() -> str | None:
    for var in ("CORE_DATABASE_NAME", "KEHRNEL_DATABASE"):
        val = (os.getenv(var) or "").strip()
        if val:
            return val
    return None


def load_bindings_resolver_from_env() -> Optional[BindingsResolver]:
    """
    Load resolver from ``KEHRNEL_BINDINGS_RESOLVER``.

    Expected value: ``module.submodule:function_name``
    Function signature is flexible, but Kehrnel calls it with keyword args:
      - bindings_ref
      - env_id
      - domain
      - strategy_id
      - op
      - context

    When no explicit resolver is configured, two auto-defaults apply in order:
    1. Built-in ``env://`` scheme — always available, no config needed.
    2. HDL encrypted-secrets resolver — activated automatically when
       ENV_SECRETS_KEY, CORE_MONGODB_URL, and CORE_DATABASE_NAME are all set.
    """
    spec = (os.getenv("KEHRNEL_BINDINGS_RESOLVER") or "").strip()
    if not spec:
        has_hdl_env = all(
            (os.getenv(name) or "").strip()
            for name in ("ENV_SECRETS_KEY", "CORE_MONGODB_URL", "CORE_DATABASE_NAME")
        )
        if has_hdl_env:
            spec = "kehrnel.engine.core.integrations.hdl.bindings_resolver:resolve_hdl_bindings"

    configured_resolver: BindingsResolver | None = None
    if spec:
        if ":" not in spec:
            raise ValueError("KEHRNEL_BINDINGS_RESOLVER must be 'module:function'")
        mod_name, fn_name = spec.split(":", 1)
        mod = importlib.import_module(mod_name)
        fn = getattr(mod, fn_name)
        if not callable(fn):
            raise TypeError(f"Resolver {spec} is not callable")
        configured_resolver = fn

    # Always chain the built-in env:// resolver first so it works regardless of
    # whether an external resolver is configured.
    def _chained_resolver(*, bindings_ref: str, **kwargs: Any) -> dict[str, Any] | None:
        if bindings_ref.startswith(_ENV_SCHEME):
            return _resolve_env_var_bindings(bindings_ref=bindings_ref, **kwargs)
        if configured_resolver is not None:
            return configured_resolver(bindings_ref=bindings_ref, **kwargs)
        return None

    return _chained_resolver


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
