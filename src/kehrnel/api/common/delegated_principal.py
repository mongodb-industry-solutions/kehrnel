"""Bounded HDL assertions and request-local installation authority.

Configuration is operator-owned. No JWT/key objects enter the Python policy or
registry; service keys are addressed by their SHA-256 fingerprint (20 hex).
"""
from __future__ import annotations

import json
import os
import re

import jwt

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.strategy_access import Authority, Owner, PackageRef

ACTIONS = frozenset({"strategy:read", "strategy:compile", "runtime:query", "blueprint:validate"})
HEADER = "x-hdl-principal-assertion"


def _strings(value, limit=64):
    if not isinstance(value, list) or len(value) > limit or any(not isinstance(v, str) or not v or len(v) > 256 for v in value) or len(set(value)) != len(value):
        raise ValueError("Invalid string list")
    return frozenset(value)


def load_authority_configuration():
    raw = os.getenv("KEHRNEL_API_KEY_AUTHORITIES", "{}")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("Invalid API key authorities")
    for fingerprint, item in data.items():
        if not re.fullmatch(r"[0-9a-f]{20}", fingerprint) or not isinstance(item, dict) or set(item) - {"kind", "scopes", "actions", "strategyRefs", "environmentIds"}:
            raise ValueError("Invalid API key authority")
        if item.get("kind") not in {"installation", "hdl-delegator"}:
            raise ValueError("Invalid authority kind")
        owners = tuple(Owner.parse(s) for s in item.get("scopes", []))
        if not owners or (item["kind"] == "installation" and len(owners) != 1) or len(set(owners)) != len(owners):
            raise ValueError("Invalid key owner ceiling")
        actions = _strings(item.get("actions", []), len(ACTIONS))
        if not actions <= ACTIONS:
            raise ValueError("Unsupported key action")
        refs = item.get("strategyRefs", [])
        if not isinstance(refs, list) or len(refs) > 64:
            raise ValueError("Invalid key package ceiling")
        parsed_refs = tuple(PackageRef.parse(r) for r in refs)
        if len(set(parsed_refs)) != len(parsed_refs):
            raise ValueError("Duplicate key package reference")
        _strings(item.get("environmentIds", []))
    return data


def verify_assertion(token, ceiling, policy, registry):
    try:
        if not isinstance(token, str) or len(token.encode("utf-8")) > 8192:
            raise ValueError("Invalid assertion size")
        header = jwt.get_unverified_header(token)
        if set(header) != {"alg", "kid", "typ"} or header["alg"] != "RS256" or header["typ"] != "hdl-delegated-principal+jwt":
            raise ValueError("Invalid assertion header")
        keys = json.loads(os.getenv("KEHRNEL_HDL_ISSUER_KEYS", "{}"))
        kid = header["kid"]
        if not isinstance(kid, str) or not isinstance(keys, dict) or kid not in keys or not isinstance(keys[kid], str):
            raise ValueError("Unknown issuer key")
        issuer = os.getenv("KEHRNEL_HDL_ISSUER")
        audience = os.getenv("KEHRNEL_HDL_AUDIENCE")
        if not issuer or not audience:
            raise ValueError("Delegation is not configured")
        required = {"iss", "aud", "sub", "iat", "nbf", "exp", "jti", "v", "scope", "workspace", "actions", "strategyRefs"}
        claims = jwt.decode(token, keys[kid], algorithms=["RS256"], issuer=issuer, audience=audience, leeway=5,
                            options={"require": sorted(required), "strict_aud": True})
        if not required <= set(claims) or set(claims) - required - {"environment"}:
            raise ValueError("Invalid assertion claims")
        if type(claims["v"]) is not int or claims["v"] != 1:
            raise ValueError("Invalid assertion version")
        if any(type(claims[k]) is not int or claims[k] < 0 for k in ("iat", "nbf", "exp")) or claims["nbf"] != claims["iat"] or not 0 < claims["exp"] - claims["iat"] <= 60:
            raise ValueError("Invalid assertion lifetime")
        for k in ("sub", "jti"):
            if not isinstance(claims[k], str) or not claims[k] or len(claims[k]) > 256:
                raise ValueError("Invalid actor/trace id")
        owner = Owner.parse(claims["scope"])
        if Owner.parse(claims["workspace"]) != owner or owner not in tuple(Owner.parse(s) for s in ceiling["scopes"]):
            raise ValueError("Scope exceeds key ceiling")
        actions = _strings(claims["actions"], len(ACTIONS))
        if not actions or not actions <= ACTIONS:
            raise ValueError("Unsupported assertion action")
        effective_actions = actions & _strings(ceiling["actions"], len(ACTIONS))
        refs_data = claims["strategyRefs"]
        if not isinstance(refs_data, list) or len(refs_data) > 64:
            raise ValueError("Invalid assertion package references")
        # Structural validation names artifacts as data; it does not retrieve them.
        # Every package/runtime action still requires admitted package references.
        if not refs_data and actions != {"blueprint:validate"}:
            raise ValueError("Invalid assertion package references")
        refs = frozenset(PackageRef.parse(r) for r in refs_data)
        if len(refs) != len(refs_data):
            raise ValueError("Duplicate assertion references")
        effective_refs = refs & frozenset(PackageRef.parse(r) for r in ceiling["strategyRefs"])
        environment = claims.get("environment")
        env_id = runtime_id = None
        environments = frozenset()
        if environment is not None:
            if not isinstance(environment, dict) or set(environment) != {"id", "runtimeId"} or any(not isinstance(v, str) or not v or len(v) > 256 for v in environment.values()):
                raise ValueError("Invalid environment binding")
            env_id, runtime_id = environment["id"], environment["runtimeId"]
            if runtime_id in _strings(ceiling["environmentIds"]):
                environments = frozenset({runtime_id})
        if "runtime:query" in effective_actions and not environments:
            raise ValueError("Runtime query requires an exact environment ceiling")
        authority = Authority(owner, effective_refs, effective_actions, environments, True, claims["sub"], env_id, runtime_id)
        # Claims cannot turn unresolved or unadmitted references into inventory.
        if any(not policy._current(registry, r) or r not in policy.records for r in refs):
            raise ValueError("Unresolved assertion package")
        return authority
    except (ValueError, TypeError, KeyError, jwt.PyJWTError, OSError, KehrnelError):
        raise KehrnelError("INVALID_PRINCIPAL_ASSERTION", 401, "Invalid delegated principal assertion") from None


def bind_request_authority(request):
    configuration = request.app.state.api_key_authorities
    ceiling = configuration.get(getattr(request.state, "authenticated_api_key", None))
    token = request.headers.get(HEADER)
    request.state.strategy_authority = Authority()
    request.state.hdl_delegator = bool(ceiling and ceiling["kind"] == "hdl-delegator")
    if token and not request.state.hdl_delegator:
        raise KehrnelError("INVALID_PRINCIPAL_ASSERTION", 401, "Invalid delegated principal assertion")
    if not ceiling:
        return
    if request.state.hdl_delegator:
        if token:
            request.state.strategy_authority = verify_assertion(token, ceiling, request.app.state.strategy_access_policy, request.app.state.strategy_runtime.registry)
    else:
        request.state.strategy_authority = Authority(Owner.parse(ceiling["scopes"][0]),
            frozenset(PackageRef.parse(r) for r in ceiling["strategyRefs"]), _strings(ceiling["actions"], len(ACTIONS)), _strings(ceiling["environmentIds"]))


def request_authority(request):
    return getattr(request.state, "strategy_authority", Authority())


def require_package(request, strategy_id, action="strategy:read"):
    registry = request.app.state.strategy_runtime.registry
    if os.getenv("KEHRNEL_AUTH_ENABLED", "true").lower() not in {"true", "1", "yes"}:
        manifest = registry.get_manifest(strategy_id)
        if manifest is None:
            raise KehrnelError("STRATEGY_NOT_FOUND", 404, "Strategy not found")
        return manifest
    return request.app.state.strategy_access_policy.require(registry, strategy_id, request_authority(request), action)


def require_runtime_package(request, env_id, domain, action):
    if os.getenv("KEHRNEL_AUTH_ENABLED", "true").lower() not in {"true", "1", "yes"}:
        # Retain SDK/dev-mode runtime validation and existing error contracts.
        return None
    authority = request_authority(request)
    if action not in authority.actions:
        raise KehrnelError("FORBIDDEN", 403, "Runtime operation is outside the granted authority")
    if env_id not in authority.environments or (authority.delegated and env_id != authority.runtime_id):
        raise KehrnelError("FORBIDDEN", 403, "Runtime environment is outside the granted authority")
    registry = request.app.state.strategy_runtime.registry
    activation = registry.get_activation(env_id, domain)
    if not activation:
        raise KehrnelError("PREPARATION_REQUIRED", 409, "An explicit strategy activation is required")
    manifest = require_package(request, activation.strategy_id, action)
    if activation.version != manifest.version:
        raise KehrnelError("PREPARATION_REQUIRED", 409, "Installed strategy differs from the runtime binding")
    # Runtime's existing activation-integrity validation still checks its
    # manifest/config digest before dispatch. No implicit activation occurs here.
    return manifest


def enforce_delegator_route(request):
    if not getattr(request.state, "hdl_delegator", False):
        return
    path = request.url.path
    path = path[3:] if path.startswith("/v1/") else path
    if request.method == "GET" and (path == "/strategies" or re.fullmatch(r"/strategies/[^/]+(?:/(?:spec|endpoints|assets/.+))?", path) or
            re.fullmatch(r"/api/persistence-strategies/[^/]+", path) or path.startswith(("/docs", "/redoc", "/openapi"))):
        # Inventory/diagnostics are not package retrieval.
        if path not in {"/strategies/installed", "/strategies/diagnostics", "/strategies/load"}:
            return
    if request.method == "GET" and request.url.path in {"/v1/source-profiles", "/v1/architecture-resolution-inputs"}:
        authority = request_authority(request)
        if authority.delegated and "strategy:read" in authority.actions:
            return
        raise KehrnelError("FORBIDDEN", 403, "Delegation does not grant design catalog retrieval")
    if request.method == "POST" and path == "/solution-blueprints/canonicalize":
        authority = request_authority(request)
        if authority.delegated and "blueprint:validate" in authority.actions:
            return
        raise KehrnelError("FORBIDDEN", 403, "Delegation does not grant Blueprint validation")
    if request.method == "POST" and re.fullmatch(r"/strategies/[^/]+/deployment-config/compile", path):
        return
    if request.method == "POST" and re.fullmatch(r"/environments/[^/]+/(?:query|compile_query)", path):
        return
    raise KehrnelError("FORBIDDEN", 403, "Delegation does not grant this operation")
