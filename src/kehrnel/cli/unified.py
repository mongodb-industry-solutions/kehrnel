"""Unified kehrnel CLI (auth/context/core/common/domain/strategy/workflows)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional, List

import typer
import yaml
from dotenv import find_dotenv, load_dotenv

# Load .env.local (then .env as fallback) so env vars like MONGODB_URI are
# available to CLI commands without requiring `source .env.local` in the shell.
load_dotenv(find_dotenv(".env.local", usecwd=True), override=False)
load_dotenv(find_dotenv(".env", usecwd=True), override=False)

from kehrnel.cli.state import load_cli_state, save_cli_state, mask_api_key

TOP_LEVEL_HELP = """Unified {kehrnel} CLI

\b
Help discovery (at every level):
  kehrnel --help
  kehrnel core env --help
  kehrnel core env op --help
  kehrnel core env query --help

\b
Pass-through command help:
  kehrnel common map -- --help
  kehrnel common map-skeleton -- --help
  kehrnel common transform -- --help
  kehrnel common validate -- --help

\b
Workflow commands:
  kehrnel resource --help
  kehrnel op list
  kehrnel op schema synthetic_generate_batch
  kehrnel run synthetic_generate_batch --from resource://src --to resource://dst
"""

CORE_HELP = """Core runtime operations

\b
Next help levels:
  kehrnel core env --help
  kehrnel core env op --help
  kehrnel core env query --help
"""

ENV_HELP = """Environment-scoped runtime operations

\b
Useful detailed help:
  kehrnel core env op --help
  kehrnel core env query --help
  kehrnel core env compile-query --help
"""

COMMON_HELP = """Cross-domain shared operations

\b
Delegated tool help (pass-through):
  kehrnel common map -- --help
  kehrnel common map-skeleton -- --help
  kehrnel common transform -- --help
  kehrnel common validate -- --help
"""

RESOURCE_HELP = """Reusable source/sink profiles (file, mongo, ...)."""

OPS_HELP = """Discover available strategy operations and schemas."""

app = typer.Typer(help=TOP_LEVEL_HELP)
auth_app = typer.Typer(help="Authenticate once and reuse credentials")
context_app = typer.Typer(help="Set/get runtime context (env/domain/strategy/data-mode/source/sink)")
core_app = typer.Typer(help=CORE_HELP)
env_app = typer.Typer(help=ENV_HELP)
common_app = typer.Typer(help=COMMON_HELP)
domain_app = typer.Typer(help="Domain-scoped operations")
strategy_app = typer.Typer(help="Strategy-scoped operations")
resource_app = typer.Typer(help=RESOURCE_HELP)
ops_app = typer.Typer(help=OPS_HELP)

app.add_typer(auth_app, name="auth")
app.add_typer(context_app, name="context")
app.add_typer(core_app, name="core")
core_app.add_typer(env_app, name="env")
app.add_typer(common_app, name="common")
app.add_typer(domain_app, name="domain")
app.add_typer(strategy_app, name="strategy")
app.add_typer(resource_app, name="resource")
app.add_typer(ops_app, name="op")


def _state() -> dict:
    return load_cli_state()


def _save(state: dict) -> None:
    path = save_cli_state(state)
    typer.echo(f"State updated: {path}")


def _resolve_runtime_url(runtime_url: Optional[str] = None) -> Optional[str]:
    state = _state()
    return (
        runtime_url
        or state["context"].get("runtime_url")
        or state["auth"].get("runtime_url")
        or os.getenv("KEHRNEL_RUNTIME_URL")
    )


def _resolve_api_key(api_key: Optional[str] = None) -> Optional[str]:
    state = _state()
    return api_key or state["auth"].get("api_key") or os.getenv("KEHRNEL_API_KEY")


def _require_strategy(strategy: Optional[str]) -> str:
    if strategy:
        return strategy
    state = _state()
    selected = state["context"].get("strategy")
    if not selected:
        raise typer.BadParameter(
            "No strategy selected. Use `kehrnel context set --strategy ...` or pass --strategy."
        )
    return selected


def _require_domain(domain: Optional[str]) -> str:
    if domain:
        return domain
    state = _state()
    selected = state["context"].get("domain")
    if not selected:
        raise typer.BadParameter(
            "No domain selected. Use `kehrnel context set --domain ...` or pass --domain."
        )
    return selected


def _require_env(environment: Optional[str]) -> str:
    if environment:
        return environment
    state = _state()
    selected = state["context"].get("environment") or os.getenv("KEHRNEL_DEFAULT_ENV_ID")
    if not selected:
        raise typer.BadParameter(
            "No environment selected. Use `kehrnel context set --env ...` or pass --env."
        )
    return selected


def _resolve_data_mode(data_mode: Optional[str] = None) -> Optional[str]:
    state = _state()
    return data_mode or state["context"].get("data_mode")


def _coerce_cli_value(value: str) -> Any:
    text = value.strip()
    if text == "":
        return ""
    try:
        return json.loads(text)
    except Exception:
        return text


def _parse_kv_pairs(items: list[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise typer.BadParameter(f"Invalid --set entry '{item}'. Use KEY=VALUE.")
        key, raw = item.split("=", 1)
        key = key.strip()
        if not key:
            raise typer.BadParameter(f"Invalid --set entry '{item}'. KEY cannot be empty.")
        out[key] = _coerce_cli_value(raw)
    return out


def _resolve_resource_profile(name: str) -> Dict[str, Any]:
    state = _state()
    resources = state.get("resources") if isinstance(state, dict) else None
    if not isinstance(resources, dict):
        raise typer.BadParameter("No resources configured. Use `kehrnel resource add ...`.")
    profile = resources.get(name)
    if not isinstance(profile, dict):
        raise typer.BadParameter(f"Unknown resource profile: {name}")
    return profile


def _resolve_data_ref(ref: Optional[str]) -> Optional[Dict[str, Any]]:
    if not ref:
        return None
    text = ref.strip()
    if not text:
        return None
    if text.startswith("resource://"):
        name = text[len("resource://"):].strip()
        if not name:
            raise typer.BadParameter("resource:// reference must include a profile name")
        profile = _resolve_resource_profile(name)
        return {"ref": text, "type": "resource", "name": name, "profile": profile}
    if "://" not in text:
        # Plain local path shorthand
        return {"ref": f"file://{text}", "type": "file", "path": text}
    scheme = urllib.parse.urlparse(text).scheme.lower()
    if scheme == "file":
        path = text[len("file://"):]
        return {"ref": text, "type": "file", "path": path}
    if scheme in {"mongo", "mongodb"}:
        return {"ref": text, "type": "mongo", "uri": text}
    return {"ref": text, "type": scheme}


def _fetch_ops_catalog(base_url: str, api_key: Optional[str]) -> list[Dict[str, Any]]:
    """Fetch operation catalog, preferring /ops and falling back to /strategies."""
    base = base_url.rstrip("/")
    status, data = _http_json("GET", f"{base}/ops", api_key)
    if status < 400 and isinstance(data, dict) and isinstance(data.get("ops"), list):
        return [row for row in data.get("ops", []) if isinstance(row, dict)]

    status, data = _http_json("GET", f"{base}/strategies", api_key)
    strategies = data.get("strategies") if isinstance(data, dict) else None
    if status >= 400 or not isinstance(strategies, list):
        return []

    ops: list[Dict[str, Any]] = []
    for row in strategies:
        if not isinstance(row, dict):
            continue
        strategy_id = row.get("id") or row.get("strategy_id")
        domain = row.get("domain")
        for op in row.get("ops") or []:
            if not isinstance(op, dict):
                continue
            ops.append(
                {
                    "strategy_id": strategy_id,
                    "domain": domain,
                    "name": op.get("name"),
                    "kind": op.get("kind"),
                    "summary": op.get("summary"),
                    "input_schema": op.get("input_schema") or {},
                    "output_schema": op.get("output_schema") or {},
                }
            )
    return ops


def _run_module(module: str, args: list[str]) -> None:
    cmd = [sys.executable, "-m", module, *args]
    res = subprocess.run(cmd)
    raise typer.Exit(res.returncode)


def _http_json(method: str, url: str, api_key: Optional[str] = None, payload: Optional[dict] = None) -> tuple[int, dict]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url=url, method=method.upper(), data=body)
    req.add_header("Accept", "application/json")
    if body is not None:
        req.add_header("Content-Type", "application/json")
    if api_key:
        req.add_header("X-API-Key", api_key)

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
            return resp.status, data
    except urllib.error.HTTPError as exc:
        raw = ""
        try:
            raw = exc.read().decode("utf-8")
        except Exception:
            raw = ""
        try:
            data = json.loads(raw) if raw else {}
        except Exception:
            data = {"error": raw or str(exc)}
        return int(getattr(exc, "code", 599) or 599), data
    except Exception as exc:
        return 599, {"error": str(exc)}

def _load_json_or_yaml(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
    except Exception:
        data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise typer.BadParameter(f"{path} did not parse into an object")
    return data


def _choose_from_list(label: str, options: list[str], default: Optional[str] = None) -> str:
    if not options:
        raise typer.BadParameter(f"No options available for {label}")
    if default and default in options:
        chosen_default = default
    else:
        chosen_default = options[0]
    typer.echo(f"Available {label}:")
    for opt in options:
        typer.echo(f"- {opt}")
    chosen = typer.prompt(f"Select {label}", default=chosen_default)
    chosen = (chosen or "").strip()
    if chosen not in options:
        raise typer.BadParameter(f"Invalid {label}: {chosen}")
    return chosen


@app.command("setup")
def setup(
    runtime_url: Optional[str] = typer.Option(None, help="Runtime base URL, ex: http://localhost:8000"),
    api_key: Optional[str] = typer.Option(None, help="API key (X-API-Key). If omitted, you will be prompted."),
    env: Optional[str] = typer.Option(None, "--env", help="Environment key/id (default: dev)"),
    domain: Optional[str] = typer.Option(None, help="Domain id (openehr, fhir, ...)"),
    strategy: Optional[str] = typer.Option(None, help="Strategy id (ex: openehr.rps_dual)"),
    data_mode: Optional[str] = typer.Option(None, "--data-mode", help="Data mode/profile (strategy-defined, ex: profile.search_shortcuts)"),
    source: Optional[str] = typer.Option(None, "--source", help="Default source reference (file://, mongo://, resource://...)"),
    sink: Optional[str] = typer.Option(None, "--sink", help="Default sink reference (file://, mongo://, resource://...)"),
    activate: bool = typer.Option(False, "--activate", help="Optionally activate the selected strategy now"),
    bindings_ref: Optional[str] = typer.Option(None, "--bindings-ref", help="Bindings reference (recommended/required when auth enabled)"),
    version: str = typer.Option("latest", "--version", help="Strategy version to activate"),
    config_file: Optional[Path] = typer.Option(None, "--config", help="Activation config JSON/YAML file"),
    skip_health: bool = typer.Option(False, "--skip-health", help="Skip /health check"),
    non_interactive: bool = typer.Option(False, "--non-interactive", help="Fail instead of prompting when fields are missing"),
):
    """
    Interactive first-run setup.

    This command:
    - stores runtime URL + API key (optional) under `kehrnel auth`
    - selects env/domain/strategy under `kehrnel context`
    - optionally activates the strategy in the selected environment
    """
    state = _state()

    # Runtime URL
    resolved_runtime = _resolve_runtime_url(runtime_url)
    if not resolved_runtime:
        if non_interactive:
            raise typer.BadParameter("No runtime URL configured. Pass --runtime-url or set KEHRNEL_RUNTIME_URL.")
        resolved_runtime = typer.prompt("Runtime URL", default="http://localhost:8000")
    resolved_runtime = resolved_runtime.rstrip("/")

    # API key (optional: runtime may be running with auth disabled in dev)
    resolved_api_key = api_key or state["auth"].get("api_key") or os.getenv("KEHRNEL_API_KEY")
    if resolved_api_key is None:
        resolved_api_key = ""
    resolved_api_key = (resolved_api_key or "").strip()
    if not resolved_api_key and not non_interactive:
        resolved_api_key = (typer.prompt("API key (X-API-Key) (leave empty if auth disabled)", default="", hide_input=True, show_default=False) or "").strip()

    # Persist auth/runtime so subsequent commands work.
    state["auth"]["runtime_url"] = resolved_runtime
    if resolved_api_key:
        state["auth"]["api_key"] = resolved_api_key
    _save(state)

    # Connectivity check
    if not skip_health:
        st, resp = _http_json("GET", f"{resolved_runtime}/health", resolved_api_key or None)
        if st >= 400:
            typer.echo(json.dumps({"status": st, "url": f"{resolved_runtime}/health", "response": resp}, indent=2))
            raise typer.Exit(1)

    # Strategy discovery to help selection.
    st, data = _http_json("GET", f"{resolved_runtime}/strategies", resolved_api_key or None)
    strategies = []
    if isinstance(data, dict):
        strategies = data.get("strategies") or []

    if st >= 400 and not (domain and strategy):
        typer.echo(json.dumps({"status": st, "url": f"{resolved_runtime}/strategies", "response": data}, indent=2))
        raise typer.BadParameter("Cannot list strategies from runtime. Pass --domain/--strategy or fix connectivity/auth.")

    # Environment
    resolved_env = (env or state["context"].get("environment") or os.getenv("KEHRNEL_DEFAULT_ENV_ID") or "dev").strip()
    if not resolved_env:
        if non_interactive:
            raise typer.BadParameter("Missing environment. Pass --env.")
        resolved_env = typer.prompt("Environment id", default="dev").strip()

    # Domain
    resolved_domain = (domain or state["context"].get("domain") or "").strip().lower() or None
    domain_options = sorted({str((s or {}).get("domain") or "").strip().lower() for s in strategies if isinstance(s, dict) and (s or {}).get("domain")})
    if not resolved_domain:
        if len(domain_options) == 1:
            resolved_domain = domain_options[0]
        elif domain_options and not non_interactive:
            default_dom = "openehr" if "openehr" in domain_options else domain_options[0]
            resolved_domain = _choose_from_list("domains", domain_options, default=default_dom)
        elif non_interactive:
            raise typer.BadParameter("Missing domain. Pass --domain.")
        else:
            resolved_domain = typer.prompt("Domain", default="openehr").strip().lower()

    # Strategy
    resolved_strategy = (strategy or state["context"].get("strategy") or "").strip() or None
    strategy_options = []
    for s in strategies:
        if not isinstance(s, dict):
            continue
        if str((s.get("domain") or "")).strip().lower() != resolved_domain:
            continue
        sid = (s.get("id") or s.get("strategy_id") or "").strip()
        if sid:
            strategy_options.append(sid)
    strategy_options = sorted(set(strategy_options))
    if not resolved_strategy:
        if len(strategy_options) == 1:
            resolved_strategy = strategy_options[0]
        elif strategy_options and not non_interactive:
            default_sid = "openehr.rps_dual" if "openehr.rps_dual" in strategy_options else strategy_options[0]
            resolved_strategy = _choose_from_list("strategies", strategy_options, default=default_sid)
        elif non_interactive:
            raise typer.BadParameter("Missing strategy. Pass --strategy.")
        else:
            resolved_strategy = typer.prompt("Strategy", default="openehr.rps_dual").strip()

    # Persist context selections (and runtime override for context).
    state = _state()
    state["context"]["environment"] = resolved_env
    state["context"]["domain"] = resolved_domain
    state["context"]["strategy"] = resolved_strategy
    state["context"]["runtime_url"] = resolved_runtime
    if data_mode is not None:
        state["context"]["data_mode"] = data_mode
    if source is not None:
        state["context"]["source"] = source
    if sink is not None:
        state["context"]["sink"] = sink
    _save(state)

    typer.echo("")
    typer.echo("Selected context:")
    typer.echo(
        json.dumps(
            {
                "runtime_url": resolved_runtime,
                "env": resolved_env,
                "domain": resolved_domain,
                "strategy": resolved_strategy,
                "data_mode": state["context"].get("data_mode"),
                "source": state["context"].get("source"),
                "sink": state["context"].get("sink"),
            },
            indent=2,
        )
    )

    if activate:
        config = _load_json_or_yaml(config_file) if config_file else {}
        resolved_bindings_ref = (bindings_ref or "").strip()
        if not resolved_bindings_ref and not non_interactive:
            resolved_bindings_ref = (typer.prompt("bindings_ref (ex: env://DB_BINDINGS)", default="", show_default=False) or "").strip()
        payload = {
            "domain": resolved_domain,
            "strategy_id": resolved_strategy,
            "version": version,
            "config": config,
            "bindings_ref": resolved_bindings_ref or None,
            "allow_plaintext_bindings": False,
            "bindings": {},
            "force": False,
        }
        st, resp = _http_json("POST", f"{resolved_runtime}/environments/{resolved_env}/activate", resolved_api_key or None, payload)
        typer.echo(json.dumps({"status": st, "response": resp}, indent=2))
        raise typer.Exit(0 if st < 400 else 1)

    typer.echo("")
    typer.echo("Next steps:")
    typer.echo(f"- kehrnel core health")
    typer.echo(f"- kehrnel strategy list --domain {resolved_domain}")
    typer.echo(f"- kehrnel core env endpoints --env {resolved_env}")


@auth_app.command("login")
def auth_login(
    api_key: str = typer.Option(..., prompt=True, hide_input=True),
    runtime_url: Optional[str] = typer.Option(None, help="Default runtime URL, ex: http://localhost:8000"),
):
    state = _state()
    state["auth"]["api_key"] = api_key
    if runtime_url:
        state["auth"]["runtime_url"] = runtime_url.rstrip("/")
    _save(state)
    typer.echo("Authenticated.")


@auth_app.command("logout")
def auth_logout():
    state = _state()
    state["auth"]["api_key"] = None
    _save(state)
    typer.echo("Logged out.")


@auth_app.command("whoami")
def auth_whoami():
    state = _state()
    typer.echo(f"api_key: {mask_api_key(state['auth'].get('api_key'))}")
    typer.echo(f"runtime_url: {state['auth'].get('runtime_url') or '(not set)'}")


@context_app.command("show")
def context_show():
    state = _state()
    typer.echo(json.dumps(state["context"], indent=2))


@context_app.command("set")
def context_set(
    environment: Optional[str] = typer.Option(None, "--env", help="Environment key/id"),
    domain: Optional[str] = typer.Option(None, help="Domain id (openehr, fhir, ...)"),
    strategy: Optional[str] = typer.Option(None, help="Strategy id"),
    runtime_url: Optional[str] = typer.Option(None, help="Runtime URL for this context"),
    data_mode: Optional[str] = typer.Option(None, "--data-mode", help="Data mode/profile"),
    source: Optional[str] = typer.Option(None, "--source", help="Default source reference"),
    sink: Optional[str] = typer.Option(None, "--sink", help="Default sink reference"),
):
    if not any([environment, domain, strategy, runtime_url, data_mode, source, sink]):
        raise typer.BadParameter("Provide at least one context field to set.")
    state = _state()
    if environment is not None:
        state["context"]["environment"] = environment
    if domain is not None:
        state["context"]["domain"] = domain
    if strategy is not None:
        state["context"]["strategy"] = strategy
    if runtime_url is not None:
        state["context"]["runtime_url"] = runtime_url.rstrip("/")
    if data_mode is not None:
        state["context"]["data_mode"] = data_mode
    if source is not None:
        state["context"]["source"] = source
    if sink is not None:
        state["context"]["sink"] = sink
    _save(state)


@context_app.command("clear")
def context_clear():
    state = _state()
    state["context"] = {
        "environment": None,
        "domain": None,
        "strategy": None,
        "runtime_url": None,
        "data_mode": None,
        "source": None,
        "sink": None,
    }
    _save(state)


@core_app.command("health")
def core_health(
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    status, data = _http_json("GET", f"{base.rstrip('/')}/health", _resolve_api_key(api_key))
    typer.echo(json.dumps({"status": status, "url": base, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@core_app.command("api")
def core_api(ctx: typer.Context):
    """Run the API server command."""
    _run_module("kehrnel.api.app", ctx.args)



@env_app.command("list")
def env_list(
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    status, data = _http_json("GET", f"{base.rstrip('/')}/environments", _resolve_api_key(api_key))
    typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@env_app.command("show")
def env_show(
    env: Optional[str] = typer.Option(None, "--env", help="Environment key/id"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    env_id = _require_env(env)
    status, data = _http_json("GET", f"{base.rstrip('/')}/environments/{env_id}", _resolve_api_key(api_key))
    typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@env_app.command("create")
def env_create(
    env: str = typer.Option(..., "--env", help="Environment key/id"),
    name: Optional[str] = typer.Option(None, "--name"),
    description: Optional[str] = typer.Option(None, "--description"),
    bindings_ref: Optional[str] = typer.Option(None, "--bindings-ref"),
    metadata: Optional[Path] = typer.Option(None, "--metadata", help="Metadata JSON/YAML file"),
    set_items: Optional[List[str]] = typer.Option(None, "--set", help="Metadata KEY=VALUE (repeatable)"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    metadata_payload = _load_json_or_yaml(metadata) if metadata else {}
    metadata_payload.update(_parse_kv_pairs(set_items or []))
    payload = {
        "env_id": env,
        "name": name,
        "description": description,
        "bindings_ref": bindings_ref,
        "metadata": metadata_payload,
    }
    status, data = _http_json("POST", f"{base.rstrip('/')}/environments", _resolve_api_key(api_key), payload)
    typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@env_app.command("update")
def env_update(
    env: str = typer.Option(..., "--env", help="Environment key/id"),
    name: Optional[str] = typer.Option(None, "--name"),
    description: Optional[str] = typer.Option(None, "--description"),
    bindings_ref: Optional[str] = typer.Option(None, "--bindings-ref"),
    metadata: Optional[Path] = typer.Option(None, "--metadata", help="Metadata JSON/YAML file"),
    set_items: Optional[List[str]] = typer.Option(None, "--set", help="Metadata KEY=VALUE (repeatable)"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    payload: Dict[str, Any] = {}
    if name is not None:
        payload["name"] = name
    if description is not None:
        payload["description"] = description
    if bindings_ref is not None:
        payload["bindings_ref"] = bindings_ref
    if metadata or set_items:
        metadata_payload = _load_json_or_yaml(metadata) if metadata else {}
        metadata_payload.update(_parse_kv_pairs(set_items or []))
        payload["metadata"] = metadata_payload
    status, data = _http_json("PATCH", f"{base.rstrip('/')}/environments/{env}", _resolve_api_key(api_key), payload)
    typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@env_app.command("delete")
def env_delete(
    env: str = typer.Option(..., "--env", help="Environment key/id"),
    force: bool = typer.Option(False, "--force", help="Delete active activations and history too"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    url = f"{base.rstrip('/')}/environments/{env}"
    if force:
        url += "?force=true"
    status, data = _http_json("DELETE", url, _resolve_api_key(api_key))
    typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@env_app.command("endpoints")
def env_endpoints(
    env: Optional[str] = typer.Option(None, "--env", help="Environment key/id"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    env_id = _require_env(env)
    status, data = _http_json("GET", f"{base.rstrip('/')}/environments/{env_id}/endpoints", _resolve_api_key(api_key))
    typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@env_app.command("activate")
def env_activate(
    env: Optional[str] = typer.Option(None, "--env", help="Environment key/id"),
    domain: Optional[str] = typer.Option(None, "--domain", help="Domain id (openehr, fhir, ...)"),
    strategy_id: Optional[str] = typer.Option(None, "--strategy", help="Strategy id"),
    version: str = typer.Option("latest", "--version"),
    config_file: Optional[Path] = typer.Option(None, "--config", help="Config JSON/YAML file"),
    bindings_ref: Optional[str] = typer.Option(None, "--bindings-ref", help="Bindings reference (recommended/required when auth enabled)"),
    allow_plaintext_bindings: bool = typer.Option(False, "--allow-plaintext-bindings", help="Dev/test only: allow plaintext bindings payload"),
    bindings_file: Optional[Path] = typer.Option(None, "--bindings", help="Bindings JSON/YAML file (dev/test only)"),
    force: bool = typer.Option(False, "--force", help="Force activation even if already active"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    """
    Activate a strategy in an environment (runtime API).

    Wrapper for POST /environments/{env}/activate.
    """
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    env_id = _require_env(env)
    chosen_domain = _require_domain(domain)
    chosen_strategy = _require_strategy(strategy_id)

    config = _load_json_or_yaml(config_file) if config_file else {}
    bindings = _load_json_or_yaml(bindings_file) if bindings_file else {}

    payload = {
        "domain": chosen_domain,
        "strategy_id": chosen_strategy,
        "version": version,
        "config": config,
        "bindings_ref": bindings_ref,
        "allow_plaintext_bindings": bool(allow_plaintext_bindings),
        "bindings": bindings,
        "force": bool(force),
    }
    status, data = _http_json("POST", f"{base.rstrip('/')}/environments/{env_id}/activate", _resolve_api_key(api_key), payload)
    typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@env_app.command("op")
def env_op(
    op: str = typer.Argument(..., help="Operation name (strategy-specific)"),
    env: Optional[str] = typer.Option(None, "--env", help="Environment key/id"),
    domain: Optional[str] = typer.Option(None, "--domain", help="Domain id (openehr, fhir, ...)"),
    payload_file: Optional[Path] = typer.Option(None, "--payload", help="Payload JSON/YAML file"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    env_id = _require_env(env)
    chosen_domain = _require_domain(domain)
    payload = _load_json_or_yaml(payload_file) if payload_file else {}
    status, data = _http_json(
        "POST",
        f"{base.rstrip('/')}/environments/{env_id}/activations/{chosen_domain}/ops/{op}",
        _resolve_api_key(api_key),
        payload,
    )
    typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@env_app.command("compile-query")
def env_compile_query(
    env: Optional[str] = typer.Option(None, "--env", help="Environment key/id"),
    domain: Optional[str] = typer.Option(None, "--domain", help="Domain id (openehr, fhir, ...)"),
    aql_file: Optional[Path] = typer.Option(None, "--aql", help="AQL file (convenience for openEHR)"),
    payload_file: Optional[Path] = typer.Option(None, "--payload", help="Payload JSON/YAML file (generic)"),
    debug: bool = typer.Option(False, "--debug"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    env_id = _require_env(env)
    chosen_domain = _require_domain(domain)
    if payload_file:
        payload = _load_json_or_yaml(payload_file)
    else:
        payload = {"domain": chosen_domain}
        if aql_file:
            payload["aql"] = aql_file.read_text(encoding="utf-8")
    url = f"{base.rstrip('/')}/environments/{env_id}/compile_query"
    if debug:
        url += "?debug=true"
    status, data = _http_json("POST", url, _resolve_api_key(api_key), payload)
    typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@env_app.command("query")
def env_query(
    env: Optional[str] = typer.Option(None, "--env", help="Environment key/id"),
    domain: Optional[str] = typer.Option(None, "--domain", help="Domain id (openehr, fhir, ...)"),
    aql_file: Optional[Path] = typer.Option(None, "--aql", help="AQL file (convenience for openEHR)"),
    payload_file: Optional[Path] = typer.Option(None, "--payload", help="Payload JSON/YAML file (generic)"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    env_id = _require_env(env)
    chosen_domain = _require_domain(domain)
    if payload_file:
        payload = _load_json_or_yaml(payload_file)
    else:
        payload = {"domain": chosen_domain}
        if aql_file:
            payload["aql"] = aql_file.read_text(encoding="utf-8")
    status, data = _http_json("POST", f"{base.rstrip('/')}/environments/{env_id}/query", _resolve_api_key(api_key), payload)
    typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    raise typer.Exit(0 if status < 400 else 1)


@strategy_app.command("use")
def strategy_use(strategy_id: str, domain: Optional[str] = typer.Option(None)):
    state = _state()
    state["context"]["strategy"] = strategy_id
    if domain:
        state["context"]["domain"] = domain
    _save(state)
    typer.echo(f"Selected strategy: {strategy_id}")


@strategy_app.command("current")
def strategy_current():
    state = _state()
    typer.echo(state["context"].get("strategy") or "(not set)")


@strategy_app.command("list")
def strategy_list(
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
    domain: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured.")
    status, data = _http_json("GET", f"{base.rstrip('/')}/strategies", _resolve_api_key(api_key))
    strategies = data.get("strategies") if isinstance(data, dict) else data
    if not isinstance(strategies, list):
        typer.echo(json.dumps({"status": status, "response": data}, indent=2))
        raise typer.Exit(1)
    domain_filter = (domain or "").strip().lower() if domain is not None else None
    for row in strategies:
        sid = row.get("id") or row.get("strategy_id")
        sdomain = row.get("domain")
        sdomain_norm = str(sdomain or "").strip().lower()
        if domain_filter and sdomain_norm != domain_filter:
            continue
        typer.echo(f"{sid}\t{sdomain}\tv{row.get('version', '?')}")


@strategy_app.command("build-search-index")
def strategy_build_search_index(
    env: Optional[str] = typer.Option(None, "--env", help="Environment key/id"),
    runtime_url: Optional[str] = typer.Option(None, help="Runtime base URL"),
    api_key: Optional[str] = typer.Option(None, help="API key"),
    domain: Optional[str] = typer.Option(None, help="Domain id (defaults to current context or openehr)"),
    strategy: Optional[str] = typer.Option(None, help="Strategy id (defaults to current context strategy)"),
    include_stored_source: bool = typer.Option(True, "--stored-source/--no-stored-source", help="Include storedSource.include in the generated definition"),
    out: Optional[Path] = typer.Option(None, "--out", help="Write only the generated definition JSON to a file"),
    json_output: bool = typer.Option(False, "--json", help="Print the full operation response instead of only the definition"),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured.")

    env_id = _require_env(env)
    selected_strategy = _require_strategy(strategy)
    selected_domain = (
        domain
        or _state()["context"].get("domain")
        or "openehr"
    )

    payload = {"include_stored_source": include_stored_source}
    run_body = {
        "operation": "build_search_index_definition",
        "payload": payload,
        "domain": selected_domain,
        "strategy_id": selected_strategy,
    }

    status, data = _http_json(
        "POST",
        f"{base.rstrip('/')}/environments/{env_id}/run",
        _resolve_api_key(api_key),
        run_body,
    )
    if status >= 400:
        typer.echo(json.dumps({"status": status, "response": data}, indent=2))
        raise typer.Exit(1)

    definition = data.get("definition") if isinstance(data, dict) else None
    if out is not None:
        if not isinstance(definition, dict):
            raise typer.BadParameter("Runtime did not return a definition object.")
        out.write_text(json.dumps(definition, indent=2) + "\n", encoding="utf-8")
        typer.echo(f"Wrote search index definition to {out}")

    if json_output:
        typer.echo(json.dumps({"status": status, "response": data}, indent=2))
    elif isinstance(definition, dict):
        typer.echo(json.dumps(definition, indent=2))
        warnings = data.get("warnings") if isinstance(data, dict) else None
        if isinstance(warnings, list) and warnings:
            typer.echo(json.dumps({"warnings": warnings}, indent=2))
    else:
        typer.echo(json.dumps({"status": status, "response": data}, indent=2))


@resource_app.command("list")
def resource_list(json_output: bool = typer.Option(False, "--json", help="Output as JSON")):
    state = _state()
    resources = state.get("resources") if isinstance(state, dict) else {}
    if not isinstance(resources, dict):
        resources = {}
    if json_output:
        typer.echo(json.dumps({"resources": resources}, indent=2))
        return
    if not resources:
        typer.echo("(no resources configured)")
        return
    for name, cfg in sorted(resources.items()):
        if not isinstance(cfg, dict):
            typer.echo(f"{name}\tunknown")
            continue
        rtype = cfg.get("type") or cfg.get("driver") or cfg.get("provider") or "unknown"
        target = cfg.get("uri") or cfg.get("path") or ""
        typer.echo(f"{name}\t{rtype}\t{target}")


@resource_app.command("show")
def resource_show(name: str):
    profile = _resolve_resource_profile(name)
    typer.echo(json.dumps({"name": name, "profile": profile}, indent=2))


@resource_app.command("add")
def resource_add(
    name: str = typer.Argument(..., help="Resource profile name"),
    rtype: Optional[str] = typer.Option(None, "--type", help="Resource type (file, mongo, s3, ...)"),
    uri: Optional[str] = typer.Option(None, "--uri", help="Connection or resource URI"),
    path: Optional[str] = typer.Option(None, "--path", help="Filesystem path (for file resources)"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    collection: Optional[str] = typer.Option(None, "--collection", help="Collection name"),
    fmt: Optional[str] = typer.Option(None, "--format", help="Data format (json, ndjson, parquet, ...)"),
    profile_file: Optional[Path] = typer.Option(None, "--from-file", help="Load profile JSON/YAML and merge"),
    set_values: list[str] = typer.Option([], "--set", help="Additional KEY=VALUE entries (JSON values accepted)"),
):
    if "://" in name or "/" in name:
        raise typer.BadParameter("Resource name must be a simple identifier (no URI or slash).")

    profile: Dict[str, Any] = {}
    if profile_file is not None:
        profile = _load_json_or_yaml(profile_file)

    if rtype is not None:
        profile["type"] = rtype
    if uri is not None:
        profile["uri"] = uri
    if path is not None:
        profile["path"] = path
        profile.setdefault("type", "file")
        profile.setdefault("uri", f"file://{path}")
    if db is not None:
        profile["database"] = db
    if collection is not None:
        profile["collection"] = collection
    if fmt is not None:
        profile["format"] = fmt

    if set_values:
        profile.update(_parse_kv_pairs(set_values))

    if not isinstance(profile, dict) or not profile:
        raise typer.BadParameter("Resource profile is empty. Provide --type/--uri/--from-file/--set.")

    state = _state()
    resources = state.setdefault("resources", {})
    if not isinstance(resources, dict):
        resources = {}
        state["resources"] = resources
    resources[name] = profile
    _save(state)
    typer.echo(f"Saved resource profile: {name}")


@resource_app.command("remove")
def resource_remove(name: str):
    state = _state()
    resources = state.get("resources")
    if not isinstance(resources, dict) or name not in resources:
        raise typer.BadParameter(f"Unknown resource profile: {name}")
    resources.pop(name, None)
    _save(state)
    typer.echo(f"Removed resource profile: {name}")


@resource_app.command("use")
def resource_use(
    source: Optional[str] = typer.Option(None, "--source", help="Resource profile name for default source"),
    sink: Optional[str] = typer.Option(None, "--sink", help="Resource profile name for default sink"),
):
    if source is None and sink is None:
        raise typer.BadParameter("Provide --source and/or --sink.")
    state = _state()
    if source is not None:
        _resolve_resource_profile(source)
        state["context"]["source"] = f"resource://{source}"
    if sink is not None:
        _resolve_resource_profile(sink)
        state["context"]["sink"] = f"resource://{sink}"
    _save(state)
    typer.echo(json.dumps({"source": state["context"].get("source"), "sink": state["context"].get("sink")}, indent=2))


@ops_app.command("list")
def op_list(
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
    domain: Optional[str] = typer.Option(None, help="Filter by domain"),
    strategy: Optional[str] = typer.Option(None, help="Filter by strategy id"),
    kind: Optional[str] = typer.Option(None, help="Filter by op kind"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured.")
    ops = _fetch_ops_catalog(base, _resolve_api_key(api_key))
    domain_filter = (domain or "").strip().lower() if domain is not None else None
    strategy_filter = (strategy or "").strip() if strategy is not None else None
    kind_filter = (kind or "").strip().lower() if kind is not None else None

    rows: list[Dict[str, Any]] = []
    for row in ops:
        name = row.get("name")
        strategy_id = row.get("strategy_id")
        row_domain = row.get("domain")
        row_kind = row.get("kind")
        if not name:
            continue
        if domain_filter and str(row_domain or "").strip().lower() != domain_filter:
            continue
        if strategy_filter and str(strategy_id or "").strip() != strategy_filter:
            continue
        if kind_filter and str(row_kind or "").strip().lower() != kind_filter:
            continue
        rows.append(row)

    if json_output:
        typer.echo(json.dumps({"ops": rows}, indent=2))
        return

    if not rows:
        typer.echo("(no ops matched)")
        return

    for row in rows:
        typer.echo(
            f"{row.get('name')}\t{row.get('kind') or '?'}\t{row.get('domain') or '?'}\t{row.get('strategy_id') or '?'}"
        )


@ops_app.command("schema")
def op_schema(
    name: str = typer.Argument(..., help="Operation name"),
    strategy: Optional[str] = typer.Option(None, help="Strategy id (defaults to current context strategy)"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured.")

    selected_strategy = strategy or _state()["context"].get("strategy")
    if not selected_strategy:
        # fallback: infer strategy if op name is unique across catalog
        candidates = [row for row in _fetch_ops_catalog(base, _resolve_api_key(api_key)) if row.get("name") == name]
        strategy_ids = sorted({str(row.get("strategy_id")) for row in candidates if row.get("strategy_id")})
        if len(strategy_ids) == 1:
            selected_strategy = strategy_ids[0]
        elif len(strategy_ids) > 1:
            raise typer.BadParameter(
                f"Operation '{name}' exists in multiple strategies: {', '.join(strategy_ids)}. Pass --strategy."
            )
        else:
            raise typer.BadParameter(
                f"Operation '{name}' not found. Use `kehrnel op list` to inspect available operations."
            )

    status, manifest = _http_json(
        "GET",
        f"{base.rstrip('/')}/strategies/{selected_strategy}",
        _resolve_api_key(api_key),
    )
    if status >= 400 or not isinstance(manifest, dict):
        typer.echo(json.dumps({"status": status, "response": manifest}, indent=2))
        raise typer.Exit(1)

    for op in manifest.get("ops") or []:
        if not isinstance(op, dict):
            continue
        if str(op.get("name")) != name:
            continue
        typer.echo(
            json.dumps(
                {
                    "strategy_id": manifest.get("id") or selected_strategy,
                    "domain": manifest.get("domain"),
                    "name": op.get("name"),
                    "kind": op.get("kind"),
                    "summary": op.get("summary"),
                    "input_schema": op.get("input_schema") or {},
                    "output_schema": op.get("output_schema") or {},
                },
                indent=2,
            )
        )
        return
    raise typer.BadParameter(f"Operation '{name}' not found in strategy '{selected_strategy}'.")


@ops_app.command("capabilities")
def op_capabilities(
    env: Optional[str] = typer.Option(None, "--env", help="Environment key/id"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
    include_schemas: bool = typer.Option(True, "--schemas/--no-schemas", help="Include operation input/output schemas"),
    domain: Optional[str] = typer.Option(None, help="Filter strategy operations by domain"),
    strategy: Optional[str] = typer.Option(None, help="Filter strategy operations by strategy id"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured.")
    env_id = _require_env(env)
    suffix = "?include_schemas=true" if include_schemas else "?include_schemas=false"
    status, data = _http_json(
        "GET",
        f"{base.rstrip('/')}/environments/{env_id}/capabilities{suffix}",
        _resolve_api_key(api_key),
    )
    if status >= 400 or not isinstance(data, dict):
        typer.echo(json.dumps({"status": status, "response": data}, indent=2))
        raise typer.Exit(1)

    if domain is None and strategy is None:
        if json_output:
            typer.echo(json.dumps(data, indent=2))
        else:
            std = (data.get("operations") or {}).get("standard") or []
            strat = (data.get("operations") or {}).get("strategy") or []
            typer.echo(f"env={data.get('env_id')} standard_ops={len(std)} strategy_ops={len(strat)}")
            for row in strat:
                if not isinstance(row, dict):
                    continue
                typer.echo(
                    f"{row.get('name')}\t{row.get('kind') or '?'}\t{row.get('domain') or '?'}\t{row.get('strategy_id') or '?'}"
                )
        return

    domain_filter = (domain or "").strip().lower() if domain is not None else None
    strategy_filter = (strategy or "").strip() if strategy is not None else None
    rows = []
    for row in (data.get("operations") or {}).get("strategy") or []:
        if not isinstance(row, dict):
            continue
        if domain_filter and str(row.get("domain") or "").strip().lower() != domain_filter:
            continue
        if strategy_filter and str(row.get("strategy_id") or "").strip() != strategy_filter:
            continue
        rows.append(row)

    if json_output:
        typer.echo(json.dumps({"env_id": env_id, "strategy_operations": rows}, indent=2))
        return
    if not rows:
        typer.echo("(no strategy operations matched)")
        return
    for row in rows:
        typer.echo(
            f"{row.get('name')}\t{row.get('kind') or '?'}\t{row.get('domain') or '?'}\t{row.get('strategy_id') or '?'}"
        )


@app.command("run")
def run_operation(
    operation: str = typer.Argument(..., help="Operation id (query, transform, ingest, plan, apply, or strategy op)"),
    env: Optional[str] = typer.Option(None, "--env", help="Environment key/id"),
    domain: Optional[str] = typer.Option(None, "--domain", help="Domain id (openehr, fhir, ...)"),
    strategy: Optional[str] = typer.Option(None, "--strategy", help="Strategy id (optional metadata for payload)"),
    data_mode: Optional[str] = typer.Option(None, "--data-mode", help="Data mode/profile"),
    source_ref: Optional[str] = typer.Option(None, "--from", help="Source reference (file://, mongo://, resource://name, path)"),
    sink_ref: Optional[str] = typer.Option(None, "--to", help="Sink reference (file://, mongo://, resource://name, path)"),
    payload_file: Optional[Path] = typer.Option(None, "--payload", help="Payload JSON/YAML file"),
    set_values: list[str] = typer.Option([], "--set", help="Payload KEY=VALUE entries (JSON values accepted)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Set payload.dry_run=true"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug mode where supported (compile_query)"),
    runtime_url: Optional[str] = typer.Option(None),
    api_key: Optional[str] = typer.Option(None),
):
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured.")
    env_id = _require_env(env)

    payload = _load_json_or_yaml(payload_file) if payload_file else {}
    if not isinstance(payload, dict):
        raise typer.BadParameter("Payload must be a JSON/YAML object.")
    if set_values:
        payload.update(_parse_kv_pairs(set_values))

    state = _state()
    selected_domain = (
        domain
        or (payload.get("domain") if isinstance(payload, dict) else None)
        or state["context"].get("domain")
    )
    selected_strategy = strategy or state["context"].get("strategy")
    selected_mode = _resolve_data_mode(data_mode)
    selected_source = source_ref or state["context"].get("source")
    selected_sink = sink_ref or state["context"].get("sink")

    if selected_domain and "domain" not in payload:
        payload["domain"] = selected_domain
    if selected_strategy and "strategy_id" not in payload and "strategy" not in payload:
        payload["strategy_id"] = selected_strategy
    if selected_mode and "data_mode" not in payload:
        payload["data_mode"] = selected_mode
    if selected_source and "source" not in payload:
        payload["source"] = _resolve_data_ref(selected_source)
    if selected_sink and "sink" not in payload:
        payload["sink"] = _resolve_data_ref(selected_sink)
    if dry_run:
        payload["dry_run"] = True

    resolved_source = _resolve_data_ref(selected_source) if selected_source else None
    resolved_sink = _resolve_data_ref(selected_sink) if selected_sink else None
    run_body: Dict[str, Any] = {
        "operation": operation,
        "payload": payload,
    }
    if selected_domain:
        run_body["domain"] = selected_domain
    if selected_strategy:
        run_body["strategy_id"] = selected_strategy
    if selected_mode:
        run_body["data_mode"] = selected_mode
    if resolved_source is not None:
        run_body["source"] = resolved_source
    if resolved_sink is not None:
        run_body["sink"] = resolved_sink
    if debug:
        run_body["debug"] = True

    auth = _resolve_api_key(api_key)
    status, data = _http_json("POST", f"{base.rstrip('/')}/environments/{env_id}/run", auth, run_body)
    if status < 400:
        typer.echo(json.dumps({"status": status, "operation": operation, "response": data}, indent=2))
        raise typer.Exit(0)

    # Fallback only when /run endpoint itself is unavailable on older runtimes.
    detail = str((data or {}).get("detail") or "").strip().lower() if isinstance(data, dict) else ""
    run_endpoint_missing = status in (404, 405) and detail in {"not found", "method not allowed"}
    if not run_endpoint_missing:
        typer.echo(json.dumps({"status": status, "operation": operation, "response": data}, indent=2))
        raise typer.Exit(1)

    # Backward-compatible fallback for runtimes that do not expose /run.
    op_key = operation.strip().lower().replace("-", "_")
    direct = {"transform", "ingest", "plan", "apply", "query", "compile_query"}
    if op_key in direct:
        if op_key in {"query", "compile_query"} and not payload.get("domain"):
            raise typer.BadParameter("domain is required for query/compile_query. Use --domain or context.")
        url = f"{base.rstrip('/')}/environments/{env_id}/{op_key}"
        if op_key == "compile_query" and debug:
            url += "?debug=true"
        status, data = _http_json("POST", url, auth, payload)
    else:
        chosen_domain = str(payload.get("domain") or "").strip().lower()
        if not chosen_domain:
            raise typer.BadParameter("domain is required for strategy operations. Use --domain or context.")
        url = f"{base.rstrip('/')}/environments/{env_id}/activations/{chosen_domain}/ops/{operation}"
        status, data = _http_json("POST", url, auth, payload)

    typer.echo(
        json.dumps(
            {
                "status": status,
                "operation": operation,
                "response": data,
                "fallback": True,
            },
            indent=2,
        )
    )
    raise typer.Exit(0 if status < 400 else 1)


@domain_app.command("list")
def domain_list():
    typer.echo("openehr")
    typer.echo("fhir")


@domain_app.command(
    "openehr",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def domain_openehr(
    ctx: typer.Context,
    action: str = typer.Argument(..., help="validate|generate|transform|ingest|map|identify"),
):
    mapping = {
        "validate": "kehrnel.cli.validate",
        "generate": "kehrnel.cli.generate",
        "transform": "kehrnel.cli.transform",
        "ingest": "kehrnel.cli.ingest",
        "map": "kehrnel.cli.map",
        "identify": "kehrnel.cli.identify",
    }
    module = mapping.get(action)
    if not module:
        raise typer.BadParameter(f"Unsupported openEHR action: {action}")
    _run_module(module, ctx.args)


@common_app.command(
    "transform",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def common_transform(
    ctx: typer.Context,
    strategy: Optional[str] = typer.Option(None, help="Strategy override"),
    domain: Optional[str] = typer.Option(None, help="Domain override"),
):
    selected_strategy = _require_strategy(strategy)
    selected_domain = _require_domain(domain)
    # Print to stderr so stdout can be used for JSON or file output.
    typer.echo(f"Using strategy={selected_strategy} domain={selected_domain}", err=True)
    _run_module("kehrnel.cli.transform", ctx.args)


@common_app.command(
    "ingest",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def common_ingest(
    ctx: typer.Context,
    strategy: Optional[str] = typer.Option(None, help="Strategy override"),
    domain: Optional[str] = typer.Option(None, help="Domain override"),
):
    selected_strategy = _require_strategy(strategy)
    selected_domain = _require_domain(domain)
    typer.echo(f"Using strategy={selected_strategy} domain={selected_domain}", err=True)
    _run_module("kehrnel.cli.ingest", ctx.args)


@common_app.command(
    "validate",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def common_validate(
    ctx: typer.Context,
    strategy: Optional[str] = typer.Option(None, help="Strategy override"),
    domain: Optional[str] = typer.Option(None, help="Domain override"),
):
    selected_strategy = _require_strategy(strategy)
    selected_domain = _require_domain(domain)
    typer.echo(f"Using strategy={selected_strategy} domain={selected_domain}", err=True)
    _run_module("kehrnel.cli.validate", ctx.args)


@common_app.command(
    "generate",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def common_generate(
    ctx: typer.Context,
    strategy: Optional[str] = typer.Option(None, help="Strategy override"),
    domain: Optional[str] = typer.Option(None, help="Domain override"),
):
    selected_strategy = _require_strategy(strategy)
    selected_domain = _require_domain(domain)
    typer.echo(f"Using strategy={selected_strategy} domain={selected_domain}", err=True)
    _run_module("kehrnel.cli.generate", ctx.args)


@common_app.command(
    "map",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def common_map(
    ctx: typer.Context,
    strategy: Optional[str] = typer.Option(None, help="Strategy override"),
    domain: Optional[str] = typer.Option(None, help="Domain override"),
):
    selected_strategy = _require_strategy(strategy)
    selected_domain = _require_domain(domain)
    typer.echo(f"Using strategy={selected_strategy} domain={selected_domain}", err=True)
    _run_module("kehrnel.cli.map", ctx.args)


@common_app.command(
    "identify",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def common_identify(ctx: typer.Context):
    _run_module("kehrnel.cli.identify", ctx.args)


@common_app.command(
    "bundles",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def common_bundles(ctx: typer.Context):
    _run_module("kehrnel.cli.bundles", ctx.args)


@common_app.command(
    "validate-pack",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def common_validate_pack(ctx: typer.Context):
    _run_module("kehrnel.cli.validate_pack", ctx.args)

@common_app.command(
    "map-skeleton",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def common_map_skeleton(ctx: typer.Context):
    _run_module("kehrnel.cli.map_skeleton", ctx.args)


@app.command("version")
def version():
    from kehrnel import __version__

    typer.echo(__version__)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
