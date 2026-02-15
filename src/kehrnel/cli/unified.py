"""Unified kehrnel CLI (auth/context/core/common/domain/strategy)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Optional

import typer
import yaml

from kehrnel.cli.state import load_cli_state, save_cli_state, mask_api_key

app = typer.Typer(help="Unified {kehrnel} CLI")
auth_app = typer.Typer(help="Authenticate once and reuse credentials")
context_app = typer.Typer(help="Set/get runtime context (env/domain/strategy)")
core_app = typer.Typer(help="Core runtime operations")
env_app = typer.Typer(help="Environment-scoped runtime operations")
common_app = typer.Typer(help="Cross-domain shared operations")
domain_app = typer.Typer(help="Domain-scoped operations")
strategy_app = typer.Typer(help="Strategy-scoped operations")

app.add_typer(auth_app, name="auth")
app.add_typer(context_app, name="context")
app.add_typer(core_app, name="core")
core_app.add_typer(env_app, name="env")
app.add_typer(common_app, name="common")
app.add_typer(domain_app, name="domain")
app.add_typer(strategy_app, name="strategy")


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
):
    if not any([environment, domain, strategy, runtime_url]):
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
    _save(state)


@context_app.command("clear")
def context_clear():
    state = _state()
    state["context"] = {
        "environment": None,
        "domain": None,
        "strategy": None,
        "runtime_url": None,
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
    for row in strategies:
        sid = row.get("id") or row.get("strategy_id")
        sdomain = row.get("domain")
        if domain and sdomain != domain:
            continue
        typer.echo(f"{sid}\t{sdomain}\tv{row.get('version', '?')}")


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
