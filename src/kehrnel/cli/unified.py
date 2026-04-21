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
from rich import box
from rich.console import Console
from rich.json import JSON as RichJSON
from rich.panel import Panel
from rich.table import Table

# Load .env.local (then .env as fallback) so env vars like MONGODB_URI are
# available to CLI commands without requiring `source .env.local` in the shell.
load_dotenv(find_dotenv(".env.local", usecwd=True), override=False)
load_dotenv(find_dotenv(".env", usecwd=True), override=False)

from kehrnel.cli.state import load_cli_state, save_cli_state, mask_api_key

LOCAL_GUIDE_URL = os.getenv("KEHRNEL_GUIDE_URL", "http://localhost:8080/guide")

TOP_LEVEL_HELP = """Unified kehrnel CLI

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

\b
Local guide (when the docs site is running):
  """ + LOCAL_GUIDE_URL + """
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

app = typer.Typer(help=TOP_LEVEL_HELP, rich_markup_mode="rich")
auth_app = typer.Typer(help="Authenticate once and reuse credentials", rich_markup_mode="rich")
context_app = typer.Typer(help="Set/get runtime context (env/domain/strategy/data-mode/source/sink)", rich_markup_mode="rich")
core_app = typer.Typer(help=CORE_HELP, rich_markup_mode="rich")
env_app = typer.Typer(help=ENV_HELP, rich_markup_mode="rich")
common_app = typer.Typer(help=COMMON_HELP, rich_markup_mode="rich")
domain_app = typer.Typer(help="Domain-scoped operations", rich_markup_mode="rich")
strategy_app = typer.Typer(help="Strategy-scoped operations", rich_markup_mode="rich")
resource_app = typer.Typer(help=RESOURCE_HELP, rich_markup_mode="rich")
ops_app = typer.Typer(help=OPS_HELP, rich_markup_mode="rich")

stdout_console = Console()
stderr_console = Console(stderr=True)

app.add_typer(auth_app, name="auth")
app.add_typer(context_app, name="context")
app.add_typer(core_app, name="core")
core_app.add_typer(env_app, name="env")
app.add_typer(common_app, name="common")
app.add_typer(domain_app, name="domain")
app.add_typer(strategy_app, name="strategy")
app.add_typer(resource_app, name="resource")
app.add_typer(ops_app, name="op")


@app.callback(invoke_without_command=True)
def app_callback(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit(0)


def _state() -> dict:
    return load_cli_state()


def _save(state: dict) -> None:
    path = save_cli_state(state)
    typer.echo(f"State updated: {path}")


def _env_flag(name: str) -> bool:
    return str(os.getenv(name, "")).strip().lower() in {"1", "true", "yes", "on"}


def _rich_stdout_enabled() -> bool:
    if _env_flag("KEHRNEL_CLI_PLAIN"):
        return False
    if _env_flag("KEHRNEL_CLI_RICH"):
        return True
    return bool(getattr(sys.stdout, "isatty", lambda: False)())


def _rich_stderr_enabled() -> bool:
    if _env_flag("KEHRNEL_CLI_PLAIN"):
        return False
    if _env_flag("KEHRNEL_CLI_RICH"):
        return True
    return bool(getattr(sys.stderr, "isatty", lambda: False)())


def _dump_json(data: Any) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False)


def _emit_json(data: Any) -> None:
    typer.echo(_dump_json(data))


def _deep_get(obj: Any, *path: str) -> Any:
    cur = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _extract_activation_record(data: Any, domain: Optional[str] = None, strategy_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    activation = _deep_get(data, "activation") or _deep_get(data, "response", "activation")
    if isinstance(activation, dict):
        return activation

    activations = _deep_get(data, "activations") or _deep_get(data, "response", "activations")
    if not isinstance(activations, dict):
        return None
    if domain and isinstance(activations.get(domain), dict):
        return activations.get(domain)
    if strategy_id:
        for item in activations.values():
            if isinstance(item, dict) and item.get("strategy_id") == strategy_id:
                return item
    if len(activations) == 1:
        only = next(iter(activations.values()))
        return only if isinstance(only, dict) else None
    return None


def _extract_database_name(data: Any, domain: Optional[str] = None, strategy_id: Optional[str] = None) -> Optional[str]:
    activation = _extract_activation_record(data, domain=domain, strategy_id=strategy_id)
    db_name = _deep_get(activation or {}, "bindings_meta", "db", "database")
    if isinstance(db_name, str) and db_name.strip():
        return db_name.strip()
    db_name = _deep_get(data, "bindings_meta", "db", "database") or _deep_get(data, "response", "bindings_meta", "db", "database")
    if isinstance(db_name, str) and db_name.strip():
        return db_name.strip()
    return None


def _fetch_env_snapshot(base: str, env_id: Optional[str], api_key: Optional[str]) -> Optional[Dict[str, Any]]:
    if not env_id or not _rich_stdout_enabled():
        return None
    status, data = _http_json("GET", f"{base.rstrip('/')}/environments/{env_id}", api_key)
    if status >= 400 or not isinstance(data, dict):
        return None
    return data


def _resolve_cli_context(
    *,
    base: Optional[str],
    env_id: Optional[str],
    domain: Optional[str],
    strategy_id: Optional[str],
    api_key: Optional[str],
    snapshot: Optional[Dict[str, Any]] = None,
    response_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    ctx: Dict[str, str] = {}
    if base:
        ctx["runtime"] = base.rstrip("/")
    if env_id:
        ctx["environment"] = env_id
    if domain:
        ctx["domain"] = domain
    if strategy_id:
        ctx["strategy"] = strategy_id

    source = response_data if isinstance(response_data, dict) else {}
    activation = _extract_activation_record(source, domain=domain, strategy_id=strategy_id)
    if not activation and isinstance(snapshot, dict):
        activation = _extract_activation_record(snapshot, domain=domain, strategy_id=strategy_id)

    if isinstance(activation, dict):
        if not ctx.get("domain") and isinstance(activation.get("domain"), str):
            ctx["domain"] = activation["domain"]
        if not ctx.get("strategy") and isinstance(activation.get("strategy_id"), str):
            ctx["strategy"] = activation["strategy_id"]
        version = activation.get("strategy_version") or activation.get("version")
        if isinstance(version, str) and version.strip():
            ctx["version"] = version.strip()
        db_name = _deep_get(activation, "bindings_meta", "db", "database")
        if isinstance(db_name, str) and db_name.strip():
            ctx["database"] = db_name.strip()
    return ctx


def _status_style(status: int) -> str:
    if status < 300:
        return "green"
    if status < 400:
        return "yellow"
    return "red"


def _friendly_domain_label(value: Optional[str]) -> str:
    text = str(value or "").strip()
    lower = text.lower()
    if lower == "openehr":
        return "openEHR"
    if lower == "fhir":
        return "FHIR"
    return text or "—"


def _emit_rich_kv_panel(title: str, rows: List[tuple[str, str]], *, border_style: str = "cyan") -> None:
    if not rows:
        return
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    for label, value in rows:
        grid.add_row(label, value)
    stdout_console.print(Panel.fit(grid, title=title, border_style=border_style, box=box.ROUNDED))


def _emit_rich_table(
    title: str,
    columns: List[str],
    rows: List[List[str]],
    *,
    context_rows: Optional[List[tuple[str, str]]] = None,
    empty_message: str = "(no rows)",
    guide_lines: Optional[List[str]] = None,
) -> None:
    header_rows = list(context_rows or [])
    header_rows.append(("items", str(len(rows))))
    _emit_rich_kv_panel(title, header_rows, border_style="cyan")

    if not rows:
        stdout_console.print(Panel.fit(empty_message, border_style="yellow", box=box.ROUNDED))
    else:
        table = Table(box=box.SIMPLE_HEAVY)
        for column in columns:
            table.add_column(column)
        for row in rows:
            table.add_row(*row)
        stdout_console.print(table)

    if guide_lines:
        guide_table = Table.grid(padding=(0, 1))
        for line in guide_lines:
            guide_table.add_row(f"[dim]-[/dim] {line}")
        stdout_console.print(Panel.fit(guide_table, title="More Info", border_style="blue", box=box.ROUNDED))


def _build_summary_rows(kind: str, data: Any, operation: Optional[str] = None, out_path: Optional[Path] = None) -> List[tuple[str, str]]:
    rows: List[tuple[str, str]] = []
    result = _deep_get(data, "result") or _deep_get(data, "response", "result")
    initialization = _deep_get(data, "initialization") or _deep_get(data, "response", "initialization")
    activation = _extract_activation_record(data)

    if kind == "health":
        if isinstance(data, dict):
            for key in ("ok", "status", "service", "version"):
                if key in data and data.get(key) is not None:
                    rows.append((key.replace("_", " "), str(data.get(key))))
        return rows

    if kind == "activate" and isinstance(activation, dict):
        rows.append(("strategy", str(activation.get("strategy_id") or "—")))
        rows.append(("version", str(activation.get("strategy_version") or activation.get("version") or "—")))
        rows.append(("activation id", str(activation.get("activation_id") or "—")))
        rows.append(("database", str(_deep_get(activation, "bindings_meta", "db", "database") or "—")))
        if isinstance(initialization, dict):
            created = _deep_get(initialization, "artifacts", "created") or []
            warnings = _deep_get(initialization, "artifacts", "warnings") or []
            dict_seeded = _deep_get(initialization, "dictionaries", "seeded") or {}
            rows.append(("artifacts created", str(len(created) if isinstance(created, list) else 0)))
            rows.append(("artifact warnings", str(len(warnings) if isinstance(warnings, list) else 0)))
            if isinstance(dict_seeded, dict):
                rows.append(("dictionary seeds", ", ".join(f"{k}={v}" for k, v in dict_seeded.items()) or "0"))
        return rows

    if kind == "env-show" and isinstance(data, dict):
        environment = data.get("environment") or {}
        activations = data.get("activations") or {}
        rows.append(("name", str(environment.get("name") or environment.get("env_id") or data.get("env_id") or "—")))
        rows.append(("activation domains", str(len(activations) if isinstance(activations, dict) else 0)))
        if isinstance(environment.get("updated_at"), str):
            rows.append(("updated", environment["updated_at"]))
        if isinstance(environment.get("bindings_ref"), str) and environment.get("bindings_ref"):
            rows.append(("bindings ref", environment["bindings_ref"]))
        return rows

    if kind == "query" and isinstance(result, dict):
        plan = _deep_get(result, "explain", "plan") or {}
        rows_value = result.get("rows")
        rows.append(("engine", str(result.get("engine_used") or result.get("engine") or plan.get("engine") or "—")))
        rows.append(("scope", str(plan.get("scope") or _deep_get(result, "explain", "scope") or "—")))
        rows.append(("collection", str(plan.get("collection") or "—")))
        rows.append(("rows", str(len(rows_value) if isinstance(rows_value, list) else 0)))
        warnings = _deep_get(result, "explain", "warnings") or plan.get("warnings") or []
        rows.append(("warnings", str(len(warnings) if isinstance(warnings, list) else 0)))
        return rows

    if kind == "compile-query" and isinstance(result, dict):
        plan = result.get("plan") or {}
        explain = plan.get("explain") or {}
        rows.append(("engine", str(result.get("engine") or plan.get("engine") or "—")))
        rows.append(("scope", str(plan.get("scope") or explain.get("scope") or "—")))
        rows.append(("collection", str(plan.get("collection") or "—")))
        rows.append(("stage0", str(explain.get("stage0") or "—")))
        warnings = plan.get("warnings") or explain.get("warnings") or []
        rows.append(("warnings", str(len(warnings) if isinstance(warnings, list) else 0)))
        return rows

    if kind == "build-search-index":
        definition = _deep_get(data, "definition")
        if not isinstance(definition, dict):
            definition = _deep_get(data, "result", "definition") or _deep_get(data, "response", "result", "definition")
        if isinstance(definition, dict):
            fields = _deep_get(definition, "mappings", "fields")
            rows.append(("mapped fields", str(len(fields) if isinstance(fields, dict) else 0)))
        warnings = _deep_get(data, "warnings") or _deep_get(data, "result", "warnings") or _deep_get(data, "response", "result", "warnings") or []
        rows.append(("warnings", str(len(warnings) if isinstance(warnings, list) else 0)))
        if out_path is not None:
            rows.append(("output file", str(out_path)))
        return rows

    if kind == "run":
        if isinstance(data, dict):
            dispatch = data.get("dispatch") or {}
            if operation:
                rows.append(("operation", operation))
            if isinstance(dispatch, dict):
                if dispatch.get("scope") is not None:
                    rows.append(("dispatch scope", str(dispatch.get("scope"))))
                if dispatch.get("op") is not None:
                    rows.append(("dispatch op", str(dispatch.get("op"))))
        if isinstance(result, dict):
            if operation == "ingest":
                rows.append(("processed", str(result.get("processed", "—"))))
                inserted = result.get("inserted_counts") or {}
                if isinstance(inserted, dict):
                    rows.append(("base inserts", str(inserted.get("base", 0))))
                    rows.append(("search inserts", str(inserted.get("search", 0))))
            elif operation == "build_search_index_definition":
                rows.append(("result", "definition generated"))
            elif result.get("ok") is not None:
                rows.append(("ok", str(result.get("ok"))))
        return rows

    if isinstance(data, dict):
        if data.get("ok") is not None:
            rows.append(("ok", str(data.get("ok"))))
        if data.get("env_id") is not None:
            rows.append(("env id", str(data.get("env_id"))))
    return rows


def _extract_rows_preview(data: Any) -> Optional[List[Dict[str, Any]]]:
    result = _deep_get(data, "result") or _deep_get(data, "response", "result")
    rows = result.get("rows") if isinstance(result, dict) else None
    if isinstance(rows, list) and rows and all(isinstance(item, dict) for item in rows):
        return rows[:5]
    return None


def _build_guidance(kind: str, *, env_id: Optional[str], domain: Optional[str], strategy_id: Optional[str], operation: Optional[str] = None) -> List[str]:
    lines: List[str] = []
    if kind == "activate" and env_id and domain:
        lines.append(f"kehrnel core env show --env {env_id}")
        lines.append(f"kehrnel op capabilities --env {env_id}")
        lines.append(f"kehrnel core env query --env {env_id} --domain {domain} --aql <file>")
    elif kind == "query" and env_id and domain:
        lines.append(f"kehrnel core env compile-query --env {env_id} --domain {domain} --aql <file> --debug")
        lines.append(f"kehrnel core env show --env {env_id}")
        lines.append(f"kehrnel op capabilities --env {env_id}")
    elif kind == "compile-query" and env_id and domain:
        lines.append(f"kehrnel core env query --env {env_id} --domain {domain} --aql <file>")
        lines.append(f"kehrnel core env show --env {env_id}")
        lines.append(f"kehrnel op capabilities --env {env_id}")
    elif kind == "run" and env_id:
        lines.append(f"kehrnel core env show --env {env_id}")
        lines.append(f"kehrnel op capabilities --env {env_id}")
        if operation == "ingest" and domain:
            lines.append(f"kehrnel core env query --env {env_id} --domain {domain} --aql <file>")
    elif kind == "build-search-index" and env_id and strategy_id:
        lines.append(f"kehrnel core env show --env {env_id}")
        lines.append(f"kehrnel strategy build-search-index --env {env_id} --domain {domain or 'openehr'} --strategy {strategy_id} --out .kehrnel/search-index.json")
    elif env_id:
        lines.append(f"kehrnel core env show --env {env_id}")
        lines.append(f"kehrnel op capabilities --env {env_id}")
    if LOCAL_GUIDE_URL:
        lines.append(LOCAL_GUIDE_URL)
    return lines


def _render_rich_response(
    *,
    title: str,
    status: int,
    data: Any,
    base: Optional[str] = None,
    env_id: Optional[str] = None,
    domain: Optional[str] = None,
    strategy_id: Optional[str] = None,
    api_key: Optional[str] = None,
    kind: str,
    operation: Optional[str] = None,
    out_path: Optional[Path] = None,
    show_definition_json: bool = False,
) -> None:
    snapshot = _fetch_env_snapshot(base or "", env_id, api_key)
    context = _resolve_cli_context(
        base=base,
        env_id=env_id,
        domain=domain,
        strategy_id=strategy_id,
        api_key=api_key,
        snapshot=snapshot,
        response_data=data if isinstance(data, dict) else None,
    )

    header = Table.grid(padding=(0, 2))
    header.add_column(style="bold cyan")
    header.add_column()
    for key in ("runtime", "environment", "domain", "strategy", "version", "database"):
        if context.get(key):
            header.add_row(key, context[key])
    stdout_console.print(
        Panel.fit(
            header,
            title=f"{title} · HTTP {status}",
            border_style=_status_style(status),
            box=box.ROUNDED,
        )
    )

    summary_rows = _build_summary_rows(kind, data, operation=operation, out_path=out_path)
    if summary_rows:
        summary = Table(box=box.SIMPLE_HEAVY, show_header=False)
        summary.add_column(style="bold")
        summary.add_column()
        for label, value in summary_rows:
            summary.add_row(label, value)
        stdout_console.print(summary)

    preview_rows = _extract_rows_preview(data)
    if preview_rows:
        columns: List[str] = []
        for row in preview_rows:
            for key in row.keys():
                if key not in columns:
                    columns.append(key)
        table = Table(title="Rows Preview", box=box.SIMPLE_HEAVY)
        for column in columns:
            table.add_column(column)
        for row in preview_rows:
            table.add_row(*[str(row.get(column, "—")) for column in columns])
        stdout_console.print(table)

    if show_definition_json:
        definition = _deep_get(data, "definition")
        if not isinstance(definition, dict):
            definition = _deep_get(data, "result", "definition") or _deep_get(data, "response", "result", "definition")
        if isinstance(definition, dict):
            stdout_console.print(Panel(RichJSON(_dump_json(definition)), title="Index Definition", border_style="cyan"))

    guidance = _build_guidance(kind, env_id=env_id, domain=domain or context.get("domain"), strategy_id=strategy_id or context.get("strategy"), operation=operation)
    if guidance:
        guide_table = Table.grid(padding=(0, 1))
        for line in guidance:
            guide_table.add_row(f"[dim]-[/dim] {line}")
        stdout_console.print(Panel.fit(guide_table, title="More Info", border_style="blue", box=box.ROUNDED))


def _emit_api_response(
    *,
    title: str,
    status: int,
    data: Any,
    base: Optional[str] = None,
    env_id: Optional[str] = None,
    domain: Optional[str] = None,
    strategy_id: Optional[str] = None,
    api_key: Optional[str] = None,
    kind: str,
    operation: Optional[str] = None,
    out_path: Optional[Path] = None,
    show_definition_json: bool = False,
    plain_wrapper: Optional[Dict[str, Any]] = None,
) -> None:
    payload = plain_wrapper if plain_wrapper is not None else {"status": status, "response": data}
    if _rich_stdout_enabled():
        _render_rich_response(
            title=title,
            status=status,
            data=data,
            base=base,
            env_id=env_id,
            domain=domain,
            strategy_id=strategy_id,
            api_key=api_key,
            kind=kind,
            operation=operation,
            out_path=out_path,
            show_definition_json=show_definition_json,
        )
    else:
        _emit_json(payload)


def _emit_pass_through_banner(command_name: str, strategy_id: str, domain: str) -> None:
    if _rich_stderr_enabled():
        table = Table.grid(padding=(0, 2))
        table.add_column(style="bold cyan")
        table.add_column()
        table.add_row("command", command_name)
        table.add_row("strategy", strategy_id)
        table.add_row("domain", domain)
        stderr_console.print(Panel.fit(table, title="Kehrnel", border_style="cyan", box=box.ROUNDED))
    else:
        typer.echo(f"Using strategy={strategy_id} domain={domain}", err=True)


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


def _load_local_ingest_documents(path: Path) -> List[Dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".ndjson":
        documents: List[Dict[str, Any]] = []
        for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                raise typer.BadParameter(f"Invalid JSON on line {line_number} of {path}") from exc
            if not isinstance(parsed, dict):
                raise typer.BadParameter(f"Each NDJSON line in {path} must be a JSON object.")
            documents.append(parsed)
        if not documents:
            raise typer.BadParameter(f"No documents found in {path}.")
        return documents

    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"The file {path} is not valid JSON.") from exc

    if isinstance(parsed, dict) and isinstance(parsed.get("documents"), list):
        documents = parsed["documents"]
    elif isinstance(parsed, list):
        documents = parsed
    elif isinstance(parsed, dict):
        documents = [parsed]
    else:
        raise typer.BadParameter(f"The file {path} must contain a JSON object or array of objects.")

    if not documents:
        raise typer.BadParameter(f"No documents found in {path}.")
    if not all(isinstance(item, dict) for item in documents):
        raise typer.BadParameter(f"Every document in {path} must be a JSON object.")
    return documents


def _maybe_expand_local_ingest_file_payload(operation: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if operation.strip().lower() != "ingest":
        return payload
    if not isinstance(payload, dict) or "documents" in payload or "file_path" not in payload:
        return payload

    raw_path = payload.get("file_path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        return payload

    expanded_path = os.path.expandvars(os.path.expanduser(raw_path.strip()))
    if "://" in expanded_path:
        return payload

    candidate = Path(expanded_path)
    candidate = candidate.resolve() if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
    if not candidate.exists():
        return payload
    if not candidate.is_file():
        raise typer.BadParameter(f"ingest file_path is not a file: {candidate}")
    if candidate.suffix.lower() not in {".json", ".ndjson"}:
        raise typer.BadParameter("ingest file_path must point to a .json or .ndjson file.")

    expanded = dict(payload)
    expanded.pop("file_path", None)
    expanded["documents"] = _load_local_ingest_documents(candidate)
    return expanded


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
            resolved_bindings_ref = (
                typer.prompt(
                    "bindings_ref (example for built-in HDL resolver: env:dev or hdl:env:dev)",
                    default="",
                    show_default=False,
                )
                or ""
            ).strip()
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
    rows = [
        ("api key", mask_api_key(state["auth"].get("api_key"))),
        ("runtime", state["auth"].get("runtime_url") or "(not set)"),
    ]
    if _rich_stdout_enabled():
        _emit_rich_kv_panel("Authentication", rows)
    else:
        for label, value in rows:
            typer.echo(f"{label}: {value}")


@context_app.command("show")
def context_show():
    state = _state()
    if _rich_stdout_enabled():
        rows = [(key.replace("_", " "), str(value or "—")) for key, value in state["context"].items()]
        _emit_rich_kv_panel("Context", rows)
    else:
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
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("GET", f"{base.rstrip('/')}/health", resolved_api_key)
    _emit_api_response(
        title="Runtime Health",
        status=status,
        data=data,
        base=base,
        api_key=resolved_api_key,
        kind="health",
        plain_wrapper={"status": status, "url": base, "response": data},
    )
    raise typer.Exit(0 if status < 400 else 1)


@core_app.command("api")
def core_api(ctx: typer.Context):
    """Run the API server command."""
    _run_module("kehrnel.api.app", ctx.args)



@env_app.command("list")
def env_list(
    runtime_url: Optional[str] = typer.Option(None, "--runtime-url", help="Runtime base URL"),
    api_key: Optional[str] = typer.Option(None, "--api-key", help="API key"),
):
    """List environments available in the current runtime."""
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured. Set via `kehrnel auth login` or `kehrnel context set`.")
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("GET", f"{base.rstrip('/')}/environments", resolved_api_key)
    if _rich_stdout_enabled() and status < 400 and isinstance(data, dict):
        envs = data.get("environments") if isinstance(data.get("environments"), list) else []
        rows: List[List[str]] = []
        for item in envs:
            if not isinstance(item, dict):
                continue
            rows.append(
                [
                    str(item.get("env_id") or "—"),
                    str(item.get("name") or "—"),
                    str(item.get("activation_count") or 0),
                    ", ".join(item.get("activation_domains") or []) or "—",
                ]
            )
        _emit_rich_table(
            "Environments",
            ["Environment", "Name", "Activations", "Domains"],
            rows,
            context_rows=[("runtime", base.rstrip("/"))],
            empty_message="No environments available.",
            guide_lines=[
                "kehrnel core env create --env dev --name \"Development\"",
                "kehrnel core env show --env <env>",
                LOCAL_GUIDE_URL,
            ],
        )
    else:
        _emit_json({"status": status, "response": data})
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
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("GET", f"{base.rstrip('/')}/environments/{env_id}", resolved_api_key)
    _emit_api_response(
        title="Environment",
        status=status,
        data=data,
        base=base,
        env_id=env_id,
        api_key=resolved_api_key,
        kind="env-show",
    )
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
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("POST", f"{base.rstrip('/')}/environments", resolved_api_key, payload)
    _emit_api_response(
        title="Environment Created",
        status=status,
        data=data,
        base=base,
        env_id=env,
        api_key=resolved_api_key,
        kind="env-show",
    )
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
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("PATCH", f"{base.rstrip('/')}/environments/{env}", resolved_api_key, payload)
    _emit_api_response(
        title="Environment Updated",
        status=status,
        data=data,
        base=base,
        env_id=env,
        api_key=resolved_api_key,
        kind="env-show",
    )
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
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("DELETE", url, resolved_api_key)
    _emit_api_response(
        title="Environment Deleted",
        status=status,
        data=data,
        base=base,
        env_id=env,
        api_key=resolved_api_key,
        kind="env-show",
    )
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
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("GET", f"{base.rstrip('/')}/environments/{env_id}/endpoints", resolved_api_key)
    _emit_api_response(
        title="Environment Endpoints",
        status=status,
        data=data,
        base=base,
        env_id=env_id,
        api_key=resolved_api_key,
        kind="env-show",
    )
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
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("POST", f"{base.rstrip('/')}/environments/{env_id}/activate", resolved_api_key, payload)
    _emit_api_response(
        title="Strategy Activation",
        status=status,
        data=data,
        base=base,
        env_id=env_id,
        domain=chosen_domain,
        strategy_id=chosen_strategy,
        api_key=resolved_api_key,
        kind="activate",
    )
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
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json(
        "POST",
        f"{base.rstrip('/')}/environments/{env_id}/activations/{chosen_domain}/ops/{op}",
        resolved_api_key,
        payload,
    )
    _emit_api_response(
        title="Strategy Operation",
        status=status,
        data=data,
        base=base,
        env_id=env_id,
        domain=chosen_domain,
        api_key=resolved_api_key,
        strategy_id=_state()["context"].get("strategy"),
        kind="run",
        operation=op,
    )
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
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("POST", url, resolved_api_key, payload)
    _emit_api_response(
        title="Query Compilation",
        status=status,
        data=data,
        base=base,
        env_id=env_id,
        domain=chosen_domain,
        api_key=resolved_api_key,
        kind="compile-query",
    )
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
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("POST", f"{base.rstrip('/')}/environments/{env_id}/query", resolved_api_key, payload)
    _emit_api_response(
        title="Query Result",
        status=status,
        data=data,
        base=base,
        env_id=env_id,
        domain=chosen_domain,
        api_key=resolved_api_key,
        kind="query",
    )
    raise typer.Exit(0 if status < 400 else 1)


@strategy_app.command("use")
def strategy_use(strategy_id: str, domain: Optional[str] = typer.Option(None)):
    state = _state()
    state["context"]["strategy"] = strategy_id
    if domain:
        state["context"]["domain"] = domain
    _save(state)
    if _rich_stdout_enabled():
        rows = [("strategy", strategy_id)]
        if domain:
            rows.append(("domain", _friendly_domain_label(domain)))
        _emit_rich_kv_panel("Selected Strategy", rows)
    else:
        typer.echo(f"Selected strategy: {strategy_id}")


@strategy_app.command("current")
def strategy_current():
    """Show the strategy currently selected in the local CLI context."""
    state = _state()
    current = state["context"].get("strategy") or "(not set)"
    if _rich_stdout_enabled():
        _emit_rich_kv_panel("Active Strategy", [("strategy", current)])
    else:
        typer.echo(current)


@strategy_app.command("list")
def strategy_list(
    runtime_url: Optional[str] = typer.Option(None, "--runtime-url", help="Runtime base URL"),
    api_key: Optional[str] = typer.Option(None, "--api-key", help="API key"),
    domain: Optional[str] = typer.Option(None, "--domain", help="Filter by domain id, for example: openehr"),
):
    """List strategies exposed by the runtime."""
    base = _resolve_runtime_url(runtime_url)
    if not base:
        raise typer.BadParameter("No runtime URL configured.")
    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json("GET", f"{base.rstrip('/')}/strategies", resolved_api_key)
    strategies = data.get("strategies") if isinstance(data, dict) else data
    if not isinstance(strategies, list):
        _emit_json({"status": status, "response": data})
        raise typer.Exit(1)
    domain_filter = (domain or "").strip().lower() if domain is not None else None
    filtered: List[Dict[str, Any]] = []
    for row in strategies:
        sid = row.get("id") or row.get("strategy_id")
        sdomain = row.get("domain")
        sdomain_norm = str(sdomain or "").strip().lower()
        if domain_filter and sdomain_norm != domain_filter:
            continue
        filtered.append(row)
    if _rich_stdout_enabled():
        rows = [
            [
                str(item.get("id") or item.get("strategy_id") or "—"),
                _friendly_domain_label(item.get("domain")),
                f"v{item.get('version', '?')}",
                str(item.get("maturity") or "—"),
            ]
            for item in filtered
            if isinstance(item, dict)
        ]
        context_rows = [("runtime", base.rstrip("/"))]
        if domain_filter:
            context_rows.append(("domain filter", _friendly_domain_label(domain_filter)))
        _emit_rich_table(
            "Strategies",
            ["Strategy", "Domain", "Version", "Maturity"],
            rows,
            context_rows=context_rows,
            empty_message="No strategies matched the current filter.",
            guide_lines=[
                "kehrnel strategy list --domain openehr",
                "kehrnel strategy current",
                LOCAL_GUIDE_URL,
            ],
        )
    else:
        for row in filtered:
            sid = row.get("id") or row.get("strategy_id")
            sdomain = row.get("domain")
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

    resolved_api_key = _resolve_api_key(api_key)
    status, data = _http_json(
        "POST",
        f"{base.rstrip('/')}/environments/{env_id}/run",
        resolved_api_key,
        run_body,
    )
    if status >= 400:
        _emit_api_response(
            title="Search Index Definition",
            status=status,
            data=data,
            base=base,
            env_id=env_id,
            domain=selected_domain,
            strategy_id=selected_strategy,
            api_key=resolved_api_key,
            kind="build-search-index",
        )
        raise typer.Exit(1)

    definition = None
    warnings = None
    if isinstance(data, dict):
        definition = data.get("definition")
        warnings = data.get("warnings")
        if not isinstance(definition, dict):
            result_payload = data.get("result")
            if isinstance(result_payload, dict):
                nested_definition = result_payload.get("definition")
                if isinstance(nested_definition, dict):
                    definition = nested_definition
                    nested_warnings = result_payload.get("warnings")
                    if isinstance(nested_warnings, list):
                        warnings = nested_warnings
        if not isinstance(definition, dict):
            response_payload = data.get("response")
            if isinstance(response_payload, dict):
                result_payload = response_payload.get("result")
                if isinstance(result_payload, dict):
                    nested_definition = result_payload.get("definition")
                    if isinstance(nested_definition, dict):
                        definition = nested_definition
                        nested_warnings = result_payload.get("warnings")
                        if isinstance(nested_warnings, list):
                            warnings = nested_warnings
    if out is not None:
        if not isinstance(definition, dict):
            raise typer.BadParameter("Runtime did not return a definition object.")
        out.write_text(json.dumps(definition, indent=2) + "\n", encoding="utf-8")
        if not _rich_stdout_enabled():
            typer.echo(f"Wrote search index definition to {out}")

    if json_output:
        _emit_json({"status": status, "response": data})
    elif isinstance(definition, dict):
        _emit_api_response(
            title="Search Index Definition",
            status=status,
            data=data,
            base=base,
            env_id=env_id,
            domain=selected_domain,
            strategy_id=selected_strategy,
            api_key=resolved_api_key,
            kind="build-search-index",
            out_path=out,
            show_definition_json=True,
            plain_wrapper=definition,
        )
        if not _rich_stdout_enabled() and isinstance(warnings, list) and warnings:
            _emit_json({"warnings": warnings})
    else:
        _emit_api_response(
            title="Search Index Definition",
            status=status,
            data=data,
            base=base,
            env_id=env_id,
            domain=selected_domain,
            strategy_id=selected_strategy,
            api_key=resolved_api_key,
            kind="build-search-index",
        )


@resource_app.command("list")
def resource_list(json_output: bool = typer.Option(False, "--json", help="Output as JSON")):
    """List saved source and sink profiles from the local CLI context."""
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
    if _rich_stdout_enabled():
        rows: List[List[str]] = []
        for name, cfg in sorted(resources.items()):
            if not isinstance(cfg, dict):
                rows.append([name, "unknown", ""])
                continue
            rtype = cfg.get("type") or cfg.get("driver") or cfg.get("provider") or "unknown"
            target = cfg.get("uri") or cfg.get("path") or ""
            rows.append([name, str(rtype), str(target)])
        _emit_rich_table(
            "Resource Profiles",
            ["Name", "Type", "Target"],
            rows,
            empty_message="No resource profiles configured.",
            guide_lines=[
                "kehrnel resource add <name> --type file --path <path>",
                "kehrnel resource show <name>",
            ],
        )
    else:
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
    if _rich_stdout_enabled():
        rows = [("name", name)] + [(str(key), str(value)) for key, value in profile.items()]
        _emit_rich_kv_panel("Resource Profile", rows)
    else:
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
    if _rich_stdout_enabled():
        _emit_rich_kv_panel(
            "Default Resources",
            [
                ("source", str(state["context"].get("source") or "—")),
                ("sink", str(state["context"].get("sink") or "—")),
            ],
        )
    else:
        typer.echo(json.dumps({"source": state["context"].get("source"), "sink": state["context"].get("sink")}, indent=2))


@ops_app.command("list")
def op_list(
    runtime_url: Optional[str] = typer.Option(None, "--runtime-url", help="Runtime base URL"),
    api_key: Optional[str] = typer.Option(None, "--api-key", help="API key"),
    domain: Optional[str] = typer.Option(None, help="Filter by domain id, for example: openehr"),
    strategy: Optional[str] = typer.Option(None, help="Filter by strategy id"),
    kind: Optional[str] = typer.Option(None, help="Filter by operation kind"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """List operations exposed by the runtime, with optional filters."""
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
    if _rich_stdout_enabled():
        table_rows = [
            [
                str(row.get("name") or "—"),
                str(row.get("kind") or "—"),
                _friendly_domain_label(row.get("domain")),
                str(row.get("strategy_id") or "—"),
            ]
            for row in rows
        ]
        context_rows = []
        if domain_filter:
            context_rows.append(("domain filter", _friendly_domain_label(domain_filter)))
        if strategy_filter:
            context_rows.append(("strategy filter", strategy_filter))
        if kind_filter:
            context_rows.append(("kind filter", kind_filter))
        _emit_rich_table(
            "Operations",
            ["Name", "Kind", "Domain", "Strategy"],
            table_rows,
            context_rows=context_rows or None,
            empty_message="No operations matched the current filter.",
            guide_lines=[
                "kehrnel op capabilities --env <env>",
                "kehrnel op schema <operation> --strategy <strategy>",
                LOCAL_GUIDE_URL,
            ],
        )
    else:
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
    runtime_url: Optional[str] = typer.Option(None, "--runtime-url", help="Runtime base URL"),
    api_key: Optional[str] = typer.Option(None, "--api-key", help="API key"),
    include_schemas: bool = typer.Option(True, "--schemas/--no-schemas", help="Include operation input/output schemas"),
    domain: Optional[str] = typer.Option(None, help="Filter strategy operations by domain id"),
    strategy: Optional[str] = typer.Option(None, help="Filter strategy operations by strategy id"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Show the effective operations available in an environment."""
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
            if _rich_stdout_enabled():
                context_rows = [
                    ("environment", str(data.get("env_id") or env_id)),
                    ("standard ops", str(len(std))),
                    ("strategy ops", str(len(strat))),
                ]
                rows = []
                for row in strat:
                    if not isinstance(row, dict):
                        continue
                    rows.append(
                        [
                            str(row.get("name") or "—"),
                            str(row.get("kind") or "—"),
                            _friendly_domain_label(row.get("domain")),
                            str(row.get("strategy_id") or "—"),
                        ]
                    )
                _emit_rich_table(
                    "Environment Capabilities",
                    ["Name", "Kind", "Domain", "Strategy"],
                    rows,
                    context_rows=context_rows,
                    empty_message="No strategy capabilities found.",
                    guide_lines=[
                        f"kehrnel core env show --env {env_id}",
                        "kehrnel op schema <operation> --strategy <strategy>",
                        LOCAL_GUIDE_URL,
                    ],
                )
            else:
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
    if _rich_stdout_enabled():
        table_rows = [
            [
                str(row.get("name") or "—"),
                str(row.get("kind") or "—"),
                _friendly_domain_label(row.get("domain")),
                str(row.get("strategy_id") or "—"),
            ]
            for row in rows
        ]
        context_rows = [("environment", env_id)]
        if domain_filter:
            context_rows.append(("domain filter", _friendly_domain_label(domain_filter)))
        if strategy_filter:
            context_rows.append(("strategy filter", strategy_filter))
        _emit_rich_table(
            "Environment Capabilities",
            ["Name", "Kind", "Domain", "Strategy"],
            table_rows,
            context_rows=context_rows,
            empty_message="No strategy capabilities matched the current filter.",
            guide_lines=[
                f"kehrnel core env show --env {env_id}",
                "kehrnel op schema <operation> --strategy <strategy>",
                LOCAL_GUIDE_URL,
            ],
        )
    else:
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
    payload = _maybe_expand_local_ingest_file_payload(operation, payload)

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
        _emit_api_response(
            title="Operation Result",
            status=status,
            data=data,
            base=base,
            env_id=env_id,
            domain=selected_domain,
            strategy_id=selected_strategy,
            api_key=auth,
            kind="run",
            operation=operation,
            plain_wrapper={"status": status, "operation": operation, "response": data},
        )
        raise typer.Exit(0)

    # Fallback only when /run endpoint itself is unavailable on older runtimes.
    detail = str((data or {}).get("detail") or "").strip().lower() if isinstance(data, dict) else ""
    run_endpoint_missing = status in (404, 405) and detail in {"not found", "method not allowed"}
    if not run_endpoint_missing:
        _emit_api_response(
            title="Operation Result",
            status=status,
            data=data,
            base=base,
            env_id=env_id,
            domain=selected_domain,
            strategy_id=selected_strategy,
            api_key=auth,
            kind="run",
            operation=operation,
            plain_wrapper={"status": status, "operation": operation, "response": data},
        )
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

    _emit_api_response(
        title="Operation Result",
        status=status,
        data=data,
        base=base,
        env_id=env_id,
        domain=selected_domain,
        strategy_id=selected_strategy,
        api_key=auth,
        kind="run",
        operation=operation,
        plain_wrapper={
            "status": status,
            "operation": operation,
            "response": data,
            "fallback": True,
        },
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
    _emit_pass_through_banner("transform", selected_strategy, selected_domain)
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
    _emit_pass_through_banner("ingest", selected_strategy, selected_domain)
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
    _emit_pass_through_banner("validate", selected_strategy, selected_domain)
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
    _emit_pass_through_banner("generate", selected_strategy, selected_domain)
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
    _emit_pass_through_banner("map", selected_strategy, selected_domain)
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
