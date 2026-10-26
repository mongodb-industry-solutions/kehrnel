"""Strategy-pack validation command backed by the canonical loader."""
from __future__ import annotations

import json
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_loader import load_strategy_manifest


app = typer.Typer(add_completion=False, rich_markup_mode="rich")
console = Console()


def _resolve_manifest(path: Path) -> Path:
    if path.name == "manifest.json":
        return path
    direct = path / "manifest.json"
    if direct.exists():
        return direct
    specification = path / "specification" / "manifest.json"
    if specification.exists():
        return specification
    return direct


def _error_payload(error: KehrnelError, manifest_path: Path) -> dict:
    diagnostics = error.details.get("diagnostics") or [
        {
            "code": error.code,
            "message": str(error),
            "path": "/manifest" if error.code == "MANIFEST_NOT_FOUND" else "/",
            "severity": "error",
        }
    ]
    errors = error.details.get("errors") or [item["message"] for item in diagnostics]
    return {
        "valid": False,
        "manifestPath": str(manifest_path),
        "errors": errors,
        "diagnostics": diagnostics,
    }






@app.command()
def main(
    path: Path = typer.Argument(..., help="Path to strategy pack directory or manifest.json"),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON diagnostics"),
):
    """Validate a strategy pack for portability, safety and completeness."""

    manifest_path = _resolve_manifest(path)
    try:
        manifest = load_strategy_manifest(manifest_path)
    except KehrnelError as exc:
        payload = _error_payload(exc, manifest_path)
        if json_output:
            typer.echo(json.dumps(payload, indent=2))
        else:
            table = Table(title="Strategy Pack Errors", show_header=True)
            table.add_column("Path")
            table.add_column("Code")
            table.add_column("Issue")
            for diagnostic in payload["diagnostics"]:
                table.add_row(
                    str(diagnostic.get("path") or "/"),
                    str(diagnostic.get("code") or "PACK_INVALID"),
                    str(diagnostic.get("message") or "Invalid strategy pack"),
                )
            console.print(table)
        raise typer.Exit(1)

    payload = {
        "valid": True,
        "manifestPath": str(manifest_path),
        "strategyId": manifest.id,
        "version": manifest.version,
        "packageDigest": manifest.package_digest,
        "contractDigest": manifest.contract_digest,
        "errors": [],
        "diagnostics": [],
    }
    if json_output:
        typer.echo(json.dumps(payload, indent=2))
    else:
        console.print(f"[green]✓ Pack valid[/green] ({manifest.id}@{manifest.version})")


if __name__ == "__main__":
    app()
