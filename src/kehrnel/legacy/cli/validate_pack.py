# src/kehrnel/legacy/cli/validate_pack.py   →  `kehrnel-validate-pack`
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from kehrnel.core.pack_validator import StrategyPackValidator

app = typer.Typer(add_completion=False, rich_markup_mode="rich")
console = Console()


@app.command()
def main(
    path: Path = typer.Argument(..., help="Path to strategy pack directory or manifest.json"),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON diagnostics"),
):
    """
    Validate a strategy pack for portability and completeness.
    """
    manifest_path = path if path.name == "manifest.json" else path / "manifest.json"
    if not manifest_path.exists():
        typer.secho(f"manifest.json not found under {path}", fg="red", err=True)
        raise typer.Exit(2)
    try:
        manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - user input
        typer.secho(f"Invalid manifest.json: {exc}", fg="red", err=True)
        raise typer.Exit(2)
    validator = StrategyPackValidator(manifest_data, manifest_path.parent)
    errors = validator.validate()
    if json_output:
        typer.echo(json.dumps({"valid": len(errors) == 0, "errors": errors}, indent=2))
    else:
        if not errors:
            console.print(f"[green]✓ Pack valid[/green] ({manifest_data.get('id')})")
        else:
            table = Table(title="Strategy Pack Errors", show_header=True)
            table.add_column("Issue")
            for err in errors:
                table.add_row(err)
            console.print(table)
    raise typer.Exit(code=0 if not errors else 1)


if __name__ == "__main__":
    app()
