from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from kehrnel.core.bundle_store import BundleStore
from kehrnel.core.bundles import validate_bundle, compute_bundle_digest
from kehrnel.core.errors import KehrnelError

app = typer.Typer(add_completion=False, rich_markup_mode="rich")
console = Console()


def _store(path: Optional[str]) -> BundleStore:
    root = Path(path) if path else Path(".kehrnel/bundles")
    return BundleStore(root)


@app.command("validate-bundle")
def validate_bundle_cmd(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    errors = validate_bundle(data)
    if errors:
        typer.secho("Bundle invalid:", fg="red")
        for e in errors:
            typer.echo(f"- {e}")
        raise typer.Exit(1)
    typer.secho(f"Bundle valid (digest {compute_bundle_digest(data)})", fg="green")


@app.command("import-bundle")
def import_bundle_cmd(path: Path, upsert: bool = typer.Option(False, "--upsert"), store_path: Optional[str] = typer.Option(None, "--store")):
    store = _store(store_path)
    data = json.loads(path.read_text(encoding="utf-8"))
    try:
        res = store.save_bundle(data, mode="upsert" if upsert else "error")
        typer.secho(f"Imported bundle {res['bundle_id']} ({res['digest']})", fg="green")
    except KehrnelError as exc:
        typer.secho(f"Error: {exc}", fg="red")
        raise typer.Exit(1)


@app.command("list-bundles")
def list_bundles_cmd(store_path: Optional[str] = typer.Option(None, "--store")):
    store = _store(store_path)
    bundles = store.list_bundles()
    table = Table(title="Bundles", show_header=True)
    table.add_column("bundle_id")
    table.add_column("domain")
    table.add_column("kind")
    table.add_column("version")
    table.add_column("digest")
    for b in bundles:
        table.add_row(b.get("bundle_id") or "", b.get("domain") or "", b.get("kind") or "", b.get("version") or "", b.get("digest") or "")
    console.print(table)


@app.command("export-bundle")
def export_bundle_cmd(bundle_id: str, out: Path, store_path: Optional[str] = typer.Option(None, "--store")):
    store = _store(store_path)
    try:
        bundle = store.get_bundle(bundle_id)
    except KehrnelError as exc:
        typer.secho(f"Error: {exc}", fg="red")
        raise typer.Exit(1)
    out.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    typer.secho(f"Wrote bundle to {out}", fg="green")


if __name__ == "__main__":
    app()
