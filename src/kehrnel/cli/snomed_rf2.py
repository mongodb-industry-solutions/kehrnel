"""Convert a licensed SNOMED CT RF2 Snapshot release into derived concept JSON.

The output is the JSON array consumed by the ``snomed_ingest_release`` operation
of the ``snomedct.mongodb`` strategy. This command never downloads anything: it
only reads a release the operator already holds a licence for.

    python -m kehrnel.cli.snomed_rf2 ./SnomedCT_Release.zip --out ./release.json
"""
from __future__ import annotations

import json
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from kehrnel.engine.domains.snomedct.rf2 import Rf2Error, convert_rf2_to_json

app = typer.Typer(add_completion=False, rich_markup_mode="rich")
console = Console()


@app.command()
def main(
    source: Path = typer.Argument(
        ...,
        exists=True,
        readable=True,
        help="RF2 release: a Snapshot directory or a release .zip",
    ),
    out: Path = typer.Option(..., "--out", "-o", help="Destination JSON array file"),
    include_inactive: bool = typer.Option(
        False,
        "--include-inactive/--active-only",
        help="Also emit inactive concept rows (inactive descriptions/relationships are always dropped)",
    ),
    compact: bool = typer.Option(False, "--compact", help="Emit compact JSON instead of pretty-printed"),
    json_output: bool = typer.Option(False, "--json", help="Emit the conversion report as JSON"),
):
    """
    Convert RF2 Snapshot files to the derived concept JSON array.

    `inferredAncestorIds` is written as the FULL transitive IS-A closure, which
    is what ECL `<` / `<<` expansion matches on at query time.
    """
    try:
        report = convert_rf2_to_json(
            source,
            out,
            include_inactive_concepts=include_inactive,
            indent=None if compact else 2,
        )
    except Rf2Error as exc:
        typer.secho(str(exc), fg="red", err=True)
        raise typer.Exit(2)

    if json_output:
        typer.echo(json.dumps(report, indent=2))
    else:
        stats = report["stats"]
        table = Table(title=f"RF2 -> JSON ({report['output']})", show_header=True)
        table.add_column("Metric")
        table.add_column("Count", justify="right")
        for key, value in stats.items():
            table.add_row(key, str(value))
        console.print(table)
        console.print(f"[green]✓ wrote[/green] {report['output']}")


if __name__ == "__main__":
    app()
