"""Mapping skeleton helpers used by unified CLI commands."""
from __future__ import annotations
from pathlib import Path
import sys
import yaml
import typer, logging

from kehrnel.engine.common.mapping.skeleton import build_skeleton

app = typer.Typer(add_completion=False, rich_markup_mode="rich")

@app.command()
def generate(
    template: Path = typer.Argument(..., exists=True, help="OPT or web-template.json"),
    output: Path = typer.Option(Path("-"), "-o", "--output", help="'-' → stdout"),
    helpers: bool = typer.Option(False, "--helpers/--no-helpers", help="Include GUI helper model"),
    include_header: bool = typer.Option(False, "--include-header/--no-include-header", help="Include context/protocol/header DV_*"),
    macros: bool = typer.Option(True, "--macros/--raw", help="Use code/term shortcuts in DV_CODED_TEXT"),
    log: str = typer.Option("simple", "--log", "-l", help="quiet | simple | debug"),
):
    log = (log or "simple").lower()
    if log not in {"quiet", "simple", "debug"}:
        raise typer.BadParameter("must be one of: quiet, simple, debug")
    if log == "debug":
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")

    def on_status(msg: str):
        if log == "simple":
            if msg.startswith("done") or msg.startswith("reading"):
                typer.echo(msg)
        elif log == "debug":
            typer.echo(msg)

    sk = build_skeleton(
        template,
        use_macros=macros,
        include_header=include_header,
        include_helpers=helpers,
        on_status=(None if log == "quiet" else on_status),
        suppress_generator_noise=(log != "debug"),
    )

    text = yaml.safe_dump(sk, sort_keys=False, allow_unicode=True)
    if output == Path("-"):
        sys.stdout.write(text)
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
        typer.echo(f"✓ wrote {output}")

def main():
    app()

if __name__ == "__main__":
    main()
