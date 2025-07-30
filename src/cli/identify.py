"""CLI for document type identification (`kehrnel-identify`)."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Iterable, List

import typer

from mapper.document_identifier import DocumentIdentifier 

app = typer.Typer(add_completion=False, rich_markup_mode="rich")


# ───────────────────────── helpers ──────────────────────────
def _iter_files(base: Path, glob: str | None, recurse: bool) -> Iterable[Path]:
    """Yield every file under *base* (respecting glob + recursion flags)."""
    if base.is_file():
        yield base
        return

    pattern = f"**/{glob or '*'}" if recurse else (glob or "*")
    yield from (p for p in base.glob(pattern) if p.is_file())


# ───────────────────────────── CLI ──────────────────────────────
@app.command()
def main(
    document: Path = typer.Option(
        ..., "-d", "--document",
        help="File *or* directory to identify",
        exists=True,
        readable=True,
    ),
    output: Path = typer.Option(
        Path("-"), "-o", "--output",
        help="'-' → stdout; otherwise a single consolidated JSON file is written",
        dir_okay=False,
    ),
    glob: str = typer.Option(
        "*", "--glob",
        help="Only consider files that match this glob pattern "
        "(default: '*'  – i.e. every file)",
    ),
    recurse: bool = typer.Option(
        True, "--recursive/--no-recursive",
        help="Walk sub-directories (default: yes)",
    ),
    pattern_file: List[Path] = typer.Option(
        [],                       
        "--patterns", "-p",
        help="Extra YAML or JSON file(s) containing pattern definitions "
            "(can be repeated)",
        exists=True,
        readable=True,
        dir_okay=False,
    ),
    no_default: bool = typer.Option(
     False, "--no-default",
     help="Ignore the built-in src/mapper/patterns.yaml",
    ),
    debug: bool = typer.Option(
        False, "--debug",
        help="Trace pattern evaluation (passed straight to mapper)",
    ),
):
    """
    Identify the type of every document under *document* and emit **one**
    JSON array with all the results.
    """

    identifier = DocumentIdentifier(
        debug=debug,
        pattern_files=pattern_file,
        include_default=not no_default,
    )
    results: list[dict] = []
    counts: Counter[str] = Counter()

    # ─── classify every file ───────────────────────────────────────────
    for path in _iter_files(document, glob, recurse):
        try:
            res = identifier.identify_document(path)
            res["file"] = str(path)             # remember original location
            results.append(res)

            counts[res.get("documentType", "unknown")] += 1

            if debug:
                typer.secho(f"[✓] {path}", fg="green")
        except Exception as exc:               # keep going even on errors
            results.append({
                "file": str(path),
                "error": str(exc),
            })
            counts["error"] += 1
            typer.secho(f"[✗] {path} → {exc}", fg="red", err=True)

    # ─── single write ─────────────────────────────────────────────────
    out_text = json.dumps(results, indent=2, ensure_ascii=False)
    if output == Path("-"):
        sys.stdout.write(out_text)
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(out_text)
        typer.secho(f"✓ {len(results)} items written to {output}", fg="green")

    # ─── final one-line summary ───────────────────────────────────────
    if counts:
        summary = " • ".join(f"{k}: {v}" for k, v in counts.most_common())
        typer.secho(summary, fg="bright_blue", err=True)


if __name__ == "__main__":
    app()