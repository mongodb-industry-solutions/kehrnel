# cli/ingest.py  →  console-script: kehrnel-ingest
from __future__ import annotations
import json, sys, typer
from pathlib import Path
from ingest.bulk import run as ingest_bulk
from transform.core import Transformer, load_default_cfg

app = typer.Typer(help="Bulk-ingest flattened docs into a persistence driver")

@app.command("file")
def from_file(
    jsonl: Path = typer.Argument(..., exists=True, readable=True,
                                 help="NDJSON file with flattened docs"),
    driver_cfg: Path = typer.Option(..., "-d", help="YAML/JSON driver config"),
    tf_cfg:     Path = typer.Option(None, "-c", help="transform config"),
    workers:    int  = typer.Option(4, help="parallel insert workers")
):
    """Read an **NDJSON** file (one flattened doc per line) and ingest it."""
    tf = Transformer(load_default_cfg(tf_cfg))
    ingest_bulk(tf, jsonl, driver_cfg, workers)

@app.command("mongo-catchup")
def from_mongo(
    src_cfg:  Path = typer.Option(..., help="Source Mongo config JSON"),
    driver_cfg: Path = typer.Option(..., help="Target driver YAML/JSON"),
    limit: int = typer.Option(None, help="patient limit")
):
    """A convenience command that mimics the old *ingestion.py* behaviour
    (read canonical comps from one Mongo collection, flatten & write to
    another)."""
    ingest_bulk.from_mongo(src_cfg, driver_cfg, limit)


if __name__ == "__main__":
    app()s