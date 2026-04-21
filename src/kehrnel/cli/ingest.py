"""Ingestion helpers used by unified CLI commands."""
from __future__ import annotations
import json
import logging
import typer
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import yaml

from kehrnel.persistence import get_driver, MongoSource
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener

app = typer.Typer(help="Bulk-ingest flattened docs into a persistence driver")
log = logging.getLogger(__name__)

@app.command("file")
def from_file(
    jsonl: Path = typer.Argument(..., exists=True, readable=True,
                                 help="NDJSON file with flattened docs"),
    driver_cfg: Path = typer.Option(..., "-d", help="YAML/JSON driver config"),
    workers:    int  = typer.Option(4, help="parallel insert workers")
):
    """Read an **NDJSON** file (one flattened doc per line) and ingest it."""
    drv = get_driver(driver_cfg)
    drv.connect()

    def producer() -> Iterator[Dict[str, Any]]:
        with jsonl.open("r", encoding="utf-8") as fh:
            for line in fh:
                yield json.loads(line)

    drv.insert_many(producer(), workers=workers)
    inserted = getattr(getattr(drv, "stats", None), "inserted", 0)
    typer.echo(json.dumps({"status": 200, "ok": True, "inserted": inserted}))

@app.command("mongo-catchup")
def from_mongo(
    src_cfg:  Path = typer.Option(..., help="Source Mongo config JSON"),
    driver_cfg: Path = typer.Option(..., help="Target driver YAML/JSON"),
    limit: int = typer.Option(None, help="patient limit")
):
    """A convenience command that mimics the old *ingestion.py* behaviour
    (read canonical comps from one Mongo collection, flatten & write to
    another)."""
    source_cfg = json.loads(src_cfg.read_text(encoding="utf-8"))
    source = MongoSource(source_cfg, limit=limit)
    drv = get_driver(driver_cfg)
    drv.connect()

    # Canonical "catchup" uses literal encoding (no dictionaries required).
    flattener = CompositionFlattener(
        db=None,
        config={
            "role": "primary",
            "apply_shortcuts": False,
            "paths": {"separator": "."},
            "collections": {},
        },
        mappings_path=str(
            Path(__file__).resolve().parents[2]
            / "engine"
            / "strategies"
            / "openehr"
            / "rps_dual"
            / "ingest"
            / "config"
            / "flattener_mappings_f.jsonc"
        ),
        mappings_content=None,
        coding_opts={"arcodes": {"strategy": "literal"}, "atcodes": {"strategy": "literal"}},
    )

    inserted = 0
    for raw in source.iter_compositions():
        base_doc, search_doc = flattener.transform_composition(raw)
        drv.insert_one(base_doc)
        inserted += 1
        if search_doc and search_doc.get("sn"):
            drv.insert_one(search_doc, search=True)
            inserted += 1
    typer.echo(json.dumps({"status": 200, "ok": True, "inserted": inserted}))


@app.command("init-driver")
def init_driver(
    out: Path = typer.Option(Path(".kehrnel/driver.mongo.yaml"), "--out", "-o", help="Where to write the driver YAML"),
    use_env_var: bool = typer.Option(True, "--env/--no-env", help="Use an env var placeholder for the connection string"),
    env_var_name: str = typer.Option("MONGODB_URI", "--env-var", help="Env var name to reference when --env"),
    connection_string: Optional[str] = typer.Option(None, "--uri", prompt=False, hide_input=True, help="MongoDB connection string (only used when --no-env)"),
    database_name: str = typer.Option(..., "--db", prompt=True, help="Database name"),
    compositions_collection: str = typer.Option("compositions_rps", "--compositions", prompt=True, help="Base compositions collection"),
    search_collection: str = typer.Option("compositions_search", "--search", prompt=True, help="Search collection"),
):
    """
    Create a persistence driver config for `kehrnel ingest`.

    By default, writes `${MONGODB_URI}` so you don't store secrets in the YAML.
    """
    if use_env_var:
        uri_value = f"${{{env_var_name}}}"
    else:
        uri_value = connection_string or typer.prompt("MongoDB connection string", hide_input=True)

    cfg = {
        "driver": "mongodb",
        "connection_string": uri_value,
        "database_name": database_name,
        "compositions_collection": compositions_collection,
        "search_collection": search_collection,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    typer.echo(f"✓ wrote {out}")
    if use_env_var:
        typer.echo(f"Set {env_var_name} in your environment before running ingest.")


@app.command("init-source-mongo")
def init_source_mongo(
    out: Path = typer.Option(Path(".kehrnel/source.mongo.json"), "--out", "-o", help="Where to write the source JSON"),
    use_env_var: bool = typer.Option(True, "--env/--no-env", help="Use an env var placeholder for the connection string"),
    env_var_name: str = typer.Option("MONGODB_URI", "--env-var", help="Env var name to reference when --env"),
    connection_string: Optional[str] = typer.Option(None, "--uri", prompt=False, hide_input=True, help="MongoDB connection string (only used when --no-env)"),
    database_name: str = typer.Option(..., "--db", prompt=True, help="Database name"),
    source_collection: str = typer.Option(..., "--collection", prompt=True, help="Source compositions collection"),
):
    """Create a source Mongo config JSON for `kehrnel ingest mongo-catchup`."""
    if use_env_var:
        uri_value = f"${{{env_var_name}}}"
    else:
        uri_value = connection_string or typer.prompt("MongoDB connection string", hide_input=True)

    cfg = {
        "connection_string": uri_value,
        "database_name": database_name,
        "source_collection": source_collection,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    typer.echo(f"✓ wrote {out}")
    if use_env_var:
        typer.echo(f"Set {env_var_name} in your environment before running mongo-catchup.")


if __name__ == "__main__":
    app()
