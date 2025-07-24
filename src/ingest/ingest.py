import typer
from kehrnel.ingest.bulk import run as bulk_run

app = typer.Typer(help="Bulk ingest compositions into the CDR")

@app.command("ingest")
def ingest(
    config: str = typer.Option(
        "config.json",
        "-c",
        "--config",
        help="Path to the ingestion configuration file"
    )
):
    """
    Run a bulk ingestion of compositions according to the given config.
    """
    try:
        bulk_run(config)
    except Exception as e:
        typer.echo(f"❌ Bulk ingest failed: {e}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()