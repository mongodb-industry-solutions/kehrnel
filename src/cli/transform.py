import typer
from kehrnel.transform.single import run as single_run

app = typer.Typer(help="Transform and ingest a single composition")

@app.command("transform")
def transform(
    file: str = typer.Option(
        None,
        "-f",
        "--file",
        help="Path to a composition JSON file to transform"
    ),
    comp_id: str = typer.Option(
        None,
        "--id",
        help="Composition ID to fetch & transform from the source database"
    ),
    config: str = typer.Option(
        "config.json",
        "-c",
        "--config",
        help="Path to the transformation configuration file"
    )
):
    """
    Transform (and optionally ingest) a single composition.
    You must provide either --file or --id.
    """
    if not file and not comp_id:
        typer.echo("❌ Error: you must supply either --file or --id")
        raise typer.Exit(code=1)

    try:
        single_run(
            config_path=config,
            file_path=file,
            composition_id=comp_id,
        )
    except Exception as e:
        typer.echo(f"❌ Transformation failed: {e}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()