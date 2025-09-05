from __future__ import annotations
import json, sys
from pathlib import Path
import typer
from core.parser import TemplateParser     
from tabulate import tabulate

app = typer.Typer(add_completion=False)

def _walk(node, depth=0, base=""):
    """yield (json_path, rm_type, required?) tuples depth-first."""
    here = f"{base}/{node['aqlPath'].lstrip('/').replace('[', '/').replace(']','')}"
    yield here, node["rmType"], node.get("min", 0) > 0
    for ch in node.get("children", []):
        yield from _walk(ch, depth + 1, here)

@app.command()
def main(
    template: Path = typer.Argument(..., exists=True, help=".opt or web-template.json"),
    macros: bool = typer.Option(True, "--macros/--raw", help="Use macro shortcuts"),
    output: Path = typer.Option(Path("-"), "-o", help="'-' → stdout"),
):
    """
    Generate a YAML mapping skeleton from an OPT or web-template.
    """
    tpl = TemplateParser(template)
    root = tpl.web_template   # already JSON

    rows = []
    for jpath, rm, req in _walk(root):
        if rm in {"DV_TEXT", "DV_CODED_TEXT", "DV_DATE", "DV_DATE_TIME",
                  "DV_BOOLEAN", "DV_QUANTITY", "DV_COUNT"}:
            if macros and rm == "DV_CODED_TEXT":
                stub = {"code": "", "term": ""}
            else:
                stub = "xpath: "  # placeholder
            rows.append((jpath, rm, "yes" if req else ""))
    if output == Path("-"):
        sys.stdout.write(
            "# skeleton mapping\n" +
            "\n".join(f"{p}: {json.dumps(s, ensure_ascii=False)}" for p, _, _ in rows)
        )
    else:
        output.write_text(
            "\n".join(f"{p}: {json.dumps(s, ensure_ascii=False)}" for p, _, _ in rows)
        )
        typer.echo(f"✓ skeleton written to {output}")

if __name__ == "__main__":
    app()