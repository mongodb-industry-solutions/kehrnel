# ──────────────────────────────────────────────────────────────────────────────
# src/cli/validate.py   →  `kehrnel-validate`
# ──────────────────────────────────────────────────────────────────────────────
"""Validate an existing composition JSON against an OPT template."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from core import kehrnelValidator, TemplateParser
from core.models import Severity

app = typer.Typer(add_completion=False, rich_markup_mode="rich")
console = Console()


def format_severity(severity: Severity) -> str:
    """Format severity with color."""
    colors = {
        Severity.ERROR: "[red]ERROR[/red]",
        Severity.WARNING: "[yellow]WARNING[/yellow]",
        Severity.INFO: "[blue]INFO[/blue]"
    }
    return colors.get(severity, str(severity.value))


@app.command()
def main(
    composition: Path = typer.Option(..., "-c", help="Composition JSON"),
    opt: Path = typer.Option(..., "-t", help="OPT template (.opt)"),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Show detailed validation information"),
    json_output: bool = typer.Option(False, "--json", help="Output results as JSON"),
    no_color: bool = typer.Option(False, "--no-color", help="Disable colored output"),
    fail_on_warning: bool = typer.Option(False, "--fail-on-warning", help="Exit with error code if warnings are found"),
    show_stats: bool = typer.Option(False, "-s", "--stats", help="Show validation statistics"),
):
    """
    Validate an OpenEHR composition against a template.
    
    Exit codes:
    - **0**: Composition is valid
    - **1**: Validation errors found (or warnings if --fail-on-warning)
    - **2**: File not found or parse error
    """
    
    # Validate input files
    if not composition.exists():
        typer.secho(f"Error: Composition file not found: {composition}", fg="red", err=True)
        raise typer.Exit(2)
    
    if not opt.exists():
        typer.secho(f"Error: Template file not found: {opt}", fg="red", err=True)
        raise typer.Exit(2)
    
    try:
        # Load composition
        data = json.loads(composition.read_text())
    except json.JSONDecodeError as e:
        typer.secho(f"Error: Invalid JSON in composition file: {e}", fg="red", err=True)
        raise typer.Exit(2)
    
    try:
        # Parse template
        tpl = TemplateParser(opt)
    except Exception as e:
        typer.secho(f"Error parsing template: {e}", fg="red", err=True)
        raise typer.Exit(2)
    
    # Validate
    issues = kehrnelValidator(tpl).validate(data)
    
    # Handle JSON output
    if json_output:
        result = {
            "valid": len(issues) == 0,
            "template_id": tpl.template_id,
            "issues": [
                {
                    "path": issue.path,
                    "message": issue.message,
                    "severity": issue.severity.value,
                    "code": issue.code,
                    "expected": issue.expected,
                    "found": issue.found
                }
                for issue in issues
            ],
            "summary": {
                "total": len(issues),
                "errors": sum(1 for i in issues if i.severity == Severity.ERROR),
                "warnings": sum(1 for i in issues if i.severity == Severity.WARNING),
                "info": sum(1 for i in issues if i.severity == Severity.INFO)
            }
        }
        typer.echo(json.dumps(result, indent=2))
    else:
        # Human-readable output
        if verbose:
            console.print(f"[dim]Validating: {composition}[/dim]")
            console.print(f"[dim]Template: {opt}[/dim]")
            console.print(f"[dim]Template ID: '{tpl.template_id}' (length: {len(tpl.template_id)})[/dim]")
            if tpl.template_id and tpl.template_id != tpl.template_id.strip():
                console.print(f"[yellow]Warning: Template ID contains whitespace[/yellow]")
            console.print()
        
        if not issues:
            if no_color:
                typer.echo("✓ Composition is valid")
            else:
                console.print("[green]✓ Composition is valid[/green]")
        else:
            # Show issues
            if verbose and not no_color:
                # Use rich table for verbose output
                table = Table(title="Validation Issues", show_header=True)
                table.add_column("Severity", style="bold")
                table.add_column("Path")
                table.add_column("Message")
                if verbose:
                    table.add_column("Details")
                
                for issue in issues:
                    details = []
                    if issue.code:
                        details.append(f"Code: {issue.code}")
                    if issue.expected is not None:
                        details.append(f"Expected: {issue.expected}")
                    if issue.found is not None:
                        details.append(f"Found: {issue.found}")
                    
                    if verbose and details:
                        detail_str = "\n".join(details)
                    else:
                        detail_str = issue.code or ""
                    
                    severity_str = format_severity(issue.severity) if not no_color else issue.severity.value
                    
                    if verbose:
                        table.add_row(
                            severity_str,
                            issue.path,
                            issue.message,
                            detail_str
                        )
                    else:
                        table.add_row(
                            severity_str,
                            issue.path,
                            issue.message
                        )
                
                console.print(table)
            else:
                # Simple output
                for issue in issues:
                    if no_color:
                        typer.echo(f"[{issue.severity.value}] {issue.path}: {issue.message}")
                    else:
                        severity_str = format_severity(issue.severity)
                        console.print(f"{severity_str} {issue.path}: {issue.message}")
                        
                    if verbose:
                        if issue.code:
                            typer.echo(f"         Code: {issue.code}")
                        if issue.expected is not None:
                            typer.echo(f"         Expected: {issue.expected}")
                        if issue.found is not None:
                            typer.echo(f"         Found: {issue.found}")
        
        # Show statistics if requested
        if show_stats and issues:
            console.print("\n[bold]Validation Summary:[/bold]")
            
            error_count = sum(1 for i in issues if i.severity == Severity.ERROR)
            warning_count = sum(1 for i in issues if i.severity == Severity.WARNING)
            info_count = sum(1 for i in issues if i.severity == Severity.INFO)
            
            stats_table = Table(show_header=False, box=None)
            stats_table.add_column("Type", style="bold")
            stats_table.add_column("Count", justify="right")
            
            if error_count > 0:
                stats_table.add_row("[red]Errors[/red]", str(error_count))
            if warning_count > 0:
                stats_table.add_row("[yellow]Warnings[/yellow]", str(warning_count))
            if info_count > 0:
                stats_table.add_row("[blue]Info[/blue]", str(info_count))
            stats_table.add_row("[dim]Total[/dim]", f"[dim]{len(issues)}[/dim]")
            
            console.print(stats_table)
    
    # Determine exit code
    error_count = sum(1 for i in issues if i.severity == Severity.ERROR)
    warning_count = sum(1 for i in issues if i.severity == Severity.WARNING)
    
    if error_count > 0:
        raise typer.Exit(1)
    elif fail_on_warning and warning_count > 0:
        raise typer.Exit(1)
    else:
        raise typer.Exit(0)


if __name__ == "__main__":
    app()