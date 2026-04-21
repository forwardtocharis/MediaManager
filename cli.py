"""
cli.py — MediaManager command-line interface.

Usage:
  python cli.py scan
  python cli.py status
  python cli.py identify [--limit N]
  python cli.py export-llm [--output FILE]
  python cli.py import-llm --input FILE
  python cli.py export-manual [--output FILE]
  python cli.py import-manual --input FILE
  python cli.py apply [--phase 1|2|manual|all] [--dry-run] [--limit N]
  python cli.py rollback [--dry-run]
  python cli.py resume-api --api tmdb|omdb
  python cli.py cancel-api --api tmdb|omdb
  python cli.py duplicates
"""

import json
import logging
import sys
from pathlib import Path

import click
import yaml
from rich.console import Console
from rich.table import Table
from rich import box

console = Console()

# ─── Config loading ───────────────────────────────────────────────────────────

def _load_config(config_path: str = "config.yml") -> dict:
    cfg_file = Path(config_path)
    if not cfg_file.exists():
        console.print(f"[red]Config file not found: {cfg_file.resolve()}[/red]")
        console.print("[dim]Copy config.yml and fill in your API keys and paths.[/dim]")
        sys.exit(1)
    with open(cfg_file, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _setup(cfg: dict) -> None:
    """Initialize DB and logging from config."""
    from src import db
    db_path = cfg.get("database", {}).get("path", "mediamanager.db")
    db.init(db_path)

    log_level = cfg.get("logging", {}).get("level", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(levelname)s %(name)s: %(message)s",
    )


def _make_clients(cfg: dict):
    """Instantiate TMDB and OMDB clients. Returns (tmdb, omdb)."""
    from src.api.tmdb import TMDBClient
    from src.api.omdb import OMDBClient

    api_cfg = cfg.get("api", {})
    rl = cfg.get("rate_limits", {})

    tmdb_key = api_cfg.get("tmdb_key", "").strip()
    omdb_key = api_cfg.get("omdb_key", "").strip()

    tmdb = None
    omdb = None

    if tmdb_key:
        tmdb_rl = rl.get("tmdb", {})
        tmdb = TMDBClient(
            api_key=tmdb_key,
            daily_limit=tmdb_rl.get("daily_limit", 0),
            requests_per_second=tmdb_rl.get("requests_per_second", 40),
        )
    else:
        console.print("[yellow]Warning: No TMDB API key configured.[/yellow]")

    if omdb_key:
        omdb_rl = rl.get("omdb", {})
        omdb = OMDBClient(
            api_key=omdb_key,
            daily_limit=omdb_rl.get("daily_limit", 950),
            requests_per_second=omdb_rl.get("requests_per_second", 2),
        )

    return tmdb, omdb


def _media_extensions(cfg: dict) -> set[str]:
    return set(cfg.get("files", {}).get("media_extensions", []))


def _subtitle_extensions(cfg: dict) -> set[str]:
    return set(cfg.get("files", {}).get("subtitle_extensions", []))


# ─── CLI group ────────────────────────────────────────────────────────────────

@click.group()
@click.option("--config", default="config.yml", show_default=True,
              help="Path to config.yml")
@click.pass_context
def cli(ctx, config):
    """MediaManager — Identify, rename, and tag your media library."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config


# ─── scan ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.pass_context
def scan(ctx):
    """Phase 0: Walk the NAS and populate the database."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)

    from src.scanner import scan as do_scan, link_subtitles_to_media

    source = cfg.get("source", {})
    movies_path = source.get("movies_path", "").strip() or None
    tv_path = source.get("tv_path", "").strip() or None

    stats = do_scan(
        movies_path=movies_path,
        tv_path=tv_path,
        media_extensions=_media_extensions(cfg),
        subtitle_extensions=_subtitle_extensions(cfg),
    )

    # Link subtitles to their media files
    with console.status("Linking subtitle files to media..."):
        linked = link_subtitles_to_media()

    _print_scan_summary(stats, linked)


def _print_scan_summary(stats: dict, linked: int) -> None:
    table = Table(title="Scan Summary", box=box.ROUNDED)
    table.add_column("Metric", style="cyan")
    table.add_column("Count", justify="right", style="bold")
    table.add_row("Media files found", str(stats["media_found"]))
    table.add_row("Media files new (added to DB)", str(stats["media_new"]))
    table.add_row("Subtitle files found", str(stats["subtitles_found"]))
    table.add_row("Subtitle files new", str(stats["subtitles_new"]))
    table.add_row("Extras detected", str(stats["extras_found"]))
    table.add_row("Subtitles linked to media", str(linked))
    table.add_row("Errors", str(stats["errors"]))
    console.print(table)


# ─── status ───────────────────────────────────────────────────────────────────

@cli.command()
@click.pass_context
def status(ctx):
    """Show overall pipeline status."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)

    from src import db

    status_counts = db.count_by_status()
    phase_counts = db.count_by_phase()

    # Status table
    status_table = Table(title="Media Files by Status", box=box.ROUNDED)
    status_table.add_column("Status", style="cyan")
    status_table.add_column("Count", justify="right", style="bold")
    for status_name, count in sorted(status_counts.items()):
        color = {
            "identified": "green",
            "applied": "bright_green",
            "pending": "yellow",
            "needs_llm": "orange3",
            "needs_manual": "red",
            "error": "bold red",
            "skipped": "dim",
        }.get(status_name, "white")
        status_table.add_row(f"[{color}]{status_name}[/{color}]", str(count))
    console.print(status_table)

    # Phase table
    phase_table = Table(title="Identified Files by Phase", box=box.ROUNDED)
    phase_table.add_column("Phase", style="cyan")
    phase_table.add_column("Count", justify="right", style="bold")
    phase_labels = {0: "Unprocessed", 1: "Auto (Phase 1)", 2: "LLM (Phase 2)", 3: "Manual"}
    for phase_num, count in sorted(phase_counts.items()):
        phase_table.add_row(phase_labels.get(phase_num, str(phase_num)), str(count))
    console.print(phase_table)

    # API rate limit status
    from src import db as _db
    _db.init_rate_limit("tmdb")
    _db.init_rate_limit("omdb")
    api_table = Table(title="API Rate Limit Status", box=box.ROUNDED)
    api_table.add_column("API")
    api_table.add_column("Requests Today", justify="right")
    api_table.add_column("Paused?", style="bold")
    api_table.add_column("Reason")
    for api_name in ("tmdb", "omdb"):
        row = _db.get_rate_limit(api_name)
        if row:
            paused = "[red]YES[/red]" if row["paused"] else "[green]No[/green]"
            api_table.add_row(api_name.upper(), str(row["requests_today"]),
                              paused, row["pause_reason"] or "")
    console.print(api_table)


# ─── identify ─────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--limit", default=None, type=int, help="Max files to process")
@click.pass_context
def identify(ctx, limit):
    """Phase 1: Auto-identify pending media files via TMDB/OMDB."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)

    tmdb, omdb = _make_clients(cfg)
    if not tmdb:
        console.print("[red]TMDB API key required for identification. Add it to config.yml.[/red]")
        sys.exit(1)

    from src.identifier import run as do_identify
    ident_cfg = cfg.get("identification", {})

    stats = do_identify(
        tmdb=tmdb,
        omdb=omdb,
        auto_confirm_threshold=ident_cfg.get("auto_confirm_threshold", 75),
        llm_threshold=ident_cfg.get("llm_threshold", 50),
        limit=limit,
    )

    _print_identify_summary(stats)


def _print_identify_summary(stats: dict) -> None:
    table = Table(title="Identification Results", box=box.ROUNDED)
    table.add_column("Result", style="cyan")
    table.add_column("Count", justify="right", style="bold")
    table.add_row("[green]Auto-identified[/green]", str(stats["identified"]))
    table.add_row("[orange3]Needs LLM review[/orange3]", str(stats["needs_llm"]))
    table.add_row("[red]Needs manual review[/red]", str(stats["needs_manual"]))
    table.add_row("Errors", str(stats["errors"]))
    table.add_row("Total processed", str(stats["processed"]))
    console.print(table)


# ─── export-llm ───────────────────────────────────────────────────────────────

@cli.command("export-llm")
@click.option("--output", default="llm_review.csv", show_default=True,
              help="Output CSV path")
@click.pass_context
def export_llm(ctx, output):
    """Phase 2a: Export unidentified files to CSV for LLM review."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)

    from src.llm_export import export_for_llm
    out = Path(output)
    prompt = out.parent / (out.stem + "_prompt.txt")
    export_for_llm(out, prompt)


# ─── import-llm ───────────────────────────────────────────────────────────────

@cli.command("import-llm")
@click.option("--input", "input_file", required=True, help="LLM-completed CSV path")
@click.option("--no-validate", is_flag=True, default=False,
              help="Skip TMDB re-validation (accept LLM answers as-is)")
@click.pass_context
def import_llm(ctx, input_file, no_validate):
    """Phase 2b: Import LLM-completed CSV and re-validate against TMDB."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)

    tmdb, _ = _make_clients(cfg)
    from src.llm_export import import_from_llm
    stats = import_from_llm(Path(input_file), tmdb, validate=not no_validate)

    table = Table(title="LLM Import Results", box=box.ROUNDED)
    table.add_column("Result", style="cyan")
    table.add_column("Count", justify="right", style="bold")
    table.add_row("[green]Confirmed[/green]", str(stats["confirmed"]))
    table.add_row("[red]Failed validation[/red]", str(stats["failed_validation"]))
    table.add_row("[yellow]LLM skipped (couldn't ID)[/yellow]", str(stats["skipped"]))
    console.print(table)


# ─── export-manual ────────────────────────────────────────────────────────────

@cli.command("export-manual")
@click.option("--output", default="manual_review.csv", show_default=True,
              help="Output CSV path")
@click.pass_context
def export_manual(ctx, output):
    """Phase 2c: Export items needing manual review to CSV."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)

    from src.llm_export import export_for_manual
    export_for_manual(Path(output))


# ─── import-manual ────────────────────────────────────────────────────────────

@cli.command("import-manual")
@click.option("--input", "input_file", required=True, help="Manually-edited CSV path")
@click.pass_context
def import_manual(ctx, input_file):
    """Phase 2c: Import manually-completed CSV."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)

    tmdb, _ = _make_clients(cfg)
    from src.llm_export import import_from_manual
    stats = import_from_manual(Path(input_file), tmdb)
    console.print(f"[green]Imported:[/green] {stats['confirmed']} confirmed, "
                  f"{stats['failed_validation']} failed validation")


# ─── apply ────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--phase", "phases", multiple=True, default=["1"],
              type=click.Choice(["1", "2", "manual", "all"]),
              help="Which phases to apply (can specify multiple times)")
@click.option("--dry-run/--no-dry-run", default=True, show_default=True,
              help="Preview changes without writing files")
@click.option("--limit", default=None, type=int, help="Max files to process")
@click.pass_context
def apply(ctx, phases, dry_run, limit):
    """Phase 3: Apply renames, moves, and NFO writes."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)

    if not dry_run:
        console.print(
            "[bold red]WARNING: This will move/rename files on your NAS. "
            "Ensure you have tested with --dry-run first.[/bold red]"
        )
        if not click.confirm("Continue?"):
            console.print("Aborted.")
            return

    source = cfg.get("source", {})
    output = cfg.get("output", {})

    movies_out = (output.get("movies_path") or source.get("movies_path", "")).strip()
    tv_out = (output.get("tv_path") or source.get("tv_path", "")).strip()

    if not movies_out or not tv_out:
        console.print("[red]Output paths not configured. Check config.yml.[/red]")
        sys.exit(1)

    from src.applier import run as do_apply
    stats = do_apply(
        movies_output=movies_out,
        tv_output=tv_out,
        phases=list(phases),
        dry_run=dry_run,
        limit=limit,
    )

    table = Table(title=f"Apply Results ({'DRY RUN' if dry_run else 'LIVE'})", box=box.ROUNDED)
    table.add_column("Result", style="cyan")
    table.add_column("Count", justify="right", style="bold")
    table.add_row("[green]Applied[/green]", str(stats["applied"]))
    table.add_row("[yellow]Skipped[/yellow]", str(stats["skipped"]))
    table.add_row("[red]Errors[/red]", str(stats["errors"]))
    table.add_row("Total processed", str(stats["processed"]))
    console.print(table)


# ─── rollback ─────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--dry-run/--no-dry-run", default=True, show_default=True)
@click.pass_context
def rollback(ctx, dry_run):
    """Reverse all applied changes using the manifest."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)

    if not dry_run:
        console.print("[bold red]WARNING: This will attempt to reverse all file moves.[/bold red]")
        if not click.confirm("Continue?"):
            return

    from src.applier import rollback_all
    stats = rollback_all(dry_run=dry_run)
    console.print(f"Rollback: {stats['restored']} restored, {stats['errors']} errors")


# ─── resume-api / cancel-api ──────────────────────────────────────────────────

@cli.command("resume-api")
@click.option("--api", required=True, type=click.Choice(["tmdb", "omdb"]))
@click.pass_context
def resume_api(ctx, api):
    """Unpause a rate-limited API to allow identification to continue."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)
    from src import db
    db.init_rate_limit(api)
    db.set_api_paused(api, False)
    console.print(f"[green]{api.upper()} API resumed.[/green]")


@cli.command("cancel-api")
@click.option("--api", required=True, type=click.Choice(["tmdb", "omdb"]))
@click.pass_context
def cancel_api(ctx, api):
    """Permanently pause an API (until manually resumed)."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)
    from src import db
    db.init_rate_limit(api)
    db.set_api_paused(api, True, "Manually cancelled by user")
    console.print(f"[yellow]{api.upper()} API paused.[/yellow]")


# ─── duplicates ───────────────────────────────────────────────────────────────

@cli.command()
@click.pass_context
def duplicates(ctx):
    """Review and resolve duplicate media files."""
    cfg = _load_config(ctx.obj["config_path"])
    _setup(cfg)

    from src import db

    dups = db.get_pending_duplicates()
    if not dups:
        console.print("[green]No pending duplicates.[/green]")
        return

    for dup in dups:
        media_ids = json.loads(dup["media_ids"])
        console.print(f"\n[bold]Duplicate group — TMDB ID: {dup['tmdb_id']}[/bold]")

        table = Table(box=box.SIMPLE)
        table.add_column("DB ID", width=6)
        table.add_column("Filename", max_width=50)
        table.add_column("Size", justify="right", width=10)
        table.add_column("Resolution")
        table.add_column("Original Path", max_width=50)

        for mid in media_ids:
            row = db.get_media_file(mid)
            if row:
                size_mb = f"{(row['file_size'] or 0) / (1024*1024):.0f} MB"
                table.add_row(
                    str(row["id"]), row["filename"],
                    size_mb, row.get("resolution") or "unknown",
                    row["original_path"]
                )
        console.print(table)

        choice = click.prompt(
            f"Action: [keep <id>] to keep one file, [all] to keep all, [skip] to decide later",
            default="skip"
        )

        if choice.startswith("keep "):
            keep_id = choice.split()[1]
            db.resolve_duplicate(dup["id"], f"keep_id:{keep_id}")
            # Mark others as skipped
            for mid in media_ids:
                if str(mid) != keep_id:
                    db.update_media_file(mid, status="skipped",
                                         notes=f"Duplicate — kept ID {keep_id}")
            console.print(f"[green]Keeping ID {keep_id}.[/green]")
        elif choice == "all":
            db.resolve_duplicate(dup["id"], "keep_all")
            console.print("[green]Keeping all copies.[/green]")
        else:
            console.print("[dim]Skipped — decide later.[/dim]")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli(obj={})
