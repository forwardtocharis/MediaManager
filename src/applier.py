"""
applier.py — Phase 3: Apply renames, moves, and metadata writes.

Design:
  - Dry-run mode always available (no writes)
  - Per-phase targeting (apply only Phase 1 results, Phase 2, manual, or all)
  - Copy → size-verify → write NFO → rename subtitles → delete original
  - Every operation logged to apply_manifest for rollback
  - Safe path collision handling via ensure_unique_path
"""

import json
import logging
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from src import db
from src.utils.file_utils import (
    build_movie_path, build_movie_extra_path,
    build_tv_episode_path, build_subtitle_path, build_nfo_path,
    build_tvshow_nfo_path, safe_copy, safe_delete, ensure_unique_path,
)
from src.utils.nfo_writer import write_movie_nfo, write_tvshow_nfo, write_episode_nfo

logger = logging.getLogger(__name__)
console = Console()

# Phase → minimum phase number required in DB
_PHASE_FILTER = {
    "1": 1,
    "2": 2,
    "manual": 3,
}


def compute_proposed_paths(row, movies_output: str, tv_output: str) -> dict | None:
    """
    Compute the proposed file & sidecar paths for a row WITHOUT writing anything.
    Returns a dict with keys: proposed_path, proposed_folder, proposed_filename,
    nfo_path, and (for TV) tvshow_nfo_path.
    Returns None if the path cannot be determined (missing title etc.)
    """
    ext = row["extension"] or ".mkv"
    ctype = (row["confirmed_type"] or "movie").lower()
    orig_parent = str(Path(row["original_path"]).parent)
    is_extra = bool(row["is_extra"])

    try:
        if ctype == "tv":
            base = tv_output or orig_parent
            title  = row["confirmed_title"] or "Unknown Show"
            season  = row["season"]   or row.get("guessed_season") or 1
            episode = row["episode"]  or row.get("guessed_episode") or 1
            ep_title = row["episode_title"] or ""
            dst = build_tv_episode_path(base, title, season, episode, ep_title, ext)
            return {
                "proposed_path":     str(dst),
                "proposed_folder":   str(dst.parent),
                "proposed_filename": dst.name,
                "nfo_path":          str(build_nfo_path(dst)),
                "tvshow_nfo_path":   str(build_tvshow_nfo_path(tv_output or orig_parent,
                                                                title)),
            }
        else:
            base  = movies_output or orig_parent
            title = row["confirmed_title"] or "Unknown"
            year  = row["confirmed_year"] or row.get("guessed_year") or 0
            if is_extra:
                extra_name = _extract_extra_name(
                    Path(row["original_path"]).stem, title, year)
                dst = build_movie_extra_path(base, title, year, extra_name, ext)
            else:
                dst = build_movie_path(base, title, year, ext)
            return {
                "proposed_path":     str(dst),
                "proposed_folder":   str(dst.parent),
                "proposed_filename": dst.name,
                "nfo_path":          str(build_nfo_path(dst)),
            }
    except Exception as e:
        logger.warning("Cannot compute proposed path for %s: %s", row["filename"], e)
        return None


def run(
    movies_output: str,
    tv_output: str,
    phases: list[str],
    dry_run: bool = True,
    limit: Optional[int] = None,
) -> dict:
    """
    Apply file renames/moves and metadata writes.

    phases: list of '1', '2', 'manual', or ['all']
    dry_run: if True, print what would happen but don't write anything.
    """
    if dry_run:
        console.print("[bold yellow]DRY RUN — no files will be modified.[/bold yellow]\n")
    else:
        console.print("[bold green]LIVE RUN — files will be moved.[/bold green]\n")

    # Build set of phase numbers to include
    if "all" in phases:
        phase_nums = {1, 2, 3}
    else:
        phase_nums = {int(p) if p.isdigit() else (3 if p == "manual" else int(p))
                      for p in phases}

    # Fetch identified rows for the requested phases
    with db.connect() as conn:
        placeholders = ",".join("?" * len(phase_nums))
        rows = conn.execute(
            f"SELECT * FROM media_files "
            f"WHERE status='identified' AND phase IN ({placeholders}) "
            f"ORDER BY id",
            list(phase_nums)
        ).fetchall()

    if limit:
        rows = rows[:limit]

    if not rows:
        console.print("[yellow]No identified files ready to apply for the requested phases.[/yellow]")
        return {"processed": 0, "applied": 0, "skipped": 0, "errors": 0}

    stats = {"processed": 0, "applied": 0, "skipped": 0, "errors": 0}

    # For dry-run, show a preview table and return stats (nothing was written)
    if dry_run:
        if len(rows) <= 50:
            _show_preview_table(rows, movies_output, tv_output)
        return {"processed": len(rows), "applied": 0, "skipped": 0, "errors": 0}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Applying changes", total=len(rows))

        # Track show-level NFOs we've written (avoid re-writing same show)
        written_show_nfos: set[int] = set()

        for row in rows:
            progress.advance(task)
            stats["processed"] += 1

            try:
                applied = _apply_one(
                    row, movies_output, tv_output,
                    dry_run, written_show_nfos
                )
                if applied:
                    stats["applied"] += 1
                else:
                    stats["skipped"] += 1
            except Exception as e:
                logger.error("Error applying %s: %s", row["filename"], e)
                db.update_media_file(row["id"], status="error", notes=str(e))
                stats["errors"] += 1

    return stats


def _apply_one(row, movies_output: str, tv_output: str,
               dry_run: bool, written_show_nfos: set) -> bool:
    """Apply changes for a single media file. Returns True if applied."""
    media_id = row["id"]
    src = Path(row["original_path"])
    media_type = row["confirmed_type"]
    title = row["confirmed_title"]
    year = row["confirmed_year"]
    ext = row["extension"]
    is_extra = bool(row["is_extra"])

    if not src.exists():
        logger.warning("Source file missing (already moved?): %s", src)
        db.update_media_file(media_id, status="skipped",
                             notes="Source file not found during apply")
        return False

    # ── Build destination path ──
    if media_type == "movie":
        if is_extra:
            # Use the stem of the main movie name as extra base
            extra_name = _extract_extra_name(src.stem, title, year)
            dst = build_movie_extra_path(movies_output, title, year, extra_name, ext)
        else:
            dst = build_movie_path(movies_output, title, year, ext)
    elif media_type == "tv":
        season = row["season"] or row["guessed_season"] or 1
        episode = row["episode"] or row["guessed_episode"] or 1
        ep_title = row["episode_title"] or ""
        dst = build_tv_episode_path(tv_output, title, season, episode, ep_title, ext)
    else:
        logger.warning("Unknown type for %s — skipping", src.name)
        return False

    dst = ensure_unique_path(dst)

    # ── Store proposed path ──
    db.update_media_file(media_id, proposed_path=str(dst))

    if dry_run:
        console.print(f"  [dim]{src.name}[/dim]\n  → [cyan]{dst}[/cyan]\n")
        return True

    # ── Copy → verify ──
    manifest_id = db.log_manifest_op(media_id, "media", str(src), str(dst), "copy")
    success = safe_copy(src, dst, verify=True)
    if not success:
        console.print(f"[red]Copy verification failed for {src.name}[/red]")
        db.update_media_file(media_id, status="error",
                             notes="Copy verification (size mismatch)")
        return False
    db.mark_manifest_verified(manifest_id)

    # ── Write NFO sidecar ──
    _write_nfo(row, dst, media_type, tv_output, written_show_nfos, dry_run)

    # ── Move subtitles ──
    _move_subtitles(media_id, src, dst, dry_run)

    # ── Delete original ──
    db.log_manifest_op(media_id, "media", str(src), "", "delete")
    safe_delete(src)

    db.update_media_file(media_id, status="applied", proposed_path=str(dst))
    return True


# ─── NFO writing ──────────────────────────────────────────────────────────────

def _write_nfo(row, dst: Path, media_type: str, tv_output: str,
               written_show_nfos: set, dry_run: bool) -> None:
    nfo_path = build_nfo_path(dst)
    nfo_data = _row_to_nfo_data(row)

    if dry_run:
        return

    if media_type == "movie":
        write_movie_nfo(nfo_path, nfo_data)
        db.log_manifest_op(row["id"], "nfo", "", str(nfo_path), "copy")

    elif media_type == "tv":
        # Episode NFO
        write_episode_nfo(nfo_path, nfo_data)
        db.log_manifest_op(row["id"], "nfo", "", str(nfo_path), "copy")

        # TV show-level NFO (write once per show)
        show_id = row["tmdb_id"]
        if show_id and show_id not in written_show_nfos:
            show_nfo_path = build_tvshow_nfo_path(tv_output, row["confirmed_title"])
            if not show_nfo_path.exists():
                write_tvshow_nfo(show_nfo_path, nfo_data)
                db.log_manifest_op(row["id"], "nfo", "", str(show_nfo_path), "copy")
            written_show_nfos.add(show_id)


def _row_to_nfo_data(row) -> dict:
    genres = []
    try:
        genres = json.loads(row["genres"] or "[]")
    except (json.JSONDecodeError, TypeError):
        pass
    cast = []
    try:
        cast = json.loads(row["cast"] or "[]")
    except (json.JSONDecodeError, TypeError):
        pass
    return {
        "title": row["confirmed_title"],
        "year": row["confirmed_year"],
        "plot": row["plot"],
        "genres": genres,
        "rating": row["rating"],
        "director": row["director"],
        "cast": cast,
        "imdb_id": row["imdb_id"],
        "tmdb_id": row["tmdb_id"],
        "episode_title": row["episode_title"],
        "season": row["season"],
        "episode": row["episode"],
        "air_date": row["air_date"],
    }


# ─── Subtitle moving ──────────────────────────────────────────────────────────

def _move_subtitles(media_id: int, old_video_path: Path,
                    new_video_path: Path, dry_run: bool) -> None:
    subs = db.get_subtitles_for_media(media_id)
    for sub in subs:
        sub_src = Path(sub["original_path"])
        if not sub_src.exists():
            continue
        sub_dst = build_subtitle_path(new_video_path, sub["language"], sub["extension"])
        sub_dst = ensure_unique_path(sub_dst)

        if dry_run:
            console.print(f"  [dim]{sub_src.name}[/dim] → [cyan]{sub_dst.name}[/cyan]")
            continue

        m_id = db.log_manifest_op(None, "subtitle", str(sub_src), str(sub_dst), "copy")
        if safe_copy(sub_src, sub_dst, verify=True):
            db.mark_manifest_verified(m_id)
            db.log_manifest_op(None, "subtitle", str(sub_src), "", "delete")
            safe_delete(sub_src)
            db.update_subtitle_file_by_id(
                sub["id"], status="applied", proposed_path=str(sub_dst)
            )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _extract_extra_name(stem: str, title: str, year: int) -> str:
    """
    Try to extract the 'extra name' by stripping the main title from the filename stem.
    Falls back to the raw stem.
    """
    import re
    # Remove the sanitized title from the stem if present
    clean_title = re.escape(title.lower())
    stripped = re.sub(clean_title, "", stem.lower(), flags=re.IGNORECASE).strip(" .-_")
    return stripped or stem


def _show_preview_table(rows, movies_output: str, tv_output: str) -> None:
    """Print a preview table for dry-run mode."""
    table = Table(title="Proposed Changes (Dry Run)", show_lines=True)
    table.add_column("Original", style="dim", max_width=40)
    table.add_column("Type", style="cyan", width=6)
    table.add_column("Proposed Path", style="green", max_width=60)

    for row in rows:
        title = row["confirmed_title"] or "?"
        year = row["confirmed_year"] or 0
        ext = row["extension"]
        media_type = row["confirmed_type"] or "?"

        if media_type == "movie":
            dst = build_movie_path(movies_output, title, year, ext)
        elif media_type == "tv":
            season = row["season"] or row["guessed_season"] or 1
            episode = row["episode"] or row["guessed_episode"] or 1
            dst = build_tv_episode_path(
                tv_output, title, season, episode, row["episode_title"] or "", ext
            )
        else:
            dst = Path("?") / row["filename"]

        table.add_row(row["filename"], media_type, str(dst))

    console.print(table)


# ─── Rollback ─────────────────────────────────────────────────────────────────

def rollback_all(dry_run: bool = True) -> dict:
    """
    Reverse all applied operations using the manifest.
    Copies files back, then marks manifest entries as rolled back.
    """
    ops = db.get_manifest_ops(rolled_back=False)
    if not ops:
        console.print("[yellow]No manifest operations to roll back.[/yellow]")
        return {"processed": 0, "restored": 0, "errors": 0}

    stats = {"processed": 0, "restored": 0, "errors": 0}

    if dry_run:
        console.print(f"[yellow]DRY RUN: would roll back {len(ops)} operations.[/yellow]")
        return stats

    # Process in reverse order: delete → copy (undo copy → delete)
    for op in reversed(ops):
        stats["processed"] += 1
        try:
            if op["operation"] == "delete" and op["original_path"]:
                # The original was deleted — we can't restore it from nothing
                # unless we have a backup. Count as an error and log.
                logger.warning("Cannot restore deleted original: %s", op["original_path"])
                stats["errors"] += 1
            elif op["operation"] == "copy" and op["new_path"]:
                new_path = Path(op["new_path"])
                orig_path_str = op["original_path"]
                
                if not new_path.exists():
                    continue

                if not orig_path_str:
                    # This was a new file (e.g. NFO). Just delete it.
                    if safe_delete(new_path):
                        db.mark_manifest_rolled_back(op["id"])
                        stats["restored"] += 1
                else:
                    # This was a moved file. Copy it back then delete.
                    orig_path = Path(orig_path_str)
                    if safe_copy(new_path, orig_path, verify=True):
                        safe_delete(new_path)
                        db.mark_manifest_rolled_back(op["id"])
                        stats["restored"] += 1
                    else:
                        stats["errors"] += 1
        except Exception as e:
            logger.error("Rollback error: %s", e)
            stats["errors"] += 1

    return stats
