"""
llm_export.py — Phase 2: CSV export for LLM review and re-import of results.

Workflow:
  1. export_for_llm()  → writes a CSV + a prompt .txt file
  2. User drops the CSV + prompt into their LLM of choice
  3. LLM returns a filled-in CSV
  4. import_from_llm() → re-validates LLM answers against TMDB, updates DB
  5. export_for_manual() → CSV of items LLM couldn't resolve
  6. import_from_manual() → re-validate and update DB
"""

import csv
import json
import logging
from pathlib import Path
from typing import Optional

from src import db
from src.api.tmdb import TMDBClient, RateLimitPausedError
from rich.console import Console

logger = logging.getLogger(__name__)
console = Console()

# ─── CSV column definitions ───────────────────────────────────────────────────

# Columns we populate (for LLM/human to fill in)
LLM_OUTPUT_COLS = [
    "id",
    "original_path",
    "filename",
    "parent_folder",
    "guessed_title",
    "guessed_year",
    "guessed_type",
    "guessed_season",
    "guessed_episode",
    "is_extra",
    "file_size_mb",
    "confidence",
    "notes",
    # ── Columns for LLM/human to fill ──
    "tmdb_id",
    "imdb_id",
    "confirmed_title",
    "confirmed_year",
    "confirmed_type",   # movie | tv
    "season",           # TV only
    "episode",          # TV only
    "skip",             # set to 1 to exclude this file from processing
]

MANUAL_REVIEW_EXTRA_NOTE = (
    "LLM could not confidently identify this item. "
    "Please fill in tmdb_id (or imdb_id), confirmed_title, confirmed_year, "
    "confirmed_type (movie/tv), and season/episode if TV."
)


# ─── Export for LLM ───────────────────────────────────────────────────────────

def export_for_llm(output_csv: Path, output_prompt: Path) -> int:
    """
    Export all 'needs_llm' rows to a CSV file.
    Also writes a companion prompt file explaining the task.
    Returns the number of rows exported.
    """
    rows = db.get_media_by_status("needs_llm")
    if not rows:
        console.print("[yellow]No items need LLM review.[/yellow]")
        return 0

    _write_csv(output_csv, rows, include_existing_api_data=True)
    _write_llm_prompt(output_prompt, len(rows))
    console.print(f"[green]Exported {len(rows)} items → {output_csv}[/green]")
    console.print(f"[green]LLM prompt → {output_prompt}[/green]")
    return len(rows)


def _write_csv(path: Path, rows, include_existing_api_data: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LLM_OUTPUT_COLS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            record = {
                "id": row["id"],
                "original_path": row["original_path"],
                "filename": row["filename"],
                "parent_folder": row["parent_folder"] or "",
                "guessed_title": row["guessed_title"] or "",
                "guessed_year": row["guessed_year"] or "",
                "guessed_type": row["guessed_type"] or "",
                "guessed_season": row["guessed_season"] or "",
                "guessed_episode": row["guessed_episode"] or "",
                "is_extra": row["is_extra"] or 0,
                "file_size_mb": f"{(row['file_size'] or 0) / (1024*1024):.1f}",
                "confidence": f"{row['confidence'] or 0:.1f}",
                "notes": row["notes"] or "",
                # Pre-fill from any existing API data if available
                "tmdb_id": row["tmdb_id"] or "" if include_existing_api_data else "",
                "imdb_id": row["imdb_id"] or "" if include_existing_api_data else "",
                "confirmed_title": row["confirmed_title"] or "",
                "confirmed_year": row["confirmed_year"] or "",
                "confirmed_type": row["confirmed_type"] or row["guessed_type"] or "",
                "season": row["season"] or row["guessed_season"] or "",
                "episode": row["episode"] or row["guessed_episode"] or "",
                "skip": "",
            }
            writer.writerow(record)


def _write_llm_prompt(path: Path, count: int) -> None:
    prompt = f"""You are a media identification assistant. I have a CSV file with {count} unidentified media files (movies and TV shows).

For each row in the CSV, please identify the media and fill in the following columns:
- tmdb_id: The TMDB (The Movie Database) numeric ID for this title. Search at https://www.themoviedb.org if needed.
- imdb_id: The IMDb ID (format: tt1234567) if you know it.
- confirmed_title: The correct, full title of the movie or TV show.
- confirmed_year: The release year (for movies) or first air year (for TV shows).
- confirmed_type: Either "movie" or "tv".
- season: For TV episodes, the season number (leave blank for movies).
- episode: For TV episodes, the episode number (leave blank for movies).
- skip: Set to 1 ONLY if you are genuinely unable to identify this item.

IMPORTANT RULES:
1. Only fill in fields you are confident about. If unsure about a field, leave it blank.
2. Use the filename, parent_folder, guessed_title, and guessed_year as clues.
3. Do NOT guess if you are not reasonably confident — set skip=1 instead.
4. Return ONLY the CSV data with the same columns as the input, plus the filled fields.
5. Do not add any explanation or preamble — just the CSV.

The CSV file is attached. Please return the completed CSV.
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(prompt, encoding="utf-8")


# ─── Import LLM results ───────────────────────────────────────────────────────

def import_from_llm(
    input_csv: Path,
    tmdb: TMDBClient,
    validate: bool = True,
    phase: int = 2,
) -> dict:
    """
    Read LLM-completed CSV, re-validate against TMDB, and update DB.
    Returns stats dict.
    """
    if not input_csv.exists():
        console.print(f"[red]File not found: {input_csv}[/red]")
        return {"processed": 0, "confirmed": 0, "failed_validation": 0, "skipped": 0}

    stats = {"processed": 0, "confirmed": 0, "failed_validation": 0, "skipped": 0}

    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        stats["processed"] += 1
        media_id = _to_int(row.get("id"))
        if not media_id:
            continue

        if row.get("skip", "").strip() == "1":
            db.update_media_file(media_id, status="needs_manual",
                                 notes="LLM marked as unable to identify")
            stats["skipped"] += 1
            continue

        tmdb_id = _to_int(row.get("tmdb_id"))
        confirmed_title = row.get("confirmed_title", "").strip()
        confirmed_year = _to_int(row.get("confirmed_year"))
        confirmed_type = row.get("confirmed_type", "").strip().lower()

        if not confirmed_title and not tmdb_id:
            db.update_media_file(media_id, status="needs_manual",
                                 notes="LLM provided no title or TMDB ID")
            stats["failed_validation"] += 1
            continue

        # Re-validate against TMDB
        if validate and tmdb_id and tmdb:
            try:
                if confirmed_type == "movie":
                    details = tmdb.get_movie_details(tmdb_id)
                else:
                    details = tmdb.get_tv_details(tmdb_id)

                if details:
                    _apply_llm_result(media_id, row, details, phase=phase)
                    stats["confirmed"] += 1
                else:
                    db.update_media_file(media_id, status="needs_manual",
                                         notes=f"TMDB ID {tmdb_id} returned no data")
                    stats["failed_validation"] += 1
            except RateLimitPausedError as e:
                console.print(f"\n[bold red]API paused during LLM import.[/bold red]\n{e}")
                break
        else:
            # Accept without validation (manual import or validate=False)
            _apply_llm_result(media_id, row, {}, phase=phase)
            stats["confirmed"] += 1

    return stats


def _apply_llm_result(media_id: int, csv_row: dict, api_details: dict, phase: int) -> None:
    """Merge CSV row and API details into the DB record."""
    updates = {
        "status": "identified",
        "phase": phase,
        "confirmed_title": api_details.get("confirmed_title") or csv_row.get("confirmed_title"),
        "confirmed_year": api_details.get("confirmed_year") or _to_int(csv_row.get("confirmed_year")),
        "confirmed_type": api_details.get("confirmed_type") or csv_row.get("confirmed_type"),
        "tmdb_id": api_details.get("tmdb_id") or _to_int(csv_row.get("tmdb_id")),
        "imdb_id": api_details.get("imdb_id") or csv_row.get("imdb_id"),
        "genres": json.dumps(api_details.get("genres") or []),
        "plot": api_details.get("plot"),
        "rating": api_details.get("rating"),
        "director": api_details.get("director"),
        "cast": json.dumps(api_details.get("cast") or []),
        "season": _to_int(csv_row.get("season")),
        "episode": _to_int(csv_row.get("episode")),
    }
    # Strip None values to avoid overwriting good data
    updates = {k: v for k, v in updates.items() if v is not None}
    db.update_media_file(media_id, **updates)


# ─── Manual review export / import ───────────────────────────────────────────

def export_for_manual(output_csv: Path) -> int:
    """Export 'needs_manual' rows for human editing."""
    rows = db.get_media_by_status("needs_manual")
    if not rows:
        console.print("[yellow]No items need manual review.[/yellow]")
        return 0
    _write_csv(output_csv, rows, include_existing_api_data=True)
    console.print(f"[green]Exported {len(rows)} items for manual review → {output_csv}[/green]")
    console.print(
        "[dim]Edit the file, fill in tmdb_id/confirmed_title/confirmed_year/"
        "confirmed_type/season/episode, then run: python cli.py import-manual[/dim]"
    )
    return len(rows)


def import_from_manual(input_csv: Path, tmdb: TMDBClient) -> dict:
    """Import manually-edited CSV. Same logic as LLM import but phase=3."""
    return import_from_llm(input_csv, tmdb, validate=True, phase=3)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _to_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None
