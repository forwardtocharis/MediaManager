"""
scanner.py — Phase 0: Walk the NAS source paths and populate the SQLite database.

Design principles:
  - Read once. No repeated directory walks during later phases.
  - Idempotent. Re-scanning adds new files; already-scanned files are skipped.
  - No API calls. Pure filesystem work.
"""

import logging
import re
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from src import db
from src.utils.filename_parser import parse_filename, detect_subtitle_language, is_extra
from src.utils.file_utils import is_media_file, is_subtitle_file

logger = logging.getLogger(__name__)
console = Console()


def scan(
    movies_path: Optional[str],
    tv_path: Optional[str],
    media_extensions: set[str],
    subtitle_extensions: set[str],
) -> dict:
    """
    Walk the provided source paths and populate the database.

    Returns a summary dict:
      { media_found, media_new, subtitles_found, subtitles_new,
        extras_found, errors }
    """
    stats = {
        "media_found": 0,
        "media_new": 0,
        "subtitles_found": 0,
        "subtitles_new": 0,
        "extras_found": 0,
        "errors": 0,
    }

    paths_to_scan: list[tuple[str, str]] = []  # (path, media_category)
    if movies_path:
        paths_to_scan.append((movies_path, "movie"))
    if tv_path:
        paths_to_scan.append((tv_path, "tv"))

    if not paths_to_scan:
        console.print("[red]No source paths configured. Check config.yml.[/red]")
        return stats

    for source_path, category_hint in paths_to_scan:
        source = Path(source_path)
        if not source.exists():
            console.print(f"[red]Path does not exist or is not accessible: {source}[/red]")
            stats["errors"] += 1
            continue

        console.print(f"\n[bold cyan]Scanning:[/bold cyan] {source}")

        # Collect all files first for progress display
        all_files: list[Path] = []
        with console.status(f"Enumerating files in {source.name}..."):
            try:
                all_files = list(source.rglob("*"))
            except PermissionError as e:
                console.print(f"[red]Permission denied: {e}[/red]")
                stats["errors"] += 1
                continue

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(f"Processing {source.name}", total=len(all_files))

            for file_path in all_files:
                progress.advance(task)

                if not file_path.is_file():
                    continue

                ext = file_path.suffix.lower()

                try:
                    if is_media_file(file_path, media_extensions):
                        _process_media_file(file_path, category_hint, stats)

                    elif is_subtitle_file(file_path, subtitle_extensions):
                        _process_subtitle_file(file_path, stats)

                except Exception as e:
                    logger.warning("Error processing %s: %s", file_path, e)
                    stats["errors"] += 1

        db.set_meta(f"last_scan_{category_hint}", __import__("datetime").datetime.now().isoformat())

    return stats


def _process_media_file(file_path: Path, category_hint: str, stats: dict) -> None:
    """Parse a media file and upsert it into the database."""
    stats["media_found"] += 1

    # Check if already scanned
    existing = db.get_media_by_path(str(file_path))
    if existing:
        return  # Already in DB — don't reset any work done

    file_size = file_path.stat().st_size
    parsed = parse_filename(file_path, use_parent_folder=True)

    # If category_hint is 'movie' or 'tv' and guessit returned 'unknown', use the hint
    media_type = parsed["media_type"]
    if media_type == "unknown":
        media_type = category_hint if category_hint in ("movie", "tv") else "unknown"

    extra = parsed["extra"]
    if extra:
        stats["extras_found"] += 1

    record = {
        "original_path": str(file_path),
        "filename": file_path.name,
        "extension": file_path.suffix.lower(),
        "file_size": file_size,
        "parent_folder": file_path.parent.name,
        "guessed_title": parsed["title"] or None,
        "guessed_year": parsed["year"],
        "guessed_type": media_type,
        "guessed_season": parsed["season"],
        "guessed_episode": parsed["episode"],
        "is_extra": int(extra),
        "status": "pending",
        "phase": 0,
    }

    db.upsert_media_file(record)
    stats["media_new"] += 1
    logger.debug("Added media: %s", file_path.name)


def _process_subtitle_file(file_path: Path, stats: dict) -> None:
    """Parse a subtitle file and upsert it into the database."""
    stats["subtitles_found"] += 1

    existing = db.get_subtitle_by_path(str(file_path))  # query the correct table
    if existing:
        return

    language = detect_subtitle_language(file_path)
    file_size = file_path.stat().st_size

    record = {
        "original_path": str(file_path),
        "filename": file_path.name,
        "extension": file_path.suffix.lower(),
        "file_size": file_size,
        "language": language,
        "status": "pending",
    }

    db.upsert_subtitle_file(record)
    stats["subtitles_new"] += 1
    logger.debug("Added subtitle: %s", file_path.name)


def link_subtitles_to_media() -> int:
    """
    Attempt to match unlinked subtitle files to their parent media file
    by comparing filename stems.
    Returns the number of links made.
    """
    from src.utils.file_utils import match_subtitles_to_media

    unlinked = db.get_unlinked_subtitles()
    if not unlinked:
        return 0

    # Build a map of media stem → media_id for quick lookup
    with db.connect() as conn:
        media_rows = conn.execute(
            "SELECT id, original_path FROM media_files"
        ).fetchall()

    media_stem_map: dict[str, int] = {}
    for row in media_rows:
        p = Path(row["original_path"])
        media_stem_map[p.stem.lower()] = row["id"]

    linked = 0
    for sub in unlinked:
        sub_path = Path(sub["original_path"])
        sub_stem = sub_path.stem.lower()
        # Strip language code suffix (.en, .eng, etc.)
        bare_stem = re.sub(
            r'\.(en|eng|fr|fre|es|spa|de|ger|it|ita|pt|por|ru|rus|'
            r'zh|chi|ja|jpn|ko|kor|ar|ara|nl|dut|sv|swe|no|nor|'
            r'da|dan|fi|fin|pl|pol|cs|cze|hu|hun|tr|tur|hi|hin)$',
            '', sub_stem, flags=re.IGNORECASE
        )

        # Try exact match first, then bare stem
        media_id = media_stem_map.get(sub_stem) or media_stem_map.get(bare_stem)
        if media_id:
            db.update_subtitle_file_by_id(sub["id"], parent_media_id=media_id)
            linked += 1

    return linked
