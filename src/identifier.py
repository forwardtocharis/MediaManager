"""
identifier.py — Phase 1: Automated identification of media files.

For each 'pending' file in the DB:
  1. Use guessit-parsed title/year/type as search candidates
  2. Search TMDB (primary), with OMDB fallback for corroboration
  3. Score the best match
  4. Write results back to DB with status: identified | needs_llm | needs_manual
"""

import json
import logging
from typing import Optional

from rapidfuzz import fuzz
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from src import db
from src.api.tmdb import TMDBClient, RateLimitPausedError as TMDBPaused
from src.api.omdb import OMDBClient, RateLimitPausedError as OMDBPaused

logger = logging.getLogger(__name__)
console = Console()


# ─── Scoring weights ──────────────────────────────────────────────────────────

W_TITLE = 40       # Fuzzy title match vs TMDB result
W_YEAR = 20        # Year match (exact=20, ±1=10, else 0)
W_TYPE = 20        # Movie/TV type agreement
W_CORROBORATE = 20 # TMDB + OMDB both agree on the match


def run(
    tmdb: TMDBClient,
    omdb: Optional[OMDBClient],
    auto_confirm_threshold: int,
    llm_threshold: int,
    limit: Optional[int] = None,
) -> dict:
    """
    Run Phase 1 identification on all 'pending' media files.
    Returns a summary dict.
    """
    pending = db.get_media_by_status("pending")
    if limit:
        pending = pending[:limit]

    if not pending:
        console.print("[yellow]No pending files to identify.[/yellow]")
        return {"processed": 0, "identified": 0, "needs_llm": 0, "needs_manual": 0, "errors": 0}

    stats = {"processed": 0, "identified": 0, "needs_llm": 0, "needs_manual": 0, "errors": 0}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Identifying media", total=len(pending))

        for row in pending:
            progress.advance(task)
            stats["processed"] += 1

            try:
                result = _identify_one(row, tmdb, omdb, auto_confirm_threshold, llm_threshold)
                _bucket = result.get("status", "needs_manual")
                if _bucket == "identified":
                    stats["identified"] += 1
                elif _bucket == "needs_llm":
                    stats["needs_llm"] += 1
                else:
                    stats["needs_manual"] += 1

            except (TMDBPaused, OMDBPaused) as e:
                console.print(f"\n[bold red]API rate limit hit -> pausing.[/bold red]\n{e}")
                break
            except Exception as e:
                logger.warning("Error identifying %s: %s", row["filename"], e)
                db.update_media_file(row["id"], status="error", notes=str(e))
                stats["errors"] += 1

    return stats


def _identify_one(
    row,
    tmdb: TMDBClient,
    omdb: Optional[OMDBClient],
    auto_confirm_threshold: int,
    llm_threshold: int,
) -> dict:
    """Identify a single media file. Updates DB. Returns the status dict."""
    media_id = row["id"]
    title_guess = row["guessed_title"] or ""
    year_guess = row["guessed_year"]
    type_guess = row["guessed_type"]  # 'movie' | 'tv' | 'unknown'

    if not title_guess:
        # Nothing to search on
        db.update_media_file(media_id, status="needs_manual", confidence=0,
                             notes="No title could be parsed from filename")
        return {"status": "needs_manual"}

    # ── TMDB search ──
    best_match, confidence = _tmdb_search(tmdb, title_guess, year_guess, type_guess)

    # ── OMDB corroboration ──
    omdb_agrees = False
    omdb_data = None
    if omdb and best_match and confidence >= llm_threshold:
        omdb_data = _omdb_corroborate(omdb, best_match, year_guess)
        if omdb_data:
            omdb_agrees = True
            confidence = min(100, confidence + W_CORROBORATE)

    # ── Duplicate detection ──
    if best_match and best_match.get("tmdb_id"):
        _check_for_duplicate(media_id, best_match["tmdb_id"])

    # ── Determine status ──
    if confidence >= auto_confirm_threshold and best_match:
        status = "identified"
        phase = 1
    elif confidence >= llm_threshold and best_match:
        status = "needs_llm"
        phase = 0
    else:
        status = "needs_manual" if not best_match else "needs_llm"
        phase = 0

    # ── Merge API data ──
    updates = {
        "confidence": confidence,
        "status": status,
        "phase": phase,
    }

    if best_match:
        updates.update({
            "tmdb_id": best_match.get("tmdb_id"),
            "imdb_id": best_match.get("imdb_id"),
            "confirmed_title": best_match.get("confirmed_title"),
            "confirmed_year": best_match.get("confirmed_year"),
            "confirmed_type": best_match.get("confirmed_type"),
            "genres": json.dumps(best_match.get("genres") or []),
            "plot": best_match.get("plot"),
            "rating": best_match.get("rating"),
            "director": best_match.get("director"),
            "cast": json.dumps(best_match.get("cast") or []),
        })

        # For TV: fetch episode details if we have season/episode
        if (best_match.get("confirmed_type") == "tv"
                and row["guessed_season"] and row["guessed_episode"]):
            ep = _fetch_episode_details(tmdb, best_match["tmdb_id"],
                                         row["guessed_season"], row["guessed_episode"])
            if ep:
                updates["season"] = row["guessed_season"]
                updates["episode"] = row["guessed_episode"]
                updates["episode_title"] = ep.get("episode_title")
                if ep.get("plot"):
                    updates["plot"] = ep["plot"]
                updates["air_date"] = ep.get("air_date")

    db.update_media_file(media_id, **updates)
    return {"status": status, "confidence": confidence}


# ─── TMDB helpers ─────────────────────────────────────────────────────────────

def _tmdb_search(
    tmdb: TMDBClient,
    title: str,
    year: Optional[int],
    type_guess: str,
) -> tuple[Optional[dict], float]:
    """
    Search TMDB for the best match. Returns (details_dict, confidence_score).
    Tries movie search + TV search and returns the higher-confidence result.
    """
    best_match = None
    best_score = 0.0

    candidates = []

    if type_guess in ("movie", "unknown"):
        results = tmdb.search_movie(title, year)
        for r in results[:3]:
            score = _score_tmdb_movie(r, title, year, type_guess)
            candidates.append(("movie", r, score))

    if type_guess in ("tv", "unknown"):
        results = tmdb.search_tv(title, year)
        for r in results[:3]:
            score = _score_tmdb_tv(r, title, year, type_guess)
            candidates.append(("tv", r, score))

    if not candidates:
        return None, 0.0

    # Pick the highest-scoring candidate
    candidates.sort(key=lambda x: x[2], reverse=True)
    best_type, best_result, best_score = candidates[0]

    if best_score < 10:
        return None, 0.0

    # Fetch full details for the top candidate
    tmdb_id = best_result.get("id")
    if not tmdb_id:
        return None, best_score

    if best_type == "movie":
        details = tmdb.get_movie_details(tmdb_id)
    else:
        details = tmdb.get_tv_details(tmdb_id)

    return details, min(best_score, W_TITLE + W_YEAR + W_TYPE)


def _score_tmdb_movie(result: dict, title: str, year: Optional[int],
                      type_guess: str) -> float:
    tmdb_title = result.get("title", "")
    score = 0.0

    # Title similarity
    score += (fuzz.token_sort_ratio(title.lower(), tmdb_title.lower()) / 100.0) * W_TITLE

    # Year match
    release = result.get("release_date", "")
    tmdb_year = int(release[:4]) if release and len(release) >= 4 else None
    score += _year_score(year, tmdb_year)

    # Type match
    if type_guess in ("movie", "unknown"):
        score += W_TYPE

    return score


def _score_tmdb_tv(result: dict, title: str, year: Optional[int],
                   type_guess: str) -> float:
    tmdb_title = result.get("name", "")
    score = 0.0

    score += (fuzz.token_sort_ratio(title.lower(), tmdb_title.lower()) / 100.0) * W_TITLE

    first_air = result.get("first_air_date", "")
    tmdb_year = int(first_air[:4]) if first_air and len(first_air) >= 4 else None
    score += _year_score(year, tmdb_year)

    if type_guess in ("tv", "unknown"):
        score += W_TYPE

    return score


def _year_score(guess: Optional[int], actual: Optional[int]) -> float:
    if guess is None or actual is None:
        return W_YEAR * 0.5  # Neutral when we have no year
    if guess == actual:
        return W_YEAR
    if abs(guess - actual) == 1:
        return W_YEAR * 0.5
    return 0.0


# ─── OMDB corroboration ───────────────────────────────────────────────────────

def _omdb_corroborate(omdb: OMDBClient, tmdb_match: dict,
                      year_guess: Optional[int]) -> Optional[dict]:
    """Ask OMDB for the same title; return data if it agrees."""
    title = tmdb_match.get("confirmed_title")
    year = tmdb_match.get("confirmed_year") or year_guess
    media_type = "movie" if tmdb_match.get("confirmed_type") == "movie" else "series"
    try:
        return omdb.get_details(title, year, media_type)
    except OMDBPaused:
        raise  # let the caller loop handle the API pause
    except Exception:
        return None


# ─── Episode detail fetch ─────────────────────────────────────────────────────

def _fetch_episode_details(tmdb: TMDBClient, show_id: int,
                           season: int, episode: int) -> Optional[dict]:
    try:
        return tmdb.get_episode_details(show_id, season, episode)
    except Exception:
        return None


# ─── Duplicate detection ──────────────────────────────────────────────────────

def _check_for_duplicate(current_id: int, tmdb_id: int) -> None:
    """
    Check if another media file is already identified with the same TMDB ID.
    If so, record the duplicate group.
    """
    with db.connect() as conn:
        existing = conn.execute(
            "SELECT id FROM media_files WHERE tmdb_id=? AND id!=? AND status='identified'",
            (tmdb_id, current_id)
        ).fetchall()

    if existing:
        existing_ids = [r["id"] for r in existing]
        all_ids = existing_ids + [current_id]
        # Check if a duplicate group already exists for these
        with db.connect() as conn:
            dup = conn.execute(
                "SELECT id FROM duplicates WHERE tmdb_id=? AND resolution='pending'",
                (tmdb_id,)
            ).fetchone()
        if not dup:
            db.add_duplicate_group(tmdb_id, all_ids)
            logger.info("Duplicate detected: TMDB %d → file IDs %s", tmdb_id, all_ids)
