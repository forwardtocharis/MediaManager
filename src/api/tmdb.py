"""
tmdb.py — TMDB REST API client with rate-limit awareness.

TMDB API v3 documentation: https://developer.themoviedb.org/reference
"""

import time
import logging
from typing import Optional

import requests

from src import db

logger = logging.getLogger(__name__)

_API_NAME = "tmdb"
_BASE_URL = "https://api.themoviedb.org/3"
_MIN_INTERVAL = 1.0 / 40  # 40 req/s (conservative vs 50 limit)


class TMDBClient:
    def __init__(self, api_key: str, daily_limit: int = 0,
                 requests_per_second: float = 40):
        self.api_key = api_key
        self.daily_limit = daily_limit
        self._interval = 1.0 / max(requests_per_second, 1)
        self._last_request_time: float = 0
        db.init_rate_limit(_API_NAME)

    # ─── Rate limiting ────────────────────────────────────────────────────────

    def _check_limits(self) -> None:
        if db.is_api_paused(_API_NAME):
            state = db.get_rate_limit(_API_NAME)
            reason = state["pause_reason"] if state else "unknown"
            raise RateLimitPausedError(
                f"TMDB API is paused: {reason}. "
                "Run: python cli.py resume-api --api tmdb"
            )

        if self.daily_limit > 0:
            state = db.get_rate_limit(_API_NAME)
            if state and state["requests_today"] >= self.daily_limit:
                db.set_api_paused(
                    _API_NAME, True,
                    f"Daily limit of {self.daily_limit} requests reached"
                )
                raise RateLimitPausedError(
                    f"TMDB daily limit ({self.daily_limit}) reached. "
                    "Run: python cli.py resume-api --api tmdb (tomorrow)"
                )

    def _throttle(self) -> None:
        """Sleep if necessary to respect per-second rate limit."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        wait = self._interval - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_time = time.monotonic()

    def _get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        self._check_limits()
        self._throttle()
        url = f"{_BASE_URL}/{endpoint.lstrip('/')}"
        p = {"api_key": self.api_key}
        if params:
            p.update(params)
        try:
            resp = requests.get(url, params=p, timeout=10)
            db.increment_request_count(_API_NAME)
        except requests.RequestException as e:
            logger.warning("TMDB request failed: %s", e)
            return None

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            db.set_api_paused(
                _API_NAME, True,
                f"HTTP 429 received; retry after {retry_after}s"
            )
            raise RateLimitPausedError(
                f"TMDB returned 429. Paused for {retry_after}s. "
                "Run: python cli.py resume-api --api tmdb"
            )

        if resp.status_code != 200:
            logger.debug("TMDB %s → %d", endpoint, resp.status_code)
            return None

        return resp.json()

    # ─── Search ───────────────────────────────────────────────────────────────

    def search_movie(self, title: str, year: Optional[int] = None) -> list[dict]:
        """Search movies. Returns list of result dicts ordered by popularity."""
        params = {"query": title, "include_adult": "false"}
        if year:
            params["year"] = year
        data = self._get("search/movie", params)
        return data.get("results", []) if data else []

    def search_tv(self, title: str, year: Optional[int] = None) -> list[dict]:
        """Search TV shows."""
        params = {"query": title}
        if year:
            params["first_air_date_year"] = year
        data = self._get("search/tv", params)
        return data.get("results", []) if data else []

    def get_movie_details(self, tmdb_id: int) -> Optional[dict]:
        """Get full movie details including credits."""
        data = self._get(f"movie/{tmdb_id}", {"append_to_response": "credits"})
        if not data:
            return None
        return _parse_movie_details(data)

    def get_tv_details(self, tmdb_id: int) -> Optional[dict]:
        """Get full TV show details."""
        data = self._get(f"tv/{tmdb_id}", {"append_to_response": "credits"})
        if not data:
            return None
        return _parse_tv_details(data)

    def get_episode_details(self, tmdb_id: int, season: int,
                            episode: int) -> Optional[dict]:
        """Get episode details."""
        data = self._get(f"tv/{tmdb_id}/season/{season}/episode/{episode}")
        if not data:
            return None
        return {
            "episode_title": data.get("name"),
            "plot": data.get("overview"),
            "air_date": data.get("air_date"),
        }


# ─── Response parsing ─────────────────────────────────────────────────────────

def _parse_movie_details(data: dict) -> dict:
    genres = [g["name"] for g in data.get("genres", [])]
    credits = data.get("credits", {})
    directors = [
        p["name"] for p in credits.get("crew", [])
        if p.get("job") == "Director"
    ]
    cast = [a["name"] for a in credits.get("cast", [])[:5]]
    imdb_id = data.get("imdb_id") or ""
    return {
        "confirmed_title": data.get("title"),
        "confirmed_year": _year_from_date(data.get("release_date")),
        "confirmed_type": "movie",
        "imdb_id": imdb_id,
        "tmdb_id": data.get("id"),
        "genres": genres,
        "plot": data.get("overview"),
        "rating": _rating(data.get("vote_average")),
        "director": directors[0] if directors else None,
        "cast": cast,
    }


def _parse_tv_details(data: dict) -> dict:
    genres = [g["name"] for g in data.get("genres", [])]
    credits = data.get("credits", {})
    cast = [a["name"] for a in credits.get("cast", [])[:5]]
    imdb_id = data.get("external_ids", {}).get("imdb_id", "")
    return {
        "confirmed_title": data.get("name"),
        "confirmed_year": _year_from_date(data.get("first_air_date")),
        "confirmed_type": "tv",
        "imdb_id": imdb_id,
        "tmdb_id": data.get("id"),
        "genres": genres,
        "plot": data.get("overview"),
        "rating": _rating(data.get("vote_average")),
        "director": None,
        "cast": cast,
    }


def _year_from_date(date_str: Optional[str]) -> Optional[int]:
    if not date_str or len(date_str) < 4:
        return None
    try:
        return int(date_str[:4])
    except ValueError:
        return None


def _rating(value) -> Optional[str]:
    if value is None:
        return None
    try:
        return f"{float(value):.1f}"
    except (ValueError, TypeError):
        return None


# ─── Exceptions ───────────────────────────────────────────────────────────────

class RateLimitPausedError(Exception):
    """Raised when the API is paused due to rate limiting."""
