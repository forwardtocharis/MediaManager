"""
omdb.py — OMDB REST API client with rate-limit awareness.

OMDB API documentation: https://www.omdbapi.com/
Free tier: 1,000 requests/day.
"""

import time
import logging
from typing import Optional

import requests

from src import db

logger = logging.getLogger(__name__)

_API_NAME = "omdb"
_BASE_URL = "https://www.omdbapi.com/"


class OMDBClient:
    def __init__(self, api_key: str, daily_limit: int = 950,
                 requests_per_second: float = 2):
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
                f"OMDB API is paused: {reason}. "
                "Run: python cli.py resume-api --api omdb, or click the OMDB dot in the UI header."
            )

        if self.daily_limit > 0:
            state = db.get_rate_limit(_API_NAME)
            if state and state["requests_today"] >= self.daily_limit:
                db.set_api_paused(
                    _API_NAME, True,
                    f"Daily limit of {self.daily_limit} requests reached"
                )
                raise RateLimitPausedError(
                    f"OMDB daily limit ({self.daily_limit}) reached. "
                    "Resume tomorrow or upgrade API plan. "
                    "Run: python cli.py resume-api --api omdb, or click the OMDB dot in the UI header."
                )

    def _throttle(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_time
        wait = self._interval - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_time = time.monotonic()

    def _get(self, params: dict) -> Optional[dict]:
        self._check_limits()
        self._throttle()
        p = {"apikey": self.api_key}
        p.update(params)
        try:
            resp = requests.get(_BASE_URL, params=p, timeout=10)
            db.increment_request_count(_API_NAME)
        except requests.RequestException as e:
            logger.warning("OMDB request failed: %s", e)
            return None

        if resp.status_code == 401:
            logger.error("OMDB API key invalid or daily limit exceeded.")
            db.set_api_paused(_API_NAME, True, "HTTP 401 — key invalid or daily limit")
            raise RateLimitPausedError(
                "OMDB returned 401. Daily limit may be exceeded or key is invalid."
            )

        if resp.status_code != 200:
            logger.debug("OMDB → %d", resp.status_code)
            return None

        data = resp.json()
        if data.get("Response") == "False":
            logger.debug("OMDB no result: %s", data.get("Error"))
            return None
        return data

    # ─── Search ───────────────────────────────────────────────────────────────

    def search_by_title(self, title: str, year: Optional[int] = None,
                        media_type: Optional[str] = None) -> Optional[dict]:
        """
        Search OMDB by title. media_type: 'movie' | 'series' | None
        Returns the first result dict or None.
        """
        params = {"t": title}
        if year:
            params["y"] = year
        if media_type in ("movie", "series"):
            params["type"] = media_type
        return self._get(params)

    def search_by_imdb_id(self, imdb_id: str) -> Optional[dict]:
        """Fetch full details by IMDb ID (e.g., 'tt0468569')."""
        return self._get({"i": imdb_id})

    def get_details(self, title: str, year: Optional[int] = None,
                    media_type: Optional[str] = None) -> Optional[dict]:
        """Fetch full OMDB record and return a normalized dict."""
        raw = self.search_by_title(title, year, media_type)
        if not raw:
            return None
        return _parse_omdb(raw)


# ─── Response parsing ─────────────────────────────────────────────────────────

def _parse_omdb(data: dict) -> dict:
    omdb_type = data.get("Type", "").lower()
    confirmed_type = "movie" if omdb_type == "movie" else (
        "tv" if omdb_type in ("series", "episode") else "unknown"
    )

    # Rating from Ratings array (prefer IMDb)
    rating = None
    for r in data.get("Ratings", []):
        if r.get("Source") == "Internet Movie Database":
            try:
                rating = r["Value"].split("/")[0]
            except Exception:
                pass
            break
    if not rating and data.get("imdbRating") not in (None, "N/A"):
        rating = data.get("imdbRating")

    genres = [g.strip() for g in data.get("Genre", "").split(",") if g.strip() and g.strip() != "N/A"]
    cast = [a.strip() for a in data.get("Actors", "").split(",") if a.strip() and a.strip() != "N/A"]
    director = data.get("Director", "").strip()
    if director == "N/A":
        director = None

    imdb_id = data.get("imdbID", "")
    if imdb_id == "N/A":
        imdb_id = ""

    year_raw = data.get("Year", "")
    year = None
    if year_raw and year_raw != "N/A":
        try:
            year = int(str(year_raw)[:4])
        except ValueError:
            pass

    return {
        "confirmed_title": data.get("Title"),
        "confirmed_year": year,
        "confirmed_type": confirmed_type,
        "imdb_id": imdb_id,
        "genres": genres,
        "plot": data.get("Plot") if data.get("Plot") != "N/A" else None,
        "rating": rating,
        "director": director,
        "cast": cast[:5],
    }


# ─── Exceptions ───────────────────────────────────────────────────────────────

class RateLimitPausedError(Exception):
    """Raised when OMDB is paused due to rate limiting."""
