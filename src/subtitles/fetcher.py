"""
fetcher.py — Subtitle search and download.

Search phase  (called during identification):
  Queries enabled providers and inserts candidates into subtitle_queue.
  No subtitle content is downloaded at this point.

Download phase (called during apply):
  Reads queued rows, downloads .srt bytes from each provider, writes to disk.
"""

import io
import logging
import zipfile
from dataclasses import dataclass, field
from typing import Optional

import requests

from src import db

logger = logging.getLogger(__name__)

SUBDL_SEARCH_URL = "https://api.subdl.com/api/v1/subtitles"
SUBDL_DOWNLOAD_BASE = "https://dl.subdl.com"
OS_COM_BASE = "https://api.opensubtitles.com/api/v1"

# Headers used for every OpenSubtitles.com call
_OS_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "MediaManager/1.0",
}


# ─── Data class ──────────────────────────────────────────────────────────────

@dataclass
class SubtitleResult:
    provider: str       # 'opensubtitles' | 'subdl' | 'podnapisi'
    subtitle_id: str    # provider-specific ID
    language: str       # ISO 639-1
    release_name: str   = ""
    score: float        = 0.0
    download_url: str   = ""


# ─── Search phase ─────────────────────────────────────────────────────────────

def queue_subtitles_for_media(row: dict, subtitle_cfg: dict,
                               os_hash: Optional[str] = None) -> int:
    """
    Search all enabled providers for a media row and insert results into
    subtitle_queue. Skips languages already present in the queue.
    Returns the number of entries inserted.
    """
    if not subtitle_cfg.get("enabled"):
        return 0

    media_id = row["id"] if "id" in row.keys() else row.get("id")
    existing_langs = db.get_queued_languages(media_id)

    # Also skip languages covered by already-linked sidecar subtitle_files
    existing_subs = db.get_subtitles_for_media(media_id)
    for sub in existing_subs:
        if sub["language"]:
            existing_langs.add(sub["language"])

    requested = subtitle_cfg.get("languages", ["en"])
    needed = [l for l in requested if l not in existing_langs]
    if not needed:
        return 0

    providers_cfg = subtitle_cfg.get("providers", {})
    inserted = 0

    for lang in needed:
        candidates: list[SubtitleResult] = []

        if providers_cfg.get("opensubtitles", {}).get("enabled"):
            try:
                candidates += _search_opensubtitles(row, lang,
                                                    providers_cfg["opensubtitles"],
                                                    os_hash)
            except Exception as e:
                logger.warning("OpenSubtitles search failed (%s/%s): %s",
                               row.get("confirmed_title", "?"), lang, e)

        if providers_cfg.get("subdl", {}).get("enabled"):
            try:
                candidates += _search_subdl(row, lang, providers_cfg["subdl"])
            except Exception as e:
                logger.warning("Subdl search failed (%s/%s): %s",
                               row.get("confirmed_title", "?"), lang, e)

        if providers_cfg.get("podnapisi", {}).get("enabled"):
            try:
                candidates += _search_podnapisi(row, lang)
            except Exception as e:
                logger.warning("Podnapisi search failed (%s/%s): %s",
                               row.get("confirmed_title", "?"), lang, e)

        # Insert best candidate per provider per language
        seen_providers: set[str] = set()
        for c in sorted(candidates, key=lambda x: x.score, reverse=True):
            if c.provider in seen_providers:
                continue
            seen_providers.add(c.provider)
            db.insert_subtitle_queue({
                "media_id":     media_id,
                "provider":     c.provider,
                "subtitle_id":  c.subtitle_id,
                "language":     c.language,
                "release_name": c.release_name,
                "score":        c.score,
                "download_url": c.download_url,
                "status":       "queued",
            })
            inserted += 1

    return inserted


# ─── Download phase ───────────────────────────────────────────────────────────

def download_subtitle(queue_row, providers_cfg: dict) -> Optional[bytes]:
    """
    Download a subtitle from the queue entry.
    Returns raw .srt bytes, or None on failure.
    Automatically extracts from ZIP archives where needed.
    """
    provider = queue_row["provider"]
    try:
        if provider == "opensubtitles":
            raw = _download_opensubtitles(queue_row, providers_cfg.get("opensubtitles", {}))
        elif provider == "subdl":
            raw = _download_subdl(queue_row)
        elif provider == "podnapisi":
            raw = _download_podnapisi(queue_row)
        else:
            logger.warning("Unknown provider: %s", provider)
            return None

        if raw and _is_zip(raw):
            raw = _extract_srt_from_zip(raw)
        return raw
    except Exception as e:
        logger.error("Download failed (%s / %s): %s",
                     provider, queue_row.get("subtitle_id"), e)
        return None


# ─── OpenSubtitles.com search ─────────────────────────────────────────────────

def _search_opensubtitles(row, lang: str, os_cfg: dict,
                           os_hash: Optional[str]) -> list[SubtitleResult]:
    username = os_cfg.get("username", "").strip()
    password = os_cfg.get("password", "").strip()
    api_key  = os_cfg.get("api_key", "").strip()
    if not username or not password:
        return []

    # Try subliminal provider first (uses proper scoring)
    results = _search_opensubtitles_subliminal(row, lang, username, password, os_hash)
    if results:
        return results

    # Fallback: direct REST API call
    return _search_opensubtitles_rest(row, lang, username, password, api_key, os_hash)


def _search_opensubtitles_subliminal(row, lang: str, username: str,
                                      password: str,
                                      os_hash: Optional[str]) -> list[SubtitleResult]:
    try:
        from subliminal.providers.opensubtitlescom import OpenSubtitlesCom
        from babelfish import Language
    except ImportError:
        return []

    video = _make_video(row, os_hash)
    if video is None:
        return []

    try:
        bl = Language.fromietf(lang)
    except Exception:
        try:
            bl = Language(lang)
        except Exception:
            return []

    results: list[SubtitleResult] = []
    try:
        with OpenSubtitlesCom(username=username, password=password) as provider:
            subs = provider.list_subtitles(video, {bl})
            for i, sub in enumerate(subs[:5]):
                file_id = getattr(sub, "file_id", None) or getattr(sub, "id", None)
                if not file_id:
                    continue
                release = (getattr(sub, "movie_release_name", "") or
                           getattr(sub, "release", "") or "")
                results.append(SubtitleResult(
                    provider="opensubtitles",
                    subtitle_id=str(file_id),
                    language=lang,
                    release_name=str(release),
                    score=100.0 - i * 5,
                    download_url="",
                ))
    except Exception as e:
        logger.debug("subliminal OpenSubtitles search error: %s", e)
    return results


def _search_opensubtitles_rest(row, lang: str, username: str, password: str,
                                api_key: str,
                                os_hash: Optional[str]) -> list[SubtitleResult]:
    """Direct REST fallback for when subliminal isn't available."""
    headers = {**_OS_HEADERS, "Api-Key": api_key or "MediaManager"}
    try:
        r = requests.post(f"{OS_COM_BASE}/login",
                          json={"username": username, "password": password},
                          headers=headers, timeout=10)
        r.raise_for_status()
        token = r.json().get("token")
        if not token:
            return []
        headers["Authorization"] = f"Bearer {token}"
    except Exception as e:
        logger.debug("OpenSubtitles login failed: %s", e)
        return []

    params: dict = {"languages": lang, "type": _row_type(row)}
    if os_hash:
        params["moviehash"] = os_hash
    elif row.get("imdb_id"):
        params["imdb_id"] = row["imdb_id"].lstrip("tt")
    elif row.get("tmdb_id"):
        params["tmdb_id"] = str(row["tmdb_id"])
    else:
        params["query"] = row.get("confirmed_title") or row.get("guessed_title", "")

    if _row_type(row) == "episode":
        if row.get("season"):
            params["season_number"] = row["season"]
        if row.get("episode"):
            params["episode_number"] = row["episode"]

    try:
        r = requests.get(f"{OS_COM_BASE}/subtitles",
                         params=params, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.debug("OpenSubtitles search REST failed: %s", e)
        return []

    results = []
    for i, item in enumerate((data.get("data") or [])[:5]):
        attrs = item.get("attributes", {})
        files = attrs.get("files", [{}])
        file_id = files[0].get("file_id") if files else None
        if not file_id:
            continue
        results.append(SubtitleResult(
            provider="opensubtitles",
            subtitle_id=str(file_id),
            language=lang,
            release_name=attrs.get("release", ""),
            score=100.0 - i * 5,
            download_url="",
        ))
    return results


# ─── OpenSubtitles.com download ───────────────────────────────────────────────

def _download_opensubtitles(queue_row, os_cfg: dict) -> Optional[bytes]:
    username = os_cfg.get("username", "").strip()
    password = os_cfg.get("password", "").strip()
    api_key  = os_cfg.get("api_key", "").strip()
    file_id  = queue_row["subtitle_id"]
    if not username or not password:
        return None

    headers = {**_OS_HEADERS, "Api-Key": api_key or "MediaManager"}
    try:
        r = requests.post(f"{OS_COM_BASE}/login",
                          json={"username": username, "password": password},
                          headers=headers, timeout=10)
        r.raise_for_status()
        token = r.json().get("token")
        if not token:
            return None
        headers["Authorization"] = f"Bearer {token}"

        r = requests.post(f"{OS_COM_BASE}/download",
                          json={"file_id": int(file_id)},
                          headers=headers, timeout=10)
        r.raise_for_status()
        link = r.json().get("link")
        if not link:
            return None

        r = requests.get(link, timeout=60)
        r.raise_for_status()
        return r.content
    except Exception as e:
        logger.error("OpenSubtitles download error (file_id=%s): %s", file_id, e)
        return None


# ─── Subdl search ─────────────────────────────────────────────────────────────

def _search_subdl(row, lang: str, subdl_cfg: dict) -> list[SubtitleResult]:
    api_key = subdl_cfg.get("api_key", "").strip()
    if not api_key:
        return []

    params: dict = {
        "api_key":      api_key,
        "languages":    lang.upper(),
        "subs_per_page": 5,
        "type":          _row_type_subdl(row),
    }

    if row.get("imdb_id"):
        params["imdb_id"] = row["imdb_id"]
    elif row.get("tmdb_id"):
        params["tmdb_id"] = str(row["tmdb_id"])
    else:
        title = row.get("confirmed_title") or row.get("guessed_title", "")
        if not title:
            return []
        params["film_name"] = title
        year = row.get("confirmed_year") or row.get("guessed_year")
        if year:
            params["year"] = str(year)

    if params["type"] == "tv":
        season  = row.get("season") or row.get("guessed_season")
        episode = row.get("episode") or row.get("guessed_episode")
        if season:
            params["season_number"] = str(season)
        if episode:
            params["episode_number"] = str(episode)

    try:
        r = requests.get(SUBDL_SEARCH_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.debug("Subdl search error: %s", e)
        return []

    if not data.get("status"):
        return []

    items = data.get("subtitles") or data.get("results") or []
    results = []
    for i, item in enumerate(items[:5]):
        sd_id = str(item.get("sd_id", ""))
        url   = item.get("url") or item.get("full_link") or ""
        if url and not url.startswith("http"):
            url = SUBDL_DOWNLOAD_BASE + url
        results.append(SubtitleResult(
            provider="subdl",
            subtitle_id=sd_id,
            language=lang,
            release_name=item.get("release_name", ""),
            score=90.0 - i * 5,
            download_url=url,
        ))
    return results


def _download_subdl(queue_row) -> Optional[bytes]:
    url = queue_row.get("download_url") or ""
    if not url:
        return None
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return r.content
    except Exception as e:
        logger.error("Subdl download error: %s", e)
        return None


# ─── Podnapisi search ─────────────────────────────────────────────────────────

def _search_podnapisi(row, lang: str) -> list[SubtitleResult]:
    try:
        from subliminal.providers.podnapisi import PodnapisiProvider
        from babelfish import Language
    except ImportError:
        return []

    video = _make_video(row)
    if video is None:
        return []

    try:
        bl = Language.fromietf(lang)
    except Exception:
        try:
            bl = Language(lang)
        except Exception:
            return []

    results: list[SubtitleResult] = []
    try:
        with PodnapisiProvider() as provider:
            subs = provider.list_subtitles(video, {bl})
            for i, sub in enumerate(subs[:5]):
                sub_id = str(getattr(sub, "subtitle_id", "") or getattr(sub, "pid", ""))
                if not sub_id:
                    continue
                results.append(SubtitleResult(
                    provider="podnapisi",
                    subtitle_id=sub_id,
                    language=lang,
                    release_name=getattr(sub, "release", "") or "",
                    score=85.0 - i * 5,
                    download_url="",
                ))
    except Exception as e:
        logger.debug("Podnapisi search error: %s", e)
    return results


def _download_podnapisi(queue_row) -> Optional[bytes]:
    subtitle_id = queue_row["subtitle_id"]
    url = f"https://www.podnapisi.net/subtitles/{subtitle_id}/download"
    try:
        r = requests.get(url, timeout=60,
                         headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        return r.content
    except Exception as e:
        logger.error("Podnapisi download error (id=%s): %s", subtitle_id, e)
        return None


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _row_type(row) -> str:
    t = row.get("confirmed_type") or row.get("guessed_type", "movie")
    return "episode" if t == "tv" else "movie"


def _row_type_subdl(row) -> str:
    t = row.get("confirmed_type") or row.get("guessed_type", "movie")
    return "tv" if t == "tv" else "movie"


def _make_video(row, os_hash: Optional[str] = None):
    """Build a subliminal Video object from a media row (no local file)."""
    try:
        from subliminal import Movie, Episode
    except ImportError:
        return None

    hashes = {}
    if os_hash:
        hashes["opensubtitles"] = os_hash

    title  = row.get("confirmed_title") or row.get("guessed_title") or ""
    year   = row.get("confirmed_year") or row.get("guessed_year")
    imdb   = row.get("imdb_id") or None

    ctype = row.get("confirmed_type") or row.get("guessed_type", "movie")
    if ctype == "tv":
        season  = row.get("season") or row.get("guessed_season") or 1
        episode = row.get("episode") or row.get("guessed_episode") or 1
        ep_title = row.get("episode_title") or None
        return Episode(
            title, season, episode,
            title=ep_title,
            series_imdb_id=imdb,
            hashes=hashes or None,
        )
    else:
        return Movie(
            title,
            year=year,
            imdb_id=imdb,
            hashes=hashes or None,
        )


def _is_zip(data: bytes) -> bool:
    return data[:2] == b"PK"


def _extract_srt_from_zip(data: bytes) -> Optional[bytes]:
    """Extract the first .srt (or largest text file) from a ZIP archive."""
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
        names = z.namelist()
        srt_names = [n for n in names if n.lower().endswith(".srt")]
        if srt_names:
            return z.read(srt_names[0])
        # Fall back to first non-directory entry
        file_names = [n for n in names if not n.endswith("/")]
        if file_names:
            return z.read(file_names[0])
    except Exception as e:
        logger.debug("ZIP extraction failed: %s", e)
    return None
