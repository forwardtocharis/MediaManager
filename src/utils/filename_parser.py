"""
filename_parser.py — Wrapper around guessit with extras detection and type inference.
"""

import re
from pathlib import Path
from typing import Optional

try:
    from guessit import guessit
except ImportError:
    raise ImportError("guessit is required: pip install guessit")


# ─── Extras detection ─────────────────────────────────────────────────────────

_EXTRAS_PATTERNS = [
    r"\bfeaturette\b",
    r"\bbehind[_\s\-\.]+the[_\s\-\.]+scenes?\b",
    r"\bmaking[_\s\-\.]+of\b",
    r"\btrailer\b",
    r"\binterview\b",
    r"\bdeleted[_\s\-\.]+scenes?\b",
    r"\bbloopers?\b",
    r"\bgag[_\s\-\.]+reel\b",
    r"\bbonus\b",
    r"\bshort[_\s\-\.]+film\b",
    r"\btheatrical[_\s\-\.]+cut\b",
    r"\balternate[_\s\-\.]+ending\b",
    r"\bspecial[_\s\-\.]+feature\b",
]
_EXTRAS_RE = re.compile("|".join(_EXTRAS_PATTERNS), re.IGNORECASE)

# Common release-group junk to strip before guessit (helps with edge cases)
_JUNK_RE = re.compile(
    r"\b(yify|yts|bluray|blu-ray|bdrip|brrip|dvdrip|hdtv|webrip|web-dl|webdl|"
    r"hdrip|xvid|x264|x265|h264|h265|hevc|avc|10bit|6ch|aac|dd5\.?1|ac3|dts|"
    r"1080p|720p|480p|2160p|4k|uhd|hdr|sdr|extended|theatrical|remastered|"
    r"proper|repack|internal|dubbed|dual[_\s]audio)\b",
    re.IGNORECASE
)

# Subtitle language codes commonly embedded in filenames
_LANG_RE = re.compile(
    r"[\.\-_](en|eng|english|fr|fre|french|es|spa|spanish|de|ger|german|"
    r"it|ita|italian|pt|por|portuguese|ru|rus|russian|zh|chi|chinese|ja|jpn|"
    r"japanese|ko|kor|korean|ar|ara|arabic|nl|dut|dutch|sv|swe|swedish|"
    r"no|nor|norwegian|da|dan|danish|fi|fin|finnish|pl|pol|polish|"
    r"cs|cze|czech|hu|hun|hungarian|tr|tur|turkish|hi|hin|hindi)[\.\-_]?$",
    re.IGNORECASE
)


def is_extra(filepath: str | Path) -> bool:
    """Return True if the filename suggests this is bonus/extra content."""
    name = Path(filepath).stem
    return bool(_EXTRAS_RE.search(name))


def detect_subtitle_language(filepath: str | Path) -> Optional[str]:
    """Try to detect language code from subtitle filename."""
    stem = Path(filepath).stem
    m = _LANG_RE.search(stem)
    if m:
        code = m.group(1).lower()
        # Normalize to 2-letter codes
        _map = {
            "eng": "en", "english": "en",
            "fre": "fr", "french": "fr",
            "spa": "es", "spanish": "es",
            "ger": "de", "german": "de",
            "ita": "it", "italian": "it",
            "por": "pt", "portuguese": "pt",
            "rus": "ru", "russian": "ru",
            "chi": "zh", "chinese": "zh",
            "jpn": "ja", "japanese": "ja",
            "kor": "ko", "korean": "ko",
            "ara": "ar", "arabic": "ar",
            "dut": "nl", "dutch": "nl",
            "swe": "sv", "swedish": "sv",
            "nor": "no", "norwegian": "no",
            "dan": "da", "danish": "da",
            "fin": "fi", "finnish": "fi",
            "pol": "pl", "polish": "pl",
            "cze": "cs", "czech": "cs",
            "hun": "hu", "hungarian": "hu",
            "tur": "tr", "turkish": "tr",
            "hin": "hi", "hindi": "hi",
        }
        return _map.get(code, code)
    return None


def parse_filename(filepath: str | Path, use_parent_folder: bool = True) -> dict:
    """
    Parse a media filename using guessit. Returns a normalized dict with keys:
      title, year, media_type ('movie'|'tv'|'unknown'),
      season, episode, episode_title, extra (bool)
    """
    path = Path(filepath)
    filename = path.name

    # Use parent folder as additional context for guessit if available
    # (e.g., folder "Breaking Bad (2008)" can disambiguate bare episode files)
    guess_input = filename
    parent_name = path.parent.name if use_parent_folder else ""

    try:
        guess = dict(guessit(filename))
    except Exception:
        guess = {}

    # If guessit couldn't get a title, try the parent folder
    if not guess.get("title") and parent_name:
        try:
            parent_guess = dict(guessit(parent_name))
            if parent_guess.get("title"):
                guess.setdefault("title", parent_guess.get("title"))
                guess.setdefault("year", parent_guess.get("year"))
        except Exception:
            pass

    # Fallback: walk ancestor folders to fill gaps guessit left null.
    # Handles structures like: [Show Name]\Season 1\Episode 1 - Title.mkv
    _fill_from_path(path, guess)

    # Normalize media type
    raw_type = guess.get("type", "")
    if raw_type == "movie":
        media_type = "movie"
    elif raw_type == "episode":
        media_type = "tv"
    else:
        # Fall back: if we have season/episode data it's TV
        if guess.get("season") or guess.get("episode"):
            media_type = "tv"
        else:
            media_type = "unknown"

    # Normalize episode — guessit can return a list for multi-episode files
    episode = guess.get("episode")
    if isinstance(episode, list):
        episode = episode[0]  # Use first for now; multi-ep is low priority

    return {
        "title": _clean_title(str(guess.get("title", ""))),
        "year": _to_int(guess.get("year")),
        "media_type": media_type,
        "season": _to_int(guess.get("season")),
        "episode": _to_int(episode),
        "episode_title": _clean_title(str(guess.get("episode_title", ""))),
        "extra": is_extra(filepath),
        "raw_guess": guess,
    }


_SEASON_FOLDER_RE = re.compile(r"^season\s*(\d+)$", re.IGNORECASE)
_EPISODE_FILE_RE = re.compile(r"^episode\s*(\d+)", re.IGNORECASE)


def _fill_from_path(path: Path, guess: dict) -> None:
    """
    Fill missing season/episode/title in `guess` by inspecting ancestor folders.
    Only sets values that guessit left null — never overwrites existing ones.
    """
    parts = path.parts  # e.g. ['Show Name', 'Season 1', 'Episode 1 - Title.mkv']

    for i, part in enumerate(parts):
        # Season from a folder named "Season N" or "S0N"
        if guess.get("season") is None:
            m = _SEASON_FOLDER_RE.match(part)
            if m:
                guess["season"] = int(m.group(1))

        # Show title from any ancestor that isn't a season folder and isn't the filename
        if guess.get("title") is None and i < len(parts) - 1:
            if not _SEASON_FOLDER_RE.match(part):
                try:
                    folder_guess = dict(guessit(part))
                    if folder_guess.get("title"):
                        guess["title"] = folder_guess["title"]
                        guess.setdefault("year", folder_guess.get("year"))
                except Exception:
                    pass

    # Episode number from filename stem starting with "Episode N"
    if guess.get("episode") is None:
        m = _EPISODE_FILE_RE.match(path.stem)
        if m:
            guess["episode"] = int(m.group(1))


def _clean_title(title: str) -> str:
    """Strip extra whitespace and common junk from a title."""
    if not title:
        return ""
    # Remove junk tokens that might bleed through
    cleaned = _JUNK_RE.sub(" ", title).strip()
    # Collapse multiple spaces
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned


def _to_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
