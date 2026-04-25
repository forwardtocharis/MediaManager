"""
file_utils.py — Safe file operations, path sanitization, subtitle matching.
"""

import hashlib
import os
import re
import shutil
from pathlib import Path
from typing import Optional

# ─── Filename sanitization ────────────────────────────────────────────────────

# Characters illegal on Windows/SMB shares
_ILLEGAL_RE = re.compile(r'[\\/*?"<>|]')
# Colon gets special treatment — replace with dash
_COLON_RE = re.compile(r'\s*:\s*')
# Trailing dots/spaces are also illegal on Windows
_TRAILING_RE = re.compile(r'[\s.]+$')


def sanitize_filename(name: str) -> str:
    """
    Make a string safe for use as a Windows/SMB filename component.
    - Colons become ' -'
    - Other illegal chars are removed
    - Trailing dots/spaces stripped
    """
    name = _COLON_RE.sub(' - ', name)
    name = _ILLEGAL_RE.sub('', name)
    name = _TRAILING_RE.sub('', name)
    # Collapse multiple spaces
    name = re.sub(r' {2,}', ' ', name).strip()
    return name


def sanitize_path_component(name: str, max_length: int = 200) -> str:
    """Sanitize and truncate a path component."""
    result = sanitize_filename(name)
    return result[:max_length]


# ─── Path building ────────────────────────────────────────────────────────────

def decade_folder(year: int) -> str:
    """Return decade string like '1980s', '2000s'."""
    decade = (year // 10) * 10
    return f"{decade}s"


def build_movie_path(base_dir: str | Path, title: str, year: int,
                     extension: str, use_decade_folders: bool = True) -> Path:
    """
    Build the proposed output path for a movie file.
    Structure: <base_dir>/[decade/]<Title (Year)>/<Title (Year)>.<ext>
    """
    base = Path(base_dir)
    # Use a slightly shorter limit for the title to leave room for " (Year)"
    safe_title = sanitize_path_component(title, max_length=190)
    folder_name = sanitize_path_component(f"{safe_title} ({year})")
    filename = f"{folder_name}{extension}"

    if use_decade_folders:
        base = base / decade_folder(year)

    return base / folder_name / filename


def build_movie_extra_path(base_dir: str | Path, title: str, year: int,
                           extra_name: str, extension: str) -> Path:
    """
    Build path for bonus/extra content.
    Structure: <base_dir>/<decade>/<Title (Year)>/<Title (Year)>.<Extra Name>.<ext>
    """
    base = Path(base_dir)
    safe_title = sanitize_path_component(title)
    folder_name = sanitize_path_component(f"{safe_title} ({year})")
    safe_extra = sanitize_path_component(extra_name)
    filename = f"{folder_name}.{safe_extra}{extension}"
    return base / decade_folder(year) / folder_name / filename


def build_tv_episode_path(base_dir: str | Path, show_title: str, season: int,
                          episode: int, episode_title: str,
                          extension: str) -> Path:
    """
    Build the proposed output path for a TV episode.
    Structure: <base_dir>/<Show>/<Season ##>/<Show> - S##E## - <Episode Title>.<ext>
    """
    base = Path(base_dir)
    safe_show = sanitize_path_component(show_title)
    season_folder = f"Season {season:02d}"
    ep_part = f"S{season:02d}E{episode:02d}"
    if episode_title:
        safe_ep_title = sanitize_path_component(episode_title)
        file_stem = f"{safe_show} - {ep_part} - {safe_ep_title}"
    else:
        file_stem = f"{safe_show} - {ep_part}"
    filename = f"{file_stem}{extension}"
    return base / safe_show / season_folder / filename


def build_subtitle_path(video_path: Path, language: Optional[str],
                        subtitle_ext: str) -> Path:
    """
    Build a subtitle path co-located with the video file.
    e.g., if video is 'The Thing (1982).mkv', subtitle becomes:
      'The Thing (1982).en.srt'  (if language known)
      'The Thing (1982).srt'     (if language unknown)
    """
    stem = video_path.stem
    if language:
        return video_path.parent / f"{stem}.{language}{subtitle_ext}"
    return video_path.parent / f"{stem}{subtitle_ext}"


def build_nfo_path(video_path: Path) -> Path:
    """Return the NFO sidecar path next to a video file."""
    return video_path.with_suffix('.nfo')


def build_tvshow_nfo_path(base_dir: str | Path, show_title: str) -> Path:
    """Return path for the tvshow.nfo at the show root."""
    safe_show = sanitize_path_component(show_title)
    return Path(base_dir) / safe_show / "tvshow.nfo"


# ─── Safe file operations ─────────────────────────────────────────────────────

def file_hash(path: Path, chunk_size: int = 1 << 20) -> str:
    """Compute SHA-256 of a file. Uses 1 MB chunks for large files."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_copy(src: Path, dst: Path, verify: bool = True) -> bool:
    """
    Copy src → dst, creating parent directories as needed.
    If verify=True, confirms dst size matches src size before returning True.
    Does NOT delete src — caller is responsible for deletion after verification.
    Returns True on success.

    Size verification (not SHA-256) is intentional: shutil.copy2 is reliable,
    and hashing multi-GB media files doubles the I/O time. A size match is
    sufficient to catch truncated writes caused by disk-full errors, which are
    the only realistic failure mode here.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)

    if verify:
        src_size = src.stat().st_size
        dst_size = dst.stat().st_size
        if src_size != dst_size:
            # Sizes differ — remove the bad copy
            dst.unlink(missing_ok=True)
            return False
    return True


def safe_delete(path: Path) -> bool:
    """Delete a file. Returns True if deleted, False if it didn't exist."""
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def ensure_unique_path(path: Path) -> Path:
    """
    If path already exists, append a counter suffix to avoid collisions.
    e.g., 'Movie (2000).mkv' → 'Movie (2000).2.mkv'
    """
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    counter = 2
    while True:
        candidate = parent / f"{stem}.{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


# ─── Media / subtitle file detection ─────────────────────────────────────────

def is_media_file(path: Path, media_extensions: set[str]) -> bool:
    return path.suffix.lower() in media_extensions


def is_subtitle_file(path: Path, subtitle_extensions: set[str]) -> bool:
    return path.suffix.lower() in subtitle_extensions


def match_subtitles_to_media(
    subtitle_paths: list[Path],
    media_path: Path,
) -> list[Path]:
    """
    Return subtitle paths whose stem is a prefix of (or matches) the media stem.
    e.g., 'Movie.en.srt' matches 'Movie.mkv'
    """
    media_stem = media_path.stem.lower()
    matches = []
    for sub in subtitle_paths:
        # Strip language suffix from subtitle stem for comparison
        sub_stem = sub.stem.lower()
        # Remove trailing .en, .eng etc.
        bare_stem = re.sub(
            r'\.(en|eng|fr|fre|es|spa|de|ger|it|ita|pt|por|ru|rus|'
            r'zh|chi|ja|jpn|ko|kor|ar|ara|nl|dut|sv|swe|no|nor|'
            r'da|dan|fi|fin|pl|pol|cs|cze|hu|hun|tr|tur|hi|hin)$',
            '', sub_stem, flags=re.IGNORECASE
        )
        if bare_stem == media_stem or sub_stem == media_stem:
            matches.append(sub)
    return matches
