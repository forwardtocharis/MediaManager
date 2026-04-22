"""
embedder.py — Subtitle embedding via FFmpeg (local and SSH).

Strategy behaviour:
  sidecar  — always place a .srt sidecar, never embed
  embed    — embed into container; error if FFmpeg unavailable or fails
  hybrid   — try embed first; if it fails fall back to sidecar automatically
"""

import logging
import os
import posixpath
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Extensions that support embedded subtitle streams
_EMBED_OK = {".mp4", ".m4v", ".mkv", ".mov"}

# Subtitle codec per container
_SUB_CODEC = {
    ".mkv": "srt",
    ".mp4": "mov_text",
    ".m4v": "mov_text",
    ".mov": "mov_text",
}


# ─── Local FFmpeg ─────────────────────────────────────────────────────────────

def check_ffmpeg_local() -> Optional[str]:
    """Return path to local ffmpeg binary, or None."""
    return shutil.which("ffmpeg")


def apply_local(video_path: str, srt_path: str,
                language: str = "en",
                strategy: str = "hybrid") -> tuple[bool, str]:
    """
    Apply subtitle to a local video file.
    Returns (success, mode) where mode is 'embedded', 'sidecar', or 'error'.
    """
    ext = Path(video_path).suffix.lower()

    if strategy == "sidecar" or ext not in _EMBED_OK:
        ok = _sidecar_local(video_path, srt_path, language)
        return (ok, "sidecar") if ok else (False, "error")

    ffmpeg = check_ffmpeg_local()
    if not ffmpeg:
        if strategy == "embed":
            logger.warning("FFmpeg not found locally; cannot embed %s", video_path)
            return False, "error"
        ok = _sidecar_local(video_path, srt_path, language)
        return (ok, "sidecar") if ok else (False, "error")

    ok, err = _ffmpeg_local(ffmpeg, video_path, srt_path, language, ext)
    if ok:
        return True, "embedded"

    if strategy == "hybrid":
        logger.info("FFmpeg embed failed (%s), falling back to sidecar", err[:80])
        ok = _sidecar_local(video_path, srt_path, language)
        return (ok, "sidecar") if ok else (False, "error")

    return False, "error"


def _ffmpeg_local(ffmpeg: str, video: str, srt: str,
                  lang: str, ext: str) -> tuple[bool, str]:
    sub_codec = _SUB_CODEC.get(ext, "mov_text")
    tmp = video + ".sub_tmp" + ext
    cmd = [
        ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
        "-i", video,
        "-i", srt,
        "-c", "copy",
        "-c:s", sub_codec,
        f"-metadata:s:s:0", f"language={lang}",
        tmp,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode == 0 and os.path.exists(tmp):
            os.replace(tmp, video)
            return True, ""
        err = result.stderr
        if os.path.exists(tmp):
            os.unlink(tmp)
        return False, err
    except subprocess.TimeoutExpired:
        if os.path.exists(tmp):
            os.unlink(tmp)
        return False, "FFmpeg timed out"
    except Exception as e:
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass
        return False, str(e)


def _sidecar_local(video: str, srt: str, lang: str) -> bool:
    stem = Path(video).stem
    dst  = Path(video).parent / f"{stem}.{lang}.srt"
    try:
        shutil.copy2(srt, dst)
        return True
    except Exception as e:
        logger.error("Sidecar copy failed: %s", e)
        return False


# ─── SSH FFmpeg ───────────────────────────────────────────────────────────────

def apply_ssh(session, video_posix: str, srt_posix: str,
              language: str = "en",
              strategy: str = "hybrid") -> tuple[bool, str]:
    """
    Apply subtitle to a NAS video file via SSH FFmpeg.
    Returns (success, mode).
    """
    ext = posixpath.splitext(video_posix)[1].lower()

    if strategy == "sidecar" or ext not in _EMBED_OK:
        ok = _sidecar_ssh(session, video_posix, srt_posix, language)
        return (ok, "sidecar") if ok else (False, "error")

    ffmpeg = session.check_ffmpeg()
    if not ffmpeg:
        if strategy == "embed":
            logger.warning("FFmpeg not found on NAS; cannot embed %s", video_posix)
            return False, "error"
        ok = _sidecar_ssh(session, video_posix, srt_posix, language)
        return (ok, "sidecar") if ok else (False, "error")

    # Check disk space (need ~110% of video file size free)
    try:
        video_size = session.file_size(video_posix)
        free_bytes  = session.disk_free_bytes(posixpath.dirname(video_posix))
        if free_bytes > 0 and free_bytes < video_size * 1.1:
            logger.warning(
                "Insufficient NAS disk space for mux (need ~%d MB, have %d MB)",
                video_size // 1_000_000, free_bytes // 1_000_000,
            )
            if strategy == "hybrid":
                ok = _sidecar_ssh(session, video_posix, srt_posix, language)
                return (ok, "sidecar") if ok else (False, "error")
            return False, "error"
    except Exception:
        pass  # space check is best-effort

    ok, err = _ffmpeg_ssh(session, ffmpeg, video_posix, srt_posix, language, ext)
    if ok:
        return True, "embedded"

    if strategy == "hybrid":
        logger.info("SSH FFmpeg failed (%s), falling back to sidecar", err[:80])
        ok = _sidecar_ssh(session, video_posix, srt_posix, language)
        return (ok, "sidecar") if ok else (False, "error")

    return False, "error"


def _ffmpeg_ssh(session, ffmpeg: str, video: str, srt: str,
                lang: str, ext: str) -> tuple[bool, str]:
    sub_codec = _SUB_CODEC.get(ext, "mov_text")
    tmp = video + ".sub_tmp" + ext
    cmd = (
        f"{shlex.quote(ffmpeg)} -y -hide_banner -loglevel error "
        f"-i {shlex.quote(video)} "
        f"-i {shlex.quote(srt)} "
        f"-c copy -c:s {sub_codec} "
        f"-metadata:s:s:0 language={shlex.quote(lang)} "
        f"{shlex.quote(tmp)}"
    )
    stdout, stderr, code = session.run_command(cmd, timeout=3600)
    if code == 0 and session.exists(tmp):
        try:
            session.rename(tmp, video)
            return True, ""
        except Exception as e:
            session.delete(tmp)
            return False, str(e)
    session.delete(tmp)
    return False, stderr[:300]


def _sidecar_ssh(session, video_posix: str, srt_posix: str, lang: str) -> bool:
    stem = posixpath.splitext(video_posix)[0]
    dst  = f"{stem}.{lang}.srt"
    try:
        session.rename(srt_posix, dst)
        return True
    except Exception as e:
        logger.error("SSH sidecar placement failed: %s", e)
        return False
