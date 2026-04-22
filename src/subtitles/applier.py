"""
applier.py — Orchestrates subtitle download + placement during the Apply phase.

Called after a media file has been renamed/moved to its final destination.
Reads subtitle_queue entries (status='queued'), downloads each one, transfers
to the NAS if needed, then embeds or places as sidecar per strategy.
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

from src import db
from src.subtitles import fetcher, embedder

logger = logging.getLogger(__name__)


def apply_subtitle_queue(
    media_id: int,
    final_video_path: str,   # Windows/local path used in the DB
    subtitle_cfg: dict,
    session=None,             # SSHSession if SSH mode, None for local
    dry_run: bool = False,
    log_cb=None,
) -> dict:
    """
    Download and apply all queued subtitles for a media file.

    Returns a summary dict: {embedded: int, sidecar: int, skipped: int, errors: int}
    """
    stats = {"embedded": 0, "sidecar": 0, "skipped": 0, "errors": 0}

    if not subtitle_cfg.get("enabled"):
        return stats

    strategy = subtitle_cfg.get("storage_strategy", "hybrid")
    providers_cfg = subtitle_cfg.get("providers", {})

    # Pick the best queued subtitle per language
    best_per_lang = _best_queued_per_language(media_id)
    if not best_per_lang:
        return stats

    def log(msg: str, level: str = "info"):
        logger.info("[subtitles] %s", msg)
        if log_cb:
            log_cb(msg, level)

    for lang, queue_row in best_per_lang.items():
        queue_id = queue_row["id"]
        db.update_subtitle_queue(queue_id, status="downloading")

        if dry_run:
            log(f"[dry-run] would fetch {lang} subtitle via {queue_row['provider']}")
            db.update_subtitle_queue(queue_id, status="queued")
            stats["skipped"] += 1
            continue

        # 1. Download .srt to local temp file
        srt_bytes = fetcher.download_subtitle(queue_row, providers_cfg)
        if not srt_bytes:
            log(f"Download failed for {lang} ({queue_row['provider']})", "warning")
            db.update_subtitle_queue(queue_id, status="error",
                                      error_msg="download returned no content")
            stats["errors"] += 1
            continue

        with tempfile.NamedTemporaryFile(suffix=f".{lang}.srt",
                                         delete=False) as tmp:
            tmp.write(srt_bytes)
            local_srt = tmp.name

        try:
            if session is not None:
                ok, mode = _apply_ssh(session, final_video_path, local_srt,
                                       lang, strategy)
            else:
                ok, mode = _apply_local(final_video_path, local_srt, lang, strategy)

            if ok:
                db.update_subtitle_queue(queue_id, status=mode)
                log(f"{lang}: {mode} ✓  ({queue_row['provider']})")
                stats[mode] += 1
            else:
                db.update_subtitle_queue(queue_id, status="error",
                                          error_msg="embed/sidecar failed")
                log(f"{lang}: apply failed ({queue_row['provider']})", "warning")
                stats["errors"] += 1
        finally:
            try:
                os.unlink(local_srt)
            except OSError:
                pass

    return stats


# ─── Local apply ──────────────────────────────────────────────────────────────

def _apply_local(final_video_path: str, local_srt: str,
                 lang: str, strategy: str) -> tuple[bool, str]:
    ok, mode = embedder.apply_local(final_video_path, local_srt, lang, strategy)
    return ok, mode


# ─── SSH apply ────────────────────────────────────────────────────────────────

def _apply_ssh(session, final_video_win: str, local_srt: str,
               lang: str, strategy: str) -> tuple[bool, str]:
    import posixpath

    video_posix = session.to_posix(final_video_win)
    video_dir   = posixpath.dirname(video_posix)
    stem        = posixpath.splitext(posixpath.basename(video_posix))[0]
    remote_srt  = posixpath.join(video_dir, f"{stem}.{lang}.srt.tmp")

    # Transfer the .srt to the NAS
    try:
        session.upload_file(local_srt, remote_srt)
    except Exception as e:
        logger.error("SRT upload to NAS failed: %s", e)
        return False, "error"

    # Embed or sidecar on NAS
    ok, mode = embedder.apply_ssh(session, video_posix, remote_srt, lang, strategy)

    # If sidecar was placed, the rename already moved remote_srt to the final name.
    # If embed succeeded or failed, clean up any leftover temp .srt on NAS.
    if mode == "embedded" or not ok:
        session.delete(remote_srt)

    return ok, mode


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _best_queued_per_language(media_id: int) -> dict[str, object]:
    """Return {lang: queue_row} for the best-scored queued entry per language."""
    rows = db.get_subtitle_queue_for_media(media_id, status="queued")
    best: dict[str, object] = {}
    for row in rows:
        lang = row["language"]
        if lang not in best or row["score"] > best[lang]["score"]:
            best[lang] = row
    return best
