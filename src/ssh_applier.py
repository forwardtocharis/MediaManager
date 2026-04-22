"""
ssh_applier.py — SSH-based apply: runs rename/move/NFO operations directly on the NAS.

The PC sends instructions; the NAS executes them locally. Files never traverse
the network as data — only the commands do.

Requires: paramiko
"""

import logging
import posixpath
from pathlib import PureWindowsPath

logger = logging.getLogger(__name__)


# ─── SSH session wrapper ───────────────────────────────────────────────────────

class SSHSession:
    """
    Thin wrapper around a paramiko SSH + SFTP session.
    Holds one connection for the lifetime of an apply run.
    """

    def __init__(self, host: str, port: int, username: str,
                 password: str | None = None, key_path: str | None = None,
                 nas_path_map: dict | None = None):
        """
        nas_path_map: optional dict that maps a Windows UNC/drive prefix to its
        POSIX path on the NAS.
        e.g. {"\\\\diskstation\\chat": "/volume1/chat"}
        """
        import paramiko
        import os

        self._path_map: dict[str, str] = nas_path_map or {}

        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        known_hosts = os.path.expanduser("~/.ssh/known_hosts")
        if os.path.exists(known_hosts):
            try:
                self._client.load_host_keys(known_hosts)
            except Exception as e:
                logger.warning("SSH: could not load known_hosts: %s", e)

        connect_kwargs: dict = dict(hostname=host, port=port, username=username,
                                    timeout=15, look_for_keys=True, allow_agent=True)
        if key_path:
            connect_kwargs["key_filename"] = key_path
        if password:
            connect_kwargs["password"] = password

        self._client.connect(**connect_kwargs)
        self._sftp = self._client.open_sftp()

    # ── Path translation ──────────────────────────────────────────────────────

    def to_posix(self, win_path: str) -> str:
        """
        Convert a Windows path (UNC or drive letter) to its POSIX equivalent
        on the NAS, using the configured path map.

        e.g. '\\\\diskstation\\chat\\Movies\\foo.mkv'
             → '/volume1/chat/Movies/foo.mkv'
        """
        # Collapse all backslashes to forward slashes, then deduplicate leading slashes
        # so both \\server\share and \\\\server\\share normalise to //server/share.
        normalized = win_path.replace("\\", "/")
        logger.debug("to_posix input: %r  normalized: %r", win_path, normalized)

        for win_prefix, posix_prefix in self._path_map.items():
            win_norm = win_prefix.replace("\\", "/")
            norm_stripped = normalized.lstrip("/")
            prefix_stripped = win_norm.lstrip("/")
            logger.debug("  trying prefix %r (stripped: %r) match=%s",
                         win_prefix, prefix_stripped,
                         norm_stripped.lower().startswith(prefix_stripped.lower()))
            if norm_stripped.lower().startswith(prefix_stripped.lower()):
                relative = norm_stripped[len(prefix_stripped):].lstrip("/")
                result = posixpath.join(posix_prefix.rstrip("/"), relative)
                logger.debug("  -> mapped to %r", result)
                return result

        logger.debug("  -> no mapping, returning normalized: %r", normalized)
        return normalized

    # ── Remote operations ─────────────────────────────────────────────────────

    def exists(self, posix_path: str) -> bool:
        try:
            self._sftp.stat(posix_path)
            logger.debug("SSH exists: %r -> True", posix_path)
            return True
        except Exception as e:
            parent = posixpath.dirname(posix_path)
            try:
                entries = self._sftp.listdir(parent)
                logger.debug("SSH exists: %r -> False (%s: %s); parent %r contains: %s",
                             posix_path, type(e).__name__, e, parent, entries)
            except Exception as list_err:
                logger.debug("SSH exists: %r -> False (%s: %s); parent %r not listable: %s",
                             posix_path, type(e).__name__, e, parent, list_err)
            return False

    def file_size(self, posix_path: str) -> int:
        return self._sftp.stat(posix_path).st_size

    def makedirs(self, posix_path: str) -> None:
        """Recursively create directories on the remote (mkdir -p equivalent)."""
        parts = posix_path.split("/")
        current = ""
        for part in parts:
            if not part:
                current = "/"
                continue
            current = posixpath.join(current, part)
            try:
                self._sftp.stat(current)
            except FileNotFoundError:
                self._sftp.mkdir(current)

    def rename(self, src: str, dst: str) -> None:
        """Atomic rename on the NAS (same-filesystem move)."""
        self.makedirs(posixpath.dirname(dst))
        self._sftp.rename(src, dst)

    def write_text(self, posix_path: str, content: str) -> None:
        """Write a UTF-8 text file on the NAS."""
        self.makedirs(posixpath.dirname(posix_path))
        with self._sftp.open(posix_path, "w") as f:
            f.write(content)

    def delete(self, posix_path: str) -> bool:
        try:
            self._sftp.remove(posix_path)
            return True
        except FileNotFoundError:
            return False

    def ensure_unique(self, posix_path: str) -> str:
        """If path exists, append a counter suffix."""
        if not self.exists(posix_path):
            return posix_path
        base, ext = posixpath.splitext(posix_path)
        counter = 2
        while True:
            candidate = f"{base}.{counter}{ext}"
            if not self.exists(candidate):
                return candidate
            counter += 1

    def close(self) -> None:
        try:
            self._sftp.close()
            self._client.close()
        except Exception:
            pass

    def test(self) -> str:
        """Verify connectivity by listing the root via SFTP and return the server banner."""
        transport = self._client.get_transport()
        banner = transport.remote_version if transport else "unknown"
        self._sftp.stat("/")
        return banner

    def diagnose_path_map(self, share_name: str) -> list[str]:
        """
        Walk the NAS filesystem to find where a named share lives.
        Returns a list of diagnostic lines suitable for display in the UI.
        """
        lines = []

        def _listdir(path):
            try:
                return self._sftp.listdir(path)
            except Exception as e:
                return [f"<error: {e}>"]

        # List filesystem roots
        root_entries = _listdir("/")
        lines.append(f"/ contains: {root_entries}")

        # Check common Synology volume roots
        for vol in root_entries:
            vol_path = f"/{vol}"
            try:
                self._sftp.stat(vol_path)
                entries = _listdir(vol_path)
                lines.append(f"{vol_path}/ contains: {entries}")
                # If this volume contains the share, dig one level deeper
                for entry in entries:
                    if entry.lower() == share_name.lower():
                        share_path = f"{vol_path}/{entry}"
                        sub = _listdir(share_path)
                        lines.append(f"  ** FOUND {share_path}/ contains: {sub[:10]}")
            except Exception:
                pass

        return lines


# ─── Connection factory ────────────────────────────────────────────────────────

def session_from_config(ssh_cfg: dict) -> SSHSession:
    """Build an SSHSession from the 'ssh' block in config.yml."""
    host     = (ssh_cfg.get("host") or "").strip()
    port     = int(ssh_cfg.get("port") or 22)
    username = (ssh_cfg.get("username") or "").strip()
    password = (ssh_cfg.get("password") or "").strip() or None
    key_path = (ssh_cfg.get("key_path") or "").strip() or None

    raw_map: dict = ssh_cfg.get("path_map") or {}
    return SSHSession(host=host, port=port, username=username,
                      password=password, key_path=key_path,
                      nas_path_map=raw_map)


# ─── SSH apply_one ─────────────────────────────────────────────────────────────

def apply_one_ssh(row, movies_output: str, tv_output: str,
                  dry_run: bool, written_show_nfos: set,
                  session: SSHSession, debug_cb=None) -> bool:
    """
    SSH equivalent of applier._apply_one.
    All file ops run on the NAS; only NFO content is built locally and pushed.
    Returns True if the file was processed, or a reason string if skipped.
    """
    from src import db
    from src.utils.file_utils import (
        build_movie_path, build_movie_extra_path,
        build_tv_episode_path, build_subtitle_path, build_nfo_path,
        build_tvshow_nfo_path,
    )
    from src.utils.nfo_writer import write_movie_nfo, write_tvshow_nfo, write_episode_nfo
    import io, tempfile, os

    def dbg(msg):
        if debug_cb:
            debug_cb(msg)

    media_id   = row["id"]
    src_win    = row["original_path"]
    media_type = row["confirmed_type"]
    title      = row["confirmed_title"]
    year       = row["confirmed_year"]
    ext        = row["extension"]
    is_extra   = bool(row["is_extra"])

    src_posix = session.to_posix(src_win)
    dbg(f"src_win={src_win!r}  →  src_posix={src_posix!r}")
    dbg(f"type={media_type!r}  title={title!r}  year={year!r}  ext={ext!r}  is_extra={is_extra}")

    if not session.exists(src_posix):
        parent = posixpath.dirname(src_posix)
        try:
            entries = session._sftp.listdir(parent)
            dbg(f"DEBUG listdir {parent!r}: {entries}")
        except Exception as le:
            dbg(f"DEBUG listdir {parent!r}: failed — {le}")
        reason = f"SSH: source file not found on NAS at {src_posix!r}"
        logger.warning("SSH: source not found: %s", src_posix)
        db.update_media_file(media_id, status="skipped", notes=reason)
        dbg(f"SKIP — {reason}")
        return reason

    dbg(f"source exists ✓")

    # ── Build destination (Windows path, then convert) ──
    if media_type == "movie":
        if is_extra:
            from src.applier import _extract_extra_name
            extra_name = _extract_extra_name(
                PureWindowsPath(src_win).stem, title, year)
            dst_win = str(build_movie_extra_path(movies_output, title, year, extra_name, ext))
        else:
            dst_win = str(build_movie_path(movies_output, title, year, ext))
    elif media_type == "tv":
        season   = row["season"] or row["guessed_season"] or 1
        episode  = row["episode"] or row["guessed_episode"] or 1
        ep_title = row["episode_title"] or ""
        dbg(f"TV: season={season}  episode={episode}  ep_title={ep_title!r}  tv_output={tv_output!r}")
        dst_win  = str(build_tv_episode_path(tv_output, title, season, episode, ep_title, ext))
    else:
        reason = f"SSH: unknown/unconfirmed type '{media_type}'"
        logger.warning("SSH: unknown type for %s — skipping", src_win)
        dbg(f"SKIP — {reason}")
        return reason

    dst_posix = session.ensure_unique(session.to_posix(dst_win))
    dbg(f"dst_win={dst_win!r}  →  dst_posix={dst_posix!r}")

    db.update_media_file(media_id, proposed_path=dst_win)

    if dry_run:
        dbg(f"dry_run=True — would rename {src_posix!r} → {dst_posix!r}")
        return True

    # ── Rename/move on NAS ──
    manifest_id = db.log_manifest_op(media_id, "media", src_win, dst_win, "copy")
    try:
        session.rename(src_posix, dst_posix)
    except Exception as e:
        logger.error("SSH rename failed %s → %s: %s", src_posix, dst_posix, e)
        db.update_media_file(media_id, status="error",
                             notes=f"SSH rename failed: {e}")
        return False
    db.mark_manifest_verified(manifest_id)

    # ── Write NFO sidecar ──
    _write_nfo_ssh(row, dst_win, media_type, tv_output,
                   written_show_nfos, session)

    # ── Move subtitles ──
    _move_subtitles_ssh(media_id, dst_win, session)

    db.update_media_file(media_id, status="applied", proposed_path=dst_win)
    return True


# ─── NFO via SSH ──────────────────────────────────────────────────────────────

def _write_nfo_ssh(row, dst_win: str, media_type: str,
                   tv_output: str, written_show_nfos: set,
                   session: SSHSession) -> None:
    from src import db
    from src.utils.nfo_writer import write_movie_nfo, write_tvshow_nfo, write_episode_nfo
    from src.applier import _row_to_nfo_data
    from src.utils.file_utils import build_nfo_path, build_tvshow_nfo_path
    from pathlib import Path
    import tempfile, os

    nfo_data = _row_to_nfo_data(row)

    def _push_nfo(writer_fn, win_path: str) -> None:
        posix_path = session.to_posix(win_path)
        with tempfile.NamedTemporaryFile(suffix=".nfo", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            writer_fn(Path(tmp_path), nfo_data)
            with open(tmp_path, "r", encoding="utf-8") as f:
                content = f.read()
            session.write_text(posix_path, content)
            db.log_manifest_op(row["id"], "nfo", "", win_path, "copy")
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    nfo_win = str(build_nfo_path(Path(dst_win)))

    if media_type == "movie":
        _push_nfo(write_movie_nfo, nfo_win)
    elif media_type == "tv":
        _push_nfo(write_episode_nfo, nfo_win)
        show_id = row["tmdb_id"]
        if show_id and show_id not in written_show_nfos:
            show_nfo_win = str(build_tvshow_nfo_path(tv_output, row["confirmed_title"]))
            show_nfo_posix = session.to_posix(show_nfo_win)
            if not session.exists(show_nfo_posix):
                _push_nfo(write_tvshow_nfo, show_nfo_win)
            written_show_nfos.add(show_id)


# ─── Subtitle moving via SSH ──────────────────────────────────────────────────

def _move_subtitles_ssh(media_id: int, new_video_win: str,
                        session: SSHSession) -> None:
    from src import db
    from src.utils.file_utils import build_subtitle_path
    from pathlib import Path

    subs = db.get_subtitles_for_media(media_id)
    for sub in subs:
        sub_src_win   = sub["original_path"]
        sub_src_posix = session.to_posix(sub_src_win)
        if not session.exists(sub_src_posix):
            continue

        sub_dst_win = str(build_subtitle_path(
            Path(new_video_win), sub["language"], sub["extension"]))
        sub_dst_posix = session.ensure_unique(session.to_posix(sub_dst_win))

        m_id = db.log_manifest_op(None, "subtitle", sub_src_win, sub_dst_win, "copy")
        try:
            session.rename(sub_src_posix, sub_dst_posix)
            db.mark_manifest_verified(m_id)
            db.log_manifest_op(None, "subtitle", sub_src_win, "", "delete")
            db.update_subtitle_file_by_id(
                sub["id"], status="applied", proposed_path=sub_dst_win)
        except Exception as e:
            logger.error("SSH subtitle move failed %s: %s", sub_src_win, e)
