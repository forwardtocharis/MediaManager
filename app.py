"""
app.py — MediaManager Flask web application.

Run:  python app.py
Then open: http://localhost:5000
"""

import json
import logging
import queue
import threading
import uuid
import webbrowser
import os
from pathlib import Path

import yaml
from flask import (Flask, Response, jsonify, redirect, render_template,
                   request, stream_with_context, url_for)

# ─── App setup ────────────────────────────────────────────────────────────────

app = Flask(__name__)
# fallback to current string if env var not set to preserve backwards compatibility
app.secret_key = os.environ.get("MEDIAMANAGER_SECRET_KEY", "mediamanager-ui-secret")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ─── Config management ────────────────────────────────────────────────────────

_config_path = Path("config.yml")
_config: dict = {}


def load_config() -> dict:
    global _config
    if _config_path.exists():
        with open(_config_path, encoding="utf-8") as f:
            _config = yaml.safe_load(f) or {}
    return _config


def save_config(cfg: dict) -> None:
    global _config
    _config = cfg
    with open(_config_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)


def get_config() -> dict:
    return _config


def _deep_merge(base: dict, upd: dict) -> None:
    for k, v in upd.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def init_db():
    from src import db
    cfg = get_config()
    db_path = cfg.get("database", {}).get("path", "mediamanager.db")
    db.init(db_path)
    db.init_rate_limit("tmdb")
    db.init_rate_limit("omdb")


# ─── Background job management ────────────────────────────────────────────────

_job_queues: dict[str, queue.Queue] = {}
_job_queues_lock = threading.Lock()


def create_job() -> str:
    job_id = str(uuid.uuid4())[:8]
    with _job_queues_lock:
        _job_queues[job_id] = queue.Queue()
    return job_id


def send_event(job_id: str, event_type: str, data: dict) -> None:
    with _job_queues_lock:
        q = _job_queues.get(job_id)
    if q:
        q.put({"type": event_type, **data})


def end_job(job_id: str) -> None:
    with _job_queues_lock:
        q = _job_queues.get(job_id)
    if q:
        q.put(None)


# ─── SSE stream endpoint ──────────────────────────────────────────────────────

@app.route("/stream/<job_id>")
def stream(job_id):
    def generate():
        with _job_queues_lock:
            q = _job_queues.get(job_id)
        if not q:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Job not found'})}\n\n"
            return
        while True:
            try:
                msg = q.get(timeout=30)
                if msg is None:
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
                    with _job_queues_lock:
                        _job_queues.pop(job_id, None)
                    break
                yield f"data: {json.dumps(msg)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'ping'})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Connection": "keep-alive"},
    )


# ─── Page routes ──────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    return render_template("dashboard.html")

@app.route("/library")
def library():
    return render_template("library.html")

@app.route("/pipeline")
def pipeline():
    return render_template("pipeline.html")

@app.route("/settings")
def settings():
    return render_template("settings.html", config=get_config())

@app.route("/duplicates")
def duplicates_page():
    return render_template("duplicates.html")

@app.route("/verify")
def verify_page():
    return render_template("verify.html")


# ─── API: Status ──────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    from src import db
    status_counts = db.count_by_status()
    phase_counts = db.count_by_phase()
    api_state = {}
    for api_name in ("tmdb", "omdb"):
        row = db.get_rate_limit(api_name)
        if row:
            api_state[api_name] = dict(row)
    return jsonify({
        "status_counts": status_counts,
        "phase_counts": {str(k): v for k, v in phase_counts.items()},
        "api_state": api_state,
    })


# ─── API: Files ───────────────────────────────────────────────────────────────

@app.route("/api/files")
def api_files():
    from src import db
    status  = request.args.get("status", "all")
    mtype   = request.args.get("type", "all")
    search  = request.args.get("search", "").strip()
    page    = max(1, int(request.args.get("page", 1)))
    per_page = min(max(1, int(request.args.get("per_page", 50))), 500)
    sort_by  = request.args.get("sort", "id")
    sort_dir = request.args.get("dir", "asc")

    valid_sorts = {"id", "filename", "confidence", "status",
                   "confirmed_title", "guessed_title", "file_size"}
    if sort_by not in valid_sorts:
        sort_by = "id"
    order_sql = f"ORDER BY {sort_by} {'DESC' if sort_dir == 'desc' else 'ASC'}"

    where_parts, params = [], []
    if status != "all":
        where_parts.append("status = ?"); params.append(status)
    if mtype == "movie":
        where_parts.append("(confirmed_type='movie' OR (confirmed_type IS NULL AND guessed_type='movie'))")
    elif mtype == "tv":
        where_parts.append("(confirmed_type='tv' OR (confirmed_type IS NULL AND guessed_type='tv'))")
    if search:
        where_parts.append("(filename LIKE ? OR guessed_title LIKE ? OR confirmed_title LIKE ?)")
        like = f"%{search}%"; params += [like, like, like]

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    with db.connect() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM media_files {where_sql}", params
        ).fetchone()[0]
        offset = (page - 1) * per_page
        rows = conn.execute(
            f"SELECT * FROM media_files {where_sql} {order_sql} LIMIT ? OFFSET ?",
            params + [per_page, offset]
        ).fetchall()

    files = [_row_to_dict(r) for r in rows]
    return jsonify({
        "files": files, "total": total, "page": page,
        "per_page": per_page,
        "total_pages": max(1, (total + per_page - 1) // per_page),
    })


def _row_to_dict(row) -> dict:
    keys = ["id", "filename", "extension", "file_size", "parent_folder",
            "original_path", "guessed_title", "guessed_year", "guessed_type",
            "guessed_season", "guessed_episode", "is_extra",
            "tmdb_id", "imdb_id", "confirmed_title", "confirmed_year",
            "confirmed_type", "season", "episode", "episode_title",
            "genres", "plot", "rating", "director", "cast",
            "confidence", "status", "phase", "notes", "proposed_path"]
    return {k: row[k] for k in keys if k in row.keys()}


@app.route("/api/files/<int:file_id>", methods=["GET"])
def api_get_file(file_id):
    from src import db
    row = db.get_media_file(file_id)
    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify(_row_to_dict(row))


@app.route("/api/files/<int:file_id>", methods=["PUT"])
def api_update_file(file_id):
    from src import db
    data = request.get_json() or {}
    allowed = {"confirmed_title", "confirmed_year", "confirmed_type",
               "tmdb_id", "imdb_id", "season", "episode", "episode_title",
               "status", "notes", "genres", "plot", "rating", "director", "phase"}
    updates = {k: v for k, v in data.items() if k in allowed}
    if updates:
        db.update_media_file(file_id, **updates)
    row = db.get_media_file(file_id)
    return jsonify(_row_to_dict(row))


@app.route("/api/files/<int:file_id>/skip", methods=["POST"])
def api_skip_file(file_id):
    from src import db
    db.update_media_file(file_id, status="skipped", notes="Manually skipped")
    return jsonify({"ok": True})


@app.route("/api/files/<int:file_id>/reset", methods=["POST"])
def api_reset_file(file_id):
    from src import db
    db.update_media_file(file_id, status="pending", phase=0, confidence=0,
                         confirmed_title=None, confirmed_year=None,
                         confirmed_type=None, tmdb_id=None, imdb_id=None,
                         notes="Reset via UI")
    return jsonify({"ok": True})


# ─── API: TMDB lookups ────────────────────────────────────────────────────────

@app.route("/api/tmdb/search", methods=["POST"])
def api_tmdb_search():
    from src.api.tmdb import TMDBClient
    data = request.get_json() or {}
    title = data.get("title", "").strip()
    year = data.get("year")
    mtype = data.get("type", "movie")
    cfg = get_config()
    key = cfg.get("api", {}).get("tmdb_key", "").strip()
    if not key:
        return jsonify({"error": "TMDB key not configured"}), 400
    try:
        client = TMDBClient(api_key=key)
        if mtype == "tv":
            raw = client.search_tv(title, year)[:6]
            results = [{"tmdb_id": r.get("id"), "title": r.get("name"),
                        "year": (r.get("first_air_date") or "")[:4],
                        "type": "tv", "overview": (r.get("overview") or "")[:120]}
                       for r in raw]
        else:
            raw = client.search_movie(title, year)[:6]
            results = [{"tmdb_id": r.get("id"), "title": r.get("title"),
                        "year": (r.get("release_date") or "")[:4],
                        "type": "movie", "overview": (r.get("overview") or "")[:120]}
                       for r in raw]
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tmdb/details", methods=["POST"])
def api_tmdb_details():
    from src.api.tmdb import TMDBClient
    data = request.get_json() or {}
    tmdb_id = data.get("tmdb_id")
    mtype   = data.get("type", "movie")
    cfg = get_config()
    key = cfg.get("api", {}).get("tmdb_key", "").strip()
    if not key:
        return jsonify({"error": "TMDB key not configured"}), 400
    try:
        client = TMDBClient(api_key=key)
        details = (client.get_tv_details(int(tmdb_id)) if mtype == "tv"
                   else client.get_movie_details(int(tmdb_id)))
        return jsonify(details or {})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── API: Config ──────────────────────────────────────────────────────────────

def _redact_config(cfg: dict) -> dict:
    import copy
    c = copy.deepcopy(cfg)
    for k in ("tmdb_key", "omdb_key"):
        if k in c.get("api", {}) and c["api"][k]:
            c["api"][k] = "***"
    if "api_key" in c.get("llm", {}) and c["llm"]["api_key"]:
        c["llm"]["api_key"] = "***"
    if "password" in c.get("ssh", {}) and c["ssh"]["password"]:
        c["ssh"]["password"] = "***"
    return c


def _make_apply_fn(cfg: dict):
    """
    Return (apply_fn, cleanup_fn) where apply_fn has the same signature as
    applier._apply_one but routes to SSH when enabled.
    cleanup_fn() must be called when the job finishes to close the SSH session.
    """
    ssh_cfg = cfg.get("ssh", {})
    if ssh_cfg.get("enabled"):
        from src.ssh_applier import session_from_config, apply_one_ssh
        session = session_from_config(ssh_cfg)
        def apply_fn(row, movies_out, tv_out, dry_run, written_show_nfos):
            return apply_one_ssh(row, movies_out, tv_out, dry_run,
                                 written_show_nfos, session)
        def cleanup():
            session.close()
        return apply_fn, cleanup
    else:
        from src.applier import _apply_one
        return _apply_one, lambda: None

@app.route("/api/config", methods=["GET"])
def api_get_config():
    return jsonify(_redact_config(get_config()))


@app.route("/api/config", methods=["POST"])
def api_save_config():
    data = request.get_json() or {}
    cfg = get_config().copy()
    _deep_merge(cfg, data)
    save_config(cfg)
    init_db()
    return jsonify({"ok": True})


@app.route("/api/config/test/tmdb", methods=["POST"])
def api_test_tmdb():
    from src.api.tmdb import TMDBClient
    key = (request.get_json() or {}).get("api_key", "").strip()
    if not key:
        return jsonify({"ok": False, "message": "No API key provided"})
    try:
        client = TMDBClient(api_key=key)
        results = client.search_movie("The Matrix", 1999)
        return jsonify({"ok": bool(results), "message": "Connected!" if results else "No results"})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/config/test/omdb", methods=["POST"])
def api_test_omdb():
    from src.api.omdb import OMDBClient
    key = (request.get_json() or {}).get("api_key", "").strip()
    if not key:
        return jsonify({"ok": False, "message": "No API key provided"})
    try:
        client = OMDBClient(api_key=key)
        result = client.search_by_title("The Matrix", 1999, "movie")
        return jsonify({"ok": bool(result), "message": "Connected!" if result else "No result"})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/config/test/ssh", methods=["POST"])
def api_test_ssh():
    try:
        import paramiko  # noqa: F401
    except ImportError:
        return jsonify({"ok": False, "message": "paramiko not installed — use the Install button in SSH settings"})
    try:
        from src.ssh_applier import session_from_config
        data = request.get_json() or {}
        saved_ssh = get_config().get("ssh", {})
        ssh_cfg = {
            "host":     (data.get("host") or "").strip(),
            "port":     int(data.get("port") or 22),
            "username": (data.get("username") or "").strip(),
            # If the UI sent an empty password, fall back to the saved one
            "password": (data.get("password") or "").strip() or (saved_ssh.get("password") or ""),
            "key_path": (data.get("key_path") or "").strip() or (saved_ssh.get("key_path") or ""),
            "path_map": data.get("path_map") or saved_ssh.get("path_map") or {},
        }
        logger.info("SSH test: host=%s port=%s user=%s password_len=%d key_path=%r",
                    ssh_cfg["host"], ssh_cfg["port"], ssh_cfg["username"],
                    len(ssh_cfg["password"]), ssh_cfg["key_path"])
        if not ssh_cfg["host"] or not ssh_cfg["username"]:
            return jsonify({"ok": False, "message": "Host and username are required"})
        session = session_from_config(ssh_cfg)
        hostname = session.test()
        session.close()
        return jsonify({"ok": True, "message": f"Connected to {hostname}"})
    except Exception as e:
        logger.error("SSH test failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/jobs/install-dep", methods=["POST"])
def job_install_dep():
    package = (request.get_json() or {}).get("package", "").strip()
    # Allowlist — only permit packages we explicitly support installing
    allowed = {"paramiko"}
    if package not in allowed:
        return jsonify({"error": f"Package '{package}' is not in the install allowlist"}), 400

    job_id = create_job()

    def run():
        import subprocess, sys
        try:
            send_event(job_id, "log", {"message": f"Installing {package}...", "level": "info"})
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", package],
                capture_output=True, text=True, timeout=120
            )
            if result.returncode == 0:
                send_event(job_id, "log", {"message": f"{package} installed successfully.", "level": "success"})
                send_event(job_id, "complete", {"ok": True})
            else:
                send_event(job_id, "log", {"message": result.stderr.strip() or "Install failed.", "level": "error"})
                send_event(job_id, "complete", {"ok": False})
        except Exception as e:
            send_event(job_id, "log", {"message": str(e), "level": "error"})
            send_event(job_id, "complete", {"ok": False})
        finally:
            end_job(job_id)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"job_id": job_id})


# ─── API: LLM ─────────────────────────────────────────────────────────────────

@app.route("/api/llm/providers", methods=["GET"])
def api_llm_providers():
    from src.llm import detect_providers
    return jsonify({"providers": detect_providers()})


@app.route("/api/llm/test", methods=["POST"])
def api_llm_test():
    from src.llm import test_connection
    d = request.get_json() or {}
    result = test_connection(
        provider=d.get("provider", ""),
        model=d.get("model", ""),
        endpoint=d.get("endpoint", ""),
        api_key=d.get("api_key", ""),
    )
    return jsonify(result)


# ─── API: Duplicates ──────────────────────────────────────────────────────────

@app.route("/api/duplicates", methods=["GET"])
def api_duplicates():
    from src import db
    dups = db.get_pending_duplicates()
    result = []
    for dup in dups:
        media_ids = json.loads(dup["media_ids"])
        files = []
        for mid in media_ids:
            row = db.get_media_file(mid)
            if row:
                files.append({"id": row["id"], "filename": row["filename"],
                               "file_size": row["file_size"],
                               "original_path": row["original_path"],
                               "extension": row["extension"]})
        result.append({"id": dup["id"], "tmdb_id": dup["tmdb_id"], "files": files})
    return jsonify({"duplicates": result})


@app.route("/api/duplicates/<int:dup_id>/resolve", methods=["POST"])
def api_resolve_duplicate(dup_id):
    from src import db
    resolution = (request.get_json() or {}).get("resolution", "keep_all")
    db.resolve_duplicate(dup_id, resolution)
    if resolution.startswith("keep_id:"):
        keep_id = int(resolution.split(":")[1])
        with db.connect() as conn:
            row = conn.execute("SELECT media_ids FROM duplicates WHERE id=?",
                               (dup_id,)).fetchone()
        if row:
            for mid in json.loads(row["media_ids"]):
                if mid != keep_id:
                    db.update_media_file(mid, status="skipped",
                                         notes=f"Duplicate — kept ID {keep_id}")
    return jsonify({"ok": True})


# ─── API: Rate limits ─────────────────────────────────────────────────────────

@app.route("/api/ratelimit/<api_name>/resume", methods=["POST"])
def api_resume(api_name):
    from src import db
    if api_name not in ("tmdb", "omdb"):
        return jsonify({"error": "Invalid API"}), 400
    db.set_api_paused(api_name, False)
    return jsonify({"ok": True})


@app.route("/api/ratelimit/<api_name>/pause", methods=["POST"])
def api_pause(api_name):
    from src import db
    if api_name not in ("tmdb", "omdb"):
        return jsonify({"error": "Invalid API"}), 400
    db.set_api_paused(api_name, True, "Manually paused via UI")
    return jsonify({"ok": True})


# ─── API: Filesystem browser ──────────────────────────────────────────────────

# Paths that must never be browsed on Linux (virtual/sensitive filesystems)
_BLOCKED_PATH_PREFIXES = (
    "/proc", "/sys", "/dev", "/run", "/boot",
)


def _is_safe_browse_path(p: Path) -> bool:
    """Return False if the resolved path points to a sensitive system directory."""
    try:
        resolved = str(p.resolve())
    except Exception:
        return False
    for blocked in _BLOCKED_PATH_PREFIXES:
        if resolved == blocked or resolved.startswith(blocked + "/"):
            return False
    return True


@app.route("/api/browse")
def api_browse():
    import string
    path = request.args.get("path", "").strip()
    if not path:
        drives = []
        for letter in string.ascii_uppercase:
            d = f"{letter}:\\"
            if Path(d).exists():
                drives.append({"name": d, "path": d, "type": "drive"})
        return jsonify({"entries": drives, "current": "", "parent": None})
    try:
        # Use Path without .resolve() so UNC paths (\\server\share) are preserved
        p = Path(path)
        if not p.is_absolute():
            return jsonify({"error": "Path must be absolute"}), 400
        if not _is_safe_browse_path(p):
            return jsonify({"error": "Access to this path is not permitted"}), 403
        if not p.exists():
            return jsonify({"error": "Path not found"}), 404
        entries = [{"name": c.name, "path": str(c), "type": "directory"}
                   for c in sorted(p.iterdir()) if c.is_dir()
                   if _is_safe_browse_path(c)]
        # For UNC roots like \\server\share, parent should be None (can't go higher)
        raw_parent = p.parent
        parent = None if raw_parent == p or str(raw_parent) == str(p) else str(raw_parent)
        return jsonify({"entries": entries, "current": str(p), "parent": parent})
    except PermissionError:
        return jsonify({"error": "Permission denied"}), 403
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── API: Manifest / Rollback ─────────────────────────────────────────────────

@app.route("/api/manifest")
def api_manifest():
    from src import db
    ops = db.get_manifest_ops(rolled_back=False)
    return jsonify({"operations": [dict(op) for op in ops[:200]]})


@app.route("/api/folders/delete", methods=["POST"])
def api_folders_delete():
    """Delete a list of source folders supplied by the user after an apply run."""
    body = request.get_json(silent=True) or {}
    paths = body.get("paths", [])
    if not isinstance(paths, list):
        return jsonify({"error": "paths must be a list"}), 400

    deleted = []
    errors = []
    for raw in paths:
        p = Path(raw)
        if not p.is_dir():
            errors.append({"path": raw, "error": "not a directory or already gone"})
            continue
        try:
            import shutil
            shutil.rmtree(p)
            deleted.append(str(p))
            logger.info("User-deleted folder: %s", p)
        except Exception as exc:
            errors.append({"path": raw, "error": str(exc)})

    return jsonify({"deleted": deleted, "errors": errors})


@app.route("/api/rollback", methods=["POST"])
def api_rollback():
    from src.applier import rollback_all
    dry = (request.get_json() or {}).get("dry_run", True)
    stats = rollback_all(dry_run=dry)
    return jsonify(stats)


# ─── Jobs ─────────────────────────────────────────────────────────────────────

@app.route("/api/jobs/scan", methods=["POST"])
def job_scan():
    cfg = get_config()
    job_id = create_job()

    def run():
        try:
            from src.scanner import scan, link_subtitles_to_media
            source = cfg.get("source", {})
            files_cfg = cfg.get("files", {})
            scan_mode = cfg.get("scan_mode", "both")
            movies_path = source.get("movies_path", "").strip() or None
            tv_path = source.get("tv_path", "").strip() or None

            if scan_mode == "movies":
                tv_path = None
            elif scan_mode == "tv":
                movies_path = None

            if not movies_path and not tv_path:
                send_event(job_id, "log", {"message": "No source paths configured for the selected scan mode.", "level": "error"})
                return

            send_event(job_id, "log", {"message": "Starting scan...", "level": "info"})
            stats = scan(
                movies_path=movies_path, tv_path=tv_path,
                media_extensions=set(files_cfg.get("media_extensions", [])),
                subtitle_extensions=set(files_cfg.get("subtitle_extensions", [])),
            )
            linked = link_subtitles_to_media()
            send_event(job_id, "log", {
                "message": f"Scan complete — {stats['media_new']} new files, {linked} subtitles linked.",
                "level": "success"})
            send_event(job_id, "stats", stats)
        except Exception as e:
            send_event(job_id, "log", {"message": f"Error: {e}", "level": "error"})
            logger.error("Scan error: %s", e, exc_info=True)
        finally:
            end_job(job_id)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/api/jobs/identify", methods=["POST"])
def job_identify():
    cfg = get_config()
    job_id = create_job()
    body = request.get_json(silent=True) or {}
    limit = body.get("limit")

    def run():
        try:
            from src import db as _db
            from src.api.tmdb import TMDBClient, RateLimitPausedError as TP
            from src.api.omdb import OMDBClient, RateLimitPausedError as OP
            from src.identifier import _identify_one

            api_cfg = cfg.get("api", {})
            rl = cfg.get("rate_limits", {})
            ident = cfg.get("identification", {})
            tmdb_key = api_cfg.get("tmdb_key", "").strip()
            omdb_key = api_cfg.get("omdb_key", "").strip()

            if not tmdb_key:
                send_event(job_id, "log", {"message": "TMDB key not configured.", "level": "error"})
                return

            tmdb_rl = rl.get("tmdb", {})
            omdb_rl = rl.get("omdb", {})
            tmdb = TMDBClient(api_key=tmdb_key,
                              daily_limit=tmdb_rl.get("daily_limit", 0),
                              requests_per_second=tmdb_rl.get("requests_per_second", 40))
            omdb = (OMDBClient(api_key=omdb_key,
                               daily_limit=omdb_rl.get("daily_limit", 950),
                               requests_per_second=omdb_rl.get("requests_per_second", 2))
                    if omdb_key else None)

            pending = _db.get_media_by_status("pending")
            if limit:
                pending = pending[:int(limit)]
            total = len(pending)
            send_event(job_id, "log", {"message": f"Identifying {total} files...", "level": "info"})
            send_event(job_id, "progress", {"current": 0, "total": total})

            identified = needs_llm = needs_manual = errors = 0
            for i, row in enumerate(pending):
                try:
                    result = _identify_one(
                        row, tmdb, omdb,
                        ident.get("auto_confirm_threshold", 75),
                        ident.get("llm_threshold", 50))
                    s = result.get("status", "needs_manual")
                    if s == "identified": identified += 1
                    elif s == "needs_llm": needs_llm += 1
                    else: needs_manual += 1
                    send_event(job_id, "progress", {
                        "current": i + 1, "total": total,
                        "filename": row["filename"], "result": s,
                        "confidence": result.get("confidence", 0)})
                except (TP, OP) as e:
                    send_event(job_id, "log", {"message": f"API paused: {e}", "level": "warning"})
                    break
                except Exception as e:
                    errors += 1
                    logger.warning("Identify %s: %s", row["filename"], e)

            send_event(job_id, "log", {
                "message": f"Done — {identified} identified, {needs_llm} need LLM, {needs_manual} need manual, {errors} errors.",
                "level": "success"})
            send_event(job_id, "complete", {
                "identified": identified, "needs_llm": needs_llm,
                "needs_manual": needs_manual, "errors": errors})
        except Exception as e:
            send_event(job_id, "log", {"message": f"Error: {e}", "level": "error"})
            logger.error("Identify error: %s", e, exc_info=True)
        finally:
            end_job(job_id)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/api/jobs/llm", methods=["POST"])
def job_llm():
    cfg = get_config()
    job_id = create_job()
    llm_cfg = cfg.get("llm", {})
    provider   = llm_cfg.get("provider", "")
    model      = llm_cfg.get("model", "")
    endpoint   = llm_cfg.get("endpoint", "")
    api_key    = llm_cfg.get("api_key", "")
    batch_size = int(llm_cfg.get("batch_size", 20))

    if not provider or provider == "none":
        return jsonify({"error": "No LLM provider configured"}), 400

    def run():
        try:
            from src import db as _db
            from src.llm import run_llm_pass
            from src.api.tmdb import TMDBClient
            tmdb_key = cfg.get("api", {}).get("tmdb_key", "").strip()
            tmdb = TMDBClient(api_key=tmdb_key) if tmdb_key else None
            files = _db.get_media_by_status("needs_llm")
            total = len(files)
            if not files:
                send_event(job_id, "log", {"message": "No files need LLM review.", "level": "info"})
                return
            send_event(job_id, "log", {"message": f"Running LLM pass on {total} files...", "level": "info"})
            send_event(job_id, "progress", {"current": 0, "total": total})

            def cb(current, total, filename, result):
                send_event(job_id, "progress", {
                    "current": current, "total": total,
                    "filename": filename, "result": result})

            stats = run_llm_pass(
                files=files, provider=provider, model=model,
                endpoint=endpoint, api_key=api_key,
                batch_size=batch_size, tmdb=tmdb, progress_cb=cb)
            send_event(job_id, "log", {
                "message": f"LLM complete — {stats['confirmed']} confirmed, {stats['skipped']} skipped, {stats['errors']} errors.",
                "level": "success"})
            send_event(job_id, "complete", stats)
        except Exception as e:
            send_event(job_id, "log", {"message": f"Error: {e}", "level": "error"})
            logger.error("LLM error: %s", e, exc_info=True)
        finally:
            end_job(job_id)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/api/jobs/apply", methods=["POST"])
def job_apply():
    cfg = get_config()
    job_id = create_job()
    body = request.get_json(silent=True) or {}
    phases  = body.get("phases", ["1"])
    dry_run = body.get("dry_run", True)

    source = cfg.get("source", {})
    output = cfg.get("output", {})
    movies_out = (output.get("movies_path") or source.get("movies_path", "")).strip()
    tv_out     = (output.get("tv_path")     or source.get("tv_path", "")).strip()

    def run():
        apply_fn, cleanup = _make_apply_fn(cfg)
        try:
            from src import db as _db

            if "all" in phases:
                phase_nums = [1, 2, 3]
            else:
                phase_nums = [int(p) if str(p).isdigit() else 3 for p in phases]

            with _db.connect() as conn:
                placeholders = ",".join("?" * len(phase_nums))
                rows = conn.execute(
                    f"SELECT * FROM media_files WHERE status='identified' "
                    f"AND phase IN ({placeholders}) ORDER BY id",
                    phase_nums).fetchall()

            ssh_mode = cfg.get("ssh", {}).get("enabled", False)
            total = len(rows)
            send_event(job_id, "log", {
                "message": f"{'[DRY RUN] ' if dry_run else ''}{'[SSH] ' if ssh_mode else ''}Applying {total} files...",
                "level": "info"})
            send_event(job_id, "progress", {"current": 0, "total": total})

            applied = skipped = errors = 0
            written_show_nfos: set = set()
            source_dirs: set = set()
            for i, row in enumerate(rows):
                try:
                    ok = apply_fn(row, movies_out, tv_out, dry_run, written_show_nfos)
                    if ok:
                        applied += 1
                        source_dirs.add(Path(row["original_path"]).parent)
                    else:
                        skipped += 1
                    send_event(job_id, "progress", {
                        "current": i + 1, "total": total,
                        "filename": row["filename"],
                        "result": "applied" if ok else "skipped"})
                except Exception as e:
                    errors += 1
                    send_event(job_id, "log", {"message": f"Error: {row['filename']}: {e}", "level": "error"})

            send_event(job_id, "log", {
                "message": f"Done — {applied} {'[dry] applied' if dry_run else 'applied'}, {skipped} skipped, {errors} errors.",
                "level": "success"})
            send_event(job_id, "complete", {"applied": applied, "skipped": skipped, "errors": errors})

            if not dry_run and source_dirs and not ssh_mode:
                from src.applier import cleanup_source_folders
                folder_result = cleanup_source_folders(source_dirs)
                if folder_result["deleted"]:
                    send_event(job_id, "log", {
                        "message": f"Auto-deleted {len(folder_result['deleted'])} empty folder(s).",
                        "level": "info"})
                if folder_result["non_empty"]:
                    send_event(job_id, "folder_cleanup", {"data": folder_result["non_empty"]})
        except Exception as e:
            send_event(job_id, "log", {"message": f"Error: {e}", "level": "error"})
            logger.error("Apply error: %s", e, exc_info=True)
        finally:
            cleanup()
            end_job(job_id)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"job_id": job_id})


# ─── API: Verify (before/after preview) ──────────────────────────────────────

@app.route("/api/verify")
def api_verify():
    """Return before/after data for all identified files without writing anything."""
    from src import db as _db
    from src.applier import compute_proposed_paths

    cfg    = get_config()
    source = cfg.get("source", {})
    output = cfg.get("output", {})
    movies_out = (output.get("movies_path") or source.get("movies_path", "")).strip()
    tv_out     = (output.get("tv_path")     or source.get("tv_path",     "")).strip()

    # Allow filtering by phase via ?phase=1,2,3 or phase=all
    phase_arg = request.args.get("phase", "all")
    if phase_arg == "all":
        phase_nums = [1, 2, 3]
    else:
        phase_nums = [int(p) for p in phase_arg.split(",") if p.strip().isdigit()]

    with _db.connect() as conn:
        placeholders = ",".join("?" * len(phase_nums))
        rows = conn.execute(
            f"SELECT * FROM media_files "
            f"WHERE status='identified' AND phase IN ({placeholders}) ORDER BY id",
            phase_nums
        ).fetchall()

    # Preload all subtitles for the matched media files in one query
    subtitles_by_media_id: dict = {}
    if rows:
        media_ids = [r["id"] for r in rows]
        sub_placeholders = ",".join("?" * len(media_ids))
        with _db.connect() as conn_subs:
            all_subs = conn_subs.execute(
                f"SELECT * FROM subtitle_files WHERE parent_media_id IN ({sub_placeholders})",
                media_ids
            ).fetchall()
        for sub in all_subs:
            mid = sub["parent_media_id"]
            subtitles_by_media_id.setdefault(mid, []).append(sub)

    files = []
    summary = {"total": 0, "movies": 0, "tv_episodes": 0,
               "path_changed": 0, "filename_changed": 0, "metadata_only": 0}

    for row in rows:
        try:
            genres = []
            try:
                genres = json.loads(row["genres"] or "[]")
            except Exception:
                pass

            # ── BEFORE (what exists on disk now) ──────────────────────────
            orig = Path(row["original_path"])
            before = {
                "path":     row["original_path"],
                "folder":   str(orig.parent),
                "filename": orig.name,
                "title":    row["guessed_title"] or "",
                "year":     row["guessed_year"],
                "type":     row["guessed_type"] or "",
                "file_size_mb": round(row["file_size"] / 1024 / 1024, 1)
                                 if row["file_size"] else None,
                "extension": row["extension"],
                "season":   row["guessed_season"],
                "episode":  row["guessed_episode"],
            }

            # ── AFTER (what we will write) ────────────────────────────────
            proposed = compute_proposed_paths(row, movies_out, tv_out)
            if not proposed:
                continue

            after = {
                "path":          proposed["proposed_path"],
                "folder":        proposed["proposed_folder"],
                "filename":      proposed["proposed_filename"],
                "title":         row["confirmed_title"] or "",
                "year":          row["confirmed_year"],
                "type":          row["confirmed_type"] or "",
                "tmdb_id":       row["tmdb_id"],
                "imdb_id":       row["imdb_id"],
                "genres":        genres,
                "rating":        row["rating"],
                "plot":          row["plot"],
                "director":      row["director"],
                "season":        row["season"],
                "episode":       row["episode"],
                "episode_title": row["episode_title"],
                "nfo_path":      proposed["nfo_path"],
                "tvshow_nfo_path": proposed.get("tvshow_nfo_path"),
                "file_size_mb":  before["file_size_mb"],
                "extension":     row["extension"],
            }

            path_changed     = before["path"]     != after["path"]
            filename_changed = before["filename"] != after["filename"]

            # ── Subtitle changes ──────────────────────────────────────────
            subtitle_changes = []
            try:
                from src.utils.file_utils import build_subtitle_path
                for sub in subtitles_by_media_id.get(row["id"], []):
                    sub_dst = build_subtitle_path(
                        Path(proposed["proposed_path"]),
                        sub["language"], sub["extension"]
                    )
                    subtitle_changes.append({
                        "before": sub["original_path"],
                        "before_filename": Path(sub["original_path"]).name,
                        "after":  str(sub_dst),
                        "after_filename": sub_dst.name,
                    })
            except Exception:
                pass

            changes = {
                "path_changed":      path_changed,
                "filename_changed":  filename_changed,
                "title_changed":     before["title"].lower() != after["title"].lower()
                                      if before["title"] else bool(after["title"]),
                "year_changed":      before["year"] != after["year"],
                "type_changed":      before["type"] != after["type"],
                "has_genres":        bool(genres),
                "has_plot":          bool(row["plot"]),
                "has_rating":        bool(row["rating"]),
                "has_tmdb":          bool(row["tmdb_id"]),
                "has_imdb":          bool(row["imdb_id"]),
                "nfo_will_be_written": True,
                "subtitles_count":   len(subtitle_changes),
            }

            ctype = (row["confirmed_type"] or "movie").lower()
            summary["total"] += 1
            if ctype == "tv":
                summary["tv_episodes"] += 1
            else:
                summary["movies"] += 1
            if path_changed:
                summary["path_changed"] += 1
            elif filename_changed:
                summary["filename_changed"] += 1
            else:
                summary["metadata_only"] += 1

            files.append({
                "id":      row["id"],
                "phase":   row["phase"],
                "confidence": row["confidence"],
                "is_extra": bool(row["is_extra"]),
                "before":  before,
                "after":   after,
                "changes": changes,
                "subtitle_changes": subtitle_changes,
            })
        except Exception as e:
            logger.warning("Verify compute error for id %s: %s", row["id"], e)

    return jsonify({"files": files, "summary": summary})


@app.route("/api/jobs/apply-selected", methods=["POST"])
def job_apply_selected():
    """Apply changes for a specific list of file IDs."""
    cfg = get_config()
    body = request.get_json(silent=True) or {}
    file_ids = [int(i) for i in (body.get("file_ids") or [])][:500]
    dry_run  = body.get("dry_run", True)

    if not file_ids:
        return jsonify({"error": "No file IDs provided"}), 400

    source = cfg.get("source", {})
    output = cfg.get("output", {})
    movies_out = (output.get("movies_path") or source.get("movies_path", "")).strip()
    tv_out     = (output.get("tv_path")     or source.get("tv_path", "")).strip()

    job_id = create_job()

    def run():
        apply_fn, cleanup = _make_apply_fn(cfg)
        try:
            from src import db as _db

            with _db.connect() as conn:
                placeholders = ",".join("?" * len(file_ids))
                rows = conn.execute(
                    f"SELECT * FROM media_files WHERE id IN ({placeholders}) ORDER BY id",
                    file_ids
                ).fetchall()

            ssh_mode = cfg.get("ssh", {}).get("enabled", False)
            total = len(rows)
            label = f"{'[DRY RUN] ' if dry_run else ''}{'[SSH] ' if ssh_mode else ''}"
            send_event(job_id, "log", {
                "message": f"{label}Applying {total} selected files...",
                "level": "info"})
            send_event(job_id, "progress", {"current": 0, "total": total})

            applied = skipped = errors = 0
            written_show_nfos: set = set()
            source_dirs: set = set()

            for i, row in enumerate(rows):
                try:
                    ok = apply_fn(row, movies_out, tv_out, dry_run, written_show_nfos)
                    if ok:
                        applied += 1
                        source_dirs.add(Path(row["original_path"]).parent)
                    else:
                        skipped += 1
                    send_event(job_id, "progress", {
                        "current": i + 1, "total": total,
                        "filename": row["filename"],
                        "result": "applied" if ok else "skipped"})
                except Exception as e:
                    errors += 1
                    send_event(job_id, "log", {
                        "message": f"Error {row['filename']}: {e}",
                        "level": "error"})

            send_event(job_id, "log", {
                "message": (
                    f"Done — {applied} {'(dry) ' if dry_run else ''}applied, "
                    f"{skipped} skipped, {errors} errors."),
                "level": "success"})
            send_event(job_id, "complete",
                       {"applied": applied, "skipped": skipped, "errors": errors,
                        "dry_run": dry_run})

            if not dry_run and source_dirs and not ssh_mode:
                from src.applier import cleanup_source_folders
                folder_result = cleanup_source_folders(source_dirs)
                if folder_result["deleted"]:
                    send_event(job_id, "log", {
                        "message": f"Auto-deleted {len(folder_result['deleted'])} empty folder(s).",
                        "level": "info"})
                if folder_result["non_empty"]:
                    send_event(job_id, "folder_cleanup", {"data": folder_result["non_empty"]})
        except Exception as e:
            send_event(job_id, "log", {"message": f"Error: {e}", "level": "error"})
            logger.error("apply-selected error: %s", e, exc_info=True)
        finally:
            cleanup()
            end_job(job_id)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"job_id": job_id})


# ─── Startup ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    load_config()
    init_db()

    def _open_browser():
        import time
        time.sleep(1.2)
        webbrowser.open("http://localhost:5000")

    threading.Thread(target=_open_browser, daemon=True).start()
    print("\n  MediaManager UI -> http://localhost:5000\n")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
