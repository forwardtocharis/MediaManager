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
from pathlib import Path

import yaml
from flask import (Flask, Response, jsonify, redirect, render_template,
                   request, stream_with_context, url_for)

# ─── App setup ────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = "mediamanager-ui-secret"
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


def create_job() -> str:
    job_id = str(uuid.uuid4())[:8]
    _job_queues[job_id] = queue.Queue()
    return job_id


def send_event(job_id: str, event_type: str, data: dict) -> None:
    if job_id in _job_queues:
        _job_queues[job_id].put({"type": event_type, **data})


def end_job(job_id: str) -> None:
    if job_id in _job_queues:
        _job_queues[job_id].put(None)


# ─── SSE stream endpoint ──────────────────────────────────────────────────────

@app.route("/stream/<job_id>")
def stream(job_id):
    def generate():
        q = _job_queues.get(job_id)
        if not q:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Job not found'})}\n\n"
            return
        while True:
            try:
                msg = q.get(timeout=30)
                if msg is None:
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
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
    per_page = int(request.args.get("per_page", 50))
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

@app.route("/api/config", methods=["GET"])
def api_get_config():
    return jsonify(get_config())


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
        # Always offer UNC input option
        return jsonify({"entries": drives, "current": "", "parent": None})
    try:
        p = Path(path)
        if not p.exists():
            return jsonify({"error": "Path not found"}), 404
        entries = [{"name": c.name, "path": str(c), "type": "directory"}
                   for c in sorted(p.iterdir()) if c.is_dir()]
        parent = str(p.parent) if p.parent != p else None
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
            movies_path = source.get("movies_path", "").strip() or None
            tv_path = source.get("tv_path", "").strip() or None

            if not movies_path and not tv_path:
                send_event(job_id, "log", {"message": "No source paths configured.", "level": "error"})
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
        try:
            from src import db as _db
            from src.applier import _apply_one

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

            total = len(rows)
            send_event(job_id, "log", {
                "message": f"{'[DRY RUN] ' if dry_run else ''}Applying {total} files...",
                "level": "info"})
            send_event(job_id, "progress", {"current": 0, "total": total})

            applied = skipped = errors = 0
            written_show_nfos: set = set()
            for i, row in enumerate(rows):
                try:
                    ok = _apply_one(row, movies_out, tv_out, dry_run, written_show_nfos)
                    if ok: applied += 1
                    else:  skipped += 1
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
        except Exception as e:
            send_event(job_id, "log", {"message": f"Error: {e}", "level": "error"})
            logger.error("Apply error: %s", e, exc_info=True)
        finally:
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
            from pathlib import Path as _Path
            orig = _Path(row["original_path"])
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
                with _db.connect() as conn2:
                    subs = conn2.execute(
                        "SELECT * FROM subtitle_files WHERE parent_media_id=?",
                        (row["id"],)
                    ).fetchall()
                from src.utils.file_utils import build_subtitle_path
                for sub in subs:
                    sub_dst = build_subtitle_path(
                        _Path(proposed["proposed_path"]),
                        sub["language"], sub["extension"]
                    )
                    subtitle_changes.append({
                        "before": sub["original_path"],
                        "before_filename": _Path(sub["original_path"]).name,
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
                summary["path_changed"] += 1
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
    file_ids = [int(i) for i in (body.get("file_ids") or [])]
    dry_run  = body.get("dry_run", True)

    if not file_ids:
        return jsonify({"error": "No file IDs provided"}), 400

    source = cfg.get("source", {})
    output = cfg.get("output", {})
    movies_out = (output.get("movies_path") or source.get("movies_path", "")).strip()
    tv_out     = (output.get("tv_path")     or source.get("tv_path", "")).strip()

    job_id = create_job()

    def run():
        try:
            from src import db as _db
            from src.applier import _apply_one

            with _db.connect() as conn:
                placeholders = ",".join("?" * len(file_ids))
                rows = conn.execute(
                    f"SELECT * FROM media_files WHERE id IN ({placeholders}) ORDER BY id",
                    file_ids
                ).fetchall()

            total = len(rows)
            label = "[DRY RUN] " if dry_run else ""
            send_event(job_id, "log", {
                "message": f"{label}Applying {total} selected files...",
                "level": "info"})
            send_event(job_id, "progress", {"current": 0, "total": total})

            applied = skipped = errors = 0
            written_show_nfos: set = set()

            for i, row in enumerate(rows):
                try:
                    ok = _apply_one(row, movies_out, tv_out, dry_run, written_show_nfos)
                    if ok: applied += 1
                    else:  skipped += 1
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
        except Exception as e:
            send_event(job_id, "log", {"message": f"Error: {e}", "level": "error"})
            logger.error("apply-selected error: %s", e, exc_info=True)
        finally:
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
