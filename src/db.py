"""
db.py — SQLite database layer for MediaManager.

Handles schema creation, connection management, and all CRUD helpers.
"""

import datetime
import sqlite3
import json
from pathlib import Path
from typing import Optional, Any
from contextlib import contextmanager


# ─── Schema ──────────────────────────────────────────────────────────────────

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS scan_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS media_files (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    original_path    TEXT    NOT NULL UNIQUE,
    filename         TEXT    NOT NULL,
    extension        TEXT    NOT NULL,
    file_size        INTEGER,
    parent_folder    TEXT,

    -- Parsed guesses from filename
    guessed_title    TEXT,
    guessed_year     INTEGER,
    guessed_type     TEXT,    -- 'movie' | 'tv' | 'unknown'
    guessed_season   INTEGER,
    guessed_episode  INTEGER,
    is_extra         INTEGER DEFAULT 0,  -- boolean

    -- API-confirmed data
    tmdb_id          INTEGER,
    imdb_id          TEXT,
    confirmed_title  TEXT,
    confirmed_year   INTEGER,
    confirmed_type   TEXT,
    season           INTEGER,
    episode          INTEGER,
    episode_title    TEXT,
    genres           TEXT,   -- JSON array
    plot             TEXT,
    rating           TEXT,
    director         TEXT,
    cast             TEXT,   -- JSON array (top 5)
    air_date         TEXT,
    poster_url       TEXT,

    -- Workflow state
    confidence       REAL    DEFAULT 0,
    phase            INTEGER DEFAULT 0,   -- 0=unprocessed, 1=auto, 2=llm, 3=manual
    status           TEXT    DEFAULT 'pending',
    -- statuses: pending | identified | needs_llm | needs_manual | applied | skipped | error

    proposed_path    TEXT,   -- what Phase 3 will rename to
    notes            TEXT,

    created_at       TEXT    DEFAULT (datetime('now')),
    updated_at       TEXT    DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_media_status   ON media_files(status);
CREATE INDEX IF NOT EXISTS idx_media_phase    ON media_files(phase);
CREATE INDEX IF NOT EXISTS idx_media_type     ON media_files(confirmed_type);
CREATE INDEX IF NOT EXISTS idx_media_tmdb     ON media_files(tmdb_id);

CREATE TABLE IF NOT EXISTS subtitle_files (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    original_path    TEXT    NOT NULL UNIQUE,
    filename         TEXT    NOT NULL,
    extension        TEXT    NOT NULL,
    file_size        INTEGER,
    language         TEXT,   -- detected from filename (e.g. 'en', 'es')
    parent_media_id  INTEGER REFERENCES media_files(id) ON DELETE SET NULL,
    status           TEXT    DEFAULT 'pending',
    proposed_path    TEXT,
    created_at       TEXT    DEFAULT (datetime('now')),
    updated_at       TEXT    DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_sub_parent ON subtitle_files(parent_media_id);

CREATE TABLE IF NOT EXISTS apply_manifest (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id         INTEGER,
    file_type        TEXT    DEFAULT 'media',  -- 'media' | 'subtitle' | 'nfo'
    original_path    TEXT    NOT NULL,
    new_path         TEXT    NOT NULL,
    operation        TEXT    NOT NULL,  -- 'copy' | 'delete' | 'mkdir'
    verified         INTEGER DEFAULT 0,  -- copy verified by size match
    rolled_back      INTEGER DEFAULT 0,
    applied_at       TEXT    DEFAULT (datetime('now')),
    rolled_back_at   TEXT
);

CREATE TABLE IF NOT EXISTS subtitle_queue (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id      INTEGER NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    provider      TEXT    NOT NULL,   -- opensubtitles | subdl | podnapisi
    subtitle_id   TEXT,               -- provider-specific ID (file_id, sd_id, etc.)
    language      TEXT    NOT NULL,   -- ISO 639-1 code
    release_name  TEXT,
    score         REAL    DEFAULT 0,
    download_url  TEXT,               -- direct URL where available (Subdl)
    status        TEXT    DEFAULT 'queued',
    -- queued | downloading | embedded | sidecar | skipped | error
    error_msg     TEXT,
    created_at    TEXT    DEFAULT (datetime('now')),
    updated_at    TEXT    DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_subq_media   ON subtitle_queue(media_id, status);
CREATE INDEX IF NOT EXISTS idx_subq_lang    ON subtitle_queue(media_id, language);

CREATE TABLE IF NOT EXISTS rate_limit_state (
    api              TEXT    PRIMARY KEY,
    requests_today   INTEGER DEFAULT 0,
    reset_date       TEXT    DEFAULT (date('now')),
    paused           INTEGER DEFAULT 0,
    pause_reason     TEXT
);

CREATE TABLE IF NOT EXISTS duplicates (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    tmdb_id          INTEGER,
    media_ids        TEXT    NOT NULL,  -- JSON array of media_file ids
    resolution       TEXT    DEFAULT 'pending',  -- 'pending' | 'keep_id:{id}' | 'keep_all'
    created_at       TEXT    DEFAULT (datetime('now'))
);
"""

COLUMN_WHITELISTS = {
    "media_files": {
        "id", "original_path", "filename", "extension", "file_size", "parent_folder",
        "guessed_title", "guessed_year", "guessed_type", "guessed_season", "guessed_episode", "is_extra",
        "tmdb_id", "imdb_id", "confirmed_title", "confirmed_year", "confirmed_type",
        "season", "episode", "episode_title", "genres", "plot", "rating", "director",
        "cast", "air_date", "poster_url", "confidence", "phase", "status",
        "proposed_path", "notes", "created_at", "updated_at"
    },
    "subtitle_files": {
        "id", "original_path", "filename", "extension", "file_size", "language",
        "parent_media_id", "status", "proposed_path", "created_at", "updated_at"
    },
    "subtitle_queue": {
        "id", "media_id", "provider", "subtitle_id", "language", "release_name",
        "score", "download_url", "status", "error_msg", "created_at", "updated_at"
    }
}


def _validate_columns(table_name: str, columns: list[str]) -> None:
    """Raise ValueError if any column is not in the whitelist for the given table."""
    whitelist = COLUMN_WHITELISTS.get(table_name)
    if not whitelist:
        raise ValueError(f"No whitelist defined for table: {table_name}")
    for col in columns:
        if col not in whitelist:
            raise ValueError(f"Invalid column '{col}' for table '{table_name}'")


# ─── Connection management ────────────────────────────────────────────────────

_db_path: Optional[Path] = None


def init(db_path: str | Path) -> None:
    """Set the database path and create schema if needed."""
    global _db_path
    _db_path = Path(db_path)
    _db_path.parent.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        conn.executescript(SCHEMA)


def _get_path() -> Path:
    if _db_path is None:
        raise RuntimeError("db.init() must be called before using the database.")
    return _db_path


@contextmanager
def connect():
    """Context manager that yields a sqlite3 connection with row_factory set."""
    conn = sqlite3.connect(_get_path(), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─── scan_meta helpers ────────────────────────────────────────────────────────

def set_meta(key: str, value: Any) -> None:
    with connect() as conn:
        conn.execute(
            "INSERT INTO scan_meta(key, value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value))
        )


def get_meta(key: str, default: Any = None) -> Optional[str]:
    with connect() as conn:
        row = conn.execute("SELECT value FROM scan_meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


# ─── media_files helpers ──────────────────────────────────────────────────────

def upsert_media_file(data: dict) -> int:
    """Insert or update a media file record. Returns the row id."""
    fields = list(data.keys())
    _validate_columns("media_files", fields)
    placeholders = ", ".join(["?"] * len(fields))
    col_list = ", ".join(fields)
    updates = ", ".join(f"{f}=excluded.{f}" for f in fields if f != "original_path")
    sql = (
        f"INSERT INTO media_files({col_list}) VALUES({placeholders}) "
        f"ON CONFLICT(original_path) DO UPDATE SET {updates}, "
        f"updated_at=datetime('now')"
    )
    with connect() as conn:
        cur = conn.execute(sql, list(data.values()))
        if cur.lastrowid:
            return cur.lastrowid
        row = conn.execute(
            "SELECT id FROM media_files WHERE original_path=?",
            (data["original_path"],)
        ).fetchone()
        return row["id"]


def get_media_file(media_id: int) -> Optional[sqlite3.Row]:
    with connect() as conn:
        return conn.execute("SELECT * FROM media_files WHERE id=?", (media_id,)).fetchone()


def get_media_by_path(path: str) -> Optional[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM media_files WHERE original_path=?", (path,)
        ).fetchone()


def get_subtitle_by_path(path: str) -> Optional[sqlite3.Row]:
    """Return a subtitle_files row for the given path, or None."""
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM subtitle_files WHERE original_path=?", (path,)
        ).fetchone()


def update_media_file(media_id: int, **kwargs) -> None:
    if not kwargs:
        return
    _validate_columns("media_files", list(kwargs.keys()))
    sets = ", ".join(f"{k}=?" for k in kwargs)
    values = list(kwargs.values()) + [media_id]
    with connect() as conn:
        conn.execute(
            f"UPDATE media_files SET {sets}, updated_at=datetime('now') WHERE id=?",
            values
        )


def delete_media_file(media_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM media_files WHERE id=?", (media_id,))


def reset_media_to_pending(media_ids: list[int]) -> int:
    """Reset a list of media file IDs back to 'pending' so they can be re-identified."""
    if not media_ids:
        return 0
    placeholders = ",".join("?" * len(media_ids))
    reset_fields = {
        "status": "pending",
        "phase": 0,
        "confidence": None,
        "tmdb_id": None,
        "imdb_id": None,
        "confirmed_title": None,
        "confirmed_year": None,
        "confirmed_type": None,
        "season": None,
        "episode": None,
        "episode_title": None,
        "genres": None,
        "plot": None,
        "rating": None,
        "director": None,
        "cast": None,
        "air_date": None,
        "notes": None,
    }
    sets = ", ".join(f"{k}=?" for k in reset_fields)
    values = list(reset_fields.values()) + media_ids
    with connect() as conn:
        conn.execute(
            f"UPDATE media_files SET {sets}, updated_at=datetime('now') WHERE id IN ({placeholders})",
            values
        )
    return len(media_ids)


def get_all_media_paths() -> list[sqlite3.Row]:
    """Return id + original_path for every media file in the DB."""
    with connect() as conn:
        return conn.execute("SELECT id, original_path FROM media_files").fetchall()


def get_media_by_status(status: str | list) -> list[sqlite3.Row]:
    with connect() as conn:
        if isinstance(status, list):
            placeholders = ",".join("?" * len(status))
            return conn.execute(
                f"SELECT * FROM media_files WHERE status IN ({placeholders}) ORDER BY id",
                status
            ).fetchall()
        return conn.execute(
            "SELECT * FROM media_files WHERE status=? ORDER BY id", (status,)
        ).fetchall()


def get_media_by_phase(phase: int) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM media_files WHERE phase=? ORDER BY id", (phase,)
        ).fetchall()


def count_by_status() -> dict[str, int]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) as n FROM media_files GROUP BY status"
        ).fetchall()
        return {r["status"]: r["n"] for r in rows}


def count_by_phase() -> dict[int, int]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT phase, COUNT(*) as n FROM media_files GROUP BY phase"
        ).fetchall()
        return {r["phase"]: r["n"] for r in rows}


# ─── subtitle_files helpers ───────────────────────────────────────────────────

def upsert_subtitle_file(data: dict) -> int:
    fields = list(data.keys())
    _validate_columns("subtitle_files", fields)
    placeholders = ", ".join(["?"] * len(fields))
    col_list = ", ".join(fields)
    updates = ", ".join(f"{f}=excluded.{f}" for f in fields if f != "original_path")
    sql = (
        f"INSERT INTO subtitle_files({col_list}) VALUES({placeholders}) "
        f"ON CONFLICT(original_path) DO UPDATE SET {updates}, "
        f"updated_at=datetime('now')"
    )
    with connect() as conn:
        cur = conn.execute(sql, list(data.values()))
        if cur.lastrowid:
            return cur.lastrowid
        row = conn.execute(
            "SELECT id FROM subtitle_files WHERE original_path=?",
            (data["original_path"],)
        ).fetchone()
        return row["id"]


def get_subtitles_for_media(media_id: int) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM subtitle_files WHERE parent_media_id=?", (media_id,)
        ).fetchall()


def get_unlinked_subtitles() -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM subtitle_files WHERE parent_media_id IS NULL"
        ).fetchall()


def update_subtitle_file_by_id(subtitle_id: int, **kwargs) -> None:
    if not kwargs:
        return
    _validate_columns("subtitle_files", list(kwargs.keys()))
    sets = ", ".join(f"{k}=?" for k in kwargs)
    values = list(kwargs.values()) + [subtitle_id]
    with connect() as conn:
        conn.execute(
            f"UPDATE subtitle_files SET {sets}, updated_at=datetime('now') WHERE id=?",
            values
        )


# ─── apply_manifest helpers ───────────────────────────────────────────────────

def log_manifest_op(media_id: Optional[int], file_type: str,
                    original_path: str, new_path: str, operation: str,
                    verified: bool = False) -> int:
    with connect() as conn:
        cur = conn.execute(
            "INSERT INTO apply_manifest(media_id, file_type, original_path, "
            "new_path, operation, verified) VALUES(?,?,?,?,?,?)",
            (media_id, file_type, original_path, new_path, operation, int(verified))
        )
        return cur.lastrowid


def mark_manifest_verified(manifest_id: int) -> None:
    with connect() as conn:
        conn.execute(
            "UPDATE apply_manifest SET verified=1 WHERE id=?", (manifest_id,)
        )


def get_manifest_ops(rolled_back: bool = False) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM apply_manifest WHERE rolled_back=? ORDER BY id",
            (int(rolled_back),)
        ).fetchall()


def mark_manifest_rolled_back(manifest_id: int) -> None:
    with connect() as conn:
        conn.execute(
            "UPDATE apply_manifest SET rolled_back=1, rolled_back_at=datetime('now') "
            "WHERE id=?", (manifest_id,)
        )


# ─── rate_limit_state helpers ─────────────────────────────────────────────────

def init_rate_limit(api: str) -> None:
    with connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO rate_limit_state(api) VALUES(?)", (api,)
        )


def get_rate_limit(api: str) -> Optional[sqlite3.Row]:
    """Fetch rate limit state, resetting if a new day has started."""
    today = datetime.date.today().isoformat()
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM rate_limit_state WHERE api=?", (api,)
        ).fetchone()
        if row and row["reset_date"] != today:
            conn.execute(
                "UPDATE rate_limit_state SET requests_today=0, reset_date=?, paused=0, pause_reason=NULL "
                "WHERE api=?",
                (today, api)
            )
            row = conn.execute(
                "SELECT * FROM rate_limit_state WHERE api=?", (api,)
            ).fetchone()
        return row



def increment_request_count(api: str) -> int:
    """Increment counter, reset if new day. Returns new count."""
    today = datetime.date.today().isoformat()
    with connect() as conn:
        row = conn.execute(
            "SELECT requests_today, reset_date FROM rate_limit_state WHERE api=?",
            (api,)
        ).fetchone()
        if not row or row["reset_date"] != today:
            conn.execute(
                "UPDATE rate_limit_state SET requests_today=1, reset_date=?, paused=0 "
                "WHERE api=?",
                (today, api)
            )
            return 1
        new_count = row["requests_today"] + 1
        conn.execute(
            "UPDATE rate_limit_state SET requests_today=? WHERE api=?",
            (new_count, api)
        )
        return new_count


def set_api_paused(api: str, paused: bool, reason: str = "") -> None:
    with connect() as conn:
        conn.execute(
            "UPDATE rate_limit_state SET paused=?, pause_reason=? WHERE api=?",
            (int(paused), reason, api)
        )


def is_api_paused(api: str) -> bool:
    row = get_rate_limit(api)
    return bool(row and row["paused"])


# ─── duplicate helpers ────────────────────────────────────────────────────────

def add_duplicate_group(tmdb_id: int, media_ids: list[int]) -> int:
    with connect() as conn:
        cur = conn.execute(
            "INSERT INTO duplicates(tmdb_id, media_ids) VALUES(?,?)",
            (tmdb_id, json.dumps(media_ids))
        )
        return cur.lastrowid


def get_pending_duplicates() -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM duplicates WHERE resolution='pending'"
        ).fetchall()


def resolve_duplicate(dup_id: int, resolution: str) -> None:
    with connect() as conn:
        conn.execute(
            "UPDATE duplicates SET resolution=? WHERE id=?",
            (resolution, dup_id)
        )


# ─── subtitle_queue helpers ───────────────────────────────────────────────────

def insert_subtitle_queue(data: dict) -> int:
    """Insert a new subtitle queue entry. Returns the row id."""
    fields = list(data.keys())
    _validate_columns("subtitle_queue", fields)
    placeholders = ", ".join(["?"] * len(fields))
    col_list = ", ".join(fields)
    sql = f"INSERT INTO subtitle_queue({col_list}) VALUES({placeholders})"
    with connect() as conn:
        cur = conn.execute(sql, list(data.values()))
        return cur.lastrowid


def get_subtitle_queue_for_media(media_id: int,
                                  status: str | None = None) -> list[sqlite3.Row]:
    with connect() as conn:
        if status:
            return conn.execute(
                "SELECT * FROM subtitle_queue WHERE media_id=? AND status=? ORDER BY score DESC",
                (media_id, status)
            ).fetchall()
        return conn.execute(
            "SELECT * FROM subtitle_queue WHERE media_id=? ORDER BY score DESC",
            (media_id,)
        ).fetchall()


def get_queued_languages(media_id: int) -> set[str]:
    """Languages that already have a queued/applied subtitle for this media."""
    with connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT language FROM subtitle_queue WHERE media_id=? "
            "AND status IN ('queued','downloading','embedded','sidecar')",
            (media_id,)
        ).fetchall()
    return {r["language"] for r in rows}


def update_subtitle_queue(queue_id: int, **kwargs) -> None:
    if not kwargs:
        return
    _validate_columns("subtitle_queue", list(kwargs.keys()))
    sets = ", ".join(f"{k}=?" for k in kwargs)
    values = list(kwargs.values()) + [queue_id]
    with connect() as conn:
        conn.execute(
            f"UPDATE subtitle_queue SET {sets}, updated_at=datetime('now') WHERE id=?",
            values
        )


def get_best_subtitle_per_language(media_id: int) -> list[sqlite3.Row]:
    """Return the highest-scoring queued entry per language for a media file."""
    with connect() as conn:
        return conn.execute(
            """
            SELECT * FROM subtitle_queue
            WHERE media_id=? AND status='queued'
              AND id IN (
                SELECT id FROM subtitle_queue sq2
                WHERE sq2.media_id=subtitle_queue.media_id
                  AND sq2.language=subtitle_queue.language
                  AND sq2.status='queued'
                ORDER BY score DESC LIMIT 1
              )
            ORDER BY language
            """,
            (media_id,)
        ).fetchall()


def init_rate_limit_subtitle() -> None:
    """Initialise rate-limit rows for subtitle providers."""
    for api in ("opensubtitles", "subdl", "podnapisi"):
        init_rate_limit(api)
