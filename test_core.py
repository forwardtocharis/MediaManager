"""
test_core.py — Smoke tests for the core utilities.

Run: python test_core.py
"""

import sys
import json
import tempfile
from pathlib import Path

# ── File utils ────────────────────────────────────────────────────────────────

from src.utils.file_utils import (
    sanitize_filename, decade_folder, build_movie_path,
    build_tv_episode_path, build_subtitle_path, build_nfo_path,
)

def test_sanitize():
    assert sanitize_filename("Batman: The Movie") == "Batman - The Movie"
    assert sanitize_filename('File "name" with?stuff') == "File name withstuff"
    assert sanitize_filename("Trailing dot.") == "Trailing dot"
    assert sanitize_filename("A  B") == "A B"
    print("  OK sanitize_filename")

def test_decade():
    assert decade_folder(1984) == "1980s"
    assert decade_folder(2000) == "2000s"
    assert decade_folder(2023) == "2020s"
    assert decade_folder(1999) == "1990s"
    print("  OK decade_folder")

def test_movie_path():
    p = build_movie_path("Z:/Movies", "The Dark Knight", 2008, ".mkv")
    assert p.parts[-1] == "The Dark Knight (2008).mkv"
    assert "2000s" in str(p)
    print("  OK build_movie_path")

def test_movie_path_colon():
    p = build_movie_path("Z:/Movies", "Batman: The Movie", 1966, ".mp4")
    assert "Batman - The Movie (1966)" in str(p)
    print("  OK build_movie_path with colon")

def test_tv_path():
    p = build_tv_episode_path(
        "Z:/TV", "Breaking Bad", 1, 1, "Pilot", ".mkv"
    )
    assert p.parts[-1] == "Breaking Bad - S01E01 - Pilot.mkv"
    assert "Season 01" in str(p)
    print("  OK build_tv_episode_path")

def test_tv_no_title():
    p = build_tv_episode_path("Z:/TV", "The Office", 2, 5, "", ".mkv")
    assert p.parts[-1] == "The Office - S02E05.mkv"
    print("  OK build_tv_episode_path without episode title")

def test_subtitle_path():
    video = Path("Z:/Movies/2000s/The Dark Knight (2008)/The Dark Knight (2008).mkv")
    sub = build_subtitle_path(video, "en", ".srt")
    assert sub.name == "The Dark Knight (2008).en.srt"
    print("  OK build_subtitle_path")

# ── Filename parser ───────────────────────────────────────────────────────────

from src.utils.filename_parser import parse_filename, is_extra, detect_subtitle_language

def test_parse_movie():
    r = parse_filename("The.Dark.Knight.2008.1080p.BluRay.mkv")
    assert r["title"].lower() == "the dark knight"
    assert r["year"] == 2008
    assert r["media_type"] in ("movie", "unknown")
    assert r["extra"] is False
    print("  OK parse_filename (movie)")

def test_parse_tv():
    r = parse_filename("Breaking.Bad.S01E01.Pilot.mkv")
    assert "breaking bad" in r["title"].lower()
    assert r["season"] == 1
    assert r["episode"] == 1
    assert r["media_type"] == "tv"
    print("  OK parse_filename (TV)")

def test_is_extra():
    assert is_extra("The.Dark.Knight.Behind.The.Scenes.mkv") is True
    assert is_extra("The.Dark.Knight.Featurette.mkv") is True
    assert is_extra("The.Dark.Knight.2008.mkv") is False
    print("  OK is_extra")

def test_subtitle_lang():
    assert detect_subtitle_language("Movie.en.srt") == "en"
    assert detect_subtitle_language("Movie.eng.srt") == "en"
    assert detect_subtitle_language("Movie.es.srt") == "es"
    assert detect_subtitle_language("Movie.srt") is None
    print("  OK detect_subtitle_language")

# ── NFO writer ────────────────────────────────────────────────────────────────

from src.utils.nfo_writer import write_movie_nfo, write_episode_nfo
import xml.etree.ElementTree as ET

def test_movie_nfo():
    with tempfile.TemporaryDirectory() as tmp:
        nfo = Path(tmp) / "test.nfo"
        write_movie_nfo(nfo, {
            "title": "The Dark Knight",
            "year": 2008,
            "plot": "A great film.",
            "genres": ["Action", "Drama"],
            "rating": "9.0",
            "director": "Christopher Nolan",
            "cast": ["Christian Bale", "Heath Ledger"],
            "imdb_id": "tt0468569",
            "tmdb_id": 155,
        })
        assert nfo.exists()
        tree = ET.parse(nfo)
        root = tree.getroot()
        assert root.tag == "movie"
        assert root.find("title").text == "The Dark Knight"
        assert root.find("year").text == "2008"
    print("  OK write_movie_nfo")

def test_episode_nfo():
    with tempfile.TemporaryDirectory() as tmp:
        nfo = Path(tmp) / "ep.nfo"
        write_episode_nfo(nfo, {
            "episode_title": "Pilot",
            "season": 1,
            "episode": 1,
            "plot": "First episode.",
            "air_date": "2008-01-20",
        })
        assert nfo.exists()
        tree = ET.parse(nfo)
        root = tree.getroot()
        assert root.tag == "episodedetails"
        assert root.find("season").text == "1"
    print("  OK write_episode_nfo")

# ── DB layer ──────────────────────────────────────────────────────────────────

from src import db

def test_db():
    with tempfile.TemporaryDirectory() as tmp:
        db.init(Path(tmp) / "test.db")
        mid = db.upsert_media_file({
            "original_path": "/nas/movies/test.mkv",
            "filename": "test.mkv",
            "extension": ".mkv",
            "file_size": 1000,
            "guessed_title": "Test Movie",
            "guessed_year": 2020,
            "guessed_type": "movie",
            "status": "pending",
            "phase": 0,
        })
        assert mid > 0

        row = db.get_media_file(mid)
        assert row["guessed_title"] == "Test Movie"

        db.update_media_file(mid, status="identified", confidence=85.0)
        row = db.get_media_file(mid)
        assert row["status"] == "identified"
        assert row["confidence"] == 85.0

        counts = db.count_by_status()
        assert counts.get("identified", 0) == 1

    print("  OK DB layer (upsert, get, update, count)")


# ── Run all ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_sanitize,
        test_decade,
        test_movie_path,
        test_movie_path_colon,
        test_tv_path,
        test_tv_no_title,
        test_subtitle_path,
        test_parse_movie,
        test_parse_tv,
        test_is_extra,
        test_subtitle_lang,
        test_movie_nfo,
        test_episode_nfo,
        test_db,
    ]

    passed = 0
    failed = 0
    print("\nRunning MediaManager smoke tests...\n")
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"  FAIL {t.__name__}: {e}")
            failed += 1

    print("\n" + "-"*40)
    print(f"  {passed} passed  |  {failed} failed")
    print("-"*40 + "\n")
    sys.exit(0 if failed == 0 else 1)
