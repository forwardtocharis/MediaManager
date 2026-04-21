"""
tests/test_identifier_llm.py — Tests for identifier scoring, llm batching,
and llm_export CSV round-trip.
"""

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src import db


# ─── Identifier scoring ───────────────────────────────────────────────────────

class TestIdentifierScoring(unittest.TestCase):
    def setUp(self):
        from src.identifier import _score_tmdb_movie, _score_tmdb_tv, _year_score
        self._score_movie = _score_tmdb_movie
        self._score_tv = _score_tmdb_tv
        self._year_score = _year_score

    def test_perfect_movie_score(self):
        result = {"title": "The Dark Knight", "release_date": "2008-07-18"}
        score = self._score_movie(result, "The Dark Knight", 2008, "movie")
        # title=40, year=20, type=20 → 80
        self.assertAlmostEqual(score, 80.0, places=0)

    def test_movie_score_wrong_year(self):
        result = {"title": "The Dark Knight", "release_date": "2005-01-01"}
        score = self._score_movie(result, "The Dark Knight", 2008, "movie")
        # title=40, year=0 (off by 3), type=20 → 60
        self.assertAlmostEqual(score, 60.0, places=0)

    def test_movie_score_off_by_one_year(self):
        result = {"title": "The Dark Knight", "release_date": "2007-01-01"}
        score = self._score_movie(result, "The Dark Knight", 2008, "movie")
        # title=40, year=10 (off by 1), type=20 → 70
        self.assertAlmostEqual(score, 70.0, places=0)

    def test_movie_score_title_mismatch(self):
        result = {"title": "Batman Begins", "release_date": "2008-01-01"}
        score = self._score_movie(result, "The Dark Knight", 2008, "movie")
        # title < 40, year=20, type=20 — should be substantially less than 80
        self.assertLess(score, 75.0)

    def test_tv_perfect_score(self):
        result = {"name": "Breaking Bad", "first_air_date": "2008-01-20"}
        score = self._score_tv(result, "Breaking Bad", 2008, "tv")
        self.assertAlmostEqual(score, 80.0, places=0)

    def test_tv_wrong_type_still_matches(self):
        result = {"name": "Breaking Bad", "first_air_date": "2008-01-20"}
        # type_guess = "unknown" still awards W_TYPE
        score = self._score_tv(result, "Breaking Bad", 2008, "unknown")
        self.assertAlmostEqual(score, 80.0, places=0)

    def test_year_score_exact(self):
        self.assertEqual(self._year_score(2008, 2008), 20)

    def test_year_score_off_by_one(self):
        self.assertEqual(self._year_score(2008, 2009), 10)

    def test_year_score_no_year(self):
        self.assertEqual(self._year_score(None, 2008), 10)  # neutral = W_YEAR * 0.5

    def test_year_score_mismatch(self):
        self.assertEqual(self._year_score(2000, 2008), 0)


# ─── LLM batch processing ─────────────────────────────────────────────────────

class TestLLMBatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        db.init(Path(cls.tmp.name) / "test.db")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def _make_row(self, media_id: int, filename: str) -> dict:
        db.upsert_media_file({
            "original_path": f"/nas/{filename}",
            "filename": filename,
            "extension": ".mkv",
            "file_size": 1000,
            "guessed_title": filename.replace(".mkv", ""),
            "guessed_year": 2008,
            "guessed_type": "movie",
            "status": "needs_llm",
            "phase": 0,
        })
        return dict(db.get_media_by_path(f"/nas/{filename}"))

    def test_build_batch_input(self):
        from src.llm import _build_batch_input
        row = self._make_row(1, "inception.mkv")
        batch = _build_batch_input([row])
        self.assertEqual(len(batch), 1)
        self.assertIn("filename", batch[0])
        self.assertIn("id", batch[0])

    @patch("openai.OpenAI")
    def test_run_llm_pass_confirmed(self, mock_openai_cls):
        from src.llm import run_llm_pass

        row = self._make_row(2, "the.dark.knight.mkv")

        llm_response = json.dumps([{
            "id": row["id"],
            "confirmed_title": "The Dark Knight",
            "confirmed_year": 2008,
            "confirmed_type": "movie",
            "tmdb_id": None,
            "skip": False,
        }])
        mock_choice = MagicMock()
        mock_choice.message.content = llm_response
        mock_resp = MagicMock()
        mock_resp.choices = [mock_choice]
        mock_openai_cls.return_value.chat.completions.create.return_value = mock_resp

        stats = run_llm_pass(
            files=[row], provider="ollama", model="llama3",
            endpoint="", api_key="", batch_size=20, tmdb=None)

        self.assertEqual(stats["confirmed"], 1)
        self.assertEqual(stats["errors"], 0)

    @patch("openai.OpenAI")
    def test_run_llm_pass_skip(self, mock_openai_cls):
        from src.llm import run_llm_pass

        row = self._make_row(3, "unknown_file.mkv")

        llm_response = json.dumps([{
            "id": row["id"],
            "skip": True,
        }])
        mock_choice = MagicMock()
        mock_choice.message.content = llm_response
        mock_resp = MagicMock()
        mock_resp.choices = [mock_choice]
        mock_openai_cls.return_value.chat.completions.create.return_value = mock_resp

        stats = run_llm_pass(
            files=[row], provider="ollama", model="llama3",
            endpoint="", api_key="", batch_size=20, tmdb=None)

        self.assertEqual(stats["skipped"], 1)
        updated = db.get_media_file(row["id"])
        self.assertEqual(updated["status"], "needs_manual")

    def test_custom_provider_no_key_raises(self):
        from src.llm import run_llm_pass
        with self.assertRaises(ValueError, msg="Custom LLM endpoint requires an API key."):
            run_llm_pass(
                files=[], provider="custom", model="x",
                endpoint="http://example.com/v1", api_key="",
                batch_size=20)


# ─── LLM export CSV round-trip ────────────────────────────────────────────────

class TestLLMExportRoundTrip(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        db.init(Path(cls.tmp.name) / "test.db")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def _seed_needs_llm(self, filename: str) -> int:
        db.upsert_media_file({
            "original_path": f"/nas/{filename}",
            "filename": filename,
            "extension": ".mkv",
            "file_size": 5_000_000,
            "guessed_title": "Inception",
            "guessed_year": 2010,
            "guessed_type": "movie",
            "status": "needs_llm",
            "phase": 0,
        })
        return db.get_media_by_path(f"/nas/{filename}")["id"]

    def test_export_creates_csv_and_prompt(self):
        from src.llm_export import export_for_llm
        self._seed_needs_llm("inception.mkv")
        out_csv = Path(self.tmp.name) / "llm.csv"
        out_prompt = Path(self.tmp.name) / "llm_prompt.txt"
        count = export_for_llm(out_csv, out_prompt)
        self.assertGreater(count, 0)
        self.assertTrue(out_csv.exists())
        self.assertTrue(out_prompt.exists())
        with open(out_csv) as f:
            rows = list(csv.DictReader(f))
        self.assertTrue(any(r["guessed_title"] == "Inception" for r in rows))

    def test_import_llm_confirms_file(self):
        from src.llm_export import export_for_llm, import_from_llm
        media_id = self._seed_needs_llm("parasite.mkv")
        out_csv = Path(self.tmp.name) / "llm2.csv"
        out_prompt = Path(self.tmp.name) / "llm2_prompt.txt"
        export_for_llm(out_csv, out_prompt)

        # Simulate LLM filling in the CSV
        rows = []
        with open(out_csv) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                if int(row["id"]) == media_id:
                    row["confirmed_title"] = "Parasite"
                    row["confirmed_year"] = "2019"
                    row["confirmed_type"] = "movie"
                    row["tmdb_id"] = "496243"
                rows.append(row)

        filled_csv = Path(self.tmp.name) / "llm2_filled.csv"
        with open(filled_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        stats = import_from_llm(filled_csv, tmdb=None, validate=False)
        self.assertGreater(stats["confirmed"], 0)
        updated = db.get_media_file(media_id)
        self.assertEqual(updated["status"], "identified")
        self.assertEqual(updated["phase"], 2)

    def test_import_manual_sets_phase_3(self):
        from src.llm_export import import_from_manual
        media_id = self._seed_needs_llm("oldboy.mkv")

        # Write a minimal manual CSV directly
        manual_csv = Path(self.tmp.name) / "manual.csv"
        fieldnames = [
            "id", "original_path", "filename", "parent_folder",
            "guessed_title", "guessed_year", "guessed_type",
            "guessed_season", "guessed_episode", "is_extra",
            "file_size_mb", "confidence", "notes",
            "tmdb_id", "imdb_id", "confirmed_title", "confirmed_year",
            "confirmed_type", "season", "episode", "skip",
        ]
        with open(manual_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({
                "id": media_id, "original_path": "/nas/oldboy.mkv",
                "filename": "oldboy.mkv", "parent_folder": "nas",
                "guessed_title": "Oldboy", "guessed_year": 2003,
                "guessed_type": "movie", "guessed_season": "", "guessed_episode": "",
                "is_extra": 0, "file_size_mb": "4.8", "confidence": "0.0", "notes": "",
                "tmdb_id": "6972", "imdb_id": "tt0364569",
                "confirmed_title": "Oldboy", "confirmed_year": "2003",
                "confirmed_type": "movie", "season": "", "episode": "", "skip": "",
            })

        stats = import_from_manual(manual_csv, tmdb=None)
        self.assertGreater(stats["confirmed"], 0)
        updated = db.get_media_file(media_id)
        self.assertEqual(updated["status"], "identified")
        self.assertEqual(updated["phase"], 3)


if __name__ == "__main__":
    unittest.main()
