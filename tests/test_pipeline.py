import unittest
import tempfile
import os
import sqlite3
from pathlib import Path
from src import db, scanner, applier
from src.utils.file_utils import build_movie_path

class TestPipeline(unittest.TestCase):
    def setUp(self):
        # Setup temporary directories for NAS simulation
        self.tmp_nas = tempfile.TemporaryDirectory()
        self.nas_path = Path(self.tmp_nas.name)
        self.movies_src = self.nas_path / "Movies_Source"
        self.movies_dst = self.nas_path / "Movies_Output"
        self.movies_src.mkdir()
        self.movies_dst.mkdir()

        # Create a fake movie file
        self.fake_movie = self.movies_src / "The.Batman.2022.mkv"
        self.fake_movie.write_text("dummy media content")
        
        # Create a fake subtitle
        self.fake_sub = self.movies_src / "The.Batman.2022.en.srt"
        self.fake_sub.write_text("dummy subtitle content")

        # Setup temporary DB
        self.tmp_db_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_db_dir.name) / "test_pipeline.db"
        db.init(self.db_path)

    def tearDown(self):
        self.tmp_nas.cleanup()
        self.tmp_db_dir.cleanup()

    def test_full_pipeline_flow(self):
        # 1. Scan Phase
        stats = scanner.scan(
            movies_path=str(self.movies_src),
            tv_path=None,
            media_extensions={".mkv"},
            subtitle_extensions={".srt"}
        )
        self.assertEqual(stats["media_new"], 1)
        self.assertEqual(stats["subtitles_new"], 1)

        # Link subtitles
        links = scanner.link_subtitles_to_media()
        self.assertEqual(links, 1)

        # 2. Identification Simulation (Skip real API, update DB directly)
        rows = db.get_media_by_status("pending")
        self.assertEqual(len(rows), 1)
        movie_id = rows[0]["id"]
        
        db.update_media_file(movie_id, 
            status="identified", 
            phase=1,
            confirmed_title="The Batman",
            confirmed_year=2022,
            confirmed_type="movie",
            confidence=100.0,
            tmdb_id=414906
        )

        # 3. Apply Phase
        # We'll use applier.run with dry_run=False
        apply_stats = applier.run(
            movies_output=str(self.movies_dst),
            tv_output=str(self.movies_dst), # not used in this test
            phases=["1"],
            dry_run=False
        )
        
        self.assertEqual(apply_stats["applied"], 1)

        # 4. Verify Files Moved
        expected_path = build_movie_path(str(self.movies_dst), "The Batman", 2022, ".mkv")
        self.assertTrue(expected_path.exists(), f"Expected movie at {expected_path}")
        self.assertTrue(expected_path.with_suffix(".nfo").exists(), "NFO should exist")
        
        # Check subtitle renamed and moved
        # build_subtitle_path logic: {video_name}.{lang}.{ext}
        expected_sub = expected_path.parent / (expected_path.stem + ".en.srt")
        self.assertTrue(expected_sub.exists(), f"Expected subtitle at {expected_sub}")

        # Check original is gone
        self.assertFalse(self.fake_movie.exists())

        # 5. Rollback Phase
        rollback_stats = applier.rollback_all(dry_run=False)
        self.assertGreaterEqual(rollback_stats["restored"], 1)
        
        # Verify original restored
        self.assertTrue(self.fake_movie.exists())
        self.assertTrue(self.fake_sub.exists())
        # Verify moved files are gone
        self.assertFalse(expected_path.exists())

if __name__ == '__main__':
    unittest.main()
