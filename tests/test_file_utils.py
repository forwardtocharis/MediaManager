import unittest
from pathlib import Path
from src.utils.file_utils import build_movie_path

class TestFileUtils(unittest.TestCase):
    def test_build_movie_path_default(self):
        # Default should use decade folders
        p = build_movie_path("/base", "The Dark Knight", 2008, ".mkv")
        self.assertEqual(str(p), str(Path("/base/2000s/The Dark Knight (2008)/The Dark Knight (2008).mkv")))

    def test_build_movie_path_with_decade(self):
        p = build_movie_path("/base", "The Dark Knight", 2008, ".mkv", use_decade_folders=True)
        self.assertEqual(str(p), str(Path("/base/2000s/The Dark Knight (2008)/The Dark Knight (2008).mkv")))

    def test_build_movie_path_without_decade(self):
        p = build_movie_path("/base", "The Dark Knight", 2008, ".mkv", use_decade_folders=False)
        self.assertEqual(str(p), str(Path("/base/The Dark Knight (2008)/The Dark Knight (2008).mkv")))

    def test_build_movie_path_sanitization(self):
        # Colon should become ' - '
        p = build_movie_path("/base", "Batman: The Movie", 1966, ".mp4", use_decade_folders=False)
        self.assertEqual(str(p), str(Path("/base/Batman - The Movie (1966)/Batman - The Movie (1966).mp4")))

        # Illegal characters should be removed
        p = build_movie_path("/base", 'File "name" with?stuff', 2020, ".mkv", use_decade_folders=False)
        self.assertEqual(str(p), str(Path("/base/File name withstuff (2020)/File name withstuff (2020).mkv")))

    def test_build_movie_path_decades(self):
        self.assertIn("1980s", str(build_movie_path("/base", "Movie", 1984, ".mkv")))
        self.assertIn("1990s", str(build_movie_path("/base", "Movie", 1999, ".mkv")))
        self.assertIn("2000s", str(build_movie_path("/base", "Movie", 2000, ".mkv")))
        self.assertIn("2020s", str(build_movie_path("/base", "Movie", 2023, ".mkv")))

    def test_build_movie_path_truncation(self):
        long_title = "A" * 300
        p = build_movie_path("/base", long_title, 2024, ".mkv", use_decade_folders=False)
        # folder_name = sanitize_path_component(f"{safe_title} (2024)")
        # sanitize_path_component truncates to 200
        self.assertLessEqual(len(p.parent.name), 200)
        self.assertTrue(p.parent.name.endswith("(2024)"))

if __name__ == "__main__":
    unittest.main()
