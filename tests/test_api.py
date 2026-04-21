import unittest
from unittest.mock import patch, MagicMock
from src.api.tmdb import TMDBClient, RateLimitPausedError
from src.api.omdb import OMDBClient
from src import db
import tempfile
from pathlib import Path

class TestAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.TemporaryDirectory()
        db_path = Path(cls.tmp_dir.name) / "test.db"
        db.init(db_path)
        db.init_rate_limit("tmdb")
        db.init_rate_limit("omdb")

    @classmethod
    def tearDownClass(cls):
        cls.tmp_dir.cleanup()

    @patch("src.api.tmdb.requests.get")
    def test_tmdb_search_movie(self, mock_get):
        # Mock successful response
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "results": [
                {"id": 155, "title": "The Dark Knight", "release_date": "2008-07-16"}
            ]
        }
        mock_get.return_value = mock_resp

        client = TMDBClient(api_key="fake_key")
        results = client.search_movie("The Dark Knight", 2008)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["title"], "The Dark Knight")
        self.assertEqual(results[0]["id"], 155)

    @patch("src.api.tmdb.requests.get")
    def test_tmdb_rate_limit_pause(self, mock_get):
        # Mock 429 response
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.headers = {"Retry-After": "10"}
        mock_get.return_value = mock_resp

        client = TMDBClient(api_key="fake_key")
        
        with self.assertRaises(RateLimitPausedError):
            client.search_movie("Any Movie")

        # Verify DB reflects pause
        state = db.get_rate_limit("tmdb")
        self.assertTrue(state["paused"])
        self.assertIn("HTTP 429", state["pause_reason"])

        # Cleanup: resume for other tests
        db.set_api_paused("tmdb", False)

    @patch("src.api.omdb.requests.get")
    def test_omdb_get_details(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "Title": "Inception",
            "Year": "2010",
            "imdbID": "tt1375666",
            "Type": "movie",
            "Response": "True"
        }
        mock_get.return_value = mock_resp

        client = OMDBClient(api_key="fake_key")
        result = client.get_details("tt1375666")

        self.assertIsNotNone(result)
        self.assertEqual(result["confirmed_title"], "Inception")
        self.assertNotIn("tmdb_id", result)

if __name__ == "__main__":
    unittest.main()
