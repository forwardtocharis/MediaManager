import unittest
from app import app
import json

class TestApp(unittest.TestCase):
    def setUp(self):
        import tempfile
        from pathlib import Path
        from src import db
        import app as mediamanager_app
        
        self.tmp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.tmp_dir.name) / "test_app.db"
        db.init(db_path)
        
        # Mock config
        mediamanager_app._config = {
            "api": {"tmdb_key": "test", "omdb_key": "test"},
            "database": {"path": str(db_path)},
            "llm": {"provider": "ollama"}
        }
        
        app.config['TESTING'] = True
        self.client = app.test_client()

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_dashboard_route(self):
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

    def test_api_status(self):
        response = self.client.get('/api/status')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('status_counts', data)

    def test_api_config(self):
        response = self.client.get('/api/config')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('api', data)

    def test_api_files_pagination(self):
        response = self.client.get('/api/files?page=1&per_page=10')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('files', data)
        self.assertIn('total', data)

    def test_api_llm_providers(self):
        response = self.client.get('/api/llm/providers')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('providers', data)

    def test_api_folders_delete_security(self):
        import app as mediamanager_app
        from pathlib import Path
        import shutil

        # Set up mock config for source paths
        src_dir = Path(self.tmp_dir.name) / "src_test"
        movies_dir = src_dir / "movies"
        tv_dir = src_dir / "tv"
        movies_dir.mkdir(parents=True, exist_ok=True)
        tv_dir.mkdir(parents=True, exist_ok=True)

        mediamanager_app._config["source"] = {
            "movies_path": str(movies_dir),
            "tv_path": str(tv_dir)
        }

        # Valid subdirectories
        valid_movie_sub = movies_dir / "MovieToDel"
        valid_movie_sub.mkdir()
        valid_tv_sub = tv_dir / "TVToDel"
        valid_tv_sub.mkdir()

        # External directory (should not be allowed)
        external_dir = Path(self.tmp_dir.name) / "external_secrets"
        external_dir.mkdir()

        paths_to_delete = [
            str(valid_movie_sub),
            str(valid_tv_sub),
            str(external_dir),       # Invalid: outside source dirs
            str(movies_dir),         # Invalid: is the source dir itself
            str(movies_dir / "NonExistent") # Invalid: not a directory / doesn't exist
        ]

        response = self.client.post('/api/folders/delete', json={"paths": paths_to_delete})
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)

        deleted = data.get("deleted", [])
        errors = data.get("errors", [])

        # The valid subdirectories should be deleted
        self.assertIn(str(valid_movie_sub), deleted)
        self.assertIn(str(valid_tv_sub), deleted)
        self.assertFalse(valid_movie_sub.exists())
        self.assertFalse(valid_tv_sub.exists())

        # The external directory should NOT be deleted
        self.assertTrue(external_dir.exists())
        self.assertTrue(movies_dir.exists())

        # Check errors
        error_paths = [e["path"] for e in errors]
        self.assertIn(str(external_dir), error_paths)
        self.assertIn(str(movies_dir), error_paths)
        self.assertIn(str(movies_dir / "NonExistent"), error_paths)

if __name__ == '__main__':
    unittest.main()
