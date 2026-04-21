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

if __name__ == '__main__':
    unittest.main()
