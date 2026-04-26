import time
import json
import sqlite3
import os
import tempfile
from app import app
from src import db

# Setup test data
with tempfile.NamedTemporaryFile(delete=False) as tmp:
    db_path = tmp.name

db.init(db_path)

with db.connect() as conn:
    # Create tables if not exists
    # The app actually relies on db.init() to create tables if they don't exist
    pass

with db.connect() as conn:
    # Clear existing data
    conn.execute("DELETE FROM duplicates")
    conn.execute("DELETE FROM media_files")

    # Insert media files
    for i in range(1000):
        conn.execute(
            "INSERT INTO media_files (id, filename, file_size, original_path, extension) VALUES (?, ?, ?, ?, ?)",
            (i, f"file_{i}.mp4", 1024, f"/path/to/file_{i}.mp4", "mp4")
        )

    # Insert duplicates
    for i in range(100):
        conn.execute(
            "INSERT INTO duplicates (id, tmdb_id, media_ids, resolution) VALUES (?, ?, ?, ?)",
            (i, 1000 + i, json.dumps([i * 10 + j for j in range(10)]), "pending")
        )

# Benchmark
client = app.test_client()

start = time.time()
for _ in range(10):
    response = client.get('/api/duplicates')
    assert response.status_code == 200
end = time.time()

print(f"Time taken for 10 requests: {end - start:.4f} seconds")

# Clean up
os.remove(db_path)
