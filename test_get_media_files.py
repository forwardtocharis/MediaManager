import tempfile
import os
from src import db

with tempfile.NamedTemporaryFile(delete=False) as tmp:
    db_path = tmp.name

db.init(db_path)

with db.connect() as conn:
    conn.execute(
        "INSERT INTO media_files (id, filename, file_size, original_path, extension) VALUES (?, ?, ?, ?, ?)",
        (1, "file_1.mp4", 1024, "/path/to/file_1.mp4", "mp4")
    )
    conn.execute(
        "INSERT INTO media_files (id, filename, file_size, original_path, extension) VALUES (?, ?, ?, ?, ?)",
        (2, "file_2.mp4", 1024, "/path/to/file_2.mp4", "mp4")
    )

def get_media_files(media_ids: list[int]):
    if not media_ids:
        return []
    with db.connect() as conn:
        placeholders = ",".join("?" for _ in media_ids)
        query = f"SELECT * FROM media_files WHERE id IN ({placeholders})"
        return conn.execute(query, media_ids).fetchall()

def test_get_media_files():
    files = get_media_files([1, 2])
    print(list(dict(f) for f in files))

if __name__ == "__main__":
    test_get_media_files()

os.remove(db_path)
