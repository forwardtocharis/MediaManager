# MediaManager

A multi-phase CLI tool for identifying, renaming, and tagging media files (movies, TV shows) stored on a NAS or network share.

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure

Edit `config.yml`:

```yaml
api:
  tmdb_key: "your_tmdb_key_here"   # https://www.themoviedb.org/settings/api
  omdb_key: "your_omdb_key_here"   # https://www.omdbapi.com/apikey.aspx

source:
  movies_path: "/path/to/movies"
  tv_path:     "/path/to/tvshows"
```

### 3. Run the pipeline

```bash
# Step 0: Scan your NAS (read-only, no changes)
python cli.py scan

# Check status
python cli.py status

# Step 1: Auto-identify (TMDB + OMDB)
python cli.py identify

# Check status again
python cli.py status

# Step 2a: Export unresolved files for LLM review
python cli.py export-llm --output llm_review.csv
# → Opens llm_review.csv and llm_review_prompt.txt
# → Paste the prompt into ChatGPT/Claude/Gemini, attach the CSV, get back a filled CSV

# Step 2b: Import LLM results
python cli.py import-llm --input llm_results.csv

# Step 2c: Export remaining manual review items
python cli.py export-manual --output manual.csv
# → Edit manual.csv yourself, fill in tmdb_id/confirmed_title/year/type/season/episode
python cli.py import-manual --input manual.csv

# Step 3: Preview proposed changes (SAFE — no writes)
python cli.py apply --phase 1 --dry-run

# Step 3: Apply changes for Phase 1 items (moves files!)
python cli.py apply --phase 1 --no-dry-run

# Step 3: Apply all phases at once
python cli.py apply --phase all --no-dry-run
```

---

## All Commands

| Command | Description |
|---|---|
| `scan` | Walk NAS paths and scan all media files into the database |
| `status` | Show pipeline status by phase and API rate limit state |
| `identify [--limit N]` | Phase 1 auto-identification via TMDB/OMDB |
| `export-llm [--output FILE]` | Export unresolved files as CSV + LLM prompt |
| `import-llm --input FILE` | Import LLM-completed CSV, re-validate against TMDB |
| `export-manual [--output FILE]` | Export items needing human review |
| `import-manual --input FILE` | Import manually-completed CSV |
| `apply --phase 1\|2\|manual\|all [--dry-run] [--limit N]` | Apply renames/NFOs (dry-run by default) |
| `rollback [--dry-run]` | Reverse all applied file moves |
| `duplicates` | Interactive duplicate resolution |
| `resume-api --api tmdb\|omdb` | Unpause a rate-limited API |
| `cancel-api --api tmdb\|omdb` | Manually pause an API |

---

## Output Structure

### Movies
```
Movies/
  1980s/
    The Thing (1982)/
      The Thing (1982).mkv
      The Thing (1982).nfo
      The Thing (1982).en.srt
  2000s/
    The Dark Knight (2008)/
      The Dark Knight (2008).mkv
      The Dark Knight (2008).nfo
```

### TV Shows
```
TV Shows/
  Breaking Bad/
    tvshow.nfo
    Season 01/
      Breaking Bad - S01E01 - Pilot.mkv
      Breaking Bad - S01E01 - Pilot.nfo
```

### Extras/Bonus Content
```
The Thing (1982)/
  The Thing (1982).mkv
  The Thing (1982).Behind The Scenes.mkv
```

---

## Safety

- **`apply` is dry-run by default** — always shows a preview table first.
- Files are **copied and size-verified** before the original is deleted.
- Every operation is logged to `apply_manifest` in the database.
- **Rollback** reverses all moves using the manifest.

---

## API Rate Limits

- **TMDB**: ~40 requests/second (conservative). No daily limit enforced by default.
- **OMDB**: 2 req/sec. Daily limit set to 950 (free tier is 1,000/day).
- When a limit is hit, the API is **paused automatically** with a message.
- To resume: `python cli.py resume-api --api tmdb`

---

## LLM CSV Format

The exported CSV includes these columns for the LLM to fill in:

| Column | Fill in |
|---|---|
| `tmdb_id` | TMDB numeric ID (search at themoviedb.org) |
| `imdb_id` | IMDb ID (tt1234567) |
| `confirmed_title` | Correct full title |
| `confirmed_year` | Release year |
| `confirmed_type` | `movie` or `tv` |
| `season` | Season number (TV only) |
| `episode` | Episode number (TV only) |
| `skip` | Set to `1` if truly unidentifiable |

---

## Config Reference

See `config.yml` for all settings with inline documentation.

Key settings:
- `identification.auto_confirm_threshold` (default 75) — minimum score to auto-confirm
- `identification.llm_threshold` (default 50) — minimum score to send to LLM vs manual
- `files.media_extensions` — list of extensions to scan
- `files.extras_keywords` — keywords that flag bonus content

---

## Running Tests

```bash
python test_core.py
```

All 14 tests cover: filename sanitization, decade folders, path building, guessit parsing, extras detection, subtitle language detection, NFO XML generation, and the DB layer.
