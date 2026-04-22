# MediaManager

![MediaManager Dashboard](file:///C:/Users/forwa/.gemini/antigravity/brain/9bbf37be-a76a-4dbf-9d7d-3f9fed870dbb/mediamanager_dashboard_mockup_1776858043321.png)

A modern, multi-phase media automation suite designed for NAS power users. MediaManager combines a powerful **Web Dashboard** for identification and review with a **Hybrid CLI** for high-performance local file operations.

---

## ✨ Features

- **Modern Web Dashboard**: Real-time pipeline monitoring, interactive duplicate resolution, and visual confirmation of metadata matches.
- **Smart Identification**: Multi-stage auto-discovery using TMDB and OMDB APIs with score-based confidence thresholds.
- **AI-Powered Review**: Integrated LLM support (Ollama, LM Studio, or OpenAI) to resolve complex filenames that confuse traditional scrapers.
- **Hybrid NAS Workflow**: Run the UI on your PC while executing "Apply" and "Scan" tasks natively on your NAS via SSH — **0% network traffic, 100% disk speed.**
- **Robust Safety**: Copy-verify-delete logic with a full manifest for atomic rollbacks.

---

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Launch the Web UI
```bash
python app.py
```
Open **[http://localhost:5000](http://localhost:5000)** to configure your paths and API keys directly in the browser.

---

## 🛠 Hybrid NAS Optimization

For users with media on a networked NAS (Synology, TrueNAS, etc.), MediaManager offers a unique hybrid mode:

1. **PC (Dashboard)**: Run the server on your workstation. Use the web interface to identify movies and confirm changes.
2. **NAS (Engine)**: Enable **SSH Apply** in Settings. The NAS will execute all renames and moves locally on its own disks.

### Benefits:
- **Instant Moves**: Moves files across the same volume in milliseconds.
- **Zero Latency**: No large files travel over your Wi-Fi or Ethernet.
- **Reliable**: No network timeouts during long batch operations.

---

## 💻 CLI Usage

While the Dashboard is recommended for daily use, the CLI remains available for automation and headless environments:

| Command | Description |
|---|---|
| `scan` | Deep-scan all source paths into the local database |
| `identify` | Auto-match files against TMDB/OMDB |
| `apply --no-dry-run` | Commit renames and NFO generation |
| `rollback` | Reverse all moves using the transaction manifest |
| `duplicates` | Interactive terminal-based duplicate resolution |

---

## 📂 Output Convention

### Movies
```
Movies/
  1980s/
    The Thing (1982)/
      The Thing (1982).mkv
      The Thing (1982).nfo
      The Thing (1982).Behind The Scenes.mkv
```

### TV Shows
```
TV Shows/
  Breaking Bad/
    tvshow.nfo
    Season 01/
      Breaking Bad - S01E01 - Pilot.mkv
```

---

## 🛡 Safety & Reliability

- **Dry Run by Default**: All operations show a preview of changes before execution.
- **Verification**: Checksums and file size comparisons are performed before deleting original files.
- **Manifests**: Every move is logged in `mediamanager.db` for traceability.

---

## 🧪 Development

Run tests with:
```bash
python test_core.py
```
Includes 14+ coverage vectors for NFO generation, path logic, and scraper accuracy.
