"""
llm.py — Unified LLM client for Ollama, LM Studio, OpenAI-compatible endpoints.

Both Ollama and LM Studio expose OpenAI-compatible chat completions APIs:
  Ollama:    http://localhost:11434/v1
  LM Studio: http://localhost:1234/v1

We use the `openai` Python package with a custom base_url for all of them.
"""

import json
import logging
import re
from typing import Callable, Optional

import requests

logger = logging.getLogger(__name__)

# ─── Provider metadata ────────────────────────────────────────────────────────

_PROVIDER_ENDPOINTS = {
    "ollama":    "http://localhost:11434/v1",
    "lmstudio":  "http://localhost:1234/v1",
    "openai":    "https://api.openai.com/v1",
}

_PROVIDER_DEFAULT_KEY = {
    "ollama":   "ollama",
    "lmstudio": "lm-studio",
}

_CLOUD_MODELS = {
    "openai": ["gpt-4o", "gpt-4o-mini", "gpt-3.5-turbo"],
}

# ─── System prompt ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a media identification assistant. Given a list of unidentified media files, identify each one.

Return ONLY a valid JSON array — no markdown, no explanation, no code fences. Each element must be a JSON object with:
- "id": the numeric id from input (required, do not change)
- "confirmed_title": exact official title (null if cannot identify)
- "confirmed_year": release year as integer (null if unknown)
- "confirmed_type": "movie" or "tv" (null if unknown)
- "tmdb_id": TMDB numeric ID if you know it with confidence (null otherwise)
- "season": season number for TV episodes as integer (null for movies or if unknown)
- "episode": episode number for TV episodes as integer (null for movies or if unknown)
- "skip": true ONLY if you genuinely cannot identify this file

Use the filename, parent folder name, year, and type hints as clues. Be confident when you know—do not skip obvious titles."""


# ─── Provider detection ───────────────────────────────────────────────────────

def detect_providers() -> list[dict]:
    """
    Attempt to connect to Ollama and LM Studio.
    Returns a list of provider dicts with available model lists.
    """
    providers = []

    # Ollama
    try:
        r = requests.get("http://localhost:11434/api/tags", timeout=2)
        if r.ok:
            models = [m["name"] for m in r.json().get("models", [])]
            providers.append({
                "id": "ollama", "label": "Ollama (Local)",
                "endpoint": _PROVIDER_ENDPOINTS["ollama"],
                "models": models, "available": True,
            })
        else:
            raise ConnectionError("not ok")
    except Exception:
        providers.append({
            "id": "ollama", "label": "Ollama (Local)",
            "endpoint": _PROVIDER_ENDPOINTS["ollama"],
            "models": [], "available": False,
        })

    # LM Studio
    try:
        r = requests.get("http://localhost:1234/v1/models", timeout=2)
        if r.ok:
            models = [m["id"] for m in r.json().get("data", [])]
            providers.append({
                "id": "lmstudio", "label": "LM Studio (Local)",
                "endpoint": _PROVIDER_ENDPOINTS["lmstudio"],
                "models": models, "available": True,
            })
        else:
            raise ConnectionError("not ok")
    except Exception:
        providers.append({
            "id": "lmstudio", "label": "LM Studio (Local)",
            "endpoint": _PROVIDER_ENDPOINTS["lmstudio"],
            "models": [], "available": False,
        })

    # Cloud providers (always listed, key validity checked separately)
    providers.append({
        "id": "openai", "label": "OpenAI (Cloud)",
        "endpoint": _PROVIDER_ENDPOINTS["openai"],
        "models": _CLOUD_MODELS["openai"], "available": True,
    })

    providers.append({
        "id": "custom", "label": "Custom Endpoint",
        "endpoint": "", "models": [], "available": True,
    })

    return providers


def test_connection(provider: str, model: str,
                    endpoint: str = "", api_key: str = "") -> dict:
    """Send a trivial request to verify connectivity. Returns {ok, message}."""
    try:
        from openai import OpenAI
        base_url = _resolve_endpoint(provider, endpoint)
        key = api_key or _PROVIDER_DEFAULT_KEY.get(provider, "sk-placeholder")
        client = OpenAI(base_url=base_url, api_key=key)
        resp = client.chat.completions.create(
            model=model or "llama3.2",
            messages=[{"role": "user", "content": 'Reply with the single word "connected".'}],
            max_tokens=10, temperature=0,
        )
        reply = resp.choices[0].message.content.strip()
        return {"ok": True, "message": f'Connected! Model replied: "{reply[:60]}"'}
    except Exception as e:
        return {"ok": False, "message": str(e)}


# ─── LLM batch pass ───────────────────────────────────────────────────────────

def run_llm_pass(
    files: list,
    provider: str,
    model: str,
    endpoint: str,
    api_key: str,
    batch_size: int = 20,
    tmdb=None,
    progress_cb: Optional[Callable] = None,
) -> dict:
    """
    Run LLM identification on a list of media_files DB rows.
    Re-validates results against TMDB if a client is provided.
    Calls progress_cb(current, total, filename, result_status) after each file.
    Returns stats dict: {confirmed, skipped, errors, processed}.
    """
    from openai import OpenAI
    from src import db

    base_url = _resolve_endpoint(provider, endpoint)
    key = api_key or _PROVIDER_DEFAULT_KEY.get(provider, "sk-placeholder")
    client = OpenAI(base_url=base_url, api_key=key)

    stats = {"confirmed": 0, "skipped": 0, "errors": 0, "processed": 0}
    total = len(files)

    for batch_start in range(0, total, batch_size):
        batch = files[batch_start : batch_start + batch_size]
        batch_input = _build_batch_input(batch)

        try:
            results = _call_llm(client, model, batch_input)
        except Exception as e:
            logger.error("LLM batch error: %s", e)
            for row in batch:
                db.update_media_file(row["id"], status="needs_manual",
                                     notes=f"LLM error: {str(e)[:200]}")
                stats["errors"] += 1
                stats["processed"] += 1
                if progress_cb:
                    progress_cb(stats["processed"], total, row["filename"], "error")
            continue

        result_map = {r.get("id"): r for r in results if r.get("id") is not None}

        for row in batch:
            stats["processed"] += 1
            row_result = result_map.get(row["id"])
            file_status = "skipped"

            if not row_result or row_result.get("skip"):
                db.update_media_file(row["id"], status="needs_manual",
                                     notes="LLM could not identify")
                stats["skipped"] += 1
            else:
                file_status = _apply_llm_api_result(row, row_result, tmdb, db)
                if file_status == "confirmed":
                    stats["confirmed"] += 1
                else:
                    stats["skipped"] += 1

            if progress_cb:
                progress_cb(stats["processed"], total, row["filename"], file_status)

    return stats


def _apply_llm_api_result(row, result: dict, tmdb, db) -> str:
    """Apply a single LLM API result to the DB. Returns 'confirmed' or 'skipped'."""
    tmdb_id = result.get("tmdb_id")
    confirmed_type = (result.get("confirmed_type") or "movie").lower()
    confirmed_data = None

    if tmdb and tmdb_id:
        try:
            if confirmed_type == "tv":
                confirmed_data = tmdb.get_tv_details(int(tmdb_id))
            else:
                confirmed_data = tmdb.get_movie_details(int(tmdb_id))
        except Exception as e:
            logger.warning("TMDB validation failed: %s", e)

    api = confirmed_data or {}
    updates = {
        "status": "identified",
        "phase": 2,
        "confirmed_title": api.get("confirmed_title") or result.get("confirmed_title"),
        "confirmed_year": api.get("confirmed_year") or result.get("confirmed_year"),
        "confirmed_type": api.get("confirmed_type") or confirmed_type,
        "tmdb_id": api.get("tmdb_id") or tmdb_id,
        "imdb_id": api.get("imdb_id"),
        "genres": json.dumps(api.get("genres") or []),
        "plot": api.get("plot"),
        "rating": api.get("rating"),
        "director": api.get("director"),
        "cast": json.dumps(api.get("cast") or []),
        "season": result.get("season"),
        "episode": result.get("episode"),
    }
    updates = {k: v for k, v in updates.items() if v is not None}
    if not updates.get("confirmed_title"):
        db.update_media_file(row["id"], status="needs_manual",
                             notes="LLM provided no title")
        return "skipped"

    db.update_media_file(row["id"], **updates)
    return "confirmed"


def _build_batch_input(batch) -> list[dict]:
    return [
        {
            "id": row["id"],
            "filename": row["filename"],
            "parent_folder": row["parent_folder"] or "",
            "guessed_title": row["guessed_title"] or "",
            "guessed_year": row["guessed_year"],
            "guessed_type": row["guessed_type"] or "",
            "guessed_season": row["guessed_season"],
            "guessed_episode": row["guessed_episode"],
        }
        for row in batch
    ]


def _call_llm(client, model: str, batch_input: list) -> list[dict]:
    """
    Send a batch to the LLM and parse the JSON response.
    Retries once if the first response is unparseable.
    """
    user_content = json.dumps(batch_input, ensure_ascii=False)

    for attempt in range(2):
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",
                 "content": f"Identify these media files and return a JSON array:\n{user_content}"},
            ],
            temperature=0.1,
            max_tokens=3000,
        )
        content = resp.choices[0].message.content.strip()

        # Strip markdown code fences if present
        fenced = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", content, re.DOTALL)
        if fenced:
            content = fenced.group(1)

        # Find the JSON array boundaries
        start = content.find("[")
        end = content.rfind("]") + 1
        if start >= 0 and end > start:
            content = content[start:end]

        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            if attempt == 0:
                logger.warning("LLM returned invalid JSON (attempt 1): %s", e)
                continue
            raise ValueError(f"LLM returned unparseable JSON after retry: {e}")

    return []


def _resolve_endpoint(provider: str, custom_endpoint: str) -> str:
    if provider == "custom" and custom_endpoint:
        return custom_endpoint
    return _PROVIDER_ENDPOINTS.get(provider, custom_endpoint or "http://localhost:11434/v1")
