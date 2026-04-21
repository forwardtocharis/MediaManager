"""
nfo_writer.py — Generate Kodi-compatible NFO sidecar XML files.

NFO files generated here are compatible with:
  - Kodi (native)
  - Jellyfin (with NFO plugin or native support)
  - Plex (via XBMCnfoMoviesImporter / XBMCnfoTVImporter agents)
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
import json


def _indent(elem: ET.Element, level: int = 0) -> None:
    """Add pretty-print indentation to an ElementTree element."""
    indent = "\n" + "  " * level
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = indent + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = indent
        for child in elem:
            _indent(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = indent
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = indent
    if not level:
        elem.tail = "\n"


def _sub(parent: ET.Element, tag: str, text: Optional[str]) -> Optional[ET.Element]:
    """Add a child element only if text is non-empty."""
    if text:
        el = ET.SubElement(parent, tag)
        el.text = str(text)
        return el
    return None


def write_movie_nfo(nfo_path: Path, data: dict) -> None:
    """
    Write a movie NFO file.
    data keys (all optional except title):
      title, year, plot, genres (list), rating, director,
      cast (list of names), imdb_id, tmdb_id, poster_url
    """
    root = ET.Element("movie")

    _sub(root, "title", data.get("title"))
    _sub(root, "originaltitle", data.get("title"))
    _sub(root, "year", str(data.get("year", "")))
    _sub(root, "plot", data.get("plot"))
    _sub(root, "outline", data.get("plot"))
    _sub(root, "rating", str(data.get("rating", "")))
    _sub(root, "director", data.get("director"))

    # Genres
    genres = data.get("genres")
    if isinstance(genres, str):
        try:
            genres = json.loads(genres)
        except Exception:
            genres = [genres]
    for genre in (genres or []):
        _sub(root, "genre", genre)

    # Cast
    cast = data.get("cast")
    if isinstance(cast, str):
        try:
            cast = json.loads(cast)
        except Exception:
            cast = [cast]
    for actor_name in (cast or []):
        actor_el = ET.SubElement(root, "actor")
        _sub(actor_el, "name", actor_name)

    # Unique IDs
    if data.get("imdb_id"):
        uid = ET.SubElement(root, "uniqueid")
        uid.set("type", "imdb")
        uid.set("default", "true")
        uid.text = str(data["imdb_id"])

    if data.get("tmdb_id"):
        uid = ET.SubElement(root, "uniqueid")
        uid.set("type", "tmdb")
        uid.text = str(data["tmdb_id"])

    _indent(root)
    tree = ET.ElementTree(root)
    nfo_path.parent.mkdir(parents=True, exist_ok=True)
    with open(nfo_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>\n')
        tree.write(f, encoding="unicode", xml_declaration=False)


def write_tvshow_nfo(nfo_path: Path, data: dict) -> None:
    """
    Write a tvshow.nfo file at the show root directory.
    data keys: title, year, plot, genres (list), rating, imdb_id, tmdb_id
    """
    root = ET.Element("tvshow")

    _sub(root, "title", data.get("title"))
    _sub(root, "year", str(data.get("year", "")))
    _sub(root, "plot", data.get("plot"))
    _sub(root, "rating", str(data.get("rating", "")))

    genres = data.get("genres")
    if isinstance(genres, str):
        try:
            genres = json.loads(genres)
        except Exception:
            genres = [genres]
    for genre in (genres or []):
        _sub(root, "genre", genre)

    if data.get("imdb_id"):
        uid = ET.SubElement(root, "uniqueid")
        uid.set("type", "imdb")
        uid.set("default", "true")
        uid.text = str(data["imdb_id"])

    if data.get("tmdb_id"):
        uid = ET.SubElement(root, "uniqueid")
        uid.set("type", "tmdb")
        uid.text = str(data["tmdb_id"])

    _indent(root)
    tree = ET.ElementTree(root)
    nfo_path.parent.mkdir(parents=True, exist_ok=True)
    with open(nfo_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>\n')
        tree.write(f, encoding="unicode", xml_declaration=False)


def write_episode_nfo(nfo_path: Path, data: dict) -> None:
    """
    Write an episode NFO file next to the video file.
    data keys: title, season, episode, plot, air_date, rating, imdb_id, tmdb_id
    """
    root = ET.Element("episodedetails")

    _sub(root, "title", data.get("episode_title") or data.get("title"))
    _sub(root, "season", str(data.get("season", "")))
    _sub(root, "episode", str(data.get("episode", "")))
    _sub(root, "plot", data.get("plot"))
    _sub(root, "aired", data.get("air_date"))
    _sub(root, "rating", str(data.get("rating", "")))

    if data.get("imdb_id"):
        uid = ET.SubElement(root, "uniqueid")
        uid.set("type", "imdb")
        uid.set("default", "true")
        uid.text = str(data["imdb_id"])

    if data.get("tmdb_id"):
        uid = ET.SubElement(root, "uniqueid")
        uid.set("type", "tmdb")
        uid.text = str(data["tmdb_id"])

    _indent(root)
    tree = ET.ElementTree(root)
    nfo_path.parent.mkdir(parents=True, exist_ok=True)
    with open(nfo_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>\n')
        tree.write(f, encoding="unicode", xml_declaration=False)
