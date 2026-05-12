"""Pydantic models for the manifest tables.

``Video`` is one row per video; ``Frame`` is one row per frame. Schemas locked
in DESIGN §6. Datetimes are tz-aware UTC. The source CSV is naive
(``media_time`` arrives as ``"2024-05-03 19:11:12.333"`` with no offset, but
the trailing ``Z`` on filename timestamps confirms UTC intent), and DuckDB's
parquet round-trip silently re-interprets naive timestamps through the local
timezone — anchoring to UTC at ingest avoids that footgun.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class Video(BaseModel):
    video_id: str
    media_path: str
    frame_count: int
    media_time: datetime
    cruise: str
    camera: str


class Frame(BaseModel):
    video_id: str
    frame_index: int
    frame_time: datetime
    status: str
    cruise: str
