"""Domain exception types and FastAPI exception handlers.

The exception types are domain-level — raised by the manifest, extractor, and
encoder. The handlers that translate them into HTTP responses (404 / 416 /
500) are wired in when the FastAPI app lands.
"""
from __future__ import annotations


class VideoNotFoundError(Exception):
    """The requested ``video_id`` is not in the manifest."""


class FrameOutOfRangeError(Exception):
    """The requested ``frame_index`` is outside ``[0, frame_count)``."""


class FrameExtractionError(Exception):
    """Frame bytes could not be produced from the source AVI.

    Covers missing source files, unopenable AVIs, decode failures, and encode
    failures — anything that prevents the service from returning frame bytes
    despite the manifest claiming the frame should exist.
    """
