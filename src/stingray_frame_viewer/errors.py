"""Domain exception types and FastAPI exception handlers.

The exception types are domain-level — raised by the manifest, extractor, and
encoder. ``install_handlers`` wires them into a FastAPI app and translates
them into the HTTP responses documented in DESIGN.md.
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


class VideoNotFoundError(Exception):
    """The requested ``video_id`` is not in the manifest."""

    def __init__(self, video_id: str) -> None:
        self.video_id = video_id
        super().__init__(f"video_id={video_id!r} not in manifest")


class FrameOutOfRangeError(Exception):
    """The requested ``frame_index`` is outside ``[0, frame_count)``."""

    def __init__(self, video_id: str, frame_index: int, frame_count: int) -> None:
        self.video_id = video_id
        self.frame_index = frame_index
        self.frame_count = frame_count
        super().__init__(
            f"frame_index={frame_index} out of range for video_id={video_id!r} "
            f"(frame_count={frame_count})"
        )


class FrameExtractionError(Exception):
    """Frame bytes could not be produced from the source AVI.

    Covers missing source files, unopenable AVIs, decode failures, and encode
    failures — anything that prevents the service from returning frame bytes
    despite the manifest claiming the frame should exist.
    """


def _video_not_found(_request: Request, exc: VideoNotFoundError) -> JSONResponse:
    return JSONResponse(
        {"detail": str(exc), "video_id": exc.video_id},
        status_code=404,
    )


def _frame_out_of_range(_request: Request, exc: FrameOutOfRangeError) -> JSONResponse:
    return JSONResponse(
        {
            "detail": str(exc),
            "video_id": exc.video_id,
            "frame_index": exc.frame_index,
            "frame_count": exc.frame_count,
        },
        status_code=416,
    )


def _frame_extraction_failed(_request: Request, exc: FrameExtractionError) -> JSONResponse:
    # Don't leak filesystem details to the client; log-side debugging happens
    # via the exception's str() in service logs.
    return JSONResponse({"detail": "frame extraction failed"}, status_code=500)


def install_handlers(app: FastAPI) -> None:
    """Register the domain → HTTP-status mapping on ``app``."""
    app.add_exception_handler(VideoNotFoundError, _video_not_found)
    app.add_exception_handler(FrameOutOfRangeError, _frame_out_of_range)
    app.add_exception_handler(FrameExtractionError, _frame_extraction_failed)
