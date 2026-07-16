"""FastAPI route handlers.

Endpoints (see DESIGN.md): ``GET /health``, ``GET /videos/{video_id}``,
``GET /frames/{video_id}/{frame_index}``. The ``/neighbors`` endpoint is
deferred. Handlers are sync ``def`` so FastAPI runs them in its threadpool.

The manifest and settings are read off ``request.app.state`` via tiny
``Depends`` shims (``get_manifest`` / ``get_settings``) so tests can override
them with ``app.dependency_overrides``.
"""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from .cache import FrameCache, cache_key
from .config import Settings
from .encoder import encode
from .errors import FrameOutOfRangeError, VideoNotFoundError
from .extractor import extract_frame
from .models import Video

router = APIRouter()

_FRAME_CACHE_CONTROL = "public, max-age=31536000, immutable"
_ALLOWED_FORMATS = ("png", "jpeg")


def get_manifest(request: Request) -> dict[str, Video]:
    return request.app.state.manifest


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_cache(request: Request) -> FrameCache | None:
    # None when the cache is disabled (Phase 1) or the app was built without a
    # lifespan (route tests wire app.state manually).
    return getattr(request.app.state, "cache", None)


@router.get("/health")
def health() -> Response:
    # Reachable only after uvicorn's lifespan startup completes, which is
    # when the manifest is loaded. No separate readiness gate needed.
    return JSONResponse({"status": "ok"})


@router.get("/videos/{video_id}")
def get_video(
    video_id: str,
    manifest: dict[str, Video] = Depends(get_manifest),
) -> dict:
    video = manifest.get(video_id)
    if video is None:
        raise VideoNotFoundError(video_id)
    return {
        "video_id": video.video_id,
        "media_basename": Path(video.media_path).name,
        "frame_count": video.frame_count,
        "last_frame_index": video.frame_count - 1,
        "media_time": video.media_time.isoformat(),
        "cruise": video.cruise,
        "camera": video.camera,
    }


@router.get("/frames/{video_id}/{frame_index}")
def get_frame(
    video_id: str,
    frame_index: int,
    format: str = Query("png"),
    manifest: dict[str, Video] = Depends(get_manifest),
    settings: Settings = Depends(get_settings),
    cache: FrameCache | None = Depends(get_cache),
) -> Response:
    if format not in _ALLOWED_FORMATS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown format: {format!r} (allowed: {', '.join(_ALLOWED_FORMATS)})",
        )
    video = manifest.get(video_id)
    if video is None:
        raise VideoNotFoundError(video_id)
    if frame_index < 0 or frame_index >= video.frame_count:
        raise FrameOutOfRangeError(video_id, frame_index, video.frame_count)

    # Phase 2 lazy write-through. A single GET doubles as the existence check:
    # on a hit it returns the stored bytes; on a miss it returns None (see
    # FrameCache.get), so we extract + encode on the fly and write the frame
    # back under a stable key.
    key = cache_key(video_id, frame_index, format, prefix=settings.cache_prefix)
    body: bytes | None = cache.get(key) if cache is not None else None

    if body is None:
        frame = extract_frame(video.media_path, frame_index)
        body = encode(frame, format, jpeg_quality=settings.jpeg_quality)
        if cache is not None:
            cache.put(key, body)  # best-effort; failures are logged, not raised

    media_type = "image/png" if format == "png" else "image/jpeg"
    # video_id is the verbatim CSV `media` column, which the manifest treats
    # as immutable — so the immutable Cache-Control is safe even on the
    # on-the-fly extraction path.
    return Response(
        content=body,
        media_type=media_type,
        headers={"Cache-Control": _FRAME_CACHE_CONTROL},
    )
