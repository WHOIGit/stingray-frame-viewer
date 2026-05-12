"""FastAPI route handlers.

Endpoints (see DESIGN.md): ``GET /health``, ``GET /videos/{video_id}``,
``GET /frames/{video_id}/{frame_index}``, and ``GET
/frames/{video_id}/{frame_index}/neighbors`` (M6). Handlers are sync ``def``
so FastAPI runs them in its threadpool.

Implemented in M4.
"""
from __future__ import annotations
