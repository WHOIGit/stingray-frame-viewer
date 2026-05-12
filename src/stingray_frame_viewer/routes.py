"""FastAPI route handlers.

Endpoints (DESIGN §5): ``GET /health``, ``GET /videos/{video_id}``,
``GET /frames/{video_id}/{frame_index}``, and ``GET
/frames/{video_id}/{frame_index}/neighbors`` (M6). Handlers are sync ``def``
so FastAPI runs them in its threadpool (DESIGN §11).

Implemented in M4.
"""
from __future__ import annotations
