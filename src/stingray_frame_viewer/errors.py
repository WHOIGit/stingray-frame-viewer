"""Exception types and FastAPI exception handlers.

Maps domain exceptions to HTTP status codes (see DESIGN.md):
``VideoNotFoundError`` → 404, ``FrameOutOfRangeError`` → 416,
``FrameExtractionError`` → 500.

Implemented in M4.
"""
from __future__ import annotations
