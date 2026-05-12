"""Exception types and FastAPI exception handlers.

Maps domain exceptions to HTTP status codes per DESIGN §5:
``VideoNotFoundError`` → 404, ``FrameOutOfRangeError`` → 416,
``FrameExtractionError`` → 500.

Implemented in M4.
"""
from __future__ import annotations
