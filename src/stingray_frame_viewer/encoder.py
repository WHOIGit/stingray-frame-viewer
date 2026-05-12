"""Frame-encoding wrappers around ``cv2.imencode``.

Public surface (M3): ``encode(frame: np.ndarray, fmt: Literal['png','jpeg'],
jpeg_quality: int = 90) -> bytes``. PNG is lossless; JPEG is opt-in per
DESIGN §8.

Implemented in M3.
"""
from __future__ import annotations
