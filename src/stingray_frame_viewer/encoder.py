"""Frame-encoding wrappers around ``cv2.imencode``.

Public surface: ``encode(frame: np.ndarray, fmt: Literal['png','jpeg'],
jpeg_quality: int = 90) -> bytes``. PNG is lossless and is the default; JPEG
is permitted but only the viewer UI should use it (annotation pipelines
always request PNG — see DESIGN.md).
"""
from __future__ import annotations

from typing import Literal

import cv2
import numpy as np

from .errors import FrameExtractionError

Format = Literal["png", "jpeg"]


def encode(frame: np.ndarray, fmt: Format, jpeg_quality: int = 90) -> bytes:
    """Encode a 2D uint8 grayscale array to PNG or JPEG bytes.

    Raises :class:`ValueError` for unknown ``fmt`` (programmer error;
    surfaced as 400 by the route layer). Raises
    :class:`FrameExtractionError` if ``cv2.imencode`` itself fails — that is
    operationally rare but should be a 500 if it happens.
    """
    if fmt == "png":
        ok, buf = cv2.imencode(".png", frame)
    elif fmt == "jpeg":
        ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, int(jpeg_quality)])
    else:
        raise ValueError(f"unknown frame format: {fmt!r}")

    if not ok:
        raise FrameExtractionError(f"cv2.imencode failed for fmt={fmt!r}")

    return buf.tobytes()
