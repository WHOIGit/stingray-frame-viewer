"""Shared pytest fixtures.

The synthetic-AVI fixture uses the FFV1 lossless codec. The real Stingray
AVIs are technically uncompressed (empty fourcc) rather than FFV1-compressed,
but both are independently-decodable-per-frame and exercise the same
``cv2.VideoCapture`` seek+decode path. FFV1 is portable across the macOS and
Linux opencv-python wheels we test against; truly uncompressed writing via
``cv2.VideoWriter`` is not reliably supported.
"""
from __future__ import annotations

import cv2
import numpy as np
import pytest


SYNTHETIC_FRAMES = 10
SYNTHETIC_H = 64
SYNTHETIC_W = 96


def _synthetic_frame(i: int) -> np.ndarray:
    """Distinguishable 2D uint8 frame for index ``i`` (0..255 gray gradient)."""
    return np.full((SYNTHETIC_H, SYNTHETIC_W), (i + 1) * 20, dtype=np.uint8)


@pytest.fixture
def synthetic_avi(tmp_path) -> str:
    """Build a small lossless grayscale AVI with frames that round-trip byte-exact."""
    path = tmp_path / "synth.avi"
    fourcc = cv2.VideoWriter_fourcc(*"FFV1")
    writer = cv2.VideoWriter(str(path), fourcc, 10, (SYNTHETIC_W, SYNTHETIC_H), isColor=False)
    if not writer.isOpened():
        pytest.skip("FFV1 codec not available in this OpenCV build")
    try:
        for i in range(SYNTHETIC_FRAMES):
            writer.write(_synthetic_frame(i))
    finally:
        writer.release()
    return str(path)


@pytest.fixture
def expected_frame():
    """Helper to construct the expected synthetic frame for a given index."""
    return _synthetic_frame
