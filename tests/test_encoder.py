"""Unit tests for the PNG/JPEG encoder."""
from __future__ import annotations

import cv2
import numpy as np
import pytest

from stingray_frame_viewer.encoder import encode


def _gradient(h: int = 64, w: int = 96) -> np.ndarray:
    """Horizontal grayscale gradient — gives the encoder real content to compress."""
    row = np.linspace(0, 255, w, dtype=np.uint8)
    return np.tile(row, (h, 1))


def test_png_is_lossless():
    frame = _gradient()
    blob = encode(frame, "png")
    decoded = cv2.imdecode(np.frombuffer(blob, dtype=np.uint8), cv2.IMREAD_GRAYSCALE)
    assert np.array_equal(decoded, frame)


def test_png_signature():
    blob = encode(_gradient(), "png")
    assert blob.startswith(b"\x89PNG\r\n\x1a\n")


def test_jpeg_is_approximate():
    frame = _gradient()
    blob = encode(frame, "jpeg", jpeg_quality=90)
    decoded = cv2.imdecode(np.frombuffer(blob, dtype=np.uint8), cv2.IMREAD_GRAYSCALE)
    assert decoded.shape == frame.shape
    # JPEG@90 should be visually close — assert mean abs diff < 5 levels out of 255
    assert np.abs(decoded.astype(int) - frame.astype(int)).mean() < 5


def test_jpeg_signature():
    blob = encode(_gradient(), "jpeg")
    assert blob.startswith(b"\xff\xd8\xff")


def test_jpeg_quality_affects_size():
    frame = _gradient()
    small = encode(frame, "jpeg", jpeg_quality=20)
    large = encode(frame, "jpeg", jpeg_quality=95)
    assert len(small) < len(large)


def test_unknown_format_raises():
    with pytest.raises(ValueError, match="unknown frame format"):
        encode(_gradient(), "webp")  # type: ignore[arg-type]
