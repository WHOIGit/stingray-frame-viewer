"""Unit tests for the OpenCV-based frame extractor."""
from __future__ import annotations

import numpy as np
import pytest

from stingray_frame_viewer.errors import FrameExtractionError
from stingray_frame_viewer.extractor import extract_frame


def test_returns_2d_uint8_grayscale(synthetic_avi, expected_frame):
    frame = extract_frame(synthetic_avi, 0)
    assert frame.ndim == 2
    assert frame.dtype == np.uint8
    assert frame.shape == expected_frame(0).shape


@pytest.mark.parametrize("idx", [0, 3, 5, 9])
def test_seek_returns_correct_frame(synthetic_avi, expected_frame, idx):
    frame = extract_frame(synthetic_avi, idx)
    assert np.array_equal(frame, expected_frame(idx))


def test_seek_can_jump_backwards(synthetic_avi, expected_frame):
    """A second extract on the same path should land on the requested index, not the next one."""
    _ = extract_frame(synthetic_avi, 8)
    frame = extract_frame(synthetic_avi, 2)
    assert np.array_equal(frame, expected_frame(2))


def test_missing_file_raises(tmp_path):
    missing = tmp_path / "nope.avi"
    with pytest.raises(FrameExtractionError, match="not found"):
        extract_frame(str(missing), 0)


def test_unopenable_file_raises(tmp_path):
    """A file that exists but isn't a valid AVI should raise FrameExtractionError."""
    bogus = tmp_path / "not-an-avi.avi"
    bogus.write_bytes(b"this is not a video container")
    with pytest.raises(FrameExtractionError):
        extract_frame(str(bogus), 0)
