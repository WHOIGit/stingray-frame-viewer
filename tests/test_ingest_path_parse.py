"""Path-parse unit tests for the ingest CLI."""
from __future__ import annotations

import pytest

from stingray_frame_viewer.ingest.aggregate import parse_cruise_camera


def test_happy_path():
    p = (
        "/proj/nes-lter/Stingray/data/NESLTER_AR99/Basler_avA2300-25gm/"
        "20260113T193007.771Z/Basler_avA2300-25gm-001-20260113T193007.771Z.avi"
    )
    assert parse_cruise_camera(p) == ("NESLTER_AR99", "Basler_avA2300-25gm")


def test_no_stingray_segment():
    with pytest.raises(ValueError, match="Stingray"):
        parse_cruise_camera("/some/other/path/cruise/camera/x.avi")


def test_too_short():
    with pytest.raises(ValueError, match="too short"):
        parse_cruise_camera("/proj/Stingray/data")
