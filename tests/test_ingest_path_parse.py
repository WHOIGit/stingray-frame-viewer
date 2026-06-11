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


def test_arbitrary_mount_prefix():
    # AVIs mounted outside /proj/nes-lter/Stingray/data must still parse: only
    # the trailing {cruise}/{camera}/{timestamp}/{file} structure is required.
    p = (
        "/mnt/stingray_data/NESLTER_AR99/Basler_avA2300-25gm/"
        "20260113T205620.709Z/Basler_avA2300-25gm-003-20260113T205820.621Z.avi"
    )
    assert parse_cruise_camera(p) == ("NESLTER_AR99", "Basler_avA2300-25gm")


def test_relative_path_minimal():
    # Exactly four segments (no mount prefix at all) is the minimum valid form.
    assert parse_cruise_camera("CRUISE/CAM/20260101T000000.000Z/f.avi") == ("CRUISE", "CAM")


def test_too_short():
    with pytest.raises(ValueError, match="too short"):
        parse_cruise_camera("/proj/Stingray")
