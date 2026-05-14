"""Unit tests for the ingest CLI's --exclude filtering."""
from __future__ import annotations

from stingray_frame_viewer.ingest.__main__ import _apply_excludes

PATHS = [
    "/data/media_list/ISIIS1/20251018_AR95.csv",
    "/data/media_list/ISIIS1/20251018_AR95_fast.csv",
    "/data/media_list/ISIIS1/20250418_AR88.csv",
    "/data/media_list/ISIIS2/20250418_AR88.csv",
    "/data/media_list/ISIIS2/20250418_AR88_fast.csv",
]


def test_no_patterns_keeps_everything():
    kept, dropped = _apply_excludes(PATHS, [])
    assert kept == PATHS
    assert dropped == []


def test_fast_suffix_pattern():
    kept, dropped = _apply_excludes(PATHS, ["*_fast.csv"])
    assert dropped == [
        "/data/media_list/ISIIS1/20251018_AR95_fast.csv",
        "/data/media_list/ISIIS2/20250418_AR88_fast.csv",
    ]
    assert "/data/media_list/ISIIS1/20251018_AR95.csv" in kept
    assert "/data/media_list/ISIIS2/20250418_AR88.csv" in kept


def test_full_path_pattern_targets_one_file():
    # Excluding the ISIIS2 AR88 outlier must NOT also drop ISIIS1's AR88.
    kept, dropped = _apply_excludes(PATHS, ["*/ISIIS2/20250418_AR88.csv"])
    assert dropped == ["/data/media_list/ISIIS2/20250418_AR88.csv"]
    assert "/data/media_list/ISIIS1/20250418_AR88.csv" in kept


def test_multiple_patterns_are_unioned():
    kept, dropped = _apply_excludes(
        PATHS, ["*_fast.csv", "*/ISIIS2/20250418_AR88.csv"]
    )
    assert set(dropped) == {
        "/data/media_list/ISIIS1/20251018_AR95_fast.csv",
        "/data/media_list/ISIIS2/20250418_AR88_fast.csv",
        "/data/media_list/ISIIS2/20250418_AR88.csv",
    }
    assert kept == [
        "/data/media_list/ISIIS1/20251018_AR95.csv",
        "/data/media_list/ISIIS1/20250418_AR88.csv",
    ]


def test_pattern_matching_nothing_is_a_noop():
    kept, dropped = _apply_excludes(PATHS, ["*.parquet"])
    assert kept == PATHS
    assert dropped == []
