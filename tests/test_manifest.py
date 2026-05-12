"""Manifest round-trip tests against a local DuckDB+Parquet store."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from stingray_frame_viewer.manifest import (
    ensure_videos_table,
    load_manifest,
    lookup,
    open_store,
)
from stingray_frame_viewer.models import Video


@pytest.fixture
def store(tmp_path):
    s = open_store(str(tmp_path))
    ensure_videos_table(s)
    return s


def _video(video_id: str, cruise: str, camera: str, frame_count: int = 100) -> Video:
    return Video(
        video_id=video_id,
        media_path=f"/proj/nes-lter/Stingray/data/{cruise}/{camera}/20240503T191112.333Z/{video_id}.avi",
        frame_count=frame_count,
        media_time=datetime(2024, 5, 3, 19, 11, 12, 333000, tzinfo=timezone.utc),
        cruise=cruise,
        camera=camera,
    )


def test_videos_roundtrip(store):
    records = [
        _video("A", "NESLTER_EN715", "Basler_avA2300-25gm"),
        _video("B", "NESLTER_EN715", "Basler_avA2300-25gm", frame_count=250),
        _video("C", "NESLTER_AR99", "Basler_avA2300-25gm"),
    ]
    store.write("videos", [r.model_dump() for r in records])

    manifest = load_manifest(store)
    assert set(manifest) == {"A", "B", "C"}
    assert manifest["B"].frame_count == 250
    assert isinstance(manifest["A"].media_time, datetime)
    # load_manifest normalizes to UTC (DuckDB returns local-TZ on read)
    assert manifest["A"].media_time.tzinfo == timezone.utc
    assert manifest["A"].media_time == datetime(2024, 5, 3, 19, 11, 12, 333000, tzinfo=timezone.utc)
    assert manifest["A"].cruise == "NESLTER_EN715"


def test_lookup_missing(store):
    store.write("videos", [_video("X", "C1", "Cam1").model_dump()])
    manifest = load_manifest(store)
    assert lookup(manifest, "X").video_id == "X"
    assert lookup(manifest, "MISSING") is None


def test_distinct_values_after_write(store):
    """The ingest CLI relies on distinct_values for partition-conflict detection."""
    store.write(
        "videos",
        [
            _video("A", "NESLTER_EN715", "Basler_avA2300-25gm").model_dump(),
            _video("B", "NESLTER_AR99", "Basler_avA2300-25gm").model_dump(),
        ],
    )
    rows = store.distinct_values("videos", ["cruise", "camera"])
    pairs = {(r["cruise"], r["camera"]) for r in rows}
    assert pairs == {
        ("NESLTER_EN715", "Basler_avA2300-25gm"),
        ("NESLTER_AR99", "Basler_avA2300-25gm"),
    }
