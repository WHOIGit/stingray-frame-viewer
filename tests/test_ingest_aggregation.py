"""Polars aggregation tests against synthetic CSVs."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from stingray_frame_viewer.ingest.aggregate import (
    aggregate_frames,
    aggregate_videos,
    count_bad_file_videos,
    count_id_link_nonempty,
)

HEADER = "media_path,media,media_time,frame,times,status,id,link"


def _path(cruise: str, camera: str, stem: str) -> str:
    return (
        f"/proj/nes-lter/Stingray/data/{cruise}/{camera}/"
        f"20260113T193007.771Z/{stem}.avi"
    )


def _row(media_path: str, media: str, media_time: str, frame: int, times: str,
         status: str = "ok", id_: str = "", link: str = "") -> str:
    return f"{media_path},{media},{media_time},{frame},{times},{status},{id_},{link}"


def _write_csv(tmp_path, name: str, rows: list[str]) -> str:
    p = tmp_path / name
    p.write_text("\n".join([HEADER, *rows]) + "\n")
    return str(p)


@pytest.fixture
def sample_csv(tmp_path):
    media_path_a = _path("NESLTER_AR99", "Basler_avA2300-25gm", "video_a")
    media_path_b = _path("NESLTER_AR99", "Basler_avA2300-25gm", "video_b")
    rows = [
        _row(media_path_a, "video_a", "2026-01-13 19:30:07.771", 0, "2026-01-13 19:30:07.837"),
        _row(media_path_a, "video_a", "2026-01-13 19:30:07.771", 1, "2026-01-13 19:30:07.904"),
        _row(media_path_a, "video_a", "2026-01-13 19:30:07.771", 2, "2026-01-13 19:30:07.970"),
        _row(media_path_b, "video_b", "2026-01-13 19:31:00.000", 0, "2026-01-13 19:31:00.066"),
        _row(media_path_b, "video_b", "2026-01-13 19:31:00.000", 1, "2026-01-13 19:31:00.133"),
    ]
    return _write_csv(tmp_path, "sample.csv", rows)


def test_aggregate_videos_counts_frames(sample_csv):
    df = aggregate_videos([sample_csv]).sort("video_id")
    assert df["video_id"].to_list() == ["video_a", "video_b"]
    assert df["frame_count"].to_list() == [3, 2]
    assert df["cruise"].to_list() == ["NESLTER_AR99", "NESLTER_AR99"]
    assert df["camera"].to_list() == ["Basler_avA2300-25gm", "Basler_avA2300-25gm"]
    media_times = df["media_time"].to_list()
    assert all(isinstance(t, datetime) for t in media_times)
    # Anchored to UTC at ingest (avoids parquet round-trip TZ drift).
    # Polars uses zoneinfo.ZoneInfo("UTC"); equality-by-tzinfo-object is fragile.
    assert all(t.utcoffset() == timedelta(0) for t in media_times)


def test_aggregate_frames_sorted(sample_csv):
    df = aggregate_frames([sample_csv])
    assert df.height == 5
    assert df.columns == ["video_id", "frame_index", "frame_time", "status", "cruise"]
    # Sorted by (cruise, video_id, frame_index)
    video_ids = df["video_id"].to_list()
    frame_indices = df["frame_index"].to_list()
    assert video_ids == ["video_a", "video_a", "video_a", "video_b", "video_b"]
    assert frame_indices == [0, 1, 2, 0, 1]


def test_count_id_link_zero_when_empty(sample_csv):
    assert count_id_link_nonempty([sample_csv]) == 0


def test_bad_file_videos_excluded(tmp_path):
    """A status='bad_file' sentinel row (null frame) should drop its video entirely."""
    good_path = _path("NESLTER_AR99", "Cam1", "good")
    bad_path = _path("NESLTER_AR99", "Cam1", "bad")
    rows = [
        _row(good_path, "good", "2026-01-13 19:30:07.771", 0, "2026-01-13 19:30:07.837"),
        _row(good_path, "good", "2026-01-13 19:30:07.771", 1, "2026-01-13 19:30:07.904"),
        # Bad-file sentinel: status="bad_file", frame and times null/empty
        f"{bad_path},bad,2026-01-13 19:31:00.000,,,bad_file,,",
    ]
    csv = _write_csv(tmp_path, "with_bad.csv", rows)
    assert count_bad_file_videos([csv]) == 1
    videos = aggregate_videos([csv])
    assert videos["video_id"].to_list() == ["good"]
    frames = aggregate_frames([csv])
    assert frames["video_id"].to_list() == ["good", "good"]


def test_count_id_link_detects_nonempty(tmp_path):
    media_path = _path("NESLTER_AR99", "Cam1", "v")
    rows = [
        _row(media_path, "v", "2026-01-13 19:30:07.771", 0, "2026-01-13 19:30:07.837",
             id_="ROI-42"),
        _row(media_path, "v", "2026-01-13 19:30:07.771", 1, "2026-01-13 19:30:07.904",
             link="https://example/x"),
        _row(media_path, "v", "2026-01-13 19:30:07.771", 2, "2026-01-13 19:30:07.970"),
    ]
    csv = _write_csv(tmp_path, "dirty.csv", rows)
    assert count_id_link_nonempty([csv]) == 2
