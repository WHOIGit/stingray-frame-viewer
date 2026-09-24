"""Polars aggregation tests against synthetic CSVs."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from stingray_frame_viewer.ingest.aggregate import (
    aggregate_frames,
    aggregate_videos,
    count_excluded_videos,
    count_id_link_nonempty,
)

HEADER = "media_path,media,media_time,frame,times,status,id,link"


def _path(cruise: str, camera: str, stem: str) -> str:
    return (
        f"/proj/nes-lter/Stingray/data/{cruise}/{camera}/"
        f"20260113T193007.771Z/{stem}.avi"
    )


def _lrauv_path(cruise: str, camera: str, stem: str) -> str:
    return (
        f"/mnt/sosiknas2/Lab_data/LRAUV/{cruise}/basler-videos/{camera}/"
        f"2024-02-12-14-34-27.063/{stem}.avi"
    )


# LRAUV CSV format: different column order, extra columns, no id/link.
LRAUV_HEADER = "media_path,media_size,media,media_time,frame_count,status,frame,times"


def _lrauv_row(media_path: str, media: str, media_time: str, frame: int, times: str,
               frame_count: int = 2) -> str:
    return f"{media_path},716505088,{media},{media_time},{frame_count},valid,{frame},{times}"


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


def test_cruise_expr_matches_parser():
    """The Polars cruise regex must agree with parse_cruise_camera (they are
    two encodings of the same path contract; this guards them from drifting)."""
    import polars as pl

    from stingray_frame_viewer.ingest.aggregate import _cruise_expr, parse_cruise_camera

    paths = [
        _path("NESLTER_AR99", "Cam1", "v"),
        _path("NESLTER_EN688", "Basler_avA2300-25gm", "vid"),
        "/some/other/prefix/Stingray/data/CRUISE_X/CamZ/20260101T000000.000Z/f.avi",
        # Arbitrary mount root, no "Stingray" segment (regression: prefix-agnostic).
        "/mnt/stingray_data/CRUISE_Y/CamW/20260101T000000.000Z/g.avi",
        # LRAUV layout: extra basler-videos segment, spaces in the cruise.
        _lrauv_path("LRAUV LTER 20240212", "pixys_isiis_camera", "v"),
        # Spaced cruise in the plain Stingray layout.
        _path("CRUISE WITH SPACES", "Cam1", "v"),
    ]
    via_expr = (
        pl.DataFrame({"media_path": paths})
        .select(_cruise_expr().alias("cruise"))["cruise"]
        .to_list()
    )
    via_parser = [parse_cruise_camera(p)[0] for p in paths]
    assert via_expr == via_parser


def test_iter_frame_chunks_one_cruise_at_a_time(tmp_path):
    """Chunked frames cover the same rows as aggregate_frames, partitioned by
    cruise and sorted within each chunk."""
    from stingray_frame_viewer.ingest.aggregate import iter_frame_chunks

    a = _path("CRUISE_A", "Cam1", "va")
    b = _path("CRUISE_B", "Cam1", "vb")
    rows = [
        _row(a, "va", "2026-01-13 19:30:07.771", 1, "2026-01-13 19:30:07.904"),
        _row(a, "va", "2026-01-13 19:30:07.771", 0, "2026-01-13 19:30:07.837"),
        _row(b, "vb", "2026-01-13 19:31:00.000", 0, "2026-01-13 19:31:00.066"),
    ]
    csv = _write_csv(tmp_path, "two_cruises.csv", rows)

    chunks = dict(iter_frame_chunks([csv], ["CRUISE_A", "CRUISE_B"]))
    assert set(chunks) == {"CRUISE_A", "CRUISE_B"}
    # Each chunk holds only its cruise, sorted by (video_id, frame_index).
    assert chunks["CRUISE_A"]["frame_index"].to_list() == [0, 1]
    assert chunks["CRUISE_A"]["cruise"].unique().to_list() == ["CRUISE_A"]
    assert chunks["CRUISE_B"]["frame_index"].to_list() == [0]
    total = sum(c.height for c in chunks.values())
    assert total == aggregate_frames([csv]).height


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
    assert count_excluded_videos([csv]).bad_file == 1
    videos = aggregate_videos([csv])
    assert videos["video_id"].to_list() == ["good"]
    frames = aggregate_frames([csv])
    assert frames["video_id"].to_list() == ["good", "good"]


def test_skip_directory_videos_excluded(tmp_path):
    """A video parked under a skip/ directory is excluded from the manifest and
    never reaches the cruise/camera parser (the extra segment would misparse it)."""
    good = _path("NESLTER_AR88", "Basler_avA2300-25gm", "good")
    # Real-world shape from the data: an extra 'skip' segment after the camera.
    skipped = (
        "/proj/nes-lter/Stingray/data/NESLTER_AR88/Basler_a2a2840-14gmBAS/skip/"
        "20250424T143241.787Z/Basler_a2a2840-14gmBAS-004-20250424T143435.560Z.avi"
    )
    rows = [
        _row(good, "good", "2025-04-25 15:22:15.851", 0, "2025-04-25 15:22:15.917"),
        _row(good, "good", "2025-04-25 15:22:15.851", 1, "2025-04-25 15:22:15.984"),
        _row(skipped, "skipvid", "2025-04-24 14:32:41.787", 0, "2025-04-24 14:32:41.853"),
        _row(skipped, "skipvid", "2025-04-24 14:32:41.787", 1, "2025-04-24 14:32:41.920"),
    ]
    csv = _write_csv(tmp_path, "with_skip.csv", rows)
    assert count_excluded_videos([csv]).skipped == 1
    # The skip video is absent from both tables — no phantom cruise/camera.
    assert aggregate_videos([csv])["video_id"].to_list() == ["good"]
    assert set(aggregate_frames([csv])["video_id"].to_list()) == {"good"}


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


@pytest.fixture
def mixed_csvs(tmp_path):
    """One old-format Stingray CSV plus one LRAUV-format CSV."""
    old = _write_csv(tmp_path, "old.csv", [
        _row(_path("NESLTER_AR99", "Cam1", "vold"), "vold", "2026-01-13 19:30:07.771", 0,
             "2026-01-13 19:30:07.837"),
        _row(_path("NESLTER_AR99", "Cam1", "vold"), "vold", "2026-01-13 19:30:07.771", 1,
             "2026-01-13 19:30:07.904"),
    ])
    lp = _lrauv_path("LRAUV LTER 20240212", "pixys_isiis_camera", "vnew")
    new = tmp_path / "lrauv.csv"
    new.write_text("\n".join([
        LRAUV_HEADER,
        _lrauv_row(lp, "vnew", "2024-02-12 14:34:27.063", 0, "2024-02-12 14:34:27.063"),
        _lrauv_row(lp, "vnew", "2024-02-12 14:34:27.063", 1, "2024-02-12 14:34:27.130"),
    ]) + "\n")
    return old, str(new)


@pytest.mark.parametrize("reverse", [False, True])
def test_mixed_csv_formats(mixed_csvs, reverse):
    """Regression: a single multi-file scan_csv raised 'schema names differ' when
    headers differed. Reading by column name must handle either file order."""
    paths = list(reversed(mixed_csvs)) if reverse else list(mixed_csvs)
    df = aggregate_videos(paths).sort("video_id")
    assert df["video_id"].to_list() == ["vnew", "vold"]
    assert df["cruise"].to_list() == ["LRAUV_LTER_20240212", "NESLTER_AR99"]
    assert df["camera"].to_list() == ["pixys_isiis_camera", "Cam1"]
    assert df["frame_count"].to_list() == [2, 2]
    assert df["media_time"].to_list()[0].replace(tzinfo=None) == datetime(2024, 2, 12, 14, 34, 27, 63000)
    assert df["media_time"].null_count() == 0
    assert aggregate_frames(paths)["frame_time"].null_count() == 0
    assert count_id_link_nonempty(paths) == 0


def test_lrauv_frame_chunks_use_normalized_cruise(mixed_csvs):
    from stingray_frame_viewer.ingest.aggregate import iter_frame_chunks

    chunks = dict(iter_frame_chunks(list(mixed_csvs), ["LRAUV_LTER_20240212"]))
    chunk = chunks["LRAUV_LTER_20240212"]
    assert chunk["video_id"].to_list() == ["vnew", "vnew"]
    assert chunk["frame_index"].to_list() == [0, 1]
    assert chunk["status"].to_list() == ["valid", "valid"]


def test_missing_required_column_names_file(tmp_path):
    p = tmp_path / "no_status.csv"
    p.write_text(
        "media_path,media,media_time,frame,times\n"
        f"{_path('C', 'Cam', 'v')},v,2026-01-13 19:30:07.771,0,2026-01-13 19:30:07.837\n"
    )
    with pytest.raises(ValueError, match=r"no_status\.csv: missing required column\(s\): status"):
        aggregate_videos([str(p)])
