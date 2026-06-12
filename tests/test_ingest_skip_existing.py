"""End-to-end tests for the ingest CLI's append-only / --skip-existing behavior.

These drive ``main()`` against a real on-disk DuckDBParquetStore (tmp_path) so
they exercise the partition-conflict logic and the frames-partition filtering
that keeps a partially-existing cruise from re-appending its existing camera's
frames.
"""
from __future__ import annotations

import collections

from stingray_frame_viewer.ingest.__main__ import main
from stingray_frame_viewer.manifest import open_store

HEADER = "media_path,media,media_time,frame,times,status,id,link"
ROOT = "/proj/nes-lter/Stingray/data"

A = f"{ROOT}/NESLTER_AR88/CamA/20250101T000000.000Z/a.avi"  # existing cruise+camera
B = f"{ROOT}/NESLTER_AR88/CamB/20250102T000000.000Z/b.avi"  # new camera, existing cruise
C = f"{ROOT}/NESLTER_AR99/CamA/20250103T000000.000Z/c.avi"  # new cruise


def _row(media_path: str, media: str, frame: int) -> str:
    return f"{media_path},{media},2025-01-01 00:00:00.000,{frame},2025-01-01 00:00:00.066,ok,x,y"


def _csv(tmp_path, name: str, rows: list[str]) -> str:
    p = tmp_path / name
    p.write_text("\n".join([HEADER, *rows]) + "\n")
    return str(p)


def _frame_counts(store_root: str):
    store = open_store(store_root)
    vids = {
        (r["cruise"], r["camera"], r["video_id"])
        for r in store.bulk_read("videos").to_pylist()
    }
    frames = collections.Counter(
        (r["cruise"], r["video_id"]) for r in store.bulk_read("frames").to_pylist()
    )
    return vids, frames


def test_skip_existing_writes_only_new_partitions(tmp_path):
    store_root = str(tmp_path / "store")

    # Seed: AR88/CamA already in the manifest (vidA, 2 frames).
    seed = _csv(tmp_path, "seed.csv", [_row(A, "vidA", 0), _row(A, "vidA", 1)])
    assert main(["--csv", seed, "--store-root", store_root, "--frames"]) == 0

    # Full corpus: existing AR88/CamA + new AR88/CamB (partial cruise) + new AR99/CamA.
    full = _csv(
        tmp_path,
        "all.csv",
        [
            _row(A, "vidA", 0), _row(A, "vidA", 1),
            _row(B, "vidB", 0), _row(B, "vidB", 1), _row(B, "vidB", 2),
            _row(C, "vidC", 0),
        ],
    )

    # Default is all-or-nothing: any conflict refuses the whole run.
    assert main(["--csv", full, "--store-root", store_root, "--frames"]) == 2

    # --skip-existing writes only the new partitions.
    assert main(["--csv", full, "--store-root", store_root, "--frames", "--skip-existing"]) == 0

    vids, frames = _frame_counts(store_root)
    assert vids == {
        ("NESLTER_AR88", "CamA", "vidA"),
        ("NESLTER_AR88", "CamB", "vidB"),
        ("NESLTER_AR99", "CamA", "vidC"),
    }
    # The existing camera's frames must NOT be re-appended into the cruise's
    # (cruise-keyed) frame partition: vidA stays at 2, not 4.
    assert frames[("NESLTER_AR88", "vidA")] == 2
    assert frames[("NESLTER_AR88", "vidB")] == 3
    assert frames[("NESLTER_AR99", "vidC")] == 1


def test_skip_existing_is_idempotent(tmp_path):
    store_root = str(tmp_path / "store")
    csv = _csv(tmp_path, "one.csv", [_row(C, "vidC", 0), _row(C, "vidC", 1)])

    assert main(["--csv", csv, "--store-root", store_root, "--frames", "--skip-existing"]) == 0
    # Re-running with everything already present writes nothing and still succeeds.
    assert main(["--csv", csv, "--store-root", store_root, "--frames", "--skip-existing"]) == 0

    _vids, frames = _frame_counts(store_root)
    assert frames[("NESLTER_AR99", "vidC")] == 2  # not doubled to 4
