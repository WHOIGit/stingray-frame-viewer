"""Phase 1 route tests: HTTP service without the S3 cache."""
from __future__ import annotations

from stingray_frame_viewer.encoder import encode

TEST_VIDEO_ID = "vid-1"

IMMUTABLE = "public, max-age=31536000, immutable"


def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_get_video_ok(client):
    r = client.get(f"/videos/{TEST_VIDEO_ID}")
    assert r.status_code == 200
    body = r.json()
    assert body["video_id"] == TEST_VIDEO_ID
    assert body["frame_count"] == 10
    assert body["last_frame_index"] == 9
    assert body["cruise"] == "NESLTER_TEST"
    assert body["camera"] == "Cam0"
    assert body["media_basename"] == "synth.avi"
    assert body["media_time"].startswith("2024-05-03T19:11:12")
    assert "media_path" not in body


def test_get_video_missing(client):
    r = client.get("/videos/no-such-video")
    assert r.status_code == 404
    body = r.json()
    assert body["video_id"] == "no-such-video"
    assert "not in manifest" in body["detail"]


def test_get_frame_png_default(client, expected_frame):
    r = client.get(f"/frames/{TEST_VIDEO_ID}/0")
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/png"
    assert r.headers["cache-control"] == IMMUTABLE
    # Byte-equal to encoding the expected synthetic frame directly.
    assert r.content == encode(expected_frame(0), "png")


def test_get_frame_jpeg(client):
    r = client.get(f"/frames/{TEST_VIDEO_ID}/0", params={"format": "jpeg"})
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/jpeg"
    assert r.headers["cache-control"] == IMMUTABLE
    assert r.content.startswith(b"\xff\xd8\xff")


def test_get_frame_out_of_range_high(client):
    r = client.get(f"/frames/{TEST_VIDEO_ID}/10")
    assert r.status_code == 416
    body = r.json()
    assert body["frame_index"] == 10
    assert body["frame_count"] == 10


def test_get_frame_out_of_range_negative(client):
    # FastAPI parses {frame_index} as int — negative values must reach the handler.
    r = client.get(f"/frames/{TEST_VIDEO_ID}/-1")
    assert r.status_code == 416


def test_get_frame_video_missing(client):
    r = client.get("/frames/no-such-video/0")
    assert r.status_code == 404


def test_get_frame_bad_format(client):
    r = client.get(f"/frames/{TEST_VIDEO_ID}/0", params={"format": "webp"})
    assert r.status_code == 400
    assert "webp" in r.json()["detail"]
