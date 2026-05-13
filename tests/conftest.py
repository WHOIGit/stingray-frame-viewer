"""Shared pytest fixtures.

The synthetic-AVI fixture uses the FFV1 lossless codec. The real Stingray
AVIs are technically uncompressed (empty fourcc) rather than FFV1-compressed,
but both are independently-decodable-per-frame and exercise the same
``cv2.VideoCapture`` seek+decode path. FFV1 is portable across the macOS and
Linux opencv-python wheels we test against; truly uncompressed writing via
``cv2.VideoWriter`` is not reliably supported.

The ``routes_app`` / ``client`` fixtures bypass FastAPI's lifespan and wire a
fake in-memory manifest onto ``app.state`` directly. This keeps route tests
independent of the real DuckDBParquetStore.
"""
from __future__ import annotations

from datetime import datetime, timezone

import cv2
import numpy as np
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from stingray_frame_viewer.config import Settings
from stingray_frame_viewer.errors import install_handlers
from stingray_frame_viewer.models import Video
from stingray_frame_viewer.routes import router as routes_router


SYNTHETIC_FRAMES = 10
SYNTHETIC_H = 64
SYNTHETIC_W = 96


def _synthetic_frame(i: int) -> np.ndarray:
    """Distinguishable 2D uint8 frame for index ``i`` (0..255 gray gradient)."""
    return np.full((SYNTHETIC_H, SYNTHETIC_W), (i + 1) * 20, dtype=np.uint8)


@pytest.fixture
def synthetic_avi(tmp_path) -> str:
    """Build a small lossless grayscale AVI with frames that round-trip byte-exact."""
    path = tmp_path / "synth.avi"
    fourcc = cv2.VideoWriter_fourcc(*"FFV1")
    writer = cv2.VideoWriter(str(path), fourcc, 10, (SYNTHETIC_W, SYNTHETIC_H), isColor=False)
    if not writer.isOpened():
        pytest.skip("FFV1 codec not available in this OpenCV build")
    try:
        for i in range(SYNTHETIC_FRAMES):
            writer.write(_synthetic_frame(i))
    finally:
        writer.release()
    return str(path)


@pytest.fixture
def expected_frame():
    """Helper to construct the expected synthetic frame for a given index."""
    return _synthetic_frame


TEST_VIDEO_ID = "vid-1"


@pytest.fixture
def fake_manifest(synthetic_avi) -> dict[str, Video]:
    """Single-video manifest backed by the synthetic AVI fixture."""
    return {
        TEST_VIDEO_ID: Video(
            video_id=TEST_VIDEO_ID,
            media_path=synthetic_avi,
            frame_count=10,
            media_time=datetime(2024, 5, 3, 19, 11, 12, 333000, tzinfo=timezone.utc),
            cruise="NESLTER_TEST",
            camera="Cam0",
        ),
    }


@pytest.fixture
def routes_app(fake_manifest) -> FastAPI:
    """Bare FastAPI app wired up like ``create_app()`` but without lifespan.

    ``app.state`` is populated manually so tests don't need to reach into the
    real DuckDBParquetStore.
    """
    app = FastAPI()
    app.include_router(routes_router)
    install_handlers(app)
    app.state.manifest = fake_manifest
    app.state.settings = Settings(store_root="unused", jpeg_quality=90, default_format="png")
    return app


@pytest.fixture
def client(routes_app) -> TestClient:
    return TestClient(routes_app)
