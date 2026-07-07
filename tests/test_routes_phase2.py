"""Phase 2 route tests: the lazy VAST S3 write-through cache.

These mirror ``test_routes_phase1.py`` but wire a fake cache onto the route's
``get_cache`` dependency. The fake duck-types ``FrameCache``'s public surface
(``probe`` / ``get`` / ``put``) with an in-memory dict, and records calls so we
can assert the miss → extract → write-through and hit → serve-from-cache paths
without touching S3.

The route only ever calls those three methods, so a real ``BucketStore`` /
boto3 client is never needed here.
"""
from __future__ import annotations

from stingray_frame_viewer.cache import cache_key
from stingray_frame_viewer.encoder import encode
from stingray_frame_viewer.routes import get_cache

TEST_VIDEO_ID = "vid-1"
IMMUTABLE = "public, max-age=31536000, immutable"


class FakeCache:
    """In-memory stand-in for ``FrameCache`` that records interactions."""

    def __init__(self, seed: dict[str, bytes] | None = None) -> None:
        self.store: dict[str, bytes] = dict(seed or {})
        self.probes: list[str] = []
        self.gets: list[str] = []
        self.puts: list[str] = []
        self.get_returns_none = False  # simulate a read that fails after a hit

    def probe(self, key: str) -> bool:
        self.probes.append(key)
        return key in self.store

    def get(self, key: str) -> bytes | None:
        self.gets.append(key)
        if self.get_returns_none:
            return None
        return self.store.get(key)

    def put(self, key: str, body: bytes) -> None:
        self.puts.append(key)
        self.store[key] = body


def _use_cache(app, cache: FakeCache) -> None:
    app.dependency_overrides[get_cache] = lambda: cache


def test_cache_miss_extracts_and_writes_through(client, routes_app, expected_frame):
    cache = FakeCache()
    _use_cache(routes_app, cache)

    r = client.get(f"/frames/{TEST_VIDEO_ID}/0")

    assert r.status_code == 200
    expected = encode(expected_frame(0), "png")
    assert r.content == expected
    assert r.headers["content-type"] == "image/png"
    assert r.headers["cache-control"] == IMMUTABLE

    key = cache_key(TEST_VIDEO_ID, 0, "png")
    assert cache.probes == [key]          # probed once
    assert cache.gets == []               # miss → never read
    assert cache.puts == [key]            # wrote the freshly encoded frame
    assert cache.store[key] == expected   # ...and stored the exact bytes served


def test_cache_hit_serves_stored_bytes_without_extracting(client, routes_app):
    key = cache_key(TEST_VIDEO_ID, 0, "png")
    # Sentinel bytes that are NOT what the encoder would produce, so a match
    # proves the response came from the cache rather than a fresh extraction.
    sentinel = b"\x89PNG\r\n\x1a\n--served-from-cache-sentinel--"
    cache = FakeCache(seed={key: sentinel})
    _use_cache(routes_app, cache)

    r = client.get(f"/frames/{TEST_VIDEO_ID}/0")

    assert r.status_code == 200
    assert r.content == sentinel                      # served from cache
    assert r.headers["content-type"] == "image/png"
    assert r.headers["cache-control"] == IMMUTABLE
    assert cache.probes == [key]
    assert cache.gets == [key]
    assert cache.puts == []                           # nothing written on a hit


def test_cache_get_failure_falls_back_to_extraction(client, routes_app, expected_frame):
    key = cache_key(TEST_VIDEO_ID, 0, "png")
    cache = FakeCache(seed={key: b"stale-unreadable"})
    cache.get_returns_none = True  # probe says hit, but the read comes back empty
    _use_cache(routes_app, cache)

    r = client.get(f"/frames/{TEST_VIDEO_ID}/0")

    assert r.status_code == 200
    assert r.content == encode(expected_frame(0), "png")  # re-extracted, not stale
    assert cache.probes == [key]
    assert cache.gets == [key]
    assert cache.puts == [key]                            # re-written after fallback


def test_cache_jpeg_uses_separate_key(client, routes_app):
    png_key = cache_key(TEST_VIDEO_ID, 0, "png")
    cache = FakeCache(seed={png_key: b"a-png-object"})
    _use_cache(routes_app, cache)

    r = client.get(f"/frames/{TEST_VIDEO_ID}/0", params={"format": "jpeg"})

    assert r.status_code == 200
    assert r.content.startswith(b"\xff\xd8\xff")  # real JPEG, freshly encoded
    jpeg_key = cache_key(TEST_VIDEO_ID, 0, "jpeg")
    assert jpeg_key == f"{TEST_VIDEO_ID}_0.jpg"
    assert jpeg_key != png_key
    # The seeded PNG must not satisfy a JPEG request: distinct key → a miss →
    # written under the JPEG key, leaving the PNG object untouched.
    assert cache.puts == [jpeg_key]
    assert cache.store[png_key] == b"a-png-object"


def test_cache_disabled_behaves_like_phase1(client, expected_frame):
    # No get_cache override and the routes_app fixture never sets app.state.cache,
    # so get_cache() returns None and the route serves purely on the fly.
    r = client.get(f"/frames/{TEST_VIDEO_ID}/0")
    assert r.status_code == 200
    assert r.content == encode(expected_frame(0), "png")
