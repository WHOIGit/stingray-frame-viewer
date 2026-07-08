"""VAST S3 cache via amplify-storage-utils.

Public surface (M5): ``probe(key) -> bool``, ``get(key) -> bytes | None``,
``put(key, body)``. The cache key shape is ``{video_id}_{frame_index}.{ext}``
(see DESIGN.md) and the bytes path is intended to migrate to a shared
substrate later without breaking the public URL contract.

Content-Type is set on the FastAPI response (derived from the cache-key
extension / requested format), not on the S3 object — ``BucketStore.put``
does not expose a content-type parameter and DESIGN guidance is not to reach
around the storage abstraction.

All cache operations are best-effort. A probe/get failure is treated as a
miss so the route falls back to on-the-fly extraction; a put failure is
swallowed (logged) so a cache-write hiccup never fails a request.

The ``boto3`` / ``storage.s3`` imports are deferred into ``from_settings`` so
a Phase-1 deployment (cache disabled) doesn't need the S3 stack resolvable at
import time.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .config import Settings

from botocore.exceptions import BotoCoreError, ClientError
if TYPE_CHECKING:  # avoid importing the S3 stack unless the cache is enabled
    from storage.s3 import BucketStore

logger = logging.getLogger(__name__)

# Requested format -> cache-key extension. Kept explicit so PNG and JPEG
# variants live under distinct keys (see DESIGN "Storage and caching").
_EXT = {"png": "png", "jpeg": "jpg"}


def cache_key(video_id: str, frame_index: int, fmt: str) -> str:
    """Stable S3 key for an encoded frame: ``{video_id}_{frame_index}.{ext}``.

    ``video_id`` is the verbatim CSV ``media`` column, which the manifest
    treats as immutable, so the key is stable for the life of the object.
    """
    return f"{video_id}_{frame_index}.{_EXT[fmt]}"


class FrameCache:
    """Thin, best-effort wrapper over ``amplify-storage-utils`` ``BucketStore``.

    This class is the migration surface named in DESIGN: the public
    ``probe``/``get``/``put`` methods stay stable even if the bytes move from
    VAST S3 to a shared substrate later.
    """

    def __init__(self, store: "BucketStore") -> None:
        self._store = store

    @classmethod
    def from_settings(cls, settings: Settings) -> "FrameCache":
        """Build a cache from ``STINGRAY_CACHE_*`` settings.

        VAST S3 is reached via an explicit, path-style endpoint URL. boto3
        still wants a region for SigV4 signing even though VAST ignores it,
        so a placeholder is supplied. The client is constructed here and
        injected into ``BucketStore`` — the storage lib does not build
        clients itself.
        """
        if not settings.cache_bucket:
            raise ValueError(
                "STINGRAY_CACHE_BUCKET is required when STINGRAY_CACHE_ENABLED=true"
            )

        import boto3
        from botocore.config import Config
        from storage.s3 import BucketStore

        client = boto3.client(
            "s3",
            endpoint_url=settings.cache_s3_endpoint,
            aws_access_key_id=settings.cache_s3_access_key,
            aws_secret_access_key=settings.cache_s3_secret_key,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}),
        )
        return cls(BucketStore(settings.cache_bucket, client=client))

    def probe(self, key: str) -> bool:
        """True if ``key`` exists. Any error is treated as a miss."""
        try:
            return self._store.exists(key)
        except Exception:  # noqa: BLE001 - cache is best-effort
            logger.warning("cache probe failed for key=%s", key, exc_info=True)
            return False

    def get(self, key: str) -> bytes | None:
        """Return cached bytes, or ``None`` on a miss / degraded cache.
 
        Three failure tiers, all of which degrade to on-the-fly extraction
        (the Video is the source of truth, so a cache problem must never deny a
        frame we could still produce) but with different loudness:
 
        * ``NoSuchKey`` / ``404`` — the normal cold-cache miss. Silent.
        * any other ``ClientError`` — S3 answered with an error (AccessDenied,
          throttle, 5xx). Logged at ERROR so it surfaces during bring-up.
        * ``BotoCoreError`` — couldn't reach S3 at all (VAST down, DNS,
          connect/read timeout). These do NOT carry a ``.response`` and are a
          distinct subclass from ``ClientError``. Logged at ERROR with a trace.
 
        Anything else (a ``KeyError``/``TypeError`` in our own code) is a bug,
        not a storage condition, so it is deliberately NOT caught — let it
        propagate rather than hide behind a silent ``None``.
 
        Note: a sustained VAST outage silently degrades every request to
        extraction (loud in logs, but nothing is paged).
        """
 
        try:
            return self._store.get(key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                return None  # expected miss — stay quiet
            logger.error("cache get: S3 returned error %s for key=%s", code, key)
            return None  # degrade, but loud
        except BotoCoreError:
            logger.error("cache get: cannot reach S3 for key=%s", key, exc_info=True)
            return None  # VAST down / timeout — degrade, but loud

    def put(self, key: str, body: bytes) -> None:
        """Write ``body`` under ``key``. Failures are swallowed (logged)."""
        try:
            self._store.put(key, body)
        except Exception:  # noqa: BLE001 - cache is best-effort
            logger.warning("cache put failed for key=%s", key, exc_info=True)
