"""VAST S3 cache via amplify-storage-utils.

Public surface (M5): ``probe(key) -> bool``, ``get(key) -> bytes``,
``put(key, body)``. The cache key shape is ``{video_id}_{frame_index}.{ext}``
(DESIGN §9) and mirrors improv's convention so the bytes path can migrate
later without breaking the public URL contract.

Content-Type is set on the FastAPI response (derived from the cache-key
extension), not on the S3 object — ``BucketStore.put`` does not expose a
content-type parameter and DESIGN guidance is not to reach around the
storage abstraction.

Implemented in M5.
"""
from __future__ import annotations
