"""OpenCV-based AVI frame extraction.

Public surface (M3): ``extract_frame(media_path: str, frame_index: int) -> np.ndarray``
returning a 2D uint8 grayscale array. Uses ``cv2.CAP_PROP_POS_FRAMES``, which
is reliable on Stingray AVIs because they are uncompressed (every frame is a
keyframe).

Implemented in M3. The comment block below records the ground truth measured
by ``scripts/inspect_avi.py`` so future readers don't have to re-derive it.
"""
from __future__ import annotations

# -- Measured AVI properties (scripts/inspect_avi.py, M1) ----------------------
# Sample:  Basler_avA2300-25gm-180-20240910T121738.522Z.avi
#   dimensions:   2330 x 1750  (width x height)
#   dtype:        uint8
#   channels:     3 (single-channel grayscale broadcast: B == G == R)
#   frame count:  900
#   fps:          ~15
#   fourcc:       empty (uncompressed AVI; every frame is independently decodable)
#   value range:  16..255 observed in frame 0
#
# Implication for extractor: cv2.VideoCapture.read() returns (H, W, 3) uint8.
# Convert with cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) to get (H, W) uint8 before
# encoding. Seek via cv2.CAP_PROP_POS_FRAMES is safe (see DESIGN.md).
# ------------------------------------------------------------------------------
