"""Confirm Stingray AVI properties before relying on them in extractor code.

Usage:
    python scripts/inspect_avi.py /path/to/sample.avi

Reads the file with cv2.VideoCapture and prints frame count, reported FPS,
container-reported width/height, and the dtype/shape/channel layout of the
first decoded frame. Used once during M1 (DESIGN §7 bit-depth confirmation).
"""
from __future__ import annotations

import argparse
import sys

import cv2
import numpy as np


def inspect(path: str) -> int:
    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        print(f"ERROR: could not open {path}", file=sys.stderr)
        return 1

    try:
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        fourcc_int = int(cap.get(cv2.CAP_PROP_FOURCC))
        fourcc = "".join(chr((fourcc_int >> (8 * i)) & 0xFF) for i in range(4))

        print(f"path:        {path}")
        print(f"fourcc:      {fourcc!r}")
        print(f"reported:    {width}x{height}, {frame_count} frames, {fps:g} fps")

        ok, frame = cap.read()
        if not ok or frame is None:
            print("ERROR: could not decode the first frame", file=sys.stderr)
            return 2

        print(f"frame.shape: {frame.shape}")
        print(f"frame.dtype: {frame.dtype}")
        if frame.ndim == 3:
            channels = frame.shape[2]
            print(f"channels:    {channels}")
            if channels >= 3:
                b, g, r = frame[..., 0], frame[..., 1], frame[..., 2]
                ident_bg = bool(np.array_equal(b, g))
                ident_gr = bool(np.array_equal(g, r))
                print(f"B==G:        {ident_bg}")
                print(f"G==R:        {ident_gr}")
                if ident_bg and ident_gr:
                    print("interpretation: grayscale broadcast across 3 channels")
        else:
            print("channels:    1 (single-channel)")

        print(f"min/max:     {int(frame.min())} / {int(frame.max())}")
    finally:
        cap.release()

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("path", help="Path to an AVI file")
    args = parser.parse_args()
    return inspect(args.path)


if __name__ == "__main__":
    sys.exit(main())
