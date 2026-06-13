"""Shared rendering helpers for the gambling visuals (cards, wheel, coins)."""
from __future__ import annotations

import os
from functools import lru_cache

from PIL import ImageFont

_FONT_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/Library/Fonts/Arial Bold.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
]


@lru_cache(maxsize=None)
def load_font(size: int) -> ImageFont.FreeTypeFont:
    """A bold TTF at the given size, falling back to Pillow's default font."""
    for path in _FONT_CANDIDATES:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                continue
    try:
        return ImageFont.load_default(size)
    except TypeError:  # Pillow < 10.1 — unsized bitmap default
        return ImageFont.load_default()
