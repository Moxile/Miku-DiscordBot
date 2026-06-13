"""Pillow rendering of a static European roulette wheel with the ball in the result pocket."""
from __future__ import annotations

import io
import math
from functools import lru_cache

from PIL import Image, ImageDraw

from .render_utils import load_font

# Real single-zero wheel sequence, clockwise from 0.
WHEEL_ORDER = [
    0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 30, 8, 23, 10,
    5, 24, 16, 33, 1, 20, 14, 31, 9, 22, 18, 29, 7, 28, 12, 35, 3, 26,
]
RED_NUMBERS = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}
N = len(WHEEL_ORDER)
SECTOR = 360 / N

SS = 2
SIZE = 460
HS = SIZE * SS
CX = CY = HS // 2

# radii (hi-res px, from center)
R_RIM_OUT = 452
R_RIM_IN = 412
R_POCKET = 406
R_HUB = 250
R_BALL_TRACK = 388
R_NUM = 332
R_CENTER = 150

# colors
BG = (24, 60, 46)
POCKET_GREEN = (30, 130, 76)
POCKET_RED = (200, 40, 45)
POCKET_BLACK = (28, 28, 32)
RIM = (110, 72, 38)
GOLD = (212, 175, 96)
SEP = (225, 225, 228)
HUB = (52, 40, 28)
WHITE = (245, 245, 245)
SHADOW = (0, 0, 0, 110)


def _pocket_color(number: int):
    if number == 0:
        return POCKET_GREEN
    return POCKET_RED if number in RED_NUMBERS else POCKET_BLACK


def _angle(idx: int) -> float:
    """Center angle (degrees) of order position idx, with pocket 0 at the top."""
    return idx * SECTOR - 90


@lru_cache(maxsize=1)
def _base_wheel() -> Image.Image:
    """The wheel itself (rim, colored pockets, numbers, hub) — independent of the result."""
    img = Image.new("RGBA", (HS, HS), BG + (255,))
    d = ImageDraw.Draw(img)

    # wooden rim with gold edges
    d.ellipse([CX - R_RIM_OUT, CY - R_RIM_OUT, CX + R_RIM_OUT, CY + R_RIM_OUT], fill=RIM)
    d.ellipse([CX - R_RIM_OUT, CY - R_RIM_OUT, CX + R_RIM_OUT, CY + R_RIM_OUT], outline=GOLD, width=4 * SS)
    d.ellipse([CX - R_RIM_IN, CY - R_RIM_IN, CX + R_RIM_IN, CY + R_RIM_IN], outline=GOLD, width=3 * SS)

    # colored pocket wedges (drawn full to centre, then hub circle covers the middle)
    box = [CX - R_POCKET, CY - R_POCKET, CX + R_POCKET, CY + R_POCKET]
    for i, number in enumerate(WHEEL_ORDER):
        a = _angle(i)
        d.pieslice(box, a - SECTOR / 2, a + SECTOR / 2, fill=_pocket_color(number),
                   outline=SEP, width=max(1, SS // 2))

    # numbers, rotated to sit radially
    font = load_font(34)
    for i, number in enumerate(WHEEL_ORDER):
        a = _angle(i)
        rad = math.radians(a)
        tile = Image.new("RGBA", (70, 50), (0, 0, 0, 0))
        td = ImageDraw.Draw(tile)
        s = str(number)
        bbox = td.textbbox((0, 0), s, font=font)
        td.text(((70 - (bbox[2] - bbox[0])) / 2 - bbox[0], (50 - (bbox[3] - bbox[1])) / 2 - bbox[1]),
                s, font=font, fill=WHITE)
        tile = tile.rotate(-(a + 90), resample=Image.BICUBIC, expand=True)
        x = CX + R_NUM * math.cos(rad)
        y = CY + R_NUM * math.sin(rad)
        img.alpha_composite(tile, (int(x - tile.width / 2), int(y - tile.height / 2)))

    # hub
    d.ellipse([CX - R_HUB, CY - R_HUB, CX + R_HUB, CY + R_HUB], fill=HUB, outline=GOLD, width=3 * SS)
    return img


def render_wheel(result: int) -> io.BytesIO:
    """Render the wheel with the ball resting in `result`'s pocket. Returns a PNG BytesIO."""
    img = _base_wheel().copy()
    d = ImageDraw.Draw(img)

    idx = WHEEL_ORDER.index(result)
    rad = math.radians(_angle(idx))
    bx = CX + R_BALL_TRACK * math.cos(rad)
    by = CY + R_BALL_TRACK * math.sin(rad)
    br = 15 * SS
    shadow = Image.new("RGBA", (HS, HS), (0, 0, 0, 0))
    ImageDraw.Draw(shadow).ellipse([bx - br + 3, by - br + 4, bx + br + 3, by + br + 4], fill=SHADOW)
    img.alpha_composite(shadow)
    d.ellipse([bx - br, by - br, bx + br, by + br], fill=WHITE, outline=(180, 180, 180), width=SS)
    d.ellipse([bx - br * 0.45, by - br * 0.7, bx + br * 0.1, by - br * 0.15], fill=(255, 255, 255))

    # centre readout: result number on a disc tinted by its colour
    color = _pocket_color(result)
    d.ellipse([CX - R_CENTER, CY - R_CENTER, CX + R_CENTER, CY + R_CENTER], fill=color, outline=GOLD, width=3 * SS)
    big = load_font(96)
    s = str(result)
    bbox = d.textbbox((0, 0), s, font=big)
    d.text((CX - (bbox[2] - bbox[0]) / 2 - bbox[0], CY - (bbox[3] - bbox[1]) / 2 - bbox[1] - 14),
           s, font=big, fill=WHITE)
    label = "GREEN" if result == 0 else ("RED" if result in RED_NUMBERS else "BLACK")
    small = load_font(30)
    lb = d.textbbox((0, 0), label, font=small)
    d.text((CX - (lb[2] - lb[0]) / 2 - lb[0], CY + 60), label, font=small, fill=WHITE)

    out = img.convert("RGB").resize((SIZE, SIZE), Image.LANCZOS)
    buf = io.BytesIO()
    out.save(buf, format="PNG")
    buf.seek(0)
    return buf
