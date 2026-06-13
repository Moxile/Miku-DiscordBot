"""Pillow rendering of betflip results as a row of coins (won = green ring, lost = red ring)."""
from __future__ import annotations

import io
import math
from functools import lru_cache

from PIL import Image, ImageDraw

from .render_utils import load_font

SS = 2
COIN_D = 84
GAP = 16
MARGIN = 30
PER_ROW = 10
MAX_COINS = 50          # cap on coins actually drawn; tally still counts every flip

FELT = (29, 105, 74)
GOLD = (230, 185, 70)
GOLD_DARK = (168, 128, 42)
LETTER = (92, 62, 22)
WIN = (70, 190, 100)
LOSS = (214, 64, 64)
TEXT = (242, 242, 242)


@lru_cache(maxsize=8)
def _coin(face: str, won: bool) -> Image.Image:
    D = COIN_D * SS
    img = Image.new("RGBA", (D, D), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    ring = WIN if won else LOSS
    d.ellipse([0, 0, D - 1, D - 1], fill=ring)
    inset = int(D * 0.12)
    d.ellipse([inset, inset, D - 1 - inset, D - 1 - inset], fill=GOLD, outline=GOLD_DARK, width=2 * SS)
    d.ellipse([int(D * 0.24), int(D * 0.24), int(D * 0.76), int(D * 0.76)], outline=GOLD_DARK, width=2 * SS)
    font = load_font(int(D * 0.42))
    bbox = d.textbbox((0, 0), face, font=font)
    d.text((D / 2 - (bbox[2] - bbox[0]) / 2 - bbox[0], D / 2 - (bbox[3] - bbox[1]) / 2 - bbox[1]),
           face, font=font, fill=LETTER)
    return img.resize((COIN_D, COIN_D), Image.LANCZOS)


def render_coins(choice: str, flips, wins: int, losses: int) -> io.BytesIO:
    """`choice` and each flip are "H"/"T"; wins/losses count every flip (not just drawn ones)."""
    choice = choice.upper()
    shown = flips[:MAX_COINS]
    extra = len(flips) - len(shown)

    cols = min(len(shown), PER_ROW) or 1
    rows = max(1, math.ceil(len(shown) / PER_ROW))
    header_h, summary_h = 46, 44
    note_h = 30 if extra else 0

    width = max(MARGIN * 2 + cols * COIN_D + (cols - 1) * GAP, 360)
    height = MARGIN + header_h + rows * COIN_D + (rows - 1) * GAP + summary_h + note_h + MARGIN

    img = Image.new("RGB", (width, height), FELT)
    d = ImageDraw.Draw(img)

    def centered(text, y, font, fill=TEXT):
        bbox = d.textbbox((0, 0), text, font=font)
        d.text((width / 2 - (bbox[2] - bbox[0]) / 2 - bbox[0], y), text, font=font, fill=fill)

    centered(f"Your call: {'HEADS' if choice == 'H' else 'TAILS'}", MARGIN, load_font(28))

    top = MARGIN + header_h
    for i, face in enumerate(shown):
        r, c = divmod(i, PER_ROW)
        row_n = min(len(shown) - r * PER_ROW, PER_ROW)
        row_w = row_n * COIN_D + (row_n - 1) * GAP
        x0 = (width - row_w) // 2
        x = x0 + c * (COIN_D + GAP)
        y = top + r * (COIN_D + GAP)
        coin = _coin(face, face == choice)
        img.paste(coin, (x, y), coin)

    y = top + rows * COIN_D + (rows - 1) * GAP + 8
    if extra:
        centered(f"+{extra} more flips", y, load_font(22))
        y += note_h
    centered(f"{wins} W   /   {losses} L", y, load_font(28))

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf
