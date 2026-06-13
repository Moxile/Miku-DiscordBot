"""Pillow rendering of the static roulette betting layout (the felt board)."""
from __future__ import annotations

import io
from functools import lru_cache

from PIL import Image, ImageDraw

from .render_utils import load_font
from .wheel import RED_NUMBERS

SS = 2
CELL = 60 * SS
ZERO_W = CELL
COL21_W = int(CELL * 1.1)
MARGIN = 22 * SS

FELT = (16, 78, 56)
CELL_GREEN = (24, 96, 70)
NUM_RED = (200, 40, 45)
NUM_BLACK = (30, 30, 34)
ZERO_GREEN = (32, 138, 80)
BORDER = (235, 235, 235)
TEXT = (245, 245, 245)


def _num_color(n: int):
    if n == 0:
        return ZERO_GREEN
    return NUM_RED if n in RED_NUMBERS else NUM_BLACK


def render_board() -> io.BytesIO:
    """A fresh PNG BytesIO of the (cached) betting layout — safe to attach repeatedly."""
    return io.BytesIO(_board_png())


@lru_cache(maxsize=1)
def _board_png() -> bytes:
    grid_w = 12 * CELL
    width = MARGIN * 2 + ZERO_W + grid_w + COL21_W
    height = MARGIN * 2 + 3 * CELL + CELL + CELL  # numbers + dozens + outside

    img = Image.new("RGB", (width, height), FELT)
    d = ImageDraw.Draw(img)
    num_font = load_font(int(CELL * 0.42))
    lbl_font = load_font(int(CELL * 0.34))

    def cell(box, fill, text=None, font=lbl_font, color=TEXT):
        d.rectangle(box, fill=fill, outline=BORDER, width=max(1, SS))
        if text:
            bb = d.textbbox((0, 0), text, font=font)
            cx = (box[0] + box[2]) / 2 - (bb[2] - bb[0]) / 2 - bb[0]
            cy = (box[1] + box[3]) / 2 - (bb[3] - bb[1]) / 2 - bb[1]
            d.text((cx, cy), text, font=font, fill=color)

    x0, y0 = MARGIN, MARGIN

    # 0 cell (spans the three number rows)
    cell([x0, y0, x0 + ZERO_W, y0 + 3 * CELL], ZERO_GREEN, "0", font=num_font)

    # number grid: column j -> top 3(j+1), middle 3(j+1)-1, bottom 3(j+1)-2
    gx = x0 + ZERO_W
    for j in range(12):
        for row, offset in enumerate((0, 1, 2)):  # top, middle, bottom
            n = 3 * (j + 1) - offset
            bx = gx + j * CELL
            by = y0 + row * CELL
            cell([bx, by, bx + CELL, by + CELL], _num_color(n), str(n), font=num_font)

    # 2:1 column-bet cells on the right
    cx0 = gx + grid_w
    for row in range(3):
        by = y0 + row * CELL
        cell([cx0, by, cx0 + COL21_W, by + CELL], CELL_GREEN, "2:1")

    # dozens row
    dy = y0 + 3 * CELL
    for k, label in enumerate(("1st 12", "2nd 12", "3rd 12")):
        bx = gx + k * 4 * CELL
        cell([bx, dy, bx + 4 * CELL, dy + CELL], CELL_GREEN, label)

    # bottom outside row: 1-18, EVEN, RED, BLACK, ODD, 19-36 (each spans 2 columns)
    oy = dy + CELL
    specs = [("1-18", CELL_GREEN), ("EVEN", CELL_GREEN), ("red", NUM_RED),
             ("black", NUM_BLACK), ("ODD", CELL_GREEN), ("19-36", CELL_GREEN)]
    for k, (label, fill) in enumerate(specs):
        bx = gx + k * 2 * CELL
        box = [bx, oy, bx + 2 * CELL, oy + CELL]
        if label in ("red", "black"):
            d.rectangle(box, fill=CELL_GREEN, outline=BORDER, width=max(1, SS))
            cxm = (box[0] + box[2]) / 2
            cym = (box[1] + box[3]) / 2
            r = CELL * 0.34
            d.polygon([(cxm, cym - r), (cxm + r, cym), (cxm, cym + r), (cxm - r, cym)], fill=fill)
        else:
            cell(box, fill, label)

    out = img.resize((width // SS, height // SS), Image.LANCZOS)
    buf = io.BytesIO()
    out.save(buf, format="PNG")
    return buf.getvalue()
