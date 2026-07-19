"""Pillow rendering of a company's share-price history as a line chart."""
from __future__ import annotations

import io

from PIL import Image, ImageDraw

from cogs.gambling.render_utils import load_font

SS = 2  # supersample factor for crisp downscaling

WIDTH, HEIGHT = 760, 380
ML, MR, MT, MB = 78, 26, 60, 46  # plot margins (left/right/top/bottom)

BG = (22, 24, 31)
PANEL = (29, 32, 41)
GRID = (52, 56, 68)
TEXT = (228, 230, 236)
SUBTEXT = (146, 150, 162)
UP = (66, 196, 124)
DOWN = (224, 84, 84)
MARKER_RING = (255, 255, 255)
ZERO_LINE = (120, 124, 138)


def _dashed_hline(d, x0, x1, y, fill, width, dash, gap):
    x = x0
    while x < x1:
        d.line([x, y, min(x + dash, x1), y], fill=fill, width=width)
        x += dash + gap


def render_price_chart(name: str, points, period_label: str | None = None, zero_line: bool = False) -> io.BytesIO:
    """Render a price line chart to a PNG BytesIO.

    `points` is a chronological list of (datetime, price). The line is drawn green when the
    latest price is at or above the first, red otherwise. `period_label` (e.g. "Past 7 days")
    is drawn as a small subtitle under the company name when provided. `zero_line` draws a
    dashed reference line (and axis label) at 0, and forces 0 into the plotted range even if
    every point is on one side of it — for charts like gambling P/L where "above/below zero"
    is the whole point, not just an incidental gridline value.
    """
    w, h = WIDTH * SS, HEIGHT * SS
    ml, mr, mt, mb = ML * SS, MR * SS, MT * SS, MB * SS

    img = Image.new("RGB", (w, h), BG)
    d = ImageDraw.Draw(img)

    x0, y0, x1, y1 = ml, mt, w - mr, h - mb
    pw, ph = x1 - x0, y1 - y0
    d.rectangle([x0, y0, x1, y1], fill=PANEL)

    prices = [p for _, p in points]
    lo, hi = min(prices), max(prices)
    if zero_line:
        lo, hi = min(lo, 0), max(hi, 0)
    if lo == hi:  # flat history — pad so the line sits mid-panel
        pad = max(1, abs(hi) // 10)
        lo, hi = lo - pad, hi + pad
    else:
        margin = (hi - lo) * 0.10
        lo, hi = lo - margin, hi + margin
    rng = hi - lo or 1

    n = len(points)

    def fx(i):
        return x0 + (pw / 2 if n == 1 else i / (n - 1) * pw)

    def fy(v):
        return y1 - (v - lo) / rng * ph

    font_axis = load_font(17 * SS)

    # horizontal gridlines + price labels
    for t in range(5):
        gy = y0 + t / 4 * ph
        d.line([x0, gy, x1, gy], fill=GRID, width=SS)
        val = hi - t / 4 * rng
        lab = f"{int(round(val)):,}"
        bb = d.textbbox((0, 0), lab, font=font_axis)
        d.text((x0 - 12 * SS - (bb[2] - bb[0]), gy - (bb[3] - bb[1]) / 2 - bb[1]),
               lab, font=font_axis, fill=SUBTEXT)

    if zero_line and lo <= 0 <= hi:
        zy = y1 - (0 - lo) / rng * ph
        _dashed_hline(d, x0, x1, zy, fill=ZERO_LINE, width=SS, dash=14 * SS, gap=10 * SS)
        bb = d.textbbox((0, 0), "0", font=font_axis)
        d.text((x0 - 12 * SS - (bb[2] - bb[0]), zy - (bb[3] - bb[1]) / 2 - bb[1]),
               "0", font=font_axis, fill=TEXT)

    up = prices[-1] >= prices[0]
    col = UP if up else DOWN
    pts = [(fx(i), fy(p)) for i, p in enumerate(prices)]

    # translucent area fill under the line
    overlay = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    area = [(pts[0][0], y1)] + pts + [(pts[-1][0], y1)]
    ImageDraw.Draw(overlay).polygon(area, fill=col + (54,))
    img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")
    d = ImageDraw.Draw(img)

    # price line
    if n == 1:
        d.line([x0, pts[0][1], x1, pts[0][1]], fill=col, width=3 * SS)
    else:
        d.line(pts, fill=col, width=3 * SS, joint="curve")

    # marker on the latest price
    lx, ly = pts[-1]
    r = 5 * SS
    d.ellipse([lx - r, ly - r, lx + r, ly + r], fill=col, outline=MARKER_RING, width=SS)

    # title (truncated if long) + current price / change on the right
    font_title = load_font(25 * SS)
    title = name if len(name) <= 26 else name[:25] + "…"
    d.text((ml, 14 * SS), title, font=font_title, fill=TEXT)
    if period_label:
        d.text((ml, 41 * SS), period_label, font=load_font(14 * SS), fill=SUBTEXT)

    chg = prices[-1] - prices[0]
    pct = (chg / prices[0] * 100) if prices[0] else 0
    info = f"{prices[-1]:,}  {'+' if chg >= 0 else '-'}{abs(pct):.1f}%"
    font_cur = load_font(22 * SS)
    bb = d.textbbox((0, 0), info, font=font_cur)
    d.text((x1 - (bb[2] - bb[0]), 21 * SS), info, font=font_cur, fill=col)

    # x-axis: first and last dates
    font_x = load_font(16 * SS)
    d.text((x0, y1 + 12 * SS), points[0][0].strftime("%b %d"), font=font_x, fill=SUBTEXT)
    last = points[-1][0].strftime("%b %d")
    bb = d.textbbox((0, 0), last, font=font_x)
    d.text((x1 - (bb[2] - bb[0]), y1 + 12 * SS), last, font=font_x, fill=SUBTEXT)

    img = img.resize((WIDTH, HEIGHT), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf
