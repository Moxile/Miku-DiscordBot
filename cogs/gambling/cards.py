"""Pillow rendering for blackjack: real-looking card faces composited into a table image.

Pure, synchronous rendering — no Discord or DB imports. Suits are drawn as vector shapes
(so we never depend on a font shipping the ♠♥♦♣ glyphs); only rank text uses a font, with a
safe fallback to Pillow's built-in default font.
"""
from __future__ import annotations

import io
from functools import lru_cache

from PIL import Image, ImageDraw

from .render_utils import load_font as _font

# ── Geometry (card faces are drawn at SS× then downscaled for antialiasing) ──
SS = 2
CARD_W, CARD_H = 130, 182
CORNER_R = 14
GAP = 18           # space between cards in a row
MARGIN = 34        # canvas padding
HEADER_H = 72      # top strip reserved for the deck indicator
LABEL_H = 40       # height reserved above each row for its label
ROW_GAP = 26       # vertical space between rows

# ── Colors ──
FELT = (29, 105, 74)
FELT_EDGE = (18, 70, 50)
CARD_BG = (250, 250, 248)
CARD_EDGE = (176, 176, 182)
RED = (196, 30, 45)
BLACK = (33, 33, 38)
BACK_BG = (40, 55, 110)
BACK_LINE = (95, 115, 195)
TEXT = (240, 240, 240)
TEXT_DIM = (200, 210, 205)
HIGHLIGHT = (250, 205, 80)

RED_SUITS = {"♥️", "♦️", "♥", "♦"}


def _suit_kind(suit: str) -> str:
    if suit in ("♥️", "♥"):
        return "heart"
    if suit in ("♦️", "♦"):
        return "diamond"
    if suit in ("♠️", "♠"):
        return "spade"
    return "club"


# ── Vector suit drawing (coords in the card's hi-res pixel space) ──
def _draw_suit(draw: ImageDraw.ImageDraw, suit: str, cx: float, cy: float, s: float, color):
    kind = _suit_kind(suit)
    h = s / 2
    if kind == "heart":
        draw.ellipse([cx - h, cy - s * 0.30, cx, cy + s * 0.08], fill=color)
        draw.ellipse([cx, cy - s * 0.30, cx + h, cy + s * 0.08], fill=color)
        draw.polygon([(cx - h, cy - s * 0.11), (cx + h, cy - s * 0.11), (cx, cy + h)], fill=color)
    elif kind == "diamond":
        w = s * 0.34
        draw.polygon([(cx, cy - h), (cx + w, cy), (cx, cy + h), (cx - w, cy)], fill=color)
    elif kind == "spade":
        # inverted heart + flared stem
        draw.ellipse([cx - h, cy - s * 0.08, cx, cy + s * 0.30], fill=color)
        draw.ellipse([cx, cy - s * 0.08, cx + h, cy + s * 0.30], fill=color)
        draw.polygon([(cx - h, cy + s * 0.11), (cx + h, cy + s * 0.11), (cx, cy - h)], fill=color)
        draw.polygon([(cx, cy + s * 0.05), (cx + s * 0.18, cy + h), (cx - s * 0.18, cy + h)], fill=color)
    else:  # club
        r = s * 0.26
        draw.ellipse([cx - r, cy - h, cx + r, cy - h + 2 * r], fill=color)
        draw.ellipse([cx - s * 0.40, cy - s * 0.06, cx - s * 0.40 + 2 * r, cy - s * 0.06 + 2 * r], fill=color)
        draw.ellipse([cx + s * 0.40 - 2 * r, cy - s * 0.06, cx + s * 0.40, cy - s * 0.06 + 2 * r], fill=color)
        draw.polygon([(cx, cy), (cx + s * 0.18, cy + h), (cx - s * 0.18, cy + h)], fill=color)


def _corner_tile(rank: str, suit: str, color) -> Image.Image:
    """A small transparent tile holding the rank over a tiny pip (for card corners)."""
    w, h = int(CARD_W * SS * 0.32), int(CARD_H * SS * 0.30)
    tile = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(tile)
    font = _font(int(h * 0.42))
    bbox = d.textbbox((0, 0), rank, font=font)
    tw = bbox[2] - bbox[0]
    d.text(((w - tw) / 2 - bbox[0], 0), rank, font=font, fill=color)
    pip = h * 0.34
    _draw_suit(d, suit, w / 2, h * 0.72, pip, color)
    return tile


@lru_cache(maxsize=64)
def _render_card(rank: str, suit: str) -> Image.Image:
    """A single card face at display resolution (CARD_W × CARD_H), RGBA."""
    W, H = CARD_W * SS, CARD_H * SS
    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([0, 0, W - 1, H - 1], radius=CORNER_R * SS, fill=CARD_BG, outline=CARD_EDGE, width=SS)

    color = RED if suit in RED_SUITS else BLACK
    inset = int(W * 0.06)

    tile = _corner_tile(rank, suit, color)
    img.alpha_composite(tile, (inset, inset))
    img.alpha_composite(tile.rotate(180), (W - inset - tile.width, H - inset - tile.height))

    _draw_suit(d, suit, W / 2, H / 2, W * 0.46, color)

    return img.resize((CARD_W, CARD_H), Image.LANCZOS)


@lru_cache(maxsize=1)
def _render_back() -> Image.Image:
    """A patterned card back at display resolution, RGBA."""
    W, H = CARD_W * SS, CARD_H * SS
    base = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(base).rounded_rectangle(
        [0, 0, W - 1, H - 1], radius=CORNER_R * SS, fill=BACK_BG, outline=(225, 225, 230), width=2 * SS
    )

    # diagonal lattice drawn over a copy, then composited back only inside an inset
    # rectangle so the lines never spill past the rounded corners
    lines = base.copy()
    ld = ImageDraw.Draw(lines)
    pad = int(W * 0.12)
    step = int(W * 0.16)
    span = H - 2 * pad
    for off in range(-H, W, step):
        ld.line([(pad + off, pad), (pad + off + span, H - pad)], fill=BACK_LINE, width=SS)
        ld.line([(pad + off, H - pad), (pad + off + span, pad)], fill=BACK_LINE, width=SS)
    mask = Image.new("L", (W, H), 0)
    ImageDraw.Draw(mask).rectangle([pad, pad, W - pad, H - pad], fill=255)

    return Image.composite(lines, base, mask).resize((CARD_W, CARD_H), Image.LANCZOS)


def _row_width(n: int) -> int:
    return n * CARD_W + max(0, n - 1) * GAP


def _hand_value(cards) -> int:
    value, aces = 0, 0
    for rank, _ in cards:
        if rank in ("J", "Q", "K"):
            value += 10
        elif rank == "A":
            aces += 1
            value += 11
        else:
            value += int(rank)
    while value > 21 and aces > 0:
        value -= 10
        aces -= 1
    return value


def _draw_deck_indicator(d, img, table_w, deck_left, small_font):
    """Draw the little card-stack + count in the top-right header strip."""
    stack_w, stack_h = 44, 60
    deck_x = table_w - MARGIN - stack_w - 6
    deck_y = MARGIN + 6
    for k in range(3):
        d.rounded_rectangle(
            [deck_x + k * 3, deck_y - k * 3, deck_x + stack_w + k * 3, deck_y + stack_h - k * 3],
            radius=6, fill=BACK_BG, outline=(225, 225, 230), width=2,
        )
    deck_text = f"Deck: {deck_left}"
    tw = d.textbbox((0, 0), deck_text, font=small_font)[2]
    d.text((deck_x - 14 - tw, deck_y + stack_h / 2 - 14), deck_text, font=small_font, fill=TEXT_DIM)


def render_highlow(current_card, next_card=None, *, deck_left=0, outcome=None) -> io.BytesIO:
    """Composite the high-low table to a PNG returned as a BytesIO.

    `current_card` is a (rank, suit); `next_card` is the same once revealed, or None
    to keep it face-down. `outcome` is None / "win" / "loss" to tint the border.
    """
    MID = 70  # gap between the two cards (room for the "vs" marker)
    row_w = CARD_W * 2 + MID
    table_w = MARGIN * 2 + max(row_w, 320)
    table_h = MARGIN * 2 + HEADER_H + LABEL_H + CARD_H

    edge = FELT_EDGE
    if outcome == "win":
        edge = (70, 170, 100)
    elif outcome == "loss":
        edge = (180, 60, 70)

    img = Image.new("RGB", (table_w, table_h), FELT)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, table_w - 1, table_h - 1], outline=edge, width=6)

    label_font = _font(26)
    small_font = _font(20)
    vs_font = _font(34)

    _draw_deck_indicator(d, img, table_w, deck_left, small_font)

    top = MARGIN + HEADER_H + LABEL_H
    x0 = (table_w - row_w) // 2

    cur_img = _render_card(*current_card)
    d.text((x0, top - LABEL_H + 6), "CURRENT", font=label_font, fill=TEXT)
    img.paste(cur_img, (x0, top), cur_img)

    nx = x0 + CARD_W + MID
    if next_card is None:
        nxt_img = _render_back()
        nlabel = "NEXT — ?"
    else:
        nxt_img = _render_card(*next_card)
        nlabel = "NEXT"
    d.text((nx, top - LABEL_H + 6), nlabel, font=label_font, fill=TEXT)
    img.paste(nxt_img, (nx, top), nxt_img)

    mid_color = HIGHLIGHT
    if outcome == "win":
        mid_color = (110, 210, 140)
    elif outcome == "loss":
        mid_color = (225, 110, 110)
    bbox = d.textbbox((0, 0), "vs", font=vs_font)
    vw, vh = bbox[2] - bbox[0], bbox[3] - bbox[1]
    d.text((x0 + CARD_W + MID / 2 - vw / 2 - bbox[0], top + CARD_H / 2 - vh / 2 - bbox[1]),
           "vs", font=vs_font, fill=mid_color)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf


def render_table(
    dealer_cards,
    player_hands,
    *,
    current_hand: int = 0,
    hide_dealer: bool = True,
    deck_left: int = 0,
    highlight_current: bool = True,
) -> io.BytesIO:
    """Composite the full blackjack table to a PNG returned as a BytesIO.

    `dealer_cards` is a list of (rank, suit); `player_hands` is a list of such lists.
    """
    rows = 1 + len(player_hands)
    widest = max([len(dealer_cards)] + [len(h) for h in player_hands])
    table_w = MARGIN * 2 + max(_row_width(widest), 320)
    table_h = MARGIN * 2 + HEADER_H + rows * (LABEL_H + CARD_H) + (rows - 1) * ROW_GAP

    img = Image.new("RGB", (table_w, table_h), FELT)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, table_w - 1, table_h - 1], outline=FELT_EDGE, width=6)

    label_font = _font(26)
    small_font = _font(20)

    def place_row(cards, top, label, highlight=False):
        row_w = _row_width(len(cards))
        x0 = (table_w - row_w) // 2
        if highlight:
            d.rounded_rectangle(
                [x0 - 12, top - LABEL_H + 2, x0 + row_w + 12, top + CARD_H + 10],
                radius=16, outline=HIGHLIGHT, width=4,
            )
        d.text((x0, top - LABEL_H + 6), label, font=label_font, fill=TEXT)
        x = x0
        for card in cards:
            img.paste(card, (x, top), card)
            x += CARD_W + GAP

    # Deck indicator in the top header strip (right-aligned, clear of all card rows)
    _draw_deck_indicator(d, img, table_w, deck_left, small_font)

    # Dealer row
    top = MARGIN + HEADER_H + LABEL_H
    if hide_dealer:
        shown = [_render_card(*dealer_cards[0])] + [_render_back()] * (len(dealer_cards) - 1)
        dealer_label = f"DEALER — {_hand_value([dealer_cards[0]])}+?"
    else:
        shown = [_render_card(*c) for c in dealer_cards]
        dealer_label = f"DEALER — {_hand_value(dealer_cards)}"
    place_row(shown, top, dealer_label)

    # Player rows
    for i, hand in enumerate(player_hands):
        top += CARD_H + ROW_GAP + LABEL_H
        label_prefix = "YOU" if len(player_hands) == 1 else f"HAND {i + 1}"
        label = f"{label_prefix} — {_hand_value(hand)}"
        cards = [_render_card(*c) for c in hand]
        place_row(cards, top, label, highlight=highlight_current and i == current_hand and len(player_hands) > 1)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf
