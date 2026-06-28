import random
import re

ZERO_WIDTH_CHARS = ("​", "‌", "⁠")  # ZWSP, ZWNJ, word joiner
ZW_PATTERN = re.compile("[" + "".join(ZERO_WIDTH_CHARS) + "]")


def salt_text(text: str, density: float = 0.12) -> str:
    """Sprinkle invisible zero-width characters inside words.

    They render with no visual width, so a person reading and retyping the
    text never produces them — but selecting and copy-pasting the message
    carries them straight into the clipboard, which fails the mistake check.
    """
    out = []
    n = len(text)
    for i, ch in enumerate(text):
        out.append(ch)
        if ch != " " and i < n - 1 and text[i + 1] != " " and random.random() < density:
            out.append(random.choice(ZERO_WIDTH_CHARS))
    return "".join(out)


def edit_distance(a: str, b: str) -> int:
    if a == b:
        return 0
    la, lb = len(a), len(b)
    prev = list(range(lb + 1))
    for i in range(1, la + 1):
        curr = [i] + [0] * lb
        for j in range(1, lb + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            curr[j] = min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost)
        prev = curr
    return prev[lb]
