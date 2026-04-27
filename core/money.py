from __future__ import annotations

"""Shared parser for money amounts with k/m/b suffix support."""


_SUFFIXES = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}


class AmountError(ValueError):
    """Raised when a money amount can't be parsed."""


def parse_amount(value: str, wallet_balance: int | None = None) -> int:
    """Parse a money amount.

    Accepts:
      - Plain integers: "100"
      - k/m/b suffixes (case-insensitive, decimals allowed): "5k", "2.5m", "1b"
      - Thousands separators: "1_000", "1,000"
      - "all" (case-insensitive), if wallet_balance is provided

    Returns an int. Raises AmountError if the input can't be parsed or is
    non-positive.
    """
    if value is None:
        raise AmountError("No amount given.")
    s = str(value).strip().lower()
    if not s:
        raise AmountError("No amount given.")

    if s == "all":
        if wallet_balance is None:
            raise AmountError("'all' is not allowed here.")
        return int(wallet_balance)

    s = s.replace("_", "").replace(",", "")

    multiplier = 1
    if s and s[-1] in _SUFFIXES:
        multiplier = _SUFFIXES[s[-1]]
        s = s[:-1]

    try:
        number = float(s)
    except ValueError:
        raise AmountError(
            f"`{value}` is not a valid amount. Use a number like `100`, `5k`, `2.5m`, or `1b`."
        )

    result = int(number * multiplier)
    if result <= 0:
        raise AmountError("Amount must be positive.")
    return result
