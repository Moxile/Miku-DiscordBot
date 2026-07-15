from __future__ import annotations

"""Black-Scholes option pricing and a realized-volatility estimator.

Finnhub's free tier has no option chain, so we price premiums ourselves. Options
are treated as European (settled only at expiry), which is exactly what closed-form
Black-Scholes prices. All prices here are per single share in USD; the caller scales
by the contract multiplier and contract count to reach whole coins.
"""

import datetime
import math

from cogs.realstocks.db import get_price_history
from config import OPTION_DEFAULT_IV, OPTION_MIN_IV, OPTION_MAX_IV

YEAR_SECONDS = 365 * 24 * 3600


def _norm_cdf(x: float) -> float:
    """Standard-normal CDF via the error function (no scipy dependency)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def intrinsic_value(opt_type: str, spot: float, strike: float) -> float:
    """Payoff per share if exercised now: max(0, S-K) for a call, max(0, K-S) for a put."""
    if opt_type == "call":
        return max(0.0, spot - strike)
    return max(0.0, strike - spot)


def black_scholes(opt_type: str, spot: float, strike: float, days: float,
                  iv: float, rate: float) -> float:
    """Black-Scholes price per share in USD for a European call/put.

    `days` is calendar days to expiry (fractional allowed). Degenerate inputs
    (expired, zero vol, non-positive spot/strike) fall back to intrinsic value.
    The result is floored at intrinsic for calls (always) and at zero, so a
    quoted premium is never negative."""
    T = max(days, 0.0) / 365.0
    intrinsic = intrinsic_value(opt_type, spot, strike)
    if spot <= 0 or strike <= 0:
        return max(0.0, intrinsic)
    if T <= 0 or iv <= 0:
        return intrinsic

    sqrt_t = math.sqrt(T)
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * T) / (iv * sqrt_t)
    d2 = d1 - iv * sqrt_t
    discount = math.exp(-rate * T)
    if opt_type == "call":
        price = spot * _norm_cdf(d1) - strike * discount * _norm_cdf(d2)
    else:
        price = strike * discount * _norm_cdf(-d2) - spot * _norm_cdf(-d1)
    return max(price, 0.0)


async def estimate_iv(pool, symbol: str) -> float:
    """Annualized volatility from the symbol's recorded price history, clamped to
    [OPTION_MIN_IV, OPTION_MAX_IV]. Falls back to OPTION_DEFAULT_IV when there aren't
    enough points. Recorded prices are lot-scaled, but volatility is computed from
    log *returns*, so the lot factor cancels out."""
    history = await get_price_history(pool, symbol, limit=120)  # (price, recorded_at), oldest first
    if len(history) < 20:
        return OPTION_DEFAULT_IV

    returns: list[float] = []
    deltas: list[float] = []
    prev = history[0]
    for cur in history[1:]:
        if prev["price"] > 0 and cur["price"] > 0:
            returns.append(math.log(cur["price"] / prev["price"]))
            deltas.append((cur["recorded_at"] - prev["recorded_at"]).total_seconds())
        prev = cur
    if len(returns) < 20:
        return OPTION_DEFAULT_IV

    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    step_sd = math.sqrt(variance)
    avg_delta = sum(deltas) / len(deltas)
    if avg_delta <= 0:
        return OPTION_DEFAULT_IV

    annualized = step_sd * math.sqrt(YEAR_SECONDS / avg_delta)
    return min(OPTION_MAX_IV, max(OPTION_MIN_IV, annualized))


def days_until(expiry: datetime.datetime) -> float:
    """Fractional calendar days from now until `expiry` (never negative)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    return max(0.0, (expiry - now).total_seconds() / 86400)
