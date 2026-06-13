"""Single-stock market simulation math.

Mirrors the revenue / weekly-financial rules used in cogs/market/cog.py and
cogs/market/db.py so we can explore how treasury and dividends evolve for a
given user count and activity profile — and back out a sensible IPO price.

The pure functions here are shared by the standalone CLI (simulate_market.py at
the repo root) and the in-bot `.ipohelper` command.
"""

from __future__ import annotations

import random
import statistics
from dataclasses import dataclass
from decimal import Decimal

from config import (
    REVENUE_BASE_MULTIPLIER,
    REVENUE_INNER_EXP,
    REVENUE_OUTER_EXP,
    LEVEL_BASE_THRESHOLD,
    COST_FACTOR_MEAN,
    COST_FLOOR,
    DIVIDEND_PROFIT_SHARE,
    LEVEL_UP_TREASURY_CONSUME,
    DILUTION_MAX_RATE,
    DILUTION_PROFIT_SCALE,
)


@dataclass
class SimResult:
    day: int
    week: int
    daily_revenue: int
    weekly_revenue: int = 0
    cost: int = 0
    profit: int = 0
    dividend_per_share: int = 0
    dividends_paid: int = 0
    treasury: int = 0
    level: int = 0
    revenue_multiplier: int = 0
    leveled_up: bool = False
    new_shares: int = 0


@dataclass
class Company:
    total_shares: int = 1000
    treasury: int = 100_000
    revenue_multiplier: int = REVENUE_BASE_MULTIPLIER
    level: int = 0
    # shares held by users (the rest sit in the unsold IPO pool and do not
    # receive dividends — the treasury keeps their implicit share)
    user_shares: int = 1000


def daily_revenue(char_counts: list[int], revenue_multiplier: int) -> int:
    raw = sum(c ** REVENUE_INNER_EXP for c in char_counts if c > 0)
    return int(raw ** REVENUE_OUTER_EXP * revenue_multiplier)


def process_week(company: Company, weekly_revenue: int) -> dict:
    # Cost mirrors the live Sunday task (cogs/market/cog.py): a floor plus a
    # percentage of treasury. The cog randomises the rate over 5–10 %; we use the
    # mean (COST_FACTOR_MEAN) for a stable, reproducible recommendation.
    cost = max(COST_FLOOR, int(COST_FACTOR_MEAN * company.treasury))
    profit = weekly_revenue - cost
    dividend_pool = int(DIVIDEND_PROFIT_SHARE * profit)
    dividend_per_share = dividend_pool // company.total_shares
    # The cog only pays a dividend when it is positive; a loss-making week pays nothing.
    if dividend_per_share < 0:
        dividend_per_share = 0
    dividends_paid = dividend_per_share * company.user_shares
    company.treasury += weekly_revenue - dividends_paid - cost

    leveled_up = False
    next_level = company.level + 1
    threshold = LEVEL_BASE_THRESHOLD * (2 ** (next_level - 1))
    if company.treasury >= threshold:
        company.treasury -= int(LEVEL_UP_TREASURY_CONSUME * company.treasury)
        company.revenue_multiplier *= 2
        company.level = next_level
        leveled_up = True

    new_shares = 0
    if profit > 0:
        dilution_rate = min(DILUTION_MAX_RATE, Decimal(profit) / DILUTION_PROFIT_SCALE)
        new_shares = max(1, int(dilution_rate * company.total_shares))
        company.total_shares += new_shares

    return {
        "cost": cost,
        "profit": profit,
        "dividend_per_share": dividend_per_share,
        "dividends_paid": dividends_paid,
        "leveled_up": leveled_up,
        "new_shares": new_shares,
    }


def sample_char_count(mean: float, std: float, rng: random.Random) -> int:
    """One user's daily char count — normal, clipped at 0."""
    return max(0, int(rng.gauss(mean, std)))


def simulate(
    users: int,
    mean: float,
    std: float,
    days: int,
    total_shares: int = 1000,
    user_shares: int | None = None,
    seed: int | None = None,
) -> list[SimResult]:
    rng = random.Random(seed)
    company = Company(
        total_shares=total_shares,
        user_shares=user_shares if user_shares is not None else total_shares,
    )

    results: list[SimResult] = []
    week_revenue_accum = 0

    for day in range(1, days + 1):
        char_counts = [sample_char_count(mean, std, rng) for _ in range(users)]
        rev = daily_revenue(char_counts, company.revenue_multiplier)
        week_revenue_accum += rev

        r = SimResult(
            day=day,
            week=(day - 1) // 7 + 1,
            daily_revenue=rev,
            treasury=company.treasury,
            level=company.level,
            revenue_multiplier=company.revenue_multiplier,
        )

        # Every 7th day closes a week (mirrors the Sunday task)
        if day % 7 == 0:
            info = process_week(company, week_revenue_accum)
            r.weekly_revenue = week_revenue_accum
            r.cost = info["cost"]
            r.profit = info["profit"]
            r.dividend_per_share = info["dividend_per_share"]
            r.dividends_paid = info["dividends_paid"]
            r.treasury = company.treasury
            r.level = company.level
            r.revenue_multiplier = company.revenue_multiplier
            r.leveled_up = info["leveled_up"]
            r.new_shares = info["new_shares"]
            week_revenue_accum = 0

        results.append(r)

    return results


# Default candidate weekly dividend yields shown in the recommendation table.
DEFAULT_TARGET_YIELDS = (0.03, 0.05, 0.08, 0.12)


def recommend_ipo(
    users: int,
    mean: float,
    std: float,
    total_shares: int = 10000,
    target_yields: tuple[float, ...] = DEFAULT_TARGET_YIELDS,
    weeks: int = 12,
    seed: int = 0,
) -> dict:
    """Project a steady early-life dividend-per-share and turn target weekly
    yields into recommended IPO prices.

    `weekly yield = DPS / ipo_price`, so `ipo_price = DPS / target_yield`. DPS is
    the median weekly dividend-per-share over the company's early life — up to and
    including the first level-up week — which avoids the distortion from the
    revenue-multiplier doubling. Loss-making weeks count as 0 (no dividend paid).
    """
    results = simulate(
        users=users, mean=mean, std=std, days=weeks * 7,
        total_shares=total_shares, seed=seed,
    )
    weekly = [r for r in results if r.day % 7 == 0]

    early: list[int] = []
    for r in weekly:
        early.append(max(0, r.dividend_per_share))
        if r.leveled_up:
            break
    if not early:
        early = [max(0, r.dividend_per_share) for r in weekly]

    dps = int(statistics.median(early)) if early else 0

    rows = []
    for y in target_yields:
        price = round(dps / y) if dps > 0 else 0
        payback = round(price / dps) if dps > 0 else 0
        rows.append({"yield": y, "ipo_price": price, "payback": payback})

    return {"dps": dps, "rows": rows, "total_shares": total_shares}
