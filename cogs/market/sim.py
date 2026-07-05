"""Single-stock market simulation math (v2 cost / risk / leveling model).

Pure functions shared by the standalone CLI (simulate_market.py at the repo
root) and — once wired in — the live market cog. The model:

  raw            = Σ chars_u ** REVENUE_INNER_EXP           (per-user, compressed)
  daily_revenue  = raw ** REVENUE_OUTER_EXP × multiplier    (multiplier is per-company)
  weekly_revenue = Σ daily_revenue

  overhead       = COST_DANGER_RATIO × (1 − COST_VARIABLE_RATE)
                   × base_weekly_revenue × LEVEL_MULT_STEP**level   (× shock some weeks)
  cost           = overhead + COST_VARIABLE_RATE × weekly_revenue
  profit         = weekly_revenue − cost
  dividend_pool  = DIVIDEND_PROFIT_SHARE × profit
  dps            = dividend_pool // total_shares            (0 on a loss week)
  treasury      += weekly_revenue − dividends_paid − cost   (bankrupt if < 0)

  level up when  treasury ≥ LEVEL_TREASURY_WEEKS × weekly_revenue (cap MAX_COMPANY_LEVEL)
  dilution       only when weekly_revenue > baseline, scaled by the excess

The multiplier is *solved* at listing so that, at baseline activity, DPS hits a
target yield — see `solve_multiplier`. The overhead is anchored to the revenue
the company was priced for (base_weekly_revenue = multiplier × R1), so a channel
that fails to sustain its listed activity goes loss-making. That gap between live
activity and the frozen baseline is the risk.
"""

from __future__ import annotations

import random
import statistics
from dataclasses import dataclass

from config import (
    REVENUE_INNER_EXP,
    REVENUE_OUTER_EXP,
    REVENUE_BASE_MULTIPLIER,
    DIVIDEND_PROFIT_SHARE,
    COST_VARIABLE_RATE,
    COST_DANGER_RATIO,
    SHOCK_PROB,
    SHOCK_MIN,
    SHOCK_MAX,
    LEVEL_MULT_STEP,
    MAX_COMPANY_LEVEL,
    LEVEL_TREASURY_WEEKS,
    LEVEL_UP_TREASURY_CONSUME,
    DILUTION_MAX_RATE,
    DILUTION_GAIN,
    DEFAULT_TOTAL_SHARES,
    DEFAULT_IPO_PRICE,
    DEFAULT_TARGET_YIELD,
)

# DIVIDEND_PROFIT_SHARE / DILUTION_MAX_RATE are Decimals in config; the sim math is float.
_DIV_SHARE = float(DIVIDEND_PROFIT_SHARE)
_DILUTION_MAX = float(DILUTION_MAX_RATE)


@dataclass
class SimResult:
    day: int
    week: int
    daily_revenue: int
    weekly_revenue: int = 0
    overhead: int = 0
    cost: int = 0
    profit: int = 0
    dividend_per_share: int = 0
    dividends_paid: int = 0
    treasury: int = 0
    level: int = 0
    revenue_multiplier: float = 0.0
    leveled_up: bool = False
    new_shares: int = 0
    total_shares: int = 0
    shocked: bool = False
    bankrupt: bool = False


@dataclass
class Company:
    total_shares: int = DEFAULT_TOTAL_SHARES
    treasury: int = 100_000
    revenue_multiplier: float = REVENUE_BASE_MULTIPLIER
    level: int = 0
    # shares held by users (the rest sit in the unsold IPO pool and do not
    # receive dividends — the treasury keeps their implicit share)
    user_shares: int = DEFAULT_TOTAL_SHARES
    # weekly revenue the company was IPO-priced for; anchors overhead & dilution.
    base_weekly_revenue: float = 0.0
    base_ipo_price: int = DEFAULT_IPO_PRICE
    bankrupt: bool = False


def daily_revenue(char_counts: list[int], revenue_multiplier: float,
                  inner_exp: float = REVENUE_INNER_EXP, outer_exp: float = REVENUE_OUTER_EXP) -> int:
    raw = sum(c ** inner_exp for c in char_counts if c > 0)
    return int(raw ** outer_exp * revenue_multiplier)


def activity_sensitivity(inner_exp: float = REVENUE_INNER_EXP, outer_exp: float = REVENUE_OUTER_EXP) -> float:
    """Exponent linking a uniform activity change to revenue: revenue ∝ activity**(inner·outer).

    Default 0.25·0.75 = 0.1875 → revenue barely moves with activity. Raise the exponents
    toward 1.0 to make a cooling channel actually cut revenue (and dividends).
    """
    return inner_exp * outer_exp


def danger_activity_level(inner_exp: float = REVENUE_INNER_EXP, outer_exp: float = REVENUE_OUTER_EXP) -> float:
    """Fraction of baseline *activity* at which the company turns structurally loss-making.

    Structural loss starts when revenue < COST_DANGER_RATIO·baseline, and
    revenue ∝ activity**s, so activity = COST_DANGER_RATIO**(1/s).
    """
    s = activity_sensitivity(inner_exp, outer_exp)
    return COST_DANGER_RATIO ** (1 / s) if s > 0 else 0.0


def baseline_profit_fraction() -> float:
    """Fraction of baseline revenue that becomes profit when live activity == baseline.

    profit = revenue − overhead − variable·revenue, and at baseline
    overhead = d·(1−v)·revenue, so profit/revenue = (1−v)·(1−d).
    """
    return (1 - COST_VARIABLE_RATE) * (1 - COST_DANGER_RATIO)


def expected_weekly_revenue_unit(users: int, mean: float, std: float,
                                 samples: int = 400, seed: int = 12345,
                                 inner_exp: float = REVENUE_INNER_EXP,
                                 outer_exp: float = REVENUE_OUTER_EXP) -> float:
    """Monte-Carlo estimate of weekly revenue at multiplier = 1 for an activity profile.

    Mirrors what `.ipohelper` measures from real history (R1). Used by the solver.
    """
    rng = random.Random(seed)
    total = 0.0
    for _ in range(samples):
        cc = [max(0, int(rng.gauss(mean, std))) for _ in range(users)]
        total += daily_revenue(cc, 1, inner_exp, outer_exp)
    return total / samples * 7


def solve_multiplier(r1_weekly: float, target_dps: float, total_shares: int) -> float:
    """Revenue multiplier so that, at baseline activity, DPS == target_dps.

    DPS = DIVIDEND_PROFIT_SHARE · (1−v)(1−d) · E / shares, with E = multiplier · R1,
    so multiplier = target_dps · shares / (DIVIDEND_PROFIT_SHARE · (1−v)(1−d) · R1).
    """
    if r1_weekly <= 0:
        return 0.0
    frac = _DIV_SHARE * baseline_profit_fraction()
    baseline_revenue = target_dps * total_shares / frac
    return baseline_revenue / r1_weekly


def process_week(company: Company, weekly_revenue: int, rng: random.Random) -> dict:
    """Run one weekly close: cost, dividends, treasury, leveling, dilution.

    Mutates `company`. `rng` drives shock events. Returns a summary dict.
    """
    level_scale = LEVEL_MULT_STEP ** company.level
    baseline = company.base_weekly_revenue * level_scale

    overhead = COST_DANGER_RATIO * (1 - COST_VARIABLE_RATE) * baseline
    shocked = rng.random() < SHOCK_PROB
    if shocked:
        overhead *= rng.uniform(SHOCK_MIN, SHOCK_MAX)

    cost = int(overhead + COST_VARIABLE_RATE * weekly_revenue)
    profit = weekly_revenue - cost
    dividend_pool = int(_DIV_SHARE * profit)
    dividend_per_share = dividend_pool // company.total_shares if company.total_shares else 0
    if dividend_per_share < 0:
        dividend_per_share = 0
    dividends_paid = dividend_per_share * company.user_shares
    company.treasury += weekly_revenue - dividends_paid - cost

    bankrupt = company.treasury < 0
    if bankrupt:
        company.bankrupt = True

    leveled_up = False
    if not bankrupt and company.level < MAX_COMPANY_LEVEL:
        threshold = LEVEL_TREASURY_WEEKS * weekly_revenue
        if company.treasury >= threshold:
            company.treasury -= int(LEVEL_UP_TREASURY_CONSUME * company.treasury)
            company.revenue_multiplier *= LEVEL_MULT_STEP
            company.level += 1
            leveled_up = True

    # Dilution: mint shares only when the company beats its (level-scaled) baseline.
    new_shares = 0
    if not bankrupt and baseline > 0 and weekly_revenue > baseline:
        excess = (weekly_revenue - baseline) / baseline
        rate = min(_DILUTION_MAX, DILUTION_GAIN * excess)
        new_shares = int(rate * company.total_shares)
        company.total_shares += new_shares  # diluted shares sit in the IPO pool

    return {
        "overhead": int(overhead),
        "cost": cost,
        "profit": profit,
        "dividend_per_share": dividend_per_share,
        "dividends_paid": dividends_paid,
        "leveled_up": leveled_up,
        "new_shares": new_shares,
        "shocked": shocked,
        "bankrupt": bankrupt,
    }


def sample_char_count(mean: float, std: float, rng: random.Random) -> int:
    """One user's daily char count — normal, clipped at 0."""
    return max(0, int(rng.gauss(mean, std)))


def simulate(
    users: int,
    mean: float,
    std: float,
    days: int,
    total_shares: int = DEFAULT_TOTAL_SHARES,
    ipo_price: int = DEFAULT_IPO_PRICE,
    target_yield: float = DEFAULT_TARGET_YIELD,
    revenue_multiplier: float | None = None,
    user_shares: int | None = None,
    activity_scale=None,
    inner_exp: float | None = None,
    outer_exp: float | None = None,
    seed: int | None = None,
) -> list[SimResult]:
    """Simulate a company's life day by day.

    The multiplier is solved from the activity profile + (ipo_price, target_yield,
    total_shares) unless `revenue_multiplier` is given explicitly. `activity_scale`
    optionally warps live activity per day (a callable day -> factor, e.g. to model
    a channel going quiet); it does NOT move the frozen baseline, so it drives risk.
    `inner_exp`/`outer_exp` override the revenue exponents to tune how much activity
    matters (default = config values); the baseline solve uses the same exponents,
    so at baseline activity DPS stays on target regardless of the choice.
    """
    rng = random.Random(seed)

    inner = inner_exp if inner_exp is not None else REVENUE_INNER_EXP
    outer = outer_exp if outer_exp is not None else REVENUE_OUTER_EXP

    r1 = expected_weekly_revenue_unit(users, mean, std, seed=(seed or 0) + 999,
                                      inner_exp=inner, outer_exp=outer)
    target_dps = target_yield * ipo_price
    mult = revenue_multiplier if revenue_multiplier is not None else solve_multiplier(r1, target_dps, total_shares)
    base_weekly_revenue = mult * r1

    company = Company(
        total_shares=total_shares,
        revenue_multiplier=mult,
        user_shares=user_shares if user_shares is not None else total_shares,
        base_weekly_revenue=base_weekly_revenue,
        base_ipo_price=ipo_price,
    )

    results: list[SimResult] = []
    week_revenue_accum = 0

    for day in range(1, days + 1):
        scale = activity_scale(day) if activity_scale else 1.0
        char_counts = [sample_char_count(mean * scale, std * scale, rng) for _ in range(users)]
        rev = daily_revenue(char_counts, company.revenue_multiplier, inner, outer)
        week_revenue_accum += rev

        r = SimResult(
            day=day,
            week=(day - 1) // 7 + 1,
            daily_revenue=rev,
            treasury=company.treasury,
            level=company.level,
            revenue_multiplier=company.revenue_multiplier,
            total_shares=company.total_shares,
        )

        # Every 7th day closes a week (mirrors the live Sunday task).
        if day % 7 == 0 and not company.bankrupt:
            info = process_week(company, week_revenue_accum, rng)
            r.weekly_revenue = week_revenue_accum
            r.overhead = info["overhead"]
            r.cost = info["cost"]
            r.profit = info["profit"]
            r.dividend_per_share = info["dividend_per_share"]
            r.dividends_paid = info["dividends_paid"]
            r.treasury = company.treasury
            r.level = company.level
            r.revenue_multiplier = company.revenue_multiplier
            r.leveled_up = info["leveled_up"]
            r.new_shares = info["new_shares"]
            r.total_shares = company.total_shares
            r.shocked = info["shocked"]
            r.bankrupt = info["bankrupt"]
            week_revenue_accum = 0

        results.append(r)
        if company.bankrupt:
            break

    return results


# Default candidate weekly dividend yields shown in the recommendation table.
DEFAULT_TARGET_YIELDS = (0.03, 0.05, 0.08, 0.12)


def recommend_ipo(
    users: int,
    mean: float,
    std: float,
    total_shares: int = DEFAULT_TOTAL_SHARES,
    ipo_price: int = DEFAULT_IPO_PRICE,
    target_yield: float = DEFAULT_TARGET_YIELD,
    target_yields: tuple[float, ...] = DEFAULT_TARGET_YIELDS,
    weeks: int = 12,
    seed: int = 0,
) -> dict:
    """Solve the per-company multiplier for (ipo_price, target_yield, total_shares)
    and report the projected steady DPS plus a yield → IPO-price table.

    Returns a dict with the same ``dps`` / ``rows`` / ``total_shares`` keys the live
    ``.ipohelper`` embed reads, augmented with ``multiplier`` / ``base_weekly_revenue``.
    """
    r1 = expected_weekly_revenue_unit(users, mean, std, seed=seed + 999)
    target_dps = target_yield * ipo_price
    multiplier = solve_multiplier(r1, target_dps, total_shares)
    base_weekly_revenue = multiplier * r1

    # Steady DPS = median weekly DPS over the early, pre-first-level-up life.
    results = simulate(
        users=users, mean=mean, std=std, days=weeks * 7,
        total_shares=total_shares, ipo_price=ipo_price, target_yield=target_yield,
        seed=seed,
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

    return {
        "dps": dps,
        "rows": rows,
        "total_shares": total_shares,
        "multiplier": multiplier,
        "base_weekly_revenue": base_weekly_revenue,
    }
