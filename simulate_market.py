"""Single-stock market simulator.

Mirrors the revenue / weekly-financial rules used in cogs/market.py and
cogs/utils/db.py so you can explore how treasury and dividends evolve for a
given user count and activity profile.

Usage:
    python simulate_market.py --users 20 --mean 400 --std 150 --days 180
    python simulate_market.py --users 50 --mean 800 --std 300 --days 365 --csv out.csv
"""

from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass, field

from config import (
    REVENUE_BASE_MULTIPLIER,
    REVENUE_INNER_EXP,
    REVENUE_OUTER_EXP,
    LEVEL_BASE_THRESHOLD,
    COST_FACTOR,
    DIVIDEND_REVENUE_SHARE,
    LEVEL_UP_TREASURY_CONSUME,
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
    cost = int(COST_FACTOR * company.treasury)
    dividend_pool = int(DIVIDEND_REVENUE_SHARE * weekly_revenue)
    dividend_per_share = dividend_pool // company.total_shares
    dividends_paid = dividend_per_share * company.user_shares
    profit = weekly_revenue - cost
    company.treasury += weekly_revenue - dividends_paid - cost

    leveled_up = False
    next_level = company.level + 1
    threshold = LEVEL_BASE_THRESHOLD * (2 ** (next_level - 1))
    if company.treasury >= threshold:
        company.treasury -= int(LEVEL_UP_TREASURY_CONSUME * company.treasury)
        company.revenue_multiplier *= 2
        company.level = next_level
        leveled_up = True

    return {
        "cost": cost,
        "profit": profit,
        "dividend_per_share": dividend_per_share,
        "dividends_paid": dividends_paid,
        "leveled_up": leveled_up,
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
            week_revenue_accum = 0

        results.append(r)

    return results


def print_weekly_report(results: list[SimResult]) -> None:
    header = (
        f"{'Week':>4} {'WeeklyRev':>10} {'Cost':>8} {'Profit':>9} "
        f"{'DPS':>5} {'DivPaid':>9} {'Treasury':>10} {'Lvl':>3} {'Mult':>5}"
    )
    print(header)
    print("-" * len(header))
    total_dividends = 0
    for r in results:
        if r.day % 7 != 0:
            continue
        total_dividends += r.dividends_paid
        flag = "  <-- LEVEL UP!" if r.leveled_up else ""
        print(
            f"{r.week:>4} {r.weekly_revenue:>10} {r.cost:>8} {r.profit:>9} "
            f"{r.dividend_per_share:>5} {r.dividends_paid:>9} {r.treasury:>10} "
            f"{r.level:>3} {r.revenue_multiplier:>5}{flag}"
        )
    print("-" * len(header))
    final = results[-1]
    print(
        f"End state: treasury={final.treasury}, level={final.level}, "
        f"multiplier={final.revenue_multiplier}, total dividends paid to users={total_dividends}"
    )


def write_csv(results: list[SimResult], path: str) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "day", "week", "daily_revenue", "weekly_revenue", "cost", "profit",
            "dividend_per_share", "dividends_paid", "treasury", "level",
            "revenue_multiplier", "leveled_up",
        ])
        for r in results:
            w.writerow([
                r.day, r.week, r.daily_revenue, r.weekly_revenue, r.cost,
                r.profit, r.dividend_per_share, r.dividends_paid, r.treasury,
                r.level, r.revenue_multiplier, int(r.leveled_up),
            ])


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--users", type=int, required=True, help="Number of active users")
    p.add_argument("--mean", type=float, required=True, help="Mean daily char count per user")
    p.add_argument("--std", type=float, required=True, help="Std deviation of daily char count")
    p.add_argument("--days", type=int, required=True, help="Simulation length in days")
    p.add_argument("--total-shares", type=int, default=1000)
    p.add_argument("--user-shares", type=int, default=None,
                   help="Shares held by users (rest sit in IPO). Defaults to --total-shares.")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--csv", type=str, default=None, help="Optional CSV output path (per-day detail)")
    args = p.parse_args()

    results = simulate(
        users=args.users,
        mean=args.mean,
        std=args.std,
        days=args.days,
        total_shares=args.total_shares,
        user_shares=args.user_shares,
        seed=args.seed,
    )
    print_weekly_report(results)
    if args.csv:
        write_csv(results, args.csv)
        print(f"Per-day detail written to {args.csv}")


if __name__ == "__main__":
    main()
