"""Single-stock market simulator (CLI).

Thin command-line front-end over the shared simulation math in
``cogs/market/sim.py`` (which mirrors the revenue / weekly-financial rules used
by the live market cog). Use it to explore how treasury and dividends evolve for
a given user count and activity profile.

Usage:
    python simulate_market.py --users 20 --mean 400 --std 150 --days 180
    python simulate_market.py --users 50 --mean 800 --std 300 --days 365 --csv out.csv
"""

from __future__ import annotations

import argparse
import csv

from cogs.market.sim import SimResult, simulate


def print_weekly_report(results: list[SimResult]) -> None:
    header = (
        f"{'Week':>4} {'WeeklyRev':>10} {'Cost':>8} {'Profit':>9} "
        f"{'DPS':>5} {'DivPaid':>9} {'Treasury':>10} {'Lvl':>3} {'Mult':>5} {'NewShares':>9}"
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
            f"{r.level:>3} {r.revenue_multiplier:>5} {r.new_shares:>9}{flag}"
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
            "revenue_multiplier", "leveled_up", "new_shares",
        ])
        for r in results:
            w.writerow([
                r.day, r.week, r.daily_revenue, r.weekly_revenue, r.cost,
                r.profit, r.dividend_per_share, r.dividends_paid, r.treasury,
                r.level, r.revenue_multiplier, int(r.leveled_up), r.new_shares,
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
