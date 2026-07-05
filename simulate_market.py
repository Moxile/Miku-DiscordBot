"""Single-stock market simulator (CLI) — v2 cost / risk / leveling model.

Thin command-line front-end over the shared simulation math in
``cogs/market/sim.py``. Use it to tune the balance: pick an activity profile and
IPO parameters, and watch how dividends, treasury, levels, shocks and (optionally)
a channel going quiet play out over the company's life.

Examples:
    # Your target: 500 shares, IPO 100, 5% weekly yield, measured activity
    python simulate_market.py --users 37 --mean 103 --std 80 --days 365 \\
        --total-shares 500 --ipo-price 100 --yield 0.05

    # Force a specific multiplier instead of solving one
    python simulate_market.py --users 37 --mean 103 --std 80 --days 180 --multiplier 95

    # Stress test: the channel loses 60% of its activity after day 120
    python simulate_market.py --users 37 --mean 103 --std 80 --days 365 \\
        --quiet-after 120 --quiet-factor 0.4
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys

# Copy-pasteable invocation printed when the script is run with no arguments.
USAGE_TEMPLATE = """\
Run it with an activity profile and IPO parameters. Copy, paste, and edit:

    python simulate_market.py --users 37 --mean 103 --std 80 --days 365 \\
        --total-shares 500 --ipo-price 100 --yield 0.05

Required:
    --users N          active users in the channel
    --mean N           mean daily chars per user
    --std N            std deviation of daily chars per user
    --days N           how many days to simulate

Common options (with defaults):
    --total-shares 500     shares issued (market cap = shares × ipo-price)
    --ipo-price 100        price per share at IPO
    --yield 0.05           target weekly dividend yield at baseline
    --multiplier X         force a revenue multiplier instead of solving one
    --inner-exp 0.25       per-user compression exponent
    --outer-exp 0.75       user-count exponent (raise both toward 1.0 so
                           activity decline actually cuts revenue)
    --seed N               fix randomness for a reproducible run
    --csv out.csv          write per-day detail to a CSV

Stress test a channel that cools off:
    python simulate_market.py --users 37 --mean 103 --std 80 --days 365 \\
        --quiet-after 120 --quiet-factor 0.4   # activity drops to 40% after day 120

Full option list:  python simulate_market.py --help
"""

from cogs.market.sim import (
    SimResult,
    simulate,
    expected_weekly_revenue_unit,
    solve_multiplier,
    baseline_profit_fraction,
    activity_sensitivity,
    danger_activity_level,
)


def _weeklies(results: list[SimResult]) -> list[SimResult]:
    return [r for r in results if r.day % 7 == 0]


def print_weekly_report(results: list[SimResult]) -> None:
    header = (
        f"{'Wk':>3} {'WeeklyRev':>10} {'Overhead':>9} {'Cost':>9} {'Profit':>10} "
        f"{'DPS':>5} {'DivPaid':>10} {'Treasury':>12} {'Lv':>3} {'Mult':>7} {'Shares':>7} {'New':>4}"
    )
    print(header)
    print("-" * len(header))
    for r in _weeklies(results):
        flags = ""
        if r.shocked:
            flags += " ⚡SHOCK"
        if r.leveled_up:
            flags += " ⬆LEVEL"
        if r.profit < 0:
            flags += " 🔻LOSS"
        if r.bankrupt:
            flags += " 💀BANKRUPT"
        print(
            f"{r.week:>3} {r.weekly_revenue:>10} {r.overhead:>9} {r.cost:>9} {r.profit:>10} "
            f"{r.dividend_per_share:>5} {r.dividends_paid:>10} {r.treasury:>12} "
            f"{r.level:>3} {r.revenue_multiplier:>7.1f} {r.total_shares:>7} {r.new_shares:>4}{flags}"
        )
    print("-" * len(header))


def print_summary(results: list[SimResult], args, multiplier: float, base_weekly_revenue: float,
                  inner: float, outer: float) -> None:
    weekly = _weeklies(results)
    dps_series = [r.dividend_per_share for r in weekly]
    loss_weeks = sum(1 for r in weekly if r.profit < 0)
    shock_weeks = sum(1 for r in weekly if r.shocked)
    zero_div_weeks = sum(1 for r in weekly if r.dividend_per_share == 0)
    total_div = sum(r.dividends_paid for r in weekly)
    bankrupt = any(r.bankrupt for r in results)
    final = results[-1]

    steady_dps = int(statistics.median(dps_series)) if dps_series else 0
    market_cap_ipo = args.total_shares * args.ipo_price
    realized_yield = (steady_dps / args.ipo_price) if args.ipo_price else 0

    sens = activity_sensitivity(inner, outer)
    danger = danger_activity_level(inner, outer)

    print("\nSETUP")
    print(f"  activity          : {args.users} users · mean {args.mean:.0f} · std {args.std:.0f} chars/day")
    print(f"  R1 (weekly rev @1) : {expected_weekly_revenue_unit(args.users, args.mean, args.std, inner_exp=inner, outer_exp=outer):,.0f}")
    print(f"  solved multiplier  : {multiplier:,.2f}   (baseline weekly revenue = {base_weekly_revenue:,.0f})")
    print(f"  IPO                : {args.total_shares:,} shares × {args.ipo_price:,} = {market_cap_ipo:,} market cap")
    print(f"  target yield       : {args.__dict__['yield']*100:.1f}% / week   (margin (1-v)(1-d) = {baseline_profit_fraction()*100:.0f}%)")
    print(f"  revenue exponents  : inner {inner} · outer {outer}  →  revenue ∝ activity^{sens:.3f}")
    print(f"  structural loss at : activity below {danger*100:.0f}% of baseline (before shocks)")

    print("\nOUTCOME")
    print(f"  steady DPS (median): {steady_dps}  →  realized yield {realized_yield*100:.2f}% / week")
    print(f"  loss weeks         : {loss_weeks}/{len(weekly)}   zero-dividend weeks: {zero_div_weeks}   shock weeks: {shock_weeks}")
    print(f"  total dividends    : {total_div:,}")
    print(f"  final              : treasury {final.treasury:,} · level {final.level} · "
          f"multiplier {final.revenue_multiplier:,.1f} · {final.total_shares:,} shares")
    if bankrupt:
        print(f"  💀 BANKRUPT on week {final.week} (day {final.day}) — treasury went negative.")


def write_csv(results: list[SimResult], path: str) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "day", "week", "daily_revenue", "weekly_revenue", "overhead", "cost",
            "profit", "dividend_per_share", "dividends_paid", "treasury", "level",
            "revenue_multiplier", "leveled_up", "new_shares", "total_shares",
            "shocked", "bankrupt",
        ])
        for r in results:
            w.writerow([
                r.day, r.week, r.daily_revenue, r.weekly_revenue, r.overhead, r.cost,
                r.profit, r.dividend_per_share, r.dividends_paid, r.treasury, r.level,
                round(r.revenue_multiplier, 4), int(r.leveled_up), r.new_shares,
                r.total_shares, int(r.shocked), int(r.bankrupt),
            ])


def main() -> None:
    # Bare invocation → print a ready-to-run command instead of an argparse error.
    if len(sys.argv) == 1:
        print(USAGE_TEMPLATE)
        return

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--users", type=int, required=True, help="Number of active users")
    p.add_argument("--mean", type=float, required=True, help="Mean daily char count per user")
    p.add_argument("--std", type=float, required=True, help="Std deviation of daily char count")
    p.add_argument("--days", type=int, required=True, help="Simulation length in days")
    p.add_argument("--total-shares", type=int, default=500)
    p.add_argument("--ipo-price", type=int, default=100)
    p.add_argument("--yield", type=float, default=0.05, dest="yield",
                   help="Target weekly dividend yield at baseline (default 0.05 = 5%%)")
    p.add_argument("--multiplier", type=float, default=None,
                   help="Force a specific revenue multiplier instead of solving one")
    p.add_argument("--inner-exp", type=float, default=None,
                   help="Override REVENUE_INNER_EXP (per-user compression; default 0.25)")
    p.add_argument("--outer-exp", type=float, default=None,
                   help="Override REVENUE_OUTER_EXP (user-count returns; default 0.75). "
                        "Raise both toward 1.0 to make activity decline actually cut revenue.")
    p.add_argument("--user-shares", type=int, default=None,
                   help="Shares held by users (rest sit in IPO). Defaults to --total-shares.")
    p.add_argument("--quiet-after", type=int, default=None,
                   help="Day after which the channel activity drops (stress test)")
    p.add_argument("--quiet-factor", type=float, default=1.0,
                   help="Activity multiplier applied after --quiet-after (e.g. 0.4 = 60%% drop)")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--csv", type=str, default=None, help="Optional CSV output path (per-day detail)")
    args = p.parse_args()

    activity_scale = None
    if args.quiet_after is not None:
        cutoff, factor = args.quiet_after, args.quiet_factor
        activity_scale = lambda day: factor if day > cutoff else 1.0

    # Resolve exponents (fall back to config defaults inside sim if None).
    from config import REVENUE_INNER_EXP, REVENUE_OUTER_EXP
    inner = args.inner_exp if args.inner_exp is not None else REVENUE_INNER_EXP
    outer = args.outer_exp if args.outer_exp is not None else REVENUE_OUTER_EXP

    # Report the solved multiplier/baseline up front (same math simulate() uses).
    r1 = expected_weekly_revenue_unit(args.users, args.mean, args.std, inner_exp=inner, outer_exp=outer)
    target_dps = args.__dict__["yield"] * args.ipo_price
    multiplier = args.multiplier if args.multiplier is not None else solve_multiplier(r1, target_dps, args.total_shares)
    base_weekly_revenue = multiplier * r1

    results = simulate(
        users=args.users,
        mean=args.mean,
        std=args.std,
        days=args.days,
        total_shares=args.total_shares,
        ipo_price=args.ipo_price,
        target_yield=args.__dict__["yield"],
        revenue_multiplier=args.multiplier,
        user_shares=args.user_shares,
        activity_scale=activity_scale,
        inner_exp=args.inner_exp,
        outer_exp=args.outer_exp,
        seed=args.seed,
    )

    print_weekly_report(results)
    print_summary(results, args, multiplier, base_weekly_revenue, inner, outer)
    if args.csv:
        write_csv(results, args.csv)
        print(f"\nPer-day detail written to {args.csv}")


if __name__ == "__main__":
    main()
