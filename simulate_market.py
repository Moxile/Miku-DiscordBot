"""
Market price simulation — visualises how the volume-adaptive MM behaves over:
  • Intraday  : each data point = one trade  (shows per-trade nudge)
  • One week  : each data point = one day    (shows daily revenue accumulation)
  • Multi-week: each data point = one week   (shows weekly settlement)

Run:  python simulate_market.py
Requires matplotlib (already a bot dependency).
"""

import math
import random
import sys

# ── Pull constants + pure functions from the cog ─────────────────────────────
sys.path.insert(0, ".")
from cogs.market import (
    Market,
    MM_ALPHA_LOW, MM_ALPHA_HIGH,
    MM_MAX_MOVE_LOW, MM_MAX_MOVE_HIGH,
    MM_TRADE_IMPACT_LOW, MM_TRADE_IMPACT_HIGH,
    MM_SPREAD_MULT_LOW, MM_SPREAD_MULT_HIGH,
    MM_STABILITY_MATURITY,
    MM_BASE_SPREAD, MM_A, MM_B, MM_C, MM_K,
    MM_CONFIDENCE_WEEKS,
    MM_TRADE_FAIR_WEIGHT, MM_BOOK_VALUE_WEIGHT,
    MM_TREASURY_UPKEEP_RATE,
    MM_TREASURY_GROWTH_RATE,
    IPO_TOTAL_SHARES,
)

# ── Simulation parameters (tweak these) ──────────────────────────────────────

IPO_PRICE       = 100.0
INTRADAY_TRADES = 60          # trades in one session
WEEKLY_DAYS     = 7           # days in the weekly sim
MULTIWEEK_WEEKS = 16          # weeks in the long sim
TRADES_PER_DAY  = 10          # used in weekly/multi-week sims
TREASURY        = float(IPO_TOTAL_SHARES * IPO_PRICE)  # IPO proceeds seed the treasury
DIVIDEND_PCT    = 0.10

# Revenue per day (weighted chars): controls how fast the price grows
REVENUE_PER_DAY_LOW  = 50     # quiet channel
REVENUE_PER_DAY_HIGH = 400    # busy channel

# Sentiment: fraction of trades that are buys vs sells (0.5 = neutral)
SENTIMENT = 0.55              # slight buy pressure

random.seed(42)

# ── Helpers ───────────────────────────────────────────────────────────────────

def adaptive_trade_impact(trade_count: int) -> float:
    s = Market._volume_stability(trade_count)
    return MM_TRADE_IMPACT_LOW - s * (MM_TRADE_IMPACT_LOW - MM_TRADE_IMPACT_HIGH)

def simulate_trade_nudge(fair: float, trade_count: int, is_buy: bool) -> float:
    """Simulate a single market order nudging the fair price."""
    impact = adaptive_trade_impact(trade_count)
    # Buys push price above fair; sells push below.  Magnitude = 1–3% of fair.
    offset = random.uniform(0.01, 0.03) * fair * (1 if is_buy else -1)
    trade_price = fair + offset
    return fair + impact * (trade_price - fair)

def simulate_daily_revenue(channel_activity: str = "medium") -> float:
    """Return a random daily revenue figure based on channel activity."""
    if channel_activity == "low":
        base = REVENUE_PER_DAY_LOW
    elif channel_activity == "high":
        base = REVENUE_PER_DAY_HIGH
    else:
        base = (REVENUE_PER_DAY_LOW + REVENUE_PER_DAY_HIGH) / 2
    return base * random.uniform(0.7, 1.3)

def weekly_settle(fair: float, weekly_rev: float, last_rev: float,
                  treasury: float, trade_count: int,
                  weeks_of_history: int) -> tuple[float, float]:
    """One weekly settlement cycle. Returns (new_fair, new_treasury)."""
    # Apply noise like the real settle does
    gross = weekly_rev * random.uniform(0.95, 1.05)
    costs = gross * 0.05          # simplified: 5% cost ratio
    actual = max(0.0, gross - costs)

    retained = actual * (1 - DIVIDEND_PCT)
    upkeep = treasury * MM_TREASURY_UPKEEP_RATE
    new_treasury = max(0.0, treasury - upkeep + retained)

    new_fair = Market.compute_fair_price(
        fair, actual, last_rev,
        ipo_price=IPO_PRICE,
        weeks_of_history=weeks_of_history,
        recent_trade_prices=None,   # weekly sim doesn't track recent trades
        treasury=new_treasury,
        total_shares=IPO_TOTAL_SHARES,
        trade_count=trade_count,
    )
    # Treasury growth boost — only if treasury actually grew this week
    market_cap = new_fair * IPO_TOTAL_SHARES
    treasury_grew = retained > upkeep
    if market_cap > 0 and new_treasury > 0 and treasury_grew:
        boost = (new_treasury / market_cap) * MM_TREASURY_GROWTH_RATE
        new_fair *= (1 + boost)
    new_fair = max(new_fair, 0.01)
    return new_fair, new_treasury, actual

# ═════════════════════════════════════════════════════════════════════════════
# Scenario 1 — Intraday  (per-trade resolution, single day)
# ═════════════════════════════════════════════════════════════════════════════

def run_intraday(starting_trades: int = 0, label: str = ""):
    """Simulate one trading session of INTRADAY_TRADES trades.
    starting_trades controls maturity (0 = brand-new, 500 = mature)."""
    fair = IPO_PRICE
    trade_count = starting_trades
    prices = [fair]
    labels_x = [0]

    for i in range(1, INTRADAY_TRADES + 1):
        is_buy = random.random() < SENTIMENT
        fair = simulate_trade_nudge(fair, trade_count, is_buy)
        fair = max(fair, 0.01)
        trade_count += 1
        prices.append(fair)
        labels_x.append(i)

    title = f"Intraday — {label or f'{starting_trades} prior trades'}"
    return labels_x, prices, title

# ═════════════════════════════════════════════════════════════════════════════
# Scenario 2 — One week  (per-day resolution)
# ═════════════════════════════════════════════════════════════════════════════

def run_one_week(channel_activity: str = "medium", starting_trades: int = 0):
    fair = IPO_PRICE
    trade_count = starting_trades
    treasury = TREASURY
    last_rev = 0.0
    weekly_rev = 0.0

    prices = [fair]
    labels_x = [0]

    for day in range(1, WEEKLY_DAYS + 1):
        # Intraday trading
        for _ in range(TRADES_PER_DAY):
            is_buy = random.random() < SENTIMENT
            fair = simulate_trade_nudge(fair, trade_count, is_buy)
            fair = max(fair, 0.01)
            trade_count += 1

        # Revenue accumulates daily
        daily_rev = simulate_daily_revenue(channel_activity)
        weekly_rev += daily_rev

        prices.append(fair)
        labels_x.append(day)

    # End-of-week settlement on the last day
    fair, treasury, actual_rev = weekly_settle(
        fair, weekly_rev, last_rev, treasury, trade_count, weeks_of_history=1
    )
    prices[-1] = fair   # replace last day price with post-settlement price

    title = f"One Week ({channel_activity} activity, {starting_trades} prior trades)"
    return labels_x, prices, title

# ═════════════════════════════════════════════════════════════════════════════
# Scenario 3 — Multi-week  (per-week resolution)
# ═════════════════════════════════════════════════════════════════════════════

def run_multiweek(channel_activity: str = "medium"):
    fair = IPO_PRICE
    trade_count = 0
    treasury = TREASURY
    last_rev = 0.0

    prices = [fair]
    labels_x = [0]

    for week in range(1, MULTIWEEK_WEEKS + 1):
        # Accumulate trades and revenue for the week
        weekly_rev = 0.0
        for day in range(WEEKLY_DAYS):
            for _ in range(TRADES_PER_DAY):
                is_buy = random.random() < SENTIMENT
                fair = simulate_trade_nudge(fair, trade_count, is_buy)
                fair = max(fair, 0.01)
                trade_count += 1
            weekly_rev += simulate_daily_revenue(channel_activity)

        fair, treasury, last_rev = weekly_settle(
            fair, weekly_rev, last_rev, treasury, trade_count,
            weeks_of_history=week
        )
        prices.append(fair)
        labels_x.append(week)

    title = f"Multi-week ({channel_activity} activity, {MULTIWEEK_WEEKS} weeks)"
    return labels_x, prices, title

# ═════════════════════════════════════════════════════════════════════════════
# Plotting
# ═════════════════════════════════════════════════════════════════════════════

def plot(axes_data: list[tuple]):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError:
        print("matplotlib not installed — printing table output only")
        return

    n = len(axes_data)
    fig, axes = plt.subplots(1, n, figsize=(7 * n, 5))
    if n == 1:
        axes = [axes]

    for ax, (xs, ys, title) in zip(axes, axes_data):
        color = "#5865F2"
        ax.plot(xs, ys, color=color, linewidth=1.8, marker="o", markersize=3)
        ax.fill_between(xs, ys, IPO_PRICE, where=[y >= IPO_PRICE for y in ys],
                        alpha=0.12, color="#43b581", label="above IPO")
        ax.fill_between(xs, ys, IPO_PRICE, where=[y < IPO_PRICE for y in ys],
                        alpha=0.12, color="#f04747", label="below IPO")
        ax.axhline(IPO_PRICE, color="grey", linewidth=0.8, linestyle="--", label=f"IPO {IPO_PRICE:.0f}")

        # Annotate first and last
        ax.annotate(f"{ys[0]:.2f}", (xs[0], ys[0]), textcoords="offset points",
                    xytext=(6, 4), fontsize=8, color="grey")
        ax.annotate(f"{ys[-1]:.2f}", (xs[-1], ys[-1]), textcoords="offset points",
                    xytext=(6, 4), fontsize=9, fontweight="bold", color=color)

        chg = (ys[-1] - ys[0]) / ys[0] * 100
        sign = "+" if chg >= 0 else ""
        ax.set_title(f"{title}\n{sign}{chg:.1f}% total change", fontsize=10)
        ax.set_ylabel("Fair Price")
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)

    fig.suptitle("Market Price Simulation", fontsize=13, fontweight="bold", y=1.02)
    fig.tight_layout()
    out = "simulation_output.png"
    fig.savefig(out, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"\nChart saved → {out}")

# ═════════════════════════════════════════════════════════════════════════════
# Text table output
# ═════════════════════════════════════════════════════════════════════════════

def print_table(xs, ys, title, x_label="t"):
    w = max(len(str(x)) for x in xs)
    sep = "-" * 50
    print(f"\n{sep}")
    print(f"  {title}")
    print(sep)
    print(f"  {x_label:>{w}}   Price      d(prev)   d(IPO)")
    prev = ys[0]
    for x, y in zip(xs, ys):
        d_prev = y - prev
        d_ipo  = y - IPO_PRICE
        print(f"  {x:>{w}}   {y:>8.2f}   {d_prev:>+7.2f}   {d_ipo:>+7.2f}")
        prev = y
    print(sep)
    final_chg = (ys[-1] - ys[0]) / ys[0] * 100
    sign = "+" if final_chg >= 0 else ""
    print(f"  Total change: {sign}{final_chg:.1f}%")

# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"IPO price: {IPO_PRICE}  |  Float: {IPO_TOTAL_SHARES} shares  |  Maturity at: {MM_STABILITY_MATURITY} trades")
    print(f"Sentiment: {SENTIMENT:.0%} buys  |  Activity: low={REVENUE_PER_DAY_LOW} / high={REVENUE_PER_DAY_HIGH} rev/day")

    # ── Intraday ────────────────────────────────────────────────────────────
    xs_id_new,   ys_id_new,   title_id_new   = run_intraday(starting_trades=0,   label="Brand-new stock")
    xs_id_mid,   ys_id_mid,   title_id_mid   = run_intraday(starting_trades=250,  label="Mid-maturity (250 trades)")
    xs_id_mat,   ys_id_mat,   title_id_mat   = run_intraday(starting_trades=500,  label="Mature stock (500 trades)")

    print_table(xs_id_new, ys_id_new, title_id_new, x_label="trade")
    print_table(xs_id_mid, ys_id_mid, title_id_mid, x_label="trade")
    print_table(xs_id_mat, ys_id_mat, title_id_mat, x_label="trade")

    # ── One-week ────────────────────────────────────────────────────────────
    xs_wk_low,  ys_wk_low,  title_wk_low  = run_one_week("low",    starting_trades=0)
    xs_wk_med,  ys_wk_med,  title_wk_med  = run_one_week("medium", starting_trades=0)
    xs_wk_high, ys_wk_high, title_wk_high = run_one_week("high",   starting_trades=0)

    print_table(xs_wk_low,  ys_wk_low,  title_wk_low,  x_label="day")
    print_table(xs_wk_med,  ys_wk_med,  title_wk_med,  x_label="day")
    print_table(xs_wk_high, ys_wk_high, title_wk_high, x_label="day")

    # ── Multi-week ──────────────────────────────────────────────────────────
    xs_mw_low,  ys_mw_low,  title_mw_low  = run_multiweek("low")
    xs_mw_med,  ys_mw_med,  title_mw_med  = run_multiweek("medium")
    xs_mw_high, ys_mw_high, title_mw_high = run_multiweek("high")

    print_table(xs_mw_low,  ys_mw_low,  title_mw_low,  x_label="week")
    print_table(xs_mw_med,  ys_mw_med,  title_mw_med,  x_label="week")
    print_table(xs_mw_high, ys_mw_high, title_mw_high, x_label="week")

    # ── Plot everything ─────────────────────────────────────────────────────
    # Row 1: intraday comparisons (maturity)
    # Row 2: one-week comparisons (activity level)
    # Row 3: multi-week comparisons (activity level)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker

        fig, axes = plt.subplots(3, 3, figsize=(21, 15))

        rows = [
            [
                (xs_id_new, ys_id_new, title_id_new, "trade"),
                (xs_id_mid, ys_id_mid, title_id_mid, "trade"),
                (xs_id_mat, ys_id_mat, title_id_mat, "trade"),
            ],
            [
                (xs_wk_low,  ys_wk_low,  title_wk_low,  "day"),
                (xs_wk_med,  ys_wk_med,  title_wk_med,  "day"),
                (xs_wk_high, ys_wk_high, title_wk_high, "day"),
            ],
            [
                (xs_mw_low,  ys_mw_low,  title_mw_low,  "week"),
                (xs_mw_med,  ys_mw_med,  title_mw_med,  "week"),
                (xs_mw_high, ys_mw_high, title_mw_high, "week"),
            ],
        ]

        for row_i, row in enumerate(rows):
            for col_i, (xs, ys, title, x_label) in enumerate(row):
                ax = axes[row_i][col_i]
                color = "#5865F2"
                ax.plot(xs, ys, color=color, linewidth=1.6,
                        marker="o" if len(xs) <= 60 else None, markersize=3)
                ax.fill_between(xs, ys, IPO_PRICE,
                                where=[y >= IPO_PRICE for y in ys],
                                alpha=0.12, color="#43b581")
                ax.fill_between(xs, ys, IPO_PRICE,
                                where=[y < IPO_PRICE for y in ys],
                                alpha=0.12, color="#f04747")
                ax.axhline(IPO_PRICE, color="grey", linewidth=0.8,
                           linestyle="--", label=f"IPO {IPO_PRICE:.0f}")
                ax.annotate(f"{ys[-1]:.2f}", (xs[-1], ys[-1]),
                            textcoords="offset points", xytext=(6, 4),
                            fontsize=9, fontweight="bold", color=color)
                chg = (ys[-1] - ys[0]) / ys[0] * 100
                sign = "+" if chg >= 0 else ""
                ax.set_title(f"{title}\n{sign}{chg:.1f}% total", fontsize=9)
                ax.set_xlabel(x_label)
                ax.set_ylabel("Price")
                ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f"))
                ax.grid(alpha=0.25)

        row_labels = ["Intraday (per trade)", "One Week (per day)", "Multi-week (per week)"]
        for ax, label in zip(axes[:, 0], row_labels):
            ax.set_ylabel(f"{label}\nPrice", fontsize=9)

        fig.suptitle("Market Price Simulation — Volume-Adaptive MM", fontsize=14,
                     fontweight="bold")
        fig.tight_layout()
        out = "simulation_output.png"
        fig.savefig(out, dpi=120, bbox_inches="tight")
        plt.close(fig)
        print(f"\nChart saved → {out}")

    except ImportError:
        print("\nmatplotlib not available — text output only")
