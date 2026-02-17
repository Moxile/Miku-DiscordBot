"""
Standalone test script for market revenue & dividend simulation.
Simulates multiple weeks of channel activity and settlement without Discord.

Usage: python test_market.py
"""
import asyncio
import random
import datetime
import aiosqlite

# Reuse pure functions and constants from the market cog
from cogs.market import (
    compute_weighted_chars,
    Market,
    MM_USER_ID,
    MM_STARTING_CASH,
    MM_STARTING_SHARES,
    IPO_TOTAL_SHARES,
)

# -- Config ----------------------------------------------------------
NUM_WEEKS = 4
IPO_PRICE = 100.0
DIVIDEND_PCT = 0.10
CHANNEL_ID = 1
GUILD_ID = 1

# Fake users: (user_id, shares_owned, starting_cash)
USERS = [
    (1001, 50, 5000),
    (1002, 100, 5000),
    (1003, 30, 5000),
]
# Messages per user per day: (min, max) char count per message, messages per day
MSGS_PER_DAY = (2, 6)
CHARS_PER_MSG = (20, 400)


async def create_tables(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS economy (
            user_id INTEGER PRIMARY KEY,
            cash INTEGER DEFAULT 0,
            bank INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS companies (
            channel_id   INTEGER PRIMARY KEY,
            guild_id     INTEGER NOT NULL,
            name         TEXT NOT NULL,
            ipo_price    REAL NOT NULL,
            fair_price   REAL NOT NULL,
            last_revenue REAL DEFAULT 0,
            total_shares INTEGER DEFAULT 1000,
            dividend_pct REAL DEFAULT 0.10,
            created_at   TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS orders (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id    INTEGER NOT NULL,
            channel_id  INTEGER NOT NULL,
            user_id     INTEGER NOT NULL,
            side        TEXT NOT NULL,
            price       REAL NOT NULL,
            quantity    INTEGER NOT NULL,
            remaining   INTEGER NOT NULL,
            is_mm       INTEGER DEFAULT 0,
            created_at  TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS holdings (
            user_id    INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            quantity   INTEGER NOT NULL DEFAULT 0,
            avg_cost   REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (user_id, channel_id)
        );
        CREATE TABLE IF NOT EXISTS mm_state (
            channel_id      INTEGER PRIMARY KEY,
            cash            REAL NOT NULL,
            inventory       INTEGER NOT NULL,
            fair_price      REAL NOT NULL,
            volatility      REAL DEFAULT 0.01,
            last_quote_time TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trades (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            buyer_id   INTEGER NOT NULL,
            seller_id  INTEGER NOT NULL,
            price      REAL NOT NULL,
            quantity   INTEGER NOT NULL,
            timestamp  TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS channel_revenue (
            channel_id          INTEGER NOT NULL,
            week_start          TEXT NOT NULL,
            accumulated_revenue REAL DEFAULT 0,
            last_revenue        REAL DEFAULT 0,
            PRIMARY KEY (channel_id, week_start)
        );
        CREATE TABLE IF NOT EXISTS user_daily_chars (
            user_id    INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            date       TEXT NOT NULL,
            char_count INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, channel_id, date)
        );
        CREATE TABLE IF NOT EXISTS price_history (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            timestamp  TEXT NOT NULL,
            price      REAL NOT NULL
        );
    """)


async def setup_ipo(db: aiosqlite.Connection):
    """Create the company, MM state, and initial user holdings."""
    now = datetime.datetime.utcnow().isoformat()
    total_user_shares = sum(u[1] for u in USERS)
    mm_inventory = MM_STARTING_SHARES - total_user_shares
    mm_cash = MM_STARTING_CASH + total_user_shares * IPO_PRICE  # MM sold shares to users

    await db.execute(
        "INSERT INTO companies (channel_id, guild_id, name, ipo_price, fair_price, "
        "last_revenue, total_shares, dividend_pct, created_at) "
        "VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)",
        (CHANNEL_ID, GUILD_ID, "test-channel", IPO_PRICE, IPO_PRICE,
         IPO_TOTAL_SHARES, DIVIDEND_PCT, now),
    )
    await db.execute(
        "INSERT INTO mm_state (channel_id, cash, inventory, fair_price, volatility, last_quote_time) "
        "VALUES (?, ?, ?, ?, 0.01, ?)",
        (CHANNEL_ID, mm_cash, mm_inventory, IPO_PRICE, now),
    )
    await db.execute(
        "INSERT INTO holdings (user_id, channel_id, quantity, avg_cost) VALUES (?, ?, ?, ?)",
        (MM_USER_ID, CHANNEL_ID, mm_inventory, IPO_PRICE),
    )

    for user_id, shares, cash in USERS:
        await db.execute(
            "INSERT INTO economy (user_id, cash, bank) VALUES (?, ?, 0)",
            (user_id, cash),
        )
        await db.execute(
            "INSERT INTO holdings (user_id, channel_id, quantity, avg_cost) VALUES (?, ?, ?, ?)",
            (user_id, CHANNEL_ID, shares, IPO_PRICE),
        )

    await db.commit()


def week_start(dt: datetime.date) -> datetime.date:
    return dt - datetime.timedelta(days=dt.weekday())


async def simulate_week_activity(db: aiosqlite.Connection, week_monday: datetime.date):
    """Simulate 7 days of message activity and accumulate revenue."""
    ws = week_monday.isoformat()

    for day_offset in range(7):
        day = week_monday + datetime.timedelta(days=day_offset)
        day_str = day.isoformat()

        for user_id, _, _ in USERS:
            num_msgs = random.randint(*MSGS_PER_DAY)
            for _ in range(num_msgs):
                chars = random.randint(*CHARS_PER_MSG)

                # Read existing char count for today
                async with db.execute(
                    "SELECT char_count FROM user_daily_chars "
                    "WHERE user_id = ? AND channel_id = ? AND date = ?",
                    (user_id, CHANNEL_ID, day_str),
                ) as cur:
                    row = await cur.fetchone()
                old_chars = row[0] if row else 0
                new_total = old_chars + chars

                delta = compute_weighted_chars(new_total) - compute_weighted_chars(old_chars)

                await db.execute(
                    "INSERT INTO user_daily_chars (user_id, channel_id, date, char_count) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(user_id, channel_id, date) DO UPDATE SET char_count = char_count + ?",
                    (user_id, CHANNEL_ID, day_str, chars, chars),
                )

                await db.execute(
                    "INSERT INTO channel_revenue (channel_id, week_start, accumulated_revenue) "
                    "VALUES (?, ?, ?) "
                    "ON CONFLICT(channel_id, week_start) DO UPDATE "
                    "SET accumulated_revenue = accumulated_revenue + ?",
                    (CHANNEL_ID, ws, delta, delta),
                )

    await db.commit()


async def settle_week(db: aiosqlite.Connection, week_monday: datetime.date) -> dict:
    """Run weekly settlement. Returns summary dict."""
    ws = week_monday.isoformat()

    # Get accumulated revenue
    async with db.execute(
        "SELECT accumulated_revenue FROM channel_revenue "
        "WHERE channel_id = ? AND week_start = ?",
        (CHANNEL_ID, ws),
    ) as cur:
        row = await cur.fetchone()
    accumulated = row[0] if row else 0.0

    actual_revenue = accumulated * random.uniform(0.95, 1.05)

    # Get company
    async with db.execute(
        "SELECT fair_price, last_revenue, total_shares FROM companies WHERE channel_id = ?",
        (CHANNEL_ID,),
    ) as cur:
        comp = await cur.fetchone()
    fair_price, last_revenue, total_shares = comp

    # Pay dividends
    async with db.execute(
        "SELECT user_id, quantity FROM holdings "
        "WHERE channel_id = ? AND user_id != ? AND quantity > 0",
        (CHANNEL_ID, MM_USER_ID),
    ) as cur:
        holders = await cur.fetchall()

    dividends_total = actual_revenue * DIVIDEND_PCT
    payouts = {}
    if dividends_total > 0:
        for holder_id, qty in holders:
            payout = int(dividends_total * (qty / total_shares))
            if payout > 0:
                await db.execute(
                    "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
                    (payout, holder_id),
                )
                payouts[holder_id] = payout

    # Update company last_revenue
    await db.execute(
        "UPDATE companies SET last_revenue = ? WHERE channel_id = ?",
        (actual_revenue, CHANNEL_ID),
    )

    # Update fair price
    async with db.execute(
        "SELECT fair_price, inventory FROM mm_state WHERE channel_id = ?",
        (CHANNEL_ID,),
    ) as cur:
        mm = await cur.fetchone()
    mm_fair, mm_inventory = mm

    if last_revenue > 0:
        new_fair = Market.compute_fair_price(mm_fair, actual_revenue, last_revenue)
    else:
        new_fair = mm_fair

    await db.execute(
        "UPDATE mm_state SET fair_price = ? WHERE channel_id = ?",
        (new_fair, CHANNEL_ID),
    )
    await db.execute(
        "UPDATE companies SET fair_price = ? WHERE channel_id = ?",
        (new_fair, CHANNEL_ID),
    )
    await db.commit()

    # Compute MM quotes for display
    spread, skew = Market.compute_spread_and_skew(
        new_fair, 0.01, mm_inventory, 0, total_shares
    )
    bid = max(new_fair - spread / 2 + skew, 0.01)
    ask = max(new_fair + spread / 2 + skew, bid + 0.01)

    return {
        "accumulated": accumulated,
        "actual_revenue": actual_revenue,
        "dividends_total": dividends_total,
        "payouts": payouts,
        "old_fair": mm_fair,
        "new_fair": new_fair,
        "bid": bid,
        "ask": ask,
        "mm_inventory": mm_inventory,
    }


async def print_state(db: aiosqlite.Connection, label: str):
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"{'=' * 60}")

    async with db.execute(
        "SELECT fair_price, last_revenue FROM companies WHERE channel_id = ?",
        (CHANNEL_ID,),
    ) as cur:
        comp = await cur.fetchone()
    print(f"  Company fair price: ${comp[0]:,.2f}  |  Last revenue: ${comp[1]:,.0f}")

    async with db.execute(
        "SELECT cash, inventory, fair_price FROM mm_state WHERE channel_id = ?",
        (CHANNEL_ID,),
    ) as cur:
        mm = await cur.fetchone()
    print(f"  MM cash: ${mm[0]:,.2f}  |  MM inventory: {mm[1]} shares")

    print(f"  {'-' * 50}")
    print(f"  {'User':<10} {'Shares':>8} {'Cash':>12} {'Avg Cost':>10}")
    print(f"  {'-' * 50}")
    for user_id, _, _ in USERS:
        async with db.execute(
            "SELECT quantity, avg_cost FROM holdings "
            "WHERE user_id = ? AND channel_id = ?",
            (user_id, CHANNEL_ID),
        ) as cur:
            h = await cur.fetchone()
        qty, avg = h if h else (0, 0.0)
        async with db.execute(
            "SELECT cash FROM economy WHERE user_id = ?", (user_id,),
        ) as cur:
            cash = (await cur.fetchone())[0]
        print(f"  User {user_id:<5} {qty:>7,} {cash:>11,} ${avg:>9,.2f}")


async def main():
    random.seed(42)  # Reproducible results

    async with aiosqlite.connect(":memory:") as db:
        await create_tables(db)
        await setup_ipo(db)

        await print_state(db, "INITIAL STATE (Post-IPO)")

        # Simulate NUM_WEEKS weeks
        start_monday = datetime.date(2025, 1, 6)  # A Monday

        for week_num in range(1, NUM_WEEKS + 1):
            week_monday = start_monday + datetime.timedelta(weeks=week_num - 1)

            print(f"\n{'=' * 60}")
            print(f"  WEEK {week_num}  ({week_monday} -> {week_monday + datetime.timedelta(days=6)})")
            print(f"{'=' * 60}")

            await simulate_week_activity(db, week_monday)
            result = await settle_week(db, week_monday)

            print(f"  Accumulated revenue:  ${result['accumulated']:>10,.2f}")
            print(f"  Actual revenue:       ${result['actual_revenue']:>10,.2f}")
            print(f"  Dividends paid:       ${result['dividends_total']:>10,.2f}")
            print(f"  Fair price:           ${result['old_fair']:>8,.2f} -> ${result['new_fair']:,.2f}")
            print(f"  MM quotes:            bid ${result['bid']:,.2f}  /  ask ${result['ask']:,.2f}")

            if result["payouts"]:
                print(f"  Dividend payouts:")
                for uid, payout in sorted(result["payouts"].items()):
                    print(f"    User {uid}: +${payout:,}")

        await print_state(db, "FINAL STATE")


if __name__ == "__main__":
    asyncio.run(main())
