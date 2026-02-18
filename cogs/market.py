import io
import math
import random
import datetime
import aiosqlite
import discord
from discord.ext import commands, tasks
from utils import is_guild_owner, check_channel_allowed

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

DB_PATH = "data/economy.db"
MM_USER_ID = 0

# MM tuning constants
MM_ALPHA = 0.3
MM_BASE_SPREAD = 0.8
MM_A = 1.5
MM_B = 1.0
MM_C = 1.0
MM_K = 0.05
MM_TARGET_INVENTORY_PCT = 0.35
MM_MAX_WEEKLY_MOVE = 0.08        # Cap weekly fair-price change at +/-8%
MM_TRADE_IMPACT = 0.02           # Each trade nudges fair price 2% toward trade price
MM_STARTING_CASH = 30_000.0
MM_STARTING_SHARES = 1000
IPO_TOTAL_SHARES = 1000

MARKET_BUY_SENTINEL = 999_999.99
MARKET_SELL_SENTINEL = 0.01


# --- Revenue helpers (module-level, pure functions) ---

def compute_weighted_chars(total_chars: int) -> float:
    """Compute the total weighted character contribution for a user in a day."""
    if total_chars <= 0:
        return 0.0
    if total_chars <= 100:
        return total_chars * 1.2
    if total_chars <= 500:
        return 100 * 1.2 + (total_chars - 100) * 1.0
    # 100*1.2 + 400*1.0 = 520
    base = 520.0
    extra = 0.0
    for i in range(500, min(total_chars, 2000)):
        weight = 0.01 + 0.99 / (1 + math.exp(0.02 * (i - 500)))
        extra += weight
    # Beyond 2000 chars, weight is essentially 0.01 per char
    if total_chars > 2000:
        extra += (total_chars - 2000) * 0.01
    return base + extra


class Market(commands.Cog):
    _owner_commands = {"ipo", "setdividend", "companyinfo", "delist", "charstats"}

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: aiosqlite.Connection = None
        # Cache company channel IDs to avoid DB hit on every message
        self._company_channels: set[int] = set()

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.command.name in self._owner_commands:
            return True
        return await check_channel_allowed(
            self.db, ctx.guild.id, "market", ctx.channel.id, ctx.command.name
        )

    # ── Setup / Teardown ─────────────────────────────────────────────

    async def cog_load(self):
        self.db = await aiosqlite.connect(DB_PATH)
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS companies (
                channel_id   INTEGER PRIMARY KEY,
                guild_id     INTEGER NOT NULL,
                name         TEXT NOT NULL,
                ipo_price    REAL NOT NULL,
                fair_price   REAL NOT NULL,
                last_revenue REAL DEFAULT 0,
                total_shares INTEGER DEFAULT 1000,
                dividend_pct REAL DEFAULT 0.10,
                created_at   TEXT NOT NULL
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS orders (
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
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS holdings (
                user_id    INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                quantity   INTEGER NOT NULL DEFAULT 0,
                avg_cost   REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, channel_id)
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS mm_state (
                channel_id      INTEGER PRIMARY KEY,
                cash            REAL NOT NULL,
                inventory       INTEGER NOT NULL,
                fair_price      REAL NOT NULL,
                volatility      REAL DEFAULT 0.01,
                last_quote_time TEXT NOT NULL
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS trades (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                buyer_id   INTEGER NOT NULL,
                seller_id  INTEGER NOT NULL,
                price      REAL NOT NULL,
                quantity   INTEGER NOT NULL,
                timestamp  TEXT NOT NULL
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS channel_revenue (
                channel_id          INTEGER NOT NULL,
                week_start          TEXT NOT NULL,
                accumulated_revenue REAL DEFAULT 0,
                last_revenue        REAL DEFAULT 0,
                PRIMARY KEY (channel_id, week_start)
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS user_daily_chars (
                user_id    INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                date       TEXT NOT NULL,
                char_count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, channel_id, date)
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS market_settings (
                guild_id     INTEGER PRIMARY KEY,
                dividend_pct REAL DEFAULT 0.10
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS price_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                timestamp  TEXT NOT NULL,
                price      REAL NOT NULL
            )"""
        )
        await self.db.commit()

        # Load company channel cache
        async with self.db.execute("SELECT channel_id FROM companies") as cur:
            rows = await cur.fetchall()
        self._company_channels = {r[0] for r in rows}

        self.weekly_settlement_loop.start()
        self.hourly_mm_refresh_loop.start()

    async def cog_unload(self):
        self.weekly_settlement_loop.cancel()
        self.hourly_mm_refresh_loop.cancel()
        if self.db:
            await self.db.close()

    # ── DB Helpers ───────────────────────────────────────────────────

    async def get_cash(self, user_id: int) -> int:
        async with self.db.execute(
            "SELECT cash FROM economy WHERE user_id = ?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0

    async def update_cash(self, user_id: int, amount: int):
        await self.db.execute(
            "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
            (amount, user_id),
        )

    async def ensure_economy_row(self, user_id: int):
        await self.db.execute(
            "INSERT OR IGNORE INTO economy (user_id, cash, bank) VALUES (?, 0, 0)",
            (user_id,),
        )

    async def get_company(self, channel_id: int) -> dict | None:
        async with self.db.execute(
            "SELECT channel_id, guild_id, name, ipo_price, fair_price, last_revenue, "
            "total_shares, dividend_pct, created_at FROM companies WHERE channel_id = ?",
            (channel_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return {
            "channel_id": row[0], "guild_id": row[1], "name": row[2],
            "ipo_price": row[3], "fair_price": row[4], "last_revenue": row[5],
            "total_shares": row[6], "dividend_pct": row[7], "created_at": row[8],
        }

    async def get_holdings(self, user_id: int, channel_id: int) -> tuple[int, float]:
        async with self.db.execute(
            "SELECT quantity, avg_cost FROM holdings WHERE user_id = ? AND channel_id = ?",
            (user_id, channel_id),
        ) as cur:
            row = await cur.fetchone()
        return (row[0], row[1]) if row else (0, 0.0)

    async def update_holdings(self, user_id: int, channel_id: int, qty_delta: int,
                              price: float | None = None):
        """Update holdings. price is used for weighted avg cost on buys.
        Pass price=None to restore shares without changing avg_cost (e.g. on cancel)."""
        old_qty, old_avg = await self.get_holdings(user_id, channel_id)
        new_qty = old_qty + qty_delta
        if new_qty <= 0:
            await self.db.execute(
                "DELETE FROM holdings WHERE user_id = ? AND channel_id = ?",
                (user_id, channel_id),
            )
            return
        if qty_delta > 0 and price is not None and price > 0:
            new_avg = (old_qty * old_avg + qty_delta * price) / new_qty
        else:
            new_avg = old_avg
        await self.db.execute(
            """INSERT INTO holdings (user_id, channel_id, quantity, avg_cost)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, channel_id) DO UPDATE SET quantity = ?, avg_cost = ?""",
            (user_id, channel_id, new_qty, new_avg, new_qty, new_avg),
        )

    async def get_mm_state(self, channel_id: int) -> dict | None:
        async with self.db.execute(
            "SELECT channel_id, cash, inventory, fair_price, volatility, last_quote_time "
            "FROM mm_state WHERE channel_id = ?", (channel_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return {
            "channel_id": row[0], "cash": row[1], "inventory": row[2],
            "fair_price": row[3], "volatility": row[4], "last_quote_time": row[5],
        }

    async def cancel_mm_orders(self, channel_id: int):
        await self.db.execute(
            "DELETE FROM orders WHERE channel_id = ? AND is_mm = 1", (channel_id,),
        )

    async def get_daily_volume(self, channel_id: int) -> int:
        today = datetime.date.today().isoformat()
        async with self.db.execute(
            "SELECT COALESCE(SUM(quantity), 0) FROM trades "
            "WHERE channel_id = ? AND date(timestamp) = ?",
            (channel_id, today),
        ) as cur:
            row = await cur.fetchone()
        return row[0]

    async def get_recent_trade_prices(self, channel_id: int, n: int = 20) -> list[float]:
        async with self.db.execute(
            "SELECT price FROM trades WHERE channel_id = ? ORDER BY timestamp DESC LIMIT ?",
            (channel_id, n),
        ) as cur:
            rows = await cur.fetchall()
        return [r[0] for r in rows]

    async def record_trade(self, channel_id: int, buyer_id: int, seller_id: int,
                           price: float, quantity: int):
        now = datetime.datetime.utcnow().isoformat()
        await self.db.execute(
            "INSERT INTO trades (channel_id, buyer_id, seller_id, price, quantity, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (channel_id, buyer_id, seller_id, price, quantity, now),
        )

    async def record_price(self, channel_id: int, price: float):
        now = datetime.datetime.utcnow().isoformat()
        await self.db.execute(
            "INSERT INTO price_history (channel_id, timestamp, price) VALUES (?, ?, ?)",
            (channel_id, now, price),
        )

    async def get_dividend_pct(self, guild_id: int) -> float:
        async with self.db.execute(
            "SELECT dividend_pct FROM market_settings WHERE guild_id = ?", (guild_id,),
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0.10

    @staticmethod
    def _week_start(dt: datetime.date = None) -> datetime.date:
        if dt is None:
            dt = datetime.date.today()
        return dt - datetime.timedelta(days=dt.weekday())  # Monday

    # ── MM Engine ────────────────────────────────────────────────────

    async def compute_volatility(self, channel_id: int) -> float:
        prices = await self.get_recent_trade_prices(channel_id, 20)
        if len(prices) < 2:
            return 0.01
        returns = []
        for i in range(len(prices) - 1):
            if prices[i + 1] > 0 and prices[i] > 0:
                returns.append(math.log(prices[i] / prices[i + 1]))
        if not returns:
            return 0.01
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        return max(math.sqrt(variance), 0.001)

    @staticmethod
    def compute_fair_price(old_fair: float, estimated_revenue: float,
                           last_revenue: float) -> float:
        if last_revenue <= 0:
            return old_fair
        ratio = estimated_revenue / last_revenue
        new_fair = old_fair * (1 + MM_ALPHA * (ratio - 1))
        # Clamp so a single update can't move price more than MM_MAX_WEEKLY_MOVE
        lower = old_fair * (1 - MM_MAX_WEEKLY_MOVE)
        upper = old_fair * (1 + MM_MAX_WEEKLY_MOVE)
        return max(lower, min(upper, new_fair))

    @staticmethod
    def compute_spread_and_skew(fair_price: float, volatility: float,
                                inventory: int, daily_volume: int,
                                total_shares: int = 1000) -> tuple[float, float]:
        volume_target = MM_TARGET_INVENTORY_PCT * max(daily_volume, 100)
        supply_target = MM_TARGET_INVENTORY_PCT * total_shares
        target_inv = max(volume_target, supply_target)
        inv_dev = (inventory - target_inv) / max(target_inv, 1)

        a = max(1.0, min(2.0, MM_A * (350 / max(daily_volume, 1))))
        spread = MM_BASE_SPREAD * (a * volatility + MM_B * abs(inv_dev)) + MM_C * inv_dev ** 2
        # Spread as a fraction of fair price, minimum 0.5% of fair price
        spread = max(spread, fair_price * 0.005)
        skew = -MM_K * inv_dev * fair_price
        return spread, skew

    async def refresh_mm_quotes(self, channel_id: int):
        company = await self.get_company(channel_id)
        if not company:
            return
        mm = await self.get_mm_state(channel_id)
        if not mm:
            return

        # Update volatility
        volatility = await self.compute_volatility(channel_id)

        # Compute estimated revenue
        week_start = self._week_start().isoformat()
        async with self.db.execute(
            "SELECT accumulated_revenue FROM channel_revenue "
            "WHERE channel_id = ? AND week_start = ?",
            (channel_id, week_start),
        ) as cur:
            rev_row = await cur.fetchone()
        accumulated = rev_row[0] if rev_row else 0.0

        today = datetime.date.today()
        days_elapsed = max((today - self._week_start()).days, 1)
        estimated_revenue = (accumulated / days_elapsed) * 7

        # Update fair price
        fair_price = self.compute_fair_price(
            mm["fair_price"], estimated_revenue, company["last_revenue"]
        )
        fair_price = max(fair_price, 0.01)

        daily_volume = await self.get_daily_volume(channel_id)
        spread, skew = self.compute_spread_and_skew(
            fair_price, volatility, mm["inventory"], daily_volume, company["total_shares"]
        )

        bid = max(fair_price - spread / 2 + skew, 0.01)
        ask = max(fair_price + spread / 2 + skew, bid + 0.01)

        # Cancel old MM orders
        await self.cancel_mm_orders(channel_id)

        now = datetime.datetime.utcnow().isoformat()

        # MM buy order — only if MM has cash
        bid_qty = min(100, int(mm["cash"] / bid)) if bid > 0 else 0
        if bid_qty > 0:
            await self.db.execute(
                "INSERT INTO orders (guild_id, channel_id, user_id, side, price, quantity, remaining, is_mm, created_at) "
                "VALUES (?, ?, ?, 'buy', ?, ?, ?, 1, ?)",
                (company["guild_id"], channel_id, MM_USER_ID,
                 round(bid, 2), bid_qty, bid_qty, now),
            )

        # MM sell order — only if MM has shares
        ask_qty = min(100, mm["inventory"])
        if ask_qty > 0:
            await self.db.execute(
                "INSERT INTO orders (guild_id, channel_id, user_id, side, price, quantity, remaining, is_mm, created_at) "
                "VALUES (?, ?, ?, 'sell', ?, ?, ?, 1, ?)",
                (company["guild_id"], channel_id, MM_USER_ID,
                 round(ask, 2), ask_qty, ask_qty, now),
            )

        # Update state
        await self.db.execute(
            "UPDATE mm_state SET fair_price = ?, volatility = ?, last_quote_time = ? "
            "WHERE channel_id = ?",
            (fair_price, volatility, now, channel_id),
        )
        await self.db.execute(
            "UPDATE companies SET fair_price = ? WHERE channel_id = ?",
            (fair_price, channel_id),
        )
        await self.db.commit()

    # ── Order Matching Engine ────────────────────────────────────────

    async def execute_trade(self, channel_id: int, buyer_id: int, seller_id: int,
                            price: float, quantity: int):
        """Execute a trade. Cash/shares are already reserved via order placement.
        This only handles delivery: buyer gets shares, seller gets cash.
        MM is special — its orders don't pre-reserve, so we adjust mm_state directly."""
        cost = price * quantity

        # Buyer receives shares
        if buyer_id == MM_USER_ID:
            await self.db.execute(
                "UPDATE mm_state SET cash = cash - ?, inventory = inventory + ? "
                "WHERE channel_id = ?", (cost, quantity, channel_id),
            )
        else:
            # Buyer's cash was already reserved on order placement — just give shares
            await self.update_holdings(buyer_id, channel_id, quantity, price)

        # Seller receives cash
        if seller_id == MM_USER_ID:
            await self.db.execute(
                "UPDATE mm_state SET cash = cash + ?, inventory = inventory - ? "
                "WHERE channel_id = ?", (cost, quantity, channel_id),
            )
        else:
            # Seller's shares were already reserved on order placement — just give cash
            await self.update_cash(seller_id, int(cost))

        await self.record_trade(channel_id, buyer_id, seller_id, price, quantity)
        await self.record_price(channel_id, price)

        # Nudge fair price toward trade price for organic movement
        mm = await self.get_mm_state(channel_id)
        if mm:
            nudged = mm["fair_price"] + MM_TRADE_IMPACT * (price - mm["fair_price"])
            await self.db.execute(
                "UPDATE mm_state SET fair_price = ? WHERE channel_id = ?",
                (nudged, channel_id),
            )
            await self.db.execute(
                "UPDATE companies SET fair_price = ? WHERE channel_id = ?",
                (nudged, channel_id),
            )

    async def match_orders(self, channel_id: int, new_order_id: int) -> list[dict]:
        """Match a newly placed order against the book. Returns list of fills."""
        async with self.db.execute(
            "SELECT id, user_id, side, price, remaining FROM orders WHERE id = ?",
            (new_order_id,),
        ) as cur:
            order_row = await cur.fetchone()
        if not order_row:
            return []

        order_id, user_id, side, price, remaining = order_row
        fills = []

        if side == "buy":
            # Match against sells: lowest price first, then oldest
            async with self.db.execute(
                "SELECT id, user_id, price, remaining FROM orders "
                "WHERE channel_id = ? AND side = 'sell' AND price <= ? AND id != ? "
                "ORDER BY price ASC, created_at ASC",
                (channel_id, price, order_id),
            ) as cur:
                asks = await cur.fetchall()

            for ask_id, seller_id, ask_price, ask_remaining in asks:
                if remaining <= 0:
                    break
                fill_qty = min(remaining, ask_remaining)
                fill_price = ask_price  # fill at resting order's price

                await self.execute_trade(channel_id, user_id, seller_id, fill_price, fill_qty)

                remaining -= fill_qty
                ask_remaining -= fill_qty

                if ask_remaining <= 0:
                    await self.db.execute("DELETE FROM orders WHERE id = ?", (ask_id,))
                else:
                    await self.db.execute(
                        "UPDATE orders SET remaining = ? WHERE id = ?",
                        (ask_remaining, ask_id),
                    )

                fills.append({"price": fill_price, "quantity": fill_qty, "counterparty": seller_id})

        else:  # sell
            # Match against buys: highest price first, then oldest
            async with self.db.execute(
                "SELECT id, user_id, price, remaining FROM orders "
                "WHERE channel_id = ? AND side = 'buy' AND price >= ? AND id != ? "
                "ORDER BY price DESC, created_at ASC",
                (channel_id, price, order_id),
            ) as cur:
                bids = await cur.fetchall()

            for bid_id, buyer_id, bid_price, bid_remaining in bids:
                if remaining <= 0:
                    break
                fill_qty = min(remaining, bid_remaining)
                fill_price = bid_price

                await self.execute_trade(channel_id, buyer_id, user_id, fill_price, fill_qty)

                remaining -= fill_qty
                bid_remaining -= fill_qty

                if bid_remaining <= 0:
                    await self.db.execute("DELETE FROM orders WHERE id = ?", (bid_id,))
                else:
                    await self.db.execute(
                        "UPDATE orders SET remaining = ? WHERE id = ?",
                        (bid_remaining, bid_id),
                    )

                fills.append({"price": fill_price, "quantity": fill_qty, "counterparty": buyer_id})

        # Update or remove the new order
        if remaining <= 0:
            await self.db.execute("DELETE FROM orders WHERE id = ?", (order_id,))
        else:
            await self.db.execute(
                "UPDATE orders SET remaining = ? WHERE id = ?",
                (remaining, order_id),
            )

        await self.db.commit()

        # Refresh MM quotes after any trade
        if fills:
            await self.refresh_mm_quotes(channel_id)

        return fills

    async def place_limit_order(self, guild_id: int, channel_id: int, user_id: int,
                                side: str, price: float, quantity: int,
                                is_mm: int = 0) -> tuple[int, list[dict]]:
        """Insert a limit order and run matching. Returns (order_id, fills)."""
        now = datetime.datetime.utcnow().isoformat()
        async with self.db.execute(
            "INSERT INTO orders (guild_id, channel_id, user_id, side, price, quantity, remaining, is_mm, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (guild_id, channel_id, user_id, side, round(price, 2), quantity, quantity, is_mm, now),
        ) as cur:
            order_id = cur.lastrowid
        await self.db.commit()

        fills = await self.match_orders(channel_id, order_id)
        return order_id, fills

    # ── Revenue System ───────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        if message.channel.id not in self._company_channels:
            return
        char_count = len(message.content)
        if char_count <= 0:
            return

        channel_id = message.channel.id
        user_id = message.author.id
        today = datetime.date.today().isoformat()

        # Get existing char count for today
        async with self.db.execute(
            "SELECT char_count FROM user_daily_chars "
            "WHERE user_id = ? AND channel_id = ? AND date = ?",
            (user_id, channel_id, today),
        ) as cur:
            row = await cur.fetchone()
        old_chars = row[0] if row else 0
        new_total = old_chars + char_count

        # Compute incremental weighted contribution
        delta = compute_weighted_chars(new_total) - compute_weighted_chars(old_chars)

        # Update char count
        await self.db.execute(
            """INSERT INTO user_daily_chars (user_id, channel_id, date, char_count)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, channel_id, date) DO UPDATE SET char_count = char_count + ?""",
            (user_id, channel_id, today, char_count, char_count),
        )

        # Add to weekly revenue
        week_start = self._week_start().isoformat()
        await self.db.execute(
            """INSERT INTO channel_revenue (channel_id, week_start, accumulated_revenue)
               VALUES (?, ?, ?)
               ON CONFLICT(channel_id, week_start) DO UPDATE
               SET accumulated_revenue = accumulated_revenue + ?""",
            (channel_id, week_start, delta, delta),
        )
        await self.db.commit()

    async def settle_weekly_revenue(self, channel_id: int):
        company = await self.get_company(channel_id)
        if not company:
            return

        week_start = self._week_start().isoformat()
        async with self.db.execute(
            "SELECT accumulated_revenue FROM channel_revenue "
            "WHERE channel_id = ? AND week_start = ?",
            (channel_id, week_start),
        ) as cur:
            row = await cur.fetchone()
        accumulated = row[0] if row else 0.0

        # Apply small random factor
        actual_revenue = accumulated * random.uniform(0.95, 1.05)

        # Compute dividends
        dividend_pct = await self.get_dividend_pct(company["guild_id"])
        dividends_total = actual_revenue * dividend_pct

        # Get all holders (excluding MM)
        async with self.db.execute(
            "SELECT user_id, quantity FROM holdings "
            "WHERE channel_id = ? AND user_id != ? AND quantity > 0",
            (channel_id, MM_USER_ID),
        ) as cur:
            holders = await cur.fetchall()

        # Total shares held by players
        total_player_shares = sum(h[1] for h in holders)
        if total_player_shares > 0 and dividends_total > 0:
            for holder_id, qty in holders:
                share_pct = qty / company["total_shares"]
                payout = int(dividends_total * share_pct)
                if payout > 0:
                    await self.ensure_economy_row(holder_id)
                    await self.update_cash(holder_id, payout)

        # Update company state
        await self.db.execute(
            "UPDATE companies SET last_revenue = ? WHERE channel_id = ?",
            (actual_revenue, channel_id),
        )
        await self.db.execute(
            "UPDATE channel_revenue SET last_revenue = ? "
            "WHERE channel_id = ? AND week_start = ?",
            (actual_revenue, channel_id, week_start),
        )
        await self.db.commit()

        # Update fair price based on actual revenue
        mm = await self.get_mm_state(channel_id)
        if mm and company["last_revenue"] > 0:
            new_fair = self.compute_fair_price(
                mm["fair_price"], actual_revenue, company["last_revenue"]
            )
            await self.db.execute(
                "UPDATE mm_state SET fair_price = ? WHERE channel_id = ?",
                (new_fair, channel_id),
            )
            await self.db.execute(
                "UPDATE companies SET fair_price = ? WHERE channel_id = ?",
                (new_fair, channel_id),
            )
            await self.db.commit()

        await self.refresh_mm_quotes(channel_id)

        # Announce dividends if there's a channel we can post to
        channel = self.bot.get_channel(channel_id)
        if channel and dividends_total > 0:
            embed = discord.Embed(
                title=f"Weekly Report — {company['name']}",
                color=discord.Color.gold(),
            )
            embed.add_field(name="Revenue", value=f"{actual_revenue:,.0f} \U0001f338")
            embed.add_field(name="Dividends Paid", value=f"{dividends_total:,.0f} \U0001f338")
            embed.add_field(name="Shareholders", value=str(len(holders)))
            try:
                await channel.send(embed=embed)
            except discord.Forbidden:
                pass

    # ── Task Loops ───────────────────────────────────────────────────

    @tasks.loop(time=datetime.time(hour=23, minute=55, tzinfo=datetime.timezone.utc))
    async def weekly_settlement_loop(self):
        if datetime.datetime.now(datetime.timezone.utc).weekday() != 6:
            return
        async with self.db.execute("SELECT channel_id FROM companies") as cur:
            rows = await cur.fetchall()
        for (channel_id,) in rows:
            await self.settle_weekly_revenue(channel_id)

    @weekly_settlement_loop.before_loop
    async def before_weekly_settlement(self):
        await self.bot.wait_until_ready()

    @tasks.loop(hours=1)
    async def hourly_mm_refresh_loop(self):
        async with self.db.execute("SELECT channel_id FROM companies") as cur:
            rows = await cur.fetchall()
        for (channel_id,) in rows:
            await self.refresh_mm_quotes(channel_id)

    @hourly_mm_refresh_loop.before_loop
    async def before_hourly_mm_refresh(self):
        await self.bot.wait_until_ready()

    # ── Commands ─────────────────────────────────────────────────────

    @commands.command()
    async def ipo(self, ctx: commands.Context, channel: discord.TextChannel, price: float = None):
        """Register a channel as a company on the stock market.
        Usage: {prefix}ipo #channel 100"""
        if price is None:
            price = 100.0
        if price <= 0:
            await ctx.send("IPO price must be positive.")
            return

        existing = await self.get_company(channel.id)
        if existing:
            await ctx.send(f"**#{channel.name}** is already a listed company.")
            return

        now = datetime.datetime.utcnow().isoformat()

        await self.db.execute(
            "INSERT INTO companies (channel_id, guild_id, name, ipo_price, fair_price, "
            "last_revenue, total_shares, dividend_pct, created_at) "
            "VALUES (?, ?, ?, ?, ?, 0, ?, 0.10, ?)",
            (channel.id, ctx.guild.id, channel.name, price, price,
             IPO_TOTAL_SHARES, now),
        )
        await self.db.execute(
            "INSERT INTO mm_state (channel_id, cash, inventory, fair_price, volatility, last_quote_time) "
            "VALUES (?, ?, ?, ?, 0.01, ?)",
            (channel.id, MM_STARTING_CASH, MM_STARTING_SHARES, price, now),
        )
        await self.db.execute(
            "INSERT INTO holdings (user_id, channel_id, quantity, avg_cost) VALUES (?, ?, ?, ?)",
            (MM_USER_ID, channel.id, MM_STARTING_SHARES, price),
        )

        # Record initial price
        await self.record_price(channel.id, price)
        await self.db.commit()

        self._company_channels.add(channel.id)

        # Place initial MM quotes
        await self.refresh_mm_quotes(channel.id)

        embed = discord.Embed(
            title="IPO Successful",
            description=f"**#{channel.name}** is now listed on the market!",
            color=discord.Color.green(),
        )
        embed.add_field(name="IPO Price", value=f"{price:,.2f} \U0001f338")
        embed.add_field(name="Total Shares", value=f"{IPO_TOTAL_SHARES:,}")
        embed.add_field(name="Market Cap", value=f"{price * IPO_TOTAL_SHARES:,.0f} \U0001f338")
        embed.set_footer(text=f"Use {ctx.prefix}orderbook to see the order book · {ctx.prefix}mbuy to purchase shares")
        await ctx.send(embed=embed)

    @commands.command()
    async def limitbuy(self, ctx: commands.Context, channel: discord.TextChannel,
                       shares: int, price: float):
        """Place a limit buy order. Usage: {prefix}limitbuy #channel 10 99.50"""
        company = await self.get_company(channel.id)
        if not company:
            await ctx.send("This channel is not a registered company.")
            return
        if shares < 1:
            await ctx.send("You must buy at least 1 share.")
            return
        if price <= 0:
            await ctx.send("Price must be positive.")
            return

        cost = int(price * shares)
        cash = await self.get_cash(ctx.author.id)
        if cash < cost:
            await ctx.send(f"You need **{cost:,}** \U0001f338 but only have **{cash:,}** \U0001f338.")
            return

        # Reserve cash
        await self.ensure_economy_row(ctx.author.id)
        await self.update_cash(ctx.author.id, -cost)
        await self.db.commit()

        order_id, fills = await self.place_limit_order(
            ctx.guild.id, channel.id, ctx.author.id, "buy", price, shares
        )

        filled_qty = sum(f["quantity"] for f in fills)
        remaining = shares - filled_qty

        # Refund cash for unfilled portion if price improved
        if fills:
            total_cost_actual = sum(f["price"] * f["quantity"] for f in fills)
            reserved_for_filled = price * filled_qty
            savings = int(reserved_for_filled - total_cost_actual)
            if savings > 0:
                await self.update_cash(ctx.author.id, savings)
                await self.db.commit()

        embed = discord.Embed(
            title="Limit Buy Order",
            description=f"**#{channel.name}** — {shares} shares @ {price:,.2f} \U0001f338",
            color=discord.Color.green(),
        )
        if fills:
            fill_lines = [f"{f['quantity']} @ {f['price']:,.2f} \U0001f338" for f in fills]
            embed.add_field(name="Filled", value="\n".join(fill_lines), inline=False)
        if remaining > 0:
            embed.add_field(name="Resting", value=f"{remaining} shares @ {price:,.2f} \U0001f338")
            embed.set_footer(text=f"Order ID: {order_id} · Use {ctx.prefix}cancel {order_id} to cancel")
        else:
            embed.set_footer(text="Fully filled!")
        await ctx.send(embed=embed)

    @commands.command()
    async def limitsell(self, ctx: commands.Context, channel: discord.TextChannel,
                        shares: int, price: float):
        """Place a limit sell order. Usage: {prefix}limitsell #channel 10 101.50"""
        company = await self.get_company(channel.id)
        if not company:
            await ctx.send("This channel is not a registered company.")
            return
        if shares < 1:
            await ctx.send("You must sell at least 1 share.")
            return
        if price <= 0:
            await ctx.send("Price must be positive.")
            return

        qty, _ = await self.get_holdings(ctx.author.id, channel.id)
        if qty < shares:
            await ctx.send(f"You only hold **{qty}** shares of **#{channel.name}**.")
            return

        # Reserve shares by reducing holdings
        await self.update_holdings(ctx.author.id, channel.id, -shares)
        await self.db.commit()

        order_id, fills = await self.place_limit_order(
            ctx.guild.id, channel.id, ctx.author.id, "sell", price, shares
        )

        filled_qty = sum(f["quantity"] for f in fills)
        remaining = shares - filled_qty

        # Filled shares: cash already credited via execute_trade
        # Unfilled shares: stay reserved in the order

        embed = discord.Embed(
            title="Limit Sell Order",
            description=f"**#{channel.name}** — {shares} shares @ {price:,.2f} \U0001f338",
            color=discord.Color.red(),
        )
        if fills:
            fill_lines = [f"{f['quantity']} @ {f['price']:,.2f} \U0001f338" for f in fills]
            embed.add_field(name="Filled", value="\n".join(fill_lines), inline=False)
        if remaining > 0:
            embed.add_field(name="Resting", value=f"{remaining} shares @ {price:,.2f} \U0001f338")
            embed.set_footer(text=f"Order ID: {order_id} · Use {ctx.prefix}cancel {order_id} to cancel")
        else:
            embed.set_footer(text="Fully filled!")
        await ctx.send(embed=embed)

    @commands.command(name="mbuy")
    async def market_buy(self, ctx: commands.Context, channel: discord.TextChannel, shares: int):
        """Buy shares at market price. Usage: {prefix}mbuy #channel 5"""
        company = await self.get_company(channel.id)
        if not company:
            await ctx.send("This channel is not a registered company.")
            return
        if shares < 1:
            await ctx.send("You must buy at least 1 share.")
            return

        # Check available liquidity
        async with self.db.execute(
            "SELECT COALESCE(SUM(remaining), 0) FROM orders "
            "WHERE channel_id = ? AND side = 'sell'",
            (channel.id,),
        ) as cur:
            row = await cur.fetchone()
        available = row[0]
        if available < shares:
            await ctx.send(
                f"Order book too thin — only **{available}** shares available. "
                f"Use `{ctx.prefix}limitbuy` to place a limit order instead."
            )
            return

        # Estimate worst-case cost (highest ask * shares)
        async with self.db.execute(
            "SELECT MAX(price) FROM orders WHERE channel_id = ? AND side = 'sell'",
            (channel.id,),
        ) as cur:
            row = await cur.fetchone()
        worst_price = row[0] if row[0] else 0
        worst_cost = int(worst_price * shares)

        cash = await self.get_cash(ctx.author.id)
        if cash < worst_cost:
            await ctx.send(f"You may need up to **{worst_cost:,}** \U0001f338 but only have **{cash:,}** \U0001f338.")
            return

        # Reserve worst-case cash
        await self.ensure_economy_row(ctx.author.id)
        await self.update_cash(ctx.author.id, -worst_cost)
        await self.db.commit()

        order_id, fills = await self.place_limit_order(
            ctx.guild.id, channel.id, ctx.author.id, "buy", MARKET_BUY_SENTINEL, shares
        )

        filled_qty = sum(f["quantity"] for f in fills)
        actual_cost = sum(f["price"] * f["quantity"] for f in fills)

        # Cancel any unfilled remainder
        async with self.db.execute(
            "SELECT remaining FROM orders WHERE id = ?", (order_id,),
        ) as cur:
            rem_row = await cur.fetchone()
        if rem_row and rem_row[0] > 0:
            await self.db.execute("DELETE FROM orders WHERE id = ?", (order_id,))

        # Refund difference between reserved and actual cost
        refund = worst_cost - int(actual_cost)
        if refund > 0:
            await self.update_cash(ctx.author.id, refund)
            await self.db.commit()

        if not fills:
            await ctx.send("No fills — order book is empty.")
            return

        avg_price = actual_cost / filled_qty if filled_qty > 0 else 0
        embed = discord.Embed(
            title="Market Buy",
            description=f"Bought **{filled_qty}** shares of **#{channel.name}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="Avg Price", value=f"{avg_price:,.2f} \U0001f338")
        embed.add_field(name="Total Cost", value=f"{actual_cost:,.0f} \U0001f338")
        await ctx.send(embed=embed)

    @commands.command(name="msell")
    async def market_sell(self, ctx: commands.Context, channel: discord.TextChannel, shares: int):
        """Sell shares at market price. Usage: {prefix}msell #channel 5"""
        company = await self.get_company(channel.id)
        if not company:
            await ctx.send("This channel is not a registered company.")
            return
        if shares < 1:
            await ctx.send("You must sell at least 1 share.")
            return

        qty, _ = await self.get_holdings(ctx.author.id, channel.id)
        if qty < shares:
            await ctx.send(f"You only hold **{qty}** shares of **#{channel.name}**.")
            return

        # Check available bids
        async with self.db.execute(
            "SELECT COALESCE(SUM(remaining), 0) FROM orders "
            "WHERE channel_id = ? AND side = 'buy'",
            (channel.id,),
        ) as cur:
            row = await cur.fetchone()
        available = row[0]
        if available < shares:
            await ctx.send(
                f"Order book too thin — only **{available}** shares of buy orders available. "
                f"Use `{ctx.prefix}limitsell` to place a limit order instead."
            )
            return

        # Reserve shares
        await self.update_holdings(ctx.author.id, channel.id, -shares)
        await self.db.commit()

        order_id, fills = await self.place_limit_order(
            ctx.guild.id, channel.id, ctx.author.id, "sell", MARKET_SELL_SENTINEL, shares
        )

        filled_qty = sum(f["quantity"] for f in fills)
        total_proceeds = sum(f["price"] * f["quantity"] for f in fills)

        # Cancel any unfilled remainder and return shares
        async with self.db.execute(
            "SELECT remaining FROM orders WHERE id = ?", (order_id,),
        ) as cur:
            rem_row = await cur.fetchone()
        if rem_row and rem_row[0] > 0:
            unfilled = rem_row[0]
            await self.db.execute("DELETE FROM orders WHERE id = ?", (order_id,))
            await self.update_holdings(ctx.author.id, channel.id, unfilled)
            await self.db.commit()

        if not fills:
            await ctx.send("No fills — order book is empty.")
            return

        avg_price = total_proceeds / filled_qty if filled_qty > 0 else 0
        embed = discord.Embed(
            title="Market Sell",
            description=f"Sold **{filled_qty}** shares of **#{channel.name}**",
            color=discord.Color.red(),
        )
        embed.add_field(name="Avg Price", value=f"{avg_price:,.2f} \U0001f338")
        embed.add_field(name="Total Proceeds", value=f"{total_proceeds:,.0f} \U0001f338")
        await ctx.send(embed=embed)

    @commands.command()
    async def cancel(self, ctx: commands.Context, order_id: int):
        """Cancel an open order and get refunded. Usage: {prefix}cancel 42"""
        async with self.db.execute(
            "SELECT id, channel_id, user_id, side, price, remaining FROM orders WHERE id = ?",
            (order_id,),
        ) as cur:
            row = await cur.fetchone()

        if not row:
            await ctx.send("Order not found.")
            return
        if row[2] != ctx.author.id:
            await ctx.send("That's not your order.")
            return

        _, channel_id, user_id, side, price, remaining = row

        if side == "buy":
            refund = int(price * remaining)
            await self.update_cash(user_id, refund)
        else:
            # Return shares
            await self.update_holdings(user_id, channel_id, remaining)

        await self.db.execute("DELETE FROM orders WHERE id = ?", (order_id,))
        await self.db.commit()

        company = await self.get_company(channel_id)
        name = company["name"] if company else "Unknown"

        embed = discord.Embed(
            title="Order Cancelled",
            description=f"{'Buy' if side == 'buy' else 'Sell'} order for **#{name}** cancelled.",
            color=discord.Color.light_grey(),
        )
        if side == "buy":
            embed.add_field(name="Refunded", value=f"{int(price * remaining):,} \U0001f338")
        else:
            embed.add_field(name="Shares Returned", value=str(remaining))
        await ctx.send(embed=embed)

    @commands.command()
    async def orderbook(self, ctx: commands.Context, channel: discord.TextChannel):
        """View the order book for a company. Usage: {prefix}orderbook #channel"""
        company = await self.get_company(channel.id)
        if not company:
            await ctx.send("This channel is not a registered company.")
            return

        # Top 10 bids (highest first)
        async with self.db.execute(
            "SELECT price, SUM(remaining), MAX(is_mm) FROM orders "
            "WHERE channel_id = ? AND side = 'buy' AND remaining > 0 "
            "GROUP BY price ORDER BY price DESC LIMIT 10",
            (channel.id,),
        ) as cur:
            bids = await cur.fetchall()

        # Top 10 asks (lowest first)
        async with self.db.execute(
            "SELECT price, SUM(remaining), MAX(is_mm) FROM orders "
            "WHERE channel_id = ? AND side = 'sell' AND remaining > 0 "
            "GROUP BY price ORDER BY price ASC LIMIT 10",
            (channel.id,),
        ) as cur:
            asks = await cur.fetchall()

        embed = discord.Embed(
            title=f"Order Book — #{channel.name}",
            color=discord.Color.blurple(),
        )

        # Asks: lowest price first (best ask at top)
        ask_lines = []
        for price, qty, is_mm in asks:
            tag = " [MM]" if is_mm else ""
            ask_lines.append(f"{price:,.2f} \U0001f338 x {qty}{tag}")
        embed.add_field(
            name="Asks (Sell)",
            value="\n".join(ask_lines) if ask_lines else "Empty",
            inline=True,
        )

        # Bids: highest price first (best bid at top)
        bid_lines = []
        for price, qty, is_mm in bids:
            tag = " [MM]" if is_mm else ""
            bid_lines.append(f"{price:,.2f} \U0001f338 x {qty}{tag}")
        embed.add_field(
            name="Bids (Buy)",
            value="\n".join(bid_lines) if bid_lines else "Empty",
            inline=True,
        )

        # Spread info
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None
        if best_bid and best_ask:
            spread = best_ask - best_bid
            mid = (best_ask + best_bid) / 2
            embed.set_footer(
                text=f"Spread: {spread:,.2f} \U0001f338 ({spread/mid*100:.1f}%) · Mid: {mid:,.2f} \U0001f338 · Fair: {company['fair_price']:,.2f} \U0001f338"
            )
        else:
            embed.set_footer(text=f"Fair Price: {company['fair_price']:,.2f} \U0001f338")

        await ctx.send(embed=embed)

    @commands.command()
    async def portfolio(self, ctx: commands.Context):
        """View your stock portfolio and P&L."""
        async with self.db.execute(
            "SELECT h.channel_id, h.quantity, h.avg_cost, c.name, c.fair_price "
            "FROM holdings h JOIN companies c ON h.channel_id = c.channel_id "
            "WHERE h.user_id = ? AND h.quantity > 0",
            (ctx.author.id,),
        ) as cur:
            rows = await cur.fetchall()

        if not rows:
            await ctx.send("You don't own any stocks.")
            return

        embed = discord.Embed(
            title=f"{ctx.author.display_name}'s Portfolio",
            color=discord.Color.blue(),
        )

        total_value = 0
        total_cost = 0
        for channel_id, qty, avg_cost, name, fair_price in rows:
            current_value = qty * fair_price
            cost_basis = qty * avg_cost
            pnl = current_value - cost_basis
            pnl_pct = (pnl / cost_basis * 100) if cost_basis > 0 else 0
            sign = "+" if pnl >= 0 else ""

            total_value += current_value
            total_cost += cost_basis

            embed.add_field(
                name=f"#{name}",
                value=(
                    f"**{qty}** shares @ {avg_cost:,.2f} \U0001f338\n"
                    f"Current: {fair_price:,.2f} \U0001f338\n"
                    f"P&L: {sign}{pnl:,.0f} \U0001f338 ({sign}{pnl_pct:.1f}%)"
                ),
                inline=True,
            )

        total_pnl = total_value - total_cost
        sign = "+" if total_pnl >= 0 else ""
        embed.set_footer(
            text=f"Total Value: {total_value:,.0f} \U0001f338 · Total P&L: {sign}{total_pnl:,.0f} \U0001f338"
        )
        await ctx.send(embed=embed)

    @commands.command()
    async def stockinfo(self, ctx: commands.Context, channel: discord.TextChannel,
                        timeframe: str = "7d"):
        """View stock info and price chart. Usage: {prefix}stockinfo #channel [7d/30d/all]"""
        company = await self.get_company(channel.id)
        if not company:
            await ctx.send("This channel is not a registered company.")
            return

        if timeframe not in ("7d", "30d", "all"):
            await ctx.send("Invalid timeframe. Choose: `7d`, `30d`, `all`.")
            return

        # Fetch price history
        if timeframe == "all":
            query = "SELECT timestamp, price FROM price_history WHERE channel_id = ? ORDER BY timestamp ASC"
            params = (channel.id,)
        else:
            days = 7 if timeframe == "7d" else 30
            cutoff = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).isoformat()
            query = "SELECT timestamp, price FROM price_history WHERE channel_id = ? AND timestamp >= ? ORDER BY timestamp ASC"
            params = (channel.id, cutoff)

        async with self.db.execute(query, params) as cur:
            history = await cur.fetchall()

        # Basic info embed
        daily_vol = await self.get_daily_volume(channel.id)
        market_cap = company["fair_price"] * company["total_shares"]

        embed = discord.Embed(
            title=f"#{channel.name}",
            color=discord.Color.blurple(),
        )
        embed.add_field(name="Price", value=f"{company['fair_price']:,.2f} \U0001f338")
        embed.add_field(name="IPO Price", value=f"{company['ipo_price']:,.2f} \U0001f338")
        embed.add_field(name="Market Cap", value=f"{market_cap:,.0f} \U0001f338")
        embed.add_field(name="Today's Volume", value=f"{daily_vol:,} shares")
        embed.add_field(name="Last Revenue", value=f"{company['last_revenue']:,.0f} \U0001f338")
        embed.add_field(name="Dividend", value=f"{company['dividend_pct']*100:.0f}%")

        file = None
        if HAS_MATPLOTLIB and len(history) >= 2:
            timestamps = [datetime.datetime.fromisoformat(h[0]) for h in history]
            prices = [h[1] for h in history]

            fig, ax = plt.subplots(figsize=(10, 4))
            ax.plot(timestamps, prices, color="#5865F2", linewidth=1.5)
            ax.fill_between(timestamps, prices, alpha=0.1, color="#5865F2")
            ax.set_title(f"#{channel.name} — {timeframe}", fontsize=14)
            ax.set_ylabel("Price (\U0001f338)")
            ax.grid(alpha=0.3)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
            fig.autofmt_xdate()
            fig.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=100)
            plt.close(fig)
            buf.seek(0)
            file = discord.File(buf, filename="chart.png")
            embed.set_image(url="attachment://chart.png")
        elif not HAS_MATPLOTLIB:
            embed.set_footer(text="Install matplotlib for price charts: pip install matplotlib")
        else:
            embed.set_footer(text="Not enough price data for a chart yet.")

        if file:
            await ctx.send(embed=embed, file=file)
        else:
            await ctx.send(embed=embed)

    # --- Open orders for a user ---

    @commands.command()
    async def myorders(self, ctx: commands.Context):
        """View your open orders across all companies."""
        async with self.db.execute(
            "SELECT o.id, o.side, o.price, o.remaining, c.name "
            "FROM orders o JOIN companies c ON o.channel_id = c.channel_id "
            "WHERE o.user_id = ? AND o.remaining > 0 AND o.is_mm = 0 "
            "ORDER BY o.created_at DESC",
            (ctx.author.id,),
        ) as cur:
            rows = await cur.fetchall()

        if not rows:
            await ctx.send("You have no open orders.")
            return

        embed = discord.Embed(
            title=f"{ctx.author.display_name}'s Open Orders",
            color=discord.Color.blue(),
        )
        for order_id, side, price, remaining, name in rows:
            side_label = "BUY" if side == "buy" else "SELL"
            embed.add_field(
                name=f"#{order_id} — {side_label}",
                value=f"**#{name}** · {remaining} shares @ {price:,.2f} \U0001f338",
                inline=False,
            )
        embed.set_footer(text=f"Use {ctx.prefix}cancel <id> to cancel an order")
        await ctx.send(embed=embed)

    # --- Settings (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def setdividend(self, ctx: commands.Context, pct: float):
        """Set the dividend percentage (0-100). Server owner only."""
        if pct < 0 or pct > 100:
            await ctx.send("Percentage must be between 0 and 100.")
            return
        decimal_pct = pct / 100
        await self.db.execute(
            "INSERT INTO market_settings (guild_id, dividend_pct) VALUES (?, ?) "
            "ON CONFLICT(guild_id) DO UPDATE SET dividend_pct = ?",
            (ctx.guild.id, decimal_pct, decimal_pct),
        )
        await self.db.commit()
        embed = discord.Embed(
            title="Dividend Rate Updated",
            description=f"Dividend payout set to **{pct:.0f}%** of weekly revenue.",
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    @commands.command()
    @is_guild_owner()
    async def companyinfo(self, ctx: commands.Context, channel: discord.TextChannel):
        """View private company diagnostics. Server owner only.
        Usage: {prefix}companyinfo #channel"""
        company = await self.get_company(channel.id)
        if not company:
            await ctx.send("This channel is not a registered company.")
            return

        mm = await self.get_mm_state(channel.id)
        daily_vol = await self.get_daily_volume(channel.id)

        # --- Revenue ---
        week_start = self._week_start().isoformat()
        async with self.db.execute(
            "SELECT accumulated_revenue FROM channel_revenue "
            "WHERE channel_id = ? AND week_start = ?",
            (channel.id, week_start),
        ) as cur:
            rev_row = await cur.fetchone()
        accumulated = rev_row[0] if rev_row else 0.0

        today = datetime.date.today()
        days_elapsed = max((today - self._week_start()).days, 1)
        estimated_weekly = (accumulated / days_elapsed) * 7

        # --- Shareholders ---
        async with self.db.execute(
            "SELECT user_id, quantity FROM holdings "
            "WHERE channel_id = ? AND quantity > 0 AND user_id != ? "
            "ORDER BY quantity DESC",
            (channel.id, MM_USER_ID),
        ) as cur:
            holders = await cur.fetchall()
        num_holders = len(holders)
        player_shares = sum(h[1] for h in holders)

        # --- Activity (unique posters this week) ---
        async with self.db.execute(
            "SELECT COUNT(DISTINCT user_id), SUM(char_count) FROM user_daily_chars "
            "WHERE channel_id = ? AND date >= ?",
            (channel.id, week_start),
        ) as cur:
            act_row = await cur.fetchone()
        active_users = act_row[0] if act_row else 0
        total_chars = act_row[1] if act_row and act_row[1] else 0

        # --- Recent trades ---
        async with self.db.execute(
            "SELECT COUNT(*), COALESCE(SUM(quantity), 0) FROM trades "
            "WHERE channel_id = ? AND timestamp >= ?",
            (channel.id, week_start),
        ) as cur:
            trade_row = await cur.fetchone()
        weekly_trades = trade_row[0]
        weekly_volume = trade_row[1]

        # --- Open orders ---
        async with self.db.execute(
            "SELECT side, COUNT(*), SUM(remaining) FROM orders "
            "WHERE channel_id = ? AND remaining > 0 AND is_mm = 0 "
            "GROUP BY side",
            (channel.id,),
        ) as cur:
            order_rows = await cur.fetchall()
        open_buys = open_sells = buy_qty = sell_qty = 0
        for side, cnt, qty in order_rows:
            if side == "buy":
                open_buys, buy_qty = cnt, int(qty)
            else:
                open_sells, sell_qty = cnt, int(qty)

        # --- Build embed ---
        embed = discord.Embed(
            title=f"Company Info — #{channel.name}",
            color=discord.Color.dark_gold(),
        )

        # Financials
        embed.add_field(
            name="Financials",
            value=(
                f"Fair Price: **{company['fair_price']:,.2f}** \U0001f338\n"
                f"IPO Price: {company['ipo_price']:,.2f} \U0001f338\n"
                f"Last Revenue: {company['last_revenue']:,.0f} \U0001f338\n"
                f"This Week Revenue: {accumulated:,.0f} \U0001f338\n"
                f"Est. Weekly: {estimated_weekly:,.0f} \U0001f338\n"
                f"Dividend: {company['dividend_pct']*100:.0f}%"
            ),
            inline=True,
        )

        # Market Maker
        if mm:
            embed.add_field(
                name="Market Maker",
                value=(
                    f"Cash: {mm['cash']:,.2f} \U0001f338\n"
                    f"Inventory: {mm['inventory']:,} shares\n"
                    f"Volatility: {mm['volatility']:.4f}\n"
                    f"Last Quote: {mm['last_quote_time'][:16]}"
                ),
                inline=True,
            )

        # Shareholders
        top_holders = ""
        for uid, qty in holders[:5]:
            member = ctx.guild.get_member(uid)
            name = member.display_name if member else f"User {uid}"
            pct = qty / company['total_shares'] * 100
            top_holders += f"{name}: {qty:,} ({pct:.1f}%)\n"
        embed.add_field(
            name=f"Shareholders ({num_holders} players)",
            value=(
                f"Player shares: {player_shares:,} / {company['total_shares']:,}\n"
                f"MM shares: {mm['inventory']:,}\n"
                + (f"**Top holders:**\n{top_holders}" if top_holders else "No player holders yet.")
            ),
            inline=False,
        )

        # Activity
        embed.add_field(
            name="Activity (this week)",
            value=(
                f"Active users: {active_users}\n"
                f"Total characters: {total_chars:,}\n"
                f"Trades: {weekly_trades:,} ({weekly_volume:,} shares)"
            ),
            inline=True,
        )

        # Order Book
        embed.add_field(
            name="Open Orders (players)",
            value=(
                f"Buy: {open_buys} orders ({buy_qty:,} shares)\n"
                f"Sell: {open_sells} orders ({sell_qty:,} shares)"
            ),
            inline=True,
        )

        embed.add_field(
            name="Volume",
            value=f"Today: {daily_vol:,} shares",
            inline=True,
        )

        embed.set_footer(text=f"Created: {company['created_at'][:10]}")
        await ctx.send(embed=embed)

    @commands.command()
    async def market(self, ctx: commands.Context):
        """List all registered companies and their current prices."""
        async with self.db.execute(
            "SELECT c.channel_id, c.name, c.fair_price, c.ipo_price, c.total_shares, c.last_revenue "
            "FROM companies c WHERE c.guild_id = ? ORDER BY c.name",
            (ctx.guild.id,),
        ) as cur:
            rows = await cur.fetchall()

        if not rows:
            await ctx.send(f"No companies listed yet. Use `{ctx.prefix}ipo` to register one.")
            return

        embed = discord.Embed(
            title="Stock Market",
            color=discord.Color.blurple(),
        )

        for channel_id, name, fair_price, ipo_price, total_shares, last_revenue in rows:
            change = ((fair_price - ipo_price) / ipo_price * 100) if ipo_price > 0 else 0
            sign = "+" if change >= 0 else ""
            market_cap = fair_price * total_shares
            daily_vol = await self.get_daily_volume(channel_id)

            embed.add_field(
                name=f"#{name}",
                value=(
                    f"Price: **{fair_price:,.2f}** \U0001f338 ({sign}{change:.1f}%)\n"
                    f"Cap: {market_cap:,.0f} \U0001f338 · Vol: {daily_vol:,}"
                ),
                inline=True,
            )

        embed.set_footer(text=f"Use {ctx.prefix}stockinfo #channel for details · {ctx.prefix}mbuy #channel <shares> to buy")
        await ctx.send(embed=embed)

    @commands.command()
    @is_guild_owner()
    async def charstats(self, ctx: commands.Context, channel: discord.TextChannel,
                        period: str = "week"):
        """View character counts per user in a channel. Owner only.
        Usage: {prefix}charstats #channel [week|all]"""
        company = await self.get_company(channel.id)
        if not company:
            await ctx.send("This channel is not a registered company.")
            return

        if period not in ("week", "all"):
            await ctx.send("Invalid period. Use `week` or `all`.")
            return

        if period == "week":
            week_start = self._week_start().isoformat()
            async with self.db.execute(
                "SELECT user_id, SUM(char_count) FROM user_daily_chars "
                "WHERE channel_id = ? AND date >= ? "
                "GROUP BY user_id ORDER BY SUM(char_count) DESC",
                (channel.id, week_start),
            ) as cur:
                rows = await cur.fetchall()
            period_label = f"This week (since {week_start})"
        else:
            async with self.db.execute(
                "SELECT user_id, SUM(char_count) FROM user_daily_chars "
                "WHERE channel_id = ? "
                "GROUP BY user_id ORDER BY SUM(char_count) DESC",
                (channel.id,),
            ) as cur:
                rows = await cur.fetchall()
            period_label = "All time"

        if not rows:
            await ctx.send(f"No character data recorded for #{channel.name} yet.")
            return

        total_chars = sum(r[1] for r in rows)
        lines = []
        for uid, chars in rows:
            member = ctx.guild.get_member(uid)
            name = member.display_name if member else f"User {uid}"
            pct = chars / total_chars * 100 if total_chars > 0 else 0
            weighted = compute_weighted_chars(chars)
            lines.append(f"**{name}**: {chars:,} chars ({pct:.1f}%) → {weighted:,.1f} weighted")

        # Discord embed field max is 1024 chars — paginate if needed
        embed = discord.Embed(
            title=f"Character Stats — #{channel.name}",
            description=f"**Period:** {period_label}\n**Total raw chars:** {total_chars:,}",
            color=discord.Color.blurple(),
        )
        chunk = ""
        page = 1
        for line in lines:
            if len(chunk) + len(line) + 1 > 1000:
                embed.add_field(name=f"Users (page {page})", value=chunk, inline=False)
                chunk = ""
                page += 1
            chunk += line + "\n"
        if chunk:
            embed.add_field(name=f"Users (page {page})", value=chunk, inline=False)

        await ctx.send(embed=embed)

    @commands.command()
    @is_guild_owner()
    async def delist(self, ctx: commands.Context, channel: discord.TextChannel):
        """Remove a company from the market. All shares and orders are lost. Server owner only."""
        company = await self.get_company(channel.id)
        if not company:
            await ctx.send("This channel is not a registered company.")
            return

        await self.db.execute("DELETE FROM companies WHERE channel_id = ?", (channel.id,))
        await self.db.execute("DELETE FROM orders WHERE channel_id = ?", (channel.id,))
        await self.db.execute("DELETE FROM holdings WHERE channel_id = ?", (channel.id,))
        await self.db.execute("DELETE FROM mm_state WHERE channel_id = ?", (channel.id,))
        await self.db.execute("DELETE FROM trades WHERE channel_id = ?", (channel.id,))
        await self.db.execute("DELETE FROM channel_revenue WHERE channel_id = ?", (channel.id,))
        await self.db.execute("DELETE FROM user_daily_chars WHERE channel_id = ?", (channel.id,))
        await self.db.execute("DELETE FROM price_history WHERE channel_id = ?", (channel.id,))
        await self.db.commit()

        self._company_channels.discard(channel.id)

        embed = discord.Embed(
            title="Company Delisted",
            description=f"**#{channel.name}** has been removed from the market. All shares and orders are lost.",
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    # ── Error Handler ────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return

        if isinstance(error, commands.CheckFailure):
            if str(error) == "channel_restricted":
                return
            await ctx.send("Only the server owner can use this command.")
        elif isinstance(error, commands.ChannelNotFound):
            await ctx.send("Could not find that channel. Make sure to mention it with #.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Check `{ctx.prefix}help {ctx.command}` for usage.")
        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing argument. Check `{ctx.prefix}help {ctx.command}` for usage.")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(Market(bot))
