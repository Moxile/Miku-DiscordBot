import discord
from discord.ext import commands

from config import MAIN_CURRENCY_EMOJI


_VALID_MODES = {"wallet", "bank", "port", "portfolio", "waifu"}


class Leaderboard(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    # ── Queries ──

    async def _lb_wallet(self, guild_id: int) -> list:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """SELECT user_id, wallet AS score
                   FROM balances WHERE guild_id = $1
                   ORDER BY wallet DESC LIMIT 10""",
                guild_id,
            )

    async def _lb_bank(self, guild_id: int) -> list:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """SELECT user_id, bank AS score
                   FROM balances WHERE guild_id = $1
                   ORDER BY bank DESC LIMIT 10""",
                guild_id,
            )

    async def _lb_portfolio(self, guild_id: int) -> list:
        """Portfolio value = SUM(quantity * last_trade_price per company, fallback to ipo_price)."""
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """WITH last_prices AS (
                       SELECT DISTINCT ON (guild_id, stock_channel_id)
                              guild_id, stock_channel_id, price
                       FROM trade_history
                       ORDER BY guild_id, stock_channel_id, traded_at DESC
                   ),
                   prices AS (
                       SELECT c.guild_id, c.stock_channel_id,
                              COALESCE(lp.price, c.ipo_price) AS price
                       FROM companies c
                       LEFT JOIN last_prices lp
                              ON lp.guild_id = c.guild_id
                             AND lp.stock_channel_id = c.stock_channel_id
                       WHERE c.guild_id = $1
                   )
                   SELECT p.user_id, COALESCE(SUM(p.quantity * pr.price), 0) AS score
                   FROM portfolios p
                   JOIN prices pr ON pr.guild_id = p.guild_id AND pr.stock_channel_id = p.stock_channel_id
                   WHERE p.guild_id = $1 AND p.quantity > 0
                   GROUP BY p.user_id
                   ORDER BY score DESC
                   LIMIT 10""",
                guild_id,
            )

    async def _lb_waifu(self, guild_id: int) -> list:
        """Harem value = SUM of owned waifu values per owner."""
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """SELECT owner_id AS user_id, SUM(value) AS score
                   FROM waifus
                   WHERE guild_id = $1 AND owner_id IS NOT NULL
                   GROUP BY owner_id
                   ORDER BY score DESC
                   LIMIT 10""",
                guild_id,
            )

    async def _lb_net(self, guild_id: int) -> list:
        """Net worth = wallet + bank + portfolio value + harem value."""
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """WITH last_prices AS (
                       SELECT DISTINCT ON (guild_id, stock_channel_id)
                              guild_id, stock_channel_id, price
                       FROM trade_history
                       ORDER BY guild_id, stock_channel_id, traded_at DESC
                   ),
                   prices AS (
                       SELECT c.guild_id, c.stock_channel_id,
                              COALESCE(lp.price, c.ipo_price) AS price
                       FROM companies c
                       LEFT JOIN last_prices lp
                              ON lp.guild_id = c.guild_id
                             AND lp.stock_channel_id = c.stock_channel_id
                       WHERE c.guild_id = $1
                   ),
                   port_value AS (
                       SELECT p.user_id, COALESCE(SUM(p.quantity * pr.price), 0) AS port
                       FROM portfolios p
                       JOIN prices pr ON pr.guild_id = p.guild_id AND pr.stock_channel_id = p.stock_channel_id
                       WHERE p.guild_id = $1 AND p.quantity > 0
                       GROUP BY p.user_id
                   ),
                   harem_value AS (
                       SELECT owner_id AS user_id, COALESCE(SUM(value), 0) AS harem
                       FROM waifus
                       WHERE guild_id = $1 AND owner_id IS NOT NULL
                       GROUP BY owner_id
                   )
                   SELECT b.user_id,
                          (b.wallet + b.bank
                           + COALESCE(pv.port, 0)
                           + COALESCE(hv.harem, 0)) AS score
                   FROM balances b
                   LEFT JOIN port_value pv ON pv.user_id = b.user_id
                   LEFT JOIN harem_value hv ON hv.user_id = b.user_id
                   WHERE b.guild_id = $1
                   ORDER BY score DESC
                   LIMIT 10""",
                guild_id,
            )

    # ── Display ──

    async def _build_embed(self, ctx: commands.Context, title: str, rows: list, invoker_id: int) -> discord.Embed:
        embed = discord.Embed(title=title, color=discord.Color.from_rgb(255, 215, 0))
        if not rows:
            embed.description = "No data yet."
            return embed

        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        lines = []
        invoker_in_top = False
        for i, row in enumerate(rows, 1):
            uid = row["user_id"]
            if uid == invoker_id:
                invoker_in_top = True
            member = ctx.guild.get_member(uid)
            name = member.display_name if member else f"User {uid}"
            prefix = medals.get(i, f"`{i}.`")
            lines.append(f"{prefix} **{name}** — {MAIN_CURRENCY_EMOJI} {row['score']:,}")
        embed.description = "\n".join(lines)

        if not invoker_in_top:
            embed.set_footer(text="You are not in the top 10.")
        return embed

    # ── Command ──

    @commands.command(aliases=["leaderboard"])
    async def lb(self, ctx: commands.Context, mode: str = None):
        """Show leaderboards. Use `.lb` for net worth, or specify a mode:
        `wallet`, `bank`, `port` (portfolio), `waifu` (harem value).
        Usage: .lb [wallet|bank|port|waifu]"""
        guild_id = ctx.guild.id
        invoker_id = ctx.author.id

        if mode is None:
            rows = await self._lb_net(guild_id)
            title = "Net Worth Leaderboard"
        else:
            mode = mode.lower()
            if mode not in _VALID_MODES:
                await ctx.send(
                    f"Unknown mode `{mode}`. Valid options: `wallet`, `bank`, `port`, `waifu`."
                )
                return
            if mode == "wallet":
                rows = await self._lb_wallet(guild_id)
                title = "Wallet Leaderboard"
            elif mode == "bank":
                rows = await self._lb_bank(guild_id)
                title = "Bank Leaderboard"
            elif mode in ("port", "portfolio"):
                rows = await self._lb_portfolio(guild_id)
                title = "Portfolio Leaderboard"
            else:
                rows = await self._lb_waifu(guild_id)
                title = "Harem Value Leaderboard"

        embed = await self._build_embed(ctx, title, rows, invoker_id)
        await ctx.send(embed=embed)

    @lb.error
    async def lb_error(self, ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.send("Usage: `.lb [wallet|bank|port|waifu]`")
