import math

import discord
from discord.ext import commands


_VALID_MODES = {"wallet", "bank", "port", "portfolio", "waifu"}

PER_PAGE = 10


class LeaderboardPaginator(discord.ui.View):
    """Pages through a full leaderboard, 10 entries at a time."""

    def __init__(self, ctx: commands.Context, title: str, rows: list, invoker_id: int, timeout=180, start_page: int = 0):
        super().__init__(timeout=timeout)
        self.ctx = ctx
        self.title = title
        self.rows = rows
        self.invoker_id = invoker_id
        self.currency = ctx.bot.get_currency(ctx.guild.id)
        self.max_page = max(0, math.ceil(len(rows) / PER_PAGE) - 1)
        self.page = max(0, min(start_page, self.max_page))
        self.message: discord.Message | None = None
        # Global rank of the invoker across the whole leaderboard, if present.
        self.invoker_rank = next((i for i, r in enumerate(rows, 1) if r["user_id"] == invoker_id), None)
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.max_page

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title=self.title, color=discord.Color.from_rgb(255, 215, 0))
        if not self.rows:
            embed.description = "No data yet."
            return embed

        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        start = self.page * PER_PAGE
        page_rows = self.rows[start:start + PER_PAGE]
        lines = []
        for rank, row in enumerate(page_rows, start + 1):
            uid = row["user_id"]
            member = self.ctx.guild.get_member(uid)
            name = member.display_name if member else f"User {uid}"
            prefix = medals.get(rank, f"`{rank}.`")
            lines.append(f"{prefix} **{name}** — {self.currency.emoji} {row['score']:,}")
        embed.description = "\n".join(lines)

        footer = f"Page {self.page + 1}/{self.max_page + 1} • {len(self.rows)} ranked"
        footer += f" • You are #{self.invoker_rank}" if self.invoker_rank else " • You are unranked"
        embed.set_footer(text=footer)
        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "This leaderboard isn't yours — run `.lb` to get your own.", ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


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
                   ORDER BY wallet DESC""",
                guild_id,
            )

    async def _lb_bank(self, guild_id: int) -> list:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """SELECT user_id, bank AS score
                   FROM balances WHERE guild_id = $1
                   ORDER BY bank DESC""",
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
                   ORDER BY score DESC""",
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
                   ORDER BY score DESC""",
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
                   ORDER BY score DESC""",
                guild_id,
            )

    # ── Command ──

    @commands.command(aliases=["leaderboard"])
    async def lb(self, ctx: commands.Context, *args: str):
        """Show leaderboards. Use `.lb` for net worth, or specify a mode:
        `wallet`, `bank`, `port` (portfolio), `waifu` (harem value).
        Pages through everyone 10 at a time — use the buttons to scroll,
        or jump straight to a page by adding its number.
        Usage: .lb [wallet|bank|port|waifu] [page]"""
        guild_id = ctx.guild.id

        mode = None
        page = 1
        for arg in args:
            if arg.isdigit():
                page = int(arg)
            else:
                mode = arg.lower()

        if mode is None:
            rows = await self._lb_net(guild_id)
            title = "Net Worth Leaderboard"
        else:
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

        # Only rank players with something to show.
        rows = [r for r in rows if (r["score"] or 0) > 0]

        view = LeaderboardPaginator(ctx, title, rows, ctx.author.id, start_page=page - 1)
        view.message = await ctx.send(embed=view.build_embed(), view=view)

    @lb.error
    async def lb_error(self, ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.send("Usage: `.lb [wallet|bank|port|waifu] [page]`")
