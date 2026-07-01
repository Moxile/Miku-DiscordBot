from __future__ import annotations

import math
import re

import discord
from discord.ext import commands

from core.checks import has_permissions_or_owner
from core.names import format_name


_VALID_MODES = {"wallet", "bank", "port", "portfolio", "waifu", "emoji"}

CUSTOM_EMOJI_RE = re.compile(r"<(a)?:([A-Za-z0-9_~]+):(\d+)>")


def _parse_emoji(bot: commands.Bot, raw: str) -> tuple[str, bool, str] | None:
    """Return (emoji_key, is_custom, display_str) or None if unresolvable."""
    raw = raw.strip()
    match = CUSTOM_EMOJI_RE.fullmatch(raw)
    if match:
        animated = match.group(1) == "a"
        name = match.group(2)
        emoji_id = int(match.group(3))
        if not any(e.id == emoji_id for e in bot.emojis):
            return None
        return str(emoji_id), True, raw
    if not raw:
        return None
    return raw, False, raw


PER_PAGE = 10


class LeaderboardPaginator(discord.ui.View):
    """Pages through a full leaderboard, 10 entries at a time."""

    def __init__(self, ctx: commands.Context, title: str, rows: list, invoker_id: int, timeout=180, start_page: int = 0, score_label: str | None = None):
        super().__init__(timeout=timeout)
        self.ctx = ctx
        self.title = title
        self.rows = rows
        self.invoker_id = invoker_id
        self._score_label = score_label
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
        score_label = self._score_label or self.ctx.bot.get_currency(self.ctx.guild.id).emoji
        start = self.page * PER_PAGE
        page_rows = self.rows[start:start + PER_PAGE]
        lines = []
        for rank, row in enumerate(page_rows, start + 1):
            uid = row["user_id"]
            member = self.ctx.guild.get_member(uid)
            name = format_name(member, self.ctx.guild, fallback=f"User {uid}")
            prefix = medals.get(rank, f"`{rank}.`")
            lines.append(f"{prefix} **{name}** — {score_label} {row['score']:,}")
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

    async def _excluded_ids(self, guild_id: int) -> set:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT user_id FROM lb_excluded WHERE guild_id = $1", guild_id,
            )
        return {r["user_id"] for r in rows}

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

    async def _get_reaction_config(self, guild_id: int):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT emoji_key, is_custom, emoji_display FROM reaction_lb_config WHERE guild_id = $1",
                guild_id,
            )

    async def _lb_reactions(self, guild_id: int) -> list:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """SELECT user_id, count AS score
                   FROM reaction_lb_counts
                   WHERE guild_id = $1 AND count > 0
                   ORDER BY count DESC""",
                guild_id,
            )

    # ── Reaction listeners ──

    async def _reaction_event(self, payload: discord.RawReactionActionEvent, delta: int) -> None:
        if payload.guild_id is None:
            return
        config = await self._get_reaction_config(payload.guild_id)
        if config is None:
            return

        if config["is_custom"]:
            emoji_key = str(payload.emoji.id) if payload.emoji.id else payload.emoji.name
        else:
            emoji_key = payload.emoji.name

        if emoji_key != config["emoji_key"]:
            return

        # Fetch the message author — we need the channel to look up the message.
        channel = self.bot.get_channel(payload.channel_id)
        if channel is None:
            return
        try:
            message = await channel.fetch_message(payload.message_id)
        except discord.HTTPException:
            return

        author = message.author
        if author.bot or author.id == payload.user_id:
            return

        async with self.pool.acquire() as conn:
            if delta > 0:
                await conn.execute(
                    """INSERT INTO reaction_lb_counts (guild_id, user_id, count)
                       VALUES ($1, $2, 1)
                       ON CONFLICT (guild_id, user_id) DO UPDATE
                       SET count = reaction_lb_counts.count + 1""",
                    payload.guild_id, author.id,
                )
            else:
                await conn.execute(
                    """UPDATE reaction_lb_counts
                       SET count = GREATEST(0, count - 1)
                       WHERE guild_id = $1 AND user_id = $2""",
                    payload.guild_id, author.id,
                )

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        await self._reaction_event(payload, 1)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        await self._reaction_event(payload, -1)

    # ── Command ──

    @commands.command(aliases=["leaderboard"], extras={"example": ".lb wallet 2"})
    async def lb(self, ctx: commands.Context, *args: str):
        """Show leaderboards. Use `.lb` for net worth, or specify a mode:
        `wallet`, `bank`, `port` (portfolio), `waifu` (harem value), `emoji` (reaction leaderboard).
        Pages through everyone 10 at a time — use the buttons to scroll,
        or jump straight to a page by adding its number.
        Usage: .lb [wallet|bank|port|waifu|emoji] [page]"""
        guild_id = ctx.guild.id

        mode = None
        page = 1
        for arg in args:
            if arg.isdigit():
                page = int(arg)
            else:
                mode = arg.lower()

        score_label = None

        if mode is None:
            rows = await self._lb_net(guild_id)
            title = "Net Worth Leaderboard"
        else:
            if mode not in _VALID_MODES:
                await ctx.send(
                    f"Unknown mode `{mode}`. Valid options: `wallet`, `bank`, `port`, `waifu`, `emoji`."
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
            elif mode == "emoji":
                config = await self._get_reaction_config(guild_id)
                if config is None:
                    await ctx.send("No reaction emoji has been set. An admin can set one with `.lbemoji <emoji>`.")
                    return
                rows = await self._lb_reactions(guild_id)
                score_label = config["emoji_display"]
                title = f"{config['emoji_display']} Reaction Leaderboard"
            else:
                rows = await self._lb_waifu(guild_id)
                title = "Harem Value Leaderboard"

        # Drop excluded players, then only rank those with something to show.
        excluded = await self._excluded_ids(guild_id)
        rows = [r for r in rows if r["user_id"] not in excluded and (r["score"] or 0) > 0]

        view = LeaderboardPaginator(ctx, title, rows, ctx.author.id, start_page=page - 1, score_label=score_label)
        view.message = await ctx.send(embed=view.build_embed(), view=view)

    @commands.command(extras={"example": ".lbexclude @user"})
    @has_permissions_or_owner(manage_guild=True)
    async def lbexclude(self, ctx: commands.Context, member: discord.Member):
        """Hide a member from all leaderboards. Requires Manage Server."""
        await self.pool.execute(
            """INSERT INTO lb_excluded (guild_id, user_id) VALUES ($1, $2)
               ON CONFLICT DO NOTHING""",
            ctx.guild.id, member.id,
        )
        await ctx.send(f"**{format_name(member, ctx.guild)}** is now hidden from leaderboards.")

    @commands.command(extras={"example": ".lbinclude @user"})
    @has_permissions_or_owner(manage_guild=True)
    async def lbinclude(self, ctx: commands.Context, member: discord.Member):
        """Re-add a previously excluded member to leaderboards. Requires Manage Server."""
        result = await self.pool.execute(
            "DELETE FROM lb_excluded WHERE guild_id = $1 AND user_id = $2",
            ctx.guild.id, member.id,
        )
        if result == "DELETE 0":
            await ctx.send(f"**{format_name(member, ctx.guild)}** wasn't excluded.")
        else:
            await ctx.send(f"**{format_name(member, ctx.guild)}** is back on the leaderboards.")

    @commands.command()
    @has_permissions_or_owner(manage_guild=True)
    async def lbexcluded(self, ctx: commands.Context):
        """List members currently hidden from leaderboards. Requires Manage Server."""
        excluded = await self._excluded_ids(ctx.guild.id)
        if not excluded:
            await ctx.send("No one is excluded from the leaderboards.")
            return
        names = [
            format_name(ctx.guild.get_member(uid), ctx.guild, fallback=f"User {uid}")
            for uid in excluded
        ]
        embed = discord.Embed(
            title="Excluded from leaderboards",
            description="\n".join(f"• {n}" for n in names),
            color=discord.Color.from_rgb(255, 215, 0),
        )
        await ctx.send(embed=embed)

    @commands.command(extras={"example": ".lbemoji ⭐"})
    @has_permissions_or_owner(manage_guild=True)
    async def lbemoji(self, ctx: commands.Context, emoji: str | None = None):
        """Set (or clear) the emoji tracked by the reaction leaderboard. Requires Manage Server.
        Use `.lbemoji` with no argument to see the current setting.
        Use `.lbemoji clear` to stop tracking."""
        guild_id = ctx.guild.id

        if emoji is None:
            config = await self._get_reaction_config(guild_id)
            if config is None:
                await ctx.send("No reaction emoji is currently set. Use `.lbemoji <emoji>` to set one.")
            else:
                await ctx.send(f"Currently tracking {config['emoji_display']} for the reaction leaderboard.")
            return

        if emoji.lower() == "clear":
            await self.pool.execute("DELETE FROM reaction_lb_config WHERE guild_id = $1", guild_id)
            await ctx.send("Reaction leaderboard emoji cleared. Tracking stopped.")
            return

        parsed = _parse_emoji(ctx.bot, emoji)
        if parsed is None:
            await ctx.send("Couldn't resolve that emoji. Make sure custom emojis are from this server.")
            return

        emoji_key, is_custom, display = parsed
        await self.pool.execute(
            """INSERT INTO reaction_lb_config (guild_id, emoji_key, is_custom, emoji_display)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (guild_id) DO UPDATE
               SET emoji_key = $2, is_custom = $3, emoji_display = $4""",
            guild_id, emoji_key, is_custom, display,
        )
        await ctx.send(f"Now tracking {display} reactions for the reaction leaderboard.")

