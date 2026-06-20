import math

import discord
from discord.ext import commands

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction
from cogs.missions.db import (
    create_mission, get_missions, get_mission, get_mission_by_name,
    add_funding, set_mission_status, delete_mission, remove_member_data,
)
from core.checks import require_channel, WrongChannel, invalidate, require_not_locked, UserLocked
from core.money import parse_amount, AmountError
from core.currency import Currency

MISSIONS_PER_PAGE = 5
BAR_WIDTH = 20


def _progress_bar(funded: int, goal: int) -> str:
    ratio = min(funded / goal, 1.0) if goal > 0 else 0.0
    filled = int(ratio * BAR_WIDTH)
    bar = "█" * filled + "░" * (BAR_WIDTH - filled)
    return f"`[{bar}]` {ratio * 100:.1f}%"


def _mission_field(mission, emoji: str) -> tuple[str, str]:
    bar = _progress_bar(mission["funded"], mission["goal"])
    remaining = max(mission["goal"] - mission["funded"], 0)
    name = f"#{mission['id']} — {mission['name']}"
    value = (
        f"{mission['description']}\n"
        f"{bar}\n"
        f"{mission['funded']:,} / {mission['goal']:,}{emoji} funded"
        + (f" — **{remaining:,}{emoji} to go**" if remaining > 0 else " — **FUNDED!**")
    )
    return name, value


class MissionsPaginator(discord.ui.View):
    def __init__(self, missions: list, invoker_id: int, currency: Currency, *, timeout=120):
        super().__init__(timeout=timeout)
        self.missions = missions
        self.invoker_id = invoker_id
        self.currency = currency
        self.page = 0
        self.max_page = max(0, math.ceil(len(missions) / MISSIONS_PER_PAGE) - 1)
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.max_page

    def build_embed(self) -> discord.Embed:
        start = self.page * MISSIONS_PER_PAGE
        page_missions = self.missions[start:start + MISSIONS_PER_PAGE]
        embed = discord.Embed(title="Active Missions", color=discord.Color.from_rgb(255, 140, 0))
        embed.set_footer(text=f"Page {self.page + 1}/{self.max_page + 1} — {len(self.missions)} mission(s) | Use .fund <name> <amount> to contribute")
        for m in page_missions:
            name, value = _mission_field(m, self.currency.emoji)
            embed.add_field(name=name, value=value, inline=False)
        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message("This isn't your mission list.", ephemeral=True)
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


class Missions(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    async def cog_command_error(self, ctx, error):
        if isinstance(error, WrongChannel):
            await ctx.send(str(error), delete_after=10)
        elif isinstance(error, UserLocked):
            return
        else:
            raise error

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Clean up mission contributions when a member leaves, is kicked, or is banned."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await remove_member_data(conn, member.guild.id, member.id)

    # ── Owner: manage missions ──

    @commands.command()
    @commands.is_owner()
    async def addmission(self, ctx, *, args: str = ""):
        """Add a new mission. Usage: .addmission <goal> <name> | <description>
        Example: .addmission 50000 Operation Aurora | Fund the construction of a new base."""
        parts = args.split("|", 1)
        first_tokens = parts[0].strip().split()
        if len(first_tokens) < 2:
            await ctx.send("Usage: `.addmission <goal> <name> | <description>`")
            return

        try:
            goal = parse_amount(first_tokens[0])
        except AmountError as e:
            await ctx.send(str(e))
            return

        if goal <= 0:
            await ctx.send("Goal must be positive.")
            return

        name = " ".join(first_tokens[1:])
        description = parts[1].strip() if len(parts) > 1 else ""

        mission = await create_mission(self.pool, ctx.guild.id, name, description, goal)

        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(
            title=f"Mission #{mission['id']} Created",
            color=discord.Color.from_rgb(255, 140, 0),
        )
        embed.add_field(name="Name", value=name, inline=True)
        embed.add_field(name="Goal", value=f"{goal:,}{cur.emoji}", inline=True)
        if description:
            embed.add_field(name="Description", value=description, inline=False)
        embed.set_footer(text="Players can fund it with .fund <name> <amount>")
        await ctx.send(embed=embed)

    @commands.command()
    @commands.is_owner()
    async def deletemission(self, ctx, mission_id: int):
        """Delete a mission by ID (owner only)."""
        mission = await get_mission(self.pool, ctx.guild.id, mission_id)
        if not mission:
            await ctx.send(f"Mission #{mission_id} not found.")
            return
        await delete_mission(self.pool, ctx.guild.id, mission_id)
        await ctx.send(f"Mission #{mission_id} **{mission['name']}** deleted.")

    @commands.command()
    @commands.is_owner()
    async def setmissionchannel(self, ctx, channel: discord.TextChannel = None):
        """Set (or clear) the channel where mission commands are restricted to."""
        if channel is None:
            await self.pool.execute(
                "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'missions_channel'",
                ctx.guild.id,
            )
            invalidate(ctx.guild.id, "missions_channel")
            await ctx.send("Mission channel restriction removed — `.missions` and `.fund` allowed everywhere.")
        else:
            await self.pool.execute(
                """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, 'missions_channel', $2)
                   ON CONFLICT (guild_id, key) DO UPDATE SET value = $2""",
                ctx.guild.id, str(channel.id),
            )
            invalidate(ctx.guild.id, "missions_channel")
            await ctx.send(f"Mission commands restricted to {channel.mention}.")

    # ── Players: browse and fund ──

    @commands.command()
    @require_channel("missions_channel")
    async def missions(self, ctx):
        """Show all active missions with funding progress bars."""
        rows = await get_missions(self.pool, ctx.guild.id)
        if not rows:
            await ctx.send("No active missions right now.")
            return
        rows = list(rows)
        view = MissionsPaginator(rows, ctx.author.id, self.bot.get_currency(ctx.guild.id))
        await ctx.send(embed=view.build_embed(), view=view)

    @commands.command()
    @require_not_locked()
    @require_channel("missions_channel")
    async def fund(self, ctx, *, args: str = ""):
        """Fund a mission from your wallet. Usage: .fund <mission name> <amount>
        Example: .fund Operation Aurora 5000"""
        tokens = args.rsplit(None, 1)
        if len(tokens) < 2:
            await ctx.send("Usage: `.fund <mission name> <amount>`")
            return

        mission_name, amount_str = tokens
        try:
            amount = parse_amount(amount_str)
        except AmountError as e:
            await ctx.send(str(e))
            return

        if amount <= 0:
            await ctx.send("Amount must be positive.")
            return

        cur = self.bot.get_currency(ctx.guild.id)
        mission = await get_mission_by_name(self.pool, ctx.guild.id, mission_name)
        if not mission:
            await ctx.send(f"No mission named **{mission_name}** found.")
            return
        if mission["status"] != "active":
            await ctx.send(f"Mission **{mission['name']}** is no longer active.")
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                bal = await ensure_wallet(conn, ctx.guild.id, ctx.author.id)
                if bal["wallet"] < amount:
                    await ctx.send(
                        f"You only have {bal['wallet']:,}{cur.emoji} in your wallet."
                    )
                    return

                await update_wallet(conn, ctx.guild.id, ctx.author.id, -amount)
                await add_transaction(
                    conn, ctx.guild.id, ctx.author.id, -amount, "mission_fund",
                    f"Funded mission #{mission['id']}: {mission['name']}",
                )
                updated = await add_funding(conn, mission["id"], ctx.guild.id, ctx.author.id, amount)

                completed = updated["funded"] >= updated["goal"]
                if completed:
                    await set_mission_status(conn, mission["id"], "completed")

        name, value = _mission_field(updated, cur.emoji)
        embed = discord.Embed(
            title=f"Mission Funded!",
            color=discord.Color.green() if completed else discord.Color.from_rgb(255, 140, 0),
        )
        embed.add_field(name="Your contribution", value=f"{amount:,}{cur.emoji}", inline=True)
        embed.add_field(name=name, value=value, inline=False)

        if completed:
            embed.description = f"**Mission #{mission_id} is fully funded!** It will now take place."
            embed.set_footer(text=updated["description"] or "")

        await ctx.send(embed=embed)
