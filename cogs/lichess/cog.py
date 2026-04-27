from __future__ import annotations
import asyncio
import base64
import hashlib
import os
import secrets
import urllib.parse
from datetime import datetime, timezone

import aiohttp
import asyncpg
import discord
from aiohttp import web
from discord.ext import commands

from config import (
    LICHESS_VARIANTS,
    RATING_ROLE_MIN_DEFAULT,
    RATING_ROLE_STEP_DEFAULT,
    RATING_SEPARATOR_ROLE,
)
from cogs.lichess.ratings import (
    exchange_code,
    fetch_account,
    fetch_user,
    is_rating_role,
    rating_tier,
    role_name,
)

LICHESS_CLIENT_ID = os.getenv("LICHESS_CLIENT_ID", "")
LICHESS_REDIRECT_URI = os.getenv("LICHESS_REDIRECT_URI", "http://localhost:8080/callback")
OAUTH_PORT = int(os.getenv("OAUTH_PORT", "8080"))

_SUCCESS_HTML = """<!DOCTYPE html>
<html><head><title>Connected!</title>
<style>body{{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#2b2d31}}
.box{{background:#313338;color:#dbdee1;padding:2rem 3rem;border-radius:12px;text-align:center}}
h1{{color:#57f287}}p{{color:#b5bac1}}</style></head>
<body><div class="box"><h1>&#10003; Linked!</h1>
<p>Your Lichess account <strong>{username}</strong> is now connected to Discord.</p>
<p>You can close this tab.</p></div></body></html>"""

_ERROR_HTML = """<!DOCTYPE html>
<html><head><title>Error</title>
<style>body{{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#2b2d31}}
.box{{background:#313338;color:#dbdee1;padding:2rem 3rem;border-radius:12px;text-align:center}}
h1{{color:#ed4245}}p{{color:#b5bac1}}</style></head>
<body><div class="box"><h1>&#10007; Error</h1><p>{message}</p></div></body></html>"""


def _pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


async def _get_setting(pool: asyncpg.Pool, guild_id: int, key: str, default: int) -> int:
    row = await pool.fetchrow(
        "SELECT value FROM guild_settings WHERE guild_id=$1 AND key=$2",
        guild_id, key,
    )
    return int(row["value"]) if row else default


async def _set_setting(pool: asyncpg.Pool, guild_id: int, key: str, value: int) -> None:
    await pool.execute(
        """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, $2, $3)
           ON CONFLICT (guild_id, key) DO UPDATE SET value = EXCLUDED.value""",
        guild_id, key, str(value),
    )


async def _get_rating_min(pool: asyncpg.Pool, guild_id: int) -> int:
    return await _get_setting(pool, guild_id, "lichess_rating_min", RATING_ROLE_MIN_DEFAULT)


async def _get_rating_step(pool: asyncpg.Pool, guild_id: int) -> int:
    return await _get_setting(pool, guild_id, "lichess_rating_step", RATING_ROLE_STEP_DEFAULT)


async def _ensure_separator(guild: discord.Guild) -> discord.Role:
    """Find or create the visual separator role, return it."""
    sep = discord.utils.get(guild.roles, name=RATING_SEPARATOR_ROLE)
    if sep is None:
        sep = await guild.create_role(
            name=RATING_SEPARATOR_ROLE,
            color=discord.Color.default(),
            hoist=False,
            mentionable=False,
        )
    return sep


async def _find_or_create_rating_role(
    guild: discord.Guild, sep: discord.Role, rname: str
) -> discord.Role:
    """Find or create a rating role positioned just below the separator."""
    existing = discord.utils.get(guild.roles, name=rname)
    if existing:
        return existing
    new_role = await guild.create_role(
        name=rname,
        color=discord.Color.default(),
        hoist=False,
        mentionable=False,
    )
    target_position = max(1, sep.position - 1)
    try:
        await new_role.edit(position=target_position)
    except discord.HTTPException:
        pass
    return new_role


async def apply_rating_roles(
    guild: discord.Guild,
    member: discord.Member,
    lichess_username: str,
    pool: asyncpg.Pool,
) -> None:
    """Fetch Lichess ratings and assign/remove variant rating roles for a member."""
    min_rating = await _get_rating_min(pool, guild.id)
    step = await _get_rating_step(pool, guild.id)

    async with aiohttp.ClientSession() as session:
        try:
            user_data = await fetch_user(session, lichess_username)
        except Exception:
            return

    perfs = user_data.get("perfs", {})
    sep = await _ensure_separator(guild)

    for variant in LICHESS_VARIANTS:
        vname = variant["name"]
        vkey = variant["key"]
        perf = perfs.get(vkey, {})

        roles_to_remove = [
            r for r in member.roles if is_rating_role(r.name, vname)
        ]
        if roles_to_remove:
            try:
                await member.remove_roles(*roles_to_remove, reason="Lichess rating sync")
            except discord.HTTPException:
                pass

        games = perf.get("games", 0)
        provisional = perf.get("prov", False)
        if games == 0 or provisional:
            continue

        rating = perf.get("rating", 0)
        tier = rating_tier(rating, min_rating, step)
        if tier is None:
            continue

        rname = role_name(vname, tier)
        try:
            target_role = await _find_or_create_rating_role(guild, sep, rname)
            await member.add_roles(target_role, reason="Lichess rating sync")
        except discord.HTTPException:
            pass


async def _delete_stale_rating_roles(
    guild: discord.Guild, min_rating: int, step: int
) -> None:
    """Delete Discord roles that no longer map to a valid tier under the current settings."""
    for role in list(guild.roles):
        for variant in LICHESS_VARIANTS:
            if not is_rating_role(role.name, variant["name"]):
                continue
            try:
                tier = int(role.name.rsplit(" ", 1)[-1])
            except ValueError:
                continue
            if tier < min_rating or (tier - min_rating) % step != 0:
                try:
                    await role.delete(reason="Lichess settings changed — stale tier")
                except discord.HTTPException:
                    pass


async def full_guild_sync(guild: discord.Guild, pool: asyncpg.Pool) -> None:
    """Re-apply rating roles for all connected members, then prune stale roles."""
    min_rating = await _get_rating_min(pool, guild.id)
    step = await _get_rating_step(pool, guild.id)
    await _delete_stale_rating_roles(guild, min_rating, step)

    rows = await pool.fetch(
        "SELECT discord_user_id, lichess_username FROM lichess_connections WHERE guild_id=$1",
        guild.id,
    )
    for row in rows:
        member = guild.get_member(row["discord_user_id"])
        if member:
            await apply_rating_roles(guild, member, row["lichess_username"], pool)


class Lichess(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @property
    def pool(self) -> asyncpg.Pool:
        return self.bot.pool

    # ── OAuth callback handler (called by the embedded aiohttp server) ──

    async def handle_callback(self, request: web.Request) -> web.Response:
        code = request.rel_url.query.get("code")
        state = request.rel_url.query.get("state")

        def error(msg: str) -> web.Response:
            return web.Response(
                text=_ERROR_HTML.format(message=msg),
                content_type="text/html",
                status=400,
            )

        if not code or not state:
            return error("Missing code or state parameter.")

        row = await self.pool.fetchrow(
            "SELECT * FROM lichess_oauth_pending WHERE state=$1", state
        )
        if row is None:
            return error("Unknown or expired state. Please run .connect again.")

        age = (datetime.now(timezone.utc) - row["created_at"]).total_seconds()
        if age > 600:
            await self.pool.execute(
                "DELETE FROM lichess_oauth_pending WHERE state=$1", state
            )
            return error("This link has expired (>10 min). Please run .connect again.")

        discord_user_id: int = row["discord_user_id"]
        guild_id: int = row["guild_id"]
        channel_id: int = row["channel_id"]
        verifier: str = row["code_verifier"]

        async with aiohttp.ClientSession() as session:
            try:
                token = await exchange_code(
                    session, code, verifier, LICHESS_CLIENT_ID, LICHESS_REDIRECT_URI
                )
                lichess_username = await fetch_account(session, token)
            except Exception as exc:
                return error(f"Failed to verify Lichess account: {exc}")

        await self.pool.execute(
            """INSERT INTO lichess_connections (guild_id, discord_user_id, lichess_username, access_token)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (guild_id, discord_user_id)
               DO UPDATE SET lichess_username=EXCLUDED.lichess_username,
                             access_token=EXCLUDED.access_token,
                             connected_at=NOW()""",
            guild_id, discord_user_id, lichess_username, token,
        )
        await self.pool.execute(
            "DELETE FROM lichess_oauth_pending WHERE state=$1", state
        )

        guild = self.bot.get_guild(guild_id)
        member = guild.get_member(discord_user_id) if guild else None
        if guild and member:
            asyncio.create_task(apply_rating_roles(guild, member, lichess_username, self.pool))

        channel = self.bot.get_channel(channel_id)
        if channel:
            embed = discord.Embed(
                description=f"Successfully linked **{lichess_username}** to your Discord account! Rating roles are being applied.",
                color=discord.Color.green(),
            )
            try:
                await channel.send(f"<@{discord_user_id}>", embed=embed)
            except discord.HTTPException:
                pass

        return web.Response(
            text=_SUCCESS_HTML.format(username=lichess_username),
            content_type="text/html",
        )

    # ── Commands ──

    @commands.command(name="connect")
    async def connect(self, ctx: commands.Context):
        """Link your Lichess account to your Discord via OAuth."""
        if not LICHESS_CLIENT_ID:
            await ctx.send("Lichess OAuth is not configured on this bot.")
            return

        verifier, challenge = _pkce_pair()
        state = secrets.token_urlsafe(32)

        await self.pool.execute(
            """INSERT INTO lichess_oauth_pending
               (state, discord_user_id, guild_id, channel_id, code_verifier)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (state) DO NOTHING""",
            state, ctx.author.id, ctx.guild.id, ctx.channel.id, verifier,
        )

        params = urllib.parse.urlencode({
            "response_type": "code",
            "client_id": LICHESS_CLIENT_ID,
            "redirect_uri": LICHESS_REDIRECT_URI,
            "code_challenge_method": "S256",
            "code_challenge": challenge,
            "state": state,
        })
        auth_url = f"https://lichess.org/oauth?{params}"

        embed = discord.Embed(
            title="Connect your Lichess account",
            description=(
                f"[Click here to authorise on Lichess]({auth_url})\n\n"
                "You'll be redirected to Lichess to log in and approve the connection. "
                "The link expires in **10 minutes**."
            ),
            color=discord.Color.blurple(),
        )
        try:
            await ctx.author.send(embed=embed)
            await ctx.message.add_reaction("✅")
        except discord.Forbidden:
            await ctx.send(embed=embed)

    @commands.command(name="disconnect")
    async def disconnect(self, ctx: commands.Context):
        """Unlink your Lichess account from this server."""
        row = await self.pool.fetchrow(
            "SELECT lichess_username FROM lichess_connections WHERE guild_id=$1 AND discord_user_id=$2",
            ctx.guild.id, ctx.author.id,
        )
        if not row:
            await ctx.send("You don't have a Lichess account linked on this server.")
            return

        await self.pool.execute(
            "DELETE FROM lichess_connections WHERE guild_id=$1 AND discord_user_id=$2",
            ctx.guild.id, ctx.author.id,
        )

        roles_to_remove = [
            r for r in ctx.author.roles
            if any(is_rating_role(r.name, v["name"]) for v in LICHESS_VARIANTS)
        ]
        if roles_to_remove:
            try:
                await ctx.author.remove_roles(*roles_to_remove, reason="Lichess disconnect")
            except discord.HTTPException:
                pass

        embed = discord.Embed(
            description=f"Unlinked **{row['lichess_username']}** from your Discord account.",
            color=discord.Color.orange(),
        )
        await ctx.send(embed=embed)

    @commands.command(name="updateroles", aliases=["ur"])
    async def updateroles(self, ctx: commands.Context):
        """Refresh your Lichess rating roles based on your current ratings."""
        row = await self.pool.fetchrow(
            "SELECT lichess_username FROM lichess_connections WHERE guild_id=$1 AND discord_user_id=$2",
            ctx.guild.id, ctx.author.id,
        )
        if not row:
            await ctx.send("You don't have a Lichess account linked. Use `.connect` first.")
            return

        msg = await ctx.send("Fetching your ratings...")
        await apply_rating_roles(ctx.guild, ctx.author, row["lichess_username"], self.pool)
        await msg.edit(content=None, embed=discord.Embed(
            description=f"Rating roles updated for **{row['lichess_username']}**.",
            color=discord.Color.green(),
        ))

    @commands.command(name="profile")
    async def profile(self, ctx: commands.Context, *, player: str = None):
        """Show Lichess ratings and stats. Usage: .profile [username]"""
        if player:
            lichess_username = player.strip()
        else:
            row = await self.pool.fetchrow(
                "SELECT lichess_username FROM lichess_connections WHERE guild_id=$1 AND discord_user_id=$2",
                ctx.guild.id, ctx.author.id,
            )
            if not row:
                await ctx.send("You don't have a Lichess account linked. Use `.connect` or provide a username: `.profile username`")
                return
            lichess_username = row["lichess_username"]

        async with ctx.typing():
            async with aiohttp.ClientSession() as session:
                try:
                    data = await fetch_user(session, lichess_username)
                except aiohttp.ClientResponseError as exc:
                    if exc.status == 404:
                        await ctx.send(f"Player **{lichess_username}** not found on Lichess.")
                    else:
                        await ctx.send("Failed to fetch Lichess profile.")
                    return
                except Exception:
                    await ctx.send("Failed to fetch Lichess profile.")
                    return

        perfs = data.get("perfs", {})
        username = data.get("username", lichess_username)
        created_ms = data.get("createdAt")
        created_str = (
            f"<t:{created_ms // 1000}:D>" if created_ms else "Unknown"
        )

        embed = discord.Embed(
            title=username,
            url=f"https://lichess.org/@/{username}",
            color=discord.Color.blurple(),
        )

        for variant in LICHESS_VARIANTS:
            perf = perfs.get(variant["key"], {})
            games = perf.get("games", 0)
            if games == 0:
                embed.add_field(name=variant["name"], value="No games", inline=True)
                continue
            rating = perf.get("rating", "?")
            prov = " ?" if perf.get("prov") else ""
            embed.add_field(
                name=variant["name"],
                value=f"**{rating}{prov}** | {games} games",
                inline=True,
            )

        puzzle = perfs.get("puzzle", {})
        if puzzle.get("games", 0) > 0:
            embed.add_field(
                name="Puzzles",
                value=f"**{puzzle.get('rating', '?')}** | {puzzle.get('games', 0)} puzzles",
                inline=True,
            )

        count = data.get("count", {})
        if count:
            total = count.get("all", 0)
            wins = count.get("win", 0)
            losses = count.get("loss", 0)
            draws = count.get("draw", 0)
            embed.add_field(
                name="Overall record",
                value=f"{wins}W / {draws}D / {losses}L ({total} total)",
                inline=False,
            )

        embed.set_footer(text=f"Joined Lichess: {created_str}")
        await ctx.send(embed=embed)

    @commands.group(name="lichessset", invoke_without_command=True)
    @commands.has_permissions(administrator=True)
    async def lichessset(self, ctx: commands.Context):
        """Configure Lichess rating role settings. Subcommands: min, step"""
        min_r = await _get_rating_min(self.pool, ctx.guild.id)
        step_r = await _get_rating_step(self.pool, ctx.guild.id)
        embed = discord.Embed(title="Lichess Rating Role Settings", color=discord.Color.blurple())
        embed.add_field(name="Minimum rating", value=str(min_r), inline=True)
        embed.add_field(name="Step size", value=str(step_r), inline=True)
        await ctx.send(embed=embed)

    @lichessset.command(name="min")
    @commands.has_permissions(administrator=True)
    async def lichessset_min(self, ctx: commands.Context, value: int):
        """Set the minimum rating for roles. Triggers a full re-sync."""
        step = await _get_rating_step(self.pool, ctx.guild.id)
        if value < 0:
            await ctx.send("Minimum rating must be 0 or higher.")
            return
        if value % step != 0:
            await ctx.send(f"Minimum rating must be a multiple of the step ({step}).")
            return

        await _set_setting(self.pool, ctx.guild.id, "lichess_rating_min", value)
        msg = await ctx.send(f"Minimum rating set to **{value}**. Syncing all members...")
        asyncio.create_task(full_guild_sync(ctx.guild, self.pool))
        await msg.edit(content=f"Minimum rating set to **{value}**. Background sync started.")

    @lichessset.command(name="step")
    @commands.has_permissions(administrator=True)
    async def lichessset_step(self, ctx: commands.Context, value: int):
        """Set the rating step size (e.g. 50, 100, 200). Triggers a full re-sync."""
        if value <= 0:
            await ctx.send("Step must be a positive integer.")
            return

        await _set_setting(self.pool, ctx.guild.id, "lichess_rating_step", value)
        msg = await ctx.send(f"Step size set to **{value}**. Syncing all members and removing stale roles...")
        asyncio.create_task(full_guild_sync(ctx.guild, self.pool))
        await msg.edit(content=f"Step size set to **{value}**. Background sync started.")
