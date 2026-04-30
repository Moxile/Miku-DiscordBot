from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import aiohttp
import discord
from aiohttp import web
from discord.ext import commands, tasks

from config import LICHESS_RATING_ROLE_DEFAULTS, LICHESS_VARIANTS, RATING_SEPARATOR_ROLE
from cogs.lichess.api import exchange_code, extract_ratings, fetch_account
from cogs.lichess.db import (
    delete_account,
    get_account,
    get_all_rating_roles,
    get_profile_style,
    get_rating_role_config,
    get_ratings,
    list_all_linked_users,
    update_last_synced,
    upsert_account,
    upsert_profile_style,
    upsert_rating_role,
    upsert_rating_role_config,
    upsert_ratings,
)
from cogs.lichess.oauth import (
    PendingOAuth,
    PendingStore,
    build_auth_url,
    generate_pkce_pair,
    generate_state,
)
from cogs.lichess.profile import STYLES, build_profile_embed
from cogs.lichess.ratings import sync_member

_VARIANT_KEYS = [v["key"] for v in LICHESS_VARIANTS]

_SUCCESS_HTML = """<!DOCTYPE html>
<html><head><title>Linked!</title><style>
body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#1a1a2e}
.card{background:#16213e;color:#eee;padding:2rem 3rem;border-radius:12px;text-align:center;box-shadow:0 4px 20px rgba(0,0,0,.4)}
h1{color:#a78bfa;margin-bottom:.5rem}p{color:#aaa}
</style></head>
<body><div class="card"><h1>✓ Linked!</h1><p>Your Lichess account has been linked.<br>You can close this tab.</p></div></body></html>"""

_ERROR_HTML = """<!DOCTYPE html>
<html><head><title>Error</title><style>
body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#1a1a2e}
.card{{background:#16213e;color:#eee;padding:2rem 3rem;border-radius:12px;text-align:center;box-shadow:0 4px 20px rgba(0,0,0,.4)}}
h1{{color:#ef4444;margin-bottom:.5rem}}p{{color:#aaa}}
</style></head>
<body><div class="card"><h1>✗ Error</h1><p>{message}</p></div></body></html>"""


class Lichess(commands.Cog, name="Lichess"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.pending: PendingStore = PendingStore()
        self._refresh_cooldowns: Dict[int, float] = {}
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def pool(self):
        return self.bot.pool

    async def cog_load(self) -> None:
        self._session = aiohttp.ClientSession()
        self.periodic_refresh.start()

    async def cog_unload(self) -> None:
        self.periodic_refresh.cancel()
        if self._session:
            await self._session.close()

    # ── OAuth callback ──────────────────────────────────────────────────────

    async def handle_callback(self, request: web.Request) -> web.Response:
        params = request.rel_url.query
        state = params.get("state")
        code = params.get("code")
        error = params.get("error")

        def err(msg: str) -> web.Response:
            return web.Response(
                text=_ERROR_HTML.format(message=msg),
                content_type="text/html",
                status=400,
            )

        if error:
            return err(f"Lichess returned: {error}")
        if not state or not code:
            return err("Missing state or code. Please try .lichess link again.")

        pending = self.pending.pop(state)
        if pending is None:
            return err("Unknown or expired session. Please try .lichess link again.")

        client_id = os.getenv("LICHESS_CLIENT_ID", "")
        redirect_uri = os.getenv("LICHESS_REDIRECT_URI", "")

        try:
            token_data = await exchange_code(
                self._session, code, pending.code_verifier, redirect_uri, client_id
            )
        except ValueError as e:
            return err(str(e))

        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        expires_in = token_data.get("expires_in")
        token_expires_at = None
        if expires_in:
            token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))

        try:
            account = await fetch_account(self._session, access_token)
        except ValueError as e:
            return err(str(e))

        lichess_id = account["id"].lower()
        lichess_username = account["username"]
        ratings = extract_ratings(account, _VARIANT_KEYS)

        async with self.pool.acquire() as conn:
            await upsert_account(
                conn,
                pending.discord_user_id,
                lichess_id,
                lichess_username,
                access_token,
                refresh_token,
                token_expires_at,
            )
            await upsert_ratings(conn, pending.discord_user_id, ratings)
            await update_last_synced(conn, pending.discord_user_id)
            ratings_rows = await get_ratings(conn, pending.discord_user_id)

        for guild in self.bot.guilds:
            member = guild.get_member(pending.discord_user_id)
            if member:
                await sync_member(guild, member, ratings_rows, self.pool)

        user = self.bot.get_user(pending.discord_user_id)
        if user:
            try:
                embed = discord.Embed(
                    title="Lichess account linked!",
                    description=(
                        f"Successfully linked to "
                        f"**[{lichess_username}](https://lichess.org/@/{lichess_username})**.\n"
                        f"Rating roles have been applied. Use `.profile` to view your profile."
                    ),
                    color=discord.Color.green(),
                )
                await user.send(embed=embed)
            except discord.Forbidden:
                pass

        return web.Response(text=_SUCCESS_HTML, content_type="text/html")

    # ── Background task ─────────────────────────────────────────────────────

    @tasks.loop(hours=12)
    async def periodic_refresh(self) -> None:
        async with self.pool.acquire() as conn:
            users = await list_all_linked_users(conn)
        for row in users:
            try:
                account = await fetch_account(self._session, row["access_token"])
                ratings = extract_ratings(account, _VARIANT_KEYS)
                async with self.pool.acquire() as conn:
                    await upsert_ratings(conn, row["user_id"], ratings)
                    await update_last_synced(conn, row["user_id"])
                    ratings_rows = await get_ratings(conn, row["user_id"])
                for guild in self.bot.guilds:
                    member = guild.get_member(row["user_id"])
                    if member:
                        await sync_member(guild, member, ratings_rows, self.pool)
                await asyncio.sleep(1)
            except Exception:
                pass

    @periodic_refresh.before_loop
    async def before_refresh(self) -> None:
        await self.bot.wait_until_ready()

    # ── Listeners ────────────────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        if member.bot:
            return
        async with self.pool.acquire() as conn:
            account = await get_account(conn, member.id)
            if account is None:
                return
            ratings_rows = await get_ratings(conn, member.id)
        await sync_member(member.guild, member, ratings_rows, self.pool)

    # ── .lichess commands ────────────────────────────────────────────────────

    @commands.group(name="lichess", invoke_without_command=True)
    async def lichess_group(self, ctx: commands.Context) -> None:
        """Lichess integration. Subcommands: link, unlink, refresh, setup, roles, config."""
        await ctx.send_help(ctx.command)

    @lichess_group.command(name="link")
    async def lichess_link(self, ctx: commands.Context) -> None:
        """Link your Lichess account via OAuth."""
        client_id = os.getenv("LICHESS_CLIENT_ID")
        redirect_uri = os.getenv("LICHESS_REDIRECT_URI")
        if not client_id or not redirect_uri:
            await ctx.send("Lichess OAuth is not configured on this bot.")
            return

        code_verifier, code_challenge = generate_pkce_pair()
        state = generate_state()
        self.pending.add(state, PendingOAuth(discord_user_id=ctx.author.id, code_verifier=code_verifier))
        url = build_auth_url(client_id, redirect_uri, state, code_challenge)

        embed = discord.Embed(
            title="Link your Lichess account",
            description=(
                f"[Click here to authenticate with Lichess]({url})\n\n"
                "This link expires in **10 minutes**."
            ),
            color=discord.Color.from_rgb(108, 92, 231),
        )
        try:
            await ctx.author.send(embed=embed)
            if ctx.guild:
                await ctx.message.add_reaction("✅")
        except discord.Forbidden:
            await ctx.send(embed=embed)

    @lichess_group.command(name="unlink")
    async def lichess_unlink(self, ctx: commands.Context) -> None:
        """Unlink your Lichess account and remove rating roles."""
        async with self.pool.acquire() as conn:
            deleted = await delete_account(conn, ctx.author.id)
        if not deleted:
            await ctx.send("You don't have a linked Lichess account.")
            return

        for guild in self.bot.guilds:
            member = guild.get_member(ctx.author.id)
            if member is None:
                continue
            async with self.pool.acquire() as conn:
                all_rows = await get_all_rating_roles(conn, guild.id)
            managed_ids = {r["role_id"] for r in all_rows}
            sep_role = discord.utils.get(guild.roles, name=RATING_SEPARATOR_ROLE)
            to_remove = [r for r in member.roles if r.id in managed_ids]
            if sep_role and sep_role in member.roles:
                to_remove.append(sep_role)
            if to_remove:
                try:
                    await member.remove_roles(*to_remove, reason="Lichess unlinked")
                except discord.Forbidden:
                    pass

        await ctx.send("Your Lichess account has been unlinked and rating roles removed.")

    @lichess_group.command(name="refresh")
    async def lichess_refresh(self, ctx: commands.Context) -> None:
        """Refresh your Lichess ratings and roles. 5-minute cooldown."""
        now = time.monotonic()
        last = self._refresh_cooldowns.get(ctx.author.id, 0)
        if now - last < 300:
            remaining = int(300 - (now - last))
            await ctx.send(f"Please wait {remaining}s before refreshing again.")
            return
        self._refresh_cooldowns[ctx.author.id] = now

        async with self.pool.acquire() as conn:
            account = await get_account(conn, ctx.author.id)
        if account is None:
            await ctx.send("You don't have a linked Lichess account. Use `.lichess link` first.")
            return

        msg = await ctx.send("Refreshing your Lichess data…")
        try:
            acct_data = await fetch_account(self._session, account["access_token"])
            ratings = extract_ratings(acct_data, _VARIANT_KEYS)
            async with self.pool.acquire() as conn:
                await upsert_ratings(conn, ctx.author.id, ratings)
                await update_last_synced(conn, ctx.author.id)
                ratings_rows = await get_ratings(conn, ctx.author.id)
            for guild in self.bot.guilds:
                member = guild.get_member(ctx.author.id)
                if member:
                    await sync_member(guild, member, ratings_rows, self.pool)
            await msg.edit(content="Ratings and roles updated!")
        except Exception as e:
            await msg.edit(content=f"Failed to refresh: {e}")

    # ── Admin: setup ─────────────────────────────────────────────────────────

    @lichess_group.command(name="setup")
    @commands.has_permissions(manage_roles=True)
    @commands.bot_has_permissions(manage_roles=True)
    async def lichess_setup(self, ctx: commands.Context, variant: str = None) -> None:
        """Create rating roles for all (or one) variant. Usage: .lichess setup [variant]"""
        if variant:
            variant = variant.lower()
            targets = [v for v in LICHESS_VARIANTS if v["key"] == variant]
            if not targets:
                await ctx.send(
                    f"Unknown variant. Valid: {', '.join(v['key'] for v in LICHESS_VARIANTS)}"
                )
                return
        else:
            targets = LICHESS_VARIANTS

        msg = await ctx.send("Creating rating roles…")
        created = 0

        sep_role = discord.utils.get(ctx.guild.roles, name=RATING_SEPARATOR_ROLE)
        if sep_role is None:
            await ctx.guild.create_role(name=RATING_SEPARATOR_ROLE, mentionable=False)

        async with self.pool.acquire() as conn:
            for v in targets:
                key = v["key"]
                name = v["name"]
                defaults = LICHESS_RATING_ROLE_DEFAULTS.get(
                    key, {"min": 2000, "step": 100, "max": 2700}
                )
                cfg_row = await get_rating_role_config(conn, ctx.guild.id, key)
                min_r = cfg_row["min_rating"] if cfg_row else defaults["min"]
                step = cfg_row["step"] if cfg_row else defaults["step"]
                max_r = cfg_row["max_rating"] if cfg_row else defaults["max"]

                tier = min_r
                while tier <= max_r:
                    role_name = f"{name} {tier}"
                    existing = discord.utils.get(ctx.guild.roles, name=role_name)
                    if existing is None:
                        existing = await ctx.guild.create_role(name=role_name, mentionable=False)
                        created += 1
                    await upsert_rating_role(conn, ctx.guild.id, key, tier, existing.id)
                    tier += step

        await msg.edit(
            content=f"Done. Created {created} new role(s). Use `.lichess roles list` to review."
        )

    # ── Admin: roles subgroup ─────────────────────────────────────────────────

    @lichess_group.group(name="roles", invoke_without_command=True)
    @commands.has_permissions(manage_roles=True)
    async def lichess_roles(self, ctx: commands.Context) -> None:
        """Manage rating role bindings. Subcommands: list, bind."""
        await ctx.send_help(ctx.command)

    @lichess_roles.command(name="list")
    @commands.has_permissions(manage_roles=True)
    async def lichess_roles_list(self, ctx: commands.Context) -> None:
        """List all configured rating role bindings."""
        async with self.pool.acquire() as conn:
            all_rows = await get_all_rating_roles(conn, ctx.guild.id)
        if not all_rows:
            await ctx.send("No rating roles configured. Run `.lichess setup` first.")
            return

        by_variant: Dict[str, list] = {}
        for row in all_rows:
            by_variant.setdefault(row["variant"], []).append(row)

        embed = discord.Embed(
            title="Lichess Rating Roles", color=discord.Color.from_rgb(108, 92, 231)
        )
        for v in LICHESS_VARIANTS:
            rows = by_variant.get(v["key"], [])
            if not rows:
                continue
            lines = []
            for row in rows:
                role = ctx.guild.get_role(row["role_id"])
                role_str = role.mention if role else f"*(deleted {row['role_id']})*"
                lines.append(f"`{row['tier']}+` → {role_str}")
            embed.add_field(name=v["name"], value="\n".join(lines), inline=True)
        await ctx.send(embed=embed)

    @lichess_roles.command(name="bind")
    @commands.has_permissions(manage_roles=True)
    async def lichess_roles_bind(
        self, ctx: commands.Context, variant: str, tier: int, *, role: discord.Role
    ) -> None:
        """Manually bind a rating tier to a role. Usage: .lichess roles bind <variant> <tier> <role>"""
        variant = variant.lower()
        if variant not in _VARIANT_KEYS:
            await ctx.send(f"Unknown variant. Valid: {', '.join(_VARIANT_KEYS)}")
            return
        if role.is_default() or role.managed or role >= ctx.guild.me.top_role:
            await ctx.send(
                "Invalid role — can't use @everyone, managed roles, or roles above my top role."
            )
            return
        async with self.pool.acquire() as conn:
            await upsert_rating_role(conn, ctx.guild.id, variant, tier, role.id)
        await ctx.send(f"Bound `{variant} {tier}+` → {role.mention}")

    # ── Admin: config ─────────────────────────────────────────────────────────

    @lichess_group.command(name="config")
    @commands.has_permissions(manage_roles=True)
    async def lichess_config(
        self,
        ctx: commands.Context,
        variant: str,
        min_rating: int,
        step: int,
        max_rating: int,
        enabled: bool = True,
    ) -> None:
        """Configure rating tier settings for a variant.
        Usage: .lichess config <variant> <min> <step> <max> [enabled]"""
        variant = variant.lower()
        if variant not in _VARIANT_KEYS:
            await ctx.send(f"Unknown variant. Valid: {', '.join(_VARIANT_KEYS)}")
            return
        if step <= 0 or min_rating < 0 or max_rating <= min_rating:
            await ctx.send("Invalid values: step must be > 0, max > min, min >= 0.")
            return
        async with self.pool.acquire() as conn:
            await upsert_rating_role_config(
                conn, ctx.guild.id, variant, min_rating, step, max_rating, enabled
            )
        await ctx.send(
            f"Updated `{variant}` config: min={min_rating}, step={step}, "
            f"max={max_rating}, enabled={enabled}"
        )

    # ── .profile commands ─────────────────────────────────────────────────────

    @commands.group(name="profile", invoke_without_command=True)
    async def profile_cmd(self, ctx: commands.Context, *, member: discord.Member = None) -> None:
        """Show a chess profile. Usage: .profile [@user]"""
        target = member or ctx.author
        async with self.pool.acquire() as conn:
            account = await get_account(conn, target.id)
            if account is None:
                who = "You don't have" if target == ctx.author else f"{target.display_name} doesn't have"
                await ctx.send(f"{who} a linked Lichess account.")
                return
            ratings_rows = await get_ratings(conn, target.id)
            style = await get_profile_style(conn, target.id)

        display = ctx.guild.get_member(target.id) if ctx.guild else target
        embed = build_profile_embed(display or target, account, ratings_rows, style)
        await ctx.send(embed=embed)

    @profile_cmd.command(name="style")
    async def profile_style(self, ctx: commands.Context, style: str) -> None:
        """Set your profile style. Available: default, gold, chess. Usage: .profile style <style>"""
        style = style.lower()
        if style not in STYLES:
            await ctx.send(f"Unknown style. Available: {', '.join(STYLES.keys())}")
            return
        async with self.pool.acquire() as conn:
            await upsert_profile_style(conn, ctx.author.id, style)
        await ctx.send(f"Profile style set to **{style}**.")

    # ── Error handler ──────────────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError) -> None:
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You need Manage Roles permission for that command.")
        elif isinstance(error, commands.BotMissingPermissions):
            await ctx.send("I'm missing Manage Roles permission.")
        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing argument. Check `.help {ctx.command.qualified_name}`.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Check `.help {ctx.command.qualified_name}`.")
        else:
            raise error
