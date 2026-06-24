from __future__ import annotations
import os
import asyncpg
import discord
from aiohttp import web
from discord.ext import commands
from dotenv import load_dotenv

from config import PREFIX
from core.currency import Currency, DEFAULT_CURRENCY
from core.db_init import init_db
from core.help import Help

load_dotenv()

EXTENSIONS = [
    "cogs.management",
    "cogs.moderation",
    "cogs.economy",
    "cogs.gambling",
    "cogs.market",
    "cogs.shop",
    "cogs.predictions",
    "cogs.offers",
    "cogs.reminders",
    "cogs.waifu",
    "cogs.leaderboard",
    "cogs.acro",
    "cogs.gte",
    "cogs.utility",
    "cogs.reaction_roles",
    "cogs.counting",
    "cogs.lichess",
    "cogs.wolfrandom",
    "cogs.bot_reactions",
    "cogs.missions",
    "cogs.errors",  # global command-error handler — must load last (catch-all)
]


class MikuBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(
            command_prefix=PREFIX, intents=intents, owner_id=1261733355443978255,
            allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=True),
        )
        self.pool: asyncpg.Pool | None = None
        self.oauth_runner: web.AppRunner | None = None
        self._disabled_cogs_cache: dict[int, set[str]] = {}
        self._owner_role_cache: dict[int, int | None] = {}
        self._currency_cache: dict[int, Currency] = {}

    def get_currency(self, guild_id: int | None) -> Currency:
        """Return the configured currency for a guild, or the default if unset."""
        if guild_id is None:
            return DEFAULT_CURRENCY
        return self._currency_cache.get(guild_id, DEFAULT_CURRENCY)

    async def _is_cog_disabled(self, guild_id: int, cog_name: str) -> bool:
        if guild_id not in self._disabled_cogs_cache:
            rows = await self.pool.fetch(
                "SELECT cog_name FROM disabled_cogs WHERE guild_id = $1", guild_id
            )
            self._disabled_cogs_cache[guild_id] = {row["cog_name"] for row in rows}
        return cog_name in self._disabled_cogs_cache[guild_id]

    async def _get_owner_role(self, guild_id: int) -> int | None:
        """Return the role id that grants owner-command access in this guild, or None."""
        if guild_id not in self._owner_role_cache:
            row = await self.pool.fetchrow(
                "SELECT value FROM guild_settings WHERE guild_id = $1 AND key = 'owner_role'",
                guild_id,
            )
            self._owner_role_cache[guild_id] = int(row["value"]) if row else None
        return self._owner_role_cache[guild_id]

    async def is_bot_owner(self, user: discord.abc.User) -> bool:
        """The application/bot owner only — ignores guild-owner and owner-role grants."""
        return await commands.Bot.is_owner(self, user)

    async def is_owner(self, user: discord.abc.User) -> bool:
        """The bot owner, the guild owner, anyone with the Administrator permission,
        or anyone holding the configured owner role.

        Administrator is granted explicitly because it already gives a member
        owner-equivalent control over the guild (manage roles/channels, kick/ban, etc.).
        """
        if await self.is_bot_owner(user):
            return True
        guild = getattr(user, "guild", None)
        if guild is None:
            return False
        if user.id == guild.owner_id:
            return True
        permissions = getattr(user, "guild_permissions", None)
        if permissions is not None and permissions.administrator:
            return True
        roles = getattr(user, "roles", None)
        if roles is None or self.pool is None:
            return False
        owner_role_id = await self._get_owner_role(guild.id)
        return owner_role_id is not None and any(r.id == owner_role_id for r in roles)

    async def invoke(self, ctx: commands.Context):
        if ctx.command and ctx.cog and ctx.guild:
            cog_name = ctx.cog.__class__.__name__
            if cog_name != "Management" and await self._is_cog_disabled(ctx.guild.id, cog_name):
                return
        await super().invoke(ctx)

    async def setup_hook(self):
        self.pool = await asyncpg.create_pool(
            dsn=os.getenv("DATABASE_URL"),
            min_size=2,
            max_size=10,
        )
        await init_db(self.pool)

        for row in await self.pool.fetch("SELECT guild_id, name, emoji FROM guild_currency"):
            self._currency_cache[row["guild_id"]] = Currency(row["name"], row["emoji"])

        for ext in EXTENSIONS:
            await self.load_extension(ext)

        # Start the embedded OAuth callback server
        lichess_cog = self.cogs.get("Lichess")
        if lichess_cog and os.getenv("LICHESS_CLIENT_ID"):
            app = web.Application()
            app.router.add_get("/callback", lichess_cog.handle_callback)
            runner = web.AppRunner(app)
            await runner.setup()
            port = int(os.getenv("OAUTH_PORT", "8080"))
            site = web.TCPSite(runner, "0.0.0.0", port)
            await site.start()
            self.oauth_runner = runner

    async def close(self):
        if self.oauth_runner:
            await self.oauth_runner.cleanup()
        if self.pool:
            await self.pool.close()
        await super().close()


# Guarded so child processes (e.g. the calc worker under the multiprocessing
# "spawn" start method, which re-imports this module) don't start a second bot.
if __name__ == "__main__":
    bot = MikuBot()
    bot.help_command = Help(verify_checks=False, command_attrs={"aliases": ["h"]})
    bot.run(os.getenv("DISCORD_TOKEN"))
