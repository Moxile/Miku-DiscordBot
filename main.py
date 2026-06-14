from __future__ import annotations
import os
import asyncpg
import discord
from aiohttp import web
from discord.ext import commands
from dotenv import load_dotenv

from config import PREFIX
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
]


class MikuBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix=PREFIX, intents=intents)
        self.pool: asyncpg.Pool | None = None
        self.oauth_runner: web.AppRunner | None = None
        self._disabled_cogs_cache: dict[int, set[str]] = {}

    async def _is_cog_disabled(self, guild_id: int, cog_name: str) -> bool:
        if guild_id not in self._disabled_cogs_cache:
            rows = await self.pool.fetch(
                "SELECT cog_name FROM disabled_cogs WHERE guild_id = $1", guild_id
            )
            self._disabled_cogs_cache[guild_id] = {row["cog_name"] for row in rows}
        return cog_name in self._disabled_cogs_cache[guild_id]

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


bot = MikuBot()
bot.help_command = Help(verify_checks=False, command_attrs={"aliases": ["h"]})
bot.run(os.getenv("DISCORD_TOKEN"))
