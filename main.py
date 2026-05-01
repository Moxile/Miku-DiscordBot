from __future__ import annotations
import os
import asyncpg
import discord
from aiohttp import web
from discord.ext import commands
from dotenv import load_dotenv

from config import PREFIX
from core.db_init import init_db

load_dotenv()

COG_COLORS = {
    "Economy": discord.Color.green(),
    "Gambling": discord.Color.gold(),
    "Market": discord.Color.blue(),
    "Moderation": discord.Color.red(),
    "Shop": discord.Color.purple(),
    "Predictions": discord.Color.teal(),
    "Reminders": discord.Color.from_rgb(255, 182, 193),
    "Waifu": discord.Color.from_rgb(255, 105, 180),
    "Leaderboard": discord.Color.from_rgb(255, 215, 0),
    "Acro": discord.Color.orange(),
    "GTE": discord.Color.dark_green(),
    "Utility": discord.Color.blurple(),
    "ReactionRoles": discord.Color.fuchsia(),
    "WolfRandom": discord.Color.dark_teal(),
    "BotReactions": discord.Color.from_rgb(255, 165, 0),
}

EXTENSIONS = [
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
]


class Help(commands.HelpCommand):
    async def command_callback(self, ctx, /, *, command=None):
        """Override to support case-insensitive cog lookup."""
        if command:
            for name, cog in ctx.bot.cogs.items():
                if name.lower() == command.lower():
                    return await self.send_cog_help(cog)
        return await super().command_callback(ctx, command=command)

    async def send_bot_help(self, mapping):
        ctx = self.context
        embed = discord.Embed(title="Help", description="Use `.help <category>` or `.help <command>` for details.", color=discord.Color.blurple())
        for cog, cmds in mapping.items():
            filtered = await self.filter_commands(cmds, sort=True)
            if not filtered:
                continue
            name = cog.qualified_name if cog else "Other"
            value = ", ".join(f"`{c.name}`" for c in filtered)
            embed.add_field(name=name, value=value, inline=False)
        try:
            await ctx.author.send(embed=embed)
            await ctx.message.add_reaction("✅")
        except discord.Forbidden:
            await ctx.send(embed=embed)

    async def send_cog_help(self, cog):
        color = COG_COLORS.get(cog.qualified_name, discord.Color.greyple())
        embed = discord.Embed(title=f"{cog.qualified_name} Commands", color=color)
        filtered = await self.filter_commands(cog.get_commands(), sort=True)
        for cmd in filtered:
            label = cmd.name
            if cmd.aliases:
                label += " (" + ", ".join(cmd.aliases) + ")"
            for check in cmd.checks:
                if "is_owner" in str(check):
                    label += " [Owner]"
                    break
                if "has_permissions" in str(check):
                    label += " [Admin]"
                    break
            embed.add_field(
                name=label,
                value=cmd.short_doc or "No description",
                inline=False,
            )
        embed.set_footer(text=f"Use .help <command> for more info on a command.")
        await self.get_destination().send(embed=embed)

    async def send_command_help(self, command):
        cog = command.cog
        color = COG_COLORS.get(cog.qualified_name, discord.Color.greyple()) if cog else discord.Color.greyple()
        embed = discord.Embed(title=f".{command.name}", description=command.help or "No description", color=color)
        embed.add_field(name="Usage", value=f"`{self.get_command_signature(command)}`", inline=False)
        if command.aliases:
            embed.add_field(name="Aliases", value=", ".join(f"`{a}`" for a in command.aliases), inline=False)
        if cog:
            siblings = [c.name for c in cog.get_commands() if c != command and not c.hidden]
            if siblings:
                embed.set_footer(text=f"Related: {', '.join(siblings)}")
        await self.get_destination().send(embed=embed)

    async def send_error_message(self, error):
        embed = discord.Embed(title="Not Found", description=error, color=discord.Color.red())
        await self.get_destination().send(embed=embed)


class MikuBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix=PREFIX, intents=intents)
        self.pool: asyncpg.Pool | None = None
        self.oauth_runner: web.AppRunner | None = None

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
bot.help_command = Help(command_attrs={"aliases": ["h"]})
bot.run(os.getenv("DISCORD_TOKEN"))
