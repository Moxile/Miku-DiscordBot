from .cog import BotReactions
from . import schema


async def setup(bot):
    await bot.add_cog(BotReactions(bot))
