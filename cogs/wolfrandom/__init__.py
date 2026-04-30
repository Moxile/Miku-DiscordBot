from .cog import WolfRandom


async def setup(bot):
    await bot.add_cog(WolfRandom(bot))
