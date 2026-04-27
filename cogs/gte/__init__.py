from .cog import GTE


async def setup(bot):
    await bot.add_cog(GTE(bot))
