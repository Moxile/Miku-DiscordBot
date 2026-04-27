from .cog import Market


async def setup(bot):
    await bot.add_cog(Market(bot))
