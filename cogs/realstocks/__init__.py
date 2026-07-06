from .cog import RealStocks


async def setup(bot):
    await bot.add_cog(RealStocks(bot))
