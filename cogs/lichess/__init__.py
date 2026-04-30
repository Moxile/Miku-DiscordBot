from cogs.lichess.cog import Lichess


async def setup(bot):
    await bot.add_cog(Lichess(bot))
