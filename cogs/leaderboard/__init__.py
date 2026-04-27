from .cog import Leaderboard


async def setup(bot):
    await bot.add_cog(Leaderboard(bot))
