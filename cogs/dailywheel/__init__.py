from .cog import DailyWheel


async def setup(bot):
    await bot.add_cog(DailyWheel(bot))
