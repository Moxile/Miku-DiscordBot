from .cog import TypeContest


async def setup(bot):
    await bot.add_cog(TypeContest(bot))
