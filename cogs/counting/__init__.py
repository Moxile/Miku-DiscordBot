from .cog import Counting


async def setup(bot):
    await bot.add_cog(Counting(bot))
