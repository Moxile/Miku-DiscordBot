from .cog import Hub


async def setup(bot):
    await bot.add_cog(Hub(bot))
