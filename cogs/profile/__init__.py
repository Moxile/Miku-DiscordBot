from .cog import Profile


async def setup(bot):
    await bot.add_cog(Profile(bot))
