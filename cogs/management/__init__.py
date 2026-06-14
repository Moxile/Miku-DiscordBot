from .cog import Management


async def setup(bot):
    await bot.add_cog(Management(bot))
