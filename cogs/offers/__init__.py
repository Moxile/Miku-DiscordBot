from .cog import Offers


async def setup(bot):
    await bot.add_cog(Offers(bot))
