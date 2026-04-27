from .cog import Predictions


async def setup(bot):
    await bot.add_cog(Predictions(bot))
