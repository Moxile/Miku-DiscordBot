from cogs.missions.cog import Missions


async def setup(bot):
    await bot.add_cog(Missions(bot))
