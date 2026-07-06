from core.ui import PageEntry, register_page

from cogs.missions.cog import Missions
from cogs.missions.ui import MissionsPage


async def setup(bot):
    register_page(PageEntry(
        key="missions",
        label="Missions",
        emoji="🎯",
        description="Browse active missions and fund them",
        factory=MissionsPage,
        cog_name="Missions",
    ))
    await bot.add_cog(Missions(bot))
