from core.ui import PageEntry, register_page

from .cog import Economy
from .ui import EconomyPage


async def setup(bot):
    register_page(PageEntry(
        key="economy",
        label="Economy",
        emoji="💰",
        description="Balance, deposits, work, crime, salaries, gifts",
        factory=EconomyPage,
        cog_name="Economy",
    ))
    await bot.add_cog(Economy(bot))
