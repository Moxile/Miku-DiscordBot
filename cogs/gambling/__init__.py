from core.ui import PageEntry, register_page

from .cog import Gambling
from .ui import GamblingPage


async def setup(bot):
    register_page(PageEntry(
        key="gambling",
        label="Games",
        emoji="🎰",
        description="Bet and play games",
        factory=GamblingPage,
        cog_name="Gambling",
    ))
    await bot.add_cog(Gambling(bot))
