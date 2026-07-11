from core.ui import PageEntry, register_page

from .cog import Bets
from .ui import BetsPage


async def setup(bot):
    register_page(PageEntry(
        key="bets",
        label="Bets",
        emoji="🎲",
        description="Bookmaker bets — view, place & host fixed-odds bets",
        factory=BetsPage,
        cog_name="Bets",
    ))
    await bot.add_cog(Bets(bot))
