from core.ui import PageEntry, register_page

from .cog import Leaderboard
from .ui import LeaderboardPage


async def setup(bot):
    register_page(PageEntry(
        key="leaderboard",
        label="Leaderboard",
        emoji="🏆",
        description="Net worth, wallet, bank, portfolio, harem, reaction rankings",
        factory=LeaderboardPage,
        cog_name="Leaderboard",
    ))
    await bot.add_cog(Leaderboard(bot))
