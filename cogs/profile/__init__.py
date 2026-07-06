from core.ui import PageEntry, register_page

from .cog import Profile
from .ui import ProfilePage


async def setup(bot):
    register_page(PageEntry(
        key="profile",
        label="Profile",
        emoji="👤",
        description="Balances, gambling stats, holdings, and a history graph",
        factory=ProfilePage,
        cog_name="Profile",
    ))
    await bot.add_cog(Profile(bot))
