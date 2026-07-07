from core.ui import PageEntry, register_page

from .cog import Management
from .ui import AdminPage


async def setup(bot):
    register_page(PageEntry(
        key="admin",
        label="Settings",
        emoji="⚙️",
        description="Manage server settings and features",
        factory=AdminPage,
        cog_name="Management",
        owner_only=True,
    ))
    await bot.add_cog(Management(bot))
