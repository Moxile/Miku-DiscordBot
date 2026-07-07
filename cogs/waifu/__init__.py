from core.ui import PageEntry, register_page

from .cog import Waifu
from .ui import WaifuPage


async def setup(bot):
    register_page(PageEntry(
        key="waifu",
        label="Waifu",
        emoji="💕",
        description="Manage your waifu and harem",
        factory=WaifuPage,
        cog_name="Waifu",
    ))
    await bot.add_cog(Waifu(bot))
