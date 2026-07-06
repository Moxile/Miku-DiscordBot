from core.ui import PageEntry, register_page

from .cog import Shop
from .ui import ShopPage


async def setup(bot):
    register_page(PageEntry(
        key="shop",
        label="Shop",
        emoji="🛍️",
        description="Buy items and roles, view your inventory",
        factory=ShopPage,
        cog_name="Shop",
    ))
    await bot.add_cog(Shop(bot))
