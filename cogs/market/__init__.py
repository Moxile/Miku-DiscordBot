from core.ui import PageEntry, register_page

from .cog import Market
from .ui import MarketPage


async def setup(bot):
    register_page(PageEntry(
        key="market",
        label="Market",
        emoji="📈",
        description="Trade server stocks: buy, sell, limit orders, portfolio",
        factory=MarketPage,
        cog_name="Market",
    ))
    await bot.add_cog(Market(bot))
