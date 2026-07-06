from core.ui import PageEntry, register_page

from .cog import RealStocks
from .ui import RealStocksPage


async def setup(bot):
    register_page(PageEntry(
        key="realstocks",
        label="Real Stocks",
        emoji="🌐",
        description="Trade real-world stocks at live prices",
        factory=RealStocksPage,
        cog_name="RealStocks",
    ))
    await bot.add_cog(RealStocks(bot))
