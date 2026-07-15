from core.ui import PageEntry, register_page

from .cog import CFDTrading
from .ui import CFDPage


async def setup(bot):
    register_page(PageEntry(
        key="cfd",
        label="CFD Trading",
        emoji="📊",
        description="Leveraged long/short bets on real stocks",
        factory=CFDPage,
        cog_name="CFDTrading",
    ))
    await bot.add_cog(CFDTrading(bot))
