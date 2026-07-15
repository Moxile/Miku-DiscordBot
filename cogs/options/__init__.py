from core.ui import PageEntry, register_page

from .cog import Options
from .ui import OptionsPage


async def setup(bot):
    register_page(PageEntry(
        key="options",
        label="Options",
        emoji="🎟️",
        description="Buy European calls/puts on real stocks",
        factory=OptionsPage,
        cog_name="Options",
    ))
    await bot.add_cog(Options(bot))
