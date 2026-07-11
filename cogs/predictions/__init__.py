from core.ui import PageEntry, register_page

from .cog import Predictions
from .ui import PredictionsPage


async def setup(bot):
    register_page(PageEntry(
        key="predictions",
        label="Predictions",
        emoji="🔮",
        description="Pool predictions — view, bet on & create predictions",
        factory=PredictionsPage,
        cog_name="Predictions",
    ))
    await bot.add_cog(Predictions(bot))
