from __future__ import annotations

"""Registry of top-level Miku Menu pages.

Each feature cog registers its page at extension-setup time; the home page
renders one button per visible entry. Registration is idempotent by key so
extension reloads don't duplicate entries.
"""

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class PageEntry:
    key: str                    # stable identifier, e.g. "economy"
    label: str                  # button text on the home page
    emoji: str
    description: str            # one-liner shown on the home page
    factory: Callable           # (hub) -> Page
    cog_name: str = None        # hidden in guilds where this cog is disabled
    owner_only: bool = False    # hidden unless bot.is_owner(user) passes


_REGISTRY: dict[str, PageEntry] = {}


def register_page(entry: PageEntry) -> None:
    _REGISTRY[entry.key] = entry


def page_entries() -> list[PageEntry]:
    """All registered entries, in registration order."""
    return list(_REGISTRY.values())
