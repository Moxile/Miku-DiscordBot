"""Retry helper for transient Discord API failures.

discord.py's HTTPClient only auto-retries 500/502/504/524 (see http.py's
request loop) — 503 (the Envoy "upstream connect error" edge hiccup) is not
in that set and raises immediately. send_resilient covers that gap for
call sites where a lost message would otherwise strand a player mid-game.
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, TypeVar

import discord

T = TypeVar("T")

RETRYABLE_STATUSES = {500, 502, 503, 504, 524}


async def send_resilient(
    coro_factory: Callable[[], Awaitable[T]],
    *,
    retries: int = 3,
    base_delay: float = 1.0,
    files: list[discord.File] | None = None,
) -> T:
    """Call an async Discord API action, retrying on transient 5xx failures.

    coro_factory is a zero-arg callable returning a fresh coroutine each call
    (e.g. ``lambda: ctx.send(...)``), since a coroutine object can't be reused
    across attempts. Pass any discord.File objects the call reuses via
    ``files=`` so their stream position is rewound before each retry — a
    failed request has already fully read the stream, and Discord's own
    per-request retry loop only resets it within a single HTTP call.
    """
    last_exc: discord.HTTPException | None = None
    for attempt in range(retries):
        for f in files or ():
            f.reset(seek=attempt)
        try:
            return await coro_factory()
        except discord.HTTPException as e:
            if e.status not in RETRYABLE_STATUSES:
                raise
            last_exc = e
            if attempt < retries - 1:
                await asyncio.sleep(base_delay * (attempt + 1))
    raise last_exc


async def refund_and_release(pool, guild_id: int, user_id: int, amount: int, games: dict, key) -> None:
    """Undo a bet that never made it to the player: drop the lock entry (if
    present) and refund the wallet."""
    from cogs.economy.db import update_wallet  # deferred: avoids a cogs<->core circular import

    games.pop(key, None)
    if amount:
        await update_wallet(pool, guild_id, user_id, amount)
