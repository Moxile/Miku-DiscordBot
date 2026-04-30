from __future__ import annotations

from typing import List, Optional

import asyncpg

from core.db import Conn


async def get_account(conn: Conn, user_id: int) -> Optional[asyncpg.Record]:
    return await conn.fetchrow("SELECT * FROM lichess_accounts WHERE user_id = $1", user_id)


async def upsert_account(
    conn: Conn,
    user_id: int,
    lichess_id: str,
    lichess_username: str,
    access_token: str,
    refresh_token: Optional[str] = None,
    token_expires_at=None,
) -> None:
    await conn.execute(
        """
        INSERT INTO lichess_accounts
            (user_id, lichess_id, lichess_username, access_token, refresh_token, token_expires_at, last_synced_at)
        VALUES ($1, $2, $3, $4, $5, $6, now())
        ON CONFLICT (user_id) DO UPDATE SET
            lichess_id       = EXCLUDED.lichess_id,
            lichess_username = EXCLUDED.lichess_username,
            access_token     = EXCLUDED.access_token,
            refresh_token    = EXCLUDED.refresh_token,
            token_expires_at = EXCLUDED.token_expires_at,
            last_synced_at   = now()
        """,
        user_id, lichess_id, lichess_username, access_token, refresh_token, token_expires_at,
    )


async def delete_account(conn: Conn, user_id: int) -> bool:
    result = await conn.execute("DELETE FROM lichess_accounts WHERE user_id = $1", user_id)
    return result != "DELETE 0"


async def update_last_synced(conn: Conn, user_id: int) -> None:
    await conn.execute(
        "UPDATE lichess_accounts SET last_synced_at = now() WHERE user_id = $1", user_id
    )


async def upsert_ratings(conn: Conn, user_id: int, ratings: List[dict]) -> None:
    await conn.executemany(
        """
        INSERT INTO lichess_ratings (user_id, variant, rating, games, prov, updated_at)
        VALUES ($1, $2, $3, $4, $5, now())
        ON CONFLICT (user_id, variant) DO UPDATE SET
            rating     = EXCLUDED.rating,
            games      = EXCLUDED.games,
            prov       = EXCLUDED.prov,
            updated_at = now()
        """,
        [(user_id, r["variant"], r["rating"], r["games"], r["prov"]) for r in ratings],
    )


async def get_ratings(conn: Conn, user_id: int) -> List[asyncpg.Record]:
    return await conn.fetch("SELECT * FROM lichess_ratings WHERE user_id = $1", user_id)


async def get_rating_role_config(conn: Conn, guild_id: int, variant: str) -> Optional[asyncpg.Record]:
    return await conn.fetchrow(
        "SELECT * FROM lichess_rating_role_config WHERE guild_id = $1 AND variant = $2",
        guild_id, variant,
    )


async def upsert_rating_role_config(
    conn: Conn,
    guild_id: int,
    variant: str,
    min_rating: int,
    step: int,
    max_rating: int,
    enabled: bool,
) -> None:
    await conn.execute(
        """
        INSERT INTO lichess_rating_role_config (guild_id, variant, min_rating, step, max_rating, enabled)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (guild_id, variant) DO UPDATE SET
            min_rating = EXCLUDED.min_rating,
            step       = EXCLUDED.step,
            max_rating = EXCLUDED.max_rating,
            enabled    = EXCLUDED.enabled
        """,
        guild_id, variant, min_rating, step, max_rating, enabled,
    )


async def get_rating_roles(conn: Conn, guild_id: int, variant: str) -> List[asyncpg.Record]:
    return await conn.fetch(
        "SELECT * FROM lichess_rating_roles WHERE guild_id = $1 AND variant = $2 ORDER BY tier",
        guild_id, variant,
    )


async def get_all_rating_roles(conn: Conn, guild_id: int) -> List[asyncpg.Record]:
    return await conn.fetch(
        "SELECT * FROM lichess_rating_roles WHERE guild_id = $1 ORDER BY variant, tier",
        guild_id,
    )


async def upsert_rating_role(conn: Conn, guild_id: int, variant: str, tier: int, role_id: int) -> None:
    await conn.execute(
        """
        INSERT INTO lichess_rating_roles (guild_id, variant, tier, role_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (guild_id, variant, tier) DO UPDATE SET role_id = EXCLUDED.role_id
        """,
        guild_id, variant, tier, role_id,
    )


async def delete_rating_role(conn: Conn, guild_id: int, variant: str, tier: int) -> bool:
    result = await conn.execute(
        "DELETE FROM lichess_rating_roles WHERE guild_id = $1 AND variant = $2 AND tier = $3",
        guild_id, variant, tier,
    )
    return result != "DELETE 0"


async def list_all_linked_users(conn: Conn) -> List[asyncpg.Record]:
    return await conn.fetch("SELECT user_id, access_token FROM lichess_accounts")


async def get_profile_style(conn: Conn, user_id: int) -> str:
    row = await conn.fetchrow("SELECT style FROM chess_profiles WHERE user_id = $1", user_id)
    return row["style"] if row else "default"


async def upsert_profile_style(conn: Conn, user_id: int, style: str) -> None:
    await conn.execute(
        """
        INSERT INTO chess_profiles (user_id, style) VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET style = EXCLUDED.style
        """,
        user_id, style,
    )
