import asyncpg

from core.db import Conn


async def create_mission(conn: Conn, guild_id: int, name: str, description: str, goal: int) -> asyncpg.Record:
    return await conn.fetchrow(
        "INSERT INTO missions (guild_id, name, description, goal) VALUES ($1, $2, $3, $4) RETURNING *",
        guild_id, name, description, goal,
    )


async def get_missions(conn: Conn, guild_id: int, status: str = "active"):
    return await conn.fetch(
        "SELECT * FROM missions WHERE guild_id = $1 AND status = $2 ORDER BY created_at ASC",
        guild_id, status,
    )


async def get_mission(conn: Conn, guild_id: int, mission_id: int) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "SELECT * FROM missions WHERE id = $1 AND guild_id = $2",
        mission_id, guild_id,
    )


async def get_mission_by_name(conn: Conn, guild_id: int, name: str) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "SELECT * FROM missions WHERE guild_id = $1 AND lower(name) = lower($2)",
        guild_id, name,
    )


async def add_funding(conn: Conn, mission_id: int, guild_id: int, user_id: int, amount: int) -> asyncpg.Record:
    await conn.execute(
        "INSERT INTO mission_contributions (mission_id, guild_id, user_id, amount) VALUES ($1, $2, $3, $4)",
        mission_id, guild_id, user_id, amount,
    )
    return await conn.fetchrow(
        "UPDATE missions SET funded = funded + $2 WHERE id = $1 RETURNING *",
        mission_id, amount,
    )


async def set_mission_status(conn: Conn, mission_id: int, status: str) -> None:
    await conn.execute(
        "UPDATE missions SET status = $2 WHERE id = $1",
        mission_id, status,
    )


async def delete_mission(conn: Conn, guild_id: int, mission_id: int) -> str:
    return await conn.execute(
        "DELETE FROM missions WHERE id = $1 AND guild_id = $2",
        mission_id, guild_id,
    )
