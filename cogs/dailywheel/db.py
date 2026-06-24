from core.db import Conn


async def get_prizes(conn: Conn, guild_id: int):
    return await conn.fetch(
        "SELECT * FROM wheel_prizes WHERE guild_id = $1 ORDER BY id", guild_id,
    )


async def add_currency_prize(conn: Conn, guild_id: int, weight: int, amount: int, text: str):
    return await conn.fetchrow(
        """INSERT INTO wheel_prizes (guild_id, kind, weight, amount, text)
           VALUES ($1, 'currency', $2, $3, $4) RETURNING *""",
        guild_id, weight, amount, text,
    )


async def add_message_prize(conn: Conn, guild_id: int, weight: int, text: str):
    return await conn.fetchrow(
        """INSERT INTO wheel_prizes (guild_id, kind, weight, text)
           VALUES ($1, 'message', $2, $3) RETURNING *""",
        guild_id, weight, text,
    )


async def add_role_prize(conn: Conn, guild_id: int, weight: int, role_id: int, text: str):
    return await conn.fetchrow(
        """INSERT INTO wheel_prizes (guild_id, kind, weight, role_id, text)
           VALUES ($1, 'role', $2, $3, $4) RETURNING *""",
        guild_id, weight, role_id, text,
    )


async def remove_prize(conn: Conn, guild_id: int, prize_id: int):
    return await conn.fetchrow(
        "DELETE FROM wheel_prizes WHERE guild_id = $1 AND id = $2 RETURNING *",
        guild_id, prize_id,
    )


async def get_last_spin(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetchval(
        "SELECT last_spin FROM wheel_spins WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def record_spin(conn: Conn, guild_id: int, user_id: int):
    await conn.execute(
        """INSERT INTO wheel_spins (guild_id, user_id, last_spin)
           VALUES ($1, $2, (now() AT TIME ZONE 'UTC')::date)
           ON CONFLICT (guild_id, user_id) DO UPDATE SET last_spin = EXCLUDED.last_spin""",
        guild_id, user_id,
    )


async def remove_member_data(conn: Conn, guild_id: int, user_id: int):
    await conn.execute(
        "DELETE FROM wheel_spins WHERE guild_id = $1 AND user_id = $2", guild_id, user_id,
    )
