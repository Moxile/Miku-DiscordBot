import asyncpg

from core.db import Conn


async def create_reminder(conn: Conn, guild_id: int, user_id: int, channel_id: int,
                           message: str, remind_at) -> asyncpg.Record:
    return await conn.fetchrow(
        """INSERT INTO reminders (guild_id, user_id, channel_id, message, remind_at)
           VALUES ($1, $2, $3, $4, $5) RETURNING *""",
        guild_id, user_id, channel_id, message, remind_at,
    )


async def get_due_reminders(conn: Conn) -> list:
    return await conn.fetch(
        "SELECT * FROM reminders WHERE remind_at <= NOW() ORDER BY remind_at",
    )


async def delete_reminder(conn: Conn, reminder_id: int):
    await conn.execute("DELETE FROM reminders WHERE id = $1", reminder_id)


async def get_user_reminders(conn: Conn, guild_id: int, user_id: int) -> list:
    return await conn.fetch(
        "SELECT * FROM reminders WHERE guild_id = $1 AND user_id = $2 ORDER BY remind_at",
        guild_id, user_id,
    )


async def cancel_reminder(conn: Conn, guild_id: int, user_id: int, reminder_id: int) -> asyncpg.Record:
    return await conn.fetchrow(
        "DELETE FROM reminders WHERE id = $1 AND guild_id = $2 AND user_id = $3 RETURNING *",
        reminder_id, guild_id, user_id,
    )
