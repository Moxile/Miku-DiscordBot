from core.db import Conn


async def create_item(conn: Conn, guild_id: int, name: str, price: int,
                      description: str = None, item_type: str = "item",
                      role_given: int = None, role_duration: int = None):
    """Create a new shop item. role_duration (seconds) marks a temporary role; NULL = permanent."""
    return await conn.fetchrow(
        """INSERT INTO items (guild_id, name, price, description, item_type, role_given, role_duration)
           VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *""",
        guild_id, name, price, description, item_type, role_given, role_duration,
    )


async def delete_item(conn: Conn, guild_id: int, name: str):
    """Delete a shop item by name."""
    return await conn.fetchrow(
        "DELETE FROM items WHERE guild_id = $1 AND LOWER(name) = LOWER($2) RETURNING *",
        guild_id, name,
    )


async def get_item_by_name(conn: Conn, guild_id: int, name: str):
    """Get a shop item by name (case-insensitive)."""
    return await conn.fetchrow(
        "SELECT * FROM items WHERE guild_id = $1 AND LOWER(name) = LOWER($2) AND is_available = TRUE",
        guild_id, name,
    )


async def get_shop_items(conn: Conn, guild_id: int):
    """Get all available shop items for a guild."""
    return await conn.fetch(
        "SELECT * FROM items WHERE guild_id = $1 AND is_available = TRUE ORDER BY price ASC",
        guild_id,
    )


async def get_inventory(conn: Conn, guild_id: int, user_id: int):
    """Get a user's inventory."""
    return await conn.fetch(
        """SELECT i.name, i.description, i.item_type, inv.quantity
           FROM inventory inv JOIN items i ON inv.item_id = i.id
           WHERE inv.guild_id = $1 AND inv.user_id = $2 AND inv.quantity > 0
           ORDER BY i.name""",
        guild_id, user_id,
    )


async def add_to_inventory(conn: Conn, guild_id: int, user_id: int, item_id: int, quantity: int = 1):
    """Add an item to a user's inventory."""
    await conn.execute(
        """INSERT INTO inventory (guild_id, user_id, item_id, quantity)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (guild_id, user_id, item_id)
           DO UPDATE SET quantity = inventory.quantity + $4""",
        guild_id, user_id, item_id, quantity,
    )


async def grant_temp_role(conn: Conn, guild_id: int, user_id: int, role_id: int, duration_seconds: int):
    """Record a temporary role grant, extending the expiry if one already exists.

    Returns the new expires_at. GREATEST(existing, NOW()) keeps extend safe even if a
    prior grant already lapsed but its row hasn't been swept yet.
    """
    return await conn.fetchval(
        """INSERT INTO temporary_roles (guild_id, user_id, role_id, expires_at)
           VALUES ($1, $2, $3, NOW() + ($4 || ' seconds')::interval)
           ON CONFLICT (guild_id, user_id, role_id)
           DO UPDATE SET expires_at = GREATEST(temporary_roles.expires_at, NOW())
                                      + ($4 || ' seconds')::interval
           RETURNING expires_at""",
        guild_id, user_id, role_id, str(duration_seconds),
    )


async def get_expired_temp_roles(conn: Conn):
    """Get all temporary role grants that have expired."""
    return await conn.fetch(
        "SELECT id, guild_id, user_id, role_id FROM temporary_roles WHERE expires_at <= NOW()",
    )


async def delete_temp_role(conn: Conn, grant_id: int):
    """Delete a temporary role grant by its id."""
    await conn.execute("DELETE FROM temporary_roles WHERE id = $1", grant_id)


async def remove_member_data(conn: Conn, guild_id: int, user_id: int):
    """Delete a member's inventory and temp-role grants when they leave/are removed from the guild."""
    await conn.execute(
        "DELETE FROM inventory WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )
    await conn.execute(
        "DELETE FROM temporary_roles WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )
