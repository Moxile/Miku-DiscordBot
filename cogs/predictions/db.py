from core.db import Conn


async def create_prediction(conn: Conn, guild_id: int, creator_id: int, question: str,
                             options: list[str]):
    """Create a prediction with options. Returns (prediction, options_list)."""
    pred = await conn.fetchrow(
        "INSERT INTO predictions (guild_id, creator_id, question) VALUES ($1, $2, $3) RETURNING *",
        guild_id, creator_id, question,
    )
    opt_rows = []
    for i, label in enumerate(options, 1):
        row = await conn.fetchrow(
            "INSERT INTO prediction_options (prediction_id, label, option_index) VALUES ($1, $2, $3) RETURNING *",
            pred["id"], label.strip(), i,
        )
        opt_rows.append(row)
    return pred, opt_rows


async def get_prediction(conn: Conn, prediction_id: int):
    return await conn.fetchrow("SELECT * FROM predictions WHERE id = $1", prediction_id)


async def get_prediction_options(conn: Conn, prediction_id: int):
    return await conn.fetch(
        "SELECT * FROM prediction_options WHERE prediction_id = $1 ORDER BY option_index",
        prediction_id,
    )


async def get_prediction_bets(conn: Conn, prediction_id: int):
    return await conn.fetch(
        "SELECT * FROM prediction_bets WHERE prediction_id = $1",
        prediction_id,
    )


async def get_option_totals(conn: Conn, prediction_id: int):
    """Get total bet amount per option."""
    return await conn.fetch(
        """SELECT option_id, COALESCE(SUM(amount), 0) AS total
           FROM prediction_bets WHERE prediction_id = $1
           GROUP BY option_id""",
        prediction_id,
    )


async def place_prediction_bet(conn: Conn, prediction_id: int, option_id: int,
                                guild_id: int, user_id: int, amount: int):
    return await conn.fetchrow(
        """INSERT INTO prediction_bets (prediction_id, option_id, guild_id, user_id, amount)
           VALUES ($1, $2, $3, $4, $5) RETURNING *""",
        prediction_id, option_id, guild_id, user_id, amount,
    )


async def close_prediction(conn: Conn, prediction_id: int):
    await conn.execute(
        "UPDATE predictions SET status = 'closed' WHERE id = $1", prediction_id,
    )


async def resolve_prediction(conn: Conn, prediction_id: int, winner_option_id: int):
    await conn.execute(
        "UPDATE predictions SET status = 'resolved', winner_option_id = $2 WHERE id = $1",
        prediction_id, winner_option_id,
    )


async def get_winning_bets(conn: Conn, prediction_id: int, option_id: int):
    return await conn.fetch(
        "SELECT * FROM prediction_bets WHERE prediction_id = $1 AND option_id = $2",
        prediction_id, option_id,
    )


async def get_active_predictions(conn: Conn, guild_id: int):
    return await conn.fetch(
        "SELECT * FROM predictions WHERE guild_id = $1 AND status IN ('open', 'closed') ORDER BY id",
        guild_id,
    )


async def remove_member_data(conn: Conn, guild_id: int, user_id: int):
    """Delete a member's prediction bets when they leave/are removed from the guild.

    Predictions they created (predictions.creator_id) are left intact since other members
    may have bet on them.
    """
    await conn.execute(
        "DELETE FROM prediction_bets WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )
