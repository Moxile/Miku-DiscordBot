import asyncpg

# Type alias: pool or connection (both support execute/fetch/fetchrow)
Conn = asyncpg.Pool | asyncpg.Connection


async def ensure_wallet(conn: Conn, guild_id: int, user_id: int) -> asyncpg.Record:
    """Returns and ensures a user has a wallet (Creates if not exists)"""
    await conn.execute(
        "INSERT INTO balances (guild_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        guild_id, user_id,
    )
    return await conn.fetchrow(
        "SELECT * FROM balances WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def update_wallet(conn: Conn, guild_id: int, user_id: int, amount: int) -> int:
    """Changes users wallet balance by a certain amount and returns the new balance"""
    row = await conn.fetchrow(
        "UPDATE balances SET wallet = wallet + $3 WHERE guild_id = $1 AND user_id = $2 RETURNING wallet",
        guild_id, user_id, amount,
    )
    return row["wallet"]

async def update_bank(conn: Conn, guild_id: int, user_id: int, amount: int) -> int:
    """Changes users bank balance by a certain amount and returns the new balance"""
    row = await conn.fetchrow(
        "UPDATE balances SET bank = bank + $3 WHERE guild_id = $1 AND user_id = $2 RETURNING bank",
        guild_id, user_id, amount,
    )
    return row["bank"]


async def add_transaction(conn: Conn, guild_id: int, user_id: int, amount: int, tx_type: str, description: str = None):
    """Log a transaction"""
    await conn.execute(
        "INSERT INTO transactions (guild_id, user_id, amount, tx_type, description) VALUES ($1, $2, $3, $4, $5)",
        guild_id, user_id, amount, tx_type, description,
    )



async def lock_wallet(conn: asyncpg.Connection, guild_id: int, user_id: int) -> asyncpg.Record:
    """Lock and return the user's balance row. Must be called within a transaction."""
    return await conn.fetchrow(
        "SELECT * FROM balances WHERE guild_id = $1 AND user_id = $2 FOR UPDATE",
        guild_id, user_id,
    )

async def lock_company(conn: asyncpg.Connection, guild_id: int, stock_channel_id: int) -> asyncpg.Record:
    """Lock and return the company row (for IPO share updates). Must be called within a transaction."""
    return await conn.fetchrow(
        "SELECT * FROM companies WHERE guild_id = $1 AND stock_channel_id = $2 FOR UPDATE",
        guild_id, stock_channel_id,
    )



async def get_company(conn: Conn, guild_id: int, stock_channel_id: int):
    return await conn.fetchrow(
        "SELECT * FROM companies WHERE guild_id = $1 AND stock_channel_id = $2",
        guild_id, stock_channel_id,
    )

async def list_companies(conn: Conn, guild_id: int):
    return await conn.fetch(
        "SELECT * FROM companies WHERE guild_id = $1 ORDER BY listed_at",
        guild_id,
    )

async def create_company(conn: Conn, guild_id: int, stock_channel_id: int, name: str, listed_by: int,
                          total_shares: int = 100, ipo_price: int = 100):
    await conn.execute(
        """INSERT INTO companies (guild_id, stock_channel_id, name, total_shares, available_ipo_shares, ipo_price, listed_by)
           VALUES ($1, $2, $3, $4, $4, $5, $6)""",
        guild_id, stock_channel_id, name, total_shares, ipo_price, listed_by,
    )

async def delete_company(conn: Conn, guild_id: int, stock_channel_id: int):
    """Delete a company and all related data (cascades to portfolios, orders, trades, etc.)."""
    return await conn.fetchrow(
        "DELETE FROM companies WHERE guild_id = $1 AND stock_channel_id = $2 RETURNING name",
        guild_id, stock_channel_id,
    )


async def get_portfolio(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetch(
        "SELECT * FROM portfolios WHERE guild_id = $1 AND user_id = $2 AND quantity > 0",
        guild_id, user_id,
    )

async def get_holding(conn: Conn, guild_id: int, user_id: int, stock_channel_id: int):
    row = await conn.fetchrow(
        "SELECT quantity FROM portfolios WHERE guild_id = $1 AND user_id = $2 AND stock_channel_id = $3",
        guild_id, user_id, stock_channel_id,
    )
    return row["quantity"] if row else 0

async def update_holding(conn: Conn, guild_id: int, user_id: int, stock_channel_id: int, quantity_change: int):
    await conn.execute(
        """INSERT INTO portfolios (guild_id, user_id, stock_channel_id, quantity)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (guild_id, user_id, stock_channel_id)
           DO UPDATE SET quantity = portfolios.quantity + $4""",
        guild_id, user_id, stock_channel_id, quantity_change,
    )

async def get_open_orders(conn: Conn, guild_id: int, stock_channel_id: int, side: str = None):
    if side:
        order = "ASC" if side == "sell" else "DESC"
        return await conn.fetch(
            f"SELECT * FROM orders WHERE guild_id = $1 AND stock_channel_id = $2 AND side = $3 AND remaining > 0 ORDER BY price {order}, created_at ASC",
            guild_id, stock_channel_id, side,
        )
    return await conn.fetch(
        "SELECT * FROM orders WHERE guild_id = $1 AND stock_channel_id = $2 AND remaining > 0 ORDER BY price DESC, created_at ASC",
        guild_id, stock_channel_id,
    )

async def get_open_orders_locked(conn: asyncpg.Connection, guild_id: int, stock_channel_id: int, side: str):
    """Get open orders with FOR UPDATE lock. Must be called within a transaction."""
    order = "ASC" if side == "sell" else "DESC"
    return await conn.fetch(
        f"SELECT * FROM orders WHERE guild_id = $1 AND stock_channel_id = $2 AND side = $3 AND remaining > 0 ORDER BY price {order}, created_at ASC FOR UPDATE",
        guild_id, stock_channel_id, side,
    )

async def get_user_orders(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetch(
        "SELECT * FROM orders WHERE guild_id = $1 AND user_id = $2 AND remaining > 0 ORDER BY created_at DESC",
        guild_id, user_id,
    )

async def get_escrowed_shares(conn: Conn, guild_id: int, user_id: int, stock_channel_id: int):
    """Returns the total shares locked in open sell orders for a user on a stock."""
    row = await conn.fetchrow(
        "SELECT COALESCE(SUM(remaining), 0) AS total FROM orders "
        "WHERE guild_id = $1 AND user_id = $2 AND stock_channel_id = $3 AND side = 'sell' AND remaining > 0",
        guild_id, user_id, stock_channel_id,
    )
    return row["total"]

async def create_order(conn: Conn, guild_id: int, stock_channel_id: int, user_id: int, side: str, quantity: int, price: int):
    return await conn.fetchrow(
        """INSERT INTO orders (guild_id, stock_channel_id, user_id, side, quantity, remaining, price)
           VALUES ($1, $2, $3, $4, $5, $5, $6) RETURNING id""",
        guild_id, stock_channel_id, user_id, side, quantity, price,
    )

async def cancel_order(conn: Conn, guild_id: int, order_id: int, user_id: int):
    return await conn.fetchrow(
        "DELETE FROM orders WHERE id = $1 AND guild_id = $2 AND user_id = $3 AND remaining > 0 RETURNING *",
        order_id, guild_id, user_id,
    )

async def add_trade(conn: Conn, guild_id: int, stock_channel_id: int, buyer_id: int, seller_id: int,
                     quantity: int, price: int, trade_type: str = "market"):
    await conn.execute(
        """INSERT INTO trade_history (guild_id, stock_channel_id, buyer_id, seller_id, quantity, price, trade_type)
           VALUES ($1, $2, $3, $4, $5, $6, $7)""",
        guild_id, stock_channel_id, buyer_id, seller_id, quantity, price, trade_type,
    )

async def get_last_trade_price(conn: Conn, guild_id: int, stock_channel_id: int):
    row = await conn.fetchrow(
        "SELECT price FROM trade_history WHERE guild_id = $1 AND stock_channel_id = $2 ORDER BY traded_at DESC LIMIT 1",
        guild_id, stock_channel_id,
    )
    return row["price"] if row else None


# ── Revenue / Treasury helpers ──

async def upsert_char_count(conn: Conn, guild_id: int, stock_channel_id: int,
                            user_id: int, activity_date, char_count: int):
    """Increment character count for a user in a company channel for a given date."""
    await conn.execute(
        """INSERT INTO channel_activity (guild_id, stock_channel_id, user_id, activity_date, char_count)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (guild_id, stock_channel_id, user_id, activity_date)
           DO UPDATE SET char_count = channel_activity.char_count + $5""",
        guild_id, stock_channel_id, user_id, activity_date, char_count,
    )

async def compute_daily_revenue(conn: Conn, guild_id: int, stock_channel_id: int,
                                 activity_date, revenue_multiplier: int) -> int:
    """Compute daily revenue from char counts and store it. Returns the computed revenue."""
    row = await conn.fetchrow(
        """SELECT COALESCE(SUM(SQRT(SQRT(char_count))), 0) AS raw_sum
           FROM channel_activity
           WHERE guild_id = $1 AND stock_channel_id = $2 AND activity_date = $3""",
        guild_id, stock_channel_id, activity_date,
    )
    revenue = int(row["raw_sum"] ** 0.75 * revenue_multiplier)
    await conn.execute(
        """INSERT INTO company_revenue (guild_id, stock_channel_id, revenue_date, revenue)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (guild_id, stock_channel_id, revenue_date)
           DO UPDATE SET revenue = $4""",
        guild_id, stock_channel_id, activity_date, revenue,
    )
    return revenue

async def get_weekly_revenue(conn: Conn, guild_id: int, stock_channel_id: int,
                              week_start, week_end) -> list:
    """Get daily revenue records for a date range (inclusive)."""
    return await conn.fetch(
        """SELECT revenue_date, revenue FROM company_revenue
           WHERE guild_id = $1 AND stock_channel_id = $2
             AND revenue_date >= $3 AND revenue_date <= $4
           ORDER BY revenue_date""",
        guild_id, stock_channel_id, week_start, week_end,
    )

async def get_weekly_revenue_total(conn: Conn, guild_id: int, stock_channel_id: int,
                                    week_start, week_end) -> int:
    """Sum of revenue for a date range."""
    row = await conn.fetchrow(
        """SELECT COALESCE(SUM(revenue), 0) AS total FROM company_revenue
           WHERE guild_id = $1 AND stock_channel_id = $2
             AND revenue_date >= $3 AND revenue_date <= $4""",
        guild_id, stock_channel_id, week_start, week_end,
    )
    return row["total"]

async def update_treasury(conn: Conn, guild_id: int, stock_channel_id: int, amount: int) -> int:
    """Change company treasury by amount. Returns new treasury value."""
    row = await conn.fetchrow(
        """UPDATE companies SET treasury = treasury + $3
           WHERE guild_id = $1 AND stock_channel_id = $2
           RETURNING treasury""",
        guild_id, stock_channel_id, amount,
    )
    return row["treasury"]

async def set_company_level(conn: Conn, guild_id: int, stock_channel_id: int,
                             level: int, new_multiplier: int, treasury_cost: int):
    """Level up a company: deduct treasury, set new level and multiplier."""
    await conn.execute(
        """UPDATE companies
           SET company_level = $3, revenue_multiplier = $4, treasury = treasury - $5
           WHERE guild_id = $1 AND stock_channel_id = $2""",
        guild_id, stock_channel_id, level, new_multiplier, treasury_cost,
    )

async def get_shareholders(conn: Conn, guild_id: int, stock_channel_id: int):
    """Get all shareholders with quantity > 0 for a company."""
    return await conn.fetch(
        """SELECT user_id, quantity FROM portfolios
           WHERE guild_id = $1 AND stock_channel_id = $2 AND quantity > 0""",
        guild_id, stock_channel_id,
    )

async def create_item(conn: Conn, guild_id: int, name: str, price: int,
                      description: str = None, item_type: str = "item",
                      role_given: int = None):
    """Create a new shop item."""
    return await conn.fetchrow(
        """INSERT INTO items (guild_id, name, price, description, item_type, role_given)
           VALUES ($1, $2, $3, $4, $5, $6) RETURNING *""",
        guild_id, name, price, description, item_type, role_given,
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


async def get_avg_buy_price(conn: Conn, guild_id: int, user_id: int, stock_channel_id: int):
    """Compute average buy price for a user's stock from trade history."""
    row = await conn.fetchrow(
        """SELECT COALESCE(SUM(quantity * price), 0) AS total_cost,
                  COALESCE(SUM(quantity), 0) AS total_qty
           FROM trade_history
           WHERE guild_id = $1 AND buyer_id = $2 AND stock_channel_id = $3""",
        guild_id, user_id, stock_channel_id,
    )
    if row["total_qty"] == 0:
        return 0
    return row["total_cost"] // row["total_qty"]


# ── Predictions ──

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


# ── Offers (bookmaker-style bets) ──

async def create_offer(conn: Conn, guild_id: int, channel_id: int, host_id: int,
                        description: str, odds: float, min_stake: int, max_stake: int,
                        pool: int):
    return await conn.fetchrow(
        """INSERT INTO offers (guild_id, channel_id, host_id, description, odds,
                                min_stake, max_stake, pool, pool_remaining)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8) RETURNING *""",
        guild_id, channel_id, host_id, description, odds, min_stake, max_stake, pool,
    )


async def get_offer(conn: Conn, offer_id: int):
    return await conn.fetchrow("SELECT * FROM offers WHERE id = $1", offer_id)


async def lock_offer(conn: asyncpg.Connection, offer_id: int):
    """Row-lock the offer for a transaction."""
    return await conn.fetchrow("SELECT * FROM offers WHERE id = $1 FOR UPDATE", offer_id)


async def get_active_offers(conn: Conn, guild_id: int):
    return await conn.fetch(
        "SELECT * FROM offers WHERE guild_id = $1 AND status = 'open' ORDER BY id",
        guild_id,
    )


async def get_offer_takes(conn: Conn, offer_id: int):
    return await conn.fetch(
        "SELECT * FROM offer_takes WHERE offer_id = $1 ORDER BY placed_at",
        offer_id,
    )


async def add_offer_take(conn: Conn, offer_id: int, user_id: int, stake: int, liability: int):
    return await conn.fetchrow(
        """INSERT INTO offer_takes (offer_id, user_id, stake, liability)
           VALUES ($1, $2, $3, $4) RETURNING *""",
        offer_id, user_id, stake, liability,
    )


async def decrement_offer_pool(conn: Conn, offer_id: int, amount: int):
    return await conn.fetchrow(
        """UPDATE offers SET pool_remaining = pool_remaining - $2
           WHERE id = $1 RETURNING pool_remaining""",
        offer_id, amount,
    )


async def set_offer_status(conn: Conn, offer_id: int, status: str):
    await conn.execute(
        "UPDATE offers SET status = $2, closed_at = NOW() WHERE id = $1",
        offer_id, status,
    )


# ── Reminders ──

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


# ── Waifu ──

async def ensure_waifu(conn: Conn, guild_id: int, user_id: int) -> asyncpg.Record:
    await conn.execute(
        "INSERT INTO waifus (guild_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        guild_id, user_id,
    )
    return await conn.fetchrow(
        "SELECT * FROM waifus WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def get_waifu(conn: Conn, guild_id: int, user_id: int) -> asyncpg.Record:
    return await conn.fetchrow(
        "SELECT * FROM waifus WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def get_harem(conn: Conn, guild_id: int, owner_id: int) -> list:
    return await conn.fetch(
        "SELECT * FROM waifus WHERE guild_id = $1 AND owner_id = $2 ORDER BY value DESC",
        guild_id, owner_id,
    )


async def set_waifu_owner(conn: Conn, guild_id: int, user_id: int,
                           new_owner_id: int, new_value: int):
    await conn.execute(
        """UPDATE waifus SET owner_id = $3, value = $4, last_bought_at = NOW()
           WHERE guild_id = $1 AND user_id = $2""",
        guild_id, user_id, new_owner_id, new_value,
    )


async def set_engagement(conn: Conn, guild_id: int, user_id: int):
    """Set engaged_since to NOW() if not already set."""
    await conn.execute(
        """UPDATE waifus SET engaged_since = NOW()
           WHERE guild_id = $1 AND user_id = $2 AND engaged_since IS NULL""",
        guild_id, user_id,
    )


async def set_marriage(conn: Conn, guild_id: int, user_a: int, user_b: int):
    """Marry two users: set spouse_id on both."""
    await conn.execute(
        "UPDATE waifus SET spouse_id = $3 WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_a, user_b,
    )
    await conn.execute(
        "UPDATE waifus SET spouse_id = $3 WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_b, user_a,
    )


async def dissolve_marriage(conn: Conn, guild_id: int, user_a: int, user_b: int):
    """Divorce: clear spouse_id and engaged_since on both."""
    for uid in (user_a, user_b):
        await conn.execute(
            "UPDATE waifus SET spouse_id = NULL, engaged_since = NULL WHERE guild_id = $1 AND user_id = $2",
            guild_id, uid,
        )


# ── Reaction Roles ──

async def add_reaction_role(conn: Conn, guild_id: int, channel_id: int, message_id: int,
                             emoji: str, is_custom: bool, role_id: int, created_by: int) -> asyncpg.Record:
    return await conn.fetchrow(
        """INSERT INTO reaction_roles (guild_id, channel_id, message_id, emoji, is_custom, role_id, created_by)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (guild_id, message_id, emoji) DO NOTHING
           RETURNING *""",
        guild_id, channel_id, message_id, emoji, is_custom, role_id, created_by,
    )


async def get_reaction_role(conn: Conn, guild_id: int, message_id: int, emoji: str) -> asyncpg.Record:
    return await conn.fetchrow(
        """SELECT * FROM reaction_roles
           WHERE guild_id = $1 AND message_id = $2 AND emoji = $3""",
        guild_id, message_id, emoji,
    )


async def remove_reaction_role(conn: Conn, guild_id: int, message_id: int, emoji: str) -> asyncpg.Record:
    return await conn.fetchrow(
        """DELETE FROM reaction_roles
           WHERE guild_id = $1 AND message_id = $2 AND emoji = $3
           RETURNING *""",
        guild_id, message_id, emoji,
    )


async def list_reaction_roles_for_message(conn: Conn, guild_id: int, message_id: int) -> list:
    return await conn.fetch(
        """SELECT * FROM reaction_roles
           WHERE guild_id = $1 AND message_id = $2
           ORDER BY created_at""",
        guild_id, message_id,
    )


async def list_reaction_roles_for_guild(conn: Conn, guild_id: int) -> list:
    return await conn.fetch(
        """SELECT * FROM reaction_roles
           WHERE guild_id = $1
           ORDER BY message_id, created_at""",
        guild_id,
    )


async def clear_reaction_roles_for_message(conn: Conn, guild_id: int, message_id: int) -> int:
    result = await conn.execute(
        "DELETE FROM reaction_roles WHERE guild_id = $1 AND message_id = $2",
        guild_id, message_id,
    )
    # result is like "DELETE <n>"
    try:
        return int(result.split()[-1])
    except (ValueError, IndexError):
        return 0


async def delete_reaction_roles_for_role(conn: Conn, guild_id: int, role_id: int):
    await conn.execute(
        "DELETE FROM reaction_roles WHERE guild_id = $1 AND role_id = $2",
        guild_id, role_id,
    )


async def decay_waifu_values(conn: Conn, base_value: int, decay_rate: float):
    """Decay all waifu values above base toward base by decay_rate percent of the excess."""
    await conn.execute(
        """UPDATE waifus
           SET value = GREATEST($1, value - FLOOR((value - $1) * $2)::BIGINT)
           WHERE value > $1
             AND last_bought_at IS NOT NULL
             AND last_bought_at < NOW() - INTERVAL '24 hours'""",
        base_value, decay_rate,
    )
