"""Aggregate schema initialization across all features.

Each feature owns its own `schema.py` exporting:
- `SCHEMA`: a SQL string with CREATE TABLE statements
- `MIGRATIONS` (optional): list of SQL strings for idempotent ALTER/UPDATE statements
- `CONSTRAINTS` (optional): list of ALTER TABLE ... ADD CONSTRAINT statements that
  may already exist; DuplicateObjectError on each is swallowed
"""

import asyncpg

from core import schema as core_schema
from cogs.economy import schema as economy_schema
from cogs.market import schema as market_schema
from cogs.shop import schema as shop_schema
from cogs.predictions import schema as predictions_schema
from cogs.offers import schema as offers_schema
from cogs.reminders import schema as reminders_schema
from cogs.waifu import schema as waifu_schema
from cogs.reaction_roles import schema as reaction_roles_schema
from cogs.counting import schema as counting_schema
from cogs.lichess import schema as lichess_schema


# Order matters: economy + market must run before features that reference balances/companies.
# Features without FKs can run in any order after that.
_FEATURE_MODULES = [
    core_schema,
    economy_schema,
    market_schema,
    shop_schema,
    predictions_schema,
    offers_schema,
    reminders_schema,
    waifu_schema,
    reaction_roles_schema,
    counting_schema,
    lichess_schema,
]


async def init_db(pool: asyncpg.Pool) -> None:
    for module in _FEATURE_MODULES:
        if hasattr(module, "SCHEMA"):
            await pool.execute(module.SCHEMA)

    for module in _FEATURE_MODULES:
        for stmt in getattr(module, "MIGRATIONS", []):
            await pool.execute(stmt)

    for module in _FEATURE_MODULES:
        for stmt in getattr(module, "CONSTRAINTS", []):
            try:
                await pool.execute(stmt)
            except asyncpg.DuplicateObjectError:
                pass
