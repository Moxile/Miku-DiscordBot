from typing import Union
import asyncpg

# Type alias: pool or connection (both support execute/fetch/fetchrow)
Conn = Union[asyncpg.Pool, asyncpg.Connection]
