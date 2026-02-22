import discord
import aiosqlite
from discord.ext import commands
from utils import is_guild_owner, check_channel_allowed, log_tx

DB_PATH = "data/economy.db"


def _can_manage_bets(ctx: commands.Context, bet_role_id: int | None) -> bool:
    """Return True if the invoker is the guild owner or has the designated bet role."""
    if ctx.author == ctx.guild.owner:
        return True
    if bet_role_id and any(r.id == bet_role_id for r in ctx.author.roles):
        return True
    return False


class Bets(commands.Cog):
    _owner_commands = {"setbetrole"}

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: aiosqlite.Connection = None

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.command.name in self._owner_commands:
            return True
        return await check_channel_allowed(
            self.db, ctx.guild.id, "bets", ctx.channel.id, ctx.command.name
        )

    async def cog_load(self):
        self.db = await aiosqlite.connect(DB_PATH)
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS bet_settings (
                guild_id    INTEGER PRIMARY KEY,
                bet_role_id INTEGER
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS bets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                channel_id  INTEGER NOT NULL,
                creator_id  INTEGER NOT NULL,
                statement   TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'open',
                winner_idx  INTEGER,
                message_id  INTEGER
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS bet_options (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id  INTEGER NOT NULL,
                label   TEXT NOT NULL
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS bet_entries (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id     INTEGER NOT NULL,
                option_id  INTEGER NOT NULL,
                user_id    INTEGER NOT NULL,
                amount     INTEGER NOT NULL,
                UNIQUE(bet_id, user_id)
            )"""
        )
        await self.db.commit()

    async def cog_unload(self):
        if self.db:
            await self.db.close()

    # ── Helpers ───────────────────────────────────────────────────

    async def _get_bet_role(self, guild_id: int) -> int | None:
        async with self.db.execute(
            "SELECT bet_role_id FROM bet_settings WHERE guild_id = ?", (guild_id,)
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else None

    async def _get_cash(self, user_id: int) -> int:
        async with self.db.execute(
            "SELECT cash FROM economy WHERE user_id = ?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0

    async def _build_embed(self, bet_id: int) -> discord.Embed | None:
        """Build the status embed for a bet."""
        async with self.db.execute(
            "SELECT statement, status, winner_idx FROM bets WHERE id = ?", (bet_id,)
        ) as cur:
            bet_row = await cur.fetchone()
        if not bet_row:
            return None
        statement, status, winner_idx = bet_row

        async with self.db.execute(
            "SELECT id, label FROM bet_options WHERE bet_id = ? ORDER BY id", (bet_id,)
        ) as cur:
            options = await cur.fetchall()

        # Gather totals per option
        totals: dict[int, int] = {opt_id: 0 for opt_id, _ in options}
        async with self.db.execute(
            "SELECT option_id, SUM(amount) FROM bet_entries WHERE bet_id = ? GROUP BY option_id",
            (bet_id,),
        ) as cur:
            for opt_id, total in await cur.fetchall():
                totals[opt_id] = total or 0

        grand_total = sum(totals.values())

        status_label = {"open": "🟢 Open", "locked": "🔒 Locked", "closed": "🔴 Closed"}.get(status, status)
        color = {"open": discord.Color.green(), "locked": discord.Color.orange(), "closed": discord.Color.red()}.get(status, discord.Color.default())
        embed = discord.Embed(title=f"Bet #{bet_id}", description=f"**{statement}**\n{status_label}", color=color)

        for i, (opt_id, label) in enumerate(options):
            opt_total = totals[opt_id]
            pct = f"{opt_total / grand_total * 100:.1f}%" if grand_total else "—"
            marker = " ✅" if (status == "closed" and winner_idx is not None and i == winner_idx) else ""
            embed.add_field(
                name=f"{i + 1}. {label}{marker}",
                value=f"{opt_total:,} 🌸 ({pct})",
                inline=True,
            )

        if status == "open":
            footer = f"Total pool: {grand_total:,} 🌸 | Use .bet {bet_id} <option#> <amount> to place a bet"
        elif status == "locked":
            footer = f"Total pool: {grand_total:,} 🌸 | Betting is closed — awaiting results"
        else:
            footer = f"Total pool: {grand_total:,} 🌸"
        embed.set_footer(text=footer)
        return embed

    # ── Commands ──────────────────────────────────────────────────

    @commands.command()
    @is_guild_owner()
    async def setbetrole(self, ctx: commands.Context, role: discord.Role = None):
        """Set (or clear) the role that can create/close bets. Owner only.
        Usage: .setbetrole @BetMaster   — grants role access
               .setbetrole              — clears the role (owner-only access)
        """
        role_id = role.id if role else None
        await self.db.execute(
            """INSERT INTO bet_settings (guild_id, bet_role_id) VALUES (?, ?)
               ON CONFLICT(guild_id) DO UPDATE SET bet_role_id = ?""",
            (ctx.guild.id, role_id, role_id),
        )
        await self.db.commit()
        if role:
            await ctx.send(f"Members with **{role.name}** can now create and close bets.")
        else:
            await ctx.send("Bet creation is now restricted to the server owner only.")

    @commands.command()
    async def createbet(self, ctx: commands.Context, *, args: str = None):
        """Create a new bet. Requires owner or bet role.
        Usage: .createbet "Statement" | Option A | Option B | Option C
        Example: .createbet "Will it rain today?" | Yes | No | Maybe
        """
        bet_role_id = await self._get_bet_role(ctx.guild.id)
        if not _can_manage_bets(ctx, bet_role_id):
            await ctx.send("You don't have permission to create bets.")
            return

        if not args:
            await ctx.send(
                f'Usage: `{ctx.prefix}createbet "Statement" | Option A | Option B | ...`\n'
                f'Example: `{ctx.prefix}createbet "Will it rain today?" | Yes | No`'
            )
            return

        parts = [p.strip() for p in args.split("|")]
        if len(parts) < 3:
            await ctx.send("You need a statement and at least 2 options, separated by `|`.")
            return

        statement = parts[0].strip('"').strip("'").strip()
        option_labels = parts[1:]
        if len(option_labels) > 9:
            await ctx.send("Maximum 9 options per bet.")
            return

        # Check for a bet already open in this channel
        async with self.db.execute(
            "SELECT id FROM bets WHERE guild_id = ? AND channel_id = ? AND status = 'open'",
            (ctx.guild.id, ctx.channel.id),
        ) as cur:
            existing = await cur.fetchone()
        if existing:
            await ctx.send(
                f"There's already an open bet in this channel (#{existing[0]}). "
                f"Close it first with `{ctx.prefix}closebet {existing[0]} <winner#>`."
            )
            return

        async with self.db.execute(
            "INSERT INTO bets (guild_id, channel_id, creator_id, statement) VALUES (?, ?, ?, ?)",
            (ctx.guild.id, ctx.channel.id, ctx.author.id, statement),
        ) as cur:
            bet_id = cur.lastrowid

        for label in option_labels:
            await self.db.execute(
                "INSERT INTO bet_options (bet_id, label) VALUES (?, ?)",
                (bet_id, label),
            )
        await self.db.commit()

        embed = await self._build_embed(bet_id)
        msg = await ctx.send(embed=embed)

        await self.db.execute(
            "UPDATE bets SET message_id = ? WHERE id = ?", (msg.id, bet_id)
        )
        await self.db.commit()

    @commands.command()
    async def bet(self, ctx: commands.Context, bet_id: int = None, option: int = None, amount: int = None):
        """Place a bet on an open bet.
        Usage: .bet <bet_id> <option#> <amount>
        Example: .bet 3 2 500
        """
        if bet_id is None or option is None or amount is None:
            await ctx.send(f"Usage: `{ctx.prefix}bet <bet_id> <option#> <amount>`")
            return

        if amount <= 0:
            await ctx.send("Bet amount must be positive.")
            return

        async with self.db.execute(
            "SELECT status, channel_id FROM bets WHERE id = ? AND guild_id = ?",
            (bet_id, ctx.guild.id),
        ) as cur:
            bet_row = await cur.fetchone()

        if not bet_row:
            await ctx.send(f"Bet #{bet_id} not found in this server.")
            return
        status, channel_id = bet_row
        if status == "locked":
            await ctx.send(f"Bet #{bet_id} is locked — betting is closed.")
            return
        if status != "open":
            await ctx.send(f"Bet #{bet_id} is no longer open.")
            return

        async with self.db.execute(
            "SELECT id, label FROM bet_options WHERE bet_id = ? ORDER BY id", (bet_id,)
        ) as cur:
            options = await cur.fetchall()

        if option < 1 or option > len(options):
            await ctx.send(f"Invalid option. Choose between 1 and {len(options)}.")
            return

        chosen_opt_id, chosen_label = options[option - 1]

        # Check if user already placed a bet on this bet_id
        async with self.db.execute(
            "SELECT id, option_id, amount FROM bet_entries WHERE bet_id = ? AND user_id = ?",
            (bet_id, ctx.author.id),
        ) as cur:
            existing_entry = await cur.fetchone()

        if existing_entry:
            await ctx.send(
                f"You've already placed a bet on bet #{bet_id}. "
                "You can only place one bet per event."
            )
            return

        cash = await self._get_cash(ctx.author.id)
        if amount > cash:
            await ctx.send(f"You only have **{cash:,}** 🌸.")
            return

        # Deduct cash
        await self.db.execute(
            "UPDATE economy SET cash = cash - ? WHERE user_id = ?",
            (amount, ctx.author.id),
        )
        await log_tx(self.db, ctx.author.id, -amount, f"bet:{bet_id}:entry")
        await self.db.execute(
            "INSERT INTO bet_entries (bet_id, option_id, user_id, amount) VALUES (?, ?, ?, ?)",
            (bet_id, chosen_opt_id, ctx.author.id, amount),
        )
        await self.db.commit()

        embed = await self._build_embed(bet_id)
        # Try to edit the original bet message
        async with self.db.execute(
            "SELECT message_id, channel_id FROM bets WHERE id = ?", (bet_id,)
        ) as cur:
            row = await cur.fetchone()
        if row and row[0]:
            try:
                ch = ctx.guild.get_channel(row[1])
                msg = await ch.fetch_message(row[0])
                await msg.edit(embed=embed)
            except Exception:
                pass

        await ctx.send(
            f"{ctx.author.mention} placed **{amount:,}** 🌸 on **{chosen_label}** (bet #{bet_id})."
        )

    @commands.command()
    async def lockbet(self, ctx: commands.Context, bet_id: int = None):
        """Lock a bet so no more bets can be placed. Requires owner or bet role.
        Usage: .lockbet <bet_id>
        """
        bet_role_id = await self._get_bet_role(ctx.guild.id)
        if not _can_manage_bets(ctx, bet_role_id):
            await ctx.send("You don't have permission to lock bets.")
            return

        if bet_id is None:
            await ctx.send(f"Usage: `{ctx.prefix}lockbet <bet_id>`")
            return

        async with self.db.execute(
            "SELECT status FROM bets WHERE id = ? AND guild_id = ?",
            (bet_id, ctx.guild.id),
        ) as cur:
            bet_row = await cur.fetchone()

        if not bet_row:
            await ctx.send(f"Bet #{bet_id} not found in this server.")
            return
        if bet_row[0] != "open":
            await ctx.send(f"Bet #{bet_id} is already {bet_row[0]}.")
            return

        await self.db.execute(
            "UPDATE bets SET status = 'locked' WHERE id = ?", (bet_id,)
        )
        await self.db.commit()

        embed = await self._build_embed(bet_id)

        # Try to edit the original bet message
        async with self.db.execute(
            "SELECT message_id, channel_id FROM bets WHERE id = ?", (bet_id,)
        ) as cur:
            row = await cur.fetchone()
        if row and row[0]:
            try:
                ch = ctx.guild.get_channel(row[1])
                msg = await ch.fetch_message(row[0])
                await msg.edit(embed=embed)
            except Exception:
                pass

        await ctx.send(f"Bet #{bet_id} is now locked — no more bets can be placed.")

    @commands.command()
    async def closebet(self, ctx: commands.Context, bet_id: int = None, winner: int = None):
        """Close a bet and pay out winners. Requires owner or bet role.
        Usage: .closebet <bet_id> <winning_option#>
        """
        bet_role_id = await self._get_bet_role(ctx.guild.id)
        if not _can_manage_bets(ctx, bet_role_id):
            await ctx.send("You don't have permission to close bets.")
            return

        if bet_id is None or winner is None:
            await ctx.send(f"Usage: `{ctx.prefix}closebet <bet_id> <winning_option#>`")
            return

        async with self.db.execute(
            "SELECT status, statement FROM bets WHERE id = ? AND guild_id = ?",
            (bet_id, ctx.guild.id),
        ) as cur:
            bet_row = await cur.fetchone()

        if not bet_row:
            await ctx.send(f"Bet #{bet_id} not found in this server.")
            return
        status, statement = bet_row
        if status == "closed":
            await ctx.send(f"Bet #{bet_id} is already closed.")
            return

        async with self.db.execute(
            "SELECT id, label FROM bet_options WHERE bet_id = ? ORDER BY id", (bet_id,)
        ) as cur:
            options = await cur.fetchall()

        if winner < 1 or winner > len(options):
            await ctx.send(f"Invalid option. Choose between 1 and {len(options)}.")
            return

        winner_idx = winner - 1
        winning_opt_id, winning_label = options[winner_idx]

        # Fetch all entries
        async with self.db.execute(
            "SELECT user_id, option_id, amount FROM bet_entries WHERE bet_id = ?",
            (bet_id,),
        ) as cur:
            entries = await cur.fetchall()

        total_pool = sum(e[2] for e in entries)
        winning_pool = sum(e[2] for e in entries if e[1] == winning_opt_id)
        losers_pool = total_pool - winning_pool

        # Mark bet closed
        await self.db.execute(
            "UPDATE bets SET status = 'closed', winner_idx = ? WHERE id = ?",
            (winner_idx, bet_id),
        )

        # Pay out winners proportionally
        # Each winner gets their stake back + proportional share of loser pool
        payouts: list[tuple[int, int]] = []
        for user_id, opt_id, amount in entries:
            if opt_id == winning_opt_id and winning_pool > 0:
                share = int(losers_pool * amount / winning_pool)
                payout = amount + share
                payouts.append((user_id, payout))

        for user_id, payout in payouts:
            await self.db.execute(
                "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
                (payout, user_id),
            )
            await log_tx(self.db, user_id, payout, f"bet:{bet_id}:payout")

        await self.db.commit()

        # Build result embed
        embed = await self._build_embed(bet_id)

        result_lines = []
        if payouts:
            for user_id, payout in sorted(payouts, key=lambda x: -x[1]):
                member = ctx.guild.get_member(user_id)
                name = member.display_name if member else f"<{user_id}>"
                result_lines.append(f"• {name} → **{payout:,}** 🌸")
        else:
            result_lines.append("No winners — all bets refunded to pool (no winning entries).")
            # Refund everyone if no winners
            for user_id, opt_id, amount in entries:
                await self.db.execute(
                    "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
                    (amount, user_id),
                )
                await log_tx(self.db, user_id, amount, f"bet:{bet_id}:refund")
            await self.db.commit()

        embed.add_field(
            name="Payouts",
            value="\n".join(result_lines) if result_lines else "—",
            inline=False,
        )

        # Try to edit the original message
        async with self.db.execute(
            "SELECT message_id, channel_id FROM bets WHERE id = ?", (bet_id,)
        ) as cur:
            row = await cur.fetchone()
        if row and row[0]:
            try:
                ch = ctx.guild.get_channel(row[1])
                msg = await ch.fetch_message(row[0])
                await msg.edit(embed=embed)
            except Exception:
                pass

        await ctx.send(embed=embed)

    @commands.command()
    async def cancelbet(self, ctx: commands.Context, bet_id: int = None):
        """Cancel an open bet and refund all entries. Requires owner or bet role.
        Usage: .cancelbet <bet_id>
        """
        bet_role_id = await self._get_bet_role(ctx.guild.id)
        if not _can_manage_bets(ctx, bet_role_id):
            await ctx.send("You don't have permission to cancel bets.")
            return

        if bet_id is None:
            await ctx.send(f"Usage: `{ctx.prefix}cancelbet <bet_id>`")
            return

        async with self.db.execute(
            "SELECT status FROM bets WHERE id = ? AND guild_id = ?",
            (bet_id, ctx.guild.id),
        ) as cur:
            bet_row = await cur.fetchone()

        if not bet_row:
            await ctx.send(f"Bet #{bet_id} not found in this server.")
            return
        if bet_row[0] == "closed":
            await ctx.send(f"Bet #{bet_id} is already closed.")
            return

        async with self.db.execute(
            "SELECT user_id, amount FROM bet_entries WHERE bet_id = ?", (bet_id,)
        ) as cur:
            entries = await cur.fetchall()

        for user_id, amount in entries:
            await self.db.execute(
                "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
                (amount, user_id),
            )
            await log_tx(self.db, user_id, amount, f"bet:{bet_id}:cancel_refund")

        await self.db.execute(
            "UPDATE bets SET status = 'closed' WHERE id = ?", (bet_id,)
        )
        await self.db.commit()

        refund_count = len(entries)
        total_refunded = sum(a for _, a in entries)
        await ctx.send(
            f"Bet #{bet_id} cancelled. Refunded **{total_refunded:,}** 🌸 to {refund_count} participant(s)."
        )

    @commands.command()
    async def viewbet(self, ctx: commands.Context, bet_id: int = None):
        """View the current status of a bet.
        Usage: .viewbet <bet_id>
        """
        if bet_id is None:
            await ctx.send(f"Usage: `{ctx.prefix}viewbet <bet_id>`")
            return

        async with self.db.execute(
            "SELECT id FROM bets WHERE id = ? AND guild_id = ?",
            (bet_id, ctx.guild.id),
        ) as cur:
            if not await cur.fetchone():
                await ctx.send(f"Bet #{bet_id} not found in this server.")
                return

        embed = await self._build_embed(bet_id)
        await ctx.send(embed=embed)

    @commands.command()
    async def openbets(self, ctx: commands.Context):
        """List all open bets in this server."""
        async with self.db.execute(
            "SELECT id, statement, channel_id FROM bets WHERE guild_id = ? AND status = 'open' ORDER BY id DESC",
            (ctx.guild.id,),
        ) as cur:
            rows = await cur.fetchall()

        if not rows:
            await ctx.send("No open bets in this server.")
            return

        embed = discord.Embed(title="Open Bets", color=discord.Color.green())
        for bet_id, statement, channel_id in rows:
            embed.add_field(
                name=f"#{bet_id} — {statement}",
                value=f"In <#{channel_id}> | `.bet {bet_id} <option#> <amount>`",
                inline=False,
            )
        await ctx.send(embed=embed)

    # ── Error Handler ─────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return

        if isinstance(error, commands.CheckFailure):
            if str(error) == "channel_restricted":
                return
            await ctx.send("Only the server owner can use this command.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Check `{ctx.prefix}help {ctx.command}` for usage.")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(Bets(bot))
