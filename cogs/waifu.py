import datetime
import discord
import aiosqlite
from discord.ext import commands, tasks
from utils import is_guild_owner, check_channel_allowed

DB_PATH = "data/economy.db"

# ── Default settings ─────────────────────────────────────────────────
DEFAULT_BASE_VALUE = 500
DEFAULT_CLAIM_INCREASE_PCT = 0.25
DEFAULT_DAILY_DECAY_PCT = 0.05
DEFAULT_MARRIAGE_FEE_MULTIPLIER = 1.5
DEFAULT_GIFT_THRESHOLD_PCT = 0.02
DEFAULT_AFFINITY_DISCOUNT = 0.50
DEFAULT_STEAL_MARRIED_MULTIPLIER = 2.0

SETTINGS_KEYS = [
    "base_value", "claim_increase_pct", "daily_decay_pct",
    "marriage_fee_multiplier", "gift_threshold_pct",
    "affinity_discount", "steal_married_multiplier",
]

SETTINGS_DEFAULTS = {
    "base_value": DEFAULT_BASE_VALUE,
    "claim_increase_pct": DEFAULT_CLAIM_INCREASE_PCT,
    "daily_decay_pct": DEFAULT_DAILY_DECAY_PCT,
    "marriage_fee_multiplier": DEFAULT_MARRIAGE_FEE_MULTIPLIER,
    "gift_threshold_pct": DEFAULT_GIFT_THRESHOLD_PCT,
    "affinity_discount": DEFAULT_AFFINITY_DISCOUNT,
    "steal_married_multiplier": DEFAULT_STEAL_MARRIED_MULTIPLIER,
}

# ── Bond constants ───────────────────────────────────────────────────
BOND_MAX = 10.0
BOND_DECAY_FACTOR = 0.1   # daily decay = level * 0.1
BOND_GAIN_FACTOR = 0.5    # base gain per ratio unit
BOND_MAX_DAILY_GAIN = 1.5 # cap on bond gain per day

PROFILE_COLS = [
    "guild_id", "user_id", "value", "claimed_by", "affinity_for",
    "last_affinity_set", "last_claim_time", "engaged_to", "engaged_since",
    "married_to", "married_since", "bond_strength", "daily_gift_value",
    "last_decay_applied",
]


class Waifu(commands.Cog):
    _owner_commands = {"waifuset"}

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: aiosqlite.Connection = None

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.command.name in self._owner_commands:
            return True
        return await check_channel_allowed(
            self.db, ctx.guild.id, "waifu", ctx.channel.id, ctx.command.name
        )

    async def cog_load(self):
        self.db = await aiosqlite.connect(DB_PATH)
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS waifu_profiles (
                guild_id          INTEGER NOT NULL,
                user_id           INTEGER NOT NULL,
                value             INTEGER NOT NULL DEFAULT 500,
                claimed_by        INTEGER,
                affinity_for      INTEGER,
                last_affinity_set TEXT,
                last_claim_time   TEXT,
                engaged_to        INTEGER,
                engaged_since     TEXT,
                married_to        INTEGER,
                married_since     TEXT,
                bond_strength     REAL NOT NULL DEFAULT 0.0,
                daily_gift_value  INTEGER NOT NULL DEFAULT 0,
                last_decay_applied TEXT,
                PRIMARY KEY (guild_id, user_id)
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS waifu_settings (
                guild_id                 INTEGER PRIMARY KEY,
                base_value               INTEGER NOT NULL DEFAULT 500,
                claim_increase_pct       REAL NOT NULL DEFAULT 0.25,
                daily_decay_pct          REAL NOT NULL DEFAULT 0.05,
                marriage_fee_multiplier  REAL NOT NULL DEFAULT 1.5,
                gift_threshold_pct       REAL NOT NULL DEFAULT 0.02,
                affinity_discount        REAL NOT NULL DEFAULT 0.50,
                steal_married_multiplier REAL NOT NULL DEFAULT 2.0
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS waifu_proposals (
                guild_id     INTEGER NOT NULL,
                proposer_id  INTEGER NOT NULL,
                target_id    INTEGER NOT NULL,
                fee          INTEGER NOT NULL,
                proposed_at  TEXT NOT NULL,
                PRIMARY KEY (guild_id, proposer_id)
            )"""
        )
        await self.db.commit()
        self.daily_decay_loop.start()

    async def cog_unload(self):
        self.daily_decay_loop.cancel()
        if self.db:
            await self.db.close()

    # ── Helpers ───────────────────────────────────────────────────────

    async def get_settings(self, guild_id: int) -> dict:
        async with self.db.execute(
            "SELECT base_value, claim_increase_pct, daily_decay_pct, "
            "marriage_fee_multiplier, gift_threshold_pct, affinity_discount, "
            "steal_married_multiplier FROM waifu_settings WHERE guild_id = ?",
            (guild_id,),
        ) as cur:
            row = await cur.fetchone()
        if row:
            return {SETTINGS_KEYS[i]: row[i] for i in range(len(SETTINGS_KEYS))}
        return dict(SETTINGS_DEFAULTS)

    async def get_profile(self, guild_id: int, user_id: int) -> dict:
        async with self.db.execute(
            "SELECT * FROM waifu_profiles WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ) as cur:
            row = await cur.fetchone()
        if row:
            return {PROFILE_COLS[i]: row[i] for i in range(len(PROFILE_COLS))}
        settings = await self.get_settings(guild_id)
        await self.db.execute(
            "INSERT INTO waifu_profiles (guild_id, user_id, value) VALUES (?, ?, ?)",
            (guild_id, user_id, settings["base_value"]),
        )
        await self.db.commit()
        return {
            "guild_id": guild_id, "user_id": user_id,
            "value": settings["base_value"], "claimed_by": None,
            "affinity_for": None, "last_affinity_set": None,
            "last_claim_time": None, "engaged_to": None,
            "engaged_since": None, "married_to": None,
            "married_since": None, "bond_strength": 0.0,
            "daily_gift_value": 0, "last_decay_applied": None,
        }

    async def ensure_economy_row(self, user_id: int):
        await self.db.execute(
            "INSERT OR IGNORE INTO economy (user_id, cash, bank) VALUES (?, 0, 0)",
            (user_id,),
        )

    async def get_cash(self, user_id: int) -> int:
        await self.ensure_economy_row(user_id)
        async with self.db.execute(
            "SELECT cash FROM economy WHERE user_id = ?", (user_id,),
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0

    async def deduct_cash(self, user_id: int, amount: int) -> bool:
        """Atomically deduct cash. Returns True on success."""
        await self.ensure_economy_row(user_id)
        cursor = await self.db.execute(
            "UPDATE economy SET cash = cash - ? WHERE user_id = ? AND cash >= ?",
            (amount, user_id, amount),
        )
        return cursor.rowcount > 0

    async def add_cash(self, user_id: int, amount: int):
        await self.ensure_economy_row(user_id)
        await self.db.execute(
            "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
            (amount, user_id),
        )

    def calculate_claim_price(self, profile: dict, settings: dict, claimer_id: int) -> int:
        price = profile["value"]
        if profile["married_to"] is not None:
            price = int(price * settings["steal_married_multiplier"])
        if profile["affinity_for"] == claimer_id:
            price = int(price * (1.0 - settings["affinity_discount"]))
        return max(price, 1)

    def _status_text(self, profile: dict) -> str:
        if profile["married_to"]:
            return "Married"
        if profile["engaged_to"]:
            return "Engaged"
        if profile["claimed_by"]:
            return "Claimed"
        return "Single"

    def _bond_bar(self, strength: float) -> str:
        level = int(strength)
        filled = "\u2588" * level
        empty = "\u2591" * (10 - level)
        return f"`{filled}{empty}` {strength:.1f}/10"

    def _format_cooldown(self, remaining_seconds: float) -> str:
        remaining = int(remaining_seconds)
        hours, remainder = divmod(remaining, 3600)
        minutes, secs = divmod(remainder, 60)
        parts = []
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        if secs or not parts:
            parts.append(f"{secs}s")
        return " ".join(parts)

    async def _clear_engagement(self, guild_id: int, user_a: int, user_b: int):
        """Clear engagement between two users."""
        for uid in (user_a, user_b):
            await self.db.execute(
                "UPDATE waifu_profiles SET engaged_to = NULL, engaged_since = NULL "
                "WHERE guild_id = ? AND user_id = ?",
                (guild_id, uid),
            )

    async def _clear_marriage(self, guild_id: int, user_a: int, user_b: int):
        """Clear marriage between two users."""
        for uid in (user_a, user_b):
            await self.db.execute(
                "UPDATE waifu_profiles SET married_to = NULL, married_since = NULL, "
                "bond_strength = 0.0, engaged_to = NULL, engaged_since = NULL "
                "WHERE guild_id = ? AND user_id = ?",
                (guild_id, uid),
            )

    # ── Commands ──────────────────────────────────────────────────────

    @commands.command(name="waifu")
    async def waifu_profile(self, ctx: commands.Context, member: discord.Member = None):
        """View a waifu profile."""
        member = member or ctx.author
        profile = await self.get_profile(ctx.guild.id, member.id)
        settings = await self.get_settings(ctx.guild.id)

        embed = discord.Embed(
            title=f"{member.display_name}'s Waifu Profile",
            color=discord.Color.pink(),
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="Value", value=f"{profile['value']:,} \U0001f338", inline=True)

        # Status
        status = self._status_text(profile)
        if profile["married_to"]:
            status += f" to <@{profile['married_to']}>"
        elif profile["engaged_to"]:
            since = profile["engaged_since"][:10] if profile["engaged_since"] else "?"
            status += f" to <@{profile['engaged_to']}> (since {since})"
        elif profile["claimed_by"]:
            status += f" by <@{profile['claimed_by']}>"
        embed.add_field(name="Status", value=status, inline=True)

        if profile["affinity_for"]:
            embed.add_field(name="Affinity", value=f"<@{profile['affinity_for']}>", inline=True)

        if profile["married_to"]:
            embed.add_field(name="Bond", value=self._bond_bar(profile["bond_strength"]), inline=False)

        # Show claim price for the viewer
        if member != ctx.author:
            price = self.calculate_claim_price(profile, settings, ctx.author.id)
            price_label = "Claim Price"
            if profile["claimed_by"] and profile["claimed_by"] != ctx.author.id:
                price_label = "Steal Price"
            embed.add_field(name=price_label, value=f"{price:,} \U0001f338", inline=True)

        await ctx.send(embed=embed)

    # ── Claim ─────────────────────────────────────────────────────────

    @commands.command()
    async def claim(self, ctx: commands.Context, member: discord.Member):
        """Claim someone as your waifu by paying their price. 1 claim per day."""
        if member == ctx.author:
            await ctx.send("You cannot claim yourself.")
            return
        if member.bot:
            await ctx.send("You cannot claim a bot.")
            return

        guild_id = ctx.guild.id
        settings = await self.get_settings(guild_id)
        author_profile = await self.get_profile(guild_id, ctx.author.id)

        # Check 24h cooldown
        if author_profile["last_claim_time"]:
            last = datetime.datetime.fromisoformat(author_profile["last_claim_time"])
            now = datetime.datetime.now(datetime.timezone.utc)
            diff = (now - last).total_seconds()
            if diff < 86400:
                remaining = 86400 - diff
                await ctx.send(
                    f"You can only claim once per day. Try again in **{self._format_cooldown(remaining)}**."
                )
                return

        target_profile = await self.get_profile(guild_id, member.id)

        # Cannot claim someone you already have
        if target_profile["claimed_by"] == ctx.author.id:
            await ctx.send(f"You already have {member.display_name} claimed!")
            return

        price = self.calculate_claim_price(target_profile, settings, ctx.author.id)

        # Deduct cash atomically
        if not await self.deduct_cash(ctx.author.id, price):
            cash = await self.get_cash(ctx.author.id)
            await ctx.send(
                f"You need **{price:,}** \U0001f338 but only have **{cash:,}** \U0001f338."
            )
            return

        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        was_steal = (
            target_profile["claimed_by"] is not None
            and target_profile["claimed_by"] != ctx.author.id
        )
        old_claimer = target_profile["claimed_by"]

        # Handle steal: break old engagement/marriage
        if was_steal:
            if target_profile["married_to"] is not None:
                await self._clear_marriage(guild_id, member.id, target_profile["married_to"])
            elif target_profile["engaged_to"] is not None:
                await self._clear_engagement(guild_id, member.id, target_profile["engaged_to"])

        # Update target profile
        new_value = int(target_profile["value"] * (1 + settings["claim_increase_pct"]))
        await self.db.execute(
            "UPDATE waifu_profiles SET claimed_by = ?, value = ?, "
            "engaged_to = NULL, engaged_since = NULL "
            "WHERE guild_id = ? AND user_id = ?",
            (ctx.author.id, new_value, guild_id, member.id),
        )

        # Update author claim time
        await self.db.execute(
            "UPDATE waifu_profiles SET last_claim_time = ? WHERE guild_id = ? AND user_id = ?",
            (now_iso, guild_id, ctx.author.id),
        )

        # Check mutual claim -> engagement
        engaged = False
        if author_profile["claimed_by"] == member.id:
            await self.db.execute(
                "UPDATE waifu_profiles SET engaged_to = ?, engaged_since = ? "
                "WHERE guild_id = ? AND user_id = ?",
                (member.id, now_iso, guild_id, ctx.author.id),
            )
            await self.db.execute(
                "UPDATE waifu_profiles SET engaged_to = ?, engaged_since = ? "
                "WHERE guild_id = ? AND user_id = ?",
                (ctx.author.id, now_iso, guild_id, member.id),
            )
            engaged = True

        await self.db.commit()

        # Build response
        embed = discord.Embed(color=discord.Color.pink())
        if was_steal:
            embed.title = "Waifu Stolen!"
            embed.description = (
                f"{ctx.author.mention} stole {member.mention} from <@{old_claimer}> "
                f"for **{price:,}** \U0001f338!"
            )
        else:
            embed.title = "Waifu Claimed!"
            embed.description = (
                f"{ctx.author.mention} claimed {member.mention} "
                f"for **{price:,}** \U0001f338!"
            )

        if engaged:
            embed.add_field(
                name="\U0001f48d Engaged!",
                value=f"{ctx.author.mention} and {member.mention} are now engaged!",
                inline=False,
            )

        embed.add_field(name="New Value", value=f"{new_value:,} \U0001f338", inline=True)
        await ctx.send(embed=embed)

    # ── Affinity ──────────────────────────────────────────────────────

    @commands.command()
    async def affinity(self, ctx: commands.Context, member: discord.Member = None):
        """Set your waifu affinity toward someone (50% discount for them). Once per day.
        Use without a mention to clear your affinity."""
        guild_id = ctx.guild.id
        profile = await self.get_profile(guild_id, ctx.author.id)

        # Check cooldown
        if profile["last_affinity_set"]:
            last = datetime.datetime.fromisoformat(profile["last_affinity_set"])
            now = datetime.datetime.now(datetime.timezone.utc)
            diff = (now - last).total_seconds()
            if diff < 86400:
                remaining = 86400 - diff
                await ctx.send(
                    f"You can only change affinity once per day. "
                    f"Try again in **{self._format_cooldown(remaining)}**."
                )
                return

        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        if member is None:
            # Clear affinity
            await self.db.execute(
                "UPDATE waifu_profiles SET affinity_for = NULL, last_affinity_set = ? "
                "WHERE guild_id = ? AND user_id = ?",
                (now_iso, guild_id, ctx.author.id),
            )
            await self.db.commit()
            await ctx.send("Your waifu affinity has been cleared.")
            return

        if member == ctx.author:
            await ctx.send("You cannot set affinity toward yourself.")
            return
        if member.bot:
            await ctx.send("You cannot set affinity toward a bot.")
            return

        await self.db.execute(
            "UPDATE waifu_profiles SET affinity_for = ?, last_affinity_set = ? "
            "WHERE guild_id = ? AND user_id = ?",
            (member.id, now_iso, guild_id, ctx.author.id),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Affinity Set",
            description=(
                f"{ctx.author.mention} set their affinity toward {member.mention}.\n"
                f"{member.mention} can now claim you at a **50% discount**!"
            ),
            color=discord.Color.pink(),
        )
        await ctx.send(embed=embed)

    # ── Gift ──────────────────────────────────────────────────────────

    @commands.command()
    async def gift(self, ctx: commands.Context, member: discord.Member, amount: int):
        """Gift flowers to someone. If married, strengthens your bond."""
        if member == ctx.author:
            await ctx.send("You cannot gift yourself.")
            return
        if member.bot:
            await ctx.send("You cannot gift a bot.")
            return
        if amount <= 0:
            await ctx.send("Amount must be positive.")
            return

        # Deduct cash
        if not await self.deduct_cash(ctx.author.id, amount):
            cash = await self.get_cash(ctx.author.id)
            await ctx.send(f"You only have **{cash:,}** \U0001f338 on hand.")
            return

        # Add cash to recipient
        await self.add_cash(member.id, amount)

        guild_id = ctx.guild.id
        author_profile = await self.get_profile(guild_id, ctx.author.id)
        settings = await self.get_settings(guild_id)

        # Check if married to this person in this guild
        bond_msg = ""
        if author_profile["married_to"] == member.id:
            partner_profile = await self.get_profile(guild_id, member.id)
            threshold = max(int(partner_profile["value"] * settings["gift_threshold_pct"]), 1)

            if amount >= threshold:
                # Calculate bond gain
                old_daily = author_profile["daily_gift_value"]
                new_daily = old_daily + amount

                old_ratio = old_daily / threshold if old_daily >= threshold else 0
                old_contrib = min(old_ratio * BOND_GAIN_FACTOR, BOND_MAX_DAILY_GAIN)

                new_ratio = new_daily / threshold
                new_contrib = min(new_ratio * BOND_GAIN_FACTOR, BOND_MAX_DAILY_GAIN)

                gain = max(0.0, new_contrib - old_contrib)
                new_bond = min(BOND_MAX, author_profile["bond_strength"] + gain)

                # Update both profiles with the new bond
                now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
                await self.db.execute(
                    "UPDATE waifu_profiles SET daily_gift_value = ?, bond_strength = ?, "
                    "last_gift_time = ? WHERE guild_id = ? AND user_id = ?",
                    (new_daily, round(new_bond, 4), now_iso, guild_id, ctx.author.id),
                )
                # Sync bond on partner's profile too
                await self.db.execute(
                    "UPDATE waifu_profiles SET bond_strength = ? WHERE guild_id = ? AND user_id = ?",
                    (round(new_bond, 4), guild_id, member.id),
                )

                if gain > 0:
                    bond_msg = f"\n\U0001f495 Bond +{gain:.2f} \u2192 {self._bond_bar(new_bond)}"
                else:
                    bond_msg = f"\n\U0001f495 Bond at daily cap \u2014 {self._bond_bar(new_bond)}"
            else:
                bond_msg = (
                    f"\nGift below bond threshold of **{threshold:,}** \U0001f338. "
                    f"No bond progress."
                )

        await self.db.commit()

        embed = discord.Embed(
            title="Gift Sent!",
            description=(
                f"{ctx.author.mention} gifted **{amount:,}** \U0001f338 to {member.mention}!"
                f"{bond_msg}"
            ),
            color=discord.Color.pink(),
        )
        await ctx.send(embed=embed)

    # ── Propose ───────────────────────────────────────────────────────

    @commands.command()
    async def propose(self, ctx: commands.Context, member: discord.Member, fee: int = None):
        """Propose marriage. Must be engaged for 7+ days. Fee must be >= 1.5x their value."""
        guild_id = ctx.guild.id
        author_profile = await self.get_profile(guild_id, ctx.author.id)
        settings = await self.get_settings(guild_id)

        if author_profile["married_to"] is not None:
            await ctx.send("You are already married!")
            return

        if author_profile["engaged_to"] != member.id:
            await ctx.send(f"You are not engaged to {member.display_name}.")
            return

        # Check 7-day engagement
        if author_profile["engaged_since"]:
            engaged_dt = datetime.datetime.fromisoformat(author_profile["engaged_since"])
            now = datetime.datetime.now(datetime.timezone.utc)
            days = (now - engaged_dt).days
            if days < 7:
                remaining = 7 - days
                await ctx.send(
                    f"You must be engaged for at least 7 days. "
                    f"**{remaining}** day(s) remaining."
                )
                return

        target_profile = await self.get_profile(guild_id, member.id)
        min_fee = int(target_profile["value"] * settings["marriage_fee_multiplier"])

        if fee is None:
            fee = min_fee
        elif fee < min_fee:
            await ctx.send(
                f"Fee must be at least **{min_fee:,}** \U0001f338 "
                f"({settings['marriage_fee_multiplier']}x their value)."
            )
            return

        # Refund existing proposal if any
        async with self.db.execute(
            "SELECT fee FROM waifu_proposals WHERE guild_id = ? AND proposer_id = ?",
            (guild_id, ctx.author.id),
        ) as cur:
            old = await cur.fetchone()
        if old:
            await self.add_cash(ctx.author.id, old[0])
            await self.db.execute(
                "DELETE FROM waifu_proposals WHERE guild_id = ? AND proposer_id = ?",
                (guild_id, ctx.author.id),
            )

        # Deduct fee
        if not await self.deduct_cash(ctx.author.id, fee):
            cash = await self.get_cash(ctx.author.id)
            await ctx.send(
                f"You need **{fee:,}** \U0001f338 but only have **{cash:,}** \U0001f338."
            )
            return

        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        await self.db.execute(
            "INSERT INTO waifu_proposals (guild_id, proposer_id, target_id, fee, proposed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (guild_id, ctx.author.id, member.id, fee, now_iso),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="\U0001f48d Marriage Proposal!",
            description=(
                f"{ctx.author.mention} proposed to {member.mention}!\n\n"
                f"Fee: **{fee:,}** \U0001f338 (both must pay)\n"
                f"{member.mention}, use `.accept` to accept! Expires in 24 hours."
            ),
            color=discord.Color.pink(),
        )
        await ctx.send(embed=embed)

    # ── Accept ────────────────────────────────────────────────────────

    @commands.command()
    async def accept(self, ctx: commands.Context):
        """Accept a pending marriage proposal."""
        guild_id = ctx.guild.id

        async with self.db.execute(
            "SELECT proposer_id, fee, proposed_at FROM waifu_proposals "
            "WHERE guild_id = ? AND target_id = ?",
            (guild_id, ctx.author.id),
        ) as cur:
            row = await cur.fetchone()

        if not row:
            await ctx.send("You have no pending proposals.")
            return

        proposer_id, fee, proposed_at = row
        proposed_dt = datetime.datetime.fromisoformat(proposed_at)
        now = datetime.datetime.now(datetime.timezone.utc)

        # Check expiry
        if (now - proposed_dt).total_seconds() > 86400:
            # Refund proposer and delete
            await self.add_cash(proposer_id, fee)
            await self.db.execute(
                "DELETE FROM waifu_proposals WHERE guild_id = ? AND target_id = ?",
                (guild_id, ctx.author.id),
            )
            await self.db.commit()
            await ctx.send("That proposal has expired. The fee has been refunded.")
            return

        # Deduct fee from acceptor
        if not await self.deduct_cash(ctx.author.id, fee):
            cash = await self.get_cash(ctx.author.id)
            await ctx.send(
                f"You need **{fee:,}** \U0001f338 to accept but only have **{cash:,}** \U0001f338."
            )
            return

        now_iso = now.isoformat()

        # Set married on both
        for uid in (ctx.author.id, proposer_id):
            partner = proposer_id if uid == ctx.author.id else ctx.author.id
            await self.db.execute(
                "UPDATE waifu_profiles SET married_to = ?, married_since = ?, "
                "engaged_to = NULL, engaged_since = NULL "
                "WHERE guild_id = ? AND user_id = ?",
                (partner, now_iso, guild_id, uid),
            )

        # Delete proposal
        await self.db.execute(
            "DELETE FROM waifu_proposals WHERE guild_id = ? AND target_id = ?",
            (guild_id, ctx.author.id),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="\U0001f492 Just Married!",
            description=(
                f"<@{proposer_id}> and {ctx.author.mention} are now married!\n"
                f"Each paid **{fee:,}** \U0001f338.\n\n"
                f"Gift each other daily to strengthen your bond!"
            ),
            color=discord.Color.pink(),
        )
        await ctx.send(embed=embed)

    # ── Divorce ───────────────────────────────────────────────────────

    @commands.command()
    async def divorce(self, ctx: commands.Context):
        """End your marriage."""
        guild_id = ctx.guild.id
        profile = await self.get_profile(guild_id, ctx.author.id)

        if profile["married_to"] is None:
            await ctx.send("You are not married.")
            return

        spouse_id = profile["married_to"]
        await self._clear_marriage(guild_id, ctx.author.id, spouse_id)

        # Clean up any proposals between the two
        await self.db.execute(
            "DELETE FROM waifu_proposals WHERE guild_id = ? AND "
            "(proposer_id IN (?, ?) AND target_id IN (?, ?))",
            (guild_id, ctx.author.id, spouse_id, ctx.author.id, spouse_id),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Divorced",
            description=f"{ctx.author.mention} divorced <@{spouse_id}>.",
            color=discord.Color.dark_gray(),
        )
        await ctx.send(embed=embed)

    # ── Leaderboard ───────────────────────────────────────────────────

    @commands.command(aliases=["wlb"])
    async def waifulb(self, ctx: commands.Context):
        """View the waifu value leaderboard."""
        async with self.db.execute(
            "SELECT user_id, value FROM waifu_profiles "
            "WHERE guild_id = ? ORDER BY value DESC LIMIT 10",
            (ctx.guild.id,),
        ) as cur:
            rows = await cur.fetchall()

        if not rows:
            await ctx.send("No waifu profiles yet.")
            return

        lines = []
        for i, (user_id, value) in enumerate(rows, 1):
            member = ctx.guild.get_member(user_id)
            name = member.display_name if member else f"Unknown ({user_id})"
            lines.append(f"**{i}.** {name} \u2014 {value:,} \U0001f338")

        embed = discord.Embed(
            title="Waifu Leaderboard",
            description="\n".join(lines),
            color=discord.Color.pink(),
        )
        await ctx.send(embed=embed)

    # ── Marriages ─────────────────────────────────────────────────────

    @commands.command()
    async def marriages(self, ctx: commands.Context):
        """View all marriages in this server."""
        async with self.db.execute(
            "SELECT user_id, married_to, married_since, bond_strength "
            "FROM waifu_profiles WHERE guild_id = ? AND married_to IS NOT NULL "
            "AND user_id < married_to ORDER BY bond_strength DESC",
            (ctx.guild.id,),
        ) as cur:
            rows = await cur.fetchall()

        if not rows:
            await ctx.send("No marriages in this server yet.")
            return

        lines = []
        for user_id, spouse_id, since, bond in rows:
            m1 = ctx.guild.get_member(user_id)
            m2 = ctx.guild.get_member(spouse_id)
            n1 = m1.display_name if m1 else str(user_id)
            n2 = m2.display_name if m2 else str(spouse_id)
            date = since[:10] if since else "?"
            level = int(bond)
            lines.append(f"**{n1}** & **{n2}** \u2014 Bond {level}/10 (since {date})")

        embed = discord.Embed(
            title="Server Marriages",
            description="\n".join(lines),
            color=discord.Color.pink(),
        )
        await ctx.send(embed=embed)

    # ── Settings (Owner) ──────────────────────────────────────────────

    @commands.command()
    @is_guild_owner()
    async def waifuset(self, ctx: commands.Context, setting: str, value: str):
        """Configure waifu settings. Server owner only.
        Settings: base_value, claim_increase_pct, daily_decay_pct,
        marriage_fee_multiplier, gift_threshold_pct, affinity_discount,
        steal_married_multiplier"""
        setting = setting.lower()
        if setting not in SETTINGS_DEFAULTS:
            valid = ", ".join(SETTINGS_DEFAULTS.keys())
            await ctx.send(f"Invalid setting. Valid: {valid}")
            return

        # Parse value
        try:
            if setting == "base_value":
                parsed = int(value)
                if parsed < 1:
                    await ctx.send("Base value must be at least 1.")
                    return
            else:
                parsed = float(value)
                if parsed < 0:
                    await ctx.send("Value must be non-negative.")
                    return
        except ValueError:
            await ctx.send("Invalid value.")
            return

        await self.db.execute(
            f"INSERT INTO waifu_settings (guild_id, {setting}) VALUES (?, ?) "
            f"ON CONFLICT(guild_id) DO UPDATE SET {setting} = ?",
            (ctx.guild.id, parsed, parsed),
        )

        # When base_value changes, update all profiles below the new minimum
        extra = ""
        if setting == "base_value":
            cursor = await self.db.execute(
                "UPDATE waifu_profiles SET value = ? WHERE guild_id = ? AND value < ?",
                (parsed, ctx.guild.id, parsed),
            )
            if cursor.rowcount > 0:
                extra = f"\nUpdated **{cursor.rowcount}** profile(s) below the new minimum."

        await self.db.commit()

        embed = discord.Embed(
            title="Waifu Setting Updated",
            description=f"**{setting}** set to **{parsed}**.{extra}",
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    # ── Daily Decay Background Task ───────────────────────────────────

    @tasks.loop(time=datetime.time(hour=0, minute=5, tzinfo=datetime.timezone.utc))
    async def daily_decay_loop(self):
        """Apply daily value decay and bond decay to all waifu profiles."""
        today = datetime.date.today().isoformat()

        async with self.db.execute(
            "SELECT DISTINCT guild_id FROM waifu_profiles "
            "WHERE last_decay_applied IS NULL OR last_decay_applied < ?",
            (today,),
        ) as cur:
            guilds = await cur.fetchall()

        for (guild_id,) in guilds:
            settings = await self.get_settings(guild_id)
            base_value = settings["base_value"]
            decay_pct = settings["daily_decay_pct"]

            async with self.db.execute(
                "SELECT user_id, value, married_to, bond_strength "
                "FROM waifu_profiles "
                "WHERE guild_id = ? AND (last_decay_applied IS NULL OR last_decay_applied < ?)",
                (guild_id, today),
            ) as cur:
                profiles = await cur.fetchall()

            for user_id, value, married_to, bond_strength in profiles:
                # Value decay
                if married_to is not None:
                    decay_reduction = bond_strength / BOND_MAX
                    effective_decay = decay_pct * (1.0 - decay_reduction)
                else:
                    effective_decay = decay_pct

                new_value = max(base_value, int(value * (1.0 - effective_decay)))

                # Bond decay (only if married)
                new_bond = bond_strength
                if married_to is not None and bond_strength > 0:
                    bond_decay = bond_strength * BOND_DECAY_FACTOR
                    new_bond = max(0.0, bond_strength - bond_decay)

                await self.db.execute(
                    "UPDATE waifu_profiles SET value = ?, bond_strength = ?, "
                    "daily_gift_value = 0, last_decay_applied = ? "
                    "WHERE guild_id = ? AND user_id = ?",
                    (new_value, round(new_bond, 4), today, guild_id, user_id),
                )

            await self.db.commit()

    @daily_decay_loop.before_loop
    async def before_daily_decay(self):
        await self.bot.wait_until_ready()

    # ── Error Handler ─────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return
        if isinstance(error, commands.CheckFailure):
            if str(error) == "channel_restricted":
                return
            await ctx.send("Only the server owner can use this command.")
        elif isinstance(error, commands.MemberNotFound):
            await ctx.send("Could not find that member.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Check `{ctx.prefix}help {ctx.command}` for usage.")
        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing argument: `{error.param.name}`.")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(Waifu(bot))
