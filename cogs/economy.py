import discord
from discord.ext import commands
from discord import app_commands
from cogs.utils.db import ensure_wallet, update_wallet, update_bank, add_transaction

import datetime
import random

from config import MAIN_CURRENCY_EMOJI, CURRENCY_NAME, WORK_COOLDOWN

class Economy(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool
    

    @commands.command(aliases=["dep", "d"])
    async def deposit(self, ctx, amount: str):
        """Deposit money from you wallet into your banj account. You can specify and amount or use 'all' to deposit everything."""
        bal = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        wallet = bal["wallet"]
        if amount.lower() == "all":
            amount = wallet
        elif not amount.isdigit():
            await ctx.send("Please enter a valid amount to deposit.")
            return
        else:
            amount = int(amount)

        if amount <= 0:
            await ctx.send("Amount must be greater than zero.")
            return
        if wallet < amount:
            await ctx.send("You can't deposit more than you have in your wallet!")
            return
        
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -amount)
        await update_bank(self.pool, ctx.guild.id, ctx.author.id, amount)
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, amount, "deposit")

        embed = discord.Embed(title="Deposit", description=f"You deposited {amount}{MAIN_CURRENCY_EMOJI} into your bank account!", color=discord.Color.blue())
        await ctx.send(embed=embed)

    @commands.command(aliases=["with", "w"])
    async def withdraw(self, ctx, amount: str):
        """Withdraw money from your bank account into your wallet. You can specify and amount or use 'all' to withdraw everything."""
        bal = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        bank = bal["bank"]
        if amount.lower() == "all":
            amount = bank
        elif not amount.isdigit():
            await ctx.send("Please enter a valid amount to withdraw.")
            return
        else:
            amount = int(amount)

        if amount <= 0:
            await ctx.send("Amount must be greater than zero.")
            return
        if bank < amount:
            await ctx.send("You can't withdraw more than you have in your bank account!")
            return
        
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, amount)
        await update_bank(self.pool, ctx.guild.id, ctx.author.id, -amount)
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, amount, "withdraw")

        embed = discord.Embed(title="Withdraw", description=f"You withdrew {amount}{MAIN_CURRENCY_EMOJI} from your bank account!", color=discord.Color.blue())
        await ctx.send(embed=embed)

    @commands.command(aliases=["bal", "b", "$"])
    async def balance(self, ctx, member: discord.Member = None):
        """Check your balance or someone else's balance. You can mention a member to check their balance."""
        member = member or ctx.author
        bal = await ensure_wallet(self.pool, ctx.guild.id, member.id)
        wallet = bal["wallet"]
        bank = bal["bank"]

        embed = discord.Embed(title=f"{member.display_name}'s Balance", color=discord.Color.green())
        embed.add_field(name=f"Wallet ({CURRENCY_NAME})", value=f"{wallet}{MAIN_CURRENCY_EMOJI}")
        embed.add_field(name=f"Bank ({CURRENCY_NAME})", value=f"{bank}{MAIN_CURRENCY_EMOJI}")
        embed.add_field(name="Total", value=f"{wallet + bank}{MAIN_CURRENCY_EMOJI}")
        embed.set_thumbnail(url=member.display_avatar.url)

        await ctx.send(embed=embed)

    @balance.error
    async def balance_error(self, ctx, error):
        if isinstance(error, commands.MemberNotFound):
            await ctx.send("Member not found. Please mention a valid member or provide a valid user ID.")

    @commands.command()
    async def work(self, ctx):
        """Work to earn some money"""
        cooldown = await self.pool.fetchval(
                    "SELECT expires_at FROM cooldowns WHERE guild_id = $1 AND user_id = $2 AND command = 'work' AND expires_at > now()",
                    ctx.guild.id, ctx.author.id,
                )

        if cooldown is None:
            earnings = random.randint(100, 300)
            await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
            await update_wallet(self.pool, ctx.guild.id, ctx.author.id, earnings)
            await add_transaction(self.pool, ctx.guild.id, ctx.author.id, earnings, "work", "Earnings from work")
            await self.pool.execute(
                """
                INSERT INTO cooldowns (guild_id, user_id, command, expires_at)
                VALUES ($1, $2, 'work', $3)
                ON CONFLICT (guild_id, user_id, command) DO UPDATE SET expires_at = EXCLUDED.expires_at
                """,
                ctx.guild.id, ctx.author.id, datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=WORK_COOLDOWN)
            )

            embed = discord.Embed(title="Work", description=f"You earned {earnings}{MAIN_CURRENCY_EMOJI} from your work!", color=discord.Color.blue())
            await ctx.send(embed=embed)
            return
        
        remaining = cooldown - datetime.datetime.now(datetime.timezone.utc)
        minutes, seconds = divmod(int(remaining.total_seconds()), 60)
        await ctx.send(f"You need to wait *{minutes}m {seconds}s* before you can work again.")


    
    @commands.command()
    async def gift(self, ctx, member: discord.Member, amount: int):
        """Gift money from your wallet to another user's wallet. You must mention the recipient and specify the amount."""
        if amount <= 0:
            await ctx.send("Amount must be greater than zero.")
            return
        bal = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        if bal["wallet"] < amount:
            await ctx.send("You can't give more than you have in your wallet!")
            return

        await ensure_wallet(self.pool, ctx.guild.id, member.id)
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -amount)
        await update_wallet(self.pool, ctx.guild.id, member.id, amount)
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, -amount, "gift", f"Gift to {member}")
        await add_transaction(self.pool, ctx.guild.id, member.id, amount, "gift", f"Gift from {ctx.author}")
        await ctx.send(f"You gifted {amount}{MAIN_CURRENCY_EMOJI} to {member.mention}!")

    @commands.command()
    @commands.is_owner()
    async def add(self, ctx, member: discord.Member, amount: int):
        """Admin: Add money to a user's wallet."""
        if amount <= 0:
            await ctx.send("Amount must be greater than zero.")
            return

        await ensure_wallet(self.pool, ctx.guild.id, member.id)
        await update_wallet(self.pool, ctx.guild.id, member.id, amount)
        await add_transaction(self.pool, ctx.guild.id, member.id, amount, "admin_add", f"Added by {ctx.author}")

    @commands.command()
    @commands.is_owner()
    async def remove(self, ctx, member: discord.Member, amount: int):
        """Admin: Remove money from a user's wallet and bank."""
        if amount <= 0:
            await ctx.send("Amount must be greater than zero.")
            return

        bal = await ensure_wallet(self.pool, ctx.guild.id, member.id)
        if bal["wallet"] + bal["bank"] < amount:
            await ctx.send(f"{member.display_name} only has {bal['wallet'] + bal['bank']}{MAIN_CURRENCY_EMOJI} total.")
            return

        from_wallet = min(amount, bal["wallet"])
        from_bank = amount - from_wallet

        if from_wallet > 0:
            await update_wallet(self.pool, ctx.guild.id, member.id, -from_wallet)
        if from_bank > 0:
            await update_bank(self.pool, ctx.guild.id, member.id, -from_bank)
        await add_transaction(self.pool, ctx.guild.id, member.id, -amount, "admin_remove", f"Removed by {ctx.author}")


async def setup(bot):
    await bot.add_cog(Economy(bot))