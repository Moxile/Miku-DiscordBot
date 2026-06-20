from __future__ import annotations

from dataclasses import dataclass, field

import discord
from discord.ext import commands

# Per-cog accent colors, used to tint category/cog embeds.
COG_COLORS = {
    "Economy": discord.Color.green(),
    "Gambling": discord.Color.gold(),
    "Market": discord.Color.blue(),
    "Moderation": discord.Color.red(),
    "Shop": discord.Color.purple(),
    "Predictions": discord.Color.teal(),
    "Reminders": discord.Color.from_rgb(255, 182, 193),
    "Waifu": discord.Color.from_rgb(255, 105, 180),
    "Leaderboard": discord.Color.from_rgb(255, 215, 0),
    "Acro": discord.Color.orange(),
    "GTE": discord.Color.dark_green(),
    "Utility": discord.Color.blurple(),
    "ReactionRoles": discord.Color.fuchsia(),
    "WolfRandom": discord.Color.dark_teal(),
    "BotReactions": discord.Color.from_rgb(255, 165, 0),
    "Missions": discord.Color.from_rgb(255, 140, 0),
    "Offers": discord.Color.from_rgb(46, 204, 113),
    "Counting": discord.Color.from_rgb(149, 165, 166),
    "Lichess": discord.Color.from_rgb(120, 120, 120),
}


@dataclass
class Category:
    key: str           # lowercase id used in `.help <category>` and the dropdown
    emoji: str
    name: str
    description: str
    cogs: list[str] = field(default_factory=list)  # cog qualified names, in display order
    admin_hub: bool = False  # if True, collect every admin/owner command across all cogs


# The 19 cogs grouped into a handful of scannable themes. The dropdown and the
# `.help` landing page are both built from this list, so order here = order shown.
CATEGORIES: list[Category] = [
    Category("economy", "💰", "Economy",
             "Wallet, bank, work, gifts, shop, missions & offers.",
             ["Economy", "Shop", "Missions", "Offers"]),
    Category("games", "🎲", "Games & Gambling",
             "Coinflip, blackjack, roulette & party games.",
             ["Gambling", "GTE", "Acro", "WolfRandom"]),
    Category("market", "📈", "Market",
             "Stocks, trading, orders, dividends & predictions.",
             ["Market", "Predictions"]),
    Category("collectibles", "🎴", "Collectibles",
             "Collect, trade & show off waifus.",
             ["Waifu"]),
    Category("chess", "♟️", "Chess",
             "Link Lichess, sync rating roles & set your profile.",
             ["Lichess"]),
    Category("stats", "🏆", "Stats",
             "Leaderboards & rankings.",
             ["Leaderboard"]),
    Category("utility", "🛠️", "Utility",
             "Calculator, color preview, reminders & counting.",
             ["Utility", "Reminders", "Counting"]),
    Category("admin", "🔧", "Admin & Setup",
             "Every owner/admin setup command — channels, currency, shop, stocks, "
             "salaries, crime, roles, with moderation in its own section.",
             # Display order within the hub; any other cog with admin commands is
             # appended automatically. Moderation is last so it stands on its own.
             ["Management", "Economy", "Shop", "Market", "Missions", "Predictions",
              "Gambling", "Lichess", "Counting", "ReactionRoles", "BotReactions", "Moderation"],
             admin_hub=True),
]

CATEGORY_BY_KEY = {c.key: c for c in CATEGORIES}


def _truncate(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _tag(cmd: commands.Command) -> str:
    """Return a ` [Admin]`/` [Owner]` suffix for permission-gated commands."""
    for check in cmd.checks:
        s = str(check)
        if "is_owner" in s or "guild_or_bot_owner" in s:
            return " [Owner]"
        if "has_permissions" in s or "has_guild_permissions" in s:
            return " [Admin]"
    return ""


def _is_admin(cmd: commands.Command) -> bool:
    """True if a command is owner- or admin-gated (i.e. it carries a tag)."""
    return bool(_tag(cmd))


def _command_lines(cmd: commands.Command, *, admin: bool | None = None) -> list[str]:
    """A compact one-liner for a command, plus a subcommand line for groups.

    admin=None  -> command + every subcommand (legacy behavior).
    admin=False -> only public commands; the subcommand line drops admin subs.
    admin=True  -> only admin commands; a group shows if it (or any subcommand)
                   is admin, listing just the admin subcommands.
    """
    if not isinstance(cmd, commands.Group):
        if admin is not None and _is_admin(cmd) != admin:
            return []
    line = f"`{cmd.name}`{_tag(cmd)}"
    if cmd.short_doc:
        line += f" — {cmd.short_doc}"
    lines = [_truncate(line, 110)]
    if isinstance(cmd, commands.Group):
        subs = sorted((s for s in cmd.commands if not s.hidden), key=lambda c: c.name)
        if admin is not None:
            subs = [s for s in subs if _is_admin(s) == admin]
            show_group = (_is_admin(cmd) or bool(subs)) if admin else (not _is_admin(cmd))
            if not show_group:
                return []
        if subs:
            lines.append(" ↳ " + " ".join(f"`{s.name}`" for s in subs))
    return lines


def _add_packed(embed: discord.Embed, name: str, lines: list[str]) -> None:
    """Add `lines` under field `name`, splitting into '(cont.)' fields to respect
    the 1024-char field limit."""
    chunk = ""
    first = True
    for line in lines:
        addition = line if not chunk else "\n" + line
        if len(chunk) + len(addition) > 1024:
            embed.add_field(name=name if first else f"{name} (cont.)", value=chunk, inline=False)
            first, chunk = False, line
        else:
            chunk += addition
    if chunk:
        embed.add_field(name=name if first else f"{name} (cont.)", value=chunk, inline=False)


def _count_commands(cog: commands.Cog, *, admin: bool) -> int:
    """Count the commands a cog contributes to a public (admin=False) or the
    Admin hub (admin=True) view — matching what `_command_lines` would render."""
    total = 0
    for cmd in cog.get_commands():
        if cmd.hidden:
            continue
        if isinstance(cmd, commands.Group):
            matching = [s for s in cmd.commands if not s.hidden and _is_admin(s) == admin]
            show_group = (_is_admin(cmd) or bool(matching)) if admin else (not _is_admin(cmd))
            if show_group:
                total += 1
            total += len(matching)
        elif _is_admin(cmd) == admin:
            total += 1
    return total


def _admin_hub_cogs(bot: commands.Bot, cat: Category) -> list[str]:
    """The Admin hub's preferred cog order, with any other cog appended."""
    return list(cat.cogs) + [n for n in bot.cogs if n not in cat.cogs]


def build_category_embed(bot: commands.Bot, cat: Category) -> discord.Embed:
    if cat.admin_hub:
        return build_admin_embed(bot, cat)
    color = COG_COLORS.get(cat.cogs[0], discord.Color.blurple())
    embed = discord.Embed(title=f"{cat.emoji} {cat.name}", description=cat.description, color=color)
    for cog_name in cat.cogs:
        cog = bot.get_cog(cog_name)
        if cog is None:
            continue
        lines: list[str] = []
        for cmd in sorted(cog.get_commands(), key=lambda c: c.name):
            if cmd.hidden:
                continue
            lines.extend(_command_lines(cmd, admin=False))
        if lines:
            _add_packed(embed, cog_name, lines)
    embed.set_footer(text="Use .help <command> for full details on any command.")
    return embed


def build_admin_embed(bot: commands.Bot, cat: Category) -> discord.Embed:
    """The Admin hub: every owner/admin setup command, grouped by feature, with
    Moderation in its own section."""
    embed = discord.Embed(title=f"{cat.emoji} {cat.name}", description=cat.description,
                          color=discord.Color.dark_grey())
    for cog_name in _admin_hub_cogs(bot, cat):
        cog = bot.get_cog(cog_name)
        if cog is None:
            continue
        lines: list[str] = []
        for cmd in sorted(cog.get_commands(), key=lambda c: c.name):
            if cmd.hidden:
                continue
            lines.extend(_command_lines(cmd, admin=True))
        if lines:
            _add_packed(embed, cog_name, lines)
    embed.set_footer(
        text="[Owner] = server/bot owner or owner-role · [Admin] = needs the Discord permission · "
             ".help <command> for details"
    )
    return embed


def build_home_embed(bot: commands.Bot) -> discord.Embed:
    embed = discord.Embed(
        title="📖 Miku — Command Help",
        description=(
            "Pick a category from the menu below, or use "
            "`.help <category>` / `.help <command>` for details."
        ),
        color=discord.Color.blurple(),
    )
    total = 0
    for cat in CATEGORIES:
        cog_names = _admin_hub_cogs(bot, cat) if cat.admin_hub else cat.cogs
        count = sum(_count_commands(c, admin=cat.admin_hub)
                    for c in (bot.get_cog(n) for n in cog_names) if c)
        total += count
        embed.add_field(
            name=f"{cat.emoji} {cat.name} ({count})",
            value=cat.description,
            inline=False,
        )
    embed.set_footer(text=f"{total} commands • Prefix: .  •  Menu stays active for 3 min")
    return embed


class HelpView(discord.ui.View):
    """A category dropdown that swaps the embed in place. Pre-renders every
    category embed up front so the select callback never touches the DB."""

    def __init__(self, author_id: int, embeds: dict[str, discord.Embed]):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.embeds = embeds
        self.message: discord.Message | None = None

        options = [discord.SelectOption(
            label="Overview", value="home", emoji="🏠",
            description="Back to the category list",
        )]
        for cat in CATEGORIES:
            options.append(discord.SelectOption(
                label=cat.name, value=cat.key, emoji=cat.emoji,
                description=_truncate(cat.description, 100),
            ))
        self.select: discord.ui.Select = discord.ui.Select(
            placeholder="Choose a category…", options=options,
        )
        self.select.callback = self._on_select
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "This help menu isn't yours — run `.help` to get your own.", ephemeral=True,
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction) -> None:
        embed = self.embeds.get(self.select.values[0], self.embeds["home"])
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_timeout(self) -> None:
        self.select.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


def _build_embeds(bot: commands.Bot) -> dict[str, discord.Embed]:
    embeds = {"home": build_home_embed(bot)}
    for cat in CATEGORIES:
        embeds[cat.key] = build_category_embed(bot, cat)
    return embeds


class Help(commands.HelpCommand):
    """Interactive, category-based help.

    Notable behavior: listings ignore channel/lock checks (`verify_checks=False`
    and we walk `get_commands()` directly), so commands no longer disappear from
    help just because you're in the wrong channel — restricted ones are tagged
    [Admin]/[Owner] instead.
    """

    async def command_callback(self, ctx, /, *, command=None):
        if command:
            key = command.lower()
            cat = CATEGORY_BY_KEY.get(key)
            if cat is None:
                for c in CATEGORIES:
                    if c.name.lower() == key:
                        cat = c
                        break
            if cat is not None:
                return await self.send_category(cat)
            for name, cog in ctx.bot.cogs.items():
                if name.lower() == key:
                    return await self.send_cog_help(cog)
        return await super().command_callback(ctx, command=command)

    async def send_bot_help(self, mapping):
        ctx = self.context
        embeds = _build_embeds(ctx.bot)
        view = HelpView(ctx.author.id, embeds)
        try:
            view.message = await ctx.author.send(embed=embeds["home"], view=view)
            await ctx.message.add_reaction("✅")
        except discord.Forbidden:
            view.message = await ctx.send(embed=embeds["home"], view=view)

    async def send_category(self, cat: Category):
        ctx = self.context
        embeds = _build_embeds(ctx.bot)
        view = HelpView(ctx.author.id, embeds)
        view.message = await self.get_destination().send(embed=embeds[cat.key], view=view)

    async def send_cog_help(self, cog):
        color = COG_COLORS.get(cog.qualified_name, discord.Color.greyple())
        embed = discord.Embed(title=f"{cog.qualified_name} Commands", color=color)
        for cmd in sorted(cog.get_commands(), key=lambda c: c.name):
            if cmd.hidden:
                continue
            label = cmd.name
            if cmd.aliases:
                label += " (" + ", ".join(cmd.aliases) + ")"
            label += _tag(cmd)
            value = cmd.short_doc or "No description"
            if isinstance(cmd, commands.Group):
                subs = sorted((s for s in cmd.commands if not s.hidden), key=lambda c: c.name)
                if subs:
                    value += "\n↳ " + " ".join(f"`{s.name}`" for s in subs)
            embed.add_field(name=label, value=_truncate(value, 1024), inline=False)
        embed.set_footer(text="Use .help <command> for more info on a command.")
        await self.get_destination().send(embed=embed)

    async def send_group_help(self, group):
        color = COG_COLORS.get(group.cog.qualified_name, discord.Color.greyple()) if group.cog else discord.Color.greyple()
        embed = discord.Embed(
            title=f".{group.qualified_name}",
            description=group.help or "No description",
            color=color,
        )
        embed.add_field(name="Usage", value=f"`{self.get_command_signature(group)}`", inline=False)
        for sub in sorted((s for s in group.commands if not s.hidden), key=lambda c: c.name):
            embed.add_field(
                name=f"{group.qualified_name} {sub.name}{_tag(sub)}",
                value=sub.short_doc or "No description",
                inline=False,
            )
        await self.get_destination().send(embed=embed)

    async def send_command_help(self, command):
        cog = command.cog
        color = COG_COLORS.get(cog.qualified_name, discord.Color.greyple()) if cog else discord.Color.greyple()
        embed = discord.Embed(
            title=f".{command.qualified_name}{_tag(command)}",
            description=command.help or "No description",
            color=color,
        )
        embed.add_field(name="Usage", value=f"`{self.get_command_signature(command)}`", inline=False)
        if command.aliases:
            embed.add_field(name="Aliases", value=", ".join(f"`{a}`" for a in command.aliases), inline=False)
        if cog:
            siblings = [c.name for c in cog.get_commands() if c != command and not c.hidden]
            if siblings:
                embed.set_footer(text=f"Related: {', '.join(siblings)}")
        await self.get_destination().send(embed=embed)

    async def send_error_message(self, error):
        embed = discord.Embed(title="Not Found", description=error, color=discord.Color.red())
        await self.get_destination().send(embed=embed)
