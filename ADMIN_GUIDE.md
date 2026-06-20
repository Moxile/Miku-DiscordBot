# Miku Bot — Server Owner & Admin Guide

How to set up and run Miku **inside your Discord server** once the bot has been invited. Includes every admin command and **how to undo** each action (items, shop, stocks, salaries, channels, …).

**Command prefix:** `.` &nbsp;|&nbsp; **Currency:** Flowers 🌸 &nbsp;|&nbsp; Type `.help` any time for the live command menu.

---

## Table of Contents

1. [Recommended Setup Order (start here)](#1-recommended-setup-order)
2. [Permission Tiers (who can run what)](#2-permission-tiers)
3. [The Owner Role — let staff run admin commands](#3-the-owner-role)
4. [Turn Features On / Off](#4-turn-features-on--off)
5. [Lock Features to Channels](#5-lock-features-to-channels)
6. [Admin Commands by Feature — with Undo](#6-admin-commands-by-feature)
   - [Shop & Items](#shop--items)
   - [Stock Market](#stock-market)
   - [Economy & Money](#economy--money)
   - [Role Salaries](#role-salaries)
   - [Missions](#missions)
   - [Predictions](#predictions)
   - [Moderation](#moderation)
   - [Reaction Roles](#reaction-roles)
   - [Bot Reactions (auto-replies)](#bot-reactions)
   - [Counting](#counting)
   - [Lichess / Chess Roles](#lichess--chess-roles)
7. [Undo / Cleanup Cheat-Sheet](#7-undo--cleanup-cheat-sheet)
8. [Full Public Command Reference](#8-full-public-command-reference)

---

## 1. Recommended Setup Order

Do these in order right after inviting the bot. Most steps are optional — skip features you don't want.

1. **Place the bot's role high.** In **Server Settings → Roles**, drag the bot's role **above** any role it will handle (normal member roles, shop roles, ...). It can only manage/moderate roles below its own.

2. **Check it responds.** Type `.help` in any channel — you'll get an interactive, categorized menu (sent to your DMs when possible).

3. **Decide who's an admin.** By default only you (server owner) can run `[Owner]` commands. To let trusted staff run them, set up an **[owner role](#3-the-owner-role)**:
   ```
   .ownerrole create Miku Admin     ← makes the role and registers it
   ```
   Then assign that role to staff in Discord.

4. **Turn off features you don't want** (e.g. gambling):
   ```
   .disable Gambling
   ```

5. **Lock noisy features to dedicated channels** (optional but recommended):
   ```
   .setgamblingchannel #casino
   .settradingchannel  #stock-market
   .setworkchannel     #work
   ```

6. **Build your economy** (only if you want the shop/market/salaries):
   - **Shop:** `.additem`, `.addrole`, `.addtemprole`
   - **Stocks:** `.ipohelper` to price, then `.listcompany`
   - **Salaries:** `.collectrole bind`
   - **Missions:** `.addmission`

7. **Set up role automation** (optional): `.reactionroles add …`, `.lichess setup`.

That's it. Everything is stored per-server and persists. The rest of this guide is reference for each feature.

> **Tip:** every "set channel" command clears back to *allowed everywhere* if you run it again with **no channel**. Every "add" has a matching "remove". See the [cheat-sheet](#7-undo--cleanup-cheat-sheet).

---

## 2. Permission Tiers

Commands fall into three access levels. In `.help`, restricted commands are tagged **[Owner]** or **[Admin]**.

| Tier | Who qualifies | Based on |
|------|---------------|----------|
| **Public** | Everyone | — |
| **[Admin]** | Members with the required **Discord permission** (Manage Server, Manage Roles, etc.) | native Discord permissions |
| **[Owner]** | The **server owner**, the **bot owner**, *or* anyone with the configured **owner role** | the bot's owner check |

**Key point:** "[Owner]" commands (money, stocks, the shop, missions, salaries, …) are **not** tied to Discord's Administrator permission. Granting someone *Administrator* does **not** give them these commands — you must give them the **owner role** instead (next section).

---

## 3. The Owner Role

By default only the **server owner** (and the bot's developer) can run `[Owner]` commands. To extend that to staff, configure an **owner role**. Only the **server owner or bot owner** can manage this setting.

| Action | Command |
|--------|---------|
| Show the current owner role | `.ownerrole` |
| **Create** a fresh role and register it | `.ownerrole create <name>` |
| **Use an existing role** as the owner role | `.ownerrole set <@role>` |
| **Undo** — stop granting owner access via any role | `.ownerrole clear` |

After this, simply assigning that role to a member in Discord gives them every `[Owner]` command. `.ownerrole clear` revokes it for everyone (you keep access as server owner).

---

## 4. Turn Features On / Off

Each feature can be toggled per-server. Requires **Manage Server**.

| Action | Command | Example |
|--------|---------|---------|
| Disable a feature | `.disable <feature>` | `.disable Gambling` |
| **Undo** — re-enable it | `.enable <feature>` | `.enable Gambling` |

Feature names (case-insensitive): `Economy`, `Shop`, `Missions`, `Offers`, `Gambling`, `GTE`, `Acro`, `WolfRandom`, `Market`, `Predictions`, `Waifu`, `Lichess`, `Leaderboard`, `Utility`, `Reminders`, `Counting`, `Moderation`, `ReactionRoles`, `BotReactions`.

> The management commands themselves can't be disabled, so you can never lock yourself out of `.enable`.

---

## 5. Lock Features to Channels

Several features can be **restricted to a single channel**. Same pattern everywhere:
- **Set:** run the command with a channel.
- **Undo / clear** (allow everywhere again): run the **same command with no channel**.

All of these are **[Owner]** commands.

| Feature | Set to a channel | Clear (allow everywhere) |
|---------|------------------|--------------------------|
| `.work` | `.setworkchannel #channel` | `.setworkchannel` |
| Gambling (`.coinflip`, `.blackjack`, `.roulette`, …) | `.setgamblingchannel #channel` | `.setgamblingchannel` |
| Trading (`.marketbuy`, `.sellorder`, …) | `.settradingchannel #channel` | `.settradingchannel` |
| Market weekly recap / financials posts | `.setmarketownerchannel #channel` | `.setmarketownerchannel` |
| Missions (`.missions`, `.fund`) | `.setmissionchannel #channel` | `.setmissionchannel` |

> If a channel was never set, the command works everywhere. Setting then clearing fully reverts the restriction.

---

## 6. Admin Commands by Feature

> Legend: **[Owner]** = server/bot owner or owner-role holder · **[Admin]** = needs the listed Discord permission.

### Shop & Items

The shop sells plain items, **permanent roles**, and **temporary roles**. All management commands are **[Owner]**.

| Action | Command |
|--------|---------|
| Add a plain item | `.additem <price> <name>` |
| Add a role item (purchase grants a role permanently) | `.addrole <price> @role <name>` |
| Add a **temporary** role item (role expires) | `.addtemprole <price> @role <duration> <name>` |
| Set/replace an item's description | `.itemdesc <name> <description>` |
| **Undo** — remove any item from the shop | `.removeitem <name>` |

- `<duration>` uses `s`/`m`/`h`/`d` (e.g. `7d`, `12h`); a minimum length is enforced.
- `<price>` accepts shorthand like `1k`, `2.5m`.
- **To "undo" any added item/role/temp-role:** `.removeitem <name>`.
- **To change a description:** run `.itemdesc` again — it overwrites.
- Item names are unique per server; a duplicate name is rejected.

**Player-facing:** `.shop` (browse + buy with buttons), `.buy <name>`, `.inventory [@member]`.

---

### Stock Market

Companies are tied to **text channels**. Management is **[Owner]**.

| Action | Command |
|--------|---------|
| Estimate a fair IPO price from a channel's activity | `.ipohelper #channel [days] [total_shares] [target_yield%]` |
| **List a company** (create a stock) | `.listcompany #channel <name> [ipo_price=100] [total_shares=10000]` |
| **Undo** — delist a company (deletes shares, orders, trade history) | `.delistcompany #channel` |
| Force-compute today's revenue | `.calcrevenue` |
| Force the weekly revenue recap | `.forcerecap` |
| Force weekly financials (treasury, dividends, level-ups) | `.forcefinancials` |

- `.ipohelper` only **suggests** a price — it changes nothing, so nothing to undo. A good first step before `.listcompany`.
- **To remove a stock you listed:** `.delistcompany #channel`. ⚠️ Destructive — wipes that company's shares, open orders, and trade history.
- `.calcrevenue` / `.forcerecap` / `.forcefinancials` **manually trigger** jobs the bot already runs on a schedule (daily 00:00 UTC; recap Wednesdays; financials Sundays/Mondays). They move real Flowers and **cannot be undone** — use only to recover a missed run.

**Player-facing:** `.exchange`/`.stocks`, `.portfolio`, `.companyinfo #stock`, `.orderbook #stock`, `.marketbuy`, `.marketsell`, `.buyorder`, `.sellorder`, `.cancelorder <id>`, `.giftstocks`, `.dividendhistory`.

---

### Economy & Money

All **[Owner]**.

| Action | Command | Undo |
|--------|---------|------|
| Add Flowers to a user's wallet | `.add @member <amount>` | `.remove @member <amount>` |
| Remove Flowers from a user | `.remove @member <amount>` | `.add @member <amount>` |
| **Lock** a user out of economy commands | `.lockuser @member [--delete]` | `.unlockuser @member` |
| Unlock a user | `.unlockuser @member` | — |
| **Reset the entire economy** | `.reseteconomy` | ⚠️ **irreversible** |

- `.lockuser @member --delete` also **zeroes their balance** and returns their shares to IPO. `.unlockuser` restores access but does **not** refund a `--delete` wipe.
- `.reseteconomy` zeroes **all** wallets/banks, deletes all transactions and stock positions, and recreates every stock at 10,000 shares / its original IPO price. It asks to confirm first and **cannot be reversed**.

**Player-facing:** `.balance`, `.deposit`, `.withdraw`, `.work`, `.collect`, `.gift`, `.curtrs` (transaction log).

---

### Role Salaries

Bind a role to a recurring payout claimed with `.collect`. All **[Owner]**.

| Action | Command |
|--------|---------|
| Bind a salary to a role | `.collectrole bind @role <interval> <amount>` |
| **Undo** — remove a role's salary | `.collectrole unbind @role` |
| List all role salaries | `.collectrole list` |

`<interval>` uses `s`/`m`/`h`/`d`, e.g. `.collectrole bind @VIP 1d 500`. Members holding the role run `.collect` to claim once the timer is ready.

---

### Missions

Community funding goals. Management is **[Owner]**; funding is public.

| Action | Command |
|--------|---------|
| Add a mission | `.addmission <goal> <name> \| <description>` |
| **Undo** — delete a mission by ID | `.deletemission <id>` |
| Restrict mission commands to a channel | `.setmissionchannel #channel` (clear with no arg) |

Example: `.addmission 50000 Operation Aurora | Fund a new base.` &nbsp; **Player-facing:** `.missions`, `.fund <name> <amount>`.

---

### Predictions

| Action | Command | Access |
|--------|---------|--------|
| Set which role may create predictions | `.setpredictorrole @role` | Owner |
| Create a prediction | `.predict Question? \| Opt1 \| Opt2 [\| Opt3]` | Predictor role |
| Close betting on a prediction | `.pclose <id>` | Creator or admin |
| Resolve & pay out winners | `.presolve <id> <winning_option#>` | Creator or admin |

- To **change** who can create predictions, run `.setpredictorrole` again with a different role.
- `.pclose` stops new bets; `.presolve` pays winners and **cannot be undone**. **Player-facing:** `.predictions`, `.pbet <id> <option#> <amount>`.

---

### Moderation

Standard Discord moderation. Each needs the matching Discord permission (and the bot needs it too).

| Action | Command | Permission | Undo |
|--------|---------|-----------|------|
| Kick a member | `.kick @member [reason]` | Kick Members | (re-invite manually) |
| Ban a member | `.ban @member [reason]` | Ban Members | `.unban <user_id>` |
| Unban a user | `.unban <user_id>` | Ban Members | — |
| Timeout (mute) a member | `.mute @member <duration> [reason]` | Timeout Members | `.unmute @member` |
| Remove timeout | `.unmute @member` | Timeout Members | — |

`<duration>` examples: `10s`, `5m`, `1h`, `1d`.

---

### Reaction Roles

Give roles when members react to a message. All need **Manage Roles**. Group: `.reactionroles` (alias `.rr`).

| Action | Command |
|--------|---------|
| Bind an emoji → role on a message | `.rr add <message_link_or_id> <emoji> @role` |
| **Undo** — remove one emoji binding | `.rr remove <message> <emoji>` |
| **Undo all** bindings on a message | `.rr clear <message>` |
| List bindings (one message, or all) | `.rr list [message]` |
| Set a **default role** (given to members with no reactions) | `.rr default <message> @role` |
| **Undo** the default role | `.rr defaultremove <message>` |
| Apply the default role to all matching members now | `.rr defaultsync <message>` |

---

### Bot Reactions

Auto-replies to trigger words. Need **Manage Server**. Group: `.botreaction` (alias `.br`).

| Action | Command |
|--------|---------|
| Add a trigger → response (optionally role-gated) | `.br add <trigger> <response> [@role]` |
| **Undo** — remove a reaction | `.br remove <trigger>` |
| List all reactions | `.br list` |

Wrap multi-word triggers/responses in quotes. If a role is given, only members with that role trigger it.

---

### Counting

A counting game bound to one channel. Need **Manage Channels**.

| Action | Command |
|--------|---------|
| Bind counting to the current channel (resets the count) | `.counting bind` |
| **Undo** — unbind counting | `.counting unbind` |
| Show the current counting state | `.counting` |

---

### Lichess / Chess Roles

Auto-assigns rating roles when members link their Lichess account. Setup needs **Manage Roles** (bot too). Group: `.lichess`.

| Action | Command |
|--------|---------|
| Create rating roles for all (or one) variant | `.lichess setup [variant]` |
| List configured rating-role bindings | `.lichess roles list` |
| Bind a rating tier to a role | `.lichess roles bind …` |
| Configure variant settings (min/step/max/enabled) | `.lichess config …` |

> There is **no automatic "un-setup"** — to undo `.lichess setup`, delete the generated rating roles manually in Discord's role settings. **Player-facing:** `.lichess link`, `.lichess unlink`, `.lichess refresh`, `.profile [@user]`, `.profile style <style>`.

---

## 7. Undo / Cleanup Cheat-Sheet

| You did… | Undo with… |
|----------|------------|
| `.additem` / `.addrole` / `.addtemprole` | `.removeitem <name>` |
| `.itemdesc <name> …` | run `.itemdesc` again (overwrites) |
| `.listcompany #ch …` | `.delistcompany #ch` ⚠️ destructive |
| `.add @user <amt>` | `.remove @user <amt>` |
| `.remove @user <amt>` | `.add @user <amt>` |
| `.lockuser @user` | `.unlockuser @user` (a `--delete` wipe is **not** refunded) |
| `.reseteconomy` | ❌ irreversible |
| `.collectrole bind @role …` | `.collectrole unbind @role` |
| `.addmission …` | `.deletemission <id>` |
| `.set*channel #ch` (any) | the same command with **no channel** |
| `.setpredictorrole @role` | run again with a different role (no clear) |
| `.ownerrole create/set` | `.ownerrole clear` |
| `.disable <feature>` | `.enable <feature>` |
| `.ban` | `.unban <user_id>` |
| `.mute` | `.unmute @member` |
| `.rr add` | `.rr remove` / `.rr clear` |
| `.rr default` | `.rr defaultremove` |
| `.br add` | `.br remove <trigger>` |
| `.counting bind` | `.counting unbind` |
| `.lichess setup` | delete the generated roles manually |
| `.presolve` / `.forcefinancials` / `.calcrevenue` | ❌ not reversible (money already moved) |

---

## 8. Full Public Command Reference

Type `.help` for the interactive menu, `.help <category>` for a group, or `.help <command>` for details on any command.

**💰 Economy** — `.balance` (`bal`,`b`,`$`), `.deposit` (`dep`,`d`), `.withdraw` (`with`,`w`), `.work`, `.collect`, `.gift`, `.curtrs` (`transactions`,`txlog`)
**🛒 Shop** — `.shop`, `.buy <name>`, `.inventory` (`inv`)
**🎯 Missions** — `.missions`, `.fund <name> <amount>`
**🤝 Offers (peer bets)** — `.offer …`, `.take <id> <stake>`, `.offers`, `.offerinfo <id>`, `.closeoffer <id> win|lose`, `.canceloffer <id>`
**🎲 Gambling** — `.coinflip` (`cf`), `.betflip` (`bf`), `.blackjack` (`bj`), `.roulette`, `.russian_roulette` (`rr`)
**♟️ Chess games** — `.gte <link> <award> [duration]` (Guess the Elo), `.wolfrandom` (`wr`)
**🔤 Acro** — `.acro [bet]`
**📈 Market** — `.exchange` (`m`,`stocks`,`ex`), `.portfolio` (`p`,`port`), `.companyinfo` (`ci`), `.orderbook` (`ob`), `.marketbuy` (`mb`), `.marketsell` (`ms`), `.buyorder` (`bo`), `.sellorder` (`so`), `.cancelorder` (`co`), `.giftstocks` (`gs`), `.dividendhistory` (`dh`)
**🔮 Predictions** — `.predictions` (`preds`), `.pbet <id> <option#> <amount>`
**🎴 Waifu** — `.waifubuy` (`wbuy`), `.harem` (`waifulist`,`mywaifu`), `.waifuinfo` (`winfo`), `.waifugift` (`wgift`), `.propose`, `.accept`, `.deny`, `.divorce`
**🏆 Stats** — `.lb` / `.leaderboard [wallet|bank|port|waifu]`
**🛠️ Utility** — `.calc <expr>`, `.color <hex>`, `.remember` (`remind`,`remindme`), `.reminders`, `.cancelreminder <id>`, `.counting`
**♟️ Lichess** — `.lichess link` / `unlink` / `refresh`, `.profile [@user]`, `.profile style <style>`
