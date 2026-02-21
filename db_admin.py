#!/usr/bin/env python3
"""
db_admin.py — Interactive CLI for managing the dcbot economy database.
Run with: python db_admin.py
"""

import sqlite3
import sys
import os

DB_PATH = "data/economy.db"
MM_USER_ID = 0


def get_db() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        sys.exit(1)
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


# ── User operations ──────────────────────────────────────────────────────────

def list_users(db: sqlite3.Connection):
    rows = db.execute(
        "SELECT user_id, cash, bank FROM economy ORDER BY cash + bank DESC"
    ).fetchall()
    if not rows:
        print("No users found.")
        return
    print(f"{'user_id':<22} {'cash':>10} {'bank':>10} {'total':>10}")
    print("-" * 56)
    for r in rows:
        print(f"{r['user_id']:<22} {r['cash']:>10,} {r['bank']:>10,} {r['cash']+r['bank']:>10,}")


def delete_user(db: sqlite3.Connection, user_id: int):
    tables = [
        "economy", "holdings", "transactions",
        "user_daily_chars",
    ]
    for table in tables:
        db.execute(f"DELETE FROM {table} WHERE user_id = ?", (user_id,))
    db.commit()
    print(f"Deleted user {user_id} from all tables.")


def set_cash(db: sqlite3.Connection, user_id: int, amount: int):
    db.execute("UPDATE economy SET cash = ? WHERE user_id = ?", (amount, user_id))
    db.commit()
    print(f"Set cash for user {user_id} to {amount:,}.")


def set_bank(db: sqlite3.Connection, user_id: int, amount: int):
    db.execute("UPDATE economy SET bank = ? WHERE user_id = ?", (amount, user_id))
    db.commit()
    print(f"Set bank for user {user_id} to {amount:,}.")


# ── Company operations ───────────────────────────────────────────────────────

def list_companies(db: sqlite3.Connection):
    rows = db.execute(
        "SELECT channel_id, name, ipo_price, fair_price, total_shares, "
        "COALESCE(treasury, 0) as treasury FROM companies ORDER BY name"
    ).fetchall()
    if not rows:
        print("No companies found.")
        return
    print(f"{'channel_id':<22} {'name':<20} {'ipo':>8} {'fair':>8} {'shares':>8} {'treasury':>12}")
    print("-" * 82)
    for r in rows:
        print(
            f"{r['channel_id']:<22} {r['name']:<20} "
            f"{r['ipo_price']:>8.2f} {r['fair_price']:>8.2f} "
            f"{r['total_shares']:>8,} {r['treasury']:>12,.0f}"
        )


def delete_company(db: sqlite3.Connection, channel_id: int):
    tables = [
        "companies", "orders", "holdings", "mm_state",
        "trades", "channel_revenue", "user_daily_chars", "price_history",
    ]
    for table in tables:
        db.execute(f"DELETE FROM {table} WHERE channel_id = ?", (channel_id,))
    db.commit()
    print(f"Deleted company (channel_id={channel_id}) and all related data.")


def set_fair_price(db: sqlite3.Connection, channel_id: int, price: float):
    db.execute("UPDATE companies SET fair_price = ? WHERE channel_id = ?", (price, channel_id))
    db.execute("UPDATE mm_state SET fair_price = ? WHERE channel_id = ?", (price, channel_id))
    db.commit()
    print(f"Set fair price for channel {channel_id} to {price:.2f}.")


def set_treasury(db: sqlite3.Connection, channel_id: int, amount: float):
    db.execute("UPDATE companies SET treasury = ? WHERE channel_id = ?", (amount, channel_id))
    db.commit()
    print(f"Set treasury for channel {channel_id} to {amount:,.0f}.")


# ── Raw query ────────────────────────────────────────────────────────────────

def raw_query(db: sqlite3.Connection, sql: str):
    try:
        cur = db.execute(sql)
        if cur.description:
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            print("  ".join(f"{c:<16}" for c in cols))
            print("-" * (18 * len(cols)))
            for row in rows:
                print("  ".join(f"{str(v):<16}" for v in row))
            print(f"\n{len(rows)} row(s) returned.")
        else:
            db.commit()
            print(f"{cur.rowcount} row(s) affected.")
    except sqlite3.Error as e:
        print(f"SQL error: {e}")


# ── REPL ─────────────────────────────────────────────────────────────────────

HELP = """
Commands:
  users                          List all users
  deluser <user_id>              Delete a user entirely
  setcash <user_id> <amount>     Set a user's wallet cash
  setbank <user_id> <amount>     Set a user's bank balance

  companies                      List all companies
  delcompany <channel_id>        Delete a company and all its data
  setprice <channel_id> <price>  Set a company's fair price
  settreasury <channel_id> <amt> Set a company's treasury

  sql <query>                    Run a raw SQL statement
  help                           Show this menu
  exit / quit                    Exit
"""


def repl():
    db = get_db()
    print(f"Connected to {DB_PATH}")
    print(HELP)

    while True:
        try:
            line = input("db> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break

        if not line:
            continue

        parts = line.split(None, 2)
        cmd = parts[0].lower()

        try:
            if cmd in ("exit", "quit"):
                print("Bye.")
                break

            elif cmd == "help":
                print(HELP)

            elif cmd == "users":
                list_users(db)

            elif cmd == "deluser":
                if len(parts) < 2:
                    print("Usage: deluser <user_id>")
                    continue
                uid = int(parts[1])
                confirm = input(f"Delete user {uid} and all their data? [y/N] ")
                if confirm.lower() == "y":
                    delete_user(db, uid)

            elif cmd == "setcash":
                if len(parts) < 3:
                    print("Usage: setcash <user_id> <amount>")
                    continue
                set_cash(db, int(parts[1]), int(parts[2]))

            elif cmd == "setbank":
                if len(parts) < 3:
                    print("Usage: setbank <user_id> <amount>")
                    continue
                set_bank(db, int(parts[1]), int(parts[2]))

            elif cmd == "companies":
                list_companies(db)

            elif cmd == "delcompany":
                if len(parts) < 2:
                    print("Usage: delcompany <channel_id>")
                    continue
                cid = int(parts[1])
                confirm = input(f"Delete company {cid} and ALL its market data? [y/N] ")
                if confirm.lower() == "y":
                    delete_company(db, cid)

            elif cmd == "setprice":
                if len(parts) < 3:
                    print("Usage: setprice <channel_id> <price>")
                    continue
                set_fair_price(db, int(parts[1]), float(parts[2]))

            elif cmd == "settreasury":
                if len(parts) < 3:
                    print("Usage: settreasury <channel_id> <amount>")
                    continue
                set_treasury(db, int(parts[1]), float(parts[2]))

            elif cmd == "sql":
                if len(parts) < 2:
                    print("Usage: sql <query>")
                    continue
                raw_query(db, line[4:].strip())

            else:
                print(f"Unknown command '{cmd}'. Type 'help' for a list.")

        except ValueError as e:
            print(f"Invalid argument: {e}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")

    db.close()


if __name__ == "__main__":
    repl()
