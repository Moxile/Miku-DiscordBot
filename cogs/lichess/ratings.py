from __future__ import annotations
"""Lichess HTTP API helpers and rating-tier role logic."""

import re
import aiohttp


async def fetch_user(session: aiohttp.ClientSession, username: str) -> dict:
    """GET https://lichess.org/api/user/{username} — public profile with perfs."""
    async with session.get(f"https://lichess.org/api/user/{username}") as resp:
        resp.raise_for_status()
        return await resp.json()


async def exchange_code(
    session: aiohttp.ClientSession,
    code: str,
    verifier: str,
    client_id: str,
    redirect_uri: str,
) -> str:
    """Exchange an OAuth2 authorization code for an access token using PKCE."""
    async with session.post(
        "https://lichess.org/api/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "code_verifier": verifier,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()
        return data["access_token"]


async def fetch_account(session: aiohttp.ClientSession, token: str) -> str:
    """GET https://lichess.org/api/account — returns the authenticated username."""
    async with session.get(
        "https://lichess.org/api/account",
        headers={"Authorization": f"Bearer {token}"},
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()
        return data["username"]


def rating_tier(rating: int, min_rating: int, step: int) -> int | None:
    """Return the floored tier for a rating, or None if below min_rating."""
    if rating < min_rating:
        return None
    return (rating // step) * step


def role_name(variant_name: str, tier: int) -> str:
    return f"{variant_name} {tier}"


_RATING_ROLE_RE: dict[str, re.Pattern] = {}


def is_rating_role(role_name_str: str, variant_name: str) -> bool:
    """True if role_name_str matches the pattern '{variant_name} {digits}'."""
    if variant_name not in _RATING_ROLE_RE:
        _RATING_ROLE_RE[variant_name] = re.compile(
            rf"^{re.escape(variant_name)} \d+$"
        )
    return bool(_RATING_ROLE_RE[variant_name].match(role_name_str))
