from __future__ import annotations

from typing import Any, Dict, List

import aiohttp

LICHESS_TOKEN_URL = "https://lichess.org/api/token"
LICHESS_ACCOUNT_URL = "https://lichess.org/api/account"


async def exchange_code(
    session: aiohttp.ClientSession,
    code: str,
    code_verifier: str,
    redirect_uri: str,
    client_id: str,
) -> Dict[str, Any]:
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "code_verifier": code_verifier,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
    }
    async with session.post(LICHESS_TOKEN_URL, data=payload) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise ValueError(f"Token exchange failed ({resp.status}): {text}")
        return await resp.json()


async def fetch_account(session: aiohttp.ClientSession, access_token: str) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {access_token}"}
    async with session.get(LICHESS_ACCOUNT_URL, headers=headers) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise ValueError(f"Account fetch failed ({resp.status}): {text}")
        return await resp.json()


def extract_ratings(account: Dict[str, Any], variant_keys: List[str]) -> List[Dict[str, Any]]:
    perfs = account.get("perfs", {})
    results = []
    for key in variant_keys:
        perf = perfs.get(key, {})
        results.append({
            "variant": key,
            "rating": perf.get("rating", 1500),
            "games": perf.get("games", 0),
            "prov": perf.get("prov", False),
        })
    return results
