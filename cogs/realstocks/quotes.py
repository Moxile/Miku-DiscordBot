"""Finnhub quote client with a shared in-memory cache, plus lot-size pricing.

All guilds share one cache entry per symbol, so N servers trading NVDA cost one
API call per TTL window. Quotes are cached in USD; conversion to whole-coin
prices happens through the lot-size helpers below.

Lot sizes exist because the bot currency is integer-only at a 1:1 dollar value:
a $0.43 penny stock trades in units of 100 shares (43 coins/unit) so price
moves stay visible after rounding. The lot is frozen when the symbol is first
added and never changes, keeping existing holdings consistent.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import aiohttp

from config import REALSTOCK_MIN_UNIT_PRICE, REALSTOCK_MAX_LOT

FINNHUB_BASE = "https://finnhub.io/api/v1"


class QuoteError(Exception):
    """The quote API failed (network, rate limit, bad key)."""


class UnknownSymbolError(QuoteError):
    """Finnhub doesn't know this ticker."""


@dataclass
class Quote:
    price: float        # last traded price, USD
    prev_close: float   # previous session close, USD
    fetched_at: float   # time.monotonic() of the fetch
    exchange_ts: float  # Finnhub's last-trade timestamp (unix seconds); 0 if unknown

    def age_seconds(self) -> float:
        """Seconds since the last real trade behind this price. Large when the market
        is closed, pre-open, halted, or the feed hasn't ticked — used to block trading
        on a stale price. Returns +inf when the exchange timestamp is unknown."""
        if not self.exchange_ts:
            return float("inf")
        return max(0.0, time.time() - self.exchange_ts)


@dataclass
class CompanyProfile:
    """Slow-moving fundamentals — cached in the DB rather than fetched per view."""
    industry: str | None      # e.g. "Semiconductors"
    domain: str | None        # e.g. "nvidia.com"
    market_cap: float | None  # millions of USD, per Finnhub's profile2
    eps: float | None         # trailing-twelve-month EPS


def lot_size_for(price_usd: float) -> int:
    """Smallest power of 10 making one unit cost at least REALSTOCK_MIN_UNIT_PRICE coins."""
    lot = 1
    while price_usd * lot < REALSTOCK_MIN_UNIT_PRICE and lot < REALSTOCK_MAX_LOT:
        lot *= 10
    return lot


def unit_buy_price(price_usd: float, lot_size: int) -> int:
    """Coins to buy one unit — rounded up so round-trips always favor the house."""
    return max(1, math.ceil(price_usd * lot_size))


def unit_sell_price(price_usd: float, lot_size: int) -> int:
    """Coins received for selling one unit — rounded down (house keeps the fraction)."""
    return max(0, math.floor(price_usd * lot_size))


def unit_mid_price(price_usd: float, lot_size: int) -> int:
    """Rounded unit price used for charts and display."""
    return max(1, round(price_usd * lot_size))


class QuoteService:
    def __init__(self, api_key: str | None, ttl_seconds: int):
        self.api_key = api_key
        self.ttl = ttl_seconds
        self._cache: dict[str, Quote] = {}
        self._session: aiohttp.ClientSession | None = None

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get(self, path: str, params: dict) -> dict:
        if not self.api_key:
            raise QuoteError("FINNHUB_API_KEY is not configured.")
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10))
        try:
            async with self._session.get(
                f"{FINNHUB_BASE}{path}", params={**params, "token": self.api_key},
            ) as resp:
                if resp.status == 429:
                    raise QuoteError("Quote API rate limit hit — try again in a minute.")
                if resp.status in (401, 403):
                    raise QuoteError("Quote API rejected the key — check FINNHUB_API_KEY.")
                if resp.status != 200:
                    raise QuoteError(f"Quote API error (HTTP {resp.status}).")
                return await resp.json()
        except aiohttp.ClientError as e:
            raise QuoteError(f"Could not reach the quote API: {e}") from e

    async def get_quote(self, symbol: str, *, max_age: float | None = None) -> Quote:
        """Return a quote for `symbol`, from cache when fresher than `max_age` (default TTL)."""
        cached = self._cache.get(symbol)
        ttl = self.ttl if max_age is None else max_age
        if cached and time.monotonic() - cached.fetched_at < ttl:
            return cached

        data = await self._get("/quote", {"symbol": symbol})
        # Finnhub reports unknown tickers as an all-zero quote rather than an error.
        if not data.get("c") and not data.get("t"):
            raise UnknownSymbolError(f"Unknown ticker: {symbol}")
        quote = Quote(price=float(data["c"]),
                      prev_close=float(data.get("pc") or data["c"]),
                      fetched_at=time.monotonic(),
                      exchange_ts=float(data.get("t") or 0))
        self._cache[symbol] = quote
        return quote

    async def lookup_name(self, symbol: str) -> str | None:
        """Company name from the profile endpoint, or None when unavailable."""
        try:
            data = await self._get("/stock/profile2", {"symbol": symbol})
        except QuoteError:
            return None
        return data.get("name") or None

    async def lookup_profile(self, symbol: str) -> CompanyProfile:
        """Fundamentals for the detail view — best effort, individual fields may be
        None if Finnhub doesn't have them (or the metric endpoint isn't on-plan)."""
        industry = domain = market_cap = eps = None
        try:
            data = await self._get("/stock/profile2", {"symbol": symbol})
            industry = data.get("finnhubIndustry") or None
            domain = self._domain_from_url(data.get("weburl"))
            market_cap = data.get("marketCapitalization") or None
        except QuoteError:
            pass
        try:
            data = await self._get("/stock/metric", {"symbol": symbol, "metric": "all"})
            eps = (data.get("metric") or {}).get("epsTTM")
        except QuoteError:
            pass
        return CompanyProfile(industry=industry, domain=domain, market_cap=market_cap, eps=eps)

    @staticmethod
    def _domain_from_url(weburl: str | None) -> str | None:
        if not weburl:
            return None
        netloc = urlparse(weburl).netloc.removeprefix("www.")
        return netloc or None
