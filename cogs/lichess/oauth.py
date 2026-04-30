from __future__ import annotations

import base64
import hashlib
import secrets
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Dict, Optional

LICHESS_AUTH_URL = "https://lichess.org/oauth"


def generate_pkce_pair() -> tuple[str, str]:
    """Return (code_verifier, code_challenge) for S256 PKCE."""
    code_verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(code_verifier.encode()).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return code_verifier, code_challenge


def generate_state() -> str:
    return secrets.token_urlsafe(32)


@dataclass
class PendingOAuth:
    discord_user_id: int
    code_verifier: str
    expires_at: float = field(default_factory=lambda: time.monotonic() + 600)


class PendingStore:
    def __init__(self):
        self._store: Dict[str, PendingOAuth] = {}

    def add(self, state: str, pending: PendingOAuth) -> None:
        self._purge_expired()
        self._store[state] = pending

    def pop(self, state: str) -> Optional[PendingOAuth]:
        self._purge_expired()
        return self._store.pop(state, None)

    def _purge_expired(self) -> None:
        now = time.monotonic()
        expired = [k for k, v in self._store.items() if v.expires_at < now]
        for k in expired:
            del self._store[k]


def build_auth_url(client_id: str, redirect_uri: str, state: str, code_challenge: str) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge_method": "S256",
        "code_challenge": code_challenge,
        "state": state,
    }
    return f"{LICHESS_AUTH_URL}?{urllib.parse.urlencode(params)}"
