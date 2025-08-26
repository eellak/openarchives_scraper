"""User-Agent rotation utilities.

Provides a reasonably diverse set of modern desktop/mobile user agents and
helpers to pick one at random or deterministically (e.g., per-domain).
"""

from __future__ import annotations

import os
import random
import hashlib
from typing import List


USER_AGENTS: List[str] = [
    # Desktop Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    # Desktop Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    # Desktop Edge (Chromium)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
    # Mobile iOS Safari (iPhone)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    # Mobile Android Chrome
    "Mozilla/5.0 (Linux; Android 14; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36",
]


_RNG = random.Random()
_RNG.seed(os.urandom(16))


def random_user_agent() -> str:
    return _RNG.choice(USER_AGENTS)


def deterministic_user_agent(key: str) -> str:
    # Seed a temporary RNG based on a stable hash of the key (process-independent)
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    seed_int = int.from_bytes(digest, "big")
    r = random.Random(seed_int)
    return r.choice(USER_AGENTS)
