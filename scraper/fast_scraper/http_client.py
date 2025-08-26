"""HTTP client utilities optimized for speed and reliability."""

from typing import Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_TIMEOUT: Tuple[float, float] = (5.0, 15.0)


def create_session(user_agent: str = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0 Safari/537.36"
)) -> requests.Session:
    """Return a configured requests Session with keep-alive and retries."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })

    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)

    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_html(session: requests.Session, url: str, timeout: Tuple[float, float] = DEFAULT_TIMEOUT) -> str:
    """GET the URL and return decoded text, raising for HTTP errors."""
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding
    return response.text


def fetch_html_with_meta(
    session: requests.Session,
    url: str,
    timeout: Tuple[float, float] = DEFAULT_TIMEOUT,
) -> Tuple[str, int, float]:
    """GET the URL and return (text, status_code, elapsed_seconds)."""
    response = session.get(url, timeout=timeout)
    status = response.status_code
    response.encoding = response.apparent_encoding or response.encoding
    return response.text, status, response.elapsed.total_seconds()


