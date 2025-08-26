"""Utility helpers for the OpenArchives scraper pipeline."""

import logging
import time
from pathlib import Path
from typing import Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

DEFAULT_DELAY = 1.5


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def setup_logger() -> logging.Logger:
    """Configure and return a root logger reused across modules."""
    logger = logging.getLogger("openarchives")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------

def retry(exceptions, tries: int = 3, delay: float = DEFAULT_DELAY):
    """Simple retry decorator.

    Parameters
    ----------
    exceptions : tuple
        Exception types that trigger a retry.
    tries : int, optional
        How many attempts in total (default 3).
    delay : float, optional
        Seconds to wait between attempts (default DEFAULT_DELAY).
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger("openarchives")
            for attempt in range(1, tries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == tries:
                        logger.error("%s failed after %s attempts: %s", func.__name__, tries, exc)
                        raise
                    logger.warning("%s error (%s/%s): %s", func.__name__, attempt, tries, exc)
                    time.sleep(delay * attempt)  # Exponential-ish backoff
        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Selenium helpers
# ---------------------------------------------------------------------------

def get_driver(headless: bool = True) -> Tuple[webdriver.Chrome, WebDriverWait]:
    """Return a configured Chrome WebDriver and its WebDriverWait helper."""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1200,800")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/119.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 10)
    return driver, wait


@retry(Exception, tries=3, delay=DEFAULT_DELAY)
def safe_get(driver, url: str):
    """Navigate to *url* with retries and return page_source when successful."""
    driver.get(url)
    human_delay()
    return driver.page_source


def human_delay(delay: float = DEFAULT_DELAY):
    """Sleep helper – keeps a single place to tweak polite delays."""
    time.sleep(delay)


def load_mapping(mapping_file: str = "mapping.txt") -> str:
    """Return the raw contents of mapping.txt (used by advanced logic)."""
    file_path = Path(mapping_file)
    if file_path.exists():
        return file_path.read_text(encoding="utf-8")
    return "" 