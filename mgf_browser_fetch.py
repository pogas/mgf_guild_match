"""Render public MGF pages with Chromium; cache only validated pages for one run."""

from datetime import datetime
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import urlparse

from bs4 import BeautifulSoup
import requests


def page_selectors(url: str) -> tuple[str, ...]:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "mgf.gg":
        raise ValueError(f"Unsupported MGF page: {url}")
    if parsed.path == "/contents/guild_info.php":
        return (".guild-hero .guild-name", ".members-list .member-row")
    if parsed.path == "/contents/guild.php":
        return (".g-name a[href*='guild_info.php']",)
    raise ValueError(f"Unsupported MGF page: {url}")


class BrowserSession:
    def __init__(self, cache_dir: Path | None = None, headed: bool = False):
        self.cache_dir = cache_dir
        self.headed = headed
        self.last_fetched_at = ""
        self._playwright = None
        self._browser = None
        self._page = None
        self._last_navigation = 0.0
        self._cache = {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        try:
            if self._browser is not None:
                self._browser.close()
        finally:
            if self._playwright is not None:
                self._playwright.stop()
            self._page = self._browser = self._playwright = None

    def _start(self):
        from playwright.sync_api import sync_playwright

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(channel="chromium", headless=not self.headed)
        context = self._browser.new_context(locale="ko-KR", timezone_id="Asia/Seoul")
        self._page = context.new_page()

    def get(self, url: str, timeout: float = 45) -> requests.Response:
        selectors = page_selectors(url)
        cache_path = None
        if self.cache_dir is not None:
            cache_path = self.cache_dir / (hashlib.sha256(url.encode()).hexdigest() + ".json")
        record = self._cache.get(url)
        if record is None and cache_path is not None and cache_path.exists():
            record = json.loads(cache_path.read_text(encoding="utf-8"))
        if record is not None:
            if record.get("url") != url or not record.get("fetched_at"):
                raise ValueError(f"Invalid browser cache: {url}")
            self._validate_html(url, record["html"], selectors)
            print(f"Browser cache: {url}", flush=True)
        else:
            if self._page is None:
                self._start()
            # Keep public page navigation sequential and avoid burst requests.
            delay = 2.0 - (time.monotonic() - self._last_navigation)
            if delay > 0:
                time.sleep(delay)
            self._last_navigation = time.monotonic()
            try:
                self._page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)
                # A 403 browser-check page can navigate itself after running JS.
                # Only real report DOM is accepted; an HTTP 200 alone is insufficient.
                for selector in selectors:
                    self._page.locator(selector).first.wait_for(state="visible", timeout=timeout * 1000)
                html = self._page.content()
                self._validate_html(url, html, selectors)
            except Exception as exc:
                try:
                    title = self._page.title()
                except Exception:
                    title = "unavailable"
                raise RuntimeError(
                    f"MGF browser page unavailable (title={title!r}); existing report will be kept: {url}"
                ) from exc
            record = {"url": url, "html": html, "fetched_at": datetime.now().astimezone().isoformat()}
            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                temporary = cache_path.with_suffix(".tmp")
                temporary.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
                temporary.replace(cache_path)
            print(f"Browser fetched: {url}", flush=True)
        self._cache[url] = record
        self.last_fetched_at = record["fetched_at"]
        response = requests.Response()
        response.url = url
        response.status_code = 200
        response.encoding = "utf-8"
        response._content = record["html"].encode("utf-8")
        return response

    @staticmethod
    def _validate_html(url, html, selectors):
        soup = BeautifulSoup(html, "html.parser")
        if not all(soup.select_one(selector) for selector in selectors):
            raise ValueError(f"Expected guild data is missing: {url}")
