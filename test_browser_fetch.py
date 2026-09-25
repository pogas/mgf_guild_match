import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from mgf_browser_fetch import BrowserSession, page_selectors
import mgf_guild_export as report

URL = 'https://mgf.gg/contents/guild_info.php?g_name=test'
HTML = '''<div class="guild-hero"><div class="guild-name">test</div></div>
<div class="members-list"><div class="member-row" data-bp="10000000000000000" data-gb="123">
<span class="member-rank">1</span><a class="nick-link" href="/contents/character.php?n=test">test</a>
<div class="member-sub"><img alt="히어로">히어로 | Lv.120</div>
<div class="only-bp"><span class="power-tooltip">1경</span></div></div></div>'''


class BrowserFetchTests(unittest.TestCase):
    def session(self, cache_dir):
        session = BrowserSession(cache_dir)
        session._page = MagicMock()
        session._page.content.return_value = HTML
        return session

    def test_rendered_data_and_timestamp_are_shared_across_generators(self):
        with tempfile.TemporaryDirectory() as tmp:
            session = self.session(Path(tmp))
            _, first = report.parse_guild_page(session, URL)
            _, second = report.parse_guild_page(session, URL)
            self.assertEqual(session._page.goto.call_count, 1)
            self.assertEqual(first, second)
            with BrowserSession(Path(tmp)) as another, patch.object(another, '_start', side_effect=AssertionError('network')):
                _, cached = report.parse_guild_page(another, URL)
            self.assertEqual(first, cached)
            self.assertEqual(cached[0]['tobeol_score_value'], 123)
            self.assertEqual(cached[0]['combat_power_raw'], 10**16)

    def test_challenge_or_missing_members_is_not_saved_as_success(self):
        for html in ('<html>JavaScript를 켜주세요</html>', '<div class="guild-hero"><div class="guild-name">test</div></div>'):
            with self.subTest(html=html), tempfile.TemporaryDirectory() as tmp:
                session = self.session(Path(tmp))
                session._page.content.return_value = html
                with self.assertRaises(RuntimeError):
                    session.get(URL)
                self.assertEqual(list(Path(tmp).iterdir()), [])
                self.assertEqual(session._cache, {})

    def test_navigation_failure_never_populates_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            session = self.session(Path(tmp))
            session._page.goto.side_effect = TimeoutError('blocked')
            with self.assertRaises(RuntimeError):
                session.get(URL)
            self.assertFalse(list(Path(tmp).iterdir()))

    def test_invalid_cached_page_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            session = self.session(Path(tmp))
            session.get(URL)
            path = next(Path(tmp).glob('*.json'))
            cached = json.loads(path.read_text())
            cached['html'] = '<html>blocked</html>'
            path.write_text(json.dumps(cached))
            with BrowserSession(Path(tmp)) as fresh:
                with self.assertRaises(ValueError):
                    fresh.get(URL)

    def test_only_known_public_mgf_routes_are_supported(self):
        for url in ('http://mgf.gg/contents/guild.php', 'https://example.com/contents/guild.php', 'https://mgf.gg/login'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                page_selectors(url)

    def test_browser_is_closed_after_generation_error(self):
        session = BrowserSession()
        browser, runtime = MagicMock(), MagicMock()
        session._browser, session._playwright = browser, runtime
        with self.assertRaises(ValueError):
            with session:
                raise ValueError('generation failed')
        browser.close.assert_called_once()
        runtime.stop.assert_called_once()


if __name__ == '__main__':
    unittest.main()
