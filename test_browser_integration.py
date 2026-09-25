"""Real Chromium tests with local route fixtures; no requests to MGF."""
import os
from pathlib import Path
import tempfile
import unittest

from mgf_browser_fetch import BrowserSession
from test_browser_fetch import HTML, URL


class BrowserIntegrationTests(unittest.TestCase):
    def test_javascript_rendering_after_initial_403_and_cache_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            with BrowserSession(Path(tmp), headed=os.environ.get('MGF_BROWSER_HEADED') == '1') as session:
                session._start()
                import json
                document = '<html><body>Loading<script>setTimeout(() => {document.body.innerHTML = ' + json.dumps(HTML) + ';}, 150);</script></body></html>'
                session._page.route('https://mgf.gg/**', lambda route: route.fulfill(status=403, content_type='text/html', body=document))
                response = session.get(URL, timeout=5)
                self.assertIn('data-gb="123"', response.text)
                stamp = session.last_fetched_at
            with BrowserSession(Path(tmp)) as cached:
                self.assertEqual(cached.get(URL).text, response.text)
                self.assertEqual(cached.last_fetched_at, stamp)
                self.assertIsNone(cached._browser)

    def test_persistent_check_page_fails_without_creating_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            with BrowserSession(Path(tmp), headed=os.environ.get('MGF_BROWSER_HEADED') == '1') as session:
                session._start()
                session._page.route('https://mgf.gg/**', lambda route: route.fulfill(status=200, content_type='text/html', body='<html>Enable JavaScript</html>'))
                with self.assertRaises(RuntimeError):
                    session.get(URL, timeout=0.5)
            self.assertFalse(list(Path(tmp).iterdir()))


if __name__ == '__main__':
    unittest.main()
