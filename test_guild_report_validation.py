import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup

import mgf_guild_export as report


class GuildMatchValidationTests(unittest.TestCase):
    def page(self, declared_count, actual_count):
        badge = f'<span>총 {declared_count}개 길드가 매칭되었습니다.</span>' if declared_count is not None else ''
        links = ''.join(f'<a href="/contents/guild_info.php?g_name=길드{i}">길드{i}</a>' * 2
                        for i in range(actual_count))
        return BeautifulSoup(badge + links, 'html.parser')

    def test_four_guild_match_is_valid_when_site_declares_four(self):
        with patch.object(report, 'fetch_soup', return_value=self.page(4, 4)):
            links = report.collect_guild_links(None, 'https://mgf.gg/contents/guild.php')
        self.assertEqual(len(links), 4)
        names = [f'길드{i}' for i in range(4)]
        errors = report.validate_report_data('길드0', [{'guild_name': n} for n in names],
                                            {n: [{'nickname': n}] for n in names}, len(links))
        self.assertEqual(errors, [])

    def test_incomplete_or_unverifiable_match_is_rejected(self):
        for declared, actual in ((5, 4), (4, 5), (0, 0), (None, 4)):
            with self.subTest(declared=declared, actual=actual), \
                 patch.object(report, 'fetch_soup', return_value=self.page(declared, actual)):
                with self.assertRaises(ValueError):
                    report.collect_guild_links(None, 'https://mgf.gg/contents/guild.php')

    def test_missing_seed_members_and_detail_rows_are_still_rejected(self):
        errors = report.validate_report_data('길드0', [{'guild_name': '길드1'}], {'길드1': []}, 4)
        self.assertTrue(any('seed guild missing' in error for error in errors))
        self.assertTrue(any('count mismatch' in error for error in errors))
        self.assertTrue(any('no members' in error for error in errors))


if __name__ == '__main__':
    unittest.main()
