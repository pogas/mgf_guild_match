import copy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup
from requests import HTTPError

import mgf_guild_export as report


class TobeolReportTests(unittest.TestCase):
    def member(self, nickname="테스트", **overrides):
        return {"nickname": nickname, "level": 120, "job_name": "히어로", "combat_power": "1경",
                "tobeol_score_value": 1_000_000_000_000, "source_fetched_at": "2026-09-25T08:00:00Z",
                **overrides}

    def display(self, members, official_count=None):
        ranking = report._build_tobeol_ranking_analytics(["길드"], {"길드": members})
        return report.build_tobeol_display_ranking("길드", ranking, members, official_count)

    def test_raw_korean_and_man_units_produce_the_same_prediction(self):
        members = [self.member("한글"), self.member("원", combat_power_raw=10**16),
                   self.member("만", combat_power="", combat_power_value=10**12)]
        values = [r["simulation_score_value"] for r in self.display(members)["all_rows"]]
        self.assertEqual(values, [1_188_785_726_057] * 3)

    def test_zero_is_observed_and_totals_use_only_comparable_members(self):
        members = [self.member("기록", tobeol_score_value=100), self.member("영점", tobeol_score_value=0),
                   self.member("미확인", tobeol_score_value=None), self.member("능력치없음", combat_power="")]
        data = self.display(members, official_count=6)
        rows = {r["nickname"]: r for r in data["all_rows"]}
        summary = data["guild_summaries"][0]
        self.assertEqual(rows["영점"]["score"], "0")
        self.assertIsNone(rows["미확인"]["actual_score_value"])
        self.assertIsNone(rows["능력치없음"]["simulation_score_value"])
        self.assertEqual(summary["comparison_count"], 2)
        self.assertEqual(summary["actual_score_total"], 100)
        self.assertEqual(summary["simulation_score_total"], 2 * 1_188_785_726_057)
        self.assertEqual(summary["official_member_count"], 6)
        snapshot = report.build_tobeol_snapshot_data('길드', '2026-09-25', data)['guilds']['길드']
        self.assertEqual(snapshot['count'], 3)
        self.assertEqual(snapshot['best_rank'], 1)
        self.assertNotIn('미확인', snapshot['top10_keys'])

    def test_unobserved_advancement_stage_is_not_shown_as_zero(self):
        row = self.display([self.member(level=20)])["all_rows"][0]
        self.assertIsNone(row["simulation_score_value"])
        self.assertEqual(row["simulation_score"], "예측 불가")
        self.assertIn("unsupported_advancement_stage", row["simulation_flags"])

    def test_same_page_record_overrides_stale_ranking_values(self):
        member = self.member(tobeol_score_value=12345)
        ranking = {"guild_summaries": [{"guild_name": "길드", "count": 1}], "all_rows": [
            {"nickname": "테스트", "guild": "길드", "score": "99조", "rank": 1, "level": "Lv.100", "job": "초보자"}]}
        row = report.build_tobeol_display_ranking("길드", ranking, [member])["all_rows"][0]
        self.assertEqual(row["actual_score_value"], 12345)
        self.assertEqual(row["level"], "Lv.120")
        self.assertEqual(row["job"], "히어로")

    def test_guild_parser_reads_both_exact_metrics(self):
        soup = BeautifulSoup('''<div class="guild-hero"><div class="guild-name">길드</div></div>
        <div class="members-list"><div class="member-row" data-bp="10000000000000000" data-gb="1234567890123">
        <span class="member-rank">1</span><a class="nick-link" href="/contents/character.php?n=테스트">테스트</a>
        <div class="member-sub"><img alt="히어로">히어로 | Lv.120</div><div class="member-power">
        <div class="only-gb"><span class="power-tooltip">1,234,567,890,123</span></div>
        <div class="only-bp"><span class="power-tooltip">1경</span></div></div></div></div>''', 'html.parser')
        with patch.object(report, 'fetch_soup', return_value=soup):
            _, members = report.parse_guild_page(None, 'https://mgf.gg/contents/guild_info.php?g_name=길드')
        self.assertEqual(members[0]['combat_power_raw'], 10**16)
        self.assertEqual(members[0]['combat_power'], '1경')
        self.assertEqual(members[0]['tobeol_score_value'], 1234567890123)

    def test_offline_render_preserves_comparison_and_escapes_names(self):
        source = {"guild_name": "길드", "official_member_count": 3,
                  "fetched_at": "2026-09-25T08:00:00Z", "members": [self.member('<script>x</script>')]}
        with tempfile.TemporaryDirectory() as tmp, patch.object(report, 'fetch_tobeol_ranking', side_effect=AssertionError('network')):
            d = Path(tmp)
            report.generate_tobeol_from_source(source, d/'index.html', d/'tobeol_snapshot.json', '2026-09-25')
            html = (d/'index.html').read_text()
            snapshot = json.loads((d/'tobeol_snapshot.json').read_text())
            self.assertIn('&lt;script&gt;x&lt;/script&gt;', html)
            self.assertNotIn('<script>x</script>', html)
            self.assertIn('실제 점수 합계', html)
            self.assertIn('시뮬레이션 점수 합계', html)
            self.assertIn('공개 멤버 1명 / 길드원 3명', html)
            self.assertEqual(len(BeautifulSoup(html, 'html.parser').select('#tobeol-ranking-tbody tr')), 1)
            self.assertEqual(snapshot['guilds']['길드']['actual_score_total'], 10**12)
            self.assertEqual(snapshot['guilds']['길드']['simulation_score_total'], 1_188_785_726_057)

    def test_history_never_compares_server_and_guild_ranks(self):
        data = self.display([self.member()])
        current = report.build_tobeol_snapshot_data('길드', '2026-09-25', data)
        old = copy.deepcopy(current)
        old['snapshot_date'] = '2026-09-24'
        old['rank_scope'] = 'server_2'
        self.assertFalse(report.build_tobeol_history_analysis(current, [old])['has_previous'])

    def test_live_refresh_and_history_share_one_fetch_without_match_pages(self):
        for guild_name in ('빅딜', '셀린느'):
            with self.subTest(guild=guild_name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                guild = {'guild_name': guild_name, 'member_count': '2', 'server_display': '스카니아 2',
                         'guild_url': f'https://mgf.gg/contents/guild_info.php?g_name={guild_name}'}
                member = self.member(tobeol_score_value=0)
                args = ['mgf_guild_export.py', '--guild-name', guild_name, '--report-mode', 'tobeol',
                        '--snapshot-date', '2026-09-25']
                with patch.object(report, '_HERE', root), redirect_stdout(io.StringIO()), \
                     patch.object(report, 'parse_guild_page', return_value=(guild, [member])) as fetch, \
                     patch.object(report, 'collect_guild_links', side_effect=AssertionError('opponent lookup')), \
                     patch.object(report, 'fetch_tobeol_ranking', side_effect=AssertionError('ranking lookup')):
                    with patch.object(sys, 'argv', args):
                        report.main()
                    latest_dir = root / 'reports' / guild_name
                    with patch.object(sys, 'argv', args + ['--snapshot-mode', 'history', '--tobeol-source',
                                                         str(latest_dir / 'tobeol_source.json')]):
                        report.main()
                    self.assertEqual(fetch.call_count, 1)
                    self.assertEqual(report.extract_query_value(fetch.call_args.args[1], 'g_name'), guild_name)
                    snapshots = [json.loads((directory / 'tobeol_snapshot.json').read_text())
                                 for directory in (latest_dir, latest_dir / 'history' / '2026-09-25')]
                    self.assertEqual(snapshots[0], snapshots[1])
                    self.assertEqual(snapshots[0]['guilds'][guild_name]['actual_score_total'], 0)
                    self.assertGreater(snapshots[0]['guilds'][guild_name]['simulation_score_total'], 0)
                    self.assertEqual(snapshots[0]['source_fetched_at'], member['source_fetched_at'])
                    for directory in (latest_dir, latest_dir / 'history' / '2026-09-25'):
                        html = (directory / 'index.html').read_text()
                        self.assertIn('시뮬레이션 점수 합계', html)
                        self.assertTrue((directory / 'tobeol_source.json').exists())

    def test_live_refresh_failure_preserves_existing_report(self):
        cases = [
            HTTPError('403 Forbidden'),
            ({'guild_name': '길드'}, []),
            ({'guild_name': '다른길드'}, [self.member()]),
            ({'guild_name': '길드'}, [self.member(tobeol_score_value=None)]),
        ]
        for result in cases:
            with self.subTest(result=result), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                directory = root / 'reports' / '길드'
                directory.mkdir(parents=True)
                for name in ('index.html', 'tobeol_snapshot.json', 'tobeol_source.json'):
                    (directory / name).write_text('previous')
                kwargs = {'side_effect': result} if isinstance(result, Exception) else {'return_value': result}
                with patch.object(report, '_HERE', root), patch.object(report, 'parse_guild_page', **kwargs), \
                     patch.object(sys, 'argv', ['mgf_guild_export.py', '--guild-name', '길드', '--report-mode', 'tobeol']):
                    with self.assertRaises((HTTPError, ValueError)):
                        report.main()
                self.assertTrue(all(path.read_text() == 'previous' for path in directory.iterdir()))

    def test_league_and_training_can_preserve_separately_refreshed_tobeol(self):
        guilds = ['길드', '상대1', '상대2', '상대3', '상대4']
        pages = {}
        for name in guilds:
            guild = {'guild_name': name, 'guild_url': name, 'server_display': '스카니아 2',
                     'server_name': '스카니아', 'guild_key': name, 'global_rank': '1', 'server_rank': '1',
                     'guild_level': '10', 'guild_notice': '', 'guild_master_name': name,
                     'member_count': '1', 'guild_power': '1경', 'data_date': '2026.09.25'}
            member = self.member(name, guild_name=name, member_rank_in_guild='1',
                                 character_key=name, character_url='', level='120', is_master='N')
            pages[name] = (guild, [member])
        for mode in ('league', 'training'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                directory = root / 'reports' / '길드'
                directory.mkdir(parents=True)
                boss_files = [directory / name for name in ('index.html', 'tobeol_snapshot.json', 'tobeol_source.json')]
                for path in boss_files:
                    path.write_text('separately refreshed')
                with patch.object(report, '_HERE', root), redirect_stdout(io.StringIO()), \
                     patch.object(report, 'collect_guild_links', return_value=guilds), \
                     patch.object(report, 'parse_guild_page', side_effect=lambda session, url: pages[url]), \
                     patch.object(sys, 'argv', ['mgf_guild_export.py', '--guild-name', '길드', '--report-mode', mode,
                                               '--skip-tobeol', '--fail-on-invalid-data']):
                    report.main()
                self.assertTrue((directory / f'{mode}.html').exists())
                self.assertTrue(all(path.read_text() == 'separately refreshed' for path in boss_files))


if __name__ == '__main__':
    unittest.main()
