import copy
import hashlib
import json
import unittest
from pathlib import Path

import mgf_guild_export as export
from league_calibration import canonical_job, estimate_league_performance, load_league_model

ROOT = Path(__file__).parent
SAMPLES = ROOT / 'ResourceData/대항전/derived_2026-09-14/league_samples.json'


class LeagueCalibrationTests(unittest.TestCase):
    def test_audited_samples_and_prediction_units(self):
        model = load_league_model()
        self.assertEqual(hashlib.sha256(SAMPLES.read_bytes()).hexdigest(), model['source_sha256'])
        rows = json.loads(SAMPLES.read_text())
        audit = json.loads((ROOT / 'ResourceData/league_calibration_validation_2026-09-14.json').read_text())
        self.assertEqual(len(rows), 293)
        self.assertEqual(sum(not r['fit_eligible'] for r in rows), 12)
        fitted = {(r['match'], r['nickname']): r for r in rows}
        self.assertEqual(len(audit['predictions']), 280)
        for p in audit['predictions']:
            r = fitted[p['match'], p['nickname']]
            self.assertEqual(export.power_to_man_units(r['combat_power']), r['combat_power_man'])
            estimate = estimate_league_performance(int(r['level']), r['combat_power_man'], r['job_name'])
            self.assertAlmostEqual(estimate['value'], p['predicted_performance'], delta=1)

    def test_aliases_and_unsupported_inputs(self):
        self.assertEqual(canonical_job('아크메이지 (불, 독)'), canonical_job('불독'))
        self.assertEqual(canonical_job('새도어'), '섀도어')
        pooled = estimate_league_performance(94, 10**11, '나이트워커')
        self.assertEqual(pooled['job_factor'], 1)
        self.assertIn('pooled_job', pooled['status'])
        self.assertIn('level_extrapolation', pooled['status'])
        self.assertIn('missing_level', estimate_league_performance(0, 10**11, '비숍')['status'])
        self.assertEqual(estimate_league_performance(110, 0, '비숍')['value'], 0)

    def test_both_new_jobs_explicitly_use_common_factor(self):
        for level in [94, 99, 100, 110]:
            predictions=[estimate_league_performance(level,10**11,job)
                         for job in ['윈드브레이커','나이트워커','윈브','나워']]
            self.assertEqual(len({p['value'] for p in predictions}),1)
            for p in predictions:
                self.assertEqual(p['job_factor'],1.0)
                self.assertIn('pooled_job',p['status'])

    def test_contributions_match_both_sources_and_boundaries(self):
        table = export.parse_score_table(export.SCORE_TABLE_PATH)
        points = {r['rank']: r['score'] for r in table}
        self.assertEqual(len(points), 150)
        for r in json.loads(SAMPLES.read_text()):
            self.assertEqual(points[r['rank']], r['contribution'])
        self.assertEqual([points[i] for i in [1, 29, 30, 147, 148, 150]],
                         [1500000, 170000, 160000, 27400, 26700, 25300])
        self.assertEqual((ROOT / '길드 대항전 점수표.txt').read_bytes(),
                         (ROOT / 'ResourceData/길드 대항전 점수표.txt').read_bytes())

    def test_ranking_uses_level_and_job_then_assigns_contribution(self):
        members = {'G': [dict(guild_name='G', nickname=n, level=l, job_name=j,
                             combat_power='1000조', character_url='')
                         for n, l, j in [('A', 110, '비숍'), ('B', 110, '보우마스터'), ('C', 115, '비숍')]]}
        sim = export.build_guild_war_simulation(members, export.parse_score_table(export.SCORE_TABLE_PATH))
        self.assertEqual([r['nickname'] for r in sim['ranked_members']], ['C', 'B', 'A'])
        self.assertEqual(sim['guild_rankings'][0]['total_score'], 3650000)
        self.assertEqual(sim['score_table_preview'][0]['range'], '1,500,000 → 560,000')
        many = {'G': [dict(members['G'][0], nickname=str(i)) for i in range(151)]}
        result = export.build_guild_war_simulation(many, export.parse_score_table(export.SCORE_TABLE_PATH))
        self.assertEqual(result['ranked_members'][-1]['score'], 0)
        self.assertEqual(result['guild_rankings'][0]['scoring_count'], 150)

    def test_history_rebases_without_mutating_original_or_training(self):
        snapshot = json.loads((ROOT / 'reports/빅딜/snapshot.json').read_text())
        old = copy.deepcopy(snapshot)
        old['guilds']['빅딜']['simulation_score'] = 123
        before = copy.deepcopy(old)
        updated = export.rebase_league_snapshot(old)
        self.assertEqual(old, before)
        self.assertNotEqual(updated['guilds']['빅딜']['simulation_score'], 123)
        self.assertEqual(updated['guilds'], export.rebase_league_snapshot(snapshot)['guilds'])
        training = dict(old, report_mode='training')
        self.assertIs(export.rebase_league_snapshot(training), training)


if __name__ == '__main__':
    unittest.main()
