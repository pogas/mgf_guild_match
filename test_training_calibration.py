import copy
import hashlib
import json
import unittest
from pathlib import Path
import mgf_guild_export as e
from training_calibration import estimate_training_performance,load_training_model

ROOT=Path(__file__).parent
SAMPLES=ROOT/'ResourceData/수련장/derived_2026-09-17/training_samples.json'

class TrainingCalibrationTests(unittest.TestCase):
    def test_source_integrity_units_and_runtime_predictions(self):
        model=load_training_model()
        self.assertEqual(hashlib.sha256(SAMPLES.read_bytes()).hexdigest(),model['source_sha256'])
        rows=json.loads(SAMPLES.read_text());by_name={r['nickname']:r for r in rows}
        self.assertEqual(len(rows),514)
        self.assertEqual(sum(r['fit_eligible'] for r in rows),509)
        self.assertEqual(len({r['nickname'] for r in rows}),len(rows))
        self.assertEqual(sum(r['nickname']=='We짱구' for r in rows),1)
        self.assertEqual(by_name['냐락쿵']['observed_score'],92229387)
        self.assertEqual(by_name['팔꾸미']['observed_score'],76600008)
        audit=json.loads((ROOT/'ResourceData/training_calibration_validation_2026-09-17.json').read_text())
        self.assertEqual(len(audit['predictions']),508)
        self.assertEqual(by_name['망겜감별사']['observed_score'],24127506)
        self.assertEqual(by_name['빔냥이']['observed_score'],36373047)
        for original,current in {'익맹이':'밉상b','아이스꾸임':'익맹'}.items():
            self.assertTrue(by_name[original]['fit_eligible'])
            self.assertEqual(by_name[original]['matched_nickname'],current)
            self.assertEqual(by_name[original]['profile_guild'],'셀린느')
            self.assertGreater(by_name[original]['combat_power_man'],0)
        self.assertEqual(sum(r['guild']=='셀린느' for r in rows),26)
        self.assertEqual(model['image_file_count'],65)
        duplicates=json.loads(SAMPLES.with_name('duplicates.json').read_text())
        self.assertEqual(len(duplicates),6)
        sources={r['source'] for r in rows if r['source_kind']=='image'}
        sources.update(r['duplicate']['source'] for r in duplicates)
        self.assertEqual(len(sources),65)
        for pred in audit['predictions']:
            row=by_name[pred['nickname']]
            self.assertEqual(e.power_to_man_units(row['combat_power']),row['combat_power_man'])
            actual=estimate_training_performance(int(row['level']),row['combat_power_man'],row['job_name'])
            self.assertAlmostEqual(actual['value'],pred['fitted_prediction'],delta=1)

    def test_new_jobs_aliases_and_missing_inputs(self):
        self.assertEqual(estimate_training_performance(110,10**11,'불독'),estimate_training_performance(110,10**11,'아크메이지(불,독)'))
        for job in ['바이퍼','캡틴']:
            self.assertGreater(e.estimate_training_score(110,10**11,job),1_000_000)
        unknown=estimate_training_performance(110,10**11,'나이트워커')
        self.assertEqual(unknown['job_factor'],1)
        self.assertIn('pooled_job',unknown['status'])
        self.assertEqual(e.estimate_training_score(0,10**11,'비숍'),0)
        self.assertEqual(e.estimate_training_score(110,0,'비숍'),0)
        self.assertIn('level_extrapolation',estimate_training_performance(132,10**11,'비숍')['status'])

    def test_third_job_keeps_legacy_and_monotonic_predictions(self):
        self.assertEqual(e.estimate_training_score(99,10**10,'비숍'),e.estimate_training_score_legacy(99,10**10,'비숍'))
        self.assertGreater(e.estimate_training_score(111,10**11,'비숍'),e.estimate_training_score(110,10**11,'비숍'))
        self.assertGreater(e.estimate_training_score(110,2*10**11,'비숍'),e.estimate_training_score(110,10**11,'비숍'))

    def test_guild_totals_and_bishop_relative_display(self):
        members={'G':[dict(guild_name='G',nickname=j,job_name=j,level=110,combat_power='1000조') for j in ['비숍','캡틴','바이퍼']]}
        sim=e.build_training_simulation(members)
        self.assertEqual(sim['guild_rankings'][0]['total_score'],sum(m['score'] for m in sim['ranked_members']))
        bishop=next(r for r in sim['job_coefficient_cards']['4th'] if r['label']=='비숍')
        self.assertEqual(bishop['range'],'×1.000')
        self.assertEqual(len(sim['job_coefficient_cards']['4th']),14)

    def test_new_jobs_use_pooled_formula_on_both_sides_of_level_100(self):
        for level in [94, 99, 100, 110]:
            scores=[]
            for job in ['윈드브레이커', '나이트워커', '윈브', '나워']:
                prediction=estimate_training_performance(level,10**11,job)
                self.assertEqual(prediction['job_factor'],1.0)
                self.assertIn('pooled_job',prediction['status'])
                self.assertEqual(e.estimate_training_score(level,10**11,job),prediction['value'])
                scores.append(prediction['value'])
                sim=e.build_training_simulation({'G':[dict(guild_name='G',nickname='test',job_name=job,level=level,combat_power='1000조')]})
                self.assertEqual(sim['ranked_members'][0]['score'],prediction['value'])
                self.assertIn('pooled_job',sim['ranked_members'][0]['calibration_status'])
            self.assertEqual(len(set(scores)),1)

    def test_historical_rebase_preserves_source_and_league(self):
        s=json.loads((ROOT/'reports/빅딜/training_snapshot.json').read_text())
        original=copy.deepcopy(s);s['guilds']['빅딜']['simulation_score']=1
        before=copy.deepcopy(s);rebased=e.rebase_training_snapshot(s)
        self.assertEqual(s,before)
        self.assertEqual(rebased['guilds'],e.rebase_training_snapshot(original)['guilds'])
        league=dict(s,report_mode='league')
        self.assertIs(e.rebase_training_snapshot(league),league)

if __name__=='__main__':unittest.main()
