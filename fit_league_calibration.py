"""Fit a league performance model from audited samples (NumPy required only here).

Input power uses 만 units, target is the absolute sword-number performance value.
Rank contribution points are NOT a regression target. All validation splits keep
an entire guild out; nested folds keep parameter selection out of reported folds.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from pathlib import Path

import numpy as np

MIN_JOB_SAMPLES = 3
POWER_REFERENCE_MAN = 100_000_000_000  # 1,000조, in 만 units
LEVEL_REFERENCE = 110
CANDIDATES = [(level, ridge) for level in (0, 4, 8, 12, 16, 20, 24) for ridge in (1, 5, 15)]


def design(rows, jobs):
    return np.asarray([[1., math.log(r['combat_power_man'] / POWER_REFERENCE_MAN)]
                       + [float(r['job_name'] == j) for j in jobs] for r in rows])


def fit(rows, level_exponent, ridge, with_jobs=True):
    counts = Counter(r['job_name'] for r in rows)
    jobs = sorted(j for j, n in counts.items() if n >= MIN_JOB_SAMPLES) if with_jobs else []
    x = design(rows, jobs)
    y = np.log([r['observed_performance'] for r in rows])
    y -= level_exponent * np.log([int(r['level']) / LEVEL_REFERENCE for r in rows])
    penalty = np.diag([0., .001] + [ridge] * len(jobs))
    weights = np.ones(len(rows))
    for _ in range(20):
        beta = np.linalg.solve(x.T @ (weights[:, None] * x) + penalty, x.T @ (weights * y))
        if beta[1] < 0:
            raise ValueError('Negative power exponent: model is not suitable for ranking')
        residual = y - x @ beta
        scale = max(.05, 1.4826 * float(np.median(abs(residual - np.median(residual)))))
        updated = np.minimum(1., 1.345 * scale / np.maximum(abs(residual), 1e-9))
        if np.max(abs(weights - updated)) < 1e-5:
            break
        weights = updated
    return dict(intercept_log=float(beta[0]), power_exponent=float(beta[1]),
                level_exponent=level_exponent,
                job_factors={j: float(math.exp(b)) for j, b in zip(jobs, beta[2:])},
                regularization=ridge)


def predict(model, rows):
    return np.asarray([model['intercept_log']
        + model['power_exponent'] * math.log(r['combat_power_man'] / POWER_REFERENCE_MAN)
        + model['level_exponent'] * math.log(int(r['level']) / LEVEL_REFERENCE)
        + math.log(model['job_factors'].get(r['job_name'], 1.)) for r in rows])


def group(row):
    return row['match'] + '/' + row['guild_for_fit']


def cv(rows, params, with_jobs=True):
    out = np.empty(len(rows))
    for guild in sorted(set(map(group, rows))):
        train = [r for r in rows if group(r) != guild]
        indices = [i for i, r in enumerate(rows) if group(r) == guild]
        model = fit(train, *params, with_jobs=with_jobs)
        out[indices] = predict(model, [rows[i] for i in indices])
    return out


def metrics(rows, logs):
    actual = np.asarray([r['observed_performance'] for r in rows])
    errors = logs - np.log(actual)
    pct = abs(np.exp(errors) - 1)
    return dict(count=len(rows), log_rmse=float(np.sqrt(np.mean(errors ** 2))),
                median_abs_percent_error=float(np.median(pct) * 100),
                mean_abs_percent_error=float(np.mean(pct) * 100),
                p90_abs_percent_error=float(np.quantile(pct, .9) * 100))


def choose(rows):
    results = []
    for params in CANDIDATES:
        scores = metrics(rows, cv(rows, params))
        results.append(dict(level_exponent=params[0], ridge=params[1], **scores))
    best = min(results, key=lambda r: (r['log_rmse'], r['level_exponent'], -r['ridge']))
    return (best['level_exponent'], best['ridge']), results


def rank_metrics(rows, logs):
    result = {}
    for match in sorted(set(r['match'] for r in rows)):
        indices = [i for i, r in enumerate(rows) if r['match'] == match]
        actual = sorted(indices, key=lambda i: rows[i]['rank'])
        new = sorted(indices, key=lambda i: -logs[i])
        old = sorted(indices, key=lambda i: -rows[i]['combat_power_man'])
        actual_rank = {i: j + 1 for j, i in enumerate(actual)}
        def mae(order): return float(np.mean([abs(j + 1 - actual_rank[i]) for j, i in enumerate(order)]))
        result[match] = dict(count=len(indices), raw_power_rank_mae=mae(old), calibrated_rank_mae=mae(new))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--samples', type=Path, default=Path('ResourceData/대항전/derived_2026-09-14/league_samples.json'))
    parser.add_argument('--output', type=Path, default=Path('ResourceData/league_calibration_2026-09-14.json'))
    args = parser.parse_args()
    all_rows = json.loads(args.samples.read_text())
    eligible = [r for r in all_rows if r['fit_eligible']]
    counts = Counter(r['job_name'] for r in eligible)
    rows = [r for r in eligible if counts[r['job_name']] >= MIN_JOB_SAMPLES]
    params, candidates = choose(rows)
    nested_logs = np.empty(len(rows))
    folds = []
    for guild in sorted(set(map(group, rows))):
        train = [r for r in rows if group(r) != guild]
        indices = [i for i, r in enumerate(rows) if group(r) == guild]
        fold_params, _ = choose(train)
        test = [rows[i] for i in indices]
        nested_logs[indices] = predict(fit(train, *fold_params), test)
        folds.append(dict(held_out_guild=guild, level_exponent=fold_params[0], ridge=fold_params[1],
                          **metrics(test, nested_logs[indices])))
    model = fit(rows, *params)
    baseline_logs = cv(rows, (0, 1), with_jobs=False)
    model.update(schema_version=1, sample_date='2026-09-14', profile_date='2026-09-16',
        power_unit='man', performance_unit='absolute', power_reference_man=POWER_REFERENCE_MAN,
        level_reference=LEVEL_REFERENCE, sample_count=len(rows), extracted_count=len(all_rows),
        job_sample_counts=dict(counts), pooled_jobs=[j for j, n in counts.items() if n < MIN_JOB_SAMPLES],
        minimum_job_samples=MIN_JOB_SAMPLES,
        level_range=[min(int(r['level']) for r in rows), max(int(r['level']) for r in rows)],
        power_range_man=[min(r['combat_power_man'] for r in rows), max(r['combat_power_man'] for r in rows)],
        source_sha256=hashlib.sha256(args.samples.read_bytes()).hexdigest(),
        formula='exp(intercept_log) * (power_man / power_reference_man)^power_exponent * (level / level_reference)^level_exponent * job_factor',
        limitations=['9/14 performance paired with 9/16 profiles; growth/equipment/job changes may bias estimates.',
                    'At least 3 matching samples required for a job factor; other jobs use pooled factor 1.',
                    'Displayed performance is rounded; scores are predictions, not actual results.',
                    'Validation holds out guilds from the same measurement date; no future-date validation yet.'],
        validation=dict(method='nested leave-one-guild-out', nested=metrics(rows,nested_logs),
                        power_only=metrics(rows,baseline_logs), rank_comparison=rank_metrics(rows,nested_logs)))
    args.output.write_text(json.dumps(model,ensure_ascii=False,indent=2)+'\n')
    predictions=[dict(match=r['match'],guild=r['guild_for_fit'],nickname=r['nickname'],job_name=r['job_name'],
        actual_rank=r['rank'],observed_performance=r['observed_performance'],
        predicted_performance=round(math.exp(predict(model,[r])[0])),
        held_out_prediction=round(math.exp(nested_logs[i]))) for i,r in enumerate(rows)]
    audit=dict(candidates=candidates,folds=folds,predictions=predictions,
        training_fit=metrics(rows,predict(model,rows)),
        training_rank_comparison=rank_metrics(rows,predict(model,rows)))
    args.output.with_name('league_calibration_validation_2026-09-14.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(model,ensure_ascii=False,indent=2))


if __name__ == '__main__':
    main()
