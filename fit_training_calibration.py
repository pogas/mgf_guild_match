"""Reproducible training-content calibration; NumPy needed only for fitting."""
from __future__ import annotations
import hashlib,json,math
from collections import Counter
from pathlib import Path
import numpy as np
from fit_league_calibration import fit,predict,metrics

ROOT=Path(__file__).parent
SAMPLES=ROOT/'ResourceData/수련장/derived_2026-09-17/training_samples.json'
OUTPUT=ROOT/'ResourceData/training_calibration_2026-09-17.json'
PREVIOUS=ROOT/'ResourceData/training_calibration_2026-09-17_v1.json'
CANDIDATES=[(level,ridge) for level in (0,.5,1,2,4,8,12,16) for ridge in (1,5,15)]


def split(rows,k=5):
    counts=Counter(r['guild_for_fit'] for r in rows)
    loads=[0]*k; assignments={}
    for guild,n in sorted(counts.items(),key=lambda x:(-x[1],x[0])):
        fold=min(range(k),key=lambda j:loads[j]);assignments[guild]=fold;loads[fold]+=n
    return np.array([assignments[r['guild_for_fit']] for r in rows])


def choose(rows):
    folds=split(rows,4);results=[]
    for params in CANDIDATES:
        predictions=np.empty(len(rows))
        invalid=False
        for fold in range(4):
            train=[r for i,r in enumerate(rows) if folds[i]!=fold]
            indices=np.flatnonzero(folds==fold)
            try:
                predictions[indices]=predict(fit(train,*params),[rows[i] for i in indices])
            except ValueError:
                invalid=True
                break
        if invalid:
            continue  # Reject candidates that reverse the power/score relationship.
        results.append(dict(level_exponent=params[0],ridge=params[1],**metrics(rows,predictions)))
    best=min(results,key=lambda r:(r['log_rmse'],r['level_exponent'],-r['ridge']))
    return (best['level_exponent'],best['ridge']),results


def main():
    allrows=json.loads(SAMPLES.read_text())
    eligible=[r for r in allrows if r['fit_eligible'] and int(r['level'])>=100]
    counts=Counter(r['job_name'] for r in eligible)
    rows=[r for r in eligible if counts[r['job_name']]>=3]
    params,candidates=choose(rows);model=fit(rows,*params)
    outer=split(rows);logs=np.empty(len(rows));fold_results=[]
    for fold in range(5):
        indices=np.flatnonzero(outer==fold)
        train=[r for i,r in enumerate(rows) if outer[i]!=fold]
        chosen,_=choose(train)
        logs[indices]=predict(fit(train,*chosen),[rows[i] for i in indices])
        fold_results.append(dict(fold=fold,guilds=sorted({rows[i]['guild_for_fit'] for i in indices}),level_exponent=chosen[0],ridge=chosen[1]))
    oldlogs=np.log([r['legacy_prediction'] for r in rows])
    source_metrics={source:dict(new=metrics([r for i,r in enumerate(rows) if r['source_kind']==source],logs[[i for i,r in enumerate(rows) if r['source_kind']==source]]),legacy=metrics([r for r in rows if r['source_kind']==source],oldlogs[[i for i,r in enumerate(rows) if r['source_kind']==source]])) for source in ['image','xlsx']}
    job_metrics={j:metrics([r for r in rows if r['job_name']==j],logs[[i for i,r in enumerate(rows) if r['job_name']==j]]) for j in counts}
    source_holdout={}
    for source in ['image','xlsx']:
        train=[r for r in rows if r['source_kind']!=source];test=[r for r in rows if r['source_kind']==source]
        chosen,_=choose(train)
        source_holdout[source]=metrics(test,predict(fit(train,*chosen),test))
    model.update(schema_version=1,model_date='2026-09-17',sample_date=None,sample_date_note='Unconfirmed; screenshots named 2026-09-16',profile_retrieved_date='2026-09-17',power_unit='man',score_unit='points',power_reference_man=100_000_000_000,level_reference=110,sample_count=len(rows),extracted_count=len(allrows),job_sample_counts=dict(counts),minimum_job_samples=3,level_range=[min(int(r['level']) for r in rows),max(int(r['level']) for r in rows)],power_range_man=[min(r['combat_power_man'] for r in rows),max(r['combat_power_man'] for r in rows)],source_sha256=hashlib.sha256(SAMPLES.read_bytes()).hexdigest(),formula='exp(intercept_log)*(power_man/power_reference_man)^power_exponent*(level/level_reference)^level_exponent*job_factor',validation=dict(method='nested 5-fold guild-grouped CV; 4-fold inner model selection',new=metrics(rows,logs),legacy=metrics(rows,oldlogs),by_source=source_metrics,by_job=job_metrics,source_holdout=source_holdout),limitations=['Sample measurement date unconfirmed; power uses 9/17 retrieval with each profile source date preserved.','Screenshots are top-ranked and Excel covers a different score/level range; selection bias remains.','Fewer than 3 samples per job and Lv<100 are not fitted. Unsupported jobs use pooled factor 1, outside-range estimates flagged.','Validation is across guilds, not future measurement dates.'])
    model['revision']=3
    model['nickname_aliases']=json.loads(SAMPLES.with_name('nickname_aliases.json').read_text())['aliases']
    model['source_counts']=dict(Counter(r['source_kind'] for r in allrows))
    manifest=json.loads(SAMPLES.with_name('extraction_manifest.json').read_text())
    model['image_file_count']=manifest['image_count']
    model['duplicate_rows_removed']=manifest['duplicate_rows']
    model['validation']['guild_grouping']='Verified profile guild when available, so guild rename/glyph differences do not split one guild across folds.'
    model['limitations'][1]='Screenshots include global top 220 and selected guild-match ranks 10-114. Excel covers a different score/level range; selection bias remains.'
    model['validation']['fitted']=metrics(rows,predict(model,rows))
    previous=json.loads(PREVIOUS.read_text())
    added=[i for i,r in enumerate(rows) if r.get('added_in_refresh')]
    added_rows=[rows[i] for i in added]
    model['validation']['additional_samples_comparison']={
        'method':'Previous fixed model versus new guild-held-out predictions on newly added characters only; previous model may have seen other members of their guilds.',
        'previous':metrics(added_rows,predict(previous,added_rows)),
        'new_held_out':metrics(added_rows,logs[added]),
    }
    OUTPUT.write_text(json.dumps(model,ensure_ascii=False,indent=2)+'\n')
    audit=dict(candidates=candidates,folds=fold_results,predictions=[dict(nickname=r['nickname'],guild=r['guild_for_fit'],source=r['source_kind'],job=r['job_name'],observed_score=r['observed_score'],legacy_prediction=r['legacy_prediction'],held_out_prediction=round(math.exp(logs[i])),fitted_prediction=round(math.exp(predict(model,[r])[0]))) for i,r in enumerate(rows)])
    OUTPUT.with_name('training_calibration_validation_2026-09-17.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(model,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
