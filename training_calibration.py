"""수련장 4차 직업 실측 보정. 전투력 입력은 만 단위."""
from __future__ import annotations
import json
import math
from functools import lru_cache
from pathlib import Path
from league_calibration import canonical_job, DEFAULT_JOB_FACTORS

MODEL_PATH=Path(__file__).parent/'ResourceData/training_calibration_2026-09-17.json'

@lru_cache(maxsize=1)
def load_training_model():
    model=json.loads(MODEL_PATH.read_text(encoding='utf-8'))
    if model['schema_version']!=1 or model['power_unit']!='man':
        raise ValueError('Unsupported training model schema/unit')
    return model

def estimate_training_performance(level: int, combat_power_man: int, job: str):
    model=load_training_model()
    if level<=0 or combat_power_man<=0:
        return {'value':0,'status':'missing_input','job_factor':1.0}
    name=canonical_job(job)
    factor=DEFAULT_JOB_FACTORS.get(name,model['job_factors'].get(name,1.0));flags=[]
    if name in DEFAULT_JOB_FACTORS or name not in model['job_factors']:flags.append('pooled_job')
    if not model['level_range'][0]<=level<=model['level_range'][1]:flags.append('level_extrapolation')
    if not model['power_range_man'][0]<=combat_power_man<=model['power_range_man'][1]:flags.append('power_extrapolation')
    log_score=(model['intercept_log']+model['power_exponent']*math.log(combat_power_man/model['power_reference_man'])
               +model['level_exponent']*math.log(level/model['level_reference'])+math.log(factor))
    return {'value':round(math.exp(log_score)),'status':','.join(flags) or 'calibrated','job_factor':factor}
