"""대항전 실측 지표 예측. 전투력 입력 단위는 기존 생성기와 같은 만 단위."""
from __future__ import annotations

import json
import math
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

MODEL_PATH = Path(__file__).parent / 'ResourceData' / 'league_calibration_2026-09-14.json'
# 실측 계수를 확정하기 전까지 두 신규 직업은 공통 기준을 사용한다.
DEFAULT_JOB_FACTORS = {'윈드브레이커': 1.0, '나이트워커': 1.0}


def canonical_job(job: str) -> str:
    compact = re.sub(r'[\s(),\-]', '', str(job))
    aliases = {
        '불독': '아크메이지(불,독)', '아크메이지불독': '아크메이지(불,독)',
        '썬콜': '아크메이지(썬,콜)', '아크메이지썬콜': '아크메이지(썬,콜)',
        '새도어': '섀도어',
        '윈브': '윈드브레이커', '나워': '나이트워커',
    }
    return aliases.get(compact, compact)


@lru_cache(maxsize=1)
def load_league_model() -> dict[str, Any]:
    model = json.loads(MODEL_PATH.read_text(encoding='utf-8'))
    if model['schema_version'] != 1 or model['power_unit'] != 'man':
        raise ValueError('Unsupported league calibration schema or power unit')
    return model


def estimate_league_performance(level: int, combat_power_man: int, job: str) -> dict[str, Any]:
    model = load_league_model()
    if combat_power_man <= 0:
        return {'value': 0, 'job_factor': 1.0, 'status': 'missing_power'}
    name = canonical_job(job)
    factor = DEFAULT_JOB_FACTORS.get(name, model['job_factors'].get(name, 1.0))
    flags = []
    if name in DEFAULT_JOB_FACTORS or name not in model['job_factors']:
        flags.append('pooled_job')
    if level <= 0:
        flags.append('missing_level')
    elif not model['level_range'][0] <= level <= model['level_range'][1]:
        flags.append('level_extrapolation')
    if not model['power_range_man'][0] <= combat_power_man <= model['power_range_man'][1]:
        flags.append('power_extrapolation')
    log_value = (
        model['intercept_log']
        + model['power_exponent'] * math.log(combat_power_man / model['power_reference_man'])
        + model['level_exponent'] * math.log(max(level, 1) / model['level_reference'] if level > 0 else 1.0)
        + math.log(factor)
    )
    return {'value': max(1, round(math.exp(log_value))), 'job_factor': factor,
            'status': ','.join(flags) or 'calibrated'}
