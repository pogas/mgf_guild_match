"""길드 토벌전의 관측 점수 예측. 입력 전투력은 원 단위 (1경 = 10**16)."""
from __future__ import annotations

import argparse
import json
import math
import re
from decimal import Decimal
from pathlib import Path

DEFAULT_MODEL = Path(__file__).parent / "ResourceData/tobeol_model_2026-09-25/model.json"
UNITS = {"만": 10**4, "억": 10**8, "조": 10**12, "경": 10**16, "해": 10**20}


def korean_number(value: str) -> int:
    """해/경/조/억/만을 정확히 읽고 중복 단위, 잔여 문자열을 거부한다."""
    text = re.sub(r"[\s,]", "", str(value))
    if not text:
        raise ValueError("Empty number")
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        return int(Decimal(text))
    tokens = list(re.finditer(r"(\d+(?:\.\d+)?)([해경조억만]?)", text))
    if "".join(t.group() for t in tokens) != text:
        raise ValueError(f"Invalid Korean number: {value}")
    previous = 10**24
    total = Decimal(0)
    for token in tokens:
        unit = UNITS.get(token[2], 1)
        if unit >= previous:
            raise ValueError(f"Repeated or unordered units: {value}")
        total += Decimal(token[1]) * unit
        previous = unit
    return int(total)


ADVANCEMENT_THRESHOLDS = (10, 30, 70, 100)


def advancement_stage(level):
    return sum(level >= boundary for boundary in ADVANCEMENT_THRESHOLDS)


def feature_spec(kind, thresholds=(70, 100)):
    spec = [{"type": "intercept"}, {"type": "log_power"}]
    if kind == "power":
        return spec
    spec.append({"type": "level", "reference": 110, "scale": 10})
    if kind == "level_hinge":
        spec += [{"type": "hinge_below", "boundary": 100, "scale": 10},
                 {"type": "hinge_above", "boundary": 120, "scale": 10}]
    elif kind == "level_step":
        spec.append({"type": "step_below", "boundary": 100})
    elif kind == "power_hinge":
        spec.append({"type": "power_hinge"})
    elif kind.startswith("advancement_"):
        if kind in {"advancement_steps", "advancement_piecewise", "advancement_extended"}:
            spec += [{"type": "step_below", "boundary": t} for t in thresholds]
        if kind in {"advancement_hinges", "advancement_piecewise", "advancement_extended"}:
            spec += [{"type": "hinge_below", "boundary": t, "scale": 10} for t in thresholds]
        if kind == "advancement_extended":
            spec.append({"type": "hinge_above", "boundary": 120, "scale": 10})
    return spec


def features_from_spec(level, power, spec):
    x = math.log(power / 10**16)
    values = []
    for f in spec:
        kind = f["type"]
        if kind == "intercept": value = 1.0
        elif kind == "log_power": value = x
        elif kind == "level": value = (level - f["reference"]) / f["scale"]
        elif kind == "step_below": value = float(level < f["boundary"])
        elif kind == "hinge_below": value = max(0, (f["boundary"] - level) / f["scale"])
        elif kind == "hinge_above": value = max(0, (level - f["boundary"]) / f["scale"])
        elif kind == "power_hinge": value = max(0, x)
        else: raise ValueError(f"Unknown feature: {kind}")
        values.append(value)
    return values


def base_features(level: float, power: float, kind: str, thresholds=(70, 100)) -> list[float]:
    return features_from_spec(level, power, feature_spec(kind, thresholds))


def predict(level: float, combat_power: float, job: str, model: dict) -> dict:
    if not math.isfinite(level) or not math.isfinite(combat_power) or level <= 0 or combat_power <= 0:
        raise ValueError("Level and combat power must be finite and positive")
    name = re.sub(r"\s+", "", job)
    info = model["jobs"].get(name)
    flags = []
    factor = (info["factor"] * math.exp(info.get("power_exponent_delta", 0) * math.log(combat_power / 10**16))) if info else 1.0
    if info is None:
        flags.append("unknown_job_reference")
    elif info["count"] < 30:
        flags.append("sparse_job")
    domain = model["domain"]
    if not domain["level"][0] <= level <= domain["level"][1]:
        flags.append("level_extrapolation")
    if not domain["power"][0] <= combat_power <= domain["power"][1]:
        flags.append("power_extrapolation")
    if info:
        if not info["level_range"][0] <= level <= info["level_range"][1]:
            flags.append("job_level_extrapolation")
        if not info["power_range"][0] <= combat_power <= info["power_range"][1]:
            flags.append("job_power_extrapolation")
    stage = advancement_stage(level)
    support = model.get("advancement", {}).get("stages", {}).get(str(stage))
    if support is not None and support["train"] == 0:
        return {"score": None, "score_jo": None, "job_factor": factor, "interval_90": None,
                "flags": flags + ["unsupported_advancement_stage"], "advancement_stage": stage,
                "model_date": model["created_at"]}
    if support is not None and support["train"] < 100:
        flags.append("sparse_advancement_stage")
    features = features_from_spec(level, combat_power, model.get("features", feature_spec(model["kind"])))
    z = sum(a * b for a, b in zip(features, model["coefficients"])) + math.log(factor)
    score = math.exp(z) * 10**12
    q = model["prediction_interval_log_radius"]
    return {
        "score": round(score), "score_jo": score / 10**12, "job_factor": factor,
        "interval_90": [round(score * math.exp(-q)), round(score * math.exp(q))],
        "flags": flags, "advancement_stage": stage, "model_date": model["created_at"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--level", type=float, required=True)
    parser.add_argument("--power", required=True, help="예: '1경 5000조' 또는 원 단위 정수")
    parser.add_argument("--job", required=True)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    args = parser.parse_args()
    model = json.loads(args.model.read_text())
    print(json.dumps(predict(args.level, korean_number(args.power), args.job, model), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
