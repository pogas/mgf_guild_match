from __future__ import annotations

import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mgf_guild_export import (
    TRAINING_JOB_COEFFICIENTS_3RD,
    TRAINING_JOB_COEFFICIENTS_4TH,
    TRAINING_LEVEL_EXPONENT,
    TRAINING_POWER_EXPONENT,
    get_training_job_coefficient_by_tier,
    normalize_job_name,
    power_to_man_units,
)


HERE = Path(__file__).resolve().parent
CSV_PATH = HERE / "ResourceData" / "수련장샘플" / "merged_training_samples.csv"
OUTPUT_PATH = HERE / "ResourceData" / "수련장샘플" / "training_calibration_v2.json"
BASE_URL = "https://mgf.gg/contents/character.php"


def clean_text(value: str) -> str:
    return " ".join(str(value).split()).strip()


def parse_korean_number(value: str) -> int:
    text = clean_text(value).replace(",", "")
    if not text:
        return 0
    units = {"경": 10_000_000_000_000_000, "조": 1_000_000_000_000, "억": 100_000_000, "만": 10_000}
    import re

    total = 0
    for number, unit in re.findall(r"(\d+)\s*(경|조|억|만)?", text):
        if not number:
            continue
        total += int(number) * units.get(unit or "", 1)
    return total


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        other=0,
        allowed_methods=frozenset(["GET", "HEAD"]),
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=0.5,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=4, pool_block=True)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR,ko;q=0.9"})
    return session


def parse_character_page(session: requests.Session, nickname: str) -> dict[str, Any]:
    response = session.get(BASE_URL, params={"n": nickname}, timeout=(3.05, 10))
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    stat_map: dict[str, str] = {}
    for box in soup.select(".stat-box"):
        label = clean_text(box.select_one(".stat-label").get_text()) if box.select_one(".stat-label") else ""
        value = clean_text(box.select_one(".stat-value").get_text()) if box.select_one(".stat-value") else ""
        if label:
            stat_map[label] = value

    import re

    page_text = clean_text(soup.get_text(" ", strip=True))
    server_match = re.search(r"Scania\s+\d+", page_text)
    level_match = re.search(r"(\d+)", stat_map.get("레벨", ""))
    return {
        "server": server_match.group(0) if server_match else "",
        "level": int(level_match.group(1)) if level_match else 0,
        "job_name": stat_map.get("직업", ""),
    }


def geometric_median_ratio(rows: list[dict[str, Any]], ratio_key: str = "ratio") -> float:
    if not rows:
        return 1.0
    logs = [math.log(max(row[ratio_key], 1e-9)) for row in rows]
    return math.exp(statistics.median(logs))


def mean_abs_pct(rows: list[dict[str, Any]], get_pred) -> float:
    vals = [abs(get_pred(r) - r["observed_score"]) / r["observed_score"] for r in rows if r["observed_score"] > 0]
    return statistics.mean(vals) if vals else 0.0


def classify_bucket(power_value: int, cut1: int, cut2: int) -> str:
    if power_value < cut1:
        return "low"
    if power_value < cut2:
        return "mid"
    return "high"


def main() -> None:
    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    usable = []
    for row in rows:
        note = clean_text(row.get("note", ""))
        if not clean_text(row.get("combat_power", "")):
            continue
        if "전투력 미확인" in note or "server mismatch" in note or "서버 불일치" in note:
            continue
        usable.append(row)

    nicknames = sorted({clean_text(row["nickname"]) for row in usable})
    session = build_session()
    page_cache = {nickname: parse_character_page(session, nickname) for nickname in nicknames}

    matched: list[dict[str, Any]] = []
    for row in usable:
        nickname = clean_text(row["nickname"])
        page = page_cache[nickname]
        expected_server = clean_text(row.get("server", ""))
        if expected_server.startswith("Scania ") and page.get("server") and page["server"] != expected_server:
            continue
        level = int(page.get("level", 0))
        job_name = clean_text(page.get("job_name", ""))
        if level <= 0 or not job_name:
            continue
        observed_score = parse_korean_number(row.get("score", ""))
        combat_power_value = power_to_man_units(row.get("combat_power", ""))
        if observed_score <= 0 or combat_power_value <= 0:
            continue
        coefficient, coefficient_label = get_training_job_coefficient_by_tier(job_name, level)
        base_score = coefficient * (max(level, 1) ** TRAINING_LEVEL_EXPONENT) * (max(combat_power_value / 100_000_000, 1) ** TRAINING_POWER_EXPONENT)
        matched.append(
            {
                "nickname": nickname,
                "server": expected_server or page.get("server", ""),
                "job_name": job_name,
                "job_key": coefficient_label,
                "level": level,
                "tier": "4th" if level >= 100 else "3rd",
                "observed_score": observed_score,
                "combat_power_value": combat_power_value,
                "base_score": base_score,
                "quality_weight": 0.5 if "OCR" in clean_text(row.get("note", "")) or "uncertain" in clean_text(row.get("note", "")) else 1.0,
            }
        )

    matched.sort(key=lambda r: r["combat_power_value"])
    n = len(matched)
    cut1 = matched[n // 3]["combat_power_value"]
    cut2 = matched[(2 * n) // 3]["combat_power_value"]
    for row in matched:
        row["bucket"] = classify_bucket(row["combat_power_value"], cut1, cut2)

    # First-pass outlier detection using same job/tier/bucket, fallback to job/bucket.
    grouped_exact: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    grouped_fallback: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in matched:
        grouped_exact[(row["job_key"], row["tier"], row["bucket"])].append(row)
        grouped_fallback[(row["job_key"], row["bucket"])].append(row)

    filtered: list[dict[str, Any]] = []
    outliers: list[dict[str, Any]] = []
    for row in matched:
        exact_group = grouped_exact[(row["job_key"], row["tier"], row["bucket"])]
        fallback_group = grouped_fallback[(row["job_key"], row["bucket"])]
        group = exact_group if len(exact_group) >= 5 else fallback_group if len(fallback_group) >= 5 else []
        if not group:
            filtered.append(row)
            continue
        median_score = statistics.median(g["observed_score"] for g in group)
        if row["observed_score"] < median_score * 0.8:
            outliers.append(row)
            continue
        filtered.append(row)

    tier_multipliers = {"3rd": 1.0, "4th": 1.0}
    job_adjustments = {job: 1.0 for job in sorted(set(row["job_key"] for row in filtered))}
    bucket_multipliers = {"low": 1.0, "mid": 1.0, "high": 1.0}

    for _ in range(3):
        # bucket
        for bucket in bucket_multipliers:
            items = []
            for row in filtered:
                if row["bucket"] != bucket:
                    continue
                ratio = row["observed_score"] / (row["base_score"] * tier_multipliers[row["tier"]] * job_adjustments[row["job_key"]])
                items.append({"ratio": ratio})
            bucket_multipliers[bucket] = min(1.2, max(0.8, geometric_median_ratio(items))) if items else 1.0

        # tier (small adjustment)
        if any(row["tier"] == "3rd" for row in filtered):
            tier_ratios: dict[str, list[dict[str, Any]]] = {"3rd": [], "4th": []}
            for row in filtered:
                ratio = row["observed_score"] / (row["base_score"] * bucket_multipliers[row["bucket"]] * job_adjustments[row["job_key"]])
                tier_ratios[row["tier"]].append({"ratio": ratio})
            med4 = geometric_median_ratio(tier_ratios["4th"]) if tier_ratios["4th"] else 1.0
            med3 = geometric_median_ratio(tier_ratios["3rd"]) if tier_ratios["3rd"] else med4
            tier_multipliers["4th"] = 1.0
            tier_multipliers["3rd"] = min(1.05, max(0.95, med3 / med4 if med4 else 1.0))

        # job adjustments, shrunk toward 1
        for job_key in job_adjustments:
            ratios = []
            items = [row for row in filtered if row["job_key"] == job_key]
            for row in items:
                ratio = row["observed_score"] / (row["base_score"] * bucket_multipliers[row["bucket"]] * tier_multipliers[row["tier"]])
                ratios.append({"ratio": ratio})
            if not ratios:
                continue
            median_ratio = geometric_median_ratio(ratios)
            alpha = min(1.0, len(items) / 12)
            adjusted = 1.0 + alpha * (median_ratio - 1.0)
            job_adjustments[job_key] = min(1.15, max(0.85, adjusted))

    def predicted_current(row: dict[str, Any]) -> float:
        return row["base_score"]

    def predicted_calibrated(row: dict[str, Any]) -> float:
        return row["base_score"] * bucket_multipliers[row["bucket"]] * tier_multipliers[row["tier"]] * job_adjustments[row["job_key"]]

    current_mape = mean_abs_pct(filtered, predicted_current)
    calibrated_mape = mean_abs_pct(filtered, predicted_calibrated)

    result = {
        "matched_rows": len(matched),
        "filtered_rows": len(filtered),
        "outlier_rows": len(outliers),
        "power_bucket_thresholds": {
            "low_max_exclusive": cut1,
            "mid_max_exclusive": cut2,
        },
        "tier_multipliers": tier_multipliers,
        "bucket_multipliers": bucket_multipliers,
        "job_adjustments": job_adjustments,
        "mape_before": current_mape,
        "mape_after": calibrated_mape,
        "bucket_summary": {
            bucket: {
                "count": len([row for row in filtered if row["bucket"] == bucket]),
                "mape_before": mean_abs_pct([row for row in filtered if row["bucket"] == bucket], predicted_current),
                "mape_after": mean_abs_pct([row for row in filtered if row["bucket"] == bucket], predicted_calibrated),
            }
            for bucket in ["low", "mid", "high"]
        },
        "filtered_rows_full": filtered,
    }
    OUTPUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
