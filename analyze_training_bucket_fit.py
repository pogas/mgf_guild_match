from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mgf_guild_export import estimate_training_score, power_to_man_units


CSV_PATH = Path(r"F:\macro\mgf_guild_report\ResourceData\수련장샘플\merged_training_samples.csv")
OUTPUT_PATH = Path(r"F:\macro\mgf_guild_report\ResourceData\수련장샘플\training_bucket_analysis.json")
BASE_URL = "https://mgf.gg/contents/character.php"


def clean_text(value: str) -> str:
    return " ".join(str(value).split()).strip()


def parse_korean_number(value: str) -> int:
    text = clean_text(value).replace(",", "")
    units = {"경": 10_000_000_000_000_000, "조": 1_000_000_000_000, "억": 100_000_000, "만": 10_000}
    import re
    total = 0
    for number, unit in re.findall(r"(\d+)\s*(경|조|억|만)?", text):
        if not number:
            continue
        total += int(number) * units.get(unit or "", 1)
    return total


def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=2, connect=2, read=2, status=2, other=0, allowed_methods=frozenset(["GET", "HEAD"]), status_forcelist=[429, 500, 502, 503, 504], backoff_factor=0.5, respect_retry_after_header=True)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=4, pool_block=True)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR,ko;q=0.9"})
    return s


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


def load_usable_rows() -> list[dict[str, Any]]:
    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    usable = []
    for row in rows:
        note = clean_text(row.get("note", ""))
        if not clean_text(row.get("combat_power", "")):
            continue
        if "전투력 미확인" in note or "OCR" in note or "닉네임" in note:
            continue
        usable.append(row)
    return usable


def mean_abs_pct(rows: list[dict[str, Any]], get_pred) -> float:
    vals = [abs(get_pred(r) - r["observed_score"]) / r["observed_score"] for r in rows if r["observed_score"] > 0]
    return statistics.mean(vals) if vals else 0.0


def main() -> None:
    usable = load_usable_rows()
    session = build_session()
    matched: list[dict[str, Any]] = []
    for row in usable:
        page = parse_character_page(session, clean_text(row["nickname"]))
        if not page.get("level") or not page.get("job_name"):
            continue
        server = clean_text(row.get("server", ""))
        if server.startswith("Scania ") and page.get("server") and page["server"] != server:
            continue
        observed = parse_korean_number(row["score"])
        power = power_to_man_units(row["combat_power"])
        predicted = estimate_training_score(int(page["level"]), power, page["job_name"])
        if observed <= 0 or predicted <= 0:
            continue
        matched.append(
            {
                "nickname": row["nickname"],
                "server": server or page.get("server", ""),
                "job_name": page["job_name"],
                "level": int(page["level"]),
                "tier": "4th" if int(page["level"]) >= 100 else "3rd",
                "combat_power_value": power,
                "observed_score": observed,
                "predicted_score": predicted,
                "ratio": observed / predicted,
            }
        )

    matched.sort(key=lambda r: r["combat_power_value"])
    n = len(matched)
    cut1 = matched[n // 3]["combat_power_value"]
    cut2 = matched[(2 * n) // 3]["combat_power_value"]

    def bucket_name(value: int) -> str:
        if value < cut1:
            return "low"
        if value < cut2:
            return "mid"
        return "high"

    for row in matched:
        row["bucket"] = bucket_name(row["combat_power_value"])

    bucket_summary: dict[str, Any] = {}
    for bucket in ["low", "mid", "high"]:
        items = [r for r in matched if r["bucket"] == bucket]
        bucket_summary[bucket] = {
            "count": len(items),
            "mape": mean_abs_pct(items, lambda r: r["predicted_score"]),
            "bias": statistics.mean((r["predicted_score"] - r["observed_score"]) / r["observed_score"] for r in items) if items else 0,
            "median_ratio": statistics.median(r["ratio"] for r in items) if items else 1.0,
        }

    center_low = bucket_summary["low"]["median_ratio"]
    center_mid = bucket_summary["mid"]["median_ratio"]
    center_high = bucket_summary["high"]["median_ratio"]
    best_bucket = None
    for da in range(-120, 121, 5):
        ma = max(0.5, center_low + da / 1000)
        for db in range(-120, 121, 5):
            mb = max(0.5, center_mid + db / 1000)
            for dc in range(-120, 121, 5):
                mc = max(0.5, center_high + dc / 1000)
                def pred(row: dict[str, Any]) -> float:
                    mult = {"low": ma, "mid": mb, "high": mc}[row["bucket"]]
                    return row["predicted_score"] * mult
                mape = mean_abs_pct(matched, pred)
                if best_bucket is None or mape < best_bucket[0]:
                    best_bucket = (mape, ma, mb, mc)

    result = {
        "matched_rows": len(matched),
        "bucket_thresholds": {"low_max_exclusive": cut1, "mid_max_exclusive": cut2},
        "bucket_summary": bucket_summary,
        "global_mape": mean_abs_pct(matched, lambda r: r["predicted_score"]),
        "best_bucket_mape": best_bucket[0] if best_bucket else None,
        "best_bucket_multipliers": {"low": best_bucket[1], "mid": best_bucket[2], "high": best_bucket[3]} if best_bucket else None,
    }
    OUTPUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
