from __future__ import annotations

import csv
import io
import json
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import local
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mgf_guild_export import estimate_training_score, power_to_man_units


CSV_PATH = Path(r"F:\macro\mgf_guild_report\ResourceData\수련장샘플\수련장샘플_추출결과.csv")
OUTPUT_PATH = Path(r"F:\macro\mgf_guild_report\ResourceData\수련장샘플\training_calibration_result.json")
BASE_URL = "https://mgf.gg/contents/character.php"
MAX_WORKERS = 4
TIMEOUT = (3.05, 10)
_thread_local = local()


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
    adapter = HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=MAX_WORKERS, pool_block=True)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )
    return session


def get_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = build_session()
        _thread_local.session = session
    return session


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


def parse_character_page(nickname: str) -> dict[str, Any]:
    session = get_session()
    response = session.get(BASE_URL, params={"n": nickname}, timeout=TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    stat_map: dict[str, str] = {}
    for box in soup.select(".stat-box"):
        label = clean_text(box.select_one(".stat-label").get_text()) if box.select_one(".stat-label") else ""
        value = clean_text(box.select_one(".stat-value").get_text()) if box.select_one(".stat-value") else ""
        if label:
            stat_map[label] = value

    page_text = clean_text(soup.get_text(" ", strip=True))
    server_match = None
    import re

    server_match = re.search(r"Scania\s+\d+", page_text)
    level_raw = stat_map.get("레벨", "")
    level_match = re.search(r"(\d+)", level_raw)
    return {
        "nickname": nickname,
        "server": server_match.group(0) if server_match else "",
        "level": int(level_match.group(1)) if level_match else 0,
        "job_name": stat_map.get("직업", ""),
    }


def iter_usable_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    usable: list[dict[str, str]] = []
    for row in rows:
        note = clean_text(row.get("note", ""))
        if not clean_text(row.get("combat_power", "")):
            continue
        if "전투력 미확인" in note or "서버 불일치" in note or "OCR" in note:
            continue
        usable.append(row)
    return usable


def main() -> None:
    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    usable_rows = iter_usable_rows(rows)
    unique_nicknames = sorted({clean_text(row["nickname"]) for row in usable_rows})

    fetched: dict[str, dict[str, Any]] = {}
    failures: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(parse_character_page, nickname): nickname for nickname in unique_nicknames}
        for future in as_completed(future_map):
            nickname = future_map[future]
            try:
                fetched[nickname] = future.result()
            except Exception as exc:  # noqa: BLE001
                failures[nickname] = str(exc)

    matched_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    for row in usable_rows:
        nickname = clean_text(row["nickname"])
        page = fetched.get(nickname)
        if not page:
            skipped_rows.append({"nickname": nickname, "reason": failures.get(nickname, "fetch failed")})
            continue
        expected_server = clean_text(row.get("server", ""))
        if page.get("server") and expected_server and page["server"] != expected_server:
            skipped_rows.append({"nickname": nickname, "reason": f"server mismatch: {page['server']}"})
            continue
        level = int(page.get("level", 0))
        job_name = clean_text(page.get("job_name", ""))
        if level <= 0 or not job_name:
            skipped_rows.append({"nickname": nickname, "reason": "missing level/job"})
            continue
        observed_score = parse_korean_number(row.get("score", ""))
        combat_power_value = power_to_man_units(row.get("combat_power", ""))
        predicted_score = estimate_training_score(level, combat_power_value, job_name)
        if predicted_score <= 0:
            skipped_rows.append({"nickname": nickname, "reason": "predicted score <= 0"})
            continue
        ratio = observed_score / predicted_score
        matched_rows.append(
            {
                "nickname": nickname,
                "server": expected_server,
                "job_name": job_name,
                "level": level,
                "observed_score": observed_score,
                "combat_power_value": combat_power_value,
                "predicted_score": predicted_score,
                "ratio": ratio,
            }
        )

    ratios = [row["ratio"] for row in matched_rows]
    multiplier = statistics.median(ratios) if ratios else 1.0

    def compute_mape(multiplier_value: float) -> float:
        samples = [
            abs((row["predicted_score"] * multiplier_value) - row["observed_score"]) / row["observed_score"]
            for row in matched_rows
            if row["observed_score"] > 0
        ]
        return statistics.mean(samples) if samples else 0.0

    best_mape_multiplier = 1.0
    best_mape_value = compute_mape(1.0)
    for step in range(500, 1501):
        candidate = step / 1000
        candidate_mape = compute_mape(candidate)
        if candidate_mape < best_mape_value:
            best_mape_multiplier = candidate
            best_mape_value = candidate_mape

    abs_pct_errors_before = [abs(row["predicted_score"] - row["observed_score"]) / row["observed_score"] for row in matched_rows if row["observed_score"] > 0]
    abs_pct_errors_after = [abs((row["predicted_score"] * multiplier) - row["observed_score"]) / row["observed_score"] for row in matched_rows if row["observed_score"] > 0]

    result = {
        "usable_rows": len(usable_rows),
        "matched_rows": len(matched_rows),
        "skipped_rows": len(skipped_rows),
        "recommended_multiplier": multiplier,
        "best_mape_multiplier": best_mape_multiplier,
        "median_ratio": statistics.median(ratios) if ratios else None,
        "mape_before": statistics.mean(abs_pct_errors_before) if abs_pct_errors_before else None,
        "mape_after": statistics.mean(abs_pct_errors_after) if abs_pct_errors_after else None,
        "best_mape_after": best_mape_value,
        "matched_rows_full": matched_rows,
        "matched_preview": matched_rows[:20],
        "skipped_preview": skipped_rows[:20],
    }

    OUTPUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
