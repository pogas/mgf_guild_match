from __future__ import annotations

import argparse
import csv
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import local
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://mgf.gg/contents/character.php"
CSV_PATH = Path(r"F:\macro\mgf_guild_report\ResourceData\수련장샘플\수련장샘플_추출결과.csv")
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
    return re.sub(r"\s+", " ", value).strip()


def parse_server(soup: BeautifulSoup) -> str:
    text = clean_text(soup.get_text(" ", strip=True))
    match = re.search(r"Scania\s+\d+", text)
    return match.group(0) if match else ""


def parse_combat_power(soup: BeautifulSoup) -> str:
    full_text = clean_text(soup.get_text(" ", strip=True))

    patterns = [
        r"전투력\s*([0-9,\s경조억만]+)",
        r"전투력[^0-9경조억만]*([0-9,\s경조억만]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, full_text)
        if match:
            value = clean_text(match.group(1))
            value = re.sub(r"\s+", " ", value).strip()
            value = re.sub(r"(\d)\s+(?=\d+$)", r"\1", value)
            value = re.sub(r"^(.+?)\s+\1(\s+.*)$", r"\1\2", value)
            parts = value.split()
            if len(parts) % 2 == 0:
                half = len(parts) // 2
                if parts[:half] == parts[half:]:
                    value = " ".join(parts[:half])
            if any(unit in value for unit in ["경", "조", "억", "만"]):
                return value

    meta = soup.find("meta", attrs={"property": "og:description"})
    if meta and meta.get("content"):
        meta_text = clean_text(str(meta["content"]))
        meta_match = re.search(r"([0-9,\s경조억만]+)\s*·", meta_text)
        if meta_match:
            value = clean_text(meta_match.group(1))
            if any(unit in value for unit in ["경", "조", "억", "만"]):
                return value
    return ""


def fetch_character_info(nickname: str) -> dict[str, str]:
    session = get_session()
    response = session.get(BASE_URL, params={"n": nickname}, timeout=TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    server = parse_server(soup)
    combat_power = parse_combat_power(soup)
    return {"server": server, "combat_power": combat_power}


def enrich_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    nicknames = sorted({clean_text(row["nickname"]) for row in rows if clean_text(row.get("nickname", ""))})
    results: dict[str, dict[str, str]] = {}
    failures: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(fetch_character_info, nickname): nickname for nickname in nicknames}
        for future in as_completed(future_map):
            nickname = future_map[future]
            try:
                results[nickname] = future.result()
            except Exception as exc:  # noqa: BLE001
                failures[nickname] = str(exc)
            time.sleep(0.05)

    updated_rows: list[dict[str, Any]] = []
    for row in rows:
        nickname = clean_text(row.get("nickname", ""))
        existing_note = clean_text(row.get("note", ""))
        info = results.get(nickname, {})
        fetched_server = clean_text(info.get("server", ""))
        expected_server = clean_text(row.get("server", ""))
        note_parts = [part for part in [existing_note] if part]

        if nickname in failures:
            note_parts.append(f"전투력 조회 실패: {failures[nickname]}")
        elif not info.get("combat_power"):
            note_parts.append("전투력 미확인")
        elif fetched_server and expected_server.startswith("Scania ") and fetched_server != expected_server:
            note_parts.append(f"서버 불일치(페이지:{fetched_server})")

        updated_rows.append(
            {
                **row,
                "server": row.get("server", "") or fetched_server,
                "combat_power": info.get("combat_power", row.get("combat_power", "")),
                "note": " | ".join(dict.fromkeys(note_parts)),
            }
        )
    return updated_rows, failures


def save_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def normalize_csv_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        repaired = dict(row)
        extras = repaired.pop(None, None)
        if extras:
            repaired["score"] = ",".join([str(repaired.get("score", "")), *[str(value) for value in extras if value is not None]]).strip(",")
        normalized.append(repaired)
    return normalized


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=str(CSV_PATH))
    args = parser.parse_args()

    csv_path = Path(args.csv)
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = normalize_csv_rows(list(csv.DictReader(f)))

    fieldnames = list(rows[0].keys()) if rows else []
    if "combat_power" not in fieldnames:
        fieldnames.append("combat_power")

    updated_rows, failures = enrich_rows(rows)

    unresolved_rows = [row for row in updated_rows if not clean_text(row.get("combat_power", ""))]
    if unresolved_rows:
        retry_input = [
            {**row, "note": " | ".join(part for part in clean_text(row.get("note", "")).split(" | ") if part != "전투력 미확인")}
            for row in unresolved_rows
        ]
        retried_rows, retry_failures = enrich_rows(retry_input)
        retry_map = {(row["filename"], row["row_index_in_image"]): row for row in retried_rows}
        updated_rows = [retry_map.get((row["filename"], row["row_index_in_image"]), row) for row in updated_rows]
        failures.update(retry_failures)

    save_csv(csv_path, updated_rows, fieldnames)

    unresolved_path = csv_path.with_name("unresolved_combat_power.csv")
    unresolved_rows = [row for row in updated_rows if not clean_text(row.get("combat_power", ""))]
    save_csv(unresolved_path, unresolved_rows, fieldnames)

    print(f"Updated rows: {len(updated_rows)}")
    print(f"Unique nicknames fetched: {len({clean_text(row['nickname']) for row in rows if clean_text(row.get('nickname', ''))})}")
    print(f"Failures: {len(failures)}")
    print(f"Unresolved rows: {len(unresolved_rows)}")


if __name__ == "__main__":
    main()
