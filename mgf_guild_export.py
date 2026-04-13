import argparse
import json
import re
from collections import OrderedDict
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urljoin, urlparse

import requests
from xlsxwriter import Workbook
from xlsxwriter.exceptions import FileCreateError
from xlsxwriter.worksheet import Worksheet
from bs4 import BeautifulSoup, Tag


BASE_URL = "https://mgf.gg"
DEFAULT_GUILD_NAME = "빅딜"
_HERE = Path(__file__).parent
SCORE_TABLE_PATH = _HERE / "길드 대항전 점수표.txt"
REPORT_MODE_LABELS = {
    "league": "대항전",
    "training": "수련장",
}


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def extract_query_value(url: str, key: str) -> str:
    parsed = urlparse(url)
    return parse_qs(parsed.query).get(key, [""])[0]


def power_to_man_units(power_text: str) -> int:
    normalized = power_text.replace(",", "")
    total = 0
    for unit, multiplier in (("경", 1_000_000_000_000), ("조", 100_000_000), ("억", 10_000), ("만", 1)):
        match = re.search(rf"(\d+)\s*{unit}", normalized)
        if match:
            total += int(match.group(1)) * multiplier
    return total


def format_man_units(value: int) -> str:
    if value <= 0:
        return "0만"

    gyeong = value // 1_000_000_000_000
    remainder = value % 1_000_000_000_000
    jo = remainder // 100_000_000
    remainder = remainder % 100_000_000
    eok = remainder // 10_000
    man = remainder % 10_000
    parts: list[str] = []
    if gyeong:
        parts.append(f"{gyeong}경")
    if jo:
        parts.append(f"{jo}조")
    if eok:
        parts.append(f"{eok:,}억")
    if man:
        parts.append(f"{man:,}만")
    return " ".join(parts) if parts else "0만"


def safe_sheet_name(name: str) -> str:
    return name[:31]


def anchor_id(name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9가-힣]+", "-", name).strip("-")
    return normalized or "guild"


def safe_file_stem(name: str) -> str:
    normalized = re.sub(r'[<>:"/\\|?*]+', "-", clean_text(name)).strip(" .")
    return normalized or "guild"


def build_match_url(guild_name: str, report_mode: str) -> str:
    return f"{BASE_URL}/contents/guild.php?mode={report_mode}&stx={quote(guild_name)}"


def resolve_snapshot_date(snapshot_date: str | None) -> str:
    if snapshot_date:
        datetime.strptime(snapshot_date, "%Y-%m-%d")
        return snapshot_date
    return datetime.now().strftime("%Y-%m-%d")


def build_mode_filenames(guild_name: str, report_mode: str) -> tuple[str, str, str]:
    file_stem = safe_file_stem(guild_name)
    if report_mode == "league":
        return (
            f"{file_stem}_league_mgf_report.xlsx",
            "index.html",
            "snapshot.json",
        )
    return (
        f"{file_stem}_training_mgf_report.xlsx",
        "training.html",
        "training_snapshot.json",
    )


def build_output_paths(guild_name: str, report_mode: str, snapshot_mode: str, snapshot_date: str | None) -> tuple[Path, Path, Path]:
    file_stem = safe_file_stem(guild_name)
    guild_dir = _HERE / "reports" / file_stem
    workbook_name, html_name, snapshot_name = build_mode_filenames(guild_name, report_mode)
    if snapshot_mode == "history":
        dated_dir = guild_dir / "history" / resolve_snapshot_date(snapshot_date)
        dated_dir.mkdir(parents=True, exist_ok=True)
        output_path = dated_dir / workbook_name
        html_output_path = dated_dir / html_name
        snapshot_path = dated_dir / snapshot_name
    else:
        guild_dir.mkdir(parents=True, exist_ok=True)
        output_path = guild_dir / workbook_name
        html_output_path = guild_dir / html_name
        snapshot_path = guild_dir / snapshot_name
    return output_path, html_output_path, snapshot_path


def cleanup_old_history(guild_name: str, report_mode: str, retain_days: int) -> list[Path]:
    if retain_days <= 0:
        return []

    history_dir = _HERE / "reports" / safe_file_stem(guild_name) / "history"
    if not history_dir.exists():
        return []

    cutoff = datetime.now().date() - timedelta(days=retain_days - 1)
    deleted_paths: list[Path] = []
    for entry in history_dir.iterdir():
        if not entry.is_dir():
            continue
        try:
            entry_date = datetime.strptime(entry.name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if entry_date < cutoff:
            for child in sorted(entry.rglob("*"), reverse=True):
                if child.is_file():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
            entry.rmdir()
            deleted_paths.append(entry)
    return deleted_paths


def validate_report_data(
    guild_seed_name: str,
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
) -> list[str]:
    errors: list[str] = []
    guild_names = {str(row.get("guild_name", "")) for row in guild_rows}

    if guild_seed_name not in guild_names:
        errors.append(f"seed guild missing from matched set: {guild_seed_name}")
    if len(guild_rows) < 5:
        errors.append(f"matched guild count too low: {len(guild_rows)}")

    empty_guilds = [name for name, rows in members_by_guild.items() if not rows]
    if empty_guilds:
        errors.append(f"guilds with no members: {', '.join(empty_guilds)}")

    return errors


def format_score(value: int) -> str:
    return f"{value:,}점"


def parse_rank_number(value: str) -> int | None:
    match = re.search(r"(\d+)", value.replace(",", ""))
    return int(match.group(1)) if match else None


def describe_rank_tier(rank_value: str, label: str) -> str:
    rank_number = parse_rank_number(rank_value)
    if rank_number is None:
        return f"{label} 순위 확인 필요"
    if rank_number == 1:
        return f"{label} 1위"
    if rank_number <= 3:
        return f"{label} TOP3"
    if rank_number <= 10:
        return f"{label} TOP10"
    if rank_number <= 30:
        return f"{label} TOP30"
    return f"{label} {rank_number}위"


def describe_concentration(top3_share_pct: float, top5_share_pct: float) -> str:
    if top3_share_pct >= 80 or top5_share_pct >= 92:
        return "초집중형"
    if top3_share_pct >= 65 or top5_share_pct >= 85:
        return "상위 집중형"
    if top3_share_pct >= 50 or top5_share_pct >= 75:
        return "균형형"
    return "분산형"


def format_delta(value: int, suffix: str = "") -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:,}{suffix}"


def format_percent_delta(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.1f}%"


def build_member_key(member: dict[str, Any]) -> str:
    character_key = clean_text(str(member.get("character_key", "")))
    if character_key:
        return character_key
    return clean_text(str(member.get("nickname", "")))


def next_available_path(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 100):
        candidate = path.with_name(f"{path.stem}_{index}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"No available output path for {path}")


def build_guild_summary(guild_row: dict[str, Any], members: list[dict[str, Any]]) -> dict[str, Any]:
    levels = [int(member["level"]) for member in members if str(member.get("level", "")).isdigit()]
    member_powers = [power_to_man_units(str(member.get("combat_power", ""))) for member in members]
    top_member = max(members, key=lambda item: power_to_man_units(str(item.get("combat_power", "")))) if members else None
    guild_power_value = power_to_man_units(str(guild_row.get("guild_power", "")))
    sorted_member_powers = sorted(member_powers, reverse=True)
    avg_power_per_member_value = round(sum(member_powers) / len(member_powers)) if member_powers else 0
    if member_powers:
        sorted_member_powers_asc = sorted(member_powers)
        middle_index = len(sorted_member_powers_asc) // 2
        if len(sorted_member_powers_asc) % 2 == 0:
            median_power_value = round((sorted_member_powers_asc[middle_index - 1] + sorted_member_powers_asc[middle_index]) / 2)
        else:
            median_power_value = sorted_member_powers_asc[middle_index]
    else:
        median_power_value = 0
    top1_power_value = sorted_member_powers[0] if sorted_member_powers else 0
    top3_power_value = sum(sorted_member_powers[:3]) if sorted_member_powers else 0
    top5_power_value = sum(sorted_member_powers[:5]) if sorted_member_powers else 0
    top10_power_value = sum(sorted_member_powers[:10]) if sorted_member_powers else 0
    top1_share_pct = round((top1_power_value / guild_power_value) * 100, 1) if guild_power_value else 0
    top3_share_pct = round((top3_power_value / guild_power_value) * 100, 1) if guild_power_value else 0
    top5_share_pct = round((top5_power_value / guild_power_value) * 100, 1) if guild_power_value else 0
    top10_share_pct = round((top10_power_value / guild_power_value) * 100, 1) if guild_power_value else 0
    top_member_gap_value = top1_power_value - sorted_member_powers[1] if len(sorted_member_powers) > 1 else top1_power_value
    return {
        "guild_name": guild_row["guild_name"],
        "member_count_int": len(members),
        "guild_power_value": guild_power_value,
        "avg_level": round(sum(levels) / len(levels), 1) if levels else 0,
        "top_member_name": top_member["nickname"] if top_member else "",
        "top_member_power": top_member["combat_power"] if top_member else "",
        "top_member_job": top_member["job_name"] if top_member else "",
        "master_member_power": next((member["combat_power"] for member in members if member["is_master"] == "Y"), ""),
        "member_power_values": member_powers,
        "avg_power_per_member_value": avg_power_per_member_value,
        "avg_power_per_member_text": format_man_units(avg_power_per_member_value),
        "median_power_value": median_power_value,
        "median_power_text": format_man_units(median_power_value),
        "top1_share_pct": top1_share_pct,
        "top3_share_pct": top3_share_pct,
        "top5_share_pct": top5_share_pct,
        "top10_share_pct": top10_share_pct,
        "concentration_label": describe_concentration(top3_share_pct, top5_share_pct),
        "top_member_gap_text": format_man_units(top_member_gap_value),
    }


def parse_score_table(path: Path) -> list[dict[str, int]]:
    rows: list[dict[str, int]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = clean_text(raw_line)
        if not line:
            continue
        match = re.search(r"(\d+)위\s*:\s*([\d,]+)", line)
        if not match:
            continue
        rows.append({
            "rank": int(match.group(1)),
            "score": int(match.group(2).replace(",", "")),
        })
    return rows


def build_guild_war_simulation(
    members_by_guild: dict[str, list[dict[str, Any]]],
    score_table: list[dict[str, int]],
) -> dict[str, Any]:
    score_by_rank = {row["rank"]: row["score"] for row in score_table}
    ranked_members: list[dict[str, Any]] = []
    all_members = [
        member
        for guild_members in members_by_guild.values()
        for member in guild_members
    ]
    sorted_members = sorted(
        all_members,
        key=lambda member: (
            -power_to_man_units(str(member.get("combat_power", ""))),
            str(member.get("guild_name", "")),
            str(member.get("nickname", "")),
        ),
    )
    guild_totals: dict[str, dict[str, Any]] = {
        guild_name: {
            "guild_name": guild_name,
            "total_score": 0,
            "member_count": 0,
            "scoring_count": 0,
            "top_finisher_rank": None,
            "top_finisher_name": "",
        }
        for guild_name in members_by_guild
    }

    for index, member in enumerate(sorted_members, start=1):
        guild_name = str(member["guild_name"])
        score = score_by_rank.get(index, 0)
        ranked_member = {
            "overall_rank": index,
            "guild_name": guild_name,
            "nickname": str(member["nickname"]),
            "combat_power": str(member["combat_power"]),
            "combat_power_value": power_to_man_units(str(member.get("combat_power", ""))),
            "job_name": str(member.get("job_name", "")),
            "character_url": str(member.get("character_url", "")),
            "score": score,
        }
        ranked_members.append(ranked_member)

        guild_total = guild_totals[guild_name]
        guild_total["member_count"] += 1
        guild_total["total_score"] += score
        if score > 0:
            guild_total["scoring_count"] += 1
        if guild_total["top_finisher_rank"] is None:
            guild_total["top_finisher_rank"] = index
            guild_total["top_finisher_name"] = ranked_member["nickname"]

    guild_rankings = sorted(
        guild_totals.values(),
        key=lambda row: (-int(row["total_score"]), int(row["top_finisher_rank"] or 9999), str(row["guild_name"])),
    )
    for index, guild_row in enumerate(guild_rankings, start=1):
        guild_row["simulation_rank"] = index
        guild_row["total_score_text"] = format_score(int(guild_row["total_score"]))

    score_table_preview = [
        {"label": "1~10위", "range": "1,000,000 → 410,000"},
        {"label": "11~30위", "range": "380,000 → 160,000"},
        {"label": "31~60위", "range": "157,000 → 100,000"},
        {"label": "61~100위", "range": "99,000 → 60,300"},
        {"label": "101~150위", "range": "59,600 → 25,300"},
    ]

    return {
        "ranked_members": ranked_members,
        "guild_rankings": guild_rankings,
        "score_table": score_table,
        "score_table_preview": score_table_preview,
    }


def build_snapshot_data(
    guild_seed_name: str,
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    simulation: dict[str, Any],
    snapshot_date: str,
) -> dict[str, Any]:
    simulation_by_guild = {
        str(row["guild_name"]): {
            "simulation_rank": int(row["simulation_rank"]),
            "total_score": int(row["total_score"]),
        }
        for row in simulation["guild_rankings"]
    }
    guilds: dict[str, Any] = {}
    for guild_row in guild_rows:
        guild_name = str(guild_row["guild_name"])
        members = members_by_guild[guild_name]
        summary = build_guild_summary(guild_row, members)
        member_map: dict[str, Any] = {}
        top10_keys: list[str] = []
        sorted_members = sorted(
            members,
            key=lambda member: -power_to_man_units(str(member.get("combat_power", ""))),
        )
        for index, member in enumerate(sorted_members):
            member_key = build_member_key(member)
            member_map[member_key] = {
                "nickname": str(member.get("nickname", "")),
                "job_name": str(member.get("job_name", "")),
                "combat_power": str(member.get("combat_power", "")),
                "combat_power_value": power_to_man_units(str(member.get("combat_power", ""))),
                "rank_in_guild": parse_rank_number(str(member.get("member_rank_in_guild", ""))) or 0,
            }
            if index < 10:
                top10_keys.append(member_key)
        job_counts: dict[str, int] = {}
        for member in members:
            job_name = str(member.get("job_name", "")) or "미확인"
            job_counts[job_name] = job_counts.get(job_name, 0) + 1
        guilds[guild_name] = {
            "guild_name": guild_name,
            "guild_power_value": int(summary["guild_power_value"]),
            "member_count": int(summary["member_count_int"]),
            "avg_level": float(summary["avg_level"]),
            "global_rank": str(guild_row.get("global_rank", "")),
            "server_rank": str(guild_row.get("server_rank", "")),
            "simulation_rank": int(simulation_by_guild.get(guild_name, {}).get("simulation_rank", 0)),
            "simulation_score": int(simulation_by_guild.get(guild_name, {}).get("total_score", 0)),
            "members": member_map,
            "top10_keys": top10_keys,
            "job_counts": job_counts,
        }
    return {
        "guild_seed_name": guild_seed_name,
        "snapshot_date": snapshot_date,
        "guilds": guilds,
    }


def write_snapshot_json(snapshot_data: dict[str, Any], snapshot_path: Path) -> Path:
    snapshot_path.write_text(json.dumps(snapshot_data, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot_path


def load_history_snapshots(guild_name: str, report_mode: str) -> list[dict[str, Any]]:
    history_dir = _HERE / "reports" / safe_file_stem(guild_name) / "history"
    if not history_dir.exists():
        return []
    snapshots: list[dict[str, Any]] = []
    _, _, snapshot_name = build_mode_filenames(guild_name, report_mode)
    for snapshot_file in sorted(history_dir.glob(f"*/{snapshot_name}")):
        try:
            snapshots.append(json.loads(snapshot_file.read_text(encoding="utf-8")))
        except Exception:
            continue
    return snapshots


def build_sparkline(values: list[int], width: int = 120, height: int = 36) -> str:
    if not values:
        return ""
    if len(values) == 1:
        values = [values[0], values[0]]
    min_value = min(values)
    max_value = max(values)
    spread = max(max_value - min_value, 1)
    points: list[str] = []
    for index, value in enumerate(values):
        x = round(index * (width / (len(values) - 1)), 2)
        normalized = (value - min_value) / spread
        y = round(height - (normalized * (height - 4)) - 2, 2)
        points.append(f"{x},{y}")
    return f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" aria-hidden="true"><polyline fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" points="{" ".join(points)}" /></svg>'


def build_history_analysis(current_snapshot: dict[str, Any], history_snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    previous_snapshot = history_snapshots[-1] if history_snapshots else None
    trend_snapshots = history_snapshots[-6:] + [current_snapshot]
    guild_analysis: dict[str, Any] = {}
    for guild_name, current_guild in current_snapshot["guilds"].items():
        previous_guild = previous_snapshot["guilds"].get(guild_name) if previous_snapshot else None
        current_members = current_guild["members"]
        previous_members = previous_guild["members"] if previous_guild else {}

        joined_keys = [key for key in current_members if key not in previous_members]
        departed_keys = [key for key in previous_members if key not in current_members]

        power_changes: list[dict[str, Any]] = []
        rank_changes: list[dict[str, Any]] = []
        for key, current_member in current_members.items():
            previous_member = previous_members.get(key)
            if not previous_member:
                continue
            power_delta = int(current_member["combat_power_value"]) - int(previous_member["combat_power_value"])
            if power_delta != 0:
                power_changes.append({
                    "nickname": current_member["nickname"],
                    "delta_value": power_delta,
                    "delta_text": format_man_units(abs(power_delta)),
                    "delta_sign": "+" if power_delta > 0 else "-",
                })
            rank_delta = int(previous_member.get("rank_in_guild", 0)) - int(current_member.get("rank_in_guild", 0))
            if rank_delta != 0:
                rank_changes.append({
                    "nickname": current_member["nickname"],
                    "delta": rank_delta,
                    "current_rank": int(current_member.get("rank_in_guild", 0)),
                    "previous_rank": int(previous_member.get("rank_in_guild", 0)),
                })

        power_changes.sort(key=lambda item: item["delta_value"], reverse=True)
        rank_changes.sort(key=lambda item: item["delta"], reverse=True)

        job_delta_counts: dict[str, int] = {}
        previous_job_counts = previous_guild.get("job_counts", {}) if previous_guild else {}
        for job_name in set(current_guild["job_counts"]) | set(previous_job_counts):
            delta = int(current_guild["job_counts"].get(job_name, 0)) - int(previous_job_counts.get(job_name, 0))
            if delta != 0:
                job_delta_counts[job_name] = delta

        retained_top10_count = len(set(current_guild["top10_keys"]) & set(previous_guild.get("top10_keys", []))) if previous_guild else 0

        guild_trend_snapshots = [snapshot for snapshot in trend_snapshots if guild_name in snapshot.get("guilds", {})]
        power_values_trend = [int(snapshot["guilds"][guild_name]["guild_power_value"]) for snapshot in guild_trend_snapshots]
        simulation_values_trend = [int(snapshot["guilds"][guild_name].get("simulation_score", 0)) for snapshot in guild_trend_snapshots]
        trend_labels = [str(snapshot.get("snapshot_date", ""))[5:].replace("-", "/") for snapshot in guild_trend_snapshots]

        guild_power_delta = int(current_guild["guild_power_value"]) - int(previous_guild.get("guild_power_value", 0)) if previous_guild else 0
        previous_power_value = int(previous_guild.get("guild_power_value", 0)) if previous_guild else 0
        guild_power_delta_pct = round((guild_power_delta / previous_power_value) * 100, 1) if previous_power_value else 0
        simulation_score_delta = int(current_guild.get("simulation_score", 0)) - int(previous_guild.get("simulation_score", 0)) if previous_guild else 0
        simulation_rank_delta = int(previous_guild.get("simulation_rank", 0)) - int(current_guild.get("simulation_rank", 0)) if previous_guild else 0

        guild_analysis[guild_name] = {
            "joined_members": [current_members[key]["nickname"] for key in joined_keys[:5]],
            "departed_members": [previous_members[key]["nickname"] for key in departed_keys[:5]],
            "member_count_delta": len(joined_keys) - len(departed_keys),
            "guild_power_delta": guild_power_delta,
            "guild_power_delta_pct": guild_power_delta_pct,
            "simulation_score_delta": simulation_score_delta,
            "simulation_rank_delta": simulation_rank_delta,
            "power_risers": power_changes[:5],
            "rank_movers": rank_changes[:5],
            "job_deltas": sorted(job_delta_counts.items(), key=lambda item: abs(item[1]), reverse=True)[:3],
            "retained_top10_count": retained_top10_count,
            "retained_top10_pct": round((retained_top10_count / 10) * 100, 1) if previous_guild else 0,
            "power_trend_svg": build_sparkline(power_values_trend),
            "simulation_trend_svg": build_sparkline(simulation_values_trend),
            "trend_labels": trend_labels,
        }

    total_joined = sum(len(value["joined_members"]) for value in guild_analysis.values())
    total_departed = sum(len(value["departed_members"]) for value in guild_analysis.values())
    best_power_guild = max(guild_analysis.items(), key=lambda item: item[1]["guild_power_delta"], default=("", {"guild_power_delta": 0}))
    best_sim_guild = max(guild_analysis.items(), key=lambda item: item[1]["simulation_score_delta"], default=("", {"simulation_score_delta": 0}))
    stable_guild = max(guild_analysis.items(), key=lambda item: item[1]["retained_top10_count"], default=("", {"retained_top10_count": 0}))

    return {
        "has_previous": previous_snapshot is not None,
        "previous_date": previous_snapshot.get("snapshot_date") if previous_snapshot else "",
        "guilds": guild_analysis,
        "summary": {
            "total_joined": total_joined,
            "total_departed": total_departed,
            "best_power_guild": best_power_guild[0],
            "best_power_delta": int(best_power_guild[1].get("guild_power_delta", 0)),
            "best_sim_guild": best_sim_guild[0],
            "best_sim_delta": int(best_sim_guild[1].get("simulation_score_delta", 0)),
            "stable_guild": stable_guild[0],
            "stable_retained": int(stable_guild[1].get("retained_top10_count", 0)),
        },
    }


def fetch_soup(session: requests.Session, url: str) -> BeautifulSoup:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def collect_guild_links(session: requests.Session, league_url: str) -> list[str]:
    soup = fetch_soup(session, league_url)
    deduped: "OrderedDict[str, None]" = OrderedDict()

    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"])
        if "/contents/guild_info.php?g_name=" not in href:
            continue
        absolute_url = urljoin(BASE_URL, href)
        deduped.setdefault(absolute_url, None)

    return list(deduped.keys())


def parse_stat_pills(hero: Tag) -> dict[str, str]:
    result: dict[str, str] = {}
    for pill in hero.select(".stat-pill"):
        label_el = pill.select_one(".stat-pill-label")
        if not label_el:
            continue
        label = clean_text(label_el.get_text())
        tooltip = pill.select_one(".power-tooltip")
        value_el = pill.select_one(".stat-pill-value")
        value = clean_text(tooltip.get_text()) if tooltip else clean_text(value_el.get_text()) if value_el else ""
        result[label] = value
    return result


def parse_guild_page(session: requests.Session, guild_url: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    soup = fetch_soup(session, guild_url)
    hero = soup.select_one(".guild-hero")
    if hero is None:
        raise ValueError(f"guild hero not found: {guild_url}")

    rank_badges = hero.select(".guild-rank-badge .rank-num")
    stat_map = parse_stat_pills(hero)

    guild_name_el = hero.select_one(".guild-name")
    server_display_el = hero.select_one(".server-chip")
    guild_name = clean_text(guild_name_el.get_text()) if guild_name_el else ""
    server_display = clean_text(server_display_el.get_text()) if server_display_el else ""
    server_name = server_display.split()[0] if server_display else ""
    guild_notice_el = hero.select_one(".guild-desc-pill")
    guild_notice = clean_text(guild_notice_el.get_text()) if guild_notice_el else ""
    update_row = hero.select_one(".guild-update-row")
    data_date_match = re.search(r"(\d{4}\.\d{2}\.\d{2})", clean_text(update_row.get_text()) if update_row else "")
    master_anchor = soup.select_one(".master-card .master-nick")
    master_name = clean_text(master_anchor.get_text()) if master_anchor else ""

    guild_row = {
        "guild_name": guild_name,
        "guild_url": guild_url,
        "guild_key": extract_query_value(guild_url, "g_name"),
        "server_name": server_name,
        "server_display": server_display,
        "global_rank": clean_text(rank_badges[0].get_text()) if len(rank_badges) > 0 else "",
        "server_rank": clean_text(rank_badges[1].get_text()) if len(rank_badges) > 1 else "",
        "guild_level": stat_map.get("레벨", ""),
        "member_count": stat_map.get("길드원", ""),
        "guild_power": stat_map.get("전투력", ""),
        "guild_notice": guild_notice,
        "guild_master_name": master_name,
        "data_date": data_date_match.group(1) if data_date_match else "",
    }

    member_rows: list[dict[str, Any]] = []
    for member_row in soup.select(".members-list .member-row"):
        rank_el = member_row.select_one(".member-rank")
        nick_el = member_row.select_one(".nick-link")
        detail_el = member_row.select_one(".detail-btn")
        job_icon = member_row.select_one(".member-sub img")
        member_sub = member_row.select_one(".member-sub")
        power_tooltip = member_row.select_one(".member-power .power-tooltip")
        power_text = member_row.select_one(".member-power .power-text")

        if not nick_el or not rank_el:
            continue

        member_sub_text = clean_text(member_sub.get_text(" ", strip=True)) if member_sub else ""
        level_match = re.search(r"Lv\.(\d+)", member_sub_text)
        character_href = (
            str(detail_el["href"])
            if detail_el and detail_el.has_attr("href")
            else str(nick_el["href"])
        )
        character_url = urljoin(BASE_URL, character_href)

        member_rows.append(
            {
                "guild_name": guild_name,
                "member_rank_in_guild": clean_text(rank_el.get_text()),
                "nickname": clean_text(nick_el.get_text()),
                "character_key": extract_query_value(character_url, "n"),
                "character_url": character_url,
                "is_master": "Y" if member_row.select_one(".inline-master") else "N",
                "job_name": clean_text(str(job_icon.get("alt", ""))) if job_icon else "",
                "level": level_match.group(1) if level_match else "",
                "combat_power": clean_text(power_tooltip.get_text()) if power_tooltip else clean_text(power_text.get_text()) if power_text else "",
                "data_date": guild_row["data_date"],
            }
        )

    return guild_row, member_rows


def write_sheet(
    workbook: Workbook,
    worksheet: Worksheet,
    rows: list[dict[str, Any]],
    headers: list[str],
) -> None:
    header_format = workbook.add_format({
        "bold": True,
        "bg_color": "#D9E2F3",
        "border": 1,
    })
    cell_format = workbook.add_format({"border": 1})

    for col_idx, header in enumerate(headers):
        worksheet.write(0, col_idx, header, header_format)

    for row_idx, row in enumerate(rows, start=1):
        for col_idx, header in enumerate(headers):
            worksheet.write(row_idx, col_idx, row.get(header, ""), cell_format)

    worksheet.autofilter(0, 0, max(len(rows), 1), len(headers) - 1)
    worksheet.freeze_panes(1, 0)

    for col_idx, header in enumerate(headers):
        max_length = max([len(str(header)), *[len(str(row.get(header, ""))) for row in rows]])
        worksheet.set_column(col_idx, col_idx, min(max(max_length + 2, 12), 50))


def render_summary_cards(guild_rows: list[dict[str, Any]], members_by_guild: dict[str, list[dict[str, Any]]]) -> str:
    total_members = sum(len(rows) for rows in members_by_guild.values())
    total_power = sum(power_to_man_units(str(row.get("guild_power", ""))) for row in guild_rows)
    all_members = [member for members in members_by_guild.values() for member in members]
    top_member = max(all_members, key=lambda item: power_to_man_units(str(item.get("combat_power", "")))) if all_members else None
    avg_level_values = [int(member["level"]) for member in all_members if str(member.get("level", "")).isdigit()]
    avg_level = round(sum(avg_level_values) / len(avg_level_values), 1) if avg_level_values else 0
    updated_on = next((row.get("data_date", "") for row in guild_rows if row.get("data_date")), "")

    cards = [
        ("매칭 길드", f"{len(guild_rows)}개", "현재 그룹에 포함된 길드 수"),
        ("길드원 총합", f"{total_members}명", "5개 길드 전체 길드원 수"),
        ("길드 총 전투력", format_man_units(total_power), "길드 전투력 합산"),
        ("평균 레벨", f"Lv.{avg_level}", "전체 길드원 평균 레벨"),
        (
            "최고 전투력 멤버",
            f"{escape(top_member['nickname']) if top_member else '-'}",
            f"{escape(top_member['combat_power']) if top_member else '-'} · {escape(top_member['guild_name']) if top_member else '-'}",
        ),
        ("기준일", escape(updated_on), "페이지 노출 기준 데이터"),
    ]

    return "".join(
        f"""
        <article class=\"summary-card\">
          <p class=\"summary-label\">{label}</p>
          <strong class=\"summary-value\">{value}</strong>
          <p class=\"summary-help\">{help_text}</p>
        </article>
        """
        for label, value, help_text in cards
    )


def render_auto_summary_section(history_analysis: dict[str, Any]) -> str:
    if not history_analysis.get("has_previous"):
        return """
        <section class="auto-summary-grid">
          <article class="auto-summary-card auto-summary-card-empty">
            <p class="summary-label">히스토리 비교 준비 중</p>
            <strong class="summary-value">스냅샷 2회 이상 필요</strong>
            <p class="summary-help">내일부터 길드원 증감, 전투력 변화, 시뮬레이션 변화 요약이 자동으로 표시된다.</p>
          </article>
        </section>
        """

    summary = history_analysis["summary"]
    cards = [
        ("길드원 증감", f"+{summary['total_joined']} / -{summary['total_departed']}", f"비교 기준일 {history_analysis['previous_date']} 대비"),
        ("총 전투력 최대 상승", summary["best_power_guild"] or "-", format_man_units(abs(summary["best_power_delta"])) if summary["best_power_guild"] else "변화 없음"),
        ("대항전 점수 최대 상승", summary["best_sim_guild"] or "-", format_score(abs(summary["best_sim_delta"])) if summary["best_sim_guild"] else "변화 없음"),
        ("상위권 고정도 최고", summary["stable_guild"] or "-", f"TOP10 유지 {summary['stable_retained']}명" if summary["stable_guild"] else "데이터 없음"),
    ]
    return '<section class="auto-summary-grid">' + ''.join(
        f"""
        <article class="auto-summary-card">
          <p class="summary-label">{escape(label)}</p>
          <strong class="summary-value">{escape(str(value))}</strong>
          <p class="summary-help">{escape(str(help_text))}</p>
        </article>
        """
        for label, value, help_text in cards
    ) + '</section>'


def render_compare_cards(guild_rows: list[dict[str, Any]], members_by_guild: dict[str, list[dict[str, Any]]], history_analysis: dict[str, Any]) -> str:
    max_power = max(power_to_man_units(str(row.get("guild_power", ""))) for row in guild_rows) if guild_rows else 1
    cards: list[str] = []

    for guild_row in guild_rows:
        guild_name = str(guild_row["guild_name"])
        members = members_by_guild[guild_name]
        summary = build_guild_summary(guild_row, members)
        guild_history = history_analysis.get("guilds", {}).get(guild_name, {})
        width_pct = round(summary["guild_power_value"] / max_power * 100, 1) if max_power else 0
        anchor = anchor_id(guild_name)
        cards.append(
            f"""
            <div class="guild-card" data-modal="{escape(anchor)}">
              <div class="guild-card-top">
                <div>
                  <p class="eyebrow">{escape(str(guild_row['server_display']))}</p>
                  <h3>{escape(guild_name)}</h3>
                </div>
                <span class="rank-pill">전체 {escape(str(guild_row['global_rank']))} · 서버 {escape(str(guild_row['server_rank']))}</span>
              </div>
              <div class="rank-badge-row">
                <span class="rank-badge rank-badge-global">{escape(describe_rank_tier(str(guild_row['global_rank']), '전체'))}</span>
                <span class="rank-badge rank-badge-server">{escape(describe_rank_tier(str(guild_row['server_rank']), '서버'))}</span>
              </div>
              <div class="trend-pill-row">
                <span class="trend-pill {'positive' if guild_history.get('guild_power_delta', 0) >= 0 else 'negative'}">총전투력 {format_percent_delta(float(guild_history.get('guild_power_delta_pct', 0)))} </span>
                <span class="trend-pill {'positive' if guild_history.get('simulation_score_delta', 0) >= 0 else 'negative'}">대항점수 {format_delta(int(guild_history.get('simulation_score_delta', 0)), '점')}</span>
                <span class="trend-pill neutral">TOP10 유지 {int(guild_history.get('retained_top10_count', 0))}명</span>
              </div>
              <div class="bar-label-row"><span>길드 총 전투력</span><strong>{width_pct}%</strong></div>
              <div class="power-meter"><span style="width:{width_pct}%"></span></div>
              <div class="bar-label-row bar-label-row-secondary"><span>TOP 멤버 집중도</span><strong>TOP1 {summary['top1_share_pct']}% · TOP3 {summary['top3_share_pct']}%</strong></div>
              <div class="share-visual" aria-label="상위 전투력 비중">
                <span class="share-top1" style="width:{summary['top1_share_pct']}%"></span>
                <span class="share-top3" style="width:{max(summary['top3_share_pct'] - summary['top1_share_pct'], 0)}%"></span>
              </div>
              <div class="guild-analysis-grid">
                <article class="analysis-chip">
                  <span>TOP1 / TOP3</span>
                  <strong>{summary['top1_share_pct']}% / {summary['top3_share_pct']}%</strong>
                </article>
                <article class="analysis-chip">
                  <span>1인당 평균 전투력</span>
                  <strong>{escape(str(summary['avg_power_per_member_text']))}</strong>
                </article>
                <article class="analysis-chip">
                  <span>중앙값 전투력</span>
                  <strong>{escape(str(summary['median_power_text']))}</strong>
                </article>
                <article class="analysis-chip analysis-chip-strong">
                  <span>집중도 판정</span>
                  <strong>{escape(str(summary['concentration_label']))}</strong>
                  <em>TOP5 {summary['top5_share_pct']}% · TOP10 {summary['top10_share_pct']}%</em>
                </article>
              </div>
              <dl class="guild-metrics">
                <div><dt>길드 전투력</dt><dd>{escape(str(guild_row['guild_power']))}</dd></div>
                <div><dt>길드원 수</dt><dd>{summary['member_count_int']}명</dd></div>
                <div><dt>평균 레벨</dt><dd>Lv.{summary['avg_level']}</dd></div>
                <div><dt>TOP 멤버</dt><dd>{escape(str(summary['top_member_name']))}</dd></div>
              </dl>
              <p class="guild-note">{escape(str(guild_row['guild_notice']))}</p>
              <span class="card-jump">길드 상세 보기 ↘</span>
            </div>
            """
        )
    return "".join(cards)


def render_member_rows(members: list[dict[str, Any]]) -> str:
    rows: list[str] = []
    for member in members:
        power_value = power_to_man_units(str(member.get("combat_power", "")))
        master_badge = '<span class="badge badge-master">MASTER</span>' if member.get("is_master") == "Y" else ""
        rows.append(
            f"""
            <tr data-power="{power_value}" data-level="{escape(str(member['level']))}" data-rank="{escape(str(member['member_rank_in_guild']))}">
              <td>{escape(str(member['member_rank_in_guild']))}</td>
              <td>
                <div class="member-name-cell">
                  <a href="{escape(str(member['character_url']))}" target="_blank" rel="noreferrer">{escape(str(member['nickname']))}</a>
                  {master_badge}
                </div>
              </td>
              <td><span class="badge">{escape(str(member['job_name']))}</span></td>
              <td>Lv.{escape(str(member['level']))}</td>
              <td class="power-col">{escape(str(member['combat_power']))}</td>
            </tr>
            """
        )
    return "".join(rows)


def render_detail_comparison_section(guild_rows: list[dict[str, Any]], members_by_guild: dict[str, list[dict[str, Any]]]) -> str:
    columns: list[str] = []
    for guild_row in guild_rows:
        guild_name = str(guild_row["guild_name"])
        members = members_by_guild[guild_name]
        anchor = anchor_id(guild_name)
        member_rows = "".join(
            f"""
            <tr>
              <td><a href="{escape(str(member['character_url']))}" target="_blank" rel="noreferrer">{escape(str(member['nickname']))}</a></td>
              <td>{escape(str(member['combat_power']))}</td>
            </tr>
            """
            for member in members
        )
        columns.append(
            f"""
            <article class="detail-compare-card">
              <div class="detail-compare-head">
                <div>
                  <p class="eyebrow">{escape(str(guild_row['server_display']))}</p>
                  <h3>{escape(guild_name)}</h3>
                </div>
                <a class="mini-link" data-modal="{escape(anchor)}" href="#">상세 보기</a>
              </div>
              <div class="detail-compare-meta">
                <span>길드원 {len(members)}명</span>
                <span>{escape(str(guild_row['guild_power']))}</span>
              </div>
              <table class="detail-compare-table">
                <thead><tr><th>닉네임</th><th>전투력</th></tr></thead>
                <tbody>{member_rows}</tbody>
              </table>
            </article>
            """
        )
    return f'<section class="detail-compare-wrap">{"".join(columns)}</section>'


def render_guild_war_simulation_modal(simulation: dict[str, Any]) -> str:
    guild_cards = "".join(
        f"""
        <article class="simulation-rank-card rank-{int(guild_row['simulation_rank'])}">
          <div class="simulation-rank-top">
            <span class="simulation-rank-badge">#{int(guild_row['simulation_rank'])}</span>
            <strong>{escape(str(guild_row['guild_name']))}</strong>
          </div>
          <div class="simulation-rank-score">{escape(str(guild_row['total_score_text']))}</div>
          <dl class="simulation-rank-meta">
            <div><dt>득점 인원</dt><dd>{int(guild_row['scoring_count'])}명</dd></div>
            <div><dt>최고 순위</dt><dd>{int(guild_row['top_finisher_rank'] or 0)}위 · {escape(str(guild_row['top_finisher_name']))}</dd></div>
          </dl>
        </article>
        """
        for guild_row in simulation["guild_rankings"]
    )
    preview_cards = "".join(
        f"""
        <article class="score-rule-card">
          <span>{escape(str(row['label']))}</span>
          <strong>{escape(str(row['range']))}</strong>
        </article>
        """
        for row in simulation["score_table_preview"]
    )
    ranked_rows = "".join(
        f"""
        <tr>
          <td>{int(member['overall_rank'])}</td>
          <td>{escape(str(member['guild_name']))}</td>
          <td><a href="{escape(str(member['character_url']))}" target="_blank" rel="noreferrer">{escape(str(member['nickname']))}</a></td>
          <td>{escape(str(member['job_name']))}</td>
          <td>{escape(str(member['combat_power']))}</td>
          <td>{format_score(int(member['score']))}</td>
        </tr>
        """
        for member in simulation["ranked_members"]
    )
    return f"""
    <div class="modal-backdrop" id="modal-guild-war-simulation" role="dialog" aria-modal="true" aria-label="대항전 예상 시뮬레이션">
      <div class="modal-box simulation-modal-box">
        <button class="modal-close" aria-label="닫기">×</button>
        <section class="simulation-section">
          <div class="simulation-overview">
            <div>
              <p class="eyebrow">Guild War Projection</p>
              <h3>매칭 길드 5개 전원을 합산한 대항전 예상 시뮬레이션</h3>
              <p class="simulation-copy">모든 길드원을 전투력 순으로 다시 정렬한 뒤, 제공된 순위별 점수표를 적용해 길드별 총합 점수를 계산했다.</p>
            </div>
            <div class="score-rule-grid">{preview_cards}</div>
          </div>
          <div class="simulation-rank-grid">{guild_cards}</div>
          <div class="table-wrap simulation-table-wrap">
            <div class="table-toolbar">
              <h3>대항전 예상 개인 순위</h3>
              <div class="toolbar-actions"><span class="hint">전투력 기준 정렬 · 점수표 자동 반영</span></div>
            </div>
            <table class="member-table simulation-table">
              <thead>
                <tr>
                  <th>순위</th>
                  <th>길드</th>
                  <th>닉네임</th>
                  <th>직업</th>
                  <th>전투력</th>
                  <th>예상 점수</th>
                </tr>
              </thead>
              <tbody>{ranked_rows}</tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
    """


def render_training_simulation_modal() -> str:
    return """
    <div class="modal-backdrop" id="modal-guild-war-simulation" role="dialog" aria-modal="true" aria-label="수련장 시뮬레이터 준비 중">
      <div class="modal-box simulation-modal-box">
        <button class="modal-close" aria-label="닫기">×</button>
        <section class="simulation-section">
          <div class="simulation-overview">
            <div>
              <p class="eyebrow">Training Simulator</p>
              <h3>수련장 예상 시뮬레이터 준비 중</h3>
              <p class="simulation-copy">수련장 점수 계산표를 아직 찾는 중이라 점수 집계 로직은 비워두고, 현재는 동일한 팝업 자리만 먼저 준비했다.</p>
            </div>
            <div class="score-rule-grid">
              <article class="score-rule-card">
                <span>상태</span>
                <strong>점수표 대기</strong>
              </article>
              <article class="score-rule-card">
                <span>예정 기능</span>
                <strong>길드별 점수 합산</strong>
              </article>
            </div>
          </div>
        </section>
      </div>
    </div>
    """


def render_guild_modals(
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    history_analysis: dict[str, Any],
) -> str:
    modals: list[str] = []
    for guild_row in guild_rows:
        guild_name = str(guild_row["guild_name"])
        members = members_by_guild[guild_name]
        summary = build_guild_summary(guild_row, members)
        guild_history = history_analysis.get("guilds", {}).get(guild_name, {})
        anchor = anchor_id(guild_name)
        modals.append(
            f"""
            <div class="modal-backdrop" id="modal-{escape(anchor)}" role="dialog" aria-modal="true" aria-label="{escape(guild_name)}">
              <div class="modal-box">
                <button class="modal-close" aria-label="닫기">×</button>
                <div class="section-head">
                  <div>
                    <p class="eyebrow">{escape(str(guild_row['server_display']))} · 기준일 {escape(str(guild_row['data_date']))}</p>
                    <h2>{escape(guild_name)}</h2>
                  </div>
                  <a class="detail-link" href="{escape(str(guild_row['guild_url']))}" target="_blank" rel="noreferrer">원본 길드 페이지 보기</a>
                </div>
                <div class="section-grid">
                  <article class="info-panel">
                    <h3>길드 정보</h3>
                    <dl>
                      <div><dt>길드 마스터</dt><dd>{escape(str(guild_row['guild_master_name']))}</dd></div>
                      <div><dt>길드 레벨</dt><dd>{escape(str(guild_row['guild_level']))}</dd></div>
                      <div><dt>길드원 수</dt><dd>{summary['member_count_int']}명</dd></div>
                      <div><dt>길드 전투력</dt><dd>{escape(str(guild_row['guild_power']))}</dd></div>
                      <div><dt>전체 / 서버 순위</dt><dd>{escape(str(guild_row['global_rank']))} / {escape(str(guild_row['server_rank']))}</dd></div>
                      <div><dt>평균 레벨</dt><dd>Lv.{summary['avg_level']}</dd></div>
                    </dl>
                  </article>
                  <article class="info-panel emphasis">
                    <h3>핵심 포인트</h3>
                    <ul class="highlights">
                      <li><span>TOP 멤버</span><strong>{escape(str(summary['top_member_name']))}</strong><em>{escape(str(summary['top_member_power']))}</em></li>
                      <li><span>TOP 멤버 직업</span><strong>{escape(str(summary['top_member_job']))}</strong></li>
                      <li><span>마스터 전투력</span><strong>{escape(str(summary['master_member_power']))}</strong></li>
                      <li><span>길드 공지</span><strong>{escape(str(guild_row['guild_notice']))}</strong></li>
                    </ul>
                  </article>
                </div>
                <div class="modal-comparison-grid">
                  <article class="comparison-callout comparison-callout-rank">
                    <span>순위 포지션</span>
                    <strong>{escape(describe_rank_tier(str(guild_row['global_rank']), '전체'))}</strong>
                    <em>{escape(describe_rank_tier(str(guild_row['server_rank']), '서버'))}</em>
                  </article>
                  <article class="comparison-callout comparison-callout-focus">
                    <span>상위 멤버 집중도</span>
                    <strong>{escape(str(summary['concentration_label']))}</strong>
                    <em>TOP3 {summary['top3_share_pct']}% · TOP5 {summary['top5_share_pct']}%</em>
                  </article>
                  <article class="comparison-callout comparison-callout-gap">
                    <span>TOP 멤버 격차</span>
                    <strong>{escape(str(summary['top_member_gap_text']))}</strong>
                    <em>1위와 2위 전투력 차이</em>
                  </article>
                  <article class="comparison-callout comparison-callout-core">
                    <span>핵심 전력 범위</span>
                    <strong>TOP10 {summary['top10_share_pct']}%</strong>
                    <em>길드 총 전투력 대비 상위 10인 비중</em>
                  </article>
                </div>
                <div class="modal-history-grid">
                  <article class="history-panel">
                    <h3>일간 변화 요약</h3>
                    <ul class="history-list">
                      <li><span>길드원 증감</span><strong>{format_delta(int(guild_history.get('member_count_delta', 0)), '명')}</strong></li>
                      <li><span>총 전투력 변화</span><strong>{format_percent_delta(float(guild_history.get('guild_power_delta_pct', 0)))}</strong></li>
                      <li><span>대항전 점수 변화</span><strong>{format_delta(int(guild_history.get('simulation_score_delta', 0)), '점')}</strong></li>
                      <li><span>길드 내부 TOP10 고정도</span><strong>{int(guild_history.get('retained_top10_count', 0))} / 10</strong></li>
                    </ul>
                  </article>
                  <article class="history-panel">
                    <h3>길드원 출입 / 직업 변화</h3>
                    <ul class="history-list">
                      <li><span>신규 길드원</span><strong>{', '.join(guild_history.get('joined_members', [])[:3]) or '변동 없음'}</strong></li>
                      <li><span>이탈 길드원</span><strong>{', '.join(guild_history.get('departed_members', [])[:3]) or '변동 없음'}</strong></li>
                      <li><span>직업 분포 변화</span><strong>{', '.join(f'{job} {format_delta(delta)}' for job, delta in guild_history.get('job_deltas', [])) or '변동 없음'}</strong></li>
                    </ul>
                  </article>
                  <article class="history-panel">
                    <h3>급상승 / 순위 변화</h3>
                    <ul class="history-list history-list-compact">
                      {''.join(f'<li><span>{escape(str(item["nickname"]))}</span><strong>{item["delta_sign"]}{escape(str(item["delta_text"]))}</strong></li>' for item in guild_history.get('power_risers', [])[:3]) or '<li><span>전투력 변화</span><strong>변동 없음</strong></li>'}
                      {''.join(f'<li><span>{escape(str(item["nickname"]))}</span><strong>{format_delta(int(item["delta"]), "계단")}</strong></li>' for item in guild_history.get('rank_movers', [])[:2]) or '<li><span>길드 내부 순위</span><strong>변동 없음</strong></li>'}
                    </ul>
                  </article>
                  <article class="history-panel">
                    <h3>7일 추세</h3>
                    <div class="trend-chart-card">
                      <span>총 전투력</span>
                      <div class="trend-chart">{guild_history.get('power_trend_svg', '')}</div>
                      <div class="trend-axis">{''.join(f'<span>{escape(label)}</span>' for label in guild_history.get('trend_labels', []))}</div>
                    </div>
                    <div class="trend-chart-card">
                      <span>대항전 점수</span>
                      <div class="trend-chart trend-chart-secondary">{guild_history.get('simulation_trend_svg', '')}</div>
                      <div class="trend-axis">{''.join(f'<span>{escape(label)}</span>' for label in guild_history.get('trend_labels', []))}</div>
                    </div>
                  </article>
                </div>
                <div class="table-wrap">
                  <div class="table-toolbar">
                    <h3>길드원 목록</h3>
                    <div class="toolbar-actions">
                      <input class="member-search" type="search" placeholder="닉네임 / 직업 검색" data-target="table-{escape(anchor)}" />
                      <span class="hint">열 제목 클릭 시 정렬</span>
                    </div>
                  </div>
                  <table class="member-table" id="table-{escape(anchor)}">
                    <thead>
                      <tr>
                        <th data-sort="rank">순위</th>
                        <th data-sort="name">닉네임</th>
                        <th data-sort="job">직업</th>
                        <th data-sort="level">레벨</th>
                        <th data-sort="power">전투력</th>
                      </tr>
                    </thead>
                    <tbody>{render_member_rows(members)}</tbody>
                  </table>
                </div>
              </div>
            </div>
            """
        )
    return "".join(modals)


def build_html_report(
    guild_seed_name: str,
    report_mode: str,
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    history_analysis: dict[str, Any],
    html_output_path: Path,
) -> Path:
    # #1 fix: build nav_links outside the f-string to avoid double-brace escaping
    league_exists = (html_output_path.parent / "index.html").exists() or report_mode == "league"
    training_exists = (html_output_path.parent / "training.html").exists() or report_mode == "training"
    mode_tabs = [
        (
            "league",
            "대항전 리포트",
            "index.html",
            league_exists,
        ),
        (
            "training",
            "수련장 리포트",
            "training.html",
            training_exists,
        ),
    ]
    mode_tabs_html = "".join(
        (
            f'<a class="mode-tab {"active" if mode == report_mode else ""}" href="{href}">{escape(label)}</a>'
            if is_available
            else f'<span class="mode-tab disabled">{escape(label)}</span>'
        )
        for mode, label, href, is_available in mode_tabs
    )
    nav_links = "".join(
        '<a data-modal="' + anchor_id(str(row["guild_name"])) + '" href="#">' + escape(str(row["guild_name"])) + "</a>"
        for row in guild_rows
    )
    report_label = REPORT_MODE_LABELS[report_mode]
    summary_cards_html = render_summary_cards(guild_rows, members_by_guild)
    auto_summary_html = render_auto_summary_section(history_analysis)
    compare_cards_html = render_compare_cards(guild_rows, members_by_guild, history_analysis)
    if report_mode == "league":
        score_table = parse_score_table(SCORE_TABLE_PATH)
        simulation = build_guild_war_simulation(members_by_guild, score_table)
        simulation_modal_html = render_guild_war_simulation_modal(simulation)
    else:
        simulation_modal_html = render_training_simulation_modal()
    detail_comparison_html = render_detail_comparison_section(guild_rows, members_by_guild)
    guild_modals_html = render_guild_modals(guild_rows, members_by_guild, history_analysis)

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(guild_seed_name)} {escape(report_label)} 리포트</title>
  <style>
    :root {{
      --bg: #f7f3ec;
      --bg-alt: #fffaf3;
      --panel: rgba(255, 252, 247, 0.92);
      --panel-strong: rgba(250, 244, 236, 0.96);
      --line: rgba(110, 84, 60, 0.12);
      --text: #2e241d;
      --muted: #7a6658;
      --accent: #d47d5a;
      --accent-2: #88b17c;
      --accent-3: #ad6540;
      --shadow: 0 18px 44px rgba(78, 58, 42, 0.12);
      --radius: 22px;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Apple SD Gothic Neo", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(212, 125, 90, 0.14), transparent 30%),
        radial-gradient(circle at top right, rgba(136, 177, 124, 0.18), transparent 28%),
        linear-gradient(180deg, #f7f3ec 0%, #fbf7f1 46%, #f4ece2 100%);
      min-height: 100vh;
    }}
    body::before {{
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background-image: linear-gradient(rgba(80, 58, 40, 0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(80, 58, 40, 0.03) 1px, transparent 1px);
      background-size: 34px 34px;
      mask-image: radial-gradient(circle at center, black 52%, transparent 90%);
    }}
    a {{ color: inherit; text-decoration: none; }}
    .page {{ width: min(1320px, calc(100% - 32px)); margin: 0 auto; padding: 28px 0 56px; position: relative; }}
    .hero {{
      position: relative;
      overflow: hidden;
      padding: 36px;
      border: 1px solid var(--line);
      border-radius: 32px;
      background: linear-gradient(135deg, rgba(255, 251, 246, 0.98), rgba(247, 239, 229, 0.94));
      box-shadow: var(--shadow);
    }}
    .hero::after {{
      content: "";
      position: absolute;
      right: -48px;
      top: -48px;
      width: 180px;
      height: 180px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(212, 125, 90, 0.18), transparent 68%);
      filter: blur(12px);
    }}
    .hero-copy {{ max-width: 100%; }}
    .mode-tabs {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 18px; }}
    .mode-tab {{ display: inline-flex; align-items: center; min-height: 38px; padding: 8px 14px; border-radius: 999px; background: rgba(255,255,255,0.78); border: 1px solid rgba(110,84,60,0.1); color: var(--muted); font-size: 13px; font-weight: 800; }}
    .mode-tab.active {{ background: rgba(212,125,90,0.16); color: var(--accent-3); border-color: rgba(212,125,90,0.18); }}
    .mode-tab.disabled {{ opacity: 0.45; cursor: not-allowed; }}
    .eyebrow {{ margin: 0 0 10px; letter-spacing: .16em; text-transform: uppercase; color: var(--accent-3); font-size: 12px; font-weight: 700; }}
    .hero h1 {{ margin: 0; font-size: clamp(22px, 2.8vw, 38px); line-height: 1.15; max-width: none; white-space: nowrap; word-break: keep-all; }}
    .hero p.lead {{ max-width: 620px; color: var(--muted); font-size: 16px; line-height: 1.8; margin: 16px 0 0; }}
    .hero-nav {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 24px; }}
    .hero-nav a {{
      padding: 10px 14px;
      border-radius: 999px;
      background: rgba(255,255,255,0.62);
      border: 1px solid rgba(110, 84, 60, 0.1);
      color: var(--text);
      font-size: 14px;
      transition: all 0.18s ease;
      cursor: pointer;
    }}
    .hero-nav a:hover {{ background: rgba(212,125,90,0.12); border-color: rgba(212,125,90,0.22); }}
    .section-tabs {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 18px 0 0; }}
    .section-tabs a {{ padding: 12px 16px; border-radius: 999px; background: rgba(255,255,255,0.68); border: 1px solid rgba(110,84,60,0.1); color: var(--accent-3); font-size: 13px; font-weight: 700; }}
    .section-tabs a:hover {{ background: rgba(212,125,90,0.12); border-color: rgba(212,125,90,0.22); }}
    .summary-grid {{ display: grid; gap: 16px; margin-top: 28px; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }}
    .auto-summary-grid {{ display: grid; gap: 16px; margin-top: 18px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }}
    .auto-summary-card {{ padding: 20px 22px; background: rgba(255,252,247,0.78); border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow); }}
    .auto-summary-card-empty {{ grid-column: 1 / -1; }}
    .summary-card, .guild-card, .info-panel, .detail-compare-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }}
    .summary-card {{ padding: 22px; min-height: 152px; }}
    .summary-label {{ margin: 0; font-size: 13px; color: var(--muted); }}
    .summary-value {{ display: block; margin-top: 14px; font-size: 28px; line-height: 1.15; }}
    .summary-help {{ margin: 12px 0 0; color: var(--muted); font-size: 13px; line-height: 1.5; }}
    .section-title {{ margin: 44px 0 16px; font-size: 14px; letter-spacing: .16em; text-transform: uppercase; color: var(--accent-3); font-weight: 700; }}
    /* #2: Guild Comparison — 가로 스크롤 한 열 레이아웃 */
    .compare-scroll-wrap {{
      display: flex;
      gap: 16px;
      overflow-x: auto;
      padding-bottom: 10px;
      scroll-snap-type: x proximity;
      cursor: grab;
      user-select: none;
    }}
    .compare-scroll-wrap.dragging {{ cursor: grabbing; }}
    .compare-scroll-wrap::-webkit-scrollbar {{ height: 6px; }}
    .compare-scroll-wrap::-webkit-scrollbar-track {{ background: rgba(110,84,60,0.06); border-radius: 999px; }}
    .compare-scroll-wrap::-webkit-scrollbar-thumb {{ background: rgba(173,101,64,0.28); border-radius: 999px; }}
    .guild-card {{
      padding: 22px;
      display: block;
      flex: 0 0 300px;
      scroll-snap-align: start;
      cursor: pointer;
      transition: transform .18s ease, box-shadow .18s ease, border-color .18s ease;
    }}
    .guild-card:hover {{ transform: translateY(-3px); border-color: rgba(173,101,64,0.24); box-shadow: 0 24px 44px rgba(78, 58, 42, 0.16); }}
    .guild-card-top {{ display: flex; justify-content: space-between; gap: 16px; align-items: start; }}
    .guild-card h3 {{ margin: 6px 0 0; font-size: 28px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .rank-pill {{ padding: 8px 12px; border-radius: 999px; background: rgba(212,125,90,0.12); color: var(--accent-3); font-size: 12px; white-space: nowrap; font-weight: 700; }}
    .rank-badge-row {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }}
    .rank-badge {{ display: inline-flex; align-items: center; min-height: 28px; padding: 5px 10px; border-radius: 999px; font-size: 12px; font-weight: 800; border: 1px solid rgba(110,84,60,0.08); }}
    .rank-badge-global {{ background: rgba(212,125,90,0.14); color: var(--accent-3); }}
    .rank-badge-server {{ background: rgba(136,177,124,0.14); color: #55734f; }}
    .trend-pill-row {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }}
    .trend-pill {{ display: inline-flex; align-items: center; min-height: 28px; padding: 5px 10px; border-radius: 999px; font-size: 12px; font-weight: 700; border: 1px solid rgba(110,84,60,0.08); }}
    .trend-pill.positive {{ background: rgba(136,177,124,0.14); color: #55734f; }}
    .trend-pill.negative {{ background: rgba(212,125,90,0.14); color: var(--accent-3); }}
    .trend-pill.neutral {{ background: rgba(255,255,255,0.72); color: var(--text); }}
    .bar-label-row {{ display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-top: 12px; color: var(--muted); font-size: 12px; font-weight: 700; }}
    .bar-label-row strong {{ color: var(--text); font-size: 12px; }}
    .bar-label-row-secondary {{ margin-top: 14px; }}
    .power-meter {{ height: 10px; border-radius: 999px; background: rgba(110,84,60,0.08); overflow: hidden; margin: 8px 0 0; }}
    .power-meter span {{ display: block; height: 100%; border-radius: inherit; background: linear-gradient(90deg, var(--accent), var(--accent-2)); }}
    .guild-metrics {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin: 0; }}
    .guild-metrics div {{ padding: 12px 0; border-top: 1px solid rgba(110,84,60,0.08); }}
    .guild-metrics dt {{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    .guild-metrics dd {{ margin: 0; font-size: 15px; font-weight: 700; }}
    .share-visual {{ display: flex; height: 10px; margin-top: 12px; border-radius: 999px; overflow: hidden; background: rgba(110,84,60,0.08); }}
    .share-top1 {{ background: linear-gradient(90deg, rgba(212,125,90,0.95), rgba(212,125,90,0.78)); }}
    .share-top3 {{ background: linear-gradient(90deg, rgba(136,177,124,0.9), rgba(136,177,124,0.68)); }}
    .guild-analysis-grid {{ display: grid; gap: 10px; margin-top: 14px; }}
    .analysis-chip {{ padding: 12px 14px; border-radius: 16px; background: rgba(255,255,255,0.6); border: 1px solid rgba(110,84,60,0.08); }}
    .analysis-chip span {{ display: block; color: var(--muted); font-size: 11px; margin-bottom: 6px; letter-spacing: .04em; }}
    .analysis-chip strong {{ display: block; font-size: 15px; line-height: 1.35; }}
    .analysis-chip em {{ display: block; margin-top: 6px; color: var(--muted); font-style: normal; font-size: 12px; }}
    .analysis-chip-strong {{ background: linear-gradient(160deg, rgba(255,245,235,0.96), rgba(247,235,220,0.94)); }}
    .guild-note {{ margin: 16px 0 0; color: var(--muted); line-height: 1.6; font-size: 14px; }}
    .card-jump {{ display: inline-flex; margin-top: 16px; color: var(--accent-3); font-size: 13px; font-weight: 700; }}
    /* #3: Guild Detail Comparison — 고정폭 카드, 내부 overflow */
    .detail-compare-wrap {{
      display: flex;
      gap: 16px;
      overflow-x: auto;
      padding-bottom: 10px;
      scroll-snap-type: x proximity;
      align-items: start;
      cursor: grab;
      user-select: none;
    }}
    .detail-compare-wrap.dragging {{ cursor: grabbing; }}
    .detail-compare-wrap::-webkit-scrollbar {{ height: 6px; }}
    .detail-compare-wrap::-webkit-scrollbar-track {{ background: rgba(110,84,60,0.06); border-radius: 999px; }}
    .detail-compare-wrap::-webkit-scrollbar-thumb {{ background: rgba(173,101,64,0.28); border-radius: 999px; }}
    .detail-compare-card {{
      flex: 0 0 280px;
      min-width: 0;
      padding: 18px;
      scroll-snap-align: start;
      background: linear-gradient(180deg, rgba(255,252,247,0.98), rgba(247,239,229,0.94));
      overflow: hidden;
    }}
    .detail-compare-head {{ display: flex; justify-content: space-between; gap: 8px; align-items: start; }}
    .detail-compare-head h3 {{ margin: 4px 0 0; font-size: 20px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .mini-link {{ flex-shrink: 0; padding: 6px 9px; border-radius: 999px; background: rgba(255,255,255,0.7); border: 1px solid rgba(110,84,60,0.1); color: var(--accent-3); white-space: nowrap; font-size: 11px; font-weight: 700; }}
    .detail-compare-meta {{ display: flex; justify-content: space-between; gap: 10px; margin-top: 14px; padding-top: 12px; border-top: 1px solid rgba(110,84,60,0.08); color: var(--muted); font-size: 12px; }}
    .detail-compare-table {{ width: 100%; border-collapse: collapse; margin-top: 14px; table-layout: fixed; }}
    .detail-compare-table th {{ text-align: left; color: var(--muted); font-size: 11px; letter-spacing: .06em; text-transform: uppercase; padding: 7px 2px; border-bottom: 2px solid rgba(110,84,60,0.10); font-weight: 700; white-space: nowrap; overflow: hidden; }}
    .detail-compare-table th:first-child {{ width: 48%; }}
    .detail-compare-table th:last-child {{ width: 52%; text-align: right; }}
    .detail-compare-table td {{ padding: 8px 2px; border-bottom: 1px solid rgba(110,84,60,0.07); font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .detail-compare-table td:first-child {{ font-weight: 700; }}
    .detail-compare-table td:first-child a {{ color: var(--text); font-weight: 700; }}
    .detail-compare-table td:last-child {{ color: var(--accent-3); font-variant-numeric: tabular-nums; text-align: right; }}
    .detail-compare-table tr:hover td {{ background: rgba(255,255,255,0.35); }}
    .simulation-modal-box {{ width: min(1180px, 100%); }}
    .simulation-section {{ padding: 4px 0 0; }}
    .simulation-overview {{ display: grid; grid-template-columns: 1.15fr .85fr; gap: 18px; align-items: start; }}
    .simulation-overview h3 {{ margin: 6px 0 0; font-size: 28px; }}
    .simulation-copy {{ margin: 14px 0 0; color: var(--muted); line-height: 1.7; }}
    .score-rule-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; }}
    .score-rule-card {{ padding: 14px; border-radius: 18px; background: rgba(255,255,255,0.64); border: 1px solid rgba(110,84,60,0.08); }}
    .score-rule-card span {{ display: block; color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    .score-rule-card strong {{ display: block; font-size: 15px; }}
    .simulation-rank-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .simulation-rank-card {{ padding: 18px; border-radius: 22px; background: rgba(255,255,255,0.72); border: 1px solid rgba(110,84,60,0.08); box-shadow: 0 14px 32px rgba(78,58,42,0.08); }}
    .simulation-rank-top {{ display: flex; align-items: center; gap: 10px; }}
    .simulation-rank-top strong {{ font-size: 22px; }}
    .simulation-rank-badge {{ display: inline-flex; align-items: center; justify-content: center; min-width: 44px; height: 32px; padding: 0 10px; border-radius: 999px; background: rgba(212,125,90,0.15); color: var(--accent-3); font-size: 13px; font-weight: 800; }}
    .simulation-rank-card.rank-1 .simulation-rank-badge {{ background: rgba(212,125,90,0.24); }}
    .simulation-rank-score {{ margin-top: 14px; font-size: 30px; font-weight: 800; line-height: 1.1; }}
    .simulation-rank-meta {{ display: grid; gap: 10px; margin: 14px 0 0; }}
    .simulation-rank-meta div {{ padding-top: 10px; border-top: 1px solid rgba(110,84,60,0.08); }}
    .simulation-rank-meta dt {{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    .simulation-rank-meta dd {{ margin: 0; font-size: 14px; font-weight: 700; }}
    .simulation-table-wrap {{ margin-top: 18px; }}
    .simulation-table td:nth-child(1), .simulation-table td:nth-child(6), .simulation-table th:nth-child(1), .simulation-table th:nth-child(6) {{ white-space: nowrap; }}
    .simulation-table td:nth-child(5), .simulation-table td:nth-child(6) {{ font-variant-numeric: tabular-nums; }}
    .simulation-table td:nth-child(6) {{ color: var(--accent-3); font-weight: 800; }}
    /* #4: Modal system */
    .modal-backdrop {{
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(30, 20, 12, 0.54);
      backdrop-filter: blur(4px);
      z-index: 200;
      align-items: center;
      justify-content: center;
      padding: 20px;
    }}
    .modal-backdrop.open {{ display: flex; }}
    .modal-box {{
      background: linear-gradient(180deg, rgba(255,251,246,0.99), rgba(249,242,234,0.98));
      border: 1px solid var(--line);
      border-radius: 30px;
      box-shadow: 0 32px 80px rgba(60, 42, 28, 0.28);
      width: min(900px, 100%);
      max-height: 90vh;
      overflow-y: auto;
      padding: 28px;
      position: relative;
      animation: modal-in .22s cubic-bezier(.22,1,.36,1);
    }}
    @keyframes modal-in {{ from {{ opacity: 0; transform: scale(.95) translateY(12px); }} to {{ opacity: 1; transform: none; }} }}
    .modal-close {{
      position: sticky;
      top: 0;
      float: right;
      display: flex;
      align-items: center;
      justify-content: center;
      width: 38px;
      height: 38px;
      border-radius: 50%;
      background: rgba(255,255,255,0.8);
      border: 1px solid rgba(110,84,60,0.12);
      font-size: 20px;
      color: var(--muted);
      cursor: pointer;
      z-index: 10;
      line-height: 1;
      transition: background .15s;
    }}
    .modal-close:hover {{ background: rgba(212,125,90,0.15); color: var(--accent-3); }}
    .section-head {{ display: flex; justify-content: space-between; gap: 18px; align-items: end; margin-bottom: 18px; }}
    .section-head h2 {{ margin: 6px 0 0; font-size: 28px; }}
    .detail-link {{ padding: 11px 16px; border-radius: 999px; background: rgba(255,255,255,0.72); border: 1px solid rgba(110,84,60,0.1); color: var(--accent-3); white-space: nowrap; font-weight: 700; }}
    .section-grid {{ display: grid; grid-template-columns: 1.2fr 1fr; gap: 16px; }}
    .info-panel {{ padding: 20px; }}
    .info-panel h3 {{ margin: 0 0 14px; font-size: 18px; }}
    .info-panel dl div {{ padding: 12px 0; border-top: 1px solid rgba(110,84,60,0.08); }}
    .info-panel dt {{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    .info-panel dd {{ margin: 0; font-size: 15px; font-weight: 700; }}
    .info-panel.emphasis {{ background: linear-gradient(160deg, rgba(255,245,235,0.98), rgba(247,235,220,0.96)); }}
    .highlights {{ list-style: none; margin: 0; padding: 0; display: grid; gap: 12px; }}
    .highlights li {{ padding: 14px; border-radius: 18px; background: rgba(255,255,255,0.58); border: 1px solid rgba(110,84,60,0.06); display: grid; gap: 4px; }}
    .highlights span {{ color: var(--muted); font-size: 12px; }}
    .highlights strong {{ font-size: 16px; line-height: 1.4; }}
    .highlights em {{ color: var(--muted); font-style: normal; font-size: 13px; }}
    .modal-comparison-grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; margin-top: 16px; }}
    .comparison-callout {{ padding: 16px; border-radius: 20px; border: 1px solid rgba(110,84,60,0.08); background: rgba(255,255,255,0.68); box-shadow: 0 10px 24px rgba(78,58,42,0.06); }}
    .comparison-callout span {{ display: block; color: var(--muted); font-size: 12px; margin-bottom: 8px; }}
    .comparison-callout strong {{ display: block; font-size: 18px; line-height: 1.3; }}
    .comparison-callout em {{ display: block; margin-top: 6px; color: var(--muted); font-style: normal; font-size: 12px; line-height: 1.5; }}
    .comparison-callout-rank {{ background: linear-gradient(160deg, rgba(255,246,239,0.96), rgba(248,235,224,0.92)); }}
    .comparison-callout-focus {{ background: linear-gradient(160deg, rgba(247,250,241,0.96), rgba(233,244,227,0.94)); }}
    .comparison-callout-gap {{ background: linear-gradient(160deg, rgba(255,251,244,0.96), rgba(245,238,228,0.94)); }}
    .comparison-callout-core {{ background: linear-gradient(160deg, rgba(252,246,241,0.96), rgba(247,239,229,0.94)); }}
    .modal-history-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; margin-top: 16px; }}
    .history-panel {{ padding: 18px; border-radius: 22px; background: rgba(255,255,255,0.72); border: 1px solid rgba(110,84,60,0.08); }}
    .history-panel h3 {{ margin: 0 0 12px; font-size: 16px; }}
    .history-list {{ list-style: none; margin: 0; padding: 0; display: grid; gap: 10px; }}
    .history-list li {{ display: grid; gap: 4px; padding-top: 10px; border-top: 1px solid rgba(110,84,60,0.08); }}
    .history-list li:first-child {{ padding-top: 0; border-top: 0; }}
    .history-list span {{ color: var(--muted); font-size: 12px; }}
    .history-list strong {{ font-size: 14px; line-height: 1.5; }}
    .history-list-compact strong {{ font-size: 13px; }}
    .trend-chart-card {{ margin-top: 10px; }}
    .trend-chart-card span {{ display: block; color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    .trend-chart {{ height: 40px; color: var(--accent-3); background: rgba(255,248,243,0.7); border-radius: 14px; padding: 6px; }}
    .trend-chart svg {{ width: 100%; height: 100%; display: block; }}
    .trend-chart-secondary {{ color: #55734f; }}
    .trend-axis {{ display: flex; justify-content: space-between; gap: 6px; margin-top: 6px; color: var(--muted); font-size: 10px; }}
    .trend-axis span {{ flex: 1; text-align: center; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .table-wrap {{ margin-top: 18px; border-radius: 24px; border: 1px solid var(--line); overflow: hidden; background: rgba(255, 252, 247, 0.8); }}
    .table-toolbar {{ display: flex; justify-content: space-between; gap: 12px; align-items: center; padding: 18px 20px; border-bottom: 1px solid rgba(110,84,60,0.08); }}
    .table-toolbar h3 {{ margin: 0; font-size: 18px; }}
    .toolbar-actions {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: center; }}
    .member-search {{ min-width: 240px; border: 1px solid rgba(110,84,60,0.12); background: rgba(255,255,255,0.75); color: var(--text); border-radius: 999px; padding: 11px 14px; outline: none; }}
    .member-search::placeholder {{ color: #9d8b7d; }}
    .hint {{ color: var(--muted); font-size: 12px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 16px 18px; text-align: left; border-bottom: 1px solid rgba(110,84,60,0.07); }}
    th {{ position: sticky; top: 0; background: rgba(250,244,237,0.98); color: var(--muted); font-size: 12px; letter-spacing: .08em; text-transform: uppercase; cursor: pointer; font-weight: 700; }}
    tr:hover td {{ background: rgba(255,255,255,0.35); }}
    .member-name-cell {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }}
    .member-name-cell a {{ color: var(--text); font-weight: 700; }}
    .badge {{ display: inline-flex; align-items: center; min-height: 28px; padding: 4px 10px; border-radius: 999px; background: rgba(136,177,124,0.14); color: #55734f; font-size: 12px; border: 1px solid rgba(136,177,124,0.16); font-weight: 700; }}
    .badge-master {{ background: rgba(212, 125, 90, 0.15); color: var(--accent-3); border-color: rgba(212, 125, 90, 0.18); }}
    .power-col {{ font-variant-numeric: tabular-nums; color: var(--accent-3); font-weight: 700; }}
    .footer {{ margin-top: 28px; color: var(--muted); font-size: 13px; text-align: right; }}
    @media (max-width: 980px) {{ .section-grid, .simulation-overview, .modal-comparison-grid, .modal-history-grid {{ grid-template-columns: 1fr; }} .section-head, .table-toolbar {{ flex-direction: column; align-items: start; }} }}
    @media (max-width: 720px) {{ .page {{ width: min(100% - 20px, 1320px); }} .hero {{ padding: 20px; }} .guild-metrics {{ grid-template-columns: 1fr; }} th, td {{ padding: 12px; font-size: 13px; }} .member-search {{ min-width: 0; width: 100%; }} .guild-card {{ flex: 0 0 260px; }} .detail-compare-card {{ flex: 0 0 250px; }} .hero h1 {{ font-size: clamp(18px, 4.8vw, 26px); }} .simulation-modal-box, .modal-box {{ padding: 20px; }} }}
  </style>
</head>
<body>
  <div class="page">
    <header class="hero">
      <div class="hero-copy">
        <div class="mode-tabs">{mode_tabs_html}</div>
        <p class="eyebrow">MGF League Match Report</p>
        <h1>{escape(guild_seed_name)} {escape(report_label)} 리포트</h1>
        <p class="lead">밝고 따뜻한 톤 위에서 길드 비교와 길드원 구성을 더 읽기 쉽게 정리했다. 위에서는 길드 단위 흐름을 보고, 아래에서는 길드별 길드원을 옆으로 바로 비교할 수 있다.</p>
      </div>
      <nav class="hero-nav">{nav_links}</nav>
      <section class="summary-grid">{summary_cards_html}</section>
      {auto_summary_html}
    </header>

    <nav class="section-tabs">
      <a data-modal="guild-war-simulation" href="#">{escape(report_label)} 예상 시뮬레이터</a>
    </nav>

    <h2 class="section-title" id="guild-comparison">Guild Comparison</h2>
    <div class="compare-scroll-wrap">{compare_cards_html}</div>

    <h2 class="section-title" id="guild-detail-comparison">Guild Detail Comparison</h2>
    {detail_comparison_html}

    <p class="footer">Generated from public MGF guild pages · 길드 카드 클릭 시 상세 정보 팝업</p>
  </div>

  {guild_modals_html}
  {simulation_modal_html}

  <script>
    // modal open
    document.querySelectorAll('.guild-card[data-modal]').forEach((card) => {{
      card.addEventListener('click', () => {{
        const id = card.dataset.modal;
        const backdrop = document.getElementById('modal-' + id);
        if (backdrop) backdrop.classList.add('open');
      }});
    }});

    const enableDragScroll = (selector) => {{
      document.querySelectorAll(selector).forEach((container) => {{
        let isPointerDown = false;
        let startX = 0;
        let startScrollLeft = 0;

        container.addEventListener('pointerdown', (event) => {{
          isPointerDown = true;
          startX = event.clientX;
          startScrollLeft = container.scrollLeft;
          container.classList.add('dragging');
          container.setPointerCapture?.(event.pointerId);
        }});

        container.addEventListener('pointermove', (event) => {{
          if (!isPointerDown) return;
          const deltaX = event.clientX - startX;
          container.scrollLeft = startScrollLeft - deltaX;
        }});

        const stopDrag = (event) => {{
          if (!isPointerDown) return;
          isPointerDown = false;
          container.classList.remove('dragging');
          if (event && event.pointerId !== undefined) {{
            container.releasePointerCapture?.(event.pointerId);
          }}
        }};

        container.addEventListener('pointerup', stopDrag);
        container.addEventListener('pointercancel', stopDrag);
        container.addEventListener('pointerleave', stopDrag);
      }});
    }};

    enableDragScroll('.compare-scroll-wrap');
    enableDragScroll('.detail-compare-wrap');

    // modal close — backdrop click or close button
    document.querySelectorAll('.modal-backdrop').forEach((backdrop) => {{
      backdrop.addEventListener('click', (e) => {{
        if (e.target === backdrop) backdrop.classList.remove('open');
      }});
      backdrop.querySelector('.modal-close')?.addEventListener('click', () => {{
        backdrop.classList.remove('open');
      }});
    }});
    // ESC key
    document.addEventListener('keydown', (e) => {{
      if (e.key === 'Escape') document.querySelectorAll('.modal-backdrop.open').forEach((b) => b.classList.remove('open'));
    }});
    // nav links — open modal instead of scroll
    document.querySelectorAll('.hero-nav a[data-modal], .section-tabs a[data-modal]').forEach((a) => {{
      a.addEventListener('click', (e) => {{
        e.preventDefault();
        const backdrop = document.getElementById('modal-' + a.dataset.modal);
        if (backdrop) backdrop.classList.add('open');
      }});
    }});
    // detail compare "상세 섹션으로" links
    document.querySelectorAll('.mini-link[data-modal]').forEach((btn) => {{
      btn.addEventListener('click', (e) => {{
        e.preventDefault();
        const backdrop = document.getElementById('modal-' + btn.dataset.modal);
        if (backdrop) backdrop.classList.add('open');
      }});
    }});

    document.querySelectorAll('.member-search').forEach((input) => {{
      input.addEventListener('input', () => {{
        const table = document.getElementById(input.dataset.target);
        const keyword = input.value.trim().toLowerCase();
        table.querySelectorAll('tbody tr').forEach((row) => {{
          const text = row.innerText.toLowerCase();
          row.style.display = text.includes(keyword) ? '' : 'none';
        }});
      }});
    }});

    document.querySelectorAll('.member-table').forEach((table) => {{
      const tbody = table.querySelector('tbody');
      const directions = new Map();
      table.querySelectorAll('th').forEach((th, index) => {{
        th.addEventListener('click', () => {{
          const sortKey = th.dataset.sort;
          const current = directions.get(sortKey) === 'asc' ? 'desc' : 'asc';
          directions.set(sortKey, current);
          const rows = Array.from(tbody.querySelectorAll('tr'));
          rows.sort((a, b) => {{
            const aText = a.children[index].innerText.trim();
            const bText = b.children[index].innerText.trim();
            let aValue = aText;
            let bValue = bText;
            if (sortKey === 'power') {{
              aValue = Number(a.dataset.power || 0);
              bValue = Number(b.dataset.power || 0);
            }} else if (sortKey === 'level') {{
              aValue = Number(a.dataset.level || 0);
              bValue = Number(b.dataset.level || 0);
            }} else if (sortKey === 'rank') {{
              aValue = Number(a.dataset.rank || 0);
              bValue = Number(b.dataset.rank || 0);
            }}
            if (aValue < bValue) return current === 'asc' ? -1 : 1;
            if (aValue > bValue) return current === 'asc' ? 1 : -1;
            return 0;
          }});
          rows.forEach((row) => tbody.appendChild(row));
        }});
      }});
    }});
  </script>
</body>
</html>
"""
    target_path = html_output_path
    while True:
        try:
            target_path.write_text(html, encoding="utf-8")
            return target_path
        except PermissionError:
            target_path = next_available_path(target_path)

def build_workbook(
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    output_path: Path,
) -> Path:
    target_path = output_path
    guild_headers = [
        "guild_name",
        "guild_url",
        "guild_key",
        "server_name",
        "server_display",
        "global_rank",
        "server_rank",
        "guild_level",
        "member_count",
        "guild_power",
        "guild_notice",
        "guild_master_name",
        "data_date",
    ]
    member_headers = [
        "guild_name",
        "member_rank_in_guild",
        "nickname",
        "character_key",
        "character_url",
        "is_master",
        "job_name",
        "level",
        "combat_power",
        "data_date",
    ]

    while True:
        workbook = Workbook(target_path)
        closed = False
        try:
            guild_sheet = workbook.add_worksheet("guilds")
            write_sheet(workbook, guild_sheet, guild_rows, guild_headers)

            for guild_name, member_rows in members_by_guild.items():
                sheet_name = safe_sheet_name(guild_name)
                member_sheet = workbook.add_worksheet(sheet_name)
                write_sheet(workbook, member_sheet, member_rows, member_headers)

            workbook.close()
            closed = True
            return target_path
        except FileCreateError:
            target_path = next_available_path(target_path)
        finally:
            if not closed:
                try:
                    workbook.close()
                except Exception:
                    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MGF 매칭 길드 리포트 생성기")
    parser.add_argument(
        "--guild-name",
        default=DEFAULT_GUILD_NAME,
        help="대항전 최신화 기준 길드명 (기본값: 빅딜)",
    )
    parser.add_argument(
        "--report-mode",
        choices=["league", "training"],
        default="league",
        help="리포트 종류 (league=대항전, training=수련장)",
    )
    parser.add_argument(
        "--snapshot-mode",
        choices=["latest", "history"],
        default="latest",
        help="산출물 저장 방식 (latest 또는 history)",
    )
    parser.add_argument(
        "--snapshot-date",
        help="history 저장 날짜 (YYYY-MM-DD). 미입력 시 오늘 날짜 사용",
    )
    parser.add_argument(
        "--retain-history-days",
        type=int,
        default=0,
        help="history 폴더에서 유지할 최근 일수. 0이면 정리 안 함",
    )
    parser.add_argument(
        "--fail-on-invalid-data",
        action="store_true",
        help="매칭 결과가 비정상일 때 종료 코드 1로 실패 처리",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    guild_name = clean_text(args.guild_name)
    report_mode = args.report_mode
    league_url = build_match_url(guild_name, report_mode)
    output_path, html_output_path, snapshot_output_path = build_output_paths(guild_name, report_mode, args.snapshot_mode, args.snapshot_date)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0 Safari/537.36"
            )
        }
    )

    guild_links = collect_guild_links(session, league_url)
    guild_rows: list[dict[str, Any]] = []
    members_by_guild: dict[str, list[dict[str, Any]]] = OrderedDict()

    for guild_link in guild_links:
        guild_row, member_rows = parse_guild_page(session, guild_link)
        guild_rows.append(guild_row)
        members_by_guild[guild_row["guild_name"]] = member_rows

    validation_errors = validate_report_data(guild_name, guild_rows, members_by_guild)
    if validation_errors:
        print("Validation failed:")
        for error in validation_errors:
            print(f"- {error}")
        if args.fail_on_invalid_data:
            raise SystemExit(1)

    if report_mode == "league":
        score_table = parse_score_table(SCORE_TABLE_PATH)
        simulation = build_guild_war_simulation(members_by_guild, score_table)
    else:
        simulation = {"guild_rankings": []}
    snapshot_date = resolve_snapshot_date(args.snapshot_date) if args.snapshot_mode == "history" else datetime.now().strftime("%Y-%m-%d")
    snapshot_data = build_snapshot_data(guild_name, guild_rows, members_by_guild, simulation, snapshot_date)
    history_snapshots = load_history_snapshots(guild_name, report_mode)
    history_analysis = build_history_analysis(snapshot_data, history_snapshots)

    workbook_path = build_workbook(guild_rows, members_by_guild, output_path)
    html_report_path = build_html_report(guild_name, report_mode, guild_rows, members_by_guild, history_analysis, html_output_path)
    snapshot_path = write_snapshot_json(snapshot_data, snapshot_output_path)

    total_members = sum(len(rows) for rows in members_by_guild.values())
    deleted_history_paths = cleanup_old_history(guild_name, report_mode, args.retain_history_days)
    print(f"Guild seed: {guild_name}")
    print(f"Report mode: {report_mode}")
    print(f"Match URL: {league_url}")
    print(f"Snapshot mode: {args.snapshot_mode}")
    print(f"Created: {workbook_path}")
    print(f"Created: {html_report_path}")
    print(f"Created: {snapshot_path}")
    print(f"Guild sheets: {1 + len(members_by_guild)}")
    print(f"Guild count: {len(guild_rows)}")
    print(f"Member count: {total_members}")
    if deleted_history_paths:
        print("Deleted old history:")
        for path in deleted_history_paths:
            print(f"- {path}")
    for guild_name, rows in members_by_guild.items():
        print(f"- {guild_name}: {len(rows)} members")


if __name__ == "__main__":
    main()
