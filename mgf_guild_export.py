import argparse
import json
import math
import os
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
# 수련장 점수 추정 모델 (셀린느/빅딜 수련장 샘플 OCR + 최신 snapshot 전투력 매칭, 2026-04-13)
# 공식: 예상점수 = job_scale × (level ** 0.5) × ((combat_power / 1억) ** 0.23)
# 확정 샘플 96건 기준 참고용 추정치
# 직업 계수: MGF.GG 커뮤니티 밸런스 분석(2026-04-09) 기반
#   비숍을 1.0 기준으로 각 직업 커뮤니티 점수 비율 × 255,000
#   3차(Lv.60~99) / 4차(Lv.100+) 별도 적용
TRAINING_LEVEL_EXPONENT = 0.5
TRAINING_POWER_EXPONENT = 0.23
TRAINING_SCORE_GLOBAL_MULTIPLIER = 1.0
TRAINING_POWER_BUCKET_LOW_MAX = 493228657
TRAINING_POWER_BUCKET_MID_MAX = 58345491044
TRAINING_POWER_BUCKET_MULTIPLIERS = {
    "low": 0.88,
    "mid": 1.127,
    "high": 0.939,
}
TRAINING_TIER_MULTIPLIERS = {
    "3rd": 1.0497226721862407,
    "4th": 1.0,
}
TRAINING_JOB_ADJUSTMENTS = {
    "나이트로드": 1.15,
    "다크나이트": 0.9695971200455037,
    "보우마스터": 1.15,
    "비숍": 0.9945688058709196,
    "섀도어": 0.8995376095841514,
    "신궁": 1.043629444462388,
    "아크메이지(불,독)": 1.15,
    "아크메이지(썬,콜)": 0.8527737569106176,
    "팔라딘": 0.9441345820775653,
    "히어로": 0.9827547747860125,
}
# 3차 전직 기준 계수 (Lv.60~99) — 커뮤니티 3차 밸런스: 비숍 11.685 = 1.0 기준
TRAINING_JOB_COEFFICIENTS_3RD: dict[str, float] = {
    "비숍": 255000.0,          # 11.685 / 11.685 = 1.000
    "팔라딘": 250908.0,        # 11.496 / 11.685 = 0.984
    "히어로": 228203.0,        # 10.455 / 11.685 = 0.895
    "신궁": 224870.0,          # 10.302 / 11.685 = 0.882
    "섀도어": 215682.0,        # 9.880  / 11.685 = 0.846
    "다크나이트": 209586.0,    # 9.602  / 11.685 = 0.822
    "보우마스터": 206199.0,    # 9.447  / 11.685 = 0.809
    "나이트로드": 201753.0,    # 9.243  / 11.685 = 0.791
    "아크메이지(썬,콜)": 200816.0,  # 9.200 / 11.685 = 0.787
    "아크메이지(불,독)": 189639.0,  # 불독과 동일 취급 (3차 데이터 부재)
    "불독": 189639.0,          # 8.689  / 11.685 = 0.744
}
# 4차 전직 기준 계수 (Lv.100+) — 커뮤니티 4차 밸런스: 비숍 11.894 = 1.0 기준
TRAINING_JOB_COEFFICIENTS_4TH: dict[str, float] = {
    "비숍": 255000.0,          # 11.894 / 11.894 = 1.000
    "히어로": 246322.0,        # 11.492 / 11.894 = 0.966
    "다크나이트": 240680.0,    # 11.227 / 11.894 = 0.944
    "팔라딘": 224349.0,        # 10.467 / 11.894 = 0.880
    "신궁": 219239.0,          # 10.228 / 11.894 = 0.860
    "섀도어": 214472.0,        # 10.006 / 11.894 = 0.841
    "아크메이지(썬,콜)": 210372.0,  # 9.815  / 11.894 = 0.825
    "불독": 193833.0,          # 9.042  / 11.894 = 0.760
    "아크메이지(불,독)": 193833.0,  # 불독과 동일 취급 (4차 데이터 부재)
    "보우마스터": 173389.0,    # 8.088  / 11.894 = 0.680
    "나이트로드": 165936.0,    # 7.741  / 11.894 = 0.651
}
# 하위 호환 단일 계수 dict (레벨 정보 없을 때 fallback — 4차 기준)
TRAINING_JOB_COEFFICIENTS = TRAINING_JOB_COEFFICIENTS_4TH

TOBEOL_RANKING_CACHE_PATH = _HERE / "reports" / "tobeol_ranking_s2.json"
TOBEOL_SNAPSHOT_NAME = "tobeol_snapshot.json"
EMPTY_HISTORY_VALUE = "스냅샷 2회 이상 필요"
EMPTY_HISTORY_FALLBACK_ITEM = '<li><span>변동 없음</span><strong>-</strong></li>'
TOBEOL_UNRANKED_LABEL = "미등재"
TOBEOL_UNRANKED_MEMBER_SUFFIX = "미등재"
TOBEOL_LIKE_PREFIX = "♥ "
LABEL_JOIN = "JOIN"
LABEL_LEAVE = "LEAVE"
LABEL_NEW = "NEW"
LABEL_OUT = "OUT"


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


def find_guild_mark_path(guild_name: str) -> Path | None:
    resource_dir = _HERE / "ResourceData"
    for suffix in (".png", ".webp", ".jpg", ".jpeg", ".svg"):
        candidate = resource_dir / f"{guild_name}_길드마크{suffix}"
        if candidate.exists():
            return candidate
    return None


def build_guild_mark_map(guild_names: list[str], html_output_path: Path) -> dict[str, str]:
    mark_map: dict[str, str] = {}
    html_dir = html_output_path.parent
    for guild_name in guild_names:
        mark_path = find_guild_mark_path(guild_name)
        if not mark_path:
            continue
        relative_path = Path(os.path.relpath(mark_path, html_dir)).as_posix()
        mark_map[guild_name] = relative_path
    return mark_map


def render_guild_mark(guild_name: str, guild_mark_map: dict[str, str], class_name: str) -> str:
    mark_path = guild_mark_map.get(guild_name)
    if not mark_path:
        return ""
    return f'<img class="{class_name}" src="{escape(mark_path)}" alt="{escape(guild_name)} 길드마크" loading="lazy" />'


def build_font_face_map(html_output_path: Path) -> dict[str, str]:
    html_dir = html_output_path.parent
    font_dir = _HERE / "ResourceData" / "MaplestoryFont_TTF"
    font_map: dict[str, str] = {}
    for key, file_name in {
        "light": "Maplestory Light.ttf",
        "bold": "Maplestory Bold.ttf",
    }.items():
        font_path = font_dir / file_name
        if font_path.exists():
            font_map[key] = Path(os.path.relpath(font_path, html_dir)).as_posix()
    return font_map


def build_members_by_guild_from_snapshot(snapshot: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    members_by_guild: dict[str, list[dict[str, Any]]] = OrderedDict()
    for guild_name, guild in snapshot.get("guilds", {}).items():
        member_rows: list[dict[str, Any]] = []
        for member_key, member in guild.get("members", {}).items():
            member_rows.append(
                {
                    "guild_name": str(guild_name),
                    "nickname": str(member.get("nickname", "")),
                    "character_key": str(member_key),
                    "combat_power": str(member.get("combat_power", "")),
                    "job_name": str(member.get("job_name", "")),
                    "level": int(member.get("level", 0)) if str(member.get("level", "")).isdigit() else 0,
                    "character_url": str(member.get("character_url", "")),
                }
            )
        members_by_guild[str(guild_name)] = member_rows
    return members_by_guild


def build_simulation_rank_changes(
    current_snapshot: dict[str, Any],
    previous_snapshot: dict[str, Any] | None,
    report_mode: str,
) -> dict[str, dict[str, Any]]:
    if not previous_snapshot:
        return {}

    previous_members_by_guild = build_members_by_guild_from_snapshot(previous_snapshot)
    if report_mode == "league":
        previous_simulation = build_guild_war_simulation(previous_members_by_guild, parse_score_table(SCORE_TABLE_PATH))
    else:
        previous_simulation = build_training_simulation(previous_members_by_guild)

    previous_rank_map = {
        build_member_key(member): int(member.get("overall_rank", 0))
        for member in previous_simulation.get("ranked_members", [])
    }

    current_members_by_guild = build_members_by_guild_from_snapshot(current_snapshot)
    if report_mode == "league":
        current_simulation = build_guild_war_simulation(current_members_by_guild, parse_score_table(SCORE_TABLE_PATH))
    else:
        current_simulation = build_training_simulation(current_members_by_guild)

    rank_changes: dict[str, dict[str, Any]] = {}
    for member in current_simulation.get("ranked_members", []):
        member_key = build_member_key(member)
        current_rank = int(member.get("overall_rank", 0))
        previous_rank = previous_rank_map.get(member_key)
        if previous_rank is None:
            rank_changes[member_key] = {
                "current_rank": current_rank,
                "previous_rank": None,
                "delta": None,
                "label": "신규",
                "short_label": "NEW",
                "tone": "new",
            }
            continue

        delta = previous_rank - current_rank
        if delta > 0:
            rank_changes[member_key] = {
                "current_rank": current_rank,
                "previous_rank": previous_rank,
                "delta": delta,
                "label": f"▲ {delta} 상승",
                "short_label": f"▲{delta}",
                "tone": "up",
            }
        elif delta < 0:
            rank_changes[member_key] = {
                "current_rank": current_rank,
                "previous_rank": previous_rank,
                "delta": delta,
                "label": f"▼ {abs(delta)} 하락",
                "short_label": f"▼{abs(delta)}",
                "tone": "down",
            }
        else:
            rank_changes[member_key] = {
                "current_rank": current_rank,
                "previous_rank": previous_rank,
                "delta": 0,
                "label": "변동 없음",
                "short_label": "유지",
                "tone": "same",
            }
    return rank_changes


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
            "league.html",
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


def build_tobeol_snapshot_path(guild_name: str, snapshot_mode: str, snapshot_date: str | None) -> Path:
    guild_dir = _HERE / "reports" / safe_file_stem(guild_name)
    if snapshot_mode == "history":
        dated_dir = guild_dir / "history" / resolve_snapshot_date(snapshot_date)
        dated_dir.mkdir(parents=True, exist_ok=True)
        return dated_dir / TOBEOL_SNAPSHOT_NAME
    guild_dir.mkdir(parents=True, exist_ok=True)
    return guild_dir / TOBEOL_SNAPSHOT_NAME


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


def format_metric_delta(value: int, use_man_units: bool) -> str:
    if use_man_units:
        sign = "+" if value > 0 else "-" if value < 0 else ""
        return f"{sign}{format_man_units(abs(value))}"
    return format_delta(value, "점")


def format_rank_delta(value: int) -> str:
    if value > 0:
        return f"+{value}위"
    if value < 0:
        return f"{value}위"
    return "변동 없음"


def trend_pill_tone_class(value: int | float) -> str:
    return "tone-up" if value >= 0 else "tone-down"


def render_summary_card_html(label: str, value: str, help_text: str, extra_classes: str = "summary-card") -> str:
    return f"""
        <article class="{escape(extra_classes)}">
          <p class="summary-label">{escape(str(label))}</p>
          <strong class="summary-value">{escape(str(value))}</strong>
          <p class="summary-help">{escape(str(help_text))}</p>
        </article>
        """


def build_report_primary_summary_card(report_mode: str, history_analysis: dict[str, Any]) -> tuple[str, str, str, str]:
    copy = get_report_copy(report_mode)
    label = "오늘 핵심 변화"
    if history_analysis.get("has_previous"):
        summary = history_analysis.get("summary", {})
        previous_date = str(history_analysis.get("previous_date", "-") or "-")
        total_joined = int(summary.get("total_joined", 0))
        total_departed = int(summary.get("total_departed", 0))
        best_sim_guild = str(summary.get("best_sim_guild", "") or "")
        best_sim_delta = int(summary.get("best_sim_delta", 0))
        best_power_guild = str(summary.get("best_power_guild", "") or "")
        best_power_delta = int(summary.get("best_power_delta", 0))

        if best_sim_guild and best_sim_delta != 0:
            value = f"{best_sim_guild} {copy['simulation_metric']} 최대 상승 {format_metric_delta(best_sim_delta, copy['simulation_metric_short'] == '예상 지표')}"
            help_text = f"{previous_date} 대비 길드원 +{total_joined} / -{total_departed}"
        elif total_joined or total_departed:
            value = f"길드원 +{total_joined} / -{total_departed}"
            if best_power_guild and best_power_delta != 0:
                help_text = f"{previous_date} 대비 {best_power_guild} 전투력 최대 상승 {format_metric_delta(best_power_delta, True)}"
            else:
                help_text = f"{previous_date} 대비 전체 길드원 이동"
        elif best_power_guild and best_power_delta != 0:
            value = f"{best_power_guild} 전투력 최대 상승 {format_metric_delta(best_power_delta, True)}"
            help_text = f"{previous_date} 대비 총 전투력 변화"
        else:
            value = "직전 대비 큰 변동 없음"
            help_text = f"{previous_date} 대비 길드원 수와 핵심 지표에 큰 변화가 없습니다."
    else:
        value = f"{copy['simulation_metric']} 첫 스냅샷 확인"
        help_text = f"직전 비교 기록이 아직 없어 현재 매칭 기준 핵심 지표만 먼저 보여줍니다."

    return (label, value, help_text, "summary-card summary-card-primary")


def build_report_summary_cards(
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    history_analysis: dict[str, Any],
    report_mode: str,
) -> list[tuple[str, str, str, str]]:
    total_members = sum(len(rows) for rows in members_by_guild.values())
    total_power = sum(power_to_man_units(str(row.get("guild_power", ""))) for row in guild_rows)
    all_members = [member for members in members_by_guild.values() for member in members]
    top_member = max(all_members, key=lambda item: power_to_man_units(str(item.get("combat_power", "")))) if all_members else None
    avg_level_values = [int(member["level"]) for member in all_members if str(member.get("level", "")).isdigit()]
    avg_level = round(sum(avg_level_values) / len(avg_level_values), 1) if avg_level_values else 0
    updated_on = next((row.get("data_date", "") for row in guild_rows if row.get("data_date")), "")

    return [
        build_report_primary_summary_card(report_mode, history_analysis),
        ("매칭 길드", f"{len(guild_rows)}개", "현재 그룹에 포함된 길드 수", "summary-card"),
        ("길드원 총합", f"{total_members}명", "매칭 길드 전체 길드원 수", "summary-card"),
        ("평균 레벨", f"Lv.{avg_level}", "전체 길드원 평균 레벨", "summary-card"),
        ("길드 총 전투력", format_man_units(total_power), "길드 전투력 합산", "summary-card"),
        (
            "최고 전투력 멤버",
            f"{escape(top_member['nickname']) if top_member else '-'}",
            f"{escape(top_member['combat_power']) if top_member else '-'} · {escape(top_member['guild_name']) if top_member else '-'}",
            "summary-card",
        ),
        ("기준일", escape(updated_on), "페이지 노출 기준 데이터", "summary-card"),
    ]


def render_report_hero_meta(guild_seed_name: str, report_mode: str, guild_rows: list[dict[str, Any]], history_analysis: dict[str, Any]) -> str:
    copy = get_report_copy(report_mode)
    updated_on = next((str(row.get("data_date", "")) for row in guild_rows if row.get("data_date")), "-")
    summary = history_analysis.get("summary", {})
    member_delta = (
        f"+{int(summary.get('total_joined', 0))} / -{int(summary.get('total_departed', 0))}"
        if history_analysis.get("has_previous")
        else "첫 스냅샷 기준"
    )
    best_guild = str(summary.get("best_sim_guild", "") or guild_seed_name)
    cards = [
        ("리포트 기준", f"{guild_seed_name} · {REPORT_MODE_LABELS[report_mode]}", f"매칭 길드 {len(guild_rows)}개 비교"),
        ("업데이트 기준일", updated_on, "자동 생성 시점 공개 데이터"),
        ("핵심 비교 길드", best_guild, f"{copy['simulation_metric']} 흐름이 가장 두드러진 길드"),
        ("길드원 증감", member_delta, "직전 기록과 비교한 전체 증감"),
    ]
    return "".join(
        f"""
        <article class="hero-meta-card">
          <strong>{escape(str(label))}</strong>
          <span>{escape(str(value))}</span>
          <small>{escape(str(help_text))}</small>
        </article>
        """
        for label, value, help_text in cards
    )


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_snapshot_mode(snapshot: dict[str, Any], default: str = "league") -> str:
    return str(snapshot.get("report_mode") or default)


def _safe_snapshot_member(snapshot: dict[str, Any], guild_name: str, member_key: str, nickname: str = "") -> dict[str, Any] | None:
    guild = snapshot.get("guilds", {}).get(guild_name, {})
    members = guild.get("members", {})
    if member_key in members:
        return members[member_key]
    if nickname:
        for candidate in members.values():
            if str(candidate.get("nickname", "")) == nickname:
                return candidate
    return None


def _calculate_job_balance_score(job_counts: dict[str, int], member_count: int) -> float:
    if member_count <= 0 or not job_counts:
        return 0.0
    shares = [count / member_count for count in job_counts.values() if count > 0]
    entropy = -sum(share * math.log(share) for share in shares)
    max_entropy = math.log(max(len(shares), 1)) if shares else 1
    if max_entropy <= 0:
        return 0.0
    return round((entropy / max_entropy) * 100, 1)


def _build_projection(values: list[int]) -> dict[str, Any] | None:
    clean_values = [int(value) for value in values if int(value) >= 0]
    if len(clean_values) < 3:
        return None
    xs = list(range(len(clean_values)))
    x_mean = sum(xs) / len(xs)
    y_mean = sum(clean_values) / len(clean_values)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, clean_values))
    denominator = sum((x - x_mean) ** 2 for x in xs) or 1
    slope = numerator / denominator
    intercept = y_mean - slope * x_mean
    projected_value = max(0, round(intercept + slope * len(clean_values)))
    residuals = [y - (intercept + slope * x) for x, y in zip(xs, clean_values)]
    variance = sum(residual ** 2 for residual in residuals) / max(len(residuals), 1)
    stddev = math.sqrt(variance)
    return {
        "projected": projected_value,
        "lower": max(0, round(projected_value - stddev)),
        "upper": max(0, round(projected_value + stddev)),
        "slope": round(slope, 2),
        "data_points_used": len(clean_values),
    }


def build_member_key(member: dict[str, Any]) -> str:
    character_key = clean_text(str(member.get("character_key", "")))
    if character_key:
        return character_key
    return clean_text(str(member.get("nickname", "")))


def get_report_copy(report_mode: str) -> dict[str, str]:
    if report_mode == "training":
        return {
            "hero_eyebrow": "MGF Training Match Report",
            "lead": "위에서는 길드 단위 흐름을 보고, 아래에서는 길드별 길드원을 옆으로 바로 비교하며 수련장 예상 지표를 볼 수 있다.",
            "simulation_nav": "수련장 예상 시뮬레이터",
            "simulation_metric": "수련장 지표",
            "simulation_metric_short": "예상 지표",
            "simulation_delta": "수련장 지표 변화",
            "trend_label": "수련장 지표",
            "auto_summary_best": "수련장 지표 최대 상승",
        }
    return {
        "hero_eyebrow": "MGF League Match Report",
        "lead": "위에서는 길드 단위 흐름을 보고, 아래에서는 길드별 길드원을 옆으로 바로 비교할 수 있다.",
        "simulation_nav": "대항전 예상 시뮬레이터",
        "simulation_metric": "대항전 점수",
        "simulation_metric_short": "예상 점수",
        "simulation_delta": "대항전 점수 변화",
        "trend_label": "대항전 점수",
        "auto_summary_best": "대항전 점수 최대 상승",
    }


def normalize_job_name(job_name: str) -> str:
    return re.sub(r"\s+", "", clean_text(job_name))


def _lookup_coefficient_in(job_name: str, lookup: dict[str, float]) -> tuple[float, str] | None:
    normalized_job = normalize_job_name(job_name)
    for candidate, coefficient in lookup.items():
        normalized_candidate = normalize_job_name(candidate)
        if normalized_job == normalized_candidate or normalized_candidate in normalized_job or normalized_job in normalized_candidate:
            return coefficient, candidate
    return None


def get_training_job_coefficient_by_tier(job_name: str, level: int | None = None) -> tuple[float, str]:
    """레벨 기반 3차/4차 전직 계수 선택.
    level >= 100 → 4차 계수, level < 100 (or None) → 3차 계수.
    해당 dict에서 못 찾으면 반대 dict로 fallback.
    """
    if level is not None and level >= 100:
        primary, fallback = TRAINING_JOB_COEFFICIENTS_4TH, TRAINING_JOB_COEFFICIENTS_3RD
    else:
        primary, fallback = TRAINING_JOB_COEFFICIENTS_3RD, TRAINING_JOB_COEFFICIENTS_4TH
    result = _lookup_coefficient_in(job_name, primary) or _lookup_coefficient_in(job_name, fallback)
    return result if result is not None else (1.0, "기본")


def get_training_job_coefficient(job_name: str) -> tuple[float, str]:
    """하위 호환용 — 레벨 미상 시 4차 계수 기준."""
    result = _lookup_coefficient_in(job_name, TRAINING_JOB_COEFFICIENTS_4TH)
    return result if result is not None else (1.0, "기본")


def get_training_tier_label(level: int | None = None) -> str:
    if level is not None and level >= 100:
        return "4th"
    return "3rd"


def get_training_bucket_multiplier(combat_power_value: int) -> float:
    if combat_power_value < TRAINING_POWER_BUCKET_LOW_MAX:
        return TRAINING_POWER_BUCKET_MULTIPLIERS["low"]
    if combat_power_value < TRAINING_POWER_BUCKET_MID_MAX:
        return TRAINING_POWER_BUCKET_MULTIPLIERS["mid"]
    return TRAINING_POWER_BUCKET_MULTIPLIERS["high"]


def get_training_job_adjustment(coefficient_label: str) -> float:
    return TRAINING_JOB_ADJUSTMENTS.get(coefficient_label, 1.0)


def estimate_training_score(level: int, combat_power_value: int, job_name: str) -> int:
    """레벨 + 전투력 + 직업 기반 수련장 예상 점수 추정.
    3차(Lv.60~99) / 4차(Lv.100+) 계수 자동 선택.
    """
    safe_level = max(level, 1)
    safe_power_eok = max(combat_power_value / 100_000_000, 1)
    coefficient, coefficient_label = get_training_job_coefficient_by_tier(job_name, level)
    raw_score = coefficient * (safe_level ** TRAINING_LEVEL_EXPONENT) * (safe_power_eok ** TRAINING_POWER_EXPONENT)
    bucket_multiplier = get_training_bucket_multiplier(combat_power_value)
    tier_multiplier = TRAINING_TIER_MULTIPLIERS[get_training_tier_label(level)]
    job_adjustment = get_training_job_adjustment(coefficient_label)
    return round(raw_score * TRAINING_SCORE_GLOBAL_MULTIPLIER * bucket_multiplier * tier_multiplier * job_adjustment)


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
            "character_key": build_member_key(member),
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


def build_training_simulation(members_by_guild: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    ranked_members: list[dict[str, Any]] = []
    all_members = [
        member
        for guild_members in members_by_guild.values()
        for member in guild_members
    ]
    projected_members: list[dict[str, Any]] = []
    guild_totals: dict[str, dict[str, Any]] = {
        guild_name: {
            "guild_name": guild_name,
            "total_score": 0,
            "member_count": 0,
            "job_ratio_sum": 0.0,
            "job_count_map": {},
            "top_finisher_rank": None,
            "top_finisher_name": "",
        }
        for guild_name in members_by_guild
    }

    for member in all_members:
        guild_name = str(member.get("guild_name", ""))
        combat_power_value = power_to_man_units(str(member.get("combat_power", "")))
        job_name = str(member.get("job_name", ""))
        level_raw = member.get("level", 0)
        try:
            level = int(level_raw)
        except (ValueError, TypeError):
            level = 0
        coefficient, coefficient_label = get_training_job_coefficient_by_tier(job_name, level if level > 0 else None)
        if level > 0:
            estimated_metric_value = estimate_training_score(level, combat_power_value, job_name)
        else:
            # 레벨 정보 없을 때 점수 규모를 맞춘 전투력 기반 fallback
            bucket_multiplier = get_training_bucket_multiplier(combat_power_value)
            tier_multiplier = TRAINING_TIER_MULTIPLIERS[get_training_tier_label(None)]
            job_adjustment = get_training_job_adjustment(coefficient_label)
            estimated_metric_value = round((max(combat_power_value / 100_000_000, 1) ** TRAINING_POWER_EXPONENT) * coefficient * TRAINING_SCORE_GLOBAL_MULTIPLIER * bucket_multiplier * tier_multiplier * job_adjustment)
        projected_members.append(
            {
                "guild_name": guild_name,
                "character_key": build_member_key(member),
                "nickname": str(member.get("nickname", "")),
                "combat_power": str(member.get("combat_power", "")),
                "combat_power_value": combat_power_value,
                "job_name": str(member.get("job_name", "")),
                "level": level,
                "character_url": str(member.get("character_url", "")),
                "coefficient": coefficient,
                "coefficient_label": coefficient_label,
                "score": estimated_metric_value,
                "estimated_metric_value": estimated_metric_value,
                "estimated_metric_text": format_score(estimated_metric_value),
            }
        )

    sorted_members = sorted(
        projected_members,
        key=lambda member: (
            -int(member["estimated_metric_value"]),
            -int(member["combat_power_value"]),
            str(member["guild_name"]),
            str(member["nickname"]),
        ),
    )

    for index, member in enumerate(sorted_members, start=1):
        ranked_member = {
            "overall_rank": index,
            **member,
        }
        ranked_members.append(ranked_member)

        guild_total = guild_totals[str(member["guild_name"])]
        guild_total["member_count"] += 1
        guild_total["total_score"] += int(member["estimated_metric_value"])
        normalized_job_ratio = float(member["coefficient"]) / 800000.0
        guild_total["job_ratio_sum"] += normalized_job_ratio
        coefficient_label = str(member["coefficient_label"])
        guild_total["job_count_map"][coefficient_label] = int(guild_total["job_count_map"].get(coefficient_label, 0)) + 1
        if guild_total["top_finisher_rank"] is None:
            guild_total["top_finisher_rank"] = index
            guild_total["top_finisher_name"] = str(member["nickname"])

    guild_rankings = sorted(
        guild_totals.values(),
        key=lambda row: (-int(row["total_score"]), int(row["top_finisher_rank"] or 9999), str(row["guild_name"])),
    )
    for index, guild_row in enumerate(guild_rankings, start=1):
        guild_row["simulation_rank"] = index
        guild_row["total_score_text"] = format_score(int(guild_row["total_score"]))
        guild_row["avg_job_ratio"] = round(float(guild_row["job_ratio_sum"]) / max(int(guild_row["member_count"]), 1), 3)
        top_jobs = sorted(
            guild_row["job_count_map"].items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )[:3]
        guild_row["job_mix_text"] = ", ".join(f"{name} {count}명" for name, count in top_jobs) or "집계 없음"

    coefficient_preview = [
        {"label": "레벨 영향", "range": "level^0.2"},
        {"label": "전투력 영향", "range": "power^0.2"},
        {"label": "직업 보정", "range": "3차/4차 전직별 계수"},
        {"label": "평균 오차", "range": "약 12.1%"},
    ]

    def _make_coeff_cards(coeff_dict: dict[str, float]) -> list[dict[str, str]]:
        return [
            {"label": job_name, "range": f"×{coefficient / 255000.0:.3f}"}
            for job_name, coefficient in sorted(coeff_dict.items(), key=lambda item: (-item[1], item[0]))
        ]

    job_coefficient_cards = {
        "3rd": _make_coeff_cards(TRAINING_JOB_COEFFICIENTS_3RD),
        "4th": _make_coeff_cards(TRAINING_JOB_COEFFICIENTS_4TH),
    }

    return {
        "ranked_members": ranked_members,
        "guild_rankings": guild_rankings,
        "score_table": [],
        "score_table_preview": coefficient_preview,
        "job_coefficient_cards": job_coefficient_cards,
    }


def build_snapshot_data(
    guild_seed_name: str,
    report_mode: str,
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
                "level": int(member.get("level", 0)) if str(member.get("level", "")).isdigit() else 0,
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
        "report_mode": report_mode,
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


def load_tobeol_history_snapshots(guild_name: str) -> list[dict[str, Any]]:
    history_dir = _HERE / "reports" / safe_file_stem(guild_name) / "history"
    if not history_dir.exists():
        return []
    snapshots: list[dict[str, Any]] = []
    for snapshot_file in sorted(history_dir.glob(f"*/{TOBEOL_SNAPSHOT_NAME}")):
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
    report_mode = str(current_snapshot.get("report_mode", "league"))
    compatible_history = [
        snapshot
        for snapshot in history_snapshots
        if _safe_snapshot_mode(snapshot, report_mode) == report_mode
    ]
    previous_snapshot = compatible_history[-1] if compatible_history else None
    trend_snapshots = compatible_history[-6:] + [current_snapshot]
    guild_analysis: dict[str, Any] = {}
    for guild_name, current_guild in current_snapshot["guilds"].items():
        previous_guild = previous_snapshot["guilds"].get(guild_name) if previous_snapshot else None
        current_members = current_guild.get("members", {})
        previous_members = previous_guild.get("members", {}) if previous_guild else {}

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
            "joined_members": [current_members[key]["nickname"] for key in joined_keys],
            "departed_members": [previous_members[key]["nickname"] for key in departed_keys],
            "joined_count": len(joined_keys),
            "departed_count": len(departed_keys),
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
            "power_values_trend": power_values_trend,
            "simulation_values_trend": simulation_values_trend,
            "member_count_trend": [int(snapshot.get("guilds", {}).get(guild_name, {}).get("member_count", 0)) for snapshot in guild_trend_snapshots],
        }

    total_joined = sum(len(value["joined_members"]) for value in guild_analysis.values())
    total_departed = sum(len(value["departed_members"]) for value in guild_analysis.values())
    best_power_guild = max(guild_analysis.items(), key=lambda item: item[1]["guild_power_delta"], default=("", {"guild_power_delta": 0}))
    best_sim_guild = max(guild_analysis.items(), key=lambda item: item[1]["simulation_score_delta"], default=("", {"simulation_score_delta": 0}))
    stable_guild = max(guild_analysis.items(), key=lambda item: item[1]["retained_top10_count"], default=("", {"retained_top10_count": 0}))

    return {
        "report_mode": report_mode,
        "has_previous": previous_snapshot is not None,
        "previous_date": previous_snapshot.get("snapshot_date") if previous_snapshot else "",
        "simulation_rank_changes": build_simulation_rank_changes(current_snapshot, previous_snapshot, report_mode),
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


def render_simulation_rank_change_badge(change: dict[str, Any] | None, *, compact: bool = False) -> str:
    if not change:
        return '<span class="simulation-rank-change-badge tone-none">기록 없음</span>' if compact else ""
    tone = escape(str(change.get("tone", "none")))
    label = escape(str(change.get("short_label" if compact else "label", "")))
    previous_rank = change.get("previous_rank")
    title = ""
    if previous_rank is not None:
        title = f' title="직전 {int(previous_rank)}위 → 현재 {int(change.get("current_rank", 0))}위"'
    return f'<span class="simulation-rank-change-badge tone-{tone}"{title}>{label}</span>'


def _parse_tobeol_rows(soup: BeautifulSoup, page: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table in soup.select("table.rank-table"):
        for tr in table.select("tbody tr"):
            guild_tag = tr.select_one("a.badge-guild")
            rank_tag = tr.select_one("span.rank-total")
            if not guild_tag or not rank_tag:
                continue
            nickname_tag = tr.select_one("span.nickname")
            badge_pop = tr.select_one("span.badge-pop")
            level_tag = tr.select_one("span.level")
            job_tag = tr.select_one("span.job-name")
            score_tag = tr.select_one("span.score-kor")
            rows.append({
                "rank": _safe_int(rank_tag.get_text(strip=True)),
                "nickname": clean_text(nickname_tag.get_text()) if nickname_tag else "",
                "guild": guild_tag.get_text(strip=True),
                "likes": badge_pop.get_text(strip=True).replace("♥", "").strip() if badge_pop else "",
                "level": level_tag.get_text(strip=True) if level_tag else "",
                "job": job_tag.get_text(strip=True) if job_tag else "",
                "score": score_tag.get_text(strip=True) if score_tag else "",
                "page": page,
            })
    return rows


def fetch_tobeol_ranking(
    guild_names: list[str],
    server: int = 2,
    cache_max_age_hours: int = 12,
) -> list[dict[str, Any]]:
    """mgf.gg 서버 토벌전 랭킹을 수집해 길드명 기준으로 필터해 반환.
    결과는 TOBEOL_RANKING_CACHE_PATH에 캐시하며 cache_max_age_hours 이내면 재사용."""
    target_guilds = set(guild_names)

    if TOBEOL_RANKING_CACHE_PATH.exists():
        try:
            cached = json.loads(TOBEOL_RANKING_CACHE_PATH.read_text(encoding="utf-8"))
            cached_time = datetime.fromisoformat(str(cached.get("fetched_at", "2000-01-01")))
            if (datetime.now() - cached_time).total_seconds() < cache_max_age_hours * 3600:
                rows: list[dict[str, Any]] = cached.get("rows", [])
                return sorted(
                    [r for r in rows if r.get("guild") in target_guilds],
                    key=lambda x: _safe_int(x.get("rank", 999999)),
                )
        except Exception:
            pass

    base_url = f"{BASE_URL}/ranking/guild_boss.php"
    all_rows: list[dict[str, Any]] = []
    try:
        r = requests.get(base_url, params={"server": server, "page": 1}, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        pg_end = soup.select_one("a.pg_end")
        max_page = _safe_int(str(pg_end["href"]).split("page=")[-1]) if pg_end else 1  # type: ignore[index]
        all_rows.extend(_parse_tobeol_rows(soup, 1))
        print(f"Boss ranking: fetching {max_page} pages for server {server}...")
        for page in range(2, max_page + 1):
            try:
                pr = requests.get(base_url, params={"server": server, "page": page}, timeout=20)
                pr.raise_for_status()
                if "데이터가 없습니다" in pr.text:
                    break
                all_rows.extend(_parse_tobeol_rows(BeautifulSoup(pr.content, "html.parser"), page))
            except Exception:
                break
        TOBEOL_RANKING_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        TOBEOL_RANKING_CACHE_PATH.write_text(
            json.dumps(
                {"fetched_at": datetime.now().isoformat(), "rows": all_rows},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"Boss ranking: cached {len(all_rows)} total rows → {TOBEOL_RANKING_CACHE_PATH}")
    except Exception as exc:
        print(f"Boss ranking: fetch failed ({exc})")

    return sorted(
        [r for r in all_rows if r.get("guild") in target_guilds],
        key=lambda x: _safe_int(x.get("rank", 999999)),
    )


def _build_tobeol_ranking_analytics(guild_names: list[str]) -> dict[str, Any]:
    rows = fetch_tobeol_ranking(guild_names)
    by_guild: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_guild.setdefault(str(row.get("guild", "")), []).append(row)
    guild_summaries = [
        {
            "guild_name": name,
            "count": len(by_guild.get(name, [])),
            "best_rank": by_guild[name][0]["rank"] if by_guild.get(name) else None,
            "best_nickname": by_guild[name][0]["nickname"] if by_guild.get(name) else None,
            "best_score": by_guild[name][0]["score"] if by_guild.get(name) else None,
        }
        for name in guild_names
    ]
    return {
        "guild_summaries": guild_summaries,
        "total_found": len(rows),
        "all_rows": rows,
    }


def build_tobeol_display_ranking(
    guild_seed_name: str,
    tobeol_ranking: dict[str, Any],
    guild_members: list[dict[str, Any]],
) -> dict[str, Any]:
    ranked_rows = list(tobeol_ranking.get("all_rows", []))
    ranked_lookup = {clean_text(str(row.get("nickname", ""))): row for row in ranked_rows}
    display_rows: list[dict[str, Any]] = list(ranked_rows)

    for member in guild_members:
        nickname = clean_text(str(member.get("nickname", "")))
        if not nickname or nickname in ranked_lookup:
            continue
        level_value = member.get("level", 0)
        level_text = f"Lv.{int(level_value)}" if str(level_value).isdigit() else ""
        display_rows.append(
            {
                "rank": None,
                "nickname": str(member.get("nickname", "")),
                "guild": guild_seed_name,
                "likes": "",
                "level": level_text,
                "job": str(member.get("job_name", "")),
                "score": TOBEOL_UNRANKED_LABEL,
                "page": None,
                "is_unranked": True,
            }
        )

    display_rows.sort(
        key=lambda row: (
            1 if row.get("rank") is None else 0,
            _safe_int(row.get("rank", 999999), 999999),
            clean_text(str(row.get("nickname", ""))),
        )
    )

    guild_summaries = []
    for card in tobeol_ranking.get("guild_summaries", []):
        guild_name = str(card.get("guild_name", ""))
        total_members = len([member for member in guild_members if guild_name == guild_seed_name]) if guild_name == guild_seed_name else int(card.get("count", 0))
        ranked_count = int(card.get("count", 0))
        guild_summaries.append(
            {
                **card,
                "total_members": total_members,
                "unranked_count": max(total_members - ranked_count, 0),
            }
        )

    return {
        **tobeol_ranking,
        "all_rows": display_rows,
        "guild_summaries": guild_summaries,
    }


def build_tobeol_snapshot_data(
    guild_seed_name: str,
    snapshot_date: str,
    tobeol_ranking: dict[str, Any],
) -> dict[str, Any]:
    rows = sorted(
        list(tobeol_ranking.get("all_rows", [])),
        key=lambda row: _safe_int(row.get("rank", 999999)),
    )
    member_map: dict[str, Any] = {}
    top10_keys: list[str] = []
    for index, row in enumerate(rows):
        member_key = "::".join(
            [
                clean_text(str(row.get("nickname", ""))),
                clean_text(str(row.get("job", ""))),
                clean_text(str(row.get("level", ""))),
            ]
        )
        member_map[member_key] = {
            "nickname": str(row.get("nickname", "")),
            "job": str(row.get("job", "")),
            "level": str(row.get("level", "")),
            "rank": _safe_int(row.get("rank", 0)),
            "score": str(row.get("score", "")),
            "likes": str(row.get("likes", "")),
        }
        if index < 10:
            top10_keys.append(member_key)

    best_row = rows[0] if rows else {}
    rank_values = [_safe_int(row.get("rank", 0)) for row in rows if _safe_int(row.get("rank", 0)) > 0]

    return {
        "guild_seed_name": guild_seed_name,
        "report_mode": "tobeol",
        "snapshot_date": snapshot_date,
        "guilds": {
            guild_seed_name: {
                "guild_name": guild_seed_name,
                "count": len(rows),
                "best_rank": _safe_int(best_row.get("rank", 0)),
                "best_nickname": str(best_row.get("nickname", "")),
                "best_score": str(best_row.get("score", "")),
                "avg_rank": round(sum(rank_values) / len(rank_values), 1) if rank_values else 0,
                "members": member_map,
                "top10_keys": top10_keys,
            }
        },
    }


def build_tobeol_history_analysis(current_snapshot: dict[str, Any], history_snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    guild_name = str(current_snapshot.get("guild_seed_name", ""))
    compatible_history = [
        snapshot
        for snapshot in history_snapshots
        if _safe_snapshot_mode(snapshot, "tobeol") == "tobeol"
    ]
    previous_snapshot = compatible_history[-1] if compatible_history else None
    timeline_snapshots = compatible_history[-6:] + [current_snapshot]
    current_guild = current_snapshot.get("guilds", {}).get(guild_name, {})
    previous_guild = previous_snapshot.get("guilds", {}).get(guild_name, {}) if previous_snapshot else {}
    current_members = current_guild.get("members", {})
    previous_members = previous_guild.get("members", {}) if previous_guild else {}

    joined_keys = [key for key in current_members if key not in previous_members]
    departed_keys = [key for key in previous_members if key not in current_members]

    rank_changes: list[dict[str, Any]] = []
    for key, current_member in current_members.items():
        previous_member = previous_members.get(key)
        if not previous_member:
            continue
        rank_delta = _safe_int(previous_member.get("rank", 0)) - _safe_int(current_member.get("rank", 0))
        if rank_delta != 0:
            rank_changes.append(
                {
                    "nickname": str(current_member.get("nickname", "")),
                    "delta": rank_delta,
                    "current_rank": _safe_int(current_member.get("rank", 0)),
                    "previous_rank": _safe_int(previous_member.get("rank", 0)),
                    "current_score": str(current_member.get("score", "")),
                    "previous_score": str(previous_member.get("score", "")),
                }
            )
    rank_changes.sort(key=lambda item: item["delta"], reverse=True)

    guild_timeline = [snapshot for snapshot in timeline_snapshots if guild_name in snapshot.get("guilds", {})]
    trend_labels = [str(snapshot.get("snapshot_date", ""))[5:].replace("-", "/") for snapshot in guild_timeline]
    count_values = [_safe_int(snapshot.get("guilds", {}).get(guild_name, {}).get("count", 0)) for snapshot in guild_timeline]
    best_rank_values = [
        _safe_int(snapshot.get("guilds", {}).get(guild_name, {}).get("best_rank", 0))
        for snapshot in guild_timeline
        if _safe_int(snapshot.get("guilds", {}).get(guild_name, {}).get("best_rank", 0)) > 0
    ]
    inverted_best_rank_values = []
    if best_rank_values:
        worst_rank = max(best_rank_values)
        inverted_best_rank_values = [worst_rank - value for value in best_rank_values]

    current_best_rank = _safe_int(current_guild.get("best_rank", 0))
    previous_best_rank = _safe_int(previous_guild.get("best_rank", 0)) if previous_guild else 0

    retained_top10_count = len(set(current_guild.get("top10_keys", [])) & set(previous_guild.get("top10_keys", []))) if previous_guild else 0

    return {
        "has_previous": previous_snapshot is not None,
        "previous_date": str(previous_snapshot.get("snapshot_date", "")) if previous_snapshot else "",
        "current_date": str(current_snapshot.get("snapshot_date", "")),
        "current_guild": current_guild,
        "current_count": _safe_int(current_guild.get("count", 0)),
        "count_delta": _safe_int(current_guild.get("count", 0)) - _safe_int(previous_guild.get("count", 0)) if previous_guild else 0,
        "current_best_rank": current_best_rank,
        "best_rank_delta": previous_best_rank - current_best_rank if previous_best_rank and current_best_rank else 0,
        "retained_top10_count": retained_top10_count,
        "joined_members": [str(current_members[key].get("nickname", "")) for key in joined_keys],
        "departed_members": [str(previous_members[key].get("nickname", "")) for key in departed_keys],
        "rank_movers": rank_changes[:8],
        "trend_labels": trend_labels,
        "count_values_trend": count_values,
        "best_rank_values_trend": best_rank_values,
        "count_trend_svg": build_sparkline(count_values),
        "best_rank_trend_svg": build_sparkline(inverted_best_rank_values) if inverted_best_rank_values else "",
    }


def render_tobeol_history_section(history_analysis: dict[str, Any]) -> str:
    if not history_analysis.get("has_previous"):
        return (
            '<section class="auto-summary-grid tobeol-history-summary">'
            + render_summary_card_html(
                "토벌전 히스토리 비교 준비 중",
                EMPTY_HISTORY_VALUE,
                "다음 자동 갱신부터 토벌전 랭커 수, 최고 순위, 신규 진입/이탈 비교가 누적됩니다.",
                "auto-summary-card auto-summary-card-empty",
            )
            + "</section>"
        )

    current_guild = history_analysis.get("current_guild", {})
    current_best_rank = _safe_int(history_analysis.get("current_best_rank", 0))
    best_rank_text = f"#{current_best_rank}" if current_best_rank else "-"
    cards = [
        ("비교 기준일", history_analysis.get("previous_date", "-"), f"현재 스냅샷 {escape(str(history_analysis.get('current_date', '')))}"),
        ("랭커 수", f"{_safe_int(history_analysis.get('current_count', 0))}명", f"직전 대비 {format_delta(_safe_int(history_analysis.get('count_delta', 0)), '명')}"),
        ("최고 순위", best_rank_text, format_rank_delta(_safe_int(history_analysis.get("best_rank_delta", 0)))),
        ("TOP10 유지", f"{_safe_int(history_analysis.get('retained_top10_count', 0))}명", f"신규 {len(history_analysis.get('joined_members', []))}명 · 이탈 {len(history_analysis.get('departed_members', []))}명"),
    ]
    summary_html = '<section class="auto-summary-grid tobeol-history-summary">' + ''.join(
        render_summary_card_html(label, value, help_text, "auto-summary-card")
        for label, value, help_text in cards
    ) + "</section>"

    movement_html = ''.join(
        f'<li><span>{escape(str(name))}</span><strong>{LABEL_NEW}</strong></li>' for name in history_analysis.get("joined_members", [])[:8]
    ) or EMPTY_HISTORY_FALLBACK_ITEM
    departed_html = ''.join(
        f'<li><span>{escape(str(name))}</span><strong>{LABEL_OUT}</strong></li>' for name in history_analysis.get("departed_members", [])[:8]
    ) or EMPTY_HISTORY_FALLBACK_ITEM
    mover_html = ''.join(
        f'<li><span>{escape(str(item["nickname"]))}</span><strong>{format_rank_delta(_safe_int(item["delta"], 0))}</strong></li>'
        for item in history_analysis.get("rank_movers", [])
    ) or '<li><span>순위 변동 없음</span><strong>-</strong></li>'

    trend_count_html = history_analysis.get("count_trend_svg", "") or '<div class="history-empty">데이터 없음</div>'
    trend_rank_html = history_analysis.get("best_rank_trend_svg", "") or '<div class="history-empty">데이터 없음</div>'
    best_nickname = str(current_guild.get("best_nickname", ""))
    best_score = str(current_guild.get("best_score", ""))

    detail_html = f"""
    <section class="analytics-grid analytics-grid-2 tobeol-history-grid">
      <article class="history-panel">
        <h5>토벌전 추이</h5>
        <div class="trend-chart-grid">
          <div class="trend-chart-card">
            <span>랭커 수</span>
            <div class="trend-chart">{trend_count_html}</div>
            <p class="simulation-copy">{escape(' → '.join(history_analysis.get('trend_labels', [])) or '기록 없음')}</p>
          </div>
          <div class="trend-chart-card">
            <span>최고 순위</span>
            <div class="trend-chart trend-chart-secondary">{trend_rank_html}</div>
            <p class="simulation-copy">현재 최고 {escape(best_rank_text)} · {escape(best_nickname)}{(' · ' + best_score) if best_score else ''}</p>
          </div>
        </div>
      </article>
      <article class="history-panel analytics-list-card">
        <h5>랭커 출입 변동</h5>
        <div class="analytics-list-split">
          <div>
            <strong>신규 진입 {len(history_analysis.get('joined_members', []))}명</strong>
            <ul class="history-list history-list-compact">{movement_html}</ul>
          </div>
          <div>
            <strong>이탈 {len(history_analysis.get('departed_members', []))}명</strong>
            <ul class="history-list history-list-compact">{departed_html}</ul>
          </div>
        </div>
      </article>
      <article class="history-panel analytics-list-card">
        <h5>순위 변동 상위</h5>
        <ul class="history-list history-list-compact">{mover_html}</ul>
      </article>
    </section>
    """
    return summary_html + detail_html


def build_snapshot_analytics(
    current_snapshot: dict[str, Any],
    history_snapshots: list[dict[str, Any]],
    simulation: dict[str, Any],
) -> dict[str, Any]:
    report_mode = str(current_snapshot.get("report_mode", "league"))
    guild_seed_name = str(current_snapshot.get("guild_seed_name", ""))
    history_analysis = build_history_analysis(current_snapshot, history_snapshots)
    compatible_history = [
        snapshot
        for snapshot in history_snapshots
        if _safe_snapshot_mode(snapshot, report_mode) == report_mode
    ]
    timeline_snapshots = compatible_history[-6:] + [current_snapshot]
    seed_timeline = []
    growth_cards: list[dict[str, Any]] = []
    member_movement_cards: list[dict[str, Any]] = []
    job_distribution_cards: list[dict[str, Any]] = []
    competitor_rows: list[dict[str, Any]] = []
    personal_growth_cards: list[dict[str, Any]] = []

    guild_rankings = simulation.get("guild_rankings", [])
    guild_total_score = {
        str(row.get("guild_name", "")): _safe_int(row.get("total_score", 0))
        for row in guild_rankings
    }
    members_by_guild: dict[str, list[dict[str, Any]]] = {}
    for member in simulation.get("ranked_members", []):
        members_by_guild.setdefault(str(member.get("guild_name", "")), []).append(member)

    for snapshot in timeline_snapshots:
        seed_guild = snapshot.get("guilds", {}).get(guild_seed_name, {})
        if not seed_guild:
            continue
        seed_timeline.append(
            {
                "date": str(snapshot.get("snapshot_date", "")),
                "member_count": _safe_int(seed_guild.get("member_count", 0)),
                "guild_power_text": format_man_units(_safe_int(seed_guild.get("guild_power_value", 0))),
                "simulation_rank": _safe_int(seed_guild.get("simulation_rank", 0)),
                "simulation_score_text": format_score(_safe_int(seed_guild.get("simulation_score", 0))),
            }
        )

    for guild_name, guild in current_snapshot.get("guilds", {}).items():
        history = history_analysis.get("guilds", {}).get(guild_name, {})
        job_counts = {str(key): _safe_int(value) for key, value in guild.get("job_counts", {}).items()}
        member_count = max(_safe_int(guild.get("member_count", 0)), 1)
        sorted_jobs = sorted(job_counts.items(), key=lambda item: (-item[1], item[0]))
        top_job_name, top_job_count = sorted_jobs[0] if sorted_jobs else ("미확인", 0)
        growth_cards.append(
            {
                "guild_name": guild_name,
                "power_delta_pct": float(history.get("guild_power_delta_pct", 0)),
                "simulation_delta": _safe_int(history.get("simulation_score_delta", 0)),
                "member_delta": _safe_int(history.get("member_count_delta", 0)),
                "power_trend_svg": str(history.get("power_trend_svg", "")),
                "simulation_trend_svg": str(history.get("simulation_trend_svg", "")),
            }
        )
        member_movement_cards.append(
            {
                "guild_name": guild_name,
                "joined": list(history.get("joined_members", [])),
                "departed": list(history.get("departed_members", [])),
                "joined_count": _safe_int(history.get("joined_count", 0)),
                "departed_count": _safe_int(history.get("departed_count", 0)),
            }
        )
        job_distribution_cards.append(
            {
                "guild_name": guild_name,
                "top_job": top_job_name,
                "top_job_share": round((top_job_count / member_count) * 100, 1),
                "balance_score": _calculate_job_balance_score(job_counts, member_count),
                "jobs": [
                    {"job_name": job_name, "count": count, "share_pct": round((count / member_count) * 100, 1)}
                    for job_name, count in sorted_jobs[:5]
                ],
            }
        )

    seed_power = _safe_int(current_snapshot.get("guilds", {}).get(guild_seed_name, {}).get("guild_power_value", 0))
    seed_score = _safe_int(current_snapshot.get("guilds", {}).get(guild_seed_name, {}).get("simulation_score", 0))
    for guild_name, guild in current_snapshot.get("guilds", {}).items():
        if guild_name == guild_seed_name:
            continue
        competitor_rows.append(
            {
                "guild_name": guild_name,
                "power_gap_text": format_man_units(abs(_safe_int(guild.get("guild_power_value", 0)) - seed_power)),
                "power_gap_sign": "+" if _safe_int(guild.get("guild_power_value", 0)) >= seed_power else "-",
                "score_gap_text": format_score(abs(_safe_int(guild.get("simulation_score", 0)) - seed_score)),
                "score_gap_sign": "+" if _safe_int(guild.get("simulation_score", 0)) >= seed_score else "-",
                "rank": _safe_int(guild.get("simulation_rank", 0)),
            }
        )

    contribution_cards: list[dict[str, Any]] = []
    efficiency_cards: list[dict[str, Any]] = []
    for guild_name, ranked_members in members_by_guild.items():
        total_score = max(guild_total_score.get(guild_name, 0), 1)
        contributors = sorted(
            [
                {
                    "nickname": str(member.get("nickname", "")),
                    "job_name": str(member.get("job_name", "")),
                    "share_pct": round((_safe_int(member.get("score", 0)) / total_score) * 100, 1),
                    "score_text": format_score(_safe_int(member.get("score", 0))),
                }
                for member in ranked_members
            ],
            key=lambda item: (-float(item["share_pct"]), item["nickname"]),
        )[:5]
        efficiencies = sorted(
            [
                {
                    "nickname": str(member.get("nickname", "")),
                    "job_name": str(member.get("job_name", "")),
                    "efficiency": round((_safe_int(member.get("score", 0)) / max(_safe_int(member.get("combat_power_value", 0)), 1)) * 100_000_000, 1),
                    "score_text": format_score(_safe_int(member.get("score", 0))),
                }
                for member in ranked_members
            ],
            key=lambda item: (-float(item["efficiency"]), item["nickname"]),
        )[:5]
        contribution_cards.append({"guild_name": guild_name, "items": contributors})
        efficiency_cards.append({"guild_name": guild_name, "items": efficiencies})

        personal_candidates: list[dict[str, Any]] = []
        for member in ranked_members:
            member_key = build_member_key(member)
            member_name = str(member.get("nickname", ""))
            trend_values: list[int] = []
            trend_labels: list[str] = []
            gaps = 0
            for snapshot in timeline_snapshots:
                matched_member = _safe_snapshot_member(snapshot, guild_name, member_key, member_name)
                if not matched_member:
                    gaps += 1
                    continue
                trend_values.append(_safe_int(matched_member.get("combat_power_value", 0)))
                trend_labels.append(str(snapshot.get("snapshot_date", ""))[5:].replace("-", "/"))
            if len(trend_values) < 2:
                continue
            delta_value = trend_values[-1] - trend_values[0]
            personal_candidates.append(
                {
                    "nickname": member_name,
                    "job_name": str(member.get("job_name", "")),
                    "delta_value": delta_value,
                    "delta_text": format_man_units(abs(delta_value)),
                    "delta_sign": "+" if delta_value >= 0 else "-",
                    "sparkline_svg": build_sparkline(trend_values, width=180, height=40),
                    "trend_labels": trend_labels,
                    "gap_count": gaps,
                }
            )
        personal_candidates.sort(key=lambda item: item["delta_value"], reverse=True)
        personal_growth_cards.append({"guild_name": guild_name, "items": personal_candidates[:5]})

    prediction_rows: list[dict[str, Any]] = []
    for guild_name, history in history_analysis.get("guilds", {}).items():
        projection = _build_projection([_safe_int(value, 0) for value in history.get("simulation_values_trend", [])])
        if not projection:
            continue
        prediction_rows.append(
            {
                "guild_name": guild_name,
                "projected_value": projection["projected"],
                "projected_text": format_score(projection["projected"]),
                "band_text": f"{format_score(projection['lower'])} ~ {format_score(projection['upper'])}",
                "data_points_used": projection["data_points_used"],
            }
        )
    prediction_rows.sort(key=lambda item: int(item["projected_value"]), reverse=True)
    projected_cut = prediction_rows[0]["projected_text"] if prediction_rows else "데이터 부족"

    overview_cards = [
        {
            "label": "현재 모멘텀",
            "value": next((card["guild_name"] for card in sorted(growth_cards, key=lambda item: item["power_delta_pct"], reverse=True) if card["power_delta_pct"] > 0), "변동 확인"),
            "help": "총 전투력 상승 폭 기준",
        },
        {
            "label": "길드원 이동",
            "value": f"+{history_analysis.get('summary', {}).get('total_joined', 0)} / -{history_analysis.get('summary', {}).get('total_departed', 0)}",
            "help": "최근 비교일 기준 전체 이동",
        },
        {
            "label": "다음 컷 예측",
            "value": projected_cut,
            "help": "현재 히스토리 기반 다음 스냅샷 1위 예상 점수",
        },
    ]

    return {
        "overview_cards": overview_cards,
        "growth_cards": growth_cards,
        "member_movements": member_movement_cards,
        "job_distribution": job_distribution_cards,
        "contribution": contribution_cards,
        "efficiency": efficiency_cards,
        "personal_growth": personal_growth_cards,
        "competitors": competitor_rows,
        "timeline": seed_timeline,
        "predictions": prediction_rows,
        "projected_cut": projected_cut,
        "history_window_days": len(timeline_snapshots),
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


def render_summary_cards(
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    history_analysis: dict[str, Any],
    report_mode: str,
) -> str:
    cards = build_report_summary_cards(guild_rows, members_by_guild, history_analysis, report_mode)[:4]

    return "".join(render_summary_card_html(label, value, help_text, classes) for label, value, help_text, classes in cards)


def render_secondary_summary_cards(
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    history_analysis: dict[str, Any],
    report_mode: str,
) -> str:
    cards = build_report_summary_cards(guild_rows, members_by_guild, history_analysis, report_mode)[4:]
    if not cards:
        return ""
    return '<div class="summary-grid summary-grid-secondary">' + ''.join(
        render_summary_card_html(label, value, help_text, classes)
        for label, value, help_text, classes in cards
    ) + '</div>'


def render_auto_summary_section(history_analysis: dict[str, Any]) -> str:
    copy = get_report_copy(str(history_analysis.get("report_mode", "league")))
    if not history_analysis.get("has_previous"):
        return (
            '<section class="auto-summary-grid">'
            + render_summary_card_html(
                "히스토리 비교 준비 중",
                EMPTY_HISTORY_VALUE,
                "내일부터 길드원 증감, 전투력 변화, 시뮬레이션 변화 요약이 자동으로 표시된다.",
                "auto-summary-card auto-summary-card-empty",
            )
            + "</section>"
        )

    summary = history_analysis["summary"]
    cards = [
        ("길드원 증감", f"+{summary['total_joined']} / -{summary['total_departed']}", f"비교 기준일 {history_analysis['previous_date']} 대비"),
        ("총 전투력 최대 상승", summary["best_power_guild"] or "-", format_man_units(abs(summary["best_power_delta"])) if summary["best_power_guild"] else "변화 없음"),
        (copy["auto_summary_best"], summary["best_sim_guild"] or "-", format_man_units(abs(summary["best_sim_delta"])) if copy["simulation_metric_short"] == "예상 지표" and summary["best_sim_guild"] else format_score(abs(summary["best_sim_delta"])) if summary["best_sim_guild"] else "변화 없음"),
        ("상위권 고정도 최고", summary["stable_guild"] or "-", f"TOP10 유지 {summary['stable_retained']}명" if summary["stable_guild"] else "데이터 없음"),
    ]
    return '<section class="auto-summary-grid">' + ''.join(
        render_summary_card_html(label, value, help_text, "auto-summary-card")
        for label, value, help_text in cards
    ) + '</section>'


def _render_tobeol_ranking_html(tobeol_ranking: dict[str, Any]) -> str:
    guild_summaries: list[dict[str, Any]] = tobeol_ranking.get("guild_summaries", [])
    all_rows: list[dict[str, Any]] = tobeol_ranking.get("all_rows", [])
    if not all_rows:
        return '<p class="simulation-copy">토벌전 랭킹 데이터가 없습니다. 리포트를 다시 생성하면 자동 수집됩니다.</p>'

    summary_html = '<div class="analytics-grid analytics-grid-2">' + "".join(
        f"""
        <article class="info-panel analytics-stat-card">
          <h5>{escape(str(card["guild_name"]))}</h5>
          <div class="analytics-mini-grid">
            <span>랭커 {int(card["count"])}명 / 전체 {int(card.get("total_members", card["count"]))}명</span>
            <span>최고 순위 {"#" + str(int(card["best_rank"])) if card.get("best_rank") else "-"}</span>
          </div>
          {"<p class='simulation-copy'>" + escape(str(card["best_nickname"])) + " · " + escape(str(card["best_score"])) + (" · " + TOBEOL_UNRANKED_MEMBER_SUFFIX + " " + str(int(card.get("unranked_count", 0))) + "명" if int(card.get("unranked_count", 0)) > 0 else "") + "</p>" if card.get("best_nickname") else "<p class='simulation-copy'>해당 없음</p>"}
        </article>
        """
        for card in guild_summaries
    ) + "</div>"

    guild_names = [str(c["guild_name"]) for c in guild_summaries]
    tab_buttons = "".join(
        f'<button type="button" class="tobeol-ranking-tab{"" if i > 0 else " tobeol-ranking-tab-active"}" data-tobeol-guild="{escape(g)}">{escape(g)}</button>'
        for i, g in enumerate(["전체"] + guild_names)
    )

    rows_html = "".join(
        f"""<tr data-tobeol-guild="{escape(str(row["guild"]))}"{' class="tobeol-unranked-row"' if row.get("is_unranked") else ''}>
          <td><span class="tobeol-rank-chip{' tobeol-rank-chip-muted' if row.get("is_unranked") else ''}">{'#' + str(int(row['rank'])) if row.get('rank') is not None else TOBEOL_UNRANKED_LABEL}</span></td>
          <td class="tobeol-guild-cell">{escape(str(row["guild"]))}</td>
          <td><strong>{escape(str(row["nickname"]))}</strong><div class="tobeol-row-copy">{escape(str(row["level"]))} · {escape(str(row["job"]))}</div></td>
          <td>{escape(str(row["score"]))}</td>
          <td>{TOBEOL_LIKE_PREFIX + escape(str(row['likes'])) if str(row.get('likes', '')).strip() else '-'}</td>
        </tr>"""
        for row in all_rows
    )

    return f"""
    {summary_html}
    <div class="tobeol-ranking-tabs">{tab_buttons}</div>
    <div class="tobeol-ranking-table-wrap">
      <table class="tobeol-ranking-table">
        <thead><tr><th>순위</th><th>길드</th><th>닉네임</th><th>토벌전 점수</th><th>좋아요</th></tr></thead>
        <tbody id="tobeol-ranking-tbody">{rows_html}</tbody>
      </table>
    </div>
    <p class="simulation-copy simulation-copy-muted">mgf.gg 서버 2 기준 · 캐시 12시간</p>
    """


def render_snapshot_overview_section(history_analysis: dict[str, Any]) -> str:
    analytics = history_analysis.get("snapshot_analytics", {})
    cards = analytics.get("overview_cards", [])
    if not cards:
        return ""
    return '<section class="snapshot-overview-grid">' + "".join(
        f"""
        <article class="auto-summary-card snapshot-overview-card" data-modal="snapshot-analytics">
          <p class="summary-label">{escape(str(card['label']))}</p>
          <strong class="summary-value">{escape(str(card['value']))}</strong>
          <p class="summary-help">{escape(str(card['help']))}</p>
          <span class="card-jump">스냅샷 분석 열기 ↘</span>
        </article>
        """
        for card in cards
    ) + "</section>"


def render_snapshot_analytics_modal(history_analysis: dict[str, Any], report_mode: str) -> str:
    analytics = history_analysis.get("snapshot_analytics", {})
    if not analytics:
        return ""

    def render_module(module_id: str, title: str, takeaway: str, badge: str, body_html: str, *, expanded: bool = False) -> str:
        return f"""
        <section class="analytics-module {'expanded' if expanded else ''}">
          <button type="button" class="analytics-module-toggle" aria-expanded="{'true' if expanded else 'false'}" data-module="{escape(module_id)}">
            <div>
              <p class="eyebrow">Snapshot Module</p>
              <h4>{escape(title)}</h4>
              <p class="simulation-copy">{escape(takeaway)}</p>
            </div>
            <div class="analytics-module-toggle-meta">
              <span class="job-coefficient-summary">{escape(badge)}</span>
              <span class="simulation-section-toggle-label">상세 보기</span>
            </div>
          </button>
          <div class="analytics-module-body" {'hidden' if not expanded else ''}>
            {body_html}
          </div>
        </section>
        """

    def render_chapter(chapter_id: str, title: str, summary: str, modules_html: str, *, expanded: bool = False) -> str:
        return f"""
        <section class="analytics-chapter {'expanded' if expanded else ''}">
          <button type="button" class="analytics-chapter-toggle" aria-expanded="{'true' if expanded else 'false'}" data-chapter="{escape(chapter_id)}">
            <div>
              <p class="eyebrow">Analytics Chapter</p>
              <h3>{escape(title)}</h3>
              <p class="simulation-copy">{escape(summary)}</p>
            </div>
            <span class="simulation-section-toggle-label">상세 보기</span>
          </button>
          <div class="analytics-chapter-body" {'hidden' if not expanded else ''}>
            {modules_html}
          </div>
        </section>
        """

    growth_html = '<div class="analytics-grid analytics-grid-2">' + ''.join(
        f"""
        <article class="info-panel analytics-stat-card">
          <h5>{escape(str(card['guild_name']))}</h5>
          <div class="analytics-mini-grid">
            <span>총전투력 {format_percent_delta(float(card['power_delta_pct']))}</span>
            <span>인원 {format_delta(int(card['member_delta']), '명')}</span>
            <span>{'예상 지표' if report_mode == 'training' else '예상 점수'} {format_metric_delta(int(card['simulation_delta']), report_mode == 'training')}</span>
          </div>
          <div class="trend-chart-card"><span>총 전투력</span><div class="trend-chart">{card['power_trend_svg']}</div></div>
          <div class="trend-chart-card"><span>{'수련장 지표' if report_mode == 'training' else '대항전 점수'}</span><div class="trend-chart trend-chart-secondary">{card['simulation_trend_svg']}</div></div>
        </article>
        """
        for card in analytics.get("growth_cards", [])
    ) + '</div>'

    movement_html = '<div class="analytics-grid analytics-grid-2">' + ''.join(
        f"""
        <article class="history-panel analytics-list-card">
          <h5>{escape(str(card['guild_name']))}</h5>
          <div class="analytics-list-split">
            <div>
              <strong>신규 {int(card['joined_count'])}명</strong>
              <ul class="history-list history-list-compact">{''.join(f'<li><span>{escape(name)}</span><strong>{LABEL_JOIN}</strong></li>' for name in card['joined'][:8]) or EMPTY_HISTORY_FALLBACK_ITEM}</ul>
            </div>
            <div>
              <strong>이탈 {int(card['departed_count'])}명</strong>
              <ul class="history-list history-list-compact">{''.join(f'<li><span>{escape(name)}</span><strong>{LABEL_LEAVE}</strong></li>' for name in card['departed'][:8]) or EMPTY_HISTORY_FALLBACK_ITEM}</ul>
            </div>
          </div>
        </article>
        """
        for card in analytics.get("member_movements", [])
    ) + '</div>'

    competitor_html = '<div class="analytics-grid analytics-grid-2">' + ''.join(
        f"""
        <article class="comparison-callout comparison-callout-gap">
          <span>{escape(str(row['guild_name']))}</span>
          <strong>시뮬 {row['score_gap_sign']}{escape(str(row['score_gap_text']))}</strong>
          <em>전투력 {row['power_gap_sign']}{escape(str(row['power_gap_text']))} · 현재 {int(row['rank'])}위</em>
        </article>
        """
        for row in analytics.get("competitors", [])
    ) + '</div>'

    contribution_html = '<div class="analytics-grid analytics-grid-2">' + ''.join(
        f"""
        <article class="history-panel analytics-list-card">
          <h5>{escape(str(card['guild_name']))}</h5>
          <ul class="history-list history-list-compact">{
            ''.join(f'<li><span>{escape(item["nickname"])} · {escape(item["job_name"])} </span><strong>{escape(item["score_text"])} / {item["share_pct"]}%</strong></li>' for item in card['items'])
          }</ul>
        </article>
        """
        for card in analytics.get("contribution", [])
    ) + '</div>'

    efficiency_html = '<div class="analytics-grid analytics-grid-2">' + ''.join(
        f"""
        <article class="history-panel analytics-list-card">
          <h5>{escape(str(card['guild_name']))}</h5>
          <ul class="history-list history-list-compact">{
            ''.join(f'<li><span>{escape(item["nickname"])} · {escape(item["job_name"])} </span><strong>{item["efficiency"]:.1f} 점/억</strong></li>' for item in card['items'])
          }</ul>
        </article>
        """
        for card in analytics.get("efficiency", [])
    ) + '</div>'

    job_html = '<div class="analytics-grid analytics-grid-2">' + ''.join(
        f"""
        <article class="info-panel analytics-list-card">
          <h5>{escape(str(card['guild_name']))}</h5>
          <p class="simulation-copy">균형도 {card['balance_score']}점 · 최다 직업 {escape(str(card['top_job']))} {card['top_job_share']}%</p>
          <div class="analytics-bar-list">{
            ''.join(f'<div class="analytics-bar-item"><span>{escape(job["job_name"])} · {job["count"]}명</span><strong>{job["share_pct"]}%</strong><div class="power-meter"><span style="width:{job["share_pct"]}%"></span></div></div>' for job in card['jobs'])
          }</div>
        </article>
        """
        for card in analytics.get("job_distribution", [])
    ) + '</div>'

    personal_html = '<div class="analytics-grid analytics-grid-2">' + ''.join(
        f"""
        <article class="history-panel analytics-list-card">
          <h5>{escape(str(card['guild_name']))}</h5>
          <div class="analytics-person-list">{
            ''.join(f'<div class="analytics-person-item"><div><strong>{escape(item["nickname"])} · {escape(item["job_name"])} </strong><p>{item["delta_sign"]}{escape(item["delta_text"])} · 누락 {int(item["gap_count"])}회</p></div><div class="trend-chart">{item["sparkline_svg"]}</div></div>' for item in card['items']) or '<p class="simulation-copy">히스토리 부족</p>'
          }</div>
        </article>
        """
        for card in analytics.get("personal_growth", [])
    ) + '</div>'

    timeline_html = '<div class="analytics-timeline">' + ''.join(
        f"""
        <article class="score-rule-card analytics-timeline-card">
          <span>{escape(str(item['date']))}</span>
          <strong>{escape(str(item['guild_power_text']))}</strong>
          <em>인원 {int(item['member_count'])}명 · 시뮬 {escape(str(item['simulation_score_text']))}</em>
        </article>
        """
        for item in analytics.get("timeline", [])
    ) + '</div>'

    prediction_html = '<div class="analytics-grid analytics-grid-2">' + ''.join(
        f"""
        <article class="comparison-callout comparison-callout-focus">
          <span>{escape(str(item['guild_name']))}</span>
          <strong>{escape(str(item['projected_text']))}</strong>
          <em>예상 범위 {escape(str(item['band_text']))} · {int(item['data_points_used'])}개 스냅샷 기반</em>
        </article>
        """
        for item in analytics.get("predictions", [])
    ) + f'<article class="comparison-callout comparison-callout-core"><span>다음 컷 예상</span><strong>{escape(str(analytics.get("projected_cut", "데이터 부족")))}</strong><em>현재 보유 히스토리 기준 추정치</em></article></div>'

    chapter_1 = render_chapter(
        "chapter-now",
        "현재 상태 · 지금 무엇이 움직였는지",
        "전투력, 길드원 이동, 경쟁 구도를 먼저 가볍게 훑는다.",
        render_module("growth", "길드 성장 추이 대시보드", "길드별 최근 추세를 빠르게 확인한다.", f"{int(analytics.get('history_window_days', 0))}일 창", growth_html, expanded=True)
        + render_module("movement", "길드원 증감 / 이탈 추적", "누가 들어오고 나갔는지 길드별로 정리한다.", "현재 vs 직전", movement_html)
        + render_module("competitor", "경쟁 길드 비교 리포트", "시드 길드 기준 격차를 본다.", "현재 매칭 4길드", competitor_html),
        expanded=True,
    )
    chapter_2 = render_chapter(
        "chapter-why",
        "변화의 원인 · 무엇이 점수를 움직였는지",
        "기여도, 효율, 직업 구성을 통해 길드의 체질을 읽는다.",
        render_module("contribution", "길드 기여도 랭킹", "길드 총합에서 누가 얼마나 비중을 차지하는지 본다.", "TOP 5", contribution_html)
        + render_module("efficiency", "전투력 대비 효율 분석", "같은 전투력 대비 더 높은 점수를 내는 멤버를 본다.", "점/억", efficiency_html)
        + render_module("jobs", "직업 분포 / 밸런스 분석", "직업 집중도와 길드 구성의 균형을 본다.", "상위 5직업", job_html),
    )
    chapter_3 = render_chapter(
        "chapter-next",
        "사람과 다음 단계 · 누가 성장했고 다음 컷은 어디인지",
        "개인 성장 흐름과 히스토리 타임라인, 다음 컷 예측을 확인한다.",
        render_module("personal", "개인 성장 리포트", "현재 멤버 기준 최근 성장 흐름을 추적한다.", "길드별 TOP 5", personal_html)
        + render_module("timeline", "스냅샷 기반 히스토리 타임라인", "최근 히스토리를 날짜별 카드로 되짚어 본다.", f"{int(analytics.get('history_window_days', 0))}개 기록", timeline_html)
        + render_module("prediction", "미래 점수 예측 / 컷 예측", "현재 보유 스냅샷으로 다음 컷을 보수적으로 추정한다.", "통계 추정치", prediction_html),
    )

    overview_html = ''.join(
        f"""
        <article class="summary-card analytics-summary-card">
          <p class="summary-label">{escape(str(card['label']))}</p>
          <strong class="summary-value">{escape(str(card['value']))}</strong>
          <p class="summary-help">{escape(str(card['help']))}</p>
        </article>
        """
        for card in analytics.get("overview_cards", [])
    )

    return f"""
    <div class="modal-backdrop" id="modal-snapshot-analytics" role="dialog" aria-modal="true" aria-label="스냅샷 분석">
      <div class="modal-box simulation-modal-box snapshot-analytics-modal-box">
        <button type="button" class="modal-close" aria-label="닫기">×</button>
        <section class="simulation-section">
          <div class="simulation-overview">
            <div>
              <p class="eyebrow">Snapshot Insights</p>
              <h3>스냅샷 분석 허브</h3>
              <p class="simulation-copy">메인 화면은 가볍게 유지하고, 깊은 분석은 챕터형 허브 안에서 단계적으로 펼친다. 예측은 현재 보유한 히스토리 범위 기준의 통계 추정치다.</p>
            </div>
          </div>
          <section class="summary-grid analytics-summary-grid">{overview_html}</section>
          <div class="analytics-warning">히스토리 {int(analytics.get('history_window_days', 0))}개 스냅샷 기준 · 예측/효율은 참고용 추정치</div>
          <div class="analytics-chapter-stack">
            {chapter_1}
            {chapter_2}
            {chapter_3}
          </div>
        </section>
      </div>
    </div>
    """


def render_compare_cards(
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    history_analysis: dict[str, Any],
    guild_mark_map: dict[str, str],
) -> str:
    copy = get_report_copy(str(history_analysis.get("report_mode", "league")))
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
                  <div class="guild-card-title-row">
                    {render_guild_mark(guild_name, guild_mark_map, 'guild-card-mark')}
                    <h3>{escape(guild_name)}</h3>
                  </div>
                </div>
                <span class="rank-pill">전체 {escape(str(guild_row['global_rank']))} · 서버 {escape(str(guild_row['server_rank']))}</span>
              </div>
              <div class="rank-badge-row">
                <span class="rank-badge rank-badge-global">{escape(describe_rank_tier(str(guild_row['global_rank']), '전체'))}</span>
                <span class="rank-badge rank-badge-server">{escape(describe_rank_tier(str(guild_row['server_rank']), '서버'))}</span>
              </div>
              <div class="trend-pill-row">
                <span class="trend-pill {trend_pill_tone_class(guild_history.get('guild_power_delta', 0))}">총전투력 {format_percent_delta(float(guild_history.get('guild_power_delta_pct', 0)))} </span>
                <span class="trend-pill {trend_pill_tone_class(guild_history.get('simulation_score_delta', 0))}">{escape(copy['simulation_metric'])} {format_metric_delta(int(guild_history.get('simulation_score_delta', 0)), copy['simulation_metric_short'] == '예상 지표')}</span>
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


def render_detail_comparison_section(
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    guild_mark_map: dict[str, str],
) -> str:
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
                  <div class="guild-card-title-row detail-compare-title-row">
                    {render_guild_mark(guild_name, guild_mark_map, 'guild-card-mark')}
                    <h3>{escape(guild_name)}</h3>
                  </div>
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


def render_guild_war_simulation_modal(simulation: dict[str, Any], history_analysis: dict[str, Any]) -> str:
    simulation_rank_changes = history_analysis.get("simulation_rank_changes", {})
    guild_filter_options = "".join(
        f'<option value="{escape(str(guild_row["guild_name"]))}">{escape(str(guild_row["guild_name"]))}</option>'
        for guild_row in simulation["guild_rankings"]
    )
    preview_count = len(simulation["score_table_preview"])
    preview_cards = "".join(
        f"""
        <article class="score-rule-card">
          <span>{escape(str(row['label']))}</span>
          <strong>{escape(str(row['range']))}</strong>
        </article>
        """
        for row in simulation["score_table_preview"]
    )
    guild_cards = "".join(
        f"""
        <article class="simulation-rank-card rank-{int(guild_row['simulation_rank'])}">
          <button type="button" class="simulation-rank-toggle" aria-expanded="false">
            <div class="simulation-rank-top">
              <span class="simulation-rank-badge">#{int(guild_row['simulation_rank'])}</span>
              <strong>{escape(str(guild_row['guild_name']))}</strong>
            </div>
            <div class="simulation-rank-score">{escape(str(guild_row['total_score_text']))}</div>
            <div class="simulation-rank-summary">{int(guild_row['scoring_count'])}명 · 최고 개인 순위 {int(guild_row['top_finisher_rank'] or 0)}위</div>
            <span class="simulation-section-toggle-label">상세 보기</span>
          </button>
          <div class="simulation-rank-details" hidden>
            <dl class="simulation-rank-meta">
              <div><dt>득점 인원</dt><dd>{int(guild_row['scoring_count'])}명</dd></div>
              <div><dt>최고 개인 순위</dt><dd>{int(guild_row['top_finisher_rank'] or 0)}위 · {escape(str(guild_row['top_finisher_name']))}</dd></div>
            </dl>
          </div>
        </article>
        """
        for guild_row in simulation["guild_rankings"]
    )
    ranked_rows = "".join(
        f"""
        <tr data-guild="{escape(str(member['guild_name']))}" data-rank="{int(member['overall_rank'])}" data-power="{int(member.get('combat_power_value', 0) or 0)}">
          <td>{int(member['overall_rank'])}</td>
          <td>{escape(str(member['guild_name']))}</td>
          <td><a href="{escape(str(member['character_url']))}" target="_blank" rel="noreferrer">{escape(str(member['nickname']))}</a></td>
          <td>{escape(str(member['job_name']))}</td>
          <td>{escape(str(member['combat_power']))}</td>
          <td class="simulation-rank-change-cell">{render_simulation_rank_change_badge(simulation_rank_changes.get(build_member_key(member)), compact=True)}</td>
          <td class="simulation-score-cell">{format_score(int(member['score']))}</td>
        </tr>
        """
        for member in simulation["ranked_members"]
    )
    ranked_cards = "".join(
        f"""
        <article class="simulation-member-card" data-guild="{escape(str(member['guild_name']))}">
          <button type="button" class="simulation-member-toggle" aria-expanded="false">
            <div class="simulation-member-card-top">
              <div class="simulation-member-card-identity">
                <span class="simulation-member-rank">#{int(member['overall_rank'])}</span>
                <div class="simulation-member-primary">
                  <strong>{escape(str(member['nickname']))}</strong>
                  <p>{escape(str(member['guild_name']))} · <strong class="job-name">{escape(str(member['job_name']))}</strong></p>
                  {render_simulation_rank_change_badge(simulation_rank_changes.get(build_member_key(member)))}
                  <span class="simulation-member-power">전투력 {escape(str(member['combat_power']))}</span>
                </div>
              </div>
              <div class="simulation-member-score">
                <span>예상 점수</span>
                <strong>{format_score(int(member['score']))}</strong>
              </div>
            </div>
            <span class="simulation-member-toggle-label">상세 보기</span>
          </button>
          <div class="simulation-member-details" hidden>
            <dl class="simulation-member-meta">
              <div><dt>길드</dt><dd>{escape(str(member['guild_name']))}</dd></div>
              <div><dt>직업</dt><dd><strong class="job-name">{escape(str(member['job_name']))}</strong></dd></div>
              <div><dt>순위</dt><dd>{int(member['overall_rank'])}위</dd></div>
              <div><dt>직전 변동</dt><dd>{render_simulation_rank_change_badge(simulation_rank_changes.get(build_member_key(member)), compact=True)}</dd></div>
              <div><dt>프로필</dt><dd><a href="{escape(str(member['character_url']))}" target="_blank" rel="noreferrer">캐릭터 보기</a></dd></div>
            </dl>
          </div>
        </article>
        """
        for member in simulation["ranked_members"]
    )
    return f"""
    <div class="modal-backdrop" id="modal-guild-war-simulation" role="dialog" aria-modal="true" aria-label="대항전 예상 시뮬레이션">
      <div class="modal-box simulation-modal-box">
        <button type="button" class="modal-close" aria-label="닫기">×</button>
        <section class="simulation-section">
          <div class="simulation-overview">
            <div>
              <p class="eyebrow">Guild War Projection</p>
              <h3>대항전 예상 시뮬레이터</h3>
              <p class="simulation-copy">모든 길드원을 전투력 순으로 다시 정렬한 뒤, 제공된 순위별 점수표를 적용해 길드별 총합 점수를 계산했다.</p>
            </div>
          </div>
          <section class="job-coefficient-section simulation-preview-section">
            <button type="button" class="job-coefficient-toggle" aria-expanded="false">
              <div class="job-coefficient-head">
                <div>
                  <p class="eyebrow">Guild War Score Table</p>
                  <h3>대항전 점수표 미리보기</h3>
                  <p class="simulation-copy">순위별 배점을 기준으로 전원을 다시 합산한 결과다.</p>
                </div>
                <div class="job-coefficient-summary">{preview_count}개 구간</div>
              </div>
              <span class="simulation-section-toggle-label">상세 보기</span>
            </button>
            <div class="job-coefficient-details" hidden>
              <div class="score-rule-grid">{preview_cards}</div>
            </div>
          </section>
          <div class="simulation-rank-grid">{guild_cards}</div>
          <div class="table-wrap simulation-table-wrap">
            <div class="table-toolbar">
              <h3>대항전 예상 개인 순위</h3>
              <div class="toolbar-actions">
                <label class="table-filter-label">
                  <span>길드 필터</span>
                  <select class="guild-filter" data-target="guild-war-simulation-table">
                    <option value="">전체 길드</option>
                    {guild_filter_options}
                  </select>
                </label>
                <span class="hint">전투력 기준 정렬 · 점수표 자동 반영</span>
              </div>
            </div>
            <div class="simulation-mobile-card-list" data-target="guild-war-simulation-table">{ranked_cards}</div>
            <table class="member-table simulation-table guild-war-simulation-table" id="guild-war-simulation-table">
              <thead>
                <tr>
                  <th data-sort="rank">순위</th>
                  <th>길드</th>
                  <th>닉네임</th>
                  <th>직업</th>
                  <th data-sort="power">전투력</th>
                  <th>변동</th>
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


def render_training_simulation_modal(simulation: dict[str, Any], history_analysis: dict[str, Any]) -> str:
    simulation_rank_changes = history_analysis.get("simulation_rank_changes", {})
    guild_filter_options = "".join(
        f'<option value="{escape(str(guild_row["guild_name"]))}">{escape(str(guild_row["guild_name"]))}</option>'
        for guild_row in simulation["guild_rankings"]
    )
    job_cards_by_tier = simulation.get("job_coefficient_cards", {})
    # 불독(아크메이지(불,독) alias) 제거 — 아크메이지(불,독)으로 통합 표시
    def _filter_cards(cards: list[dict[str, str]]) -> list[dict[str, str]]:
        return [row for row in cards if str(row.get("label", "")) != "불독"]

    cards_3rd = _filter_cards(job_cards_by_tier.get("3rd", []))
    cards_4th = _filter_cards(job_cards_by_tier.get("4th", []))

    def _render_card_grid(cards: list[dict[str, str]]) -> str:
        return "".join(
            f"""
        <article class="score-rule-card">
          <span>{escape(str(row['label']))}</span>
          <strong>{escape(str(row['range']))}</strong>
        </article>
        """
            for row in cards
        )

    filtered_coefficient_cards_3rd = _render_card_grid(cards_3rd)
    filtered_coefficient_cards_4th = _render_card_grid(cards_4th)
    coefficient_count = len(cards_4th)
    guild_cards = "".join(
        f"""
        <article class="simulation-rank-card rank-{int(guild_row['simulation_rank'])}">
          <button type="button" class="simulation-rank-toggle" aria-expanded="false">
            <div class="simulation-rank-top">
              <span class="simulation-rank-badge">#{int(guild_row['simulation_rank'])}</span>
              <strong>{escape(str(guild_row['guild_name']))}</strong>
            </div>
            <div class="simulation-rank-score">{escape(str(guild_row['total_score_text']))}</div>
            <div class="simulation-rank-summary">{int(guild_row['member_count'])}명 · 최고 개인 순위 {int(guild_row['top_finisher_rank'] or 0)}위</div>
            <span class="simulation-section-toggle-label">상세 보기</span>
          </button>
          <div class="simulation-rank-details" hidden>
            <dl class="simulation-rank-meta">
              <div><dt>참여 인원</dt><dd>{int(guild_row['member_count'])}명</dd></div>
              <div><dt>평균 직업 보정</dt><dd>×{float(guild_row['avg_job_ratio']):.3f}</dd></div>
              <div><dt>주요 직업 구성</dt><dd>{escape(str(guild_row['job_mix_text']))}</dd></div>
              <div><dt>최고 예상 순위</dt><dd>{int(guild_row['top_finisher_rank'] or 0)}위 · {escape(str(guild_row['top_finisher_name']))}</dd></div>
            </dl>
          </div>
        </article>
        """
        for guild_row in simulation["guild_rankings"]
    )
    ranked_rows = "".join(
        f"""
        <tr data-guild="{escape(str(member['guild_name']))}" data-rank="{int(member['overall_rank'])}" data-power="{int(member['combat_power_value'])}" data-level="{int(member.get('level', 0) or 0)}">
          <td>{int(member['overall_rank'])}</td>
          <td>{escape(str(member['guild_name']))}</td>
          <td><a href="{escape(str(member['character_url']))}" target="_blank" rel="noreferrer">{escape(str(member['nickname']))}</a></td>
          <td><strong class="job-name">{escape(str(member['job_name']))}</strong></td>
          <td>{escape(str(member['combat_power']))}</td>
          <td>Lv.{int(member.get('level', 0)) if member.get('level') else '?'}</td>
          <td class="simulation-rank-change-cell">{render_simulation_rank_change_badge(simulation_rank_changes.get(build_member_key(member)), compact=True)}</td>
          <td class="simulation-score-cell">{escape(str(member['estimated_metric_text']))}</td>
        </tr>
        """
        for member in simulation["ranked_members"]
    )
    ranked_cards = "".join(
        f"""
        <article class="simulation-member-card" data-guild="{escape(str(member['guild_name']))}">
          <button type="button" class="simulation-member-toggle" aria-expanded="false">
            <div class="simulation-member-card-top">
              <div class="simulation-member-card-identity">
                <span class="simulation-member-rank">#{int(member['overall_rank'])}</span>
                <div class="simulation-member-primary">
                  <strong>{escape(str(member['nickname']))}</strong>
                  <p>{escape(str(member['guild_name']))} · <strong class="job-name">{escape(str(member['job_name']))}</strong></p>
                  {render_simulation_rank_change_badge(simulation_rank_changes.get(build_member_key(member)))}
                  <span class="simulation-member-power">전투력 {escape(str(member['combat_power']))}</span>
                </div>
              </div>
              <div class="simulation-member-score">
                <span>예상 점수</span>
                <strong>{format_score(int(member['score']))}</strong>
              </div>
            </div>
            <span class="simulation-member-toggle-label">상세 보기</span>
          </button>
          <div class="simulation-member-details" hidden>
            <dl class="simulation-member-meta">
              <div><dt>길드</dt><dd>{escape(str(member['guild_name']))}</dd></div>
              <div><dt>레벨</dt><dd>Lv.{int(member.get('level', 0)) if member.get('level') else '?'}</dd></div>
              <div><dt>직업</dt><dd><strong class="job-name">{escape(str(member['job_name']))}</strong></dd></div>
              <div><dt>순위</dt><dd>{int(member['overall_rank'])}위</dd></div>
              <div><dt>직전 변동</dt><dd>{render_simulation_rank_change_badge(simulation_rank_changes.get(build_member_key(member)), compact=True)}</dd></div>
              <div><dt>프로필</dt><dd><a href="{escape(str(member['character_url']))}" target="_blank" rel="noreferrer">캐릭터 보기</a></dd></div>
            </dl>
          </div>
        </article>
        """
        for member in simulation["ranked_members"]
    )
    return f"""
    <div class="modal-backdrop" id="modal-guild-war-simulation" role="dialog" aria-modal="true" aria-label="수련장 예상 시뮬레이터">
      <div class="modal-box simulation-modal-box">
        <button type="button" class="modal-close" aria-label="닫기">×</button>
        <section class="simulation-section">
          <div class="simulation-overview">
            <div>
              <p class="eyebrow">Training Simulator</p>
              <h3>수련장 예상 시뮬레이터</h3>
              <p class="simulation-copy">레벨 + 전투력 + 직업 기반 수련장 예상 점수 (실제 70명 데이터 역산 모델). 평균 오차 ~12.1% — 참고용 추정치.</p>
            </div>
          </div>
          <section class="job-coefficient-section">
            <button type="button" class="job-coefficient-toggle" aria-expanded="false">
              <div class="job-coefficient-head">
                <div>
                  <p class="eyebrow">Job Coefficients</p>
                  <h3>직업 보정 계수</h3>
                  <p class="simulation-copy">커뮤니티 밸런스 분석 기준 (MGF.GG 2026-04-09). 비숍 = 1.000 기준, 3차/4차 전직별 별도 적용.</p>
                </div>
                <div class="job-coefficient-summary">{coefficient_count}개 직업</div>
              </div>
              <span class="simulation-section-toggle-label">상세 보기</span>
            </button>
            <div class="job-coefficient-details" hidden>
              <div class="coeff-tier-tabs" role="tablist">
                <button type="button" class="coeff-tier-tab active" role="tab" data-tier="3rd">3차 전직 (Lv.60~99)</button>
                <button type="button" class="coeff-tier-tab" role="tab" data-tier="4th">4차 전직 (Lv.100+)</button>
              </div>
              <div class="coeff-tier-panel" data-tier="3rd">
                <div class="job-coefficient-grid">{filtered_coefficient_cards_3rd}</div>
              </div>
              <div class="coeff-tier-panel" data-tier="4th" hidden>
                <div class="job-coefficient-grid">{filtered_coefficient_cards_4th}</div>
              </div>
            </div>
          </section>
          <div class="simulation-rank-grid">{guild_cards}</div>
          <div class="table-wrap simulation-table-wrap">
            <div class="table-toolbar">
              <h3>수련장 예상 개인 순위</h3>
              <div class="toolbar-actions">
                <label class="table-filter-label">
                  <span>길드 필터</span>
                  <select class="guild-filter" data-target="training-simulation-table">
                    <option value="">전체 길드</option>
                    {guild_filter_options}
                  </select>
                </label>
                <span class="hint">예상 점수 = 직업 기준값 × 레벨^0.2 × 전투력^0.2</span>
              </div>
            </div>
            <div class="simulation-mobile-card-list" data-target="training-simulation-table">{ranked_cards}</div>
            <table class="member-table simulation-table training-simulation-table" id="training-simulation-table">
              <thead>
                <tr>
                  <th data-sort="rank">순위</th>
                  <th>길드</th>
                  <th>닉네임</th>
                  <th>직업</th>
                  <th data-sort="power">전투력</th>
                  <th data-sort="level">레벨</th>
                  <th>변동</th>
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


def render_guild_modals(
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    history_analysis: dict[str, Any],
    guild_mark_map: dict[str, str],
) -> str:
    copy = get_report_copy(str(history_analysis.get("report_mode", "league")))
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
                <button type="button" class="modal-close" aria-label="닫기">×</button>
                <div class="section-head">
                  <div>
                    <p class="eyebrow">{escape(str(guild_row['server_display']))} · 기준일 {escape(str(guild_row['data_date']))}</p>
                    <div class="modal-title-row">
                      {render_guild_mark(guild_name, guild_mark_map, 'modal-guild-mark')}
                      <h2>{escape(guild_name)}</h2>
                    </div>
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
                      <li><span>{escape(copy['simulation_delta'])}</span><strong>{format_metric_delta(int(guild_history.get('simulation_score_delta', 0)), copy['simulation_metric_short'] == '예상 지표')}</strong></li>
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
                      <span>{escape(copy['trend_label'])}</span>
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


def build_tobeol_html_report(
    guild_seed_name: str,
    html_output_path: Path,
    tobeol_ranking: dict[str, Any],
    tobeol_history_analysis: dict[str, Any],
    guild_members: list[dict[str, Any]],
) -> Path:
    font_face_map = build_font_face_map(html_output_path)
    font_light_path = font_face_map.get("light", "")
    font_bold_path = font_face_map.get("bold", "")
    guild_mark_map = build_guild_mark_map([guild_seed_name], html_output_path)
    hero_guild_mark_html = render_guild_mark(guild_seed_name, guild_mark_map, "hero-title-mark")
    history_html = render_tobeol_history_section(tobeol_history_analysis)
    display_ranking = build_tobeol_display_ranking(guild_seed_name, tobeol_ranking, guild_members)
    ranking_html = _render_tobeol_ranking_html(display_ranking)
    primary_summary = next(iter(display_ranking.get("guild_summaries", [])), {})
    hero_meta_html = "".join(
        f"""
        <article class="hero-meta-card">
          <strong>{escape(str(label))}</strong>
          <span>{escape(str(value))}</span>
          <small>{escape(str(help_text))}</small>
        </article>
        """
        for label, value, help_text in [
            ("길드 기준", guild_seed_name, f"랭커 {int(primary_summary.get('count', 0))}명 / 전체 {int(primary_summary.get('total_members', len(guild_members)))}명"),
            ("최고 순위", f"#{int(primary_summary.get('best_rank', 0))}" if primary_summary.get('best_rank') else "-", str(primary_summary.get('best_nickname', '해당 없음'))),
            ("히스토리 기준", str(tobeol_history_analysis.get('previous_date', '') or '첫 비교 전'), "직전 토벌전 기록과 비교"),
        ]
    )
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(guild_seed_name)} 길드 토벌전 랭킹</title>
  <style>
    @font-face {{
      font-family: "Maplestory";
      src: url("{escape(font_light_path)}") format("truetype");
      font-weight: 400;
      font-style: normal;
      font-display: swap;
    }}
    @font-face {{
      font-family: "Maplestory";
      src: url("{escape(font_bold_path)}") format("truetype");
      font-weight: 700;
      font-style: normal;
      font-display: swap;
    }}
    :root {{
      --bg: #f7f3ec;
      --bg-alt: #fffaf3;
      --sky-top: #e4f3ff;
      --sky-bottom: #f9f3e6;
      --cloud: rgba(255, 255, 255, 0.76);
      --panel: rgba(255, 252, 247, 0.92);
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
      font-family: "Maplestory", "Apple SD Gothic Neo", "Malgun Gothic", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(105, 184, 232, 0.32), transparent 30%),
        radial-gradient(circle at 78% 8%, rgba(216, 170, 82, 0.16), transparent 22%),
        radial-gradient(circle at 50% 60%, rgba(139, 199, 152, 0.10), transparent 40%),
        linear-gradient(180deg, var(--sky-top) 0%, #f4faff 22%, #faf6ee 60%, var(--sky-bottom) 100%);
      min-height: 100vh;
      overflow-x: hidden;
    }}
    input, button {{ font: inherit; }}
    a {{ color: inherit; text-decoration: none; }}
    .page {{ width: min(1320px, calc(100% - 32px)); margin: 0 auto; padding: 28px 0 56px; position: relative; z-index: 1; }}
    .hero {{ position: relative; overflow: hidden; display:grid; grid-template-columns:1.2fr .8fr; gap:18px; padding: 30px; border: 1px solid var(--line); border-radius: 32px; background: linear-gradient(180deg, rgba(255,252,247,0.98), rgba(250,243,232,0.94)); box-shadow: 0 24px 52px rgba(93,66,40,0.16); }}
    .hero-copy {{ max-width: 100%; }}
    .hero-side {{ display:grid; gap:12px; align-content:start; }}
    .hero-meta-card {{ padding:14px 16px; border-radius:18px; background: rgba(255,255,255,0.74); border:1px solid var(--line); box-shadow:0 8px 20px rgba(78,58,42,0.05); }}
    .hero-meta-card strong {{ display:block; font-size:13px; }}
    .hero-meta-card span {{ display:block; margin-top:6px; font-size:16px; font-weight:800; line-height:1.35; }}
    .hero-meta-card small {{ display:block; margin-top:6px; color:var(--muted); font-size:12px; line-height:1.55; }}
    .mode-tabs {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 18px; }}
    .mode-tab {{ display: inline-flex; align-items: center; min-height: 38px; padding: 8px 14px; border-radius: 999px; background: rgba(255,255,255,0.78); border: 1px solid rgba(110,84,60,0.1); color: var(--muted); font-size: 13px; font-weight: 800; }}
    .mode-tab.active {{ background: rgba(212,125,90,0.16); color: var(--accent-3); border-color: rgba(212,125,90,0.18); }}
    .eyebrow {{ display: inline-flex; align-items: center; gap: 8px; margin: 0 0 12px; padding: 8px 14px; border-radius: 999px; letter-spacing: .08em; text-transform: uppercase; color: var(--accent-3); font-size: 12px; font-weight: 800; background: linear-gradient(180deg, rgba(255,245,220,0.96), rgba(249,231,182,0.9)); border: 1px solid rgba(184,123,44,0.26); box-shadow: inset 0 1px 0 rgba(255,255,255,0.8); }}
    .hero h1 {{ margin: 0; font-family: "Maplestory", "Apple SD Gothic Neo", "Malgun Gothic", sans-serif; font-size: clamp(26px, 4vw, 48px); line-height: 1.1; letter-spacing: -0.02em; word-break: keep-all; }}
    .hero-title-row {{ display: flex; align-items: center; gap: 16px; }}
    .hero-title-mark {{ width: 72px; height: 72px; object-fit: contain; flex-shrink: 0; border-radius: 12px; }}
    .hero p.lead {{ max-width: 58ch; color: var(--muted); font-size: 15px; line-height: 1.72; margin: 14px 0 0; }}
    .tobeol-body {{ margin-top: 32px; }}
    .tobeol-section-head {{ margin: 0 0 4px; font-size: 13px; letter-spacing: .14em; text-transform: uppercase; color: var(--accent-3); font-weight: 700; }}
    .summary-card, .info-panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow); backdrop-filter: blur(10px); }}
    .info-panel {{ padding: 20px 22px; }}
    .info-panel h5 {{ margin: 0 0 10px; font-size: 14px; font-weight: 800; }}
    .analytics-grid {{ display: grid; gap: 14px; }}
    .analytics-grid-2 {{ grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }}
    .analytics-mini-grid {{ display: flex; flex-wrap: wrap; gap: 8px; }}
    .analytics-mini-grid span {{ display: inline-flex; align-items: center; padding: 5px 10px; border-radius: 999px; background: rgba(212,125,90,0.10); color: var(--accent-3); font-size: 12px; font-weight: 700; }}
    .analytics-stat-card p {{ margin: 8px 0 0; color: var(--muted); font-size: 12px; }}
    .simulation-copy {{ margin: 8px 0 0; color: var(--muted); font-size: 12px; line-height: 1.6; }}
    .simulation-copy-muted {{ margin-top: 10px; font-size: 11px; }}
    .auto-summary-grid {{ display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); margin: 0 0 16px; }}
    .auto-summary-card, .history-panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow); backdrop-filter: blur(10px); }}
    .auto-summary-card {{ padding: 18px 20px; }}
    .auto-summary-card-empty {{ background: rgba(255, 252, 247, 0.86); }}
    .summary-label {{ margin: 0 0 8px; color: var(--muted); font-size: 12px; font-weight: 700; letter-spacing: .06em; text-transform: uppercase; }}
    .summary-value {{ display: block; font-size: clamp(18px, 3vw, 28px); line-height: 1.2; }}
    .summary-help {{ margin: 8px 0 0; color: var(--muted); font-size: 12px; line-height: 1.6; }}
    .history-panel {{ padding: 20px 22px; }}
    .history-panel h5 {{ margin: 0 0 12px; font-size: 14px; font-weight: 800; }}
    .analytics-list-split {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }}
    .history-list {{ list-style: none; padding: 0; margin: 10px 0 0; display: grid; gap: 8px; }}
    .history-list li {{ display: flex; justify-content: space-between; gap: 10px; padding: 10px 12px; border-radius: 14px; background: rgba(255,255,255,0.72); border: 1px solid var(--line); font-size: 12px; }}
    .history-list li span {{ min-width: 0; word-break: break-all; }}
    .history-list li strong {{ flex-shrink: 0; color: var(--accent-3); }}
    .trend-chart-grid {{ display: grid; gap: 12px; grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .trend-chart-card {{ padding: 14px; border-radius: 18px; background: rgba(255,255,255,0.74); border: 1px solid var(--line); }}
    .trend-chart-card > span {{ display: block; font-size: 12px; color: var(--muted); margin-bottom: 8px; }}
    .trend-chart {{ height: 42px; color: var(--accent-3); }}
    .trend-chart svg {{ width: 100%; height: 100%; display: block; }}
    .trend-chart-secondary {{ color: var(--accent-2); }}
    .history-empty {{ display: flex; align-items: center; justify-content: center; height: 42px; color: var(--muted); font-size: 12px; }}
    /* Tobeol Ranking */
    .tobeol-ranking-tabs {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 14px 0 10px; }}
    .tobeol-ranking-tab {{ display: inline-flex; align-items: center; justify-content: center; min-height: 34px; padding: 0 14px; border-radius: 999px; border: 1px solid var(--line); background: rgba(255,255,255,0.75); color: var(--text); font-family: inherit; font-size: 12px; font-weight: 700; cursor: pointer; transition: background .15s, color .15s; }}
    .tobeol-ranking-tab.tobeol-ranking-tab-active {{ background: linear-gradient(180deg, rgba(212,125,90,0.16), rgba(212,125,90,0.10)); color: var(--accent-3); border-color: rgba(173,101,64,0.22); }}
    .tobeol-ranking-table-wrap {{ overflow: auto; border-radius: 16px; border: 1px solid var(--line); background: rgba(255,255,255,0.74); margin-top: 10px; }}
    .tobeol-ranking-table {{ width: 100%; border-collapse: collapse; min-width: 520px; }}
    .tobeol-ranking-table th, .tobeol-ranking-table td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--line); font-size: 12px; vertical-align: middle; }}
    .tobeol-ranking-table th {{ color: var(--muted); background: rgba(255,255,255,0.8); }}
    .tobeol-ranking-table tbody tr:last-child td {{ border-bottom: 0; }}
    .tobeol-ranking-table tbody tr[hidden] {{ display: none; }}
    .tobeol-rank-chip {{ display: inline-flex; align-items: center; justify-content: center; min-width: 48px; padding: 6px 10px; border-radius: 999px; background: rgba(212,125,90,0.14); color: var(--accent-3); font-size: 12px; font-weight: 700; }}
    .tobeol-rank-chip-muted {{ background: rgba(122,102,88,0.12); color: var(--muted); }}
    .tobeol-unranked-row td {{ background: rgba(255,252,247,0.82); }}
    .tobeol-guild-cell {{ font-weight: 700; }}
    .tobeol-row-copy {{ color: var(--muted); font-size: 11px; margin-top: 2px; }}
    .footer {{ margin-top: 28px; color: var(--muted); font-size: 13px; text-align: right; }}
    @media (max-width: 980px) {{ .hero {{ grid-template-columns:1fr; }} }}
    @media (max-width: 720px) {{ .hero {{ padding: 20px; border-radius: 28px; }} .hero h1 {{ font-size: clamp(20px, 5.2vw, 28px); white-space: normal; }} .hero-title-mark {{ width: 48px; height: 48px; }} .analytics-grid-2, .trend-chart-grid, .analytics-list-split {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <div class="page">
    <header class="hero">
      <div class="hero-copy">
        <div class="mode-tabs">
          <a class="mode-tab active" href="index.html">길드 리포트</a>
          <a class="mode-tab" href="league.html">대항전 리포트</a>
          <a class="mode-tab" href="training.html">수련장 리포트</a>
        </div>
        <p class="eyebrow">✦ MAPLE GUILD REPORT CONCEPT</p>
        <div class="hero-title-row">
          {hero_guild_mark_html}
          <h1>{escape(guild_seed_name)} 길드 토벌전 랭킹</h1>
        </div>
        <p class="lead">mgf.gg 서버 2 토벌전 랭킹에서 {escape(guild_seed_name)} 멤버를 확인합니다.</p>
      </div>
      <aside class="hero-side">{hero_meta_html}</aside>
    </header>
    <div class="tobeol-body">
      {history_html}
      <p class="tobeol-section-head">서버 2 토벌전 랭킹</p>
      {ranking_html}
    </div>
    <footer class="footer">mgf.gg 서버 2 기준 · 캐시 12시간</footer>
  </div>
  <script>
    document.querySelectorAll('.tobeol-ranking-tab').forEach((btn) => {{
      btn.addEventListener('click', () => {{
        const guild = btn.dataset.tobeolGuild;
        document.querySelectorAll('.tobeol-ranking-tab').forEach((b) => {{
          b.classList.toggle('tobeol-ranking-tab-active', b === btn);
        }});
        document.querySelectorAll('#tobeol-ranking-tbody tr[data-tobeol-guild]').forEach((tr) => {{
          tr.hidden = guild !== '\uc804\uccb4' && tr.dataset.tobeolGuild !== guild;
        }});
      }});
    }});
  </script>
</body>
</html>"""
    html_output_path.parent.mkdir(parents=True, exist_ok=True)
    html_output_path.write_text(html, encoding="utf-8")
    return html_output_path


def build_html_report(
    guild_seed_name: str,
    report_mode: str,
    guild_rows: list[dict[str, Any]],
    members_by_guild: dict[str, list[dict[str, Any]]],
    history_analysis: dict[str, Any],
    html_output_path: Path,
) -> Path:
    # #1 fix: build nav_links outside the f-string to avoid double-brace escaping
    mode_tabs = [
        (
            "tobeol",
            "길드 리포트",
            "index.html",
        ),
        (
            "league",
            "대항전 리포트",
            "league.html",
        ),
        (
            "training",
            "수련장 리포트",
            "training.html",
        ),
    ]
    mode_tabs_html = "".join(
        f'<a class="mode-tab {"active" if mode == report_mode else ""}" href="{href}">{escape(label)}</a>'
        for mode, label, href in mode_tabs
    )
    nav_links = "".join(
        '<a data-modal="' + anchor_id(str(row["guild_name"])) + '" href="#">' + escape(str(row["guild_name"])) + "</a>"
        for row in guild_rows
    )
    report_label = REPORT_MODE_LABELS[report_mode]
    copy = get_report_copy(report_mode)
    guild_names = [str(row["guild_name"]) for row in guild_rows]
    guild_mark_map = build_guild_mark_map(guild_names, html_output_path)
    font_face_map = build_font_face_map(html_output_path)
    font_light_path = font_face_map.get("light", "")
    font_bold_path = font_face_map.get("bold", "")
    hero_guild_mark_html = render_guild_mark(guild_seed_name, guild_mark_map, "hero-title-mark")
    summary_cards_html = render_summary_cards(guild_rows, members_by_guild, history_analysis, report_mode)
    secondary_summary_cards_html = render_secondary_summary_cards(guild_rows, members_by_guild, history_analysis, report_mode)
    auto_summary_html = render_auto_summary_section(history_analysis)
    snapshot_overview_html = render_snapshot_overview_section(history_analysis)
    compare_cards_html = render_compare_cards(guild_rows, members_by_guild, history_analysis, guild_mark_map)
    hero_meta_html = render_report_hero_meta(guild_seed_name, report_mode, guild_rows, history_analysis)
    if report_mode == "league":
        score_table = parse_score_table(SCORE_TABLE_PATH)
        simulation = build_guild_war_simulation(members_by_guild, score_table)
        simulation_modal_html = render_guild_war_simulation_modal(simulation, history_analysis)
    else:
        simulation = build_training_simulation(members_by_guild)
        simulation_modal_html = render_training_simulation_modal(simulation, history_analysis)
    detail_comparison_html = render_detail_comparison_section(guild_rows, members_by_guild, guild_mark_map)
    guild_modals_html = render_guild_modals(guild_rows, members_by_guild, history_analysis, guild_mark_map)
    snapshot_analytics_modal_html = render_snapshot_analytics_modal(history_analysis, report_mode)
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(guild_seed_name)} {escape(report_label)} 리포트</title>
  <style>
    @font-face {{
      font-family: "Maplestory";
      src: url("{escape(font_light_path)}") format("truetype");
      font-weight: 400;
      font-style: normal;
      font-display: swap;
    }}
    @font-face {{
      font-family: "Maplestory";
      src: url("{escape(font_bold_path)}") format("truetype");
      font-weight: 700;
      font-style: normal;
      font-display: swap;
    }}
    :root {{
      --bg: #f7f3ec;
      --bg-alt: #fffaf3;
      --sky-top: #e4f3ff;
      --sky-bottom: #f9f3e6;
      --cloud: rgba(255, 255, 255, 0.76);
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
      font-family: "Maplestory", "Apple SD Gothic Neo", "Malgun Gothic", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(105, 184, 232, 0.32), transparent 30%),
        radial-gradient(circle at 78% 8%, rgba(216, 170, 82, 0.16), transparent 22%),
        radial-gradient(circle at 50% 60%, rgba(139, 199, 152, 0.10), transparent 40%),
        linear-gradient(180deg, var(--sky-top) 0%, #f4faff 22%, #faf6ee 60%, var(--sky-bottom) 100%);
      min-height: 100vh;
      position: relative;
      overflow-x: hidden;
    }}
    body::before,
    body::after {{
      content: "";
      position: fixed;
      inset: auto;
      pointer-events: none;
      z-index: 0;
      filter: blur(5px);
    }}
    body::before {{
      top: 64px;
      left: -30px;
      width: 280px;
      height: 96px;
      border-radius: 999px;
      background: var(--cloud);
      box-shadow: 130px 20px 0 24px rgba(255, 255, 255, 0.58), 300px -6px 0 14px rgba(255, 255, 255, 0.44);
    }}
    body::after {{
      right: -24px;
      top: 200px;
      width: 240px;
      height: 82px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.58);
      box-shadow: -120px 22px 0 20px rgba(255, 255, 255, 0.50), -260px 4px 0 10px rgba(255, 255, 255, 0.36);
    }}
    input, button, select, textarea {{ font: inherit; }}
    a {{ color: inherit; text-decoration: none; }}
    .page {{ width: min(1320px, calc(100% - 32px)); margin: 0 auto; padding: 28px 0 56px; position: relative; z-index: 1; }}
    .hero {{
      position: relative;
      overflow: hidden;
      display: grid;
      grid-template-columns: 1.2fr .8fr;
      gap: 18px;
      padding: 30px;
      border: 1px solid var(--line);
      border-radius: 32px;
      background: linear-gradient(180deg, rgba(255,252,247,0.98), rgba(250,243,232,0.94));
      box-shadow: 0 24px 52px rgba(93, 66, 40, 0.16);
    }}
    .hero::after {{
      content: "";
      position: absolute;
      right: -48px;
      top: -48px;
      width: 180px;
      height: 180px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(216, 170, 82, 0.18), transparent 68%);
      filter: blur(12px);
    }}
    .hero::before {{
      content: "";
      position: absolute;
      inset: 0;
      background: radial-gradient(circle at top left, rgba(105, 184, 232, 0.12), transparent 24%), radial-gradient(circle at bottom right, rgba(139, 199, 152, 0.10), transparent 18%);
      pointer-events: none;
    }}
    .hero-copy {{ max-width: 100%; }}
    .hero-main {{ display: grid; gap: 16px; }}
    .hero-side {{ display: grid; gap: 12px; align-content: start; }}
    .hero-meta-card {{ padding: 14px 16px; border-radius: 18px; background: rgba(255,255,255,0.74); border: 1px solid var(--line); box-shadow: 0 8px 20px rgba(78,58,42,0.05); }}
    .hero-meta-card strong {{ display: block; font-size: 13px; }}
    .hero-meta-card span {{ display: block; margin-top: 6px; font-size: 16px; font-weight: 800; line-height: 1.35; }}
    .hero-meta-card small {{ display: block; margin-top: 6px; color: var(--muted); font-size: 12px; line-height: 1.55; }}
    .mode-tabs {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 18px; }}
    .mode-tab {{ display: inline-flex; align-items: center; min-height: 38px; padding: 8px 14px; border-radius: 999px; background: rgba(255,255,255,0.78); border: 1px solid rgba(110,84,60,0.1); color: var(--muted); font-size: 13px; font-weight: 800; }}
    .mode-tab.active {{ background: rgba(212,125,90,0.16); color: var(--accent-3); border-color: rgba(212,125,90,0.18); }}
    .eyebrow {{ display: inline-flex; align-items: center; gap: 8px; margin: 0 0 12px; padding: 8px 14px; border-radius: 999px; letter-spacing: .08em; text-transform: uppercase; color: var(--accent-3); font-size: 12px; font-weight: 800; background: linear-gradient(180deg, rgba(255,245,220,0.96), rgba(249,231,182,0.9)); border: 1px solid rgba(184,123,44,0.26); box-shadow: inset 0 1px 0 rgba(255,255,255,0.8); }}
    .hero h1 {{ margin: 0; font-family: "Maplestory", "Apple SD Gothic Neo", "Malgun Gothic", sans-serif; font-size: clamp(28px, 4.2vw, 52px); line-height: 1.08; letter-spacing: -0.02em; max-width: none; word-break: keep-all; }}
    .hero-title-row {{ display: flex; align-items: center; gap: 16px; }}
    .hero-title-mark {{ width: 72px; height: 72px; object-fit: contain; flex-shrink: 0; border-radius: 12px; }}
    .hero p.lead {{ max-width: 58ch; color: var(--muted); font-size: 15px; line-height: 1.72; margin: 14px 0 0; }}
    .hero-nav {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 8px; grid-column: 1 / -1; }}
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
    .section-tabs {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 6px 0 0; }}
    .section-tabs a {{ display: inline-flex; align-items: center; min-height: 40px; padding: 10px 16px; border-radius: 999px; background: rgba(255,255,255,0.78); border: 1px solid rgba(110,84,60,0.1); color: var(--text); font-size: 13px; font-weight: 800; box-shadow: 0 8px 16px rgba(78,58,42,0.05); transition: transform .18s ease, border-color .18s ease, background .18s ease; }}
    .section-tabs a:hover {{ transform: translateY(-1px); background: rgba(212,125,90,0.10); border-color: rgba(212,125,90,0.18); }}
    .summary-grid {{ display: grid; gap: 16px; margin-top: 18px; grid-template-columns: repeat(4, minmax(0, 1fr)); grid-column: 1 / -1; }}
    .summary-grid-secondary {{ margin-top: 14px; grid-template-columns: repeat(3, minmax(0, 1fr)); }}
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
    .summary-card {{ position: relative; overflow: hidden; padding: 22px; min-height: 152px; background: rgba(255, 251, 244, 0.84); }}
    .summary-card-primary {{ grid-column: span 2; background: linear-gradient(135deg, rgba(255,246,239,0.98), rgba(255,251,245,0.96)); border-color: rgba(212,125,90,0.20); }}
    .summary-card::before {{ content: ""; position: absolute; inset: 0; background: linear-gradient(180deg, rgba(255,255,255,0.36), rgba(255,255,255,0)); pointer-events: none; }}
    .summary-label {{ margin: 0; font-size: 13px; color: var(--muted); }}
    .summary-value {{ display: block; margin-top: 14px; font-size: 28px; line-height: 1.15; }}
    .summary-help {{ margin: 12px 0 0; color: var(--muted); font-size: 13px; line-height: 1.5; }}
    .section-title {{ margin: 44px 0 8px; font-size: 14px; letter-spacing: .16em; text-transform: uppercase; color: var(--accent-3); font-weight: 700; }}
    .section-copy {{ margin: 0 0 16px; color: var(--muted); font-size: 13px; line-height: 1.65; }}
    .summary-extra {{ grid-column: 1 / -1; padding: 18px 20px; border-radius: 22px; background: rgba(255,255,255,0.72); border: 1px solid var(--line); box-shadow: 0 10px 24px rgba(78,58,42,0.05); }}
    .summary-extra-toggle {{ width: 100%; display:flex; justify-content:space-between; gap:12px; align-items:center; border:0; background:transparent; color:inherit; text-align:left; cursor:pointer; padding:0; }}
    .summary-extra-toggle strong {{ display:block; font-size:16px; }}
    .summary-extra-toggle span {{ display:inline-flex; align-items:center; gap:6px; color:var(--accent-3); font-size:12px; font-weight:800; }}
    .summary-extra-toggle span::after {{ content:"▾"; font-size:12px; transition:transform .18s ease; }}
    .summary-extra.open .summary-extra-toggle span::after {{ transform: rotate(180deg); }}
    .summary-extra-body {{ margin-top: 14px; padding-top: 14px; border-top: 1px solid rgba(110,84,60,0.08); }}
    .mobile-section-toggle {{ display: none; width: 100%; margin: 18px 0 0; padding: 16px 18px; border: 1px solid var(--line); border-radius: 20px; background: rgba(255,255,255,0.76); color: var(--text); text-align: left; box-shadow: var(--shadow); }}
    .mobile-section-toggle strong {{ display: block; font-size: 16px; }}
    .mobile-section-toggle span {{ display: inline-flex; align-items: center; gap: 6px; margin-top: 8px; color: var(--accent-3); font-size: 12px; font-weight: 800; }}
    .mobile-section-toggle span::after {{ content: "▾"; font-size: 12px; transition: transform .18s ease; }}
    .mobile-section-panel.expanded .mobile-section-toggle span::after {{ transform: rotate(180deg); }}
    .mobile-section-body {{ display: block; }}
    .hero-stats-body {{ display: grid; gap: 18px; grid-column: 1 / -1; }}
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
    .guild-card-title-row {{ display: flex; align-items: center; gap: 12px; min-width: 0; }}
    .guild-card-mark {{ width: 42px; height: 42px; object-fit: contain; flex-shrink: 0; }}
    .guild-card h3 {{ margin: 6px 0 0; font-size: 28px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .rank-pill {{ padding: 8px 12px; border-radius: 999px; background: rgba(212,125,90,0.12); color: var(--accent-3); font-size: 12px; white-space: nowrap; font-weight: 700; }}
    .rank-badge-row {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }}
    .rank-badge {{ display: inline-flex; align-items: center; min-height: 28px; padding: 5px 10px; border-radius: 999px; font-size: 12px; font-weight: 800; border: 1px solid rgba(110,84,60,0.08); }}
    .rank-badge-global {{ background: rgba(212,125,90,0.14); color: var(--accent-3); }}
    .rank-badge-server {{ background: rgba(136,177,124,0.14); color: #55734f; }}
    .trend-pill-row {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }}
    .trend-pill {{ display: inline-flex; align-items: center; min-height: 28px; padding: 5px 10px; border-radius: 999px; font-size: 12px; font-weight: 700; border: 1px solid rgba(110,84,60,0.08); }}
    .trend-pill.positive, .trend-pill.tone-up {{ background: rgba(136,177,124,0.14); color: #55734f; }}
    .trend-pill.negative, .trend-pill.tone-down {{ background: rgba(212,125,90,0.14); color: var(--accent-3); }}
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
    .detail-compare-title-row h3 {{ margin-top: 0; }}
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
    .simulation-overview {{ display: grid; grid-template-columns: 1.15fr .85fr; gap: 18px; align-items: start; padding: 20px; border-radius: 26px; background: linear-gradient(135deg, rgba(255,252,247,0.98), rgba(248,239,225,0.92)); border: 1px solid rgba(110,84,60,0.08); box-shadow: 0 18px 36px rgba(78,58,42,0.08); }}
    .simulation-overview h3 {{ margin: 6px 0 0; font-size: 30px; }}
    .simulation-copy {{ margin: 14px 0 0; color: var(--muted); line-height: 1.7; }}
    .simulation-copy-muted {{ margin-top: 10px; font-size: 11px; }}
    .score-rule-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; }}
    .job-coefficient-section {{ margin-top: 18px; padding: 18px; border-radius: 24px; background: rgba(255,255,255,0.64); border: 1px solid rgba(110,84,60,0.08); box-shadow: 0 14px 28px rgba(78,58,42,0.06); }}
    .job-coefficient-toggle, .simulation-rank-toggle {{ width: 100%; border: 0; background: transparent; color: inherit; text-align: left; cursor: pointer; padding: 0; }}
    .job-coefficient-head h3 {{ margin: 6px 0 0; font-size: 24px; }}
    .job-coefficient-head {{ display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; }}
    .job-coefficient-summary {{ display: inline-flex; align-items: center; min-height: 32px; padding: 0 12px; border-radius: 999px; background: rgba(212,125,90,0.14); color: var(--accent-3); font-size: 12px; font-weight: 800; white-space: nowrap; }}
    .job-coefficient-details {{ margin-top: 14px; padding-top: 14px; border-top: 1px solid rgba(110,84,60,0.08); }}
    .coeff-tier-tabs {{ display: flex; gap: 8px; margin-bottom: 14px; }}
    .coeff-tier-tab {{ padding: 6px 16px; border-radius: 999px; border: 1px solid rgba(110,84,60,0.18); background: rgba(255,255,255,0.6); color: var(--muted); font-size: 13px; font-weight: 700; cursor: pointer; transition: background 0.15s, color 0.15s; }}
    .coeff-tier-tab.active {{ background: linear-gradient(180deg, rgba(255,245,220,0.98), rgba(249,231,182,0.92)); color: var(--accent-3); border-color: rgba(184,123,44,0.3); box-shadow: inset 0 1px 0 rgba(255,255,255,0.8); }}
    .job-coefficient-grid {{ display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 10px; margin-top: 0; }}
    .score-rule-card {{ padding: 14px; border-radius: 18px; background: rgba(255,255,255,0.72); border: 1px solid rgba(110,84,60,0.08); box-shadow: 0 8px 18px rgba(78,58,42,0.04); }}
    .score-rule-card span {{ display: block; color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    .score-rule-card strong {{ display: block; font-size: 15px; }}
    .score-rule-card em {{ display: block; margin-top: 6px; color: var(--muted); font-size: 12px; font-style: normal; }}
    .snapshot-overview-grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; margin-top: 18px; }}
    .snapshot-overview-card {{ cursor: pointer; position: relative; }}
    .snapshot-overview-card .card-jump {{ margin-top: 12px; display: inline-flex; color: var(--accent-3); font-size: 12px; font-weight: 700; }}
    .snapshot-analytics-modal-box {{ width: min(1180px, calc(100% - 24px)); }}
    .analytics-summary-grid {{ margin-top: 18px; }}
    .analytics-summary-card {{ background: rgba(255,255,255,0.78); }}
    .analytics-warning {{ margin-top: 14px; padding: 12px 16px; border-radius: 18px; background: rgba(212,125,90,0.10); border: 1px solid rgba(212,125,90,0.18); color: var(--accent-3); font-size: 13px; font-weight: 700; }}
    .analytics-chapter-stack {{ display: grid; gap: 14px; margin-top: 18px; }}
    .analytics-chapter {{ padding: 18px; border-radius: 24px; background: rgba(255,255,255,0.66); border: 1px solid rgba(110,84,60,0.08); box-shadow: 0 14px 28px rgba(78,58,42,0.06); }}
    .analytics-chapter-toggle, .analytics-module-toggle {{ width: 100%; border: 0; background: transparent; color: inherit; text-align: left; cursor: pointer; padding: 0; }}
    .analytics-chapter-body {{ margin-top: 14px; padding-top: 14px; border-top: 1px solid rgba(110,84,60,0.08); }}
    .analytics-module {{ padding: 16px; border-radius: 20px; background: rgba(255,251,246,0.78); border: 1px solid rgba(110,84,60,0.08); }}
    .analytics-module + .analytics-module {{ margin-top: 12px; }}
    .analytics-module-toggle {{ display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }}
    .analytics-module-toggle-meta {{ display: flex; flex-direction: column; align-items: flex-end; gap: 6px; flex-shrink: 0; }}
    .analytics-module-toggle h4, .analytics-chapter-toggle h3 {{ margin: 6px 0 0; }}
    .analytics-module-body {{ margin-top: 14px; padding-top: 14px; border-top: 1px solid rgba(110,84,60,0.08); }}
    .analytics-grid {{ display: grid; gap: 12px; }}
    .analytics-grid-2 {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .analytics-mini-grid {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 10px 0 12px; color: var(--muted); font-size: 12px; }}
    .analytics-list-card h5, .analytics-stat-card h5 {{ margin: 0 0 8px; font-size: 18px; }}
    .analytics-list-split {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }}
    .analytics-bar-list {{ display: grid; gap: 10px; }}
    .analytics-bar-item strong {{ display: block; margin-bottom: 6px; }}
    .analytics-bar-item span {{ display: block; color: var(--muted); font-size: 12px; margin-bottom: 4px; }}
    .analytics-person-list {{ display: grid; gap: 10px; }}
    .analytics-person-item {{ display: grid; grid-template-columns: minmax(0, 1fr) 180px; gap: 14px; align-items: center; padding: 12px; border-radius: 16px; background: rgba(255,255,255,0.68); border: 1px solid rgba(110,84,60,0.08); }}
    .analytics-person-item p {{ margin: 4px 0 0; color: var(--muted); font-size: 12px; }}
    .analytics-timeline {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; }}
    .analytics-timeline-card strong {{ font-size: 16px; }}
    .simulation-rank-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .simulation-rank-card {{ padding: 18px; border-radius: 24px; background: rgba(255,255,255,0.78); border: 1px solid rgba(110,84,60,0.08); box-shadow: 0 16px 34px rgba(78,58,42,0.08); }}
    .simulation-rank-top {{ display: flex; align-items: center; gap: 10px; }}
    .simulation-rank-top strong {{ font-size: 22px; }}
    .simulation-rank-badge {{ display: inline-flex; align-items: center; justify-content: center; min-width: 44px; height: 32px; padding: 0 10px; border-radius: 999px; background: rgba(212,125,90,0.15); color: var(--accent-3); font-size: 13px; font-weight: 800; }}
    .simulation-rank-card.rank-1 .simulation-rank-badge {{ background: rgba(212,125,90,0.24); }}
    .simulation-rank-score {{ margin-top: 14px; font-size: 30px; font-weight: 800; line-height: 1.1; }}
    .simulation-rank-summary {{ margin-top: 10px; color: var(--muted); font-size: 13px; font-weight: 700; }}
    .simulation-rank-meta {{ display: grid; gap: 10px; margin: 14px 0 0; }}
    .simulation-rank-meta div {{ padding-top: 10px; border-top: 1px solid rgba(110,84,60,0.08); }}
    .simulation-rank-meta dt {{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    .simulation-rank-meta dd {{ margin: 0; font-size: 14px; font-weight: 700; }}
    .simulation-rank-details {{ margin-top: 14px; padding-top: 14px; border-top: 1px solid rgba(110,84,60,0.08); }}
    .simulation-table-wrap {{ margin-top: 18px; }}
    .toolbar-actions {{ display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }}
    .table-filter-label {{ display: inline-flex; align-items: center; gap: 8px; padding: 8px 12px; border-radius: 999px; background: rgba(255,255,255,0.72); border: 1px solid rgba(110,84,60,0.1); }}
    .table-filter-label span {{ color: var(--muted); font-size: 12px; font-weight: 700; white-space: nowrap; }}
    .table-filter-label select {{ border: 0; background: transparent; color: var(--text); font-size: 13px; font-weight: 700; outline: none; min-width: 110px; }}
    .simulation-table .job-name {{ font-weight: 800; }}
    .simulation-mobile-card-list {{ display: none; gap: 12px; padding: 16px; }}
    .simulation-member-card {{ border-radius: 20px; background: rgba(255,255,255,0.78); border: 1px solid rgba(110,84,60,0.08); box-shadow: 0 12px 30px rgba(78,58,42,0.08); overflow: hidden; }}
    .simulation-member-toggle {{ width: 100%; padding: 16px; border: 0; background: transparent; color: inherit; text-align: left; cursor: pointer; }}
    .simulation-member-card-top {{ display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; }}
    .simulation-member-card-identity {{ display: flex; gap: 12px; align-items: flex-start; min-width: 0; }}
    .simulation-member-rank {{ display: inline-flex; align-items: center; justify-content: center; min-width: 44px; height: 32px; padding: 0 10px; border-radius: 999px; background: rgba(212,125,90,0.14); color: var(--accent-3); font-size: 13px; font-weight: 800; flex-shrink: 0; }}
    .simulation-member-primary {{ min-width: 0; overflow: hidden; }}
    .simulation-member-card-identity strong {{ display: block; font-size: 16px; line-height: 1.35; }}
    .simulation-rank-change-badge {{ display: inline-flex; align-items: center; justify-content: center; min-height: 26px; padding: 0 10px; border-radius: 999px; border: 1px solid rgba(110,84,60,0.08); background: rgba(255,255,255,0.82); color: var(--muted); font-size: 11px; font-weight: 800; white-space: nowrap; }}
    .simulation-member-primary .simulation-rank-change-badge {{ margin-top: 8px; }}
    .simulation-rank-change-badge.tone-up {{ background: rgba(136,177,124,0.16); border-color: rgba(136,177,124,0.24); color: #56764b; }}
    .simulation-rank-change-badge.tone-down {{ background: rgba(212,125,90,0.14); border-color: rgba(212,125,90,0.22); color: var(--accent-3); }}
    .simulation-rank-change-badge.tone-new {{ background: rgba(122,152,221,0.14); border-color: rgba(122,152,221,0.24); color: #4b6694; }}
    .simulation-rank-change-badge.tone-same, .simulation-rank-change-badge.tone-none {{ background: rgba(120,111,102,0.10); border-color: rgba(120,111,102,0.12); color: var(--muted); }}
    .simulation-member-card-identity p {{ margin: 4px 0 0; color: var(--muted); font-size: 12px; line-height: 1.45; }}
    .simulation-member-power {{ display: block; margin-top: 6px; color: var(--text); font-size: 12px; font-weight: 700; line-height: 1.45; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .simulation-member-score {{ text-align: right; flex-shrink: 0; padding: 10px 12px; border-radius: 16px; background: linear-gradient(160deg, rgba(255,246,239,0.96), rgba(248,235,224,0.92)); border: 1px solid rgba(212,125,90,0.18); box-shadow: 0 8px 18px rgba(173,101,64,0.10); }}
    .simulation-member-score span {{ display: block; color: var(--accent-3); font-size: 11px; margin-bottom: 4px; font-weight: 700; }}
    .simulation-member-score strong {{ display: block; color: var(--accent-3); font-size: 18px; line-height: 1.25; }}
    .simulation-member-toggle-label {{ display: inline-flex; align-items: center; gap: 6px; margin-top: 10px; color: var(--accent-3); font-size: 12px; font-weight: 800; }}
    .simulation-member-toggle-label::after {{ content: "▾"; display: inline-block; font-size: 12px; transition: transform .18s ease; }}
    .simulation-member-card.expanded .simulation-member-toggle-label::after {{ transform: rotate(180deg); }}
    .simulation-section-toggle-label {{ display: inline-flex; align-items: center; gap: 6px; margin-top: 12px; color: var(--accent-3); font-size: 12px; font-weight: 800; }}
    .simulation-section-toggle-label::after {{ content: "▾"; display: inline-block; font-size: 12px; transition: transform .18s ease; }}
    .job-coefficient-section.expanded .simulation-section-toggle-label::after, .simulation-rank-card.expanded .simulation-section-toggle-label::after, .job-coefficient-toggle[aria-expanded="true"] .simulation-section-toggle-label::after, .simulation-rank-toggle[aria-expanded="true"] .simulation-section-toggle-label::after, .analytics-module-toggle[aria-expanded="true"] .simulation-section-toggle-label::after, .analytics-chapter-toggle[aria-expanded="true"] .simulation-section-toggle-label::after {{ transform: rotate(180deg); }}
    .simulation-member-details {{ padding: 0 16px 16px; border-top: 1px solid rgba(110,84,60,0.08); background: rgba(255,255,255,0.38); }}
    .simulation-member-meta {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin: 14px 0 0; }}
    .simulation-member-meta div {{ padding: 12px; border-radius: 14px; background: rgba(255,255,255,0.62); border: 1px solid rgba(110,84,60,0.06); min-width: 0; }}
    .simulation-member-meta dt {{ color: var(--muted); font-size: 11px; margin-bottom: 6px; }}
    .simulation-member-meta dd {{ margin: 0; font-size: 13px; font-weight: 700; line-height: 1.45; word-break: break-word; }}
    .simulation-member-meta a {{ color: var(--accent-3); }}
    .simulation-table td:nth-child(1), .simulation-table th:nth-child(1) {{ white-space: nowrap; }}
    .simulation-table .simulation-score-cell, .simulation-table .simulation-rank-change-cell, .simulation-table td:nth-child(5), .simulation-table td:nth-child(6), .simulation-table td:nth-child(7) {{ font-variant-numeric: tabular-nums; font-family: "Apple SD Gothic Neo", "Malgun Gothic", "Segoe UI", sans-serif; }}
    .simulation-table .simulation-score-cell {{ color: var(--accent-3); font-weight: 800; }}
    .simulation-table .simulation-rank-change-cell {{ white-space: nowrap; }}
    /* Tobeol Ranking */
    .tobeol-ranking-tabs {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 14px 0 10px; }}
    .tobeol-ranking-tab {{ display: inline-flex; align-items: center; justify-content: center; min-height: 34px; padding: 0 14px; border-radius: 999px; border: 1px solid var(--line); background: rgba(255,255,255,0.75); color: var(--text); font-family: inherit; font-size: 12px; font-weight: 700; cursor: pointer; transition: background .15s, color .15s; }}
    .tobeol-ranking-tab.tobeol-ranking-tab-active {{ background: linear-gradient(180deg, rgba(212,125,90,0.16), rgba(212,125,90,0.10)); color: var(--accent-3); border-color: rgba(173,101,64,0.22); }}
    .tobeol-ranking-table-wrap {{ overflow: auto; border-radius: 16px; border: 1px solid var(--line); background: rgba(255,255,255,0.74); margin-top: 10px; }}
    .tobeol-ranking-table {{ width: 100%; border-collapse: collapse; min-width: 520px; }}
    .tobeol-ranking-table th, .tobeol-ranking-table td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--line); font-size: 12px; vertical-align: middle; }}
    .tobeol-ranking-table th {{ color: var(--muted); background: rgba(255,255,255,0.8); }}
    .tobeol-ranking-table tbody tr:last-child td {{ border-bottom: 0; }}
    .tobeol-ranking-table tbody tr[hidden] {{ display: none; }}
    .tobeol-rank-chip {{ display: inline-flex; align-items: center; justify-content: center; min-width: 48px; padding: 6px 10px; border-radius: 999px; background: rgba(212,125,90,0.14); color: var(--accent-3); font-size: 12px; font-weight: 700; }}
    .tobeol-guild-cell {{ font-weight: 700; }}
    .tobeol-row-copy {{ color: var(--muted); font-size: 11px; margin-top: 2px; }}
    /* #4: Modal system */
    .modal-backdrop {{
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(34, 23, 14, 0.56);
      backdrop-filter: blur(6px);
      z-index: 200;
      align-items: center;
      justify-content: center;
      padding: 20px;
    }}
    .modal-backdrop.open {{ display: flex; }}
    .modal-box {{
      background: linear-gradient(180deg, rgba(255,252,246,0.99), rgba(250,242,230,0.97));
      border: 1px solid var(--line);
      border-radius: 32px;
      box-shadow: 0 34px 80px rgba(60, 42, 28, 0.32);
      width: min(900px, 100%);
      max-height: 90vh;
      overflow-y: auto;
      padding: 30px;
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
    .modal-title-row {{ display: flex; align-items: center; gap: 14px; }}
    .modal-guild-mark {{ width: 56px; height: 56px; object-fit: contain; flex-shrink: 0; }}
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
    .table-wrap {{ margin-top: 18px; border-radius: 26px; border: 1px solid var(--line); overflow: hidden; background: rgba(255, 252, 247, 0.84); box-shadow: 0 18px 36px rgba(78,58,42,0.08); }}
    .table-toolbar {{ display: flex; justify-content: space-between; gap: 12px; align-items: center; padding: 18px 20px; border-bottom: 1px solid rgba(110,84,60,0.08); }}
    .table-toolbar h3 {{ margin: 0; font-size: 18px; }}
    .toolbar-actions {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: center; }}
    .member-search {{ min-width: 240px; border: 1px solid rgba(110,84,60,0.12); background: rgba(255,255,255,0.75); color: var(--text); border-radius: 999px; padding: 11px 14px; outline: none; }}
    .member-search::placeholder {{ color: #9d8b7d; }}
    .hint {{ color: var(--muted); font-size: 12px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    .member-table th, .member-table td {{ padding: 16px 18px; text-align: left; border-bottom: 1px solid rgba(110,84,60,0.07); }}
    .member-table th {{ position: sticky; top: 0; background: rgba(250,244,237,0.98); color: var(--muted); font-size: 12px; letter-spacing: .08em; text-transform: uppercase; cursor: pointer; font-weight: 700; }}
    .member-table tr:hover td {{ background: rgba(255,255,255,0.35); }}
    .member-name-cell {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }}
    .member-name-cell a {{ color: var(--text); font-weight: 700; }}
    .badge {{ display: inline-flex; align-items: center; min-height: 28px; padding: 4px 10px; border-radius: 999px; background: rgba(136,177,124,0.14); color: #55734f; font-size: 12px; border: 1px solid rgba(136,177,124,0.16); font-weight: 700; }}
    .badge-master {{ background: rgba(212, 125, 90, 0.15); color: var(--accent-3); border-color: rgba(212, 125, 90, 0.18); }}
    .power-col {{ font-variant-numeric: tabular-nums; color: var(--accent-3); font-weight: 700; }}
    .footer {{ margin-top: 28px; color: var(--muted); font-size: 13px; text-align: right; }}
    @media (max-width: 980px) {{ .section-grid, .simulation-overview, .modal-comparison-grid, .modal-history-grid, .snapshot-overview-grid, .analytics-grid-2, .analytics-list-split {{ grid-template-columns: 1fr; }} .job-coefficient-grid {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }} .section-head, .table-toolbar, .analytics-module-toggle {{ flex-direction: column; align-items: start; }} .analytics-person-item {{ grid-template-columns: 1fr; }} }}
    @media (max-width: 980px) {{ .hero {{ grid-template-columns: 1fr; }} .summary-grid, .summary-grid-secondary {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} .summary-card-primary {{ grid-column: 1 / -1; }} }}
    @media (max-width: 720px) {{ .page {{ width: min(100% - 20px, 1320px); }} .hero {{ padding: 20px; border-radius: 28px; }} .guild-metrics {{ grid-template-columns: 1fr; }} .job-coefficient-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} .member-table th, .member-table td {{ padding: 12px; font-size: 13px; }} .member-search {{ min-width: 0; width: 100%; }} .guild-card {{ flex: 0 0 260px; }} .detail-compare-card {{ flex: 0 0 250px; }} .hero h1 {{ font-size: clamp(20px, 5.2vw, 30px); white-space: normal; }} .hero-title-mark {{ width: 48px; height: 48px; }} .simulation-modal-box, .modal-box {{ padding: 20px; border-radius: 26px; }} .training-simulation-table, .guild-war-simulation-table {{ display: none; }} .simulation-mobile-card-list {{ display: grid; }} .simulation-member-score {{ padding: 9px 11px; }} .simulation-member-score strong {{ font-size: 16px; }} .simulation-member-meta {{ grid-template-columns: 1fr; }} .simulation-rank-grid {{ grid-template-columns: 1fr; }} .simulation-rank-card {{ padding: 16px; }} .simulation-rank-score {{ font-size: 24px; }} .job-coefficient-head {{ flex-direction: column; align-items: flex-start; }} .section-title {{ display: none; }} .mobile-section-toggle {{ display: block; }} .mobile-section-panel:not(.expanded) .mobile-section-body {{ display: none; }} .summary-grid, .summary-grid-secondary {{ grid-template-columns: 1fr; }} .guild-card-title-row {{ gap: 10px; }} .guild-card-mark {{ width: 34px; height: 34px; }} .modal-title-row {{ gap: 10px; }} .modal-guild-mark {{ width: 46px; height: 46px; }} .section-tabs a {{ width: 100%; justify-content: center; }} .analytics-timeline {{ grid-template-columns: 1fr; }} .analytics-module {{ padding: 14px; }} }}
  </style>
</head>
<body>
  <div class="page">
    <header class="hero">
      <div class="hero-main">
        <div class="hero-copy">
          <div class="mode-tabs">{mode_tabs_html}</div>
          <p class="eyebrow">✦ MAPLE GUILD REPORT CONCEPT</p>
          <div class="hero-title-row">
            {hero_guild_mark_html}
            <h1>{escape(guild_seed_name)} {escape(report_label)} 리포트</h1>
          </div>
          <p class="lead">{escape(copy['lead'])}</p>
          <div class="section-tabs">
             <a data-modal="guild-war-simulation" href="#">{escape(copy['simulation_nav'])}</a>
             <a data-modal="snapshot-analytics" href="#">스냅샷 분석</a>
          </div>
        </div>
      </div>
      <aside class="hero-side">{hero_meta_html}</aside>
      <nav class="hero-nav">{nav_links}</nav>
      <div class="hero-stats-body" id="hero-stats-body">
        <section class="summary-grid">{summary_cards_html}</section>
        <section class="summary-extra" id="summary-extra-panel">
          <button type="button" class="summary-extra-toggle" aria-expanded="false">
            <strong>추가 요약 보기</strong>
            <span>펼치기</span>
          </button>
          <div class="summary-extra-body" hidden>
            {secondary_summary_cards_html}
            {auto_summary_html}
            {snapshot_overview_html}
          </div>
        </section>
      </div>
    </header>

    <section class="mobile-section-panel" id="guild-comparison-section">
      <h2 class="section-title" id="guild-comparison">핵심 비교 레일</h2>
      <p class="section-copy">상단 카드에서 흐름을 읽고, 여기서 길드별 차이를 바로 스캔합니다.</p>
      <button type="button" class="mobile-section-toggle" data-target="guild-comparison-body" aria-expanded="false">
        <strong>핵심 비교 레일</strong>
        <span>상세 보기</span>
      </button>
      <div class="mobile-section-body" id="guild-comparison-body">
        <div class="compare-scroll-wrap">{compare_cards_html}</div>
      </div>
    </section>

    <section class="mobile-section-panel" id="guild-detail-comparison-section">
      <h2 class="section-title" id="guild-detail-comparison">길드 상세 비교</h2>
      <p class="section-copy">랭킹/요약 다음 단계에서 각 길드의 멤버 구성을 표처럼 읽기 편하게 비교합니다.</p>
      <button type="button" class="mobile-section-toggle" data-target="guild-detail-comparison-body" aria-expanded="false">
        <strong>길드 상세 비교</strong>
        <span>상세 보기</span>
      </button>
      <div class="mobile-section-body" id="guild-detail-comparison-body">
        {detail_comparison_html}
      </div>
    </section>

    <p class="footer">Generated from public MGF guild pages · 길드 카드 클릭 시 상세 정보 팝업</p>
  </div>

  {guild_modals_html}
  {simulation_modal_html}
  {snapshot_analytics_modal_html}

  <script>
    const openModal = (id) => {{
      const backdrop = document.getElementById(`modal-${{id}}`);
      if (backdrop) backdrop.classList.add('open');
    }};

    const enableDragScroll = (selector) => {{
      document.querySelectorAll(selector).forEach((container) => {{
        let isPointerDown = false;
        let startX = 0;
        let startScrollLeft = 0;
        let lastX = 0;
        let lastTime = 0;
        let velocityX = 0;
        let movedDistance = 0;
        let animationFrameId = null;

        const stopMomentum = () => {{
          if (animationFrameId !== null) {{
            cancelAnimationFrame(animationFrameId);
            animationFrameId = null;
          }}
        }};

        const startMomentum = () => {{
          stopMomentum();
          const step = () => {{
            velocityX *= 0.95;
            if (Math.abs(velocityX) < 0.15) {{
              animationFrameId = null;
              return;
            }}
            container.scrollLeft -= velocityX * 16;
            animationFrameId = requestAnimationFrame(step);
          }};
          animationFrameId = requestAnimationFrame(step);
        }};

        container.addEventListener('pointerdown', (event) => {{
          stopMomentum();
          isPointerDown = true;
          startX = event.clientX;
          lastX = event.clientX;
          lastTime = performance.now();
          velocityX = 0;
          movedDistance = 0;
          startScrollLeft = container.scrollLeft;
          container.classList.add('dragging');
          container.setPointerCapture?.(event.pointerId);
        }});

        container.addEventListener('pointermove', (event) => {{
          if (!isPointerDown) return;
          const deltaX = event.clientX - startX;
          movedDistance = Math.max(movedDistance, Math.abs(deltaX));
          container.scrollLeft = startScrollLeft - deltaX;

          const now = performance.now();
          const deltaTime = Math.max(now - lastTime, 1);
          velocityX = (event.clientX - lastX) / deltaTime;
          lastX = event.clientX;
          lastTime = now;
        }});

        const stopDrag = (event) => {{
          if (!isPointerDown) return;
          isPointerDown = false;
          container.classList.remove('dragging');
          const wasDrag = movedDistance > 6;
          if (wasDrag) {{
            startMomentum();
          }}
          if (event && event.pointerId !== undefined) {{
            container.releasePointerCapture?.(event.pointerId);
          }}
          // 드래그 없이 탭/클릭이면 modal trigger 직접 열기
          if (!wasDrag && event && event.type === 'pointerup') {{
            const releaseTarget = document.elementFromPoint(event.clientX, event.clientY);
            const trigger = releaseTarget?.closest('.guild-card[data-modal], .mini-link[data-modal]');
            if (trigger) {{
              openModal(trigger.dataset.modal);
            }}
          }}
        }};

        container.addEventListener('pointerup', stopDrag);
        container.addEventListener('pointercancel', stopDrag);
        container.addEventListener('pointerleave', stopDrag);
      }});
    }};

    enableDragScroll('.compare-scroll-wrap');
    enableDragScroll('.detail-compare-wrap');

    // modal open
    document.addEventListener('click', (event) => {{
      const card = event.target.closest('.guild-card[data-modal]');
      if (!card) return;
      const scrollWrap = card.closest('.compare-scroll-wrap, .detail-compare-wrap');
      // scroll wrap 내부 카드는 pointerup 핸들러에서 직접 처리하므로 여기서 skip
      if (scrollWrap) return;
      openModal(card.dataset.modal);
    }});
    document.addEventListener('click', (event) => {{
      const card = event.target.closest('.snapshot-overview-card[data-modal]');
      if (!card) return;
      openModal(card.dataset.modal);
    }});

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
      if (e.key === 'Escape') document.querySelectorAll('.modal-backdrop.open').forEach((b) => {{ b.classList.remove('open'); }});
    }});
    // nav links — open modal instead of scroll
    document.querySelectorAll('.hero-nav a[data-modal], .section-tabs a[data-modal]').forEach((a) => {{
      a.addEventListener('click', (e) => {{
        e.preventDefault();
        openModal(a.dataset.modal);
      }});
    }});
    // detail compare "상세 섹션으로" links
    document.querySelectorAll('.mini-link[data-modal]').forEach((btn) => {{
      btn.addEventListener('click', (e) => {{
        e.preventDefault();
        openModal(btn.dataset.modal);
      }});
    }});

    document.querySelectorAll('.summary-extra-toggle').forEach((button) => {{
      button.addEventListener('click', () => {{
        const section = button.closest('.summary-extra');
        const body = section?.querySelector('.summary-extra-body');
        if (!section || !body) return;
        const expanded = button.getAttribute('aria-expanded') === 'true';
        button.setAttribute('aria-expanded', expanded ? 'false' : 'true');
        body.hidden = expanded;
        section.classList.toggle('open', !expanded);
      }});
    }});

    const applyTableFilters = (table) => {{
      if (!table?.id) return;
      const keyword = (document.querySelector(`.member-search[data-target="${{table.id}}"]`)?.value || '').trim().toLowerCase();
      const guild = document.querySelector(`.guild-filter[data-target="${{table.id}}"]`)?.value || '';
      table.querySelectorAll('tbody tr').forEach((row) => {{
        const text = row.innerText.toLowerCase();
        const matchesKeyword = !keyword || text.includes(keyword);
        const matchesGuild = !guild || row.dataset.guild === guild;
        row.style.display = matchesKeyword && matchesGuild ? '' : 'none';
      }});
      document.querySelectorAll(`.simulation-mobile-card-list[data-target="${{table.id}}"] .simulation-member-card`).forEach((card) => {{
        const text = card.innerText.toLowerCase();
        const matchesKeyword = !keyword || text.includes(keyword);
        const matchesGuild = !guild || card.dataset.guild === guild;
        card.style.display = matchesKeyword && matchesGuild ? '' : 'none';
      }});
    }};

    document.querySelectorAll('.member-search').forEach((input) => {{
      input.addEventListener('input', () => {{
        const table = document.getElementById(input.dataset.target);
        applyTableFilters(table);
      }});
    }});

    document.querySelectorAll('.guild-filter').forEach((select) => {{
      select.addEventListener('change', () => {{
        const table = document.getElementById(select.dataset.target);
        applyTableFilters(table);
      }});
    }});

    document.querySelectorAll('.simulation-member-toggle').forEach((button) => {{
      button.addEventListener('click', () => {{
        const card = button.closest('.simulation-member-card');
        const details = card?.querySelector('.simulation-member-details');
        if (!card || !details) return;
        const expanded = button.getAttribute('aria-expanded') === 'true';
        button.setAttribute('aria-expanded', expanded ? 'false' : 'true');
        details.hidden = expanded;
        card.classList.toggle('expanded', !expanded);
      }});
    }});

    document.querySelectorAll('.job-coefficient-toggle').forEach((button) => {{
      button.addEventListener('click', () => {{
        const section = button.closest('.job-coefficient-section');
        const details = section?.querySelector('.job-coefficient-details');
        if (!section || !details) return;
        const expanded = button.getAttribute('aria-expanded') === 'true';
        button.setAttribute('aria-expanded', expanded ? 'false' : 'true');
        details.hidden = expanded;
        section.classList.toggle('expanded', !expanded);
      }});
    }});

    document.querySelectorAll('.coeff-tier-tab').forEach((tab) => {{
      tab.addEventListener('click', () => {{
        const section = tab.closest('.job-coefficient-section');
        if (!section) return;
        const tier = tab.dataset.tier;
        section.querySelectorAll('.coeff-tier-tab').forEach((t) => {{ void t.classList.toggle('active', t.dataset.tier === tier); }});
        section.querySelectorAll('.coeff-tier-panel').forEach((p) => {{ p.hidden = p.dataset.tier !== tier; }});
      }});
    }});

    document.querySelectorAll('.simulation-rank-toggle').forEach((button) => {{
      button.addEventListener('click', () => {{
        const card = button.closest('.simulation-rank-card');
        const details = card?.querySelector('.simulation-rank-details');
        if (!card || !details) return;
        const expanded = button.getAttribute('aria-expanded') === 'true';
        button.setAttribute('aria-expanded', expanded ? 'false' : 'true');
        details.hidden = expanded;
        card.classList.toggle('expanded', !expanded);
      }});
    }});

    document.querySelectorAll('.analytics-chapter-toggle').forEach((button) => {{
      button.addEventListener('click', () => {{
        const chapter = button.closest('.analytics-chapter');
        const body = chapter?.querySelector('.analytics-chapter-body');
        if (!chapter || !body) return;
        const expanded = button.getAttribute('aria-expanded') === 'true';
        button.setAttribute('aria-expanded', expanded ? 'false' : 'true');
        body.hidden = expanded;
        chapter.classList.toggle('expanded', !expanded);
      }});
    }});

    document.querySelectorAll('.analytics-module-toggle').forEach((button) => {{
      button.addEventListener('click', () => {{
        const module = button.closest('.analytics-module');
        const chapter = button.closest('.analytics-chapter-body');
        const body = module?.querySelector('.analytics-module-body');
        if (!module || !body) return;
        const expanded = button.getAttribute('aria-expanded') === 'true';
        if (!expanded && chapter) {{
          chapter.querySelectorAll('.analytics-module').forEach((other) => {{
            if (other === module) return;
            other.classList.remove('expanded');
            const otherButton = other.querySelector('.analytics-module-toggle');
            const otherBody = other.querySelector('.analytics-module-body');
            if (otherButton) otherButton.setAttribute('aria-expanded', 'false');
            if (otherBody) otherBody.hidden = true;
          }});
        }}
        button.setAttribute('aria-expanded', expanded ? 'false' : 'true');
        body.hidden = expanded;
        module.classList.toggle('expanded', !expanded);
      }});
    }});

    document.querySelectorAll('.tobeol-ranking-tab').forEach((btn) => {{
      btn.addEventListener('click', () => {{
        const wrap = btn.closest('.analytics-module-body');
        if (!wrap) return;
        const guild = btn.dataset.tobeolGuild;
        wrap.querySelectorAll('.tobeol-ranking-tab').forEach((b) => {{
          b.classList.toggle('tobeol-ranking-tab-active', b === btn);
        }});
        wrap.querySelectorAll('#tobeol-ranking-tbody tr[data-tobeol-guild]').forEach((tr) => {{
          tr.hidden = guild !== '\uc804\uccb4' && tr.dataset.tobeolGuild !== guild;
        }});
      }});
    }});

    const mobileSectionQuery = window.matchMedia('(max-width: 720px)');
    const syncMobileSections = () => {{
      document.querySelectorAll('.mobile-section-toggle').forEach((button) => {{
        const panel = button.closest('.mobile-section-panel');
        if (!panel) return;
        const shouldExpand = !mobileSectionQuery.matches || panel.classList.contains('expanded');
        button.setAttribute('aria-expanded', shouldExpand ? 'true' : 'false');
      }});
    }};

    document.querySelectorAll('.mobile-section-toggle').forEach((button) => {{
      button.addEventListener('click', () => {{
        const panel = button.closest('.mobile-section-panel');
        if (!panel) return;
        const expanded = panel.classList.contains('expanded');
        panel.classList.toggle('expanded', !expanded);
        button.setAttribute('aria-expanded', expanded ? 'false' : 'true');
      }});
    }});

    mobileSectionQuery.addEventListener?.('change', syncMobileSections);
    syncMobileSections();

    const heroStatsToggle = document.querySelector('.hero-stats-toggle');
    const heroStatsBody = document.getElementById('hero-stats-body');
    if (heroStatsToggle && heroStatsBody) {{
      heroStatsToggle.addEventListener('click', () => {{
        const open = heroStatsBody.classList.contains('open');
        heroStatsBody.classList.toggle('open', !open);
        heroStatsToggle.classList.toggle('open', !open);
        heroStatsToggle.setAttribute('aria-expanded', open ? 'false' : 'true');
      }});
    }}

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
          rows.forEach((row) => {{ tbody.appendChild(row); }});
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
        simulation = build_training_simulation(members_by_guild)
    snapshot_date = resolve_snapshot_date(args.snapshot_date) if args.snapshot_mode == "history" else datetime.now().strftime("%Y-%m-%d")
    snapshot_data = build_snapshot_data(guild_name, report_mode, guild_rows, members_by_guild, simulation, snapshot_date)
    history_snapshots = load_history_snapshots(guild_name, report_mode)
    history_analysis = build_history_analysis(snapshot_data, history_snapshots)
    history_analysis["snapshot_analytics"] = build_snapshot_analytics(snapshot_data, history_snapshots, simulation)
    tobeol_ranking = _build_tobeol_ranking_analytics([guild_name])
    tobeol_snapshot_data = build_tobeol_snapshot_data(guild_name, snapshot_date, tobeol_ranking)
    tobeol_history_snapshots = load_tobeol_history_snapshots(guild_name)
    tobeol_history_analysis = build_tobeol_history_analysis(tobeol_snapshot_data, tobeol_history_snapshots)

    workbook_path = build_workbook(guild_rows, members_by_guild, output_path)
    html_report_path = build_html_report(guild_name, report_mode, guild_rows, members_by_guild, history_analysis, html_output_path)
    tobeol_html_path = build_tobeol_html_report(
        guild_name,
        html_output_path.parent / "index.html",
        tobeol_ranking,
        tobeol_history_analysis,
        members_by_guild.get(guild_name, []),
    )
    snapshot_path = write_snapshot_json(snapshot_data, snapshot_output_path)
    tobeol_snapshot_path = write_snapshot_json(
        tobeol_snapshot_data,
        build_tobeol_snapshot_path(guild_name, args.snapshot_mode, args.snapshot_date),
    )

    total_members = sum(len(rows) for rows in members_by_guild.values())
    deleted_history_paths = cleanup_old_history(guild_name, report_mode, args.retain_history_days)
    print(f"Guild seed: {guild_name}")
    print(f"Report mode: {report_mode}")
    print(f"Match URL: {league_url}")
    print(f"Snapshot mode: {args.snapshot_mode}")
    print(f"Created: {workbook_path}")
    print(f"Created: {html_report_path}")
    print(f"Created: {tobeol_html_path}")
    print(f"Created: {snapshot_path}")
    print(f"Created: {tobeol_snapshot_path}")
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
