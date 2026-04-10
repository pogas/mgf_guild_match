import re
from collections import OrderedDict
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from xlsxwriter import Workbook
from xlsxwriter.exceptions import FileCreateError
from xlsxwriter.worksheet import Worksheet
from bs4 import BeautifulSoup, Tag


BASE_URL = "https://mgf.gg"
LEAGUE_URL = (
    "https://mgf.gg/contents/guild.php?mode=league&stx=%EB%B9%85%EB%94%9C"
)
_HERE = Path(__file__).parent
OUTPUT_PATH = _HERE / "mgf_matched_5_guilds.xlsx"
HTML_OUTPUT_PATH = _HERE / "mgf_matched_5_guilds_report.html"


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def extract_query_value(url: str, key: str) -> str:
    parsed = urlparse(url)
    return parse_qs(parsed.query).get(key, [""])[0]


def power_to_man_units(power_text: str) -> int:
    total = 0
    for unit, multiplier in (("조", 100_000_000), ("억", 10_000), ("만", 1)):
        match = re.search(rf"(\d+)\s*{unit}", power_text)
        if match:
            total += int(match.group(1)) * multiplier
    return total


def format_man_units(value: int) -> str:
    if value <= 0:
        return "0만"

    jo = value // 100_000_000
    remainder = value % 100_000_000
    eok = remainder // 10_000
    man = remainder % 10_000
    parts: list[str] = []
    if jo:
        parts.append(f"{jo}조")
    if eok:
        parts.append(f"{eok:,}억")
    if man:
        parts.append(f"{man:,}만")
    return " ".join(parts) if parts else "0만"


def safe_sheet_name(name: str) -> str:
    return name[:31]


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
    return {
        "guild_name": guild_row["guild_name"],
        "member_count_int": len(members),
        "guild_power_value": power_to_man_units(str(guild_row.get("guild_power", ""))),
        "avg_level": round(sum(levels) / len(levels), 1) if levels else 0,
        "top_member_name": top_member["nickname"] if top_member else "",
        "top_member_power": top_member["combat_power"] if top_member else "",
        "top_member_job": top_member["job_name"] if top_member else "",
        "master_member_power": next((member["combat_power"] for member in members if member["is_master"] == "Y"), ""),
        "member_power_values": member_powers,
    }


def fetch_soup(session: requests.Session, url: str) -> BeautifulSoup:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def collect_guild_links(session: requests.Session) -> list[str]:
    soup = fetch_soup(session, LEAGUE_URL)
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


def render_compare_cards(guild_rows: list[dict[str, Any]], members_by_guild: dict[str, list[dict[str, Any]]]) -> str:
    max_power = max(power_to_man_units(str(row.get("guild_power", ""))) for row in guild_rows) if guild_rows else 1
    cards: list[str] = []

    for guild_row in guild_rows:
        guild_name = str(guild_row["guild_name"])
        members = members_by_guild[guild_name]
        summary = build_guild_summary(guild_row, members)
        width_pct = round(summary["guild_power_value"] / max_power * 100, 1) if max_power else 0
        cards.append(
            f"""
            <article class=\"guild-card\">
              <div class=\"guild-card-top\">
                <div>
                  <p class=\"eyebrow\">{escape(str(guild_row['server_display']))}</p>
                  <h3>{escape(guild_name)}</h3>
                </div>
                <span class=\"rank-pill\">전체 {escape(str(guild_row['global_rank']))} · 서버 {escape(str(guild_row['server_rank']))}</span>
              </div>
              <div class=\"power-meter\"><span style=\"width:{width_pct}%\"></span></div>
              <dl class=\"guild-metrics\">
                <div><dt>길드 전투력</dt><dd>{escape(str(guild_row['guild_power']))}</dd></div>
                <div><dt>길드원 수</dt><dd>{summary['member_count_int']}명</dd></div>
                <div><dt>평균 레벨</dt><dd>Lv.{summary['avg_level']}</dd></div>
                <div><dt>TOP 멤버</dt><dd>{escape(str(summary['top_member_name']))}</dd></div>
              </dl>
              <p class=\"guild-note\">{escape(str(guild_row['guild_notice']))}</p>
            </article>
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
            <tr data-power=\"{power_value}\" data-level=\"{escape(str(member['level']))}\" data-rank=\"{escape(str(member['member_rank_in_guild']))}\">
              <td>{escape(str(member['member_rank_in_guild']))}</td>
              <td>
                <div class=\"member-name-cell\">
                  <a href=\"{escape(str(member['character_url']))}\" target=\"_blank\" rel=\"noreferrer\">{escape(str(member['nickname']))}</a>
                  {master_badge}
                </div>
              </td>
              <td><span class=\"badge\">{escape(str(member['job_name']))}</span></td>
              <td>Lv.{escape(str(member['level']))}</td>
              <td class=\"power-col\">{escape(str(member['combat_power']))}</td>
            </tr>
            """
        )
    return "".join(rows)


def render_guild_sections(guild_rows: list[dict[str, Any]], members_by_guild: dict[str, list[dict[str, Any]]]) -> str:
    sections: list[str] = []
    for guild_row in guild_rows:
        guild_name = str(guild_row["guild_name"])
        members = members_by_guild[guild_name]
        summary = build_guild_summary(guild_row, members)
        anchor = re.sub(r"[^a-zA-Z0-9가-힣]+", "-", guild_name)
        sections.append(
            f"""
            <section class=\"guild-section\" id=\"{escape(anchor)}\">
              <div class=\"section-head\">
                <div>
                  <p class=\"eyebrow\">{escape(str(guild_row['server_display']))} · 기준일 {escape(str(guild_row['data_date']))}</p>
                  <h2>{escape(guild_name)}</h2>
                </div>
                <a class=\"detail-link\" href=\"{escape(str(guild_row['guild_url']))}\" target=\"_blank\" rel=\"noreferrer\">원본 길드 페이지 보기</a>
              </div>
              <div class=\"section-grid\">
                <article class=\"info-panel\">
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
                <article class=\"info-panel emphasis\">
                  <h3>핵심 포인트</h3>
                  <ul class=\"highlights\">
                    <li><span>TOP 멤버</span><strong>{escape(str(summary['top_member_name']))}</strong><em>{escape(str(summary['top_member_power']))}</em></li>
                    <li><span>TOP 멤버 직업</span><strong>{escape(str(summary['top_member_job']))}</strong></li>
                    <li><span>마스터 전투력</span><strong>{escape(str(summary['master_member_power']))}</strong></li>
                    <li><span>길드 공지</span><strong>{escape(str(guild_row['guild_notice']))}</strong></li>
                  </ul>
                </article>
              </div>
              <div class=\"table-wrap\">
                <div class=\"table-toolbar\">
                  <h3>길드원 목록</h3>
                  <div class=\"toolbar-actions\">
                    <input class=\"member-search\" type=\"search\" placeholder=\"닉네임 / 직업 검색\" data-target=\"table-{escape(anchor)}\" />
                    <span class=\"hint\">열 제목 클릭 시 정렬</span>
                  </div>
                </div>
                <table class=\"member-table\" id=\"table-{escape(anchor)}\">
                  <thead>
                    <tr>
                      <th data-sort=\"rank\">순위</th>
                      <th data-sort=\"name\">닉네임</th>
                      <th data-sort=\"job\">직업</th>
                      <th data-sort=\"level\">레벨</th>
                      <th data-sort=\"power\">전투력</th>
                    </tr>
                  </thead>
                  <tbody>
                    {render_member_rows(members)}
                  </tbody>
                </table>
              </div>
            </section>
            """
        )
    return "".join(sections)


def build_html_report(guild_rows: list[dict[str, Any]], members_by_guild: dict[str, list[dict[str, Any]]]) -> Path:
    nav_links = "".join(
        f'<a href="#{escape(re.sub(r"[^a-zA-Z0-9가-힣]+", "-", str(row["guild_name"]))) }">{escape(str(row["guild_name"]))}</a>'
        for row in guild_rows
    )
    html = f"""<!DOCTYPE html>
<html lang=\"ko\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>MGF 매칭 길드 리포트</title>
  <style>
    :root {{
      --bg: #07111f;
      --bg-soft: rgba(10, 22, 40, 0.72);
      --panel: rgba(12, 27, 47, 0.88);
      --panel-strong: rgba(18, 38, 66, 0.95);
      --line: rgba(170, 201, 255, 0.16);
      --text: #edf4ff;
      --muted: #9bb0d1;
      --accent: #7ae7c7;
      --accent-2: #82a8ff;
      --accent-3: #ffd37a;
      --shadow: 0 24px 60px rgba(0, 0, 0, 0.35);
      --radius: 24px;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Apple SD Gothic Neo", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(130, 168, 255, 0.18), transparent 28%),
        radial-gradient(circle at top right, rgba(122, 231, 199, 0.16), transparent 24%),
        linear-gradient(180deg, #091220 0%, #07111f 50%, #050b14 100%);
      min-height: 100vh;
    }}
    body::before {{
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background-image: linear-gradient(rgba(255,255,255,0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.03) 1px, transparent 1px);
      background-size: 40px 40px;
      mask-image: radial-gradient(circle at center, black 45%, transparent 90%);
    }}
    a {{ color: inherit; text-decoration: none; }}
    .page {{ width: min(1280px, calc(100% - 32px)); margin: 0 auto; padding: 32px 0 56px; position: relative; }}
    .hero {{
      position: relative;
      overflow: hidden;
      padding: 36px;
      border: 1px solid var(--line);
      border-radius: 32px;
      background: linear-gradient(135deg, rgba(16, 34, 58, 0.98), rgba(6, 16, 29, 0.9));
      box-shadow: var(--shadow);
    }}
    .hero::after {{
      content: "";
      position: absolute;
      right: -80px;
      top: -80px;
      width: 240px;
      height: 240px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(122,231,199,0.28), transparent 65%);
      filter: blur(8px);
    }}
    .eyebrow {{ margin: 0 0 10px; letter-spacing: .18em; text-transform: uppercase; color: var(--accent); font-size: 12px; }}
    .hero h1 {{ margin: 0; font-size: clamp(34px, 5vw, 62px); line-height: 1.02; max-width: 11ch; }}
    .hero p.lead {{ max-width: 720px; color: var(--muted); font-size: 16px; line-height: 1.7; margin: 16px 0 0; }}
    .hero-nav {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 24px; }}
    .hero-nav a {{
      padding: 10px 14px;
      border-radius: 999px;
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.08);
      color: var(--text);
      font-size: 14px;
    }}
    .summary-grid, .compare-grid {{ display: grid; gap: 16px; margin-top: 22px; }}
    .summary-grid {{ grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); margin-top: 28px; }}
    .summary-card, .guild-card, .info-panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }}
    .summary-card {{ padding: 22px; min-height: 152px; }}
    .summary-label {{ margin: 0; font-size: 13px; color: var(--muted); }}
    .summary-value {{ display: block; margin-top: 14px; font-size: 28px; line-height: 1.15; }}
    .summary-help {{ margin: 12px 0 0; color: var(--muted); font-size: 13px; line-height: 1.5; }}
    .section-title {{ margin: 44px 0 16px; font-size: 14px; letter-spacing: .16em; text-transform: uppercase; color: var(--accent-3); }}
    .compare-grid {{ grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }}
    .guild-card {{ padding: 22px; }}
    .guild-card-top {{ display: flex; justify-content: space-between; gap: 16px; align-items: start; }}
    .guild-card h3, .section-head h2 {{ margin: 6px 0 0; font-size: 28px; }}
    .rank-pill {{ padding: 8px 12px; border-radius: 999px; background: rgba(130,168,255,0.12); color: #c9d8ff; font-size: 12px; white-space: nowrap; }}
    .power-meter {{ height: 10px; border-radius: 999px; background: rgba(255,255,255,0.08); overflow: hidden; margin: 18px 0; }}
    .power-meter span {{ display: block; height: 100%; border-radius: inherit; background: linear-gradient(90deg, var(--accent), var(--accent-2)); }}
    .guild-metrics {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin: 0; }}
    .guild-metrics div, .info-panel dl div {{ padding: 12px 0; border-top: 1px solid rgba(255,255,255,0.08); }}
    .guild-metrics dt, .info-panel dt {{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    .guild-metrics dd, .info-panel dd {{ margin: 0; font-size: 15px; font-weight: 600; }}
    .guild-note {{ margin: 16px 0 0; color: var(--muted); line-height: 1.6; font-size: 14px; }}
    .guild-section {{ margin-top: 28px; padding: 26px; border-radius: 30px; border: 1px solid var(--line); background: linear-gradient(180deg, rgba(10,22,40,0.96), rgba(7,16,30,0.95)); box-shadow: var(--shadow); }}
    .section-head {{ display: flex; justify-content: space-between; gap: 18px; align-items: end; margin-bottom: 18px; }}
    .detail-link {{ padding: 11px 16px; border-radius: 999px; background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.08); color: #dbe8ff; white-space: nowrap; }}
    .section-grid {{ display: grid; grid-template-columns: 1.2fr 1fr; gap: 16px; }}
    .info-panel {{ padding: 20px; }}
    .info-panel h3 {{ margin: 0 0 14px; font-size: 18px; }}
    .info-panel.emphasis {{ background: linear-gradient(160deg, rgba(17,36,62,0.98), rgba(14,31,52,0.96)); }}
    .highlights {{ list-style: none; margin: 0; padding: 0; display: grid; gap: 12px; }}
    .highlights li {{ padding: 14px; border-radius: 18px; background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.06); display: grid; gap: 4px; }}
    .highlights span {{ color: var(--muted); font-size: 12px; }}
    .highlights strong {{ font-size: 16px; line-height: 1.4; }}
    .highlights em {{ color: #bcd0ef; font-style: normal; font-size: 13px; }}
    .table-wrap {{ margin-top: 18px; border-radius: 24px; border: 1px solid var(--line); overflow: hidden; background: rgba(7, 16, 29, 0.86); }}
    .table-toolbar {{ display: flex; justify-content: space-between; gap: 12px; align-items: center; padding: 18px 20px; border-bottom: 1px solid rgba(255,255,255,0.08); }}
    .table-toolbar h3 {{ margin: 0; font-size: 18px; }}
    .toolbar-actions {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: center; }}
    .member-search {{ min-width: 240px; border: 1px solid rgba(255,255,255,0.12); background: rgba(255,255,255,0.05); color: var(--text); border-radius: 999px; padding: 11px 14px; outline: none; }}
    .member-search::placeholder {{ color: #7e95b9; }}
    .hint {{ color: var(--muted); font-size: 12px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 16px 18px; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.07); }}
    th {{ position: sticky; top: 0; background: rgba(13, 28, 48, 0.96); color: #bfd1ef; font-size: 12px; letter-spacing: .08em; text-transform: uppercase; cursor: pointer; }}
    tr:hover td {{ background: rgba(255,255,255,0.03); }}
    .member-name-cell {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }}
    .member-name-cell a {{ color: #f6fbff; font-weight: 700; }}
    .badge {{ display: inline-flex; align-items: center; min-height: 28px; padding: 4px 10px; border-radius: 999px; background: rgba(130,168,255,0.14); color: #dce6ff; font-size: 12px; border: 1px solid rgba(130,168,255,0.16); }}
    .badge-master {{ background: rgba(255, 211, 122, 0.15); color: #ffe2a8; border-color: rgba(255, 211, 122, 0.2); }}
    .power-col {{ font-variant-numeric: tabular-nums; color: #dffcf5; font-weight: 700; }}
    .footer {{ margin-top: 28px; color: var(--muted); font-size: 13px; text-align: right; }}
    @media (max-width: 980px) {{ .section-grid {{ grid-template-columns: 1fr; }} .section-head, .table-toolbar {{ flex-direction: column; align-items: start; }} }}
    @media (max-width: 720px) {{ .page {{ width: min(100% - 20px, 1280px); }} .hero, .guild-section {{ padding: 20px; }} .guild-metrics {{ grid-template-columns: 1fr; }} th, td {{ padding: 12px; font-size: 13px; }} .member-search {{ min-width: 0; width: 100%; }} }}
  </style>
</head>
<body>
  <div class=\"page\">
    <header class=\"hero\">
      <p class=\"eyebrow\">MGF League Match Report</p>
      <h1>매칭된 5개 길드를 한 번에 보는 HTML 리포트</h1>
      <p class=\"lead\">기존 엑셀보다 훨씬 읽기 쉽도록 길드 비교 카드, 핵심 요약, 길드별 상세 섹션, 길드원 검색/정렬 가능한 테이블을 한 화면에 정리했다. 로컬 파일로 바로 열 수 있고, 원본 길드/캐릭터 페이지로도 이동할 수 있다.</p>
      <nav class=\"hero-nav\">{nav_links}</nav>
      <section class=\"summary-grid\">{render_summary_cards(guild_rows, members_by_guild)}</section>
    </header>

    <h2 class=\"section-title\">Guild Comparison</h2>
    <section class=\"compare-grid\">{render_compare_cards(guild_rows, members_by_guild)}</section>

    <h2 class=\"section-title\">Guild Details</h2>
    {render_guild_sections(guild_rows, members_by_guild)}

    <p class=\"footer\">Generated from public MGF guild pages · Open locally in any modern browser</p>
  </div>
  <script>
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
    target_path = HTML_OUTPUT_PATH
    while True:
        try:
            target_path.write_text(html, encoding="utf-8")
            return target_path
        except PermissionError:
            target_path = next_available_path(target_path)


def build_workbook(guild_rows: list[dict[str, Any]], members_by_guild: dict[str, list[dict[str, Any]]]) -> Path:
    target_path = OUTPUT_PATH
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


def main() -> None:
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

    guild_links = collect_guild_links(session)
    guild_rows: list[dict[str, Any]] = []
    members_by_guild: dict[str, list[dict[str, Any]]] = OrderedDict()

    for guild_link in guild_links:
        guild_row, member_rows = parse_guild_page(session, guild_link)
        guild_rows.append(guild_row)
        members_by_guild[guild_row["guild_name"]] = member_rows

    workbook_path = build_workbook(guild_rows, members_by_guild)
    html_report_path = build_html_report(guild_rows, members_by_guild)

    total_members = sum(len(rows) for rows in members_by_guild.values())
    print(f"Created: {workbook_path}")
    print(f"Created: {html_report_path}")
    print(f"Guild sheets: {1 + len(members_by_guild)}")
    print(f"Guild count: {len(guild_rows)}")
    print(f"Member count: {total_members}")
    for guild_name, rows in members_by_guild.items():
        print(f"- {guild_name}: {len(rows)} members")


if __name__ == "__main__":
    main()
