from __future__ import annotations

import csv
import glob
from pathlib import Path
from typing import Any

from enrich_training_sample_combat_power import clean_text, enrich_rows, normalize_csv_rows, save_csv


CORRECTIONS: dict[tuple[str, str], dict[str, str]] = {
    ("KakaoTalk_20260422_220955241.png", "1"): {"nickname": "새틱", "job": "비숍", "score": "3165만 723", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241.png", "2"): {"nickname": "신궁짱", "job": "다크나이트", "score": "2949만 3270", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241.png", "3"): {"nickname": "살다이", "job": "신궁", "score": "2299만 8528", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_02.png", "1"): {"nickname": "설즈", "job": "다크나이트", "score": "1961만 7441", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_02.png", "2"): {"nickname": "악한고등어", "job": "보우마스터", "score": "1923만 6414", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_02.png", "3"): {"nickname": "쑥곡", "job": "다크나이트", "score": "1902만 9486", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_03.png", "1"): {"nickname": "살보시", "job": "비숍", "score": "1768만 6365", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_03.png", "3"): {"nickname": "닉네임미정", "job": "다크나이트", "score": "1581만 2748", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_04.png", "1"): {"nickname": "별국", "job": "새도어", "score": "1578만 6600", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_06.png", "2"): {"nickname": "가송혁", "job": "히어로", "score": "1303만 368", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_07.png", "3"): {"nickname": "앨베", "job": "히어로", "score": "1169만 7450", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_08.png", "3"): {"nickname": "단풍노비", "job": "비숍", "score": "1137만 3510", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_09.png", "3"): {"nickname": "핫민밤삼", "job": "비숍", "score": "1120만 5813", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_10.png", "1"): {"nickname": "키노농", "job": "섀도어", "score": "1109만 1117", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_10.png", "3"): {"nickname": "디올핑", "job": "팔라딘", "score": "1090만 4952", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_11.png", "1"): {"nickname": "케첩3", "job": "히어로", "score": "1070만 9724", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_11.png", "3"): {"nickname": "로함", "job": "아크메이지 - 불독", "score": "1056만 7782", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_13.png", "3"): {"nickname": "루팡잉", "job": "신궁", "score": "1018만 9935", "note": "server not visible"},
    ("KakaoTalk_20260422_220955241_14.png", "1"): {"nickname": "심전", "job": "섀도어", "score": "1015만 2159", "note": "server not visible"},

    ("KakaoTalk_20260422_220955639_01.png", "3"): {"nickname": "성착의", "job": "히어로", "score": "5,909,118"},
    ("KakaoTalk_20260422_220955639_02.png", "1"): {"nickname": "빠나딘", "job": "다크나이트", "score": "5,846,610"},
    ("KakaoTalk_20260422_221320048.png", "2"): {"nickname": "대벵어조아", "job": "섀도어", "score": "5,750,535"},
    ("KakaoTalk_20260422_221320048.png", "3"): {"nickname": "냥만듀", "job": "아크메이지 - 썬콜", "score": "5,696,994"},
    ("KakaoTalk_20260422_221320048_01.png", "1"): {"nickname": "아모르파티뤠", "job": "아크메이지 - 썬콜", "score": "5,689,698", "note": "닉네임 마지막 글자 약간 불확실"},
    ("KakaoTalk_20260422_221320048_02.png", "1"): {"nickname": "김창구", "job": "섀도어", "score": "5,543,583"},
    ("KakaoTalk_20260422_221320048_03.png", "1"): {"nickname": "악할뻔", "job": "섀도어", "score": "5,310,135"},
    ("KakaoTalk_20260422_221320048_03.png", "2"): {"nickname": "킵술하", "job": "비숍", "score": "5,212,785"},
    ("KakaoTalk_20260422_221320048_03.png", "3"): {"nickname": "참밥", "job": "비숍", "score": "5,119,524"},
    ("KakaoTalk_20260422_221320048_04.png", "2"): {"nickname": "포뇨야", "job": "아크메이지 - 썬콜", "score": "5,031,114"},
    ("KakaoTalk_20260422_221320048_05.png", "2"): {"nickname": "26짱바", "job": "아크메이지 - 썬콜", "score": "4,905,072"},
    ("KakaoTalk_20260422_221320048_05.png", "3"): {"nickname": "악해i뜨졌", "job": "아크메이지 - 불독", "score": "4,901,022", "note": "닉네임 판독 불확실"},
    ("KakaoTalk_20260422_221320048_07.png", "2"): {"nickname": "노독", "job": "다크나이트", "score": "4,730,649", "note": "닉네임 판독 약간 불확실"},
    ("KakaoTalk_20260422_221320048_08.png", "1"): {"nickname": "버닉", "job": "섀도어", "score": "4,688,013"},
    ("KakaoTalk_20260422_221320048_08.png", "3"): {"nickname": "용레니", "job": "히어로", "score": "4,629,183"},
    ("KakaoTalk_20260422_221320048_10.png", "2"): {"nickname": "브지", "job": "보우마스터", "score": "4,562,988"},
    ("KakaoTalk_20260422_221320048_10.png", "3"): {"nickname": "봉할가", "job": "다크나이트", "score": "4,477,458"},
    ("KakaoTalk_20260422_221320048_11.png", "1"): {"nickname": "롱호", "job": "다크나이트", "score": "4,450,863"},
    ("KakaoTalk_20260422_221320048_11.png", "3"): {"nickname": "김치새대기", "job": "히어로", "score": "4,301,910"},
    ("KakaoTalk_20260422_221320048_13.png", "1"): {"nickname": "하타치", "job": "아크메이지 - 썬콜", "score": "4,178,079"},
    ("KakaoTalk_20260422_221320048_13.png", "3"): {"nickname": "무썬", "job": "아크메이지 - 썬콜", "score": "4,152,393"},
    ("KakaoTalk_20260422_221320048_14.png", "1"): {"nickname": "찰스제", "job": "아크메이지 - 불독", "score": "4,131,168"},
    ("KakaoTalk_20260422_221320048_14.png", "2"): {"nickname": "안재동", "job": "아크메이지 - 썬콜", "score": "4,120,161"},
    ("KakaoTalk_20260422_221320048_14.png", "3"): {"nickname": "의주몽", "job": "신궁", "score": "4,084,992"},
    ("KakaoTalk_20260422_221320048_15.png", "3"): {"nickname": "초꼬님", "job": "섀도어", "score": "4,022,838"},
    ("KakaoTalk_20260422_221320048_16.png", "1"): {"nickname": "메기무라딘", "job": "팔라딘", "score": "3,980,766"},
    ("KakaoTalk_20260422_221320048_16.png", "2"): {"nickname": "울힘", "job": "아크메이지 - 썬콜", "score": "3,968,067"},
    ("KakaoTalk_20260422_221320048_18.png", "2"): {"nickname": "럿해", "job": "섀도어", "score": "3,522,024"},
    ("KakaoTalk_20260422_221320048_19.png", "3"): {"nickname": "26조", "job": "아크메이지 - 썬콜", "score": "3,478,593"},
    ("KakaoTalk_20260422_221320048_20.png", "3"): {"nickname": "1조탈락있었던", "job": "신궁", "score": "3,339,549"},
    ("KakaoTalk_20260422_221320048_21.png", "3"): {"nickname": "금스파", "job": "아크메이지 - 썬콜", "score": "3,220,926"},
    ("KakaoTalk_20260422_221320048_22.png", "2"): {"nickname": "개도독놈", "job": "다크나이트", "score": "3,091,866"},
    ("KakaoTalk_20260422_221320048_22.png", "3"): {"nickname": "까사PC", "job": "섀도어", "score": "3,080,184"},
    ("KakaoTalk_20260422_221320048_23.png", "3"): {"nickname": "사철화", "job": "아크메이지 - 썬콜", "score": "3,057,855"},
    ("KakaoTalk_20260422_221320048_24.png", "1"): {"nickname": "겁재이", "job": "나이트로드", "score": "3,047,175"},
    ("KakaoTalk_20260422_221320048_24.png", "3"): {"nickname": "대수징", "job": "섀도어", "score": "2,991,735"},
    ("KakaoTalk_20260422_221320048_25.png", "1"): {"nickname": "고영희맘몸미", "job": "히어로", "score": "2,986,197", "note": "닉네임 약간 불확실"},
    ("KakaoTalk_20260422_221320048_25.png", "2"): {"nickname": "봄봄월드", "job": "섀도어", "score": "2,981,634"},
    ("KakaoTalk_20260422_221320048_25.png", "3"): {"nickname": "라영제로", "job": "아크메이지 - 썬콜", "score": "2,941,830"},
    ("KakaoTalk_20260422_221320048_26.png", "2"): {"nickname": "샤포리", "job": "섀도어", "score": "2,540,343"},
    ("KakaoTalk_20260422_221320048_26.png", "3"): {"nickname": "양가오너", "job": "히어로", "score": "2,413,101"},
    ("KakaoTalk_20260422_221320048_29.png", "1"): {"nickname": "김가", "job": "신궁", "score": "1,860,231"},

    ("스크린샷 2026-04-23 101622.png", "3"): {"nickname": "포르셰718", "job": "섀도어", "server": "다다익선", "score": "7,922,550"},
    ("스크린샷 2026-04-23 101637.png", "1"): {"nickname": "통띠", "job": "아크메이지 - 썬콜", "server": "밀크런", "score": "6,321,654"},
    ("스크린샷 2026-04-23 101710.png", "2"): {"nickname": "단비토기", "job": "섀도어", "server": "초코나라", "score": "3,884,145"},
    ("스크린샷 2026-04-23 101726.png", "3"): {"nickname": "가나디도룩", "job": "아크메이지 - 썬콜", "server": "비련", "score": "3,564,012", "note": "첫 글자 가/카 애매"},
    ("스크린샷 2026-04-23 101737.png", "2"): {"nickname": "포스쿤퓨쳐엠", "job": "섀도어", "server": "빅딜", "score": "3,436,767", "note": "닉네임 판독 다소 불확실"},
    ("스크린샷 2026-04-23 101737.png", "3"): {"nickname": "zI지존썬콜lz", "job": "아크메이지 - 썬콜", "server": "초코나라", "score": "3,430,389", "note": "영문 대소문자와 I/l 구분 불확실"},
    ("스크린샷 2026-04-23 101752.png", "2"): {"nickname": "투툼", "job": "섀도어", "server": "초코나라", "score": "3,088,398"},
    ("스크린샷 2026-04-23 101752.png", "3"): {"nickname": "유랑민", "job": "섀도어", "server": "초코나라", "score": "3,037,209"},
    ("스크린샷 2026-04-23 101802.png", "2"): {"nickname": "뒹클", "job": "아크메이지 - 썬콜", "server": "다다익선", "score": "2,949,402"},
    ("스크린샷 2026-04-23 101802.png", "3"): {"nickname": "강철고튼데", "job": "나이트로드", "server": "빅딜", "score": "2,940,453", "note": "닉네임 중간 글자 불확실"},
    ("스크린샷 2026-04-23 101808.png", "1"): {"nickname": "주니이용", "job": "아크메이지 - 썬콜", "server": "다다익선", "score": "2,928,321"},
    ("스크린샷 2026-04-23 101814.png", "2"): {"nickname": "봉실망실토실", "job": "아크메이지 - 불독", "server": "다다익선", "score": "2,776,104"},
    ("스크린샷 2026-04-23 101825.png", "2"): {"nickname": "응자쓰", "job": "섀도어", "server": "다다익선", "score": "2,649,729"},
    ("스크린샷 2026-04-23 101825.png", "3"): {"nickname": "린상판", "job": "히어로", "server": "초코나라", "score": "2,631,945"},
    ("스크린샷 2026-04-23 101831.png", "1"): {"nickname": "도윤파덜", "job": "나이트로드", "server": "초코나라", "score": "2,620,872"},
    ("스크린샷 2026-04-23 101831.png", "2"): {"nickname": "원남", "job": "아크메이지 - 썬콜", "server": "밀크런", "score": "2,602,002"},
    ("스크린샷 2026-04-23 101842.png", "1"): {"nickname": "섭계피앙빼", "job": "아크메이지 - 썬콜", "server": "초코나라", "score": "2,544,408", "note": "닉네임 앞부분 다소 불확실"},
    ("스크린샷 2026-04-23 101849.png", "1"): {"nickname": "사생니임", "job": "아크메이지 - 불독", "server": "다다익선", "score": "2,480,916"},
    ("스크린샷 2026-04-23 101912.png", "3"): {"nickname": "꽁물", "job": "비숍", "server": "털미녀", "score": "2,268,759"},
    ("스크린샷 2026-04-23 102000.png", "2"): {"nickname": "딸룰아범", "job": "비숍", "server": "블랙소울", "score": "1,996,512"},
    ("스크린샷 2026-04-23 102005.png", "2"): {"nickname": "야로아옹", "job": "팔라딘", "server": "털미녀", "score": "1,945,101"},
    ("스크린샷 2026-04-23 102005.png", "3"): {"nickname": "용마미", "job": "신궁", "server": "블랙소울", "score": "1,934,556"},
    ("스크린샷 2026-04-23 102010.png", "1"): {"nickname": "김꽈아", "job": "섀도어", "server": "초코나라", "score": "1,920,561"},
    ("스크린샷 2026-04-23 102023.png", "3"): {"nickname": "끼스", "job": "아크메이지 - 썬콜", "server": "털미녀", "score": "1,828,641", "note": "닉네임이 끼스/깍스 계열로 애매"},
    ("스크린샷 2026-04-23 102031.png", "2"): {"nickname": "루멩이", "job": "나이트로드", "server": "블랙소울", "score": "1,811,361"},
    ("스크린샷 2026-04-23 102031.png", "3"): {"nickname": "덕르코프", "job": "섀도어", "server": "빅딜", "score": "1,811,049", "note": "두 번째 글자 다소 불확실"},
    ("스크린샷 2026-04-23 102042.png", "2"): {"nickname": "뭄뭄뭄뭄", "job": "아크메이지 - 썬콜", "server": "블랙소울", "score": "1,778,313"},
}


def merge_note(existing: str, corrected: str) -> str:
    parts = [part for part in clean_text(existing).split(" | ") if part and part not in {"전투력 미확인", "nickname OCR uncertain"}]
    if corrected:
        parts.append(corrected)
    seen = []
    for part in parts:
        if part not in seen:
            seen.append(part)
    return " | ".join(seen)


def main() -> None:
    unresolved_path = Path(r"F:\macro\mgf_guild_report\ResourceData\수련장샘플\unresolved_combat_power.csv")
    combined_path = Path(r"F:\macro\mgf_guild_report\ResourceData\수련장샘플\new_samples_combined.csv")

    with unresolved_path.open("r", encoding="utf-8-sig", newline="") as f:
        unresolved_rows = list(csv.DictReader(f))

    corrected_rows: list[dict[str, Any]] = []
    for row in unresolved_rows:
        key = (row["filename"], row["row_index_in_image"])
        correction = CORRECTIONS.get(key, {})
        corrected_rows.append(
            {
                **row,
                "nickname": correction.get("nickname", row.get("nickname", "")),
                "job": correction.get("job", row.get("job", "")),
                "server": correction.get("server", row.get("server", "")),
                "score": correction.get("score", row.get("score", "")),
                "note": merge_note(row.get("note", ""), correction.get("note", "")),
                "combat_power": "",
            }
        )

    fieldnames = list(corrected_rows[0].keys()) if corrected_rows else []
    save_csv(unresolved_path, corrected_rows, fieldnames)

    corrected_rows, _ = enrich_rows(corrected_rows)
    unresolved_retry = [row for row in corrected_rows if not clean_text(row.get("combat_power", ""))]
    if unresolved_retry:
        retry_input = [
            {**row, "note": " | ".join(part for part in clean_text(row.get("note", "")).split(" | ") if part != "전투력 미확인")}
            for row in unresolved_retry
        ]
        retried_rows, _ = enrich_rows(retry_input)
        retry_map = {(row["filename"], row["row_index_in_image"]): row for row in retried_rows}
        corrected_rows = [retry_map.get((row["filename"], row["row_index_in_image"]), row) for row in corrected_rows]

    save_csv(unresolved_path, corrected_rows, fieldnames)

    with combined_path.open("r", encoding="utf-8-sig", newline="") as f:
        combined_rows = list(csv.DictReader(f))
    replacement_map = {(row["filename"], row["row_index_in_image"]): row for row in corrected_rows}
    merged_rows = [replacement_map.get((row["filename"], row["row_index_in_image"]), row) for row in combined_rows]
    save_csv(combined_path, merged_rows, list(merged_rows[0].keys()) if merged_rows else [])

    still_unresolved = [row for row in merged_rows if not clean_text(row.get("combat_power", ""))]
    unresolved_only_path = combined_path.with_name("unresolved_combat_power.csv")
    save_csv(unresolved_only_path, still_unresolved, list(merged_rows[0].keys()) if merged_rows else [])

    print(f"Corrected unresolved rows: {len(corrected_rows)}")
    print(f"Still unresolved after retry: {len(still_unresolved)}")


if __name__ == "__main__":
    main()
