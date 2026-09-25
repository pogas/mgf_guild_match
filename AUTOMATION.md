# 리포트 자동 갱신

GitHub Actions의 기존 예약을 사용한다. 예약 실행은 GitHub 사정에 따라 지연될 수 있다.

- 00:00 KST: `queue-character-refresh.yml`에서 세 콘텐츠의 중복 없는 캐릭터 갱신 요청을 MGF에 등록한다.
- 12:05 KST: `auto-refresh-guild-reports.yml`에서 빅딜·셀린느의 대항전·수련장·토벌전 최신본과 날짜별 기록을 생성하고 커밋·푸시한다.

## 브라우저 수집

리포트 워크플로는 Playwright 1.63.0과 공식 Chromium을 설치하고, Xvfb에서 화면 모드 브라우저를 실행한다. `requests`로 받던 JavaScript 확인 페이지는 실제 브라우저에서 로드하며, 기존 길드 파서와 계산식은 그대로 사용한다.

길드명과 공개 멤버 목록(매칭 페이지는 길드 링크)이 나타난 경우에만 HTML을 받아들인다. 차단 안내, CAPTCHA, 로그인 요구 등으로 자료가 표시되지 않으면 제한 시간 후 실패 처리하며 자동으로 확인 절차를 우회하거나 제출하지 않는다. 자료 수집·검증에 실패한 리포트는 기존 파일을 유지하고, 성공한 다른 리포트만 반영한다. 실패 단계는 Actions에 실패로 남는다.

`MGF_FETCH_CACHE_DIR`는 실행 ID와 재시도 번호별로 다른 runner 임시 폴더다. 같은 실행 내에서만 URL별 HTML과 최초 수집 시각을 공유하므로 길드별 중복 조회와 최신본·기록본 간 능력치 차이를 줄인다. 이전 실행의 캐시는 재사용하지 않으며 원본 HTML과 브라우저 세션은 저장소에 커밋하지 않는다.

환경 변수:

| 변수 | 자동화 설정 | 역할 |
| --- | --- | --- |
| `MGF_FETCH_BACKEND` | `browser` | Playwright 사용. 미설정 시 기존 `requests` 사용 |
| `MGF_BROWSER_HEADED` | `1` | 화면 모드 Chromium 사용. Linux에서는 Xvfb 필요 |
| `MGF_FETCH_CACHE_DIR` | 실행별 임시 폴더 | 같은 실행에서만 공유하는 수집 캐시 |

로컬에서 사용할 때는 다음처럼 실행한다. 캐시를 지정할 경우 매번 새 폴더를 사용한다.

```sh
.venv/bin/python -m pip install playwright==1.63.0
.venv/bin/python -m playwright install --no-shell chromium
.venv/bin/python mgf_guild_export.py --guild-name 빅딜 --report-mode league --fetch-backend browser --fail-on-invalid-data --skip-tobeol
```

검증은 `test_browser_fetch.py`의 캐시·오류 처리 테스트와 `test_browser_integration.py`의 실제 Chromium 테스트를 포함한다. 브라우저 통합 테스트는 MGF 요청을 로컬 응답으로 대체해 JavaScript 렌더링과 차단 페이지 거부를 확인한다.

사이트 원본 기준일과 수집 시각은 다르다. 수집에 성공해도 MGF 원본 갱신 완료를 의미하지 않으며, 공개 멤버 수가 공식 인원보다 적은 경우 전체 길드 결과로 해석하지 않는다.
