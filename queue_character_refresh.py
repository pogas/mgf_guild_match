import argparse
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from refresh_batch_state import FileState, GitState

API_URL = "https://mgf.gg/mgf/api/api_search_request.php"
DEFAULT_GUILDS = ("빅딜", "셀린느")
DEFAULT_SNAPSHOTS = ("snapshot.json", "training_snapshot.json", "tobeol_snapshot.json")
BATCH_SIZE = 3
MIN_INTERVAL = 600
KST = timezone(timedelta(hours=9))
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
)


@dataclass
class RefreshResult:
    nickname: str
    status: str
    message: str
    queue_id: str = ""
    position: Optional[int] = None
    retry_after: float = 0.0


def iter_snapshot_members(snapshot_path: Path, guild_name: str) -> list[str]:
    data = json.loads(snapshot_path.read_text(encoding="utf-8"))
    # League and training snapshots also contain rivals. Only the directory's
    # own guild is eligible, even if another requested guild appears as a rival.
    guild = data.get("guilds", {}).get(guild_name)
    if not isinstance(guild, dict):
        raise ValueError(f"Own guild {guild_name!r} missing from {snapshot_path}")
    members = guild.get("members", {})
    values = members.values() if isinstance(members, dict) else members
    return [name for member in values if (name := str(member.get("nickname", "")).strip())]


def collect_nicknames(reports_dir: Path, guilds: list[str], snapshots: list[str]) -> list[str]:
    paths = [(reports_dir / guild / snapshot, guild) for guild in guilds for snapshot in snapshots]
    missing = [path for path, _ in paths if not path.exists()]
    if missing:
        raise FileNotFoundError("Snapshot file not found:\n" + "\n".join(f"- {path}" for path in missing))
    return list(dict.fromkeys(name for path, guild in paths for name in iter_snapshot_members(path, guild)))


def retry_after_seconds(value: str | None, now: float | None = None) -> float:
    if not value:
        return 0.0
    try:
        return max(0, int(value))
    except (ValueError, TypeError):
        try:
            return max(0, parsedate_to_datetime(value).timestamp() - (time.time() if now is None else now))
        except (ValueError, TypeError, OverflowError):
            return 0.0


def request_refresh(nickname: str, timeout: float) -> RefreshResult:
    body = json.dumps({"nick": nickname}, ensure_ascii=False).encode("utf-8")
    request = Request(API_URL, data=body, method="POST", headers={
        "Content-Type": "application/json", "Accept": "application/json",
        "User-Agent": USER_AGENT, "Origin": "https://mgf.gg",
        "Referer": f"https://mgf.gg/contents/character.php?n={quote(nickname)}",
    })
    http_status = 200
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        if exc.code == 429:
            return RefreshResult(nickname, "rate_limited", "HTTP 429",
                                 retry_after=retry_after_seconds(exc.headers.get("Retry-After") if exc.headers else None))
        http_status = exc.code
        payload = exc.read().decode("utf-8", errors="replace")
    except URLError as exc:
        return RefreshResult(nickname, "error", f"network error: {exc.reason}")
    except (TimeoutError, OSError) as exc:
        return RefreshResult(nickname, "error", f"network error: {exc}")
    try:
        data: dict[str, Any] = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("response is not an object")
    except ValueError:
        return RefreshResult(nickname, "error", f"HTTP {http_status}: invalid JSON response")
    if http_status == 200 and data.get("success"):
        return RefreshResult(nickname, "queued", "request enqueued (refresh not yet confirmed)",
                             queue_id=str(data.get("queue_id", "")),
                             position=int(data["position"]) if str(data.get("position", "")).isdigit() else None)
    error = str(data.get("error", "request failed"))[:500].replace("\n", " ")
    if "10분" in error or "최근" in error:
        return RefreshResult(nickname, "cooldown", error)
    return RefreshResult(nickname, "error", f"HTTP {http_status}: {error}")


def prepare_cycle(state, nicknames, now):
    """Finish an old cycle before rolling over; preserve pacing across midnight."""
    day = datetime.fromtimestamp(now, KST).date().isoformat()
    if state is None:
        state = {"version": 1, "cycle_day": day, "pending": list(nicknames), "records": {},
                 "not_before": 0, "rate_limit_streak": 0, "in_flight": None}
    current = set(nicknames)
    state["pending"] = [name for name in state["pending"] if name in current]
    if not state["pending"] and state["cycle_day"] < day:
        state.update(cycle_day=day, pending=list(nicknames), records={})
    else:
        known = set(state["pending"]) | state["records"].keys()
        state["pending"].extend(name for name in nicknames if name not in known)
    return state


def run_batch(store, nicknames, *, dry_run=False, timeout=20.0, delay=1.25,
              clock=time.time, sleep=time.sleep, requester=request_refresh):
    with store.lock():
        state = store.load()
        if state and state["in_flight"] is not None:
            raise ValueError("Unfinished batch claim: request outcomes may be unknown. Inspect the prior run and state branch before manual recovery; no requests sent.")
        now = clock()
        state = prepare_cycle(state, nicknames, now)
        if now < state["not_before"]:
            print(f"DEFERRED: next batch allowed at {datetime.fromtimestamp(state['not_before'], timezone.utc).isoformat()}", flush=True)
            return []
        batch = state["pending"][:BATCH_SIZE]
        if dry_run:
            for name in batch:
                print(f"DRY_RUN {name}", flush=True)
            return []
        if not batch:
            print(f"Cycle {state['cycle_day']} finished; no refresh completion is implied.", flush=True)
            return []
        # Fail closed: this claim intentionally does not expire automatically.
        # A crash or failed final save must not lose an unknown Retry-After or
        # allow another runner to repeat requests with unknown outcomes.
        state["in_flight"] = {"started_at": now, "nicknames": batch}
        store.save(state)  # CAS must succeed before the first external request.
        results = []
        for index, name in enumerate(batch):
            result = requester(name, timeout)
            finished_at = clock()
            results.append(result)
            state["not_before"] = max(state["not_before"], finished_at + MIN_INTERVAL)
            state["records"][name] = {**vars(result), "attempted_at": finished_at}
            print(f"[{index + 1}/{len(batch)}] {result.status} {name}: {result.message} queue_id={result.queue_id}", flush=True)
            if result.status == "rate_limited":
                state["rate_limit_streak"] += 1
                backoff = min(21600, MIN_INTERVAL * 2 ** min(state["rate_limit_streak"] - 1, 6))
                state["not_before"] = max(state["not_before"], finished_at + backoff, finished_at + result.retry_after)
                break  # Keep this character pending; no in-run retries or long sleep.
            state["rate_limit_streak"] = 0
            state["pending"].remove(name)  # Includes HTTP 400/errors: never starve later members.
            if index + 1 < len(batch) and delay > 0:
                sleep(delay)
        state["in_flight"] = None
        store.save(state)
        print(f"Batch: attempted={len(results)} queued={sum(r.status == 'queued' for r in results)}; remaining={len(state['pending'])}. Queued means accepted, not refreshed.", flush=True)
        return results


def main():
    parser = argparse.ArgumentParser(description="Queue at most 3 own-guild characters; persist progress between runs.")
    parser.add_argument("--reports-dir", type=Path, default=Path("reports"))
    parser.add_argument("--guild", action="append", dest="guilds")
    parser.add_argument("--snapshot", action="append", dest="snapshots")
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--delay", type=float, default=1.25, help="Short delay inside the 3-character batch.")
    parser.add_argument("--dry-run", action="store_true")
    stores = parser.add_mutually_exclusive_group(required=True)
    stores.add_argument("--checkpoint", type=Path, help="Local JSON state; protected by a file lock.")
    stores.add_argument("--state-branch", help="Durable state-only branch in origin; uses a compare-and-swap push.")
    parser.add_argument("--initialize-state", action="store_true", help="Explicitly allow first creation of a missing state branch.")
    args = parser.parse_args()
    if not 0 < args.timeout <= 60 or not 0 <= args.delay <= 10:
        parser.error("timeout must be in (0, 60] and delay in [0, 10]")
    guilds = args.guilds or list(DEFAULT_GUILDS)
    if set(guilds) - set(DEFAULT_GUILDS):
        parser.error("Only 빅딜 and 셀린느 are authorized refresh targets.")
    names = collect_nicknames(args.reports_dir, guilds, args.snapshots or list(DEFAULT_SNAPSHOTS))
    print(f"Guilds: {', '.join(guilds)}; targets: {len(names)}; batch: at most {BATCH_SIZE}", flush=True)
    store = GitState(args.state_branch, initialize=args.initialize_state) if args.state_branch else FileState(args.checkpoint)
    results = run_batch(store, names, dry_run=args.dry_run, timeout=args.timeout, delay=args.delay)
    if any(result.status == "error" for result in results):
        raise SystemExit("One or more requests failed; results and progress were saved. Later characters continue next run.")


if __name__ == "__main__":
    main()
