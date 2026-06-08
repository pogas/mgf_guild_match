import argparse
from datetime import datetime
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


API_URL = "https://mgf.gg/mgf/api/api_search_request.php"
DEFAULT_GUILDS = ("빅딜", "셀린느")
DEFAULT_SNAPSHOTS = ("snapshot.json", "training_snapshot.json")
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


def iter_snapshot_members(snapshot_path: Path) -> list[str]:
    data = json.loads(snapshot_path.read_text(encoding="utf-8"))
    names: list[str] = []
    for guild in data.get("guilds", {}).values():
        members = guild.get("members", {})
        values = members.values() if isinstance(members, dict) else members
        for member in values:
            nickname = str(member.get("nickname", "")).strip()
            if nickname:
                names.append(nickname)
    return names


def collect_nicknames(reports_dir: Path, guilds: list[str], snapshots: list[str]) -> list[str]:
    seen: set[str] = set()
    nicknames: list[str] = []
    missing: list[Path] = []

    for guild_name in guilds:
        guild_dir = reports_dir / guild_name
        for snapshot_name in snapshots:
            snapshot_path = guild_dir / snapshot_name
            if not snapshot_path.exists():
                missing.append(snapshot_path)
                continue
            for nickname in iter_snapshot_members(snapshot_path):
                if nickname in seen:
                    continue
                seen.add(nickname)
                nicknames.append(nickname)

    if missing:
        joined = "\n".join(f"- {path}" for path in missing)
        raise FileNotFoundError(f"Snapshot file not found:\n{joined}")
    return nicknames


def request_refresh(nickname: str, timeout: float) -> RefreshResult:
    body = json.dumps({"nick": nickname}, ensure_ascii=False).encode("utf-8")
    request = Request(
        API_URL,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "Origin": "https://mgf.gg",
            "Referer": f"https://mgf.gg/contents/character.php?n={quote(nickname)}",
        },
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
    except HTTPError as exc:
        if exc.code == 429:
            return RefreshResult(nickname, "rate_limited", "HTTP 429")
        return RefreshResult(nickname, "error", f"HTTP {exc.code}")
    except URLError as exc:
        return RefreshResult(nickname, "error", f"network error: {exc.reason}")
    except TimeoutError:
        return RefreshResult(nickname, "error", "timeout")

    try:
        data: dict[str, Any] = json.loads(payload)
    except json.JSONDecodeError:
        return RefreshResult(nickname, "error", "invalid JSON response")

    if data.get("success"):
        return RefreshResult(
            nickname=nickname,
            status="queued",
            message="queued",
            queue_id=str(data.get("queue_id", "")),
            position=int(data["position"]) if str(data.get("position", "")).isdigit() else None,
        )

    error = str(data.get("error", "request failed"))
    if "10분" in error or "최근" in error:
        return RefreshResult(nickname, "cooldown", error)
    return RefreshResult(nickname, "error", error)


def load_checkpoint(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Checkpoint must be a JSON object: {path}")
    return {str(key): dict(value) for key, value in data.items() if isinstance(value, dict)}


def write_checkpoint(path: Path | None, records: dict[str, dict[str, Any]]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def checkpoint_record(result: RefreshResult, attempts: int) -> dict[str, Any]:
    return {
        "status": result.status,
        "message": result.message,
        "queue_id": result.queue_id,
        "position": result.position,
        "attempts": attempts,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def request_refresh_with_retry(
    nickname: str,
    timeout: float,
    retry_rate_limit: bool,
    max_rate_limit_retries: int,
    rate_limit_wait: float,
) -> tuple[RefreshResult, int]:
    attempts = 0
    while True:
        attempts += 1
        result = request_refresh(nickname, timeout)
        if result.status != "rate_limited":
            return result, attempts
        if not retry_rate_limit or attempts > max_rate_limit_retries:
            return result, attempts
        print(
            f"RATE_LIMIT {nickname}: waiting {rate_limit_wait:.0f}s before retry "
            f"({attempts}/{max_rate_limit_retries})",
            flush=True,
        )
        time.sleep(rate_limit_wait)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Queue mgf.gg character refresh requests from latest guild snapshots.")
    parser.add_argument("--reports-dir", type=Path, default=Path("reports"), help="Report output directory.")
    parser.add_argument("--guild", action="append", dest="guilds", help="Guild report directory to scan. Repeatable.")
    parser.add_argument("--snapshot", action="append", dest="snapshots", help="Snapshot filename to scan. Repeatable.")
    parser.add_argument("--delay", type=float, default=1.25, help="Delay between requests in seconds.")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds.")
    parser.add_argument("--limit", type=int, default=0, help="Limit request count. 0 means all.")
    parser.add_argument("--dry-run", action="store_true", help="Print targets without sending refresh requests.")
    parser.add_argument("--fail-error-rate", type=float, default=0.30, help="Fail when hard error rate exceeds this value.")
    parser.add_argument("--checkpoint", type=Path, help="JSON file used to resume queued/cooldown characters.")
    parser.add_argument(
        "--retry-rate-limit",
        action="store_true",
        help="Treat HTTP 429 as a backoff signal and retry the same character.",
    )
    parser.add_argument(
        "--max-rate-limit-retries",
        type=int,
        default=12,
        help="Max HTTP 429 retries per character when --retry-rate-limit is enabled.",
    )
    parser.add_argument(
        "--rate-limit-wait",
        type=float,
        default=600.0,
        help="Seconds to wait after HTTP 429 before retrying the same character.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    guilds = args.guilds or list(DEFAULT_GUILDS)
    snapshots = args.snapshots or list(DEFAULT_SNAPSHOTS)
    nicknames = collect_nicknames(args.reports_dir, guilds, snapshots)
    if args.limit > 0:
        nicknames = nicknames[: args.limit]
    checkpoint = load_checkpoint(args.checkpoint)
    completed_statuses = {"queued", "cooldown"}

    print(f"Guilds: {', '.join(guilds)}", flush=True)
    print(f"Snapshots: {', '.join(snapshots)}", flush=True)
    print(f"Targets: {len(nicknames)} unique characters", flush=True)
    if args.checkpoint:
        completed = sum(1 for nickname in nicknames if checkpoint.get(nickname, {}).get("status") in completed_statuses)
        print(f"Checkpoint: {args.checkpoint} ({completed} already complete)", flush=True)

    if args.dry_run:
        for nickname in nicknames:
            print(f"DRY_RUN {nickname}", flush=True)
        return

    counts = {"queued": 0, "cooldown": 0, "rate_limited": 0, "error": 0, "skipped": 0}
    for index, nickname in enumerate(nicknames, start=1):
        checkpoint_status = checkpoint.get(nickname, {}).get("status")
        if checkpoint_status in completed_statuses:
            counts["skipped"] += 1
            print(f"[{index}/{len(nicknames)}] skipped {nickname}: checkpoint {checkpoint_status}", flush=True)
            continue

        result, attempts = request_refresh_with_retry(
            nickname=nickname,
            timeout=args.timeout,
            retry_rate_limit=args.retry_rate_limit,
            max_rate_limit_retries=args.max_rate_limit_retries,
            rate_limit_wait=args.rate_limit_wait,
        )
        counts[result.status] = counts.get(result.status, 0) + 1
        checkpoint[nickname] = checkpoint_record(result, attempts)
        write_checkpoint(args.checkpoint, checkpoint)
        suffix = f" queue_id={result.queue_id}" if result.queue_id else ""
        position = f" position={result.position}" if result.position is not None else ""
        attempt_text = f" attempts={attempts}" if attempts > 1 else ""
        print(f"[{index}/{len(nicknames)}] {result.status} {nickname}: {result.message}{suffix}{position}{attempt_text}", flush=True)
        if index < len(nicknames) and args.delay > 0:
            time.sleep(args.delay)

    print(
        "Summary: "
        f"queued={counts['queued']} cooldown={counts['cooldown']} "
        f"rate_limited={counts['rate_limited']} error={counts['error']} skipped={counts['skipped']}",
        flush=True,
    )
    attempted_count = max(0, len(nicknames) - counts["skipped"])
    if attempted_count == 0:
        return
    hard_error_rate = (counts["error"] + counts["rate_limited"]) / max(1, attempted_count)
    if counts["queued"] == 0 and counts["cooldown"] == 0:
        raise SystemExit("No refresh request was accepted or treated as cooldown.")
    if hard_error_rate > args.fail_error_rate:
        raise SystemExit(f"Hard error rate {hard_error_rate:.1%} exceeds limit {args.fail_error_rate:.1%}.")


if __name__ == "__main__":
    main()
