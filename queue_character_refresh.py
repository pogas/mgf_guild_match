import argparse
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    guilds = args.guilds or list(DEFAULT_GUILDS)
    snapshots = args.snapshots or list(DEFAULT_SNAPSHOTS)
    nicknames = collect_nicknames(args.reports_dir, guilds, snapshots)
    if args.limit > 0:
        nicknames = nicknames[: args.limit]

    print(f"Guilds: {', '.join(guilds)}")
    print(f"Snapshots: {', '.join(snapshots)}")
    print(f"Targets: {len(nicknames)} unique characters")

    if args.dry_run:
        for nickname in nicknames:
            print(f"DRY_RUN {nickname}")
        return

    counts = {"queued": 0, "cooldown": 0, "error": 0}
    for index, nickname in enumerate(nicknames, start=1):
        result = request_refresh(nickname, args.timeout)
        counts[result.status] = counts.get(result.status, 0) + 1
        suffix = f" queue_id={result.queue_id}" if result.queue_id else ""
        position = f" position={result.position}" if result.position is not None else ""
        print(f"[{index}/{len(nicknames)}] {result.status} {nickname}: {result.message}{suffix}{position}")
        if index < len(nicknames) and args.delay > 0:
            time.sleep(args.delay)

    print(f"Summary: queued={counts['queued']} cooldown={counts['cooldown']} error={counts['error']}")
    hard_error_rate = counts["error"] / max(1, len(nicknames))
    if counts["queued"] == 0 and counts["cooldown"] == 0:
        raise SystemExit("No refresh request was accepted or treated as cooldown.")
    if hard_error_rate > args.fail_error_rate:
        raise SystemExit(f"Hard error rate {hard_error_rate:.1%} exceeds limit {args.fail_error_rate:.1%}.")


if __name__ == "__main__":
    main()
