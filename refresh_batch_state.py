"""Durable batch state. Git saves are compare-and-swap; no report branch is changed."""
from contextlib import contextmanager
import fcntl
from datetime import date
import math
import json
from pathlib import Path
import subprocess
import uuid


def validate_state(state):
    if not isinstance(state, dict) or state.get('version') != 1:
        raise ValueError('Unsupported or malformed refresh state; refusing to reset it.')
    if (not isinstance(state.get('pending'), list)
            or not all(isinstance(name, str) for name in state['pending'])
            or len(state['pending']) != len(set(state['pending']))
            or not isinstance(state.get('records'), dict)
            or not isinstance(state.get('cycle_day'), str)
            or type(state.get('not_before')) not in (int, float)
            or not math.isfinite(state['not_before'])
            or state['not_before'] < 0
            or not isinstance(state.get('rate_limit_streak'), int)
            or state['rate_limit_streak'] < 0
            or state.get('in_flight') is not None and not isinstance(state['in_flight'], dict)):
        raise ValueError('Malformed refresh state; refusing to send requests.')
    if date.fromisoformat(state['cycle_day']).isoformat() != state['cycle_day']:
        raise ValueError('Cycle day must use YYYY-MM-DD.')
    return state


class FileState:
    def __init__(self, path):
        self.path = Path(path)

    @contextmanager
    def lock(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.with_suffix(self.path.suffix + '.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def load(self):
        if not self.path.exists():
            return None
        return validate_state(json.loads(self.path.read_text(encoding='utf-8')))

    def save(self, state):
        validate_state(state)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + '.tmp')
        temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        temporary.replace(self.path)


class GitState:
    """An orphan state-only branch, with a lease on its exact previously read SHA."""
    def __init__(self, branch='character-refresh-state', remote='origin', initialize=False):
        if branch != 'character-refresh-state':
            raise ValueError('Only the dedicated character-refresh-state branch may be written.')
        self.ref = f'refs/heads/{branch}'
        self.remote = remote
        self.initialize = initialize
        self.sha = ''
        self.git('check-ref-format', self.ref)

    def git(self, *args, input=None):
        return subprocess.run(['git', *args], input=input, text=True, check=True,
                              capture_output=True, timeout=90).stdout.strip()

    @contextmanager
    def lock(self):
        # Workflow concurrency serializes normal runs; the push lease also rejects
        # independently racing processes before either can send a second batch.
        yield

    def load(self):
        refs = self.git('ls-remote', '--refs', self.remote, self.ref)
        if not refs:
            if self.initialize:
                return None
            raise ValueError('State branch missing. Initialize it explicitly; refusing to restart a lost cycle.')
        self.git('fetch', '--no-tags', self.remote, self.ref)
        self.sha = self.git('rev-parse', 'FETCH_HEAD')
        return validate_state(json.loads(self.git('show', f'{self.sha}:state.json')))

    def save(self, state):
        validate_state(state)
        content = json.dumps(state, ensure_ascii=False, indent=2) + '\n'
        blob = self.git('hash-object', '-w', '--stdin', input=content)
        tree = self.git('mktree', input=f'100644 blob {blob}\tstate.json\n')
        parents = ['-p', self.sha] if self.sha else []
        commit = self.git('-c', 'user.name=github-actions[bot]', '-c',
                          'user.email=41898282+github-actions[bot]@users.noreply.github.com',
                          'commit-tree', tree, *parents,
                          # Unique commits avoid git treating an identical racing save as
                          # 'up to date' without checking its now-stale lease.
                          input=f'chore: 캐릭터 갱신 배치 진행 상태 저장\n\nOperation: {uuid.uuid4().hex}\n')
        self.git('push', f'--force-with-lease={self.ref}:{self.sha}',
                 self.remote, f'{commit}:{self.ref}')
        self.sha = commit
