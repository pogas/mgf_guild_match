import json
from pathlib import Path
import tempfile
import unittest

import queue_character_refresh as refresh


class RefreshTargetTests(unittest.TestCase):
    def test_default_targets_include_tobeol_only_members_and_deduplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for guild in refresh.DEFAULT_GUILDS:
                directory = root / guild
                directory.mkdir()
                sources = {
                    'snapshot.json': {'a': {'nickname': '공통'}, 'b': {'nickname': '대항전 전용'}},
                    'training_snapshot.json': [{'nickname': '공통'}, {'nickname': '수련장 전용'}],
                    'tobeol_snapshot.json': {'a': {'nickname': '공통'}, 'b': {'nickname': f'{guild} 토벌전 전용'}},
                }
                for name, members in sources.items():
                    (directory / name).write_text(json.dumps({'guilds': {guild: {'members': members}}}))
            names = refresh.collect_nicknames(root, list(refresh.DEFAULT_GUILDS), list(refresh.DEFAULT_SNAPSHOTS))
            self.assertCountEqual(names, ['공통', '대항전 전용', '수련장 전용', '빅딜 토벌전 전용', '셀린느 토벌전 전용'])

    def test_missing_tobeol_snapshot_is_not_silently_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            directory = root / '길드'
            directory.mkdir()
            for name in ('snapshot.json', 'training_snapshot.json'):
                (directory / name).write_text('{"guilds": {}}')
            with self.assertRaisesRegex(FileNotFoundError, 'tobeol_snapshot.json'):
                refresh.collect_nicknames(root, ['길드'], list(refresh.DEFAULT_SNAPSHOTS))



from copy import deepcopy
from datetime import datetime, timezone
from email.message import Message
from io import BytesIO
from unittest.mock import patch
from urllib.error import HTTPError, URLError
import subprocess

from refresh_batch_state import FileState, GitState, validate_state


class MemoryState:
    def __init__(self, state=None, fail_save=0):
        self.state = deepcopy(state)
        self.saves = 0
        self.fail_save = fail_save

    def lock(self):
        from contextlib import nullcontext
        return nullcontext()

    def load(self):
        return deepcopy(self.state)

    def save(self, state):
        self.saves += 1
        if self.saves == self.fail_save:
            raise RuntimeError('state save failed')
        self.state = deepcopy(state)


class Clock:
    def __init__(self, now=1792972800):
        self.now = now

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryState()
        self.clock = Clock()
        self.names = ['one', 'two', 'three', 'four', 'five', 'six']
        self.calls = []

    def requester(self, name, timeout):
        self.assertIsNotNone(self.store.state['in_flight'], 'claim must be persisted before POST')
        self.calls.append(name)
        self.clock.now += 2
        return refresh.RefreshResult(name, 'queued', 'accepted')

    def run_batch(self, **kwargs):
        return refresh.run_batch(self.store, self.names, clock=self.clock, sleep=self.clock.sleep,
                                 requester=kwargs.pop('requester', self.requester), **kwargs)

    def test_three_attempts_and_at_least_ten_minutes_after_last_response(self):
        self.assertEqual(len(self.run_batch()), 3)
        self.assertEqual(self.calls, self.names[:3])
        allowed = self.clock.now + 600
        self.assertEqual(self.store.state['not_before'], allowed)
        self.clock.now = allowed - 0.001
        self.assertEqual(self.run_batch(), [])
        self.assertEqual(len(self.calls), 3)
        self.clock.now = allowed
        self.run_batch()
        self.assertEqual(self.calls, self.names)

    def test_no_restart_same_day_but_new_day_cycles_again(self):
        self.run_batch()
        self.clock.now = self.store.state['not_before']
        self.run_batch()
        self.clock.now = self.store.state['not_before']
        self.assertEqual(self.run_batch(), [])
        self.clock.now += 86400
        self.run_batch()
        self.assertEqual(self.calls[-3:], self.names[:3])

    def test_midnight_does_not_reset_incomplete_cycle_or_cooldown(self):
        self.clock.now = datetime(2026, 10, 25, 14, 59, tzinfo=timezone.utc).timestamp()
        self.run_batch()
        cycle_day = self.store.state['cycle_day']
        self.clock.now = self.store.state['not_before'] - 1
        self.assertEqual(self.run_batch(), [])
        self.clock.now += 1
        self.run_batch()
        self.assertEqual(self.calls, self.names)
        self.assertEqual(self.store.state['cycle_day'], cycle_day)

    def test_roster_reorder_departures_and_additions_preserve_progress(self):
        self.run_batch()
        self.names = ['seven', 'six', 'five', 'three', 'two', 'one']
        self.clock.now = self.store.state['not_before']
        self.run_batch()
        self.assertEqual(self.calls[-3:], ['five', 'six', 'seven'])

    def test_limit_applies_after_completed_members(self):
        self.run_batch()
        self.clock.now = self.store.state['not_before']
        results = self.run_batch()
        self.assertEqual([r.nickname for r in results], self.names[3:])

    def test_400_advances_and_all_attempts_count_toward_three(self):
        def request(name, timeout):
            self.calls.append(name)
            return refresh.RefreshResult(name, 'error' if name == 'one' else 'queued', 'HTTP 400')
        self.run_batch(requester=request)
        self.assertEqual(self.calls, self.names[:3])
        self.assertEqual(self.store.state['pending'], self.names[3:])
        self.assertEqual(self.store.state['records']['one']['status'], 'error')

    def test_429_stops_immediately_and_keeps_same_member_with_retry_after(self):
        def request(name, timeout):
            self.calls.append(name)
            return refresh.RefreshResult(name, 'rate_limited', 'HTTP 429', retry_after=7200)
        self.run_batch(requester=request)
        self.assertEqual(self.calls, ['one'])
        self.assertEqual(self.store.state['pending'], self.names)
        self.assertEqual(self.store.state['not_before'], self.clock.now + 7200)
        self.assertIsNone(self.store.state['in_flight'])

    def test_429_on_third_retains_only_third_and_later_members(self):
        def request(name, timeout):
            return refresh.RefreshResult(name, 'rate_limited' if name == 'three' else 'queued', 'result')
        self.run_batch(requester=request)
        self.assertEqual(self.store.state['pending'], self.names[2:])

    def test_repeated_429_backs_off_without_sleeping_in_run(self):
        request = lambda name, timeout: refresh.RefreshResult(name, 'rate_limited', 'HTTP 429')
        for wait in [600, 1200, 2400, 4800, 9600, 19200, 21600, 21600]:
            before = self.clock.now
            self.run_batch(requester=request)
            self.assertEqual(self.store.state['not_before'], before + wait)
            self.assertEqual(self.clock.now, before)
            self.clock.now = self.store.state['not_before']

    def test_dry_run_never_sends_or_saves(self):
        self.run_batch(dry_run=True)
        self.assertEqual(self.calls, [])
        self.assertIsNone(self.store.state)
        self.assertEqual(self.store.saves, 0)
        self.run_batch()
        saved = deepcopy(self.store.state)
        self.clock.now = self.store.state['not_before']
        self.run_batch(dry_run=True)
        self.assertEqual(self.store.state, saved)

    def test_claim_save_failure_sends_nothing(self):
        self.store.fail_save = 1
        with self.assertRaisesRegex(RuntimeError, 'save failed'):
            self.run_batch()
        self.assertEqual(self.calls, [])

    def test_final_save_failure_keeps_blocking_claim_even_next_day(self):
        self.store.fail_save = 2
        with self.assertRaisesRegex(RuntimeError, 'save failed'):
            self.run_batch()
        self.assertIsNotNone(self.store.state['in_flight'])
        self.clock.now += 2 * 86400
        with self.assertRaisesRegex(ValueError, 'Unfinished batch'):
            self.run_batch()
        self.assertEqual(len(self.calls), 3)

    def test_crash_after_claim_blocks_automatic_retry(self):
        def crash(name, timeout):
            raise TimeoutError('unknown outcome')
        with self.assertRaises(TimeoutError):
            self.run_batch(requester=crash)
        self.assertIsNotNone(self.store.state['in_flight'])
        with self.assertRaisesRegex(ValueError, 'Unfinished batch'):
            self.run_batch()
        self.assertEqual(self.calls, [])

    def test_cooldown_is_not_recorded_as_queued_and_advances(self):
        self.run_batch(requester=lambda name, timeout: refresh.RefreshResult(name, 'cooldown', '최근 요청'))
        self.assertEqual(self.store.state['records']['one']['status'], 'cooldown')
        self.assertEqual(self.store.state['pending'], self.names[3:])


class RequestTests(unittest.TestCase):
    def response(self, data):
        return BytesIO(json.dumps(data).encode())

    def test_queued_only_means_enqueued_and_encodes_nickname(self):
        with patch.object(refresh, 'urlopen', return_value=self.response({'success': True, 'queue_id': '123', 'position': 4})) as urlopen:
            result = refresh.request_refresh('테스트', 20)
        self.assertEqual(result.status, 'queued')
        self.assertIn('not yet confirmed', result.message)
        self.assertEqual(result.queue_id, '123')
        self.assertEqual(result.position, 4)
        req = urlopen.call_args.args[0]
        self.assertEqual(json.loads(req.data), {'nick': '테스트'})
        self.assertEqual(req.method, 'POST')

    def test_429_retry_after_seconds(self):
        headers = Message()
        headers['Retry-After'] = '1200'
        error = HTTPError(refresh.API_URL, 429, 'Limited', headers, BytesIO(b''))
        with patch.object(refresh, 'urlopen', side_effect=error):
            result = refresh.request_refresh('one', 20)
        self.assertEqual(result.status, 'rate_limited')
        self.assertEqual(result.retry_after, 1200)

    def test_retry_after_http_date_invalid_and_past(self):
        now = datetime(2026, 10, 26, tzinfo=timezone.utc).timestamp()
        self.assertEqual(refresh.retry_after_seconds('Mon, 26 Oct 2026 00:30:00 GMT', now), 1800)
        self.assertEqual(refresh.retry_after_seconds('Mon, 25 Oct 2026 00:30:00 GMT', now), 0)
        for value in ['oops', '-1', None, 'nan']:
            self.assertEqual(refresh.retry_after_seconds(value, now), 0)

    def test_400_logs_body_and_does_not_crash(self):
        error = HTTPError(refresh.API_URL, 400, 'Bad Request', Message(), self.response({'error': 'invalid nickname'}))
        with patch.object(refresh, 'urlopen', side_effect=error):
            result = refresh.request_refresh('one', 20)
        self.assertEqual(result.status, 'error')
        self.assertEqual(result.message, 'HTTP 400: invalid nickname')

    def test_400_with_cooldown_body_is_not_a_hard_error(self):
        error = HTTPError(refresh.API_URL, 400, 'Bad Request', Message(), self.response({'error': '최근 10분 내 요청'}))
        with patch.object(refresh, 'urlopen', side_effect=error):
            self.assertEqual(refresh.request_refresh('one', 20).status, 'cooldown')

    def test_invalid_json_shape_and_network_errors(self):
        for payload in [[], None, 'not an object']:
            with patch.object(refresh, 'urlopen', return_value=self.response(payload)):
                self.assertEqual(refresh.request_refresh('one', 20).status, 'error')
        with patch.object(refresh, 'urlopen', return_value=BytesIO(b'not json')):
            self.assertEqual(refresh.request_refresh('one', 20).status, 'error')
        for error in [TimeoutError(), URLError('offline')]:
            with patch.object(refresh, 'urlopen', side_effect=error):
                self.assertEqual(refresh.request_refresh('one', 20).status, 'error')


class StateStoreTests(unittest.TestCase):
    def state(self):
        return refresh.prepare_cycle(None, ['one', 'two'], 1792972800)

    def test_code_branch_cannot_be_used_as_state(self):
        with self.assertRaisesRegex(ValueError, 'dedicated'):
            GitState(branch='main')

    def test_nonfinite_timestamp_and_invalid_day_rejected(self):
        for timestamp in [float('nan'), float('inf'), -1, True]:
            state = self.state()
            state['not_before'] = timestamp
            with self.assertRaises(ValueError):
                validate_state(state)
        state = self.state()
        state['cycle_day'] = 'not-a-date'
        with self.assertRaises(ValueError):
            validate_state(state)

    def test_file_round_trip_and_legacy_or_corrupt_state_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FileState(Path(tmp) / 'checkpoint.json')
            self.assertIsNone(store.load())
            with store.lock():
                store.save(self.state())
                self.assertEqual(store.load(), self.state())
            for bad in ['not json', '{}', '{"one": {"status":"queued"}}', '{"version": 2}']:
                store.path.write_text(bad)
                with self.assertRaises(ValueError):
                    store.load()

    def test_git_state_cas_blocks_racing_claim_and_branch_contains_only_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            remote = root / 'remote.git'
            work = root / 'work'
            subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
            subprocess.run(['git', 'init', str(work)], check=True, capture_output=True)
            def git(*args, input=None):
                return subprocess.run(['git', '-C', str(work), *args], input=input, text=True, check=True,
                                      capture_output=True).stdout.strip()
            git('remote', 'add', 'origin', str(remote))
            one = GitState(initialize=True)
            two = GitState(initialize=True)
            one.git = two.git = git
            self.assertIsNone(one.load())
            self.assertIsNone(two.load())
            state = self.state()
            state['in_flight'] = {'nicknames': ['one'], 'started_at': 1792972800}
            one.save(state)
            with self.assertRaises(subprocess.CalledProcessError):
                two.save(state)
            restored = GitState()
            restored.git = git
            self.assertEqual(restored.load(), state)
            self.assertEqual(git('ls-tree', '--name-only', restored.sha), 'state.json')
            self.assertEqual(len(git('rev-list', '--parents', '-n', '1', restored.sha).split()), 1)
            state['in_flight'] = None
            restored.save(state)
            self.assertEqual(one.load(), state)
            git('push', 'origin', '--delete', 'character-refresh-state')
            with self.assertRaisesRegex(ValueError, 'branch missing'):
                restored.load()


class TargetSafetyTests(unittest.TestCase):
    def test_all_mode_opponents_excluded_even_if_they_are_other_selected_guild(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for guild in refresh.DEFAULT_GUILDS:
                (root / guild).mkdir()
                other = next(g for g in refresh.DEFAULT_GUILDS if g != guild)
                for snapshot in refresh.DEFAULT_SNAPSHOTS:
                    data = {'guilds': {guild: {'members': [{'nickname': f'{guild}-own'}]},
                                       '상대': {'members': [{'nickname': 'rival'}]},
                                       other: {'members': [{'nickname': 'stale-other'}]}}}
                    (root / guild / snapshot).write_text(json.dumps(data))
            names = refresh.collect_nicknames(root, list(refresh.DEFAULT_GUILDS), list(refresh.DEFAULT_SNAPSHOTS))
            self.assertEqual(names, ['빅딜-own', '셀린느-own'])

    def test_missing_own_guild_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'snapshot.json'
            path.write_text('{"guilds":{"enemy":{"members":[]}}}')
            with self.assertRaisesRegex(ValueError, 'Own guild'):
                refresh.iter_snapshot_members(path, '빅딜')


if __name__ == '__main__':
    unittest.main()
