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


if __name__ == '__main__':
    unittest.main()
