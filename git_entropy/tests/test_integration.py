import os
import shlex
from contextlib import contextmanager
from unittest import TestCase
from tempfile import TemporaryDirectory
from textwrap import dedent
from subprocess import run

from .. import suggest_basic


class SimpleIntegrationTest(TestCase):

    maxDiff = None

    def test_basic_merge_case(self):
        env_overrides = {
            'GIT_CONFIG_NOSYSTEM': '1',
            'HOME': '/var/empty/doesntexist',
            'XDG_CONFIG_HOME': '/var/empty/doesntexist',
            'GIT_AUTHOR_NAME': 'git-entropy-test',
            'GIT_AUTHOR_EMAIL': 'git-entropy-test@example.org',
            'GIT_AUTHOR_DATE': '2019-05-26 14:35:38+00:00',
            'GIT_COMMITTER_NAME': 'git-entropy-revised',
            'GIT_COMMITTER_EMAIL': 'git-entropy-revised@example.org',
            'GIT_COMMITTER_DATE': '2019-05-27 14:35:38+00:00',
        }

        part_1_v1 = ['this is a file.']
        part_1_v2 = ['This is a file.', '']

        part_2_v1 = ['hello world']
        part_2_v2 = ['Hello world.']

        part_3_v1 = ['thsi is the end.']
        part_3_v2 = ['', 'This is the end.']

        with TemporaryDirectory(prefix='git-entropy-test') as cwd, update_env(
            **env_overrides
        ):

            # Initial
            test_cmd(cwd, 'git init')
            write_lines(cwd, 'test_file', part_1_v1 + part_2_v1 + part_3_v1)
            test_cmd(cwd, 'git add test_file')
            test_cmd(cwd, 'git commit -m initial')

            # Branch A
            test_cmd(cwd, 'git checkout -b A master')
            write_lines(cwd, 'test_file', part_1_v1 + part_2_v1 + part_3_v2)
            test_cmd(cwd, 'git commit -m "variant a" test_file')

            # Branch B
            test_cmd(cwd, 'git checkout -b B master')
            write_lines(cwd, 'test_file', part_1_v2 + part_2_v1 + part_3_v1)
            test_cmd(cwd, 'git commit -m "variant b" test_file')

            # Post-merge
            test_cmd(cwd, 'git checkout master')
            test_cmd(cwd, 'git merge --no-ff --no-edit A B')

            # Staged
            write_lines(cwd, 'test_file', part_1_v2 + part_2_v2 + part_3_v2)
            test_cmd(cwd, 'git add test_file')

            old_cwd = os.getcwd()
            try:
                os.chdir(cwd)
                final = suggest_basic()
            finally:
                os.chdir(old_cwd)

            res = test_cmd(
                cwd, ['git', 'range-diff', 'HEAD...' + final], capture_output=True
            )

            expected = dedent(
                r'''
                1:  8d83d63 ! 1:  7a647a3 initial
                    @@ -8,5 +8,5 @@
                     +++ b/test_file
                     @@
                     +this is a file.
                    -+hello world
                    ++Hello world.
                     +thsi is the end.
                2:  7fceb2b ! 2:  1ea94ab variant b
                    @@ -9,5 +9,5 @@
                     -this is a file.
                     +This is a file.
                     +
                    - hello world
                    + Hello world.
                      thsi is the end.
                3:  b0f7e92 ! 3:  5967efd variant a
                    @@ -7,7 +7,7 @@
                     +++ b/test_file
                     @@
                      this is a file.
                    - hello world
                    + Hello world.
                     -thsi is the end.
                     +
                     +This is the end.
            '''
            ).lstrip()

            self.assertEqual(expected, res.stdout.decode())


@contextmanager
def update_env(**kwargs):
    old_env = dict(os.environ)
    os.environ.update(kwargs)
    try:
        yield
    finally:
        changed_keys = set(kwargs)
        old_keys = set(old_env)
        for k in changed_keys - old_keys:
            del os.environ[k]
        os.environ.update(old_env)


def test_cmd(cwd, cmd, **kwargs):
    if isinstance(cmd, str):
        # test calls don't need to worry much about input validation
        cmd = shlex.split(cmd)
    kwargs = {'check': True, **kwargs, 'cwd': cwd}
    return run(cmd, **kwargs)


def write_lines(cwd, fname, lines):
    with open(os.path.join(cwd, fname), 'w') as target_file:
        for line in lines:
            target_file.write(line)
            target_file.write('\n')
