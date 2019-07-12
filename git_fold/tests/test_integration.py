from __future__ import annotations

from typing import Any, Iterator, List, Union

import os
import shlex
import asyncio
from contextlib import contextmanager
from unittest import TestCase
from tempfile import TemporaryDirectory
from textwrap import dedent
from subprocess import run, CompletedProcess

from .. import suggest_basic


class SimpleIntegrationTest(TestCase):

    maxDiff = None

    def test_basic_merge_case(self) -> None:
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

        with TemporaryDirectory(prefix='git-fold-test') as cwd, change_dir(
            cwd
        ), update_env(**env_overrides), delete_env(['GIT_DIR', 'GIT_WORK_TREE']):

            # Initial
            test_cmd('git init')
            os.mkdir('test_dir')
            write_lines('test_dir/test_file', part_1_v1 + part_2_v1 + part_3_v1)
            test_cmd('git add test_dir/test_file')
            test_cmd('git commit -m initial')

            # Branch A
            test_cmd('git checkout -b A master')
            write_lines('test_dir/test_file', part_1_v1 + part_2_v1 + part_3_v2)
            test_cmd('git commit -m "variant a" test_dir/test_file')

            # Branch B
            test_cmd('git checkout -b B master')
            write_lines('test_dir/test_file', part_1_v2 + part_2_v1 + part_3_v1)
            test_cmd('git commit -m "variant b" test_dir/test_file')

            # Post-merge
            test_cmd('git checkout master')
            test_cmd('git merge --no-ff --no-edit A B')

            # Staged
            write_lines('test_dir/test_file', part_1_v2 + part_2_v2 + part_3_v2)
            test_cmd('git add test_dir/test_file')

            old_head, new_head = asyncio.get_event_loop().run_until_complete(
                suggest_basic()
            )

            res = test_cmd(
                ['git', 'range-diff', f'{old_head}...{new_head}'], capture_output=True
            )

            expected = dedent(
                r'''
                1:  b45bf44 ! 1:  af6289a initial
                    @@ -8,5 +8,5 @@
                     +++ b/test_dir/test_file
                     @@
                     +this is a file.
                    -+hello world
                    ++Hello world.
                     +thsi is the end.
                2:  7954059 ! 2:  324846c variant b
                    @@ -9,5 +9,5 @@
                     -this is a file.
                     +This is a file.
                     +
                    - hello world
                    + Hello world.
                      thsi is the end.
                3:  c18be95 ! 3:  c4fee68 variant a
                    @@ -7,7 +7,7 @@
                     +++ b/test_dir/test_file
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
def change_dir(cwd: str) -> Iterator[None]:
    old_cwd = os.getcwd()
    try:
        os.chdir(cwd)
        yield
    finally:
        os.chdir(old_cwd)


@contextmanager
def update_env(**kwargs: str) -> Iterator[None]:
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


@contextmanager
def delete_env(keys: List[str]) -> Iterator[None]:
    present_keys = [k for k in keys if k in os.environ]
    old = [os.environ.pop(k) for k in present_keys]
    try:
        yield
    finally:
        for key, value in zip(present_keys, old):
            os.environ[key] = value


def test_cmd(cmd: Union[str, List[str]], **kwargs: Any) -> CompletedProcess:
    if isinstance(cmd, str):
        # test calls don't need to worry much about input validation
        cmd = shlex.split(cmd)
    return run(cmd, check=True, **kwargs)


def write_lines(fname: str, lines: List[str]) -> None:
    with open(fname, 'w') as target_file:
        for line in lines:
            target_file.write(line)
            target_file.write('\n')
