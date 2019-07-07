from __future__ import annotations

import os
from subprocess import run
from typing import List, NamedTuple, Optional, Union
from distutils.errors import DistutilsError
from distutils import log
from setuptools import Command, setup, find_packages


#
# The `check` subcommand
#


class Check(NamedTuple):
    cmd: Union[str, List[str]]
    shell: bool = False

    def display_cmd(self) -> str:
        return self.cmd if isinstance(self.cmd, str) else ' '.join(self.cmd)


class CheckCommand(Command):  # type: ignore
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        here = os.path.dirname(os.path.abspath(__file__))

        checks: List[Union[Check, str]] = [
            # fmt: off

            'coverage',

            # Specify files and directories directly because black's file ignoring
            # will look at the absolute path:
            # https://github.com/python/black/issues/712
            Check('black --check --diff *.py git_entropy', shell=True),

            Check(['mypy', '--strict', '--ignore-missing-imports', '.']),

            Check('pylint *.py git_entropy', shell=True),
        ]

        return_codes: List[Union[bool, int]] = []

        for check_count, check in enumerate(checks, start=1):
            self.announce(
                f'\nCheck {check_count}: {display_check(check)}', level=log.INFO
            )

            if isinstance(check, str):
                success = True
                try:
                    self.run_command(check)
                except DistutilsError:
                    success = False

                return_codes.append(success)
            else:
                res = run(check.cmd, shell=check.shell, cwd=here)

                self.announce(f'Return code: {res.returncode}', level=log.DEBUG)
                return_codes.append(res.returncode)

        self.announce('\nResults:', level=log.INFO)

        for check_count, (check, retcode) in enumerate(
            zip(checks, return_codes), start=1
        ):
            res_out = (
                '    ' if isinstance(retcode, bool) or retcode == 0 else f'({retcode})'
            )
            self.announce(
                f'{get_mark(retcode)} {res_out: <4}{check_count}: {display_check(check)}',
                level=log.INFO,
            )

        if not all(retcode_success(retcode) for retcode in return_codes):
            raise DistutilsError('Some checks unsuccessful')


def display_check(check: Union[Check, str]) -> str:
    if isinstance(check, str):
        return f'setup.py {check}'

    return check.display_cmd()


def retcode_success(retcode: Union[bool, int]) -> bool:
    if isinstance(retcode, bool):
        return retcode

    return retcode == 0


def get_mark(retcode: Union[bool, int]) -> str:
    return '✨' if retcode_success(retcode) else '❌'


#
# The `coverage` subcommand
#


class CoverageCommand(Command):  # type: ignore
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        from coverage import Coverage

        cov = Coverage()

        failed_message: Optional[str] = None
        cov.start()
        try:
            self.run_command('test')
        except DistutilsError as exc:
            failed_message = str(exc)
        finally:
            cov.save()

        cov.report()
        cov.html_report()

        if failed_message:
            raise DistutilsError(f'tests failed: {failed_message}')


#
# Package declaration
#

with open('README.md') as f:
    LONG_DESCRIPTION = f.read()

setup(
    # fmt: off
    name='git_entropy',
    version='0.0.1',

    description='A git extention for flexible, convenient change backporting',
    long_description=LONG_DESCRIPTION,

    url='https://github.com/wabain/git-entropy.git',

    author='William Bain',
    author_email='bain.william.a@gmail.com',

    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Version Control :: Git',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='git rebase git-rebase vcs version-control',

    packages=find_packages(exclude=['tests*']),

    install_requires=[],

    extras_require={
        'dev': [
            'black>=19.2<20',
            'mypy==0.711',
            'pylint>=2.3,<3',
        ],
        'test': [
            'coverage>=4.5,<5',
        ],
    },

    entry_points={
        'console_scripts': [
            'git-entropy = git_entropy.__main__:main',
        ],
    },

    cmdclass={
        'check': CheckCommand,
        'coverage': CoverageCommand,
    }
)
