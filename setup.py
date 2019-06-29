from __future__ import annotations

import os
from subprocess import run
from typing import List
from distutils.errors import DistutilsError
from setuptools import Command, setup, find_packages


class CheckCommand(Command):  # type: ignore
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        here = os.path.dirname(os.path.abspath(__file__))

        res = run(['mypy', '--strict', '--ignore-missing-imports', '.'], cwd=here)
        if res.returncode != 0:
            msg = f'Typecheck failed (exit code {res.returncode})'
            self.announce(msg)
            raise DistutilsError(msg)


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
            'mypy==0.711',
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
    }
)
