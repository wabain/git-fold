from __future__ import annotations

from typing import List

import sys
import argparse
import asyncio
from signal import SIGINT

from . import suggest_basic
from .git import call_git, call_git_no_capture
from .errors import Fatal


def main() -> None:
    try:
        run_main()
    except Fatal as exc:
        # If the child process was interrupted, exit silently
        if exc.returncode != 128 + SIGINT:
            print(f'fatal: {exc}', file=sys.stderr)
            if exc.extended:
                print(file=sys.stderr)
                print(exc.extended, file=sys.stderr)

        sys.exit(exc.returncode)
    except KeyboardInterrupt:
        asyncio.get_event_loop().stop()
        sys.exit(128 + SIGINT)


def run_main() -> None:
    parser = argparse.ArgumentParser('git entropy')

    root_opts = parser.add_mutually_exclusive_group(required=True)
    root_opts.add_argument(
        'upstream',
        nargs='?',
        help='Root commit whose children should be considered for revision',
    )
    root_opts.add_argument(
        '--root', help='Consider all commits', required=False, action='store_true'
    )

    parser.add_argument(
        'path', nargs='*', help='Staged paths to absorb (default: all staged paths)'
    )
    parser.add_argument(
        '--no-update',
        action='store_false',
        dest='update',
        help="Write the new commits, but don't update HEAD",
    )
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        perform_rewrite(paths=args.path, root_rev=args.upstream, update=args.update)
    )


async def perform_rewrite(paths: List[str], root_rev: str, update: bool) -> None:
    old_head, new_head = await suggest_basic(paths=paths, root_rev=root_rev)

    if new_head == old_head:
        return

    # TODO: Emulate the relevant pager, colorization, display option logic here
    await call_git_no_capture('range-diff', f'{old_head}...{new_head}')
    await call_git_no_capture('diff', '--staged', new_head)

    if not update:
        return

    if input('proceed? [y/N] ').lower().strip() != 'y':
        return

    await call_git(
        'update-ref', '-m', 'entropy: absorb staged changes', 'HEAD', new_head, old_head
    )
