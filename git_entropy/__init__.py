"""An "hg absorb"-style utility for git with some novel heuristics

High-level idea: given a patch and a series of prior commits, find
an assignment of amendments to the commits that yields the same output
tree while minimizing (maximizing?) the entropy of the amendment diffs.

The idea is that e.g. renamings should be backported as early as possible
and using some form of entropy metric to do so should tend to yield something
close to atomic applications of the renaming.

To have this work well, some additional plumbing will probably be needed:

* an interactive feedback mechanism (something like git add -p, maybe)
* tokenization, to have entropy computation work on units larger than
  individual characters. hooking into a word-diff regex might be good
  enough to start

Implementation
==============

For the first implementation, build on top of git blame.
"""

from __future__ import annotations

from typing import List, Optional, Tuple, Union, cast

import sys

from .diff_parser import parse_diff_hunks
from .errors import Fatal
from .git import OID, Hunk, async_call_git
from .amend import AmendmentPlan, AbstractApplyStrategy
from .apply_rewrite import GitSubprocessApplyStrategy


async def suggest_basic(
    paths: Optional[List[str]] = None, root_rev: Optional[str] = None
) -> Tuple[OID, OID]:
    head = await resolve_revision('HEAD')
    root_oid = None if root_rev is None else await resolve_revision(root_rev)

    _, diff, _ = await async_call_git(*build_initial_diff_cmd(paths))

    plan = AmendmentPlan(head=head, root=root_oid)

    for hunk in parse_diff_hunks(diff):
        await add_hunk_to_plan(hunk, plan)

    if not plan.has_amendments():
        return head, head

    # TODO: Add interactive mode

    apply_strategy = GitSubprocessApplyStrategy()
    final = await plan.write_commits(apply_strategy=apply_strategy)
    return head, final


async def add_hunk_to_plan(hunk: Hunk, plan: AmendmentPlan) -> None:
    for old_range, new_range in hunk.get_edits(old_rev=plan.head, new_rev=OID(0)):
        # Can't handle insert-only edits for now; even using a heuristic
        # like the source of the context lines, there's no guarantee that
        # intervening lines weren't added then deleted around this point.
        #
        # Need to track the lines back via diff
        if old_range is None or old_range.extent == 0:
            continue

        blame_outputs = await plan.blame_range(old_range)

        if not blame_outputs:
            continue

        if len(blame_outputs) > 1:
            # Can't handle backporting to multiple commits when there are
            # new changes to be applied
            if new_range is not None and new_range.extent > 0:
                continue

            for partial_target_range, _ in blame_outputs:
                plan.add_amended_range(partial_target_range, b'')

            continue

        target_range, _ = blame_outputs[0]

        if new_range is None:
            new_content = b''
        else:
            new_content = hunk.new_range_content(new_range.start, new_range.extent)

        await plan.add_amended_range(target_range, new_content)


def build_initial_diff_cmd(paths: Optional[List[str]]) -> List[str]:
    cmd = [
        'diff-index',
        '--cached',
        '--find-renames',
        '--patch',
        '--no-indent-heuristic',
        'HEAD',
    ]
    if paths:
        cmd.append('--')
        cmd.extend(paths)
    return cmd


async def resolve_revision(head: Union[bytes, str]) -> OID:
    try:
        _, out, _ = await async_call_git('rev-parse', '--verify', head)
    except Fatal as exc:
        raise Fatal(
            f'invalid revision {head!r}',
            returncode=exc.returncode,
            extended=exc.extended,
        )
    return OID(out.strip())
