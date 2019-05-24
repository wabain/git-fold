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

import sys

from .diff_parser import parse_diff_hunks
from .errors import Fatal
from .blame import run_blame
from .git import IndexedRange, RevList, call_git
from .amend import AmendmentPlan
from .apply_rewrite import DummyApplyStrategy, GitExecutableApplyStrategy


def suggest_basic(paths=None):
    head = resolve_revision('HEAD')

    cmd_base = ['diff-index', '--cached', '--find-renames', '--patch', '--no-indent-heuristic', 'HEAD']
    if paths:
        cmd_base.append('--')
        cmd_base.extend(paths)

    _, diff, _ = call_git(*cmd_base)

    plan = AmendmentPlan(head=head)

    for hunk in parse_diff_hunks(diff):
        for old_range, new_range in hunk.get_edits(old_rev=head, new_rev='0' * 40):
            # Can't handle insert-only edits for now; even using a heuristic
            # like the source of the context lines, there's no guarantee that
            # intervening lines weren't added then deleted around this point.
            #
            # Need to track the lines back via diff
            if old_range.extent == 0:
                continue

            blame_outputs = list(run_blame(old_range))

            if len(blame_outputs) > 1:
                # Can't handle backporting to multiple commits when there are
                # new changes to be applied
                if new_range.extent > 0:
                    continue

                for partial_target_range, _ in blame_outputs:
                    plan.amend_range(partial_target_range, b'')

                continue

            target_range, _ = blame_outputs[0]
            new_content = hunk.new_range_content(new_range.start, new_range.extent)
            plan.amend_range(target_range, new_content)

    if not plan.commits:
        return

    import pprint; pprint.pprint(plan.commits)
    # plan.write_commits(apply_strategy=DummyApplyStrategy()
    final = plan.write_commits(apply_strategy=GitExecutableApplyStrategy())
    return final


def resolve_revision(head):
    try:
        _, out, _ = call_git('rev-parse', '--verify', head)
    except Fatal as exc:
        raise Fatal(
            f'invalid revision {head!r}',
            returncode=exc.returncode,
            extended=exc.extended,
        )
    return out.strip().decode()
