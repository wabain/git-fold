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

from .diff_parser import DiffParser
from .errors import Fatal
from .blame import run_blame
from .git import RevList, call_git
from .amend import AmendmentPlan


def suggest_basic(paths):
    cmd_base = ['diff-index', '--cached', '--find-renames', '--patch', '--no-indent-heuristic', 'HEAD']
    if paths:
        cmd_base.append('--')
        cmd_base.extend(paths)

    _, diff, _ = call_git(*cmd_base)

    # rev_list = RevList.for_range('HEAD', [])
    plan = AmendmentPlan() #(rev_list)

    for hunk in DiffParser.parse_diff_hunks(diff):
        source_range = hunk.old_range()
        for old_range, new_range in run_blame(source_range):
            new_content = hunk.new_range_content(new_range.start, new_range.extent)
            plan.amend_range(old_range, new_content)

    import pprint; pprint.pprint(plan.commits)
    plan.write_commits()
