# git fold

A git extension for flexible, convenient change backporting.

git fold takes staged changes, finds the commits in which the affected lines
originate, and rewrites the branch, amending those commits to incorporate the
changes. This allows changes to be backported without touching the work tree and
without the possibility of merge conflicts.


## Usage

```
git fold (--root | <upstream>) [--no-update] [[--] <path>...]
```

Backport staged changes to commits between `upstream` or the root commit and
`HEAD`. If the `--no-update` flag is passed, then the proposed changes will only
be displayed; otherwise, the program will prompt for confirmation after
displaying the changes and then rewrite the commits.


## Installation

git fold requires Python 3.7 or later and has been tested with git version
2.19.0.

To use a pre-release version, install it with pip, specifying the commit or
branch. Before use, see the caveats under Roadmap below.

    pip install git+https://github.com/wabain/git-fold.git@master



## Background

The first sentence of the documentation for `git rebase` used to be notoriously
obscure:

> git-rebase - Forward-port local commits to the updated upstream head

While it's since been changed to a much more accessible description ("Reapply
commits on top of another base tip") it still masks one of the most common uses
of the command: editing commits. While fundamentally `git rebase` is designed
for forward porting, reapplying commits to a more recent upstream revision, its
facilities also encompass commit editing mid-rebase, commit reordering, and
commit message rewording. This can be difficult to get right, though, because
even the interactive git rebase is built on a batch editing interface, where the
changes to be applied are specified in a planning view and then executed, with
any merge conflicts arising from the changes needing to be addressed by the user
one by one.

When backporting a change into the earlier revision of a local branch, there are
affordances available that can't be leveraged when forward-porting local changes
onto updates from a shared branch, as in the canonical use of git rebase. In
particular, when tweaking work in progress it is  very often desirable to
backport a change *as far as possible* without creating merge conflicts, since
that point usually corresponds to the point where the relevant change was
introduced. The goal of git fold is to make that workflow as painless as
possible.

This project is inspired by Mercurial's
[absorb](https://gregoryszorc.com/blog/2018/11/05/absorbing-commit-changes-in-mercurial-4.8/)
command.


## Roadmap

git fold is ready for experimental use. At this stage, users should expect to
inspect each proposed diff before confirming the changes, and should be familiar
with using the [reflog](https://git-scm.com/docs/git-reflog) to revert unwanted
changes that are inadvertently approved.

A number of enhancements are expected before git fold will be recommended for
general use:

* More support for diffs that only add lines is needed. Currently, additions
  will only be backported if lines were removed at the same spot.

* Support an atomic backporting mode, where the staged changes are applied to
  the earliest commit that they can be applied to without conflicts.

* Handle for file-level backporting (e.g. file renaming, deletion, changes to
  the executable bit). Currently, deletion of all the contents of a file can be
  backported, but not deletion of the file itself.

* More graceful handling is needed for some pathological cases where branches
  split out and perform incompatible changes to a range of text to be amended,
  before merging back together. Such cases are expected to be rare in practice.

* The tool would benefit from a cleaner change preview UI and the ability to
  approve or disallow changes with more granularity.
