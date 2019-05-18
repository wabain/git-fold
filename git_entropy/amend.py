"""Utilities for in-place git history amendments

This interface combines the functionality of git amend and git rebase,
creating revisions to the log without affecting the working directory.
"""

import os
import bisect
import tempfile
from itertools import chain
from collections import namedtuple

from .git import (
    RevList,
    ls_tree,
    mk_tree,
    TreeListingEntry,
    cat_commit,
    call_git,
    GitCall,
)


AmendmentRecord = namedtuple('AmendmentRecord', 'start,extent,replacement')


class AmendmentPlan:
    def __init__(self):#, rev_list):
        # self.rev_list = rev_list
        self.commits = {}

    def amend_range(self, indexed_range, new_lines):
        for_commit = self._for_commit(indexed_range.rev)
        try:
            for_blob = for_commit[indexed_range.file]
        except KeyError:
            for_blob = AmendedBlob(indexed_range.rev, indexed_range.file, indexed_range.oid())
            for_commit[indexed_range.file] = for_blob
        for_blob.replace_lines(indexed_range.start, indexed_range.extent, new_lines)

    def _for_commit(self, sha):
        try:
            int(sha, 16)
        except ValueError:
            raise ValueError(f'Expected commit SHA, got {sha!r}')

        try:
            return self.commits[sha]
        except KeyError:
            out = self.commits[sha] = {}
            return out

    def write_commits(self):
        # For now, only support changing a blob if that file is not
        # modified by later commits
        changed_files = {}

        mapped_commits = {}
        mapped_trees = {}

        ordered_roots = RevList.for_range(list(self.commits), reverse=True, walk=False).revs

        for root in ordered_roots:
            # Already taken care of
            if root in mapped_commits:
                continue

            rev_list = RevList.for_range([f'{root}..HEAD'], reverse=True)

            for oid in chain([root], rev_list.revs):
                if oid in mapped_commits:
                    continue

                commit_info = cat_commit(oid)

                to_amend = None
                try:
                    to_amend = self.commits[oid]
                except KeyError:
                    if not any(p in mapped_commits for p in commit_info.parents):
                        continue

                if b'\n' in commit_info.message:
                    summary = commit_info.message[:commit_info.message.index(b'\n')].decode(errors='replace')
                else:
                    summary = commit_info.message.decode(errors='replace')
                print('rewrite', commit_info.oid[:6], summary)

                new_changed_files = {}

                if to_amend is not None:
                    for blob in to_amend.values():
                        if blob.file in changed_files:
                            print('multiple modifications...')
                            # raise NotImplementedError(f'multiple modifications to {blob.file}')

                        new_changed_files[blob.file] = (blob, blob.create_object())

                new_commit = self._amend_commit(
                    commit_info,
                    new_changed_files,
                    changed_files,
                    mapped_commits,
                    mapped_trees,
                )
                if new_commit != oid:
                    mapped_commits[oid] = new_commit
                    print('    =>', new_commit[:6])

                changed_files.update(new_changed_files)

    def _amend_commit(self,
                      commit_info,
                      new_changed_files,
                      changed_files,
                      mapped_commits,
                      mapped_trees):

        tree = self._create_amended_tree(commit_info,
                                         new_changed_files,
                                         changed_files,
                                         mapped_commits,
                                         mapped_trees)

        return self._write_commit_tree(commit_info, tree, mapped_commits)

    def _create_amended_tree(self,
                             commit_info,
                             new_changed_files,
                             changed_files,
                             parent_mapped_commits,
                             mapped_trees):

        changed_trees = {git_dirname(path) for path in new_changed_files}
        changed_trees.update(git_dirname(path) for path in changed_files)
        for path in list(changed_trees):
            while True:
                path = git_dirname(path)
                changed_trees.add(path)
                if path == '.':
                    break

        root_oid = commit_info.tree_oid

        for path in reversed(sorted(changed_trees)):
            if path == '.':
                tree_oid = commit_info.tree_oid
            else:
                self_listing = list(ls_tree('-d', commit_info.oid, path))
                if len(self_listing) != 1 or self_listing[0].obj_type != 'tree':
                    raise ValueError(f'unsupported directory listing {self_listing}')
                tree_oid = self_listing[0].oid

            if tree_oid in mapped_trees:
                new_tree_oid = mapped_trees[tree_oid]
            else:
                updated_entries = []

                for entry in ls_tree(tree_oid):
                    child_path = entry.path if path == '.' else f'{path}/{child_path}'
                    child_oid = entry.oid
                    if entry.obj_type == 'tree':
                        if child_oid in mapped_trees:
                            child_oid = mapped_trees[child_oid]
                    elif child_path in changed_files:
                        _, child_oid = changed_files[child_path]
                    elif child_path in new_changed_files:
                        _, child_oid = new_changed_files[child_path]

                    updated_entries.append(TreeListingEntry(
                        entry.mode, entry.obj_type, child_oid, entry.path,
                    ))

                new_tree_oid = mk_tree(updated_entries)
                mapped_trees[tree_oid] = new_tree_oid

            if path == '.':
                root_oid = new_tree_oid

        assert root_oid is not None
        return root_oid

    def _write_commit_tree(self, commit_info, tree, mapped_commits):
        parents = []
        for parent in commit_info.parents:
            parents.append('-p')
            parents.append(mapped_commits.get(parent, parent))

        _, out, _ = call_git(
            'commit-tree',
            tree,
            *parents,
            input=commit_info.message,
            env=dict(
                GIT_AUTHOR_NAME=commit_info.a_name,
                GIT_AUTHOR_EMAIL=commit_info.a_email,
                GIT_AUTHOR_DATE=commit_info.a_date,
            ),
        )
        return out.decode().strip()


class AmendedBlob:
    def __init__(self, commit, file, oid):
        self.commit = commit
        self.file = file
        self.oid = oid
        self.amendments = []

    def replace_lines(self, start, extent, new_lines):
        record = AmendmentRecord(start, extent, new_lines)
        index = bisect.bisect_left(self.amendments, record)

        if index > 0:
            prior_start, prior_extent, _ = self.amendments[index - 1]
            if prior_start + prior_extent > start:
                raise ValueError('overlapping amendments requested')

        if index + 1 < len(self.amendments):
            next_start, _, _ = self.amendments[index + 1]
            if start + extent > next_start:
                raise ValueError('overlapping amendments requested')

        self.amendments.insert(index, record)

    def write(self, output):
        # TODO: Stream instead of buffering in memory
        _, out, _ = call_git('cat-file', '-p', f'{self.commit}:{self.file}')

        amend_iter = iter(self.amendments)
        amend = next(amend_iter, None)

        #import pdb; pdb.set_trace()

        for lineno, line in enumerate(out.splitlines(keepends=True), start=1):
            if amend and lineno > amend.start + amend.extent:
                amend = next(amend_iter, None)

            if amend:
                if lineno == amend.start:
                    output.write(amend.replacement)

                if amend.start <= lineno < amend.start + amend.extent:
                    continue

            output.write(line)

    def create_object(self):
        res = GitCall()
        cmd = ['hash-object', '-tblob', '--stdin', '-w']
        with tempfile.TemporaryFile() as pipe, \
                res.call_async(*cmd, stdin=pipe):
            self.write(pipe)
        return res.stdout.decode().strip()


def git_dirname(path):
    path = os.path.dirname(path)
    if path == '':
        return '.'
    return path