"""Utilities for in-place git history amendments

This interface combines the functionality of git amend and git rebase,
creating revisions to the log without affecting the working directory.
"""

import bisect
from itertools import chain
from collections import namedtuple

from .git import ls_tree, cat_commit, call_git
from .log import CommitGraph
from .errors import Fatal
from .diff_parser import parse_diff_hunks, parse_diff_tree_summary


AmendmentRecord = namedtuple('AmendmentRecord', 'start,extent,replacement')


class AmendmentPlan:
    def __init__(self, head):
        self.head = head
        self.commits = {}

    def amend_range(self, indexed_range, new_lines):
        for_commit = self._for_commit(indexed_range.rev)
        try:
            for_blob = for_commit[indexed_range.file]
        except KeyError:
            for_blob = AmendedBlob(
                indexed_range.rev, indexed_range.file, indexed_range.oid()
            )
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

    def write_commits(self, apply_strategy):
        commit_graph = CommitGraph.build_partial(
            head=self.head, roots=list(self.commits)
        )

        # Map { old_commit: (new_commit, { path: (amended_blob, new_oid) } }
        amended_commits = {}

        for to_rewrite in commit_graph.reverse_topo_ordering(self.head):
            # FIXME: Might need a more scaleable way to do this? (e.g. batch)
            commit_info = cat_commit(to_rewrite)
            print('handling', commit_info.oneline())

            parents = commit_graph.get_parents(to_rewrite)
            new_parents, parent_amendments = self._get_amended_parents(
                parents, amended_commits
            )
            new_amendments = self.commits.get(to_rewrite)
            assert new_parents != parents or new_amendments is not None

            coalesced = self._coalesce_amended_blobs(
                commit_info, new_amendments or {}, parent_amendments
            )
            new_commit_oid, blobs_with_amendments = self._rewrite_commit(
                apply_strategy, commit_info, new_parents, coalesced
            )
            amended_commits[to_rewrite] = (
                new_commit_oid,
                {b.file: b for b in blobs_with_amendments},
            )

        return amended_commits[self.head][0]

    def _get_amended_parents(self, parents, amended_commits):
        """Return a pair (new_parents, parent_amendments)

        The former is an ordered list of the parent OIDs to be used for the
        amended commit. The latter is a dict mapping from the path to a dict
        from the original commit OID to the amended blob and the new OID for
        that path.
        """
        new_parents = []

        # Map path -> {parent_commit_oid: (amended_blob, new_oid)...}
        parent_amendments = {}

        for parent in parents:
            try:
                (new_parent, new_parent_amendments) = amended_commits[parent]
            except KeyError:
                new_parents.append(parent)
                continue

            new_parents.append(new_parent)

            for blob in new_parent_amendments.values():
                parent_amendments.setdefault(blob.file, {})[parent] = blob

        return new_parents, parent_amendments

    def _rewrite_commit(self, apply_strategy, commit_info, new_parents, amendments):
        amended_with_oids = list(self._write_amended_blobs(apply_strategy, amendments))

        new_tree_oid = apply_strategy.write_tree(commit_info, amended_with_oids)

        new_oid = apply_strategy.write_commit(commit_info, new_tree_oid, new_parents)

        return new_oid, amended_with_oids

    def _coalesce_amended_blobs(self, commit_info, new_amendments, parent_amendments):
        coalesced = dict(new_amendments)
        need_full_reconcile = set(new_amendments) & set(parent_amendments)

        parent_only = [
            (path, parents)
            for path, parents in parent_amendments.items()
            if path not in need_full_reconcile
        ]

        parent_only_oid_info = {
            entry.path: entry.oid
            for entry in ls_tree(
                commit_info.oid, '--', *(path for path, _ in parent_only)
            )
        }

        for path, parents in parent_only:
            own_blob_oid = parent_only_oid_info.get(path)

            if not own_blob_oid:
                need_full_reconcile.add(path)
                continue

            ff_parent = self._find_fast_forward_parent(
                path, commit_info, parents, own_blob_oid
            )

            if ff_parent:
                coalesced[path] = ff_parent
            else:
                need_full_reconcile.add(path)

        if need_full_reconcile:
            self._handle_parent_changes_with_diff(
                coalesced,
                commit_info,
                new_amendments,
                parent_amendments,
                need_full_reconcile,
            )

        return coalesced

    def _find_fast_forward_parent(self, path, commit_info, parents, own_blob_oid):
        for parent_blob in parents.values():
            if parent_blob.oid != own_blob_oid:
                continue
            return parent_blob.with_meta(commit_info.oid, path, own_blob_oid)
        return None

    def _handle_parent_changes_with_diff(
        self, coalesced, commit_info, new_amendments, parent_amendments, needed_paths
    ):
        """
        Run a diff against each parent to try reconstruct the chain of
        amendments identified by git blame, including renames. Getting
        something more reliable probably requires either a complete
        reimplementation of git blame, or a fork of git to output tracking
        information for intermediate commits when doing blame.

        Note that libgit2 doesn't have a good blame implementation, so that
        isn't an option.
        """
        partially_coalesced = dict(new_amendments)

        for old_parent_oid in commit_info.parents:
            self._account_for_diff_against_parent(
                partially_coalesced,
                old_parent_oid,
                commit_info,
                parent_amendments,
                needed_paths,
            )

        assert set(partially_coalesced) == needed_paths
        assert not set(coalesced) & set(partially_coalesced)
        coalesced.update(partially_coalesced)

    def _account_for_diff_against_parent(
        self,
        partially_coalesced,
        old_parent_oid,
        commit_info,
        parent_amendments,
        needed_paths,
    ):
        _, diff_tree, _ = call_git(
            'diff-tree', '-r', '--find-renames', old_parent_oid, commit_info.oid
        )
        diffed = {
            entry.old_path: entry
            for entry in parse_diff_tree_summary(diff_tree)
            if entry.old_path in needed_paths
        }

        for _, entry in sorted(diffed.items()):
            # XXX: Wrong; not sure this is an error at all but it's definitely not an error per-parent
            if entry.new_path is None:
                raise Fatal(
                    f'unexpected diff entry during rewrite at {commit_info.oid}, '
                    f'looking at {old_parent_oid}, diffing {entry.old_path}'
                )

            _, diff_output, _ = call_git(
                'diff', '--patch-with-raw', entry.old_oid, entry.new_oid
            )

            parent_changes = parent_amendments[entry.old_path][old_parent_oid]
            adjusted_changes = parent_changes.adjusted_by_diff(
                parse_diff_hunks(diff_output),
                commit=commit_info.oid,
                file=entry.new_path,
                oid=entry.new_oid,
            )

            try:
                prior = partially_coalesced[entry.new_path]
            except KeyError:
                partially_coalesced[entry.new_path] = adjusted_changes
            else:
                partially_coalesced[entry.new_path] = prior.with_merged_amendments(
                    adjusted_changes.amendments
                )

    def _write_amended_blobs(self, apply_strategy, amended_blobs):
        for amended_blob in amended_blobs.values():
            if amended_blob.amended_oid is not None:
                yield amended_blob
            else:
                new_oid = apply_strategy.write_blob(amended_blob)
                yield amended_blob.with_amended_oid(new_oid)


class AmendedBlob:
    def __init__(self, commit, file, oid, amended_oid=None):
        self.commit = commit
        self.file = file
        self.oid = oid
        self.amended_oid = amended_oid
        self.amendments = []

    def replace_lines(self, start, extent, new_lines):
        record = AmendmentRecord(start, extent, new_lines)
        index = bisect.bisect_left(self.amendments, record)

        if index > 0:
            prior_record = self.amendments[index - 1]
            if prior_record.start + prior_record.extent > start:
                raise ValueError('overlapping amendments requested')

        if index < len(self.amendments):
            next_record = self.amendments[index]

            if record == next_record:
                return

            if start + extent > next_record.start:
                raise ValueError('overlapping amendments requested')

        self.amendments.insert(index, record)

    def with_merged_amendments(self, amendments):
        copy = AmendedBlob(self.commit, self.file, self.oid, amended_oid=None)
        copy.amendments = list(self.amendments)
        for record in amendments:
            copy.replace_lines(record.start, record.extent, record.replacement)
        return copy

    def with_meta(self, commit, file, oid):
        adjusted = AmendedBlob(commit, file, oid, amended_oid=self.amended_oid)
        adjusted.amendments.extend(self.amendments)
        return adjusted

    def with_amended_oid(self, amended_oid):
        if self.amended_oid is not None:
            raise ValueError(f'Associating amended OID {amended_oid!r} with {self!r}')
        amended = AmendedBlob(self.commit, self.file, self.oid, amended_oid)
        amended.amendments.extend(self.amendments)
        return amended

    def adjusted_by_diff(self, diff_hunks, commit, file, oid):
        adjusted = AmendedBlob(commit, file, oid, amended_oid=None)

        offset = 0
        for entry in self._stream_amendments_and_diff_hunks(diff_hunks):
            if isinstance(entry, AmendmentRecord):
                adjusted.replace_lines(
                    start=entry.start + offset,
                    extent=entry.extent,
                    new_lines=entry.replacement,
                )
                continue

            # We have a FileLineMapping
            offset += entry.new_extent - entry.old_extent

        return adjusted

    def _stream_amendments_and_diff_hunks(self, diff_hunks):
        amend_iter = iter(self.amendments)
        line_map_iter = chain.from_iterable(h.map_lines() for h in diff_hunks)

        amend = next(amend_iter, None)
        line_map = next(line_map_iter, None)

        while amend and line_map:
            if amend.start < line_map.old_start:
                if amend.start + amend.extent > line_map.old_start:
                    raise ValueError('amendment overlaps diff delta')

                yield amend
                amend = next(amend_iter, None)

                continue

            if line_map.old_start + line_map.old_extent > amend.start:
                raise ValueError('amendment overlaps diff delta')

            yield line_map
            line_map = next(line_map_iter, None)

        if amend is not None:
            yield amend
            yield from amend_iter

        elif line_map is not None:
            yield line_map
            yield from line_map_iter

    def write(self, output):
        if self.amended_oid is not None:
            raise ValueError(f'Writing blob for {self!r}')

        # TODO: Stream instead of buffering in memory
        file_rev = f'{self.commit}:'.encode() + self.file
        _, out, _ = call_git('cat-file', '-p', file_rev)

        amend_iter = iter(self.amendments)
        amend = next(amend_iter, None)

        for lineno, line in enumerate(out.splitlines(keepends=True), start=1):
            if amend and lineno > amend.start + amend.extent:
                amend = next(amend_iter, None)

            if amend:
                if lineno == amend.start:
                    output.write(amend.replacement)

                if amend.start <= lineno < amend.start + amend.extent:
                    continue

            output.write(line)

    def __repr__(self):
        class_name = type(self).__name__
        file_repr = self.file.decode(errors='replace')
        if self.amended_oid is not None:
            oids = f'{self.oid[:10]} -> {self.amended_oid[:10]}'
        else:
            oids = f'from {self.oid[:10]}'
        return f'<{class_name} {self.commit[:10]}:{file_repr}, {oids}>'
