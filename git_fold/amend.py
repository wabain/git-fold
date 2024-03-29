"""Utilities for in-place git history amendments

This interface combines the functionality of git amend and git rebase,
creating revisions to the log without affecting the working directory.
"""

from __future__ import annotations

import typing
from typing import (
    cast,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import asyncio
import bisect
from abc import ABC, abstractmethod
from itertools import chain, count

from . import git
from .blame import run_blame
from .git import OID, ls_tree, call_git, call_git_background, IndexedRange
from .log import CommitGraph
from .errors import Fatal
from .diff_parser import parse_diff_hunks, parse_diff_tree_summary


class AmendmentRecord(NamedTuple):
    start: int
    extent: int
    replacement: bytes


class RewriteHandle(NamedTuple):
    """Handle associated with a git object being rewritten"""

    obj_type: str
    handle_id: int


class AmendmentPlan:
    def __init__(self, head: OID, root: Optional[OID]):
        self.head = head
        self.root = root
        self._amendments: Dict[OID, Dict[bytes, AmendedBlob[None]]] = {}

    def has_amendments(self) -> bool:
        return bool(self._amendments)

    async def blame_range(
        self, idx_range: IndexedRange
    ) -> List[Tuple[IndexedRange, IndexedRange]]:
        return await run_blame(idx_range, root_rev=self.root)

    async def add_amended_range(
        self, indexed_range: IndexedRange, new_lines: bytes
    ) -> None:
        for_commit = self._for_commit(indexed_range.rev)
        try:
            for_blob = for_commit[indexed_range.file]
        except KeyError:
            for_blob = AmendedBlob(
                indexed_range.rev,
                indexed_range.file,
                await indexed_range.blob_oid(),
                rewrite_data=None,
            )
            for_commit[indexed_range.file] = for_blob
        for_blob.replace_lines(indexed_range.start, indexed_range.extent, new_lines)

    def _for_commit(self, commit_oid: OID) -> Dict[bytes, AmendedBlob[None]]:
        try:
            return self._amendments[commit_oid]
        except KeyError:
            out = self._amendments[commit_oid] = {}
            return out

    async def write_commits(self, apply_strategy: AbstractApplyStrategy) -> OID:
        return await AmendedBranchBuilder.write(
            head=self.head, amendments=self._amendments, apply_strategy=apply_strategy
        )


class RewrittenCommit(NamedTuple):
    commit_oid: OID
    commit_handle: RewriteHandle
    blobs: List[AmendedBlob[RewriteHandle]]


AmendedBlobInRewrite = Union['AmendedBlob[None]', 'AmendedBlob[RewriteHandle]']


class AmendedBranchBuilder:
    # TODO: This code would benefit from some cleanup once the algorithm seems
    # more settled
    #
    # pylint: disable=too-few-public-methods,too-many-arguments

    @staticmethod
    async def write(
        head: OID,
        amendments: Dict[OID, Dict[bytes, AmendedBlob[None]]],
        apply_strategy: AbstractApplyStrategy,
    ) -> OID:
        commit_graph = await CommitGraph.build_partial(
            head=head, roots=list(amendments)
        )
        builder = AmendedBranchBuilder(head, amendments, commit_graph, apply_strategy)
        return await builder.apply()

    def __init__(
        self,
        head: OID,
        amendments: Dict[OID, Dict[bytes, AmendedBlob[None]]],
        commit_graph: CommitGraph,
        apply_strategy: AbstractApplyStrategy,
    ) -> None:
        self.head = head
        self.amendments = amendments
        self.commit_graph = commit_graph
        self.apply_strategy = apply_strategy

        self.amended_commits: Dict[OID, RewrittenCommit] = {}

    async def apply(self) -> OID:
        for to_rewrite in self.commit_graph.reverse_topo_ordering(self.head):
            new_amendments = self.amendments.get(to_rewrite)
            await self._start_commit_rewrite(to_rewrite, new_amendments)

        head_handle = self.amended_commits[self.head].commit_handle

        new_head = await self.apply_strategy.resolve_handle(head_handle)

        await self.apply_strategy.join()

        return new_head

    async def _start_commit_rewrite(
        self, commit_oid: OID, amendments: Optional[Dict[bytes, AmendedBlob[None]]]
    ) -> None:
        parents = self.commit_graph.get_parents(commit_oid)

        parent_handles, parent_amendments = self._get_parent_amendments(parents)
        assert parent_handles != parents or amendments is not None

        coalesced = await self._coalesce_amended_blobs(
            commit_oid, amendments or {}, parent_amendments
        )

        commit_rewrite_handle, blobs_with_handles = await self.apply_strategy.rewrite_commit(
            commit_oid, parent_handles, coalesced
        )

        self.amended_commits[commit_oid] = RewrittenCommit(
            commit_oid=commit_oid,
            commit_handle=commit_rewrite_handle,
            blobs=blobs_with_handles,
        )

    def _get_parent_amendments(
        self, parents: List[OID]
    ) -> Tuple[
        List[Union[OID, RewriteHandle]],
        Dict[bytes, Dict[OID, AmendedBlob[RewriteHandle]]],
    ]:
        """Return a pair (new_parents, parent_amendments)

        The former is an ordered list of the parent OIDs to be used for the
        amended commit. The latter is a dict mapping from the path to a dict
        from the original commit OID to the amended blob for that path.
        """
        new_parents: List[Union[OID, RewriteHandle]] = []

        # Map path -> {parent_commit_oid: amended_blob...}
        parent_amendments: Dict[bytes, Dict[OID, AmendedBlob[RewriteHandle]]] = {}

        for parent in parents:
            try:
                rewritten_parent = self.amended_commits[parent]
            except KeyError:
                new_parents.append(parent)
                continue

            new_parents.append(rewritten_parent.commit_handle)

            for blob in rewritten_parent.blobs:
                parent_amendments.setdefault(blob.file, {})[parent] = blob

        return new_parents, parent_amendments

    async def _coalesce_amended_blobs(
        self,
        commit_oid: OID,
        new_amendments: Dict[bytes, AmendedBlob[None]],
        parent_amendments: Dict[bytes, Dict[OID, AmendedBlob[RewriteHandle]]],
    ) -> List[AmendedBlobInRewrite]:
        need_full_reconcile = set(new_amendments) & set(parent_amendments)

        coalesced: Dict[bytes, AmendedBlobInRewrite] = {
            path: amended_blob
            for path, amended_blob in new_amendments.items()
            if path not in need_full_reconcile
        }

        parent_only = [
            (path, parent_blobs)
            for path, parent_blobs in parent_amendments.items()
            if path not in need_full_reconcile
        ]

        if parent_only:
            await self._reuse_parent_blob_rewrites(
                commit_oid, parent_only, coalesced, need_full_reconcile
            )

        if need_full_reconcile:
            await self._handle_parent_changes_with_diff(
                coalesced,
                commit_oid,
                new_amendments,
                parent_amendments,
                need_full_reconcile,
            )

        return list(coalesced.values())

    async def _reuse_parent_blob_rewrites(
        self,
        commit_oid: OID,
        parent_only_changes: List[Tuple[bytes, Dict[OID, AmendedBlob[RewriteHandle]]]],
        coalesced: Dict[bytes, AmendedBlobInRewrite],
        need_full_reconcile: Set[bytes],
    ) -> None:
        """
        For blobs that need to be rewritten and have no new amendments
        introduced in this commit, see if a rewrite applied to a parent can be
        reused. If the blob at the path in this commit has a new OID, add the
        path to the set of paths needing a full reconciliation.
        """
        parent_path_ls = await ls_tree(
            commit_oid, '--', *(path for path, _ in parent_only_changes)
        )
        child_blob_oids = {entry.path: entry.oid for entry in parent_path_ls}

        for path, parent_blobs in parent_only_changes:
            child_blob_oid = child_blob_oids.get(path)

            reusable_parent = (
                self._find_reusable_parent(child_blob_oid, parent_blobs.values())
                if child_blob_oid is not None
                else None
            )

            if reusable_parent is not None:
                coalesced[path] = reusable_parent.with_meta(commit_oid, path)
            else:
                need_full_reconcile.add(path)

    @staticmethod
    def _find_reusable_parent(
        child_blob_oid: OID, parent_blobs: Iterable[AmendedBlob[RewriteHandle]]
    ) -> Optional[AmendedBlob[RewriteHandle]]:
        for parent_blob in parent_blobs:
            if parent_blob.oid == child_blob_oid:
                return parent_blob

        return None

    async def _handle_parent_changes_with_diff(
        self,
        coalesced: Dict[bytes, AmendedBlobInRewrite],
        commit_oid: OID,
        new_amendments: Dict[bytes, AmendedBlob[None]],
        parent_amendments: Dict[bytes, Dict[OID, AmendedBlob[RewriteHandle]]],
        needed_paths: Set[bytes],
    ) -> None:
        """
        Run a diff against each parent to try reconstruct the chain of
        amendments identified by git blame, including renames. Getting
        something more reliable probably requires either a complete
        reimplementation of git blame, or a fork of git to output tracking
        information for intermediate commits when doing blame.

        Note that libgit2 doesn't have a good blame implementation, so that
        isn't an option.
        """
        partially_coalesced: Dict[bytes, AmendedBlobInRewrite] = {
            path: blob for path, blob in new_amendments.items() if path in needed_paths
        }
        handled: Set[bytes] = set()

        for old_parent_oid in self.commit_graph.get_parents(commit_oid):
            handled |= await self._account_for_diff_against_parent(
                partially_coalesced,
                old_parent_oid,
                commit_oid,
                parent_amendments,
                needed_paths,
            )

        assert handled == needed_paths
        assert not set(coalesced) & set(partially_coalesced)
        coalesced.update(partially_coalesced)

    @staticmethod
    async def _account_for_diff_against_parent(
        partially_coalesced: Dict[bytes, AmendedBlobInRewrite],
        old_parent_oid: OID,
        commit_oid: OID,
        parent_amendments: Dict[bytes, Dict[OID, AmendedBlob[RewriteHandle]]],
        needed_paths: Set[bytes],
    ) -> Set[bytes]:
        _, diff_tree, _ = await call_git(
            'diff-tree', '-r', '--find-renames', old_parent_oid, commit_oid
        )
        diffed = {
            entry.old_path: entry
            for entry in parse_diff_tree_summary(diff_tree)
            if entry.old_path in needed_paths
        }
        handled = set()

        for _, entry in sorted(diffed.items()):
            assert entry.old_path is not None

            # XXX: Wrong; not sure this is an error at all but it's definitely not an error per-parent
            if entry.new_path is None:
                raise Fatal(
                    f'unexpected diff entry during rewrite at {commit_oid}, '
                    f'looking at {old_parent_oid}, diffing {entry.old_path}'
                )

            _, diff_output, _ = await call_git(
                'diff', '--patch-with-raw', entry.old_oid, entry.new_oid
            )

            parent_changes = parent_amendments[entry.old_path][old_parent_oid]
            adjusted_changes = parent_changes.adjusted_by_diff(
                parse_diff_hunks(diff_output),
                commit=commit_oid,
                file=entry.new_path,
                oid=entry.new_oid,
            )

            handled.add(entry.old_path)

            try:
                prior = partially_coalesced[entry.new_path]
            except KeyError:
                partially_coalesced[entry.new_path] = adjusted_changes
            else:
                partially_coalesced[entry.new_path] = prior.with_merged_amendments(
                    adjusted_changes.amendments
                )

        return handled


D = TypeVar('D')
X = TypeVar('X')


class AmendedBlob(Generic[D]):
    def __init__(self, commit: OID, file: bytes, oid: OID, rewrite_data: D):
        self.commit = commit
        self.file = file
        self.oid = oid
        self.rewrite_data = rewrite_data
        self.amendments: List[AmendmentRecord] = []

    def replace_lines(self, start: int, extent: int, new_lines: bytes) -> None:
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

    def with_merged_amendments(
        self, amendments: List[AmendmentRecord]
    ) -> AmendedBlob[None]:
        copy = AmendedBlob(self.commit, self.file, self.oid, rewrite_data=None)
        copy.amendments = list(self.amendments)
        for record in amendments:
            copy.replace_lines(record.start, record.extent, record.replacement)
        return copy

    def with_meta(self, commit: OID, file: bytes) -> AmendedBlob[D]:
        adjusted = AmendedBlob(commit, file, self.oid, rewrite_data=self.rewrite_data)
        adjusted.amendments.extend(self.amendments)
        return adjusted

    def with_rewrite_data(self, rewrite_data: X) -> AmendedBlob[X]:
        amended = AmendedBlob(self.commit, self.file, self.oid, rewrite_data)
        amended.amendments.extend(self.amendments)
        return amended

    def adjusted_by_diff(
        self, diff_hunks: Iterator[git.Hunk], commit: OID, file: bytes, oid: OID
    ) -> AmendedBlob[None]:
        adjusted = AmendedBlob(commit, file, oid, rewrite_data=None)

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

    def _stream_amendments_and_diff_hunks(
        self, diff_hunks: Iterator[git.Hunk]
    ) -> Iterator[Union[AmendmentRecord, git.FileLineMapping]]:
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

    async def write(self, output: typing.BinaryIO) -> None:
        amend_iter = iter(self.amendments)
        amend = next(amend_iter, None)

        file_rev = bytes(self.commit) + b':' + self.file
        async with call_git_background('cat-file', '-p', file_rev) as proc:

            stdout = cast(asyncio.StreamReader, proc.stdout)

            line_count = count(start=1)

            while True:
                try:
                    line = await stdout.readuntil(b'\n')
                except asyncio.streams.IncompleteReadError as exc:
                    if not exc.partial:
                        break

                    raise

                lineno = next(line_count)

                if amend and lineno > amend.start + amend.extent:
                    amend = next(amend_iter, None)

                if amend:
                    if lineno == amend.start:
                        output.write(amend.replacement)

                    if amend.start <= lineno < amend.start + amend.extent:
                        continue

                output.write(line)

    def __repr__(self) -> str:
        class_name = type(self).__name__
        file_repr = self.file.decode(errors='replace')
        if self.rewrite_data is not None:
            rewrite = f' rewrite {self.rewrite_data}'
        else:
            rewrite = ''
        return f'<{class_name} {self.commit.short()}:{file_repr}, from {self.oid.short()}{rewrite}>'


class AbstractApplyStrategy(ABC):
    """Interface for strategies used to apply rewrites

    Each write method returns the resulting OID.
    """

    @abstractmethod
    async def rewrite_commit(
        self,
        commit_oid: OID,
        parents: List[Union[OID, RewriteHandle]],
        amendments: List[AmendedBlobInRewrite],
    ) -> Tuple[RewriteHandle, List[AmendedBlob[RewriteHandle]]]:
        raise NotImplementedError()

    @abstractmethod
    async def resolve_handle(self, handle: RewriteHandle) -> OID:
        raise NotImplementedError()

    @abstractmethod
    async def join(self) -> None:
        raise NotImplementedError()
