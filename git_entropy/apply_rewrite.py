"""Strategies used to apply rewrites
"""

from __future__ import annotations

import typing
from typing import cast, Dict, List, NamedTuple, Tuple, Union

import os
import asyncio

from .amend import AmendedBlob, AbstractApplyStrategy, RewriteHandle
from .git import (
    OID,
    CommitListingEntry,
    TreeListingEntry,
    async_call_git,
    async_call_git_background,
    cat_commit,
    async_ls_tree,
    mk_tree,
)


class RewriteRequest(NamedTuple):
    commit_oid: OID
    commit_handle: RewriteHandle
    parents: List[Union[OID, RewriteHandle]]
    amended_blobs: List[AmendedBlob[RewriteHandle]]


BackendRequest = Union[RewriteRequest]  # XXX needed?


class GitSubprocessApplyStrategy(AbstractApplyStrategy):
    """Write the commits by calling out to the git executable"""

    def __init__(self) -> None:
        self.backend = GitBackend()

    async def rewrite_commit(
        self,
        commit_oid: OID,
        parents: List[Union[OID, RewriteHandle]],
        amendments: List[Union[AmendedBlob[None], AmendedBlob[RewriteHandle]]],
    ) -> Tuple[RewriteHandle, List[AmendedBlob[RewriteHandle]]]:
        return await self.backend.request_commit_rewrite(
            commit_oid, parents, amendments
        )

    async def resolve_handle(self, handle: RewriteHandle) -> OID:
        return await self.backend.resolve_handle(handle)

    async def join(self) -> None:
        return await self.backend.join()


class GitBackend:
    def __init__(self) -> None:
        self.loop = asyncio.get_event_loop()
        self.queue: asyncio.Queue[BackendRequest] = asyncio.Queue(maxsize=100)
        self._next_handle = 1

        backend_worker = GitBackendWorker(self.loop, self.queue)
        self._backend_worker = backend_worker
        self._backend_done = backend_worker.launch()

    async def request_commit_rewrite(
        self,
        commit_oid: OID,
        parents: List[Union[OID, RewriteHandle]],
        blobs: List[Union[AmendedBlob[None], AmendedBlob[RewriteHandle]]],
    ) -> Tuple[RewriteHandle, List[AmendedBlob[RewriteHandle]]]:
        commit_handle = RewriteHandle(obj_type='commit', handle_id=self._bump_handle())

        blobs_with_handles = [
            cast(AmendedBlob[RewriteHandle], blob)
            if blob.rewrite_data is not None
            else blob.with_rewrite_data(
                RewriteHandle(obj_type='blob', handle_id=self._bump_handle())
            )
            for blob in blobs
        ]

        await self.queue.put(
            RewriteRequest(commit_oid, commit_handle, parents, blobs_with_handles)
        )
        return commit_handle, blobs_with_handles

    def _bump_handle(self) -> int:
        out = self._next_handle
        self._next_handle += 1
        return out

    async def resolve_handle(self, commit_handle: RewriteHandle) -> OID:
        return await self._backend_worker.resolve_commit_handle(commit_handle)

    async def join(self) -> None:
        await self.queue.join()
        self._backend_done.cancel()


class GitBackendWorker:
    def __init__(
        self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue[BackendRequest]
    ) -> None:
        self.loop = loop
        self.queue = queue

        self._commit_rewrites: Dict[RewriteHandle, asyncio.Future[OID]] = {}
        self._blob_rewrites: Dict[RewriteHandle, asyncio.Future[AmendedBlob[OID]]] = {}

    def launch(self) -> asyncio.Future[None]:
        return self.loop.create_task(self._run())

    async def resolve_commit_handle(self, commit_handle: RewriteHandle) -> OID:
        to_resolve = self._get_commit_rewrite_future(commit_handle)
        return await to_resolve

    async def _run(self) -> None:
        while True:
            request = await self.queue.get()

            to_resolve = self._get_commit_rewrite_future(request.commit_handle)

            print('handling request')
            self.loop.create_task(self._process_rewrite_request(request, to_resolve))

            self.queue.task_done()

    def _get_commit_rewrite_future(self, handle: RewriteHandle) -> asyncio.Future[OID]:
        try:
            to_resolve = self._commit_rewrites[handle]
        except KeyError:
            to_resolve = asyncio.Future()
            self._commit_rewrites[handle] = to_resolve

        return to_resolve

    async def _process_rewrite_request(
        self, request: RewriteRequest, to_resolve: asyncio.Future[OID]
    ) -> None:
        amended_with_oids = list(
            await asyncio.gather(
                *(self._resolve_blob(blob) for blob in request.amended_blobs)
            )
        )

        commit_info = await cat_commit(request.commit_oid)
        new_tree_oid = await self._write_tree(commit_info, amended_with_oids)

        new_parents = [
            parent if isinstance(parent, OID) else await self._commit_rewrites[parent]
            for parent in request.parents
        ]

        new_oid = await self._write_commit(commit_info, new_tree_oid, new_parents)
        to_resolve.set_result(new_oid)

        print('rewrote', commit_info.oneline())
        print('-> ', new_oid)

    async def _write_commit(
        self, commit_info: CommitListingEntry, tree: OID, new_parents: List[OID]
    ) -> OID:
        parent_args = []
        for new_parent in new_parents:
            parent_args.append('-p')
            parent_args.append(str(new_parent))

        _, out, _ = await async_call_git(
            'commit-tree',
            tree,
            *parent_args,
            input=commit_info.message,
            env=dict(
                GIT_AUTHOR_NAME=commit_info.a_name,
                GIT_AUTHOR_EMAIL=commit_info.a_email,
                GIT_AUTHOR_DATE=commit_info.a_date,
            ),
        )

        return OID(out.strip())

    async def _write_tree(
        self, commit_info: CommitListingEntry, blobs: List[AmendedBlob[OID]]
    ) -> OID:
        print(
            'amended tree:',
            ' '.join(
                b.file.decode(errors='replace') + f'={b.rewrite_data.short()}'
                for b in blobs
            ),
        )

        if not blobs:
            return commit_info.tree_oid

        new_blobs = {b.file: b.rewrite_data for b in blobs}

        dir_set = set()
        for path in new_blobs:
            subdir = os.path.dirname(path)
            while subdir:
                dir_set.add(subdir)
                subdir = os.path.dirname(subdir)

        dirs = sorted(
            dir_set, key=lambda subdir: (subdir.count(b'/'), subdir), reverse=True
        )

        # Special case: the repository's root directory
        dirs.append(b'.')

        for subdir in dirs:
            entries = []
            for entry in await async_ls_tree(
                commit_info.commit_oid, '--', subdir + b'/'
            ):
                updated_entry_oid = new_blobs.get(entry.path, entry.oid)
                entries.append(
                    TreeListingEntry(
                        mode=entry.mode,
                        obj_type=entry.obj_type,
                        oid=updated_entry_oid,
                        path=os.path.basename(entry.path),
                    )
                )

            new_blobs[subdir] = await mk_tree(entries)

        return new_blobs[b'.']

    async def _resolve_blob(self, blob: AmendedBlob[RewriteHandle]) -> AmendedBlob[OID]:
        handle = blob.rewrite_data

        try:
            existing_future = self._blob_rewrites[handle]
        except KeyError:
            to_resolve = self._blob_rewrites[handle] = asyncio.Future()
        else:
            return await existing_future

        blob_with_amended_oid = await self._write_blob(blob)
        to_resolve.set_result(blob_with_amended_oid)

        return blob_with_amended_oid

    async def _write_blob(
        self, amended_blob: AmendedBlob[RewriteHandle]
    ) -> AmendedBlob[OID]:
        print(
            'write blob:',
            amended_blob.oid.short(),
            amended_blob.file.decode(errors='replace'),
        )

        async with async_call_git_background(
            'hash-object', '-tblob', '--stdin', '-w'
        ) as proc:
            stdin = cast(asyncio.StreamWriter, proc.stdin)
            stdout = cast(asyncio.StreamReader, proc.stdout)

            await amended_blob.write(cast(typing.BinaryIO, stdin))
            await stdin.drain()
            stdin.close()

            out: bytes = await stdout.read()

        return amended_blob.with_rewrite_data(OID(out.strip()))
