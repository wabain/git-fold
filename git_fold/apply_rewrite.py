"""Strategies used to apply rewrites
"""

from __future__ import annotations

import typing
from typing import (
    cast,
    Any,
    Awaitable,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import os
import asyncio

from .amend import AmendedBlob, AbstractApplyStrategy, RewriteHandle
from .git import (
    OID,
    CommitListingEntry,
    TreeListingEntry,
    call_git,
    call_git_background,
    cat_commit,
    ls_tree,
    mk_tree,
)


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

    def cancel(self) -> None:
        self.backend.cancel()

    async def join(self) -> None:
        return await self.backend.join()


class GitBackend:
    def __init__(self) -> None:
        self.loop = asyncio.get_event_loop()
        self._next_handle = 1

        backend_worker = GitBackendWorker(self.loop)
        self._backend_worker = backend_worker
        backend_worker.launch()

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

        await self._backend_worker.schedule_commit_rewrite(
            commit_handle, commit_oid, parents, blobs_with_handles
        )

        return commit_handle, blobs_with_handles

    def _bump_handle(self) -> int:
        out = self._next_handle
        self._next_handle += 1
        return out

    async def resolve_handle(self, commit_handle: RewriteHandle) -> OID:
        return await self._backend_worker.resolve_commit_handle(commit_handle)

    def cancel(self) -> None:
        self._backend_worker.cancel()

    async def join(self) -> None:
        await self._backend_worker.join()
        self.cancel()


class PendingCommitRewriteData(NamedTuple):
    commit_oid: OID
    parents: List[Union[OID, RewriteHandle]]
    blobs: List[AmendedBlob[RewriteHandle]]


class PendingCommitRewrite(NamedTuple):
    input_future: asyncio.Future[PendingCommitRewriteData]
    task: asyncio.Task[OID]


class CommitRewriteRequest(NamedTuple):
    commit_handle: RewriteHandle
    data: PendingCommitRewriteData


T = TypeVar('T')


class GitBackendWorker:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self._queue: asyncio.Queue[CommitRewriteRequest] = asyncio.Queue(maxsize=100)

        self._commit_rewrites: Dict[RewriteHandle, PendingCommitRewrite] = {}
        self._blob_rewrites: Dict[RewriteHandle, asyncio.Future[AmendedBlob[OID]]] = {}

        self._main: Optional[asyncio.Future[None]] = None
        self._completion: asyncio.Future[None] = self.loop.create_future()

    def launch(self) -> asyncio.Future[None]:
        if self._main is not None:
            raise RuntimeError('GitBackendWorker launched more than once')

        self._main = self.loop.create_task(self._die_on_error(self._run()))
        return self._main

    async def schedule_commit_rewrite(
        self,
        commit_handle: RewriteHandle,
        commit_oid: OID,
        parents: List[Union[OID, RewriteHandle]],
        blobs: List[AmendedBlob[RewriteHandle]],
    ) -> None:
        await self._propagate_fatal_exc(
            self._queue.put(
                CommitRewriteRequest(
                    commit_handle, PendingCommitRewriteData(commit_oid, parents, blobs)
                )
            )
        )

    async def join(self) -> None:
        await self._propagate_fatal_exc(self._queue.join())

    def cancel(self) -> None:
        if not self._completion.done():
            self._completion.set_result(None)

        if self._main is not None and not self._main.done():
            self._main.cancel()

        for task in self._blob_rewrites.values():
            if not task.done():
                task.cancel()

        for pending_data in self._commit_rewrites.values():
            if not pending_data.input_future.done():
                pending_data.input_future.cancel()

            if not pending_data.task.done():
                pending_data.task.cancel()

    async def _propagate_fatal_exc(self, happy_path_awaitable: Awaitable[T]) -> T:
        happy_path: asyncio.Future[T]

        if asyncio.isfuture(happy_path_awaitable):
            happy_path = cast('asyncio.Future[T]', happy_path_awaitable)
        else:
            happy_path = self.loop.create_task(happy_path_awaitable)

        if not self._completion.done():
            futures: List[Awaitable[Any]] = [happy_path, self._completion]

            await asyncio.wait(
                futures, timeout=None, return_when=asyncio.FIRST_COMPLETED
            )

            if not self._completion.done():
                return happy_path.result()

        # At this point we know completion is done

        if not happy_path.done():
            happy_path.cancel()

        # Raise exception
        self._completion.result()

        # Fallback: there was already an orderly exit
        raise RuntimeError('future is pending after join')

    async def resolve_commit_handle(self, commit_handle: RewriteHandle) -> OID:
        try:
            rewrite_task = self._commit_rewrites[commit_handle].task
        except KeyError:
            # Spawn the rewrite task now. When the rewrite request is received
            # from the queue, the input future will be resolved with the
            # concrete data and the task can start its work.
            input_future = self.loop.create_future()
            rewrite_task = self._spawn_commit_rewrite_task(commit_handle, input_future)

        return await self._propagate_fatal_exc(rewrite_task)

    async def _run(self) -> None:
        while True:
            request = await self._queue.get()
            print('handling request')

            try:
                input_future = self._commit_rewrites[request.commit_handle].input_future
            except KeyError:
                input_future = self.loop.create_future()
                self._spawn_commit_rewrite_task(request.commit_handle, input_future)

            if not input_future.cancelled():
                input_future.set_result(request.data)

            self._queue.task_done()

    def _spawn_commit_rewrite_task(
        self,
        commit_handle: RewriteHandle,
        input_future: asyncio.Future[PendingCommitRewriteData],
    ) -> asyncio.Task[OID]:
        task = self.loop.create_task(
            self._die_on_error(
                self._process_rewrite_request(commit_handle, input_future)
            )
        )

        self._commit_rewrites[commit_handle] = PendingCommitRewrite(
            input_future=input_future, task=task
        )

        return task

    async def _die_on_error(self, coro: Awaitable[T]) -> T:
        try:
            return await coro
        except BaseException as exc:
            if not self._completion.done():
                self._completion.set_exception(exc)

            self.cancel()
            raise asyncio.CancelledError()

    async def _process_rewrite_request(
        self,
        _commit_handle: RewriteHandle,
        input_future: asyncio.Future[PendingCommitRewriteData],
    ) -> OID:
        data = await input_future

        amended_with_oids = list(
            await asyncio.gather(*(self._resolve_blob(blob) for blob in data.blobs))
        )

        commit_info = await cat_commit(data.commit_oid)
        new_tree_oid = await self._write_tree(commit_info, amended_with_oids)
        new_parents = list(await asyncio.gather(*self._resolve_parents(data.parents)))
        new_oid = await self._write_commit(commit_info, new_tree_oid, new_parents)

        print('rewrote', commit_info.oneline())
        print('-> ', new_oid)

        return new_oid

    def _resolve_parents(
        self, parents: List[Union[OID, RewriteHandle]]
    ) -> Iterator[Awaitable[OID]]:
        for parent in parents:
            if isinstance(parent, OID):
                fut = self.loop.create_future()
                fut.set_result(parent)
                yield fut
                continue

            yield self._commit_rewrites[parent].task

    async def _write_commit(
        self, commit_info: CommitListingEntry, tree: OID, new_parents: List[OID]
    ) -> OID:
        parent_args = []
        for new_parent in new_parents:
            parent_args.append('-p')
            parent_args.append(str(new_parent))

        _, out, _ = await call_git(
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
            for entry in await ls_tree(commit_info.commit_oid, '--', subdir + b'/'):
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

        async with call_git_background(
            'hash-object', '-tblob', '--stdin', '-w'
        ) as proc:
            stdin = cast(asyncio.StreamWriter, proc.stdin)
            stdout = cast(asyncio.StreamReader, proc.stdout)

            await amended_blob.write(cast(typing.BinaryIO, stdin))
            await stdin.drain()
            stdin.close()

            out: bytes = await stdout.read()

        return amended_blob.with_rewrite_data(OID(out.strip()))
