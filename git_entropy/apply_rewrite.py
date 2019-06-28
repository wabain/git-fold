"""Strategies used to apply rewrites
"""

from __future__ import annotations

import typing
from typing import cast, List

import os

from .amend import AmendedBlob, AbstractApplyStrategy
from .git import (
    OID,
    CommitListingEntry,
    TreeListingEntry,
    call_git,
    call_git_async,
    ls_tree,
    mk_tree,
)


class DummyApplyStrategy(AbstractApplyStrategy):
    """Simply return the original OIDs"""

    def write_commit(
        self, commit_info: CommitListingEntry, tree: OID, new_parents: List[OID]
    ) -> OID:
        return OID(commit_info.commit_oid.numeric + 1)

    def write_tree(
        self,
        commit_info: CommitListingEntry,
        amended_blobs_with_oids: List[AmendedBlob],
    ) -> OID:
        return OID(commit_info.tree_oid.numeric + 1)

    def write_blob(self, amended_blob: AmendedBlob) -> OID:
        return OID(amended_blob.oid.numeric + 1)


class GitExecutableApplyStrategy(AbstractApplyStrategy):
    """Write the commits by calling out to the git executable"""

    def write_commit(
        self, commit_info: CommitListingEntry, tree: OID, new_parents: List[OID]
    ) -> OID:
        parent_args = []
        for new_parent in new_parents:
            parent_args.append('-p')
            parent_args.append(str(new_parent))

        _, out, _ = call_git(
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

        commit_oid = OID(out.strip())

        print('-> new commit', commit_oid)

        return commit_oid

    def write_tree(
        self,
        commit_info: CommitListingEntry,
        amended_blobs_with_oids: List[AmendedBlob],
    ) -> OID:
        print(
            'amended tree:',
            ' '.join(
                b.file.decode(errors='replace') + f'={cast(OID, b.amended_oid).short()}'
                for b in amended_blobs_with_oids
            ),
        )

        if not amended_blobs_with_oids:
            return commit_info.tree_oid

        new_blobs = {b.file: cast(OID, b.amended_oid) for b in amended_blobs_with_oids}

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
            for entry in ls_tree(commit_info.commit_oid, '--', subdir + b'/'):
                updated_entry_oid = new_blobs.get(entry.path, entry.oid)
                entries.append(
                    TreeListingEntry(
                        mode=entry.mode,
                        obj_type=entry.obj_type,
                        oid=updated_entry_oid,
                        path=os.path.basename(entry.path),
                    )
                )

            new_blobs[subdir] = mk_tree(entries)

        return new_blobs[b'.']

    def write_blob(self, amended_blob: AmendedBlob) -> OID:
        print(
            'write blob:',
            amended_blob.commit.short(),
            amended_blob.file.decode(errors='replace'),
        )

        with call_git_async('hash-object', '-tblob', '--stdin', '-w') as proc:
            amended_blob.write(cast(typing.BinaryIO, proc.stdin))
            proc.stdin.close()

            out: bytes = proc.stdout.read()

        return OID(out.strip())
