"""Strategies used to apply rewrites
"""

import os
from abc import ABC, abstractmethod

from .git import TreeListingEntry, call_git, call_git_async, ls_tree, mk_tree


class AbstractApplyStrategy(ABC):
    """Interface for strategies used to apply rewrites

    Each write method returns the resulting OID.
    """

    @abstractmethod
    def write_commit(self, commit_info, tree, new_parents):
        raise NotImplementedError()

    @abstractmethod
    def write_tree(self, commit_info, amended_blobs_with_oids):
        """Recursively rewrite the root tree"""
        raise NotImplementedError()

    @abstractmethod
    def write_blob(self, amended_blob):
        raise NotImplementedError()


class DummyApplyStrategy(AbstractApplyStrategy):
    """Simply return the original OIDs"""

    def write_commit(self, commit_info, tree, new_parents):
        return f'{commit_info.oid}/fake-amended'

    def write_tree(self, commit_info, amended_blobs_with_oids):
        return f'{commit_info.tree_oid}/fake-amended'

    def write_blob(self, amended_blob):
        return f'{amended_blob.oid}/fake-amended'


class GitExecutableApplyStrategy(AbstractApplyStrategy):
    """Write the commits by calling out to the git executable"""

    def write_commit(self, commit_info, tree, new_parents):
        parent_args = []
        for new_parent in new_parents:
            parent_args.append('-p')
            parent_args.append(new_parent)

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

        print('-> new commit', out.decode().strip())

        return out.decode().strip()

    def write_tree(self, commit_info, amended_blobs_with_oids):
        print(
            'amended tree:',
            ' '.join(
                b.file.decode(errors='replace') + f'={oid[:10]}'
                for b, oid in amended_blobs_with_oids
            ),
        )

        if not amended_blobs_with_oids:
            return commit_info.tree_oid

        new_blobs = {b.file: new_oid for b, new_oid in amended_blobs_with_oids}

        dirs = set()
        for path in new_blobs:
            d = os.path.dirname(path)
            while d:
                dirs.add(d)
                d = os.path.dirname(d)

        dirs = sorted(dirs, key=lambda d: (d.count('/'), d), reverse=True)
        # Special case: the repository's root directory
        dirs.append('.')

        for d in dirs:
            entries = []
            for entry in ls_tree(commit_info.oid, '--', d + '/'):
                updated_entry_oid = new_blobs.get(entry.path, entry.oid)
                entries.append(
                    TreeListingEntry(
                        mode=entry.mode,
                        obj_type=entry.obj_type,
                        oid=updated_entry_oid,
                        path=os.path.basename(entry.path),
                    )
                )

            new_blobs[d] = mk_tree(entries)

        return new_blobs['.']

    def write_blob(self, amended_blob):
        print(
            'write blob:',
            amended_blob.commit[:10],
            amended_blob.file.decode(errors='replace'),
        )

        with call_git_async('hash-object', '-tblob', '--stdin', '-w') as proc:
            amended_blob.write(proc.stdin)
            proc.stdin.close()

            out = proc.stdout.read()

        return out.decode().strip()
