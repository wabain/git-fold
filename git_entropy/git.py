import os
import subprocess
from enum import Enum
from collections import namedtuple
from contextlib import contextmanager

from .errors import Fatal


TreeListingEntry = namedtuple('TreeListingEntry', 'mode,obj_type,oid,path')
CommitListingEntry = namedtuple('CommitListingEntry', 'oid,tree_oid,parents,a_name,a_email,a_date,c_name,c_email,c_date,message')


class RevList:
    def __init__(self, revs):
        self.revs = revs

    @staticmethod
    def for_range(rev_range, paths=None, reverse=False, walk=True):
        cmd = ['rev-list', '--topo-order']
        if not walk:
            cmd.append('--no-walk')
        if reverse:
            cmd.append('--reverse')
        cmd.extend(rev_range)
        cmd.append('--')
        if paths:
            cmd.extend(paths)

        _, out, _ = call_git(*cmd)
        out = out.decode()
        return RevList(out.strip().splitlines())

    def write(self, f):
        for rev in self.revs:
            print(rev, file=f)


class DiffLineType (Enum):
    Add = '+'
    Remove = '-'
    Context = ' '


class IndexedRange:
    def __init__(self, rev, file, start, extent):
        self.rev = rev
        self.file = file
        self.start = start
        self.extent = extent
        self._oid = None

    def oid(self):
        if self._oid is None:
            for entry in ls_tree(self.rev, '--', self.file):
                if entry.obj_type != 'blob':
                    # TODO: sanity check; maybe some non-blobs are okay
                    raise ValueError(f'expected {self.file} at {self.rev} to be blob; got {entry.obj_type!r}')
                self._oid = entry.oid
                break
            else:
                raise ValueError(f'No listing for {self.file} at {self.rev}')
        return self._oid

    @property
    def formatted_range(self):
        return f'{self.start},+{self.extent}'

    def __repr__(self):
        return f'<IndexedRange {self.rev} {self.file!r} {self.formatted_range}>'


class Hunk:
    def __init__(self, old_file, new_file, old_start, new_start, ops):
        self.old_file = old_file
        self.new_file = new_file
        self.old_start = old_start
        self.new_start = new_start
        self.ops = ops

    def old_range(self, rev='HEAD'):
        extent = sum(1
                     for (line_type, _) in self.ops
                     if line_type != DiffLineType.Add)

        return IndexedRange(
            rev=rev,
            file=self.old_file,
            start=self.old_start,
            extent=extent,
        )

    def new_range_content(self, start, extent):
        if extent == 0:
            return b''

        combined = []
        line_gen = (ln for t, ln in self.ops if t != DiffLineType.Remove)
        for lineno, line in enumerate(line_gen, start=self.new_start):
            if start <= lineno < start + extent:
                combined.append(line)

        return b'\n'.join(combined)

    def __repr__(self):
        if self.old_file == self.new_file:
            f_repr = repr(self.old_file)
        elif self.old_file == '/dev/null':
            f_repr = f'create {self.new_file!r}'
        elif self.new_file == '/dev/null':
            f_repr = f'delete {self.old_file!r}'
        else:
            f_repr = f'rename {self.old_file!r} to {self.new_file!r}'

        return f'<Hunk {f_repr} @@ -{self.old_start} +{self.new_start}>'


def ls_tree(*args):
    _, out, _ = call_git('ls-tree', *args)
    out = out.decode()
    for line in out.splitlines():
        yield TreeListingEntry(*line.split(maxsplit=3))


def mk_tree(entries):
    input = b'\n'.join(
        f'{e.mode} {e.obj_type} {e.oid}\t{e.path}'.encode()
        for e in entries
    )
    _, out, _ = call_git('mktree', input=input)
    return out.decode().strip()


def cat_commit(rev):
    fields = [
        '%H',   # hash
        '%T',   # tree
        '%P',   # parents
        '%an',  # author name
        '%ae',  # author email
        '%ad',  # author date
        '%cn',  # committer name
        '%ce',  # committer email
        '%cd',  # committer date
        '%B',   # body
    ]
    _, out, _ = call_git(
        'rev-list',
        '--max-count=1',
        '--format=' + '%n'.join(fields),
        '--date=raw',
        rev,
    )
    lines = out.split(b'\n', maxsplit=len(fields))

    (
        _,
        oid,
        tree_oid,
        parents,
        a_name,
        a_email,
        a_date,
        c_name,
        c_email,
        c_date,
        message,
     ) = lines

    return CommitListingEntry(
        oid=oid.decode(),
        tree_oid=tree_oid.decode(),
        parents=parents.decode().split(),
        a_name=a_name,
        a_email=a_email,
        a_date=a_date.decode(),
        c_name=c_name,
        c_email=c_email,
        c_date=c_date.decode(),
        message=message,
    )


def call_git(*args, must_succeed=True, input=None, env=None):
    command = ['git']
    command.extend(args)

    if env is not None:
        override_env = env
        env = dict(os.environ)
        env.update(override_env)

    outcome = subprocess.run(
        command,
        input=input,
        env=env,
        capture_output=True,
    )

    if must_succeed and outcome.returncode != 0:
        display_command = ' '.join(command)
        raise Fatal(
            f'failed to execute {display_command!r}',
            returncode=outcome.returncode,
            extended=outcome.stderr.decode(errors='replace'),
        )
    return outcome.returncode, outcome.stdout, outcome.stderr


class GitCall:
    def __init__(self):
        self.stdout = None
        self.stderr = None

    @contextmanager
    def call_async(self, *args, stdin=None, env=None):
        cmd = ['git']
        cmd.extend(args)

        if env is not None:
            override_env = env
            env = dict(os.environ)
            env.update(override_env)

        with subprocess.Popen(cmd, stdin=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env) as proc:
            try:
                yield proc
                self.stdout, self.stderr = proc.communicate()
            except:
                proc.kill()
                raise
            if proc.returncode != 0:
                display_command = ' '.join(cmd)
                raise Fatal(
                    f'failed to execute {display_command!r}',
                    returncode=proc.returncode,
                    extended=self.stderr.decode(errors='replace'),
                )
