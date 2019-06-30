from __future__ import annotations

from typing import (
    cast,
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import os
import asyncio
from subprocess import Popen, PIPE, run
from enum import Enum
from contextlib import asynccontextmanager, contextmanager

from .errors import Fatal


OID_MAX_VALUE = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF


class OID:
    def __init__(self, oid: Union[int, str, bytes]):
        if isinstance(oid, int):
            if not (0 <= oid <= OID_MAX_VALUE):
                raise ValueError(f'Invalid OID {oid:x}')

            self.numeric = oid
        else:
            try:
                self.numeric = int(oid, 16)
            except ValueError as exc:
                raise ValueError(f'Invalid OID {oid!r}') from exc

    def short(self) -> str:
        return str(self)[:10]

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, OID) and other.numeric == self.numeric

    def __hash__(self) -> int:
        return self.numeric

    def __bool__(self) -> bool:
        return bool(self.numeric)

    def __str__(self) -> str:
        return f'{self.numeric:040x}'

    def __bytes__(self) -> bytes:
        return str(self).encode()

    def __repr__(self) -> str:
        cls = type(self)
        return f'{cls.__name__}({str(self)!r})'


class FileLineMapping(NamedTuple):
    old_start: int
    old_extent: int
    new_start: int
    new_extent: int


class TreeListingEntry(NamedTuple):
    mode: str
    obj_type: str
    oid: OID
    path: bytes


class CommitListingEntry(NamedTuple):
    commit_oid: OID
    tree_oid: OID
    parents: List[OID]
    a_name: bytes
    a_email: bytes
    a_date: str
    c_name: bytes
    c_email: bytes
    c_date: str
    message: bytes

    def summary(self) -> bytes:
        end = self.message.find(b'\n')
        if end < 0:
            return self.message
        return self.message[:end]

    def oneline(self) -> str:
        summary = self.summary().decode(errors='replace')
        return f'{self.commit_oid.short()} {summary}'


class DiffLineType(Enum):
    Add = '+'
    Remove = '-'
    Context = ' '


class IndexedRange:
    def __init__(self, rev: OID, file: bytes, start: int, extent: int):
        self.rev = rev
        self.file = file
        self.start = start
        self.extent = extent
        self._blob_oid: Optional[OID] = None

    async def blob_oid(self) -> OID:
        if self._blob_oid is None:
            for entry in await async_ls_tree(self.rev, '--', self.file):
                if entry.obj_type != 'blob':
                    # TODO: sanity check; maybe some non-blobs are okay
                    raise ValueError(
                        f'expected {self.file} at {self.rev} to be blob; got {entry.obj_type!r}'
                    )
                self._blob_oid = entry.oid
                break
            else:
                raise ValueError(f'No listing for {self.file} at {self.rev}')
        return self._blob_oid

    @property
    def formatted_range(self) -> str:
        return f'{self.start},+{self.extent}'

    def __repr__(self) -> str:
        return f'<IndexedRange {self.rev} {self.file!r} {self.formatted_range}>'


class Hunk:
    def __init__(
        self,
        *,
        old_file: Optional[bytes],
        new_file: Optional[bytes],
        old_start: int,
        new_start: int,
        ops: List[Tuple[DiffLineType, bytes]],
    ):
        if old_file is None and not all(op_t == DiffLineType.Add for op_t, _ in ops):
            raise ValueError('non-empty old content but old file missing')

        if new_file is None and not all(op_t == DiffLineType.Remove for op_t, _ in ops):
            raise ValueError('non-empty new content but new file missing')

        self.old_file = old_file
        self.new_file = new_file
        self.old_start = old_start
        self.new_start = new_start
        self.ops = ops

    def old_range(self, rev: OID) -> Optional[IndexedRange]:
        extent = sum(1 for (line_type, _) in self.ops if line_type != DiffLineType.Add)

        if self.old_file is None:
            return None

        return IndexedRange(
            rev=rev, file=self.old_file, start=self.old_start, extent=extent
        )

    def get_edits(
        self, old_rev: OID, new_rev: OID
    ) -> Iterator[Tuple[Optional[IndexedRange], Optional[IndexedRange]]]:
        """Yield tuples (old_range, new_range) indicating the edits needed"""
        for mapping in self.map_lines():
            if self.old_file is None:
                old_range = None
            else:
                old_range = IndexedRange(
                    rev=old_rev,
                    file=self.old_file,
                    start=mapping.old_start,
                    extent=mapping.old_extent,
                )

            if self.new_file is None:
                new_range = None
            else:
                new_range = IndexedRange(
                    rev=new_rev,
                    file=self.new_file,
                    start=mapping.new_start,
                    extent=mapping.new_extent,
                )

            yield old_range, new_range

    def map_lines(self) -> Iterator[FileLineMapping]:
        """Yield line mappings indicating the edits to apply the hunk"""
        old_line, new_line = self.old_start, self.new_start
        old_mstart, new_mstart = old_line, new_line
        old_extent, new_extent = 0, 0

        for line_type, _ in self.ops:
            if line_type == DiffLineType.Context:
                if old_extent != 0 or new_extent != 0:
                    yield FileLineMapping(
                        old_mstart, old_extent, new_mstart, new_extent
                    )

                old_line += 1
                new_line += 1

                old_mstart, new_mstart = old_line, new_line
                old_extent = new_extent = 0
                continue

            if line_type == DiffLineType.Remove:
                old_extent += 1
                old_line += 1
                continue

            if line_type == DiffLineType.Add:
                new_extent += 1
                new_line += 1
                continue

            raise ValueError(line_type)

        if old_extent != 0 or new_extent != 0:
            yield FileLineMapping(old_mstart, old_extent, new_mstart, new_extent)

    def new_range_content(self, start: int, extent: int) -> bytes:
        if extent == 0:
            return b''

        combined = []
        line_gen = (ln for t, ln in self.ops if t != DiffLineType.Remove)
        for lineno, line in enumerate(line_gen, start=self.new_start):
            if start <= lineno < start + extent:
                combined.append(line)

        return b''.join(combined)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Hunk):
            return NotImplemented

        return (
            self.old_file == other.old_file
            and self.new_file == other.new_file
            and self.old_start == other.old_start
            and self.new_start == other.new_start
            and self.ops == other.ops
        )

    def __repr__(self) -> str:
        display_old_file = (
            None if self.old_file is None else self.old_file.decode(errors='replace')
        )
        display_new_file = (
            None if self.new_file is None else self.new_file.decode(errors='replace')
        )

        if self.old_file == self.new_file:
            f_repr = display_old_file
        elif self.old_file is None:
            f_repr = f'create {display_new_file}'
        elif self.new_file is None:
            f_repr = f'delete {display_old_file}'
        else:
            f_repr = f'rename {display_old_file} to {display_new_file}'

        return f'<Hunk {f_repr} @@ -{self.old_start} +{self.new_start}>'


async def async_ls_tree(*args: Union[bytes, str, OID]) -> Iterator[TreeListingEntry]:
    _, out, _ = await async_call_git('ls-tree', *args)
    return _parse_ls_tree(out)


def _parse_ls_tree(out: bytes) -> Iterator[TreeListingEntry]:
    for line in out.splitlines():
        mode, obj_type, oid, path = line.split(maxsplit=3)

        yield TreeListingEntry(
            mode=mode.decode(), obj_type=obj_type.decode(), oid=OID(oid), path=path
        )


async def mk_tree(entries: Iterable[TreeListingEntry]) -> OID:
    git_input = b'\n'.join(
        f'{e.mode} {e.obj_type} {e.oid}'.encode() + b'\t' + e.path for e in entries
    )

    _, out, _ = await async_call_git('mktree', '--missing', input=git_input)

    return OID(out.strip())


async def cat_commit(rev: OID) -> CommitListingEntry:
    fields = [
        '%H',  # hash
        '%T',  # tree
        '%P',  # parents
        '%an',  # author name
        '%ae',  # author email
        '%ad',  # author date
        '%cn',  # committer name
        '%ce',  # committer email
        '%cd',  # committer date
        '%B',  # body
    ]
    _, out, _ = await async_call_git(
        'rev-list', '--max-count=1', '--format=' + '%n'.join(fields), '--date=raw', rev
    )
    lines = out.split(b'\n', maxsplit=len(fields))

    (
        _,
        commit_oid,
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
        commit_oid=OID(commit_oid),
        tree_oid=OID(tree_oid),
        parents=[OID(p) for p in parents.split()],
        a_name=a_name,
        a_email=a_email,
        a_date=a_date.decode(),
        c_name=c_name,
        c_email=c_email,
        c_date=c_date.decode(),
        message=message,
    )


Environ = Dict[str, Union[bytes, str]]


async def async_call_git(
    *args: Union[bytes, str, OID],
    must_succeed: bool = True,
    input: Optional[Union[bytes, str]] = None,  # pylint: disable=redefined-builtin
    env: Optional[Environ] = None,
) -> Tuple[int, bytes, bytes]:

    cmd = _build_git_cmd(args)

    proc = await asyncio.create_subprocess_exec(
        *cmd, env=_setup_env(env), stdin=PIPE, stdout=PIPE, stderr=PIPE
    )

    stdout, stderr = await proc.communicate(
        input=input.encode() if isinstance(input, str) else input
    )

    if must_succeed:
        _handle_git_error(cmd, proc.returncode, lambda: stderr)

    return proc.returncode, stdout, stderr


@asynccontextmanager
async def async_call_git_background(
    *args: Union[bytes, str, OID],
    must_succeed: bool = True,
    env: Optional[Environ] = None,
) -> AsyncGenerator['asyncio.subprocess.Process', None]:

    cmd = _build_git_cmd(args)

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, env=_setup_env(env)
    )

    try:
        yield proc
        await proc.wait()
    except:
        proc.kill()
        raise

    if must_succeed and proc.returncode != 0:
        stderr = await cast(asyncio.StreamReader, proc.stderr).read()
        _handle_git_error(cmd, proc.returncode, lambda: stderr)


def call_git(
    *args: Union[bytes, str, OID],
    must_succeed: bool = True,
    input: Optional[Union[bytes, str]] = None,  # pylint: disable=redefined-builtin
    env: Optional[Environ] = None,
) -> Tuple[int, bytes, bytes]:
    code, out, err = _call_git_sync(
        *args, must_succeed=must_succeed, input=input, env=env, capture_output=True
    )
    return code, cast(bytes, out), cast(bytes, err)


def call_git_no_capture(
    *args: Union[bytes, str, OID],
    must_succeed: bool = True,
    input: Optional[Union[bytes, str]] = None,  # pylint: disable=redefined-builtin
    env: Optional[Environ] = None,
) -> int:
    code, _, _ = _call_git_sync(
        *args, must_succeed=must_succeed, input=input, env=env, capture_output=False
    )
    return code


def _call_git_sync(
    *args: Union[bytes, str, OID],
    must_succeed: bool = True,
    input: Optional[Union[bytes, str]] = None,  # pylint: disable=redefined-builtin
    env: Optional[Environ] = None,
    capture_output: bool = True,
) -> Tuple[int, Optional[bytes], Optional[bytes]]:

    cmd = _build_git_cmd(args)

    outcome = run(cmd, input=input, env=_setup_env(env), capture_output=capture_output)

    if must_succeed:
        _handle_git_error(cmd, outcome.returncode, lambda: outcome.stderr or b'')

    return outcome.returncode, outcome.stdout, outcome.stderr


@contextmanager
def call_git_background(
    *args: Union[bytes, str, OID],
    must_succeed: bool = True,
    env: Optional[Environ] = None,
) -> Iterator[Popen]:

    cmd = _build_git_cmd(args)

    with Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, env=_setup_env(env)) as proc:
        try:
            yield proc
            proc.wait()
        except:
            proc.kill()
            raise

        if must_succeed:
            _handle_git_error(cmd, proc.returncode, proc.stderr.read)


def _build_git_cmd(
    args: Sequence[Union[bytes, str, OID]]
) -> Sequence[Union[bytes, str]]:
    command: List[Union[bytes, str]] = ['git']
    command.extend(str(a) if isinstance(a, OID) else a for a in args)
    return command


def _setup_env(env: Optional[Environ]) -> Optional[Environ]:
    if env is not None:
        override_env = env
        env = cast(Environ, dict(os.environ))
        env.update(override_env)

    return env


def _handle_git_error(
    cmd: Sequence[Union[bytes, str]], returncode: int, stderr: Callable[[], bytes]
) -> None:
    if returncode == 0:
        return

    display_command = ' '.join(
        c.decode(errors='replace') if isinstance(c, bytes) else c for c in cmd
    )

    raise Fatal(
        f'failed to execute {display_command!r}',
        returncode=returncode,
        extended='\n'.join(
            f'git: {line}'.rstrip()
            for line in stderr().decode(errors='replace').splitlines()
        ),
    )
