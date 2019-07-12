from __future__ import annotations

from typing import (
    cast,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Match,
    NamedTuple,
    Optional,
    Type,
)

import re
from enum import Enum
from types import SimpleNamespace as NS

from .git import OID, Hunk, DiffLineType
from .errors import Fatal


__all__ = ('FileDiffSummary', 'parse_diff_hunks', 'parse_diff_tree_summary')


DIFF_HEADER = re.compile(rb'^diff --git a/.* b/.*')
DIFF_FSTAT = re.compile(rb'^(index|similarity index|rename|deleted file|new file) .*')
DIFF_MODE = re.compile(rb'^(old|new) mode .*')
DIFF_OLD = re.compile(rb'^--- ((?P<devnull>/dev/null)|a/(?P<fname>.*))')
DIFF_NEW = re.compile(rb'^\+\+\+ ((?P<devnull>/dev/null)|b/(?P<fname>.*))')
HUNK_REGEX = re.compile(rb'^@@ -(\d+)(,\d+)? \+(\d+)(,\d+)? @@')
DIFF_BINARY = re.compile(rb'^Binary files .* and .* differ$')

DIFF_TREE_FILE = re.compile(
    rb'^:(\d+) (\d+) ([a-f0-9]+) ([a-f0-9]+) (?=[^R])([A-Z])\t(.*)'
)
DIFF_TREE_FILE_RENAME = re.compile(
    rb'^:(\d+) (\d+) ([a-f0-9]+) ([a-f0-9]+) R(\d+)\t(.*)\t(.*)'
)


class FileDiffSummary(NamedTuple):
    old_mode: str
    new_mode: str
    old_oid: OID
    new_oid: OID
    delta_type: str
    similarity: Optional[int]
    old_path: Optional[bytes]
    new_path: Optional[bytes]


class DiffParseState(Enum):
    Invalid = -1
    Initial = 0
    DiffHeader = 1
    InHunk = 2


class DiffParseResult:
    def __init__(
        self, state: DiffParseState, attrs: Any, hunk: Optional[Hunk] = None
    ) -> None:
        self.state = state
        self.attrs = attrs
        self.hunk = hunk

    @staticmethod
    def for_diff_header() -> DiffParseResult:
        return DiffParseResult(
            DiffParseState.DiffHeader,
            NS(old_file=None, old_file_seen=False, new_file=None, new_file_seen=False),
        )

    def with_hunk(self, hunk: Hunk) -> DiffParseResult:
        return DiffParseResult(self.state, self.attrs, hunk)


DIFF_PARSE_INITIAL = DiffParseResult(DiffParseState.Initial, None)
DIFF_PARSE_INVALID = DiffParseResult(DiffParseState.Invalid, None)


DiffParseHandler = Callable[[Type['DiffParser'], Any, bytes], DiffParseResult]


def parse_diff_hunks(diff: bytes) -> Iterator[Hunk]:
    return DiffParser.parse_diff_hunks(diff)


class DiffParser:
    @classmethod
    def parse_diff_hunks(cls, diff: bytes) -> Iterator[Hunk]:
        res = DIFF_PARSE_INITIAL

        # Don't use splitlines; git can put a CR in the middle of a
        # diff line; see e.g. the history of t/t0022-crlf-rename.sh
        # in the git repo
        lines = diff.split(b'\n')
        for line_index, line in enumerate(lines):
            res = cls.handle_line_parsing(res.state, res.attrs, line)

            if res.state == DiffParseState.Invalid:
                raise Fatal(
                    f'unexpected diff content at line {line_index + 1}',
                    extended=build_context_lines(lines, line_index),
                )

            if res.hunk is not None:
                yield res.hunk

        if res.state not in [DiffParseState.Initial, DiffParseState.InHunk]:
            raise Fatal(
                'unexpected end of diff',
                extended=build_context_lines(lines, len(lines)),
            )

        # If the diff is non-empty and nothing could be parsed successfully, throw
        # an error
        if res.state == DiffParseState.Initial and any(lines):
            raise Fatal(
                'unable to locate diff content', extended=build_context_lines(lines, 0)
            )

        if res.state == DiffParseState.InHunk:
            yield Hunk(**res.attrs.__dict__)  # pylint: disable=missing-kwoa

    handlers: Dict[DiffParseState, DiffParseHandler] = {}

    def _register(  # type: ignore  # pylint: disable=no-self-argument,dangerous-default-value
        state: DiffParseState,
        handlers: Dict[DiffParseState, DiffParseHandler] = handlers,
    ) -> Callable[[DiffParseHandler], DiffParseHandler]:
        def inner(func: DiffParseHandler) -> DiffParseHandler:
            handlers[state] = func
            return func

        return inner

    @classmethod
    def handle_line_parsing(
        cls, state: DiffParseState, attrs: Any, line: bytes
    ) -> DiffParseResult:
        if state not in cls.handlers:
            raise ValueError(f'Unknown state {state!r}')  # pragma nocover

        handler: DiffParseHandler = cast(Any, cls.handlers[state]).__func__
        return handler(cls, attrs, line)

    @_register(DiffParseState.Initial)
    @classmethod
    def _handle_initial(cls, _attrs: Any, line: bytes) -> DiffParseResult:
        if not DIFF_HEADER.match(line):
            # Ignore initial diffstat output
            return DIFF_PARSE_INITIAL

        return DiffParseResult.for_diff_header()

    @_register(DiffParseState.DiffHeader)
    @classmethod
    def _handle_diff_header(cls, attrs: Any, line: bytes) -> DiffParseResult:
        # pylint: disable=too-many-return-statements

        if DIFF_FSTAT.match(line) or DIFF_MODE.match(line):
            # TODO: Should handle mode
            return DiffParseResult(DiffParseState.DiffHeader, attrs)

        if DIFF_HEADER.match(line) or DIFF_BINARY.match(line):
            # FIXME: Throwing away empty new files and binary files for now
            return DiffParseResult.for_diff_header()

        for r, v, seen in [
            (DIFF_OLD, 'old_file', 'old_file_seen'),
            (DIFF_NEW, 'new_file', 'new_file_seen'),
        ]:
            match = r.match(line)
            if not match:
                continue

            if getattr(attrs, v) is not None:
                return DIFF_PARSE_INVALID

            if match.group('devnull'):
                value = None
            else:
                value = match.group('fname')
            setattr(attrs, v, value)

            setattr(attrs, seen, True)

            return DiffParseResult(DiffParseState.DiffHeader, attrs)

        match = HUNK_REGEX.match(line)
        if match:
            if not (attrs.old_file_seen and attrs.new_file_seen):
                return DIFF_PARSE_INVALID

            return cls._handle_diff_hunk_start(line, attrs, match)

        return DIFF_PARSE_INVALID

    @_register(DiffParseState.InHunk)
    @classmethod
    def _handle_in_hunk(cls, attrs: Any, line: bytes) -> DiffParseResult:
        # pylint: disable=too-many-return-statements

        if DIFF_HEADER.match(line):
            hunk = Hunk(**attrs.__dict__)
            return DiffParseResult.for_diff_header().with_hunk(hunk)

        match = HUNK_REGEX.match(line)
        if match:
            hunk = Hunk(**attrs.__dict__)
            return cls._handle_diff_hunk_start(line, attrs, match).with_hunk(hunk)

        if not line:
            # This seems to happen occasionally, not sure when
            return DiffParseResult(DiffParseState.InHunk, attrs)

        start, remainder = line[:1], line[1:]
        # Handle the "\ No newline at end of file" line
        if start == b'\\':
            if not attrs.ops:
                return DIFF_PARSE_INVALID
            last_op, last_line = attrs.ops[-1]
            if not last_line.endswith(b'\n'):
                # Sanity check
                return DIFF_PARSE_INVALID
            attrs.ops[-1] = last_op, last_line[:-1]
            return DiffParseResult(DiffParseState.InHunk, attrs)

        remainder += b'\n'

        try:
            line_type = DiffLineType(start.decode(errors='replace'))
        except ValueError:
            return DIFF_PARSE_INVALID

        attrs.ops.append((line_type, remainder))
        return DiffParseResult(DiffParseState.InHunk, attrs)

    del _register

    @classmethod
    def _handle_diff_hunk_start(
        cls, _line: bytes, attrs: Any, match: Match[bytes]
    ) -> DiffParseResult:
        attrs = NS(
            old_file=attrs.old_file,
            new_file=attrs.new_file,
            old_start=int(match.group(1)),
            new_start=int(match.group(3)),
            ops=[],
        )

        return DiffParseResult(DiffParseState.InHunk, attrs)


def parse_diff_tree_summary(diff_tree_lines: bytes) -> List[FileDiffSummary]:
    lines = diff_tree_lines.split(b'\n')
    summary_lines = []

    for idx, line in enumerate(lines):
        if not line:
            continue

        match = DIFF_TREE_FILE.match(line)
        if match:
            old_mode = match.group(1).decode()
            new_mode = match.group(2).decode()
            old_oid = OID(match.group(3))
            new_oid = OID(match.group(4))
            delta_type = match.group(5).decode()
            new_path: Optional[bytes] = match.group(6)

            old_path = new_path if old_oid else None

            if not new_oid:
                new_path = None

            similarity = None
        else:
            match = DIFF_TREE_FILE_RENAME.match(line)
            if not match:
                raise Fatal(
                    f'unable to parse diff-tree output line {idx + 1}:',
                    extended=build_context_lines(lines, idx),
                )
            old_mode = match.group(1).decode()
            new_mode = match.group(2).decode()
            old_oid = OID(match.group(3))
            new_oid = OID(match.group(4))
            similarity = int(match.group(5))
            old_path = match.group(6)
            new_path = match.group(7)

            delta_type = 'R'

        summary_lines.append(
            FileDiffSummary(
                old_mode=old_mode,
                new_mode=new_mode,
                old_oid=old_oid,
                new_oid=new_oid,
                delta_type=delta_type,
                similarity=similarity,
                old_path=old_path,
                new_path=new_path,
            )
        )

    return summary_lines


def build_context_lines(lines: List[bytes], line_index: int) -> str:
    start_index = max(line_index - 5, 0)
    context = []
    padding = max(3, len(str(line_index + 5)))
    for i, ctx_line_src in enumerate(
        lines[start_index : line_index + 5], start=start_index
    ):
        ctx_line = ctx_line_src.decode(errors='replace')
        context.append(f'{i + 1:<{padding}} {ctx_line}')
    return '\n'.join(context)
