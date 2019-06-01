import re
from enum import Enum
from types import SimpleNamespace as NS
from collections import namedtuple

from .git import Hunk, DiffLineType
from .errors import Fatal


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

FileDiffSummary = namedtuple(
    'FileDiffSummary',
    'old_mode, new_mode, old_oid, new_oid, delta_type, similarity, old_path, new_path',
)


class DiffParseState(Enum):
    Invalid = -1
    Initial = 0
    DiffHeader = 1
    InHunk = 2


DIFF_PARSE_INVALID = (DiffParseState.Invalid, None)


def parse_diff_hunks(diff):
    return DiffParser.parse_diff_hunks(diff)


class DiffParser:
    @classmethod
    def parse_diff_hunks(cls, diff):
        state = DiffParseState.Initial
        attrs = None

        # Don't use splitlines; git can put a CR in the middle of a
        # diff line; see e.g. the history of t/t0022-crlf-rename.sh
        # in the git repo
        lines = diff.split(b'\n')
        for line_index, line in enumerate(lines):
            output = cls.handle_line_parsing(state, attrs, line)
            if len(output) == 2:
                state, attrs = output
                hunk = None
            elif len(output) == 3:
                state, attrs, hunk = output
            else:
                raise ValueError(repr(output))

            if state == DiffParseState.Invalid:
                raise Fatal(
                    f'unexpected diff content at line {line_index + 1}',
                    extended=build_context_lines(lines, line_index),
                )

            if hunk is not None:
                yield hunk

        if state not in [DiffParseState.Initial, DiffParseState.InHunk]:
            raise Fatal(
                'unexpected end of diff',
                extended=build_context_lines(lines, len(lines)),
            )

        if state == DiffParseState.InHunk:
            yield Hunk(**attrs.__dict__)  # pylint: disable=missing-kwoa

    handlers = {}

    def _register(
        state, handlers=handlers
    ):  # pylint: disable=no-self-argument,dangerous-default-value
        def inner(func):
            handlers[state] = func
            return func

        return inner

    @classmethod
    def handle_line_parsing(cls, state, attrs, line):
        if state not in cls.handlers:
            return DIFF_PARSE_INVALID

        handler = cls.handlers[state]
        return handler.__func__(cls, attrs, line)

    @_register(DiffParseState.Initial)
    @classmethod
    def _handle_initial(cls, attrs, line):
        if not DIFF_HEADER.match(line):
            # Ignore initial diffstat output
            return DiffParseState.Initial, None

        attrs = NS(old_file=None, new_file=None)
        return DiffParseState.DiffHeader, attrs

    @_register(DiffParseState.DiffHeader)
    @classmethod
    def _handle_diff_header(cls, attrs, line):
        if DIFF_FSTAT.match(line) or DIFF_MODE.match(line):
            # TODO: Should handle mode
            return DiffParseState.DiffHeader, attrs

        if DIFF_HEADER.match(line) or DIFF_BINARY.match(line):
            # FIXME: Throwing away empty new files and binary files for now
            attrs = NS(old_file=None, new_file=None)
            return DiffParseState.DiffHeader, attrs

        for r, v in [(DIFF_OLD, 'old_file'), (DIFF_NEW, 'new_file')]:
            match = r.match(line)
            if not match:
                continue

            if getattr(attrs, v) is not None:
                return DIFF_PARSE_INVALID

            value = match.group('devnull')
            if not value:
                value = match.group('fname')
            setattr(attrs, v, value)
            return DiffParseState.DiffHeader, attrs

        match = HUNK_REGEX.match(line)
        if match:
            return cls._handle_diff_hunk_start(line, attrs, match)

        return DIFF_PARSE_INVALID

    @_register(DiffParseState.InHunk)
    @classmethod
    def _handle_in_hunk(cls, attrs, line):
        # pylint: disable=too-many-return-statements

        if DIFF_HEADER.match(line):
            hunk = Hunk(**attrs.__dict__)
            attrs = NS(old_file=None, new_file=None)
            return DiffParseState.DiffHeader, attrs, hunk

        match = HUNK_REGEX.match(line)
        if match:
            hunk = Hunk(**attrs.__dict__)
            return cls._handle_diff_hunk_start(line, attrs, match) + (hunk,)

        if not line:
            # This seems to happen occasionally, not sure when
            return DiffParseState.InHunk, attrs

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
            return DiffParseState.InHunk, attrs

        remainder += b'\n'

        try:
            line_type = DiffLineType(start.decode(errors='replace'))
        except ValueError:
            return DIFF_PARSE_INVALID

        attrs.ops.append((line_type, remainder))
        return DiffParseState.InHunk, attrs

    del _register

    @classmethod
    def _handle_diff_hunk_start(cls, _line, attrs, match):
        if attrs.old_file is None or attrs.new_file is None:
            return DIFF_PARSE_INVALID

        attrs = NS(
            old_file=attrs.old_file,
            new_file=attrs.new_file,
            old_start=int(match.group(1)),
            new_start=int(match.group(3)),
            ops=[],
        )

        return DiffParseState.InHunk, attrs


def parse_diff_tree_summary(diff_tree_lines):
    lines = diff_tree_lines.split(b'\n')
    summary_lines = []

    for idx, line in enumerate(lines):
        if not line:
            continue

        match = DIFF_TREE_FILE.match(line)
        if match:
            old_mode, new_mode, old_oid, new_oid, delta_type, new_path = match.groups()
            old_mode, new_mode, old_oid, new_oid, delta_type = (
                v.decode() for v in [old_mode, new_mode, old_oid, new_oid, delta_type]
            )

            old_path = None if all(n == '0' for n in old_oid) else new_path
            if all(n == '0' for n in new_oid):
                new_path = None

            similarity = None
        else:
            match = DIFF_TREE_FILE_RENAME.match(line)
            if not match:
                raise Fatal(
                    f'unable to parse diff-tree output line {idx + 1}:',
                    extended=build_context_lines(lines, idx),
                )
            old_mode, new_mode, old_oid, new_oid, similarity, old_path, new_path = (
                match.groups()
            )
            old_mode, new_mode, old_oid, new_oid = (
                v.decode() for v in [old_mode, new_mode, old_oid, new_oid]
            )
            similarity = int(similarity)
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


def build_context_lines(lines, line_index):
    start_index = max(line_index - 5, 0)
    context = []
    padding = max(3, len(str(line_index + 5)))
    for i, ctx_line in enumerate(
        lines[start_index : line_index + 5], start=start_index
    ):
        ctx_line = ctx_line.decode(errors='replace')
        context.append(f'{i + 1:<{padding}} {ctx_line}')
    return '\n'.join(context)
