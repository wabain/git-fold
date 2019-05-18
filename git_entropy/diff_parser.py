import re
from enum import Enum
from types import SimpleNamespace as NS

from .git import Hunk, DiffLineType
from .errors import Fatal


DIFF_HEADER = re.compile(rb'^diff --git a/.* b/.*')
DIFF_FSTAT = re.compile(rb'^(index|similarity index|rename|deleted file|new file) .*')
DIFF_MODE = re.compile(rb'^(old|new) mode .*')
DIFF_OLD = re.compile(rb'^--- ((?P<devnull>/dev/null)|a/(?P<fname>.*))')
DIFF_NEW = re.compile(rb'^\+\+\+ ((?P<devnull>/dev/null)|b/(?P<fname>.*))')
HUNK_REGEX = re.compile(rb'^@@ -(\d+)(,\d+)? \+(\d+)(,\d+)? @@')
DIFF_BINARY = re.compile(rb'^Binary files .* and .* differ$')


class DiffParseState (Enum):
    Invalid = -1
    Initial = 0
    DiffHeader = 1
    InHunk = 2


DIFF_PARSE_INVALID = (DiffParseState.Invalid, None)


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
            yield Hunk(**attrs.__dict__)

    handlers = {}

    def _register(state, handlers=handlers):  #pylint: disable=no-self-argument
        def inner(fn):
            handlers[state] = fn
            return fn
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
            m = r.match(line)
            if not m:
                continue

            if getattr(attrs, v) is not None:
                return DIFF_PARSE_INVALID

            value = m.group('devnull')
            if not value:
                value = m.group('fname')
            setattr(attrs, v, value)
            return DiffParseState.DiffHeader, attrs

        m = HUNK_REGEX.match(line)
        if m:
            return cls._handle_diff_hunk_start(line, attrs, m)

        return DIFF_PARSE_INVALID

    @_register(DiffParseState.InHunk)
    @classmethod
    def _handle_in_hunk(cls, attrs, line):
        if DIFF_HEADER.match(line):
            hunk = Hunk(**attrs.__dict__)
            attrs = NS(old_file=None, new_file=None)
            return DiffParseState.DiffHeader, attrs, hunk

        m = HUNK_REGEX.match(line)
        if m:
            hunk = Hunk(**attrs.__dict__)
            return cls._handle_diff_hunk_start(line, attrs, m) + (hunk,)

        if not line:
            # This seems to happen occasionally, not sure when
            return DiffParseState.InHunk, attrs

        start, remainder = line[0], line[1:]
        if start == b'\\':
            if not attrs.ops:
                return DIFF_PARSE_INVALID
            t, ln = attrs.ops[-1]
            if not ln.endswith(b'\n'):
                return DIFF_PARSE_INVALID
            attrs.ops[-1] = t, ln[:-1]
            return DiffParseState.InHunk, attrs
        else:
            remainder += b'\n'

        try:
            line_type = DiffLineType(chr(start))
        except ValueError:
            return DIFF_PARSE_INVALID

        attrs.ops.append((line_type, remainder))
        return DiffParseState.InHunk, attrs


    del _register

    @classmethod
    def _handle_diff_hunk_start(cls, line, attrs, match):
        if attrs.old_file is None or attrs.new_file is None:
            return DIFF_PARSE_INVALID

        attrs = NS(old_file=attrs.old_file,
                   new_file=attrs.new_file,
                   old_start=int(match.group(1)),
                   new_start=int(match.group(3)),
                   ops=[])

        return DiffParseState.InHunk, attrs


def build_context_lines(lines, line_index):
    start_index = max(line_index - 5, 0)
    context = []
    padding = max(3, len(str(line_index + 5)))
    for i, ctx_line in enumerate(lines[start_index:line_index + 5], start=start_index):
        ctx_line = ctx_line.decode(errors='replace')
        context.append(f'{i + 1:<{padding}} {ctx_line}')
    return '\n'.join(context)
