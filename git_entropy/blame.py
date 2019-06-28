from __future__ import annotations

from typing import Dict, Iterator, List, NamedTuple, Optional, Tuple, Union

import itertools

from .git import call_git, IndexedRange


class BlameCommitProperties(NamedTuple):
    filename: bytes
    is_boundary: bool


class BlameLineProperties(NamedTuple):
    rev: str
    filename: bytes
    is_boundary: bool
    old_line: int
    new_line: int
    starts_seq: bool


def run_blame(
    indexed_range: IndexedRange, root_rev: Optional[str] = None
) -> List[Tuple[IndexedRange, IndexedRange]]:
    if indexed_range.extent == 0:
        return []

    if root_rev is None:
        revision_range = indexed_range.rev
    else:
        revision_range = f'{root_rev}..{indexed_range.rev}'

    _, out, _ = call_git(
        'blame',
        '--porcelain',
        f'-L{indexed_range.start},+{indexed_range.extent}',
        revision_range,
        '--',
        indexed_range.file,
    )
    return parse_blame(
        indexed_range, out.split(b'\n'), include_boundary=root_rev is None
    )


def parse_blame(
    src_range: IndexedRange, blame_lines: List[bytes], include_boundary: bool
) -> List[Tuple[IndexedRange, IndexedRange]]:
    range_mapping: List[Tuple[IndexedRange, IndexedRange]] = []

    for transformed_line in get_blame_transforms(blame_lines):
        if (not include_boundary) and transformed_line.is_boundary:
            continue

        if range_mapping and not transformed_line.starts_seq:
            last_old, last_new = range_mapping[-1]
            if (
                last_old.file == transformed_line.filename
                and last_old.start + last_old.extent == transformed_line.old_line
                and last_new.start + last_new.extent == transformed_line.new_line
            ):
                last_old.extent += 1
                last_new.extent += 1
                continue

        range_mapping.append(
            (
                IndexedRange(
                    rev=transformed_line.rev,
                    file=transformed_line.filename,
                    start=transformed_line.old_line,
                    extent=1,
                ),
                IndexedRange(
                    rev=src_range.rev,
                    file=src_range.file,
                    start=transformed_line.new_line,
                    extent=1,
                ),
            )
        )

    return range_mapping


def get_blame_transforms(blame_lines: List[bytes]) -> Iterator[BlameLineProperties]:
    """Yield a tuple for each blamed line indicating its source

    For our purposes, we interpret the porcelain blame format as follows:

      <header line (start sequence)>
      <filename>?
      <header line (!start sequence)>*

    The filename can be omitted iff there has been a previous entry for the
    given source commit.
    """
    # Properties for lines originated in a particular commit
    commit_properties: Dict[str, BlameCommitProperties] = {}

    idx = 0
    line_count = len(blame_lines)
    while idx < line_count:
        idx, rev, commit_props, old_line, new_line, starts_seq = parse_block(
            blame_lines, idx, commit_properties
        )

        # When the range is HEAD..HEAD, staged changes seem to be included in the
        # blame.
        #
        # TODO: Determine if that's exactly what's happening or if it's more
        # complicated. It's probably correct to ignore these entries regardless.
        if all(c == '0' for c in rev):
            continue

        yield BlameLineProperties(
            rev=rev,
            filename=commit_props.filename,
            is_boundary=commit_props.is_boundary,
            old_line=old_line,
            new_line=new_line,
            starts_seq=starts_seq,
        )


def parse_block(
    blame_lines: List[bytes],
    idx: int,
    commit_properties: Dict[str, BlameCommitProperties],
) -> Tuple[
    int,  # idx
    str,  # rev
    BlameCommitProperties,
    int,  # old line
    int,  # new line
    bool,  # starts seq
]:
    header_entry = as_header(blame_lines[idx].split())
    if header_entry is None:
        raise ValueError(
            f'parsing blame (line {idx + 1}): expected header, got {blame_lines[idx]!r}'
        )

    rev, old_line, new_line, starts_seq = header_entry
    has_prior_properties = rev in commit_properties

    filename = None
    is_boundary = False
    block_ended = False

    for idx, line in enumerate(
        itertools.islice(blame_lines, idx, len(blame_lines)), start=idx
    ):
        if block_ended:
            # Consume empty inter-block lines
            if not line:
                continue
            break

        if line.startswith(b'\t'):
            block_ended = True
            continue

        if line == b'boundary':
            assert not has_prior_properties
            is_boundary = True

        fname_entry = as_filename(line.split())
        if fname_entry is not None:
            assert not has_prior_properties
            filename = fname_entry
    else:
        idx += 1

    if has_prior_properties:
        props = commit_properties[rev]
    else:
        assert filename is not None
        props = BlameCommitProperties(filename=filename, is_boundary=is_boundary)
        commit_properties[rev] = props

    return idx, rev, props, old_line, new_line, starts_seq


def as_header(parts: List[bytes]) -> Optional[Tuple[str, int, int, bool]]:
    """Try to parse blame line into a tuple (oid, old_lineno, new_lineno, starts_seq)"""
    # Line in format <HASH> <OLD-LINENO> <NEW-LINENO> [COUNT]
    if not (
        3 <= len(parts) <= 4
        and is_int(parts[0], 16)
        and all(is_int(p, 10) for p in parts[1:])
    ):
        return None

    return parts[0].decode(), int(parts[1]), int(parts[2]), len(parts) == 4


def as_filename(parts: List[bytes]) -> Optional[bytes]:
    if len(parts) != 2 or parts[0] != b'filename':
        return None
    return parts[1]


def is_int(string: Union[bytes, str], base: int) -> bool:
    try:
        int(string, base)
    except ValueError:
        return False
    return True
