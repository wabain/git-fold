from .git import call_git, IndexedRange


def run_blame(indexed_range):
    if indexed_range.extent == 0:
        return []

    _, out, _ = call_git(
        'blame',
        '--porcelain',
        f'-L{indexed_range.start},+{indexed_range.extent}',
        indexed_range.rev,
        '--',
        indexed_range.file,
    )
    return parse_blame(indexed_range, out.split(b'\n'))


def parse_blame(src_range, blame_lines):
    range_mapping = []

    for rev, fname, old_line, new_line, starts_seq in get_blame_transforms(blame_lines):
        if range_mapping and not starts_seq:
            last_old, last_new = range_mapping[-1]
            if (
                last_old.file == fname
                and last_old.start + last_old.extent == old_line
                and last_new.start + last_new.extent == new_line
            ):
                last_old.extent += 1
                last_new.extent += 1
                continue

        range_mapping.append(
            (
                IndexedRange(rev=rev, file=fname, start=old_line, extent=1),
                IndexedRange(
                    rev=src_range.rev, file=src_range.file, start=new_line, extent=1
                ),
            )
        )

    return range_mapping


def get_blame_transforms(blame_lines):
    """Yield a tuple for each blamed line indicating its source

    For our purposes, we interpret the porcelain blame format as follows:

      <header line (start sequence)>
      <filename>?
      <header line (!start sequence)>*

    The filename can be omitted iff there has been a previous entry for the
    given source commit.
    """
    rev = old_line = new_line = starts_seq = filename = None

    # Name of the file for lines originated in a particular commit
    origin_fnames = {}

    for line in blame_lines:
        emit = False
        parts = line.split()

        header_entry = as_header(parts)
        if header_entry:
            rev, old_line, new_line, starts_seq = header_entry
            if starts_seq:
                filename = origin_fnames.get(rev)
                emit = filename is not None
            else:
                if filename is None:
                    raise ValueError(f'missing filename for blamed commit {rev}')
                emit = True
        else:
            fname_entry = as_filename(parts)
            if fname_entry:
                if filename is not None:
                    raise ValueError('multiple filenames specified')

                if rev is None:
                    raise ValueError('filename specified before rev')

                filename = fname_entry
                origin_fnames[rev] = filename
                emit = True

        if emit:
            yield rev, filename, old_line, new_line, starts_seq


def as_header(parts):
    """Try to parse blame line into a tuple (oid, old_lineno, new_lineno, starts_seq)"""
    # Line in format <HASH> <OLD-LINENO> <NEW-LINENO> [COUNT]
    if not (
        3 <= len(parts) <= 4
        and is_int(parts[0], 16)
        and all(is_int(p, 10) for p in parts[1:])
    ):
        return None

    return parts[0].decode(), int(parts[1]), int(parts[2]), len(parts) == 4


def as_filename(parts):
    if len(parts) != 2 or parts[0] != b'filename':
        return None
    return parts[1]


def is_int(string, base):
    try:
        int(string, base)
    except ValueError:
        return False
    return True
