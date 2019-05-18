import sys

from . import suggest_basic
from .errors import Fatal


try:
    suggest_basic(sys.argv[1:])
except Fatal as exc:
    print(f'fatal: {exc}', file=sys.stderr)
    if exc.extended:
        print(file=sys.stderr)
        print(exc.extended, file=sys.stderr)
    sys.exit(exc.returncode)
