from __future__ import annotations

from typing import Optional


class Fatal(Exception):
    def __init__(
        self, message: str, returncode: int = 1, extended: Optional[str] = None
    ):
        super().__init__(message)
        self.returncode = returncode
        self.extended = extended
