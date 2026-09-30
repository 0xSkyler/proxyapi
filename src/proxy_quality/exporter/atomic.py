"""Atomic file replacement: temp file in the same directory -> fsync -> rename.

Readers (nginx, the git publisher, users downloading files) only ever see
either the previous complete file or the new complete file.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def atomic_write_bytes(path: Path, data: bytes, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    if hasattr(os, "O_DIRECTORY"):
        try:
            dfd = os.open(path.parent, os.O_DIRECTORY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass


def atomic_write_text(path: Path, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def cleanup_temp_files(directory: Path) -> int:
    """Remove temp files left behind by a crash mid-write."""
    removed = 0
    if directory.exists():
        for p in directory.glob(".*.tmp"):
            try:
                p.unlink()
                removed += 1
            except OSError:
                pass
    return removed
