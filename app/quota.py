"""Capacity reservation, admission control and on-disk reconciliation.

Accounting model
----------------
Every task promises its **peak** space requirement at creation time::

    reserved = 2 * size

``size`` bytes are needed for the received chunk files and another ``size``
for the assembled product while finalization runs (chunks and the temp
product coexist on disk). The promise is held while the task is
``uploading`` and therefore also covers the per-chunk temp files written
during uploads.

When the task reaches a terminal state the promise is **settled**:

* ``published``: the chunk files are deleted and only the product is kept,
  so usage becomes exactly ``size`` bytes;
* ``failed`` (digest mismatch): chunks are retained as evidence and usage
  becomes their actual on-disk total.

Residual files that belong to no live task (crash leftovers, files outside
the ledger) are recorded in ``orphan_reservations`` and keep charging
capacity until they disappear. Capacity is therefore only ever released
after the corresponding files have actually been verified gone -- a
missing DB row can never make physically occupied space look free.

All admission decisions run inside ``BEGIN IMMEDIATE`` transactions, which
SQLite serializes across the two service processes: two concurrent task
creations can never both pass the limit check.
"""
from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .db import connect

_UNLIMITED = "unlimited"
_SIZE_RE = re.compile(r"^\s*(\d+)\s*(b|k|kb|kib|m|mb|mib|g|gb|gib|t|tb|tib)?\s*$", re.IGNORECASE)
_UNIT = {
    None: 1, "b": 1,
    "k": 1024, "kb": 1024, "kib": 1024,
    "m": 1024 ** 2, "mb": 1024 ** 2, "mib": 1024 ** 2,
    "g": 1024 ** 3, "gb": 1024 ** 3, "gib": 1024 ** 3,
    "t": 1024 ** 4, "tb": 1024 ** 4, "tib": 1024 ** 4,
}


class CapacityExceeded(Exception):
    """Raised when creating a task would push reservations past the cap."""

    def __init__(self, limit: int, reserved: int, needed: int):
        self.limit = limit
        self.reserved = reserved
        self.needed = needed
        self.available = max(0, limit - reserved)
        super().__init__(
            f"capacity limit {limit} bytes exceeded:"
            f" {reserved} already reserved, {needed} needed"
        )


class StorageLimitConfigError(ValueError):
    pass


def parse_limit(raw) -> int:
    """Parse ``STORAGE_LIMIT``: ``None``/``"unlimited"`` or e.g. ``512MiB``."""
    if raw is None:
        return 0  # 0 means unlimited
    text = str(raw).strip()
    if not text or text.lower() == _UNLIMITED or text == "0":
        return 0
    m = _SIZE_RE.match(text)
    if not m:
        raise StorageLimitConfigError(
            f"invalid STORAGE_LIMIT {text!r}:"
            " use a byte count like '1073741824' or a suffix like '512MiB'"
        )
    return int(m.group(1)) * _UNIT[m.group(2).lower() if m.group(2) else None]


def peak_bytes(file_size: int) -> int:
    """Peak space a task promises: chunks + assembled product coexist."""
    return 2 * file_size


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def immediate_transaction(conn: sqlite3.Connection):
    """Serialize writers across processes; roll back on any error."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def _regular_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        for name in filenames:
            p = Path(dirpath) / name
            try:
                if p.is_file() and not p.is_symlink():
                    yield p, p.stat().st_size
            except OSError:
                continue


def disk_usage_bytes(storage) -> int:
    """Total bytes of regular files under the data root (best-effort view)."""
    total = 0
    for _, size in _regular_files(storage.root):
        total += size
    return total


def _committed_bytes(conn) -> int:
    # reserved_bytes is charged in EVERY state: uploading tasks hold their
    # 2*size peak promise, and a terminal task keeps a non-zero reservation
    # only while files it still owns could not be cleaned up. Settled
    # terminal tasks have reserved_bytes = 0 and charge retained_bytes.
    row = conn.execute(
        "SELECT"
        " (SELECT COALESCE(SUM(reserved_bytes),0) FROM tasks)"
        " + (SELECT COALESCE(SUM(retained_bytes),0) FROM tasks"
        "  WHERE state='published')"
        " + (SELECT COALESCE(SUM(retained_bytes),0) FROM tasks"
        "  WHERE state='failed')"
        " + (SELECT COALESCE(SUM(size),0) FROM orphan_reservations)"
        " AS total"
    ).fetchone()
    return int(row["total"])


def capacity_snapshot(db_path, storage, limit: int) -> dict:
    """Numbers shown on the web page and returned by ``GET /api/capacity``."""
    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT state,"
            " COALESCE(SUM(reserved_bytes),0) AS reserved,"
            " COALESCE(SUM(retained_bytes),0) AS retained,"
            " COUNT(*) AS n FROM tasks GROUP BY state"
        ).fetchall()
        by_state = {r["state"]: dict(r) for r in rows}
        orphan = conn.execute(
            "SELECT COALESCE(SUM(size),0) AS bytes, COUNT(*) AS n"
            " FROM orphan_reservations"
        ).fetchone()
        reserved = _committed_bytes(conn)
    finally:
        conn.close()

    disk = disk_usage_bytes(storage)
    unlimited = limit <= 0
    available = None if unlimited else max(0, limit - reserved)
    return {
        "limit_bytes": None if unlimited else limit,
        "reserved_bytes": reserved,
        "available_bytes": available,
        "disk_used_bytes": disk,
        "over_limit": (not unlimited) and reserved > limit,
        "unlimited": unlimited,
        "tasks": {
            state: {
                "count": by_state.get(state, {"n": 0})["n"],
                "reserved_bytes": by_state.get(state, {"reserved": 0})["reserved"],
                "retained_bytes": by_state.get(state, {"retained": 0})["retained"],
            }
            for state in ("uploading", "published", "failed")
        },
        "orphan_files": {"count": orphan["n"], "bytes": orphan["bytes"]},
    }


def insert_admitted_task(
    db_path,
    limit: int,
    task_id: str,
    filename: str,
    file_size: int,
    sha: str,
    chunk_size: int,
    total_chunks: int,
    created_at: str,
) -> int:
    """Atomically check capacity and insert the task row.

    The capacity check and the INSERT run in one ``BEGIN IMMEDIATE``
    transaction, which SQLite serializes across worker processes. Two
    concurrent creations therefore cannot both pass the limit check; a
    rejected request commits nothing, leaving no task behind. Raises
    :class:`CapacityExceeded` when the promise would exceed *limit*.
    Returns the reserved peak (``2 * file_size``).
    """
    needed = peak_bytes(file_size)
    conn = connect(db_path)
    try:
        with immediate_transaction(conn):
            if limit > 0:
                reserved = _committed_bytes(conn)
                if reserved + needed > limit:
                    raise CapacityExceeded(limit, reserved, needed)
            conn.execute(
                "INSERT INTO tasks (id, filename, size, sha256, chunk_size,"
                " total_chunks, state, created_at, reserved_bytes, retained_bytes)"
                " VALUES (?,?,?,?,?,?, 'uploading', ?, ?, 0)",
                (
                    task_id,
                    filename,
                    file_size,
                    sha,
                    chunk_size,
                    total_chunks,
                    created_at,
                    needed,
                ),
            )
        return needed
    finally:
        conn.close()


def settle_published(conn, storage, task_id: str, product_size: int) -> None:
    """Release the peak promise after deleting chunks; keep product size.

    Runs on the caller's connection inside its open transaction so the
    state change and the ledger change commit atomically. Chunk files are
    unlinked first and their disappearance verified; only then is the
    ledger updated, so a failed deletion can never make the reserved space
    look free while files still occupy it. Returns True when settled,
    False when files remain and the peak promise is deliberately kept.
    """
    task_dir = storage.chunks_dir / task_id
    leftovers = []
    if task_dir.is_dir():
        for p in sorted(task_dir.iterdir()):
            try:
                p.unlink()
            except OSError as exc:
                leftovers.append((p, exc))
        if not leftovers:
            try:
                task_dir.rmdir()
            except OSError:
                pass
    if leftovers:
        # Keep charging for what is still on disk; startup reconciliation
        # will retry the cleanup on the next process start.
        return False

    conn.execute(
        "UPDATE tasks SET reserved_bytes = 0, retained_bytes = ?"
        " WHERE id = ?",
        (product_size, task_id),
    )
    return True


def settle_failed(conn, task_id: str, retained: int) -> None:
    """Digest mismatch: promise is replaced by the chunks' real footprint."""
    conn.execute(
        "UPDATE tasks SET reserved_bytes = 0, retained_bytes = ?"
        " WHERE id = ?",
        (retained, task_id),
    )


def reconcile(db_path, storage) -> dict:
    """Reconcile the capacity ledger with what is actually on disk.

    Runs at every process start (both workers; the IMMEDIATE transaction
    serializes them) and makes the invariants hold after crashes:

    1. tasks created by an older release (no reservation yet) get their
       peak promise backfilled;
    2. a ``published`` task whose chunk directory survived a crash between
       publish and cleanup is cleaned up and re-settled to its product;
    3. every other file not covered by a live task is booked as an orphan
       (still charging capacity), and vanished orphans are released.
    """
    conn = connect(db_path)
    report = {"backfilled": 0, "orphans": 0, "orphan_bytes": 0, "released": 0}
    try:
        with immediate_transaction(conn):
            tasks = conn.execute(
                "SELECT id, size, state, reserved_bytes, retained_bytes"
                " FROM tasks"
            ).fetchall()
            # Files a task nominally owns but which could not be removed and
            # therefore must keep charging through the orphan ledger.
            excluded: set[str] = set()

            for t in tasks:
                if t["state"] == "uploading" and t["reserved_bytes"] == 0:
                    # Pre-upgrade task: restore the peak promise so old
                    # tasks keep reserving space while they finish.
                    conn.execute(
                        "UPDATE tasks SET reserved_bytes = ? WHERE id = ?",
                        (peak_bytes(t["size"]), t["id"]),
                    )
                    report["backfilled"] += 1
                elif t["state"] == "published":
                    chunk_dir = storage.chunks_dir / t["id"]
                    undeletable = []
                    if chunk_dir.is_dir():
                        for p in chunk_dir.iterdir():
                            try:
                                p.unlink()
                            except OSError:
                                undeletable.append(str(p))
                        if not undeletable:
                            try:
                                chunk_dir.rmdir()
                            except OSError:
                                pass
                    product = storage.published_path(t["id"])
                    kept = product.stat().st_size if product.is_file() else 0
                    if t["reserved_bytes"] != 0 or t["retained_bytes"] != kept:
                        conn.execute(
                            "UPDATE tasks SET reserved_bytes = 0,"
                            " retained_bytes = ? WHERE id = ?",
                            (kept, t["id"]),
                        )
                    # Chunks we could not remove still occupy the volume;
                    # keep them out of the task's owned set so they land in
                    # the orphan ledger instead of being silently freed.
                    excluded.update(undeletable)
                elif t["state"] == "failed" and t["reserved_bytes"] != 0:
                    row = conn.execute(
                        "SELECT COALESCE(SUM(size),0) AS kept FROM chunks"
                        " WHERE task_id = ?",
                        (t["id"],),
                    ).fetchone()
                    conn.execute(
                        "UPDATE tasks SET reserved_bytes = 0, retained_bytes = ?"
                        " WHERE id = ?",
                        (row["kept"], t["id"]),
                    )

            # Everything physically present that a live task does not own.
            # NOTE: branches are independent (not elif) -- a published task
            # legitimately owns both its product and any not-yet-cleaned
            # chunk files; missing either would make real files look free.
            accounted: set[str] = set()
            for t in tasks:
                chunk_files = set(
                    str(p) for p in (storage.chunks_dir / t["id"]).glob("*")
                )
                if t["state"] == "uploading" or t["state"] == "failed":
                    # Chunks are the working set / retained evidence.
                    accounted.update(chunk_files)
                else:  # published: product plus surviving chunk leftovers
                    accounted.add(str(storage.published_path(t["id"])))
                    accounted.update(chunk_files)
            accounted.add(str(storage.db_path))
            accounted.add(str(storage.db_path) + "-wal")
            accounted.add(str(storage.db_path) + "-shm")
            # Per-task flock files are service-owned bookkeeping, not
            # residual user data; never charge them as orphans.
            for t in tasks:
                accounted.add(str(storage.lock_path(t["id"])))

            seen: set[str] = set()
            now = _now_iso()
            for p, size in _regular_files(storage.root):
                sp = str(p)
                seen.add(sp)
                if sp in accounted and sp not in excluded:
                    continue
                conn.execute(
                    "INSERT INTO orphan_reservations (path, size, reason, updated_at)"
                    " VALUES (?,?,?,?) ON CONFLICT(path) DO UPDATE SET"
                    " size = excluded.size, updated_at = excluded.updated_at",
                    (sp, size, "unaccounted residual file", now),
                )
                report["orphans"] += 1
                report["orphan_bytes"] += size

            gone = [
                r["path"]
                for r in conn.execute("SELECT path FROM orphan_reservations")
                if r["path"] not in seen
            ]
            for path in gone:
                conn.execute(
                    "DELETE FROM orphan_reservations WHERE path = ?", (path,)
                )
                report["released"] += 1
    finally:
        conn.close()
    return report
