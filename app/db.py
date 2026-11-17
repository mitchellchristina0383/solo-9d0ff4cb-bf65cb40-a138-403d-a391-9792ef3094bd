"""SQLite access helpers.

WAL mode allows concurrent readers with a single writer across processes;
busy_timeout makes the loser of a write race wait instead of failing.
Connections run in autocommit mode (isolation_level=None); multi-statement
state changes are wrapped in explicit ``BEGIN IMMEDIATE`` transactions (see
``app.quota``), so they are atomic across processes.
"""
from __future__ import annotations

import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    filename        TEXT NOT NULL,
    size            INTEGER NOT NULL,
    sha256          TEXT NOT NULL,
    chunk_size      INTEGER NOT NULL,
    total_chunks    INTEGER NOT NULL,
    state           TEXT NOT NULL DEFAULT 'uploading'
                    CHECK (state IN ('uploading', 'published', 'failed')),
    created_at      TEXT NOT NULL,
    published_at    TEXT,
    -- Capacity accounting (see app/quota.py):
    --   reserved_bytes: peak space promised at creation (2 * size) while the
    --                   task is still 'uploading'; 0 once it reaches a
    --                   terminal state and its usage is settled.
    --   retained_bytes: bytes actually kept on disk after settlement
    --                   (published product, or chunks kept by a failed task).
    reserved_bytes  INTEGER NOT NULL DEFAULT 0,
    retained_bytes  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chunks (
    task_id     TEXT NOT NULL REFERENCES tasks(id),
    chunk_index INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL,
    PRIMARY KEY (task_id, chunk_index)
);

-- Residual files found on disk that no live task accounts for. They keep
-- charging capacity until removed, so a crash can never "free" space that
-- is still physically occupied.
CREATE TABLE IF NOT EXISTS orphan_reservations (
    path        TEXT PRIMARY KEY,
    size        INTEGER NOT NULL,
    reason      TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# Columns added after the first release; applied with ADD COLUMN so existing
# data volumes (old tasks must keep working) are migrated in place.
_ADDED_COLUMNS = (
    ("reserved_bytes", "INTEGER NOT NULL DEFAULT 0"),
    ("retained_bytes", "INTEGER NOT NULL DEFAULT 0"),
)


def connect(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path), timeout=30.0, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    # busy_timeout must be set BEFORE journal_mode: switching to WAL needs
    # a brief exclusive lock, and without the timeout a process starting
    # concurrently with others fails immediately with "database is locked".
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    _set_wal_with_retry(conn)
    return conn


def _set_wal_with_retry(conn, attempts: int = 5) -> None:
    last = None
    for i in range(attempts):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as exc:  # locked during startup races
            last = exc
            time.sleep(0.1 * (i + 1))
    raise last


def init_db(db_path) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        existing = {r["name"] for r in conn.execute("PRAGMA table_info(tasks)")}
        for name, decl in _ADDED_COLUMNS:
            if name not in existing:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {name} {decl}")
    finally:
        conn.close()
