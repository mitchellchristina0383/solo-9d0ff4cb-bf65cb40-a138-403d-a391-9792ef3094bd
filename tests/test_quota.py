"""Capacity reservation tests.

Covers admission rejection (HTTP 507, nothing left behind), peak
reservation vs. settlement after publish/failure, the never-exceed-cap
guarantee under concurrent creation, no phantom release after write
failures and crashes, startup reconciliation of residual files, and
runtime cap reduction with old tasks still completing.
"""
import hashlib
import random
import sys
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import storage as storage_mod  # noqa: E402
from app.main import CHUNK_SIZE, create_app  # noqa: E402


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def make_client(tmp_path: Path, limit) -> TestClient:
    return TestClient(create_app(tmp_path / "data", storage_limit=limit))


def make_task(client: TestClient, data: bytes, sha=None):
    return client.post(
        "/api/tasks",
        json={
            "filename": "pkg.bin",
            "size": len(data),
            "sha256": sha or sha256(data),
        },
    )


def chunks_of(data: bytes):
    return [data[i : i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]


def upload_all(client, tid, data):
    for i, c in enumerate(chunks_of(data)):
        r = client.put(f"/api/tasks/{tid}/chunks/{i}", content=c)
        assert r.status_code == 200, (i, r.text)


def cap_of(client) -> dict:
    return client.get("/api/capacity").json()


DATA = random.Random(42).randbytes(2 * CHUNK_SIZE + 123)  # 3 chunks, ~2 MiB


def test_parse_limit_suffixes():
    from app.quota import parse_limit

    assert parse_limit(None) == 0
    assert parse_limit("0") == 0
    assert parse_limit("unlimited") == 0
    assert parse_limit("1024") == 1024
    assert parse_limit("512MiB") == 512 * 1024 ** 2
    assert parse_limit("2g") == 2 * 1024 ** 3
    with pytest.raises(Exception):
        parse_limit("not-a-size")


def test_admission_rejects_and_leaves_nothing(tmp_path):
    # Cap 5 MiB; one DATA task promises 2*~2MiB ≈ 4.2 MiB, a second ~8.4 MiB.
    c = make_client(tmp_path, 5 * 1024 ** 2)
    r1 = make_task(c, DATA)
    assert r1.status_code == 201
    tid = r1.json()["task_id"]
    assert r1.json()["reserved_bytes"] == 2 * len(DATA)

    r2 = make_task(c, DATA)
    assert r2.status_code == 507
    body = r2.json()
    assert body["error"] == "insufficient_storage"
    assert body["needed_bytes"] == 2 * len(DATA)
    assert body["available_bytes"] == 5 * 1024 ** 2 - 2 * len(DATA)

    # Rejection leaves no task row and no files behind.
    listing = c.get("/api/tasks").json()["tasks"]
    assert [t["task_id"] for t in listing] == [tid]
    data_root = tmp_path / "data"
    assert not list((data_root / "tmp").iterdir())
    chunk_dirs = list((data_root / "chunks").iterdir())
    # Chunk directories are created lazily; any existing one belongs to
    # the admitted task and contains nothing.
    assert all(p.name == tid for p in chunk_dirs)
    assert all(not any(p.iterdir()) for p in chunk_dirs)


def test_capacity_endpoint_reports_remaining(tmp_path):
    c = make_client(tmp_path, 10 * 1024 ** 2)
    snap = cap_of(c)
    assert snap["limit_bytes"] == 10 * 1024 ** 2
    assert snap["available_bytes"] == snap["limit_bytes"]
    assert snap["reserved_bytes"] == 0 and snap["over_limit"] is False

    make_task(c, DATA)
    snap = cap_of(c)
    assert snap["reserved_bytes"] == 2 * len(DATA)
    assert snap["available_bytes"] == snap["limit_bytes"] - 2 * len(DATA)
    assert snap["tasks"]["uploading"]["count"] == 1


def test_publish_settles_to_product_and_frees_peak(tmp_path):
    c = make_client(tmp_path, 10 * 1024 ** 2)
    tid = make_task(c, DATA).json()["task_id"]
    upload_all(c, tid, DATA)
    before = cap_of(c)
    assert before["reserved_bytes"] == 2 * len(DATA)

    assert c.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"
    after = cap_of(c)
    # Peak promise replaced by the single product footprint: no uploading
    # reservation remains, total committed equals the product's size.
    assert after["tasks"]["uploading"]["reserved_bytes"] == 0
    assert after["reserved_bytes"] == len(DATA)
    assert after["tasks"]["published"]["retained_bytes"] == len(DATA)
    assert after["disk_used_bytes"] >= len(DATA)
    # Chunk directory removed; only the product remains.
    assert not (tmp_path / "data" / "chunks" / tid).exists()
    assert (tmp_path / "data" / "published" / tid).stat().st_size == len(DATA)
    # The freed peak (was 2*size) allows admitting another same-sized task.
    assert make_task(c, DATA).status_code == 201


def test_failed_digest_settles_to_kept_chunks(tmp_path):
    c = make_client(tmp_path, 10 * 1024 ** 2)
    tid = make_task(c, DATA, sha=sha256(b"wrong")).json()["task_id"]
    upload_all(c, tid, DATA)
    r = c.post(f"/api/tasks/{tid}/finalize")
    assert r.status_code == 409 and r.json()["state"] == "failed"

    snap = cap_of(c)
    # No uploading promise remains; the kept chunks charge as retained.
    assert snap["tasks"]["uploading"]["reserved_bytes"] == 0
    assert snap["reserved_bytes"] == len(DATA)
    assert snap["tasks"]["failed"]["retained_bytes"] == len(DATA)
    assert snap["disk_used_bytes"] >= len(DATA)
    # Chunk evidence is still on disk and still charges capacity.
    assert len(list((tmp_path / "data" / "chunks" / tid).iterdir())) == 3


def test_disk_failure_does_not_release_capacity(tmp_path, monkeypatch):
    c = make_client(tmp_path, 10 * 1024 ** 2)
    tid = make_task(c, DATA).json()["task_id"]
    promised = cap_of(c)["reserved_bytes"]
    assert promised == 2 * len(DATA)

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(storage_mod, "replace", boom)
    r = c.put(f"/api/tasks/{tid}/chunks/0", content=chunks_of(DATA)[0])
    assert r.status_code == 500
    monkeypatch.undo()

    # The failed chunk write neither consumed nor released any reservation.
    snap = cap_of(c)
    assert snap["reserved_bytes"] == promised
    assert c.get(f"/api/tasks/{tid}").json()["received_chunks"] == []


def test_restart_keeps_reservations_and_reconciles_orphans(tmp_path):
    c1 = make_client(tmp_path, 10 * 1024 ** 2)
    tid = make_task(c1, DATA).json()["task_id"]
    upload_all(c1, tid, DATA)
    promised = 2 * len(DATA)

    # Simulate a crash leftover: an unaccounted file in the data root.
    leftover = tmp_path / "data" / "tmp" / ".pub-crash-leftover"
    leftover.write_bytes(b"x" * 1234)

    c2 = make_client(tmp_path, 10 * 1024 ** 2)  # restart: reconcile runs
    snap = cap_of(c2)
    # The uploading task's promise survives the restart unchanged.
    assert snap["reserved_bytes"] == promised + 1234
    assert snap["orphan_files"] == {"count": 1, "bytes": 1234}

    # Orphan physically removed -> its reservation is released on next start.
    leftover.unlink()
    c3 = make_client(tmp_path, 10 * 1024 ** 2)
    snap3 = cap_of(c3)
    assert snap3["orphan_files"]["count"] == 0
    assert snap3["reserved_bytes"] == promised
    # The admitted task can still finish and publish after restarts.
    assert c3.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"
    assert c3.get(f"/api/tasks/{tid}/download").content == DATA


def test_old_db_without_reservations_is_backfilled(tmp_path):
    # Create a volume with the legacy schema (no capacity columns), insert
    # an old-style task, then start the new app against it.
    import sqlite3

    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)
    legacy = sqlite3.connect(str(data_dir / "uploader.db"))
    legacy.executescript(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, filename TEXT NOT NULL, size INTEGER NOT NULL,
            sha256 TEXT NOT NULL, chunk_size INTEGER NOT NULL,
            total_chunks INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'uploading',
            created_at TEXT NOT NULL, published_at TEXT);
        CREATE TABLE chunks (
            task_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            sha256 TEXT NOT NULL, size INTEGER NOT NULL,
            PRIMARY KEY (task_id, chunk_index));
        """
    )
    legacy.execute(
        "INSERT INTO tasks VALUES ('oldtask','f.bin',?,?,1048576,3,"
        "'uploading','2026-01-01T00:00:00+00:00',NULL)",
        (len(DATA), sha256(DATA)),
    )
    legacy.commit()
    legacy.close()

    c = make_client(tmp_path, 10 * 1024 ** 2)
    st = c.get("/api/tasks/oldtask").json()
    assert st["state"] == "uploading"  # old task still usable
    snap = cap_of(c)
    assert snap["reserved_bytes"] == 2 * len(DATA)  # promise backfilled

    # And it can complete normally through the new code path.
    upload_all(c, "oldtask", DATA)
    assert c.post("/api/tasks/oldtask/finalize").json()["state"] == "published"
    assert c.get("/api/tasks/oldtask/download").content == DATA


def test_cap_lowered_pauses_new_admission_but_old_tasks_finish(tmp_path):
    c1 = make_client(tmp_path, 10 * 1024 ** 2)
    tid = make_task(c1, DATA).json()["task_id"]
    upload_all(c1, tid, DATA)

    # Operator lowers the cap below the current 2*size promise but above
    # the size of one settled product.
    low_cap = len(DATA) + 4096
    c2 = make_client(tmp_path, low_cap)  # same data dir, smaller cap
    snap = cap_of(c2)
    assert snap["over_limit"] is True and snap["available_bytes"] == 0

    # New tasks are refused; nothing is created.
    r = make_task(c2, b"small")
    assert r.status_code == 507
    assert all(t["task_id"] == tid for t in c2.get("/api/tasks").json()["tasks"])

    # The already-admitted task still resumes, publishes and downloads.
    assert c2.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"
    assert c2.get(f"/api/tasks/{tid}/download").content == DATA
    # After settlement the product (size bytes) fits the lowered cap.
    snap2 = cap_of(c2)
    assert snap2["over_limit"] is False
    assert snap2["available_bytes"] == low_cap - len(DATA)
    # A new task that fits the remaining space is admitted...
    tiny = b"x" * 100
    assert make_task(c2, tiny).status_code == 201
    # ...big ones stay blocked.
    assert make_task(c2, DATA).status_code == 507
    # The already published package is still downloadable.
    assert c2.get(f"/api/tasks/{tid}/download").status_code == 200


def test_concurrent_creation_never_exceeds_cap(tmp_path):
    # Each task promises 2*size; cap 5*size admits exactly two (4*size),
    # a third would need 6*size and must be rejected.
    cap = 5 * len(DATA)
    c = make_client(tmp_path, cap)
    results = []
    lock = threading.Lock()

    def create():
        r = make_task(c, DATA)
        with lock:
            results.append(r.status_code)

    threads = [threading.Thread(target=create) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results).count(201) == 2
    assert sorted(results).count(507) == 4
    snap = cap_of(c)
    assert snap["reserved_bytes"] == 2 * 2 * len(DATA)
    assert snap["reserved_bytes"] <= cap
    assert len(c.get("/api/tasks").json()["tasks"]) == 2


def test_empty_file_needs_no_reservation(tmp_path):
    c = make_client(tmp_path, 1)  # 1-byte cap
    r = make_task(c, b"")
    assert r.status_code == 201
    assert r.json()["reserved_bytes"] == 0
    tid = r.json()["task_id"]
    assert c.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"
    assert c.get(f"/api/tasks/{tid}/download").content == b""


def test_unlimited_cap_admits_any_task(tmp_path):
    c = make_client(tmp_path, 0)
    snap = cap_of(c)
    assert snap["unlimited"] is True and snap["available_bytes"] is None
    assert make_task(c, DATA).status_code == 201


def test_chunk_cleanup_failure_keeps_reservation(tmp_path, monkeypatch):
    """If chunks cannot be deleted at publish, the promise is retained:
    capacity is never released while files still occupy the volume."""
    c = make_client(tmp_path, 10 * 1024 ** 2)
    tid = make_task(c, DATA).json()["task_id"]
    upload_all(c, tid, DATA)

    real_unlink = Path.unlink

    def fail_chunk_unlink(self, *a, **k):
        if f"{tid}" in str(self) and self.suffix == ".part":
            raise OSError("input/output error")
        return real_unlink(self, *a, **k)

    monkeypatch.setattr(Path, "unlink", fail_chunk_unlink)
    r = c.post(f"/api/tasks/{tid}/finalize")
    assert r.status_code == 200 and r.json()["state"] == "published"
    monkeypatch.undo()

    snap = cap_of(c)
    # The 2*size promise is still held because chunk files remain on disk.
    task = next(t for t in c.get("/api/tasks").json()["tasks"]
                if t["task_id"] == tid)
    assert task["reserved_bytes"] == 2 * len(DATA)
    assert task["retained_bytes"] == 0
    assert snap["reserved_bytes"] >= 2 * len(DATA)

    # After the next process start reconciliation retries the cleanup and
    # settles the task to its real product footprint.
    c2 = make_client(tmp_path, 10 * 1024 ** 2)
    snap2 = cap_of(c2)
    assert snap2["tasks"]["published"]["retained_bytes"] == len(DATA)
    assert snap2["tasks"]["uploading"]["reserved_bytes"] == 0
    assert c2.get(f"/api/tasks/{tid}/download").content == DATA


def test_reconcile_republishes_orphan_removal_releases(tmp_path):
    """An orphan reservation disappears from the ledger once the file is
    gone, but never before -- no phantom capacity release."""
    c1 = make_client(tmp_path, 10 * 1024 ** 2)
    cap_of(c1)  # ensure initialized
    leftover = tmp_path / "data" / "chunks" / "ghost-task"
    leftover.mkdir(parents=True)
    ghost = leftover / "00000000.part"
    ghost.write_bytes(b"r" * 777)

    c2 = make_client(tmp_path, 10 * 1024 ** 2)
    snap = cap_of(c2)
    assert snap["orphan_files"] == {"count": 1, "bytes": 777}
    assert snap["reserved_bytes"] == 777

    # Restart without deleting: still charged (no phantom release).
    c3 = make_client(tmp_path, 10 * 1024 ** 2)
    assert cap_of(c3)["orphan_files"]["bytes"] == 777

    ghost.unlink()
    leftover.rmdir()
    c4 = make_client(tmp_path, 10 * 1024 ** 2)
    snap4 = cap_of(c4)
    assert snap4["orphan_files"]["count"] == 0
    assert snap4["reserved_bytes"] == 0


def test_lock_and_db_files_are_not_orphans(tmp_path):
    """Service bookkeeping (flock files, SQLite WAL/SHM) must not be
    booked as capacity-charging residual files."""
    c = make_client(tmp_path, 10 * 1024 ** 2)
    tid = make_task(c, DATA).json()["task_id"]
    upload_all(c, tid, DATA)
    c.post(f"/api/tasks/{tid}/finalize")  # creates locks/<tid>.lock
    c2 = make_client(tmp_path, 10 * 1024 ** 2)  # reconcile on restart
    snap = cap_of(c2)
    assert snap["orphan_files"]["count"] == 0
    assert snap["reserved_bytes"] == len(DATA)  # only the product charges


def test_undeletable_published_chunk_keeps_charging(tmp_path, monkeypatch):
    """A chunk surviving publish that reconciliation cannot delete must be
    booked as an orphan rather than silently freeing its space."""
    c1 = make_client(tmp_path, 10 * 1024 ** 2)
    tid = make_task(c1, DATA).json()["task_id"]
    upload_all(c1, tid, DATA)
    assert c1.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"
    # Normal settle removed chunks; plant a leftover chunk file as if the
    # unlink had failed during a previous run.
    leftover_dir = tmp_path / "data" / "chunks" / tid
    leftover_dir.mkdir(parents=True, exist_ok=True)
    ghost = leftover_dir / "00000002.part"
    ghost.write_bytes(b"g" * 999)

    real_unlink = Path.unlink

    def block_ghost(self, *a, **k):
        if self == ghost:
            raise OSError("cannot remove")
        return real_unlink(self, *a, **k)

    monkeypatch.setattr(Path, "unlink", block_ghost)
    c2 = make_client(tmp_path, 10 * 1024 ** 2)  # reconcile runs at startup
    monkeypatch.undo()

    snap = cap_of(c2)
    # Product is settled normally...
    assert snap["tasks"]["published"]["retained_bytes"] == len(DATA)
    # ...and the undeletable chunk still charges via the orphan ledger.
    assert snap["orphan_files"]["count"] == 1
    assert snap["orphan_files"]["bytes"] == 999
    assert snap["reserved_bytes"] == len(DATA) + 999

    # Once the file is finally removable, the next reconcile releases it.
    ghost.unlink()
    leftover_dir.rmdir()
    c3 = make_client(tmp_path, 10 * 1024 ** 2)
    snap3 = cap_of(c3)
    assert snap3["orphan_files"]["count"] == 0
    assert snap3["reserved_bytes"] == len(DATA)
