"""Concurrency tests for the catalog sync API against a real uvicorn server.

Guarantees under test while registrations run concurrently with sync reads:

* a continuous cursor walk delivers every committed batch exactly once
  (no missing and no duplicate batches, strict seq ordering);
* batches always stay intact — every event carries its full node list;
* historical events keep the definition captured at commit time, even though
  the same field is overwritten by later batches;
* failed (rejected) batches never produce an event;
* the initial snapshot and its cursor come from one consistent snapshot.
"""

import socket
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import cursor as cursor_mod  # noqa: E402
from app import db  # noqa: E402
from app.main import app  # noqa: E402


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def live_server(tmp_path, monkeypatch):
    db_file = tmp_path / "sync_concurrency.db"
    monkeypatch.setattr(db, "DEFAULT_DB_PATH", str(db_file))
    db.init_db(str(db_file))

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            httpx.get(f"{base_url}/health", timeout=1)
            break
        except httpx.HTTPError:
            time.sleep(0.05)
    else:
        pytest.fail("uvicorn server did not start")

    yield base_url
    server.should_exit = True
    thread.join(timeout=5)


def test_concurrent_writes_and_cursor_walk_never_skips_or_repeats(
    live_server,
):
    base_url = live_server
    total = 16

    # A shared report column gets overwritten in every single batch, while each
    # batch also adds a unique source that labels the batch.
    def good_batch(i: int) -> dict:
        return {
            "nodes": [
                {"name": f"w_{i}", "kind": "source"},
                {
                    "name": "shared_report",
                    "kind": "report",
                    "candidate_groups": [[f"w_{i}"]],
                },
            ]
        }

    def bad_batch(i: int) -> dict:
        # Cyclic: must be rejected (409) and leave no event behind.
        return {
            "nodes": [
                {"name": f"bad_{i}_x", "kind": "derived",
                 "candidate_groups": [[f"bad_{i}_y"]]},
                {"name": f"bad_{i}_y", "kind": "derived",
                 "candidate_groups": [[f"bad_{i}_x"]]},
            ]
        }

    errors: list[Exception] = []

    def writer():
        for i in range(total):
            try:
                r = httpx.post(
                    f"{base_url}/lineage/register",
                    json=good_batch(i),
                    timeout=15,
                )
                assert r.status_code == 201, r.text
                # Every few batches, interleave a rejected registration.
                if i % 4 == 1:
                    bad = httpx.post(
                        f"{base_url}/lineage/register",
                        json=bad_batch(i),
                        timeout=15,
                    )
                    assert bad.status_code == 409, bad.text
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)
            time.sleep(0.002)

    def reader(delivered: list[dict], state: dict):
        # A fresh client per request deliberately avoids pooled keep-alive
        # connection reuse while hammering the server concurrently.
        initial = httpx.get(
            f"{base_url}/lineage/sync", timeout=10
        ).json()
        cursor = initial["next_cursor"]
        # Batches already reflected in the initial snapshot are NOT replayed;
        # paging only delivers events strictly after this watermark.
        watermark = cursor_mod.decode(cursor)
        expected = total - watermark
        state["watermark"] = watermark
        state["last_cursor"] = cursor
        # Drain until every batch after the watermark has been observed.
        # Count-based draining (rather than an event + empty-page check)
        # avoids the race where the last commit lands just after an empty poll.
        deadline = time.time() + 45
        while len(delivered) < expected:
            if time.time() >= deadline:
                state["done"] = False
                return
            try:
                with httpx.Client(
                    timeout=httpx.Timeout(
                        connect=3.0, read=5.0, write=5.0, pool=3.0
                    ),
                ) as client:
                    page = client.get(
                        f"{base_url}/lineage/sync",
                        params={"cursor": cursor, "limit": 3},
                    ).json()
                    # Batches must never be split: every event keeps both
                    # nodes.
                    for batch in page["batches"]:
                        assert batch["node_count"] == 2
                        assert len(batch["nodes"]) == 2
                        # No event at or before the snapshot watermark may be
                        # replayed through the paging channel.
                        assert batch["seq"] > watermark
                        names = {n["name"] for n in batch["nodes"]}
                        marker = next(
                            n for n in names if n.startswith("w_")
                        )
                        report = next(
                            n for n in batch["nodes"]
                            if n["name"] == "shared_report"
                        )
                        # Historical definition must match the marker field
                        # that existed at commit time — never the current one.
                        assert report["candidate_groups"] == [[marker]]
                        delivered.append(batch)
                    cursor = page["next_cursor"]
                    state["last_cursor"] = cursor
                    if (
                        len(delivered) < expected
                        and not page["has_more"]
                    ):
                        time.sleep(0.005)
            except Exception as exc:  # pragma: no cover - failure path
                state["done"] = False
                errors.append(exc)
                return
        state["done"] = True

    delivered: list[dict] = []
    state: dict = {"done": False}
    reader_thread = threading.Thread(
        target=reader, args=(delivered, state), daemon=True
    )
    writer_thread = threading.Thread(target=writer, daemon=True)
    writer_thread.start()
    reader_thread.start()
    writer_thread.join(timeout=60)
    reader_thread.join(timeout=30)

    assert not writer_thread.is_alive(), "writer thread did not finish"
    assert not errors, [repr(e) for e in errors[:5]]
    assert state["done"] is True, "reader did not drain all batches in time"

    # Events inside the initial snapshot are not replayed; the paging channel
    # delivers exactly the batches committed after that watermark.
    watermark = state["watermark"]
    expected = total - watermark
    assert len(delivered) == expected
    seqs = [b["seq"] for b in delivered]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == expected  # no duplicates
    assert all(s > watermark for s in seqs)  # none overlap the snapshot

    # Snapshot (batches 1..watermark) plus pages (watermark+1..total) cover
    # every committed batch exactly once.
    assert seqs == list(range(watermark + 1, total + 1))

    markers = [
        next(n["name"] for n in b["nodes"] if n["name"].startswith("w_"))
        for b in delivered
    ]
    marker_indexes = sorted(int(m.split("_")[1]) for m in markers)
    assert marker_indexes == list(range(watermark, total))

    # The live graph keeps only the newest definition of the shared column.
    graph = httpx.get(f"{base_url}/lineage/graph", timeout=15).json()
    shared = next(
        n for n in graph["nodes"] if n["name"] == "shared_report"
    )
    assert shared["candidate_groups"] == [[f"w_{total - 1}"]]
    # Rejected nodes never made it into the graph either.
    assert not any(
        n["name"].startswith("bad_") for n in graph["nodes"]
    )

    # Polling once more from the last cursor is a stable empty page.
    tail = httpx.get(
        f"{base_url}/lineage/sync",
        params={"cursor": state["last_cursor"]},
        timeout=15,
    ).json()
    assert tail["batches"] == []
    assert tail["has_more"] is False


def test_snapshot_and_cursor_are_consistent_under_concurrent_writes(
    live_server,
):
    base_url = live_server
    stop = threading.Event()
    errors: list[Exception] = []

    def writer():
        i = 0
        while not stop.is_set():
            try:
                r = httpx.post(
                    f"{base_url}/lineage/register",
                    json={
                        "nodes": [
                            {"name": f"c_{i}", "kind": "source"},
                        ]
                    },
                    timeout=15,
                )
                assert r.status_code == 201, r.text
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)
            i += 1
            time.sleep(0.002)

    writer_thread = threading.Thread(target=writer, daemon=True)
    writer_thread.start()

    try:
        for _ in range(60):
            data = httpx.get(f"{base_url}/lineage/sync", timeout=15).json()
            snapshot_count = data["snapshot"]["counts"]["source"]
            # Everything up to the returned cursor is already reflected in the
            # returned snapshot; paging from it must not return older batches.
            page = httpx.get(
                f"{base_url}/lineage/sync",
                params={"cursor": data["next_cursor"]},
                timeout=15,
            ).json()
            for batch in page["batches"]:
                # Any batch delivered after the watermark was minted must only
                # contain nodes beyond the snapshot's committed set.
                for node in batch["nodes"]:
                    if node["kind"] == "source":
                        # Sources are uniquely named per batch here; a batch
                        # already inside the snapshot would make this fail.
                        assert node["name"] not in {
                            n["name"] for n in data["snapshot"]["nodes"]
                        }
            # Snapshot self-consistency: node list matches counts.
            assert len(data["snapshot"]["nodes"]) >= snapshot_count
            time.sleep(0.01)
    finally:
        stop.set()
        writer_thread.join(timeout=30)

    assert not errors, [repr(e) for e in errors[:5]]
