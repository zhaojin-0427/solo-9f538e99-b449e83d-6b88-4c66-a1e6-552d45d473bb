"""Functional tests for the catalog sync API (snapshot + event paging)."""


def _register(client, nodes, expected=201):
    resp = client.post("/lineage/register", json={"nodes": nodes})
    assert resp.status_code == expected, resp.text
    return resp.json()


def _drain(client, cursor, limit=2):
    """Page through every batch starting at ``cursor``; return (batches, meta)."""
    seen = []
    while True:
        resp = client.get(
            "/lineage/sync", params={"cursor": cursor, "limit": limit}
        )
        assert resp.status_code == 200, resp.text
        page = resp.json()
        assert page["snapshot"] is None
        seen.extend(page["batches"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            # One more poll must be an empty, stable page.
            tail = client.get(
                "/lineage/sync", params={"cursor": cursor, "limit": limit}
            ).json()
            assert tail["batches"] == []
            assert tail["has_more"] is False
            assert tail["next_cursor"] == cursor
            return seen, page


def test_first_request_on_empty_db_returns_snapshot_and_cursor(client):
    resp = client.get("/lineage/sync")
    assert resp.status_code == 200
    data = resp.json()
    assert data["batches"] == []
    assert data["has_more"] is False
    assert data["snapshot"] == {"nodes": [], "counts": {
        "source": 0, "derived": 0, "report": 0
    }}
    assert isinstance(data["next_cursor"], str) and data["next_cursor"]


def test_start_marker_is_equivalent_to_no_cursor(client):
    bare = client.get("/lineage/sync").json()
    marker = client.get("/lineage/sync", params={"cursor": "start"}).json()
    assert marker["snapshot"] == bare["snapshot"]
    assert marker["next_cursor"] == bare["next_cursor"]


def test_snapshot_reflects_committed_batches_and_cursor_aligns(client):
    _register(client, [
        {"name": "s1", "kind": "source"},
        {"name": "r1", "kind": "report", "candidate_groups": [["s1"]]},
    ])
    _register(client, [
        {"name": "s2", "kind": "source"},
        {"name": "r1", "kind": "report", "candidate_groups": [["s2"]]},
    ])

    data = client.get("/lineage/sync").json()
    # Snapshot is the live lineage: r1 now points at s2.
    graph = client.get("/lineage/graph").json()
    assert data["snapshot"] == graph
    nodes = {n["name"]: n for n in data["snapshot"]["nodes"]}
    assert nodes["r1"]["candidate_groups"] == [["s2"]]

    # The cursor already covers both committed batches.
    batches, _ = _drain(client, data["next_cursor"])
    assert batches == []

    # A later committed batch becomes visible on the very next poll.
    _register(client, [
        {"name": "s3", "kind": "source"},
        {"name": "r1", "kind": "report", "candidate_groups": [["s3"]]},
    ])
    page = client.get(
        "/lineage/sync", params={"cursor": data["next_cursor"]}
    ).json()
    assert len(page["batches"]) == 1
    assert page["batches"][0]["nodes"][0]["name"] == "s3"


def test_full_pagination_walkthrough(client):
    # Capture the initial cursor before any registration.
    initial_cursor = client.get("/lineage/sync").json()["next_cursor"]

    for i in range(5):
        _register(client, [{"name": f"src_{i}", "kind": "source"}])

    batches, last_page = _drain(client, initial_cursor, limit=2)
    # Five distinct batches, strictly ordered by seq, none split or repeated.
    seqs = [b["seq"] for b in batches]
    assert seqs == sorted(seqs)
    assert len(seqs) == len(set(seqs)) == 5
    assert [b["nodes"][0]["name"] for b in batches] == [
        f"src_{i}" for i in range(5)
    ]
    for b in batches:
        assert b["node_count"] == 1
        assert b["created_at"]
    assert last_page["has_more"] is False


def test_batch_keeps_overwritten_definition_not_current_one(client):
    _register(client, [
        {"name": "s1", "kind": "source"},
        {"name": "r1", "kind": "report", "candidate_groups": [["s1"]]},
    ])
    cursor_before = client.get("/lineage/sync").json()["next_cursor"]

    # Overwrite r1 twice.
    _register(client, [
        {"name": "s2", "kind": "source"},
        {"name": "r1", "kind": "report", "candidate_groups": [["s2"]]},
    ])
    _register(client, [
        {"name": "s3", "kind": "source"},
        {"name": "r1", "kind": "report", "candidate_groups": [["s3"]]},
    ])

    batches, _ = _drain(client, cursor_before)
    assert len(batches) == 2
    by_seq = {b["seq"]: b for b in batches}

    def r1_of(batch):
        return next(n for n in batch["nodes"] if n["name"] == "r1")

    # Historical events freeze the definition submitted at their commit time.
    assert r1_of(batches[0])["candidate_groups"] == [["s2"]]
    assert r1_of(batches[1])["candidate_groups"] == [["s3"]]
    # The live graph only knows the newest definition.
    graph = {
        n["name"]: n
        for n in client.get("/lineage/graph").json()["nodes"]
    }
    assert graph["r1"]["candidate_groups"] == [["s3"]]
    # seq values are globally monotonic across batches.
    assert sorted(by_seq) == list(by_seq)


def test_failed_batch_produces_no_event(client):
    _register(client, [{"name": "s1", "kind": "source"}])
    cursor_before = client.get("/lineage/sync").json()["next_cursor"]

    # A cyclic batch must roll back completely, including any event row.
    _register(client, [
        {"name": "d_x", "kind": "derived",
         "candidate_groups": [["d_y"]]},
        {"name": "d_y", "kind": "derived",
         "candidate_groups": [["d_x"]]},
    ], expected=409)
    # A validation failure must be event-less as well.
    _register(client, [
        {"name": "r_bad", "kind": "report"},
    ], expected=422)

    _register(client, [{"name": "s2", "kind": "source"}])

    batches, _ = _drain(client, cursor_before)
    assert len(batches) == 1
    assert [n["name"] for n in batches[0]["nodes"]] == ["s2"]


def test_invalid_cursor_returns_400(client):
    for token in ["not-a-cursor", "%%%", ""]:
        resp = client.get("/lineage/sync", params={"cursor": token})
        assert resp.status_code == 400, resp.text
        assert resp.json()["error"] == "invalid_cursor"


def test_limit_is_validated(client):
    assert client.get(
        "/lineage/sync", params={"limit": 0}
    ).status_code == 422
    assert client.get(
        "/lineage/sync", params={"limit": 1001}
    ).status_code == 422
