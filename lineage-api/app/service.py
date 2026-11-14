"""Lineage registration and recursive impact analysis."""

from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone

from . import cursor as cursor_codec
from . import db
from .exceptions import (
    ConflictLineageError,
    CycleDetectedError,
    InvalidCursorError,
    NotFoundLineageError,
    ValidationLineageError,
)

# Graph traversal may be deep for large lineage graphs.
_MAX_RECURSION_DEPTH = max(10_000, sys.getrecursionlimit())


# --------------------------------------------------------------------------- #
# Graph loading
# --------------------------------------------------------------------------- #

def load_graph(conn: sqlite3.Connection) -> tuple[
    dict[str, str], dict[str, list[list[str]]]
]:
    """Load every node and its candidate input groups from one snapshot."""
    kinds: dict[str, str] = {
        row["name"]: row["kind"] for row in conn.execute("SELECT name, kind FROM nodes")
    }
    groups: dict[str, list[list[str]]] = defaultdict(list)

    group_rows = conn.execute(
        "SELECT id, node_name FROM candidate_groups ORDER BY node_name, position"
    ).fetchall()
    id_to_node = {row["id"]: row["node_name"] for row in group_rows}

    if id_to_node:
        placeholders = ",".join("?" for _ in id_to_node)
        input_rows = conn.execute(
            f"SELECT group_id, input_name FROM group_inputs "
            f"WHERE group_id IN ({placeholders}) ORDER BY group_id, position",
            tuple(id_to_node.keys()),
        ).fetchall()
        inputs_by_group: dict[int, list[str]] = defaultdict(list)
        for row in input_rows:
            inputs_by_group[row["group_id"]].append(row["input_name"])
        for gid, node_name in id_to_node.items():
            groups[node_name].append(inputs_by_group.get(gid, []))

    return kinds, dict(groups)


def adjacency(
    groups: dict[str, list[list[str]]]
) -> dict[str, list[str]]:
    """Flatten candidate groups into node -> unique downstream inputs."""
    edges: dict[str, list[str]] = {}
    for node, node_groups in groups.items():
        seen: set[str] = set()
        ordered: list[str] = []
        for group in node_groups:
            for inp in group:
                if inp not in seen:
                    seen.add(inp)
                    ordered.append(inp)
        edges[node] = ordered
    return edges


# --------------------------------------------------------------------------- #
# Cycle detection (whole graph, DFS coloring)
# --------------------------------------------------------------------------- #

def find_cycle(
    kinds: dict[str, str], groups: dict[str, list[list[str]]]
) -> list[str] | None:
    """Return one cycle path (node repeated at both ends), or None."""
    edges = adjacency(groups)
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {name: WHITE for name in kinds}
    stack: list[str] = []

    def visit(node: str) -> list[str] | None:
        color[node] = GRAY
        stack.append(node)
        for nxt in edges.get(node, []):
            if color[nxt] == GRAY:
                idx = stack.index(nxt)
                return stack[idx:] + [nxt]
            if color[nxt] == WHITE:
                found = visit(nxt)
                if found is not None:
                    return found
        stack.pop()
        color[node] = BLACK
        return None

    sys.setrecursionlimit(_MAX_RECURSION_DEPTH)
    for name in sorted(kinds):
        if color[name] == WHITE:
            cycle = visit(name)
            if cycle is not None:
                return cycle
    return None


# --------------------------------------------------------------------------- #
# Batch registration (all-or-nothing)
# --------------------------------------------------------------------------- #

def register_batch(
    payload_nodes: list, db_path: str | None = None
) -> dict:
    """Validate and upsert a batch; any failure leaves the graph untouched."""
    # ---- pure payload validation (no lock held yet) ----
    seen_names: set[str] = set()
    duplicates: list[str] = []
    sources_with_inputs: list[str] = []
    empty_group_nodes: list[str] = []
    intra_group_dupes: dict[str, list[list[int]]] = {}

    for node in payload_nodes:
        if node.name in seen_names:
            duplicates.append(node.name)
        seen_names.add(node.name)

        if node.kind == "source" and node.candidate_groups:
            sources_with_inputs.append(node.name)
        if node.kind != "source" and not node.candidate_groups:
            empty_group_nodes.append(node.name)
        for i, group in enumerate(node.candidate_groups):
            if not group:
                empty_group_nodes.append(node.name)
            local_dupes = sorted(
                {inp for inp in group if group.count(inp) > 1}
            )
            if local_dupes:
                intra_group_dupes.setdefault(node.name, []).append(
                    [i, *local_dupes]
                )

    problems: dict[str, list] = {}
    if duplicates:
        problems["duplicate_names"] = sorted(set(duplicates))
    if sources_with_inputs:
        problems["source_nodes_with_inputs"] = sorted(set(sources_with_inputs))
    if empty_group_nodes:
        problems["missing_candidate_groups"] = sorted(set(empty_group_nodes))
    if intra_group_dupes:
        problems["duplicate_inputs_within_group"] = intra_group_dupes
    if problems:
        raise ValidationLineageError(
            "请求数据不合法，整批未登记", problems
        )

    with db.immediate_write(db_path) as conn:
        existing_kinds: dict[str, str] = {
            row["name"]: row["kind"]
            for row in conn.execute("SELECT name, kind FROM nodes")
        }

        # ---- node kind conflicts ----
        conflicts = sorted(
            node.name
            for node in payload_nodes
            if node.name in existing_kinds
            and existing_kinds[node.name] != node.kind
        )
        if conflicts:
            raise ConflictLineageError(
                "节点已存在且类型不同，整批未生效",
                {"kind_conflicts": conflicts},
            )

        # ---- referential integrity against post-batch node set ----
        final_nodes = dict(existing_kinds)
        for node in payload_nodes:
            final_nodes[node.name] = node.kind
        unknown: dict[str, list[str]] = {}
        for node in payload_nodes:
            missing = sorted(
                {
                    inp
                    for group in node.candidate_groups
                    for inp in group
                    if inp not in final_nodes
                }
            )
            if missing:
                unknown[node.name] = missing
        if unknown:
            raise ValidationLineageError(
                "候选输入引用了未登记的字段，整批未生效",
                {"unknown_inputs": unknown},
            )

        # ---- upsert phase 1: all nodes must exist before any edge is written ----
        for node in payload_nodes:
            conn.execute(
                "INSERT INTO nodes(name, kind) VALUES(?, ?) "
                "ON CONFLICT(name) DO UPDATE SET kind = excluded.kind",
                (node.name, node.kind),
            )

        # ---- upsert phase 2: replace candidate groups and their inputs ----
        for node in payload_nodes:
            # Replace all previously registered groups of this node.
            old_groups = conn.execute(
                "SELECT id FROM candidate_groups WHERE node_name = ?",
                (node.name,),
            ).fetchall()
            for row in old_groups:
                conn.execute(
                    "DELETE FROM group_inputs WHERE group_id = ?", (row["id"],)
                )
            conn.execute(
                "DELETE FROM candidate_groups WHERE node_name = ?", (node.name,)
            )
            for pos, group in enumerate(node.candidate_groups):
                cur = conn.execute(
                    "INSERT INTO candidate_groups(node_name, position) "
                    "VALUES(?, ?)",
                    (node.name, pos),
                )
                gid = cur.lastrowid
                for inp_pos, inp in enumerate(group):
                    conn.execute(
                        "INSERT INTO group_inputs(group_id, input_name, "
                        "position) VALUES(?, ?, ?)",
                        (gid, inp, inp_pos),
                    )

        # ---- whole-graph cycle check; roll back everything on a cycle ----
        kinds, groups = load_graph(conn)
        cycle = find_cycle(kinds, groups)
        if cycle is not None:
            raise CycleDetectedError(
                "登记将引入循环依赖，整批不生效",
                {"cycle": cycle},
            )

        # ---- immutable event: lineage and event commit atomically ----
        # Copy the definitions as submitted in THIS batch into the event log.
        # The rows are never updated afterwards, so later overwrites cannot
        # change the definition attached to an older event.
        event_cur = conn.execute(
            "INSERT INTO registration_events(created_at, node_count) "
            "VALUES(?, ?)",
            (
                datetime.now(timezone.utc).isoformat(),
                len(payload_nodes),
            ),
        )
        event_id = event_cur.lastrowid
        for pos, node in enumerate(payload_nodes):
            conn.execute(
                "INSERT INTO event_nodes(event_id, node_name, node_kind, "
                "position) VALUES(?, ?, ?, ?)",
                (event_id, node.name, node.kind, pos),
            )
            for group_pos, group in enumerate(node.candidate_groups):
                group_cur = conn.execute(
                    "INSERT INTO event_groups(event_id, node_name, position) "
                    "VALUES(?, ?, ?)",
                    (event_id, node.name, group_pos),
                )
                egid = group_cur.lastrowid
                for inp_pos, inp in enumerate(group):
                    conn.execute(
                        "INSERT INTO event_group_inputs(group_id, input_name, "
                        "position) VALUES(?, ?, ?)",
                        (egid, inp, inp_pos),
                    )

    return {
        "registered": len(payload_nodes),
        "nodes": sorted(node.name for node in payload_nodes),
        "message": "登记成功（已提交完整血缘快照）",
    }


# --------------------------------------------------------------------------- #
# Recursive impact analysis
# --------------------------------------------------------------------------- #

def analyze_impact(
    retiring: list[str], db_path: str | None = None
) -> dict:
    with db.snapshot(db_path) as conn:
        kinds, groups = load_graph(conn)

    unknown = sorted(name for name in retiring if name not in kinds)
    if unknown:
        raise NotFoundLineageError(
            "存在未登记的字段名", {"unknown": unknown}
        )

    retiring_set = set(retiring)
    sys.setrecursionlimit(_MAX_RECURSION_DEPTH)

    # availability memo: True if at least one candidate group is fully usable.
    available: dict[str, bool] = {}
    # For every unavailable node, the deepest retired causes behind it.
    root_causes: dict[str, list[str]] = {}

    def is_available(node: str) -> bool:
        if node in available:
            return available[node]
        if node in retiring_set:
            available[node] = False
            root_causes[node] = [node]
            return False

        node_groups = groups.get(node, [])
        if not node_groups:  # source node not retired
            available[node] = True
            return True

        group_ok = any(
            all(is_available(inp) for inp in group) for group in node_groups
        )
        available[node] = group_ok
        if not group_ok:
            causes: set[str] = set()
            for group in node_groups:
                for inp in group:
                    if not is_available(inp):
                        causes.update(root_causes.get(inp, [inp]))
            root_causes[node] = sorted(causes)
        return group_ok

    for name in kinds:
        is_available(name)

    unavailable = sorted(name for name, ok in available.items() if not ok)

    failed_columns = []
    for name in sorted(kinds):
        if kinds[name] != "report" or available[name]:
            continue
        failed_groups = []
        for idx, group in enumerate(groups.get(name, [])):
            missing = [
                inp for inp in group if not available[inp]
            ]
            if not missing:
                continue
            failed_groups.append(
                {
                    "group_index": idx,
                    "inputs": list(group),
                    "missing": [
                        {
                            "name": inp,
                            "kind": kinds[inp],
                            "root_causes": root_causes.get(inp, [inp]),
                        }
                        for inp in missing
                    ],
                }
            )
        failed_columns.append(
            {"name": name, "failed_groups": failed_groups}
        )

    return {
        "retiring": sorted(retiring_set),
        "failed_report_columns": failed_columns,
        "unavailable_count": len(unavailable),
        "unavailable_nodes": unavailable,
    }


# --------------------------------------------------------------------------- #
# Read helpers
# --------------------------------------------------------------------------- #

def get_graph(db_path: str | None = None) -> dict:
    with db.snapshot(db_path) as conn:
        kinds, groups = load_graph(conn)
    nodes = [
        {
            "name": name,
            "kind": kinds[name],
            "candidate_groups": groups.get(name, []),
        }
        for name in sorted(kinds)
    ]
    counts: dict[str, int] = {"source": 0, "derived": 0, "report": 0}
    for kind in kinds.values():
        counts[kind] += 1
    return {"nodes": nodes, "counts": counts}


# --------------------------------------------------------------------------- #
# Catalog sync: full snapshot on first call, ordered event batches afterwards
# --------------------------------------------------------------------------- #

def _graph_snapshot(conn: sqlite3.Connection) -> dict:
    """Serialise the live lineage graph inside an already-open snapshot."""
    kinds, groups = load_graph(conn)
    nodes = [
        {
            "name": name,
            "kind": kinds[name],
            "candidate_groups": groups.get(name, []),
        }
        for name in sorted(kinds)
    ]
    counts: dict[str, int] = {"source": 0, "derived": 0, "report": 0}
    for kind in kinds.values():
        counts[kind] += 1
    return {"nodes": nodes, "counts": counts}


def get_sync_snapshot(db_path: str | None = None) -> dict:
    """First sync request: full lineage plus a cursor for subsequent polling.

    Both the graph and the event watermark are read from one WAL snapshot
    transaction, so the returned cursor is exactly aligned with the graph:
    events committed in the same snapshot are already reflected in the graph,
    and paging from the cursor only returns strictly newer batches.
    """
    with db.snapshot(db_path) as conn:
        snapshot = _graph_snapshot(conn)
        row = conn.execute(
            "SELECT MAX(seq) AS max_seq FROM registration_events"
        ).fetchone()
        watermark = row["max_seq"] if row["max_seq"] is not None else 0
    return {
        "snapshot": snapshot,
        "batches": [],
        "next_cursor": cursor_codec.encode(watermark),
        "has_more": False,
    }


def _load_batches(
    conn: sqlite3.Connection, event_ids: list[int]
) -> list[dict]:
    """Rebuild complete batches (with their own definition copies) by seq."""
    if not event_ids:
        return []
    placeholders = ",".join("?" for _ in event_ids)

    node_rows = conn.execute(
        f"SELECT event_id, node_name, node_kind, position "
        f"FROM event_nodes WHERE event_id IN ({placeholders}) "
        f"ORDER BY event_id, position",
        tuple(event_ids),
    ).fetchall()
    group_rows = conn.execute(
        f"SELECT id, event_id, node_name, position "
        f"FROM event_groups WHERE event_id IN ({placeholders}) "
        f"ORDER BY event_id, node_name, position",
        tuple(event_ids),
    ).fetchall()
    group_ids = [row["id"] for row in group_rows]
    input_rows = []
    if group_ids:
        in_placeholders = ",".join("?" for _ in group_ids)
        input_rows = conn.execute(
            f"SELECT group_id, input_name, position "
            f"FROM event_group_inputs WHERE group_id IN ({in_placeholders}) "
            f"ORDER BY group_id, position",
            tuple(group_ids),
        ).fetchall()

    event_meta = {
        row["seq"]: row
        for row in conn.execute(
            f"SELECT seq, created_at, node_count "
            f"FROM registration_events WHERE seq IN ({placeholders})",
            tuple(event_ids),
        ).fetchall()
    }

    inputs_by_group: dict[int, list[str]] = defaultdict(list)
    for row in input_rows:
        inputs_by_group[row["group_id"]].append(row["input_name"])

    groups_by_event: dict[int, dict[str, list[list[str]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in group_rows:
        groups_by_event[row["event_id"]][row["node_name"]].append(
            inputs_by_group.get(row["id"], [])
        )

    nodes_by_event: dict[int, list[dict]] = defaultdict(list)
    for row in node_rows:
        nodes_by_event[row["event_id"]].append(
            {
                "name": row["node_name"],
                "kind": row["node_kind"],
                # Definitions are the copies captured at commit time, not the
                # node's current (possibly overwritten) definition.
                "candidate_groups": groups_by_event[row["event_id"]].get(
                    row["node_name"], []
                ),
            }
        )

    batches: list[dict] = []
    for event_id in event_ids:
        meta = event_meta[event_id]
        batches.append(
            {
                "seq": event_id,
                "created_at": meta["created_at"],
                "node_count": meta["node_count"],
                "nodes": nodes_by_event.get(event_id, []),
            }
        )
    return batches


def get_sync_batches(
    after_seq: int, limit: int, db_path: str | None = None
) -> dict:
    """Paginate committed registration batches after ``after_seq``.

    Rows are fetched strictly by monotonically increasing ``seq``; the query
    asks for ``limit + 1`` rows so the page boundary can be detected without
    ever splitting a batch. All reads run in one snapshot transaction, hence a
    sequence of calls — each resuming from the previous ``next_cursor`` —
    delivers every committed batch exactly once, even while registrations run
    concurrently.
    """
    with db.snapshot(db_path) as conn:
        rows = conn.execute(
            "SELECT seq FROM registration_events WHERE seq > ? "
            "ORDER BY seq ASC LIMIT ?",
            (after_seq, limit + 1),
        ).fetchall()
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        event_ids = [row["seq"] for row in page_rows]
        batches = _load_batches(conn, event_ids)

    last_seq = event_ids[-1] if event_ids else after_seq
    return {
        "snapshot": None,
        "batches": batches,
        "next_cursor": cursor_codec.encode(last_seq),
        "has_more": has_more,
    }


def sync(
    cursor_token: str | None, limit: int, db_path: str | None = None
) -> dict:
    """Entry point for ``GET /lineage/sync``.

    No cursor (or the literal ``start`` marker) returns the full lineage
    snapshot and the first cursor; any other token resumes ordered batch
    pagination from that point.
    """
    if cursor_token is None or cursor_token == "start":
        return get_sync_snapshot(db_path)
    try:
        after_seq = cursor_codec.decode(cursor_token)
    except ValueError as exc:
        raise InvalidCursorError(
            "游标无效或已损坏，请从最近一次返回的 next_cursor 继续",
            {"cursor": cursor_token},
        ) from exc
    return get_sync_batches(after_seq, limit, db_path)
