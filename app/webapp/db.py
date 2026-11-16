"""Persistence layer for the FastAPI web platform."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from ..avro_codec import DEFAULT_SCHEMA_V1, fingerprint
from ..db_common import get_conn

_DDL = """
CREATE TABLE IF NOT EXISTS schema_versions (
    fp                  TEXT PRIMARY KEY,
    kind                TEXT NOT NULL CHECK (kind IN ('writer', 'reader')),
    version             INTEGER NOT NULL,
    schema_json         TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    UNIQUE(kind, version)
);
CREATE TABLE IF NOT EXISTS writer_state (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    effective_fp        TEXT NOT NULL,
    effective_version   INTEGER NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id                  TEXT PRIMARY KEY,
    writer_fp           TEXT NOT NULL,
    writer_version      INTEGER NOT NULL,
    payload_json        TEXT NOT NULL,
    blob_b64            TEXT NOT NULL,
    ingested_at         TEXT NOT NULL,
    source_key          TEXT,
    content_fp          TEXT
);
CREATE TABLE IF NOT EXISTS writer_switches (
    switch_id           TEXT PRIMARY KEY,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    new_fp              TEXT NOT NULL,
    schema_json         TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'rejected')),
    target              TEXT NOT NULL DEFAULT 'local',
    last_error          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reader_switches (
    switch_id           TEXT PRIMARY KEY,
    analyzer_id         TEXT NOT NULL,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    schema_json         TEXT NOT NULL,
    writer_fp           TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'rejected')),
    last_error          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS decode_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            TEXT NOT NULL,
    analyzer_id         TEXT NOT NULL,
    reader_version      INTEGER,
    decoded_ok          INTEGER NOT NULL,
    datum_json          TEXT,
    error               TEXT,
    decoded_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sim_config (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    delay_ms            INTEGER NOT NULL DEFAULT 0,
    drop_writer_switch  INTEGER NOT NULL DEFAULT 0,
    drop_reader_switch  INTEGER NOT NULL DEFAULT 0
);
"""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def init(db_path: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.executescript(_DDL)
        _migrate_events(conn)
        fp = fingerprint(DEFAULT_SCHEMA_V1)
        conn.execute(
            "INSERT OR IGNORE INTO schema_versions(fp, kind, version, schema_json, created_at) "
            "VALUES (?, 'writer', 1, ?, ?)",
            (fp, json.dumps(DEFAULT_SCHEMA_V1), _now()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO writer_state(id, effective_fp, effective_version, updated_at) "
            "VALUES (1, ?, 1, ?)", (fp, _now()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO sim_config(id, delay_ms, drop_writer_switch, "
            "drop_reader_switch) VALUES (1, 0, 0, 0)"
        )
        conn.commit()
    finally:
        conn.close()


def _migrate_events(conn: sqlite3.Connection) -> None:
    """Upgrade a pre-existing events table in place.

    Older databases have no source-key columns. Rather than rebuilding the
    table (which would change statement semantics under rebuild hazards), add
    the columns and a partial unique index. Legacy rows get NULL source keys;
    SQLite treats NULLs as distinct in a unique index, so every legacy event
    stays untouched and remains decodable.
    """
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(events)")}
    if "source_key" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN source_key TEXT")
    if "content_fp" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN content_fp TEXT")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_events_source_key "
        "ON events(source_key) WHERE source_key IS NOT NULL"
    )


# ----- schemas --------------------------------------------------------------

def register_schema(conn: sqlite3.Connection, kind: str, version: int,
                    schema: Dict[str, Any]) -> str:
    fp = fingerprint(schema)
    conn.execute(
        "INSERT OR IGNORE INTO schema_versions(fp, kind, version, schema_json, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (fp, kind, version, json.dumps(schema), _now()),
    )
    return fp


def next_version(conn: sqlite3.Connection, kind: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS m FROM schema_versions WHERE kind = ?", (kind,)
    ).fetchone()
    # Both formats start at v1 (the default schema shipped in every service),
    # so the first *switch* must propose v2.
    return max(2, row["m"] + 1)


def get_schema_by_fp(conn: sqlite3.Connection, fp: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT schema_json FROM schema_versions WHERE fp = ?", (fp,)
    ).fetchone()
    return json.loads(row["schema_json"]) if row else None


def all_writer_schemas(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT fp, version, schema_json, created_at FROM schema_versions "
        "WHERE kind = 'writer' ORDER BY version"
    ).fetchall()
    return [dict(fp=r["fp"], version=r["version"],
                 schema=json.loads(r["schema_json"]), created_at=r["created_at"])
            for r in rows]


# ----- writer state ---------------------------------------------------------

def get_writer_state(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute("SELECT * FROM writer_state WHERE id = 1").fetchone()


def set_writer_state(conn: sqlite3.Connection, fp: str, version: int) -> None:
    conn.execute(
        "UPDATE writer_state SET effective_fp = ?, effective_version = ?, updated_at = ? "
        "WHERE id = 1", (fp, version, _now()),
    )


# ----- events ---------------------------------------------------------------

def content_fingerprint(payload: Dict[str, Any]) -> str:
    """Canonical identity of event JSON content.

    Key order and whitespace do not matter. JSON numbers with the same
    mathematical value compare equal (``1`` / ``1.0``), while booleans stay
    distinct from numbers. This is what the gateway retry semantics need:
    a re-serialised but identical event is a duplicate, not a conflict.
    """
    canonical = json.dumps(_normalise(payload), sort_keys=True,
                           separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _normalise(value: Any) -> Any:
    if isinstance(value, bool):
        return {"$bool": value}
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {str(k): _normalise(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalise(v) for v in value]
    return value


def insert_event(conn: sqlite3.Connection, *, writer_fp: str, writer_version: int,
                 payload: Dict[str, Any], blob_b64: str,
                 source_key: Optional[str] = None,
                 content_fp: Optional[str] = None) -> str:
    event_id = "evt_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO events(id, writer_fp, writer_version, payload_json, blob_b64, "
        "ingested_at, source_key, content_fp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (event_id, writer_fp, writer_version, json.dumps(payload, ensure_ascii=False),
         blob_b64, _now(), source_key, content_fp),
    )
    return event_id


def insert_event_once(conn: sqlite3.Connection, *, writer_fp: str,
                      writer_version: int, payload: Dict[str, Any],
                      blob_b64: str, source_key: str, content_fp: str
                      ) -> Tuple[Optional[str], Optional[sqlite3.Row]]:
    """Idempotent insert guarded by the unique source key.

    The write lock is taken with ``BEGIN IMMEDIATE`` before the existence
    check, so concurrent same-key ingests (threads or processes) are
    serialized and exactly one row survives. The caller must have committed
    any prior read transaction on ``conn``.

    Returns ``(event_id, None)`` when this call inserted the event, or
    ``(None, existing_row)`` when the key was already present. The caller must
    compare ``existing_row['content_fp']`` before treating the replay as a
    duplicate; a differing payload is a conflict, not a duplicate.
    """
    conn.commit()  # end any implicit read transaction; BEGIN IMMEDIATE next
    conn.execute("BEGIN IMMEDIATE")
    existing = get_event_by_source_key(conn, source_key)
    if existing is not None:
        conn.commit()
        return None, existing
    event_id = "evt_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO events(id, writer_fp, writer_version, payload_json, blob_b64, "
        "ingested_at, source_key, content_fp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (event_id, writer_fp, writer_version,
         json.dumps(payload, ensure_ascii=False), blob_b64, _now(),
         source_key, content_fp),
    )
    conn.commit()
    return event_id, None


def get_event_by_source_key(conn: sqlite3.Connection,
                            source_key: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM events WHERE source_key = ?", (source_key,)
    ).fetchone()


def list_events(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM events ORDER BY ingested_at DESC, rowid DESC"
    ).fetchall()


def get_event(conn: sqlite3.Connection, event_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()


# ----- switches -------------------------------------------------------------

def insert_writer_switch(conn: sqlite3.Connection, *, version: int, base_version: int,
                         new_fp: str, schema: Dict[str, Any], target: str) -> str:
    switch_id = "wsw_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO writer_switches(switch_id, version, base_version, new_fp, schema_json, "
        "status, target, last_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', "
        "?, NULL, ?, ?)",
        (switch_id, version, base_version, new_fp, json.dumps(schema), target, _now(), _now()),
    )
    return switch_id


def update_writer_switch(conn: sqlite3.Connection, switch_id: str, status: str,
                         error: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE writer_switches SET status = ?, last_error = ?, updated_at = ? "
        "WHERE switch_id = ?", (status, error, _now(), switch_id),
    )


def get_writer_switch(conn: sqlite3.Connection, switch_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM writer_switches WHERE switch_id = ?", (switch_id,)
    ).fetchone()


def pending_writer_switches(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM writer_switches WHERE status = 'pending' ORDER BY version"
    ).fetchall()


def insert_reader_switch(conn: sqlite3.Connection, *, analyzer_id: str, version: int,
                         base_version: int, schema: Dict[str, Any], writer_fp: str) -> str:
    switch_id = "rsw_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO reader_switches(switch_id, analyzer_id, version, base_version, schema_json, "
        "writer_fp, status, last_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, "
        "'pending', NULL, ?, ?)",
        (switch_id, analyzer_id, version, base_version, json.dumps(schema),
         writer_fp, _now(), _now()),
    )
    return switch_id


def update_reader_switch(conn: sqlite3.Connection, switch_id: str, status: str,
                         error: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE reader_switches SET status = ?, last_error = ?, updated_at = ? "
        "WHERE switch_id = ?", (status, error, _now(), switch_id),
    )


def get_reader_switch(conn: sqlite3.Connection, switch_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM reader_switches WHERE switch_id = ?", (switch_id,)
    ).fetchone()


def pending_reader_switches(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM reader_switches WHERE status = 'pending' ORDER BY created_at"
    ).fetchall()


# ----- decode results -------------------------------------------------------

def insert_decode_result(conn: sqlite3.Connection, *, event_id: str, analyzer_id: str,
                         reader_version: Optional[int], decoded_ok: bool,
                         datum: Any, error: Optional[str]) -> None:
    conn.execute(
        "INSERT INTO decode_results(event_id, analyzer_id, reader_version, decoded_ok, "
        "datum_json, error, decoded_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (event_id, analyzer_id, reader_version, int(decoded_ok),
         json.dumps(datum, ensure_ascii=False) if datum is not None else None, error, _now()),
    )


def latest_decode_results(conn: sqlite3.Connection, limit: int = 20) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM decode_results ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()


# ----- simulation -----------------------------------------------------------

def get_sim(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM sim_config WHERE id = 1").fetchone()
    return {
        "delay_ms": row["delay_ms"],
        "drop_writer_switch": bool(row["drop_writer_switch"]),
        "drop_reader_switch": bool(row["drop_reader_switch"]),
    }


def set_sim(conn: sqlite3.Connection, delay_ms: int, drop_writer: bool,
            drop_reader: bool) -> None:
    conn.execute(
        "UPDATE sim_config SET delay_ms = ?, drop_writer_switch = ?, "
        "drop_reader_switch = ? WHERE id = 1",
        (max(0, int(delay_ms)), int(bool(drop_writer)), int(bool(drop_reader))),
    )


def consume_drop_flag(conn: sqlite3.Connection, flag: str) -> bool:
    """One-shot read-and-reset inside the same transaction."""
    assert flag in {"drop_writer_switch", "drop_reader_switch"}
    row = conn.execute(f"SELECT {flag} AS v FROM sim_config WHERE id = 1").fetchone()
    armed = bool(row["v"])
    if armed:
        conn.execute(f"UPDATE sim_config SET {flag} = 0 WHERE id = 1")
    return armed
