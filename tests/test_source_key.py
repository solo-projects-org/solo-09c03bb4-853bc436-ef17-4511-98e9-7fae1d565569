"""Source-event-key idempotency tests.

Covers the gateway-retry scenario on top of the existing two-format platform:
same key + same JSON returns the first event; same key + different content is
rejected without touching the first event; retries after a write-format switch
are judged by the first content and never re-encoded; interleaved ingestion
keeps each row's original format/version; concurrent same-key ingestion keeps
exactly one row; keyless requests behave exactly as before; legacy rows and
A/B decoding keep working.

Run: PYTHONPATH=/workspace:/workspace/.pylibs \
     python3 -m unittest tests.test_source_key -v
"""
import base64
import copy
import threading
import unittest

from app.webapp import db as web_db
from app.webapp import main as web_main
from app.webapp.services import SourceConflictError

from tests.test_integration import PlatformHarness, V1, writer_v2

V2 = writer_v2()


def payload(**over):
    base = {"sample_id": "S-100", "project": "P", "measurement": 1.5}
    base.update(over)
    return base


class SourceKeyTests(unittest.TestCase):
    def setUp(self):
        self.h = PlatformHarness()
        self.web = self.h.web
        web_main.RECONCILE_INTERVAL = 3600
        web_main._bg_stop.clear()
        self._ctx = self.web
        self._ctx.__enter__()

    def tearDown(self):
        self._ctx.__exit__(None, None, None)
        web_main._bg_stop.set()

    # -- helpers -----------------------------------------------------------
    def ingest(self, body):
        return self.web.post("/api/events", json=body)

    def rows(self):
        conn = self.h.service._conn()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM events ORDER BY rowid").fetchall()]
        finally:
            conn.close()

    def stored(self, event_id):
        conn = self.h.service._conn()
        try:
            return dict(conn.execute(
                "SELECT * FROM events WHERE id = ?", (event_id,)).fetchone())
        finally:
            conn.close()

    def advance_writer(self):
        r = self.web.post("/api/switches/writer", json={"schema": V2})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["switch"]["status"], "confirmed")

    # -- tests -------------------------------------------------------------
    def test_keyless_behaviour_unchanged(self):
        a = self.ingest({"payload": payload()})
        b = self.ingest({"payload": payload()})
        self.assertEqual(a.status_code, 200)
        self.assertEqual(b.status_code, 200)
        self.assertNotEqual(a.json()["event_id"], b.json()["event_id"])
        self.assertFalse(a.json()["deduplicated"])
        self.assertIsNone(a.json()["source_key"])
        self.assertEqual(len(self.rows()), 2)
        # keyless rows carry no key at all
        self.assertTrue(all(r["source_key"] is None for r in self.rows()))
        # legacy view flags are still present
        st = self.web.get("/api/status").json()
        self.assertTrue(all(e["has_source_key"] is False for e in st["events"]))

    def test_same_key_same_content_returns_first_event(self):
        first = self.ingest({"payload": payload(), "source_key": "gw-1"}).json()
        self.assertFalse(first["deduplicated"])
        before = self.stored(first["event_id"])

        retry = self.ingest({"payload": payload(), "source_key": "gw-1"})
        self.assertEqual(retry.status_code, 200, retry.text)
        r = retry.json()
        self.assertTrue(r["ok"])
        self.assertTrue(r["deduplicated"])
        self.assertEqual(r["event_id"], first["event_id"])
        self.assertEqual(r["writer_version"], before["writer_version"])
        self.assertEqual(len(self.rows()), 1)

        after = self.stored(first["event_id"])
        # retained bytes, original format fingerprint and version unchanged
        self.assertEqual(after["blob_b64"], before["blob_b64"])
        self.assertEqual(after["writer_fp"], before["writer_fp"])
        self.assertEqual(after["writer_version"], before["writer_version"])
        self.assertEqual(after["payload_json"], before["payload_json"])
        self.assertEqual(after["ingested_at"], before["ingested_at"])

    def test_json_key_order_and_whitespace_are_equivalent(self):
        # Gateway may re-serialise the same datum in a different key order.
        first = self.ingest({
            "payload": {"sample_id": "S", "project": "P", "measurement": 1.0},
            "source_key": "gw-order"}).json()
        retry = self.ingest({
            "payload": {"measurement": 1.0, "project": "P", "sample_id": "S"},
            "source_key": "gw-order"}).json()
        self.assertTrue(retry["deduplicated"])
        self.assertEqual(retry["event_id"], first["event_id"])
        self.assertEqual(len(self.rows()), 1)

    def test_numeric_spelling_of_same_value_is_a_duplicate(self):
        first = self.ingest({
            "payload": {"sample_id": "S", "project": "P", "measurement": 1},
            "source_key": "gw-num"}).json()
        retry = self.ingest({
            "payload": {"sample_id": "S", "project": "P", "measurement": 1.0},
            "source_key": "gw-num"}).json()
        self.assertTrue(retry["deduplicated"])
        self.assertEqual(retry["event_id"], first["event_id"])
        # booleans must not be confused with numbers
        conn = self.h.service._conn()
        try:
            self.assertNotEqual(web_db.content_fingerprint({"x": True}),
                                web_db.content_fingerprint({"x": 1}))
        finally:
            conn.close()

    def test_same_key_different_content_rejected_and_old_event_intact(self):
        first = self.ingest({
            "payload": payload(measurement=1.5), "source_key": "gw-2"}).json()
        before = self.stored(first["event_id"])
        before_status = self.web.get("/api/status").json()

        r = self.ingest({
            "payload": payload(measurement=9.9), "source_key": "gw-2"})
        self.assertEqual(r.status_code, 409, r.text)
        body = r.json()
        self.assertFalse(body["ok"])
        self.assertEqual(body["existing_event_id"], first["event_id"])
        self.assertIn("gw-2", body["error"])

        after = self.stored(first["event_id"])
        self.assertEqual(after, before)
        self.assertEqual(len(self.rows()), 1)
        # and nothing was written for the rejected candidate
        st = self.web.get("/api/status").json()
        self.assertEqual(len(st["events"]), len(before_status["events"]))
        # a later identical retry still returns the first event
        ok = self.ingest({"payload": payload(), "source_key": "gw-2"}).json()
        self.assertEqual(ok["event_id"], first["event_id"])
        self.assertTrue(ok["deduplicated"])

    def test_retry_after_format_switch_is_judged_by_first_content(self):
        first = self.ingest({"payload": payload(), "source_key": "gw-3"}).json()
        self.assertEqual(first["writer_version"], 1)
        before = self.stored(first["event_id"])

        self.advance_writer()  # effective writer now v2

        retry = self.ingest({"payload": payload(), "source_key": "gw-3"}).json()
        self.assertTrue(retry["deduplicated"])
        self.assertEqual(retry["event_id"], first["event_id"])
        # reported and retained version/fp stay the original v1
        self.assertEqual(retry["writer_version"], 1)
        self.assertEqual(retry["writer_fp"], before["writer_fp"])
        after = self.stored(first["event_id"])
        self.assertEqual(after["blob_b64"], before["blob_b64"])
        self.assertEqual(after["writer_version"], 1)

        # conflict after the switch is still detected against the first content
        r = self.ingest({"payload": payload(measurement=2.0),
                         "source_key": "gw-3"})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(self.stored(first["event_id"]), before)

        # the old event is still decodable by both analyzers after the switch
        st = self.web.get("/api/status").json()
        a = self.web.post("/api/events/decode", json={
            "event_id": first["event_id"], "analyzer_id": "analyzer-a"}).json()
        self.assertTrue(a["result"]["decoded_ok"], a)
        self.assertIn("S-100", str(a["result"]["datum"]))
        self.assertEqual(
            [e for e in st["events"] if e["id"] == first["event_id"]][0]
            ["writer_version"], 1)

    def test_retry_survives_writer_that_cannot_encode_old_datum(self):
        # First event has an extra field that a *newer* incompatible-in-reverse
        # writer schema would not accept; a key retry must not try to encode.
        first = self.ingest({"payload": payload(), "source_key": "gw-4"}).json()
        before = self.stored(first["event_id"])
        conn = self.h.service._conn()
        try:
            v2_schema = copy.deepcopy(V1)
            v2_schema["fields"] = [
                {"name": "sample_id", "type": "int"},  # string payload won't fit
                {"name": "project", "type": "string"},
                {"name": "measurement", "type": "double"},
            ]
            fp = web_db.register_schema(conn, "writer", 9, v2_schema)
            web_db.set_writer_state(conn, fp, 9)
            conn.commit()
        finally:
            conn.close()

        retry = self.ingest({"payload": payload(), "source_key": "gw-4"})
        self.assertEqual(retry.status_code, 200, retry.text)
        self.assertTrue(retry.json()["deduplicated"])
        self.assertEqual(retry.json()["writer_version"], 1)
        self.assertEqual(self.stored(first["event_id"]), before)

    def test_interleaved_new_key_and_format_switch_keeps_original_format(self):
        v1_ev = self.ingest({"payload": payload(), "source_key": "gw-v1"}).json()
        self.advance_writer()
        v2_ev = self.ingest({
            "payload": payload(instrument_id="GC-1", tags=[1, 2]),
            "source_key": "gw-v2"}).json()
        # an interleaved keyless event after the switch is also v2
        plain = self.ingest({"payload": payload(
            sample_id="S-free", instrument_id="GC-2", tags=[])}).json()

        rows = {r["id"]: r for r in self.rows()}
        self.assertEqual(rows[v1_ev["event_id"]]["writer_version"], 1)
        self.assertEqual(rows[v1_ev["event_id"]]["writer_fp"],
                         self.web.get("/api/status").json()
                         ["writer_schemas"][0]["fp"])
        self.assertEqual(rows[v2_ev["event_id"]]["writer_version"], 2)
        self.assertEqual(rows[plain["event_id"]]["writer_version"], 2)
        # each blob decodes with its own recorded writer schema on both A and B
        for eid in (v1_ev["event_id"], v2_ev["event_id"], plain["event_id"]):
            for analyzer in ("analyzer-a", "analyzer-b"):
                d = self.web.post("/api/events/decode", json={
                    "event_id": eid, "analyzer_id": analyzer}).json()
                self.assertTrue(d["result"]["decoded_ok"], (eid, analyzer, d))

    def test_concurrent_same_key_keeps_exactly_one_row(self):
        results, errors = [], []

        def worker():
            try:
                results.append(self.ingest({
                    "payload": payload(), "source_key": "gw-race"}).json())
            except Exception as exc:  # pragma: no cover - test diagnostics
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 8)
        self.assertEqual(len({r["event_id"] for r in results}), 1)
        self.assertEqual(sum(1 for r in results if not r["deduplicated"]), 1)
        self.assertEqual(len(self.rows()), 1)
        # the winner's bytes are decodable
        eid = results[0]["event_id"]
        d = self.web.post("/api/events/decode", json={
            "event_id": eid, "analyzer_id": "analyzer-a"}).json()
        self.assertTrue(d["result"]["decoded_ok"])

    def test_concurrent_same_key_with_conflicting_content_keeps_one(self):
        # Exactly one content wins; the other is rejected, one row remains.
        barrier = threading.Barrier(2)
        out = {}

        def worker(name, value):
            barrier.wait()
            out[name] = self.ingest({
                "payload": payload(measurement=value),
                "source_key": "gw-race-conflict"})

        ts = [threading.Thread(target=worker, args=("a", 1.0)),
              threading.Thread(target=worker, args=("b", 2.0))]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        statuses = sorted(r.status_code for r in out.values())
        self.assertEqual(statuses, [200, 409])
        self.assertEqual(len(self.rows()), 1)

    def test_service_level_conflict_exception(self):
        self.h.service.ingest_event(payload(), source_key="gw-svc")
        with self.assertRaises(SourceConflictError):
            self.h.service.ingest_event(payload(measurement=8.0),
                                        source_key="gw-svc")

    def test_blank_source_key_falls_back_to_keyless(self):
        a = self.ingest({"payload": payload(), "source_key": "   "}).json()
        b = self.ingest({"payload": payload(), "source_key": ""}).json()
        self.assertIsNone(a["source_key"])
        self.assertFalse(a["deduplicated"])
        self.assertNotEqual(a["event_id"], b["event_id"])
        self.assertEqual(len(self.rows()), 2)

    def test_legacy_rows_still_decodable_after_migration(self):
        # Simulate a pre-existing database written by the old schema: build the
        # legacy table, then let db.init migrate it in place.
        import os
        import sqlite3
        import tempfile

        d = tempfile.mkdtemp(prefix="legacy-")
        path = os.path.join(d, "old.db")
        raw = sqlite3.connect(path)
        raw.executescript("""
        CREATE TABLE events (
            id TEXT PRIMARY KEY, writer_fp TEXT NOT NULL,
            writer_version INTEGER NOT NULL, payload_json TEXT NOT NULL,
            blob_b64 TEXT NOT NULL, ingested_at TEXT NOT NULL);
        """)
        blob = base64.b64encode(b"\x02S\x02P\x00\x00\x00\x00\x00\x00\xf8?")
        raw.execute(
            "INSERT INTO events VALUES ('evt_legacy', ?, 1, ?, ?, 't0')",
            (web_db.fingerprint(V1),
             '{"sample_id": "S", "project": "P", "measurement": 1.5}',
             blob.decode()))
        raw.commit()
        raw.close()

        web_db.init(path)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        try:
            cols = {r["name"] for r in conn.execute(
                "PRAGMA table_info(events)")}
            self.assertIn("source_key", cols)
            self.assertIn("content_fp", cols)
            row = conn.execute("SELECT * FROM events").fetchone()
            self.assertIsNone(row["source_key"])
            self.assertEqual(row["blob_b64"], blob.decode())
            # a new keyed event can coexist with the untouched legacy row
            nid, existing = web_db.insert_event_once(
                conn, writer_fp=web_db.fingerprint(V1), writer_version=1,
                payload={"sample_id": "N", "project": "P",
                         "measurement": 2.0},
                blob_b64="AAAA", source_key="gw-new", content_fp="fp")
            self.assertIsNotNone(nid)
            self.assertIsNone(existing)
        finally:
            conn.close()
        self.assertEqual(
            len(sqlite3.connect(path).execute("SELECT * FROM events").fetchall()),
            2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
