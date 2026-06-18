"""Tests for the PendingStore persistence layer."""
from __future__ import annotations

import json
import os
import unittest
from unittest import mock

from lib import pending
from lib.pending import PendingStore
from tests.support import StoreTestCase


class TestRoundTrip(StoreTestCase):
    def setUp(self):
        self.path = self.mkdtemp() / "pending.json"

    def test_add_get_has_len_remove(self):
        store = PendingStore(self.path)
        self.assertEqual(len(store), 0)
        self.assertFalse(store.has("2", 101))
        self.assertIsNone(store.get("2", 101))

        store.add("2", 101, ["7", "13"], claim_id="c1", submitter_handle="me")
        self.assertEqual(len(store), 1)
        self.assertTrue(store.has("2", 101))
        rec = store.get("2", 101)
        self.assertEqual(rec["factors"], ["7", "13"])
        self.assertEqual(rec["claim_id"], "c1")
        self.assertEqual(rec["submitter_handle"], "me")
        self.assertEqual(rec["attempts"], 0)

        store.remove("2", 101)
        self.assertEqual(len(store), 0)
        self.assertFalse(store.has("2", 101))

    def test_persists_across_reload(self):
        store = PendingStore(self.path)
        store.add("2", 101, ["7"], claim_id="c1", submitter_handle="me",
                  attempts=3, expires_at="2030-01-01T00:00:00Z")
        rec = PendingStore(self.path).get("2", 101)  # fresh instance, reads disk
        self.assertEqual(rec["factors"], ["7"])
        self.assertEqual(rec["attempts"], 3)
        self.assertEqual(rec["claim_id"], "c1")
        self.assertEqual(rec["expires_at"], "2030-01-01T00:00:00Z")

    def test_add_replaces_factors_but_preserves_history(self):
        store = PendingStore(self.path)
        store.add("2", 101, ["7", "13", "999"], claim_id="c1")
        cached_at = store.get("2", 101)["cached_at"]
        store.add("2", 101, ["13"])  # re-cache subset; metadata not passed
        rec = store.get("2", 101)
        self.assertEqual(rec["factors"], ["13"])       # replaced
        self.assertEqual(rec["cached_at"], cached_at)   # preserved
        self.assertEqual(rec["claim_id"], "c1")         # preserved

    def test_attempts_sets_value_and_updates_last_attempt(self):
        store = PendingStore(self.path)
        store.add("2", 101, ["7"], claim_id="c1")
        self.assertIsNone(store.get("2", 101)["last_attempt"])
        store.add("2", 101, ["7"], attempts=5)
        rec = store.get("2", 101)
        self.assertEqual(rec["attempts"], 5)
        self.assertIsNotNone(rec["last_attempt"])

    def test_set_expiry(self):
        store = PendingStore(self.path)
        store.add("2", 101, ["7"], claim_id="c1", expires_at="2030-01-01T00:00:00Z")
        store.set_expiry("2", 101, "2031-06-06T06:06:06Z")
        self.assertEqual(store.get("2", 101)["expires_at"], "2031-06-06T06:06:06Z")

    def test_get_returns_copy(self):
        store = PendingStore(self.path)
        store.add("2", 101, ["7"])
        store.get("2", 101)["factors"].append("tampered")
        self.assertEqual(store.get("2", 101)["factors"], ["7"])

    def test_items_returns_copies(self):
        store = PendingStore(self.path)
        store.add("2", 101, ["7"])
        store.items()[0]["factors"].append("tampered")
        self.assertEqual(store.get("2", 101)["factors"], ["7"])


class TestLoadValidation(StoreTestCase):
    def _store_with_raw(self, raw) -> PendingStore:
        path = self.mkdtemp() / "pending.json"
        path.write_text(json.dumps(raw))
        return PendingStore(path)

    def test_drops_malformed_records_on_load(self):
        good = {"base": "2", "exponent": 101, "factors": ["7"], "claim_id": "c1"}
        store = self._store_with_raw({
            "2^101": good,
            "missing_base": {"exponent": 2, "factors": ["7"]},
            "bad_exponent": {"base": "x", "exponent": "two", "factors": []},
            "bad_factor": {"base": "x", "exponent": 2, "factors": [7]},
            "bad_expires": {"base": "x", "exponent": 2, "factors": [], "expires_at": 123},
            "bad_claim": {"base": "x", "exponent": 2, "factors": [], "claim_id": 5},
            "not_a_dict": "nope",
        })
        self.assertEqual(len(store), 1)
        self.assertTrue(store.has("2", 101))

    def test_valid_accepts_none_optionals(self):
        self.assertTrue(PendingStore._valid(
            {"base": "2", "exponent": 1, "factors": ["7"],
             "expires_at": None, "claim_id": None}
        ))

    def test_corrupt_json_yields_empty_store(self):
        path = self.mkdtemp() / "pending.json"
        path.write_text("{not valid json")
        self.assertEqual(len(PendingStore(path)), 0)  # logged, not raised

    def test_non_dict_top_level_yields_empty_store(self):
        self.assertEqual(len(self._store_with_raw(["a", "b"])), 0)

    def test_missing_file_is_empty(self):
        self.assertEqual(len(PendingStore(self.mkdtemp() / "nope.json")), 0)


class TestDeadLetter(StoreTestCase):
    def setUp(self):
        self.dir = self.mkdtemp()
        self.store = PendingStore(self.dir / "pending.json")

    def test_writes_full_record_appending(self):
        big = "6365126040110985072192370612119812753312565096510491584805693"
        path = self.store.dead_letter(
            {"base": "2", "exponent": 101, "factors": [big], "attempts": 50}
        )
        self.store.dead_letter({"base": "3", "exponent": 5, "factors": ["9"]})
        self.assertEqual(path, self.dir / "pending_dropped.jsonl")
        lines = path.read_text().strip().splitlines()
        self.assertEqual(len(lines), 2)               # appended, not overwritten
        first = json.loads(lines[0])
        self.assertEqual(first["factors"], [big])     # full, untruncated
        self.assertIn("dropped_at", first)

    def test_swallows_oserror(self):
        (self.dir / "pending_dropped.jsonl").mkdir()  # make the append target fail
        self.assertIsNone(self.store.dead_letter({"base": "2", "exponent": 1, "factors": ["7"]}))


class TestAtomicPersistence(StoreTestCase):
    def setUp(self):
        self.dir = self.mkdtemp()
        self.store = PendingStore(self.dir / "pending.json")

    def test_no_temp_files_left_behind_on_success(self):
        self.store.add("2", 101, ["7"])
        self.store.remove("2", 101)
        leftovers = [p for p in os.listdir(self.dir) if p.startswith(".pending-")]
        self.assertEqual(leftovers, [])

    def test_temp_file_cleaned_up_on_write_failure(self):
        # Force the atomic rename to fail; the temp file must still be removed.
        with mock.patch.object(pending.os, "replace", side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                self.store.add("2", 101, ["7"])
        leftovers = [p for p in os.listdir(self.dir) if p.startswith(".pending-")]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
