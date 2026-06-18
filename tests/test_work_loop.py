"""Tests for the work-loop submission/retry logic (offline, OpnApi stubbed)."""
from __future__ import annotations

import inspect
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch

from lib import work_loop
from lib.api import ApiError, MaxClaimsReached, NoWorkAvailable, OpnApi
from lib.config import REPO_ROOT, resolve_under_root
from lib.pending import PendingStore
from tests.support import FakeApi, StoreTestCase


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _in_hours(h: float) -> str:
    return _iso(datetime.now(timezone.utc) + timedelta(hours=h))


# Scripted submit responses.
DOWN = ApiError("factordb down", status=500)
DUP = ApiError("duplicate", status=400)
OK = {"updated_status": {"fully_factored": False}}
DONE = {"updated_status": {"fully_factored": True}}


class TestIsTransient(unittest.TestCase):
    def test_400_is_permanent(self):
        self.assertFalse(work_loop._is_transient(ApiError("invalid", status=400)))

    def test_500_429_and_network_are_transient(self):
        for status in (500, 503, 429, None):
            self.assertTrue(work_loop._is_transient(ApiError("x", status=status)))


class TestSubmitFactors(unittest.TestCase):
    def test_defers_from_first_transient_without_hammering(self):
        api = FakeApi(submit=lambda f: DOWN)
        ok, full, deferred = work_loop._submit_factors(api, "2", 101, ["13", "7", "999"], "me")
        self.assertEqual(ok, 0)
        self.assertFalse(full)
        self.assertEqual(sorted(deferred), sorted(["13", "7", "999"]))
        # Only the smallest is attempted; the rest are deferred, not retried.
        self.assertEqual(len(api.submit_calls), 1)

    def test_permanent_reject_is_dropped_not_deferred(self):
        api = FakeApi(submit=lambda f: DUP if f == "7" else OK)
        ok, full, deferred = work_loop._submit_factors(api, "2", 101, ["7", "13"], "me")
        self.assertEqual(ok, 1)
        self.assertFalse(full)
        self.assertEqual(deferred, [])
        self.assertEqual(len(api.submit_calls), 2)

    def test_stops_early_when_fully_factored(self):
        api = FakeApi(submit=lambda f: DONE)
        ok, full, deferred = work_loop._submit_factors(api, "2", 101, ["7", "13"], "me")
        self.assertEqual(ok, 1)
        self.assertTrue(full)
        self.assertEqual(deferred, [])
        self.assertEqual(len(api.submit_calls), 1)  # second skipped


class TestDrainRecord(StoreTestCase):
    def test_fully_factored_drops_without_releasing(self):
        store = self.make_store()
        store.add("2", 101, ["7"], claim_id="c1")
        api = FakeApi(submit=lambda f: DONE)
        work_loop._drain_record(api, store, "2", 101, ["7"], "me", "c1",
                                attempts=0, max_retries=50)
        self.assertFalse(store.has("2", 101))
        self.assertEqual(api.release_calls, [])  # server auto-releases

    def test_deferred_below_cap_holds_and_increments(self):
        store = self.make_store()
        api = FakeApi(submit=lambda f: DOWN)
        work_loop._drain_record(api, store, "2", 101, ["7", "13"], "workerA", "c1",
                                attempts=0, max_retries=50, expires_at="2030-01-01T00:00:00Z")
        rec = store.get("2", 101)
        self.assertIsNotNone(rec)
        self.assertEqual(rec["attempts"], 1)
        self.assertEqual(rec["submitter_handle"], "workerA")  # cached handle preserved
        self.assertEqual(rec["expires_at"], "2030-01-01T00:00:00Z")
        self.assertEqual(api.release_calls, [])  # claim held

    def test_one_below_cap_still_holds(self):
        # Boundary: attempts 48 -> next 49 < 50, so it is held, not dropped.
        store = self.make_store()
        api = FakeApi(submit=lambda f: DOWN)
        work_loop._drain_record(api, store, "2", 101, ["7"], "me", "c1",
                                attempts=48, max_retries=50)
        self.assertEqual(store.get("2", 101)["attempts"], 49)
        self.assertEqual(api.release_calls, [])

    def test_deferred_at_cap_dead_letters_and_releases(self):
        store = self.make_store()
        big = "18244158255642650386458405939979428353384900262917314967175071042147"
        api = FakeApi(submit=lambda f: DOWN)
        work_loop._drain_record(api, store, "2", 101, [big], "me", "c1",
                                attempts=49, max_retries=50)
        self.assertFalse(store.has("2", 101))          # dropped
        self.assertEqual(len(api.release_calls), 1)    # claim released
        self.assertEqual(api.release_calls[0][0], "c1")
        dropped = store.path.with_name("pending_dropped.jsonl")
        self.assertIn(big, dropped.read_text())        # full factor preserved

    def test_max_retries_zero_means_unlimited(self):
        store = self.make_store()
        api = FakeApi(submit=lambda f: DOWN)
        work_loop._drain_record(api, store, "2", 101, ["7"], "me", "c1",
                                attempts=1000, max_retries=0)
        self.assertTrue(store.has("2", 101))           # never capped
        self.assertEqual(api.release_calls, [])

    def test_permanent_reject_releases_and_drops(self):
        store = self.make_store()
        store.add("2", 101, ["7"], claim_id="c1")
        api = FakeApi(submit=lambda f: DUP)
        work_loop._drain_record(api, store, "2", 101, ["7"], "me", "c1",
                                attempts=0, max_retries=50)
        self.assertFalse(store.has("2", 101))
        self.assertEqual(len(api.release_calls), 1)

    def test_empty_factors_releases_and_drops(self):
        store = self.make_store()
        api = FakeApi(submit=lambda f: DONE)
        work_loop._drain_record(api, store, "2", 101, [], "me", "c1",
                                attempts=0, max_retries=50)
        self.assertEqual(api.submit_calls, [])          # nothing to submit
        self.assertEqual(len(api.release_calls), 1)

    def test_never_raises_on_store_write_failure(self):
        store = self.make_store()
        store._flush = mock.Mock(side_effect=OSError("disk full"))
        api = FakeApi(submit=lambda f: DOWN)
        # Must not propagate even though store.add -> _flush raises.
        work_loop._drain_record(api, store, "2", 101, ["7"], "me", "c1",
                                attempts=0, max_retries=50)

    def test_factors_preserved_in_log_when_store_write_swallowed(self):
        store = self.make_store()
        store._flush = mock.Mock(side_effect=OSError("disk full"))
        api = FakeApi(submit=lambda f: DOWN)
        with self.assertLogs("lib.work_loop", level="ERROR") as cm:
            work_loop._drain_record(api, store, "2", 101, ["primefactor123"], "me", "c1",
                                    attempts=0, max_retries=50)
        self.assertTrue(any("primefactor123" in line for line in cm.output))


class TestExtendHeldClaims(StoreTestCase):
    def test_extends_only_near_expiry_and_updates_store(self):
        store = self.make_store()
        store.add("2", 101, ["7"], claim_id="near", expires_at=_in_hours(10))
        store.add("3", 55, ["9"], claim_id="far", expires_at=_in_hours(24 * 20))
        new = "2099-01-01T00:00:00Z"
        api = FakeApi(extend=lambda cid: {"claim": {"expiresAt": new}})

        work_loop._extend_held_claims(api, store, extend_when_hours=48, extension_days=15)

        self.assertEqual([c[0] for c in api.extend_calls], ["near"])  # far one skipped
        self.assertEqual(store.get("2", 101)["expires_at"], new)

    def test_skips_records_without_claim_or_expiry(self):
        store = self.make_store()
        store.add("2", 101, ["7"])  # no claim_id, no expires_at
        api = FakeApi(extend=lambda cid: {})
        work_loop._extend_held_claims(api, store, extend_when_hours=48, extension_days=15)
        self.assertEqual(api.extend_calls, [])

    def test_corrupt_expiry_does_not_crash_and_others_proceed(self):
        store = self.make_store()
        store.add("2", 101, ["7"], claim_id="good", expires_at=_in_hours(1))
        store._data["3^5"] = {"base": "3", "exponent": 5, "factors": ["9"],
                              "claim_id": "bad", "expires_at": 12345}  # non-string
        api = FakeApi(extend=lambda cid: {"claim": {"expiresAt": "2099-01-01T00:00:00Z"}})

        work_loop._extend_held_claims(api, store, extend_when_hours=48, extension_days=15)

        self.assertEqual([c[0] for c in api.extend_calls], ["good"])  # bad skipped safely

    def test_api_error_on_one_claim_does_not_stop_others(self):
        store = self.make_store()
        store.add("2", 101, ["7"], claim_id="bad", expires_at=_in_hours(1))
        store.add("3", 55, ["9"], claim_id="good", expires_at=_in_hours(1))

        def extend(cid):
            if cid == "bad":
                raise ApiError("server error", status=500)
            return {"claim": {"expiresAt": "2099-01-01T00:00:00Z"}}

        api = FakeApi(extend=extend)
        work_loop._extend_held_claims(api, store, extend_when_hours=48, extension_days=15)

        self.assertEqual(sorted(c[0] for c in api.extend_calls), ["bad", "good"])
        self.assertEqual(store.get("3", 55)["expires_at"], "2099-01-01T00:00:00Z")


class TestPathResolution(unittest.TestCase):
    def test_relative_joins_repo_root(self):
        self.assertEqual(resolve_under_root("data/x.json"), REPO_ROOT / "data" / "x.json")

    def test_absolute_passes_through(self):
        self.assertEqual(resolve_under_root("/tmp/abs.json"), Path("/tmp/abs.json"))

    def test_tilde_expands(self):
        self.assertEqual(resolve_under_root("~/x.json"), Path.home() / "x.json")

    def test_pending_store_default(self):
        self.assertEqual(
            work_loop._pending_store_path({}),
            REPO_ROOT / "data" / "pending_submissions.json",
        )

    def test_pending_store_configured_relative(self):
        cfg = {"work_loop": {"pending_store": "foo/bar.json"}}
        self.assertEqual(work_loop._pending_store_path(cfg), REPO_ROOT / "foo" / "bar.json")

    def test_pending_store_configured_absolute(self):
        cfg = {"work_loop": {"pending_store": "/var/lib/opn/pending.json"}}
        self.assertEqual(work_loop._pending_store_path(cfg), Path("/var/lib/opn/pending.json"))


class TestFakeApiContract(unittest.TestCase):
    def test_fakeapi_matches_opnapi_call_surface(self):
        # Guard against the hand-written stub drifting from the real client.
        for name in ("submit_factor", "extend_claim", "release_claim", "self_assign"):
            real = list(inspect.signature(getattr(OpnApi, name)).parameters)
            fake = list(inspect.signature(getattr(FakeApi, name)).parameters)
            self.assertEqual(real, fake, name)


CLAIM = {"id": "claim-1", "expiresAt": "2030-01-01T00:00:00Z"}
ENTRY = {"baseString": "2", "base": 2, "exponent": 101,
         "numberToFactor": "143", "digitCount": 3, "priority": 10}


class TestRunLoop(StoreTestCase):
    """Integration tests for run() — the loop that wires the units together."""

    def _run(self, *, self_assign, submit=None, extend=None,
             yafu=(0, ["7", "13"], ""), seed=None, once=True):
        store_path = self.mkdtemp() / "pending.json"
        if seed:
            seed_store = PendingStore(store_path)
            for rec in seed:
                seed_store.add(**rec)

        cfg = {
            "api_key": "opn_test",
            "opn": {
                "endpoint": "http://test",
                "submitter_handle": "workerA",
                "claim": {"priority": None, "estimated_days": 30, "work_notes": "n"},
                "lease": {"extend_when_hours_remaining": 48, "extension_days": 15,
                          "check_interval_seconds": 3600},
            },
            "yafu": {"dir": "/tmp", "binary": "./yafu", "threads": 1},
            "work_loop": {"no_work_sleep_seconds": 0, "max_consecutive_failures": 3,
                          "pending_store": str(store_path), "max_submit_retries": 50},
        }

        api = FakeApi(submit=submit, extend=extend, self_assign=self_assign)
        yafu_cls = MagicMock(name="YafuRunner")
        yafu_cls.return_value.run.return_value = yafu

        with ExitStack() as stack:
            stack.enter_context(patch.object(work_loop, "OpnApi", return_value=api))
            stack.enter_context(patch.object(work_loop, "YafuRunner", yafu_cls))
            stack.enter_context(patch.object(work_loop, "LeaseExtender", MagicMock()))
            stack.enter_context(patch.object(work_loop.signal, "signal", MagicMock()))
            stack.enter_context(patch.object(work_loop.time, "sleep", MagicMock()))
            rc = work_loop.run(cfg, once=once)

        return rc, api, yafu_cls, store_path

    def test_happy_path_fully_factored_releases_nothing_and_clears_store(self):
        rc, api, yafu_cls, sp = self._run(
            self_assign={"claim": CLAIM, "entry": ENTRY}, submit=lambda f: DONE,
        )
        self.assertEqual(rc, 0)
        self.assertEqual(api.release_calls, [])         # server auto-released
        self.assertEqual(len(PendingStore(sp)), 0)
        self.assertTrue(yafu_cls.return_value.run.called)

    def test_outage_caches_and_holds_claim(self):
        rc, api, yafu_cls, sp = self._run(
            self_assign={"claim": CLAIM, "entry": ENTRY}, submit=lambda f: DOWN,
        )
        self.assertEqual(rc, 0)
        rec = PendingStore(sp).get("2", 101)
        self.assertIsNotNone(rec)
        self.assertEqual(rec["submitter_handle"], "workerA")
        self.assertEqual(rec["claim_id"], "claim-1")
        self.assertEqual(rec["expires_at"], "2030-01-01T00:00:00Z")
        self.assertEqual(api.release_calls, [])         # held, not released
        self.assertTrue(yafu_cls.return_value.run.called)

    def test_reclaim_submits_cached_factors_without_refactoring(self):
        # The held claim "expired" and the server re-handed the entry: we must
        # submit the cached factors (with the *cached* handle) and never re-run
        # YAFU. Threads the new claim id / expiry into the record.
        seed = [dict(base="2", exponent=101, factors=["7"], claim_id="old-claim",
                     submitter_handle="workerOLD", expires_at="2999-01-01T00:00:00Z")]
        rc, api, yafu_cls, sp = self._run(
            self_assign={"claim": CLAIM, "entry": ENTRY}, submit=lambda f: DOWN, seed=seed,
        )
        self.assertEqual(rc, 0)
        self.assertFalse(yafu_cls.called)               # never refactored
        rec = PendingStore(sp).get("2", 101)
        self.assertEqual(rec["submitter_handle"], "workerOLD")  # cached handle, not workerA
        self.assertEqual(rec["claim_id"], "claim-1")            # new claim threaded in
        self.assertEqual(rec["expires_at"], CLAIM["expiresAt"]) # new expiry threaded in
        self.assertTrue(all(h == "workerOLD" for (_, _, _, h) in api.submit_calls))

    def test_max_claims_while_holding_waits_instead_of_exiting(self):
        seed = [dict(base="9", exponent=9, factors=["3"], claim_id="held",
                     submitter_handle="w", expires_at="2999-01-01T00:00:00Z")]
        rc, api, yafu_cls, sp = self._run(
            self_assign=MaxClaimsReached("limit", status=409), submit=lambda f: DOWN, seed=seed,
        )
        self.assertEqual(rc, 0)                          # waited (once -> 0), did not exit 2
        self.assertTrue(PendingStore(sp).has("9", 9))    # still held
        self.assertFalse(yafu_cls.called)

    def test_max_claims_without_pending_exits(self):
        rc, api, yafu_cls, sp = self._run(
            self_assign=MaxClaimsReached("limit", status=409),
        )
        self.assertEqual(rc, 2)                          # genuine limit, nothing held

    def test_no_work_once_returns_zero(self):
        rc, api, yafu_cls, sp = self._run(
            self_assign=NoWorkAvailable("none", status=404),
        )
        self.assertEqual(rc, 0)
        self.assertFalse(yafu_cls.called)


if __name__ == "__main__":
    unittest.main()
