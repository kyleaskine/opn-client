"""Shared test helpers: a stub for the OpnApi seam and a temp-dir TestCase.

The whole pending-submission feature is reachable without a live server —
``OpnApi`` is the only external seam — so these stubs let the unit tests drive
every branch (outage defer, retry cap, dead-letter, lease renewal) offline.
"""
from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any

from lib.pending import PendingStore


def _resolve(spec: Any, arg: Any) -> Any:
    """Turn a scripted endpoint spec into a return value (or a raise).

    ``spec`` may be a callable (called with ``arg``), a fixed value, or an
    ``Exception`` instance / a callable returning one (which is raised).
    """
    if spec is None:
        return {}
    value = spec(arg) if callable(spec) else spec
    if isinstance(value, Exception):
        raise value
    return value


class FakeApi:
    """Stub of the bits of OpnApi the work loop calls, recording every call.

    Each endpoint is scripted by passing ``submit`` / ``extend`` / ``self_assign``
    a callable, a fixed value, or an Exception to raise. ``submit`` is called
    with the factor string, ``extend`` with the claim id, ``self_assign`` with
    ``None``. Signatures are kept in lock-step with the real OpnApi by
    ``test_fakeapi_matches_opnapi_call_surface``.
    """

    def __init__(self, submit: Any = None, extend: Any = None, self_assign: Any = None):
        self._submit = submit
        self._extend = extend
        self._self_assign = self_assign
        self.submit_calls: list[tuple[Any, Any, str, str | None]] = []
        self.release_calls: list[tuple[str, str | None]] = []
        self.extend_calls: list[tuple[str, int]] = []
        self.self_assign_calls: list[tuple[Any, Any, Any]] = []

    def self_assign(self, priority=None, estimated_days=30, work_notes=None):
        self.self_assign_calls.append((priority, estimated_days, work_notes))
        return _resolve(self._self_assign, None)

    def submit_factor(self, base, exponent, factor, submitter_handle=None):
        self.submit_calls.append((base, exponent, factor, submitter_handle))
        return _resolve(self._submit, factor)

    def release_claim(self, claim_id, notes=None):
        self.release_calls.append((claim_id, notes))
        return {}

    def extend_claim(self, claim_id, extension_days):
        self.extend_calls.append((claim_id, extension_days))
        return _resolve(self._extend, claim_id)


class StoreTestCase(unittest.TestCase):
    """Base class providing temp dirs that are cleaned up after each test."""

    def mkdtemp(self) -> Path:
        d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        return d

    def make_store(self) -> PendingStore:
        return PendingStore(self.mkdtemp() / "pending.json")
