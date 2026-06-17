from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PendingStore:
    """Persistent store of factor submissions the server could not record yet.

    When the server returns a transient failure (e.g. FactorDB is down, so
    POST /api/submit-factor-simple replies 500 ``factordb_error``), the factors
    YAFU found would otherwise be discarded and the composite refactored. The
    server validates a factor *mathematically* before the FactorDB step, so a
    transient failure means the factor is already known-good — it is safe to
    persist verbatim and resubmit later.

    Records are keyed by ``"<base>^<exponent>"`` and the file is written
    atomically (temp file + ``os.replace``) so a crash mid-write cannot corrupt
    it. Access is guarded by a re-entrant lock so a background lease-extender
    thread and the main loop can share one instance.

    Record fields: ``base``, ``exponent``, ``factors`` (list still awaiting
    submission), ``claim_id`` and ``expires_at`` (the held claim that keeps the
    server from re-handing the composite), ``submitter_handle``, ``attempts``
    (failed submission attempts so far — drives the retry cap), and timestamps
    ``cached_at`` / ``last_attempt``.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = threading.RLock()
        self._data: dict[str, dict[str, Any]] = {}
        self._load()

    @staticmethod
    def _key(base: str, exponent: int) -> str:
        return f"{base}^{exponent}"

    @staticmethod
    def _valid(rec: Any) -> bool:
        return (
            isinstance(rec, dict)
            and isinstance(rec.get("base"), str)
            and isinstance(rec.get("exponent"), int)
            and isinstance(rec.get("factors"), list)
            and all(isinstance(f, str) for f in rec["factors"])
            and isinstance(rec.get("expires_at"), (str, type(None)))
            and isinstance(rec.get("claim_id"), (str, type(None)))
        )

    def _load(self) -> None:
        if not self.path.is_file():
            return
        try:
            with self.path.open() as fh:
                raw = json.load(fh)
        except (OSError, ValueError) as exc:
            logger.warning("Could not read pending store %s: %s", self.path, exc)
            return
        if not isinstance(raw, dict):
            return
        # Drop malformed records on load so a corrupt entry can't crash every
        # subsequent flush (a "poison record" that re-breaks the worker on each
        # restart).
        good = {k: v for k, v in raw.items() if self._valid(v)}
        dropped = len(raw) - len(good)
        if dropped:
            logger.warning("Dropped %d malformed pending record(s) from %s", dropped, self.path)
        self._data = good
        if self._data:
            logger.info(
                "Loaded %d pending factor submission(s) from %s",
                len(self._data), self.path,
            )

    def _flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=".pending-", suffix=".json"
        )
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(self._data, fh, indent=2, sort_keys=True)
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def add(
        self,
        base: str,
        exponent: int,
        factors: Iterable[str],
        *,
        claim_id: str | None = None,
        submitter_handle: str | None = None,
        attempts: int | None = None,
        expires_at: str | None = None,
    ) -> None:
        """Insert or replace the pending record for ``base^exponent``.

        ``factors`` replaces any previously cached list (callers pass the set
        that still needs submitting). ``attempts`` and ``expires_at`` are set
        when provided and otherwise preserved, as are ``cached_at`` and
        ``last_attempt``, so retry history survives across updates.
        """
        with self._lock:
            key = self._key(base, exponent)
            existing = self._data.get(key, {})
            self._data[key] = {
                "base": str(base),
                "exponent": int(exponent),
                "factors": list(factors),
                "claim_id": claim_id or existing.get("claim_id"),
                "expires_at": expires_at or existing.get("expires_at"),
                "submitter_handle": submitter_handle or existing.get("submitter_handle"),
                "attempts": attempts if attempts is not None else existing.get("attempts", 0),
                "cached_at": existing.get("cached_at") or _now_iso(),
                "last_attempt": _now_iso() if attempts is not None else existing.get("last_attempt"),
            }
            self._flush()

    def dead_letter(self, record: dict[str, Any]) -> Path | None:
        """Append a permanently-dropped record to a sibling JSONL file.

        Called when the retry cap is hit, so giving up never loses the (full)
        factors — an operator can resubmit them from this file. Append-only and
        never raises: a write failure is logged and ``None`` returned.
        """
        path = self.path.with_name(self.path.stem + "_dropped.jsonl")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps({**record, "dropped_at": _now_iso()}, sort_keys=True)
            with path.open("a") as fh:
                fh.write(line + "\n")
            return path
        except OSError as exc:
            logger.error("Could not write dead-letter file %s: %s", path, exc)
            return None

    def set_expiry(self, base: str, exponent: int, expires_at: str) -> None:
        """Update the stored claim expiry after a successful lease extension."""
        with self._lock:
            rec = self._data.get(self._key(base, exponent))
            if rec is not None:
                rec["expires_at"] = expires_at
                self._flush()

    def get(self, base: str, exponent: int) -> dict[str, Any] | None:
        with self._lock:
            rec = self._data.get(self._key(base, exponent))
            return dict(rec) if rec else None

    def has(self, base: str, exponent: int) -> bool:
        with self._lock:
            return self._key(base, exponent) in self._data

    def items(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(v) for v in self._data.values()]

    def remove(self, base: str, exponent: int) -> None:
        with self._lock:
            if self._data.pop(self._key(base, exponent), None) is not None:
                self._flush()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)
