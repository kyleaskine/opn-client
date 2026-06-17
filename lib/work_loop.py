from __future__ import annotations

import logging
import re
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .api import ApiError, MaxClaimsReached, NoWorkAvailable, OpnApi
from .config import resolve_under_root
from .pending import PendingStore
from .yafu import YafuRunner, build_snfs_expression

logger = logging.getLogger(__name__)

_DEFAULT_PENDING_STORE = "data/pending_submissions.json"
# A pending record is dropped (and its held claim released) after this many
# failed submission attempts, so a persistently-rejected factor can't hold a
# claim slot forever. Override with work_loop.max_submit_retries.
_DEFAULT_MAX_SUBMIT_RETRIES = 50


def _pending_store_path(cfg: dict[str, Any]) -> Path:
    configured = cfg.get("work_loop", {}).get("pending_store") or _DEFAULT_PENDING_STORE
    return resolve_under_root(configured)


def _parse_iso(s: str) -> datetime:
    # Server returns ISO-8601, e.g. "2026-05-14T12:34:56.000Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _extend_held_claims(
    api: OpnApi,
    store: PendingStore,
    extend_when_hours: int,
    extension_days: int,
) -> None:
    """Extend the lease on any held claim (cached submission) nearing expiry.

    A claim held for deferred factors gets no per-job extender, so without this
    a FactorDB outage longer than the claim TTL would let the lease lapse — the
    server would stop excluding the composite and another worker would refactor
    it (NfsClaimService.selfAssignClaim skips only ``status='active'`` claims).
    """
    now = datetime.now(timezone.utc)
    for rec in store.items():
        claim_id = rec.get("claim_id")
        expires_at = rec.get("expires_at")
        if not claim_id or not expires_at:
            continue
        # One broad guard per record: parse, extend, and persist can all fail
        # (bad timestamp, API error, store write), and this runs unwrapped from
        # the main loop and the extender thread — neither may be taken down.
        try:
            remaining = (_parse_iso(expires_at) - now).total_seconds() / 3600.0
            if remaining > extend_when_hours:
                continue
            result = api.extend_claim(claim_id, extension_days)
            new_expiry = (result.get("claim") or {}).get("expiresAt")
            if new_expiry:
                store.set_expiry(rec["base"], int(rec["exponent"]), new_expiry)
                logger.info(
                    "Extended held claim %s for %s^%d; new expiry %s",
                    claim_id, rec["base"], rec["exponent"], new_expiry,
                )
        except Exception as exc:
            logger.warning("Held-claim extend failed for %s (will retry): %s", claim_id, exc)


class LeaseExtender(threading.Thread):
    """Background thread that extends the current claim before it expires.

    Also sweeps held claims (cached submissions in ``store``) on each tick, so
    their leases stay alive during a long-running YAFU job when the main loop —
    which otherwise renews them between jobs — isn't running.
    """

    def __init__(
        self,
        api: OpnApi,
        claim_id: str,
        expires_at: datetime,
        extend_when_hours: int,
        extension_days: int,
        check_interval_seconds: int,
        store: PendingStore | None = None,
    ):
        super().__init__(daemon=True, name=f"lease-extender-{claim_id[:8]}")
        self.api = api
        self.claim_id = claim_id
        self.expires_at = expires_at
        self.extend_when_hours = extend_when_hours
        self.extension_days = extension_days
        self.check_interval = check_interval_seconds
        self.store = store
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        while not self._stop.wait(self.check_interval):
            now = datetime.now(timezone.utc)
            remaining = (self.expires_at - now).total_seconds() / 3600.0
            if remaining <= self.extend_when_hours:
                try:
                    result = self.api.extend_claim(self.claim_id, self.extension_days)
                    claim = result.get("claim") or {}
                    new_expiry = claim.get("expiresAt")
                    if new_expiry:
                        self.expires_at = _parse_iso(new_expiry)
                        logger.info(
                            "Extended claim %s; new expiry %s",
                            self.claim_id, self.expires_at.isoformat(),
                        )
                except Exception as exc:
                    logger.warning("Lease extend failed (will retry): %s", exc)
            if self.store is not None and len(self.store):
                _extend_held_claims(
                    self.api, self.store, self.extend_when_hours, self.extension_days,
                )


class ShutdownState:
    """Multi-level Ctrl+C state. Mirrors ecm-wrapper/lib/work_modes.py:323-360."""

    def __init__(self):
        self.sigint_count = 0
        self.finish_after_current = False
        self.abort_now = False
        self._runner: YafuRunner | None = None

    def bind_runner(self, runner: YafuRunner | None) -> None:
        self._runner = runner

    def handle(self, _signum, _frame) -> None:
        self.sigint_count += 1
        if self.sigint_count == 1:
            self.finish_after_current = True
            logger.warning(
                "SIGINT received — will exit after current job completes. "
                "Press Ctrl+C again to abandon and exit now."
            )
        elif self.sigint_count == 2:
            self.abort_now = True
            logger.warning("Second SIGINT — terminating YAFU and exiting.")
            if self._runner is not None:
                self._runner.terminate(force=False)
        else:
            logger.warning("Third SIGINT — hard exit.")
            if self._runner is not None:
                self._runner.terminate(force=True)
            raise KeyboardInterrupt()


def _is_transient(exc: ApiError) -> bool:
    """Whether a failed submission should be retried later rather than dropped.

    The server validates a factor mathematically *before* submitting it to
    FactorDB, so the only definitive "this factor is bad" response is HTTP 400
    (invalid / duplicate). Everything else — 500 ``factordb_error`` (FactorDB
    down), other 5xx, exhausted 429s, and network errors (status ``None``) —
    is the server failing to *record* a known-good factor, which is worth
    retrying.
    """
    return exc.status != 400


def _submit_factors(
    api: OpnApi,
    base: str,
    exponent: int,
    factors: list[str],
    submitter_handle: str,
) -> tuple[int, bool, list[str]]:
    """Submit each factor; return (count_submitted, fully_factored, deferred).

    Stops early once the server reports the entry is fully factored — it
    derives the final cofactor from the previous submission, so submitting
    the rest would only produce "already fully factored" 400s. The server
    also auto-releases the claim in that case, so the caller can skip its
    explicit release.

    ``deferred`` holds factors that hit a transient failure and should be
    cached for a later retry (see :func:`_is_transient`). Once one factor is
    deferred, the rest are deferred too without trying them: they would hit the
    same outage, and each attempt costs the full network-retry budget. Factors
    rejected as permanently invalid/duplicate (HTTP 400) are dropped, not
    deferred.
    """
    ok = 0
    fully_factored = False
    deferred: list[str] = []
    remaining = sorted(factors, key=len)
    for i, factor in enumerate(remaining):
        try:
            result = api.submit_factor(base, exponent, factor, submitter_handle)
        except ApiError as exc:
            if _is_transient(exc):
                rest = remaining[i:]
                logger.warning(
                    "Deferring %d factor(s) for %s^%d for later retry "
                    "(server could not record them now): %s",
                    len(rest), base, exponent, exc,
                )
                deferred.extend(rest)
                break
            logger.error(
                "Factor permanently rejected for %s^%d (%s...): %s",
                base, exponent, factor[:20], exc,
            )
            continue
        logger.info("Submitted factor (%d digits): %s...", len(factor), factor[:20])
        ok += 1
        if (result.get("updated_status") or {}).get("fully_factored"):
            fully_factored = True
            skipped = len(remaining) - (i + 1)
            if skipped:
                logger.info(
                    "Server reports entry fully factored; skipping %d remaining factor(s).",
                    skipped,
                )
            break
    return ok, fully_factored, deferred


def _drain_record(
    api: OpnApi,
    store: PendingStore,
    base: str,
    exponent: int,
    factors: list[str],
    handle: str,
    claim_id: str | None,
    *,
    attempts: int,
    max_retries: int,
    expires_at: str | None = None,
) -> None:
    """Submit cached factors for one composite and resolve its store record.

    The single place the defer/release rule lives, shared by the post-YAFU,
    flush, and re-claim paths. ``attempts`` is the prior failed-attempt count
    for this record (0 for a fresh post-YAFU deferral). Outcomes:

    - fully factored      -> drop it (the server auto-released its claim);
    - still deferred       -> keep holding; re-cache the remaining factors with
                              an incremented attempt count, unless the retry cap
                              is reached, in which case dead-letter the factors,
                              release the claim, and drop the record;
    - nothing submittable  -> release the claim and drop it.

    Never raises: any unexpected error (e.g. a store write to a full disk) is
    logged together with the factors — so they are preserved — and swallowed,
    so a retry can never take the worker down, regardless of call site.
    """
    try:
        _submitted, fully_factored, deferred = _submit_factors(
            api, base, exponent, factors, handle,
        )

        if fully_factored:
            store.remove(base, exponent)
            logger.info(
                "Pending submission for %s^%d accepted; server fully factored it.",
                base, exponent,
            )
            return

        if deferred:
            next_attempts = attempts + 1
            if max_retries and next_attempts >= max_retries:
                dropped = store.dead_letter({
                    "base": base, "exponent": exponent, "factors": deferred,
                    "claim_id": claim_id, "submitter_handle": handle,
                    "attempts": next_attempts, "reason": "max_submit_retries exceeded",
                })
                logger.error(
                    "Giving up on %s^%d after %d failed submission attempts; releasing "
                    "claim and dropping %d cached factor(s) (saved to %s): %s",
                    base, exponent, next_attempts, len(deferred),
                    dropped or "dead-letter write FAILED — see below",
                    ", ".join(deferred),
                )
                store.remove(base, exponent)
                if claim_id:
                    _safe_release(
                        api, claim_id,
                        notes=f"gave up after {next_attempts} failed submit attempts",
                    )
                return
            store.add(
                base, exponent, deferred,
                claim_id=claim_id, submitter_handle=handle,
                attempts=next_attempts, expires_at=expires_at,
            )
            logger.info(
                "Pending submission for %s^%d still deferred (%d factor(s), attempt %d); will retry.",
                base, exponent, len(deferred), next_attempts,
            )
            return

        store.remove(base, exponent)
        if claim_id:
            _safe_release(
                api, claim_id,
                notes="pending factors no longer submittable (invalid/duplicate)",
            )
        logger.info(
            "Dropped pending submission for %s^%d (no submittable factors remain).",
            base, exponent,
        )
    except Exception:
        logger.exception(
            "Unexpected error draining %s^%d; factors preserved here: %s",
            base, exponent, ", ".join(factors),
        )


def _flush_pending(
    api: OpnApi, store: PendingStore, submitter: str, max_retries: int,
) -> None:
    """Retry every cached submission once. Called before claiming new work."""
    for rec in store.items():
        try:
            _drain_record(
                api, store,
                rec["base"], int(rec["exponent"]),
                rec.get("factors") or [],
                rec.get("submitter_handle") or submitter,
                rec.get("claim_id"),
                attempts=int(rec.get("attempts") or 0),
                max_retries=max_retries,
            )
        except Exception:
            # _drain_record self-guards; this guards the per-record field
            # extraction above so a single odd record can't take the loop down.
            logger.exception(
                "Error retrying pending submission for %s; will retry next pass.",
                rec.get("base"),
            )


def run(cfg: dict[str, Any], once: bool = False) -> int:
    api = OpnApi(
        cfg["opn"]["endpoint"],
        cfg["api_key"],
        timeout=cfg.get("opn", {}).get("timeout", 30),
    )
    store = PendingStore(_pending_store_path(cfg))
    shutdown = ShutdownState()
    signal.signal(signal.SIGINT, shutdown.handle)
    signal.signal(signal.SIGTERM, shutdown.handle)

    no_work_sleep = int(cfg["work_loop"]["no_work_sleep_seconds"])
    max_failures = int(cfg["work_loop"]["max_consecutive_failures"])
    max_retries = int(cfg["work_loop"].get("max_submit_retries", _DEFAULT_MAX_SUBMIT_RETRIES))
    lease_cfg = cfg["opn"]["lease"]
    claim_cfg = cfg["opn"]["claim"]
    submitter = cfg["opn"]["submitter_handle"]

    consecutive_failures = 0

    while True:
        if shutdown.abort_now:
            return 130
        if shutdown.finish_after_current:
            logger.info("Graceful shutdown requested — not claiming new work.")
            return 0

        # Retry anything deferred during an earlier outage before taking new
        # work, and keep held claims' leases alive. On success the server
        # fully-factors the entry and releases the claim we were holding for it,
        # freeing a slot.
        if len(store):
            _flush_pending(api, store, submitter, max_retries)
            _extend_held_claims(
                api, store,
                int(lease_cfg["extend_when_hours_remaining"]),
                int(lease_cfg["extension_days"]),
            )

        try:
            result = api.self_assign(
                priority=claim_cfg.get("priority"),
                estimated_days=claim_cfg.get("estimated_days", 30),
                work_notes=claim_cfg.get("work_notes"),
            )
        except NoWorkAvailable as exc:
            logger.info("No work available (%s); sleeping %ds", exc, no_work_sleep)
            if once:
                return 0
            if shutdown.finish_after_current:
                return 0
            time.sleep(no_work_sleep)
            continue
        except MaxClaimsReached as exc:
            # All claim slots are occupied. If we're holding claims for cached
            # submissions, that's expected during an outage — wait and retry the
            # flush rather than exiting; a recovered FactorDB will free a slot.
            if len(store):
                logger.warning(
                    "Claim slots full while %d submission(s) await retry (%s); "
                    "sleeping %ds before retrying.",
                    len(store), exc, no_work_sleep,
                )
                if once:
                    return 0
                time.sleep(no_work_sleep)
                continue
            logger.error("Claim limit hit: %s. Release an existing claim to continue.", exc)
            return 2
        except ApiError as exc:
            consecutive_failures += 1
            logger.error("self_assign failed (%d/%d): %s",
                         consecutive_failures, max_failures, exc)
            if consecutive_failures >= max_failures:
                return 3
            time.sleep(min(60, 2**consecutive_failures))
            continue

        consecutive_failures = 0

        claim = result["claim"]
        entry = result["entry"]
        claim_id = claim["id"]
        base = entry["baseString"] or str(entry["base"])
        exponent = int(entry["exponent"])
        number = entry["numberToFactor"]
        expiry = _parse_iso(claim["expiresAt"])

        logger.info(
            "Claimed entry %s^%d (%d digits, priority %s). Claim %s expires %s.",
            base, exponent, entry.get("digitCount", "?"), entry.get("priority"),
            claim_id, expiry.isoformat(),
        )

        # We normally keep claims for cached submissions held so the server
        # won't re-hand them out. If one was handed back anyway (e.g. our held
        # claim expired during a very long outage), submit the cached factors
        # instead of wastefully refactoring the composite.
        if store.has(base, exponent):
            logger.info(
                "Re-claimed %s^%d which has cached factors; submitting instead of refactoring.",
                base, exponent,
            )
            cached = store.get(base, exponent) or {}
            _drain_record(
                api, store, base, exponent,
                cached.get("factors") or [],
                cached.get("submitter_handle") or submitter,
                claim_id,
                attempts=int(cached.get("attempts") or 0),
                max_retries=max_retries,
                expires_at=claim["expiresAt"],
            )
            if once:
                return 0
            continue

        expression = build_snfs_expression(base, exponent, number)
        runner = YafuRunner(cfg)
        shutdown.bind_runner(runner)

        extender = LeaseExtender(
            api,
            claim_id=claim_id,
            expires_at=expiry,
            extend_when_hours=int(lease_cfg["extend_when_hours_remaining"]),
            extension_days=int(lease_cfg["extension_days"]),
            check_interval_seconds=int(lease_cfg["check_interval_seconds"]),
            store=store,
        )
        extender.start()

        returncode = -1
        factors: list[str] = []
        try:
            returncode, factors, _ = runner.run(expression, on_line=_log_yafu_line)
        except Exception as exc:
            logger.exception("YAFU run raised: %s", exc)
        finally:
            extender.stop()
            shutdown.bind_runner(None)

        aborted = shutdown.abort_now
        if aborted:
            logger.info("Run aborted by signal; releasing claim.")
            _safe_release(api, claim_id, notes="aborted by worker signal")
            return 130

        if factors:
            # Submit, and on a transient failure cache the factors and *keep the
            # claim* — holding it makes self-assign skip this composite, so the
            # next iteration picks up new work instead of refactoring this one
            # (cached factors are retried at the top of every loop). The server
            # auto-releases the claim once the entry is fully factored; on a
            # partial/permanent result _drain_record releases it.
            _drain_record(
                api, store, base, exponent, factors, submitter, claim_id,
                attempts=0, max_retries=max_retries,
                expires_at=claim["expiresAt"],
            )
        else:
            logger.warning("YAFU produced no factors (rc=%d); releasing claim.", returncode)
            _safe_release(api, claim_id, notes=f"yafu rc={returncode}; no factors")

        if once:
            return 0


# YAFU's sieve threads produce thousands of these per run. They also race
# each other to stdout, which mangles line boundaries — so demoting them to
# DEBUG cleans up both volume and interleaving artifacts in INFO logs.
_YAFU_NOISE = re.compile(
    r"^(nfs: commencing (rational|algebraic) side lattice sieving|total yield:)"
)

# Lines worth surfacing at INFO even if the noise filter would otherwise hide
# them. Covers YAFU -v ETA output for sieving and linear algebra, plus the
# matrix-progress spinner from msieve's Lanczos.
_YAFU_HIGHLIGHT = re.compile(
    r"\bETA\b|\bestimated\b|percent complete|linear algebra|lanczos|"
    r"\bsieving ETA\b|rels found|filtering|matrix is",
    re.IGNORECASE,
)


def _log_yafu_line(line: str) -> None:
    stripped = line.strip()
    if not stripped:
        return
    if _YAFU_HIGHLIGHT.search(stripped):
        logger.info("yafu: %s", stripped)
        return
    if _YAFU_NOISE.search(stripped):
        logger.debug("yafu: %s", stripped)
    else:
        logger.info("yafu: %s", stripped)


def _safe_release(api: OpnApi, claim_id: str, notes: str | None = None) -> None:
    try:
        api.release_claim(claim_id, notes=notes)
        logger.info("Released claim %s", claim_id)
    except ApiError as exc:
        logger.warning("Release failed for %s: %s", claim_id, exc)
