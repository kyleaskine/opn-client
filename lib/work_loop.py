from __future__ import annotations

import logging
import re
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Any

from .api import ApiError, MaxClaimsReached, NoWorkAvailable, OpnApi
from .yafu import YafuRunner, build_snfs_expression

logger = logging.getLogger(__name__)


def _parse_iso(s: str) -> datetime:
    # Server returns ISO-8601, e.g. "2026-05-14T12:34:56.000Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


class LeaseExtender(threading.Thread):
    """Background thread that extends the current claim before it expires."""

    def __init__(
        self,
        api: OpnApi,
        claim_id: str,
        expires_at: datetime,
        extend_when_hours: int,
        extension_days: int,
        check_interval_seconds: int,
    ):
        super().__init__(daemon=True, name=f"lease-extender-{claim_id[:8]}")
        self.api = api
        self.claim_id = claim_id
        self.expires_at = expires_at
        self.extend_when_hours = extend_when_hours
        self.extension_days = extension_days
        self.check_interval = check_interval_seconds
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        while not self._stop.wait(self.check_interval):
            now = datetime.now(timezone.utc)
            remaining = (self.expires_at - now).total_seconds() / 3600.0
            if remaining > self.extend_when_hours:
                continue
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


def _submit_factors(
    api: OpnApi,
    base: str,
    exponent: int,
    factors: list[str],
    submitter_handle: str,
) -> tuple[int, bool]:
    """Submit each factor; return (count_submitted, fully_factored).

    Stops early once the server reports the entry is fully factored — it
    derives the final cofactor from the previous submission, so submitting
    the rest would only produce "already fully factored" 400s. The server
    also auto-releases the claim in that case, so the caller can skip its
    explicit release.
    """
    ok = 0
    fully_factored = False
    remaining = sorted(factors, key=len)
    for i, factor in enumerate(remaining):
        try:
            result = api.submit_factor(base, exponent, factor, submitter_handle)
            logger.info("Submitted factor (%d digits): %s...", len(factor), factor[:20])
            ok += 1
        except ApiError as exc:
            logger.error("Submit failed for factor %s...: %s", factor[:20], exc)
            continue
        if (result.get("updated_status") or {}).get("fully_factored"):
            fully_factored = True
            skipped = len(remaining) - (i + 1)
            if skipped:
                logger.info(
                    "Server reports entry fully factored; skipping %d remaining factor(s).",
                    skipped,
                )
            break
    return ok, fully_factored


def run(cfg: dict[str, Any], once: bool = False) -> int:
    api = OpnApi(
        cfg["opn"]["endpoint"],
        cfg["api_key"],
        timeout=cfg.get("opn", {}).get("timeout", 30),
    )
    shutdown = ShutdownState()
    signal.signal(signal.SIGINT, shutdown.handle)
    signal.signal(signal.SIGTERM, shutdown.handle)

    no_work_sleep = int(cfg["work_loop"]["no_work_sleep_seconds"])
    max_failures = int(cfg["work_loop"]["max_consecutive_failures"])
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
            submitted, fully_factored = _submit_factors(
                api, base, exponent, factors, submitter,
            )
            # Server auto-releases the claim when the entry becomes fully
            # factored; a second release would 400 with "Can only modify
            # active claims". Only release ourselves on partial factorization.
            if not fully_factored:
                _safe_release(
                    api, claim_id,
                    notes=f"yafu rc={returncode}; submitted {submitted}/{len(factors)} factor(s)",
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
