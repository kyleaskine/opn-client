from __future__ import annotations

import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)


class ApiError(Exception):
    def __init__(self, message: str, status: int | None = None, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


class NoWorkAvailable(ApiError):
    pass


class MaxClaimsReached(ApiError):
    pass


class OpnApi:
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        timeout: int = 30,
        retry_attempts: int = 3,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Worker-Key": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        url = f"{self.endpoint}{path}"
        last_exc: Exception | None = None
        for attempt in range(self.retry_attempts):
            try:
                resp = self.session.request(
                    method, url, timeout=self.timeout, **kwargs
                )
            except requests.RequestException as exc:
                last_exc = exc
                delay = 2**attempt
                logger.warning(
                    "Network error on %s %s (attempt %d/%d): %s — retrying in %ds",
                    method, path, attempt + 1, self.retry_attempts, exc, delay,
                )
                time.sleep(delay)
                continue

            if 200 <= resp.status_code < 300:
                return self._unwrap(resp)

            # 4xx except 429: don't retry.
            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                self._raise_for_status(resp, path)

            # 5xx or 429: retry with backoff.
            if attempt + 1 < self.retry_attempts:
                delay = 2**attempt
                logger.warning(
                    "%s %s returned %d (attempt %d/%d) — retrying in %ds",
                    method, path, resp.status_code, attempt + 1,
                    self.retry_attempts, delay,
                )
                time.sleep(delay)
                continue
            self._raise_for_status(resp, path)

        raise ApiError(f"Network error calling {method} {path}: {last_exc}")

    @staticmethod
    def _unwrap(resp: requests.Response) -> dict[str, Any]:
        try:
            payload = resp.json()
        except ValueError:
            raise ApiError(f"Non-JSON response from {resp.url}: {resp.text[:200]!r}",
                           status=resp.status_code)
        if isinstance(payload, dict) and payload.get("success") is True:
            return payload.get("data") or {}
        if isinstance(payload, dict) and payload.get("success") is False:
            raise ApiError(payload.get("error") or "API error",
                           status=resp.status_code, body=payload)
        return payload

    @staticmethod
    def _raise_for_status(resp: requests.Response, path: str) -> None:
        try:
            body = resp.json()
            error = body.get("error") if isinstance(body, dict) else None
        except ValueError:
            body, error = resp.text, None
        msg = error or f"{resp.status_code} from {path}"
        if resp.status_code == 404:
            raise NoWorkAvailable(msg, status=404, body=body)
        if resp.status_code == 409:
            raise MaxClaimsReached(msg, status=409, body=body)
        raise ApiError(msg, status=resp.status_code, body=body)

    # -- worker-facing endpoints --

    def self_assign(
        self,
        priority: int | None = None,
        estimated_days: int = 30,
        work_notes: str | None = None,
    ) -> dict[str, Any]:
        """Claim next available SNFS work unit. Returns {'claim': ..., 'entry': ...}."""
        body: dict[str, Any] = {"estimatedDays": estimated_days}
        if priority is not None:
            body["priority"] = priority
        if work_notes:
            body["workNotes"] = work_notes
        return self._request("POST", "/api/nfs-claims/self-assign", json=body)

    def extend_claim(self, claim_id: str, extension_days: int = 15) -> dict[str, Any]:
        body = {"claimId": claim_id, "action": "extend", "extensionDays": extension_days}
        return self._request("PUT", "/api/nfs-claims", json=body)

    def release_claim(self, claim_id: str, notes: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"claimId": claim_id, "action": "release"}
        if notes:
            body["workNotes"] = notes
        return self._request("PUT", "/api/nfs-claims", json=body)

    def submit_factor(
        self,
        base: str,
        exponent: int,
        factor: str,
        submitter_handle: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "base": str(base),
            "exponent": int(exponent),
            "factor": str(factor),
        }
        if submitter_handle:
            body["submitterHandle"] = submitter_handle
        return self._request("POST", "/api/submit-factor-simple", json=body)
