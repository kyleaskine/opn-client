from __future__ import annotations

import fcntl
import logging
import os
import re
import signal
import subprocess
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator

logger = logging.getLogger(__name__)

FACTOR_SECTION = re.compile(r"\*{3,}\s*factors?\s+found\s*\*{3,}", re.IGNORECASE)
PRIME_FACTOR = re.compile(r"^\s*P\d+\s*=\s*(\d+)\s*$")
COMPOSITE_FACTOR = re.compile(r"^\s*C\d+\s*=\s*(\d+)\s*$")


def build_snfs_expression(base: str, exponent: int, number_to_factor: str) -> str:
    """Mirror opn-tracker/src/utils/yafuCommand.ts generateYafuSnfsCommand."""
    return f"snfs(({base}^{exponent + 1}-1),{number_to_factor})"


class YafuDirBusy(Exception):
    """Another process holds the YAFU directory lock."""


@contextmanager
def yafu_dir_lock(yafu_dir: str) -> Iterator[None]:
    """Exclusive lock on a YAFU install dir.

    YAFU writes nfs.dat.chk, nfs.job, siqs.dat, factor.log, etc. straight into
    cwd, so two concurrent workers against the same dir will trample each
    other's state. This advisory flock() on `.opn-client.lock` ensures only
    one opn-client process runs per YAFU install — the kernel releases the
    lock automatically on process exit (including crashes).
    """
    lock_path = Path(yafu_dir) / ".opn-client.lock"
    fh = lock_path.open("a+")
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            fh.seek(0)
            holder = fh.read().strip() or "unknown"
            fh.close()
            raise YafuDirBusy(
                f"{yafu_dir} is already locked by another opn-client "
                f"({holder}). Only one worker can use a YAFU install at a time."
            ) from exc

        fh.seek(0)
        fh.truncate()
        fh.write(f"pid={os.getpid()} started={datetime.now().isoformat()}\n")
        fh.flush()
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    finally:
        if not fh.closed:
            fh.close()


def parse_factors(output: str) -> list[str]:
    """Extract prime factors from YAFU stdout.

    Scans for the ***factors found*** banner and returns every `Pn = <digits>`
    afterwards. Preserves multiplicity (duplicate entries indicate prime powers).
    Composite lines (`Cn = ...`) are ignored — they will be further factored by
    YAFU itself into more `Pn` lines in the same block.
    """
    primes: list[str] = []
    in_section = False
    for line in output.splitlines():
        if not in_section:
            if FACTOR_SECTION.search(line):
                in_section = True
            continue
        prime_match = PRIME_FACTOR.match(line)
        if prime_match:
            primes.append(prime_match.group(1))
            continue
        if COMPOSITE_FACTOR.match(line):
            continue
    return primes


class YafuRunner:
    """Launches YAFU from its install directory and streams output."""

    def __init__(self, cfg: dict[str, Any]):
        yafu_cfg = cfg["yafu"]
        self.cwd = str(Path(yafu_cfg["dir"]).resolve())
        self.binary = yafu_cfg["binary"]
        self.threads = int(yafu_cfg.get("threads", 8))
        self.extra_args = list(yafu_cfg.get("extra_args", []))
        self.proc: subprocess.Popen[str] | None = None

    def run(
        self,
        expression: str,
        on_line: Callable[[str], None] | None = None,
    ) -> tuple[int, list[str], str]:
        """Run YAFU with `expression`. Returns (returncode, factors, raw_output)."""
        cmd = [self.binary, expression, "-threads", str(self.threads), *self.extra_args]
        logger.info("Launching YAFU: %s (cwd=%s)", " ".join(cmd), self.cwd)

        self.proc = subprocess.Popen(
            cmd,
            cwd=self.cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )

        captured: list[str] = []
        try:
            assert self.proc.stdout is not None
            for line in self.proc.stdout:
                captured.append(line)
                if on_line is not None:
                    on_line(line.rstrip("\n"))
            returncode = self.proc.wait()
        finally:
            self.proc = None

        raw = "".join(captured)
        factors = parse_factors(raw)
        logger.info("YAFU exited %d; parsed %d prime factor(s)", returncode, len(factors))
        return returncode, factors, raw

    def terminate(self, force: bool = False) -> None:
        """Send SIGTERM (or SIGKILL if force) to the running YAFU process group."""
        if self.proc is None or self.proc.poll() is not None:
            return
        sig = signal.SIGKILL if force else signal.SIGTERM
        try:
            os.killpg(os.getpgid(self.proc.pid), sig)
            logger.info("Sent %s to YAFU (pid=%d)", sig.name, self.proc.pid)
        except ProcessLookupError:
            pass
