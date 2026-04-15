#!/usr/bin/env python3
"""OPN-tracker YAFU worker client.

Claims SNFS work from an opn-tracker server, runs YAFU from its install dir,
parses prime factors, submits them back, and repeats.
"""
from __future__ import annotations

import argparse
import logging
import logging.handlers
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from lib.config import ConfigError, load_config  # noqa: E402
from lib.yafu import YafuDirBusy, yafu_dir_lock  # noqa: E402
from lib import work_loop  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OPN YAFU worker client")
    parser.add_argument(
        "--config",
        default=str(HERE / "config.yaml"),
        help="Path to config.yaml (config.local.yaml is merged on top).",
    )
    parser.add_argument(
        "--priority",
        type=int,
        default=None,
        help="Filter claim by priority (10=O21, 5=O31, 1=O41). Overrides config.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Claim and process one assignment, then exit.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Debug-level logging.",
    )
    return parser.parse_args()


def setup_logging(cfg: dict, verbose: bool) -> None:
    level_name = "DEBUG" if verbose else cfg.get("logging", {}).get("level", "INFO")
    level = getattr(logging, level_name.upper(), logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(fmt)
    root.addHandler(stream)

    log_file = cfg.get("logging", {}).get("file")
    if log_file:
        log_path = (HERE / log_file) if not Path(log_file).is_absolute() else Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_path, maxBytes=10 * 1024 * 1024, backupCount=5,
        )
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)


def main() -> int:
    args = parse_args()
    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 1

    if args.priority is not None:
        cfg.setdefault("opn", {}).setdefault("claim", {})["priority"] = args.priority

    setup_logging(cfg, args.verbose)
    logger = logging.getLogger("opn_client")
    logger.info(
        "Starting worker: endpoint=%s handle=%s yafu_dir=%s",
        cfg["opn"]["endpoint"],
        cfg["opn"]["submitter_handle"],
        cfg["yafu"]["dir"],
    )
    try:
        with yafu_dir_lock(cfg["yafu"]["dir"]):
            return work_loop.run(cfg, once=args.once)
    except YafuDirBusy as exc:
        logger.error("%s", exc)
        return 4


if __name__ == "__main__":
    sys.exit(main())
