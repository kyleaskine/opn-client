from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class ConfigError(Exception):
    pass


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, val in override.items():
        if key in out and isinstance(out[key], dict) and isinstance(val, dict):
            out[key] = _deep_merge(out[key], val)
        else:
            out[key] = val
    return out


def load_config(path: str | Path) -> dict[str, Any]:
    """Load config.yaml and merge config.local.yaml (sibling) on top."""
    base_path = Path(path).resolve()
    if not base_path.is_file():
        raise ConfigError(f"Config file not found: {base_path}")

    with base_path.open() as fh:
        cfg = yaml.safe_load(fh) or {}

    local_path = base_path.with_name("config.local.yaml")
    if local_path.is_file():
        with local_path.open() as fh:
            overrides = yaml.safe_load(fh) or {}
        cfg = _deep_merge(cfg, overrides)

    _validate(cfg)
    return cfg


def _validate(cfg: dict[str, Any]) -> None:
    if not cfg.get("api_key"):
        raise ConfigError(
            "Missing 'api_key' in config. Copy config.local.yaml.example to "
            "config.local.yaml and set api_key to a key generated from the "
            "opn-tracker /profile page (starts with 'opn_')."
        )
    yafu_dir = cfg.get("yafu", {}).get("dir")
    if not yafu_dir or not Path(yafu_dir).is_dir():
        raise ConfigError(
            f"yafu.dir does not exist or is not a directory: {yafu_dir!r}. "
            "Set it in config.local.yaml."
        )
