#!/usr/bin/env python3
"""OPN Client Setup Wizard.

Interactively creates config.local.yaml with your personal settings.
Run before first use, or re-run to change settings (current values are
shown as defaults).

    python3 setup.py
"""
from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

if sys.version_info < (3, 9):
    print(f"Error: Python 3.9+ required (you have {sys.version_info.major}.{sys.version_info.minor})")
    sys.exit(1)


HERE = Path(__file__).resolve().parent
LOCAL_CONFIG = HERE / "config.local.yaml"


def get_input(prompt: str, default: str = "", required: bool = False) -> str:
    display = f"{prompt} [{default}]: " if default else f"{prompt}: "
    while True:
        value = input(display).strip()
        if value:
            return value
        if default:
            return default
        if required:
            print("  This field is required.")
            continue
        return ""


def get_yes_no(prompt: str, default: bool = True) -> bool:
    tag = "Y/n" if default else "y/N"
    while True:
        value = input(f"{prompt} [{tag}]: ").strip().lower()
        if not value:
            return default
        if value in ("y", "yes"):
            return True
        if value in ("n", "no"):
            return False
        print("  Please enter 'y' or 'n'.")


def detect_cpu_cores() -> int:
    try:
        return os.cpu_count() or 4
    except Exception:
        return 4


def detect_hostname() -> str:
    try:
        return platform.node() or "my-machine"
    except Exception:
        return "my-machine"


def load_existing_config() -> dict[str, Any]:
    if not LOCAL_CONFIG.is_file():
        return {}
    try:
        import yaml
        with LOCAL_CONFIG.open() as fh:
            data = yaml.safe_load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def get_nested(cfg: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = cfg
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def find_yafu_dir() -> Optional[str]:
    candidates = [
        "~/yafu",
        "~/yafu-master",
        "~/yafu2",
        "/opt/yafu",
    ]
    for path in candidates:
        expanded = Path(os.path.expanduser(path))
        if expanded.is_dir() and (expanded / "yafu").is_file():
            return str(expanded)
    # Fallback: yafu on PATH -> use its parent.
    on_path = shutil.which("yafu")
    if on_path:
        return str(Path(on_path).parent)
    return None


def verify_yafu(yafu_dir: str, binary: str) -> bool:
    """Run `yafu --version` (or similar) to confirm it executes."""
    bin_path = Path(yafu_dir) / binary if not Path(binary).is_absolute() else Path(binary)
    if not bin_path.is_file():
        print(f"  ERROR: {bin_path} does not exist.")
        return False
    if not os.access(bin_path, os.X_OK):
        print(f"  ERROR: {bin_path} is not executable.")
        return False
    try:
        # YAFU has no --version, but `yafu "1+1"` runs instantly and exits 0.
        result = subprocess.run(
            [str(bin_path), "1+1"],
            cwd=yafu_dir,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            print("  YAFU ran successfully.")
            return True
        print(f"  WARNING: YAFU exited {result.returncode}.")
        for line in (result.stdout + result.stderr).strip().splitlines()[-5:]:
            print(f"  {line}")
        return False
    except subprocess.TimeoutExpired:
        print("  WARNING: YAFU test timed out.")
        return False
    except Exception as exc:
        print(f"  ERROR: {exc}")
        return False


def yaml_quote(value: str) -> str:
    return f'"{value}"'


def main() -> int:
    print()
    print("=" * 60)
    print("  OPN Client Setup Wizard")
    print("=" * 60)
    print()

    existing = load_existing_config()
    if existing:
        print("Existing config.local.yaml found. Current values will be")
        print("shown as defaults -- press Enter to keep them.")
    else:
        print("This wizard will create config.local.yaml with your personal")
        print("settings for the opn-tracker YAFU worker client.")
    print()

    # ---- server / identity ----
    print("-" * 60)
    print("SERVER & IDENTITY")
    print("-" * 60)
    print()

    existing_endpoint = get_nested(existing, "opn", "endpoint", default="")
    if existing_endpoint:
        endpoint = get_input("opn-tracker endpoint URL", default=existing_endpoint)
    else:
        use_default = get_yes_no(
            "Use the default opn-tracker server (https://opntracker.com)?",
            default=True,
        )
        endpoint = (
            "https://opntracker.com"
            if use_default
            else get_input("Enter opn-tracker endpoint URL", required=True)
        )

    existing_handle = get_nested(existing, "opn", "submitter_handle", default="")
    handle = get_input(
        "Submitter handle (your username shown on submitted factors)",
        default=existing_handle or detect_hostname(),
        required=True,
    )

    print()
    print("The API key is generated from opn-tracker's /profile page after")
    print("signing in. Create a labeled key and paste it here. Keys start")
    print("with 'opn_' and are shown only once.")
    existing_key = existing.get("api_key") or ""
    masked = f"{existing_key[:8]}..." if existing_key else ""
    api_key = get_input(
        f"API key{' (press Enter to keep current)' if existing_key else ''}",
        default=existing_key,
        required=True,
    )
    if api_key != existing_key and not api_key.startswith("opn_"):
        print(f"  WARNING: key does not start with 'opn_' (got '{api_key[:10]}...').")
        if not get_yes_no("  Save anyway?", default=False):
            print("Setup cancelled.")
            return 1
    if existing_key and api_key == existing_key:
        print(f"  (keeping existing key {masked})")

    # ---- YAFU ----
    print()
    print("-" * 60)
    print("YAFU INSTALLATION")
    print("-" * 60)
    print()

    existing_yafu_dir = get_nested(existing, "yafu", "dir", default="")

    if existing_yafu_dir:
        yafu_dir = get_input("Path to YAFU install directory", default=existing_yafu_dir)
    else:
        detected = find_yafu_dir()
        if detected:
            print(f"Detected YAFU directory: {detected}")
            yafu_dir = get_input("Path to YAFU install directory", default=detected)
        else:
            print("YAFU directory not auto-detected. Enter the directory")
            print("containing the `yafu` binary (YAFU must be run from its")
            print("own directory because it writes nfs.dat, factor.log, etc.")
            print("directly into cwd).")
            yafu_dir = get_input("Path to YAFU install directory", required=True)

    yafu_dir = str(Path(os.path.expanduser(yafu_dir)).resolve())
    if not Path(yafu_dir).is_dir():
        print(f"  ERROR: {yafu_dir} is not a directory.")
        return 1

    existing_binary = get_nested(existing, "yafu", "binary", default="./yafu")
    binary = get_input(
        "YAFU binary name (relative to YAFU dir, or absolute path)",
        default=existing_binary,
    )

    cores = detect_cpu_cores()
    print(f"Detected {cores} CPU cores.")
    existing_threads = get_nested(existing, "yafu", "threads", default=None)
    default_threads = existing_threads if existing_threads is not None else cores
    threads_str = get_input("YAFU threads", default=str(default_threads))
    try:
        threads = int(threads_str)
    except ValueError:
        threads = default_threads

    # ---- preview & write ----
    print()
    print("-" * 60)
    print("GENERATING CONFIGURATION")
    print("-" * 60)
    print()

    lines = [
        "# opn-client local configuration",
        f"# Generated by setup.py on {detect_hostname()}",
        "# Overrides values in config.yaml. Gitignored.",
        "",
        "opn:",
        f"  endpoint: {yaml_quote(endpoint)}",
        f"  submitter_handle: {yaml_quote(handle)}",
        "",
        f"api_key: {yaml_quote(api_key)}",
        "",
        "yafu:",
        f"  dir: {yaml_quote(yafu_dir)}",
        f"  binary: {yaml_quote(binary)}",
        f"  threads: {threads}",
        "",
    ]
    content = "\n".join(lines)

    print("Configuration preview:")
    print()
    print("-" * 40)
    # Mask the api_key in the preview so it doesn't get captured in scrollback.
    preview = content.replace(api_key, f"{api_key[:8]}...{'*' * 8}") if api_key else content
    print(preview)
    print("-" * 40)
    print()

    if not get_yes_no("Save this configuration?", default=True):
        print("Setup cancelled. No files were written.")
        return 1

    LOCAL_CONFIG.write_text(content)
    try:
        os.chmod(LOCAL_CONFIG, 0o600)
    except OSError:
        pass

    print()
    print("=" * 60)
    print("  SETUP COMPLETE")
    print("=" * 60)
    print()
    print(f"Configuration saved to: {LOCAL_CONFIG}")
    print()
    print("Run the worker with:")
    print("  python3 opn_client.py            # loop forever")
    print("  python3 opn_client.py --once     # claim one job and exit")
    print()

    if get_yes_no("Verify YAFU by running `yafu \"1+1\"` now?", default=True):
        print()
        verify_yafu(yafu_dir, binary)
        print()

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\nSetup cancelled by user.")
        sys.exit(1)
