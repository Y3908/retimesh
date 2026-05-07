#!/usr/bin/env python3
"""
RetiMesh cross-platform launcher.

Equivalent to run.sh / run.bat — works on Linux, macOS, and Windows.
Run with:  python run.py [args...]

Behaviour:
  1. Verifies Python >= 3.10
  2. Creates .venv on first run
  3. Re-installs dependencies only when requirements.txt changes
  4. Re-executes retimesh.py inside the venv, forwarding all CLI args
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV_DIR = ROOT / ".venv"
REQ_FILE = ROOT / "requirements.txt"
HASH_FILE = VENV_DIR / ".req.hash"
APP = ROOT / "retimesh.py"
MIN_PY = (3, 10)


def venv_python() -> Path:
    """Return the path to the python executable inside the venv."""
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def ensure_python_version() -> None:
    if sys.version_info < MIN_PY:
        sys.exit(
            f"Error: Python {MIN_PY[0]}.{MIN_PY[1]}+ is required "
            f"(current: {sys.version.split()[0]})."
        )


def create_venv_if_missing() -> None:
    if venv_python().exists():
        return
    print(f"[run] Creating virtual environment in {VENV_DIR} ...")
    try:
        subprocess.check_call([sys.executable, "-m", "venv", str(VENV_DIR)])
    except subprocess.CalledProcessError as e:
        sys.exit(
            "Error: 'python -m venv' failed. "
            "On Debian/Ubuntu, install the venv package: "
            "sudo apt install python3-venv\n"
            f"({e})"
        )


def requirements_hash() -> str:
    return hashlib.sha256(REQ_FILE.read_bytes()).hexdigest()


def install_requirements_if_changed() -> None:
    new_hash = requirements_hash()
    old_hash = HASH_FILE.read_text().strip() if HASH_FILE.exists() else ""
    if new_hash == old_hash:
        print("[run] Dependencies up to date.")
        return

    print(f"[run] Installing/updating dependencies from {REQ_FILE.name} ...")
    py = str(venv_python())
    subprocess.check_call([py, "-m", "pip", "install", "--upgrade", "pip"])
    subprocess.check_call([py, "-m", "pip", "install", "-r", str(REQ_FILE)])
    HASH_FILE.write_text(new_hash)


def run_app(argv: list[str]) -> int:
    print(f"[run] Starting {APP.name} ...")
    # Use Popen + wait so Ctrl+C is delivered cleanly on all platforms.
    proc = subprocess.Popen([str(venv_python()), str(APP), *argv])
    try:
        return proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        return proc.wait()


def main() -> int:
    ensure_python_version()
    if not REQ_FILE.exists():
        sys.exit(f"Error: {REQ_FILE} not found.")
    if not APP.exists():
        sys.exit(f"Error: {APP} not found.")
    create_venv_if_missing()
    install_requirements_if_changed()
    return run_app(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
