#!/usr/bin/env bash
# RetiMesh launcher for Linux and macOS
# - Creates .venv on first run
# - Installs / updates dependencies only when requirements.txt changes
# - Runs retimesh.py, forwarding any arguments

set -e

# Move to the script's directory so the script works no matter where it's invoked from
cd "$(dirname "$0")"

VENV_DIR=".venv"
REQ_FILE="requirements.txt"
HASH_FILE="$VENV_DIR/.req.hash"
APP="retimesh.py"
MIN_PY_MAJOR=3
MIN_PY_MINOR=10

# ── 1. Locate a suitable Python interpreter ──────────────────────────────────
find_python() {
    for candidate in python3.14 python3.13 python3.12 python3.11 python3.10 python3 python; do
        if command -v "$candidate" >/dev/null 2>&1; then
            ver=$("$candidate" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || echo "")
            major=${ver%.*}
            minor=${ver#*.}
            if [ -n "$ver" ] && [ "$major" -ge "$MIN_PY_MAJOR" ] && [ "$minor" -ge "$MIN_PY_MINOR" ]; then
                echo "$candidate"
                return 0
            fi
        fi
    done
    return 1
}

PYTHON=$(find_python || true)
if [ -z "$PYTHON" ]; then
    echo "Error: Python ${MIN_PY_MAJOR}.${MIN_PY_MINOR}+ is required but was not found in PATH." >&2
    echo "  - macOS:  brew install python@3.12   (or download from python.org)" >&2
    echo "  - Ubuntu: sudo apt install python3 python3-venv python3-pip" >&2
    exit 1
fi
echo "[run] Using interpreter: $PYTHON ($("$PYTHON" --version))"

# ── 2. Create the virtual environment if it doesn't exist ────────────────────
if [ ! -d "$VENV_DIR" ]; then
    echo "[run] Creating virtual environment in $VENV_DIR ..."
    if ! "$PYTHON" -m venv "$VENV_DIR"; then
        echo "Error: 'python -m venv' failed." >&2
        echo "On Debian/Ubuntu, install the venv package:  sudo apt install python3-venv" >&2
        exit 1
    fi
fi

# ── 3. Activate the virtualenv ───────────────────────────────────────────────
# shellcheck disable=SC1091
. "$VENV_DIR/bin/activate"

# ── 4. Install dependencies only when requirements.txt has changed ───────────
hash_requirements() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$REQ_FILE" | awk '{print $1}'
    else
        # macOS doesn't ship sha256sum, but it does ship shasum
        shasum -a 256 "$REQ_FILE" | awk '{print $1}'
    fi
}

NEW_HASH=$(hash_requirements)
OLD_HASH=""
[ -f "$HASH_FILE" ] && OLD_HASH=$(cat "$HASH_FILE")

if [ "$NEW_HASH" != "$OLD_HASH" ]; then
    echo "[run] Installing/updating dependencies from $REQ_FILE ..."
    python -m pip install --upgrade pip
    python -m pip install -r "$REQ_FILE"
    echo "$NEW_HASH" > "$HASH_FILE"
else
    echo "[run] Dependencies up to date."
fi

# ── 5. Launch the app, forwarding all CLI arguments ──────────────────────────
echo "[run] Starting $APP ..."
exec python "$APP" "$@"
