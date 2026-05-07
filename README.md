# RetiMesh

A lightweight **Reticulum mesh communications prototype** for secure messaging,
file transfer, and voice calls, accessed through a local web UI.

RetiMesh is a single-file Python application that wraps the
[Reticulum Network Stack (RNS)](https://reticulum.network) and
[LXMF](https://github.com/markqvist/LXMF) behind a Flask + Server-Sent Events
backend, served to your browser. It targets resilient, low-bandwidth, and
infrastructure-independent communication: useful in field deployments,
emergency scenarios, or any environment where conventional networks are
unavailable, untrusted, or degraded.

> **Status:** prototype / research project. Not yet recommended for

---

## Features

- End-to-end encrypted messaging (LXMF) with cryptographic identities
- File transfer over Reticulum links
- Voice calls with low-bandwidth codec comparison hooks (codec2 optional)
- Browser-based UI, no native client to install
- Cross-platform: Linux, macOS, Windows
- Pluggable transports via Reticulum (TCP, serial, LoRa, etc.)
- One-script bootstrap (`run.sh` / `run.bat` / `run.py`): virtual environment
  and dependencies are set up on first launch

---

## Quick start

RetiMesh ships with a launcher that creates a Python virtual environment,
installs dependencies, and starts the app, all in one command. You don't
have to manage `pip` or `venv` yourself.

The simplest way to start the app is to **double-click the launcher for your
operating system** in your file browser:

- **Windows** → `run.bat`
- **Linux** → `run.sh` (mark it executable first; some file managers will
  ask whether to run or open it in a text editor, choose run)
- **macOS** → `run.command` if you've renamed `run.sh`, or run `run.sh` from
  Terminal (Finder won't execute `.sh` files on a double-click by default)
- **Any platform** → `run.py` (right-click → Open With → Python)

If you'd rather use a terminal, the equivalents are below.

### Linux / macOS

```bash
chmod +x run.sh        # one-time, only if needed
./run.sh
```

### Windows

Double-click `run.bat`, or from a terminal:

```bat
run.bat
```

### Any platform

```bash
python run.py
```

On first launch the script will:

1. Verify Python ≥ 3.10 is available
2. Create `.venv/` next to the script
3. Install everything in `requirements.txt`
4. Start `retimesh.py` and open the UI in your browser

Subsequent launches reuse the venv and skip the install step unless
`requirements.txt` has changed.

Once the server starts, open <http://127.0.0.1:5000> in your browser if it
doesn't open automatically.

---

## Manual installation

If you'd rather manage the environment yourself:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python retimesh.py
```

### Requirements

- **Python 3.10 – 3.14**
- Core dependencies: `rns`, `lxmf`, `flask`, `gevent`, `gevent-websocket`

---

## Command-line options

```text
python retimesh.py [options]

  --host HOST          Web server bind address (default: 127.0.0.1)
  --port PORT          Web server port (default: 5000)
  --storage PATH       Storage directory (default: ~/.retimesh/data)
  --rns-config PATH    Reticulum config directory
  --headless           Don't open the browser on launch
  --log-level LEVEL    DEBUG | INFO | WARNING | ERROR (default: INFO)
  --debug-rns          Enable RNS-level debug + link-handshake tracing
```

Arguments passed to `run.sh`, `run.bat`, or `run.py` are forwarded to
`retimesh.py`, e.g.:

```bash
./run.sh --host 0.0.0.0 --port <PORT_NUMBER> # Defaults to 5000
```

---

## Project layout

```
retimesh/
├── retimesh.py          # Main application (Flask + RNS + LXMF)
├── requirements.txt     # Python dependencies
├── run.sh               # Launcher for Linux & macOS
├── run.bat              # Launcher for Windows
├── run.py               # Cross-platform Python launcher
├── static/              # JS, CSS, assets for the web UI
│   ├── css/
│   └── js/
└── templates/
    └── index.html       # Main UI template
```

---

## Architecture (brief)

```
┌─────────────────────────┐                ┌──────────────────────────────┐
│  Browser UI (HTML/JS)   │ ── WebSocket ──│  Python backend              │
│  http://127.0.0.1:5000  │     (/ws)      │  Flask + gevent (single proc)│
└─────────────────────────┘                └──────────────┬───────────────┘
                                                          │
                                                  ┌───────┴────────┐
                                                  │  RNS  +  LXMF  │
                                                  └───────┬────────┘
                                                          │
                          ┌───────────────────────────────┼───────────────────────────────┐
                          │                               │                               │
                       TCP / UDP                     WiFi / Auto                  Serial / LoRa (RNode)
                                                                                  + I2P (overlay)
```

The browser talks to the Python backend over a single WebSocket on `/ws`,
which carries both JSON signaling and binary audio frames for voice calls.
The backend wraps Flask with a WSGI middleware that hands `/ws` requests to
`geventwebsocket` before they reach Flask routing, so the upgrade handshake
succeeds.

Reticulum handles all mesh routing, link establishment, and encryption on
top of whichever transports are configured in `~/.reticulum/config` ,
typically `AutoInterface` for LAN discovery, `TCPClientInterface` /
`TCPServerInterface` for IP-routed peers, `RNodeInterface` for LoRa, and
optionally `I2PInterface` as an anonymizing overlay.

---

## Troubleshooting

**`python -m venv` fails on Debian / Ubuntu**
The venv module is a separate package on Debian-family distros:
```bash
sudo apt install python3-venv python3-pip
```

**Windows shows `Terminate batch job (Y/N)?` after Ctrl+C**
That's a cmd.exe quirk, not a RetiMesh issue, the app has already shut down
cleanly. Press `Y` to dismiss the prompt, or launch via `python run.py`
to avoid it entirely.


---

## Roadmap

A Bluetooth Low Energy interface for Reticulum is currently in development.
