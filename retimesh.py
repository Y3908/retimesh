#!/usr/bin/env python3
"""
RetiMesh - Lightweight Reticulum Mesh Communications Prototype
A native Python app for secure messaging, file transfer, and voice calls
over the Reticulum Network Stack.

Architecture:
  Python (Flask + WebSocket) backend  <-->  Browser UI
  Python backend  <-->  RNS / LXMF stack  <-->  Mesh network
"""

# ── gevent availability check ─────────────────────────────────────────────────
# Real-time push uses Server-Sent Events (SSE) instead of WebSocket so that
# geventwebsocket is not required.  pywsgi.WSGIServer handles the SSE streaming
# response natively via gevent greenlets — no monkey.patch_all() needed.
try:
    import gevent as _gevent_check   # just test availability
    _GEVENT_AVAILABLE = True
    del _gevent_check
except ImportError:
    _GEVENT_AVAILABLE = False

import argparse
import base64
import hashlib
import json
import logging
import os
import re
import secrets
import sqlite3
import sys
import threading
import time
import struct
import wave
import io
from datetime import datetime
from pathlib import Path

if _GEVENT_AVAILABLE:
    import gevent
    import gevent.lock
    import gevent.event

def _sleep(seconds):
    """Cooperative sleep: uses gevent.sleep inside a greenlet, time.sleep otherwise."""
    if _GEVENT_AVAILABLE:
        gevent.sleep(seconds)
    else:
        time.sleep(seconds)

# Flask
from flask import Flask, render_template, request, jsonify, send_from_directory, make_response
from html import escape as _html_escape

# Reticulum Network Stack
import RNS
import LXMF
import RNS.vendor.umsgpack as msgpack

# ── Bluetooth Interface (optional — graceful if bleak/bless not installed) ────
# The RNS_BluetoothInterface package lives alongside retimesh.py so no
# separate installation is needed.  We add our own directory to sys.path
# once, then try to import.  If bleak/bless are missing we set a flag and
# the UI shows an "Install required" message instead of crashing.
_BT_AVAILABLE      = False
_BT_LOAD_ERROR     = None    # last exception string from _try_load_bluetooth()
BluetoothInterface = None   # populated below if import succeeds

def _try_load_bluetooth():
    global _BT_AVAILABLE, BluetoothInterface, _BT_LOAD_ERROR
    _app_dir = os.path.dirname(os.path.abspath(__file__))
    if _app_dir not in sys.path:
        sys.path.insert(0, _app_dir)
    # Python caches failed imports in sys.modules — once bleak/bless were
    # missing at startup, a plain `import` here would just hit that cached
    # failure forever and never notice the user pip-installing them.  Drop
    # those cached entries (and our wrapper module) so we get a real
    # re-import attempt.  Also force importlib to rescan finders for
    # newly-installed site-packages.
    import importlib
    for _mod in list(sys.modules.keys()):
        if (_mod == "bleak" or _mod.startswith("bleak.")
                or _mod == "bless" or _mod.startswith("bless.")
                or _mod == "RNS_BluetoothInterface"
                or _mod.startswith("RNS_BluetoothInterface.")):
            sys.modules.pop(_mod, None)
    try:
        importlib.invalidate_caches()
    except Exception:
        pass
    try:
        from RNS_BluetoothInterface import BluetoothInterface as _BT
        BluetoothInterface = _BT
        _BT_AVAILABLE = True
        _BT_LOAD_ERROR = None
    except ImportError as e:
        _BT_LOAD_ERROR = str(e)
        logging.getLogger("RetiMesh").warning(
            f"Bluetooth unavailable: {e}. "
            "Install with: pip install bleak bless"
        )
    except Exception as e:
        _BT_LOAD_ERROR = f"{type(e).__name__}: {e}"
        logging.getLogger("RetiMesh").warning(f"Bluetooth load error: {e}")

_try_load_bluetooth()

# ── RNS link-proof debug helper ───────────────────────────────────────────────
# When --debug-rns is passed, this monkey-patch wraps validate_proof() on
# RNS.Link so that every call is logged at DEBUG level.  This reveals whether
# the PROOF is reaching the link-proof handler at all, and whether it passes
# or fails the ECDH/signature check — the primary way to diagnose "link times
# out despite PROOF arriving" failures over BLE.
def _install_rns_link_debug():
    """Wrap RNS.Link.validate_proof to emit a DEBUG log on every call."""
    if getattr(RNS.Link, '_retimesh_debug_patched', False):
        return  # already patched
    _orig_validate = RNS.Link.validate_proof

    _STATUS = {0: 'PENDING', 1: 'HANDSHAKE', 2: 'ACTIVE', 3: 'STALE', 4: 'CLOSED'}

    def _debug_validate(link_self, proof_packet):
        _log = logging.getLogger("RetiMesh")
        try:
            link_id_hex = link_self.link_id.hex() if getattr(link_self, 'link_id', None) else '?'
        except Exception:
            link_id_hex = '?'
        before = _STATUS.get(link_self.status, str(link_self.status))
        _log.debug(f"[RNS] validate_proof called: link={link_id_hex[:16]}… status={before}")
        result = _orig_validate(link_self, proof_packet)
        after  = _STATUS.get(link_self.status, str(link_self.status))
        _log.debug(
            f"[RNS] validate_proof done:   link={link_id_hex[:16]}… "
            f"status={before}→{after} result={result}"
        )
        return result

    RNS.Link.validate_proof = _debug_validate
    RNS.Link._retimesh_debug_patched = True

# ─── Configuration ────────────────────────────────────────────────────────────

APP_NAME = "RetiMesh"
VERSION  = "0.1.0"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5000

# Audio codec settings for low-bandwidth comparison
AUDIO_SAMPLE_RATE = 8000    # 8kHz narrowband
AUDIO_FRAME_MS    = 40      # 40ms frames  (A-5: kept for reference; not used in hot-path)
AUDIO_CHANNELS    = 1

# R-6: Maximum size of a remotely-fetched NomadNet page (reassembly buffer cap).
# A malicious node could otherwise send an unlimited stream of chunks and exhaust
# process memory.  512 KiB is generous for any textual page content.
MAX_PAGE_SIZE = 512 * 1024   # 512 KiB

# A-4: WS_MSG_TYPES dict removed — it mapped every key to an identical string value
# (e.g. {"peer_update": "peer_update"}) and was only used in 2 of ~40 broadcast sites,
# making the code inconsistent rather than safer.  All message-type strings are now
# plain literals throughout the file.  If you want a typed enum in the future,
# use an IntEnum or StrEnum — not a dict that duplicates information.

# ─── Session Token (S-1: API Authentication) ─────────────────────────────────
# A random token is generated fresh at each startup.  The browser receives it
# in a Secure, HttpOnly cookie on the first GET of "/" and must present it on
# every subsequent API call either via that cookie or the X-Retimesh-Token
# header (used by fetch() calls that can't set cookies automatically).
_SESSION_TOKEN = secrets.token_hex(32)

# ─── Input-Validation Helpers ─────────────────────────────────────────────────
_HASH_RE = re.compile(r'^[0-9a-f]{32,64}$')

def _validate_hash(h: str) -> str:
    """Raise ValueError if h is not a plausible hex hash; return the lowercased hash."""
    if not h:
        raise ValueError("hash parameter is required")
    h = h.lower().strip()
    if not _HASH_RE.match(h):
        raise ValueError("invalid hash format (expected hex string, 10–64 chars)")
    return h

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(APP_NAME)

# ─── Database ─────────────────────────────────────────────────────────────────

class Database:
    """SQLite storage for messages, peers, and file transfer records."""

    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        conn = self._get_conn()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS peers (
                    dest_hash   TEXT PRIMARY KEY,
                    display_name TEXT DEFAULT '',
                    last_announce REAL DEFAULT 0,
                    last_seen   REAL DEFAULT 0,
                    rssi        REAL DEFAULT NULL,
                    snr         REAL DEFAULT NULL,
                    identity_hash TEXT DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    peer_hash   TEXT NOT NULL,
                    direction   TEXT NOT NULL CHECK(direction IN ('in','out')),
                    content     TEXT NOT NULL,
                    content_type TEXT DEFAULT 'text',
                    timestamp   REAL NOT NULL,
                    state       INTEGER DEFAULT 0,
                    lxmf_hash   TEXT DEFAULT NULL,
                    identity_hash TEXT DEFAULT ''
                );
                /* state: 0=sending, 1=stored(propagation), 2=delivered */

                CREATE TABLE IF NOT EXISTS file_transfers (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    peer_hash   TEXT NOT NULL,
                    direction   TEXT NOT NULL CHECK(direction IN ('in','out')),
                    filename    TEXT NOT NULL,
                    filesize    INTEGER NOT NULL,
                    file_hash   TEXT NOT NULL,
                    filepath    TEXT DEFAULT NULL,
                    timestamp   REAL NOT NULL,
                    completed   INTEGER DEFAULT 0,
                    identity_hash TEXT DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS call_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    peer_hash   TEXT NOT NULL,
                    direction   TEXT NOT NULL CHECK(direction IN ('in','out')),
                    codec       TEXT NOT NULL,
                    duration    REAL DEFAULT 0,
                    timestamp   REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS config (
                    key         TEXT PRIMARY KEY,
                    value       TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS identities (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    name        TEXT NOT NULL,
                    file_path   TEXT NOT NULL,
                    lxmf_hash   TEXT DEFAULT '',
                    created     REAL NOT NULL,
                    is_active   INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS blocked_peers (
                    dest_hash   TEXT PRIMARY KEY,
                    reason      TEXT DEFAULT '',
                    blocked_at  REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pages (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    title        TEXT NOT NULL DEFAULT 'My Page',
                    path         TEXT NOT NULL DEFAULT '/index',
                    content      TEXT NOT NULL DEFAULT '',
                    content_type TEXT DEFAULT 'text',
                    created      REAL NOT NULL,
                    updated      REAL NOT NULL,
                    is_published INTEGER DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS bookmarks (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_hash   TEXT NOT NULL,
                    path        TEXT NOT NULL DEFAULT '/index',
                    title       TEXT NOT NULL DEFAULT '',
                    added       REAL NOT NULL,
                    sort_order  INTEGER DEFAULT 0,
                    UNIQUE(node_hash, path)
                );

                CREATE TABLE IF NOT EXISTS saved_pages (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_hash    TEXT NOT NULL,
                    path         TEXT NOT NULL DEFAULT '/index',
                    title        TEXT NOT NULL DEFAULT '',
                    content      TEXT NOT NULL DEFAULT '',
                    content_type TEXT DEFAULT 'text',
                    saved_at     REAL NOT NULL,
                    UNIQUE(node_hash, path)
                );

                CREATE TABLE IF NOT EXISTS page_history (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_hash    TEXT NOT NULL,
                    path         TEXT NOT NULL DEFAULT '/index',
                    title        TEXT NOT NULL DEFAULT '',
                    visited_at   REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bluetooth_interfaces (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    name          TEXT NOT NULL UNIQUE,
                    mode          TEXT NOT NULL DEFAULT 'ble',
                    enabled       INTEGER NOT NULL DEFAULT 1,
                    discoverable  INTEGER NOT NULL DEFAULT 1,
                    scan_interval INTEGER NOT NULL DEFAULT 30,
                    max_peers     INTEGER NOT NULL DEFAULT 8,
                    target_mtu    INTEGER NOT NULL DEFAULT 512,
                    static_peers  TEXT    NOT NULL DEFAULT '',
                    created       REAL    NOT NULL,
                    updated       REAL    NOT NULL
                );
                CREATE TABLE IF NOT EXISTS alerts (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_id    TEXT    UNIQUE NOT NULL,
                    sender_hash TEXT    NOT NULL,
                    sender_name TEXT    NOT NULL DEFAULT '',
                    severity    INTEGER NOT NULL DEFAULT 0,
                    title       TEXT    NOT NULL DEFAULT '',
                    message     TEXT    NOT NULL DEFAULT '',
                    timestamp   INTEGER NOT NULL,
                    received_at REAL    NOT NULL,
                    is_read     INTEGER NOT NULL DEFAULT 0,
                    direction   TEXT    NOT NULL DEFAULT 'in'
                );

                CREATE TABLE IF NOT EXISTS groups (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id    TEXT    UNIQUE NOT NULL,
                    name        TEXT    NOT NULL,
                    type        TEXT    NOT NULL DEFAULT 'private',
                    created_at  REAL    NOT NULL,
                    is_owner    INTEGER NOT NULL DEFAULT 0,
                    members     TEXT    NOT NULL DEFAULT '[]'
                );

                CREATE TABLE IF NOT EXISTS group_messages (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id    TEXT    NOT NULL,
                    msg_id      TEXT    UNIQUE,
                    sender_hash TEXT    NOT NULL,
                    sender_name TEXT    NOT NULL DEFAULT '',
                    content     TEXT    NOT NULL DEFAULT '',
                    timestamp   REAL    NOT NULL,
                    is_read     INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS group_invites (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id    TEXT    UNIQUE NOT NULL,
                    group_name  TEXT    NOT NULL,
                    group_type  TEXT    NOT NULL DEFAULT 'private',
                    from_hash   TEXT    NOT NULL,
                    from_name   TEXT    NOT NULL DEFAULT '',
                    members     TEXT    NOT NULL DEFAULT '[]',
                    received_at REAL    NOT NULL,
                    status      TEXT    NOT NULL DEFAULT 'pending'
                );
            """)
            conn.commit()

            # ── Migrations: upgrade old schemas ──
            self._migrate(conn)
        finally:
            conn.close()

    def _migrate(self, conn):
        """Handle schema migrations for databases from older builds.

        Q-1: Only ALTER TABLE ADD COLUMN migrations live here.  CREATE TABLE
        blocks have been removed — _init_db already issues CREATE TABLE IF NOT
        EXISTS for every table, so duplicating those here was dead code that
        created confusion about which site was authoritative.
        """
        try:
            # ── messages: delivered → state column rename ────────────────────────
            msg_columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)").fetchall()}

            if "delivered" in msg_columns and "state" not in msg_columns:
                log.info("Migrating messages table: delivered → state")
                conn.execute("ALTER TABLE messages ADD COLUMN state INTEGER DEFAULT 0")
                conn.execute("UPDATE messages SET state = CASE WHEN delivered = 1 THEN 2 ELSE 0 END")
                try:
                    conn.execute("ALTER TABLE messages DROP COLUMN delivered")
                except Exception:
                    pass  # SQLite < 3.35 doesn't support DROP COLUMN — harmless
                conn.commit()
                log.info("Migration complete")

            # ── messages: add identity_hash ──────────────────────────────────────
            if "identity_hash" not in msg_columns:
                conn.execute("ALTER TABLE messages ADD COLUMN identity_hash TEXT DEFAULT ''")
                conn.commit()

            # ── messages: add is_read (unread badge) ─────────────────────────────
            # Re-read after possible ALTER above
            msg_columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)").fetchall()}
            if "is_read" not in msg_columns:
                conn.execute("ALTER TABLE messages ADD COLUMN is_read INTEGER DEFAULT 0")
                conn.execute("UPDATE messages SET is_read = 1")   # all historical = read
                conn.commit()

            # ── peers: add contact / identity fields ─────────────────────────────
            peer_columns = {row[1] for row in conn.execute("PRAGMA table_info(peers)").fetchall()}
            for col, definition in [
                ("nickname",      "TEXT DEFAULT ''"),
                ("pinned",        "INTEGER DEFAULT 0"),
                ("notes",         "TEXT DEFAULT ''"),
                ("identity_hash", "TEXT DEFAULT ''"),
            ]:
                if col not in peer_columns:
                    conn.execute(f"ALTER TABLE peers ADD COLUMN {col} {definition}")
            conn.commit()

            # ── file_transfers: add identity_hash ────────────────────────────────
            ft_columns = {row[1] for row in conn.execute("PRAGMA table_info(file_transfers)").fetchall()}
            if "identity_hash" not in ft_columns:
                conn.execute("ALTER TABLE file_transfers ADD COLUMN identity_hash TEXT DEFAULT ''")
                conn.commit()

        except Exception as e:
            log.warning(f"Migration check: {e}")

    # ── Peer operations ──

    def upsert_peer(self, dest_hash, display_name="", rssi=None, snr=None, identity_hash=""):
        conn = self._get_conn()
        try:
            now = time.time()
            conn.execute("""
                INSERT INTO peers (dest_hash, display_name, last_announce, last_seen, rssi, snr, identity_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dest_hash) DO UPDATE SET
                    display_name = COALESCE(NULLIF(excluded.display_name, ''), display_name),
                    last_announce = excluded.last_announce,
                    last_seen = excluded.last_seen,
                    rssi = COALESCE(excluded.rssi, rssi),
                    snr  = COALESCE(excluded.snr, snr),
                    identity_hash = COALESCE(NULLIF(excluded.identity_hash, ''), identity_hash)
            """, (dest_hash, display_name, now, now, rssi, snr, identity_hash))
            conn.commit()
        finally:
            conn.close()

    # ── Message operations ──

    # Message states
    MSG_SENDING   = 0
    MSG_STORED    = 1  # stored at propagation node
    MSG_DELIVERED = 2  # delivered to recipient

    def save_message(self, peer_hash, direction, content, content_type="text", lxmf_hash=None, identity_hash=""):
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO messages (peer_hash, direction, content, content_type, timestamp, state, lxmf_hash, identity_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (peer_hash, direction, content, content_type, time.time(), self.MSG_SENDING, lxmf_hash, identity_hash))
            conn.commit()
        finally:
            conn.close()

    def get_messages(self, peer_hash, limit=100, identity_hash=""):
        conn = self._get_conn()
        try:
            try:
                if identity_hash:
                    rows = conn.execute("""
                        SELECT * FROM messages WHERE peer_hash = ? AND identity_hash = ?
                        ORDER BY timestamp DESC LIMIT ?
                    """, (peer_hash, identity_hash, limit)).fetchall()
                else:
                    rows = conn.execute("""
                        SELECT * FROM messages WHERE peer_hash = ?
                        ORDER BY timestamp DESC LIMIT ?
                    """, (peer_hash, limit)).fetchall()
            except Exception:
                rows = conn.execute("""
                    SELECT *, 0 as state FROM messages WHERE peer_hash = ?
                    ORDER BY timestamp DESC LIMIT ?
                """, (peer_hash, limit)).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in reversed(rows)]

    def mark_stored(self, lxmf_hash):
        """Mark message as stored at propagation node (single tick)."""
        conn = self._get_conn()
        try:
            try:
                conn.execute("UPDATE messages SET state = ? WHERE lxmf_hash = ? AND state < ?",
                            (self.MSG_STORED, lxmf_hash, self.MSG_STORED))
            except Exception:
                pass
            conn.commit()
        finally:
            conn.close()

    def mark_delivered(self, lxmf_hash):
        """Mark message as delivered to recipient (double tick)."""
        conn = self._get_conn()
        try:
            try:
                conn.execute("UPDATE messages SET state = ? WHERE lxmf_hash = ?",
                            (self.MSG_DELIVERED, lxmf_hash))
            except Exception:
                # Fallback for old schema
                conn.execute("UPDATE messages SET delivered = 1 WHERE lxmf_hash = ?", (lxmf_hash,))
            conn.commit()
        finally:
            conn.close()

    def get_pending_messages(self, peer_hash, identity_hash=""):
        """Get messages that haven't been delivered yet for retry."""
        conn = self._get_conn()
        try:
            try:
                if identity_hash:
                    rows = conn.execute("""
                        SELECT * FROM messages WHERE peer_hash = ? AND direction = 'out' AND state < ? AND identity_hash = ?
                        ORDER BY timestamp ASC
                    """, (peer_hash, self.MSG_DELIVERED, identity_hash)).fetchall()
                else:
                    rows = conn.execute("""
                        SELECT * FROM messages WHERE peer_hash = ? AND direction = 'out' AND state < ?
                        ORDER BY timestamp ASC
                    """, (peer_hash, self.MSG_DELIVERED)).fetchall()
            except Exception:
                rows = []
        finally:
            conn.close()
        return [dict(r) for r in rows]

    def get_message(self, message_id):
        """Fetch a single message row by its primary-key id."""
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
        finally:
            conn.close()
        return dict(row) if row else None

    def delete_message(self, message_id):
        """Delete a single message by ID."""
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM messages WHERE id = ?", (message_id,))
            conn.commit()
        finally:
            conn.close()

    def delete_conversation(self, peer_hash, identity_hash=""):
        """Delete all messages with a peer for the active identity."""
        conn = self._get_conn()
        try:
            if identity_hash:
                conn.execute("DELETE FROM messages WHERE peer_hash = ? AND identity_hash = ?", (peer_hash, identity_hash))
            else:
                conn.execute("DELETE FROM messages WHERE peer_hash = ?", (peer_hash,))
            conn.commit()
        finally:
            conn.close()

    def delete_peer(self, dest_hash):
        """Delete a peer and all associated data."""
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM peers WHERE dest_hash = ?", (dest_hash,))
            conn.execute("DELETE FROM messages WHERE peer_hash = ?", (dest_hash,))
            conn.execute("DELETE FROM file_transfers WHERE peer_hash = ?", (dest_hash,))
            conn.commit()
        finally:
            conn.close()

    # ── File transfer operations ──

    def save_file_transfer(self, peer_hash, direction, filename, filesize, file_hash, filepath=None, identity_hash=""):
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO file_transfers (peer_hash, direction, filename, filesize, file_hash, filepath, timestamp, identity_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (peer_hash, direction, filename, filesize, file_hash, filepath, time.time(), identity_hash))
            conn.commit()
        finally:
            conn.close()

    # ── Call log operations ──

    def save_call(self, peer_hash, direction, codec, duration=0):
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO call_log (peer_hash, direction, codec, duration, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (peer_hash, direction, codec, duration, time.time()))
            conn.commit()
        finally:
            conn.close()

    # ── Config operations ──

    def get_config(self, key, default=None):
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
        finally:
            conn.close()
        return row["value"] if row else default

    def set_config(self, key, value):
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO config (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """, (key, str(value)))
            conn.commit()
        finally:
            conn.close()

    def get_all_config(self):
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT key, value FROM config").fetchall()
        finally:
            conn.close()
        return {r["key"]: r["value"] for r in rows}

    def wipe_data(self, scope="all"):
        """Wipe data from the DB.  Returns a dict of {table_name: rows_deleted}.

        scope:
          'messages' — chat history only (DMs)
          'groups'   — groups + group messages + members + invites
          'peers'    — known peers (will reappear on next announce)
          'alerts'   — alert broadcasts
          'files'    — file_transfers log + actual files on disk
          'all'      — every user-data table BUT preserves config + identities
                       so the user doesn't lose their setup
          'nuclear'  — every table including identities (full factory reset
                       short of deleting the storage dir entirely)
        """
        result = {}
        # Tables to clear by scope.  Everything else (config, identities)
        # is left alone unless the scope is "nuclear".
        TABLES = {
            "messages": ["messages"],
            "groups":   ["groups", "group_messages", "group_members", "group_invites"],
            "peers":    ["peers"],
            "alerts":   ["alerts"],
            "files":    ["file_transfers"],
        }
        if scope == "all":
            tables = sum(TABLES.values(), [])
        elif scope == "nuclear":
            tables = sum(TABLES.values(), []) + ["identities", "saved_pages",
                "bookmarks", "config"]
        elif scope in TABLES:
            tables = TABLES[scope]
        else:
            return {"error": f"unknown scope: {scope}"}

        conn = self._get_conn()
        try:
            for t in tables:
                try:
                    cur = conn.execute(f"DELETE FROM {t}")
                    result[t] = cur.rowcount
                except Exception as e:
                    # Table may not exist on older DBs — skip silently
                    result[t] = f"skip ({e})"
            conn.commit()
        finally:
            conn.close()
        return result

    # ── Alert operations ──

    ALERT_INFO     = 0
    ALERT_WARNING  = 1
    ALERT_CRITICAL = 2
    ALERT_SOS      = 3

    def save_alert(self, alert_id, sender_hash, sender_name, severity, title, message, timestamp, direction="in"):
        """Insert alert; returns True if new, False if duplicate (alert_id already exists)."""
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT OR IGNORE INTO alerts
                    (alert_id, sender_hash, sender_name, severity, title, message,
                     timestamp, received_at, is_read, direction)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert_id, sender_hash, sender_name, int(severity),
                title[:200], message[:500],
                int(timestamp), time.time(),
                1 if direction == "out" else 0,
                direction,
            ))
            conn.commit()
            changed = conn.execute("SELECT changes()").fetchone()[0]
        except Exception as e:
            log.warning(f"save_alert: {e}")
            changed = 0
        finally:
            conn.close()
        return changed > 0

    def get_alerts(self, limit=200):
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM alerts ORDER BY timestamp DESC LIMIT ?", (limit,)
            ).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    def mark_alert_read(self, row_id):
        conn = self._get_conn()
        try:
            conn.execute("UPDATE alerts SET is_read = 1 WHERE id = ?", (row_id,))
            conn.commit()
        finally:
            conn.close()

    def mark_all_alerts_read(self):
        conn = self._get_conn()
        try:
            conn.execute("UPDATE alerts SET is_read = 1")
            conn.commit()
        finally:
            conn.close()

    def delete_alert(self, row_id):
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM alerts WHERE id = ?", (row_id,))
            conn.commit()
        finally:
            conn.close()

    # ── Group operations ──────────────────────────────────────────────────────

    def save_group(self, group_id, name, gtype, is_owner, members):
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO groups (group_id, name, type, created_at, is_owner, members)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    name    = excluded.name,
                    type    = excluded.type,
                    is_owner = MAX(is_owner, excluded.is_owner),
                    members = excluded.members
            """, (group_id, name, gtype, time.time(), 1 if is_owner else 0,
                  json.dumps(members)))
            conn.commit()
        finally:
            conn.close()

    def get_groups(self):
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM groups ORDER BY created_at DESC"
            ).fetchall()
        finally:
            conn.close()
        result = []
        for r in rows:
            d = dict(r)
            d["members"] = json.loads(d.get("members") or "[]")
            result.append(d)
        return result

    def get_group(self, group_id):
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM groups WHERE group_id = ?", (group_id,)
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return None
        d = dict(row)
        d["members"] = json.loads(d.get("members") or "[]")
        return d

    def update_group_members(self, group_id, members):
        conn = self._get_conn()
        try:
            conn.execute("UPDATE groups SET members = ? WHERE group_id = ?",
                         (json.dumps(members), group_id))
            conn.commit()
        finally:
            conn.close()

    def rename_group(self, group_id, new_name):
        """Update the display name of a group in the database."""
        conn = self._get_conn()
        try:
            conn.execute(
                "UPDATE groups SET name = ? WHERE group_id = ?",
                (new_name, group_id)
            )
            conn.commit()
        finally:
            conn.close()

    def delete_group(self, group_id):
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM groups         WHERE group_id = ?", (group_id,))
            conn.execute("DELETE FROM group_messages WHERE group_id = ?", (group_id,))
            conn.execute("DELETE FROM group_invites  WHERE group_id = ?", (group_id,))
            conn.commit()
        finally:
            conn.close()

    def save_group_message(self, group_id, msg_id, sender_hash, sender_name, content, timestamp):
        """Insert a group message; returns True if new, False if duplicate."""
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT OR IGNORE INTO group_messages
                    (group_id, msg_id, sender_hash, sender_name, content, timestamp, is_read)
                VALUES (?, ?, ?, ?, ?, ?, 0)
            """, (group_id, msg_id, sender_hash, sender_name, content, timestamp))
            conn.commit()
            changed = conn.execute("SELECT changes()").fetchone()[0]
        except Exception as e:
            log.warning(f"save_group_message: {e}")
            changed = 0
        finally:
            conn.close()
        return changed > 0

    def get_group_messages(self, group_id, limit=100):
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM group_messages WHERE group_id = ? ORDER BY timestamp ASC LIMIT ?",
                (group_id, limit)
            ).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    def mark_group_messages_read(self, group_id):
        conn = self._get_conn()
        try:
            conn.execute("UPDATE group_messages SET is_read = 1 WHERE group_id = ?", (group_id,))
            conn.commit()
        finally:
            conn.close()

    def mark_group_message_self_read(self, msg_id):
        """Mark a single outbound group message as is_read=1 (sent by us).

        A-2: replaces the raw self.db._get_conn() pattern in send_group_message,
        ensuring the connection is always closed even if the UPDATE raises.
        """
        conn = self._get_conn()
        try:
            conn.execute("UPDATE group_messages SET is_read = 1 WHERE msg_id = ?", (msg_id,))
            conn.commit()
        finally:
            conn.close()

    def get_unread_group_counts(self):
        """Returns {group_id: unread_count} for all groups with unread messages."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT group_id, COUNT(*) as cnt FROM group_messages WHERE is_read = 0 GROUP BY group_id"
            ).fetchall()
        finally:
            conn.close()
        return {r["group_id"]: r["cnt"] for r in rows}

    def save_group_invite(self, group_id, group_name, group_type, from_hash, from_name, members):
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO group_invites
                    (group_id, group_name, group_type, from_hash, from_name, members, received_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                ON CONFLICT(group_id) DO UPDATE SET
                    group_name = excluded.group_name,
                    from_hash  = excluded.from_hash,
                    from_name  = excluded.from_name,
                    members    = excluded.members,
                    received_at = excluded.received_at,
                    status     = 'pending'
            """, (group_id, group_name, group_type, from_hash, from_name,
                  json.dumps(members), time.time()))
            conn.commit()
        finally:
            conn.close()

    def get_group_invites(self):
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM group_invites WHERE status = 'pending' ORDER BY received_at DESC"
            ).fetchall()
        finally:
            conn.close()
        result = []
        for r in rows:
            d = dict(r)
            d["members"] = json.loads(d.get("members") or "[]")
            result.append(d)
        return result

    def update_invite_status(self, group_id, status):
        conn = self._get_conn()
        try:
            conn.execute("UPDATE group_invites SET status = ? WHERE group_id = ?",
                         (status, group_id))
            conn.commit()
        finally:
            conn.close()

    def get_alert_db_id(self, alert_id_hex):
        """Return the auto-increment DB row id for a given alert_id hex string."""
        conn = self._get_conn()
        try:
            row  = conn.execute("SELECT id FROM alerts WHERE alert_id = ?", (alert_id_hex,)).fetchone()
        finally:
            conn.close()
        return row[0] if row else None

    def get_unread_alert_count(self):
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT COUNT(*) FROM alerts WHERE is_read = 0").fetchone()
        finally:
            conn.close()
        return row[0] if row else 0

    # ── Identity operations ──

    def save_identity(self, name, file_path, lxmf_hash="", is_active=0):
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO identities (name, file_path, lxmf_hash, created, is_active)
                VALUES (?, ?, ?, ?, ?)
            """, (name, file_path, lxmf_hash, time.time(), is_active))
            conn.commit()
        finally:
            conn.close()

    def get_identities(self):
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT * FROM identities ORDER BY created DESC").fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    def set_active_identity(self, identity_id):
        conn = self._get_conn()
        try:
            conn.execute("UPDATE identities SET is_active = 0")
            conn.execute("UPDATE identities SET is_active = 1 WHERE id = ?", (identity_id,))
            conn.commit()
        finally:
            conn.close()

    def delete_identity(self, identity_id):
        """Delete a non-active identity and cascade-remove all associated data."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT lxmf_hash FROM identities WHERE id = ? AND is_active = 0",
                (identity_id,)
            ).fetchone()
            if not row:
                return  # identity not found or is active — do nothing
            lxmf_hash = row[0] or ""
            if lxmf_hash:
                # Cascade: remove messages, file transfers sent/received under this identity
                conn.execute("DELETE FROM messages WHERE identity_hash = ?", (lxmf_hash,))
                conn.execute("DELETE FROM file_transfers WHERE identity_hash = ?", (lxmf_hash,))
                # Also remove group messages sent under this identity
                conn.execute("DELETE FROM group_messages WHERE sender_hash = ?", (lxmf_hash,))
                # Remove alert history associated with this identity
                conn.execute("DELETE FROM alerts WHERE sender_hash = ?", (lxmf_hash,))
            conn.execute("DELETE FROM identities WHERE id = ? AND is_active = 0", (identity_id,))
            conn.commit()
        finally:
            conn.close()

    def get_group_message_db_id(self, msg_id):
        """Look up the auto-increment PK for a group message by its UUID msg_id."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT id FROM group_messages WHERE msg_id = ?", (msg_id,)
            ).fetchone()
        finally:
            conn.close()
        return row[0] if row else None

    # ── Contact management (peer nicknames/pins/notes) ──

    def update_peer_contact(self, dest_hash, nickname=None, pinned=None, notes=None):
        conn = self._get_conn()
        try:
            updates = []
            params = []
            if nickname is not None:
                updates.append("nickname = ?")
                params.append(nickname)
            if pinned is not None:
                updates.append("pinned = ?")
                params.append(1 if pinned else 0)
            if notes is not None:
                updates.append("notes = ?")
                params.append(notes)

            if updates:
                params.append(dest_hash)
                try:
                    conn.execute(f"UPDATE peers SET {', '.join(updates)} WHERE dest_hash = ?", params)
                    conn.commit()
                except Exception as e:
                    log.warning(f"update_peer_contact: {e}")
        finally:
            conn.close()

    def get_peers(self, identity_hash=""):
        conn = self._get_conn()
        try:
            try:
                rows = conn.execute("""
                    SELECT p.*,
                           COALESCE(p.nickname, '') as nickname,
                           COALESCE(p.pinned, 0)   as pinned,
                           COALESCE(p.notes, '')    as notes,
                           (SELECT COUNT(*) FROM messages m
                            WHERE m.peer_hash = p.dest_hash
                              AND m.direction = 'in'
                              AND m.is_read = 0) as unread_count
                    FROM peers p
                    ORDER BY p.pinned DESC, p.last_seen DESC
                """).fetchall()
            except Exception:
                rows = conn.execute("SELECT * FROM peers ORDER BY last_seen DESC").fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    def mark_messages_read(self, peer_hash):
        """Mark all incoming messages from peer_hash as read."""
        conn = self._get_conn()
        try:
            conn.execute("UPDATE messages SET is_read = 1 WHERE peer_hash = ? AND direction = 'in'", (peer_hash,))
            conn.commit()
        finally:
            conn.close()

    # ── Bookmarks ──

    def save_bookmark(self, node_hash, path, title):
        conn = self._get_conn()
        try:
            try:
                conn.execute("""
                    INSERT INTO bookmarks (node_hash, path, title, added, sort_order)
                    VALUES (?, ?, ?, ?, (SELECT COALESCE(MAX(sort_order),0)+1 FROM bookmarks))
                    ON CONFLICT(node_hash, path) DO UPDATE SET title=excluded.title
                """, (node_hash, path or "/index", title or node_hash[:16], time.time()))
                conn.commit()
            except Exception as e:
                log.warning(f"save_bookmark: {e}")
        finally:
            conn.close()

    def get_bookmarks(self):
        conn = self._get_conn()
        try:
            try:
                rows = conn.execute("SELECT * FROM bookmarks ORDER BY sort_order ASC, added DESC").fetchall()
            except Exception:
                rows = []
        finally:
            conn.close()
        return [dict(r) for r in rows]

    def delete_bookmark(self, bookmark_id):
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM bookmarks WHERE id = ?", (bookmark_id,))
            conn.commit()
        finally:
            conn.close()

    # ── Saved pages (offline copies) ──

    def save_offline_page(self, node_hash, path, title, content, content_type="text"):
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO saved_pages
                    (node_hash, path, title, content, content_type, saved_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (node_hash, path, title, content, content_type, time.time()))
            conn.commit()
        except Exception as e:
            log.warning(f"save_offline_page: {e}")
        finally:
            conn.close()

    def get_offline_pages(self):
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT id, node_hash, path, title, content_type, saved_at "
                "FROM saved_pages ORDER BY saved_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_offline_page(self, page_id):
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM saved_pages WHERE id = ?", (page_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def delete_offline_page(self, page_id):
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM saved_pages WHERE id = ?", (page_id,))
            conn.commit()
        finally:
            conn.close()

    # ── Browse history ──

    def add_history(self, node_hash, path, title):
        conn = self._get_conn()
        try:
            # Keep only last 200 entries; prune oldest beyond that
            conn.execute("""
                INSERT INTO page_history (node_hash, path, title, visited_at)
                VALUES (?, ?, ?, ?)
            """, (node_hash, path, title, time.time()))
            conn.execute("""
                DELETE FROM page_history WHERE id NOT IN (
                    SELECT id FROM page_history ORDER BY visited_at DESC LIMIT 200
                )
            """)
            conn.commit()
        except Exception as e:
            log.warning(f"add_history: {e}")
        finally:
            conn.close()

    def get_history(self, limit=50):
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT id, node_hash, path, title, visited_at "
                "FROM page_history ORDER BY visited_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def clear_history(self):
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM page_history")
            conn.commit()
        finally:
            conn.close()

    # ── Bluetooth Interface operations ──

    def get_bluetooth_interfaces(self):
        conn = self._get_conn()
        try:
            try:
                rows = conn.execute(
                    "SELECT * FROM bluetooth_interfaces ORDER BY created ASC"
                ).fetchall()
            except Exception:
                rows = []
        finally:
            conn.close()
        return [dict(r) for r in rows]

    def get_bluetooth_interface(self, name):
        conn = self._get_conn()
        try:
            try:
                row = conn.execute(
                    "SELECT * FROM bluetooth_interfaces WHERE name = ?", (name,)
                ).fetchone()
            except Exception:
                row = None
        finally:
            conn.close()
        return dict(row) if row else None

    def save_bluetooth_interface(self, name, mode="ble", enabled=True,
                                 discoverable=True, scan_interval=30,
                                 max_peers=8, target_mtu=512, static_peers=""):
        now = time.time()
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO bluetooth_interfaces
                    (name, mode, enabled, discoverable, scan_interval,
                     max_peers, target_mtu, static_peers, created, updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (name, mode, int(enabled), int(discoverable),
                  scan_interval, max_peers, target_mtu, static_peers, now, now))
            conn.commit()
        finally:
            conn.close()

    def update_bluetooth_interface(self, name, **kwargs):
        # S-5: safe column-name dispatch — never interpolate user-supplied keys into SQL.
        # Map each logical kwarg name to the exact DB column name.  Only columns
        # in this table may be updated; anything else is silently ignored.
        _ALLOWED_COLS = {
            "mode":          "mode",
            "enabled":       "enabled",
            "discoverable":  "discoverable",
            "scan_interval": "scan_interval",
            "max_peers":     "max_peers",
            "target_mtu":    "target_mtu",
            "static_peers":  "static_peers",
        }
        now = time.time()
        # Build SET clause from validated column names only
        set_parts = []
        values = []
        for key, val in kwargs.items():
            col = _ALLOWED_COLS.get(key)
            if col is None:
                continue
            set_parts.append(f"{col} = ?")   # col comes from our literal dict, not user input
            values.append(val)
        if not set_parts:
            return
        set_parts.append("updated = ?")
        values.append(now)
        values.append(name)
        sql = "UPDATE bluetooth_interfaces SET " + ", ".join(set_parts) + " WHERE name = ?"
        conn = self._get_conn()
        try:
            conn.execute(sql, values)
            conn.commit()
        finally:
            conn.close()

    def delete_bluetooth_interface(self, name):
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM bluetooth_interfaces WHERE name = ?", (name,))
            conn.commit()
        finally:
            conn.close()

    # ── Blocked peers ──

    def block_peer(self, dest_hash, reason=""):
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO blocked_peers (dest_hash, reason, blocked_at)
                VALUES (?, ?, ?)
            """, (dest_hash, reason, time.time()))
            conn.commit()
        finally:
            conn.close()

    def unblock_peer(self, dest_hash):
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM blocked_peers WHERE dest_hash = ?", (dest_hash,))
            conn.commit()
        finally:
            conn.close()

    def is_blocked(self, dest_hash):
        conn = self._get_conn()
        try:
            try:
                row = conn.execute("SELECT 1 FROM blocked_peers WHERE dest_hash = ?", (dest_hash,)).fetchone()
            except Exception:
                row = None
        finally:
            conn.close()
        return row is not None

    def get_blocked_peers(self):
        conn = self._get_conn()
        try:
            try:
                rows = conn.execute("SELECT * FROM blocked_peers ORDER BY blocked_at DESC").fetchall()
            except Exception:
                rows = []
        finally:
            conn.close()
        return [dict(r) for r in rows]

    # ── Page operations ──

    def get_pages(self):
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM pages ORDER BY updated DESC"
            ).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    def get_page(self, page_id):
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT * FROM pages WHERE id = ?", (page_id,)).fetchone()
        finally:
            conn.close()
        return dict(row) if row else None

    def get_page_by_path(self, path):
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM pages WHERE path = ? AND is_published = 1", (path,)
            ).fetchone()
        finally:
            conn.close()
        return dict(row) if row else None

    def save_page(self, title, path, content, content_type="text"):
        now = time.time()
        path = path if path.startswith("/") else "/" + path
        conn = self._get_conn()
        try:
            cursor = conn.execute("""
                INSERT INTO pages (title, path, content, content_type, created, updated, is_published)
                VALUES (?, ?, ?, ?, ?, ?, 1)
            """, (title[:200], path[:200], content[:65536], content_type, now, now))
            page_id = cursor.lastrowid
            conn.commit()
        finally:
            conn.close()
        return page_id

    def update_page(self, page_id, title=None, path=None, content=None, is_published=None, content_type=None):
        now = time.time()
        conn = self._get_conn()
        try:
            if title is not None:
                conn.execute("UPDATE pages SET title = ?, updated = ? WHERE id = ?", (title[:200], now, page_id))
            if path is not None:
                p = path if path.startswith("/") else "/" + path
                conn.execute("UPDATE pages SET path = ?, updated = ? WHERE id = ?", (p[:200], now, page_id))
            if content is not None:
                conn.execute("UPDATE pages SET content = ?, updated = ? WHERE id = ?", (content[:65536], now, page_id))
            if content_type is not None:
                conn.execute("UPDATE pages SET content_type = ?, updated = ? WHERE id = ?", (content_type[:20], now, page_id))
            if is_published is not None:
                conn.execute("UPDATE pages SET is_published = ?, updated = ? WHERE id = ?", (int(is_published), now, page_id))
            conn.commit()
        finally:
            conn.close()

    def delete_page(self, page_id):
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM pages WHERE id = ?", (page_id,))
            conn.commit()
        finally:
            conn.close()


# ─── Audio Codec Abstraction ──────────────────────────────────────────────────

class AudioCodec:
    """
    Base class for audio compression codecs.
    The prototype includes multiple codecs for bandwidth comparison:
    - Raw PCM (baseline, no compression)
    - Mu-law (simple companding, 2:1 ratio)
    - Codec2 (ultra-low bitrate vocoder, if available)
    """
    name = "base"
    bitrate = 0  # bits per second

    def encode(self, pcm_data: bytes) -> bytes:
        raise NotImplementedError

    def decode(self, encoded: bytes) -> bytes:
        raise NotImplementedError

    def info(self) -> dict:
        return {"name": self.name, "bitrate": self.bitrate, "sample_rate": AUDIO_SAMPLE_RATE}


class MuLawCodec(AudioCodec):
    """
    ITU-T G.711 mu-law companding.
    Compresses 16-bit linear PCM to 8-bit mu-law = 64 kbps.
    Simple, low-latency, 2:1 compression ratio.
    """
    name = "mu_law"
    bitrate = AUDIO_SAMPLE_RATE * 8  # 64,000 bps

    MULAW_MAX  = 0x1FFF
    MULAW_BIAS = 33
    MULAW_CLIP = 8159

    # Precomputed lookup tables for speed
    _encode_table = None
    _decode_table = None

    def __init__(self):
        if MuLawCodec._encode_table is None:
            MuLawCodec._build_tables()

    @classmethod
    def _build_tables(cls):
        """Build mu-law encode/decode lookup tables."""
        # Encode table: 16-bit signed -> 8-bit mu-law
        cls._encode_table = bytearray(65536)
        for i in range(65536):
            sample = i - 32768  # convert to signed
            sign = 0x80 if sample < 0 else 0
            sample = min(abs(sample), cls.MULAW_CLIP) + cls.MULAW_BIAS

            exponent = 7
            exp_mask = 0x4000
            while exponent > 0 and not (sample & exp_mask):
                exponent -= 1
                exp_mask >>= 1

            mantissa = (sample >> (exponent + 3)) & 0x0F
            mu_byte = ~(sign | (exponent << 4) | mantissa) & 0xFF
            cls._encode_table[i] = mu_byte

        # Decode table: 8-bit mu-law -> 16-bit signed
        cls._decode_table = [0] * 256
        for i in range(256):
            mu_val = ~i & 0xFF
            sign = mu_val & 0x80
            exponent = (mu_val >> 4) & 0x07
            mantissa = mu_val & 0x0F
            sample = ((mantissa << 3) + cls.MULAW_BIAS) << exponent
            sample -= cls.MULAW_BIAS
            cls._decode_table[i] = -sample if sign else sample

    def encode(self, pcm_data: bytes) -> bytes:
        """Encode 16-bit PCM to 8-bit mu-law."""
        samples = struct.unpack(f"<{len(pcm_data)//2}h", pcm_data)
        encoded = bytearray(len(samples))
        for i, s in enumerate(samples):
            encoded[i] = self._encode_table[s + 32768]
        return bytes(encoded)

    def decode(self, encoded: bytes) -> bytes:
        """Decode 8-bit mu-law to 16-bit PCM."""
        decoded = []
        for b in encoded:
            decoded.append(self._decode_table[b])
        return struct.pack(f"<{len(decoded)}h", *decoded)


class Codec2Codec(AudioCodec):
    """
    Codec2 ultra-low bitrate vocoder — 1200-3200 bps.
    Designed specifically for HF/VHF radio and mesh networks.
    Requires pycodec2 to be installed.
    """
    name = "codec2_1200"
    bitrate = 1200  # Can be 1200, 1600, 2400, 3200

    def __init__(self, mode=None):
        self._available = False
        try:
            import pycodec2
            self._c2 = pycodec2
            # Mode constants: 1200, 1600, 2400, 3200 bps
            mode = mode or 1200
            self._mode = mode
            self.name = f"codec2_{mode}"
            self.bitrate = mode
            self._codec = pycodec2.Codec2(mode)
            self._available = True
            log.info(f"Codec2 available at {mode} bps")
        except ImportError:
            # Codec2 is optional — only available if pycodec2 was installed.
            # Don't spam warnings on every startup for the common case where
            # mu-law is the only codec the user needs.  Demoted to debug.
            log.debug("pycodec2 not installed — codec2 comparison unavailable")
            log.debug("Install with: pip install pycodec2")

    @property
    def available(self):
        return self._available

    def encode(self, pcm_data: bytes) -> bytes:
        if not self._available:
            return pcm_data
        import numpy as np
        samples = np.frombuffer(pcm_data, dtype=np.int16)
        frame_size = self._codec.samples_per_frame()
        encoded_frames = []
        for i in range(0, len(samples) - frame_size + 1, frame_size):
            frame = samples[i:i + frame_size]
            encoded_frames.append(self._codec.encode(frame))
        return b"".join(encoded_frames)

    def decode(self, encoded: bytes) -> bytes:
        if not self._available:
            return encoded
        bits_per_frame = self._codec.bits_per_frame()
        bytes_per_frame = (bits_per_frame + 7) // 8
        decoded_frames = []
        for i in range(0, len(encoded) - bytes_per_frame + 1, bytes_per_frame):
            frame = encoded[i:i + bytes_per_frame]
            decoded_frames.append(self._codec.decode(frame))
        import numpy as np
        return np.concatenate(decoded_frames).astype(np.int16).tobytes()

    def info(self):
        d = super().info()
        d["available"] = self._available
        return d


# ── Codec ID map (used in the binary wire protocol) ───────────────────────────
# Protocol byte inside each RNS audio packet: identifies how to decode the frame.
# Only two codecs are supported: mu-law (standard IP/LAN) and Codec2-1200
# (ultra-low-bandwidth for LoRa / HF radio).  All others were removed to keep
# the codec surface area small and match real-world deployment needs.
CODEC_ID_MAP: dict = {
    "mu_law":      0x00,
    "codec2_1200": 0x06,
}
CODEC_FROM_ID: dict = {v: k for k, v in CODEC_ID_MAP.items()}


# Codec registry.  Two codecs only:
#   mu_law      — standard IP / LAN transport (64 kbps)
#   codec2_1200 — LoRa / HF radio transport (1.2 kbps)
# Browsers always send and receive mu-law; the server transcodes to/from
# Codec2 on the RNS wire when the call's negotiated codec is codec2_1200.
CODECS = {}

def init_codecs():
    global CODECS
    CODECS["mu_law"] = MuLawCodec()
    c2_1200 = Codec2Codec(1200)
    if c2_1200.available:
        CODECS["codec2_1200"] = c2_1200


# ─── RNS Announce Handlers ────────────────────────────────────────────────────
#
# RNS.Transport.register_announce_handler() requires an *object* that has:
#   • aspect_filter (str class attribute): controls which announces are delivered
#   • received_announce(dest_hash, identity, app_data): called for each announce
# Passing a raw function or keyword args is incorrect and raises TypeError.

class LXMFAnnounceHandler:
    """Handles LXMF peer-discovery announces (aspect: lxmf.delivery)."""
    aspect_filter = "lxmf.delivery"

    def __init__(self, mesh_node):
        self.mesh_node = mesh_node

    def received_announce(self, destination_hash, announced_identity, app_data):
        self.mesh_node._on_announce(destination_hash, announced_identity, app_data)


class NomadNetAnnounceHandler:
    """Handles NomadNet / page-host announces (aspect: nomadnetwork.node)."""
    aspect_filter = "nomadnetwork.node"

    def __init__(self, mesh_node):
        self.mesh_node = mesh_node

    def received_announce(self, destination_hash, announced_identity, app_data):
        self.mesh_node._on_nomadnet_announce(destination_hash, announced_identity, app_data)


class AlertAnnounceHandler:
    """Handles RetiMesh emergency broadcast announces (aspect: RetiMesh.alert)."""
    aspect_filter = APP_NAME + ".alert"

    def __init__(self, mesh_node):
        self.mesh_node = mesh_node

    def received_announce(self, destination_hash, announced_identity, app_data):
        self.mesh_node._on_alert_announce(destination_hash, announced_identity, app_data)


# ─── Reticulum Mesh Backend ──────────────────────────────────────────────────

class MeshNode:
    """
    Core Reticulum node: handles identity, LXMF messaging,
    file transfers via RNS links, and audio call signaling.
    """

    def __init__(self, storage_dir, config_dir=None):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.files_dir = self.storage_dir / "files"
        self.files_dir.mkdir(exist_ok=True)

        self.identity_path = self.storage_dir / "identity"
        self.config_dir = config_dir

        self.reticulum = None
        self.identity   = None
        self.lxmf_dest  = None
        self.lxmf_router = None

        self.peers = {}                  # dest_hash_hex -> peer info
        # R-4: reverse map identity.hexhash → dest_hash_hex for O(1) caller lookup
        self._identity_to_dest: dict = {}  # identity_hexhash -> dest_hash_hex
        # WebSocket clients — one per open /ws connection.  Using WebSocket
        # (not SSE) because it's lower-overhead for the call path: binary
        # audio frames go over the same socket as signaling, no HTTP POST
        # per frame, no base64 wrapping.
        self.ws_clients = set()
        self.active_calls = {}   # dest_hash_hex -> call state
        self.active_links = {}   # dest_hash_hex -> RNS.Link

        self._db = None
        self._lock = gevent.lock.RLock() if _GEVENT_AVAILABLE else threading.Lock()
        self._active_identity_hash = ""

        # Per-peer outbound audio sequence counters (wraps at 65535)
        self._audio_seq: dict = {}

        self._page_destination = None     # RNS destination for hosting pages
        self._page_link_cache = {}        # cached open links keyed by dest_hash
        self._nomadnet_nodes = {}         # discovered NomadNet nodes: hash -> {name, seen}

        self._alert_destination = None   # RNS destination for emergency broadcast alerts

        # Rate-limit for outbound alert broadcasts: max 3 per 60-second window
        self._alert_rate: dict = {"count": 0, "window_start": 0.0}

        # Pending codec from LXMF call_request — bridges the gap between LXMF
        # message arrival and RNS Link establishment (which may arrive in either order).
        # Maps peer_hex -> codec_name (e.g. "mu_law", "opus").
        self._pending_call_codecs: dict = {}

        # Bluetooth interfaces: name -> {"iface": BluetoothInterface, "config": dict}
        self._bt_interfaces = {}

    @property
    def active_identity_hash(self):
        """The hex hash of the currently active identity."""
        if not self._active_identity_hash and self.identity:
            self._active_identity_hash = self.identity.hexhash if hasattr(self.identity, 'hexhash') else ""
        return self._active_identity_hash

    @property
    def db(self):
        if self._db is None:
            self._db = Database(str(self.storage_dir / "retimesh.db"))
        return self._db

    def start(self):
        """Initialize Reticulum, identity, and LXMF router."""
        log.info("Starting Reticulum...")
        self.reticulum = RNS.Reticulum(configdir=self.config_dir)

        # Check if there's an active identity in the database
        active_identity_path = None
        identities = self.db.get_identities()
        for ident in identities:
            if ident.get("is_active"):
                active_identity_path = ident["file_path"]
                log.info(f"Using active identity: {ident['name']} ({ident['file_path']})")
                break

        # Load identity from active DB entry, or fall back to default file
        id_path = active_identity_path or str(self.identity_path)

        if os.path.exists(id_path):
            self.identity = RNS.Identity.from_file(id_path)
            log.info(f"Loaded identity: {self.identity.hexhash}")
        else:
            self.identity = RNS.Identity()
            self.identity.to_file(str(self.identity_path))
            log.info(f"Created new identity: {self.identity.hexhash}")
            id_path = str(self.identity_path)

        # Register default identity in DB if not already there
        if not identities:
            self.db.save_identity("Default", id_path, self.identity.hexhash, is_active=1)

        # Set up LXMF router for messaging
        self.lxmf_router = LXMF.LXMRouter(
            identity=self.identity,
            storagepath=str(self.storage_dir / "lxmf"),
        )
        self.lxmf_dest = self.lxmf_router.register_delivery_identity(
            self.identity,
            display_name=APP_NAME,
        )
        self.lxmf_router.register_delivery_callback(self._on_lxmf_delivery)

        # Register announce handler for peer discovery
        self._announce_handler = LXMFAnnounceHandler(self)
        RNS.Transport.register_announce_handler(self._announce_handler)

        # Set up audio call destination (separate from LXMF, for RNS Link calls)
        self.call_dest = RNS.Destination(
            self.identity,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            "retimesh", "call"
        )
        self.call_dest.set_link_established_callback(self._on_incoming_link)

        # Set up emergency alert destination and announce handler.
        # R-1: Must be IN — we own this identity and want to receive inbound links/announces
        # addressed to it.  OUT is for addressing a *remote* destination, not a local one.
        self._alert_destination = RNS.Destination(
            self.identity,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            APP_NAME, "alert"
        )
        self._alert_handler = AlertAnnounceHandler(self)
        RNS.Transport.register_announce_handler(self._alert_handler)
        log.info(f"Alert destination ready: {RNS.prettyhexrep(self._alert_destination.hash)}")

        # Send a priming announce for the alert destination after a short
        # delay.  Some RNS versions require that a destination has been
        # announced at least once before outbound announces on that destination
        # are accepted by the Transport layer.  Without this, the first
        # user-triggered alert from this node silently fails on some setups
        # (reported on Linux machines that could receive alerts but not send
        # them).  The delay lets interfaces finish initialising first.
        def _prime_alert_dest():
            try:
                if _GEVENT_AVAILABLE:
                    import gevent as _g
                    _g.sleep(3.0)
                else:
                    time.sleep(3.0)
                self._alert_destination.announce()
                log.info("Alert destination primed")
            except Exception as _e:
                log.debug(f"Alert destination priming skipped: {_e}")
        if _GEVENT_AVAILABLE:
            import gevent as _g
            _g.spawn(_prime_alert_dest)
        else:
            threading.Thread(target=_prime_alert_dest, daemon=True).start()

        # Load propagation node from config if set
        prop_node = self.db.get_config("propagation_node")
        if prop_node:
            try:
                self.lxmf_router.set_outbound_propagation_node(bytes.fromhex(prop_node))
                log.info(f"Propagation node loaded: {prop_node[:16]}...")
                # Schedule periodic auto-sync so the user doesn't have to
                # click Sync manually every time they come online.
                self._start_propagation_sync_loop()
            except Exception as e:
                log.warning(f"Failed to load propagation node: {e}")

        # Auto-enable acting-as-propagation-node if the user previously
        # turned it on.  This keeps the setting persistent across restarts.
        if self.db.get_config("act_as_propagation_node", "false") == "true":
            try:
                if hasattr(self.lxmf_router, "enable_propagation"):
                    self.lxmf_router.enable_propagation()
                    log.info("Propagation node mode enabled from saved config")
                else:
                    log.warning("LXMF version does not support acting as a propagation node")
            except Exception as e:
                log.warning(f"Failed to enable propagation node: {e}")

        # Set up page hosting for NomadNet-compatible pages
        self._setup_page_hosting()

        log.info(f"LXMF address: {RNS.prettyhexrep(self.lxmf_dest.hash)}")
        log.info(f"Call destination: {RNS.prettyhexrep(self.call_dest.hash)}")
        log.info("Reticulum node started.")
        self._start_time = time.time()

        # Load any saved Bluetooth interfaces from the database
        self._bt_interfaces = {}
        self._load_bluetooth_interfaces()

        # Start auto-announce if enabled
        self._auto_announce_greenlet = None
        auto_announce = self.db.get_config("auto_announce", "true")
        if auto_announce == "true":
            self._start_auto_announce()

        # Send an immediate announce 2 s after startup so peers see us fast
        if _GEVENT_AVAILABLE:
            import gevent
            display_name = self.db.get_config("display_name", APP_NAME)
            gevent.spawn_later(2.0, self.send_announce, display_name)

    def _load_bluetooth_interfaces(self):
        """Load Bluetooth interfaces saved in the DB and register them with RNS."""
        if not _BT_AVAILABLE:
            return
        rows = self.db.get_bluetooth_interfaces()
        for row in rows:
            if not row.get("enabled", 1):
                continue
            name = row["name"]
            try:
                config = {
                    "mode":          row.get("mode", "ble"),
                    "discoverable":  bool(row.get("discoverable", 1)),
                    "scan_interval": int(row.get("scan_interval", 30)),
                    "max_peers":     int(row.get("max_peers", 8)),
                    "target_mtu":    int(row.get("target_mtu", 512)),
                    "static_peers":  [
                        p.strip() for p in row.get("static_peers", "").split(",")
                        if p.strip()
                    ],
                }
                # Use retry-enabled attach at startup so transient BT adapter
                # errors (power-cycle, driver reset) recover automatically.
                self._attach_bluetooth_interface_with_retry(name, config)
                log.info(f"Bluetooth interface '{name}' queued for attach (mode={config['mode']})")
            except Exception as e:
                log.error(f"Failed to queue Bluetooth interface '{name}': {e}")

    def _attach_bluetooth_interface(self, name, config):
        """Instantiate and attach a single Bluetooth interface at runtime."""
        if not _BT_AVAILABLE:
            raise RuntimeError("Bluetooth dependencies not installed. Run: pip install bleak bless")
        if name in self._bt_interfaces:
            raise ValueError(f"Interface '{name}' is already running")
        iface = BluetoothInterface(RNS.Transport, name, config=config)
        RNS.Transport.interfaces.append(iface)
        self._bt_interfaces[name] = {
            "iface":        iface,
            "config":       config,
            "attach_ts":    time.time(),
            "retry_count":  0,
            "last_error":   None,
        }
        log.info(f"Bluetooth interface '{name}' attached live")

    def _attach_bluetooth_interface_with_retry(self, name, config, max_retries=5):
        """Attach a Bluetooth interface with exponential-backoff retry (gevent-friendly).

        Uses a Fibonacci-like backoff: 2s, 4s, 8s, 16s, 30s (capped).
        Each attempt updates ``_bt_interfaces[name]["retry_count"]`` and
        ``_bt_interfaces[name]["last_error"]`` so the health endpoint can report state.
        """
        if not _GEVENT_AVAILABLE:
            # Fallback: single attempt without retry
            self._attach_bluetooth_interface(name, config)
            return

        # Use a sentinel entry so health checks can see "connecting" state
        self._bt_interfaces.setdefault(name, {
            "iface":       None,
            "config":      config,
            "attach_ts":   time.time(),
            "retry_count": 0,
            "last_error":  None,
        })

        def _attempt():
            delays = [2, 4, 8, 16, 30]
            for attempt in range(max_retries):
                # Remove stale sentinel before re-trying
                entry = self._bt_interfaces.get(name, {})
                if entry.get("iface") is not None:
                    # Already attached by another path
                    return
                try:
                    # Clear sentinel so _attach does not raise "already running"
                    self._bt_interfaces.pop(name, None)
                    self._attach_bluetooth_interface(name, config)
                    log.info(f"BT '{name}' attached on attempt {attempt + 1}")
                    return
                except Exception as exc:
                    delay = delays[min(attempt, len(delays) - 1)]
                    log.warning(
                        f"BT '{name}' attach failed (attempt {attempt + 1}/{max_retries}): {exc} "
                        f"— retrying in {delay}s"
                    )
                    self._bt_interfaces[name] = {
                        "iface":       None,
                        "config":      config,
                        "attach_ts":   time.time(),
                        "retry_count": attempt + 1,
                        "last_error":  str(exc),
                    }
                    gevent.sleep(delay)
            log.error(f"BT '{name}' failed to attach after {max_retries} attempts")

        gevent.spawn(_attempt)

    def _detach_bluetooth_interface(self, name):
        """Detach and shut down a running Bluetooth interface."""
        entry = self._bt_interfaces.pop(name, None)
        if entry is None:
            return
        iface = entry["iface"]
        try:
            iface.detach()
        except Exception:
            pass
        try:
            RNS.Transport.interfaces.remove(iface)
        except ValueError:
            pass
        log.info(f"Bluetooth interface '{name}' detached")

    def _start_auto_announce(self):
        """Start periodic auto-announce with configurable interval."""
        import gevent
        def _auto_announce_loop():
            while True:
                interval = int(self.db.get_config("announce_interval", "30"))
                interval = max(10, min(300, interval))  # clamp 10-300s
                gevent.sleep(interval)
                if self.db.get_config("auto_announce", "true") != "true":
                    break
                try:
                    display_name = self.db.get_config("display_name", APP_NAME)
                    self.send_announce(display_name)
                except Exception as e:
                    log.debug(f"Auto-announce error: {e}")
        self._auto_announce_greenlet = gevent.spawn(_auto_announce_loop)
        log.info("Auto-announce enabled")

    def _stop_auto_announce(self):
        """Stop periodic auto-announce."""
        if self._auto_announce_greenlet:
            self._auto_announce_greenlet.kill()
            self._auto_announce_greenlet = None
            log.info("Auto-announce disabled")

    def _setup_page_hosting(self):
        """Register a Reticulum destination for hosting pages (NomadNet-compatible)."""
        try:
            # Use the same identity as LXMF — one identity per node
            self._page_destination = RNS.Destination(
                self.identity,
                RNS.Destination.IN,
                RNS.Destination.SINGLE,
                "nomadnetwork",
                "node",
            )
            self._page_destination.set_link_established_callback(self._on_page_link)

            # Announce ourselves as a NomadNet-compatible node
            app_data = self.db.get_config("display_name", APP_NAME).encode("utf-8")
            self._page_destination.announce(app_data)
            log.info(f"Page hosting destination: {self._page_destination.hexhash}")

            # Listen for other NomadNet nodes — must use class-based handler
            self._nomadnet_handler = NomadNetAnnounceHandler(self)
            RNS.Transport.register_announce_handler(self._nomadnet_handler)
        except Exception as e:
            log.warning(f"Page hosting setup failed: {e}")

    def _on_page_link(self, link):
        """Called when a remote node opens a link to browse our pages."""
        link.set_packet_callback(self._on_page_request)
        link.set_link_closed_callback(self._on_page_link_closed)

    # Max bytes of page payload per RNS packet.  Leaves room for a 5-byte
    # chunk header (magic + length) under the ~383-byte RNS Link usable payload.
    _PAGE_CHUNK_SIZE = 370

    def _on_page_request(self, message, packet):
        """Handle incoming page request: message bytes = path (e.g. b'/index').

        Responses larger than the RNS Link MTU are split into chunks framed
        as: [0xAB][total_len(4 bytes, BE)][payload_slice].  The first chunk's
        total_len lets the receiver know when reassembly is complete.
        """
        try:
            path = message.decode("utf-8", errors="replace").strip()
            if not path:
                path = "/index"
            page = self.db.get_page_by_path(path)
            if page:
                response = json.dumps({
                    "title":   page["title"],
                    "content": page["content"],
                    "type":    page.get("content_type", "text"),
                }).encode("utf-8")
            else:
                response = json.dumps({
                    "title":   "Not Found",
                    "content": f"No page at {path}",
                    "type":    "text",
                }).encode("utf-8")

            total_len = len(response)
            # Split into chunks that fit inside one RNS Link packet.
            # Each chunk: [0xAB][total_len:4][payload]
            # The receiver accumulates payload bytes until total_len is reached.
            header_prefix = b"\xAB" + struct.pack(">I", total_len)
            for offset in range(0, max(total_len, 1), self._PAGE_CHUNK_SIZE):
                chunk = response[offset:offset + self._PAGE_CHUNK_SIZE]
                try:
                    RNS.Packet(packet.link, header_prefix + chunk).send()
                except Exception as e:
                    log.warning(f"Page chunk send failed at offset {offset}: {e}")
                    break
                # Small yield so RNS has time to actually transmit each packet
                # before we queue the next one (prevents back-pressure drops).
                _sleep(0.02)
            log.info(f"Served page {path} ({total_len} bytes in "
                     f"{(total_len + self._PAGE_CHUNK_SIZE - 1) // self._PAGE_CHUNK_SIZE} chunks)")
        except Exception as e:
            log.warning(f"Page request error: {e}")
            import traceback
            traceback.print_exc()

    def _on_page_link_closed(self, link):
        pass

    def _on_nomadnet_announce(self, destination_hash, announced_identity, app_data):
        """Track discovered NomadNet / page-hosting nodes."""
        h = RNS.hexrep(destination_hash, delimit=False)
        name = ""
        if app_data:
            try:
                name = app_data.decode("utf-8", errors="replace").strip()[:100]
            except Exception:
                pass
        self._nomadnet_nodes[h] = {
            "hash": h,
            "name": name or h[:16],
            "last_seen": time.time(),
        }
        log.debug(f"NomadNet node discovered: {h[:16]} ({name})")
        self._ws_broadcast({"type": "nomadnet_node", "hash": h, "name": name or h[:16]})

    def fetch_remote_page(self, dest_hash: str, path: str = "/index") -> dict:
        """Fetch a page from a remote node. Returns dict with title, content, error.

        Supports chunked responses: the remote sends one or more packets of the
        form [0xAB][total_len:4][payload_slice]; we accumulate payload until
        total_len bytes are received, then parse as JSON.
        """
        path = path if path.startswith("/") else "/" + path

        result = {"title": "", "content": "", "error": None}
        if _GEVENT_AVAILABLE:
            event = gevent.event.Event()
        else:
            event = threading.Event()

        # Reassembly state
        reasm = {"total": 0, "buf": bytearray()}

        try:
            dest_bytes = bytes.fromhex(dest_hash)
            identity = RNS.Identity.recall(dest_bytes)
            if not identity:
                if not RNS.Transport.has_path(dest_bytes):
                    RNS.Transport.request_path(dest_bytes)
                    deadline = time.time() + 15
                    while not RNS.Transport.has_path(dest_bytes) and time.time() < deadline:
                        _sleep(0.3)
                identity = RNS.Identity.recall(dest_bytes)
            if not identity:
                result["error"] = "Cannot reach destination: no path found"
                return result

            dest = RNS.Destination(
                identity,
                RNS.Destination.OUT,
                RNS.Destination.SINGLE,
                "nomadnetwork",
                "node",
            )
            link = RNS.Link(dest)

            def _on_link_up(lnk):
                try:
                    RNS.Packet(lnk, path.encode("utf-8")).send()
                except Exception as e:
                    result["error"] = str(e)
                    event.set()

            def _finalize(raw):
                try:
                    payload = json.loads(raw.decode("utf-8"))
                    result["title"]   = payload.get("title", "")
                    result["content"] = payload.get("content", "")
                    result["type"]    = payload.get("type", "text")
                except Exception:
                    result["content"] = raw.decode("utf-8", errors="replace")
                event.set()

            def _on_packet(data, pkt):
                # Chunked format: [0xAB][total_len:4][payload_slice]
                if len(data) >= 5 and data[0] == 0xAB:
                    total_len = struct.unpack(">I", data[1:5])[0]
                    if reasm["total"] == 0:
                        # R-6: reject pages that advertise a size beyond our cap
                        if total_len > MAX_PAGE_SIZE:
                            result["error"] = f"Remote page too large ({total_len} bytes, max {MAX_PAGE_SIZE})"
                            event.set()
                            return
                        reasm["total"] = total_len
                    reasm["buf"].extend(data[5:])
                    # R-6: also cap the live buffer so a malformed stream can't overflow
                    if len(reasm["buf"]) > MAX_PAGE_SIZE:
                        result["error"] = "Remote page exceeded maximum allowed size"
                        event.set()
                        return
                    if len(reasm["buf"]) >= reasm["total"]:
                        _finalize(bytes(reasm["buf"][:reasm["total"]]))
                else:
                    # Legacy single-packet response (no chunking header)
                    _finalize(data)

            def _on_link_closed(lnk):
                # If we already have partial data but never hit total_len, try to parse it
                if not event.is_set():
                    if reasm["buf"] and reasm["total"] and len(reasm["buf"]) >= reasm["total"]:
                        _finalize(bytes(reasm["buf"][:reasm["total"]]))
                    else:
                        result["error"] = "Link closed before response"
                        event.set()

            link.set_link_established_callback(_on_link_up)
            link.set_packet_callback(_on_packet)
            link.set_link_closed_callback(_on_link_closed)

            # Wait up to 30 seconds for full response (chunked pages take longer)
            event.wait(timeout=30)
            if not event.is_set():
                result["error"] = "Timeout waiting for page"

            try:
                link.teardown()
            except Exception:
                pass

        except Exception as e:
            result["error"] = str(e)

        return result

    def get_identity_info(self):
        # RNS Destination uses .hash (bytes) — convert to hex string safely
        identity_hash = self.identity.hexhash if hasattr(self.identity, 'hexhash') else ""
        lxmf_hash = ""
        if self.lxmf_dest:
            if hasattr(self.lxmf_dest, 'hexhash'):
                lxmf_hash = self.lxmf_dest.hexhash
            elif hasattr(self.lxmf_dest, 'hash'):
                lxmf_hash = RNS.hexrep(self.lxmf_dest.hash, delimit=False)
            else:
                lxmf_hash = str(self.lxmf_dest)
        return {
            "identity_hash": identity_hash,
            "lxmf_address": lxmf_hash,
        }

    # ── Announce ──

    # ── Emergency Alert Broadcasting ──────────────────────────────────────────

    def send_alert(self, severity, title, message=""):
        """Send an emergency broadcast alert via Reticulum announce."""
        import struct as _struct, os as _os

        severity = max(0, min(3, int(severity)))
        title    = str(title).strip()[:64]
        message  = str(message).strip()[:72]

        if not title:
            return {"error": "title is required"}
        if self._alert_destination is None:
            return {"error": "Alert destination not initialised"}

        # Rate limiting: max 3 alerts per 60-second rolling window
        _ALERT_RATE_LIMIT  = 3
        _ALERT_RATE_WINDOW = 60.0
        _now_rate = time.time()
        with self._lock:
            if _now_rate - self._alert_rate["window_start"] >= _ALERT_RATE_WINDOW:
                # New window: reset counter
                self._alert_rate["count"] = 0
                self._alert_rate["window_start"] = _now_rate
            if self._alert_rate["count"] >= _ALERT_RATE_LIMIT:
                remaining = int(_ALERT_RATE_WINDOW - (_now_rate - self._alert_rate["window_start"]))
                log.warning(f"Alert rate limit hit — throttled (resets in {remaining}s)")
                return {"error": f"Rate limit exceeded. You can send {_ALERT_RATE_LIMIT} alerts per {int(_ALERT_RATE_WINDOW)}s. Try again in {remaining}s."}
            self._alert_rate["count"] += 1

        # Build compact binary payload
        alert_id_bytes = _os.urandom(8)
        ts             = int(time.time())
        title_b        = title.encode("utf-8")[:64]
        message_b      = message.encode("utf-8")[:72]
        payload = (
            alert_id_bytes
            + _struct.pack(">BI", severity, ts)
            + _struct.pack(">B", len(title_b))
            + title_b
            + _struct.pack(">B", len(message_b))
            + message_b
        )

        try:
            # Announces carry app_data up to ~512 bytes in modern RNS, but
            # some interfaces (notably AutoInterface over certain switches,
            # and older RNS versions) silently drop oversized announces.
            # Keep well under the safe ceiling to guarantee delivery.
            if len(payload) > 300:
                log.warning(f"Alert payload unusually large ({len(payload)} bytes)")
            self._alert_destination.announce(app_data=payload)
            log.info(f"Alert broadcast: severity={severity} title={title!r} ({len(payload)} bytes)")
        except Exception as e:
            # Previously this just returned the error string.  Log the full
            # traceback too so the root cause (permissions on the outbound
            # multicast socket, payload-size rejection, RNS version mismatch,
            # etc.) is visible in the terminal when a user reports the bug.
            log.error(f"Alert announce failed: {e}")
            import traceback
            log.error(traceback.format_exc())
            return {"error": str(e)}

        alert_id_hex = alert_id_bytes.hex()
        my_hash      = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else "self"
        my_name      = self.db.get_config("display_name", APP_NAME)

        self.db.save_alert(
            alert_id    = alert_id_hex,
            sender_hash = my_hash,
            sender_name = my_name,
            severity    = severity,
            title       = title,
            message     = message,
            timestamp   = ts,
            direction   = "out",
        )

        db_id     = self.db.get_alert_db_id(alert_id_hex)
        alert_obj = {
            "id":          db_id,
            "alert_id":    alert_id_hex,
            "sender_hash": my_hash,
            "sender_name": my_name,
            "severity":    severity,
            "title":       title,
            "message":     message,
            "timestamp":   ts,
            "direction":   "out",
            "is_read":     1,
        }
        self._ws_broadcast({"type": "alert_sent", "alert": alert_obj})
        return {"status": "ok", "alert_id": alert_id_hex}

    def _on_alert_announce(self, destination_hash, announced_identity, app_data):
        """Handle incoming emergency broadcast alert announces."""
        import struct as _struct

        # Drop if user has disabled the feature
        if self.db.get_config("alerts_enabled", "true") != "true":
            return

        if not app_data or len(app_data) < 15:
            return  # Too short to be a valid alert payload

        try:
            alert_id_hex = app_data[:8].hex()
            severity     = _struct.unpack(">B", app_data[8:9])[0]
            ts           = _struct.unpack(">I", app_data[9:13])[0]
            title_len    = _struct.unpack(">B", app_data[13:14])[0]
            offset       = 14
            title        = app_data[offset:offset + title_len].decode("utf-8", errors="replace")
            offset      += title_len
            if offset >= len(app_data):
                message = ""
            else:
                msg_len = _struct.unpack(">B", app_data[offset:offset + 1])[0]
                message = app_data[offset + 1:offset + 1 + msg_len].decode("utf-8", errors="replace")
        except Exception as e:
            log.warning(f"Alert decode error: {e}")
            return

        severity    = max(0, min(3, severity))
        # alert_dest_hash is the raw hex of the ALERT destination — NOT the LXMF destination.
        # The two are derived from the same identity but with different name components,
        # so their hashes differ.  We keep this as the canonical alert identifier but
        # must resolve the LXMF hash separately for peer table lookups.
        alert_dest_hash = RNS.hexrep(destination_hash, delimit=False) if isinstance(destination_hash, bytes) else str(destination_hash)

        # Drop our own re-broadcast (we already stored it with direction="out").
        #
        # IMPORTANT: We MUST compare identity hashes, NOT destination hashes.
        # The alert destination and the LXMF destination are derived from the
        # SAME identity but with DIFFERENT name components — their hashes are
        # therefore DIFFERENT.  Comparing lxmf_dest.hash against the alert
        # announce's destination_hash would NEVER match, letting every node
        # receive its own alert.  Comparing identity hashes is the correct check
        # because a node's identity is constant across all its destinations.
        if announced_identity is not None and self.identity is not None:
            try:
                if announced_identity.hexhash == self.identity.hexhash:
                    return   # this is our own announce — already stored as "out"
            except Exception:
                pass

        # Drop alerts older than 24 hours — stale mesh re-broadcasts
        _now = time.time()
        if _now - ts > 86400:
            log.debug(f"Alert dropped — too old: ts={ts}, age={_now - ts:.0f}s title={title!r}")
            return

        # Resolve sender display name via identity→LXMF-dest reverse map.
        # self.peers is keyed by LXMF destination hashes; the alert destination hash
        # is a *different* hash derived from the same identity.  We use
        # _identity_to_dest to bridge between the two.
        lxmf_sender_hash = alert_dest_hash  # fallback — unknown peer
        if announced_identity is not None:
            try:
                lxmf_h = self._identity_to_dest.get(announced_identity.hexhash)
                if lxmf_h:
                    lxmf_sender_hash = lxmf_h
            except Exception:
                pass

        peer_info = self.peers.get(lxmf_sender_hash, {})
        if not peer_info:
            # Peer not in in-memory map yet — fall back to DB (handles offline peers
            # whose announces we received earlier but haven't cached since last restart)
            try:
                conn = self.db._get_conn()
                try:
                    row = conn.execute(
                        "SELECT display_name FROM peers WHERE dest_hash = ?",
                        (lxmf_sender_hash,)
                    ).fetchone()
                finally:
                    conn.close()
                if row and row[0]:
                    peer_info = {"display_name": row[0]}
            except Exception as _db_err:
                log.debug(f"Alert sender DB lookup failed: {_db_err}")

        sender_name = peer_info.get("display_name") or (lxmf_sender_hash[:16] + "…")
        # Use the LXMF hash as the canonical sender_hash stored in the DB so that
        # the UI can cross-reference alerts with contact records.
        sender_hash = lxmf_sender_hash

        inserted = self.db.save_alert(
            alert_id    = alert_id_hex,
            sender_hash = sender_hash,
            sender_name = sender_name,
            severity    = severity,
            title       = title,
            message     = message,
            timestamp   = ts,
            direction   = "in",
        )

        if not inserted:
            return  # Duplicate — already stored

        log.info(f"Alert received: sev={severity} from={sender_hash[:16]}… (alert_dest={alert_dest_hash[:16]}…) title={title!r}")

        db_id = self.db.get_alert_db_id(alert_id_hex)
        self._ws_broadcast({
            "type": "alert_received",
            "alert": {
                "id":          db_id,
                "alert_id":    alert_id_hex,
                "sender_hash": sender_hash,
                "sender_name": sender_name,
                "severity":    severity,
                "title":       title,
                "message":     message,
                "timestamp":   ts,
                "direction":   "in",
                "is_read":     0,
            },
        })

    # ── Group Chat ────────────────────────────────────────────────────────────

    @staticmethod
    def _channel_id(name):
        """Deterministic group_id for open channels from channel name."""
        import hashlib as _hl
        return _hl.sha256(f"retimesh.channel.{name.lower().strip()}".encode()).hexdigest()[:32]

    def create_group(self, name, gtype, member_hashes):
        """Create a group/channel and send invites to all initial members."""
        import uuid as _uuid
        if gtype == "channel":
            group_id = self._channel_id(name)
        else:
            group_id = _uuid.uuid4().hex

        my_hash = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else "self"
        my_name = self.db.get_config("display_name", APP_NAME)

        # Include self in members
        all_members = list({my_hash} | set(member_hashes))
        self.db.save_group(group_id, name, gtype, is_owner=True, members=all_members)

        # Send invite to each member (except self) — async fan-out so path
        # resolution timeouts (up to 5 s each) run concurrently.
        others = [h for h in member_hashes if h != my_hash]
        if others:
            payload = json.dumps({
                "type":       "group_invite",
                "group_id":   group_id,
                "group_name": name,
                "group_type": gtype,
                "members":    all_members,
                "from_hash":  my_hash,
                "from_name":  my_name,
            })
            if _GEVENT_AVAILABLE:
                gs = [gevent.spawn(self.send_message, h, payload, _skip_db=True) for h in others]
                for g in gs:
                    try:
                        g.get(timeout=9)
                    except Exception:
                        g.kill(block=False)
            else:
                for h in others:
                    self.send_message(h, payload, _skip_db=True)

        log.info(f"Group created: {name!r} ({group_id[:12]}…) type={gtype} members={len(all_members)}")
        return {"status": "ok", "group_id": group_id}

    def send_group_message(self, group_id, content):
        """Fan-out an LXMF message to every known group member except self."""
        group = self.db.get_group(group_id)
        if not group:
            return {"error": "Group not found"}

        my_hash = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else "self"
        my_name = self.db.get_config("display_name", APP_NAME)
        import uuid as _uuid
        msg_id  = _uuid.uuid4().hex
        ts      = time.time()

        payload = json.dumps({
            "type":        "group_msg",
            "group_id":    group_id,
            "group_name":  group["name"],
            "group_type":  group["type"],
            "msg_id":      msg_id,
            "sender_hash": my_hash,
            "sender_name": my_name,
            "content":     content,
            "timestamp":   ts,
        })

        # Async fan-out: spawn one greenlet per member so path-resolution timeouts
        # (up to 5 s each) run concurrently rather than stacking up serially.
        # _skip_db=True: group message payloads are JSON control frames — the actual
        # user-visible message is already persisted in the groups_messages table above.
        other_members = [h for h in group["members"] if h != my_hash]
        sent_to = []
        failed_to = []

        if _GEVENT_AVAILABLE:
            greenlets = [(h, gevent.spawn(self.send_message, h, payload, _skip_db=True)) for h in other_members]
            for h, g in greenlets:
                try:
                    result = g.get(timeout=9)  # generous timeout — path warmup can take ~5 s
                    if result and "error" in result:
                        log.warning(f"Group fan-out failed for {h[:16]}: {result['error']}")
                        failed_to.append(h)
                    else:
                        sent_to.append(h)
                except gevent.Timeout:
                    log.warning(f"Group fan-out timeout for {h[:16]}")
                    g.kill(block=False)
                    failed_to.append(h)
                except Exception as exc:
                    log.error(f"Group fan-out exception for {h[:16]}: {exc}")
                    failed_to.append(h)
        else:
            # Fallback: synchronous serial sends (no gevent available)
            for h in other_members:
                try:
                    result = self.send_message(h, payload, _skip_db=True)
                    if result and "error" in result:
                        log.warning(f"Group fan-out failed for {h[:16]}: {result['error']}")
                        failed_to.append(h)
                    else:
                        sent_to.append(h)
                except Exception as exc:
                    log.error(f"Group fan-out exception for {h[:16]}: {exc}")
                    failed_to.append(h)

        # Broadcast a partial-failure event so the UI can warn the user
        if failed_to:
            self._ws_broadcast({
                "type": "group_partial_failure",
                "group_id": group_id,
                "msg_id": msg_id,
                "failed_count": len(failed_to),
                "sent_count": len(sent_to),
            })

        # Save to local DB as own message (is_read=1)
        self.db.save_group_message(group_id, msg_id, my_hash, my_name, content, ts)
        self.db.mark_group_message_self_read(msg_id)
        db_id = self.db.get_group_message_db_id(msg_id)
        if db_id is None:
            # This should not happen in practice (INSERT just succeeded above) but
            # guard against rare race on WAL checkpoint or disk-full scenarios.
            log.warning(f"get_group_message_db_id returned None for msg_id={msg_id!r} — DB write may have failed silently")
            db_id = -1  # sentinel; UI ignores -1 for DOM keying

        msg_obj = {
            "id": db_id, "group_id": group_id, "msg_id": msg_id,
            "sender_hash": my_hash, "sender_name": my_name,
            "content": content, "timestamp": ts, "is_read": 1, "is_self": True,
        }
        self._ws_broadcast({"type": "group_message", "message": msg_obj, "group_id": group_id})
        log.info(
            f"Group msg sent to {len(sent_to)}/{len(group['members'])-1} members "
            f"of {group_id[:12]}… ({len(failed_to)} failed)"
        )
        return {"status": "ok", "msg_id": msg_id, "sent_to": len(sent_to), "failed_to": len(failed_to)}

    def accept_group_invite(self, group_id):
        """Accept a pending group invite — join the group and notify members."""
        invite = None
        for inv in self.db.get_group_invites():
            if inv["group_id"] == group_id:
                invite = inv
                break
        if not invite:
            return {"error": "Invite not found"}

        my_hash = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else "self"
        my_name = self.db.get_config("display_name", APP_NAME)

        # Add to local groups
        members = invite["members"]
        if my_hash not in members:
            members.append(my_hash)

        self.db.save_group(
            invite["group_id"], invite["group_name"], invite["group_type"],
            is_owner=False, members=members
        )
        self.db.update_invite_status(group_id, "accepted")

        # Notify all other members that we joined — async fan-out
        join_payload = json.dumps({
            "type":        "group_join",
            "group_id":    group_id,
            "group_name":  invite["group_name"],
            "group_type":  invite["group_type"],
            "sender_hash": my_hash,
            "sender_name": my_name,
            "members":     members,
        })
        others = [h for h in members if h != my_hash]
        if others:
            if _GEVENT_AVAILABLE:
                gs = [gevent.spawn(self.send_message, h, join_payload, _skip_db=True) for h in others]
                for g in gs:
                    try:
                        g.get(timeout=9)
                    except Exception:
                        g.kill(block=False)
            else:
                for h in others:
                    self.send_message(h, join_payload, _skip_db=True)

        # Notify own UI so the group appears immediately without a page refresh
        group_obj = self.db.get_group(group_id) or {
            "id":       group_id,
            "name":     invite["group_name"],
            "type":     invite["group_type"],
            "is_owner": False,
            "members":  members,
        }
        self._ws_broadcast({"type": "group_joined", "group": group_obj})

        return {"status": "ok", "group_id": group_id}

    def decline_group_invite(self, group_id):
        """Decline a pending invite."""
        self.db.update_invite_status(group_id, "declined")
        return {"status": "ok"}

    def leave_group(self, group_id):
        """Leave a group and notify other members."""
        group = self.db.get_group(group_id)
        if not group:
            return {"error": "Group not found"}

        my_hash = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else "self"
        my_name = self.db.get_config("display_name", APP_NAME)

        leave_payload = json.dumps({
            "type":        "group_leave",
            "group_id":    group_id,
            "group_name":  group["name"],
            "sender_hash": my_hash,
            "sender_name": my_name,
        })
        others = [h for h in group["members"] if h != my_hash]
        if others:
            if _GEVENT_AVAILABLE:
                gs = [gevent.spawn(self.send_message, h, leave_payload, _skip_db=True) for h in others]
                for g in gs:
                    try:
                        g.get(timeout=9)
                    except Exception:
                        g.kill(block=False)
            else:
                for h in others:
                    self.send_message(h, leave_payload, _skip_db=True)

        self.db.delete_group(group_id)
        # Notify own UI so the group disappears immediately without a page refresh
        self._ws_broadcast({"type": "group_left", "group_id": group_id})
        return {"status": "ok"}

    def join_channel(self, channel_name):
        """Join (or create) an open channel by name."""
        group_id = self._channel_id(channel_name)
        existing = self.db.get_group(group_id)
        if existing:
            return {"status": "already_member", "group_id": group_id}

        my_hash = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else "self"
        self.db.save_group(group_id, channel_name, "channel", is_owner=False, members=[my_hash])

        # Push new group/channel to UI immediately
        group_obj = self.db.get_group(group_id) or {
            "id":       group_id,
            "name":     channel_name,
            "type":     "channel",
            "is_owner": False,
            "members":  [my_hash],
        }
        self._ws_broadcast({"type": "group_joined", "group": group_obj})

        return {"status": "ok", "group_id": group_id}

    def rename_group(self, group_id, new_name):
        """Rename a group locally and fan-out a group_rename notification to all members."""
        group = self.db.get_group(group_id)
        if not group:
            return {"error": "Group not found"}
        if not group.get("is_owner"):
            return {"error": "Only the group owner can rename this group"}

        new_name = new_name.strip()
        if not new_name:
            return {"error": "Name cannot be empty"}
        if len(new_name) > 64:
            return {"error": "Name too long (max 64 chars)"}

        my_hash = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else "self"
        my_name = self.db.get_config("display_name", APP_NAME)

        # Persist locally first
        self.db.rename_group(group_id, new_name)

        # Notify all other members
        payload = json.dumps({
            "type":       "group_rename",
            "group_id":   group_id,
            "new_name":   new_name,
            "sender_hash": my_hash,
            "sender_name": my_name,
        })
        other_members = [h for h in group["members"] if h != my_hash]
        if other_members:
            if _GEVENT_AVAILABLE:
                greenlets = [gevent.spawn(self.send_message, h, payload, _skip_db=True) for h in other_members]
                for g in greenlets:
                    try:
                        g.get(timeout=9)
                    except Exception:
                        g.kill(block=False)
            else:
                for h in other_members:
                    self.send_message(h, payload, _skip_db=True)

        # Broadcast to own UI
        self._ws_broadcast({
            "type":     "group_renamed",
            "group_id": group_id,
            "new_name": new_name,
            "by_hash":  my_hash,
        })
        return {"status": "ok", "new_name": new_name}

    def add_group_member(self, group_id, member_hash):
        """Add a new member and send them an invite."""
        group = self.db.get_group(group_id)
        if not group:
            return {"error": "Group not found"}

        my_hash = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else "self"
        my_name = self.db.get_config("display_name", APP_NAME)

        if member_hash in group["members"]:
            return {"status": "already_member"}

        new_members = group["members"] + [member_hash]
        self.db.update_group_members(group_id, new_members)

        payload = json.dumps({
            "type":       "group_invite",
            "group_id":   group_id,
            "group_name": group["name"],
            "group_type": group["type"],
            "members":    new_members,
            "from_hash":  my_hash,
            "from_name":  my_name,
        })
        self.send_message(member_hash, payload, _skip_db=True)
        return {"status": "ok"}

    def kick_group_member(self, group_id, member_hash):
        """Remove a member from the group and notify all parties.

        1. Remove member from local group record.
        2. Send a ``group_leave`` notification to the kicked peer so their
           client removes itself from the group.
        3. Fan-out a ``group_member_left`` notice to all remaining members so
           their UIs update the member list without a refresh.
        """
        group = self.db.get_group(group_id)
        if not group:
            return {"error": "Group not found"}
        if not group.get("is_owner"):
            return {"error": "Only the group owner can remove members"}
        if member_hash not in group["members"]:
            return {"error": "Member not found in group"}

        my_hash = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else "self"
        my_name = self.db.get_config("display_name", APP_NAME)

        # Update local member list
        updated_members = [h for h in group["members"] if h != member_hash]
        self.db.update_group_members(group_id, updated_members)

        # Notify the kicked peer — they receive a group_leave so their client
        # removes the group from their list
        kicked_payload = json.dumps({
            "type":        "group_leave",
            "group_id":    group_id,
            "group_name":  group["name"],
            "sender_hash": member_hash,
            "sender_name": "",
            "kicked_by":   my_hash,
        })
        self.send_message(member_hash, kicked_payload, _skip_db=True)

        # Notify remaining members (excluding self and the kicked peer)
        others = [h for h in updated_members if h != my_hash]
        if others:
            notice_payload = json.dumps({
                "type":        "group_leave",
                "group_id":    group_id,
                "group_name":  group["name"],
                "sender_hash": member_hash,
                "sender_name": "",
                "kicked_by":   my_hash,
            })
            if _GEVENT_AVAILABLE:
                gs = [gevent.spawn(self.send_message, h, notice_payload, _skip_db=True) for h in others]
                for g in gs:
                    try:
                        g.get(timeout=9)
                    except Exception:
                        g.kill(block=False)
            else:
                for h in others:
                    self.send_message(h, notice_payload, _skip_db=True)

        # Broadcast to own UI
        self._ws_broadcast({
            "type":        "group_member_left",
            "group_id":    group_id,
            "member_hash": member_hash,
            "kicked_by":   my_hash,
        })
        return {"status": "ok", "removed": member_hash}

    def _handle_group_message(self, sender_hex, payload, lxmf_msg_hash):
        """Route an incoming group-related JSON payload."""
        msg_type   = payload.get("type", "")
        group_id   = payload.get("group_id", "")
        group_name = payload.get("group_name", "Unknown Group")
        group_type = payload.get("group_type", "private")

        peer_info   = self.peers.get(sender_hex, {})
        sender_name = peer_info.get("display_name") or payload.get("sender_name") or sender_hex[:16]

        if msg_type == "group_msg":
            content    = payload.get("content", "")
            msg_id     = payload.get("msg_id") or lxmf_msg_hash or ""
            ts         = float(payload.get("timestamp", time.time()))

            # --- Fix F: cap untrusted fields to prevent oversized DB writes ---
            _MAX_CONTENT = 10240   # 10 KiB hard cap on group message body
            _MAX_MSG_ID  = 64      # msg_id is a hex hash — 64 chars is plenty
            if len(content) > _MAX_CONTENT:
                log.warning(f"Group msg content truncated from {len(content)} to {_MAX_CONTENT} bytes")
                content = content[:_MAX_CONTENT]
            if len(msg_id) > _MAX_MSG_ID:
                log.warning(f"Group msg_id truncated from {len(msg_id)} to {_MAX_MSG_ID} chars")
                msg_id = msg_id[:_MAX_MSG_ID]

            # Auto-create group entry if we haven't seen it yet
            if not self.db.get_group(group_id):
                members = payload.get("members", [sender_hex])
                my_hash = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else ""
                if my_hash and my_hash not in members:
                    members.append(my_hash)
                self.db.save_group(group_id, group_name, group_type, is_owner=False, members=members)

            inserted = self.db.save_group_message(group_id, msg_id, sender_hex, sender_name, content, ts)
            if not inserted:
                return  # duplicate

            db_id = self.db.get_group_message_db_id(msg_id)

            msg_obj = {
                "id": db_id, "group_id": group_id, "msg_id": msg_id,
                "sender_hash": sender_hex, "sender_name": sender_name,
                "content": content, "timestamp": ts, "is_read": 0, "is_self": False,
            }
            self._ws_broadcast({"type": "group_message", "message": msg_obj, "group_id": group_id})
            log.info(f"Group msg received in {group_id[:12]}… from {sender_hex[:12]}…")

        elif msg_type == "group_invite":
            members    = payload.get("members", [])
            from_hash  = payload.get("from_hash", sender_hex)
            from_name  = payload.get("from_name", sender_name)
            # Don't re-invite if already a member
            if self.db.get_group(group_id):
                return
            # Fix I: check if a pending invite already exists for this group_id
            # so that mesh re-broadcasts of the same invite don't fire duplicate
            # SSE events in the UI (the DB upsert is idempotent; the broadcast is not).
            _existing_invite = any(
                inv["group_id"] == group_id
                for inv in self.db.get_group_invites()
            )
            self.db.save_group_invite(group_id, group_name, group_type, from_hash, from_name, members)
            if _existing_invite:
                log.debug(f"Duplicate group invite suppressed for {group_id[:12]}…")
                return  # already notified the UI for this invite
            self._ws_broadcast({
                "type":       "group_invite",
                "group_id":   group_id,
                "group_name": group_name,
                "group_type": group_type,
                "from_hash":  from_hash,
                "from_name":  from_name,
                "members":    members,
            })
            log.info(f"Group invite received: {group_name!r} from {sender_hex[:12]}…")

        elif msg_type == "group_join":
            # Another peer accepted the invite — update local member list
            joined_hash = payload.get("sender_hash", sender_hex)
            joined_name = payload.get("sender_name", sender_name)
            new_members = payload.get("members", [])
            group = self.db.get_group(group_id)
            if group:
                merged = list(set(group["members"]) | set(new_members) | {joined_hash})
                self.db.update_group_members(group_id, merged)
            self._ws_broadcast({
                "type":        "group_member_joined",
                "group_id":    group_id,
                "member_hash": joined_hash,
                "member_name": joined_name,
            })

        elif msg_type == "group_leave":
            left_hash = payload.get("sender_hash", sender_hex)
            my_hash   = RNS.hexrep(self.lxmf_dest.hash, delimit=False) if self.lxmf_dest else ""

            if left_hash == my_hash:
                # We received a group_leave where WE are the departing member —
                # this means we were kicked by the group owner.  Delete the group
                # from our local DB and notify the UI to remove it.
                self.db.delete_group(group_id)
                self._ws_broadcast({"type": "group_left", "group_id": group_id})
                log.info(f"Removed from group {group_id[:12]}… (kicked)")
                return

            group = self.db.get_group(group_id)
            if group:
                updated = [h for h in group["members"] if h != left_hash]
                self.db.update_group_members(group_id, updated)
            self._ws_broadcast({
                "type":        "group_member_left",
                "group_id":    group_id,
                "member_hash": left_hash,
            })

        elif msg_type == "group_rename":
            # Only the group owner can rename; accept if we're a member
            new_name = payload.get("new_name", "").strip()
            if not new_name:
                return
            group = self.db.get_group(group_id)
            if not group:
                return  # we don't know this group
            self.db.rename_group(group_id, new_name)
            self._ws_broadcast({
                "type":     "group_renamed",
                "group_id": group_id,
                "new_name": new_name,
                "by_hash":  sender_hex,
            })
            log.info(f"Group {group_id[:12]}… renamed to {new_name!r} by {sender_hex[:12]}…")

    # ── Presence ──────────────────────────────────────────────────────────────

    def send_announce(self, display_name=None):
        """Broadcast our presence on the network.

        R-3: display_name propagation semantics
        ----------------------------------------
        When ``display_name`` is provided it is written to
        ``lxmf_dest.display_name`` *before* calling
        ``lxmf_router.announce()``.  The LXMF router serialises this field
        as the ``app_data`` bytes of the underlying RNS Destination announce,
        so remote peers will decode it in their ``_on_announce`` handler and
        store it as the human-readable name for this node.  If ``display_name``
        is omitted the previously-set name (or the empty string) is re-used,
        which means a plain periodic announce does not reset the visible name.
        """
        try:
            if display_name:
                self.lxmf_dest.display_name = display_name
            self.lxmf_router.announce(self.lxmf_dest.hash)
            # Also announce the call destination so remote nodes know the path.
            # Without this, RNS.Transport.hops_to(call_dest_hash) returns
            # PATHFINDER_M (128) and the link establishment timeout becomes
            # 774 s instead of the expected ~12 s, effectively blocking calls.
            try:
                self.call_dest.announce()
            except Exception as _ann_err:
                log.debug(f"call_dest announce failed (non-fatal): {_ann_err}")
            log.info(f"Announce sent")
        except Exception as e:
            log.error(f"Announce FAILED: {e}")
            import traceback
            traceback.print_exc()

    def _on_announce(self, destination_hash, announced_identity, app_data):
        """Handle incoming announces from other nodes."""
        dest_hex = RNS.hexrep(destination_hash, delimit=False)

        # Check if this peer is blocked
        if self.db.is_blocked(dest_hex):
            log.info(f"Blocked peer announce ignored: {dest_hex[:16]}...")
            return

        display_name = ""
        if app_data:
            try:
                display_name = app_data.decode("utf-8")
            except Exception:
                display_name = dest_hex[:12]

        self.peers[dest_hex] = {
            "dest_hash": dest_hex,
            "display_name": display_name,
            "last_announce": time.time(),
            "identity": announced_identity,
        }
        # R-4: maintain reverse map so _on_incoming_link can identify the caller in O(1)
        if announced_identity and hasattr(announced_identity, "hexhash"):
            self._identity_to_dest[announced_identity.hexhash] = dest_hex
        self.db.upsert_peer(dest_hex, display_name, identity_hash=self.active_identity_hash)
        log.info(f"Peer announced: {display_name} [{dest_hex[:16]}...]")

        # Notify all WebSocket clients
        self._ws_broadcast({
            "type": "peer_update",
            "peers": self.db.get_peers(identity_hash=self.active_identity_hash),
        })

    # ── LXMF Messaging ──

    def send_message(self, dest_hex, content, content_type="text", method=None, _skip_db=False):
        """
        Send an LXMF message to a peer.
        Requests path if needed (like NomadNet does).
        Tries DIRECT first. If a propagation node is configured, falls back to PROPAGATED.

        _skip_db=True: do NOT persist the outbound message to the messages table.
        Use this for internal group control messages (invites, joins, leaves, renames)
        so they do not appear as raw JSON in the user's DM chat history.
        """
        if self.db.is_blocked(dest_hex):
            return {"error": "This peer is blocked. Unblock them to send messages."}
        try:
            dest_identity = RNS.Identity.recall(bytes.fromhex(dest_hex))
            if not dest_identity:
                if dest_hex in self.peers and "identity" in self.peers[dest_hex]:
                    dest_identity = self.peers[dest_hex]["identity"]
                else:
                    return {"error": "Unknown peer identity. Wait for their announce."}

            dest = RNS.Destination(
                dest_identity,
                RNS.Destination.OUT,
                RNS.Destination.SINGLE,
                "lxmf", "delivery"
            )

            # Ensure we have a path to the destination (NomadNet pattern).
            # IMPORTANT: use _sleep() (cooperative) — time.sleep() here would
            # block the entire gevent event loop, causing cross-OS timing issues
            # and UI freezes during the path-resolution wait.
            dest_hash_bytes = dest.hash
            if not RNS.Transport.has_path(dest_hash_bytes):
                log.info(f"No path to {dest_hex[:16]}, requesting path...")
                RNS.Transport.request_path(dest_hash_bytes)
                wait_start = time.time()
                while not RNS.Transport.has_path(dest_hash_bytes) and time.time() - wait_start < 5:
                    _sleep(0.05)   # cooperative yield — was time.sleep(0.1)
                if RNS.Transport.has_path(dest_hash_bytes):
                    log.info(f"Path to {dest_hex[:16]} resolved")
                else:
                    log.warning(f"Path to {dest_hex[:16]} not found, attempting send anyway")

            # Determine delivery method.
            #
            # Behaviour matrix (this is what the docstring promises):
            #
            #   method="propagated"   ->  always PROPAGATED (user explicit)
            #   method="direct"       ->  always DIRECT (user explicit)
            #   method="auto"/None    ->  pick based on whether we have a
            #                              path to the destination AND what
            #                              the user has configured.
            #
            # In auto mode, if there is NO path to the destination AND the
            # user has set an outbound propagation node, send PROPAGATED so
            # the message gets stored at the propagation node for the
            # offline peer.  Otherwise default to DIRECT.
            outbound_prop_set = False
            outbound_prop_hex = ""
            try:
                _opn = self.lxmf_router.get_outbound_propagation_node()
                if _opn is not None:
                    outbound_prop_set = True
                    outbound_prop_hex = RNS.hexrep(_opn, delimit=False)
            except Exception:
                pass

            has_path = RNS.Transport.has_path(dest_hash_bytes)
            log.debug(
                f"send_message decision: peer={dest_hex[:16]}.. "
                f"method={method!r} has_path={has_path} "
                f"outbound_prop={outbound_prop_hex[:16] + '..' if outbound_prop_hex else 'none'}"
            )

            if method == "propagated":
                desired_method = LXMF.LXMessage.PROPAGATED
                log.info(f"Forcing PROPAGATED to {dest_hex[:16]} (caller requested)")
            elif method == "direct":
                desired_method = LXMF.LXMessage.DIRECT
            else:
                # auto: prefer PROPAGATED when peer is unreachable AND a
                # propagation node is configured; otherwise DIRECT.
                if outbound_prop_set and not has_path:
                    desired_method = LXMF.LXMessage.PROPAGATED
                    log.info(f"No path to {dest_hex[:16]}, sending via propagation node {outbound_prop_hex[:16]}..")
                else:
                    desired_method = LXMF.LXMessage.DIRECT
                    if not outbound_prop_set:
                        log.debug("auto-mode: no outbound propagation node set, using DIRECT")

            lxmf_msg = LXMF.LXMessage(
                dest,
                self.lxmf_dest,
                content.encode("utf-8"),
                title="".encode("utf-8"),
                desired_method=desired_method,
            )

            lxmf_msg.register_delivery_callback(self._on_delivery_receipt)
            lxmf_msg.register_failed_callback(self._on_delivery_failed)
            self.lxmf_router.handle_outbound(lxmf_msg)

            # R-7: lxmf_msg.hash is computed from message content so it is normally
            # available as soon as the LXMessage object is constructed, *before*
            # handle_outbound returns.  However the LXMF internals are allowed to
            # set it lazily in some code paths (e.g. propagated queue), so we must
            # guard against None.  We allow up to 100 ms of cooperative spin before
            # giving up and saving without a hash.
            if not lxmf_msg.hash:
                deadline = time.time() + 0.1
                while not lxmf_msg.hash and time.time() < deadline:
                    _sleep(0.01)
            msg_hash = RNS.hexrep(lxmf_msg.hash, delimit=False) if lxmf_msg.hash else None
            if not _skip_db:
                self.db.save_message(dest_hex, "out", content, content_type, msg_hash, identity_hash=self.active_identity_hash)

            method_name = "PROPAGATED" if desired_method == LXMF.LXMessage.PROPAGATED else "DIRECT"
            log.info(f"Message sent ({method_name}) to {dest_hex[:16]}...")
            return {"status": "sent", "hash": msg_hash, "method": method_name}

        except Exception as e:
            log.error(f"Failed to send message: {e}")
            return {"error": str(e)}

    def set_propagation_node(self, node_hash_hex):
        """Set the outbound LXMF propagation node for store-and-forward."""
        try:
            node_hash = bytes.fromhex(node_hash_hex)
            self.lxmf_router.set_outbound_propagation_node(node_hash)
            self.db.set_config("propagation_node", node_hash_hex)
            log.info(f"Propagation node set to {node_hash_hex[:16]}...")
            # Kick off the periodic sync loop if it isn't already running.
            # No-op if it's already going (idempotent).  Without this,
            # users who set the hash from the UI after startup wouldn't
            # get auto-sync until they restarted the app.
            try:
                self._start_propagation_sync_loop()
            except Exception as exc:
                log.debug(f"Could not start propagation sync loop: {exc}")
            return {"status": "ok"}
        except Exception as e:
            log.error(f"Failed to set propagation node: {e}")
            return {"error": str(e)}

    def _start_propagation_sync_loop(self):
        """Start the periodic auto-sync greenlet if it isn't already running.

        First sync runs ~10s after this is called (gives paths time to
        establish on a freshly-started app), then every 5 minutes thereafter.
        Idempotent: calling this when the loop is already running is a no-op.

        Called from two places:
          - __init__, after loading propagation_node from config at startup
          - api_set_propagation, when the user pastes a hash and clicks Save

        The latter is important because previously the loop only started at
        boot, so changing the outbound hash from the UI didn't activate
        auto-sync until the next app restart.
        """
        if getattr(self, "_propagation_sync_running", False):
            log.debug("Propagation sync loop already running")
            return
        self._propagation_sync_running = True

        def _periodic():
            try:
                _sleep(10)  # initial settle delay
                while True:
                    try:
                        if self.lxmf_router.get_outbound_propagation_node() is not None:
                            self.lxmf_router.request_messages_from_propagation_node(self.identity)
                            log.debug("Auto-sync from propagation node requested")
                    except Exception as exc:
                        log.debug(f"Auto-sync attempt failed (will retry): {exc}")
                    _sleep(290)  # then every 5 min
            except Exception as exc:
                log.warning(f"Propagation sync loop crashed: {exc}")
                self._propagation_sync_running = False

        if _GEVENT_AVAILABLE:
            # Use the module-global `gevent` import; do NOT shadow with a
            # local import here (that's what caused the previous
            # UnboundLocalError when this lived inline in __init__).
            gevent.spawn(_periodic)
        else:
            threading.Thread(target=_periodic, daemon=True).start()

    def sync_from_propagation_node(self):
        """Request messages from the configured propagation node."""
        try:
            self.lxmf_router.request_messages_from_propagation_node(self.identity)
            log.info("Requested message sync from propagation node")
            return {"status": "syncing"}
        except Exception as e:
            log.error(f"Failed to sync from propagation node: {e}")
            return {"error": str(e)}

    # ── Act-as-propagation-node (store & forward for the whole mesh) ──
    #
    # When enabled, this node announces a propagation destination
    # (`lxmf.propagation`) and accepts inbound LXMF store-and-forward
    # traffic from the mesh.  Other nodes that configure us as their
    # outbound propagation target will deposit messages here for offline
    # peers; the LXMRouter handles autopeering and distributed sync
    # with other propagation nodes.

    def is_propagation_node_enabled(self):
        """Return True if we are currently acting as a propagation node."""
        try:
            return bool(getattr(self.lxmf_router, "propagation_node", False))
        except Exception:
            return False

    def enable_propagation_node(self):
        """Start acting as an LXMF propagation node (store & forward)."""
        if self.is_propagation_node_enabled():
            return {"status": "ok", "already_enabled": True,
                    "propagation_hash": self._propagation_node_hex()}
        try:
            # LXMRouter exposes this method in 0.4+.  It creates the
            # propagation destination, starts the inbound link listener,
            # and schedules a delayed announce on lxmf.propagation.
            if hasattr(self.lxmf_router, "enable_propagation"):
                self.lxmf_router.enable_propagation()
            else:
                return {"error": "This LXMF version does not support acting as a propagation node"}
            self.db.set_config("act_as_propagation_node", "true")
            hex_ = self._propagation_node_hex()
            log.info(f"Acting as propagation node — hash: {hex_}")
            return {"status": "ok", "propagation_hash": hex_}
        except Exception as e:
            log.error(f"enable_propagation_node failed: {e}")
            return {"error": str(e)}

    def disable_propagation_node(self):
        """Stop acting as a propagation node."""
        try:
            if hasattr(self.lxmf_router, "disable_propagation"):
                self.lxmf_router.disable_propagation()
            self.db.set_config("act_as_propagation_node", "false")
            log.info("Stopped acting as propagation node")
            return {"status": "ok"}
        except Exception as e:
            log.error(f"disable_propagation_node failed: {e}")
            return {"error": str(e)}

    def _propagation_node_hex(self):
        """Return the hex dest hash of our propagation destination, or empty."""
        try:
            pd = getattr(self.lxmf_router, "propagation_destination", None)
            if pd and hasattr(pd, "hash"):
                return RNS.hexrep(pd.hash, delimit=False)
        except Exception:
            pass
        return ""

    def _on_lxmf_delivery(self, message):
        """Handle incoming LXMF message."""
        sender_hex = RNS.hexrep(message.source_hash, delimit=False)

        # Check if this sender is blocked
        if self.db.is_blocked(sender_hex):
            log.info(f"Blocked peer message dropped: {sender_hex[:16]}...")
            return

        # Auto-add sender to peers if not already known (like NomadNet)
        if sender_hex not in self.peers:
            sender_identity = RNS.Identity.recall(message.source_hash)
            display_name = sender_hex[:12]
            self.peers[sender_hex] = {
                "dest_hash": sender_hex,
                "display_name": display_name,
                "last_announce": time.time(),
                "identity": sender_identity,
            }
            # R-4: update reverse identity→dest map so incoming calls from this
            # peer can be identified in O(1) even without a prior announce.
            if sender_identity and hasattr(sender_identity, "hexhash"):
                self._identity_to_dest[sender_identity.hexhash] = sender_hex
            self.db.upsert_peer(sender_hex, display_name, identity_hash=self.active_identity_hash)
            log.info(f"Auto-added peer from incoming message: {sender_hex[:16]}...")
            # Notify frontend of new peer
            self._ws_broadcast({
                "type": "peer_update",
                "peers": self.db.get_peers(identity_hash=self.active_identity_hash),
            })

        # Update last_seen for this peer
        self.db.upsert_peer(sender_hex, identity_hash=self.active_identity_hash)

        # Fix G: wrap decode in try/except — a malformed (non-UTF-8) message
        # must not silently swallow the entire delivery callback and leave the
        # sender's message unacknowledged with no trace in the logs.
        try:
            content = message.content.decode("utf-8", errors="replace")
        except Exception as _decode_err:
            log.warning(f"LXMF message content decode failed from {sender_hex[:16]}…: {_decode_err}")
            content = ""
        msg_hash = RNS.hexrep(message.hash, delimit=False) if message.hash else None

        # Try to parse as JSON for structured messages (file, call signal)
        content_type = "text"
        try:
            payload = json.loads(content)
            msg_type = payload.get("type", "")

            # ── Group messages (intercept before regular chat) ──
            # group_rename MUST be included here: it is a system notification that
            # must be routed to _handle_group_message, never saved as a DM.
            if msg_type in (
                "group_msg", "group_invite", "group_join",
                "group_leave", "group_rename",
            ):
                self._handle_group_message(sender_hex, payload, msg_hash)
                return  # Do NOT save as a regular DM

            if msg_type == "file":
                content_type = "file"
                # Decode and validate the file payload before touching the filesystem.
                # R-FILE-1: guard against malformed base64 that would crash the delivery cb
                try:
                    file_data = base64.b64decode(payload["data"])
                except Exception as _b64_err:
                    log.warning(
                        f"Received file with invalid base64 from {sender_hex[:16]}…: {_b64_err}"
                    )
                    return
                # File-size limit removed — accept files of any length.  The
                # underlying RNS link still imposes practical ceilings, but
                # we no longer reject incoming payloads on size alone.
                filename = payload.get("filename", "file")
                # Sanitize filename
                safe_name = "".join(c for c in filename if c.isalnum() or c in ".-_ ")
                if not safe_name:
                    safe_name = "unnamed_file"
                filepath = self.files_dir / safe_name
                try:
                    filepath.write_bytes(file_data)
                except Exception as _write_err:
                    log.error(f"Failed to write received file {safe_name!r}: {_write_err}")
                    return
                self.db.save_file_transfer(
                    sender_hex, "in", safe_name, len(file_data),
                    hashlib.sha256(file_data).hexdigest(), str(filepath),
                    identity_hash=self.active_identity_hash
                )
                content = json.dumps({
                    "type": "file",
                    "filename": safe_name,
                    "filesize": len(file_data),
                    "url": f"/files/{safe_name}",
                })

            elif msg_type == "call_request":
                content_type = "call_signal"
                codec = payload.get("codec", "mu_law")
                caller = payload.get("caller", sender_hex[:16])
                log.info(f"Incoming call from {caller} using {codec}")

                # Store codec so _on_incoming_link can use the negotiated codec
                # regardless of whether the LXMF message or the RNS Link arrives first.
                self._pending_call_codecs[sender_hex] = codec

                # Update active_calls if the RNS Link arrived before the LXMF message
                with self._lock:
                    if sender_hex in self.active_calls:
                        self.active_calls[sender_hex]["codec"] = codec

                # Notify UI about incoming call
                self._ws_broadcast({
                    "type": "call_incoming",
                    "peer_hash": sender_hex,
                    "codec": codec,
                    "caller": caller,
                })
                # Don't save call signals as chat messages
                return

        except (json.JSONDecodeError, KeyError):
            pass

        # ── Propagation ACK handling (option C: end-to-end receipts) ──
        # If this is an incoming propagation_ack, it means we previously
        # sent a propagated message and the recipient has now actually
        # received it.  Broadcast a DELIVERED state for the original hash
        # and return — these aren't user-visible chat messages.
        try:
            payload2 = json.loads(content)
            if payload2.get("type") == "propagation_ack":
                orig = payload2.get("original_hash")
                if orig:
                    try: self.db.mark_delivered(orig)
                    except Exception: pass
                    self._ws_broadcast({
                        "type": "message_state",
                        "hash": orig,
                        "state": 2,         # DELIVERED (truly end-to-end now)
                        "via_propagation": True,
                        "end_to_end": True, # distinguishes from "stored at host"
                    })
                    log.info(
                        f"Propagation ACK from {sender_hex[:16]}: "
                        f"recipient confirmed {orig[:16]}"
                    )
                return  # do NOT save the ack as a chat message
        except (json.JSONDecodeError, ValueError, TypeError):
            pass

        # Detect a receipt-request marker in incoming custom fields.  If
        # present, we'll send back an ack AFTER saving the message below.
        # This only happens for propagated messages that were retried with
        # the receipt-request — DIRECT messages get their own delivery
        # callback and don't need this.
        receipt_request_orig_hash = None
        try:
            fields = message.fields if hasattr(message, "fields") else None
            if fields and LXMF.FIELD_CUSTOM_DATA in fields:
                marker = json.loads(fields[LXMF.FIELD_CUSTOM_DATA].decode("utf-8"))
                if marker.get("retimesh_receipt_request") and marker.get("original_hash"):
                    receipt_request_orig_hash = marker["original_hash"]
        except Exception:
            pass

        try:
            self.db.save_message(sender_hex, "in", content, content_type, msg_hash, identity_hash=self.active_identity_hash)
            log.info(f"Message received from {sender_hex[:16]}...")

            self._ws_broadcast({
                "type": "chat_recv",
                "peer_hash": sender_hex,
                "content": content,
                "content_type": content_type,
                "timestamp": time.time(),
            })
        except Exception as e:
            log.error(f"save/broadcast failed for {sender_hex[:16]}: {e}")
            import traceback; traceback.print_exc()

        # Send an ACK back if this message carried a receipt-request.  We
        # do this AFTER the save so a failure here doesn't lose the actual
        # message.  The ack is fire-and-forget; if it can't be delivered
        # immediately (sender offline) it'll be propagated via our own
        # outbound propagation node if one is configured — same retry path
        # as any other message.
        if receipt_request_orig_hash:
            self._send_propagation_ack(sender_hex, receipt_request_orig_hash)

    def _send_propagation_ack(self, dest_hex, original_hash):
        """Send a tiny propagation_ack message back to the original sender.

        Confirms that we (the recipient) have actually received and stored
        a message that was originally propagated.  Lets the sender's UI
        upgrade ✓✓ stored-at-host to ✓✓ truly-delivered.

        Fire-and-forget: any exception is logged but doesn't propagate.
        Send via DIRECT (the ack is small, the original sender is now in
        our peers table from the inbound message we just received, so a
        DIRECT path likely exists).  If DIRECT fails, our own
        _on_delivery_failed will attempt PROPAGATED retry — same as for
        regular messages.
        """
        try:
            ack_payload = json.dumps({
                "type": "propagation_ack",
                "original_hash": original_hash,
            })
            # Send through the existing send_message machinery so we get
            # the same retry semantics for free.  Skip DB save — acks
            # aren't user-visible messages.
            self.send_message(
                dest_hex, ack_payload,
                content_type="text",  # delivered as plain text; receiver parses JSON
                method="auto",
                _skip_db=True,
            )
            log.debug(f"Propagation ACK queued for {dest_hex[:16]} (orig {original_hash[:16]})")
        except Exception as exc:
            log.warning(f"Failed to send propagation ack to {dest_hex[:16]}: {exc}")

    def _on_delivery_receipt(self, message):
        """Message delivered to recipient — double tick.

        If this message is a propagation-retry of an earlier DIRECT send
        that failed, we have to broadcast DELIVERED against the ORIGINAL
        message hash, not the retry's own hash.  The UI bubble is keyed
        on the original hash, so without this bridge the second tick
        never appears even though the recipient has the message.
        """
        msg_hash = RNS.hexrep(message.hash, delimit=False) if message.hash else None
        original_hash = getattr(message, "_retimesh_original_hash", None)
        ui_hash = original_hash or msg_hash

        if msg_hash:
            self.db.mark_delivered(msg_hash)
        if original_hash and original_hash != msg_hash:
            # Also mark the original DB row delivered so a page refresh
            # shows ✓✓ instead of ⏳.
            try:
                self.db.mark_delivered(original_hash)
            except Exception:
                pass
            log.info(
                f"Message delivered (via propagation): "
                f"{original_hash[:16]} (retry hash {msg_hash[:16]})"
            )
        else:
            log.info(f"Message delivered: {msg_hash[:16] if msg_hash else 'unknown'}...")
        self._ws_broadcast({
            "type": "message_state",
            "hash": ui_hash,
            "state": 2,  # delivered
        })

    def _on_delivery_failed(self, message):
        """Direct delivery failed — retry via the configured propagation node.

        LXMF does NOT automatically retry a failed DIRECT message via
        propagation; on failure it just moves the message to failed_outbound
        and calls this callback.  We have to manually construct a fresh
        LXMessage with desired_method=PROPAGATED and hand it to the router
        ourselves.

        If no propagation node is configured the message is permanently
        failed and we tell the UI so.
        """
        msg_hash = RNS.hexrep(message.hash, delimit=False) if message.hash else None
        dest_hex = RNS.hexrep(message.get_destination().hash, delimit=False) if message.get_destination() else None
        log.warning(f"Direct delivery failed for {msg_hash[:16] if msg_hash else 'unknown'}")

        outbound_prop = None
        try:
            outbound_prop = self.lxmf_router.get_outbound_propagation_node()
        except Exception:
            pass

        # Avoid infinite loop: if we already tried propagation, give up.
        already_propagated = getattr(message, "_retimesh_already_propagated", False)

        if outbound_prop is not None and not already_propagated and dest_hex:
            log.info(f"Retrying via propagation node {RNS.hexrep(outbound_prop, delimit=False)[:16]}..")
            try:
                # Recover original recipient destination + content
                dest = message.get_destination()
                # message.content is encrypted; we need the original cleartext.
                # LXMF preserves it on the outgoing message as `content` (bytes).
                # If it's not accessible, fall back to using the failed message
                # directly with desired_method changed.
                content_bytes = getattr(message, "content", None)
                if content_bytes is None:
                    content_bytes = message.packed if hasattr(message, "packed") else b""
                title_bytes = getattr(message, "title", b"") or b""

                # Don't add a receipt-request marker if this message is
                # ITSELF a propagation_ack — that would cause an ack loop
                # (recipient would ack the ack, sender would ack that, ...).
                # Acks are fire-and-forget in both directions.
                is_ack = False
                try:
                    if isinstance(content_bytes, bytes):
                        _peek = content_bytes[:64].decode("utf-8", errors="ignore")
                        if '"type": "propagation_ack"' in _peek or '"type":"propagation_ack"' in _peek:
                            is_ack = True
                except Exception:
                    pass

                retry_fields = None
                if not is_ack:
                    # Embed a custom marker on the propagated message so the
                    # receiver can send back an explicit acknowledgement when
                    # they actually pull it from the propagation node.  LXMF's
                    # built-in delivery callback only fires for DIRECT sends;
                    # for PROPAGATED, the sender otherwise has no signal that
                    # the recipient received the message — only that the
                    # propagation host stored it.  The ack mechanism (option C)
                    # lets us recover honest end-to-end delivery semantics.
                    #
                    # FIELD_CUSTOM_DATA is a free-form bytes field LXMF passes
                    # through unchanged; we use it for our receipt-request.
                    retry_fields = {
                        LXMF.FIELD_CUSTOM_DATA: json.dumps({
                            "retimesh_receipt_request": True,
                            "original_hash": (
                                getattr(message, "_retimesh_original_hash", None) or msg_hash
                            ),
                        }).encode("utf-8"),
                    }
                retry = LXMF.LXMessage(
                    dest,
                    self.lxmf_dest,
                    content_bytes if isinstance(content_bytes, bytes) else str(content_bytes).encode("utf-8"),
                    title=title_bytes if isinstance(title_bytes, bytes) else str(title_bytes).encode("utf-8"),
                    fields=retry_fields,
                    desired_method=LXMF.LXMessage.PROPAGATED,
                )
                retry._retimesh_already_propagated = True
                # Carry the original message's hash through to the retry
                # so the watcher can broadcast state updates against the
                # bubble the UI is actually tracking, not the new
                # propagation-retry hash that nothing on the frontend knows.
                retry._retimesh_original_hash = (
                    getattr(message, "_retimesh_original_hash", None) or msg_hash
                )
                retry.register_delivery_callback(self._on_delivery_receipt)
                retry.register_failed_callback(self._on_delivery_failed)
                self.lxmf_router.handle_outbound(retry)

                # Show "queued at propagation node" immediately while we
                # wait for the actual propagation push to land.
                if msg_hash:
                    try:
                        self.db.mark_stored(msg_hash)
                    except Exception as exc:
                        log.warning(f"Could not mark message {msg_hash[:16]} as stored: {exc}")
                self._ws_broadcast({
                    "type": "message_state",
                    "hash": msg_hash,
                    "state": 1,   # MSG_STORED — queued at propagation node
                    "via_propagation": True,
                })

                # LXMF doesn't fire delivery_callback for PROPAGATED messages
                # (it only fires on DIRECT delivery acknowledgement from the
                # recipient).  When a propagated message succeeds it just
                # transitions to LXMessage.SENT — meaning "stored at the
                # propagation node".  There's no end-to-end ack from the
                # eventual recipient back to the sender.
                #
                # To still give the user a "your message has been deposited"
                # signal (✓✓ on the bubble), we poll the retry's state for
                # a short while and broadcast DELIVERED once it reaches SENT.
                # If it goes to FAILED instead, our failed_callback handles it.
                _orig_hash_for_poll = msg_hash
                _retry_ref = retry
                def _watch_propagation_send():
                    deadline = time.time() + 60  # give it 60s
                    while time.time() < deadline:
                        try:
                            if _retry_ref.state == LXMF.LXMessage.SENT:
                                # Stored at the propagation node — closest
                                # equivalent to "delivered" we'll get.
                                if _orig_hash_for_poll:
                                    try: self.db.mark_delivered(_orig_hash_for_poll)
                                    except Exception: pass
                                self._ws_broadcast({
                                    "type": "message_state",
                                    "hash": _orig_hash_for_poll,
                                    "state": 2,
                                    "via_propagation": True,
                                })
                                log.info(
                                    f"Message stored at propagation node: "
                                    f"{(_orig_hash_for_poll or '')[:16]}"
                                )
                                return
                            if _retry_ref.state == LXMF.LXMessage.FAILED:
                                return  # failed_callback already fired
                        except Exception:
                            pass
                        _sleep(2)
                if _GEVENT_AVAILABLE:
                    gevent.spawn(_watch_propagation_send)
                else:
                    threading.Thread(target=_watch_propagation_send, daemon=True).start()
                return
            except Exception as exc:
                log.error(f"Propagation retry failed: {exc}")
                import traceback; traceback.print_exc()

        # No propagation configured, or propagation retry itself failed
        self._ws_broadcast({
            "type": "delivery_failed",
            "hash": msg_hash,
            "via_propagation": False,
        })

    # ── File Transfer ──

    def send_file(self, dest_hex, filename, file_data_b64):
        """Send a file via LXMF message with base64 payload.

        The full base64 blob goes over the wire to the recipient, but only
        compact metadata is persisted to the local messages table so we don't
        bloat the SQLite DB with potentially large binary content.
        """
        if self.db.is_blocked(dest_hex):
            return {"error": "This peer is blocked. Unblock them to send files."}
        file_data = base64.b64decode(file_data_b64)

        # Sanitize filename
        safe_name = "".join(c for c in filename if c.isalnum() or c in ".-_ ")
        if not safe_name:
            safe_name = "unnamed_file"

        file_hash = hashlib.sha256(file_data).hexdigest()

        # Wire payload — full base64 data so the recipient can reconstruct the file
        wire_payload = json.dumps({
            "type":     "file",
            "filename": safe_name,
            "data":     file_data_b64,
            "hash":     file_hash,
        })

        # Send without saving the huge base64 blob to the messages table
        result = self.send_message(dest_hex, wire_payload, content_type="file", _skip_db=True)

        if "error" not in result:
            # Save the file to disk first
            filepath = self.files_dir / safe_name
            filepath.write_bytes(file_data)

            # Persist compact metadata to messages table (no base64 blob)
            compact_meta = json.dumps({
                "type":     "file",
                "filename": safe_name,
                "filesize": len(file_data),
                "hash":     file_hash,
            })
            msg_hash = result.get("hash")
            self.db.save_message(
                dest_hex, "out", compact_meta, "file", msg_hash,
                identity_hash=self.active_identity_hash,
            )

            self.db.save_file_transfer(
                dest_hex, "out", safe_name, len(file_data), file_hash, str(filepath),
                identity_hash=self.active_identity_hash
            )

        return result

    # ── Audio Call (RNS Link-based) ──
    #
    # Lifted from the voice_test harness which was verified to have
    # minimal latency.  Only mu-law is supported on the wire — the codec
    # ID byte is preserved for future expansion but the receiver always
    # uses mu-law.  Key properties:
    #   - Single RNS packet type for audio (MTU-safe chunking)
    #   - Accept-signal retransmission (5 attempts, 400 ms apart,
    #     aborts on any incoming packet proving the caller saw it)
    #   - Adaptive jitter buffer on the browser side (not in Python)
    #   - No complex call state machine — just ringing/active

    PTYPE_SIGNAL = b'\x01'
    PTYPE_AUDIO  = b'\x02'
    _RNS_MAX_AUDIO = 379   # fits in one RNS Link packet under the ~383 byte MTU

    def _on_incoming_link(self, link):
        """An inbound RNS Link was opened to our voicetest.call destination."""
        log.info(f"Incoming call link! status={link.status}")
        link.set_link_closed_callback(self._on_link_closed)
        link.set_packet_callback(self._on_call_packet)

        # Identify caller by remote identity → LXMF dest reverse-lookup.
        peer_hex = None
        try:
            rid = link.get_remote_identity()
            if rid:
                peer_hex = self._identity_to_dest.get(rid.hexhash) or rid.hexhash
        except Exception:
            pass
        if not peer_hex:
            peer_hex = f"incoming_{id(link)}"

        # Reject blocked callers immediately.
        try:
            if self.db.is_blocked(peer_hex):
                log.info(f"Blocked caller {peer_hex[:16]} — tearing down link")
                link.teardown()
                return
        except Exception:
            pass

        caller_name = self.peers.get(peer_hex, {}).get("display_name") or peer_hex[:16]

        with self._lock:
            self.active_calls[peer_hex] = {
                "state": "ringing",
                "started": None,         # set when accepted
                "link": link,
                "codec": "mu_law",
                "direction": "in",
                "_accept_confirmed": False,
            }
            self.active_links[peer_hex] = link
        link._retimesh_peer = peer_hex

        self._ws_broadcast({
            "type":      "call_incoming",
            "peer_hash": peer_hex,
            "caller":    caller_name,
            "codec":     "mu_law",
        })

    def _on_link_closed(self, link):
        """An RNS Link closed — end whichever call it was part of."""
        peers_to_remove = []
        with self._lock:
            for ph, l in list(self.active_links.items()):
                if l is link:
                    peers_to_remove.append(ph)

        for peer_hex in peers_to_remove:
            with self._lock:
                call = self.active_calls.pop(peer_hex, None)
                self.active_links.pop(peer_hex, None)
                self._audio_seq.pop(peer_hex, None)
                already_ended = call.get("_ended") if call else False

            if call and not already_ended:
                started  = call.get("started")
                duration = (time.time() - started) if started else 0
                codec    = call.get("codec", "mu_law")
                direction = call.get("direction", "out")
                try:
                    self.db.save_call(peer_hex, direction, codec, duration)
                except Exception:
                    pass
                self._ws_broadcast({
                    "type":      "call_ended",
                    "peer_hash": peer_hex,
                    "duration":  round(duration, 1),
                })
                log.info(f"Call ended with {peer_hex[:16]} after {duration:.1f}s")

    def _on_call_packet(self, data, packet):
        """Incoming packet on an active call Link — either signal or audio."""
        try:
            if len(data) < 2:
                return
            ptype   = data[0:1]
            payload = data[1:]

            peer_hex = getattr(packet.link, "_retimesh_peer", None)
            if not peer_hex:
                # Fallback: scan
                for ph, l in list(self.active_links.items()):
                    if l is packet.link:
                        peer_hex = ph
                        break
            if not peer_hex:
                return

            # Any packet from the remote end implicitly confirms our accept
            if peer_hex in self.active_calls:
                self.active_calls[peer_hex]["_accept_confirmed"] = True

            if ptype == self.PTYPE_SIGNAL:
                signal = payload.decode("utf-8", errors="ignore")
                log.info(f"Signal from {peer_hex[:16]}: {signal}")
                if signal == "accept":
                    with self._lock:
                        if peer_hex in self.active_calls:
                            self.active_calls[peer_hex]["state"]   = "active"
                            self.active_calls[peer_hex]["started"] = time.time()
                    self._ws_broadcast({
                        "type":      "call_connected",
                        "peer_hash": peer_hex,
                        "codec":     "mu_law",
                    })
                elif signal == "end":
                    # Remote hung up — tear down locally (triggers _on_link_closed)
                    with self._lock:
                        if peer_hex in self.active_calls:
                            self.active_calls[peer_hex]["_ended"] = False
                    try:
                        packet.link.teardown()
                    except Exception:
                        pass

            elif ptype == self.PTYPE_AUDIO:
                # Audio layout on the wire: [seq_hi][seq_lo][codec_id][...audio]
                if len(payload) < 3:
                    return
                if not hasattr(self, "_audio_recv_logged"):
                    self._audio_recv_logged = True
                    log.info(f"First audio packet received from {peer_hex[:16]} ({len(payload) - 3} bytes)")

                wire_cid  = payload[2]
                audio     = payload[3:]

                # If the peer sent Codec2 (LoRa mode), transcode back to
                # mu-law for the browser, which can only decode mu-law.
                # The browser sees codec_id=0x00 in the WS frame regardless
                # of what was on the RNS wire.
                out_cid = wire_cid
                if wire_cid == CODEC_ID_MAP.get("codec2_1200") and "codec2_1200" in CODECS:
                    try:
                        pcm   = CODECS["codec2_1200"].decode(audio)   # Codec2 → PCM
                        audio = CODECS["mu_law"].encode(pcm)          # PCM → mu-law
                        out_cid = 0x00
                    except Exception as e:
                        log.warning(f"Codec2 decode failed: {e}")
                        return

                # Browser frame layout: [0xAA][seq_hi][seq_lo][codec_id][...audio]
                ws_frame = b"\xAA" + payload[:2] + bytes([out_cid]) + audio
                self._ws_broadcast_binary(ws_frame)

        except Exception as e:
            log.error(f"Call packet error: {e}")

    def start_call(self, dest_hex, codec_name="mu_law"):
        """Initiate a call by opening an RNS Link to the peer's voicetest.call destination."""
        if self.db.is_blocked(dest_hex):
            return {"error": "This peer is blocked. Unblock them to call."}
        if dest_hex in self.active_calls:
            return {"error": "Already in a call with this peer"}

        # Resolve peer identity.  dest_hex is the LXMF destination hash; we use
        # Identity.recall to get the identity object, then build an OUT call dest.
        dest_identity = RNS.Identity.recall(bytes.fromhex(dest_hex))
        if not dest_identity and dest_hex in self.peers:
            dest_identity = self.peers[dest_hex].get("identity")
        if not dest_identity:
            return {"error": "Unknown peer identity. Wait for their announce."}

        try:
            call_dest = RNS.Destination(
                dest_identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
                "retimesh", "call"
            )
            log.info(f"Opening call link to {dest_hex[:16]} "
                     f"(call dest {RNS.prettyhexrep(call_dest.hash)})")
            link = RNS.Link(call_dest)
            link.set_link_closed_callback(self._on_link_closed)
            link.set_packet_callback(self._on_call_packet)
            link._retimesh_peer = dest_hex

            with self._lock:
                self.active_calls[dest_hex] = {
                    "state": "connecting",
                    "started": None,
                    "link": link,
                    "codec": "mu_law",
                    "direction": "out",
                    "_accept_confirmed": False,
                }
                self.active_links[dest_hex] = link

            # Watch the link transition from connecting → ACTIVE (which means the
            # RNS handshake succeeded; the callee's overlay should now be showing).
            def _watch():
                start = time.time()
                while time.time() - start < 15:
                    if link.status == RNS.Link.ACTIVE:
                        with self._lock:
                            if dest_hex in self.active_calls:
                                self.active_calls[dest_hex]["state"] = "ringing"
                        log.info(f"Call link ACTIVE with {dest_hex[:16]}")
                        self._ws_broadcast({
                            "type":      "call_ringing",
                            "peer_hash": dest_hex,
                            "codec":     "mu_law",
                        })
                        return
                    if link.status == RNS.Link.CLOSED:
                        log.warning(f"Call link CLOSED before ACTIVE (peer {dest_hex[:16]})")
                        break
                    gevent.sleep(0.05) if _GEVENT_AVAILABLE else time.sleep(0.05)

                # Timeout or early close — clean up and notify
                with self._lock:
                    self.active_calls.pop(dest_hex, None)
                    self.active_links.pop(dest_hex, None)
                self._ws_broadcast({
                    "type":      "call_failed",
                    "peer_hash": dest_hex,
                    "reason":    "Link establishment timed out",
                })

            if _GEVENT_AVAILABLE:
                gevent.spawn(_watch)
            else:
                threading.Thread(target=_watch, daemon=True).start()

            try:
                self.db.save_call(dest_hex, "out", "mu_law")
            except Exception:
                pass
            return {"status": "connecting", "codec": "mu_law"}
        except Exception as e:
            log.error(f"start_call failed: {e}")
            import traceback; traceback.print_exc()
            return {"error": str(e)}

    def accept_call(self, peer_hex):
        """Accept an incoming call — send accept signal with retransmit."""
        if peer_hex not in self.active_calls:
            return {"error": "No incoming call from this peer"}

        with self._lock:
            self.active_calls[peer_hex]["state"]   = "active"
            self.active_calls[peer_hex]["started"] = time.time()
            self.active_calls[peer_hex]["_accept_confirmed"] = False

        link = self.active_links.get(peer_hex)
        log.info(f"Call accepted for {peer_hex[:16]}")

        def _send_accept():
            """Retransmit the accept signal up to 5 times, 400 ms apart.
            Aborts as soon as the caller sends us any packet (proving they got it)."""
            sig = self.PTYPE_SIGNAL + b"accept"
            for attempt in range(5):
                call = self.active_calls.get(peer_hex)
                if not call or call.get("_accept_confirmed"):
                    return
                if link and link.status == RNS.Link.ACTIVE:
                    try:
                        RNS.Packet(link, sig).send()
                        log.info(f"Accept signal sent (attempt {attempt + 1}/5)")
                    except Exception as e:
                        log.error(f"Accept signal send failed: {e}")
                if attempt < 4:
                    gevent.sleep(0.4) if _GEVENT_AVAILABLE else time.sleep(0.4)

        if _GEVENT_AVAILABLE:
            gevent.spawn(_send_accept)
        else:
            threading.Thread(target=_send_accept, daemon=True).start()

        # Notify our own UI that the call is now active (callee side)
        self._ws_broadcast({
            "type":      "call_connected",
            "peer_hash": peer_hex,
            "codec":     "mu_law",
        })
        return {"status": "accepted", "codec": "mu_law"}

    def end_call(self, peer_hex):
        """End a call — send 'end' signal then tear down the link."""
        with self._lock:
            call = self.active_calls.get(peer_hex, {})
            link = self.active_links.get(peer_hex)
            if peer_hex in self.active_calls:
                self.active_calls[peer_hex]["_ended"] = True   # suppress double broadcast in _on_link_closed

        if link:
            try:
                RNS.Packet(link, self.PTYPE_SIGNAL + b"end").send()
            except Exception:
                pass

        started   = call.get("started")
        duration  = (time.time() - started) if started else 0
        codec     = call.get("codec", "mu_law")
        direction = call.get("direction", "out")

        if link:
            try: link.teardown()
            except Exception: pass

        with self._lock:
            self.active_calls.pop(peer_hex, None)
            self.active_links.pop(peer_hex, None)
            self._audio_seq.pop(peer_hex, None)

        if call:
            try:
                self.db.save_call(peer_hex, direction, codec, duration)
            except Exception:
                pass

        self._ws_broadcast({
            "type":      "call_ended",
            "peer_hash": peer_hex,
            "duration":  round(duration, 1),
        })
        return {"status": "ended", "duration": round(duration, 1)}

    def send_call_audio(self, dest_hex, audio_bytes, codec_id=None):
        """Send audio over the RNS Link.  MTU-safe chunking — each RNS packet
        carries at most _RNS_MAX_AUDIO bytes of audio + 4-byte header.

        The browser always sends mu-law.  If the active call was negotiated
        as codec2_1200 (LoRa mode), we transcode mu-law → PCM → Codec2 here
        so only ~1.2 kbps goes on the RNS wire instead of 64 kbps.
        """
        link = self.active_links.get(dest_hex)
        if not link or link.status != RNS.Link.ACTIVE:
            return

        if isinstance(audio_bytes, str):
            audio_bytes = base64.b64decode(audio_bytes)
        else:
            audio_bytes = bytes(audio_bytes)
        if not audio_bytes:
            return

        # Look up the negotiated codec for this call.  If the call is
        # running as codec2_1200, transcode the browser's mu-law frame
        # into Codec2 bits before sending on the RNS wire.
        call = self.active_calls.get(dest_hex, {})
        wire_codec = call.get("codec", "mu_law")

        # Default codec_id is whatever the browser stamped (mu-law == 0)
        cid = codec_id if isinstance(codec_id, int) else 0

        if wire_codec == "codec2_1200" and "codec2_1200" in CODECS:
            try:
                pcm  = CODECS["mu_law"].decode(audio_bytes)       # mu-law → 16-bit PCM
                audio_bytes = CODECS["codec2_1200"].encode(pcm)    # PCM → Codec2 bits
                cid = CODEC_ID_MAP["codec2_1200"]                  # 0x06 on the wire
            except Exception as e:
                log.warning(f"Codec2 encode failed, falling back to mu-law: {e}")

        codec_byte = bytes([cid & 0xFF])

        chunks = []
        if len(audio_bytes) <= self._RNS_MAX_AUDIO:
            chunks.append(audio_bytes)
        else:
            for off in range(0, len(audio_bytes), self._RNS_MAX_AUDIO):
                chunks.append(audio_bytes[off:off + self._RNS_MAX_AUDIO])

        for chunk in chunks:
            with self._lock:
                seq = (self._audio_seq.get(dest_hex, 0) + 1) & 0xFFFF
                self._audio_seq[dest_hex] = seq
            pkt = self.PTYPE_AUDIO + struct.pack(">H", seq) + codec_byte + chunk
            try:
                RNS.Packet(link, pkt).send()
            except Exception as e:
                log.error(f"Audio send error: {e}")
                return


    # ── WebSocket Client Management ──
    #
    # One ws per browser connection.  Simpler than SSE: binary audio frames
    # ride the same socket as signaling, so no queue prioritization is needed
    # and audio bypasses base64 wrapping entirely.

    def register_ws(self, ws):
        """Register a new WebSocket client."""
        self.ws_clients.add(ws)

    def unregister_ws(self, ws):
        self.ws_clients.discard(ws)

    def _ws_broadcast(self, data):
        """Send a JSON event to every connected WebSocket client."""
        msg = json.dumps(data)
        dead = set()
        for ws in list(self.ws_clients):
            try:
                if hasattr(ws, "closed") and ws.closed:
                    dead.add(ws); continue
                ws.send(msg)
            except Exception:
                dead.add(ws)
        self.ws_clients -= dead

    def _ws_broadcast_binary(self, data: bytes):
        """Send raw binary audio data to every WebSocket client.
        No queue, no base64 — the browser decodes the ArrayBuffer directly
        from [0xAA][seq_hi][seq_lo][codec_id][audio_payload]."""
        payload = bytes(data)
        dead = set()
        for ws in list(self.ws_clients):
            try:
                if hasattr(ws, "closed") and ws.closed:
                    dead.add(ws); continue
                ws.send(payload)
            except Exception:
                dead.add(ws)
        self.ws_clients -= dead

    # ── Codec Comparison Utility ──

    def compare_codecs(self, pcm_data_b64):
        """
        Compare all available codecs on the same audio sample.
        Returns compression ratios, bitrates, and encoded sizes.
        """
        pcm_data = base64.b64decode(pcm_data_b64)
        original_size = len(pcm_data)
        duration_s = len(pcm_data) / (AUDIO_SAMPLE_RATE * 2)  # 16-bit = 2 bytes/sample

        results = []
        for name, codec in CODECS.items():
            try:
                encoded = codec.encode(pcm_data)
                decoded = codec.decode(encoded)
                encoded_size = len(encoded)
                compression_ratio = original_size / encoded_size if encoded_size > 0 else 0
                effective_bitrate = (encoded_size * 8) / duration_s if duration_s > 0 else 0

                results.append({
                    "codec": name,
                    "bitrate_nominal": codec.bitrate,
                    "bitrate_effective": round(effective_bitrate),
                    "original_bytes": original_size,
                    "encoded_bytes": encoded_size,
                    "decoded_bytes": len(decoded),
                    "compression_ratio": round(compression_ratio, 2),
                    "duration_s": round(duration_s, 3),
                    "latency_budget_ms": round(1000 / (codec.bitrate / (encoded_size * 8 / duration_s)), 1) if codec.bitrate > 0 and duration_s > 0 else 0,
                })
            except Exception as e:
                results.append({
                    "codec": name,
                    "error": str(e),
                })

        return results


# ─── Flask Application ────────────────────────────────────────────────────────
# When packaged with PyInstaller the process runs from a temp _MEIPASS directory.
# We resolve the bundle root here so Flask finds templates/ and static/ regardless
# of whether we're running from source or as a frozen executable.
if getattr(sys, "frozen", False):
    _BUNDLE_DIR = sys._MEIPASS          # PyInstaller extraction directory
else:
    _BUNDLE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(_BUNDLE_DIR, "templates"),
    static_folder=os.path.join(_BUNDLE_DIR, "static"),
)
node = None  # Global MeshNode instance
_wsgi_server = None  # gevent WSGIServer instance — exposed so api_restart
                     # can stop it cleanly before the child process binds,
                     # freeing the listening port immediately.

# ── S-3: Global file-upload limit removed ─────────────────────────────────────
# Previously capped at 50 MB.  Removed at user request — large file transfers
# (firmware images, archives, recordings) should be allowed through the same
# pipeline.  The transport layer (LXMF / RNS) still imposes its own per-link
# practical ceilings, but the HTTP-side limit is now uncapped.
# app.config['MAX_CONTENT_LENGTH'] is left unset (no enforcement).

# ── S-1: Authentication guard ─────────────────────────────────────────────────
# Paths that are unconditionally public (no token required):
# - "/" serves the initial HTML and sets the session cookie
# - "/profile" is the shareable public profile page
# - "/ws" does its own auth via the session cookie inside the middleware
_PUBLIC_PATHS = {"/", "/profile", "/ws"}

@app.before_request
def _check_auth():
    """Reject any API call that doesn't carry the session token."""
    if request.path in _PUBLIC_PATHS:
        return None
    if request.path.startswith("/static/") or request.path.startswith("/files/"):
        return None
    token = (request.cookies.get("retimesh_token")
             or request.headers.get("X-Retimesh-Token"))
    if token != _SESSION_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401


@app.route("/")
def index():
    resp = make_response(render_template("index.html"))
    # Deliver the session token as a cookie on first page load.
    # SameSite=Strict prevents cross-site CSRF; HttpOnly keeps it out of JS.
    resp.set_cookie(
        "retimesh_token", _SESSION_TOKEN,
        samesite="Strict", httponly=True,
        path="/",
    )
    return resp


@app.route("/api/identity")
def api_identity():
    return jsonify(node.get_identity_info())


@app.route("/api/peers")
def api_peers():
    """Return peers + which RNS interface each is currently reachable via.

    The interface_name field is computed at request time from RNS's path
    table (Transport.next_hop_interface).  It's not persisted to the DB —
    it changes as paths get learned/forgotten — so we recompute on each
    GET.  Used by the network diagram to anchor each peer to the correct
    interface node.  Falls back to "shared_instance" when RNS doesn't
    know a path; treating those as coming through the local daemon is
    truthful (they HAD to come through it to reach us at all) and avoids
    leaving peers stranded with no parent in the diagram.
    """
    peers = node.db.get_peers(identity_hash=node.active_identity_hash)
    for p in peers:
        try:
            iface = RNS.Transport.next_hop_interface(bytes.fromhex(p["dest_hash"]))
            if iface is not None:
                p["interface_name"] = getattr(iface, "name", None) or str(iface)
            else:
                p["interface_name"] = "shared_instance"
        except Exception:
            p["interface_name"] = "shared_instance"
    return jsonify(peers)


@app.route("/api/announce", methods=["POST"])
def api_announce():
    data = request.json or {}
    node.send_announce(data.get("display_name"))
    return jsonify({"status": "ok"})


@app.route("/api/messages/<peer_hash>")
def api_messages(peer_hash):
    try:                                    # S-6: validate hash format before DB query
        peer_hash = _validate_hash(peer_hash)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(node.db.get_messages(peer_hash, identity_hash=node.active_identity_hash))


@app.route("/api/send", methods=["POST"])
def api_send():
    data    = request.json or {}
    ph      = str(data.get("peer_hash", "")).strip()
    content = str(data.get("content", ""))
    # Fix L: validate hash and enforce a content-length ceiling
    try:
        ph = _validate_hash(ph)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if len(content) > 65536:
        return jsonify({"error": "content too long (max 65536 chars)"}), 400
    result = node.send_message(ph, content)
    return jsonify(result)


@app.route("/api/send_file", methods=["POST"])
def api_send_file():
    data = request.json or {}
    raw_hash = str(data.get("peer_hash", "")).strip()
    filename  = str(data.get("filename", "")).strip()
    file_data = data.get("data", "")
    try:
        raw_hash = _validate_hash(raw_hash)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if not filename:
        return jsonify({"error": "filename is required"}), 400
    if not file_data:
        return jsonify({"error": "data is required"}), 400
    result = node.send_file(raw_hash, filename, file_data)
    return jsonify(result)


@app.route("/api/codecs")
def api_codecs():
    return jsonify({name: c.info() for name, c in CODECS.items()})


@app.route("/api/codec_compare", methods=["POST"])
def api_codec_compare():
    data = request.json or {}
    pcm_data = data.get("pcm_data", "")
    if not pcm_data:
        return jsonify({"error": "pcm_data is required"}), 400
    results = node.compare_codecs(pcm_data)
    return jsonify(results)


@app.route("/api/codec/recommend")
def api_codec_recommend():
    """Recommend the best codec based on active interface bandwidth."""
    try:
        # Find the lowest bitrate interface (bottleneck)
        min_bitrate = None
        bottleneck_iface = "Unknown"
        if hasattr(RNS, 'Transport') and hasattr(RNS.Transport, 'interfaces'):
            for iface in RNS.Transport.interfaces:
                br = getattr(iface, 'bitrate', None)
                if br and (min_bitrate is None or br < min_bitrate):
                    min_bitrate = br
                    bottleneck_iface = str(iface)

        # Codec recommendations based on available bandwidth.
        # Two codecs only: mu_law (64 kbps) for normal links, codec2_1200
        # (1.2 kbps) for LoRa / HF radio.
        codecs_info = []
        for name, codec in CODECS.items():
            info = codec.info()
            fits = True
            if min_bitrate and info.get("bitrate", 0) > min_bitrate * 0.5:
                fits = False  # Don't use more than 50% of link capacity for audio
            codecs_info.append({
                "name": name,
                "bitrate": info.get("bitrate", 0),
                "compression_ratio": info.get("compression_ratio", "1:1"),
                "fits_bandwidth": fits,
                "available": info.get("available", True),
            })

        # Pick recommendation.  LoRa-class links (sub-10 kbps) need Codec2;
        # everything faster can use mu-law.
        recommended = "mu_law"
        if min_bitrate and min_bitrate < 10000 and "codec2_1200" in CODECS:
            recommended = "codec2_1200"

        return jsonify({
            "recommended": recommended,
            "bottleneck_interface": bottleneck_iface,
            "bottleneck_bitrate": min_bitrate,
            "codecs": codecs_info,
        })
    except Exception as e:
        return jsonify({"error": str(e), "recommended": "mu_law"})


# ── Optional Dependency Management ───────────────────────────────────────────
#
# Users can check and install optional pip packages (bleak, bless)
# directly from the Settings → Dependencies tab without leaving the app.
# gevent is not listed here because it is already required at startup.

_OPTIONAL_DEPS = [
    {
        "id":      "bleak",
        "package": "bleak",
        "import":  "bleak",
        "label":   "bleak",
        "desc":    "Bluetooth Low Energy (BLE) client, required for Bluetooth interface",
    },
    {
        "id":      "bless",
        "package": "bless",
        "import":  "bless",
        "label":   "bless",
        "desc":    "BLE peripheral/server, required for Bluetooth interface (some platforms)",
    },
]


def _dep_status() -> list:
    """Return status dict for each optional dependency."""
    import importlib.util
    result = []
    for dep in _OPTIONAL_DEPS:
        installed = importlib.util.find_spec(dep["import"]) is not None
        result.append({
            "id":        dep["id"],
            "label":     dep["label"],
            "desc":      dep["desc"],
            "package":   dep["package"],
            "installed": installed,
        })
    return result


@app.route("/api/deps")
def api_deps():
    """Return installation status of optional dependencies."""
    return jsonify(_dep_status())


@app.route("/api/deps/install", methods=["POST"])
def api_deps_install():
    """Install one or more optional pip packages.

    Body: {"packages": ["bleak", "bless"]} — list of package IDs to install.
    Runs pip as a subprocess; streams stdout/stderr back in the response.
    """
    import subprocess
    data = request.json or {}
    requested = data.get("packages", [])

    # Only allow IDs listed in _OPTIONAL_DEPS (whitelist)
    allowed_packages = {d["id"]: d["package"] for d in _OPTIONAL_DEPS}
    to_install = []
    for pkg_id in requested:
        if pkg_id in allowed_packages:
            to_install.append(allowed_packages[pkg_id])

    if not to_install:
        return jsonify({"error": "No valid packages specified"}), 400

    log.info(f"Installing packages via pip: {to_install}")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--quiet"] + to_install,
            capture_output=True,
            text=True,
            timeout=120,
        )
        success = result.returncode == 0
        output  = (result.stdout + result.stderr).strip()
        log.info(f"pip install {'succeeded' if success else 'failed'}: {output[:200]}")
        # Re-check status after install
        status = _dep_status()
        return jsonify({
            "success": success,
            "output":  output[:2000],   # cap at 2 KB
            "status":  status,
        })
    except subprocess.TimeoutExpired:
        return jsonify({"error": "pip install timed out (120 s)"}), 500
    except Exception as e:
        log.error(f"pip install error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/call/start", methods=["POST"])
def api_call_start():
    data = request.json or {}
    raw_hash = str(data.get("peer_hash", "")).strip()
    try:
        raw_hash = _validate_hash(raw_hash)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    codec = str(data.get("codec", "mu_law"))
    result = node.start_call(raw_hash, codec)
    return jsonify(result)


@app.route("/files/<path:filename>")
def serve_file(filename):
    return send_from_directory(str(node.files_dir), filename)


# ── Setup Wizard / Config API ──

@app.route("/api/config")
def api_get_config():
    return jsonify(node.db.get_all_config())


@app.route("/api/config", methods=["POST"])
def api_set_config():
    data = request.json or {}
    for key, value in data.items():
        node.db.set_config(str(key), str(value))
    return jsonify({"status": "ok"})


@app.route("/api/setup_complete")
def api_setup_check():
    return jsonify({"complete": node.db.get_config("setup_complete", "false") == "true"})


@app.route("/api/setup_complete", methods=["POST"])
def api_setup_done():
    node.db.set_config("setup_complete", "true")
    return jsonify({"status": "ok"})


@app.route("/api/first_run")
def api_first_run():
    """Return first-run state: whether setup has been completed and identity info."""
    setup_done  = node.db.get_config("setup_complete", "false") == "true"
    identities  = node.db.get_identities()
    has_identity = bool(identities)
    id_info      = node.get_identity_info()
    return jsonify({
        "first_run":      not setup_done,
        "has_identity":   has_identity,
        "storage_dir":    str(node.storage_dir),
        "lxmf_address":   id_info.get("lxmf_address", ""),
        "identity_hash":  id_info.get("identity_hash", ""),
        "identities":     identities,
    })


@app.route("/api/identity/import", methods=["POST"])
def api_import_identity():
    """Import an existing RNS identity file into RetiMesh."""
    data      = request.json or {}
    file_path = data.get("file_path", "").strip()
    name      = data.get("name", "Imported Identity").strip() or "Imported Identity"

    if not file_path:
        return jsonify({"error": "No file path provided"}), 400

    file_path = os.path.expanduser(file_path)
    if not os.path.isfile(file_path):
        # S-4: Never echo the supplied path — doing so allows filesystem probing.
        return jsonify({"error": "File not found"}), 404

    try:
        imported_id = RNS.Identity.from_file(file_path)
        if not imported_id:
            return jsonify({"error": "Could not load identity from file (invalid format?)"}), 400

        identities_dir = node.storage_dir / "identities"
        identities_dir.mkdir(exist_ok=True)
        dest_path = str(identities_dir / f"{imported_id.hexhash[:16]}.id")
        imported_id.to_file(dest_path)
        node.db.save_identity(name, dest_path, imported_id.hexhash, is_active=0)

        log.info(f"Identity imported: {name} [{imported_id.hexhash[:16]}]")
        return jsonify({"status": "ok", "identity_hash": imported_id.hexhash, "name": name})
    except Exception as e:
        log.error(f"Identity import error: {e}")
        return jsonify({"error": str(e)}), 500


# ── Propagation Node API ──

@app.route("/api/propagation", methods=["GET"])
def api_get_propagation():
    prop_node = node.db.get_config("propagation_node", "")
    return jsonify({"propagation_node": prop_node})


@app.route("/api/propagation", methods=["POST"])
def api_set_propagation():
    data = request.json
    node_hash = data.get("node_hash", "").strip()
    if node_hash:
        result = node.set_propagation_node(node_hash)
        return jsonify(result)
    else:
        # Clear propagation node
        node.db.set_config("propagation_node", "")
        return jsonify({"status": "ok", "message": "Propagation node cleared"})


@app.route("/api/propagation/sync", methods=["POST"])
def api_sync_propagation():
    result = node.sync_from_propagation_node()
    return jsonify(result)


# ── Act-as-propagation-node (store & forward host) ──

@app.route("/api/propagation/host", methods=["GET"])
def api_get_propagation_host():
    """Return current propagation-host status + our propagation hash if active."""
    enabled = node.is_propagation_node_enabled()
    return jsonify({
        "enabled": enabled,
        "propagation_hash": node._propagation_node_hex() if enabled else "",
    })


@app.route("/api/propagation/host", methods=["POST"])
def api_set_propagation_host():
    """Enable or disable acting as a propagation node.

    Body: {"enabled": true|false}
    """
    data = request.json or {}
    enable = bool(data.get("enabled"))
    if enable:
        result = node.enable_propagation_node()
    else:
        result = node.disable_propagation_node()
    result["enabled"] = node.is_propagation_node_enabled()
    result["propagation_hash"] = node._propagation_node_hex() if result["enabled"] else ""
    return jsonify(result)


# ── Identity Management API ──

@app.route("/api/identities")
def api_identities():
    return jsonify(node.db.get_identities())


@app.route("/api/identities", methods=["POST"])
def api_create_identity():
    data = request.json or {}
    name = data.get("name", "Unnamed")

    # Create new RNS identity
    new_id = RNS.Identity()
    identities_dir = node.storage_dir / "identities"
    identities_dir.mkdir(exist_ok=True)

    safe_name = "".join(c for c in name if c.isalnum() or c in "-_ ").strip() or "identity"
    file_path = str(identities_dir / f"{safe_name}_{int(time.time())}")
    new_id.to_file(file_path)

    node.db.save_identity(name, file_path, new_id.hexhash, is_active=0)
    return jsonify({"status": "ok", "identity_hash": new_id.hexhash})


@app.route("/api/identities/<int:identity_id>/activate", methods=["POST"])
def api_activate_identity(identity_id):
    identities = node.db.get_identities()
    target = None
    for ident in identities:
        if ident["id"] == identity_id:
            target = ident
            break

    if not target:
        return jsonify({"error": "Identity not found"}), 404

    node.db.set_active_identity(identity_id)

    # F-3: return requires_restart=True so the frontend can disable the send/call
    # buttons and display a prominent "Restart required" banner.  The identity
    # change only takes effect after a full process restart because the RNS
    # stack, LXMF router, and all destination objects are constructed once at
    # startup — they cannot be hot-swapped without reinitialising the node.
    # The SSE broadcast ensures any tab that missed the HTTP response also gets
    # the signal.
    node._ws_broadcast({
        "type":    "restart_required",
        "reason":  "identity_changed",
        "message": "A different identity was activated. Restart RetiMesh to apply.",
    })
    return jsonify({
        "status":           "ok",
        "requires_restart": True,
        "message":          "Identity activated. Restart the app to apply.",
    })


@app.route("/api/identities/<int:identity_id>", methods=["DELETE"])
def api_delete_identity(identity_id):
    node.db.delete_identity(identity_id)
    return jsonify({"status": "ok"})


@app.route("/api/identities/known")
def api_known_identities():
    """Return all peers seen via announce, merged from DB + in-memory peers dict.

    Each entry includes:
      dest_hash     — LXMF destination hash hex (the peer's reachable address)
      display_name  — human-readable name from their announce app_data
      identity_hash — RNS identity hash if known (from in-memory announce)
      last_announce — UNIX timestamp of the most recent announce seen
      last_seen     — UNIX timestamp of most recent activity (message / call / etc.)
    """
    # Start from persisted peers (gives last_seen, nickname, etc.)
    db_peers = {p["dest_hash"]: p for p in node.db.get_peers()}

    # Overlay with in-memory announce data (more recent display_name + identity_hash)
    for dest_hex, p in node.peers.items():
        identity = p.get("identity")
        identity_hash = ""
        if identity and hasattr(identity, "hexhash"):
            identity_hash = identity.hexhash
        if dest_hex in db_peers:
            db_peers[dest_hex]["identity_hash"] = identity_hash or db_peers[dest_hex].get("identity_hash", "")
            db_peers[dest_hex]["last_announce"] = p.get("last_announce", db_peers[dest_hex].get("last_announce", 0))
            if p.get("display_name"):
                db_peers[dest_hex]["display_name"] = p["display_name"]
        else:
            db_peers[dest_hex] = {
                "dest_hash":    dest_hex,
                "display_name": p.get("display_name", dest_hex[:12]),
                "identity_hash": identity_hash,
                "last_announce": p.get("last_announce", 0),
                "last_seen":    p.get("last_announce", 0),
                "rssi":         None,
                "snr":          None,
            }

    result = sorted(
        db_peers.values(),
        key=lambda x: x.get("last_announce") or x.get("last_seen") or 0,
        reverse=True,
    )
    # Return only the fields the frontend needs — avoid leaking private key material
    safe_keys = {"dest_hash", "display_name", "identity_hash", "last_announce", "last_seen", "rssi", "snr", "nickname", "notes", "pinned"}
    return jsonify([{k: v for k, v in peer.items() if k in safe_keys} for peer in result])


# ── Contact Management API ──

@app.route("/api/peers/<peer_hash>/contact", methods=["POST"])
def api_update_contact(peer_hash):
    try:                                    # S-6
        peer_hash = _validate_hash(peer_hash)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    data = request.json
    node.db.update_peer_contact(
        peer_hash,
        nickname=data.get("nickname"),
        pinned=data.get("pinned"),
        notes=data.get("notes"),
    )
    return jsonify({"status": "ok"})


# ── Delete API ──

@app.route("/api/messages/<int:message_id>", methods=["DELETE"])
def api_delete_message(message_id):
    # F-4: verify the message belongs to the currently-active identity before
    # deleting.  Without this check any authenticated browser tab could delete
    # messages from any identity stored in the database, not just the active one.
    msg = node.db.get_message(message_id)
    if not msg:
        return jsonify({"error": "Message not found"}), 404
    active_hash = node.active_identity_hash
    if active_hash and msg.get("identity_hash") and msg["identity_hash"] != active_hash:
        return jsonify({"error": "Forbidden: message belongs to a different identity"}), 403
    node.db.delete_message(message_id)
    return jsonify({"status": "ok"})


@app.route("/api/messages/<peer_hash>/all", methods=["DELETE"])
def api_delete_conversation(peer_hash):
    try:                                    # S-6
        peer_hash = _validate_hash(peer_hash)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    node.db.delete_conversation(peer_hash, identity_hash=node.active_identity_hash)
    return jsonify({"status": "ok"})


@app.route("/api/peers/<peer_hash>", methods=["DELETE"])
def api_delete_peer(peer_hash):
    try:                                    # S-6
        peer_hash = _validate_hash(peer_hash)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    node.db.delete_peer(peer_hash)
    return jsonify({"status": "ok"})


# ── Auto-Announce API ──

@app.route("/api/auto_announce", methods=["GET"])
def api_get_auto_announce():
    enabled = node.db.get_config("auto_announce", "true")
    interval = int(node.db.get_config("announce_interval", "30"))
    return jsonify({"enabled": enabled == "true", "interval": interval})


@app.route("/api/auto_announce", methods=["POST"])
def api_set_auto_announce():
    data = request.json
    if "enabled" in data:
        enabled = data["enabled"]
        node.db.set_config("auto_announce", "true" if enabled else "false")
        if enabled:
            node._start_auto_announce()
        else:
            node._stop_auto_announce()
    if "interval" in data:
        interval = max(10, min(300, int(data["interval"])))
        node.db.set_config("announce_interval", str(interval))
        # Restart auto-announce with new interval if running
        if node.db.get_config("auto_announce", "true") == "true":
            node._stop_auto_announce()
            node._start_auto_announce()
    return jsonify({"status": "ok"})


# ── Peer Blocking API ──

@app.route("/api/blocked")
def api_get_blocked():
    return jsonify(node.db.get_blocked_peers())


@app.route("/api/peers/<peer_hash>/block", methods=["POST"])
def api_block_peer(peer_hash):
    try:                                    # S-6
        peer_hash = _validate_hash(peer_hash)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    data = request.json or {}
    node.db.block_peer(peer_hash, data.get("reason", ""))
    # Also evict any cached path so the peer cannot immediately re-announce
    try:
        dest_bytes = bytes.fromhex(peer_hash)
        if hasattr(RNS.Transport, "expire_path"):
            RNS.Transport.expire_path(dest_bytes)
    except Exception:
        pass
    return jsonify({
        "status": "ok",
        "note": (
            "This peer's messages and announces are now silently discarded by RetiMesh "
            "as soon as they arrive; they never reach your inbox. "
            "Reticulum's transport layer may still route some encrypted packets as part "
            "of normal mesh infrastructure, but none of their content will be delivered "
            "to this application."
        ),
    })


@app.route("/api/peers/<peer_hash>/unblock", methods=["POST"])
def api_unblock_peer(peer_hash):
    try:                                    # S-6
        peer_hash = _validate_hash(peer_hash)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    node.db.unblock_peer(peer_hash)
    return jsonify({"status": "ok"})


@app.route("/api/block", methods=["POST"])
def api_block_manual():
    """Block any identity hash — including nodes you've never seen announce.
    Body: { "hash": "<hex>", "reason": "<optional>" }
    """
    data = request.json or {}
    raw  = (data.get("hash") or "").strip().lower().replace(":", "").replace(" ", "")
    if not raw:
        return jsonify({"error": "hash is required"}), 400
    # Basic validation: must be a hex string of reasonable length (16–64 chars)
    import re as _re
    if not _re.fullmatch(r"[0-9a-f]{10,64}", raw):
        return jsonify({"error": "Invalid hash format (expected hex string)"}), 400
    node.db.block_peer(raw, data.get("reason", "manual block"))
    try:
        dest_bytes = bytes.fromhex(raw)
        if hasattr(RNS.Transport, "expire_path"):
            RNS.Transport.expire_path(dest_bytes)
    except Exception:
        pass
    return jsonify({"status": "ok", "hash": raw})


# ── Reticulum Config API ──

@app.route("/api/rns_config", methods=["GET"])
def api_get_rns_config():
    """Read the Reticulum config file."""
    config_path = os.path.expanduser("~/.reticulum/config")
    if not os.path.exists(config_path):
        return jsonify({"error": "Config file not found", "path": config_path}), 404
    try:
        with open(config_path, "r") as f:
            content = f.read()
        return jsonify({"config": content, "path": config_path})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/rns_config", methods=["POST"])
def api_set_rns_config():
    """Write the Reticulum config file. Requires app restart to take effect."""
    data = request.json
    config_content = data.get("config", "")
    config_path = os.path.expanduser("~/.reticulum/config")
    try:
        # Backup existing config
        if os.path.exists(config_path):
            import shutil
            shutil.copy2(config_path, config_path + ".bak")
        with open(config_path, "w") as f:
            f.write(config_content)
        return jsonify({"status": "ok", "message": "Config saved. Restart the app to apply changes."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wipe", methods=["POST"])
def api_wipe():
    """Wipe data from the database.

    Body: {"scope": "all"|"messages"|"groups"|"peers"|"alerts"|"files"|"nuclear"}
    Defaults to "all" (everything except identity + config).
    """
    body = request.get_json(silent=True) or {}
    scope = body.get("scope", "all")
    log.info(f"DB wipe requested: scope={scope}")
    result = node.db.wipe_data(scope)
    if "error" in result:
        return jsonify({"status": "error", "error": result["error"]}), 400

    # If we wiped peers, drop the in-memory peer map too — otherwise the
    # sidebar would still show them until restart.
    if scope in ("all", "peers", "nuclear"):
        try:
            node.peers.clear()
        except Exception:
            pass
        try:
            node._ws_broadcast({"type": "peer_list_changed"})
        except Exception:
            pass

    # If we wiped files, also delete the actual files on disk.  Walk the
    # files directory and unlink everything; keep the dir itself.
    if scope in ("all", "files", "nuclear"):
        try:
            for child in node.files_dir.iterdir():
                try:
                    if child.is_file():
                        child.unlink()
                    elif child.is_dir():
                        import shutil
                        shutil.rmtree(child, ignore_errors=True)
                except Exception as e:
                    log.warning(f"failed to remove {child}: {e}")
        except Exception as e:
            log.warning(f"could not enumerate files dir: {e}")

    return jsonify({"status": "ok", "scope": scope, "deleted": result})


@app.route("/api/restart", methods=["POST"])
def api_restart():
    """Restart the entire RetiMesh process to apply config / identity changes.

    Unix: os.execv replaces the process in place (fast, clean).
    Windows: os.execv mangles argv when script paths contain spaces — the new
    Python interpreter sees each space as an argument separator and fails to
    open the script. We work around this by closing our listening socket,
    spawning a replacement process, then exiting.

    Port-collision fix: previous versions spawned the child first, slept 500ms,
    then exited.  On fast machines the child booted and tried to bind port
    5000 while the parent was still holding it (WinError 10048).  We now
    stop the server socket in the parent FIRST, so the port is released
    before the child's gevent.WSGIServer calls bind().  A small retry loop
    in the child adds belt-and-braces protection for the worst-case TIME_WAIT.
    """
    def _do_restart():
        # Give the HTTP response a moment to reach the browser
        if _GEVENT_AVAILABLE:
            gevent.sleep(0.6)
        else:
            time.sleep(0.6)
        log.info("Restarting RetiMesh process...")

        # STEP 1: stop the listening socket so the port is free.  We import
        # the server object from the main module scope — it was stored as a
        # global by main() for this exact purpose.
        try:
            srv = globals().get("_wsgi_server")
            if srv is not None:
                srv.stop(timeout=1)
                log.info("WSGI server stopped, port released")
        except Exception as _e:
            log.warning(f"Failed to stop WSGI server before restart: {_e}")

        # STEP 2: spawn the replacement process.  By this point the TCP
        # listening socket is closed so the child can bind() immediately.
        if sys.platform == "win32":
            import subprocess
            # CREATE_NEW_PROCESS_GROUP: child gets its own signal group so
            # Ctrl+C (or Ctrl+Break) in the same terminal reaches it after
            # the parent exits.  Without this, the child inherits the
            # parent's console group and signal delivery becomes ambiguous
            # during the brief overlap before the parent calls os._exit.
            CREATE_NEW_PROCESS_GROUP = 0x00000200
            try:
                subprocess.Popen(
                    [sys.executable] + sys.argv,
                    creationflags=CREATE_NEW_PROCESS_GROUP,
                    close_fds=False,   # inherit stdin/stdout/stderr
                )
            except Exception as e:
                log.error(f"Restart failed to spawn new process: {e}")
                return
            # Brief pause before exit so the child process handle stays valid
            if _GEVENT_AVAILABLE:
                gevent.sleep(0.3)
            else:
                time.sleep(0.3)
            os._exit(0)
        else:
            os.execv(sys.executable, [sys.executable] + sys.argv)

    if _GEVENT_AVAILABLE:
        gevent.spawn(_do_restart)
    else:
        threading.Thread(target=_do_restart, daemon=True).start()

    return jsonify({"status": "restarting",
                    "message": "Restarting… the page will reconnect automatically in a few seconds."})


# ── Network Dashboard API ──

@app.route("/api/network/status")
def api_network_status():
    """Return live network stats from RNS.Transport."""
    try:
        status = {
            "identity_hash": node.identity.hexhash if node.identity else "",
            "lxmf_address": "",
            "uptime": time.time() - node._start_time if hasattr(node, '_start_time') else 0,
            "peers_count": len(node.db.get_peers()),
            "active_links": len(node.active_links),
            "active_calls": len(node.active_calls),
        }
        if node.lxmf_dest:
            if hasattr(node.lxmf_dest, 'hexhash'):
                status["lxmf_address"] = node.lxmf_dest.hexhash
            elif hasattr(node.lxmf_dest, 'hash'):
                status["lxmf_address"] = RNS.hexrep(node.lxmf_dest.hash, delimit=False)
        if hasattr(node, '_page_destination') and node._page_destination:
            status["page_hash"] = node._page_destination.hexhash
        return jsonify(status)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/network/interfaces")
def api_network_interfaces():
    """Return info about active Reticulum interfaces."""
    try:
        interfaces = []
        if node.reticulum and hasattr(RNS, 'Transport') and hasattr(RNS.Transport, 'interfaces'):
            for iface in RNS.Transport.interfaces:
                iface_type  = type(iface).__name__
                iface_name  = str(iface)
                # Detect shared-instance / local-loopback interfaces so the UI
                # can display them with a clear label instead of a cryptic name.
                is_shared = iface_type in ("SharedInstanceInterface", "LocalInterface",
                                            "LocalServerInterface", "LocalClientInterface")
                iface_info = {
                    "name": iface_name,
                    "type": iface_type,
                    "online": bool(getattr(iface, 'online', True)),
                    "is_shared": is_shared,
                    "display_name": "Shared Instance (local)" if is_shared else iface_name,
                }
                # Traffic
                if hasattr(iface, 'rxb'):   iface_info["bytes_in"]  = iface.rxb
                if hasattr(iface, 'txb'):   iface_info["bytes_out"] = iface.txb
                if hasattr(iface, 'rxpkts'): iface_info["pkts_in"]  = iface.rxpkts
                if hasattr(iface, 'txpkts'): iface_info["pkts_out"] = iface.txpkts
                if hasattr(iface, 'bitrate') and iface.bitrate:
                    iface_info["bitrate"] = iface.bitrate
                # LoRa / RNode signal stats
                if hasattr(iface, 'r_rssi')  and iface.r_rssi  is not None: iface_info["rssi"]  = iface.r_rssi
                if hasattr(iface, 'r_snr')   and iface.r_snr   is not None: iface_info["snr"]   = iface.r_snr
                if hasattr(iface, 'r_q')     and iface.r_q     is not None: iface_info["quality"]= iface.r_q
                # Airtime & duty cycle (RNode)
                if hasattr(iface, 'r_airtime_short'): iface_info["airtime_short"] = iface.r_airtime_short
                if hasattr(iface, 'r_airtime_long'):  iface_info["airtime_long"]  = iface.r_airtime_long
                if hasattr(iface, 'r_channel_load_short'): iface_info["ch_load"]  = iface.r_channel_load_short
                # Frequency / bandwidth (LoRa)
                if hasattr(iface, 'frequency'): iface_info["frequency"] = iface.frequency
                if hasattr(iface, 'bandwidth'): iface_info["bandwidth"] = iface.bandwidth
                if hasattr(iface, 'sf'):        iface_info["sf"]        = iface.sf
                if hasattr(iface, 'cr'):        iface_info["cr"]        = iface.cr
                # TCP/UDP address
                if hasattr(iface, 'target_ip'):   iface_info["target_ip"]   = str(iface.target_ip)
                if hasattr(iface, 'target_port'): iface_info["target_port"] = iface.target_port
                if hasattr(iface, 'bind_ip'):     iface_info["bind_ip"]     = str(iface.bind_ip)
                if hasattr(iface, 'bind_port'):   iface_info["bind_port"]   = iface.bind_port
                # ── Bluetooth-specific fields ────────────────────────────────────
                if iface_type == "BluetoothInterface":
                    iface_info["is_bluetooth"] = True
                    iface_info["bt_mode"] = getattr(iface, 'config', {}).get("mode", "ble").upper()
                    try:
                        iface_info["bt_peers"] = iface.peer_count()
                    except Exception:
                        iface_info["bt_peers"] = 0
                    # For display, use the configured name directly
                    iface_info["display_name"] = iface_name
                    # ── RNS-level peers reachable via this BT interface ──────────
                    # Use the set of destination hashes the interface has directly
                    # heard announces from.  This is more reliable than walking
                    # RNS.Transport.destination_table for the BT peer list specifically.
                    bt_rns_peers = []
                    try:
                        heard = getattr(iface, '_heard_dest_hashes', set())
                        if heard:
                            all_peers = {
                                p["dest_hash"]: p
                                for p in node.db.get_peers(
                                    identity_hash=node.active_identity_hash or ""
                                )
                            }
                            for dest_hex in heard:
                                peer_row = all_peers.get(dest_hex)
                                bt_rns_peers.append({
                                    "dest_hash":    dest_hex,
                                    "hops":         1,
                                    "display_name": (
                                        peer_row.get("display_name") or
                                        peer_row.get("nickname") or
                                        dest_hex[:16]
                                    ) if peer_row else dest_hex[:16],
                                })
                    except Exception:
                        pass
                    iface_info["bt_rns_peers"] = bt_rns_peers
                interfaces.append(iface_info)
        return jsonify(interfaces)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/network/peer_interfaces")
def api_peer_interfaces():
    """Return a mapping of peer dest_hash → interface name.

    For each known peer, look up which interface RNS would use to reach
    them (via Transport.next_hop_interface).  Used by the network graph
    to attach peers to the correct interface node.

    Peers RNS doesn't know a path to are tagged with an empty string —
    the graph treats those as belonging to the shared instance node, on
    the basis that any traffic to them would have to go through the
    local Reticulum daemon first.
    """
    out = {}
    try:
        all_peers = node.db.get_peers(identity_hash=node.active_identity_hash) or []
        for p in all_peers:
            dest_hex = p.get("dest_hash")
            if not dest_hex:
                continue
            try:
                dest_bytes = bytes.fromhex(dest_hex)
                iface = RNS.Transport.next_hop_interface(dest_bytes)
                if iface is not None:
                    out[dest_hex] = str(iface)
                else:
                    out[dest_hex] = ""   # unknown → shared instance fallback
            except Exception:
                out[dest_hex] = ""
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/network/transport")
def api_network_transport():
    """Return RNS transport layer info."""
    try:
        info = {
            "transport_enabled": RNS.Transport.transport_enabled() if hasattr(RNS.Transport, 'transport_enabled') else False,
            "path_table_size": len(RNS.Transport.destination_table) if hasattr(RNS.Transport, 'destination_table') else 0,
            "link_count": len(RNS.Transport.active_links) if hasattr(RNS.Transport, 'active_links') else 0,
        }
        # Destination table
        if hasattr(RNS.Transport, 'destination_table'):
            info["destination_count"] = len(RNS.Transport.destination_table)
        return jsonify(info)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Hosted Identity Page ──

@app.route("/api/profile", methods=["GET"])
def api_get_profile():
    """Get the hosted identity profile."""
    profile = {
        "display_name": node.db.get_config("display_name", "RetiMesh User"),
        "bio": node.db.get_config("profile_bio", ""),
        "lxmf_address": "",
    }
    if node.lxmf_dest:
        if hasattr(node.lxmf_dest, 'hexhash'):
            profile["lxmf_address"] = node.lxmf_dest.hexhash
        elif hasattr(node.lxmf_dest, 'hash'):
            profile["lxmf_address"] = RNS.hexrep(node.lxmf_dest.hash, delimit=False)
    return jsonify(profile)


@app.route("/api/profile", methods=["POST"])
def api_set_profile():
    """Update the hosted identity profile."""
    data = request.json or {}
    if "display_name" in data:
        node.db.set_config("display_name", data["display_name"])
    if "bio" in data:
        node.db.set_config("profile_bio", data["bio"])
    return jsonify({"status": "ok"})


# ── Mark messages read ──

@app.route("/api/peers/<peer_hash>/read", methods=["POST"])
def api_mark_read(peer_hash):
    """Mark all incoming messages from a peer as read."""
    try:                                    # S-6
        peer_hash = _validate_hash(peer_hash)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    node.db.mark_messages_read(peer_hash)
    return jsonify({"status": "ok"})


# ── Peer route / interface lookup ──

@app.route("/api/peers/<peer_hash>/route")
def api_peer_route(peer_hash):
    """Return which RNS interface would be used to reach this peer.

    Queries the RNS path table (Transport.destination_table) for the
    next-hop entry.  The table maps destination hash → [timestamp,
    next_hop_bytes, hops, expires, random_blobs, interface, announce_pkt]
    so we can read off the interface (index 5) and hops (index 2) directly.

    Returns:
        { "has_path": bool,
          "hops": int,
          "interface": "<name>",
          "interface_type": "<class>",
          "is_bluetooth": bool,
          "bt_mode": "BLE"|"RFCOMM"|null }
    """
    try:                                    # S-6
        peer_hash = _validate_hash(peer_hash)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    result = {
        "has_path": False,
        "hops": None,
        "interface": None,
        "interface_type": None,
        "is_bluetooth": False,
        "bt_mode": None,
    }
    try:
        dest_bytes = bytes.fromhex(peer_hash)

        # RNS stores paths in Transport.destination_table (NOT path_table).
        # Layout per entry (list):
        #   [0] timestamp   [1] next_hop_bytes   [2] hops
        #   [3] expires     [4] random_blobs     [5] interface  [6] announce_packet
        #
        # The old code read [1] as interface (got raw bytes → "type=bytes") and
        # [3] as hops (got the expiry Unix timestamp ≈ 1.78 billion).  Fixed below.
        path_entry = None
        dtable = getattr(RNS.Transport, 'destination_table', None)
        if dtable is not None:
            path_entry = dtable.get(dest_bytes)
            if not path_entry:
                # Prefix-match fallback (handles truncated hashes)
                for k, v in dtable.items():
                    n = min(len(k), len(dest_bytes))
                    if k[:n] == dest_bytes[:n]:
                        path_entry = v
                        break

        if path_entry:
            result["has_path"] = True
            try:
                if isinstance(path_entry, (list, tuple)) and len(path_entry) >= 6:
                    # Standard layout: index 5 = interface, index 2 = hops
                    iface = path_entry[5]
                    if iface is not None:
                        iface_type = type(iface).__name__
                        result["interface"]      = str(iface)
                        result["interface_type"] = iface_type
                        result["is_bluetooth"]   = (iface_type == "BluetoothInterface")
                        if result["is_bluetooth"]:
                            result["bt_mode"] = getattr(iface, 'config', {}).get("mode", "ble").upper()
                    result["hops"] = int(path_entry[2])
                    # Also expose next-hop as hex for debugging
                    via = path_entry[1]
                    if isinstance(via, (bytes, bytearray)) and via:
                        result["via"] = via.hex()
                elif isinstance(path_entry, dict):
                    # Named-key layout (future RNS or shared-instance RPC response)
                    iface = path_entry.get("interface")
                    if iface is not None:
                        iface_type = type(iface).__name__
                        result["interface"]      = str(iface)
                        result["interface_type"] = iface_type
                        result["is_bluetooth"]   = (iface_type == "BluetoothInterface")
                        if result["is_bluetooth"]:
                            result["bt_mode"] = getattr(iface, 'config', {}).get("mode", "ble").upper()
                    result["hops"] = path_entry.get("hops")
                elif isinstance(path_entry, (list, tuple)) and len(path_entry) >= 3:
                    # Minimal fallback: at least timestamp/via/hops present
                    result["hops"] = int(path_entry[2])
            except Exception:
                pass
    except Exception as e:
        result["error"] = str(e)
    return jsonify(result)


# ── Bookmarks API ──

@app.route("/api/bookmarks", methods=["GET"])
def api_get_bookmarks():
    return jsonify(node.db.get_bookmarks())

@app.route("/api/bookmarks", methods=["POST"])
def api_save_bookmark():
    data = request.json or {}
    node_hash = str(data.get("node_hash", "")).strip()
    path      = str(data.get("path", "/index")).strip()
    title     = str(data.get("title", "")).strip()[:200]
    if not node_hash:
        return jsonify({"error": "node_hash required"}), 400
    node.db.save_bookmark(node_hash, path, title)
    return jsonify({"status": "ok"})

@app.route("/api/bookmarks/<int:bookmark_id>", methods=["DELETE"])
def api_delete_bookmark(bookmark_id):
    node.db.delete_bookmark(bookmark_id)
    return jsonify({"status": "ok"})


# ── Saved Pages (offline copies) API ──

@app.route("/api/saved_pages", methods=["GET"])
def api_get_saved_pages():
    return jsonify(node.db.get_offline_pages())

@app.route("/api/saved_pages", methods=["POST"])
def api_save_page_offline():
    data         = request.json or {}
    node_hash    = str(data.get("node_hash", "")).strip()
    path         = str(data.get("path", "/index")).strip()
    title        = str(data.get("title", "")).strip()[:200]
    content      = str(data.get("content", ""))
    content_type = str(data.get("content_type", "text")).strip()
    if not node_hash:
        return jsonify({"error": "node_hash required"}), 400
    node.db.save_offline_page(node_hash, path, title, content, content_type)
    return jsonify({"status": "ok"})

@app.route("/api/saved_pages/<int:page_id>", methods=["GET"])
def api_get_saved_page(page_id):
    pg = node.db.get_offline_page(page_id)
    if not pg:
        return jsonify({"error": "Not found"}), 404
    return jsonify(pg)

@app.route("/api/saved_pages/<int:page_id>", methods=["DELETE"])
def api_delete_saved_page(page_id):
    node.db.delete_offline_page(page_id)
    return jsonify({"status": "ok"})


# ── Browse History API ──

@app.route("/api/history", methods=["GET"])
def api_get_history():
    limit = min(int(request.args.get("limit", 50)), 200)
    return jsonify(node.db.get_history(limit))

@app.route("/api/history", methods=["DELETE"])
def api_clear_history():
    node.db.clear_history()
    return jsonify({"status": "ok"})

@app.route("/api/history", methods=["POST"])
def api_add_history():
    data      = request.json or {}
    node_hash = str(data.get("node_hash", "")).strip()
    path      = str(data.get("path", "/index")).strip()
    title     = str(data.get("title", "")).strip()[:200]
    if not node_hash:
        return jsonify({"error": "node_hash required"}), 400
    node.db.add_history(node_hash, path, title)
    return jsonify({"status": "ok"})


# ─── Bluetooth Interface API ──────────────────────────────────────────────────

@app.route("/api/bluetooth", methods=["GET"])
def api_bluetooth_list():
    """Return saved Bluetooth interfaces + availability flag.

    Re-attempts the bleak/bless import on every call when it previously
    failed, so the "dependencies not installed" banner clears as soon as
    the user runs `pip install bleak bless` — no full restart required.
    """
    if not _BT_AVAILABLE:
        _try_load_bluetooth()
    interfaces = node.db.get_bluetooth_interfaces()
    for iface_row in interfaces:
        name  = iface_row["name"]
        entry = node._bt_interfaces.get(name)
        iface  = entry.get("iface") if entry else None
        iface_row["running"]     = entry is not None
        iface_row["connecting"]  = entry is not None and iface is None
        iface_row["retry_count"] = entry.get("retry_count", 0) if entry else 0
        iface_row["last_error"]  = entry.get("last_error") if entry else None
        iface_row["online"]      = bool(iface and getattr(iface, "online", False))
        if iface:
            iface_row["rxb"]      = getattr(iface, "rxb", 0)
            iface_row["txb"]      = getattr(iface, "txb", 0)
            try:
                iface_row["bt_peers"] = iface.peer_count()
            except Exception:
                iface_row["bt_peers"] = 0
            # Use the interface's heard-set (populated from announce packets)
            rns_peers = []
            try:
                heard = getattr(iface, '_heard_dest_hashes', set())
                if heard:
                    all_peers = {
                        p["dest_hash"]: p
                        for p in node.db.get_peers(
                            identity_hash=node.active_identity_hash or ""
                        )
                    }
                    for dest_hex in heard:
                        peer_row = all_peers.get(dest_hex)
                        rns_peers.append({
                            "dest_hash":    dest_hex,
                            "hops":         1,
                            "display_name": (
                                peer_row.get("display_name") or
                                peer_row.get("nickname") or
                                dest_hex[:16]
                            ) if peer_row else dest_hex[:16],
                        })
            except Exception:
                pass
            iface_row["rns_peers"] = rns_peers
        else:
            iface_row["rxb"] = iface_row["txb"] = iface_row["bt_peers"] = 0
            iface_row["rns_peers"] = []
    return jsonify({
        "available":  _BT_AVAILABLE,
        "load_error": _BT_LOAD_ERROR,
        "interfaces": interfaces,
    })


@app.route("/api/bluetooth", methods=["POST"])
def api_bluetooth_add():
    """Add and immediately start a new Bluetooth interface."""
    data = request.json or {}
    name = (data.get("name") or "BT0").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    if node.db.get_bluetooth_interface(name):
        return jsonify({"error": f"Interface '{name}' already exists"}), 409

    config = {
        "mode":          data.get("mode", "ble"),
        "discoverable":  bool(data.get("discoverable", True)),
        "scan_interval": int(data.get("scan_interval", 30)),
        "max_peers":     int(data.get("max_peers", 8)),
        "target_mtu":    int(data.get("target_mtu", 512)),
        "static_peers":  [
            p.strip() for p in str(data.get("static_peers", "")).split(",")
            if p.strip()
        ],
    }
    node.db.save_bluetooth_interface(
        name          = name,
        mode          = config["mode"],
        enabled       = True,
        discoverable  = config["discoverable"],
        scan_interval = config["scan_interval"],
        max_peers     = config["max_peers"],
        target_mtu    = config["target_mtu"],
        static_peers  = ",".join(config["static_peers"]),
    )
    if _BT_AVAILABLE:
        try:
            node._attach_bluetooth_interface(name, config)
        except Exception as e:
            log.error(f"BT attach error for '{name}': {e}")
            return jsonify({"status": "saved", "warning": str(e), "name": name}), 201

    return jsonify({"status": "ok", "name": name}), 201


@app.route("/api/bluetooth/<name>", methods=["DELETE"])
def api_bluetooth_delete(name):
    """Stop and remove a Bluetooth interface."""
    if not node.db.get_bluetooth_interface(name):
        return jsonify({"error": f"Interface '{name}' not found"}), 404
    node._detach_bluetooth_interface(name)
    node.db.delete_bluetooth_interface(name)
    return jsonify({"status": "ok"})


@app.route("/api/bluetooth/<name>/toggle", methods=["POST"])
def api_bluetooth_toggle(name):
    """Enable or disable a saved Bluetooth interface."""
    data    = request.json or {}
    enabled = bool(data.get("enabled", True))
    row     = node.db.get_bluetooth_interface(name)
    if not row:
        return jsonify({"error": f"Interface '{name}' not found"}), 404
    node.db.update_bluetooth_interface(name, enabled=int(enabled))
    if enabled and _BT_AVAILABLE and name not in node._bt_interfaces:
        config = {
            "mode":          row.get("mode", "ble"),
            "discoverable":  bool(row.get("discoverable", 1)),
            "scan_interval": int(row.get("scan_interval", 30)),
            "max_peers":     int(row.get("max_peers", 8)),
            "target_mtu":    int(row.get("target_mtu", 512)),
            "static_peers":  [
                p.strip() for p in row.get("static_peers", "").split(",")
                if p.strip()
            ],
        }
        try:
            node._attach_bluetooth_interface(name, config)
        except Exception as e:
            return jsonify({"status": "saved", "warning": str(e)}), 200
    elif not enabled and name in node._bt_interfaces:
        node._detach_bluetooth_interface(name)
    return jsonify({"status": "ok", "enabled": enabled})


@app.route("/api/bluetooth/status", methods=["GET"])
def api_bluetooth_status():
    """Live status of all running Bluetooth interfaces."""
    result = {}
    for name, entry in node._bt_interfaces.items():
        iface = entry.get("iface")
        if iface is None:
            # Interface is in retry/connecting state
            result[name] = {
                "online":       False,
                "connecting":   True,
                "retry_count":  entry.get("retry_count", 0),
                "last_error":   entry.get("last_error"),
                "mode":         entry.get("config", {}).get("mode", "ble"),
            }
        else:
            result[name] = {
                "online":       bool(getattr(iface, "online", False)),
                "connecting":   False,
                "rxb":          getattr(iface, "rxb", 0),
                "txb":          getattr(iface, "txb", 0),
                "mode":         entry["config"].get("mode", "ble"),
                "retry_count":  entry.get("retry_count", 0),
                "last_error":   entry.get("last_error"),
                "attach_ts":    entry.get("attach_ts"),
            }
    return jsonify({"available": _BT_AVAILABLE, "interfaces": result})


@app.route("/api/bluetooth/<name>/health", methods=["GET"])
def api_bluetooth_health(name):
    """Detailed health metrics for a single Bluetooth interface."""
    entry = node._bt_interfaces.get(name)
    row   = node.db.get_bluetooth_interface(name)
    if not row:
        return jsonify({"error": f"Interface '{name}' not found"}), 404

    if entry is None:
        return jsonify({
            "name":        name,
            "configured":  True,
            "running":     False,
            "online":      False,
            "connecting":  False,
            "retry_count": 0,
            "last_error":  None,
        })

    iface = entry.get("iface")
    if iface is None:
        return jsonify({
            "name":        name,
            "configured":  True,
            "running":     False,
            "online":      False,
            "connecting":  True,
            "retry_count": entry.get("retry_count", 0),
            "last_error":  entry.get("last_error"),
            "attach_ts":   entry.get("attach_ts"),
        })

    bt_peers = []
    try:
        heard = getattr(iface, "_heard_dest_hashes", set())
        all_peers = {
            p["dest_hash"]: p
            for p in node.db.get_peers(identity_hash=node.active_identity_hash or "")
        }
        for dest_hex in heard:
            peer_row = all_peers.get(dest_hex)
            bt_peers.append({
                "dest_hash":    dest_hex,
                "display_name": (peer_row.get("display_name") or dest_hex[:16]) if peer_row else dest_hex[:16],
                "last_seen":    peer_row.get("last_seen") if peer_row else None,
            })
    except Exception:
        pass

    return jsonify({
        "name":        name,
        "configured":  True,
        "running":     True,
        "online":      bool(getattr(iface, "online", False)),
        "connecting":  False,
        "rxb":         getattr(iface, "rxb", 0),
        "txb":         getattr(iface, "txb", 0),
        "mode":        entry["config"].get("mode", "ble"),
        "peer_count":  len(bt_peers),
        "peers":       bt_peers,
        "retry_count": entry.get("retry_count", 0),
        "last_error":  entry.get("last_error"),
        "attach_ts":   entry.get("attach_ts"),
        "uptime_s":    round(time.time() - entry["attach_ts"], 1) if entry.get("attach_ts") else None,
    })


@app.route("/api/bluetooth/scan", methods=["GET"])
def api_bluetooth_scan():
    """Perform a BLE device discovery scan (requires bleak).

    Returns nearby BLE devices with address, name, and RSSI.  The scan runs
    for ``timeout`` seconds (default 5, max 15).  This does NOT require a
    saved interface — it uses the host's default Bluetooth adapter directly.
    """
    if not _BT_AVAILABLE:
        return jsonify({
            "error":     "Bluetooth not available",
            "available": False,
            "devices":   [],
        }), 503

    timeout = min(float(request.args.get("timeout", 5)), 15.0)

    try:
        import asyncio
        try:
            from bleak import BleakScanner
        except ImportError:
            return jsonify({
                "error":   "bleak not installed. Run: pip install bleak",
                "devices": [],
            }), 503

        async def _scan():
            devices = await BleakScanner.discover(timeout=timeout, return_adv=True)
            result  = []
            for dev, adv in devices.values():
                result.append({
                    "address": dev.address,
                    "name":    dev.name or "",
                    "rssi":    adv.rssi,
                    "uuids":   list(adv.service_uuids),
                })
            return sorted(result, key=lambda d: d["rssi"], reverse=True)

        # Run async BLE scan in a real OS thread so it never blocks the gevent
        # event loop.  asyncio.new_event_loop().run_until_complete() is a
        # *synchronous* blocking call — if called directly on a gevent greenlet
        # it starves all other greenlets for the full scan duration (up to 15 s).
        # By wrapping it in a thread (which gevent monkey-patches to a real OS
        # thread), cooperative scheduling is maintained.
        import threading

        _result   = [None]
        _exc      = [None]

        def _thread_target():
            try:
                loop = asyncio.new_event_loop()
                try:
                    _result[0] = loop.run_until_complete(_scan())
                finally:
                    loop.close()
            except Exception as e:
                _exc[0] = e

        t = threading.Thread(target=_thread_target, daemon=True)
        t.start()
        # gevent.sleep yields cooperatively while waiting for the thread
        deadline = time.time() + timeout + 2.0  # small grace over BLE timeout
        while t.is_alive() and time.time() < deadline:
            if _GEVENT_AVAILABLE:
                import gevent as _gevent
                _gevent.sleep(0.1)
            else:
                t.join(timeout=0.1)
        t.join(timeout=1.0)

        if _exc[0] is not None:
            raise _exc[0]

        devices = _result[0] or []
        return jsonify({"available": True, "devices": devices, "scan_duration_s": timeout})

    except Exception as e:
        log.error(f"BLE scan failed: {e}")
        return jsonify({"error": str(e), "devices": []}), 500


# ── Transport toggle ──

@app.route("/api/transport", methods=["GET"])
def api_get_transport():
    """Return current transport-enabled state from RNS config."""
    enabled = False
    try:
        if hasattr(RNS.Transport, 'transport_enabled'):
            enabled = bool(RNS.Transport.transport_enabled())
    except Exception:
        pass
    # Also read from config file as the authoritative value
    import configparser
    cfg_path = os.path.join(
        node.config_dir if node.config_dir else os.path.expanduser("~/.reticulum"),
        "config"
    )
    cfg_enabled = None
    if os.path.exists(cfg_path):
        try:
            cp = configparser.ConfigParser()
            cp.read(cfg_path)
            val = cp.get("reticulum", "enable_transport", fallback=None)
            if val is not None:
                cfg_enabled = val.strip().lower() in ("yes", "true", "1")
        except Exception:
            pass
    return jsonify({"transport_enabled": enabled, "config_value": cfg_enabled})

@app.route("/api/transport", methods=["POST"])
def api_set_transport():
    """Toggle Reticulum transport in the config file. Restart required."""
    data    = request.json or {}
    enabled = bool(data.get("enabled", False))

    import configparser
    cfg_dir  = node.config_dir if node.config_dir else os.path.expanduser("~/.reticulum")
    cfg_path = os.path.join(cfg_dir, "config")

    try:
        cp = configparser.ConfigParser()
        if os.path.exists(cfg_path):
            cp.read(cfg_path)
        if not cp.has_section("reticulum"):
            cp.add_section("reticulum")
        cp.set("reticulum", "enable_transport", "Yes" if enabled else "No")
        os.makedirs(cfg_dir, exist_ok=True)
        with open(cfg_path, "w") as f:
            cp.write(f)
        return jsonify({"status": "ok", "enabled": enabled,
                        "message": "Restart required to apply."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Pages API ──

@app.route("/api/pages", methods=["GET"])
def api_get_pages():
    """Return all hosted pages."""
    return jsonify(node.db.get_pages())

@app.route("/api/pages", methods=["POST"])
def api_create_page():
    data = request.json or {}
    title   = str(data.get("title",   "Untitled"))[:200]
    path    = str(data.get("path",    "/index"))[:200]
    content = str(data.get("content", ""))[:65536]
    ctype   = str(data.get("content_type", "text"))[:20]
    if not re.match(r'^/[a-zA-Z0-9_\-./]*$', path):
        return jsonify({"error": "Invalid path: use only letters, numbers, hyphens, underscores, dots, and slashes"}), 400
    page_id = node.db.save_page(title, path, content, ctype)
    return jsonify({"status": "ok", "id": page_id})

@app.route("/api/pages/<int:page_id>", methods=["GET"])
def api_get_page(page_id):
    page = node.db.get_page(page_id)
    if not page:
        return jsonify({"error": "Not found"}), 404
    return jsonify(page)

@app.route("/api/pages/<int:page_id>", methods=["PUT"])
def api_update_page(page_id):
    data = request.json or {}
    title   = str(data["title"])[:200]    if "title"        in data else None
    path    = str(data["path"])[:200]     if "path"         in data else None
    content = str(data["content"])[:65536] if "content"     in data else None
    ctype   = str(data["content_type"])[:20] if "content_type" in data else None
    pub     = data.get("is_published")
    if path and not re.match(r'^/[a-zA-Z0-9_\-./]*$', path):
        return jsonify({"error": "Invalid path"}), 400
    node.db.update_page(page_id, title, path, content, pub, ctype)
    return jsonify({"status": "ok"})

@app.route("/api/pages/<int:page_id>", methods=["DELETE"])
def api_delete_page(page_id):
    node.db.delete_page(page_id)
    return jsonify({"status": "ok"})

@app.route("/api/pages/browse", methods=["GET"])
def api_browse_page():
    """Fetch a page from a remote node."""
    dest_hash = request.args.get("hash", "").strip().lower()
    path      = request.args.get("path", "/index").strip()
    if not dest_hash or len(dest_hash) < 16:
        return jsonify({"error": "Invalid destination hash"}), 400
    if not path.startswith("/"):
        path = "/" + path
    result = node.fetch_remote_page(dest_hash, path)
    return jsonify(result)

@app.route("/api/pages/nodes", methods=["GET"])
def api_nomadnet_nodes():
    """Return discovered NomadNet / page-hosting nodes."""
    nodes = list(getattr(node, '_nomadnet_nodes', {}).values())
    # Filter to nodes seen in last 24 hours
    cutoff = time.time() - 86400
    nodes  = [n for n in nodes if n.get("last_seen", 0) > cutoff]
    return jsonify(nodes)


@app.route("/profile")
def serve_profile_page():
    """Serve the public profile page — accessible by others on LAN."""
    name = node.db.get_config("display_name", "RetiMesh User")
    bio = node.db.get_config("profile_bio", "")
    lxmf = ""
    if node.lxmf_dest:
        if hasattr(node.lxmf_dest, 'hexhash'):
            lxmf = node.lxmf_dest.hexhash
        elif hasattr(node.lxmf_dest, 'hash'):
            lxmf = RNS.hexrep(node.lxmf_dest.hash, delimit=False)
    # S-2: escape all user-controlled values to prevent stored XSS
    safe_name = _html_escape(name)
    safe_bio  = _html_escape(bio) if bio else "A RetiMesh user on the Reticulum network."
    safe_lxmf = _html_escape(lxmf)
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{safe_name} — RetiMesh</title>
<style>
body{{background:#0d1117;color:#c9d1d9;font-family:-apple-system,sans-serif;display:flex;justify-content:center;padding:40px 16px;}}
.card{{max-width:400px;text-align:center;}}
.icon{{font-size:48px;margin-bottom:12px;color:#3fb950;}}
h1{{font-size:22px;margin:0 0 4px;}}
.bio{{color:#8b949e;font-size:14px;margin:12px 0 20px;line-height:1.5;}}
.addr{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px;font-family:monospace;font-size:11px;color:#8b949e;word-break:break-all;}}
.label{{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:#484f58;margin-bottom:4px;}}
</style></head><body>
<div class="card">
<div class="icon">⬡</div>
<h1>{safe_name}</h1>
<div class="bio">{safe_bio}</div>
<div class="label">LXMF Address</div>
<div class="addr">{safe_lxmf}</div>
</div></body></html>"""


# ── WebSocket endpoint (replaces SSE /api/events + POST /api/ws_send) ─────
#
# The /ws endpoint is served by a WSGI middleware below — it must bypass
# Flask's request dispatch because Flask 3.x consumes the request body
# during routing, which breaks the geventwebsocket handshake.

def _ws_handler(ws):
    """Run the WebSocket receive loop for a single browser connection.
    The socket has already been upgraded by geventwebsocket's handler.
    """
    node.register_ws(ws)
    log.info("WS client connected")

    # Send initial state immediately on connect
    try:
        info = node.get_identity_info()
        ws.send(json.dumps({"type": "identity", "data": info}))
    except Exception as e:
        log.error(f"WS: failed to send identity: {e}")
    try:
        peers = node.db.get_peers(identity_hash=node.active_identity_hash)
        ws.send(json.dumps({"type": "peer_update", "peers": peers}))
    except Exception as e:
        log.error(f"WS: failed to send peers: {e}")
    try:
        codecs_info = {name: c.info() for name, c in CODECS.items()}
        ws.send(json.dumps({"type": "codecs", "data": codecs_info}))
    except Exception as e:
        log.error(f"WS: failed to send codecs: {e}")

    try:
        while not ws.closed:
            raw = ws.receive()
            if raw is None:
                break

            # ── Binary frame (audio) ──────────────────────────────────────
            if isinstance(raw, (bytes, bytearray)):
                if len(raw) < 4 or raw[0] != 0xAA:
                    continue
                # [0xAA][seq_hi][seq_lo][codec_id][encoded_audio]
                codec_id_byte = raw[3]
                audio_bytes   = bytes(raw[4:])
                # Route to whichever call is currently active.  The browser
                # UI only supports one call at a time so we forward to the
                # first active link.  (The old JSON path used peer_hash
                # explicitly; this is equivalent since state.callPeer is
                # always the active peer.)
                peer = next(iter(node.active_links.keys()), None)
                if peer and audio_bytes:
                    if not hasattr(node, '_audio_logged'):
                        node._audio_logged = True
                        log.info(f"First audio frame received via WS. peer={peer[:16]}")
                    node.send_call_audio(peer, audio_bytes, codec_id=codec_id_byte)
                continue

            # ── Text frame (JSON signaling) ───────────────────────────────
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if not isinstance(msg, dict):
                continue

            t = msg.get("type", "")

            # Handlers called synchronously on the WS read loop (matches build 4's
            # pattern).  The earlier greenlet-spawning version was broken: LXMF's
            # internal routing expects to run in the same greenlet that queued the
            # message, and spawning moves it to a different hub context, causing
            # messages to appear "sent" on our side but never actually transmit.
            try:
                if t == "chat_send":
                    peer_hash = msg.get("peer_hash", "")
                    content   = msg.get("content", "")
                    method    = msg.get("method", "auto")  # 'auto', 'direct', 'propagated'
                    if peer_hash and content:
                        try:
                            result = node.send_message(peer_hash, content, method=method)
                            ws.send(json.dumps({"type": "chat_sent", "result": result, "peer_hash": peer_hash}))
                        except Exception as ex:
                            log.error(f"chat_send failed: {ex}")
                            import traceback; traceback.print_exc()
                            ws.send(json.dumps({"type": "chat_sent",
                                                "result": {"error": str(ex)},
                                                "peer_hash": peer_hash}))

                elif t == "file_send":
                    peer_hash = msg.get("peer_hash", "")
                    filename  = msg.get("filename", "")
                    data      = msg.get("data", "")
                    if peer_hash and filename and data:
                        ws.send(json.dumps({"type": "file_progress", "status": "sending", "filename": filename}))
                        try:
                            result = node.send_file(peer_hash, filename, data)
                            if "error" in result:
                                ws.send(json.dumps({"type": "file_progress", "status": "error",
                                                    "filename": filename, "error": result["error"]}))
                            else:
                                ws.send(json.dumps({"type": "file_progress", "status": "done", "filename": filename}))
                            ws.send(json.dumps({"type": "file_sent", "result": result}))
                        except Exception as ex:
                            log.error(f"file_send failed: {ex}")
                            ws.send(json.dumps({"type": "file_progress", "status": "error",
                                                "filename": filename, "error": str(ex)}))

                elif t == "call_start":
                    peer_hash = msg.get("peer_hash", "")
                    if peer_hash:
                        try:
                            result = node.start_call(peer_hash, msg.get("codec", "mu_law"))
                            ws.send(json.dumps({"type": "call_status", "result": result, "peer_hash": peer_hash}))
                        except Exception as ex:
                            log.error(f"call_start failed: {ex}")
                            ws.send(json.dumps({"type": "call_status",
                                                "result": {"error": str(ex)}, "peer_hash": peer_hash}))

                elif t == "call_accept":
                    peer_hash = msg.get("peer_hash", "")
                    if peer_hash:
                        result = node.accept_call(peer_hash)
                        node._ws_broadcast({"type": "call_status", "result": result})

                elif t == "call_end":
                    peer_hash = msg.get("peer_hash", "")
                    if peer_hash:
                        result = node.end_call(peer_hash)
                        node._ws_broadcast({"type": "call_status", "result": result})

                elif t == "announce":
                    node.send_announce(msg.get("display_name"))

                elif t == "codec_compare":
                    pcm_data = msg.get("pcm_data", "")
                    if pcm_data:
                        try:
                            results = node.compare_codecs(pcm_data)
                            ws.send(json.dumps({"type": "codec_compare_result", "data": results}))
                        except Exception as ex:
                            log.error(f"codec_compare failed: {ex}")

                elif t == "ping":
                    ws.send(json.dumps({"type": "pong"}))

            except Exception as ex:
                # Don't let a malformed message kill the entire WS loop
                log.error(f"WS handler error for type '{t}': {ex}")

    except Exception as e:
        log.debug(f"WS loop exit: {e}")
    finally:
        node.unregister_ws(ws)
        log.info("WS client disconnected")


class _WebSocketMiddleware:
    """WSGI middleware that intercepts /ws requests before Flask sees them.

    Flask 3.x consumes the request body during routing, which breaks the
    WebSocket upgrade handshake.  This middleware grabs the already-upgraded
    WebSocket object (injected by geventwebsocket's WebSocketHandler) and
    hands it to our handler without invoking Flask.
    """
    def __init__(self, flask_app):
        self.flask_app = flask_app

    def __call__(self, environ, start_response):
        if environ.get("PATH_INFO") == "/ws":
            ws = environ.get("wsgi.websocket")
            if ws is None:
                start_response("400 Bad Request", [("Content-Type", "text/plain")])
                return [b"WebSocket upgrade required"]
            _ws_handler(ws)
            return []
        return self.flask_app(environ, start_response)


# ─── Emergency Alerts API ─────────────────────────────────────────────────────

@app.route("/api/alerts", methods=["GET"])
def api_get_alerts():
    """Get all alerts (received + sent) plus unread count."""
    alerts = node.db.get_alerts(limit=200)
    unread = node.db.get_unread_alert_count()
    return jsonify({"alerts": alerts, "unread_count": unread})


@app.route("/api/alerts", methods=["POST"])
def api_send_alert():
    """Send an emergency broadcast alert to the mesh."""
    data     = request.json or {}
    severity = int(data.get("severity", 0))
    title    = str(data.get("title", "")).strip()
    message  = str(data.get("message", "")).strip()

    if not title:
        return jsonify({"error": "title is required"}), 400
    if severity not in (0, 1, 2, 3):
        return jsonify({"error": "severity must be 0 (info), 1 (warning), 2 (critical) or 3 (sos)"}), 400

    result = node.send_alert(severity, title, message)
    if "error" in result:
        return jsonify(result), 500
    return jsonify(result), 201


@app.route("/api/alerts/<int:alert_id>/read", methods=["POST"])
def api_alert_mark_read(alert_id):
    """Mark a single alert as acknowledged/read."""
    node.db.mark_alert_read(alert_id)
    return jsonify({"status": "ok"})


@app.route("/api/alerts/read_all", methods=["POST"])
def api_alerts_read_all():
    """Mark every alert as read."""
    node.db.mark_all_alerts_read()
    return jsonify({"status": "ok"})


@app.route("/api/alerts/<int:alert_id>", methods=["DELETE"])
def api_delete_alert(alert_id):
    """Delete a single alert record."""
    node.db.delete_alert(alert_id)
    return jsonify({"status": "ok"})


@app.route("/api/alerts/settings", methods=["GET"])
def api_alerts_settings_get():
    """Return whether the alerts feature is enabled for this node."""
    enabled = node.db.get_config("alerts_enabled", "true") == "true"
    return jsonify({"enabled": enabled})


@app.route("/api/alerts/settings", methods=["POST"])
def api_alerts_settings_set():
    """Enable or disable the emergency alerts feature."""
    data    = request.json or {}
    enabled = bool(data.get("enabled", True))
    node.db.set_config("alerts_enabled", "true" if enabled else "false")
    return jsonify({"status": "ok", "enabled": enabled})


# ─── Group Chat API ───────────────────────────────────────────────────────────

@app.route("/api/groups", methods=["GET"])
def api_get_groups():
    """List all groups with unread counts."""
    groups  = node.db.get_groups()
    unread  = node.db.get_unread_group_counts()
    invites = node.db.get_group_invites()
    for g in groups:
        g["unread"] = unread.get(g["group_id"], 0)
    return jsonify({"groups": groups, "invites": invites})


@app.route("/api/groups", methods=["POST"])
def api_create_group():
    """Create a new private group or open channel."""
    data    = request.json or {}
    name    = str(data.get("name", "")).strip()
    gtype   = str(data.get("type", "private"))
    members = data.get("members", [])
    if not name:
        return jsonify({"error": "name is required"}), 400
    if gtype not in ("private", "channel"):
        return jsonify({"error": "type must be 'private' or 'channel'"}), 400
    # Fix J: validate every member hash before passing to create_group
    validated_members = []
    for raw_h in (members if isinstance(members, list) else []):
        try:
            validated_members.append(_validate_hash(str(raw_h)))
        except ValueError as e:
            return jsonify({"error": f"Invalid member hash: {e}"}), 400
    result = node.create_group(name, gtype, validated_members)
    if "error" in result:
        return jsonify(result), 400
    return jsonify(result), 201


@app.route("/api/groups/join", methods=["POST"])
def api_join_channel():
    """Join an open channel by name (creates if not exists)."""
    data = request.json or {}
    name = str(data.get("name", "")).strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    if len(name) > 64:
        return jsonify({"error": "Channel name too long (max 64 characters)"}), 400
    result = node.join_channel(name)
    return jsonify(result)


@app.route("/api/groups/<group_id>", methods=["GET"])
def api_get_group(group_id):
    """Get group info."""
    group = node.db.get_group(group_id)
    if not group:
        return jsonify({"error": "Group not found"}), 404
    return jsonify(group)


@app.route("/api/groups/<group_id>", methods=["DELETE"])
def api_leave_group(group_id):
    """Leave (and delete locally) a group."""
    result = node.leave_group(group_id)
    if "error" in result:
        return jsonify(result), 404
    return jsonify(result)


@app.route("/api/groups/<group_id>/messages", methods=["GET"])
def api_get_group_messages(group_id):
    """Get messages for a group."""
    limit = int(request.args.get("limit", 100))
    msgs  = node.db.get_group_messages(group_id, limit=limit)
    return jsonify({"messages": msgs})


@app.route("/api/groups/<group_id>/messages", methods=["POST"])
def api_send_group_message(group_id):
    """Send a message to a group."""
    data    = request.json or {}
    content = str(data.get("content", "")).strip()
    if not content:
        return jsonify({"error": "content is required"}), 400
    # Fix L: enforce content length limit on group messages
    if len(content) > 10240:
        return jsonify({"error": "content too long (max 10240 chars)"}), 400
    result = node.send_group_message(group_id, content)
    if "error" in result:
        return jsonify(result), 404
    return jsonify(result), 201


@app.route("/api/groups/<group_id>/read", methods=["POST"])
def api_group_mark_read(group_id):
    """Mark all messages in a group as read."""
    node.db.mark_group_messages_read(group_id)
    return jsonify({"status": "ok"})


@app.route("/api/groups/<group_id>/rename", methods=["POST"])
def api_rename_group(group_id):
    """Rename a group. Only the owner can rename; notifies all members via LXMF."""
    data = request.json or {}
    new_name = str(data.get("name", "")).strip()
    if not new_name:
        return jsonify({"error": "name is required"}), 400
    result = node.rename_group(group_id, new_name)
    if "error" in result:
        return jsonify(result), 400
    return jsonify(result)


@app.route("/api/groups/<group_id>/members", methods=["POST"])
def api_add_group_member(group_id):
    """Add a member to a group."""
    data = request.json or {}
    h    = str(data.get("hash", "")).strip()
    if not h:
        return jsonify({"error": "hash is required"}), 400
    # Fix K: validate the hash before passing it to the node
    try:
        h = _validate_hash(h)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    result = node.add_group_member(group_id, h)
    if "error" in result:
        return jsonify(result), 404
    return jsonify(result)


@app.route("/api/groups/<group_id>/members/<member_hash>", methods=["DELETE"])
def api_remove_group_member(group_id, member_hash):
    """Remove (kick) a member from a group, notifying all parties."""
    try:
        member_hash = _validate_hash(member_hash)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    result = node.kick_group_member(group_id, member_hash)
    if "error" in result:
        return jsonify(result), 400
    return jsonify(result)


@app.route("/api/groups/invites", methods=["GET"])
def api_get_group_invites():
    """Get pending group invites."""
    return jsonify({"invites": node.db.get_group_invites()})


@app.route("/api/groups/invites/<group_id>/accept", methods=["POST"])
def api_accept_group_invite(group_id):
    """Accept a group invite."""
    result = node.accept_group_invite(group_id)
    if "error" in result:
        return jsonify(result), 404
    return jsonify(result)


@app.route("/api/groups/invites/<group_id>/decline", methods=["POST"])
def api_decline_group_invite(group_id):
    """Decline a group invite."""
    result = node.decline_group_invite(group_id)
    return jsonify(result)


# ─── Entry Point ──────────────────────────────────────────────────────────────

def main():
    global node

    parser = argparse.ArgumentParser(description=f"{APP_NAME} v{VERSION}")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Web server bind address")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Web server port")
    parser.add_argument("--storage",
                        default=str(Path.home() / ".retimesh" / "data"),
                        help="Storage directory (default: ~/.retimesh/data)")
    parser.add_argument("--rns-config", default=None, help="Reticulum config directory")
    parser.add_argument("--headless", action="store_true", help="Don't open browser")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity for RetiMesh's own logger (default: INFO). "
             "DEBUG also surfaces send_message decisions, propagation "
             "fallback, etc.  For RNS-level debug, use --debug-rns.",
    )
    parser.add_argument(
        "--debug-rns",
        action="store_true",
        help=(
            "Enable RNS DEBUG logging and wrap validate_proof() to trace "
            "BLE link-handshake failures.  Use when links time out despite "
            "PROOF packets arriving.  Produces verbose output."
        ),
    )
    args = parser.parse_args()

    # Apply log level chosen on the command line.  basicConfig already ran
    # at module import (level INFO), so we override on the root logger and
    # on our named logger to make sure the change actually takes effect.
    _level = getattr(logging, args.log_level, logging.INFO)
    logging.getLogger().setLevel(_level)
    log.setLevel(_level)

    # ── RNS debug mode ─────────────────────────────────────────────────────────
    # Must be installed BEFORE Reticulum is started so the patch is in place
    # when the first PROOF arrives.
    if args.debug_rns:
        _install_rns_link_debug()
        log.info("RNS link-proof debug patch installed")

    # Initialize codecs
    init_codecs()
    # (Available-codecs listing removed from startup — internal detail.
    # Users only see this if they bump the log level to debug.)
    log.debug(f"Available codecs: {list(CODECS.keys())}")

    # Start mesh node
    node = MeshNode(args.storage, config_dir=args.rns_config)
    node.start()

    # Force RNS DEBUG log level after node start (overrides config-file loglevel)
    if args.debug_rns:
        RNS.loglevel = RNS.LOG_DEBUG
        log.info("RNS loglevel forced to DEBUG")
        log.info(
            "To enable DEBUG logging on the Ubuntu peer as well, edit "
            "~/.reticulum/config and set:  loglevel = 7"
        )

    log.info(f"{APP_NAME} v{VERSION} running at http://{args.host}:{args.port}")

    if not args.headless:
        import webbrowser
        # When bound to a wildcard address (0.0.0.0 / :: / ""), the host is
        # only valid for *listening*, not for connecting.  Some browsers will
        # try to navigate to literally "http://0.0.0.0:5000" and fail (or, on
        # Chrome 117+, refuse outright).  Map wildcards back to loopback so
        # the local browser still opens to a working URL while the server
        # remains reachable on the LAN.
        _browser_host = args.host
        if _browser_host in ("0.0.0.0", "::", "", "*"):
            _browser_host = "127.0.0.1"
        threading.Timer(1.5, lambda: webbrowser.open(f"http://{_browser_host}:{args.port}")).start()

    if not _GEVENT_AVAILABLE:
        log.critical("gevent is required but not installed. Exiting.")
        sys.exit(1)

    from gevent import pywsgi
    try:
        from geventwebsocket.handler import WebSocketHandler
    except ImportError:
        log.critical("gevent-websocket is required. Install with: pip install gevent-websocket")
        sys.exit(1)

    # WebSocket transport: /ws handles both directions (browser<->server).
    # Binary audio + JSON signaling ride the same socket.  The middleware
    # intercepts /ws before Flask routing so the handshake succeeds.
    wsgi_app = _WebSocketMiddleware(app)

    # Port-bind retry loop: on Windows restarts the previous process's
    # listening socket may still be lingering in TIME_WAIT for up to 2
    # seconds even after we close() it.  Try a few times with backoff
    # before giving up rather than crashing with WinError 10048.
    global _wsgi_server
    _wsgi_server = None
    _bind_attempts = 10
    for _attempt in range(_bind_attempts):
        try:
            _wsgi_server = pywsgi.WSGIServer(
                (args.host, args.port),
                wsgi_app,
                handler_class=WebSocketHandler,
                log=None,
            )
            _wsgi_server.init_socket()   # force bind() now so we can catch errors
            break
        except OSError as e:
            # WinError 10048 == EADDRINUSE on Windows; retry after short wait
            if _attempt == _bind_attempts - 1:
                log.critical(f"Could not bind {args.host}:{args.port} after "
                             f"{_bind_attempts} attempts: {e}")
                sys.exit(1)
            log.warning(f"Port {args.port} busy ({e}); retrying in 0.5s "
                        f"(attempt {_attempt + 1}/{_bind_attempts})")
            time.sleep(0.5)

    log.info(f"gevent WSGIServer listening on {args.host}:{args.port} (WebSocket)")

    # Windows Ctrl+C handling: gevent's hub runs in a C extension that
    # doesn't always wake from select() when Python's default SIGINT
    # handler tries to raise KeyboardInterrupt inside the greenlet.  The
    # symptom on Windows is that pressing Ctrl+C in PowerShell may not
    # stop the app; you need to press Ctrl+Break (which delivers SIGBREAK
    # at a level that bypasses the Python signal queue).
    #
    # We install our own handler on both signals that calls os._exit(0)
    # to force-exit immediately, skipping Python's at-exit cleanup.  This
    # is the simplest approach that's known to work; more aggressive
    # ctypes-based handlers turned out to interfere with normal startup.
    import signal as _signal
    def _sigint_handler(signum, frame):
        log.info("Ctrl+C received, shutting down...")
        try:
            if _wsgi_server is not None:
                _wsgi_server.stop(timeout=1)
        except Exception:
            pass
        os._exit(0)
    try:
        _signal.signal(_signal.SIGINT, _sigint_handler)
        if hasattr(_signal, "SIGBREAK"):  # Windows Ctrl+Break
            _signal.signal(_signal.SIGBREAK, _sigint_handler)
    except (ValueError, OSError) as _e:
        # Some environments (e.g. inside a thread) reject signal registration
        log.debug(f"Could not install SIGINT handler: {_e}")

    _wsgi_server.serve_forever()


if __name__ == "__main__":
    main()
