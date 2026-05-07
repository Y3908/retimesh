/**
 * RetiMesh Frontend — Vanilla JS
 * Messaging, file transfer, audio calls over RNS Link
 */
(function () {
    "use strict";
    const state = {
        ws: null, identity: null, peers: [], activePeer: null, messages: {}, codecs: {},
        callActive: false, callPeer: null, callTimer: null, callSeconds: 0, callDirection: null, callAccepted: false,
        audioContext: null, micStream: null, scriptProcessor: null, _nextPlayTime: 0,
        _audioSendCounter: 0,
        bookmarks: [], currentBrowseHash: null, currentBrowsePath: null,
        currentPageContent: null, currentPageTitle: null, currentPageContentType: null,
    };
    const SAMPLE_RATE = 8000;      // 8 kHz narrowband — matches mu-law codec
    const AUDIO_BUFFER_SIZE = 256; // samples per capture frame (32 ms at 8 kHz)

    // ─── Voice Activity Detection ───────────────────────────────────────────────
    // Only transmit frames that contain speech; skip silence to save bandwidth.
    const VAD_THRESHOLD    = 0.006;   // RMS amplitude threshold (tune if needed)
    const VAD_HANGOVER_MAX = 12;      // keep sending for N frames after last speech
    let _vadHangover = 0;

    function _hasVoice(pcmFloat32) {
        let sum = 0;
        for (let i = 0; i < pcmFloat32.length; i++) sum += pcmFloat32[i] * pcmFloat32[i];
        return Math.sqrt(sum / pcmFloat32.length) > VAD_THRESHOLD;
    }

    // ─── Adaptive Jitter Buffer ────────────────────────────────────────────────
    // Measures actual packet-arrival variance and sizes the play-out delay
    // accordingly.  Quiet network → small delay (low latency).  Noisy network →
    // larger delay (fewer glitches).  This is what makes quality consistent
    // instead of "great one call, choppy the next".
    const JitterBuffer = (function () {
        // Delay bounds (seconds).  Target will float between these based on
        // observed jitter.  Floor stays under the perceptual-delay threshold;
        // ceiling is high enough to absorb LoRa / congested-network spikes.
        const MIN_DELAY_S  = 0.020;   // 20 ms absolute floor
        const MAX_DELAY_S  = 0.150;   // 150 ms absolute ceiling
        const DEDUP_SIZE   = 512;
        // Smoothing factors for exponentially-weighted moving averages
        const EMA_MEAN   = 0.10;
        const EMA_JITTER = 0.15;

        let _audioCtx  = null;
        let _playAt    = 0;
        let _started   = false;
        let _lastArrival = 0;
        let _meanGap   = 0.040;  // initial guess: 40 ms frame cadence
        let _jitter    = 0.010;  // initial guess: 10 ms variance
        let _targetDelay = 0.040;
        // P-4: sliding-window duplicate suppression.
        //
        // The old approach used a flat Set and called .clear() when it hit DEDUP_SIZE.
        // Clearing discards the ENTIRE dedup window, so packets that arrive just
        // after the clear are silently accepted even if we already played them —
        // causing audible double-frames.
        //
        // The fix: treat `_seenSeqs` as a circular buffer of the last DEDUP_SIZE
        // sequence numbers.  When full, we evict the oldest entry before adding a
        // new one, so the window always covers a contiguous recent range.
        const _seenSeqs   = new Set();     // membership check: O(1)
        const _seenOrder  = [];            // insertion-order queue for eviction

        function _seenAdd(seq) {
            if (_seenSeqs.has(seq)) return false;          // duplicate
            if (_seenOrder.length >= DEDUP_SIZE) {
                _seenSeqs.delete(_seenOrder.shift());      // evict oldest
            }
            _seenSeqs.add(seq);
            _seenOrder.push(seq);
            return true;   // new packet
        }

        function init(audioCtx) {
            _audioCtx = audioCtx;
            _playAt = 0;
            _started = false;
            _lastArrival = 0;
            _meanGap = 0.040;
            _jitter = 0.010;
            _targetDelay = 0.040;
            _seenSeqs.clear();
            _seenOrder.length = 0;
        }

        // Update rolling arrival-jitter estimate and derive a target play-out delay.
        function _updateJitter(now) {
            if (_lastArrival > 0) {
                const gap = now - _lastArrival;
                // EMA of inter-arrival gap
                _meanGap = (1 - EMA_MEAN) * _meanGap + EMA_MEAN * gap;
                // EMA of |gap - meanGap| (mean absolute deviation — cheap proxy for jitter)
                const dev = Math.abs(gap - _meanGap);
                _jitter = (1 - EMA_JITTER) * _jitter + EMA_JITTER * dev;
                // Target delay = 2×jitter + one frame of headroom, clamped to bounds
                const candidate = 2 * _jitter + _meanGap * 0.5;
                _targetDelay = Math.max(MIN_DELAY_S, Math.min(MAX_DELAY_S, candidate));
            }
            _lastArrival = now;
        }

        // push() accepts an already-decoded Float32Array.
        function push(seq, pcmFloat32) {
            if (!_audioCtx || _audioCtx.state === "closed") return;
            if (_audioCtx.state === "suspended") _audioCtx.resume().catch(() => {});

            // P-4: sliding-window duplicate suppression (no more .clear() mass eviction)
            if (!_seenAdd(seq)) return;

            const now = _audioCtx.currentTime;
            _updateJitter(now);

            const pcm = pcmFloat32 instanceof Float32Array ? pcmFloat32 : new Float32Array(pcmFloat32);
            const buf = _audioCtx.createBuffer(1, pcm.length, _audioCtx.sampleRate);
            buf.getChannelData(0).set(pcm);
            const src = _audioCtx.createBufferSource();
            src.buffer = buf;
            src.connect(_audioCtx.destination);

            // Re-prime on first frame or if play-head has fallen behind
            if (!_started || _playAt < now) {
                _playAt = now + _targetDelay;
                _started = true;
            }
            // Soft cap: if the forward buffer is more than 2× the target delay,
            // glide back toward target by shortening this frame's schedule.
            // "Glide" (not snap) avoids the audible click a hard reset causes.
            const maxAhead = _targetDelay * 2 + 0.040;
            if (_playAt > now + maxAhead) {
                _playAt = now + _targetDelay;
            }

            src.start(_playAt);
            _playAt += buf.duration;
        }

        function reset() {
            _started = false;
            _playAt = 0;
            _lastArrival = 0;
            _seenSeqs.clear();
        }

        return { init, push, reset };
    })();

    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);
    const DOM = {
        myHash: $("#my-hash"), peerList: $("#peer-list"), peerCount: $("#peer-count"),
        welcomeScreen: $("#welcome-screen"), chatPanel: $("#chat-panel"),
        chatPeerName: $("#chat-peer-name"), chatPeerHash: $("#chat-peer-hash"),
        messagesList: $("#messages-list"), messagesContainer: $("#messages-container"),
        composeInput: $("#compose-input"), btnSend: $("#btn-send"),
        btnAnnounce: $("#btn-announce"),
        btnAttach: $("#btn-attach"), fileInput: $("#file-input"),
        btnCall: $("#btn-call"),
        callOverlay: $("#call-overlay"), callStatus: $("#call-status"),
        callPeerName: $("#call-peer-name"), callTimer: $("#call-timer"),
        callCodecInfo: $("#call-codec-info"), btnCallEnd: $("#btn-call-end"),
        btnCallAccept: $("#btn-call-accept"),
        codecModal: $("#codec-modal"), btnCodecClose: $("#btn-codec-close"),
        codecChart: $("#codec-chart"), statusDot: $("#status-dot"), statusText: $("#status-text"),
        // Identity management
        btnIdentityManage: $("#btn-identity-manage"),
        identityModal: $("#identity-modal"), btnIdentityClose: $("#btn-identity-close"),
        identityNewName: $("#identity-new-name"), btnCreateIdentity: $("#btn-create-identity"),
        btnIdentityRestart: $("#btn-identity-restart"),
        identityList: $("#identity-list"),
        // Contact management
        btnContactEdit: $("#btn-contact-edit"), btnPinPeer: $("#btn-pin-peer"),
        btnBlockPeer: $("#btn-block-peer"),
        btnClearChat: $("#btn-clear-chat"),
        btnRemoveContact: $("#btn-remove-contact"),
        contactEditPanel: $("#contact-edit-panel"),
        contactNickname: $("#contact-nickname"), contactNotes: $("#contact-notes"),
        btnContactSave: $("#btn-contact-save"), btnContactCancel: $("#btn-contact-cancel"),
        // Settings
        btnSettings: $("#btn-settings"), settingsModal: $("#settings-modal"),
        btnSettingsClose: $("#btn-settings-close"),
        toggleAutoAnnounce: $("#toggle-auto-announce"),
        blockedList: $("#blocked-list"),
        rnsConfigEditor: $("#rns-config-editor"),
        btnSaveRnsConfig: $("#btn-save-rns-config"),
        rnsConfigStatus: $("#rns-config-status"),
        // Sidebar nav
        navPeers: $("#nav-peers"), navNetwork: $("#nav-network"), navPages: $("#nav-pages"),
        viewPeers: $("#view-peers"), viewNetwork: $("#view-network"), viewPages: $("#view-pages"),
        // Network dashboard
        netUptime: $("#net-uptime"), netPeers: $("#net-peers"),
        netPaths: $("#net-paths"), netLinks: $("#net-links"),
        netInterfaces: $("#net-interfaces"),
        // Pages
        pagesMain: $("#pages-main"),
        pageHostAddress: $("#page-host-address"),
        myPagesList: $("#my-pages-list"),
        nomadnetNodesList: $("#nomadnet-nodes-list"),
        pageAddressInput: $("#page-address-input"),
        btnPageGo: $("#btn-page-go"),
        pageViewer: $("#page-viewer"),
        pageSelect: $("#page-select"),
        peTitle: $("#pe-title"), pePath: $("#pe-path"), peContent: $("#pe-content"),
        peContentType: $("#pe-content-type"), peFormatHint: $("#pe-format-hint"),
        btnPageSave: $("#btn-page-save"), btnPageDelete: $("#btn-page-delete"),
        btnPageNew: $("#btn-page-new"), pageEditorStatus: $("#page-editor-status"),
        btnNewPage: $("#btn-new-page"),
        // Mobile
        peerListHeader: $(".peer-list-header"),
    };

    // ── Codec implementations ──────────────────────────────────────────────────
    // Each codec exposes:
    //   encode(Float32Array) → Uint8Array     (mic capture → network)
    //   decode(Uint8Array)   → Float32Array   (network → speaker)

    // G.711 µ-law
    const MuLaw = {
        BIAS: 33, CLIP: 8159,
        encode(pcm) {
            const out = new Uint8Array(pcm.length);
            for (let i = 0; i < pcm.length; i++) {
                let s = pcm instanceof Float32Array
                    ? Math.max(-32768, Math.min(32767, Math.round(pcm[i] * 32767)))
                    : pcm[i];
                const sign = s < 0 ? 0x80 : 0;
                s = Math.min(Math.abs(s), this.CLIP) + this.BIAS;
                let exp = 7, mask = 0x4000;
                while (exp > 0 && !(s & mask)) { exp--; mask >>= 1; }
                out[i] = ~(sign | (exp << 4) | ((s >> (exp + 3)) & 0x0F)) & 0xFF;
            }
            return out;
        },
        decode(mu) {
            const out = new Float32Array(mu.length);
            for (let i = 0; i < mu.length; i++) {
                const v = ~mu[i] & 0xFF, sign = v & 0x80, exp = (v >> 4) & 7, man = v & 0x0F;
                let s = ((man << 3) + this.BIAS) << exp; s -= this.BIAS;
                out[i] = (sign ? -s : s) / 32768.0;
            }
            return out;
        }
    };

    // ── Codec registry ─────────────────────────────────────────────────────────
    // id: byte written into binary WS/RNS frame (must match Python CODEC_ID_MAP)
    // maxSamples: max capture frame so the encoded output + 4-byte wire header
    //             fits within the RNS Link usable payload (~383 bytes).
    //             Formula: (383 - 4) = 379 bytes max encoded audio.
    //             mu-law: 1 byte/sample → 379 samples → use 256
    //
    // Only mu-law is implemented in the browser.  If the call negotiates
    // codec2_1200 (for LoRa), the server transcodes transparently on both
    // ends — the browser always sends and receives mu-law frames.
    const RNS_MAX_AUDIO = 379;  // bytes of encoded audio that fit in one RNS packet
    const CODEC_DEFS = {
        mu_law: { id: 0x00, sampleRate: 8000, maxSamples: 256, encode: f => MuLaw.encode(f), decode: b => MuLaw.decode(b) },
    };
    const CODEC_BY_ID = {};
    for (const [key, def] of Object.entries(CODEC_DEFS)) CODEC_BY_ID[def.id] = { ...def, key };

    // ── WebSocket connection (bidirectional, replaces SSE+POST) ──
    // One socket carries both JSON signaling and binary audio.  Lower
    // latency than the old SSE+HTTP-POST split because there's no per-frame
    // HTTP round-trip, no base64 wrapping for audio, and no head-of-line
    // blocking between signaling and audio.
    function connectWS() {
        if (state.ws) { try { state.ws.close(); } catch(_) {} }

        const proto = location.protocol === "https:" ? "wss:" : "ws:";
        const ws = new WebSocket(`${proto}//${location.host}/ws`);
        ws.binaryType = "arraybuffer";
        state.ws = ws;

        ws.onopen = () => {
            DOM.statusDot.classList.add("connected");
            DOM.statusText.textContent = "Connected";
        };

        ws.onclose = () => {
            DOM.statusDot.classList.remove("connected");
            DOM.statusText.textContent = "Disconnected, retrying...";
            // Reconnect after a short delay
            setTimeout(connectWS, 2000);
        };

        ws.onerror = () => {
            // onclose will fire next and schedule the reconnect
        };

        ws.onmessage = (e) => {
            // Binary frame → audio.  Layout: [0xAA][seq_hi][seq_lo][codec_id][audio...]
            if (e.data instanceof ArrayBuffer) {
                _handleBinaryAudio(e.data);
                return;
            }
            // Text frame → JSON signaling
            try {
                const msg = JSON.parse(e.data);
                handleWSMessage(msg);
            } catch (err) { console.error("WS parse:", err); }
        };
    }

    // Send a JSON message to the server.  Fire-and-forget.
    function wsSend(data) {
        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
            try { state.ws.send(JSON.stringify(data)); }
            catch (err) { console.error("wsSend error:", err); }
        }
    }

    // Send raw binary (audio) to the server — used by the call path.
    function wsSendBinary(buf) {
        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
            try { state.ws.send(buf); }
            catch (err) { console.error("wsSendBinary error:", err); }
        }
    }

    // Handle incoming binary audio frame from server.
    // Frame layout: [0xAA][seq_hi][seq_lo][codec_id][...audio_bytes]
    function _handleBinaryAudio(buffer) {
        const view = new DataView(buffer);
        if (view.byteLength < 4 || view.getUint8(0) !== 0xAA) return;
        const seq       = view.getUint16(1, false);   // big-endian
        const codecId   = view.getUint8(3);            // used to select decoder
        const audioBytes = new Uint8Array(buffer, 4);

        // Look up the codec definition; fall back to µ-law if unknown
        const codecDef = CODEC_BY_ID[codecId] || CODEC_DEFS.mu_law;

        // Ensure the playback AudioContext is at the right sample rate for this codec
        const neededSR = codecDef.sampleRate || 8000;
        if (!state.audioContext || state.audioContext.state === "closed"
            || state.audioContext.sampleRate !== neededSR) {
            if (state.audioContext && state.audioContext.state !== "closed")
                state.audioContext.close().catch(() => {});
            state.audioContext = new AudioContext({ sampleRate: neededSR, latencyHint: "interactive" });
            JitterBuffer.init(state.audioContext);
        }

        if (!state.callActive || !audioBytes.length) return;

        // No voice before accept: incoming callee must explicitly accept first.
        // Outgoing caller doesn't need this gate — they implicitly consented.
        if (state.callDirection === "in" && !state.callAccepted) return;

        // Decode bytes → Float32 PCM using the codec identified in the frame header,
        // then hand to the jitter buffer for ordered playback.
        JitterBuffer.push(seq, codecDef.decode(audioBytes));
    }

    function handleWSMessage(msg) {
        switch (msg.type) {
            case "identity":
                state.identity = msg.data;
                DOM.myHash.textContent = msg.data.lxmf_address || msg.data.identity_hash || "";
                DOM.myHash.title = "Click to copy · " + (msg.data.lxmf_address || msg.data.identity_hash || "");
                // Load active identity name
                fetch("/api/identities").then(r=>r.json()).then(ids => {
                    const active = ids.find(i => i.is_active);
                    const nameEl = document.getElementById("identity-name");
                    if (nameEl && active) nameEl.textContent = active.name;
                }).catch(()=>{});
                break;
            case "peer_update": {
                // P-5: merge server peers into the local list, preserving any
                // unread_count that the UI has accumulated locally since the last
                // server-side read.  A plain replace (state.peers = msg.peers)
                // wipes those locally-incremented counters every time a new
                // announce arrives, causing the unread badge to reset to the
                // DB value (often 0 for the current session window).
                const oldCount = state.peers.length;
                const newPeers = msg.peers || [];
                const localCounts = {};
                state.peers.forEach(p => {
                    if (p.unread_count) localCounts[p.dest_hash] = p.unread_count;
                });
                state.peers = newPeers.map(p => {
                    const localUnread = localCounts[p.dest_hash] || 0;
                    const serverUnread = parseInt(p.unread_count) || 0;
                    return Object.assign({}, p, {
                        // Keep whichever count is higher: local (live session) or DB
                        unread_count: Math.max(localUnread, serverUnread),
                    });
                });
                window._cachedPeers = state.peers;   // for group member picker
                renderPeerList();
                if (state.peers.length > oldCount && oldCount > 0) {
                    const newest = state.peers[0];
                    const pName = (newest && newest.display_name) || "New peer";
                    showToast(pName + " joined the network", "info");
                }
                break;
            }
            case "chat_recv": {
                // Q-2: block scope prevents const/let from leaking into adjacent cases
                addMessage(msg.peer_hash, "in", msg.content, msg.content_type, msg.timestamp);
                playNotificationSound();
                _triggerNetTraffic(msg.peer_hash, "in");
                // Increment local unread count if this peer isn't currently open
                if (state.activePeer !== msg.peer_hash) {
                    const p = state.peers.find(x => x.dest_hash === msg.peer_hash);
                    if (p) { p.unread_count = (parseInt(p.unread_count) || 0) + 1; }
                    renderPeerList();
                }
                break;
            }
            case "chat_sent": {
                // Q-2: block scope
                // Store the lxmf_hash on the most recent outgoing message for this peer
                if (msg.result && msg.result.hash && msg.peer_hash) {
                    _triggerNetTraffic(msg.peer_hash, "out");
                    const peerMsgs = state.messages[msg.peer_hash];
                    if (peerMsgs) {
                        for (let i = peerMsgs.length - 1; i >= 0; i--) {
                            if (peerMsgs[i].direction === "out" && !peerMsgs[i].lxmf_hash) {
                                peerMsgs[i].lxmf_hash = msg.result.hash;
                                break;
                            }
                        }
                    }
                }
                break;
            }
            case "file_sent": {
                // Same pattern as chat_sent — attach the LXMF hash to the
                // most recent outgoing file message so delivery receipts
                // (message_state events) can update its tick from ○ → ✓ → ✓✓.
                // Previously this case was missing entirely, so file and
                // voice-note bubbles stayed stuck at "sending" forever
                // even after successful delivery.
                const h = msg.result && msg.result.hash;
                const peer = (msg.result && msg.result.peer_hash) || state.activePeer;
                if (peer) _triggerNetTraffic(peer, "out");
                if (h) {
                    const peerMsgs = peer ? state.messages[peer] : null;
                    if (peerMsgs) {
                        for (let i = peerMsgs.length - 1; i >= 0; i--) {
                            if (peerMsgs[i].direction === "out"
                                && peerMsgs[i].content_type === "file"
                                && !peerMsgs[i].lxmf_hash) {
                                peerMsgs[i].lxmf_hash = h;
                                break;
                            }
                        }
                    }
                }
                break;
            }
            case "delivery_receipt": updateMessageState(msg.hash, 2); break;
            case "message_state": updateMessageState(msg.hash, msg.state, {
                via_propagation: !!msg.via_propagation,
                end_to_end: !!msg.end_to_end,
            }); break;
            // "delivery_failed" handled below with block scope (Q-2)
            case "file_progress":
                // Inline per-filename progress row at the bottom of the chat.
                // LXMF delivers files as one opaque payload, so we can't give
                // incremental byte-level percentages — but a visible "sending
                // xyz.pdf" row with a pulsing progress bar is far better than
                // the previous fire-and-forget toast.  The row stays up from
                // "sending" until "done" or "error".
                _renderFileProgressRow(msg);
                break;
            case "codecs": state.codecs = msg.data; break;
            // NomadNet page-host discovered — refresh sidebar list if pages tab open
            case "nomadnet_node":
                if (DOM.navPages && DOM.navPages.classList.contains("active")) {
                    loadNomadnetNodes();
                }
                break;
            case "call_incoming": onCallIncoming(msg.peer_hash, msg.codec); break;
            case "call_ringing":
                DOM.callStatus.textContent = "Ringing...";
                showToast("Calling " + getPeerName(msg.peer_hash) + "…", "info");
                break;
            case "call_connected": onCallConnected(msg.peer_hash, msg.codec); break;
            case "call_ended": onCallEnded(msg.peer_hash); break;
            case "call_failed": onCallFailed(msg.peer_hash, msg.reason); break;
            case "call_audio": onCallAudio(msg.data); break;  // legacy JSON fallback
            case "pong": break;  // keepalive reply — no action needed
            case "call_status":
                if (msg.result && msg.result.error) onCallFailed(msg.peer_hash, msg.result.error);
                break;
            case "group_message":
                onGroupMessage(msg.message);
                break;
            case "group_invite":
                onGroupInviteSSE(msg);
                break;
            case "group_member_joined":
                onGroupMemberJoined(msg);
                break;
            case "group_member_left":
                onGroupMemberLeft(msg);
                break;
            case "group_renamed":
                onGroupRenamed(msg);
                break;
            // group_left: we left or were kicked from a group — remove it from UI immediately
            case "group_left": {
                const gid = msg.group_id;
                if (gid) {
                    // If this group was open, close it and show welcome screen
                    if (typeof _activeGroupId !== "undefined" && _activeGroupId === gid) {
                        _activeGroupId = null;
                        const gPanel = document.getElementById("group-chat-panel");
                        const welcome = document.getElementById("welcome-screen");
                        if (gPanel)   gPanel.classList.add("hidden");
                        if (welcome)  welcome.classList.remove("hidden");
                    }
                    loadGroups();
                    showToast("You left the group", "info");
                }
                break;
            }
            // group_joined: we joined a group (invite accepted or channel join confirmed) — add to list
            case "group_joined": {
                loadGroups();
                if (msg.group && msg.group.name)
                    showToast(`Joined: ${msg.group.name}`, "success");
                break;
            }
            case "alert_received":
                onAlertReceived(msg.alert);
                break;
            case "alert_sent":
                onAlertSent(msg.alert);
                break;
            case "restart_required": {
                // F-3: server told us an identity was activated — show banner and
                // disable send/call controls until the user restarts the app.
                const banner = document.getElementById("restart-banner");
                if (banner) {
                    banner.textContent = msg.message || "Restart required to apply changes.";
                    banner.style.display = "block";
                }
                // Disable compose / call buttons to signal broken state
                if (DOM.sendBtn)    DOM.sendBtn.disabled    = true;
                if (DOM.callBtn)    DOM.callBtn.disabled    = true;
                showToast(msg.message || "Restart required", "warning");
                break;
            }
            case "group_partial_failure": {
                // A-3: some group fan-out sends failed
                showToast(
                    `Message sent to ${msg.sent_count} member(s); ${msg.failed_count} failed.`,
                    "warning"
                );
                break;
            }
            case "delivery_failed": {
                // Q-2: block scope
                updateMessageState(msg.hash, -1);
                if (msg.via_propagation) {
                    showToast("Direct delivery failed, retrying via propagation node…", "info");
                }
                break;
            }
            default: console.log("WS:", msg);
        }
    }

    // ── Peer List ──
    function renderPeerList() {
        DOM.peerCount.textContent = state.peers.length;
        if (!state.peers.length) { DOM.peerList.innerHTML = '<li class="peer-empty">No peers discovered yet.<br>Send an announce to start.</li>'; return; }
        DOM.peerList.innerHTML = state.peers.map(p => {
            const nickname = p.nickname || "";
            const name = nickname || p.display_name || p.dest_hash.substring(0,12);
            const active = state.activePeer === p.dest_hash ? "active" : "";
            const pinIcon = p.pinned ? `<span class="peer-pin"><svg viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="17" x2="12" y2="22"/><path d="M5 17h14l-1.5-3v-5l2-2V4H5.5v3l2 2v5z"/></svg></span>` : "";
            const onlineDot = isPeerOnline(p.last_seen || p.last_announce)
                ? '<span class="peer-online-dot"></span>'
                : '<span class="peer-offline-dot"></span>';
            const unread = (p.unread_count && state.activePeer !== p.dest_hash && parseInt(p.unread_count) > 0)
                ? `<span class="peer-unread-badge">${parseInt(p.unread_count) > 99 ? "99+" : p.unread_count}</span>`
                : "";
            return `<li class="peer-item ${active}" data-hash="${p.dest_hash}">
                <div class="peer-avatar">${name.charAt(0).toUpperCase()}</div>
                <div class="peer-info"><div class="peer-name">${onlineDot}${escapeHtml(name)}${pinIcon}</div><div class="peer-hash-short">${p.dest_hash.substring(0,10)}...</div></div>
                <div class="peer-item-right">${unread}<span class="peer-time">${timeAgo(p.last_seen||p.last_announce)}</span></div></li>`;
        }).join("");
        $$(".peer-item").forEach(el => el.addEventListener("click", () => selectPeer(el.dataset.hash)));
    }
    function selectPeer(hash) {
        state.activePeer = hash;
        const peer = state.peers.find(p => p.dest_hash === hash);
        DOM.welcomeScreen.classList.add("hidden"); DOM.chatPanel.classList.remove("hidden");
        // Hide network visualizer when selecting a peer
        const netVis = document.getElementById("net-visualizer");
        if (netVis) { netVis.classList.add("hidden"); stopNetworkGraph(); }
        // Hide the group chat panel if a group was open, and clear active
        // group id so coming back to the groups tab doesn't auto-reopen it.
        const groupPanel = document.getElementById("group-chat-panel");
        if (groupPanel) groupPanel.classList.add("hidden");
        _activeGroupId = null;
        document.querySelectorAll(".group-item.active")
            .forEach(el => el.classList.remove("active"));
        const displayName = peer ? (peer.nickname || peer.display_name || hash.substring(0,16)) : hash.substring(0,16);
        DOM.chatPeerName.textContent = displayName;
        DOM.chatPeerHash.textContent = hash;
        _updateChatAvatar(displayName);

        // Load and display the interface used to reach this peer
        _refreshPeerRoute(hash);

        // Update pin button state
        if (DOM.btnPinPeer) {
            _updatePinIcon(peer && peer.pinned);
        }

        // Hide contact edit panel
        if (DOM.contactEditPanel) DOM.contactEditPanel.classList.add("hidden");

        // U-2: use matchMedia for the mobile breakpoint check instead of
        // window.innerWidth.  matchMedia respects CSS media-query semantics
        // (handles devicePixelRatio, orientation, and zoom correctly) and avoids
        // triggering unnecessary layout reflows.
        if (window.matchMedia("(max-width: 600px)").matches) {
            DOM.peerList.classList.remove("expanded");
            if (DOM.peerListHeader) DOM.peerListHeader.classList.remove("expanded");
        }

        // Mark messages as read when opening conversation
        fetch(`/api/peers/${hash}/read`, { method: "POST" }).catch(() => {});
        // Clear local unread count immediately for instant UI response
        const peerObj = state.peers.find(p => p.dest_hash === hash);
        if (peerObj) peerObj.unread_count = 0;

        renderPeerList(); loadMessages(hash);
    }

    // ── Per-peer route/interface badge ──
    async function _refreshPeerRoute(hash) {
        const badgeEl = document.getElementById("chat-iface-badge");
        if (!badgeEl) return;
        badgeEl.textContent = "";
        badgeEl.className   = "chat-iface-badge";
        try {
            const res  = await fetch(`/api/peers/${hash}/route`);
            const data = await res.json();
            if (!data.has_path || !data.interface) {
                // No cached route — leave the badge empty rather than showing
                // a "no route cached" notice that worried users into thinking
                // the chat was broken.  Reticulum will look up a path on the
                // first send anyway.
                badgeEl.textContent = "";
                badgeEl.className   = "chat-iface-badge";
                return;
            }
            // Build a short label
            const itype = data.interface_type || "";
            let label;
            if (data.is_bluetooth) {
                label = `via BT · ${data.bt_mode || "BLE"}`;
                badgeEl.className = "chat-iface-badge chat-iface-badge-bt";
            } else if (itype.includes("TCP")) {
                label = "via TCP";
                badgeEl.className = "chat-iface-badge chat-iface-badge-tcp";
            } else if (itype === "RNodeInterface") {
                label = "via LoRa";
                badgeEl.className = "chat-iface-badge chat-iface-badge-lora";
            } else if (itype === "I2PInterface") {
                label = "via I2P";
                badgeEl.className = "chat-iface-badge chat-iface-badge-i2p";
            } else if (itype === "AutoInterface") {
                label = "via WiFi/Auto";
                badgeEl.className = "chat-iface-badge chat-iface-badge-wifi";
            } else {
                label = `via ${itype.replace("Interface","") || "?"}`;
                badgeEl.className = "chat-iface-badge chat-iface-badge-unknown";
            }
            if (data.hops != null && data.hops > 0) label += ` · ${data.hops} hop${data.hops !== 1 ? "s" : ""}`;
            badgeEl.textContent = label;
        } catch(e) {
            badgeEl.textContent = "";
        }
    }

    // ── Messages ──
    function loadMessages(ph) { fetch(`/api/messages/${ph}`).then(r=>r.json()).then(msgs => { state.messages[ph]=msgs; renderMessages(ph); }).catch(console.error); }
    function addMessage(ph, dir, content, ct, ts) {
        if (!state.messages[ph]) state.messages[ph] = [];
        state.messages[ph].push({ direction:dir, content, content_type:ct||"text", timestamp:ts||Date.now()/1000, delivered:0 });
        if (state.activePeer === ph) renderMessages(ph);
    }
    /** Build the HTML for one message (no DOM creation yet). */
    function _msgHTML(m) {
        const cls  = m.direction === "out" ? "outgoing" : "incoming";
        const time = formatTime(m.timestamp);
        let status = "";
        if (m.direction === "out") {
            const st = m.state !== undefined ? m.state : (m.delivered ? 2 : 0);
            if (st === -1)  status = '<span class="tick-failed">✗</span>';
            else if (st === 0)  status = '<span class="tick-sending">○</span>';
            else if (st === 1)  status = '<span class="tick-stored">✓</span>';
            else if (st >= 2) {
                // Two flavours of "delivered":
                //   end_to_end       = recipient confirmed via propagation_ack
                //   via_propagation  = stored at propagation host, not yet ack'd
                //   neither          = direct delivery confirmed by recipient
                // Use a grey ✓✓ for the stored-at-host case (matches WhatsApp's
                // grey-ticks convention) and the accent ✓✓ for actual receipt.
                if (m.via_propagation && !m.end_to_end) {
                    status = '<span class="tick-stored-at-host" title="Stored at propagation node, awaiting recipient">✓✓</span>';
                } else {
                    status = '<span class="tick-delivered" title="Delivered to recipient">✓✓</span>';
                }
            }
        }
        let body;
        if (m.content_type === "file") {
            try {
                const f   = JSON.parse(m.content);
                const url = f.url || `/files/${encodeURIComponent(f.filename)}`;
                if (f.filename && f.filename.toLowerCase().endsWith(".vnote")) {
                    // Voice note: show a compact player.  Duration is
                    // stored in the .vnote header; we estimate from filesize
                    // for the inline label (0.125 KB/sec at mu-law 8 kHz).
                    // The real duration is read when the user hits play.
                    const estSecs = f.filesize ? Math.max(1, Math.round((f.filesize - 10) / 8000)) : 0;
                    const sizeStr = f.filesize ? formatBytes(f.filesize) : "";
                    const bars = Array.from({length: 18}, (_, i) =>
                        `<div class="voice-note-bar" style="height:${4 + ((i*7)%16)}px;"></div>`).join("");
                    const safeUrl = url.replace(/'/g, "\\'");
                    body = `<div class="voice-note">
                        <button class="voice-note-play" onclick="_playVoiceNote('${safeUrl}', this)" title="Play">▶</button>
                        <div class="voice-note-waveform">${bars}</div>
                        <span class="voice-note-time">${estSecs}s${sizeStr ? ` · ${sizeStr}` : ""}</span>
                    </div>`;
                } else {
                    body = `<div class="message-file"><span class="file-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" style="width:18px;height:18px"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg></span><div><a class="file-name" href="${url}" target="_blank" download>${escapeHtml(f.filename)}</a><div class="file-size">${f.filesize ? formatBytes(f.filesize) : ""}</div></div></div>`;
                }
            } catch { body = `<div>${escapeHtml(m.content)}</div>`; }
        } else {
            body = `<div>${escapeHtml(m.content)}</div>`;
        }
        const msgId    = m.id || "";
        const deleteBtn = msgId ? `<button class="msg-delete-btn" data-id="${msgId}" title="Delete">✕</button>` : "";
        return { cls, time, status, body, deleteBtn };
    }

    /**
     * P-3: Incremental message rendering.
     *
     * Instead of rebuilding innerHTML on every call (which destroys file
     * attachment links, loses scroll position, and flashes the UI), we:
     *   1. Build a keyed index of already-rendered elements by msgKey.
     *   2. Append only NEW messages at the bottom.
     *   3. For existing messages, patch only the status indicator (tick).
     *   4. Remove elements whose keys are no longer in the message list.
     *
     * msgKey = m.id (if set) else String(m.timestamp) — unique per message.
     */
    function renderMessages(ph) {
        const msgs    = (state.messages[ph] || []).filter(m => m.content_type !== "call_signal");
        const list    = DOM.messagesList;

        // Build keyed index of current DOM nodes
        const rendered = {};
        list.querySelectorAll("[data-msgkey]").forEach(el => {
            rendered[el.dataset.msgkey] = el;
        });

        const seen = new Set();
        const wasAtBottom = DOM.messagesContainer.scrollTop + DOM.messagesContainer.clientHeight
                            >= DOM.messagesContainer.scrollHeight - 40;

        msgs.forEach(m => {
            const key = String(m.id || m.timestamp);
            seen.add(key);

            if (rendered[key]) {
                // Update status tick only — don't touch the rest of the element
                const statusEl = rendered[key].querySelector(".msg-status");
                if (statusEl && m.direction === "out") {
                    const st = m.state !== undefined ? m.state : (m.delivered ? 2 : 0);
                    let newStatus = "";
                    if (st === -1)      newStatus = '<span class="tick-failed">✗</span>';
                    else if (st === 0)  newStatus = '<span class="tick-sending">○</span>';
                    else if (st === 1)  newStatus = '<span class="tick-stored">✓</span>';
                    else if (st >= 2) {
                        if (m.via_propagation && !m.end_to_end) {
                            newStatus = '<span class="tick-stored-at-host" title="Stored at propagation node, awaiting recipient">✓✓</span>';
                        } else {
                            newStatus = '<span class="tick-delivered" title="Delivered to recipient">✓✓</span>';
                        }
                    }
                    if (statusEl.innerHTML !== newStatus) statusEl.innerHTML = newStatus;
                }
            } else {
                // New message — create element and append
                const { cls, time, status, body, deleteBtn } = _msgHTML(m);
                const div = document.createElement("div");
                div.className = `message ${cls}`;
                div.dataset.msgkey = key;
                div.innerHTML = `${deleteBtn}${body}<div class="msg-meta"><span>${time}</span><span class="msg-status">${status}</span></div>`;
                if (deleteBtn) {
                    div.querySelector(".msg-delete-btn").addEventListener("click", (e) => {
                        e.stopPropagation();
                        deleteMessage(parseInt(div.querySelector(".msg-delete-btn").dataset.id));
                    });
                }
                list.appendChild(div);
            }
        });

        // Remove elements whose messages have been deleted
        Object.keys(rendered).forEach(key => {
            if (!seen.has(key)) list.removeChild(rendered[key]);
        });

        // Auto-scroll only if we were already at the bottom
        if (wasAtBottom) DOM.messagesContainer.scrollTop = DOM.messagesContainer.scrollHeight;
    }
    function updateMessageState(hash, newState, meta) {
        for (const msgs of Object.values(state.messages)) {
            for (const m of msgs) {
                if (m.lxmf_hash === hash) {
                    m.state = newState;
                    // Track HOW the state was reached so the renderer
                    // can pick the right tick glyph.  via_propagation=true
                    // means the message went through a propagation node
                    // (so even a "delivered" state means stored-at-host
                    // unless end_to_end is also true).
                    if (meta) {
                        if (meta.via_propagation) m.via_propagation = true;
                        if (meta.end_to_end)      m.end_to_end = true;
                    }
                }
            }
        }
        if (state.activePeer) renderMessages(state.activePeer);
    }
    async function sendMessage() {
        // U-1: await wsSend and mark the message as failed (-1) if it throws,
        // so the user sees a ✗ indicator instead of a stuck ○ "sending" state.
        const text = DOM.composeInput.value.trim();
        if (!text || !state.activePeer) return;
        const peer = state.activePeer;
        const ts   = Date.now() / 1000;
        addMessage(peer, "out", text, "text", ts);
        DOM.composeInput.value = "";
        try {
            await wsSend({ type: "chat_send", peer_hash: peer, content: text });
        } catch (err) {
            // POST itself failed (network error, server down) — mark latest msg failed
            console.error("sendMessage failed:", err);
            const msgs = state.messages[peer];
            if (msgs) {
                for (let i = msgs.length - 1; i >= 0; i--) {
                    if (msgs[i].direction === "out" && msgs[i].content === text) {
                        msgs[i].state = -1;
                        break;
                    }
                }
            }
            if (state.activePeer === peer) renderMessages(peer);
        }
    }
    function sendFile(file) {
        if (!state.activePeer) return;
        const reader = new FileReader();
        reader.onload = () => {
            wsSend({ type:"file_send", peer_hash:state.activePeer, filename:file.name, data:reader.result.split(",")[1] });
            addMessage(state.activePeer, "out", JSON.stringify({type:"file",filename:file.name,filesize:file.size,url:`/files/${encodeURIComponent(file.name)}`}), "file");
        };
        reader.readAsDataURL(file);
    }

    // ── Voice notes ────────────────────────────────────────────────────
    // Push-to-talk style recorder.  Audio is captured at 8 kHz, encoded
    // to mu-law (1 byte/sample = 8 kbps), wrapped in a minimal .vnote
    // container, and sent through the existing file-send pipe.  The
    // receiver detects the .vnote extension and renders a compact
    // playback widget instead of a download chip.
    //
    // Why mu-law and not Codec2? Voice notes aren't time-sensitive, so
    // the 53× bandwidth advantage of Codec2 (1.2 kbps) doesn't buy as
    // much as it does for live calls.  Mu-law lets us decode purely in
    // the browser without any WASM dependency, and the file is small
    // enough (~60 KB/minute) to move easily even over LoRa.
    //
    // .vnote file layout (binary):
    //   "VNOTE\0"  (6 bytes, magic)
    //   duration_ms (4 bytes, big-endian uint32)
    //   mu-law samples @ 8 kHz
    const VNOTE_MAGIC = new Uint8Array([0x56, 0x4E, 0x4F, 0x54, 0x45, 0x00]); // "VNOTE\0"

    // Voice-note container v2 layout (backward compatible with v1):
    //   [0..5]    magic "VNOTE\0"
    //   [6..9]    duration_ms uint32 BE
    //   [10..11]  sample_rate uint16 BE  (0 = legacy 8 kHz)
    //   [12..]    mu-law samples
    //
    // Legacy v1 files don't have the sample_rate field — instead byte 10
    // is the first audio sample.  The reader detects this by checking
    // whether byte 10 looks like a plausible sample-rate value
    // (one of 8000, 11025, 16000, 22050, 24000, 32000, 44100, 48000).
    // If not, fall back to 8 kHz and treat byte 10 as audio.
    const VNOTE_VALID_RATES = new Set([8000, 11025, 16000, 22050, 24000, 32000, 44100, 48000]);

    // Quality presets.  Sample rate is the one stored in the .vnote header
    // and used by the receiver to reconstruct the playback clock.
    const VNOTE_QUALITY = {
        low:    { sampleRate:  8000, label: "Low (8 kHz, ~60 KB/min)"   },
        medium: { sampleRate: 16000, label: "Medium (16 kHz, ~120 KB/min)" },
        high:   { sampleRate: 24000, label: "High (24 kHz, ~180 KB/min)"   },
    };

    function _getVoiceQuality() {
        const saved = localStorage.getItem("retimesh_voice_quality");
        return (saved && VNOTE_QUALITY[saved]) ? saved : "low";
    }
    function _setVoiceQuality(q) {
        if (VNOTE_QUALITY[q]) localStorage.setItem("retimesh_voice_quality", q);
    }

    const _voice = {
        recording: false,
        stream: null,
        ctx: null,
        workletNode: null,
        samples: [],
        startedAt: 0,
        targetRate: 8000,
    };

    async function startVoiceRecord() {
        if (_voice.recording) return;
        if (!state.activePeer) { showToast("Select a peer first", "error"); return; }
        const quality = _getVoiceQuality();
        _voice.targetRate = VNOTE_QUALITY[quality].sampleRate;
        try {
            _voice.stream = await navigator.mediaDevices.getUserMedia({ audio: {
                channelCount: 1, sampleRate: _voice.targetRate,
                echoCancellation: true, noiseSuppression: true,
            }});
            _voice.ctx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: _voice.targetRate });
            const src = _voice.ctx.createMediaStreamSource(_voice.stream);

            // ScriptProcessorNode is deprecated but universally available and
            // adequate for a 8 kHz capture.  AudioWorklet would be cleaner
            // but needs more setup.  Buffer is 2048 samples = ~256 ms chunks.
            const proc = _voice.ctx.createScriptProcessor(2048, 1, 1);
            proc.onaudioprocess = (e) => {
                if (!_voice.recording) return;
                const input = e.inputBuffer.getChannelData(0);
                // Copy because the buffer is reused by the audio thread
                _voice.samples.push(new Float32Array(input));
            };
            src.connect(proc);
            proc.connect(_voice.ctx.destination);
            _voice.workletNode = proc;
            _voice.startedAt = Date.now();
            _voice.samples = [];
            _voice.recording = true;
            document.getElementById("btn-voice-record")?.classList.add("recording");
            showToast("Recording: click again to send", "info");
        } catch (err) {
            showToast("Microphone access denied", "error");
            console.error("startVoiceRecord:", err);
            _cleanupVoiceRecord();
        }
    }

    async function stopVoiceRecordAndSend() {
        if (!_voice.recording) return;
        _voice.recording = false;
        document.getElementById("btn-voice-record")?.classList.remove("recording");
        const durationMs = Date.now() - _voice.startedAt;
        const actualRate = _voice.ctx ? _voice.ctx.sampleRate : 8000;

        // Flatten captured chunks into one Float32Array of PCM
        let total = 0;
        for (const c of _voice.samples) total += c.length;
        const pcm = new Float32Array(total);
        let offset = 0;
        for (const c of _voice.samples) { pcm.set(c, offset); offset += c.length; }

        _cleanupVoiceRecord();

        if (durationMs < 500 || total < 800) {
            showToast("Hold to record (too short)", "info");
            return;
        }

        // Downsample to the target rate if the browser gave us a higher one.
        // Chrome/Firefox often ignore the sampleRate hint and give 44.1/48 kHz.
        // A simple decimation-average is fine for voice.
        const targetRate = _voice.targetRate || 8000;
        let pcmOut = pcm;
        if (actualRate > targetRate) {
            const ratio = actualRate / targetRate;
            const outLen = Math.floor(pcm.length / ratio);
            pcmOut = new Float32Array(outLen);
            for (let i = 0; i < outLen; i++) {
                const start = Math.floor(i * ratio);
                const end   = Math.floor((i + 1) * ratio);
                let sum = 0;
                for (let j = start; j < end; j++) sum += pcm[j];
                pcmOut[i] = sum / (end - start);
            }
        }

        // Encode Float32 → mu-law (MuLaw.encode handles Float32 directly)
        const mulawBytes = MuLaw.encode(pcmOut);

        // Build .vnote container v2: magic + duration + sample_rate + audio
        const body = new Uint8Array(VNOTE_MAGIC.length + 4 + 2 + mulawBytes.length);
        body.set(VNOTE_MAGIC, 0);
        const dv = new DataView(body.buffer);
        dv.setUint32(VNOTE_MAGIC.length, durationMs, false);   // big-endian
        dv.setUint16(VNOTE_MAGIC.length + 4, targetRate, false);
        body.set(mulawBytes, VNOTE_MAGIC.length + 4 + 2);

        // Send through the existing file-send pipe.  Filename format puts
        // a date stamp + duration so the user sees something meaningful
        // in the files directory.
        const stamp = new Date().toISOString().replace(/[:T]/g, "-").slice(0, 19);
        const secs  = Math.round(durationMs / 1000);
        const fname = `voice-${stamp}-${secs}s.vnote`;

        // base64-encode without using FileReader to avoid an extra round-trip
        let bin = "";
        for (let i = 0; i < body.length; i++) bin += String.fromCharCode(body[i]);
        const b64 = btoa(bin);

        wsSend({ type: "file_send", peer_hash: state.activePeer, filename: fname, data: b64 });
        addMessage(state.activePeer, "out", JSON.stringify({
            type: "file", filename: fname, filesize: body.length,
            url: `/files/${encodeURIComponent(fname)}`,
        }), "file");
    }

    function _cleanupVoiceRecord() {
        try { _voice.workletNode && _voice.workletNode.disconnect(); } catch (_) {}
        try { _voice.ctx && _voice.ctx.close(); } catch (_) {}
        try { _voice.stream && _voice.stream.getTracks().forEach(t => t.stop()); } catch (_) {}
        _voice.workletNode = null;
        _voice.ctx = null;
        _voice.stream = null;
        _voice.samples = [];
    }

    function toggleVoiceRecord() {
        if (_voice.recording) stopVoiceRecordAndSend();
        else                  startVoiceRecord();
    }

    // ── Voice-note playback ────────────────────────────────────────────
    // Fetches the .vnote file, validates magic, decodes mu-law, plays it
    // back via a WebAudio buffer.  Only one voice note plays at a time.
    let _vnotePlayingBtn = null;
    let _vnotePlayingSrc = null;
    async function playVoiceNote(url, btn) {
        // Stop any currently playing voice note
        if (_vnotePlayingSrc) {
            try { _vnotePlayingSrc.stop(); } catch (_) {}
            if (_vnotePlayingBtn) _vnotePlayingBtn.textContent = "▶";
        }

        try {
            const res = await fetch(url);
            if (!res.ok) {
                showToast(`Voice note not found (HTTP ${res.status})`, "error");
                return;
            }
            const buf = new Uint8Array(await res.arrayBuffer());
            if (buf.length < VNOTE_MAGIC.length + 4) {
                showToast(`Voice note too short (${buf.length} bytes)`, "error");
                return;
            }

            // Verify magic with detail on mismatch so we can diagnose
            // corrupted files
            for (let i = 0; i < VNOTE_MAGIC.length; i++) {
                if (buf[i] !== VNOTE_MAGIC[i]) {
                    const got = Array.from(buf.slice(0, 6))
                        .map(b => b.toString(16).padStart(2, "0")).join(" ");
                    console.warn(`Voice note magic mismatch at byte ${i}: expected 56 4E 4F 54 45 00, got ${got}`);
                    showToast("Voice note header invalid, check console for bytes", "error");
                    return;
                }
            }
            // Read sample rate from v2 header (bytes 10-11).  If that value
            // doesn't look like a plausible rate, this must be a legacy v1
            // file — byte 10 is actually audio, so treat the rate as 8 kHz
            // and the audio start at byte 10.
            const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
            let sampleRate = 8000;
            let audioStart = VNOTE_MAGIC.length + 4;  // v1 default (10)
            try {
                const maybeRate = dv.getUint16(VNOTE_MAGIC.length + 4, false);
                if (VNOTE_VALID_RATES.has(maybeRate)) {
                    sampleRate = maybeRate;
                    audioStart = VNOTE_MAGIC.length + 4 + 2;  // v2 (12)
                }
            } catch (_) {
                // Buffer too short for uint16 — stick with v1 defaults
            }
            const mulaw = buf.subarray(audioStart);
            const pcmFloat = MuLaw.decode(mulaw);  // Float32Array

            const ctx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate });
            const audioBuf = ctx.createBuffer(1, pcmFloat.length, sampleRate);
            audioBuf.getChannelData(0).set(pcmFloat);
            const src = ctx.createBufferSource();
            src.buffer = audioBuf;
            src.connect(ctx.destination);
            src.start(0);
            _vnotePlayingSrc = src;
            _vnotePlayingBtn = btn;
            if (btn) btn.textContent = "■";
            src.onended = () => {
                if (btn) btn.textContent = "▶";
                try { ctx.close(); } catch (_) {}
                _vnotePlayingSrc = null;
                _vnotePlayingBtn = null;
            };
        } catch (err) {
            showToast("Could not play voice note: " + (err?.message || err), "error");
            console.error("playVoiceNote:", err);
        }
    }
    // Expose so inline onclick handlers in rendered messages can reach it
    window._playVoiceNote = playVoiceNote;

    // ── Audio Call ──

    /* Draw the static codec bandwidth comparison chart */
    function _drawCodecChart() {
        const canvas = document.getElementById("codec-chart");
        if (!canvas) return;
        const ctx = canvas.getContext("2d");
        const W = canvas.offsetWidth || 420;
        canvas.width  = W;
        canvas.height = 110;

        const codecs = [
            { label: "µ-law G.711", bps: 64000, color: "#58a6ff" },
            { label: "Codec2 1200", bps: 1200,  color: "#d29922" },
        ];
        const maxBps = Math.max(...codecs.map(c => c.bps));
        const barH   = 22;
        const labelW = 100;
        const gap    = 14;

        ctx.clearRect(0, 0, W, 110);
        ctx.font = "11px system-ui, sans-serif";

        codecs.forEach((c, i) => {
            const y       = i * (barH + gap) + 8;
            const barMaxW = W - labelW - 56;
            const barW    = Math.round((c.bps / maxBps) * barMaxW);

            // Label
            ctx.fillStyle = "#8b949e";
            ctx.textAlign = "right";
            ctx.fillText(c.label, labelW - 6, y + barH * 0.68);

            // Bar
            ctx.fillStyle = c.color + "55";
            ctx.beginPath();
            ctx.roundRect(labelW, y, barW, barH, 4);
            ctx.fill();
            ctx.fillStyle = c.color;
            ctx.beginPath();
            ctx.roundRect(labelW, y, Math.min(barW, 6), barH, [4, 0, 0, 4]);
            ctx.fill();

            // Value
            ctx.fillStyle = "#cdd9e5";
            ctx.textAlign = "left";
            const label = c.bps >= 1000 ? (c.bps/1000).toFixed(c.bps >= 10000 ? 0 : 1) + " kbps" : c.bps + " bps";
            ctx.fillText(label, labelW + barW + 6, y + barH * 0.68);
        });
    }

    /* Initiate call directly — codec selection UI removed.  Always uses
       µ-law (G.711) which works on every link RetiMesh supports.  The
       previous flow opened a "Choose a codec" modal; the user requested
       removing that intermediate step. */
    async function startCall() {
        if (!state.activePeer || state.callActive) return;
        _initiateCall("mu_law");
    }

    function _initiateCall(codec) {
        state.callActive = true; state.callPeer = state.activePeer; state.callDirection = "out"; state.callSeconds = 0;
        state.callCodec = codec;
        const codecLabels = { "mu_law": "mu-law · 64 kbps", "codec2_1200": "Codec2 · 1.2 kbps" };
        showCallUI("Connecting...", DOM.chatPeerName.textContent, false);
        DOM.callCodecInfo.textContent = "Codec: " + (codecLabels[codec] || codec);
        wsSend({ type: "call_start", peer_hash: state.activePeer, codec: codec });
    }
    function onCallIncoming(ph, codec) {
        if (state.callActive) return;
        state.callActive = true; state.callPeer = ph; state.callDirection = "in";
        state.callSeconds = 0; state.callCodec = codec || "mu_law";
        showCallUI("Incoming call…", getPeerName(ph), true);
        showToast("Incoming call from " + getPeerName(ph), "info");
    }
    // Map codec IDs to human-readable labels
    const CODEC_LABELS = {
        "mu_law":      "G.711 mu-law · 64 kbps",
        "codec2_1200": "Codec2 · 1.2 kbps (LoRa)",
    };
    function _codecLabel(c) { return CODEC_LABELS[c] || (c || "mu-law"); }

    function onCallConnected(ph, codec) {
        state.callActive = true; state.callPeer = ph;
        // Caller's implicit accept happens here when the callee signals connect.
        // Callee sets this flag explicitly in acceptCall().
        state.callAccepted = true;
        DOM.callStatus.textContent = "Connected";
        // Show the actual negotiated codec — previously always showed "mu-law"
        if (codec) state.callCodec = codec;
        DOM.callCodecInfo.textContent = "Codec: " + _codecLabel(codec || state.callCodec);
        if (DOM.btnCallAccept) DOM.btnCallAccept.classList.add("hidden");
        // Only start timer + capture for the outgoing caller.
        // The callee already started both in acceptCall() and must not restart them.
        if (state.callDirection !== "in") {
            startCallTimer();
            startAudioCapture();
        }
    }
    function onCallEnded(ph) {
        stopAudioCapture(); hideCallUI();
        state.callActive = false; state.callPeer = null; state.callAccepted = false;
        showToast("Call ended", "info");
    }
    function onCallFailed(ph, reason) {
        stopAudioCapture(); hideCallUI();
        state.callActive = false; state.callPeer = null; state.callAccepted = false;
        console.warn("Call failed:", reason);
        showToast("Call failed: " + (reason || "unknown"), "error");
    }

    // onCallAudio() is no longer used — audio is now received as binary
    // WebSocket frames handled by _handleBinaryAudio() + JitterBuffer.
    // Kept as a no-op in case the server still sends a JSON call_audio event.
    function onCallAudio(b64) {
        // Legacy JSON path: decode base64, feed to jitter buffer with a dummy seq
        if (!state.callActive) return;
        if (!state.audioContext || state.audioContext.state === "closed") {
            state.audioContext = new AudioContext({ sampleRate: SAMPLE_RATE, latencyHint: "interactive" });
            JitterBuffer.init(state.audioContext);
        }
        try {
            const bytes = base64ToUint8Array(b64);
            // Use a rolling sequence counter so duplicate suppression works
            if (!state._legacySeq) state._legacySeq = 0;
            JitterBuffer.push(state._legacySeq++ & 0xFFFF, bytes);
        } catch (e) { console.error("Legacy audio playback:", e); }
    }

    function acceptCall() {
        if (!state.callPeer) return;
        state.callAccepted = true;
        wsSend({ type: "call_accept", peer_hash: state.callPeer });
        DOM.callStatus.textContent = "Connected";
        DOM.callCodecInfo.textContent = "Codec: " + _codecLabel(state.callCodec);
        if (DOM.btnCallAccept) DOM.btnCallAccept.classList.add("hidden");
        startCallTimer();
        startAudioCapture();
    }
    function endCall() { if (state.callPeer) wsSend({ type:"call_end", peer_hash:state.callPeer }); stopAudioCapture(); hideCallUI(); state.callActive=false; state.callPeer=null; state.callAccepted=false; }

    // ── AudioWorklet processor code (injected as a Blob URL) ──────────────────
    // AudioWorklet runs in a dedicated real-time audio thread, unlike the
    // deprecated ScriptProcessor which ran on the main JS thread and caused
    // audio glitches whenever the UI was busy.
    // _WORKLET_CODE is a template string; bufferSize is injected at runtime
    // to match the codec's sample rate (8kHz → 256 samples; 16kHz → 512 samples).
    function _makeWorkletCode(bufferSize) { return `
class RetimeshCapture extends AudioWorkletProcessor {
    constructor(options) {
        super();
        this._buf    = new Float32Array(${bufferSize});
        this._offset = 0;
        this._seq    = 0;
        this._active = true;
        this.port.onmessage = (e) => { if (e.data === 'stop') this._active = false; };
    }
    process(inputs) {
        if (!this._active) return false;
        const ch = inputs[0] && inputs[0][0];
        if (!ch) return true;
        for (let i = 0; i < ch.length; i++) {
            this._buf[this._offset++] = ch[i];
            if (this._offset >= this._buf.length) {
                // Transfer a copy to the main thread
                const frame = this._buf.slice();
                this.port.postMessage({ seq: this._seq++, pcm: frame }, [frame.buffer]);
                this._offset = 0;
            }
        }
        return true;
    }
}
registerProcessor('retimesh-capture', RetimeshCapture);
`; }    // end _makeWorkletCode

    async function startAudioCapture() {
        // Guard against double-capture (e.g. callee: acceptCall() then onCallConnected()).
        // If a micStream is already live, capture is already running — do nothing.
        if (state.micStream) return;

        // Use the codec's native sample rate for the capture context so there is
        // zero resampling loss.  For 16 kHz codecs (ADPCM-WB, PCM-WB) this gives
        // noticeably crisper audio than downsampling from 48 kHz.
        const codecKey    = state.callCodec || "mu_law";
        const codecDef    = CODEC_DEFS[codecKey] || CODEC_DEFS.mu_law;
        const captureSR   = codecDef.sampleRate || 8000;

        try {
            state.micStream = await navigator.mediaDevices.getUserMedia({
                audio: {
                    sampleRate:       captureSR,
                    channelCount:     1,
                    echoCancellation: true,
                    noiseSuppression: true,
                    autoGainControl:  true,
                },
            });

            // Dedicated capture context at the codec's sample rate.
            const captureCtx = new AudioContext({
                sampleRate:  captureSR,
                latencyHint: "interactive",
            });
            state._captureCtx = captureCtx;

            // Reset VAD hangover counter
            _vadHangover = 0;
            let _capSeq = 0;  // outbound sequence number

            // Frame size: use the codec's maxSamples to stay within RNS MTU
            const frameSamples = codecDef.maxSamples || 256;

            // Try AudioWorklet (modern, runs off main thread)
            let _useWorklet = false;
            try {
                const workletSrc = _makeWorkletCode(frameSamples);
                const blob    = new Blob([workletSrc], { type: "application/javascript" });
                const blobUrl = URL.createObjectURL(blob);
                await captureCtx.audioWorklet.addModule(blobUrl);
                URL.revokeObjectURL(blobUrl);

                const source  = captureCtx.createMediaStreamSource(state.micStream);
                const worklet = new AudioWorkletNode(captureCtx, "retimesh-capture");
                state._workletNode = worklet;

                worklet.port.onmessage = (e) => {
                    if (!state.callActive || !state.callPeer) return;
                    const { seq, pcm } = e.data;

                    // Voice Activity Detection — skip silent frames
                    if (_hasVoice(pcm)) {
                        _vadHangover = VAD_HANGOVER_MAX;
                    } else {
                        if (_vadHangover <= 0) return;
                        _vadHangover--;
                    }

                    _sendAudioBinary(new Float32Array(pcm), seq);
                };

                source.connect(worklet);
                // Worklet needs a destination node to keep running (even if silent output)
                worklet.connect(captureCtx.destination);
                _useWorklet = true;
                console.log("Audio capture: AudioWorklet ✓");
            } catch (workletErr) {
                // Fallback to ScriptProcessor for older browsers / environments
                console.warn("AudioWorklet unavailable, using ScriptProcessor fallback:", workletErr);
            }

            if (!_useWorklet) {
                const source = captureCtx.createMediaStreamSource(state.micStream);
                // ScriptProcessor is deprecated but still works in most browsers
                // Buffer size MUST be a power of 2 — maxSamples is already power-of-2
                const proc = captureCtx.createScriptProcessor(frameSamples, 1, 1);
                state.scriptProcessor = proc;

                proc.onaudioprocess = (e) => {
                    if (!state.callActive || !state.callPeer) return;
                    const input = e.inputBuffer.getChannelData(0);

                    if (_hasVoice(input)) {
                        _vadHangover = VAD_HANGOVER_MAX;
                    } else {
                        if (_vadHangover <= 0) return;
                        _vadHangover--;
                    }

                    _sendAudioBinary(input, _capSeq++);
                };

                source.connect(proc);
                proc.connect(captureCtx.destination);
                console.log("Audio capture: ScriptProcessor fallback");
            }

        } catch (err) {
            console.error("Mic failed:", err);
            DOM.callStatus.textContent = "Mic access denied: please allow microphone permission";
        }
    }

    // Send an encoded audio frame to the server via HTTP POST (base64-encoded).
    // Replaces the old binary WebSocket frame path.
    function _sendAudioBinary(pcmFloat32, seq) {
        const codecKey = state.callCodec || "mu_law";
        const def      = CODEC_DEFS[codecKey] || CODEC_DEFS.mu_law;
        const encoded  = def.encode(pcmFloat32);
        // Pack into the binary wire layout: [0xAA][seq_hi][seq_lo][codec_id][...audio]
        const buf    = new Uint8Array(4 + encoded.length);
        buf[0] = 0xAA;
        buf[1] = (seq >> 8) & 0xFF;
        buf[2] = seq & 0xFF;
        buf[3] = def.id;
        buf.set(encoded, 4);
        // Send the raw binary frame over the WebSocket — no base64, no POST.
        if (!state.callPeer) return;
        wsSendBinary(buf.buffer);
    }

    function stopAudioCapture() {
        if (state._workletNode) {
            try { state._workletNode.port.postMessage("stop"); } catch (_) {}
            try { state._workletNode.disconnect(); } catch (_) {}
            state._workletNode = null;
        }
        if (state.scriptProcessor) {
            try { state.scriptProcessor.disconnect(); } catch (_) {}
            state.scriptProcessor = null;
        }
        if (state.micStream) {
            state.micStream.getTracks().forEach(t => t.stop());
            state.micStream = null;
        }
        if (state._captureCtx && state._captureCtx.state !== "closed") {
            state._captureCtx.close().catch(() => {});
            state._captureCtx = null;
        }
        if (state.audioContext && state.audioContext.state !== "closed") {
            state.audioContext.close().catch(() => {});
            state.audioContext = null;
        }
        JitterBuffer.reset();
        clearInterval(state.callTimer);
        _vadHangover = 0;
    }

    function showCallUI(status, name, showAccept) {
        // Close any open modals/panels that could sit above the call overlay
        document.querySelectorAll(".modal:not(.hidden), .settings-modal:not(.hidden)").forEach(m => m.classList.add("hidden"));
        DOM.callOverlay.classList.remove("hidden");
        DOM.callStatus.textContent = status;
        DOM.callPeerName.textContent = name;
        DOM.callTimer.textContent = "00:00";
        _updateCallAvatar(name);
        DOM.callCodecInfo.textContent = "Codec: mu-law · 64 kbps";
        if (DOM.btnCallAccept) { showAccept ? DOM.btnCallAccept.classList.remove("hidden") : DOM.btnCallAccept.classList.add("hidden"); }
    }
    function hideCallUI() { DOM.callOverlay.classList.add("hidden"); clearInterval(state.callTimer); state.callSeconds = 0; }
    function startCallTimer() {
        clearInterval(state.callTimer); state.callSeconds = 0;
        state.callTimer = setInterval(() => { state.callSeconds++; DOM.callTimer.textContent = String(Math.floor(state.callSeconds/60)).padStart(2,"0")+":"+String(state.callSeconds%60).padStart(2,"0"); }, 1000);
    }
    function getPeerName(h) { const p = state.peers.find(p=>p.dest_hash===h); return p ? (p.display_name||h.substring(0,16)) : h.substring(0,16); }

    // ── Contact Management ──
    function showContactEdit() {
        if (!state.activePeer || !DOM.contactEditPanel) return;
        const peer = state.peers.find(p => p.dest_hash === state.activePeer);
        DOM.contactNickname.value = (peer && peer.nickname) || "";
        DOM.contactNotes.value = (peer && peer.notes) || "";
        DOM.contactEditPanel.classList.remove("hidden");
    }

    function hideContactEdit() {
        if (DOM.contactEditPanel) DOM.contactEditPanel.classList.add("hidden");
    }

    async function saveContact() {
        if (!state.activePeer) return;
        const nickname = DOM.contactNickname.value.trim();
        const notes = DOM.contactNotes.value.trim();

        try {
            await fetch(`/api/peers/${state.activePeer}/contact`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ nickname, notes }),
            });
            // Update local peer data
            const peer = state.peers.find(p => p.dest_hash === state.activePeer);
            if (peer) {
                peer.nickname = nickname;
                peer.notes = notes;
            }
            DOM.chatPeerName.textContent = nickname || (peer && peer.display_name) || state.activePeer.substring(0, 16);
            renderPeerList();
            hideContactEdit();
            showToast("Contact saved", "success");
        } catch (e) { console.error("Save contact failed:", e); }
    }

    async function togglePin() {
        if (!state.activePeer) return;
        const peer = state.peers.find(p => p.dest_hash === state.activePeer);
        const newPinned = !(peer && peer.pinned);

        try {
            await fetch(`/api/peers/${state.activePeer}/contact`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ pinned: newPinned }),
            });
            if (peer) peer.pinned = newPinned ? 1 : 0;
            if (DOM.btnPinPeer) {
                _updatePinIcon(newPinned);
            }
            renderPeerList();
        } catch (e) { console.error("Toggle pin failed:", e); }
    }

    function clearChat() {
        if (!state.activePeer) return;
        showConfirm(
            "Clear all messages in this conversation? This cannot be undone.",
            async () => {
                try {
                    await fetch(`/api/messages/${state.activePeer}/all`, { method: "DELETE" });
                    state.messages[state.activePeer] = [];
                    renderMessages(state.activePeer);
                    // Update unread badge in peer list immediately
                    const peer = state.peers.find(p => p.dest_hash === state.activePeer);
                    if (peer) { peer.unread_count = 0; renderPeerList(); }
                    showToast("Conversation cleared", "info");
                } catch(e) { console.error("Clear chat failed:", e); }
            },
            { title: "Clear Conversation", okLabel: "Clear All", okClass: "btn btn-danger" }
        );
    }

    // ── Remove Contact ────────────────────────────────────────────────────────
    // Permanently removes the peer from the contact list (and all their stored
    // messages and file transfer records).  The user is warned that this cannot
    // be undone.  Optionally, they can also block the peer at the same time.
    function removeContact() {
        if (!state.activePeer) return;
        const peer = state.peers.find(p => p.dest_hash === state.activePeer);
        const name = peer
            ? (peer.nickname || peer.display_name || state.activePeer.substring(0, 16) + "…")
            : state.activePeer.substring(0, 16) + "…";

        // Build a custom confirm dialog with an optional "block too" checkbox
        const dlg = document.createElement("div");
        dlg.className = "modal-overlay";
        dlg.style.cssText = "z-index:9999;";
        dlg.innerHTML = `
            <div class="modal-card" style="max-width:380px;">
                <div class="modal-header">
                    <h3 style="display:flex;align-items:center;gap:8px;font-size:15px;font-weight:700">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"
                             style="width:17px;height:17px;color:var(--red)">
                            <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
                            <circle cx="9" cy="7" r="4"/>
                            <line x1="17" y1="11" x2="23" y2="11"/>
                        </svg>
                        Remove ${escapeHtml(name)}?
                    </h3>
                </div>
                <div class="modal-body">
                    <p style="font-size:13px;color:var(--text-secondary);margin:0 0 12px;">
                        This will permanently delete <strong>${escapeHtml(name)}</strong> from your
                        contact list along with all their messages and file transfer history.
                        This cannot be undone.
                    </p>
                    <label style="display:flex;align-items:center;gap:8px;font-size:13px;
                                  color:var(--text-secondary);cursor:pointer;margin-bottom:16px;">
                        <input type="checkbox" id="_rmv_block_too" style="width:14px;height:14px;">
                        Also block this peer (prevent future messages)
                    </label>
                    <div style="display:flex;gap:8px;">
                        <button id="_rmv_cancel" class="btn" style="flex:1;justify-content:center;padding:0.5714rem 1rem;background:var(--bg-elev);color:var(--text-primary);border:1px solid var(--border-strong);border-radius:var(--radius-sm);font-weight:500;">Cancel</button>
                        <button id="_rmv_confirm" class="btn" style="flex:1;justify-content:center;padding:0.5714rem 1rem;background:#f8514933;color:#f85149;border:1px solid #f8514955;border-radius:var(--radius-sm);font-weight:700;">Remove</button>
                    </div>
                </div>
            </div>`;
        document.body.appendChild(dlg);

        new Promise(resolve => {
            dlg.querySelector("#_rmv_cancel").addEventListener("click",  () => { dlg.remove(); resolve(null); });
            dlg.querySelector("#_rmv_confirm").addEventListener("click", () => {
                const blockToo = dlg.querySelector("#_rmv_block_too").checked;
                dlg.remove();
                resolve(blockToo);
            });
        }).then(async blockToo => {
            if (blockToo === null) return;   // cancelled
            const peerHash = state.activePeer;
            try {
                // Optionally block first so any in-flight messages are dropped
                if (blockToo) {
                    await fetch(`/api/peers/${peerHash}/block`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ reason: "Blocked when removed" }),
                    });
                }
                // Delete the peer record (cascades: messages + file_transfers)
                const res = await fetch(`/api/peers/${peerHash}`, { method: "DELETE" });
                if (!res.ok) { showToast("Failed to remove contact", "error"); return; }

                // Close chat panel and return to welcome screen
                state.activePeer = null;
                delete state.messages[peerHash];
                const chatPanel = document.getElementById("chat-panel");
                const welcome   = document.getElementById("welcome-screen");
                if (chatPanel)  chatPanel.classList.add("hidden");
                if (welcome)    welcome.classList.remove("hidden");

                // Remove peer from local state and refresh list
                state.peers = state.peers.filter(p => p.dest_hash !== peerHash);
                renderPeerList();

                showToast(
                    blockToo ? `${name} removed and blocked` : `${name} removed from contacts`,
                    "info"
                );
            } catch(e) { console.error("Remove contact failed:", e); showToast("Failed to remove contact", "error"); }
        });
    }

    async function deleteMessage(messageId) {
        try {
            await fetch(`/api/messages/${messageId}`, { method: "DELETE" });
            // Remove from local state
            if (state.activePeer && state.messages[state.activePeer]) {
                state.messages[state.activePeer] = state.messages[state.activePeer].filter(m => m.id !== messageId);
                renderMessages(state.activePeer);
            }
        } catch(e) { console.error("Delete message failed:", e); }
    }

    // ── UI Scale ──────────────────────────────────────────────────────────────
    // Persists the scale factor (75–150 %) in localStorage and applies it as a
    // CSS custom property on <html>.  All rem/em units in the stylesheet respond
    // automatically because the root font-size is `calc(14px * var(--ui-scale))`.
    function initUIScale() {
        const input = document.getElementById("ui-scale-slider");  // now type=number
        if (!input) return;

        // Restore saved value (default 100)
        const saved = parseInt(localStorage.getItem("retimesh_ui_scale") || "100", 10);
        const clamped = Math.max(75, Math.min(150, isNaN(saved) ? 100 : saved));
        input.value = clamped;
        _applyUIScale(clamped);

        // Number input doesn't fire continuously like a slider — change/blur
        // only.  No rAF throttling needed.
        const commit = () => {
            let v = parseInt(input.value, 10);
            if (isNaN(v)) v = 100;
            v = Math.max(75, Math.min(150, v));
            input.value = v;
            _applyUIScale(v);
            localStorage.setItem("retimesh_ui_scale", String(v));
        };
        input.addEventListener("change", commit);
        input.addEventListener("blur", commit);
        input.addEventListener("keydown", e => {
            if (e.key === "Enter") { e.preventDefault(); input.blur(); }
        });
    }

    function _applyUIScale(pct) {
        // The stylesheet was rewritten to use rem units everywhere except
        // borders, shadows, and other things that should not scale.
        // Setting the html element's font-size scales every rem-sized
        // element proportionally — text, padding, gaps, sizes, all of it.
        //
        // We apply font-size directly (instead of just a CSS variable) so
        // it overrides the calc(14px * var(--ui-scale)) rule reliably.
        // Belt-and-suspenders: also set --ui-scale so any code that reads
        // it sees the same factor.
        document.documentElement.style.zoom = "";
        document.body.style.zoom = "";
        const factor = (pct / 100).toFixed(2);
        document.documentElement.style.setProperty("--ui-scale", factor);
        document.documentElement.style.fontSize = (14 * parseFloat(factor)) + "px";
    }

    // ── Dark / Light Theme Toggle ────────────────────────────────────────────
    function initTheme() {
        // Restore saved theme from localStorage
        const saved = localStorage.getItem("retimesh_theme") || "dark";
        document.documentElement.dataset.theme = saved;
        _updateThemeIcon(saved);

        const btn = document.getElementById("btn-theme-toggle");
        if (!btn) return;
        btn.addEventListener("click", () => {
            const current = document.documentElement.dataset.theme;
            const next = current === "dark" ? "light" : "dark";
            document.documentElement.dataset.theme = next;
            localStorage.setItem("retimesh_theme", next);
            _updateThemeIcon(next);
        });
    }

    // ── Wipe / Reset Data ────────────────────────────────────────────────
    // Confirmation-then-POST flow; wipes are not undoable so we make the
    // user explicitly approve.  After a successful wipe we reload the
    // page so all in-memory state matches the new DB.
    function initWipeData() {
        const btn   = document.getElementById("btn-wipe-data");
        const sel   = document.getElementById("wipe-scope-select");
        if (!btn || !sel) return;

        const SCOPE_LABELS = {
            messages: "ALL chat history",
            groups:   "ALL groups, group messages, and invites",
            peers:    "the peer list (peers will reappear on next announce)",
            alerts:   "ALL alerts",
            files:    "ALL files and transfer history",
            all:      "EVERYTHING: chats, groups, peers, alerts, files (identity + config kept)",
            nuclear:  "EVERYTHING INCLUDING your identity. You will need to set up again from scratch.",
        };

        btn.addEventListener("click", async () => {
            const scope = sel.value;
            const what  = SCOPE_LABELS[scope] || scope;
            if (!confirm(`Reset Data\n\nThis will permanently delete ${what}.\n\nThis cannot be undone. Continue?`)) {
                return;
            }
            btn.disabled = true;
            btn.textContent = "Resetting…";
            try {
                const res = await fetch("/api/wipe", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ scope }),
                });
                const data = await res.json();
                if (data.status === "ok") {
                    showToast(`Wiped: ${Object.keys(data.deleted || {}).join(", ") || scope}`, "success");
                    setTimeout(() => location.reload(), 800);
                } else {
                    showToast("Wipe failed: " + (data.error || "unknown"), "error");
                    btn.disabled = false;
                    btn.textContent = "Reset";
                }
            } catch (e) {
                showToast("Wipe failed: " + e.message, "error");
                btn.disabled = false;
                btn.textContent = "Reset";
            }
        });
    }
    function _updateThemeIcon(theme) {
        const btn = document.getElementById("btn-theme-toggle");
        if (!btn) return;
        if (theme === "dark") {
            btn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>`;
        } else {
            btn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>`;
        }
    }

    // ── Pin icon helper ──────────────────────────────────────────────────────
    function _updatePinIcon(pinned) {
        const btn = document.getElementById("btn-pin-peer");
        if (!btn) return;
        if (pinned) {
            // Pinned: solid pushpin filled with accent colour
            btn.innerHTML = `<svg viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" style="color:var(--accent)"><line x1="12" y1="17" x2="12" y2="22" stroke-width="2"/><path d="M5 17h14l-1.5-3v-5l2-2V4H5.5v3l2 2v5z"/></svg>`;
            btn.title = "Unpin contact";
        } else {
            // Unpinned: outline pushpin
            btn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="17" x2="12" y2="22"/><path d="M5 17h14l-1.5-3v-5l2-2V4H5.5v3l2 2v5z"/></svg>`;
            btn.title = "Pin contact";
        }
    }

    // ── Chat avatar (initials from name) ─────────────────────────────────────
    function _getInitials(name) {
        if (!name) return "?";
        const parts = name.trim().split(/\s+/);
        if (parts.length >= 2) return (parts[0][0] + parts[parts.length-1][0]).toUpperCase();
        return name.substring(0,2).toUpperCase();
    }
    function _updateChatAvatar(name) {
        const el = document.getElementById("chat-peer-avatar");
        if (el) el.textContent = _getInitials(name);
    }
    function _updateCallAvatar(name) {
        const el = document.getElementById("call-avatar");
        if (el) el.textContent = _getInitials(name);
    }

    // ── Message search ────────────────────────────────────────────────────────
    function initMessageSearch() {
        const btnOpen  = document.getElementById("btn-msg-search");
        const bar      = document.getElementById("msg-search-bar");
        const input    = document.getElementById("msg-search-input");
        const btnClose = document.getElementById("btn-msg-search-close");
        if (!btnOpen || !bar || !input || !btnClose) return;

        btnOpen.addEventListener("click", () => {
            bar.classList.toggle("hidden");
            if (!bar.classList.contains("hidden")) {
                input.focus();
                input.select();
            } else {
                input.value = "";
                _clearMessageSearch();
            }
        });
        btnClose.addEventListener("click", () => {
            bar.classList.add("hidden");
            input.value = "";
            _clearMessageSearch();
        });
        input.addEventListener("input", () => _runMessageSearch(input.value.trim()));
        input.addEventListener("keydown", e => {
            if (e.key === "Escape") {
                bar.classList.add("hidden");
                input.value = "";
                _clearMessageSearch();
            }
        });
    }
    /**
     * F-5: highlight search matches using a TreeWalker over TEXT NODES only.
     *
     * The old approach used `bubble.innerHTML = bubble.textContent.replace(...)`,
     * which serialised the entire bubble's content as plain text, wiping any HTML
     * elements inside it (file attachment links, SVG icons, etc.).
     *
     * The TreeWalker approach:
     *   1. Walk only leaf text nodes — never touches elements.
     *   2. For each matching text node, split it into three sibling nodes:
     *      [pre-match text] [<mark>match</mark>] [post-match text]
     *   3. Mark each inserted <mark> with data-search-mark="1" so _clearMessageSearch
     *      can find and remove them cleanly.
     */
    function _runMessageSearch(query) {
        _clearMessageSearch();   // always reset before re-highlighting
        if (!query) return;
        const lq  = query.toLowerCase();
        const esc = query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const re  = new RegExp(esc, "gi");

        // Operate on the whole messages container — works with the new incremental renderer
        const container = document.getElementById("messages-list") || DOM.messagesList;
        if (!container) return;

        const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT, null);
        const textNodes = [];
        let node;
        while ((node = walker.nextNode())) {
            // Skip text inside existing <mark> tags (prevents double-wrapping)
            if (node.parentElement && node.parentElement.dataset.searchMark) continue;
            if (node.textContent.toLowerCase().includes(lq)) textNodes.push(node);
        }

        // Work on a snapshot — modifying the DOM while walking can skip nodes
        textNodes.forEach(tn => {
            const frag = document.createDocumentFragment();
            let remaining = tn.textContent;
            let match;
            re.lastIndex = 0;
            let lastIdx = 0;
            while ((match = re.exec(remaining)) !== null) {
                if (match.index > lastIdx) {
                    frag.appendChild(document.createTextNode(remaining.slice(lastIdx, match.index)));
                }
                const mark = document.createElement("mark");
                mark.dataset.searchMark = "1";
                mark.textContent = match[0];
                frag.appendChild(mark);
                lastIdx = match.index + match[0].length;
            }
            if (lastIdx < remaining.length) {
                frag.appendChild(document.createTextNode(remaining.slice(lastIdx)));
            }
            tn.parentNode.replaceChild(frag, tn);
        });

        // Show/hide message bubbles based on whether they contain a mark
        (container.querySelectorAll ? container : document).querySelectorAll(".message").forEach(el => {
            const hasMatch = el.querySelector("[data-search-mark]");
            el.style.display = hasMatch ? "" : "none";
            if (hasMatch) el.classList.add("msg-highlight");
        });
    }

    function _clearMessageSearch() {
        // Unwrap all <mark data-search-mark> elements, restoring raw text nodes
        const container = document.getElementById("messages-list") || DOM.messagesList;
        if (!container) return;
        container.querySelectorAll("[data-search-mark]").forEach(mark => {
            const parent = mark.parentNode;
            if (!parent) return;
            parent.replaceChild(document.createTextNode(mark.textContent), mark);
            parent.normalize();   // merge adjacent text nodes
        });
        // Restore visibility
        container.querySelectorAll(".message").forEach(el => {
            el.style.display = "";
            el.classList.remove("msg-highlight");
        });
        // NOTE: do NOT clear input.value here.  This function is called from
        // _runMessageSearch on every keystroke to reset the highlight before
        // re-applying it.  Wiping the input on every keystroke caused only
        // the most recent character to ever appear.  The input is cleared
        // explicitly by the open-toggle and close-button handlers instead.
    }

    // ── Peer search / filter ──────────────────────────────────────────────────
    function initPeerSearch() {
        const input = document.getElementById("peer-search-input");
        if (!input) return;
        input.addEventListener("input", () => {
            const q = input.value.toLowerCase().trim();
            document.querySelectorAll("#peer-list .peer-item").forEach(item => {
                const name = (item.querySelector(".peer-name")?.textContent || "").toLowerCase();
                const hash = (item.querySelector(".peer-hash-short")?.textContent || "").toLowerCase();
                item.style.display = (!q || name.includes(q) || hash.includes(q)) ? "" : "none";
            });
        });
    }

    // ── Quick Peer Switcher (Ctrl+K) ──────────────────────────────────────────
    function initQuickSwitcher() {
        const overlay = document.getElementById("quick-switcher-overlay");
        const input   = document.getElementById("quick-switcher-input");
        const list    = document.getElementById("quick-switcher-list");
        if (!overlay || !input || !list) return;

        document.addEventListener("keydown", e => {
            if ((e.ctrlKey || e.metaKey) && e.key === "k") {
                e.preventDefault();
                overlay.classList.toggle("hidden");
                if (!overlay.classList.contains("hidden")) {
                    input.value = "";
                    _renderQuickSwitcher("");
                    input.focus();
                }
            }
            if (e.key === "Escape" && !overlay.classList.contains("hidden")) {
                overlay.classList.add("hidden");
            }
        });
        overlay.addEventListener("click", e => {
            if (e.target === overlay) overlay.classList.add("hidden");
        });
        input.addEventListener("input", () => _renderQuickSwitcher(input.value.trim()));
        input.addEventListener("keydown", e => {
            if (e.key === "ArrowDown" || e.key === "ArrowUp") {
                e.preventDefault();
                const items = list.querySelectorAll(".quick-switcher-item");
                const focused = list.querySelector(".quick-switcher-item.focused");
                const idx = Array.from(items).indexOf(focused);
                items.forEach(i => i.classList.remove("focused"));
                let next = e.key === "ArrowDown" ? idx + 1 : idx - 1;
                next = Math.max(0, Math.min(items.length - 1, next));
                if (items[next]) items[next].classList.add("focused");
            }
            if (e.key === "Enter") {
                const focused = list.querySelector(".quick-switcher-item.focused") || list.querySelector(".quick-switcher-item");
                if (focused) { focused.click(); overlay.classList.add("hidden"); }
            }
        });
    }
    function _renderQuickSwitcher(query) {
        const list = document.getElementById("quick-switcher-list");
        if (!list) return;
        const q = query.toLowerCase();
        const filtered = state.peers.filter(p => {
            const name = (p.nickname || p.display_name || p.dest_hash || "").toLowerCase();
            return !q || name.includes(q) || p.dest_hash.includes(q);
        }).slice(0, 8);
        if (!filtered.length) {
            list.innerHTML = `<div class="quick-switcher-empty">No peers found</div>`;
            return;
        }
        list.innerHTML = filtered.map((p, i) => {
            const name = p.nickname || p.display_name || p.dest_hash.substring(0,12);
            const initials = _getInitials(name);
            return `<div class="quick-switcher-item${i===0?' focused':''}" data-hash="${p.dest_hash}">
                <div class="peer-avatar">${initials}</div>
                <div>${name}</div>
                <div style="font-family:var(--font-mono);font-size:10px;color:var(--text-muted);margin-left:auto">${p.dest_hash.substring(0,8)}…</div>
            </div>`;
        }).join("");
        list.querySelectorAll(".quick-switcher-item").forEach(item => {
            item.addEventListener("click", () => {
                const overlay = document.getElementById("quick-switcher-overlay");
                if (overlay) overlay.classList.add("hidden");
                selectPeer(item.dataset.hash);
            });
        });
    }

    // ── Notification sounds ───────────────────────────────────────────────────
    const _sounds = { enabled: true };
    function initSounds() {
        const toggle = document.getElementById("toggle-sounds");
        if (toggle) {
            const saved = localStorage.getItem("retimesh_sounds");
            _sounds.enabled = saved !== "false";
            toggle.checked = _sounds.enabled;
            toggle.addEventListener("change", () => {
                _sounds.enabled = toggle.checked;
                localStorage.setItem("retimesh_sounds", toggle.checked);
            });
        }
        // Voice-note quality selector — same settings block, so share init
        const qSel = document.getElementById("voice-quality-select");
        if (qSel) {
            qSel.value = _getVoiceQuality();
            qSel.addEventListener("change", () => _setVoiceQuality(qSel.value));
        }
        // Peer-offline threshold (minutes) — persisted in localStorage and
        // consumed by isPeerOnline() on every render, so changes take effect
        // without a reload.
        const offEl = document.getElementById("offline-threshold-input");
        if (offEl) {
            const saved = parseInt(localStorage.getItem("retimesh_offline_threshold_min") || "5", 10);
            offEl.value = (isFinite(saved) && saved > 0) ? saved : 5;
            offEl.addEventListener("change", () => {
                let v = parseInt(offEl.value, 10);
                if (!isFinite(v) || v < 1)    v = 1;
                if (v > 1440)                 v = 1440;
                offEl.value = v;
                localStorage.setItem("retimesh_offline_threshold_min", String(v));
                // Re-render peer list so online/offline dots update right away
                if (typeof renderPeerList === "function") renderPeerList();
            });
        }
    }
    // Q-4: Singleton AudioContext for notification sounds.
    // Browsers cap concurrent AudioContexts at ~6.  Creating a new one per
    // notification (the old pattern) silently exhausts that limit after a few
    // messages, causing all subsequent notification sounds to be skipped.
    // Re-using one context avoids the limit entirely.
    let _notifCtx = null;
    function _getNotifCtx() {
        if (!_notifCtx || _notifCtx.state === "closed") {
            try {
                _notifCtx = new (window.AudioContext || window.webkitAudioContext)();
            } catch(e) { return null; }
        }
        if (_notifCtx.state === "suspended") {
            _notifCtx.resume().catch(() => {});
        }
        return _notifCtx;
    }

    function playNotificationSound() {
        if (!_sounds.enabled) return;
        try {
            const ctx = _getNotifCtx();
            if (!ctx) return;
            const osc  = ctx.createOscillator();
            const gain = ctx.createGain();
            osc.connect(gain); gain.connect(ctx.destination);
            osc.frequency.setValueAtTime(880, ctx.currentTime);
            osc.frequency.exponentialRampToValueAtTime(660, ctx.currentTime + 0.08);
            gain.gain.setValueAtTime(0.12, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.18);
            osc.start(ctx.currentTime);
            osc.stop(ctx.currentTime + 0.18);
        } catch(e) { /* no audio context */ }
    }

    // ── Global keyboard shortcuts ─────────────────────────────────────────────
    function initKeyboardShortcuts() {
        document.addEventListener("keydown", e => {
            // / → focus address bar (when browse panel is visible)
            if (e.key === "/" && !["INPUT","TEXTAREA"].includes(document.activeElement.tagName)) {
                const addrInput = document.getElementById("page-address-input");
                const pagesMain = document.getElementById("pages-main");
                if (addrInput && pagesMain && !pagesMain.classList.contains("hidden")) {
                    e.preventDefault();
                    addrInput.focus();
                }
            }
            // Esc → close any open modal
            if (e.key === "Escape") {
                const modals = document.querySelectorAll(".modal-overlay:not(.hidden), .wizard-overlay:not(.hidden)");
                if (modals.length) { modals[modals.length-1].classList.add("hidden"); }
                // Also close quick switcher
                const qs = document.getElementById("quick-switcher-overlay");
                if (qs && !qs.classList.contains("hidden")) qs.classList.add("hidden");
            }
        });
    }

    // ── Pages: hosting & browsing ─────────────────────────────────────────
    let _pagesData     = [];    // local pages cache
    let _editingPageId = null;  // currently editing page ID (null = new)

    /* Tab switching inside the Pages main panel */
    function initPagesTabs() {
        // Top-level tabs: Browse | Edit Page
        const tabs   = document.querySelectorAll(".pages-tab[data-ptab]");
        const panels = { browse: document.getElementById("ppanel-browse"), host: document.getElementById("ppanel-host") };
        tabs.forEach(tab => {
            tab.addEventListener("click", () => {
                tabs.forEach(t => t.classList.remove("active"));
                tab.classList.add("active");
                Object.values(panels).forEach(p => { if(p) p.classList.add("hidden"); });
                const target = panels[tab.dataset.ptab];
                if (target) target.classList.remove("hidden");
                if (tab.dataset.ptab === "host") loadMyPages();
            });
        });

        // Sub-tabs inside "Edit Page" panel: My Pages | New Page
        const etabs   = document.querySelectorAll(".page-editor-tab[data-etab]");
        const epanels = {
            "my-pages": document.getElementById("epanel-my-pages"),
            "new-page":  document.getElementById("epanel-new-page"),
        };
        etabs.forEach(tab => {
            tab.addEventListener("click", () => {
                etabs.forEach(t => t.classList.remove("active"));
                tab.classList.add("active");
                Object.values(epanels).forEach(p => { if(p) p.classList.add("hidden"); });
                const target = epanels[tab.dataset.etab];
                if (target) target.classList.remove("hidden");
            });
        });

        // Close button on inline editor card
        const editCloseBtn = document.getElementById("btn-page-edit-close");
        if (editCloseBtn) {
            editCloseBtn.addEventListener("click", () => {
                const card = document.getElementById("page-edit-card");
                if (card) card.classList.add("hidden");
                _editingPageId = null;
            });
        }

        // New Page form: format hint
        const newTypeSel = document.getElementById("pe-new-content-type");
        const newHintEl  = document.getElementById("pe-new-format-hint");
        if (newTypeSel && newHintEl) {
            newTypeSel.addEventListener("change", () => {
                newHintEl.textContent = FORMAT_HINTS[newTypeSel.value] || "";
            });
            newHintEl.textContent = FORMAT_HINTS[newTypeSel.value] || "";
        }

        // New Page save button
        const btnNewSave = document.getElementById("btn-new-page-save");
        if (btnNewSave) btnNewSave.addEventListener("click", saveNewPage);
    }

    /* Load and render the user's hosted pages in the sidebar */
    async function loadMyPages() {
        try {
            const res  = await fetch("/api/pages");
            _pagesData = await res.json();
        } catch(_) { _pagesData = []; }

        // ── Sidebar list (legacy) ──
        const listEl = DOM.myPagesList;
        if (listEl) {
            if (!_pagesData.length) {
                listEl.innerHTML = '<div class="settings-empty">No pages yet.</div>';
            } else {
                listEl.innerHTML = _pagesData.map(p =>
                    `<div class="page-list-item" data-id="${p.id}">
                        <div class="page-list-item-info">
                            <div class="page-list-item-title">${escapeHtml(p.title)}</div>
                            <div class="page-list-item-path">${escapeHtml(p.path)}</div>
                        </div>
                        <div class="page-list-actions">
                            <button class="btn-icon-xs" data-edit="${p.id}" title="Edit page"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg></button>
                        </div>
                    </div>`
                ).join("");
                listEl.querySelectorAll("[data-edit]").forEach(btn => {
                    btn.addEventListener("click", () => openPageEditor(parseInt(btn.dataset.edit)));
                });
            }
        }

        // ── Editor panel "My Pages" inner list ──
        const innerEl = document.getElementById("my-pages-list-inner");
        if (innerEl) {
            if (!_pagesData.length) {
                innerEl.innerHTML = `<div class="page-list-empty">
                    No pages yet — click <strong>＋ New Page</strong> to create one.
                </div>`;
            } else {
                innerEl.innerHTML = _pagesData.map(p =>
                    `<div class="page-list-item" data-id="${p.id}">
                        <div class="page-list-item-info">
                            <div class="page-list-item-title">${escapeHtml(p.title)}</div>
                            <div class="page-list-item-path">${escapeHtml(p.path)}</div>
                        </div>
                        <div class="page-list-actions">
                            <button class="btn btn-secondary btn-sm" data-edit="${p.id}" title="Edit">Edit</button>
                        </div>
                    </div>`
                ).join("");
                innerEl.querySelectorAll("[data-edit]").forEach(btn => {
                    btn.addEventListener("click", () => openPageEditor(parseInt(btn.dataset.edit)));
                });
            }
        }

        // Populate legacy select (hidden but kept for back-compat)
        if (DOM.pageSelect) {
            DOM.pageSelect.innerHTML = '<option value="">— New page —</option>' +
                _pagesData.map(p => `<option value="${p.id}">${escapeHtml(p.title)} (${escapeHtml(p.path)})</option>`).join("");
        }

        // Load page hash from network status
        try {
            const st = await fetch("/api/network/status").then(r => r.json());
            if (DOM.pageHostAddress && st.page_hash) {
                DOM.pageHostAddress.textContent = st.page_hash;
                DOM.pageHostAddress.title = "Click to copy";
                DOM.pageHostAddress.style.cursor = "pointer";
                DOM.pageHostAddress.onclick = () => {
                    navigator.clipboard.writeText(st.page_hash).then(() => showToast("Hash copied", "info"));
                };
            }
        } catch(_) {}
    }

    /* Load discovered NomadNet nodes into sidebar */
    async function loadNomadnetNodes() {
        try {
            const nodes = await fetch("/api/pages/nodes").then(r => r.json());
            const el = DOM.nomadnetNodesList;
            if (!el) return;
            if (!nodes.length) {
                el.innerHTML = '<div class="settings-empty">No hosts discovered yet.<br>Announce to find NomadNet nodes.</div>';
            } else {
                el.innerHTML = nodes.map(n =>
                    `<div class="known-node-item" data-hash="${escapeHtml(n.hash)}" title="Browse this node's pages">
                        <div>
                            <div class="known-node-name">${escapeHtml(n.name || n.hash.substring(0,16))}</div>
                            <div class="known-node-hash">${n.hash.substring(0, 24)}…</div>
                        </div>
                        <span style="color:var(--accent);font-size:11px;">Browse →</span>
                    </div>`
                ).join("");
                el.querySelectorAll(".known-node-item").forEach(item => {
                    item.addEventListener("click", () => browseNodeHash(item.dataset.hash));
                });
            }
        } catch(_) {}
    }

    /* Open page editor for a specific page (or clear for new) */
    function openPageEditor(pageId) {
        // Switch to Pages tab then to host sub-tab > My Pages sub-tab
        if (DOM.navPages && !DOM.navPages.classList.contains("active")) DOM.navPages.click();
        const hostTab = document.querySelector("[data-ptab='host']");
        if (hostTab && !hostTab.classList.contains("active")) hostTab.click();
        const myPagesTab = document.querySelector("[data-etab='my-pages']");
        if (myPagesTab && !myPagesTab.classList.contains("active")) myPagesTab.click();

        const card = document.getElementById("page-edit-card");

        if (pageId) {
            const page = _pagesData.find(p => p.id === pageId);
            if (!page) return;
            _editingPageId = pageId;
            if (DOM.peTitle)   DOM.peTitle.value   = page.title;
            if (DOM.pePath)    DOM.pePath.value     = page.path;
            if (DOM.peContent) DOM.peContent.value  = page.content;
            if (DOM.peContentType) DOM.peContentType.value = page.content_type || "mu";
            if (DOM.pageSelect) DOM.pageSelect.value = String(pageId);
            if (DOM.btnPageDelete) DOM.btnPageDelete.style.display = "";
            // Update card header title
            const cardTitle = document.getElementById("page-edit-card-title");
            if (cardTitle) cardTitle.textContent = page.title;
            if (card) card.classList.remove("hidden");
            _updateFormatHint();
            // Scroll the card into view
            if (card) card.scrollIntoView({ behavior: "smooth", block: "start" });
        } else {
            if (card) card.classList.add("hidden");
            _newPageEditor();
        }
    }

    const FORMAT_HINTS = {
        mu:   "Micron markup, Reticulum/NomadNet native format. Use `>` for headings, `!` for bold, `/` for italic, `_` for underline. Browseable by NomadNet clients.",
        html: "HTML, rendered in the browser. Use standard HTML tags. Not displayable natively by NomadNet clients.",
        text: "Plain text, no formatting. Readable by all clients.",
    };
    function _updateFormatHint() {
        if (!DOM.peFormatHint || !DOM.peContentType) return;
        DOM.peFormatHint.textContent = FORMAT_HINTS[DOM.peContentType.value] || "";
    }

    function _newPageEditor() {
        _editingPageId = null;
        if (DOM.peTitle)    DOM.peTitle.value    = "";
        if (DOM.pePath)     DOM.pePath.value     = "/index";
        if (DOM.peContent)  DOM.peContent.value  = "";
        if (DOM.peContentType) DOM.peContentType.value = "mu";
        if (DOM.pageSelect) DOM.pageSelect.value = "";
        if (DOM.btnPageDelete) DOM.btnPageDelete.style.display = "none";
        _updateFormatHint();
        if (DOM.peTitle) DOM.peTitle.focus();
    }

    async function savePage() {
        const title       = (DOM.peTitle?.value        || "").trim();
        const path        = (DOM.pePath?.value          || "").trim();
        const content     = (DOM.peContent?.value       || "").trim();
        const content_type = (DOM.peContentType?.value  || "mu");
        if (!title) { showToast("Please enter a page title", "warning"); return; }
        if (!path)  { showToast("Please enter a path (e.g. /index)", "warning"); return; }

        try {
            let res;
            if (_editingPageId) {
                res = await fetch(`/api/pages/${_editingPageId}`, {
                    method: "PUT",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ title, path, content, content_type }),
                });
            } else {
                res = await fetch("/api/pages", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ title, path, content, content_type }),
                });
                const data = await res.json();
                if (data.id) _editingPageId = data.id;
            }
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                showToast(err.error || "Save failed", "error");
                return;
            }
            if (DOM.pageEditorStatus) {
                DOM.pageEditorStatus.textContent = "✓ Saved";
                setTimeout(() => { if(DOM.pageEditorStatus) DOM.pageEditorStatus.textContent = ""; }, 2500);
            }
            if (DOM.btnPageDelete) DOM.btnPageDelete.style.display = "";
            showToast("Page saved", "info");
            await loadMyPages();
        } catch(e) {
            showToast("Save error: " + e.message, "error");
        }
    }

    function deletePage() {
        if (!_editingPageId) return;
        showConfirm(
            "Delete this page? This cannot be undone.",
            async () => {
                try {
                    await fetch(`/api/pages/${_editingPageId}`, { method: "DELETE" });
                    const card = document.getElementById("page-edit-card");
                    if (card) card.classList.add("hidden");
                    _editingPageId = null;
                    showToast("Page deleted", "info");
                    await loadMyPages();
                } catch(e) {
                    showToast("Delete error: " + e.message, "error");
                }
            },
            { title: "Delete Page", okLabel: "Delete", okClass: "btn btn-danger" }
        );
    }

    /* Create a brand-new page from the "New Page" sub-tab */
    async function saveNewPage() {
        const titleEl   = document.getElementById("pe-new-title");
        const pathEl    = document.getElementById("pe-new-path");
        const contentEl = document.getElementById("pe-new-content");
        const typeEl    = document.getElementById("pe-new-content-type");
        const statusEl  = document.getElementById("page-new-editor-status");

        const title        = (titleEl?.value   || "").trim();
        const path         = (pathEl?.value     || "").trim() || "/index";
        const content      = (contentEl?.value  || "").trim();
        const content_type = (typeEl?.value     || "mu");

        if (!title) { showToast("Please enter a page title", "warning"); return; }

        try {
            const res = await fetch("/api/pages", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ title, path, content, content_type }),
            });
            const data = await res.json();
            if (!res.ok) { showToast(data.error || "Create failed", "error"); return; }

            showToast("Page created!", "success");
            if (statusEl) {
                statusEl.textContent = "✓ Created";
                setTimeout(() => { if (statusEl) statusEl.textContent = ""; }, 2500);
            }
            // Clear the new-page form
            if (titleEl)   titleEl.value   = "";
            if (contentEl) contentEl.value = "";
            if (pathEl)    pathEl.value    = "/index";

            // Switch to My Pages tab and open the new page in the editor
            await loadMyPages();
            const myPagesTab = document.querySelector("[data-etab='my-pages']");
            if (myPagesTab) myPagesTab.click();
            if (data.id) openPageEditor(data.id);
        } catch(e) {
            showToast("Create error: " + e.message, "error");
        }
    }

    /* Browse a remote node's page */
    async function browsePage() {
        const raw   = (DOM.pageAddressInput?.value || "").trim();
        if (!raw) { showToast("Enter a node hash to browse", "warning"); return; }

        // Parse: "hash:/path", "hash /path", "hash/path", or just "hash"
        let hash = raw, path = "/index";
        const colonSlash = raw.indexOf(":/");
        if (colonSlash > 0) {
            hash = raw.substring(0, colonSlash).trim();
            path = raw.substring(colonSlash + 1).trim();
        } else {
            const spaceIdx = raw.indexOf(" ");
            if (spaceIdx > 0) {
                hash = raw.substring(0, spaceIdx).trim();
                path = raw.substring(spaceIdx + 1).trim() || "/index";
            } else {
                // Natural form: "hash/path" — look for a single slash that's
                // preceded by something that looks like a hex hash (32+ chars).
                // Hex hashes are 32 chars exactly, so the first slash after
                // position 30+ marks the start of the path.
                const slashIdx = raw.indexOf("/");
                if (slashIdx >= 32) {
                    hash = raw.substring(0, slashIdx).trim();
                    path = raw.substring(slashIdx).trim() || "/index";
                }
            }
        }
        hash = hash.replace(/^0x/, "").toLowerCase();

        browseNodeHash(hash, path);
    }

    // Rewrite every CSS selector inside a remote page's <style> blocks so
    // the styles only apply inside the given scope (e.g. ".remote-page-root"),
    // not to the whole document.  Also strips <html>/<body>/<head> tags so
    // their content lifts up into the scope container.
    function _scopeHtmlStyles(html, scope) {
        // Rewrite a single stylesheet body.  Walks top-level rules only —
        // does not descend into @media (those are left as-is, their inner
        // rules ride along on whatever the outer scope says, which is fine
        // for our purposes).
        function rewriteCss(css) {
            return css.replace(/([^{}]+)\{([^{}]*)\}/g, (match, selectorList, body) => {
                // Skip @-rules (keyframes, font-face, media, etc) — leave intact
                const trimmed = selectorList.trim();
                if (trimmed.startsWith("@")) return match;

                const scoped = trimmed.split(",").map(sel => {
                    const s = sel.trim();
                    if (!s) return "";
                    // body/html/:root → the scope container itself
                    if (/^(body|html|:root)$/i.test(s)) return scope;
                    if (/^(body|html|:root)\b/i.test(s)) {
                        return s.replace(/^(body|html|:root)\b/i, scope);
                    }
                    // Everything else: descendant of scope
                    return scope + " " + s;
                }).filter(Boolean).join(", ");

                return scoped + " {" + body + "}";
            });
        }

        // Rewrite <style>...</style> blocks
        let out = html.replace(/<style\b[^>]*>([\s\S]*?)<\/style>/gi,
            (_, css) => "<style>" + rewriteCss(css) + "</style>");

        // Strip DOCTYPE, html, head, body tags — their contents stay in place.
        // Keep title out of the rendered body (it's already shown above).
        out = out.replace(/<!DOCTYPE[^>]*>/gi, "")
                 .replace(/<\/?(html|body)\b[^>]*>/gi, "")
                 .replace(/<head\b[^>]*>([\s\S]*?)<\/head>/gi, (_, h) => {
                     // Keep <style> and <link> from head; drop <title>, <meta>, etc.
                     const styles = (h.match(/<style\b[^>]*>[\s\S]*?<\/style>/gi) || []).join("");
                     const links  = (h.match(/<link\b[^>]*>/gi) || []).join("");
                     return styles + links;
                 });
        return out;
    }

    // ── Page Bookmarks ──
    // state.bookmarks: array of {id, node_hash, path, title, added}
    // state.currentBrowseHash / state.currentBrowsePath: last loaded page

    async function loadBookmarks() {
        try {
            const res = await fetch("/api/bookmarks");
            state.bookmarks = await res.json();
        } catch(e) { state.bookmarks = []; }
        renderBookmarksList();
        _updateBookmarkBtn();
    }

    function renderBookmarksList() {
        const el = document.getElementById("bookmarks-list");
        if (!el) return;
        const bm = state.bookmarks || [];
        if (!bm.length) {
            el.innerHTML = '<div class="bookmark-empty">No bookmarks yet.<br>Browse a page and press the bookmark button to save it.</div>';
            return;
        }
        el.innerHTML = bm.map(b => {
            const label = escapeHtml(b.title || b.path);
            const sub   = escapeHtml(b.node_hash.substring(0,16) + "… " + b.path);
            return `<div class="bookmark-item" data-hash="${b.node_hash}" data-path="${escapeHtml(b.path)}">
                <div class="bookmark-item-info">
                    <div class="bookmark-item-title">${label}</div>
                    <div class="bookmark-item-sub">${sub}</div>
                </div>
                <button class="btn-bookmark-del" data-id="${b.id}" title="Remove bookmark">✕</button>
            </div>`;
        }).join("");
        el.querySelectorAll(".bookmark-item").forEach(row => {
            row.addEventListener("click", e => {
                if (e.target.classList.contains("btn-bookmark-del")) return;
                browseNodeHash(row.dataset.hash, row.dataset.path);
            });
        });
        el.querySelectorAll(".btn-bookmark-del").forEach(btn => {
            btn.addEventListener("click", async e => {
                e.stopPropagation();
                await fetch(`/api/bookmarks/${btn.dataset.id}`, { method: "DELETE" });
                await loadBookmarks();
            });
        });
    }

    function _updateBookmarkBtn() {
        const btn = document.getElementById("btn-bookmark-page");
        if (!btn) return;
        const hash = state.currentBrowseHash;
        const path = state.currentBrowsePath || "/index";
        if (!hash) { btn.style.display = "none"; return; }
        btn.style.display = "";
        const saved = (state.bookmarks || []).some(b => b.node_hash === hash && b.path === path);
        btn.innerHTML    = saved
            ? `<svg viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="1.75" style="color:var(--yellow)"><path d="m19 21-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>`
            : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"><path d="m19 21-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>`;
        btn.title        = saved ? "Remove bookmark" : "Bookmark this page";
        btn.dataset.saved = saved ? "1" : "0";
    }

    async function toggleBookmark() {
        const hash = state.currentBrowseHash;
        const path = state.currentBrowsePath || "/index";
        if (!hash) return;
        const existing = (state.bookmarks || []).find(b => b.node_hash === hash && b.path === path);
        if (existing) {
            await fetch(`/api/bookmarks/${existing.id}`, { method: "DELETE" });
        } else {
            const title = document.querySelector("#page-viewer .page-viewer-title")?.textContent || path;
            await fetch("/api/bookmarks", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ node_hash: hash, path, title }),
            });
        }
        await loadBookmarks();
    }

    async function browseNodeHash(hash, path = "/index") {
        // Track current location for bookmark button
        state.currentBrowseHash = hash;
        state.currentBrowsePath = path;
        // Show loading state
        if (DOM.pageAddressInput) DOM.pageAddressInput.value = hash + (path !== "/index" ? ":" + path : "");
        if (DOM.pageViewer) {
            DOM.pageViewer.innerHTML = `<div class="page-viewer-empty"><div style="font-size:28px;margin-bottom:8px;">⏳</div><div style="color:var(--text-muted)">Connecting to node…<br><span style="font-size:11px;font-family:var(--font-mono)">${hash.substring(0,20)}…</span></div></div>`;
        }

        // Switch to browse tab and pages main panel
        if (DOM.navPages && !DOM.navPages.classList.contains("active")) DOM.navPages.click();
        const browseTab = document.querySelector("[data-ptab='browse']");
        if (browseTab && !browseTab.classList.contains("active")) browseTab.click();

        try {
            const res  = await fetch(`/api/pages/browse?hash=${encodeURIComponent(hash)}&path=${encodeURIComponent(path)}`);
            const data = await res.json();

            if (data.error) {
                if (DOM.pageViewer) DOM.pageViewer.innerHTML =
                    `<div class="page-viewer-empty"><div style="font-size:28px;margin-bottom:8px;">❌</div><div style="color:var(--red)">${escapeHtml(data.error)}</div></div>`;
                return;
            }

            // Render page content.  Trust the server's declared type first;
            // fall back to sniffing for tags if no type was sent.
            let rendered = data.content || "";
            const declaredType = (data.type || "").toLowerCase();

            let isHtml;
            if (declaredType === "html") {
                isHtml = true;
            } else if (declaredType === "text" || declaredType === "plain") {
                isHtml = false;
            } else {
                // Unknown / legacy — sniff
                isHtml = /<[a-z][^>]*>/i.test(rendered);
            }

            const pageTitle = data.title || "Untitled";
            // ── Store in browse history ──
            fetch("/api/history", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ node_hash: hash, path, title: pageTitle }),
            }).catch(() => {});

            // Cache content for offline save
            state.currentPageContent      = rendered;
            state.currentPageTitle        = pageTitle;
            state.currentPageContentType  = declaredType || (isHtml ? "html" : "text");

            if (DOM.pageViewer) {
                const titleHtml = `<div class="page-viewer-title">${escapeHtml(pageTitle)}</div>`;
                const metaHtml  = `<div class="page-viewer-meta">Source: ${escapeHtml(hash.substring(0,20))}… · Path: ${escapeHtml(path)}</div>`;

                if (isHtml) {
                    // Detect whether the original page contained any active
                    // content before we strip it, so we can warn the user
                    // when we render a partial / static-only version.  Pages
                    // come from arbitrary mesh peers, so executing their JS
                    // or honouring their inline event handlers is a real
                    // security risk — we strip both unconditionally.
                    const hadScripts  = /<script\b/i.test(rendered);
                    const hadHandlers = /\bon\w+\s*=/i.test(rendered);
                    const hadActive   = hadScripts || hadHandlers;

                    const safeContent = rendered.replace(/<script[\s\S]*?<\/script>/gi, "")
                                                 .replace(/on\w+\s*=/gi, "data-blocked=");
                    const scoped = _scopeHtmlStyles(safeContent, ".remote-page-root");

                    // Banner explaining what was stripped.  Without this the
                    // viewer just shows a "blank" page when the original
                    // relied on JS to populate its content (e.g. wikis,
                    // SPAs, JS-built tables of contents) and users can't
                    // tell whether the page is actually empty or whether
                    // the browser blocked something.
                    const noticeHtml = hadActive
                        ? `<div class="page-script-notice">
                               <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" aria-hidden="true"><path d="M12 9v4M12 17h.01"/><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/></svg>
                               <div class="page-script-notice-text">
                                   <strong>Scripts blocked for security.</strong>
                                   This page came from a remote mesh peer, so its <code>&lt;script&gt;</code> blocks and inline event handlers (<code>onclick</code>, <code>oninput</code>, …) were stripped before rendering. Pages that build their content with JavaScript will appear partially or fully blank — only the static HTML is shown below.
                               </div>
                           </div>`
                        : "";

                    DOM.pageViewer.innerHTML =
                        titleHtml +
                        noticeHtml +
                        `<div class="remote-page-root">${scoped}</div>` +
                        metaHtml;
                } else {
                    DOM.pageViewer.innerHTML =
                        titleHtml +
                        `<div class="page-viewer-content">${escapeHtml(rendered).replace(/\n/g, "<br>")}</div>` +
                        metaHtml;
                }
            }
            // Show "save offline" button after successful load
            const btnSave = document.getElementById("btn-save-offline");
            if (btnSave) btnSave.style.display = "";
        } catch(e) {
            if (DOM.pageViewer) DOM.pageViewer.innerHTML =
                `<div class="page-viewer-empty"><div style="color:var(--red)">Error: ${escapeHtml(e.message)}</div></div>`;
        }
        _updateBookmarkBtn();
        _refreshBrowserSidebar();
    }

    async function savePageOffline() {
        const hash    = state.currentBrowseHash;
        const path    = state.currentBrowsePath || "/index";
        const content = state.currentPageContent;
        const title   = state.currentPageTitle || path;
        const ctype   = state.currentPageContentType || "text";
        if (!hash || !content) { showToast("No page loaded to save", "warning"); return; }
        try {
            const res = await fetch("/api/saved_pages", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ node_hash: hash, path, title, content, content_type: ctype }),
            });
            if (!res.ok) throw new Error((await res.json()).error || "Save failed");
            showToast("Page saved offline ✓", "success");
            _refreshBrowserSidebar();
        } catch(e) {
            showToast("Save failed: " + e.message, "error");
        }
    }

    // Load and display a saved offline page
    async function loadOfflinePage(pageId) {
        try {
            const res  = await fetch(`/api/saved_pages/${pageId}`);
            const data = await res.json();
            if (!data || data.error) { showToast("Page not found", "error"); return; }
            state.currentBrowseHash       = data.node_hash;
            state.currentBrowsePath       = data.path;
            state.currentPageContent      = data.content;
            state.currentPageTitle        = data.title;
            state.currentPageContentType  = data.content_type;
            if (DOM.pageAddressInput) DOM.pageAddressInput.value = data.node_hash + ":" + data.path;
            const isHtml  = data.content_type === "html";
            const pgTitle = `<div class="page-viewer-title">${escapeHtml(data.title)}</div>`;
            const meta    = `<div class="page-viewer-meta offline-badge">Saved offline · ${data.node_hash.substring(0,20)}… · ${data.path}</div>`;
            if (DOM.pageViewer) {
                if (isHtml) {
                    const safe   = data.content.replace(/<script[\s\S]*?<\/script>/gi, "").replace(/on\w+\s*=/gi,"data-blocked=");
                    const scoped = _scopeHtmlStyles(safe, ".remote-page-root");
                    DOM.pageViewer.innerHTML = pgTitle + `<div class="remote-page-root">${scoped}</div>` + meta;
                } else {
                    DOM.pageViewer.innerHTML = pgTitle + `<div class="page-viewer-content">${escapeHtml(data.content).replace(/\n/g,"<br>")}</div>` + meta;
                }
            }
            _updateBookmarkBtn();
        } catch(e) {
            showToast("Load error: " + e.message, "error");
        }
    }

    // ── Browser sidebar (history + saved + bookmarks combined) ──
    async function _refreshBrowserSidebar() {
        await loadBookmarks();
        _renderHistory();
        _renderSavedPages();
    }

    async function _renderHistory() {
        const el = document.getElementById("history-list");
        if (!el) return;
        try {
            const items = await fetch("/api/history?limit=20").then(r => r.json());
            if (!items.length) {
                el.innerHTML = '<div class="bookmark-empty">No history yet.</div>';
                return;
            }
            el.innerHTML = items.map(h => {
                const label = escapeHtml(h.title || h.path);
                const sub   = escapeHtml(h.node_hash.substring(0,14) + "… " + h.path);
                return `<div class="browser-history-item" data-hash="${h.node_hash}" data-path="${escapeHtml(h.path)}">
                    <div class="bookmark-item-info">
                        <div class="bookmark-item-title">${label}</div>
                        <div class="bookmark-item-sub">${sub}</div>
                    </div>
                </div>`;
            }).join("");
            el.querySelectorAll(".browser-history-item").forEach(row => {
                row.addEventListener("click", () => browseNodeHash(row.dataset.hash, row.dataset.path));
            });
        } catch(e) {}
    }

    async function _renderSavedPages() {
        const el = document.getElementById("saved-pages-list");
        if (!el) return;
        try {
            const pages = await fetch("/api/saved_pages").then(r => r.json());
            if (!pages.length) {
                el.innerHTML = '<div class="bookmark-empty">No saved pages.</div>';
                return;
            }
            el.innerHTML = pages.map(p => `
                <div class="bookmark-item">
                    <div class="bookmark-item-info" style="cursor:pointer" data-id="${p.id}">
                        <div class="bookmark-item-title">${escapeHtml(p.title || p.path)}</div>
                        <div class="bookmark-item-sub">${escapeHtml(p.node_hash.substring(0,14))}… · ${escapeHtml(p.path)}</div>
                    </div>
                    <button class="btn-bookmark-del" data-del="${p.id}" title="Delete saved page">✕</button>
                </div>
            `).join("");
            el.querySelectorAll("[data-id]").forEach(el2 => {
                el2.addEventListener("click", () => loadOfflinePage(parseInt(el2.dataset.id)));
            });
            el.querySelectorAll("[data-del]").forEach(btn => {
                btn.addEventListener("click", async () => {
                    await fetch(`/api/saved_pages/${btn.dataset.del}`, { method: "DELETE" });
                    _renderSavedPages();
                });
            });
        } catch(e) {}
    }

    // Listen for NomadNet node announcements from the backend
    // (handled in the main WS message dispatcher)

    // ── Mobile Sidebar Toggle ──
    function initMobileToggle() {
        // Legacy peer-list header toggle
        if (DOM.peerListHeader) {
            DOM.peerListHeader.addEventListener("click", () => {
                DOM.peerList.classList.toggle("expanded");
                DOM.peerListHeader.classList.toggle("expanded");
            });
        }

        // Floating button that collapses/expands the whole sidebar on phones
        const floatBtn = document.getElementById("mobile-sidebar-toggle");
        const sidebar  = document.getElementById("sidebar");
        if (!floatBtn || !sidebar) return;

        // Icon helpers — keep the SVG markup in one place so the open/closed
        // states stay in sync everywhere we update them.
        const _ICON_HAMBURGER = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg>`;
        const _ICON_CLOSE     = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>`;

        // Make sure the button starts in the right state.  On mobile the
        // sidebar is closed by default (CSS: left:-100%), so the button
        // should show the hamburger (= "tap to open"), not the X.
        floatBtn.innerHTML = _ICON_HAMBURGER;
        floatBtn.setAttribute("aria-label", "Show peer list");

        floatBtn.addEventListener("click", () => {
            // Use the `open` class — that's what the CSS keys off (an earlier
            // build toggled `mobile-collapsed`, which had no matching CSS
            // rule, so the button silently did nothing on phones).
            const isOpen = sidebar.classList.toggle("open");
            floatBtn.innerHTML = isOpen ? _ICON_CLOSE : _ICON_HAMBURGER;
            floatBtn.setAttribute("aria-label", isOpen ? "Hide peer list" : "Show peer list");
        });

        // Auto-collapse sidebar when a peer is selected on mobile
        const _mobileCollapse = () => {
            if (window.matchMedia("(max-width: 640px)").matches && sidebar.classList.contains("open")) {
                sidebar.classList.remove("open");
                floatBtn.innerHTML = _ICON_HAMBURGER;
                floatBtn.setAttribute("aria-label", "Show peer list");
            }
        };
        // Hook into peer-list click
        document.getElementById("peer-list")?.addEventListener("click", () => {
            setTimeout(_mobileCollapse, 100);
        });

        // Tap-outside-to-close: when the sidebar is open on mobile, tapping
        // the dimmed overlay (anywhere outside the sidebar) closes it.
        document.addEventListener("click", (e) => {
            if (!sidebar.classList.contains("open")) return;
            if (!window.matchMedia("(max-width: 640px)").matches) return;
            // Ignore taps on the sidebar itself or on the toggle button.
            if (sidebar.contains(e.target) || floatBtn.contains(e.target)) return;
            _mobileCollapse();
        });

        // Welcome-screen mobile CTAs: on phones the sidebar is offscreen,
        // so the welcome screen surfaces "Announce" and "Open peers" buttons
        // directly.  Wire them up to the existing sidebar controls.
        const ctaAnnounce = document.getElementById("welcome-cta-announce");
        if (ctaAnnounce) {
            ctaAnnounce.addEventListener("click", (e) => {
                e.stopPropagation();
                document.getElementById("btn-announce")?.click();
            });
        }
        const ctaPeers = document.getElementById("welcome-cta-peers");
        if (ctaPeers) {
            ctaPeers.addEventListener("click", (e) => {
                // Stop propagation: otherwise the tap-outside-to-close
                // document handler fires next and immediately closes the
                // sidebar we just opened.
                e.stopPropagation();
                if (!sidebar.classList.contains("open")) {
                    sidebar.classList.add("open");
                    floatBtn.innerHTML = _ICON_CLOSE;
                    floatBtn.setAttribute("aria-label", "Hide peer list");
                }
            });
        }
    }

    // ════════════════════════════════════════════════════════════════
    // ENHANCED NETWORK GRAPH
    // ════════════════════════════════════════════════════════════════
    //
    // Layout:
    //   • Center = "You" node
    //   • Middle ring = interface nodes (coloured by type: LoRa/TCP/WiFi/…)
    //   • Outer ring = peers, placed near the interface they arrived on
    //
    // Animated packets travel along the edge path at a speed and colour
    // reflecting the interface type and online/offline state.

    let _graphAnimFrame   = null;
    let _graphInterfaces  = [];
    let _graphPeerIfaceMap = {};   // dest_hash → iface name (server-side from RNS)
    let _graphPeerIfaceTimer = null;
    let _graphHoverNode   = null;   // hovered peer (for tooltip)
    // Drag support: keys are "peer_<hash>" or "iface_<name>", values are {x, y}
    const _graphUserPos   = {};     // user-set positions (drag overrides)
    let _graphDragState   = null;   // {key, ox, oy} while dragging
    let _graphLastIfaceNodes = [];  // cached from last draw frame for hit-testing

    // Traffic visualization: when a real chat message is sent/received, we
    // store {peer_hash, ts, dir} entries here.  The graph draws bigger,
    // brighter packets along the corresponding peer's edge for ~2 seconds
    // after the event.  Old entries auto-prune in the next render frame.
    const _netTraffic = [];
    function _triggerNetTraffic(peerHash, dir) {
        if (!peerHash) return;
        _netTraffic.push({ peerHash, dir, ts: performance.now() });
        // Cap memory — keep last 50 events
        if (_netTraffic.length > 50) _netTraffic.shift();
    }
    // Expose for other code paths that want to trigger (file_sent, alerts, etc.)
    window._triggerNetTraffic = _triggerNetTraffic;
    let _graphDragMoved   = false;  // true if mouse moved >5px since mousedown

    // Zoom/pan viewport — applied as a transform before drawing.
    // All hit-testing converts screen coords → world coords via _screenToWorld().
    let _graphZoom = 1;
    let _graphPanX = 0;
    let _graphPanY = 0;
    let _graphPanState = null;      // {startMX, startMY, startPX, startPY} during pan
    const ZOOM_MIN = 0.3;
    const ZOOM_MAX = 4.0;

    // Convert screen coords (mouse) → world coords (canvas pre-transform)
    function _screenToWorld(mx, my) {
        return { x: (mx - _graphPanX) / _graphZoom, y: (my - _graphPanY) / _graphZoom };
    }

    // Interface-type → { color, label, speed (packet travel) }
    // Distinct, readable colors that match the legend in index.html 1:1.
    // Keep these in sync with the .net-vis-legend rgba()/hex values, and
    // with the "You" node colour below — the legend is the source of truth
    // (earlier builds drifted: AutoInterface was #3fb950 while the legend
    // showed #22c55e, so peers on WiFi looked dim-green while the legend
    // promised bright-green).
    const IFACE_STYLE = {
        AutoInterface:      { color: "#22c55e", label: "WiFi/Auto",   speed: 0.8 },
        TCPServerInterface: { color: "#60a5fa", label: "TCP Server",  speed: 1.0 },
        TCPClientInterface: { color: "#60a5fa", label: "TCP Client",  speed: 1.0 },
        UDPInterface:       { color: "#a78bfa", label: "UDP",         speed: 0.9 },
        I2PInterface:       { color: "#14b8a6", label: "I2P",         speed: 0.4 },
        RNodeInterface:     { color: "#fbbf24", label: "LoRa/RNode",  speed: 0.2 },
        LocalInterface:     { color: "#f778ba", label: "Local",       speed: 1.0 },
        BluetoothInterface: { color: "#3b82f6", label: "Bluetooth",   speed: 0.6 },
    };

    function _ifaceStyle(type) {
        return IFACE_STYLE[type] || { color: "#8b949e", label: type || "?", speed: 0.6 };
    }

    // Collapse RNS AutoInterface sub-interfaces (one per physical NIC:
    // wlan0, eth0, lo, ...) into a single canonical "WiFi/Auto" node.
    //
    // Why this matters in the topology view:
    //   • RNS exposes every NIC as its own AutoInterface in
    //     RNS.Transport.interfaces.  Drawn naively, that produces three
    //     separate "WiFi" nodes for the same logical link.
    //   • The peer→interface map (/api/network/peer_interfaces) returns
    //     whichever sub-interface name a packet last arrived on, so two
    //     peers reachable via the same network can end up on different
    //     graph nodes — confusing and visually messy.
    //   • Sub-interfaces with no peers attached still render as ghost
    //     nodes hanging off "You", which is what the user reported.
    //
    // The collapse keeps one representative AutoInterface per group and
    // rewrites any peer-map entry that points at a redundant sub-interface
    // so it lands on the canonical node.  Rxb/Txb totals are summed so the
    // tooltip still reflects the true traffic across all NICs.
    function _collapseAutoInterfaces(ifaces, peerMap) {
        if (!Array.isArray(ifaces) || !ifaces.length) {
            return { ifaces: ifaces || [], peerMap: peerMap || {} };
        }
        const autoList = ifaces.filter(i => i && i.type === "AutoInterface");
        // Nothing to collapse if we have ≤1 AutoInterface
        if (autoList.length <= 1) {
            return { ifaces, peerMap: peerMap || {} };
        }
        // Pick the canonical: prefer one that already has peers in peerMap
        // (so the user doesn't see peers jump nodes when the canonical is
        // chosen), otherwise the first.
        const peerCounts = {};
        for (const dest in (peerMap || {})) {
            const n = peerMap[dest];
            if (n) peerCounts[n] = (peerCounts[n] || 0) + 1;
        }
        let canonical = autoList[0];
        let bestCount = peerCounts[canonical.name] || 0;
        for (const a of autoList) {
            const c = peerCounts[a.name] || 0;
            if (c > bestCount) { canonical = a; bestCount = c; }
        }
        // Sum traffic counters across the group onto the canonical node.
        const merged = Object.assign({}, canonical, {
            display_name: "WiFi / Auto",
            bytes_in:  autoList.reduce((s, a) => s + (a.bytes_in  || 0), 0),
            bytes_out: autoList.reduce((s, a) => s + (a.bytes_out || 0), 0),
            pkts_in:   autoList.reduce((s, a) => s + (a.pkts_in   || 0), 0),
            pkts_out:  autoList.reduce((s, a) => s + (a.pkts_out  || 0), 0),
        });
        // Drop the redundant sub-interfaces, keep everything else as-is.
        const redundantNames = new Set(
            autoList.filter(a => a !== canonical).map(a => a.name)
        );
        const newIfaces = ifaces
            .filter(i => !(i.type === "AutoInterface" && redundantNames.has(i.name)))
            .map(i => (i === canonical ? merged : i));
        // Rewrite peer map: any reference to a redundant sub-interface
        // becomes a reference to the canonical one.
        const newPeerMap = {};
        for (const dest in (peerMap || {})) {
            const n = peerMap[dest];
            newPeerMap[dest] = redundantNames.has(n) ? canonical.name : n;
        }
        return { ifaces: newIfaces, peerMap: newPeerMap };
    }

    // Assign each peer to one of the available interfaces (round-robin since
    // RNS doesn't expose which interface a peer arrived on).
    function _peerIfaceIndex(peerIdx, numIfaces) {
        return numIfaces ? peerIdx % numIfaces : 0;
    }

    function drawNetworkGraph() {
        const canvas = document.getElementById("net-graph-canvas");
        const vis    = document.getElementById("net-visualizer");
        if (!canvas || !vis || vis.classList.contains("hidden")) return;

        const dpr  = window.devicePixelRatio || 1;
        const rect = vis.getBoundingClientRect();

        // If the element hasn't been laid out yet (zero size), retry next frame.
        // This happens when the tab is switched and CSS hasn't reflowed yet.
        if (!rect.width || !rect.height) {
            _graphAnimFrame = requestAnimationFrame(drawNetworkGraph);
            return;
        }

        if (canvas.width  !== Math.round(rect.width  * dpr) ||
            canvas.height !== Math.round(rect.height * dpr)) {
            canvas.width  = Math.round(rect.width  * dpr);
            canvas.height = Math.round(rect.height * dpr);
            canvas.style.width  = rect.width  + "px";
            canvas.style.height = rect.height + "px";
        }

        const ctx = canvas.getContext("2d");
        ctx.save();
        ctx.scale(dpr, dpr);
        ctx.clearRect(0, 0, rect.width, rect.height);

        // Apply viewport transform (zoom + pan).  All drawing below uses world
        // coordinates centered on the logical viewport center.  Hit-testing
        // converts mouse screen coords via _screenToWorld().
        ctx.translate(_graphPanX, _graphPanY);
        ctx.scale(_graphZoom, _graphZoom);

        const W = rect.width, H = rect.height;
        const cx = W / 2,    cy = H / 2;
        const now = Date.now();

        const peers  = state.peers     || [];
        const _allIfaces = _graphInterfaces || [];

        // Hide interface nodes that have nothing attached.  Without this
        // filter, every NIC RNS knows about — including ones that no
        // peer is reachable through — shows up as a lonely "WiFi" node
        // hanging off "You".  We always keep:
        //   • shared-instance interfaces (they're informational by design)
        //   • Bluetooth interfaces (the user explicitly configured them
        //     and may want to see them even before any peer pairs)
        const _peerIfaceNameSet = new Set();
        for (const dest in (_graphPeerIfaceMap || {})) {
            const n = _graphPeerIfaceMap[dest];
            if (n) _peerIfaceNameSet.add(n);
        }
        // Bluetooth interfaces also "own" their bt_rns_peers entries
        const _btReachableNames = new Set();
        _allIfaces.forEach(iface => {
            if (iface.type === "BluetoothInterface" && Array.isArray(iface.bt_rns_peers) && iface.bt_rns_peers.length) {
                _btReachableNames.add(iface.name);
            }
        });
        const ifaces = _allIfaces.filter(iface => {
            if (iface.is_shared) return true;
            if (iface.type === "BluetoothInterface") return true;
            return _peerIfaceNameSet.has(iface.name) || _btReachableNames.has(iface.name);
        });

        // ── Radii ──
        // Tidier tree-style layout (was a tight orbital ring):
        //   • Interfaces sit on a generous arc around You so their labels
        //     never collide.
        //   • Peers attach in compact, evenly-spaced fan behind their
        //     interface (away from You) — no animated wobble, no arc
        //     overlap with neighbouring interfaces.
        const ifaceRing  = Math.min(W, H) * 0.28;  // bigger ring → more breathing room
        const peerOrbit  = Math.min(W, H) * 0.20;  // distance from iface to each peer
        const peerStep   = Math.min(W, H) * 0.085; // sibling separation along the fan

        // ── Interface positions ── (use user drag override if set)
        // Distribute interfaces evenly around the circle starting from the
        // top (-π/2).  Single interface goes straight up; two go up + down,
        // etc.  No rotation animation, so the tree stays still.
        const ifaceNodes = ifaces.map((iface, i) => {
            const angle   = (i / Math.max(ifaces.length, 1)) * Math.PI * 2 - Math.PI / 2;
            const style   = _ifaceStyle(iface.type);
            const key     = "iface_" + (iface.name || i);
            const userPos = _graphUserPos[key];
            return {
                x:     userPos ? userPos.x : cx + Math.cos(angle) * ifaceRing,
                y:     userPos ? userPos.y : cy + Math.sin(angle) * ifaceRing,
                key, iface, style, angle,
            };
        });

        // Build a name → index lookup so peers can resolve their interface
        // by the string returned from the server's /api/network/peer_interfaces.
        const ifaceNameToIdx = {};
        ifaces.forEach((iface, idx) => {
            if (iface.name) ifaceNameToIdx[iface.name] = idx;
        });

        // Find the index of the shared-instance interface (used as the
        // fallback bucket for peers whose physical interface RNS doesn't
        // know yet).  If there's no shared-instance interface in the
        // list, fall back to interface 0.
        let sharedIdx = -1;
        ifaces.forEach((iface, idx) => {
            if (iface.is_shared && sharedIdx === -1) sharedIdx = idx;
        });
        const fallbackIdx = sharedIdx >= 0 ? sharedIdx
                           : (ifaces.length > 0 ? 0 : -1);

        // ── BT-reachable peer map ──
        const btPeerToIfaceIdx = {};
        ifaces.forEach((iface, ifaceIdx) => {
            if (iface.type === "BluetoothInterface") {
                const btPeers = iface.bt_rns_peers || [];
                btPeers.forEach(bp => {
                    if (bp.dest_hash) btPeerToIfaceIdx[bp.dest_hash] = ifaceIdx;
                });
            }
        });

        // For each interface, count how many peers will attach so we can
        // arrange peers around their interface in a small orbit.
        const peerOrderInIface = {};   // peer.dest_hash → index within its iface's peers
        const peerCountInIface = {};   // ifaceIdx → count
        peers.forEach(p => {
            // Resolve attachment priority:
            //   1. RNS-reported next-hop interface (from server peer_interfaces map)
            //   2. BT-reachable peer
            //   3. Shared instance fallback
            let iIdx = -1;
            const ifaceName = _graphPeerIfaceMap[p.dest_hash];
            if (ifaceName && ifaceName in ifaceNameToIdx) {
                iIdx = ifaceNameToIdx[ifaceName];
            } else if (p.dest_hash in btPeerToIfaceIdx) {
                iIdx = btPeerToIfaceIdx[p.dest_hash];
            } else {
                iIdx = fallbackIdx;
            }
            p._graphIfaceIdx = iIdx;
            const k = String(iIdx);
            peerOrderInIface[p.dest_hash] = peerCountInIface[k] || 0;
            peerCountInIface[k] = (peerCountInIface[k] || 0) + 1;
        });

        // ── Peer positions ──
        // Tidy fan behind each interface, mirroring the hand-drawn tree:
        //   • peerOrbit pushes peers a fixed distance away from You,
        //     past their interface (so the iface sits between You and the
        //     peer, just like the reference drawing).
        //   • Multiple peers on the same interface fan out perpendicular
        //     to that iface→outward axis with constant spacing — no
        //     overlapping with neighbouring interfaces' peers.
        //   • No sin/cos wobble — keeps the tree visually still.
        const peerNodes = peers.map((p, i) => {
            const iIdx  = p._graphIfaceIdx;
            const iNode = iIdx >= 0 ? ifaceNodes[iIdx] : null;

            const order = peerOrderInIface[p.dest_hash] || 0;
            const total = peerCountInIface[String(iIdx)] || 1;

            // Outward unit vector: from You → iface, continued past it.
            const outAngle = iNode ? Math.atan2(iNode.y - cy, iNode.x - cx) : 0;
            const ox = Math.cos(outAngle), oy = Math.sin(outAngle);
            // Perpendicular unit vector for sibling spread.
            const px = -oy, py = ox;

            // Anchor point sits a fixed distance behind the interface.
            const ax = (iNode ? iNode.x : cx) + ox * peerOrbit;
            const ay = (iNode ? iNode.y : cy) + oy * peerOrbit;
            // Sibling offset: centred line, evenly stepped.
            const off = (order - (total - 1) / 2) * peerStep;

            const key     = "peer_" + p.dest_hash;
            const userPos = _graphUserPos[key];
            const btOnline = p.dest_hash in btPeerToIfaceIdx;
            return {
                x:      userPos ? userPos.x : ax + px * off,
                y:      userPos ? userPos.y : ay + py * off,
                key, peer: p, iIdx, iNode,
                hash: p.dest_hash,
                online: btOnline || isPeerOnline(p.last_seen || p.last_announce),
            };
        });

        // ── Draw: center → interface edges ──
        ifaceNodes.forEach(n => {
            const style = n.style;
            ctx.beginPath();
            ctx.moveTo(cx, cy);
            ctx.lineTo(n.x, n.y);
            ctx.strokeStyle = style.color + "44";   // 26% opacity
            ctx.lineWidth   = 1.5;
            ctx.setLineDash([3, 5]);
            ctx.stroke();
            ctx.setLineDash([]);
        });

        // ── Draw: interface → peer edges ──
        peerNodes.forEach(n => {
            if (!n.iNode) return;
            const edgeColor = n.online
                ? (n.iNode.style.color + "55")
                : "rgba(72,79,88,0.18)";
            ctx.beginPath();
            ctx.moveTo(n.iNode.x, n.iNode.y);
            ctx.lineTo(n.x, n.y);
            ctx.strokeStyle = edgeColor;
            ctx.lineWidth   = n.online ? 1.2 : 0.8;
            ctx.stroke();
        });

        // ── Animated packets: center → interface ──
        ifaceNodes.forEach((n, i) => {
            const t = ((now / 900 * n.style.speed) + i * 0.4) % 1;
            const px = cx + (n.x - cx) * t;
            const py = cy + (n.y - cy) * t;
            ctx.beginPath();
            ctx.arc(px, py, 2.5, 0, Math.PI * 2);
            ctx.fillStyle = n.style.color + "cc";
            ctx.fill();
        });

        // ── Animated packets: interface → peer (online only) ──
        peerNodes.forEach((n, i) => {
            if (!n.online || !n.iNode) return;
            const speed = n.iNode.style.speed;
            const t = ((now / 1200 * speed) + i * 0.35) % 1;
            const px = n.iNode.x + (n.x - n.iNode.x) * t;
            const py = n.iNode.y + (n.y - n.iNode.y) * t;
            ctx.beginPath();
            ctx.arc(px, py, 2, 0, Math.PI * 2);
            ctx.fillStyle = n.iNode.style.color + "99";
            ctx.fill();
        });

        // ── Real-traffic packets: bigger, brighter, color-coded.  Drawn
        //    when an actual chat / file event happens on a peer's edge.
        //    Lives for 2s; spawns 3 staggered packets so the "burst" is
        //    visible even on a single send.
        const TRAFFIC_LIFETIME_MS = 2000;
        const tNow = performance.now();
        // Prune old events
        while (_netTraffic.length && tNow - _netTraffic[0].ts > TRAFFIC_LIFETIME_MS) {
            _netTraffic.shift();
        }
        for (const ev of _netTraffic) {
            const age = tNow - ev.ts;
            const peer = peerNodes.find(p => p.hash === ev.peerHash);
            if (!peer || !peer.iNode) continue;
            // 3 packets staggered by ~150ms — the shape of a "burst"
            for (let k = 0; k < 3; k++) {
                const lag = k * 150;
                if (age < lag) continue;
                const local = (age - lag) / TRAFFIC_LIFETIME_MS;
                if (local > 1) continue;
                const t = ev.dir === "out" ? local : 1 - local;
                const sx = peer.iNode.x, sy = peer.iNode.y;
                const ex = peer.x, ey = peer.y;
                const px = sx + (ex - sx) * t;
                const py = sy + (ey - sy) * t;
                const fade = 1 - local;
                // Outgoing = accent blue (matches TCP in legend), incoming = green (matches WiFi/Auto)
                const color = ev.dir === "out" ? "#60a5fa" : "#22c55e";
                ctx.save();
                ctx.globalAlpha = fade * 0.95;
                // Glow
                const glow = ctx.createRadialGradient(px, py, 0, px, py, 8);
                glow.addColorStop(0, color);
                glow.addColorStop(1, color + "00");
                ctx.fillStyle = glow;
                ctx.beginPath(); ctx.arc(px, py, 8, 0, Math.PI * 2); ctx.fill();
                // Core
                ctx.fillStyle = color;
                ctx.beginPath(); ctx.arc(px, py, 3.5, 0, Math.PI * 2); ctx.fill();
                ctx.restore();
            }
        }

        // ── Draw: interface nodes ──
        ifaceNodes.forEach(n => {
            const style = n.style;
            const r = 16;
            // Glow
            const grd = ctx.createRadialGradient(n.x, n.y, 0, n.x, n.y, r * 2);
            grd.addColorStop(0, style.color + "28");
            grd.addColorStop(1, "transparent");
            ctx.beginPath(); ctx.arc(n.x, n.y, r * 2, 0, Math.PI * 2);
            ctx.fillStyle = grd; ctx.fill();
            // Circle
            ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
            ctx.fillStyle = style.color + "1a";
            ctx.fill();
            ctx.strokeStyle = style.color;
            ctx.lineWidth = 1.5; ctx.stroke();
            // Label
            ctx.fillStyle  = style.color;
            ctx.font       = "bold 9px sans-serif";
            ctx.textAlign  = "center";
            // Icon based on type — Bluetooth gets the ♦ glyph + mode label
            // Shared-instance node: render with a neutral "Shared" label and
            // no bitrate readout (the synthetic 1000 Mbps figure isn't
            // meaningful and looked like a real WiFi link).
            let icon;
            if (n.iface.is_shared)                      icon = "Shared";
            else if (n.iface.type === "RNodeInterface")      icon = "LoRa";
            else if (n.iface.type === "I2PInterface")   icon = "I2P";
            else if (n.iface.type.includes("TCP"))      icon = "TCP";
            else if (n.iface.type === "UDPInterface")   icon = "UDP";
            else if (n.iface.type === "BluetoothInterface") icon = "BT";
            else                                        icon = "WiFi";
            ctx.fillText(icon, n.x, n.y + 3);

            // Bluetooth: show mode (BLE/RFCOMM) + peer count below icon
            if (n.iface.type === "BluetoothInterface") {
                ctx.fillStyle = style.color + "cc";
                ctx.font      = "7px sans-serif";
                const btMode  = n.iface.bt_mode || "BLE";
                const btPeers = n.iface.bt_peers != null ? `${n.iface.bt_peers}p` : "";
                ctx.fillText(btMode + (btPeers ? " · " + btPeers : ""), n.x, n.y + r + 13);
            } else if (n.iface.bitrate && !n.iface.is_shared) {
                ctx.fillStyle = style.color + "bb";
                ctx.font      = "8px sans-serif";
                ctx.fillText(formatBitrate(n.iface.bitrate), n.x, n.y + r + 13);
            }
        });

        // ── Draw: center (You) ──
        const youR = 22;
        // Pulse ring
        const pulse = 0.5 + 0.5 * Math.sin(now / 900);
        ctx.beginPath(); ctx.arc(cx, cy, youR + 8 + pulse * 6, 0, Math.PI * 2);
        ctx.strokeStyle = `rgba(34,197,94,${0.06 + 0.06 * pulse})`;
        ctx.lineWidth = 2; ctx.stroke();
        // Fill + border
        ctx.beginPath(); ctx.arc(cx, cy, youR, 0, Math.PI * 2);
        ctx.fillStyle = "rgba(34,197,94,0.14)"; ctx.fill();
        ctx.strokeStyle = "#22c55e"; ctx.lineWidth = 2; ctx.stroke();
        ctx.fillStyle = "#22c55e";
        ctx.font = "bold 11px sans-serif"; ctx.textAlign = "center";
        ctx.fillText("You", cx, cy + 4);

        // ── Draw: peer nodes ──
        peerNodes.forEach(n => {
            const r  = 13;
            const ac = n.iNode ? n.iNode.style.color : "#60a5fa";
            // Pulse on recently active
            if (n.online) {
                const pls = 0.5 + 0.5 * Math.sin(now / 1400 + n.peer.dest_hash.charCodeAt(0));
                ctx.beginPath(); ctx.arc(n.x, n.y, r + 5 + pls * 3, 0, Math.PI * 2);
                ctx.strokeStyle = ac + "33";
                ctx.lineWidth = 1.5; ctx.stroke();
            }
            // Fill — stronger when online so interface color is clearly visible
            ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
            ctx.fillStyle = n.online ? ac + "33" : "rgba(72,79,88,0.15)";
            ctx.fill();
            // Border — solid when online, dashed when offline
            ctx.strokeStyle = n.online ? ac : "#6e7681";
            ctx.lineWidth = 2;
            if (!n.online) ctx.setLineDash([3, 3]);
            ctx.stroke();
            ctx.setLineDash([]);

            // RSSI indicator (if available)
            if (n.peer.rssi != null) {
                const strength = Math.max(0, Math.min(1, (n.peer.rssi + 120) / 80));
                ctx.fillStyle = `rgba(34,197,94,${strength * 0.8})`;
                ctx.fillRect(n.x + r - 2, n.y - r, 4, 6);
            }

            // Name label
            const label = (n.peer.nickname || n.peer.display_name || n.peer.dest_hash.substring(0, 8));
            ctx.fillStyle  = n.online ? "#c9d1d9" : "#484f58";
            ctx.font       = n === _graphHoverNode ? "bold 10px sans-serif" : "10px sans-serif";
            ctx.textAlign  = "center";
            const short    = label.length > 12 ? label.substring(0, 11) + "…" : label;
            ctx.fillText(short, n.x, n.y + r + 14);
        });

        // ── No-peers message ──
        if (!peers.length) {
            ctx.fillStyle = "#484f58";
            ctx.font = "13px sans-serif"; ctx.textAlign = "center";
            ctx.fillText("No peers discovered yet", cx, cy + (ifaces.length ? peerRing + 30 : 60));
            ctx.font = "11px sans-serif";
            ctx.fillText("Click 'Announce' to broadcast your presence", cx, cy + (ifaces.length ? peerRing + 50 : 80));
        }

        ctx.restore();
        _graphAnimFrame = requestAnimationFrame(drawNetworkGraph);

        // Cache node arrays for hit-testing (hover, drag, click)
        _graphLastPeerNodes  = peerNodes;
        _graphLastIfaceNodes = ifaceNodes;
    }

    let _graphLastPeerNodes  = [];

    /* Find which node (peer or iface) is under a canvas point. Returns {key, node, type}. */
    function _graphNodeAt(mx, my) {
        for (const n of _graphLastPeerNodes) {
            const dx = n.x - mx, dy = n.y - my;
            if (Math.sqrt(dx*dx + dy*dy) < 20) return { key: n.key, node: n, type: "peer" };
        }
        for (const n of _graphLastIfaceNodes) {
            const dx = n.x - mx, dy = n.y - my;
            if (Math.sqrt(dx*dx + dy*dy) < 22) return { key: n.key, node: n, type: "iface" };
        }
        return null;
    }

    function _initGraphInteraction() {
        const canvas  = document.getElementById("net-graph-canvas");
        const tooltip = document.getElementById("graph-tooltip");
        if (!canvas || !tooltip) return;

        // ── Mouse drag ──────────────────────────────────────────────────
        canvas.addEventListener("mousedown", (e) => {
            const rect = canvas.getBoundingClientRect();
            const mx = e.clientX - rect.left;
            const my = e.clientY - rect.top;

            // Middle-click or shift+left-click → start panning
            if (e.button === 1 || (e.button === 0 && e.shiftKey)) {
                _graphPanState = { startMX: mx, startMY: my, startPX: _graphPanX, startPY: _graphPanY };
                canvas.style.cursor = "move";
                e.preventDefault();
                return;
            }
            if (e.button !== 0) return;

            // Hit-test in world coords
            const w = _screenToWorld(mx, my);
            const hit = _graphNodeAt(w.x, w.y);
            if (hit) {
                _graphDragState = {
                    key: hit.key, type: hit.type, node: hit.node,
                    startMX: mx, startMY: my,
                    startNX: hit.node.x, startNY: hit.node.y,
                };
                _graphDragMoved = false;
                canvas.style.cursor = "grabbing";
                e.preventDefault();
            } else {
                // Empty space left-click → pan
                _graphPanState = { startMX: mx, startMY: my, startPX: _graphPanX, startPY: _graphPanY };
                canvas.style.cursor = "move";
            }
        });

        canvas.addEventListener("mousemove", (e) => {
            const rect = canvas.getBoundingClientRect();
            const mx = e.clientX - rect.left;
            const my = e.clientY - rect.top;

            // ── Panning ──
            if (_graphPanState) {
                _graphPanX = _graphPanState.startPX + (mx - _graphPanState.startMX);
                _graphPanY = _graphPanState.startPY + (my - _graphPanState.startMY);
                tooltip.classList.add("hidden");
                return;
            }

            // ── Dragging a node (in world coords) ──
            if (_graphDragState) {
                const dx = (mx - _graphDragState.startMX) / _graphZoom;
                const dy = (my - _graphDragState.startMY) / _graphZoom;
                if (Math.abs(dx) > 4 || Math.abs(dy) > 4) _graphDragMoved = true;
                _graphUserPos[_graphDragState.key] = {
                    x: _graphDragState.startNX + dx,
                    y: _graphDragState.startNY + dy,
                };
                tooltip.classList.add("hidden");
                return;
            }

            // ── Hover (world coords) ──
            const w = _screenToWorld(mx, my);
            const hit = _graphNodeAt(w.x, w.y);
            _graphHoverNode = hit && hit.type === "peer" ? hit.node : null;

            if (hit) {
                canvas.style.cursor = "grab";
                if (hit.type === "peer") {
                    const p = hit.node.peer;
                    const name   = p.nickname || p.display_name || p.dest_hash.substring(0, 16);
                    const iface  = hit.node.iNode ? hit.node.iNode.style.label : "Unknown";
                    const status = hit.node.online ? "● Online" : "○ Offline";
                    const rssi   = p.rssi != null ? ` · RSSI ${p.rssi} dBm` : "";
                    tooltip.innerHTML =
                        `<div class="graph-tooltip-name">${escapeHtml(name)}</div>` +
                        `<div class="graph-tooltip-hash">${p.dest_hash.substring(0, 20)}…</div>` +
                        `<div class="graph-tooltip-iface">${status} via ${escapeHtml(iface)}${rssi}</div>` +
                        `<div class="graph-tooltip-iface" style="color:var(--text-muted);font-size:10px;margin-top:2px;">Click to chat · Drag to move</div>`;
                    tooltip.style.left = (mx + 16) + "px";
                    tooltip.style.top  = (my - 10) + "px";
                    tooltip.classList.remove("hidden");
                } else {
                    const n = hit.node;
                    tooltip.innerHTML =
                        `<div class="graph-tooltip-name">${escapeHtml(n.iface.name || n.iface.type)}</div>` +
                        `<div class="graph-tooltip-iface">${n.iface.type}${n.iface.online === false ? " · Offline" : " · Online"}</div>`;
                    tooltip.style.left = (mx + 16) + "px";
                    tooltip.style.top  = (my - 10) + "px";
                    tooltip.classList.remove("hidden");
                }
            } else {
                canvas.style.cursor = "";
                tooltip.classList.add("hidden");
            }
        });

        canvas.addEventListener("mouseup", (e) => {
            const wasDragging = _graphDragState && _graphDragMoved;
            const hitNode = _graphDragState ? _graphDragState.node : null;
            _graphDragState = null;
            _graphPanState = null;
            canvas.style.cursor = "";

            // Click (no significant drag movement) on a peer → open chat
            if (!wasDragging && hitNode && hitNode.peer) {
                selectPeer(hitNode.peer.dest_hash);
            }
        });

        canvas.addEventListener("mouseleave", () => {
            _graphDragState = null;
            _graphPanState = null;
            _graphHoverNode = null;
            canvas.style.cursor = "";
            if (tooltip) tooltip.classList.add("hidden");
        });

        // ── Mouse wheel zoom (zoom toward cursor position) ─────────────
        canvas.addEventListener("wheel", (e) => {
            e.preventDefault();
            const rect = canvas.getBoundingClientRect();
            const mx = e.clientX - rect.left;
            const my = e.clientY - rect.top;
            // World point under cursor before zoom
            const wBefore = _screenToWorld(mx, my);
            // Zoom factor: positive deltaY = zoom out
            const factor = Math.exp(-e.deltaY * 0.001);
            const newZoom = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, _graphZoom * factor));
            if (newZoom === _graphZoom) return;
            _graphZoom = newZoom;
            // Adjust pan so the world point under the cursor stays under the cursor
            _graphPanX = mx - wBefore.x * _graphZoom;
            _graphPanY = my - wBefore.y * _graphZoom;
        }, { passive: false });

        // ── Touch drag ──────────────────────────────────────────────────
        canvas.addEventListener("touchstart", (e) => {
            if (e.touches.length !== 1) return;
            const rect = canvas.getBoundingClientRect();
            const t = e.touches[0];
            const mx = t.clientX - rect.left;
            const my = t.clientY - rect.top;
            const hit = _graphNodeAt(mx, my);
            if (hit) {
                _graphDragState = {
                    key: hit.key, type: hit.type, node: hit.node,
                    startMX: mx, startMY: my,
                    startNX: hit.node.x, startNY: hit.node.y,
                };
                _graphDragMoved = false;
                e.preventDefault();
            }
        }, { passive: false });

        canvas.addEventListener("touchmove", (e) => {
            if (!_graphDragState || e.touches.length !== 1) return;
            const rect = canvas.getBoundingClientRect();
            const t = e.touches[0];
            const mx = t.clientX - rect.left;
            const my = t.clientY - rect.top;
            const dx = mx - _graphDragState.startMX;
            const dy = my - _graphDragState.startMY;
            if (Math.abs(dx) > 4 || Math.abs(dy) > 4) _graphDragMoved = true;
            _graphUserPos[_graphDragState.key] = {
                x: _graphDragState.startNX + dx,
                y: _graphDragState.startNY + dy,
            };
            e.preventDefault();
        }, { passive: false });

        canvas.addEventListener("touchend", (e) => {
            const wasDragging = _graphDragState && _graphDragMoved;
            const hitNode = _graphDragState ? _graphDragState.node : null;
            _graphDragState = null;
            if (!wasDragging && hitNode && hitNode.peer) {
                selectPeer(hitNode.peer.dest_hash);
            }
        });

        // ── Reset layout button ─────────────────────────────────────────
        const resetBtn = document.getElementById("btn-graph-reset");
        if (resetBtn) {
            resetBtn.addEventListener("click", () => {
                Object.keys(_graphUserPos).forEach(k => delete _graphUserPos[k]);
                _graphZoom = 1;
                _graphPanX = 0;
                _graphPanY = 0;
                showToast("Layout reset to default", "info");
            });
        }

        // ── Zoom in / out buttons ──────────────────────────────────────
        const zoomInBtn  = document.getElementById("btn-graph-zoom-in");
        const zoomOutBtn = document.getElementById("btn-graph-zoom-out");
        function zoomAtCenter(factor) {
            const rect = canvas.getBoundingClientRect();
            const mx = rect.width  / 2;
            const my = rect.height / 2;
            const wBefore = _screenToWorld(mx, my);
            const newZoom = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, _graphZoom * factor));
            if (newZoom === _graphZoom) return;
            _graphZoom = newZoom;
            _graphPanX = mx - wBefore.x * _graphZoom;
            _graphPanY = my - wBefore.y * _graphZoom;
        }
        if (zoomInBtn)  zoomInBtn.addEventListener("click",  () => zoomAtCenter(1.25));
        if (zoomOutBtn) zoomOutBtn.addEventListener("click", () => zoomAtCenter(1 / 1.25));
    }

    function startNetworkGraph() {
        if (_graphAnimFrame) cancelAnimationFrame(_graphAnimFrame);

        // Load interface data then kick off the render loop.
        // Double-rAF ensures the browser has done at least one layout pass
        // after un-hiding the canvas container, so getBoundingClientRect()
        // returns real dimensions instead of 0×0.
        Promise.all([
            fetch("/api/network/interfaces").then(r => r.json()).catch(() => []),
            fetch("/api/network/peer_interfaces").then(r => r.json()).catch(() => ({})),
        ]).then(([ifaces, peerMap]) => {
            const collapsed = _collapseAutoInterfaces(ifaces || [], peerMap || {});
            _graphInterfaces   = collapsed.ifaces;
            _graphPeerIfaceMap = collapsed.peerMap;
        }).finally(() => {
            requestAnimationFrame(() => requestAnimationFrame(drawNetworkGraph));
        });

        // Refresh the peer→iface mapping every 10s so newly-discovered
        // peers attach to the right interface as paths form.  Re-run the
        // AutoInterface collapse with the latest peer map so peers don't
        // suddenly jump onto sub-interface ghost nodes.
        if (_graphPeerIfaceTimer) clearInterval(_graphPeerIfaceTimer);
        _graphPeerIfaceTimer = setInterval(() => {
            fetch("/api/network/peer_interfaces")
                .then(r => r.json())
                .then(data => {
                    const collapsed = _collapseAutoInterfaces(_graphInterfaces, data || {});
                    _graphInterfaces   = collapsed.ifaces;
                    _graphPeerIfaceMap = collapsed.peerMap;
                })
                .catch(() => {});
        }, 10000);

        _initGraphInteraction();
    }

    function stopNetworkGraph() {
        if (_graphAnimFrame) {
            cancelAnimationFrame(_graphAnimFrame);
            _graphAnimFrame = null;
        }
        if (_graphPeerIfaceTimer) {
            clearInterval(_graphPeerIfaceTimer);
            _graphPeerIfaceTimer = null;
        }
    }

    // ── Sidebar Navigation ──
    function _hideAllMainPanels() {
        const netVis = document.getElementById("net-visualizer");
        if (netVis)           netVis.classList.add("hidden");
        if (DOM.chatPanel)    DOM.chatPanel.classList.add("hidden");
        if (DOM.pagesMain)    DOM.pagesMain.classList.add("hidden");
        if (DOM.welcomeScreen) DOM.welcomeScreen.classList.add("hidden");
        const groupPanel = document.getElementById("group-chat-panel");
        if (groupPanel) groupPanel.classList.add("hidden");
        const alertsMain = document.getElementById("alerts-main");
        if (alertsMain) alertsMain.classList.add("hidden");
    }

    function initSidebarNav() {
        const navAlerts  = document.getElementById("nav-alerts");
        const viewAlerts = document.getElementById("view-alerts");
        const navGroups  = document.getElementById("nav-groups");
        const viewGroups = document.getElementById("view-groups");
        const navBtns   = [DOM.navPeers, DOM.navNetwork, DOM.navPages, navGroups, navAlerts].filter(Boolean);
        const views     = {
            peers:   DOM.viewPeers,
            network: DOM.viewNetwork,
            pages:   DOM.viewPages,
            groups:  viewGroups,
            alerts:  viewAlerts,
        };
        const netVis = document.getElementById("net-visualizer");

        navBtns.forEach(btn => {
            if (!btn) return;
            btn.addEventListener("click", () => {
                navBtns.forEach(b => b.classList.remove("active"));
                btn.classList.add("active");
                Object.values(views).forEach(v => { if(v) v.classList.add("hidden"); });
                const target = views[btn.dataset.view];
                if (target) target.classList.remove("hidden");

                if (btn.dataset.view === "network") {
                    refreshNetworkDashboard();
                    _hideAllMainPanels();
                    if (netVis) {
                        netVis.classList.remove("hidden");
                        startNetworkGraph();
                    }
                } else if (btn.dataset.view === "pages") {
                    _hideAllMainPanels();
                    if (DOM.pagesMain) DOM.pagesMain.classList.remove("hidden");
                    stopNetworkGraph();
                    loadMyPages();
                    loadNomadnetNodes();
                } else if (btn.dataset.view === "groups") {
                    _hideAllMainPanels();
                    if (netVis) netVis.classList.add("hidden");
                    stopNetworkGraph();
                    if (DOM.pagesMain) DOM.pagesMain.classList.add("hidden");
                    // Show group chat panel if a group is active, else welcome
                    if (_activeGroupId) {
                        const panel = document.getElementById("group-chat-panel");
                        if (panel) panel.classList.remove("hidden");
                    } else {
                        DOM.welcomeScreen?.classList.remove("hidden");
                    }
                    loadGroups();
                } else if (btn.dataset.view === "alerts") {
                    _hideAllMainPanels();
                    if (netVis) netVis.classList.add("hidden");
                    stopNetworkGraph();
                    if (DOM.pagesMain) DOM.pagesMain.classList.add("hidden");
                    // Show the alerts dashboard in the main panel so the
                    // previously-empty area shows stats + detail view.
                    const alertsMain = document.getElementById("alerts-main");
                    if (alertsMain) alertsMain.classList.remove("hidden");
                    loadAlerts();
                } else {
                    // Peers tab — restore chat or welcome.
                    // Bug fix: we were not hiding all main panels here, so if
                    // a group chat was open its panel stayed visible layered
                    // behind the DM UI.  Now we hide everything first, then
                    // selectively show the right DM view.
                    _hideAllMainPanels();
                    stopNetworkGraph();
                    if (state.activePeer) {
                        if (DOM.chatPanel) DOM.chatPanel.classList.remove("hidden");
                    } else if (DOM.welcomeScreen) {
                        DOM.welcomeScreen.classList.remove("hidden");
                    }
                }

                if (btn.dataset.view === "rns-config") loadRnsConfig();
                if (btn.dataset.view === "blocked")    loadBlockedPeers();
            });
        });
    }

    // ── Network Dashboard ──
    let _netPollTimer = null;

    async function refreshNetworkDashboard() {
        try {
            const [statusRes, ifaceRes, transportRes] = await Promise.all([
                fetch("/api/network/status").then(r=>r.json()).catch(()=>({})),
                fetch("/api/network/interfaces").then(r=>r.json()).catch(()=>[]),
                fetch("/api/network/transport").then(r=>r.json()).catch(()=>({})),
            ]);

            // Uptime
            if (DOM.netUptime && statusRes.uptime !== undefined) {
                const s = Math.floor(statusRes.uptime);
                const h = Math.floor(s/3600), m = Math.floor((s%3600)/60);
                DOM.netUptime.textContent = h > 0 ? `${h}h ${m}m` : `${m}m ${s%60}s`;
            }
            if (DOM.netPeers) DOM.netPeers.textContent = statusRes.peers_count || 0;
            if (DOM.netPaths) DOM.netPaths.textContent = transportRes.path_table_size || 0;
            if (DOM.netLinks) DOM.netLinks.textContent = statusRes.active_links || 0;

            // Interfaces — richer cards.
            // Hide the Shared Instance and AutoInterface cards from the
            // dashboard:
            //   • Shared Instance is purely informational (always present,
            //     no real traffic).
            //   • AutoInterface entries duplicate one another (one per
            //     physical NIC) and clutter the panel without adding
            //     useful information beyond what the topology graph shows.
            const _dashIfaces = (ifaceRes || []).filter(i =>
                i && !i.is_shared && i.type !== "AutoInterface"
            );
            if (DOM.netInterfaces) {
                if (!_dashIfaces.length) {
                    DOM.netInterfaces.innerHTML = '<div class="settings-empty">No interfaces detected</div>';
                } else {
                    DOM.netInterfaces.innerHTML = _dashIfaces.map(iface => {
                        const online = iface.online !== false;
                        const badgeCls = online ? "net-iface-badge online" : "net-iface-badge offline";
                        const badgeTxt = online ? "Online" : "Offline";

                        // Stat cells
                        const stats = [];
                        if (iface.bytes_in  !== undefined) stats.push({ label:"↓ Received",  val: formatBytes(iface.bytes_in)  });
                        if (iface.bytes_out !== undefined) stats.push({ label:"↑ Sent",       val: formatBytes(iface.bytes_out) });
                        if (iface.pkts_in   !== undefined) stats.push({ label:"Pkts In",     val: iface.pkts_in.toLocaleString()  });
                        if (iface.pkts_out  !== undefined) stats.push({ label:"Pkts Out",    val: iface.pkts_out.toLocaleString() });
                        if (iface.bitrate)                 stats.push({ label:"Bitrate",     val: formatBitrate(iface.bitrate)    });
                        if (iface.frequency)               stats.push({ label:"Frequency",   val: (iface.frequency/1e6).toFixed(3) + " MHz" });
                        if (iface.bandwidth)               stats.push({ label:"Bandwidth",   val: (iface.bandwidth/1e3).toFixed(0) + " kHz" });
                        if (iface.sf)                      stats.push({ label:"Spreading",   val: "SF" + iface.sf });
                        if (iface.target_ip)               stats.push({ label:"Target",      val: iface.target_ip + ":" + (iface.target_port||"?") });
                        if (iface.bind_ip)                 stats.push({ label:"Bind",        val: iface.bind_ip   + ":" + (iface.bind_port||"?") });

                        // Signal quality bar (LoRa RSSI: typically -50 to -120 dBm)
                        let signalBar = "";
                        if (iface.rssi !== undefined && iface.rssi !== null) {
                            // Map -50 (excellent) → 100%, -120 (poor) → 0%
                            const pct = Math.max(0, Math.min(100, Math.round((iface.rssi + 120) / 70 * 100)));
                            const cls = pct > 70 ? "excellent" : pct > 45 ? "good" : pct > 20 ? "fair" : "poor";
                            const snrTxt = iface.snr !== undefined ? `, SNR ${iface.snr} dB` : "";
                            signalBar = `
                                <div class="signal-bar-wrap">
                                    <div class="net-iface-stat-label">Signal (RSSI ${iface.rssi} dBm${snrTxt})</div>
                                    <div class="signal-bar-track"><div class="signal-bar-fill ${cls}" style="width:${pct}%"></div></div>
                                </div>`;
                        }

                        // Airtime / channel load (LoRa)
                        if (iface.airtime_short !== undefined) stats.push({ label:"Airtime (short)", val: (iface.airtime_short*100).toFixed(1) + "%" });
                        if (iface.ch_load !== undefined)       stats.push({ label:"Ch. Load",        val: (iface.ch_load*100).toFixed(1) + "%" });

                        const statCells = stats.map(s =>
                            `<div class="net-iface-stat">
                                <div class="net-iface-stat-label">${escapeHtml(s.label)}</div>
                                <div class="net-iface-stat-value">${escapeHtml(String(s.val))}</div>
                            </div>`
                        ).join("");

                        const typeName   = iface.type.replace("Interface", "").replace("RNode","LoRa / RNode");
                        const isShared   = iface.is_shared;
                        const displayLbl = iface.display_name || iface.name || typeName;
                        // Shared Instance: show a brief explanation, no traffic stats
                        if (isShared) {
                            return `<div class="net-iface-card net-iface-card-shared">
                                <div class="net-iface-card-header">
                                    <span class="net-iface-card-title">${escapeHtml(displayLbl)}</span>
                                    <span class="net-iface-badge">${escapeHtml(typeName || "Local")}</span>
                                    <span class="net-iface-badge online">Local</span>
                                </div>
                                <div class="net-iface-shared-note">
                                    This entry represents the local Reticulum shared-instance connection.
                                    It is always present and allows multiple apps on the same machine
                                    to share one set of physical interfaces — no external traffic flows through it.
                                </div>
                            </div>`;
                        }

                        // Bluetooth gets its own card layout with BT-specific fields
                        if (iface.is_bluetooth) {
                            const btMode  = iface.bt_mode  || "BLE";
                            const btPeers = iface.bt_peers != null ? iface.bt_peers : "—";
                            if (iface.bytes_in  !== undefined) stats.push({ label:"↓ Received",  val: formatBytes(iface.bytes_in)  });
                            if (iface.bytes_out !== undefined) stats.push({ label:"↑ Sent",       val: formatBytes(iface.bytes_out) });
                            const btCells = stats.map(s =>
                                `<div class="net-iface-stat">
                                    <div class="net-iface-stat-label">${escapeHtml(s.label)}</div>
                                    <div class="net-iface-stat-value">${escapeHtml(String(s.val))}</div>
                                </div>`
                            ).join("");
                            return `<div class="net-iface-card net-iface-card-bt">
                                <div class="net-iface-card-header">
                                    <span class="net-iface-bt-icon">&#x2B16;</span>
                                    <span class="net-iface-card-title">${escapeHtml(iface.name || "Bluetooth")}</span>
                                    <span class="net-iface-badge net-iface-badge-bt">${escapeHtml(btMode)}</span>
                                    <span class="${badgeCls}">${badgeTxt}</span>
                                </div>
                                <div class="net-iface-bt-peers">
                                    <span class="net-iface-stat-label">Connected peers</span>
                                    <span class="net-iface-bt-peer-count">${btPeers}</span>
                                </div>
                                <div class="net-iface-stats-grid">${btCells}</div>
                            </div>`;
                        }

                        return `<div class="net-iface-card">
                            <div class="net-iface-card-header">
                                <span class="net-iface-card-title">${escapeHtml(iface.name || typeName)}</span>
                                <span class="net-iface-badge">${escapeHtml(typeName)}</span>
                                <span class="${badgeCls}">${badgeTxt}</span>
                            </div>
                            ${signalBar}
                            <div class="net-iface-stats-grid">${statCells}</div>
                        </div>`;
                    }).join("");
                }
            }

        } catch(e) { console.error("Dashboard refresh error:", e); }

        // Poll every 5 seconds while network view is visible
        clearTimeout(_netPollTimer);
        if (DOM.viewNetwork && !DOM.viewNetwork.classList.contains("hidden")) {
            _netPollTimer = setTimeout(refreshNetworkDashboard, 5000);
        }
    }

    // ── Settings Panel ──
    function openSettings() {
        DOM.settingsModal.classList.remove("hidden");
        loadSettingsState();
    }

    async function loadSettingsState() {
        // Auto-announce toggle + interval
        try {
            const res = await fetch("/api/auto_announce");
            const data = await res.json();
            DOM.toggleAutoAnnounce.checked = data.enabled;
            const intervalInput = document.getElementById("announce-interval");
            if (intervalInput) intervalInput.value = data.interval || 30;
        } catch(e) {}

        // Transport toggle
        loadTransportSetting();

        // Propagation host toggle + hash display
        loadPropagationHostSetting();
        // Outbound propagation node hash
        loadOutboundPropagationSetting();

        // Refresh RNS config/interfaces on every settings open so the
        // list always reflects disk state after a restart.  Previously
        // this only ran when the user clicked the Network tab, so if
        // they restarted, reopened settings, and jumped straight to
        // the Network tab, they saw stale cached interface rows.
        loadRnsConfig();

        // Blocked peers list
        loadBlockedPeers();
    }

    async function loadPropagationHostSetting() {
        try {
            const res = await fetch("/api/propagation/host");
            const data = await res.json();
            const toggle = document.getElementById("toggle-propagation-host");
            const infoRow = document.getElementById("propagation-host-info-row");
            const hashEl = document.getElementById("propagation-host-hash");
            if (toggle) toggle.checked = !!data.enabled;
            if (infoRow) infoRow.style.display = data.enabled ? "" : "none";
            if (hashEl) hashEl.textContent = data.propagation_hash || "—";
        } catch(e) { console.warn("loadPropagationHostSetting:", e); }
    }

    async function loadOutboundPropagationSetting() {
        try {
            const res = await fetch("/api/propagation");
            const data = await res.json();
            const input = document.getElementById("outbound-propagation-input");
            if (input) input.value = data.propagation_node || "";
        } catch(e) { console.warn("loadOutboundPropagationSetting:", e); }
    }

    async function loadBlockedPeers() {
        try {
            const res = await fetch("/api/blocked");
            const blocked = await res.json();
            if (!blocked.length) {
                DOM.blockedList.innerHTML = '<div class="settings-empty">No blocked peers.</div>';
                return;
            }
            DOM.blockedList.innerHTML = blocked.map(b => {
                const name = getPeerName(b.dest_hash);
                return `<div class="blocked-item">
                    <div>
                        <div class="blocked-item-hash">${name !== b.dest_hash.substring(0,16) ? escapeHtml(name) + ' · ' : ''}${b.dest_hash.substring(0,20)}...</div>
                        ${b.reason ? '<div class="blocked-item-reason">' + escapeHtml(b.reason) + '</div>' : ''}
                    </div>
                    <button class="btn btn-unblock" data-hash="${b.dest_hash}">Unblock</button>
                </div>`;
            }).join("");

            DOM.blockedList.querySelectorAll(".btn-unblock").forEach(btn => {
                btn.addEventListener("click", async () => {
                    await fetch(`/api/peers/${btn.dataset.hash}/unblock`, { method: "POST" });
                    showToast("Peer unblocked", "success");
                    loadBlockedPeers();
                });
            });
        } catch(e) {}
    }

    /* Block any identity by manually pasting a hash */
    async function blockManualHash() {
        const hashInput   = document.getElementById("block-manual-hash");
        const reasonInput = document.getElementById("block-manual-reason");
        const raw    = (hashInput?.value || "").trim();
        const reason = (reasonInput?.value || "").trim();
        if (!raw) { showToast("Paste a hash to block", "warning"); return; }

        try {
            const res  = await fetch("/api/block", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ hash: raw, reason }),
            });
            const data = await res.json();
            if (data.error) { showToast(data.error, "error"); return; }
            showToast(`Blocked ${raw.substring(0,16)}…`, "success");
            if (hashInput)   hashInput.value   = "";
            if (reasonInput) reasonInput.value = "";
            loadBlockedPeers();
        } catch(e) {
            showToast("Block failed: " + e.message, "error");
        }
    }

    async function blockPeer() {
        if (!state.activePeer) return;
        const peer = state.peers.find(p => p.dest_hash === state.activePeer);
        const name = peer ? (peer.nickname || peer.display_name || state.activePeer.substring(0,16)) : state.activePeer.substring(0,16);

        // Show an informative confirmation dialog (custom element, not bare confirm())
        const dlg = document.createElement("div");
        dlg.className = "modal-overlay";
        dlg.style.cssText = "z-index:9999;";
        dlg.innerHTML = `
            <div class="modal-card" style="max-width:360px;">
                <div class="modal-header">
                    <h3 style="display:flex;align-items:center;gap:8px;font-size:15px;font-weight:700"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" style="width:17px;height:17px;color:var(--red)"><circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/></svg> Block ${escapeHtml(name)}?</h3>
                </div>
                <div class="modal-body">
                    <p style="font-size:13px;color:var(--text-secondary);margin:0 0 10px;">
                        Their messages and announces will be discarded the moment they arrive —
                        they will never reach your inbox or peer list.
                    </p>
                    <div class="block-notice">
                        <strong>Note about Reticulum transport:</strong> RetiMesh drops all
                        packets from this peer at the application layer. The Reticulum network
                        may still route some encrypted traffic as part of normal mesh forwarding,
                        but none of their content will be delivered to this app. You can unblock
                        them any time from Settings → Blocked Peers.
                    </div>
                    <div style="display:flex;gap:8px;margin-top:16px;">
                        <button id="_blk_cancel" class="btn" style="flex:1;justify-content:center;padding:0.5714rem 1rem;background:var(--bg-elev);color:var(--text-primary);border:1px solid var(--border-strong);border-radius:var(--radius-sm);font-weight:500;">Cancel</button>
                        <button id="_blk_confirm" class="btn" style="flex:1;justify-content:center;padding:0.5714rem 1rem;background:#f8514933;color:#f85149;border:1px solid #f8514955;border-radius:var(--radius-sm);font-weight:700;">Block</button>
                    </div>
                </div>
            </div>`;
        document.body.appendChild(dlg);

        await new Promise(resolve => {
            dlg.querySelector("#_blk_cancel").addEventListener("click",  () => { dlg.remove(); resolve(false); });
            dlg.querySelector("#_blk_confirm").addEventListener("click", () => { dlg.remove(); resolve(true);  });
        }).then(async confirmed => {
            if (!confirmed) return;
            try {
                const res  = await fetch(`/api/peers/${state.activePeer}/block`, {
                    method:  "POST",
                    headers: {"Content-Type": "application/json"},
                    body:    JSON.stringify({ reason: "Blocked by user" }),
                });
                const data = await res.json().catch(() => ({}));
                showToast(name + " blocked, messages discarded on arrival", "info");
                // Refresh peer list so the blocked peer is visually marked / removed
                try {
                    const pr = await fetch("/api/peers");
                    if (pr.ok) {
                        state.peers = await pr.json();
                        renderPeerList();
                    }
                } catch (_) {}
            } catch(e) { console.error("Block failed:", e); }
        });
    }

    async function loadRnsConfig() {
        try {
            const res = await fetch("/api/rns_config");
            const data = await res.json();
            if (data.config) {
                DOM.rnsConfigEditor.value = data.config;
                parseAndDisplayInterfaces(data.config);
            }
        } catch(e) {}
    }

    // ── Transport Toggle ──
    async function loadTransportSetting() {
        try {
            const res  = await fetch("/api/transport");
            const data = await res.json();
            const chk  = document.getElementById("toggle-transport");
            if (!chk) return;
            // Prefer config file value; fall back to runtime value
            const enabled = (data.config_value !== null && data.config_value !== undefined)
                ? data.config_value
                : data.transport_enabled;
            chk.checked = !!enabled;
        } catch(e) {}
    }

    async function setTransport(enabled) {
        try {
            const res  = await fetch("/api/transport", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ enabled }),
            });
            const data = await res.json();
            if (data.error) { showToast(data.error, "error"); return; }
            showToast("Transport setting saved. Restart to apply.", "info");
        } catch(e) {
            showToast("Failed to save transport setting", "error");
        }
    }

    // Interface type field templates
    const IFACE_TEMPLATES = {
        "AutoInterface": {
            fields: [
                { name: "name", label: "Name", default: "Default Interface" },
            ],
            template: (f) => `  [[${f.name}]]\n    type = AutoInterface\n    enabled = Yes\n`
        },
        "TCPServerInterface": {
            fields: [
                { name: "name", label: "Name", default: "TCP Server" },
                { name: "listen_ip", label: "Listen IP", default: "0.0.0.0" },
                { name: "listen_port", label: "Port", default: "4242" },
            ],
            template: (f) => `  [[${f.name}]]\n    type = TCPServerInterface\n    enabled = Yes\n    listen_ip = ${f.listen_ip}\n    listen_port = ${f.listen_port}\n`
        },
        "TCPClientInterface": {
            fields: [
                { name: "name", label: "Name", default: "TCP Client" },
                { name: "target_host", label: "Target Host", default: "192.168.1.x" },
                { name: "target_port", label: "Target Port", default: "4242" },
            ],
            template: (f) => `  [[${f.name}]]\n    type = TCPClientInterface\n    enabled = Yes\n    target_host = ${f.target_host}\n    target_port = ${f.target_port}\n`
        },
        "UDPInterface": {
            fields: [
                { name: "name", label: "Name", default: "UDP Interface" },
                { name: "listen_ip", label: "Listen IP", default: "0.0.0.0" },
                { name: "listen_port", label: "Listen Port", default: "4243" },
                { name: "forward_ip", label: "Forward IP", default: "255.255.255.255" },
                { name: "forward_port", label: "Forward Port", default: "4243" },
            ],
            template: (f) => `  [[${f.name}]]\n    type = UDPInterface\n    enabled = Yes\n    listen_ip = ${f.listen_ip}\n    listen_port = ${f.listen_port}\n    forward_ip = ${f.forward_ip}\n    forward_port = ${f.forward_port}\n`
        },
        "I2PInterface": {
            fields: [
                { name: "name", label: "Name", default: "I2P Interface" },
                { name: "peers", label: "Peers (hash)", default: "" },
            ],
            template: (f) => `  [[${f.name}]]\n    type = I2PInterface\n    enabled = Yes\n    connectable = yes\n${f.peers ? '    peers = ' + f.peers + '\n' : ''}`
        },
        "RNodeInterface": {
            fields: [
                { name: "name", label: "Name", default: "RNode LoRa" },
                { name: "port", label: "Serial Port", default: "/dev/ttyUSB0" },
                { name: "frequency", label: "Frequency (Hz)", default: "867200000" },
                { name: "bandwidth", label: "Bandwidth (Hz)", default: "125000" },
                { name: "txpower", label: "TX Power (dBm)", default: "7" },
                { name: "spreadingfactor", label: "Spreading Factor", default: "8" },
            ],
            template: (f) => `  [[${f.name}]]\n    type = RNodeInterface\n    enabled = Yes\n    port = ${f.port}\n    frequency = ${f.frequency}\n    bandwidth = ${f.bandwidth}\n    txpower = ${f.txpower}\n    spreadingfactor = ${f.spreadingfactor}\n`
        },
        // ── Yggdrasil: overlay-IPv6 networks ─────────────────────────────
        // Yggdrasil (https://yggdrasil-network.github.io/) is a self-arranging
        // encrypted IPv6 mesh.  It runs as a separate OS-level daemon that
        // creates a TUN device (e.g. tun0 on Linux, Windows TAP adapter) with a
        // static IPv6 in the 200::/7 range.  RetiMesh doesn't embed Yggdrasil;
        // we use plain TCP interfaces bound to the Yggdrasil tun to reach
        // other RetiMesh peers across the overlay.  This mirrors how the
        // official RNS manual documents Yggdrasil usage: a TCPServerInterface
        // that listens on the overlay address, and TCPClientInterfaces on
        // peers that connect to it.
        //
        // Prerequisite: install and run yggdrasil-go separately on each node.
        // Get each node's IPv6 with `yggdrasilctl getSelf`.
        "YggdrasilListen": {
            fields: [
                { name: "name",        label: "Name",              default: "Yggdrasil Listen" },
                { name: "listen_ip",   label: "Yggdrasil IPv6",    default: "::" },
                { name: "listen_port", label: "Listen Port",       default: "4343" },
            ],
            // Bind to the Yggdrasil overlay address so only nodes reachable
            // via Yggdrasil can connect.  Setting listen_ip = ::  binds to
            // all interfaces (recommended for convenience); advanced users
            // can paste the node's Yggdrasil-specific IPv6 here instead.
            template: (f) => `  [[${f.name}]]\n    type = TCPServerInterface\n    enabled = Yes\n    listen_ip = ${f.listen_ip}\n    listen_port = ${f.listen_port}\n`
        },
        "YggdrasilConnect": {
            fields: [
                { name: "name",        label: "Name",                default: "Yggdrasil Peer" },
                { name: "target_host", label: "Remote Yggdrasil IPv6", default: "200::xxxx:xxxx:xxxx:xxxx" },
                { name: "target_port", label: "Remote Port",          default: "4343" },
            ],
            // IPv6 literals in TCP target_host are wrapped in square brackets
            // by convention; RNS handles both forms, but wrapping is safer
            // if the user pastes an address that also contains a port.
            template: (f) => `  [[${f.name}]]\n    type = TCPClientInterface\n    enabled = Yes\n    target_host = ${f.target_host}\n    target_port = ${f.target_port}\n`
        },
    };

    function parseAndDisplayInterfaces(configText) {
        const ifaceList = document.getElementById("iface-config-list");
        if (!ifaceList) return;

        // Simple parser: find [[Name]] blocks under [interfaces]
        const lines = configText.split("\n");
        const interfaces = [];
        let inInterfaces = false;
        let current = null;

        for (const line of lines) {
            const trimmed = line.trim();
            if (trimmed === "[interfaces]") { inInterfaces = true; continue; }
            if (trimmed.startsWith("[") && !trimmed.startsWith("[[") && inInterfaces) { inInterfaces = false; continue; }
            if (!inInterfaces) continue;

            const nameMatch = trimmed.match(/^\[\[(.+)\]\]$/);
            if (nameMatch) {
                if (current) interfaces.push(current);
                current = { name: nameMatch[1], props: {} };
                continue;
            }
            if (current && trimmed.includes("=")) {
                const [key, ...rest] = trimmed.split("=");
                current.props[key.trim()] = rest.join("=").trim();
            }
        }
        if (current) interfaces.push(current);

        if (!interfaces.length) {
            ifaceList.innerHTML = '<div class="settings-empty">No interfaces configured.</div>';
            return;
        }

        ifaceList.innerHTML = interfaces.map((iface, i) => {
            const type = iface.props.type || "Unknown";
            const enabled = (iface.props.enabled || "").toLowerCase();
            const isOn = enabled === "yes" || enabled === "true";
            const badge = isOn
                ? '<span class="iface-enabled-badge on">Enabled</span>'
                : '<span class="iface-enabled-badge off">Disabled</span>';

            let detail = "";
            if (iface.props.listen_ip) detail += `listen: ${iface.props.listen_ip}:${iface.props.listen_port || ""} `;
            if (iface.props.target_host) detail += `target: ${iface.props.target_host}:${iface.props.target_port || ""} `;
            if (iface.props.port) detail += `port: ${iface.props.port} `;
            if (iface.props.frequency) detail += `freq: ${(parseInt(iface.props.frequency)/1e6).toFixed(1)} MHz `;

            return `<div class="iface-config-item">
                <div class="iface-config-info">
                    <div class="iface-config-name">${escapeHtml(iface.name)} ${badge}</div>
                    <div class="iface-config-detail">${type} ${detail}</div>
                </div>
                <div class="iface-config-actions">
                    <button class="btn btn-iface-toggle" data-idx="${i}" data-enabled="${isOn}">${isOn ? "Disable" : "Enable"}</button>
                    <button class="btn btn-iface-remove" data-idx="${i}">Remove</button>
                </div>
            </div>`;
        }).join("");

        // Bind toggle/remove buttons
        ifaceList.querySelectorAll(".btn-iface-toggle").forEach(btn => {
            btn.addEventListener("click", () => {
                const idx = parseInt(btn.dataset.idx);
                const iface = interfaces[idx];
                const isOn = btn.dataset.enabled === "true";
                toggleInterfaceInConfig(iface.name, !isOn);
            });
        });

        ifaceList.querySelectorAll(".btn-iface-remove").forEach(btn => {
            btn.addEventListener("click", () => {
                const idx = parseInt(btn.dataset.idx);
                const iface = interfaces[idx];
                showConfirm(
                    `Remove interface "${iface.name}"? It will be deleted from the config.`,
                    () => removeInterfaceFromConfig(iface.name),
                    { title: "Remove Interface", okLabel: "Remove", okClass: "btn btn-danger" }
                );
            });
        });
    }

    function toggleInterfaceInConfig(ifaceName, enable) {
        let config = DOM.rnsConfigEditor.value;
        const regex = new RegExp(`(\\[\\[${escapeRegex(ifaceName)}\\]\\][\\s\\S]*?enabled\\s*=\\s*)(Yes|No|True|False)`, "i");
        config = config.replace(regex, `$1${enable ? "Yes" : "No"}`);
        DOM.rnsConfigEditor.value = config;
        saveRnsConfigAndReload();
    }

    function removeInterfaceFromConfig(ifaceName) {
        let config = DOM.rnsConfigEditor.value;
        // Remove the [[Name]] block and all indented lines after it until next [[ or [
        const regex = new RegExp(`\\s*\\[\\[${escapeRegex(ifaceName)}\\]\\][\\s\\S]*?(?=\\s*\\[|$)`, "");
        config = config.replace(regex, "\n");
        DOM.rnsConfigEditor.value = config;
        saveRnsConfigAndReload();
    }

    function escapeRegex(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }

    async function saveRnsConfigAndReload() {
        await saveRnsConfig();
        // Re-parse to refresh the list
        parseAndDisplayInterfaces(DOM.rnsConfigEditor.value);
    }

    function initIfaceTypeSelect() {
        const select = document.getElementById("iface-type-select");
        const fieldsDiv = document.getElementById("iface-fields");
        const addBtn = document.getElementById("btn-add-iface");
        if (!select || !fieldsDiv || !addBtn) return;

        select.addEventListener("change", () => {
            const type = select.value;
            const tmpl = IFACE_TEMPLATES[type];
            if (!tmpl) { fieldsDiv.innerHTML = ""; return; }

            // Yggdrasil needs the external daemon running — surface that
            // prerequisite inline so users know what to install.
            let hint = "";
            if (type === "YggdrasilListen" || type === "YggdrasilConnect") {
                hint = `<div class="iface-hint" style="background:var(--bg-secondary);border-left:3px solid #a78bfa;padding:10px 12px;margin-bottom:10px;border-radius:4px;font-size:12px;line-height:1.5;">
                    <strong>Prerequisite:</strong> install and run
                    <a href="https://yggdrasil-network.github.io/installation.html" target="_blank" rel="noopener" style="color:#a78bfa;">yggdrasil-go</a>
                    as a separate service on this machine. Each node gets a
                    static IPv6 in the <code>200::/7</code> range. Find yours
                    with <code>yggdrasilctl getSelf</code>.
                    ${type === "YggdrasilConnect"
                        ? "<br>Ask the peer for their Yggdrasil IPv6 and paste it below."
                        : "<br>Share this node's Yggdrasil IPv6 with peers so they can connect."}
                </div>`;
            }

            fieldsDiv.innerHTML = hint + tmpl.fields.map(f =>
                `<div class="iface-field-row">
                    <span class="iface-field-label">${f.label}</span>
                    <input type="text" class="iface-field-input" data-field="${f.name}" value="${f.default}" placeholder="${f.default}">
                </div>`
            ).join("");
        });

        addBtn.addEventListener("click", async () => {
            const type = select.value;
            const tmpl = IFACE_TEMPLATES[type];
            if (!tmpl) { showToast("Select an interface type first", "error"); return; }

            // Collect field values
            const fields = {};
            fieldsDiv.querySelectorAll(".iface-field-input").forEach(inp => {
                fields[inp.dataset.field] = inp.value.trim() || inp.placeholder;
            });

            // Generate config block (all other types → INI file)
            const block = tmpl.template(fields);

            // Append to config
            let config = DOM.rnsConfigEditor.value;
            if (!config.includes("[interfaces]")) {
                config += "\n[interfaces]\n";
            }
            config = config.trimEnd() + "\n\n" + block;
            DOM.rnsConfigEditor.value = config;
            saveRnsConfigAndReload();
            showToast(`${fields.name || type} added. Restart to apply.`, "success");
            select.value = "";
            fieldsDiv.innerHTML = "";
        });
    }

    // ── Bluetooth interface list (shown in RNS Config tab) ───────────────────
    async function saveRnsConfig() {
        const content = DOM.rnsConfigEditor.value;
        try {
            const res = await fetch("/api/rns_config", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ config: content }),
            });
            const data = await res.json();
            if (data.status === "ok") {
                DOM.rnsConfigStatus.textContent = "Saved! Restart to apply.";
                showToast("Config saved. Restart to apply.", "success");
                setTimeout(() => { DOM.rnsConfigStatus.textContent = ""; }, 4000);
            } else {
                DOM.rnsConfigStatus.textContent = "Error: " + (data.error || "Unknown");
            }
        } catch(e) { DOM.rnsConfigStatus.textContent = "Save failed"; }
    }

    function initSettingsTabs() {
        const tabs = document.querySelectorAll(".settings-tab");
        const panels = {
            "general":         document.getElementById("settings-general"),
            "blocked":         document.getElementById("settings-blocked"),
            "rns-config":      document.getElementById("settings-rns-config"),
            "bluetooth":       document.getElementById("settings-bluetooth"),
            "alerts-settings": document.getElementById("settings-alerts-settings"),
            "dependencies":    document.getElementById("settings-dependencies"),
        };

        tabs.forEach(tab => {
            tab.addEventListener("click", () => {
                tabs.forEach(t => t.classList.remove("active"));
                tab.classList.add("active");
                Object.values(panels).forEach(p => { if(p) p.classList.add("hidden"); });
                const target = panels[tab.dataset.tab];
                if (target) target.classList.remove("hidden");

                // Load data for the tab
                if (tab.dataset.tab === "rns-config")      loadRnsConfig();
                if (tab.dataset.tab === "bluetooth") {
                    loadBluetoothInterfaces();
                    _wireBtScanBtn();
                }
                if (tab.dataset.tab === "alerts-settings")  _loadAlertsSettings();
                if (tab.dataset.tab === "dependencies")      loadDependencies();
                if (tab.dataset.tab === "blocked") {
                    loadBlockedPeers();
                    // Wire manual-block button once
                    const btnBlock = document.getElementById("btn-block-manual");
                    if (btnBlock && !btnBlock._wired) {
                        btnBlock._wired = true;
                        btnBlock.addEventListener("click", blockManualHash);
                        const hashInput = document.getElementById("block-manual-hash");
                        if (hashInput) hashInput.addEventListener("keydown", e => { if (e.key === "Enter") blockManualHash(); });
                    }
                }
            });
        });

        // Wire Bluetooth add button once
        const btnBtAdd = document.getElementById("btn-bt-add");
        if (btnBtAdd && !btnBtAdd._wired) {
            btnBtAdd._wired = true;
            btnBtAdd.addEventListener("click", addBluetoothInterface);
        }
    }

    // ── Bluetooth Interface Management ──

    async function loadBluetoothInterfaces() {
        const listEl  = document.getElementById("bt-iface-list");
        const warnEl  = document.getElementById("bt-unavailable-banner");
        const okEl    = document.getElementById("bt-available-banner");
        const addSect = document.getElementById("bt-add-section");
        if (!listEl) return;
        try {
            const res  = await fetch("/api/bluetooth");
            const data = await res.json();
            if (warnEl) {
                warnEl.classList.toggle("hidden", data.available);
                // When the import is still failing, replace the generic
                // "install bleak bless" copy with the actual reason — the
                // missing dependency might not be one of those packages
                // (e.g. the RNS_BluetoothInterface wrapper itself), and
                // showing the real error name helps the user fix it.
                if (!data.available && data.load_error) {
                    const err = String(data.load_error);
                    let hint;
                    if (/RNS_BluetoothInterface/i.test(err)) {
                        hint = "RNS_BluetoothInterface package not found alongside retimesh.py.";
                    } else if (/\bbleak\b/i.test(err)) {
                        hint = "Missing 'bleak': run pip install bleak then revisit this tab.";
                    } else if (/\bbless\b/i.test(err)) {
                        hint = "Missing 'bless': run pip install bless then revisit this tab.";
                    } else {
                        hint = "Bluetooth unavailable: " + err;
                    }
                    // Preserve the icon (first child) and replace text only.
                    const iconHTML = warnEl.querySelector("svg")?.outerHTML || "";
                    warnEl.innerHTML = iconHTML + " " + escapeHtml(hint);
                }
            }
            if (okEl)    okEl.classList.toggle("hidden",   !data.available);
            if (addSect) addSect.style.opacity = data.available ? "1" : "0.5";
            const ifaces = data.interfaces || [];
            if (!ifaces.length) {
                listEl.innerHTML = '<div class="settings-empty">No Bluetooth interfaces configured.</div>';
                return;
            }
            listEl.innerHTML = ifaces.map(iface => {
                const statusDot = iface.online
                    ? '<span class="bt-dot bt-dot-online" title="Online">●</span>'
                    : '<span class="bt-dot bt-dot-offline" title="Offline">●</span>';
                const rxkb = ((iface.rxb || 0) / 1024).toFixed(1);
                const txkb = ((iface.txb || 0) / 1024).toFixed(1);
                const bleConnected = iface.bt_peers != null ? iface.bt_peers : 0;
                // RNS-level peers reachable via this interface
                const rnsPeers = iface.rns_peers || [];
                const rnsPeersHtml = rnsPeers.length
                    ? `<div class="bt-rns-peers">
                           <span class="bt-rns-peers-label">Reachable via BT:</span>
                           ${rnsPeers.map(rp =>
                               `<button class="btn btn-xs btn-primary bt-rns-msg" data-hash="${escapeHtml(rp.dest_hash)}"
                                        title="Open chat with ${escapeHtml(rp.display_name)}">
                                    💬 ${escapeHtml(rp.display_name)}
                                </button>`
                           ).join("")}
                       </div>`
                    : (iface.online
                        ? `<div class="bt-rns-peers"><span class="bt-rns-peers-label" style="color:var(--text-muted)">No RNS peers discovered yet</span></div>`
                        : "");
                return `<div class="bt-iface-item" data-name="${escapeHtml(iface.name)}">
                    <div class="bt-iface-info">
                        <span class="bt-iface-name">${statusDot} ${escapeHtml(iface.name)}</span>
                        <span class="bt-iface-meta">mode: ${escapeHtml(iface.mode || 'ble')} &nbsp;·&nbsp;
                            BLE peers: ${bleConnected} &nbsp;·&nbsp; ↓${rxkb}KB ↑${txkb}KB</span>
                        ${rnsPeersHtml}
                    </div>
                    <div class="bt-iface-actions">
                        <label class="toggle-switch" title="${iface.enabled ? 'Enabled' : 'Disabled'}">
                            <input type="checkbox" class="bt-toggle" data-name="${escapeHtml(iface.name)}"
                                   ${iface.enabled ? 'checked' : ''}>
                            <span class="toggle-slider"></span>
                        </label>
                        <button class="btn btn-sm btn-danger bt-remove" data-name="${escapeHtml(iface.name)}"
                                title="Remove interface">✕</button>
                    </div>
                </div>`;
            }).join("");
            listEl.querySelectorAll(".bt-toggle").forEach(chk => {
                chk.addEventListener("change", async () => {
                    const name = chk.dataset.name; const enabled = chk.checked;
                    try {
                        await fetch(`/api/bluetooth/${encodeURIComponent(name)}/toggle`, {
                            method: "POST", headers: {"Content-Type": "application/json"},
                            body: JSON.stringify({ enabled }),
                        });
                        showToast(`Bluetooth '${name}' ${enabled ? "enabled" : "disabled"}`, "info");
                        setTimeout(loadBluetoothInterfaces, 1000);
                    } catch (e) { showToast("Toggle failed: " + e.message, "error"); chk.checked = !enabled; }
                });
            });
            listEl.querySelectorAll(".bt-remove").forEach(btn => {
                btn.addEventListener("click", () => {
                    const name = btn.dataset.name;
                    showConfirm(
                        `Remove Bluetooth interface "${name}"? It will stop immediately.`,
                        async () => {
                            try {
                                const res = await fetch(`/api/bluetooth/${encodeURIComponent(name)}`, {method: "DELETE"});
                                if (!res.ok) throw new Error(await res.text());
                                showToast(`Interface "${name}" removed`, "success");
                                loadBluetoothInterfaces();
                            } catch (e) { showToast("Remove failed: " + e.message, "error"); }
                        },
                        { title: "Remove Bluetooth Interface", okLabel: "Remove", okClass: "btn btn-danger" }
                    );
                });
            });
            // "Message" buttons for RNS peers reachable via BT
            listEl.querySelectorAll(".bt-rns-msg").forEach(btn => {
                btn.addEventListener("click", () => {
                    const hash = btn.dataset.hash;
                    if (hash) selectPeer(hash);
                });
            });
        } catch (e) {
            listEl.innerHTML = `<div class="settings-empty">Error: ${escapeHtml(e.message)}</div>`;
        }
    }

    async function addBluetoothInterface() {
        const statusEl = document.getElementById("bt-add-status");
        const name     = (document.getElementById("bt-name")?.value || "").trim();
        if (!name) {
            if (statusEl) { statusEl.textContent = "Name is required."; statusEl.style.color = "var(--red)"; }
            return;
        }
        const payload = {
            name,
            mode:          document.getElementById("bt-mode")?.value || "ble",
            scan_interval: parseInt(document.getElementById("bt-scan-interval")?.value || "30"),
            max_peers:     parseInt(document.getElementById("bt-max-peers")?.value || "8"),
            target_mtu:    parseInt(document.getElementById("bt-target-mtu")?.value || "512"),
            discoverable:  document.getElementById("bt-discoverable")?.checked ?? true,
            static_peers:  document.getElementById("bt-static-peers")?.value || "",
        };
        if (statusEl) { statusEl.textContent = "Adding…"; statusEl.style.color = ""; }
        try {
            const res  = await fetch("/api/bluetooth", {
                method: "POST", headers: {"Content-Type": "application/json"},
                body: JSON.stringify(payload),
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
            if (statusEl) statusEl.textContent = "";
            showToast(`Bluetooth interface '${name}' added and started`, "success");
            const nameEl = document.getElementById("bt-name");
            if (nameEl) nameEl.value = `BT${Date.now() % 100}`;
            loadBluetoothInterfaces();
        } catch (e) {
            if (statusEl) { statusEl.textContent = e.message; statusEl.style.color = "var(--red)"; }
            showToast("Failed to add Bluetooth interface: " + e.message, "error");
        }
    }

    // ── Bluetooth device scan (BLE discovery) ────────────────────────────────
    async function scanBluetoothDevices() {
        const btn     = document.getElementById("btn-bt-scan");
        const resultsEl = document.getElementById("bt-scan-results");
        if (!resultsEl) return;

        if (btn) { btn.disabled = true; btn.textContent = "Scanning…"; }
        resultsEl.innerHTML = '<div class="settings-empty">Scanning for BLE devices (5 s)…</div>';
        resultsEl.classList.remove("hidden");

        try {
            const res  = await fetch("/api/bluetooth/scan?timeout=5");
            const data = await res.json();
            if (!res.ok || data.error) {
                resultsEl.innerHTML = `<div class="settings-empty" style="color:var(--red)">Scan failed: ${escapeHtml(data.error || res.statusText)}</div>`;
                return;
            }
            const devices = data.devices || [];
            if (!devices.length) {
                resultsEl.innerHTML = '<div class="settings-empty">No BLE devices found nearby.</div>';
                return;
            }
            resultsEl.innerHTML = devices.map(d => {
                const rssiBar = Math.max(0, Math.min(100, 100 + (d.rssi || -100)));
                return `
                <div class="bt-scan-device">
                    <div class="bt-scan-device-info">
                        <span class="bt-scan-device-name">${escapeHtml(d.name || "Unknown")}</span>
                        <span class="bt-scan-device-addr">${escapeHtml(d.address)}</span>
                    </div>
                    <div class="bt-scan-device-rssi" title="RSSI: ${d.rssi} dBm">
                        <div class="bt-rssi-bar" style="width:${rssiBar}%"></div>
                        <span>${d.rssi} dBm</span>
                    </div>
                </div>`;
            }).join("");
        } catch (e) {
            resultsEl.innerHTML = `<div class="settings-empty" style="color:var(--red)">Scan error: ${escapeHtml(e.message)}</div>`;
        } finally {
            if (btn) { btn.disabled = false; btn.textContent = "Scan for BLE Devices"; }
        }
    }

    // Wire scan button once when the Bluetooth tab loads
    function _wireBtScanBtn() {
        const btn = document.getElementById("btn-bt-scan");
        if (btn && !btn._wired) {
            btn._wired = true;
            btn.addEventListener("click", scanBluetoothDevices);
        }
    }

    // ── Identity Management ──
    async function loadIdentities() {
        try {
            const res = await fetch("/api/identities");
            const identities = await res.json();
            renderIdentityList(identities);
        } catch (e) { console.error("Failed to load identities:", e); }
    }

    function renderIdentityList(identities) {
        if (!identities.length) {
            // Show current identity as the only one
            const current = state.identity || {};
            DOM.identityList.innerHTML = `
                <div class="identity-item active">
                    <div class="identity-item-info">
                        <div class="identity-item-name">Default Identity</div>
                        <div class="identity-item-hash">${current.lxmf_address || current.identity_hash || "loading..."}</div>
                    </div>
                    <span class="identity-item-badge">Active</span>
                </div>`;
            return;
        }

        DOM.identityList.innerHTML = identities.map(id => {
            const isActive = id.is_active ? "active" : "";
            const badge = id.is_active ? '<span class="identity-item-badge">Active</span>' : "";
            const actions = id.is_active ? "" : `
                <div class="identity-item-actions">
                    <button class="btn btn-secondary btn-sm btn-id-activate" data-id="${id.id}">Activate</button>
                    <button class="btn btn-danger btn-sm btn-id-delete" data-id="${id.id}">Delete</button>
                </div>`;
            return `
                <div class="identity-item ${isActive}">
                    <div class="identity-item-info">
                        <div class="identity-item-name">${escapeHtml(id.name)}</div>
                        <div class="identity-item-hash">${id.lxmf_hash || id.file_path.split("/").pop()}</div>
                    </div>
                    ${badge}${actions}
                </div>`;
        }).join("");

        // Bind activate/delete buttons
        DOM.identityList.querySelectorAll(".btn-id-activate").forEach(btn => {
            btn.addEventListener("click", async () => {
                const idId = btn.dataset.id;
                const res = await fetch(`/api/identities/${idId}/activate`, { method: "POST" });
                const data = await res.json();
                if (data.status === "ok") {
                    loadIdentities();
                    // Show prominent restart banner
                    const banner = document.createElement("div");
                    banner.className = "identity-restart-banner";
                    banner.innerHTML = '<strong>Restart Required</strong><br>Close and reopen the app to switch to this identity.';
                    // Remove any existing banner first
                    const old = DOM.identityModal.querySelector(".identity-restart-banner");
                    if (old) old.remove();
                    DOM.identityList.parentElement.appendChild(banner);
                }
            });
        });

        DOM.identityList.querySelectorAll(".btn-id-delete").forEach(btn => {
            btn.addEventListener("click", () => {
                showConfirm(
                    "Delete this identity? This will also permanently remove all messages, file transfers, and alerts associated with it. This cannot be undone.",
                    async () => {
                        const res = await fetch(`/api/identities/${btn.dataset.id}`, { method: "DELETE" });
                        if (!res.ok) {
                            showToast("Failed to delete identity", "error");
                            return;
                        }
                        showToast("Identity and associated data deleted", "info");
                        loadIdentities();
                    },
                    { title: "Delete Identity & Data", okLabel: "Delete Permanently", okClass: "btn btn-danger" }
                );
            });
        });
    }

    // ── Known Peers (seen via RNS announce) ──────────────────────────────
    async function loadKnownPeers() {
        const list = document.getElementById("known-peers-list");
        if (!list) return;
        try {
            const res = await fetch("/api/identities/known");
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const peers = await res.json();
            renderKnownPeers(peers);
        } catch (e) {
            if (list) list.innerHTML = '<div class="identity-list-empty">Failed to load peers.</div>';
            console.error("loadKnownPeers error:", e);
        }
    }

    function renderKnownPeers(peers) {
        const list = document.getElementById("known-peers-list");
        if (!list) return;

        if (!peers || !peers.length) {
            list.innerHTML = '<div class="identity-list-empty">No peers discovered yet. Trigger an announce to find peers.</div>';
            return;
        }

        list.innerHTML = peers.map(p => {
            const name    = p.display_name || p.nickname || p.dest_hash.slice(0, 12);
            const initial = name.charAt(0).toUpperCase();
            const hash    = p.dest_hash || "";
            const idHash  = p.identity_hash || "";
            const ago     = p.last_announce ? _timeAgo(p.last_announce) : (p.last_seen ? _timeAgo(p.last_seen) : "unknown");
            return `
                <div class="known-peer-item" data-dest="${escapeHtml(hash)}">
                    <div class="known-peer-avatar">${escapeHtml(initial)}</div>
                    <div class="known-peer-info">
                        <div class="known-peer-name">${escapeHtml(name)}</div>
                        <div class="known-peer-hash" title="${escapeHtml(hash)}">${hash.slice(0, 20)}…</div>
                        ${idHash ? `<div class="known-peer-hash" title="Identity: ${escapeHtml(idHash)}" style="opacity:0.6">id: ${idHash.slice(0,16)}…</div>` : ""}
                    </div>
                    <div class="known-peer-meta">${escapeHtml(ago)}</div>
                    <div class="known-peer-actions">
                        <button class="btn btn-sm btn-secondary btn-kp-message" data-dest="${escapeHtml(hash)}" data-name="${escapeHtml(name)}" title="Open conversation">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:11px;height:11px"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
                        </button>
                    </div>
                </div>`;
        }).join("");

        // Bind "Message" button — open DM conversation and close modal
        list.querySelectorAll(".btn-kp-message").forEach(btn => {
            btn.addEventListener("click", () => {
                const dest = btn.dataset.dest;
                // Close identity modal first
                const modal = document.getElementById("identity-modal");
                if (modal) modal.classList.add("hidden");
                // Navigate to the peer's conversation (adds to peer list if needed)
                selectPeer(dest);
            });
        });
    }

    /** Format a UNIX timestamp as a relative time string, e.g. "3 min ago". */
    function _timeAgo(ts) {
        const diff = Math.floor(Date.now() / 1000 - ts);
        if (diff < 5)    return "just now";
        if (diff < 60)   return `${diff}s ago`;
        if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
        if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
        return `${Math.floor(diff / 86400)}d ago`;
    }

    async function createIdentity() {
        const name = DOM.identityNewName.value.trim();
        if (!name) { DOM.identityNewName.focus(); return; }

        DOM.btnCreateIdentity.textContent = "Creating...";
        DOM.btnCreateIdentity.disabled = true;

        try {
            const res = await fetch("/api/identities", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ name }),
            });
            const data = await res.json();
            if (data.status === "ok") {
                DOM.identityNewName.value = "";
                loadIdentities();
            }
        } catch (e) { console.error("Create identity failed:", e); }

        DOM.btnCreateIdentity.textContent = "Create";
        DOM.btnCreateIdentity.disabled = false;
    }

    // ── Events ──
    function bindEvents() {
        DOM.btnSend.addEventListener("click", sendMessage);
        DOM.composeInput.addEventListener("keydown", e => { if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();sendMessage();} });
        DOM.btnAnnounce.addEventListener("click", async () => {
            let name = "RetiMesh User";
            try {
                const cfg = await fetch("/api/config").then(r=>r.json());
                if (cfg.display_name) name = cfg.display_name;
            } catch(e) {}
            wsSend({type:"announce", display_name: name});
            // Visual feedback
            DOM.btnAnnounce.classList.add("sent");
            DOM.btnAnnounce.textContent = "Announced!";
            showToast("Announce sent to the network", "success");
            setTimeout(() => {
                DOM.btnAnnounce.classList.remove("sent");
                DOM.btnAnnounce.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"><path d="M5 12H3M12 5V3M12 21v-2M4.22 4.22l1.42 1.42M18.36 5.64l-1.42 1.42M20 12h-2M16.95 16.95l1.41 1.41M5.64 18.36l1.41-1.41"/><circle cx="12" cy="12" r="4"/></svg>Announce`;
            }, 2000);
        });
        DOM.btnAttach.addEventListener("click", () => DOM.fileInput.click());
        DOM.fileInput.addEventListener("change", e => { for(const f of e.target.files) sendFile(f); e.target.value=""; });

        // Voice-note recorder — toggle start/stop on click
        const btnVoice = document.getElementById("btn-voice-record");
        if (btnVoice) btnVoice.addEventListener("click", toggleVoiceRecord);
        DOM.btnCall.addEventListener("click", startCall);
        DOM.btnCallEnd.addEventListener("click", endCall);
        if(DOM.btnCallAccept) DOM.btnCallAccept.addEventListener("click", acceptCall);
        // Codec picker modal close button
        if (DOM.btnCodecClose) DOM.btnCodecClose.addEventListener("click", () => DOM.codecModal.classList.add("hidden"));
        // Wire "Call with X" buttons inside the codec picker
        document.querySelectorAll(".btn-codec-start").forEach(btn => {
            btn.addEventListener("click", () => {
                if (btn.disabled) return;
                DOM.codecModal.classList.add("hidden");
                _initiateCall(btn.dataset.codec || "mu_law");
            });
        });
        // "Click to copy" on the identity bar: the title is stored as
        // "Click to copy · <hash>" for tooltip display, but we must copy
        // ONLY the hash, not the tooltip string.  Prefer textContent (the
        // visible hash) as the source of truth, and fall back to stripping
        // the prefix from title if textContent is temporarily something
        // else (e.g. "Copied!" during feedback).
        DOM.myHash.addEventListener("click", () => {
            let hash = (DOM.myHash.textContent || "").trim();
            if (!hash || hash === "Copied!") {
                const t = (DOM.myHash.title || "").trim();
                const sep = t.indexOf("·");
                hash = sep >= 0 ? t.slice(sep + 1).trim() : t;
            }
            if (!hash) return;
            navigator.clipboard.writeText(hash).then(() => {
                const o = DOM.myHash.textContent;
                DOM.myHash.textContent = "Copied!";
                setTimeout(() => DOM.myHash.textContent = o, 1200);
            });
        });

        // Identity management
        if (DOM.btnIdentityManage) {
            DOM.btnIdentityManage.addEventListener("click", () => {
                DOM.identityModal.classList.remove("hidden");
                loadIdentities();
                loadKnownPeers();
            });
        }
        if (DOM.btnIdentityClose) DOM.btnIdentityClose.addEventListener("click", () => DOM.identityModal.classList.add("hidden"));
        // Refresh known peers button
        const btnRefreshKnown = document.getElementById("btn-refresh-known-peers");
        if (btnRefreshKnown) btnRefreshKnown.addEventListener("click", () => loadKnownPeers());
        if (DOM.btnCreateIdentity) DOM.btnCreateIdentity.addEventListener("click", createIdentity);
        if (DOM.identityNewName) DOM.identityNewName.addEventListener("keydown", e => { if(e.key==="Enter") createIdentity(); });
        if (DOM.btnIdentityRestart) {
            DOM.btnIdentityRestart.addEventListener("click", () => {
                showConfirm(
                    "Restart RetiMesh? Any active calls will be dropped.",
                    async () => {
                const btn = DOM.btnIdentityRestart;
                btn.disabled = true;
                btn.innerHTML = '<span class="restart-icon">↻</span> Restarting…';
                try {
                    const res = await fetch("/api/restart", { method: "POST" });
                    const data = await res.json();
                    if (data.status === "restarting") {
                        showToast("Restarting, reconnecting automatically…", "info");
                        // Close the modal; the WebSocket will auto-reconnect
                        // once the new process binds the port.
                        DOM.identityModal.classList.add("hidden");
                        // Give the server ~2.5 s to come back, then force a reload
                        // so the UI state is fully fresh.
                        setTimeout(() => location.reload(), 2500);
                    } else {
                        showToast("Restart failed", "error");
                    }
                } catch (err) {
                    // The POST may return an error because the server is exiting —
                    // that's fine, still wait and reload.
                    showToast("Restarting, reconnecting automatically…", "info");
                    DOM.identityModal.classList.add("hidden");
                    setTimeout(() => location.reload(), 2500);
                }
                    },
                    { title: "Restart RetiMesh", okLabel: "Restart", okClass: "btn btn-danger" }
                );
            });
        }

        // Contact management
        if (DOM.btnContactEdit) DOM.btnContactEdit.addEventListener("click", showContactEdit);
        if (DOM.btnContactCancel) DOM.btnContactCancel.addEventListener("click", hideContactEdit);
        if (DOM.btnContactSave) DOM.btnContactSave.addEventListener("click", saveContact);
        if (DOM.btnPinPeer) DOM.btnPinPeer.addEventListener("click", togglePin);
        if (DOM.btnBlockPeer)      DOM.btnBlockPeer.addEventListener("click", blockPeer);
        if (DOM.btnClearChat)      DOM.btnClearChat.addEventListener("click", clearChat);
        if (DOM.btnRemoveContact)  DOM.btnRemoveContact.addEventListener("click", removeContact);

        // Settings
        if (DOM.btnSettings) DOM.btnSettings.addEventListener("click", openSettings);
        if (DOM.btnSettingsClose) DOM.btnSettingsClose.addEventListener("click", () => DOM.settingsModal.classList.add("hidden"));
        if (DOM.toggleAutoAnnounce) {
            DOM.toggleAutoAnnounce.addEventListener("change", async () => {
                const enabled = DOM.toggleAutoAnnounce.checked;
                await fetch("/api/auto_announce", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ enabled }),
                });
                showToast(enabled ? "Auto-announce enabled" : "Auto-announce disabled", "info");
            });
        }
        const intervalInput = document.getElementById("announce-interval");
        if (intervalInput) {
            intervalInput.addEventListener("change", async () => {
                const interval = parseInt(intervalInput.value) || 30;
                await fetch("/api/auto_announce", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ interval }),
                });
                showToast(`Announce interval set to ${interval}s`, "info");
            });
        }
        const transportToggle = document.getElementById("toggle-transport");
        if (transportToggle) {
            transportToggle.addEventListener("change", () => setTransport(transportToggle.checked));
        }

        // ── Propagation node — act-as-host toggle ─────────────────────────
        const propHostToggle = document.getElementById("toggle-propagation-host");
        if (propHostToggle) {
            propHostToggle.addEventListener("change", async () => {
                const enabled = propHostToggle.checked;
                try {
                    const res = await fetch("/api/propagation/host", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ enabled }),
                    });
                    const data = await res.json();
                    if (data.error) {
                        showToast("Propagation host: " + data.error, "error");
                        // Revert the toggle on failure so the UI stays truthful
                        propHostToggle.checked = !enabled;
                        return;
                    }
                    const infoRow = document.getElementById("propagation-host-info-row");
                    const hashEl  = document.getElementById("propagation-host-hash");
                    if (infoRow) infoRow.style.display = data.enabled ? "" : "none";
                    if (hashEl)  hashEl.textContent    = data.propagation_hash || "—";
                    showToast(data.enabled
                        ? "Acting as propagation node"
                        : "Propagation hosting disabled", "info");
                } catch (e) {
                    showToast("Propagation host request failed", "error");
                    propHostToggle.checked = !enabled;
                }
            });
        }

        // ── Propagation node — outbound save / sync buttons ───────────────
        const btnPropSave = document.getElementById("btn-outbound-propagation-save");
        if (btnPropSave) {
            btnPropSave.addEventListener("click", async () => {
                const input = document.getElementById("outbound-propagation-input");
                const hashVal = (input?.value || "").trim();
                try {
                    const res = await fetch("/api/propagation", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ node_hash: hashVal }),
                    });
                    const data = await res.json();
                    if (data.error) {
                        showToast("Save failed: " + data.error, "error");
                    } else {
                        showToast(hashVal ? "Propagation node saved" : "Propagation node cleared", "info");
                    }
                } catch (e) {
                    showToast("Save failed", "error");
                }
            });
        }
        const btnPropSync = document.getElementById("btn-outbound-propagation-sync");
        if (btnPropSync) {
            btnPropSync.addEventListener("click", async () => {
                try {
                    const res = await fetch("/api/propagation/sync", { method: "POST" });
                    const data = await res.json();
                    if (data.error) {
                        showToast("Sync failed: " + data.error, "error");
                    } else {
                        showToast("Syncing messages from propagation node…", "info");
                    }
                } catch (e) {
                    showToast("Sync failed", "error");
                }
            });
        }
        if (DOM.btnSaveRnsConfig) DOM.btnSaveRnsConfig.addEventListener("click", saveRnsConfig);
        initSettingsTabs();
        initSidebarNav();
        initIfaceTypeSelect();

        // ── Restart RetiMesh button ──────────────────────────────────────────────
        const restartBtn    = document.getElementById("btn-restart-rns");
        const restartStatus = document.getElementById("restart-status");
        if (restartBtn) {
            restartBtn.addEventListener("click", () => {
                showConfirm(
                    "Restart RetiMesh now? The app will reconnect automatically; no messages will be lost.",
                    async () => {
                restartBtn.disabled = true;
                if (restartStatus) {
                    restartStatus.classList.remove("hidden");
                    restartStatus.textContent = "⏳ Restarting… reconnecting in a few seconds.";
                }
                try {
                    await fetch("/api/restart", { method: "POST" });
                } catch (_) {
                    // Expected — server closes the connection immediately on restart
                }
                // SSE will reconnect automatically once the process
                // is back up. Re-enable the button after enough time for the restart.
                setTimeout(() => {
                    restartBtn.disabled = false;
                    if (restartStatus) restartStatus.classList.add("hidden");
                }, 9000);
                    },
                    { title: "Restart RetiMesh", okLabel: "Restart Now", okClass: "btn btn-danger" }
                );
            });
        }

        // ── Pages ────────────────────────────────────────────────────────────────
        initPagesTabs();
        if (DOM.btnPageGo)     DOM.btnPageGo.addEventListener("click", browsePage);
        if (DOM.pageAddressInput) {
            DOM.pageAddressInput.addEventListener("keydown", e => { if(e.key==="Enter") browsePage(); });
        }
        // Bookmark button
        const btnBookmark = document.getElementById("btn-bookmark-page");
        if (btnBookmark) btnBookmark.addEventListener("click", toggleBookmark);

        // Save-offline button
        const btnSaveOff = document.getElementById("btn-save-offline");
        if (btnSaveOff) btnSaveOff.addEventListener("click", savePageOffline);

        // Clear history button
        const btnClearHist = document.getElementById("btn-clear-history");
        if (btnClearHist) {
            btnClearHist.addEventListener("click", () => {
                showConfirm(
                    "Clear all browsing history?",
                    async () => {
                        await fetch("/api/history", { method: "DELETE" });
                        _refreshBrowserSidebar();
                        showToast("History cleared", "info");
                    },
                    { title: "Clear History", okLabel: "Clear", okClass: "btn btn-danger" }
                );
            });
        }

        // Browser sidebar collapse toggles
        document.querySelectorAll(".browser-sidebar-toggle").forEach(btn => {
            btn.addEventListener("click", () => {
                const targetId = btn.dataset.target;
                const body = document.getElementById(targetId);
                if (!body) return;
                const collapsed = body.classList.toggle("collapsed");
                btn.classList.toggle("collapsed", collapsed);
            });
        });

        // Initial sidebar load
        _refreshBrowserSidebar();
        loadBookmarks();
        if (DOM.btnPageSave)   DOM.btnPageSave.addEventListener("click", savePage);
        if (DOM.btnPageDelete) DOM.btnPageDelete.addEventListener("click", deletePage);
        if (DOM.btnPageNew)    DOM.btnPageNew.addEventListener("click", _newPageEditor);
        if (DOM.btnNewPage)    DOM.btnNewPage.addEventListener("click", () => { openPageEditor(null); });
        if (DOM.peContentType) DOM.peContentType.addEventListener("change", _updateFormatHint);
        if (DOM.pageSelect) {
            DOM.pageSelect.addEventListener("change", () => {
                const id = parseInt(DOM.pageSelect.value);
                if (id) openPageEditor(id); else _newPageEditor();
            });
        }
        _updateFormatHint();

        // Mobile peer list toggle
        initMobileToggle();
        initTheme();
        initUIScale();
        initWipeData();
        initTour();

        // ── Groups & Channels ─────────────────────────────────────────────
        initGroups();

        // ── Emergency Alerts ──────────────────────────────────────────────
        initAlerts();

        // ── Creative features ─────────────────────────────────────────────
        initMessageSearch();
        initPeerSearch();
        initQuickSwitcher();
        initSounds();
        initKeyboardShortcuts();
        initTooltips();

        // ── Optional dependency check (non-blocking, best-effort) ─────────
        // Small delay so the main UI renders first before the toast appears.
        setTimeout(_checkDepsOnStartup, 3000);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ── Groups & Channels ────────────────────────────────────────────────────
    // ══════════════════════════════════════════════════════════════════════════

    let _activeGroupId  = null;   // currently open group chat
    let _selectedGType  = "private";
    let _selectedMembers = new Set();

    // ── Badge ──────────────────────────────────────────────────────────────────

    function updateGroupNavBadge(total) {
        const badge = document.getElementById("group-nav-badge");
        if (!badge) return;
        if (total > 0) {
            badge.textContent = total > 99 ? "99+" : String(total);
            badge.classList.remove("hidden");
        } else {
            badge.classList.add("hidden");
        }
    }

    // ── Render group list in sidebar ───────────────────────────────────────────

    function renderGroupList(groups, invites) {
        const list = document.getElementById("group-list");
        if (!list) return;

        if ((!groups || groups.length === 0) && (!invites || invites.length === 0)) {
            list.innerHTML = `<div class="settings-empty">No groups yet. Create one or join a channel.</div>`;
            return;
        }

        let html = "";
        for (const g of groups) {
            const isChannel  = g.type === "channel";
            const avatarCls  = isChannel ? " type-channel" : "";
            const icon       = isChannel
                ? `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"><path d="M4 6h16M4 12h16M4 18h7"/></svg>`
                : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>`;
            const prefix     = isChannel ? "#" : "";
            const membersTxt = `${(g.members || []).length} member${(g.members||[]).length !== 1 ? "s" : ""}`;
            const unread     = g.unread || 0;
            const activeCls  = _activeGroupId === g.group_id ? " active" : "";
            const badge      = unread > 0
                ? `<span class="group-item-unread">${unread > 99 ? "99+" : unread}</span>` : "";

            html += `
            <div class="group-item${activeCls}" data-gid="${escapeHtml(g.group_id)}">
                <div class="group-item-avatar${avatarCls}">${icon}</div>
                <div class="group-item-body">
                    <div class="group-item-name">${escapeHtml(prefix + g.name)}</div>
                    <div class="group-item-preview">${escapeHtml(membersTxt)}</div>
                </div>
                ${badge}
            </div>`;
        }
        list.innerHTML = html;

        list.querySelectorAll(".group-item").forEach(el => {
            el.addEventListener("click", () => openGroupChat(el.dataset.gid));
        });

        // Invites section
        const invSec  = document.getElementById("group-invites-section");
        const invList = document.getElementById("group-invites-list");
        if (invSec && invList) {
            if (invites && invites.length > 0) {
                invSec.classList.remove("hidden");
                invList.innerHTML = invites.map(inv => `
                <div class="group-invite-card" data-inv-gid="${escapeHtml(inv.group_id)}">
                    <div class="group-invite-title">${escapeHtml((inv.group_type === "channel" ? "#" : "") + inv.group_name)}</div>
                    <div class="group-invite-from">Invited by ${escapeHtml(inv.from_name || inv.from_hash.slice(0,12)+"…")} · ${(inv.members||[]).length} member${(inv.members||[]).length!==1?"s":""}</div>
                    <div class="group-invite-actions">
                        <button class="btn-sm btn-primary inv-accept-btn" data-gid="${escapeHtml(inv.group_id)}">Accept</button>
                        <button class="btn-sm btn-secondary inv-decline-btn" data-gid="${escapeHtml(inv.group_id)}">Decline</button>
                    </div>
                </div>`).join("");
                invList.querySelectorAll(".inv-accept-btn").forEach(b =>
                    b.addEventListener("click", () => acceptGroupInvite(b.dataset.gid)));
                invList.querySelectorAll(".inv-decline-btn").forEach(b =>
                    b.addEventListener("click", () => declineGroupInvite(b.dataset.gid)));
            } else {
                invSec.classList.add("hidden");
                invList.innerHTML = "";
            }
        }
    }

    async function loadGroups() {
        try {
            const res  = await fetch("/api/groups");
            if (!res.ok) return;
            const data = await res.json();
            renderGroupList(data.groups || [], data.invites || []);
            const totalUnread = (data.groups || []).reduce((s, g) => s + (g.unread || 0), 0);
            const inviteCount = (data.invites || []).length;
            updateGroupNavBadge(totalUnread + inviteCount);
        } catch (e) {
            console.warn("loadGroups failed:", e);
        }
    }

    // ── Group chat ────────────────────────────────────────────────────────────

    function _myHash() {
        // Read own LXMF hash from the identity bar (id="my-hash")
        const bar = document.getElementById("my-hash");
        return bar ? bar.textContent.trim().replace(/\s/g, "") : "";
    }

    function renderGroupMessages(messages) {
        const container = document.getElementById("group-chat-messages");
        if (!container) return;
        if (!messages || messages.length === 0) {
            container.innerHTML = `<div class="chat-empty-hint">No messages yet. Say hello!</div>`;
            return;
        }
        const myHash = _myHash();
        let html = "";
        let lastDate = null;
        for (const m of messages) {
            const d = new Date((m.timestamp || 0) * 1000);
            const dateStr = d.toLocaleDateString([], {month:"short", day:"numeric"});
            if (dateStr !== lastDate) {
                html += `<div class="group-msg-divider">${escapeHtml(dateStr)}</div>`;
                lastDate = dateStr;
            }
            const isSelf = m.sender_hash === myHash || m.is_self;
            const cls    = isSelf ? "self" : "other";
            const name   = escapeHtml(m.sender_name || m.sender_hash.slice(0,12)+"…");
            const time   = d.toLocaleTimeString([], {hour:"2-digit", minute:"2-digit"});
            html += `
            <div class="group-msg ${cls}" data-mid="${m.id || ""}">
                <div class="group-msg-sender">${name}</div>
                <div class="group-msg-bubble">${escapeHtml(m.content)}</div>
                <div class="group-msg-time">${escapeHtml(time)}</div>
            </div>`;
        }
        container.innerHTML = html;
        container.scrollTop = container.scrollHeight;
    }

    function _appendGroupMessage(msg, myHash) {
        const container = document.getElementById("group-chat-messages");
        if (!container) return;
        const empty = container.querySelector(".chat-empty-hint");
        if (empty) empty.remove();

        const isSelf = msg.sender_hash === myHash || msg.is_self;
        const cls    = isSelf ? "self" : "other";
        const name   = escapeHtml(msg.sender_name || msg.sender_hash.slice(0,12)+"…");
        const d      = new Date((msg.timestamp || 0) * 1000);
        const time   = d.toLocaleTimeString([], {hour:"2-digit", minute:"2-digit"});

        const div = document.createElement("div");
        div.className = `group-msg ${cls}`;
        div.dataset.mid = msg.id || "";
        div.innerHTML = `
            <div class="group-msg-sender">${name}</div>
            <div class="group-msg-bubble">${escapeHtml(msg.content)}</div>
            <div class="group-msg-time">${escapeHtml(time)}</div>`;
        container.appendChild(div);
        container.scrollTop = container.scrollHeight;
    }

    async function openGroupChat(groupId) {
        _activeGroupId = groupId;

        // Highlight active item in sidebar
        document.querySelectorAll(".group-item").forEach(el =>
            el.classList.toggle("active", el.dataset.gid === groupId));

        try {
            const [gRes, mRes] = await Promise.all([
                fetch(`/api/groups/${groupId}`),
                fetch(`/api/groups/${groupId}/messages`),
            ]);
            if (!gRes.ok) return;
            const group = await gRes.json();
            const mData = mRes.ok ? await mRes.json() : { messages: [] };

            // Update header
            const nameEl    = document.getElementById("group-chat-name");
            const metaEl    = document.getElementById("group-chat-meta");
            const avEl      = document.getElementById("group-chat-avatar");
            const renameBtn = document.getElementById("btn-group-rename");
            if (nameEl) nameEl.textContent = (group.type === "channel" ? "#" : "") + group.name;
            if (metaEl) metaEl.textContent = `${(group.members||[]).length} member${(group.members||[]).length!==1?"s":""}`;
            if (avEl)   avEl.className = `group-chat-avatar${group.type === "channel" ? " type-channel" : ""}`;
            // Show rename button only for owners
            if (renameBtn) renameBtn.style.display = group.is_owner ? "" : "none";

            // Store group info on panel for leave / info
            const panel = document.getElementById("group-chat-panel");
            if (panel) panel.dataset.groupId = groupId;

            renderGroupMessages(mData.messages || []);

            // Show panel, hide others
            _hideAllMainPanels();
            if (panel) panel.classList.remove("hidden");

            // Mark messages read
            fetch(`/api/groups/${groupId}/read`, { method: "POST" }).catch(()=>{});

            // Clear unread badge for this group in sidebar
            const item = document.querySelector(`.group-item[data-gid="${groupId}"]`);
            if (item) {
                const badge = item.querySelector(".group-item-unread");
                if (badge) badge.remove();
            }
            // Recalculate nav badge
            const remaining = document.querySelectorAll(".group-item-unread");
            let total = 0;
            remaining.forEach(b => total += parseInt(b.textContent)||0);
            const invCount = document.querySelectorAll(".group-invite-card").length;
            updateGroupNavBadge(total + invCount);

        } catch (e) {
            console.warn("openGroupChat failed:", e);
        }
    }

    async function sendGroupMessage() {
        const input = document.getElementById("group-chat-input");
        const panel = document.getElementById("group-chat-panel");
        const groupId = panel ? panel.dataset.groupId : _activeGroupId;
        if (!input || !groupId) return;
        const content = input.value.trim();
        if (!content) return;
        input.value = "";
        input.style.height = "";
        try {
            await fetch(`/api/groups/${groupId}/messages`, {
                method:  "POST",
                headers: { "Content-Type": "application/json" },
                body:    JSON.stringify({ content }),
            });
        } catch (e) {
            showToast("Failed to send message", "error");
        }
    }

    // ── SSE group event handlers ──────────────────────────────────────────────

    function onGroupMessage(msg) {
        const groupId = msg.group_id;
        const isActiveGroup = groupId === _activeGroupId;

        if (isActiveGroup) {
            // ── Active group: append message to open chat ────────────────────────
            _appendGroupMessage(msg, _myHash());
            // Mark read immediately — no need to bump unread counter
            fetch(`/api/groups/${groupId}/read`, { method: "POST" }).catch(()=>{});
        } else {
            // ── Background group: update sidebar item in-place ───────────────────
            const senderName = msg.sender_name || msg.sender_hash.slice(0,12)+"…";
            showToast(`New message in group from ${senderName}`, "info");

            // Update preview text and unread badge without full loadGroups() refetch.
            // Fall back to a full reload if we can't find the sidebar element.
            const itemEl = document.querySelector(`.group-item[data-gid="${CSS.escape(groupId)}"]`);
            if (itemEl) {
                // Update preview snippet
                const preview = itemEl.querySelector(".group-item-preview");
                if (preview) {
                    const snippet = (msg.content || "").slice(0, 60) + ((msg.content || "").length > 60 ? "…" : "");
                    preview.textContent = `${senderName}: ${snippet}`;
                }
                // Bump unread badge (.group-item-unread)
                const badge = itemEl.querySelector(".group-item-unread");
                if (badge) {
                    const cur = parseInt(badge.textContent, 10) || 0;
                    const next = cur + 1;
                    badge.textContent = next > 99 ? "99+" : String(next);
                } else {
                    // Badge span not present yet — create and inject it into the name row
                    const nameRow = itemEl.querySelector(".group-item-name");
                    if (nameRow) {
                        const span = document.createElement("span");
                        span.className = "group-item-unread";
                        span.textContent = "1";
                        nameRow.appendChild(span);
                    } else {
                        // Can't find expected DOM structure — fall back to full reload
                        loadGroups();
                        return;
                    }
                }
                // Move this item to the top of the list (most-recent-first order)
                const list = itemEl.parentElement;
                if (list && list.firstChild !== itemEl) {
                    list.insertBefore(itemEl, list.firstChild);
                }

                // Recalculate nav badge from all in-DOM unread counts
                const allBadges = document.querySelectorAll(".group-item-unread");
                let navTotal = 0;
                allBadges.forEach(b => navTotal += (parseInt(b.textContent, 10) || 0));
                const invCount = document.querySelectorAll(".group-invite-card").length;
                updateGroupNavBadge(navTotal + invCount);
            } else {
                // Group not yet rendered in the sidebar (new group?) — full reload
                loadGroups();
            }
        }
    }

    function onGroupInviteSSE(data) {
        loadGroups();  // Refresh to show invite
        showToast(`Group invite: ${data.group_name} from ${data.from_name || data.from_hash.slice(0,12)+"…"}`, "info");
    }

    function onGroupMemberJoined(data) {
        if (data.group_id === _activeGroupId) {
            const container = document.getElementById("group-chat-messages");
            if (container) {
                const div = document.createElement("div");
                div.className = "group-msg-divider";
                div.textContent = `${data.member_name || data.member_hash.slice(0,12)+"…"} joined the group`;
                container.appendChild(div);
                container.scrollTop = container.scrollHeight;
            }
        }
        loadGroups();
    }

    function onGroupMemberLeft(data) {
        if (data.group_id === _activeGroupId) {
            const container = document.getElementById("group-chat-messages");
            if (container) {
                const div = document.createElement("div");
                div.className = "group-msg-divider";
                div.textContent = `${data.member_hash.slice(0,12)}… left the group`;
                container.appendChild(div);
                container.scrollTop = container.scrollHeight;
            }
        }
        loadGroups();
    }

    // ── Invite actions ────────────────────────────────────────────────────────

    async function acceptGroupInvite(groupId) {
        try {
            const res = await fetch(`/api/groups/invites/${groupId}/accept`, { method: "POST" });
            if (!res.ok) { showToast("Failed to accept invite", "error"); return; }
            showToast("Joined group!", "success");
            loadGroups();
        } catch (_) { showToast("Error accepting invite", "error"); }
    }

    async function declineGroupInvite(groupId) {
        try {
            await fetch(`/api/groups/invites/${groupId}/decline`, { method: "POST" });
            // Remove invite card from DOM
            const card = document.querySelector(`.group-invite-card[data-inv-gid="${groupId}"]`);
            if (card) card.remove();
            loadGroups();
        } catch (_) {}
    }

    // ── Create / Join modals ──────────────────────────────────────────────────

    function _openCreateGroupModal() {
        const modal = document.getElementById("create-group-modal");
        if (!modal) return;
        // Reset
        const nameInput = document.getElementById("create-group-name");
        if (nameInput) nameInput.value = "";
        _selectedGType   = "private";
        _selectedMembers = new Set();
        document.querySelectorAll(".group-type-btn").forEach(b =>
            b.classList.toggle("active", b.dataset.gtype === "private"));
        _buildMemberPicker(document.getElementById("group-member-picker"));
        modal.classList.remove("hidden");
        if (nameInput) nameInput.focus();
    }

    function _closeCreateGroupModal() {
        const m = document.getElementById("create-group-modal");
        if (m) m.classList.add("hidden");
    }

    function _buildMemberPicker(container) {
        if (!container) return;
        // Build from known peers
        const peers = window._cachedPeers || [];
        if (peers.length === 0) {
            container.innerHTML = `<div class="settings-empty" style="font-size:11px;padding:8px 0;">No peers discovered yet.</div>`;
            return;
        }
        container.innerHTML = peers.map(p => `
        <label class="group-member-pick-item">
            <input type="checkbox" class="member-check" value="${escapeHtml(p.dest_hash)}">
            <label>${escapeHtml(p.display_name || p.dest_hash.slice(0,16)+"…")}</label>
        </label>`).join("");
        container.querySelectorAll(".member-check").forEach(cb => {
            cb.addEventListener("change", () => {
                if (cb.checked) _selectedMembers.add(cb.value);
                else _selectedMembers.delete(cb.value);
            });
        });
    }

    async function _confirmCreateGroup() {
        const nameInput = document.getElementById("create-group-name");
        const name = nameInput ? nameInput.value.trim() : "";
        if (!name) { showToast("Please enter a group name", "warning"); nameInput && nameInput.focus(); return; }

        const confirmBtn = document.getElementById("btn-create-group-confirm");
        if (confirmBtn) { confirmBtn.disabled = true; confirmBtn.textContent = "Creating…"; }

        try {
            const res = await fetch("/api/groups", {
                method:  "POST",
                headers: { "Content-Type": "application/json" },
                body:    JSON.stringify({
                    name,
                    type:    _selectedGType,
                    members: Array.from(_selectedMembers),
                }),
            });
            const data = await res.json();
            if (!res.ok || data.error) {
                showToast("Failed to create group: " + (data.error || res.statusText), "error");
            } else {
                _closeCreateGroupModal();
                showToast(`Group "${name}" created!`, "success");
                await loadGroups();
                openGroupChat(data.group_id);
            }
        } catch (e) {
            showToast("Error creating group", "error");
        } finally {
            if (confirmBtn) { confirmBtn.disabled = false; confirmBtn.textContent = "Create Group"; }
        }
    }

    function _openJoinChannelModal() {
        const modal = document.getElementById("join-channel-modal");
        if (!modal) return;
        const nameInput = document.getElementById("join-channel-name");
        if (nameInput) nameInput.value = "";
        modal.classList.remove("hidden");
        if (nameInput) nameInput.focus();
    }

    function _closeJoinChannelModal() {
        const m = document.getElementById("join-channel-modal");
        if (m) m.classList.add("hidden");
    }

    async function _confirmJoinChannel() {
        const nameInput = document.getElementById("join-channel-name");
        const name = nameInput ? nameInput.value.trim() : "";
        if (!name) { showToast("Please enter a channel name", "warning"); nameInput && nameInput.focus(); return; }
        try {
            const res = await fetch("/api/groups/join", {
                method:  "POST",
                headers: { "Content-Type": "application/json" },
                body:    JSON.stringify({ name }),
            });
            const data = await res.json();
            if (!res.ok || data.error) {
                showToast("Failed to join channel: " + (data.error || res.statusText), "error");
            } else {
                _closeJoinChannelModal();
                showToast(`Joined #${name}!`, "success");
                await loadGroups();
                openGroupChat(data.group_id);
            }
        } catch (e) {
            showToast("Error joining channel", "error");
        }
    }

    // ── Group info modal ──────────────────────────────────────────────────────

    async function openGroupInfoModal(groupId) {
        const modal = document.getElementById("group-info-modal");
        if (!modal) return;
        try {
            const res = await fetch(`/api/groups/${groupId}`);
            if (!res.ok) return;
            const group = await res.json();

            const titleEl = document.getElementById("group-info-modal-title");
            if (titleEl) titleEl.textContent = group.name;

            const typeEl = document.getElementById("group-info-type-row");
            if (typeEl) typeEl.innerHTML = `
                <span class="group-type-badge${group.type==="channel"?" channel":""}">${group.type==="channel"?"Channel":"Private Group"}</span>
                <span>${(group.members||[]).length} member${(group.members||[]).length!==1?"s":""}</span>`;

            const myHash = _myHash();
            const membersList = document.getElementById("group-info-members-list");
            if (membersList) {
                membersList.innerHTML = (group.members || []).map(h => {
                    const isSelf = h === myHash;
                    const peer   = (window._cachedPeers||[]).find(p=>p.dest_hash===h);
                    const name   = peer ? peer.display_name : h.slice(0,16)+"…";
                    return `
                    <div class="group-info-member-row">
                        <span class="group-info-member-name${isSelf?" group-info-member-self":""}">${escapeHtml(name)}${isSelf?" (you)":""}</span>
                        <span class="group-info-member-hash">${escapeHtml(h.slice(0,12))}…</span>
                        ${!isSelf && group.is_owner
                            ? `<button class="btn-remove-member" data-hash="${escapeHtml(h)}" data-gid="${escapeHtml(groupId)}" title="Remove member">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:12px;height:12px"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                               </button>`
                            : ""}
                    </div>`;
                }).join("");

                membersList.querySelectorAll(".btn-remove-member").forEach(btn => {
                    btn.addEventListener("click", async () => {
                        await fetch(`/api/groups/${btn.dataset.gid}/members/${btn.dataset.hash}`, { method: "DELETE" });
                        btn.closest(".group-info-member-row").remove();
                        showToast("Member removed", "info");
                        loadGroups();
                    });
                });
            }

            // Add member picker
            const addPicker = document.getElementById("group-info-add-picker");
            _buildMemberPicker(addPicker);

            // Wire add-by-hash button
            const addBtn = document.getElementById("btn-group-info-add");
            if (addBtn) {
                addBtn._gid = groupId;
                addBtn.onclick = async () => {
                    const hashInput = document.getElementById("group-info-add-hash");
                    const h = hashInput ? hashInput.value.trim() : "";
                    if (!h) {
                        // Check picker selections instead
                        const checked = addPicker ? addPicker.querySelectorAll(".member-check:checked") : [];
                        for (const cb of checked) {
                            await fetch(`/api/groups/${groupId}/members`, {
                                method:  "POST",
                                headers: { "Content-Type": "application/json" },
                                body:    JSON.stringify({ hash: cb.value }),
                            });
                        }
                        if (checked.length) { showToast("Member(s) added", "success"); loadGroups(); openGroupInfoModal(groupId); }
                        return;
                    }
                    const res = await fetch(`/api/groups/${groupId}/members`, {
                        method:  "POST",
                        headers: { "Content-Type": "application/json" },
                        body:    JSON.stringify({ hash: h }),
                    });
                    const data = await res.json();
                    if (data.error) { showToast(data.error, "error"); return; }
                    showToast("Member added", "success");
                    if (hashInput) hashInput.value = "";
                    openGroupInfoModal(groupId);
                    loadGroups();
                };
            }

            // Wire leave button
            const leaveBtn = document.getElementById("btn-group-info-leave");
            if (leaveBtn) leaveBtn.onclick = () => _leaveGroup(groupId);

            modal.dataset.groupId = groupId;
            modal.classList.remove("hidden");
        } catch (e) {
            console.warn("openGroupInfoModal failed:", e);
        }
    }

    async function _leaveGroup(groupId) {
        const group = await fetch(`/api/groups/${groupId}`).then(r=>r.json()).catch(()=>null);
        const name  = group ? group.name : "this group";
        showConfirm(
            `Leave "${name}"? You will stop receiving messages and be removed from the group.`,
            async () => {
                await fetch(`/api/groups/${groupId}`, { method: "DELETE" });
                document.getElementById("group-info-modal")?.classList.add("hidden");
                document.getElementById("group-chat-panel")?.classList.add("hidden");
                document.getElementById("welcome-screen")?.classList.remove("hidden");
                _activeGroupId = null;
                loadGroups();
                showToast("Left group", "info");
            },
            { title: "Leave Group", okLabel: "Leave", okClass: "btn btn-danger" }
        );
    }

    async function _promptRenameGroup(groupId) {
        // Fetch current name for the modal default value
        let currentName = "";
        try {
            const g = await fetch(`/api/groups/${groupId}`).then(r => r.json());
            currentName = g.name || "";
        } catch (_) {}

        // Build a custom inline dialog (consistent with the rest of the app's UI)
        const dlg = document.createElement("div");
        dlg.className = "modal-overlay";
        dlg.style.cssText = "z-index:9999;";
        dlg.innerHTML = `
            <div class="modal-card" style="max-width:360px;">
                <div class="modal-header">
                    <h3 style="font-size:15px;font-weight:700;margin:0;">Rename Group</h3>
                </div>
                <div class="modal-body">
                    <label style="display:block;font-size:12px;color:var(--text-secondary);margin-bottom:6px;">New name</label>
                    <input id="_rename_input" class="input" type="text" maxlength="64"
                           value="${escapeHtml(currentName)}"
                           style="width:100%;box-sizing:border-box;margin-bottom:14px;">
                    <div style="display:flex;gap:8px;">
                        <button id="_rename_cancel" class="btn" style="flex:1;justify-content:center;padding:0.5714rem 1rem;background:var(--bg-elev);color:var(--text-primary);border:1px solid var(--border-strong);border-radius:var(--radius-sm);font-weight:500;">Cancel</button>
                        <button id="_rename_confirm" class="btn btn-primary" style="flex:1;justify-content:center;padding:0.5714rem 1rem;">Rename</button>
                    </div>
                </div>
            </div>`;
        document.body.appendChild(dlg);

        const input   = dlg.querySelector("#_rename_input");
        const btnOk   = dlg.querySelector("#_rename_confirm");
        const btnCancel = dlg.querySelector("#_rename_cancel");

        // Select all text on open so user can type without clearing first
        setTimeout(() => { input.focus(); input.select(); }, 30);

        await new Promise(resolve => {
            const doRename = async () => {
                const trimmed = input.value.trim();
                if (!trimmed || trimmed === currentName) { dlg.remove(); resolve(); return; }
                btnOk.disabled = true; btnOk.textContent = "Renaming…";
                try {
                    const res = await fetch(`/api/groups/${groupId}/rename`, {
                        method:  "POST",
                        headers: { "Content-Type": "application/json" },
                        body:    JSON.stringify({ name: trimmed }),
                    });
                    const data = await res.json();
                    if (!res.ok || data.error) {
                        showToast("Rename failed: " + (data.error || res.statusText), "error");
                        btnOk.disabled = false; btnOk.textContent = "Rename";
                        return;
                    }
                    // Update the open chat header immediately
                    const nameEl = document.getElementById("group-chat-name");
                    if (nameEl) nameEl.textContent = trimmed;
                    showToast(`Group renamed to "${trimmed}"`, "success");
                    loadGroups();
                } catch (e) {
                    showToast("Error renaming group", "error");
                    btnOk.disabled = false; btnOk.textContent = "Rename";
                    return;
                }
                dlg.remove(); resolve();
            };

            btnOk.addEventListener("click", doRename);
            btnCancel.addEventListener("click", () => { dlg.remove(); resolve(); });
            input.addEventListener("keydown", e => {
                if (e.key === "Enter") doRename();
                if (e.key === "Escape") { dlg.remove(); resolve(); }
            });
            // Close on backdrop click
            dlg.addEventListener("click", e => { if (e.target === dlg) { dlg.remove(); resolve(); } });
        });
    }

    function onGroupRenamed(data) {
        // Update chat header if this group is open
        if (data.group_id === _activeGroupId) {
            const nameEl = document.getElementById("group-chat-name");
            if (nameEl) nameEl.textContent = data.new_name;
            // System message in chat
            const container = document.getElementById("group-chat-messages");
            if (container) {
                const div = document.createElement("div");
                div.className = "group-msg-divider";
                div.textContent = `Group renamed to "${data.new_name}"`;
                container.appendChild(div);
                container.scrollTop = container.scrollHeight;
            }
        }
        loadGroups();
    }

    // ── Wire everything ───────────────────────────────────────────────────────

    function initGroups() {
        // Sidebar buttons
        const btnCreate = document.getElementById("btn-group-create");
        if (btnCreate) btnCreate.addEventListener("click", _openCreateGroupModal);

        // Channel mode was removed — groups are always private now.
        // _selectedGType stays "private" throughout.

        document.getElementById("btn-create-group-confirm")?.addEventListener("click", _confirmCreateGroup);
        document.getElementById("btn-create-group-cancel")?.addEventListener("click", _closeCreateGroupModal);
        document.getElementById("btn-create-group-close")?.addEventListener("click", _closeCreateGroupModal);
        document.getElementById("create-group-modal")?.addEventListener("click", e => {
            if (e.target === e.currentTarget) _closeCreateGroupModal();
        });
        document.getElementById("create-group-name")?.addEventListener("keydown", e => {
            if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); _confirmCreateGroup(); }
        });

        // Join channel modal
        document.getElementById("btn-join-channel-confirm")?.addEventListener("click", _confirmJoinChannel);
        document.getElementById("btn-join-channel-cancel")?.addEventListener("click", _closeJoinChannelModal);
        document.getElementById("btn-join-channel-close")?.addEventListener("click", _closeJoinChannelModal);
        document.getElementById("join-channel-modal")?.addEventListener("click", e => {
            if (e.target === e.currentTarget) _closeJoinChannelModal();
        });
        document.getElementById("join-channel-name")?.addEventListener("keydown", e => {
            if (e.key === "Enter") { e.preventDefault(); _confirmJoinChannel(); }
        });

        // Group info modal
        document.getElementById("btn-group-info")?.addEventListener("click", () => {
            if (_activeGroupId) openGroupInfoModal(_activeGroupId);
        });
        document.getElementById("btn-group-info-close")?.addEventListener("click", () => {
            document.getElementById("group-info-modal")?.classList.add("hidden");
        });
        document.getElementById("group-info-modal")?.addEventListener("click", e => {
            if (e.target === e.currentTarget) e.currentTarget.classList.add("hidden");
        });

        // Leave button in chat header
        document.getElementById("btn-group-leave")?.addEventListener("click", () => {
            if (_activeGroupId) _leaveGroup(_activeGroupId);
        });

        // Rename button in chat header (only shown for group owners)
        document.getElementById("btn-group-rename")?.addEventListener("click", () => {
            if (_activeGroupId) _promptRenameGroup(_activeGroupId);
        });

        // Send message
        const sendBtn = document.getElementById("btn-group-send");
        if (sendBtn) sendBtn.addEventListener("click", sendGroupMessage);

        const chatInput = document.getElementById("group-chat-input");
        if (chatInput) {
            chatInput.addEventListener("keydown", e => {
                if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault();
                    sendGroupMessage();
                }
            });
            // Auto-resize textarea
            chatInput.addEventListener("input", () => {
                chatInput.style.height = "auto";
                chatInput.style.height = Math.min(chatInput.scrollHeight, 120) + "px";
            });
        }

        // Escape closes any open group modal
        document.addEventListener("keydown", e => {
            if (e.key !== "Escape") return;
            const modals = ["create-group-modal","join-channel-modal","group-info-modal"];
            for (const id of modals) {
                const m = document.getElementById(id);
                if (m && !m.classList.contains("hidden")) { m.classList.add("hidden"); e.stopPropagation(); return; }
            }
        });

        // Initial load
        loadGroups();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ── Emergency Broadcast & Alert System ───────────────────────────────────
    // ══════════════════════════════════════════════════════════════════════════

    const SEV_LABELS = ["Info", "Warning", "Critical", "SOS"];
    const SEV_CLASSES = ["sev-0", "sev-1", "sev-2", "sev-3"];

    let _alertBannerTimer = null;   // auto-dismiss timer for banner

    /** Update the unread badge on the sidebar Alerts nav button */
    function updateAlertBadge(count) {
        const badge = document.getElementById("alert-nav-badge");
        if (!badge) return;
        if (count > 0) {
            badge.textContent = count > 99 ? "99+" : String(count);
            badge.classList.remove("hidden");
        } else {
            badge.classList.add("hidden");
        }
    }

    /** Show the sticky banner for Critical (2) and SOS (3) alerts */
    function showAlertBanner(alert) {
        const banner  = document.getElementById("alert-banner");
        const sevEl   = document.getElementById("alert-banner-sev");
        const textEl  = document.getElementById("alert-banner-text");
        if (!banner || !sevEl || !textEl) return;

        sevEl.textContent  = SEV_LABELS[alert.severity] || "Alert";
        sevEl.className    = "alert-banner-sev " + (SEV_CLASSES[alert.severity] || "");
        textEl.textContent = alert.title + (alert.message ? ": " + alert.message : "");
        banner.dataset.alertId = alert.id || "";
        banner.classList.remove("hidden");

        // Store so "View" button can jump to alerts tab
        banner.dataset.rowId = alert.id || "";

        // Auto-dismiss SOS after 30 s, Critical after 15 s
        clearTimeout(_alertBannerTimer);
        const delay = alert.severity >= 3 ? 30000 : 15000;
        _alertBannerTimer = setTimeout(hideAlertBanner, delay);
    }

    function hideAlertBanner() {
        clearTimeout(_alertBannerTimer);
        const banner = document.getElementById("alert-banner");
        if (banner) banner.classList.add("hidden");
    }

    /** Render one alert card HTML string */
    // Keep the latest fetched alerts here so sidebar clicks can open
    // a detail view in the main panel without refetching.
    let _alertsCache = [];

    function _updateAlertsDashboardStats(alerts) {
        const counts = { 3: 0, 2: 0, 1: 0, 0: 0, unread: 0 };
        for (const a of alerts) {
            if (counts[a.severity] !== undefined) counts[a.severity]++;
            if (!a.is_read && a.direction === "in") counts.unread++;
        }
        const set = (id, n) => { const el = document.getElementById(id); if (el) el.textContent = n; };
        set("alerts-stat-sos",      counts[3]);
        set("alerts-stat-critical", counts[2]);
        set("alerts-stat-warning",  counts[1]);
        set("alerts-stat-info",     counts[0]);
        set("alerts-stat-unread",   counts.unread);
    }

    function _renderAlertDetail(a) {
        const host = document.getElementById("alerts-main-detail");
        if (!host) return;
        const sevLabel = SEV_LABELS[a.severity] || "Info";
        const sevCls   = SEV_CLASSES[a.severity] || "sev-0";
        const ts       = new Date((a.timestamp || 0) * 1000);
        const tsStr    = ts.toLocaleString();
        const when     = timeAgo(a.timestamp);
        const name     = escapeHtml(a.sender_name || "Unknown peer");
        const hash     = escapeHtml(a.sender_hash || "");
        const title    = escapeHtml(a.title || "(no title)");
        const msg      = escapeHtml(a.message || "");
        const dirLabel = a.direction === "out" ? "Sent by you" : "Received";

        const canAck = !a.is_read && a.direction === "in";

        host.innerHTML = `
            <div class="alert-detail-card">
                <div class="alert-detail-sev-row">
                    <span class="alert-sev-badge ${sevCls}">${escapeHtml(sevLabel)}</span>
                    <span class="alert-sev-badge sev-out" style="margin-left:auto;">${dirLabel}</span>
                </div>
                <div class="alert-detail-title">${title}</div>
                ${msg ? `<div class="alert-detail-message">${msg}</div>` : ""}
                <div class="alert-detail-meta">
                    <div class="alert-detail-meta-item">
                        <div class="alert-detail-meta-label">From</div>
                        <div class="alert-detail-meta-value">${name}</div>
                    </div>
                    <div class="alert-detail-meta-item">
                        <div class="alert-detail-meta-label">Sender Hash</div>
                        <div class="alert-detail-meta-value" style="font-family:var(--font-mono,monospace);font-size:11px;">${hash}</div>
                    </div>
                    <div class="alert-detail-meta-item">
                        <div class="alert-detail-meta-label">Time</div>
                        <div class="alert-detail-meta-value">${escapeHtml(tsStr)} · ${escapeHtml(when)}</div>
                    </div>
                    <div class="alert-detail-meta-item">
                        <div class="alert-detail-meta-label">Severity</div>
                        <div class="alert-detail-meta-value">${escapeHtml(sevLabel)}</div>
                    </div>
                </div>
                <div class="alert-detail-actions">
                    ${canAck ? `<button class="btn" id="alert-detail-ack">✓ Acknowledge</button>` : ""}
                    <button class="btn btn-danger" id="alert-detail-del">Delete alert</button>
                </div>
            </div>`;

        if (canAck) {
            document.getElementById("alert-detail-ack")?.addEventListener("click", () => {
                dismissAlert(a.id);
            });
        }
        document.getElementById("alert-detail-del")?.addEventListener("click", () => {
            deleteAlertItem(a.id);
            // Restore empty state
            host.innerHTML = `<div class="alerts-main-empty">
                <div class="alerts-main-empty-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1" style="width:64px;height:64px;opacity:0.3;"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg></div>
                <div class="alerts-main-empty-title">Select an alert to view details</div>
                <div class="alerts-main-empty-sub">Emergency broadcasts from peers appear in the sidebar. Click one to read the full message and metadata here.</div>
            </div>`;
        });
    }

    function renderAlertItem(a) {
        const sevLabel  = SEV_LABELS[a.severity] || "Info";
        const sevCls    = SEV_CLASSES[a.severity] || "sev-0";
        const when      = timeAgo(a.timestamp);
        const name      = escapeHtml(a.sender_name || (a.sender_hash || "").slice(0, 8) + "…");
        const title     = escapeHtml(a.title || "(no title)");
        const msg       = escapeHtml(a.message || "");
        const unreadCls = (!a.is_read && a.direction === "in") ? " unread" : "";
        // severity class on the item itself drives the ::before coloured bar
        const itemCls   = `alert-item${unreadCls} ${sevCls}`;
        const dirBadge  = a.direction === "out"
            ? `<span class="alert-sev-badge sev-out" title="Sent by you">Sent</span>`
            : `<span class="alert-sev-badge ${sevCls}">${escapeHtml(sevLabel)}</span>`;

        return `
        <div class="${itemCls}" data-id="${a.id || ""}" data-sev="${a.severity}">
            <div class="alert-item-top">
                ${dirBadge}
                <span class="alert-item-title">${title}</span>
            </div>
            ${msg ? `<div class="alert-item-message">${msg}</div>` : ""}
            <div class="alert-item-meta">
                <span>${name}</span>
                <span class="alert-item-meta-sep">·</span>
                <span>${escapeHtml(when)}</span>
            </div>
            <div class="alert-item-actions">
                ${(!a.is_read && a.direction === "in")
                    ? `<button class="btn-alert-ack alert-ack-btn" data-id="${a.id || ""}" title="Mark read">✓ Acknowledge</button>`
                    : ""}
                <button class="btn-alert-del alert-del-btn" data-id="${a.id || ""}" title="Delete alert">Delete</button>
            </div>
        </div>`;
    }

    /** Render the full alert list into #alert-sidebar-list */
    function renderAlertList(alerts) {
        const container = document.getElementById("alert-sidebar-list");
        if (!container) return;
        _alertsCache = alerts || [];
        _updateAlertsDashboardStats(_alertsCache);

        if (!alerts || alerts.length === 0) {
            container.innerHTML = `<div class="settings-empty">No alerts yet.</div>`;
            return;
        }
        container.innerHTML = alerts.map(renderAlertItem).join("");

        // Clicking anywhere on an alert card opens the detail view in the
        // main panel.  Action buttons inside stop propagation so they
        // don't also trigger this.
        container.querySelectorAll(".alert-item").forEach(el => {
            el.addEventListener("click", () => {
                const id = parseInt(el.dataset.id);
                const alert = _alertsCache.find(a => a.id === id);
                if (alert) _renderAlertDetail(alert);
            });
        });

        // Wire up ack buttons
        container.querySelectorAll(".alert-ack-btn").forEach(btn => {
            btn.addEventListener("click", (e) => {
                e.stopPropagation();
                dismissAlert(parseInt(btn.dataset.id));
            });
        });
        // Wire up delete buttons
        container.querySelectorAll(".alert-del-btn").forEach(btn => {
            btn.addEventListener("click", (e) => {
                e.stopPropagation();
                deleteAlertItem(parseInt(btn.dataset.id));
            });
        });
    }

    /** Fetch alerts from backend and render */
    async function loadAlerts() {
        try {
            const res  = await fetch("/api/alerts");
            if (!res.ok) return;
            const data = await res.json();
            renderAlertList(data.alerts || []);
            updateAlertBadge(data.unread_count || 0);
        } catch (e) {
            console.warn("loadAlerts failed:", e);
        }
    }

    /** Prepend a single alert card to the list without full re-render */
    function _prependAlert(alert) {
        const container = document.getElementById("alert-sidebar-list");
        if (!container) return;
        // Remove "no alerts" placeholder if present
        const empty = container.querySelector(".settings-empty");
        if (empty) empty.remove();
        const div = document.createElement("div");
        div.innerHTML = renderAlertItem(alert);
        const card = div.firstElementChild;
        container.prepend(card);
        // Wire buttons
        const ackBtn = card.querySelector(".alert-ack-btn");
        if (ackBtn) ackBtn.addEventListener("click", () => dismissAlert(parseInt(ackBtn.dataset.id)));
        const delBtn = card.querySelector(".alert-del-btn");
        if (delBtn) delBtn.addEventListener("click", () => deleteAlertItem(parseInt(delBtn.dataset.id)));
    }

    /** Called when SSE pushes an incoming alert */
    function onAlertReceived(alert) {
        if (!alert) return;
        // Add to sidebar list
        _prependAlert(alert);
        // Bump badge
        const badge = document.getElementById("alert-nav-badge");
        const cur   = badge ? (parseInt(badge.textContent) || 0) : 0;
        updateAlertBadge(cur + 1);
        // Show banner for Critical / SOS
        if (alert.severity >= 2) {
            showAlertBanner(alert);
        }
        // Toast notification
        const sevLabel = SEV_LABELS[alert.severity] || "Alert";
        const name     = alert.sender_name || alert.sender_hash.slice(0, 8) + "…";
        showToast(`${sevLabel} alert from ${name}: ${alert.title}`,
                  alert.severity >= 2 ? "error" : "warning");
    }

    /** Called when SSE confirms we successfully sent an alert */
    function onAlertSent(alert) {
        if (!alert) return;
        _prependAlert(alert);
        showToast("Alert broadcast sent to mesh", "success");
    }

    /** Mark one incoming alert as read */
    async function dismissAlert(id) {
        try {
            await fetch(`/api/alerts/${id}/read`, { method: "POST" });
        } catch (_) {}
        // Update DOM
        const card = document.querySelector(`.alert-item[data-id="${id}"]`);
        if (card) {
            card.classList.remove("unread");
            const ackBtn = card.querySelector(".alert-ack-btn");
            if (ackBtn) ackBtn.remove();
        }
        // Decrement badge
        const badge = document.getElementById("alert-nav-badge");
        if (badge) {
            const newCount = Math.max(0, (parseInt(badge.textContent) || 1) - 1);
            updateAlertBadge(newCount);
        }
    }

    /** Delete an alert from DB and DOM */
    async function deleteAlertItem(id) {
        try {
            const res = await fetch(`/api/alerts/${id}`, { method: "DELETE" });
            if (!res.ok) { showToast("Failed to delete alert", "error"); return; }
        } catch (_) { showToast("Failed to delete alert", "error"); return; }
        const card = document.querySelector(`.alert-item[data-id="${id}"]`);
        if (card) {
            // If it was unread, adjust badge
            if (card.classList.contains("unread")) {
                const badge = document.getElementById("alert-nav-badge");
                if (badge) updateAlertBadge(Math.max(0, (parseInt(badge.textContent) || 1) - 1));
            }
            card.remove();
        }
        // Show empty state if no more cards
        const container = document.getElementById("alert-sidebar-list");
        if (container && !container.querySelector(".alert-item")) {
            container.innerHTML = `<div class="settings-empty">No alerts yet.</div>`;
        }
    }

    // ── Alert Composer ────────────────────────────────────────────────────────

    let _selectedSeverity = 2; // Default: Critical

    function _openAlertComposer() {
        const modal = document.getElementById("alert-composer-modal");
        if (!modal) return;
        // Reset form
        const titleInput = document.getElementById("alert-composer-title");
        const msgInput   = document.getElementById("alert-composer-message");
        const titleCount = document.getElementById("alert-title-count");
        const msgCount   = document.getElementById("alert-msg-count");
        if (titleInput)  { titleInput.value = ""; }
        if (msgInput)    { msgInput.value   = ""; }
        if (titleCount)  titleCount.textContent = "0";
        if (msgCount)    msgCount.textContent   = "0";
        // Reset severity to Critical (default)
        _selectedSeverity = 2;
        document.querySelectorAll(".alert-sev-btn").forEach(btn => {
            btn.classList.toggle("active", parseInt(btn.dataset.sev) === 2);
        });
        modal.classList.remove("hidden");
        if (titleInput) titleInput.focus();
    }

    function _closeAlertComposer() {
        const modal = document.getElementById("alert-composer-modal");
        if (modal) modal.classList.add("hidden");
    }

    async function sendAlert() {
        const titleInput = document.getElementById("alert-composer-title");
        const msgInput   = document.getElementById("alert-composer-message");
        const sendBtn    = document.getElementById("btn-alert-composer-send");
        const title      = (titleInput ? titleInput.value.trim() : "");
        const message    = (msgInput   ? msgInput.value.trim()   : "");

        if (!title) {
            showToast("Please enter an alert title", "warning");
            if (titleInput) titleInput.focus();
            return;
        }

        const savedHTML = sendBtn ? sendBtn.innerHTML : "";
        if (sendBtn) { sendBtn.disabled = true; sendBtn.textContent = "Broadcasting…"; }

        try {
            const res  = await fetch("/api/alerts", {
                method:  "POST",
                headers: { "Content-Type": "application/json" },
                body:    JSON.stringify({ severity: _selectedSeverity, title, message }),
            });
            const data = await res.json();
            if (!res.ok || data.error) {
                showToast("Failed to send alert: " + (data.error || res.statusText), "error");
            } else {
                _closeAlertComposer();
                // Switch to alerts view so user sees the sent item
                const navBtn = document.getElementById("nav-alerts");
                if (navBtn) navBtn.click();
            }
        } catch (e) {
            showToast("Network error sending alert", "error");
        } finally {
            if (sendBtn) { sendBtn.disabled = false; sendBtn.innerHTML = savedHTML; }
        }
    }

    // ── Alert Settings (in Settings modal) ───────────────────────────────────

    async function _loadAlertsSettings() {
        try {
            const res  = await fetch("/api/alerts/settings");
            if (!res.ok) return;
            const data = await res.json();
            const toggle = document.getElementById("toggle-alerts-enabled");
            if (toggle) toggle.checked = data.enabled !== false;
        } catch (_) {}
    }

    async function _setAlertsEnabled(enabled) {
        try {
            await fetch("/api/alerts/settings", {
                method:  "POST",
                headers: { "Content-Type": "application/json" },
                body:    JSON.stringify({ enabled }),
            });
            showToast(enabled ? "Alerts enabled" : "Alerts disabled", "info");
        } catch (_) {
            showToast("Failed to update alert settings", "error");
        }
    }

    // ── Optional Dependencies Management ─────────────────────────────────────

    async function loadDependencies() {
        const listEl    = document.getElementById("deps-list");
        const actionsEl = document.getElementById("deps-actions");
        const statusEl  = document.getElementById("deps-status");
        if (!listEl) return;

        listEl.innerHTML = '<div class="settings-empty">Checking…</div>';
        try {
            const res  = await fetch("/api/deps");
            const deps = await res.json();
            _renderDepsTable(deps, listEl, actionsEl);

            // Wire install button once
            const btnInstall = document.getElementById("btn-install-missing");
            if (btnInstall && !btnInstall._wired) {
                btnInstall._wired = true;
                btnInstall.addEventListener("click", installMissingDeps);
            }
        } catch (e) {
            listEl.innerHTML = '<div class="settings-empty">Failed to check dependencies.</div>';
        }
    }

    function _renderDepsTable(deps, listEl, actionsEl) {
        if (!deps || !deps.length) {
            listEl.innerHTML = '<div class="settings-empty">No optional packages defined.</div>';
            return;
        }
        const missing = deps.filter(d => !d.installed);
        listEl.innerHTML = deps.map(d => `
            <div class="dep-item">
                <div class="dep-status-icon ${d.installed ? 'installed' : 'missing'}">
                    ${d.installed
                        ? `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>`
                        : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`
                    }
                </div>
                <div class="dep-info">
                    <div class="dep-name">${escapeHtml(d.label)}</div>
                    <div class="dep-desc">${escapeHtml(d.desc)}</div>
                </div>
                <div class="dep-badge ${d.installed ? 'installed' : 'missing'}">
                    ${d.installed ? 'Installed' : 'Not installed'}
                </div>
            </div>`).join('');

        if (actionsEl) {
            actionsEl.style.display = missing.length > 0 ? 'flex' : 'none';
        }
    }

    async function installMissingDeps() {
        const listEl    = document.getElementById("deps-list");
        const statusEl  = document.getElementById("deps-status");
        const outputBox = document.getElementById("deps-output-box");
        const outputEl  = document.getElementById("deps-output");
        const btnInstall = document.getElementById("btn-install-missing");
        if (!btnInstall) return;

        // Collect missing package IDs
        let missing = [];
        try {
            const res  = await fetch("/api/deps");
            const deps = await res.json();
            missing = deps.filter(d => !d.installed).map(d => d.id);
        } catch (_) {}

        if (!missing.length) {
            showToast("All packages already installed", "success");
            return;
        }

        btnInstall.disabled = true;
        btnInstall.textContent = "Installing…";
        if (statusEl) statusEl.textContent = "Running pip install…";
        if (outputBox) outputBox.classList.remove("hidden");
        if (outputEl)  outputEl.textContent = "";

        try {
            const res  = await fetch("/api/deps/install", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ packages: missing }),
            });
            const data = await res.json();

            if (outputEl && data.output) outputEl.textContent = data.output;

            if (data.success) {
                if (statusEl) statusEl.textContent = "Installed! Please restart RetiMesh.";
                showToast("Packages installed, restart required", "success");
            } else {
                if (statusEl) statusEl.textContent = "Installation failed: see output below.";
                showToast("Installation failed", "error");
            }

            // Re-render the status table with fresh data
            if (data.status && listEl) {
                const actionsEl = document.getElementById("deps-actions");
                _renderDepsTable(data.status, listEl, actionsEl);
            }
        } catch (e) {
            if (statusEl) statusEl.textContent = "Network error during installation.";
            showToast("pip install failed: " + e.message, "error");
        } finally {
            btnInstall.disabled = false;
            btnInstall.textContent = "Install Missing Packages";
        }
    }

    // Check deps on startup and show a toast if any are missing
    async function _checkDepsOnStartup() {
        try {
            const res  = await fetch("/api/deps");
            const deps = await res.json();
            const missing = deps.filter(d => !d.installed);
            if (missing.length > 0) {
                const names = missing.map(d => d.label).join(", ");
                showToast(
                    `Optional packages not installed: ${names}. Open Settings → Dependencies to install.`,
                    "info"
                );
            }
        } catch (_) {}  // fail silently — deps check is best-effort
    }

    // ── Wire everything together ──────────────────────────────────────────────

    function initAlerts() {
        // ── Sidebar: Mark all read ────────────────────────────────────────────
        const btnReadAll = document.getElementById("btn-alerts-read-all");
        if (btnReadAll) {
            btnReadAll.addEventListener("click", async () => {
                try {
                    await fetch("/api/alerts/read_all", { method: "POST" });
                    updateAlertBadge(0);
                    // Remove unread class and ack buttons from all visible cards
                    document.querySelectorAll(".alert-item.unread").forEach(card => {
                        card.classList.remove("unread");
                        const ackBtn = card.querySelector(".alert-ack-btn");
                        if (ackBtn) ackBtn.remove();
                    });
                } catch (_) {
                    showToast("Failed to mark alerts read", "error");
                }
            });
        }

        // ── Alert banner: View / Close ────────────────────────────────────────
        const bannerView = document.getElementById("alert-banner-view");
        if (bannerView) {
            bannerView.addEventListener("click", () => {
                hideAlertBanner();
                const navBtn = document.getElementById("nav-alerts");
                if (navBtn) navBtn.click();
            });
        }
        const bannerClose = document.getElementById("alert-banner-close");
        if (bannerClose) {
            bannerClose.addEventListener("click", hideAlertBanner);
        }

        // ── Composer open button: only one entry point now ────────────────────
        // The sidebar bell button was removed (redundant with the prominent
        // "+ New Alert" in the main dashboard header).  Keep only the main
        // dashboard wire-up.
        const btnMainNew = document.getElementById("btn-alerts-main-new");
        if (btnMainNew) btnMainNew.addEventListener("click", _openAlertComposer);

        // ── Composer: severity picker ─────────────────────────────────────────
        document.querySelectorAll(".alert-sev-btn").forEach(btn => {
            btn.addEventListener("click", () => {
                _selectedSeverity = parseInt(btn.dataset.sev);
                document.querySelectorAll(".alert-sev-btn").forEach(b => b.classList.remove("active"));
                btn.classList.add("active");
            });
        });

        // ── Composer: character counters ──────────────────────────────────────
        const titleInput = document.getElementById("alert-composer-title");
        const titleCount = document.getElementById("alert-title-count");
        if (titleInput && titleCount) {
            titleInput.addEventListener("input", () => {
                titleCount.textContent = titleInput.value.length;
            });
        }
        const msgInput  = document.getElementById("alert-composer-message");
        const msgCount  = document.getElementById("alert-msg-count");
        if (msgInput && msgCount) {
            msgInput.addEventListener("input", () => {
                msgCount.textContent = msgInput.value.length;
            });
        }

        // ── Composer: send / cancel / close ───────────────────────────────────
        const btnSend   = document.getElementById("btn-alert-composer-send");
        if (btnSend)   btnSend.addEventListener("click", sendAlert);

        const btnCancel = document.getElementById("btn-alert-composer-cancel");
        if (btnCancel) btnCancel.addEventListener("click", _closeAlertComposer);

        const btnClose  = document.getElementById("btn-alert-composer-close");
        if (btnClose)  btnClose.addEventListener("click", _closeAlertComposer);

        // Close on backdrop click
        const modal = document.getElementById("alert-composer-modal");
        if (modal) {
            modal.addEventListener("click", e => {
                if (e.target === modal) _closeAlertComposer();
            });
        }

        // ── Composer: Escape key ──────────────────────────────────────────────
        document.addEventListener("keydown", e => {
            if (e.key === "Escape") {
                const m = document.getElementById("alert-composer-modal");
                if (m && !m.classList.contains("hidden")) {
                    e.stopPropagation();
                    _closeAlertComposer();
                }
            }
        });

        // ── Settings: toggle alerts enabled ──────────────────────────────────
        const alertToggle = document.getElementById("toggle-alerts-enabled");
        if (alertToggle) {
            alertToggle.addEventListener("change", () => _setAlertsEnabled(alertToggle.checked));
        }

        // ── Settings tab opened: refresh settings state ───────────────────────
        // Watch for the alerts-settings panel becoming visible
        const alertsPanel = document.getElementById("settings-alerts-settings");
        if (alertsPanel) {
            const panelObserver = new MutationObserver(() => {
                if (!alertsPanel.classList.contains("hidden")) {
                    _loadAlertsSettings();
                }
            });
            panelObserver.observe(alertsPanel, { attributes: true, attributeFilter: ["class"] });
        }

        // ── Initial load ──────────────────────────────────────────────────────
        loadAlerts();
    }

    // ── Tooltips: body-level floating div, never clipped by overflow:hidden ──
    function initTooltips() {
        // Create the single shared tooltip div at body level
        const tip = document.createElement("div");
        tip.id = "_ui-tip";
        tip.setAttribute("aria-hidden", "true");
        document.body.appendChild(tip);

        let hideTimer = null;

        function showTip(el) {
            const label = el.dataset.tip;
            if (!label) return;
            clearTimeout(hideTimer);
            tip.textContent = label;
            tip.classList.add("visible");

            // Position: above the element by default, flip below if too close to top
            const rect = el.getBoundingClientRect();
            const GAP = 6;
            const tipH = tip.offsetHeight || 24;
            const tipW = tip.offsetWidth  || 10;

            let top, left;
            if (rect.top - tipH - GAP < 4) {
                // Not enough room above — show below
                top = rect.bottom + GAP;
                tip.dataset.dir = "down";
            } else {
                top = rect.top - tipH - GAP;
                tip.dataset.dir = "up";
            }
            // Horizontally centred, clamped to viewport
            left = rect.left + rect.width / 2 - tipW / 2;
            left = Math.max(6, Math.min(left, window.innerWidth - tipW - 6));

            tip.style.top  = top  + "px";
            tip.style.left = left + "px";
        }

        function hideTip() {
            tip.classList.remove("visible");
        }

        // Harvest title → data-tip, wire events, remove native tooltip
        function applyTips(root) {
            root.querySelectorAll(
                "button[title], a[title], [class*='btn'][title]"
            ).forEach(el => {
                if (el.title && !el.dataset.tip) {
                    el.dataset.tip = el.title;
                    el.removeAttribute("title");
                }
            });
            // Attach listeners to any [data-tip] without them yet
            root.querySelectorAll("[data-tip]:not([data-tip-init])").forEach(el => {
                el.dataset.tipInit = "1";
                el.addEventListener("mouseenter", () => showTip(el));
                el.addEventListener("mouseleave", hideTip);
                el.addEventListener("focus",      () => showTip(el));
                el.addEventListener("blur",       hideTip);
                el.addEventListener("click",      hideTip);
            });
        }

        applyTips(document);

        // Watch for dynamically added elements
        const observer = new MutationObserver(muts => {
            muts.forEach(m => m.addedNodes.forEach(n => {
                if (n.nodeType === 1) applyTips(n);
            }));
        });
        observer.observe(document.body, { childList: true, subtree: true });
    }

    // ── Utils ──
    function escapeHtml(s){const d=document.createElement("div");d.textContent=s;return d.innerHTML;}
    function formatTime(ts){return new Date(ts*1000).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"});}
    function formatBytes(b){if(b<1024)return b+" B";if(b<1048576)return(b/1024).toFixed(1)+" KB";return(b/1048576).toFixed(2)+" MB";}

    // ── Inline file / voice-note progress rows ─────────────────────────
    // Keyed by filename so the same row updates as status changes
    // (sending → done → disappears after a moment).
    const _fileProgressEls = new Map();
    function _renderFileProgressRow(msg) {
        const container = DOM.chatMessages || document.getElementById("chat-messages");
        if (!container) return;
        const key = msg.filename || "unknown";

        if (msg.status === "sending") {
            let row = _fileProgressEls.get(key);
            if (!row) {
                row = document.createElement("div");
                row.className = "message out file-progress-row";
                row.innerHTML = `
                    <div class="msg-bubble" style="max-width:85%;">
                        <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" style="width:16px;height:16px;flex-shrink:0;"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
                            <span style="font-size:13px;">Sending <strong>${escapeHtml(key)}</strong>${msg.pct != null ? ' · ' + msg.pct + '%' : '…'}</span>
                        </div>
                        <div style="height:4px;background:rgba(255,255,255,0.1);border-radius:2px;overflow:hidden;">
                            <div class="fp-bar" style="height:100%;width:${msg.pct != null ? msg.pct + '%' : '30%'};background:var(--accent, #58a6ff);${msg.pct == null ? 'animation:fp-pulse 1.4s ease-in-out infinite;' : 'transition:width .2s;'}"></div>
                        </div>
                    </div>`;
                container.appendChild(row);
                _fileProgressEls.set(key, row);
                container.scrollTop = container.scrollHeight;
            } else if (msg.pct != null) {
                const label = row.querySelector("span");
                const bar   = row.querySelector(".fp-bar");
                if (label) label.innerHTML = `Sending <strong>${escapeHtml(key)}</strong> · ${msg.pct}%`;
                if (bar)   { bar.style.width = msg.pct + "%"; bar.style.animation = "none"; bar.style.transition = "width .2s"; }
            }
        } else if (msg.status === "done" || msg.status === "error") {
            const row = _fileProgressEls.get(key);
            if (row) {
                if (msg.status === "error") {
                    row.querySelector(".msg-bubble").innerHTML =
                        `<div style="font-size:13px;color:#ef4444;">✗ Failed to send <strong>${escapeHtml(key)}</strong>: ${escapeHtml(msg.error || "unknown error")}</div>`;
                    setTimeout(() => { row.remove(); _fileProgressEls.delete(key); }, 5000);
                } else {
                    // On success, just remove — the real file message will
                    // already have been appended via the normal chat flow.
                    row.remove();
                    _fileProgressEls.delete(key);
                }
            }
        }
    }
    function formatBitrate(b){if(b<1000)return b+" bps";if(b<1e6)return(b/1000).toFixed(1)+" kbps";return(b/1e6).toFixed(2)+" Mbps";}
    function timeAgo(ts){if(!ts)return"";const d=Date.now()/1000-ts;if(d<60)return"now";if(d<3600)return Math.floor(d/60)+"m";if(d<86400)return Math.floor(d/3600)+"h";return Math.floor(d/86400)+"d";}
    function uint8ArrayToBase64(a){let b="";for(let i=0;i<a.length;i++)b+=String.fromCharCode(a[i]);return btoa(b);}
    function base64ToUint8Array(s){const b=atob(s),a=new Uint8Array(b.length);for(let i=0;i<b.length;i++)a[i]=b.charCodeAt(i);return a;}
    // ── Peer-offline threshold (configurable) ─────────────────────────────────
    // Stored as integer minutes in localStorage; defaults to 5 minutes to
    // match the previous hard-coded behaviour.  Read fresh on every check so
    // changes apply immediately without needing to refresh the page.
    function _peerOfflineThresholdSec() {
        const m = parseInt(localStorage.getItem("retimesh_offline_threshold_min") || "5", 10);
        return (isFinite(m) && m > 0 ? m : 5) * 60;
    }
    function isPeerOnline(ts) { return ts && (Date.now()/1000 - ts) < _peerOfflineThresholdSec(); }

    /**
     * showConfirm — in-app confirmation dialog (replaces browser confirm())
     * @param {string}   message  — body text shown to the user
     * @param {Function} onOk     — called when user clicks the confirm button
     * @param {object}   [opts]   — { title, okLabel, okClass, cancelLabel }
     */
    function showConfirm(message, onOk, opts = {}) {
        const {
            title       = "Confirm",
            okLabel     = "Confirm",
            okClass     = "btn btn-danger",
            cancelLabel = "Cancel",
        } = opts;

        // Build overlay
        const overlay = document.createElement("div");
        overlay.className = "modal-overlay confirm-overlay";
        overlay.style.cssText = "z-index:9999;";

        overlay.innerHTML = `
        <div class="modal-card confirm-card" style="max-width:380px;width:90vw;">
            <div class="modal-header" style="padding-bottom:0;border-bottom:none;">
                <span class="modal-title">${escapeHtml(title)}</span>
            </div>
            <div class="modal-body" style="padding-top:10px;padding-bottom:8px;">
                <p class="confirm-message">${escapeHtml(message)}</p>
            </div>
            <div class="confirm-footer">
                <button class="btn btn-secondary confirm-cancel">${escapeHtml(cancelLabel)}</button>
                <button class="${okClass} confirm-ok">${escapeHtml(okLabel)}</button>
            </div>
        </div>`;

        const close = () => overlay.remove();

        overlay.querySelector(".confirm-cancel").addEventListener("click", close);
        overlay.querySelector(".confirm-ok").addEventListener("click", () => {
            close();
            onOk();
        });
        // Click outside to cancel
        overlay.addEventListener("click", e => { if (e.target === overlay) close(); });
        // Escape key
        const onKey = e => { if (e.key === "Escape") { close(); document.removeEventListener("keydown", onKey); } };
        document.addEventListener("keydown", onKey);

        document.body.appendChild(overlay);
        // Focus the cancel button by default (safer)
        setTimeout(() => overlay.querySelector(".confirm-cancel").focus(), 50);
    }

    function showToast(message, type) {
        const container = document.getElementById("toast-container");
        if (!container) return;
        const icons = {
            success: `<svg class="toast-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>`,
            error:   `<svg class="toast-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>`,
            info:    `<svg class="toast-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`,
            warning: `<svg class="toast-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`,
        };
        const toast = document.createElement("div");
        toast.className = "toast" + (type ? " " + type : "");
        toast.innerHTML = (icons[type] || icons.info) + `<span>${escapeHtml(message)}</span>`;
        container.appendChild(toast);
        setTimeout(() => { if (toast.parentNode) toast.remove(); }, 3500);
    }

    // ── Guided Tour (Spotlight System) ──
    const TOUR_STEPS = [
        {
            target:     "#identity-bar",
            requireTab: "peers",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><rect x="2" y="5" width="20" height="14" rx="2"/><path d="M2 10h20"/></svg>`,
            title:   "Your Identity",
            desc:    "This is your mesh identity: a unique Ed25519/X25519 keypair. Your address is shown here. Use the settings icon to create extra identities.",
        },
        {
            target:     "#btn-announce",
            requireTab: "peers",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><path d="M5 12H3M12 5V3M12 21v-2M4.22 4.22l1.42 1.42M18.36 5.64l-1.42 1.42M20 12h-2M16.95 16.95l1.41 1.41M5.64 18.36l1.41-1.41"/><circle cx="12" cy="12" r="4"/></svg>`,
            title:   "Announce Yourself",
            desc:    "Click Announce to broadcast your presence over every interface. Nearby peers will discover you and appear in the list.",
        },
        {
            target:     "#peer-list",
            requireTab: "peers",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>`,
            title:   "Peer List",
            desc:    "Discovered peers appear here. Click any peer to open a secure, end-to-end encrypted conversation. Use Ctrl+K to quickly jump between peers.",
        },
        {
            target:     "#nav-network",
            requireTab: "peers",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><circle cx="12" cy="5" r="2"/><circle cx="5" cy="19" r="2"/><circle cx="19" cy="19" r="2"/><path d="M12 7v4M7 17.5l4.5-6.5M17 17.5l-4.5-6.5"/></svg>`,
            title:   "Network Tab",
            desc:    "See your live mesh topology: interfaces, known paths, and hop counts. Nodes are colour-coded by interface type.",
        },
        {
            target:     "#net-visualizer",
            requireTab: "network",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><circle cx="12" cy="5" r="2"/><circle cx="5" cy="19" r="2"/><circle cx="19" cy="19" r="2"/><path d="M12 7v4M7 17.5l4.5-6.5M17 17.5l-4.5-6.5"/></svg>`,
            title:   "Live Topology",
            desc:    "This canvas shows your mesh in real time. Hover a node to see details, click to open a chat.",
            fallback: true,
        },
        // ── Pages tab walkthrough ──────────────────────────────────────
        {
            // 1. Pages tab icon — general intro.  Spotlight the sidebar
            // nav button itself so the user sees where the feature lives
            // before we open it.  We start from the peers tab so the nav
            // button is highlighted (not already active).
            target:     "#nav-pages",
            requireTab: "peers",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>`,
            title:   "Pages Tab",
            desc:    "Host and browse NomadNet-style micropages over the mesh, a tiny offline web. Open it to publish your own pages or visit other nodes' pages.",
        },
        {
            // 2. Browse sub-tab.  prePages ensures the Browse panel is
            // active before we measure positions; fallback covers the
            // empty-state case (no bookmarks, no history).
            target:     "#ppanel-browse",
            requireTab: "pages",
            prePages:   "browse",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>`,
            title:   "Browse",
            desc:    "Enter any node hash (or hash/path) to fetch a remote page. Bookmarks, recent history, and pages saved for offline reading live in the left rail.",
            fallback: true,
        },
        {
            // 3. My Pages sub-tab.  prePages switches to the host panel.
            target:     "#ppanel-host",
            requireTab: "pages",
            prePages:   "host",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>`,
            title:   "My Pages",
            desc:    "Author and host your own pages. Write Markdown or micron, save, then share your node hash so peers across the mesh can read them.",
            fallback: true,
        },

        // ── Groups tab walkthrough ─────────────────────────────────────
        {
            // 1. Groups tab icon — general intro.
            target:     "#nav-groups",
            requireTab: "peers",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>`,
            title:   "Groups Tab",
            desc:    "Encrypted multi-peer chats. Create a group, invite peers, and messages fan out across the mesh; every member receives every message.",
        },
        {
            // 2. Sidebar — group list + create button.  Spotlights the
            // whole groups sidebar view so the + button and group list
            // are both visible together.
            target:     "#view-groups",
            requireTab: "groups",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>`,
            title:   "Create a Group",
            desc:    "All your groups and pending invites live here. Tap the + button to spin up a new group, name it, and pick which peers to invite.",
            fallback: true,
        },

        // ── Alerts tab walkthrough ─────────────────────────────────────
        {
            // 1. Alerts tab icon — general intro.
            target:     "#nav-alerts",
            requireTab: "peers",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>`,
            title:   "Alerts Tab",
            desc:    "Send and receive emergency broadcasts to every reachable peer. Built for the moments that matter: outages, location pings, urgent updates.",
        },
        {
            // 2. Sidebar — broadcast list (sent + received).
            target:     "#view-alerts",
            requireTab: "alerts",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>`,
            title:   "Broadcast Inbox",
            desc:    "Sent and received alerts appear in this sidebar, tagged by severity. Click any alert to read its full message and metadata in the main panel.",
            fallback: true,
        },
        {
            // 3. Main window — stat cards dashboard.
            target:     "#alerts-main",
            requireTab: "alerts",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>`,
            title:   "Alert Dashboard",
            desc:    "At-a-glance counts by severity (SOS, Critical, Warning, Info), plus an unread tally. Selecting a sidebar alert opens its full details below the cards.",
            fallback: true,
        },
        {
            // 4. New Alert button — the alert creation flow.  Targets
            // the prominent button in the alerts main header.
            target:     "#btn-alerts-main-new",
            requireTab: "alerts",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>`,
            title:   "Send a Broadcast",
            desc:    "Compose a new emergency broadcast here. Pick a severity, write the message, and it fans out to every peer in range.",
            fallback: true,
        },
        {
            target:      ".compose-bar",
            requireTab:  "peers",
            requireDemo: "chat",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>`,
            title:   "Send Messages",
            desc:    "Type here and press Enter or Send. Messages use LXMF: store-and-forward, works even when the peer is offline. Use the paperclip to attach files.",
        },
        {
            target:      ".chat-actions",
            requireTab:  "peers",
            requireDemo: "chat",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><circle cx="12" cy="12" r="3"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14M4.93 4.93a10 10 0 0 0 0 14.14"/></svg>`,
            title:   "Chat Actions",
            desc:    "Pin or block a contact, search messages, start an encrypted voice call, edit their nickname, or clear the conversation.",
        },
        {
            target:     "#btn-settings",
            requireTab: "peers",
            icon:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:28px;height:28px;color:var(--accent)"><circle cx="12" cy="12" r="3"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14M4.93 4.93a10 10 0 0 0 0 14.14"/></svg>`,
            title:   "Settings",
            desc:    "Configure auto-announce, manage blocked peers, add Reticulum interfaces (TCP, LoRa, Bluetooth), and adjust preferences.",
        },
    ];

    let _tourIdx   = 0;
    let _tourRAF   = null;   // pending rAF for spotlight animation
    let _tourKbFn  = null;   // keyboard listener ref (for cleanup)

    /* Persist tour-completed flag to server config */
    function node_db_set(key, val) {
        fetch("/api/config", {
            method:  "POST",
            headers: { "Content-Type": "application/json" },
            body:    JSON.stringify({ [key]: val }),
        }).catch(() => {});
    }

    /* Position the spotlight cutout over the target element */
    function _positionSpotlight(el) {
        const spot = document.getElementById("tour-spotlight");
        if (!spot) return;
        const PAD = 8;
        if (!el) {
            // Centre-screen pulse when no target
            const cx = window.innerWidth  / 2;
            const cy = window.innerHeight / 2;
            spot.style.cssText = `left:${cx-60}px;top:${cy-30}px;width:120px;height:60px;border-radius:12px;`;
            return;
        }
        const r = el.getBoundingClientRect();
        spot.style.cssText = [
            `left:${r.left   - PAD}px`,
            `top:${r.top    - PAD}px`,
            `width:${r.width  + PAD*2}px`,
            `height:${r.height + PAD*2}px`,
            `border-radius:10px`,
        ].join(";");
    }

    /* Position the card beside / above / below the spotlight area */
    function _positionCard(el) {
        const card = document.getElementById("tour-card");
        if (!card) return;
        const MARGIN   = 20;
        const CARD_W   = 322;
        const CARD_H   = card.offsetHeight || 200;
        const vw       = window.innerWidth;
        const vh       = window.innerHeight;

        let top, left;

        if (!el) {
            // Centred
            top  = (vh - CARD_H) / 2;
            left = (vw - CARD_W) / 2;
        } else {
            const r = el.getBoundingClientRect();
            const PAD = 8;

            const spaceBelow  = vh - r.bottom - PAD;
            const spaceAbove  = r.top - PAD;
            const spaceRight  = vw - r.right  - PAD;
            const spaceLeft   = r.left - PAD;

            if (spaceBelow >= CARD_H + MARGIN) {
                // Below target
                top  = r.bottom + PAD + MARGIN;
                left = Math.min(Math.max(r.left, MARGIN), vw - CARD_W - MARGIN);
            } else if (spaceAbove >= CARD_H + MARGIN) {
                // Above target
                top  = r.top - PAD - MARGIN - CARD_H;
                left = Math.min(Math.max(r.left, MARGIN), vw - CARD_W - MARGIN);
            } else if (spaceRight >= CARD_W + MARGIN) {
                // Right of target
                top  = Math.min(Math.max(r.top, MARGIN), vh - CARD_H - MARGIN);
                left = r.right + PAD + MARGIN;
            } else if (spaceLeft >= CARD_W + MARGIN) {
                // Left of target
                top  = Math.min(Math.max(r.top, MARGIN), vh - CARD_H - MARGIN);
                left = r.left - PAD - MARGIN - CARD_W;
            } else {
                // Fallback: bottom-centre
                top  = vh - CARD_H - MARGIN;
                left = (vw - CARD_W) / 2;
            }
        }

        // Final clamp: never let the card overflow the viewport in either
        // axis.  Without this, a tall card (long description + dots wrapping
        // to two lines) targeting an element near the bottom of the screen
        // could push its action buttons (Skip / Finish) off-screen.
        top  = Math.max(MARGIN, Math.min(top,  vh - CARD_H - MARGIN));
        left = Math.max(MARGIN, Math.min(left, vw - CARD_W - MARGIN));

        card.style.top   = top  + "px";
        card.style.left  = left + "px";
        card.style.width = CARD_W + "px";
    }

    /* Render progress dots */
    function _renderDots(idx) {
        const dotsEl = document.getElementById("tour-dots");
        if (!dotsEl) return;
        dotsEl.innerHTML = TOUR_STEPS.map((_, i) =>
            `<span class="tour-dot${i === idx ? " active" : ""}"></span>`
        ).join("");
    }

    /* ── Tour Demo Chat ──────────────────────────────────────────────────────────
     * When a tour step targets a chat-panel element (compose-bar, chat-actions)
     * there may be no active conversation, so those elements are hidden.
     * We temporarily inject a demo peer + message so the spotlight has something
     * real to land on, then clean it all up when the tour moves past those steps.
     */
    let _tourDemoActive = false;

    function _tourDemoChat() {
        if (_tourDemoActive) return;   // already showing demo
        if (state.activePeer) return;  // real chat is open — nothing to fake

        _tourDemoActive = true;

        // Show chat panel, hide welcome screen
        if (DOM.welcomeScreen) DOM.welcomeScreen.classList.add("hidden");
        if (DOM.chatPanel)     DOM.chatPanel.classList.remove("hidden");

        // Populate header with demo peer name & hash
        if (DOM.chatPeerName) DOM.chatPeerName.textContent = "Alice (demo)";
        if (DOM.chatPeerHash) DOM.chatPeerHash.textContent = "a1b2c3d4…";

        // Inject a couple of demo messages into the messages list
        const list = document.getElementById("messages-list");
        if (list) {
            list.innerHTML = "";   // clear any real messages visible (shouldn't be any)
            list.insertAdjacentHTML("beforeend", `
                <div class="message inbound tour-demo-msg">
                    <span class="msg-text">Hey! Can you hear me?</span>
                    <span class="msg-meta">Alice · just now</span>
                </div>
                <div class="message outbound tour-demo-msg">
                    <span class="msg-text">Yes! Crystal clear. This is a demo message for the guide.</span>
                    <span class="msg-meta">You · just now</span>
                </div>
            `);
        }
    }

    function _tourDemoChatCleanup() {
        if (!_tourDemoActive) return;
        _tourDemoActive = false;

        // Remove demo messages
        document.querySelectorAll(".tour-demo-msg").forEach(el => el.remove());

        // Restore normal welcome / chat state
        if (!state.activePeer) {
            if (DOM.chatPanel)     DOM.chatPanel.classList.add("hidden");
            if (DOM.welcomeScreen) DOM.welcomeScreen.classList.remove("hidden");
            if (DOM.chatPeerName)  DOM.chatPeerName.textContent = "";
            if (DOM.chatPeerHash)  DOM.chatPeerHash.textContent = "";
        }
    }

    /* Navigate the sidebar to the tab needed by a tour step */
    function _tourEnsureTab(step) {
        if (!step.requireTab) return;
        const navBtn = document.querySelector(`[data-view="${step.requireTab}"]`);
        if (navBtn && !navBtn.classList.contains("active")) {
            navBtn.click();   // reuse the real tab-switch logic (refreshes graph etc.)
        }
        // On mobile the sidebar is offscreen by default, so steps that
        // spotlight a sidebar element (#nav-*, #peer-list, #identity-bar,
        // #btn-announce) would otherwise point at nothing.  Open the
        // sidebar in that case so the spotlight has something to land on.
        if (window.matchMedia("(max-width: 640px)").matches) {
            const sidebar = document.getElementById("sidebar");
            const target  = step.target ? document.querySelector(step.target) : null;
            const inSidebar = target && sidebar && sidebar.contains(target);
            const floatBtn  = document.getElementById("mobile-sidebar-toggle");
            if (inSidebar && sidebar && !sidebar.classList.contains("open")) {
                sidebar.classList.add("open");
                if (floatBtn) {
                    floatBtn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>`;
                    floatBtn.setAttribute("aria-label", "Hide peer list");
                }
            } else if (!inSidebar && sidebar && sidebar.classList.contains("open")) {
                // Step targets the main panel — close sidebar so it doesn't
                // cover the spotlight.
                sidebar.classList.remove("open");
                if (floatBtn) {
                    floatBtn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg>`;
                    floatBtn.setAttribute("aria-label", "Show peer list");
                }
            }
        }
    }

    /* Show a specific tour step */
    function _showStep(idx) {
        if (idx >= TOUR_STEPS.length) { _endTour(); return; }
        _tourIdx = idx;

        const step = TOUR_STEPS[idx];

        // Switch to the required tab before measuring element positions
        _tourEnsureTab(step);

        // Pages tab has two sub-tabs (Browse / My Pages).  If a step needs
        // a specific one active, click the corresponding pill so the right
        // panel is visible before we measure element positions.
        if (step.prePages) {
            const ptab = document.querySelector(`.pages-tab[data-ptab="${step.prePages}"]`);
            if (ptab && !ptab.classList.contains("active")) ptab.click();
        }

        // Show or clean up the demo chat panel as needed by this step
        if (step.requireDemo === "chat") {
            _tourDemoChat();
        } else {
            _tourDemoChatCleanup();
        }

        let el = document.querySelector(step.target);

        // Elements are now always visible when demo is active — no skipping needed.
        // If element is still absent/hidden and no fallback, skip to next step.
        if (!el && !step.fallback) { _showStep(idx + 1); return; }
        if (el && el.offsetParent === null && !step.fallback) { _showStep(idx + 1); return; }
        if (el && el.offsetParent === null) el = null; // centred card

        // Populate card content
        const iconEl  = document.getElementById("tour-card-icon");
        const titleEl = document.getElementById("tour-card-title");
        const descEl  = document.getElementById("tour-card-desc");
        const nextBtn = document.getElementById("btn-tour-next");
        if (iconEl)  iconEl.innerHTML   = step.icon  || "";
        if (titleEl) titleEl.textContent = step.title || "";
        if (descEl)  descEl.textContent  = step.desc  || "";
        if (nextBtn) nextBtn.textContent  = idx === TOUR_STEPS.length - 1 ? "Finish" : "Next";

        _renderDots(idx);

        // Wait one frame so the card re-measures its new height before positioning
        if (_tourRAF) cancelAnimationFrame(_tourRAF);
        _tourRAF = requestAnimationFrame(() => {
            _positionSpotlight(el);
            _positionCard(el);

            // Scroll target into view if off-screen
            if (el) {
                el.scrollIntoView({ behavior: "smooth", block: "nearest" });
            }
        });
    }

    /* Public: start the tour from step 0 */
    function startTour() {
        const shell = document.getElementById("tour-shell");
        if (!shell) return;
        shell.classList.remove("hidden");
        _showStep(0);
    }

    /* Internal: close the tour */
    function _endTour() {
        const shell = document.getElementById("tour-shell");
        if (shell) shell.classList.add("hidden");
        if (_tourKbFn) {
            document.removeEventListener("keydown", _tourKbFn);
            _tourKbFn = null;
        }
        _tourDemoChatCleanup();   // restore normal UI state if demo was active
        _tourIdx = 0;             // reset so reopening via help-button starts fresh
        node_db_set("tour_completed", "true");
    }

    /* Wire up tour UI buttons + keyboard shortcuts */
    function initTour() {
        const nextBtn = document.getElementById("btn-tour-next");
        const skipBtn = document.getElementById("btn-tour-skip");
        const helpBtn = document.getElementById("btn-help");   // ? button in sidebar footer

        if (nextBtn) nextBtn.addEventListener("click", () => _showStep(_tourIdx + 1));
        if (skipBtn) skipBtn.addEventListener("click", _endTour);
        if (helpBtn) helpBtn.addEventListener("click", startTour);

        // Click on the dimmed backdrop (anywhere outside the card / spotlight)
        // also closes the tour.  This is a UX safety net so users never feel
        // trapped by the overlay if a button click somehow misfires.
        const tourShell = document.getElementById("tour-shell");
        if (tourShell) {
            tourShell.addEventListener("click", (e) => {
                // Only dismiss when the user clicks the overlay itself,
                // not the card (.tour-card has pointer-events:all and stops here).
                if (e.target === tourShell) _endTour();
            });
        }

        // Re-position on resize
        window.addEventListener("resize", () => {
            if (!document.getElementById("tour-shell")?.classList.contains("hidden")) {
                _showStep(_tourIdx);  // re-run current step to reflow positions
            }
        });

        // Keyboard navigation
        _tourKbFn = (e) => {
            const shell = document.getElementById("tour-shell");
            if (!shell || shell.classList.contains("hidden")) return;
            if (e.key === "ArrowRight" || e.key === "Enter") { e.preventDefault(); _showStep(_tourIdx + 1); }
            if (e.key === "ArrowLeft")                        { e.preventDefault(); if (_tourIdx > 0) _showStep(_tourIdx - 1); }
            if (e.key === "Escape")                           { e.preventDefault(); _endTour(); }
        };
        document.addEventListener("keydown", _tourKbFn);
    }

    // ── Setup Wizard ──
    let _wizardIdentityReady = false;

    function _wizardShowStep(stepId) {
        ["wizard-step-welcome", "wizard-step-identity"].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.classList.toggle("hidden", id !== stepId);
        });
    }

    function _wizardSetError(msg) {
        const el = document.getElementById("wizard-id-error");
        if (!el) return;
        el.textContent = msg;
        el.classList.toggle("hidden", !msg);
    }

    function _wizardMarkReady() {
        _wizardIdentityReady = true;
        const btn = document.getElementById("wizard-finish");
        if (btn) btn.disabled = false;
    }

    function initWizard() {
        const overlay = document.getElementById("wizard-overlay");

        // Step nav: Welcome → Identity
        const toIdentity = document.getElementById("wizard-btn-to-identity");
        if (toIdentity) toIdentity.addEventListener("click", () => _wizardShowStep("wizard-step-identity"));

        // Identity tab switching
        document.querySelectorAll(".wizard-id-tab").forEach(tab => {
            tab.addEventListener("click", () => {
                document.querySelectorAll(".wizard-id-tab").forEach(t => t.classList.remove("active"));
                tab.classList.add("active");
                const which = tab.dataset.tab;
                document.getElementById("wizard-panel-new").classList.toggle("hidden", which !== "new");
                document.getElementById("wizard-panel-import").classList.toggle("hidden", which !== "import");
                _wizardSetError("");
            });
        });

        // Create new identity
        const btnCreate = document.getElementById("wizard-btn-create-id");
        if (btnCreate) {
            btnCreate.addEventListener("click", async () => {
                _wizardSetError("");
                const name = (document.getElementById("wizard-id-name")?.value || "").trim() || "My Node";
                btnCreate.textContent = "Generating…";
                btnCreate.disabled = true;
                try {
                    const res  = await fetch("/api/identities", {
                        method:  "POST",
                        headers: {"Content-Type": "application/json"},
                        body:    JSON.stringify({ name }),
                    });
                    const data = await res.json();
                    if (data.status === "ok") {
                        document.getElementById("wizard-new-hash").textContent = data.identity_hash || "—";
                        document.getElementById("wizard-new-path").textContent = data.file_path    || "—";
                        document.getElementById("wizard-new-result").classList.remove("hidden");
                        _wizardMarkReady();
                    } else {
                        _wizardSetError(data.error || "Failed to create identity.");
                    }
                } catch (e) {
                    _wizardSetError("Network error: is the server running?");
                }
                btnCreate.textContent = "Generate identity";
                btnCreate.disabled = false;
            });
        }

        // Import identity
        const btnImport = document.getElementById("wizard-btn-import-id");
        if (btnImport) {
            btnImport.addEventListener("click", async () => {
                _wizardSetError("");
                const filePath = (document.getElementById("wizard-import-path")?.value || "").trim();
                const name     = (document.getElementById("wizard-import-name")?.value || "").trim() || "Imported Identity";
                if (!filePath) { _wizardSetError("Please enter a file path."); return; }
                btnImport.textContent = "Importing…";
                btnImport.disabled = true;
                try {
                    const res  = await fetch("/api/identity/import", {
                        method:  "POST",
                        headers: {"Content-Type": "application/json"},
                        body:    JSON.stringify({ file_path: filePath, name }),
                    });
                    const data = await res.json();
                    if (data.status === "ok") {
                        document.getElementById("wizard-import-hash").textContent = data.identity_hash || "—";
                        document.getElementById("wizard-import-result").classList.remove("hidden");
                        _wizardMarkReady();
                    } else {
                        _wizardSetError(data.error || "Could not import identity.");
                    }
                } catch (e) {
                    _wizardSetError("Network error: is the server running?");
                }
                btnImport.textContent = "Import identity";
                btnImport.disabled = false;
            });
        }

        // Finish button
        const btnFinish = document.getElementById("wizard-finish");
        if (btnFinish) {
            btnFinish.addEventListener("click", async () => {
                await fetch("/api/setup_complete", {
                    method:  "POST",
                    headers: {"Content-Type": "application/json"},
                    body:    "{}",
                });
                // Reload identity bar
                const info = await fetch("/api/identity").then(r => r.json()).catch(() => ({}));
                if (info.lxmf_address) {
                    DOM.myHash.textContent = info.lxmf_address;
                    DOM.myHash.title       = "Click to copy · " + info.lxmf_address;
                }
                // Send first announce with the chosen name
                const name = (document.getElementById("wizard-id-name")?.value || "").trim() || "RetiMesh User";
                wsSend({ type: "announce", display_name: name });

                if (overlay) overlay.classList.add("hidden");
                setTimeout(startTour, 600);
            });
        }
    }

    async function checkSetupAndInit() {
        bindEvents();
        connectWS();

        try {
            const res  = await fetch("/api/first_run");
            const data = await res.json();

            if (data.first_run) {
                const overlay = document.getElementById("wizard-overlay");
                if (overlay) overlay.classList.remove("hidden");

                // If an identity already exists (e.g. default was auto-created), let user skip
                if (data.has_identity) {
                    _wizardIdentityReady = true;
                    const btn = document.getElementById("wizard-finish");
                    if (btn) btn.disabled = false;
                    // Pre-fill hash so user can see their existing identity
                    const hashEl = document.getElementById("wizard-new-hash");
                    const pathEl = document.getElementById("wizard-new-path");
                    if (hashEl) hashEl.textContent = data.lxmf_address || data.identity_hash || "—";
                    if (pathEl) pathEl.textContent = data.storage_dir  || "—";
                    const resultEl = document.getElementById("wizard-new-result");
                    if (resultEl) resultEl.classList.remove("hidden");
                }

                initWizard();
            }
            // If NOT first run: skip wizard entirely — just load the app normally
        } catch (e) {
            console.error("Setup check failed:", e);
        }
    }

    if(document.readyState==="loading") document.addEventListener("DOMContentLoaded", checkSetupAndInit);
    else checkSetupAndInit();
})();
