#!/bin/bash
# Healthcheck + self-heal script for essaygrader2's Tailscale Funnel entry.
#
# THIS SCRIPT MUST NEVER CALL `tailscale funnel reset`. That command is
# GLOBAL — it clears every app's funnel entry on this node, not just this
# one. This machine also runs Flight Plan Inspector (owns the funnel's bare
# hostname, :443) and lessonplanner (:8443), each with its own watchdog. FPI's
# watchdog is the only one permitted to call the global reset, and only as a
# last resort after its own non-destructive re-assert fails — see its
# deploy/README.md. On 2026-08-24, an earlier version of FPI's watchdog reset
# unconditionally and knocked lessonplanner offline until lessonplanner's own
# watchdog re-asserted its mapping ~5 minutes later. To avoid repeating that
# as a three-way "reset war", this script — like lessonplanner's — only ever
# re-asserts its OWN :10000 -> localhost:8002 mapping. That is a no-op if the
# mapping is already healthy, and self-heals within one 5-minute cycle if
# something else (e.g. FPI's last-resort reset) cleared the whole node's
# funnel config out from under it.
#
# Run via launchd (com.easyjet.essaygrader2-funnel-watchdog.plist) every 5
# minutes. Silent when healthy, logs when it has to re-assert.

set -u
HOST="antonios-grammaticas-imacpro.tail0c25c8.ts.net"
FUNNEL_PORT=10000
BACKEND_PORT=8002
TAILSCALE="/usr/local/bin/tailscale"
CURL="/usr/bin/curl"
DIG="/usr/bin/dig"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*"; }

# Resolve the hostname via public DNS to get the current edge IP. This tells us
# what the internet sees (not what Tailscale returns locally). Skip silently if
# DNS lookup fails (offline, network down, etc.).
# `dig +short` prints its FAILURES to stdout, e.g.
#     ;; connection timed out; no servers could be reached
# so a non-empty answer is NOT proof of success — validate the shape, and try a
# second resolver before giving up (the same fix lessonplanner's and FPI's
# watchdogs both shipped after a 2026-08-24 false-positive incident).
resolve_ingress() {
    local server attempt ip
    for server in 8.8.8.8 1.1.1.1; do
        for attempt in 1 2; do
            ip=$("$DIG" +short +time=5 +tries=1 "@$server" "$HOST" 2>/dev/null \
                 | grep -Eo '^[0-9]{1,3}(\.[0-9]{1,3}){3}$' | head -1)
            if [ -n "$ip" ]; then
                printf '%s\n' "$ip"
                return 0
            fi
        done
    done
    return 1
}

if ! INGRESS_IP=$(resolve_ingress); then
    log "SKIP: public DNS lookup for $HOST failed on all resolvers (offline?)"
    exit 0
fi

# Test the public URL via the resolved IP. This is what end users actually
# experience. Any HTTP status (including 401) is healthy — only 000 (TLS/connect
# error) means the edge is stale or the :10000 entry is missing entirely.
external_check() {
    "$CURL" -s -o /dev/null -w "%{http_code}" \
        --resolve "$HOST:$FUNNEL_PORT:$INGRESS_IP" --max-time 20 "https://$HOST:$FUNNEL_PORT/"
}

code=$(external_check)
if [ "$code" != "000" ]; then exit 0; fi   # healthy — stay silent

# Unhealthy: re-assert our own mapping only. No global reset (see header comment).
log "STALE/MISSING: external check via $INGRESS_IP returned 000 — re-asserting :$FUNNEL_PORT"
"$TAILSCALE" funnel --bg --https="$FUNNEL_PORT" "localhost:$BACKEND_PORT" >/dev/null 2>&1
sleep 45  # edge propagation time

code=$(external_check)
if [ "$code" != "000" ]; then
    log "HEALED: external check now returns HTTP $code"
else
    log "STILL DOWN after re-assert (HTTP 000) — manual attention needed"
fi
