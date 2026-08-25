# Hosting essaygrader2

Same architecture as the sibling apps on this Mac (lessonplanner, flight-plan-inspector):
a single FastAPI+uvicorn process kept alive by launchd, published via Tailscale Funnel.
See `web/app.py` for the server itself.

## Port assignment on this Mac

| App | Local bind | Public Funnel port |
|---|---|---|
| flight-plan-inspector | `127.0.0.1:8000` | `:443` (bare hostname) |
| lessonplanner | `127.0.0.1:8001` | `:8443` |
| **essaygrader2** | `127.0.0.1:8002` | **`:10000`** (Funnel's only remaining supported port) |

## Install

1. Set `EG2_AUTH_PASSWORD` in `.env` (project root) — see `.env.example`. The session cookie's
   signing key is derived from this password (same approach as lessonplanner's `web/auth.py`),
   so there's no separate secret to manage; rotating the password invalidates every session.
2. Substitute the two placeholders in both plist templates and copy them into `~/Library/LaunchAgents/`:
   ```bash
   PROJECT_DIR="/Users/mymacmini/Apps/essaygrader2"
   for f in deploy/com.easyjet.essaygrader2.plist deploy/com.easyjet.essaygrader2-funnel-watchdog.plist; do
     sed -e "s#__PROJECT_DIR__#$PROJECT_DIR#g" -e "s#__USERNAME__#$(whoami)#g" \
       "$f" > ~/Library/LaunchAgents/"$(basename "$f")"
   done
   launchctl load ~/Library/LaunchAgents/com.easyjet.essaygrader2.plist
   launchctl load ~/Library/LaunchAgents/com.easyjet.essaygrader2-funnel-watchdog.plist
   ```
3. Confirm the backend is up locally: `curl http://127.0.0.1:8002/healthz`.
4. Set up the Funnel mapping once (the watchdog only re-asserts it thereafter, it does not create it from nothing):
   ```bash
   tailscale funnel --bg --https=10000 "localhost:8002"
   ```
5. Confirm `tailscale funnel status` shows `:10000 -> localhost:8002` **alongside**, not instead of, the two existing sibling mappings.

## The gotcha

`tailscale funnel reset` is **global** — it wipes every app's Funnel mapping on this node,
not just the caller's. Only flight-plan-inspector's watchdog is allowed to call it, as owner
of the bare hostname (`:443`), and only as a last resort. `deploy/funnel_watchdog.sh` — like
lessonplanner's — must never call it; it only ever re-asserts its own `:10000` mapping. See
the header comment in `funnel_watchdog.sh` for the full incident this avoids repeating
(2026-08-24: an unconditional reset in flight-plan-inspector's watchdog knocked lessonplanner
offline for ~5 minutes).

**Before installing the watchdog plist, re-read `funnel_watchdog.sh` and confirm the string
`tailscale funnel reset` does not appear anywhere as an executed command** — only in the
comment explaining why it must not be.

## Uninstall

```bash
launchctl unload ~/Library/LaunchAgents/com.easyjet.essaygrader2.plist
launchctl unload ~/Library/LaunchAgents/com.easyjet.essaygrader2-funnel-watchdog.plist
rm ~/Library/LaunchAgents/com.easyjet.essaygrader2*.plist
```
