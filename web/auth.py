"""Session-cookie login for remote hosting (Tailscale Funnel).

Stateless signed cookies: <expiry_unix>.<hmac-sha256>, where the HMAC key is
derived from the shared EG2_AUTH_PASSWORD. Restarts don't log anyone out, and
rotating the password invalidates every outstanding session at once.

Modeled directly on lessonplanner's backend/auth.py (the same pattern already
proven on this Mac) rather than reinvented. Also supports HTTP Basic Auth in
parallel, for scripts/curl.
"""
from __future__ import annotations

import hashlib
import hmac
import time

SESSION_COOKIE = "eg2_session"
SESSION_TTL_SECONDS = 30 * 24 * 3600  # 30 days


def _session_key(password: str) -> bytes:
    return hashlib.sha256(f"eg2-session:{password}".encode()).digest()


def _sign(payload: str, password: str) -> str:
    return hmac.new(
        _session_key(password), payload.encode(), hashlib.sha256
    ).hexdigest()


def mint_session(password: str, now: float | None = None) -> str:
    """A fresh cookie value valid for SESSION_TTL_SECONDS."""
    expiry = str(int((now if now is not None else time.time()) + SESSION_TTL_SECONDS))
    return f"{expiry}.{_sign(expiry, password)}"


def verify_session(cookie: str, password: str, now: float | None = None) -> bool:
    """True only for an untampered, unexpired cookie minted with this password."""
    expiry, _, signature = cookie.partition(".")
    if not expiry or not signature:
        return False
    if not hmac.compare_digest(signature, _sign(expiry, password)):
        return False
    try:
        return (now if now is not None else time.time()) < int(expiry)
    except ValueError:
        return False


# Self-contained login page — no dependency on templates or external CSS/JS.
LOGIN_PAGE_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>essaygrader2 — Sign in</title>
<style>
  body {{
    margin: 0; min-height: 100vh; display: flex; align-items: center;
    justify-content: center; background: #0f172a; color: #e2e8f0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  }}
  .card {{
    width: min(90vw, 22rem); padding: 2rem; border-radius: 1rem;
    background: #1e293b; box-shadow: 0 10px 30px rgba(0,0,0,.4);
    border-top: 10px solid #38bdf8;
  }}
  .badge {{
    display: inline-block; margin: 0 0 1rem; padding: .3rem .7rem;
    border-radius: 9999px; background: #38bdf8; color: #0f172a;
    font-size: .7rem; font-weight: 800; letter-spacing: .08em;
    text-transform: uppercase;
  }}
  h1 {{ font-size: 1.35rem; margin: 0 0 .25rem; line-height: 1.25; }}
  h1 span {{ color: #38bdf8; }}
  p.sub {{ margin: 0 0 1.5rem; font-size: .85rem; color: #94a3b8; }}
  label {{ display: block; font-size: .8rem; font-weight: 600; margin-bottom: .4rem; }}
  input {{
    width: 100%; box-sizing: border-box; padding: .6rem .75rem;
    border-radius: .5rem; border: 1px solid #475569; background: #0f172a;
    color: #e2e8f0; font-size: 1rem;
  }}
  input:focus {{ outline: 2px solid #38bdf8; border-color: transparent; }}
  input.reveal {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
  button {{
    width: 100%; margin-top: 1rem; padding: .65rem; border: 0;
    border-radius: 9999px; background: #38bdf8; color: #0f172a;
    font-size: 1rem; font-weight: 700; cursor: pointer;
  }}
  button:hover {{ background: #0ea5e9; }}
  button.show {{
    width: auto; margin: .5rem 0 0; padding: .3rem .7rem;
    background: transparent; color: #94a3b8; font-size: .8rem;
    font-weight: 600; border: 1px solid #475569; border-radius: .5rem;
  }}
  button.show:hover {{ background: #0f172a; color: #e2e8f0; }}
  .err {{
    margin: 0 0 1rem; padding: .5rem .75rem; border-radius: .5rem;
    background: #4c1d24; border: 1px solid #9f1239; color: #fda4af;
    font-size: .85rem;
  }}
</style>
</head>
<body>
  <main class="card">
    <div class="badge">essaygrader2</div>
    <h1>Instructor <span>Essay Grader</span></h1>
    <p class="sub">Sign in with the access password</p>
    {error}
    <form method="post" action="/login">
      <label for="eg2_password">Password</label>
      <input id="eg2_password" name="password" type="password"
             autocomplete="current-password" autocapitalize="off"
             autocorrect="off" spellcheck="false" autofocus required>
      <button type="button" class="show" id="toggle"
              aria-controls="eg2_password" aria-pressed="false">Show password</button>
      <button type="submit">Sign in</button>
    </form>
    <script>
      (function () {{
        var input = document.getElementById('eg2_password');
        var btn = document.getElementById('toggle');
        btn.addEventListener('click', function () {{
          var shown = input.type === 'text';
          input.type = shown ? 'password' : 'text';
          input.classList.toggle('reveal', !shown);
          btn.textContent = shown ? 'Show password' : 'Hide password';
          btn.setAttribute('aria-pressed', String(!shown));
          input.focus();
        }});
      }})();
    </script>
  </main>
</body>
</html>
"""

LOGIN_ERROR_HTML = '<p class="err">That password is not correct — please try again.</p>'


def render_login_page(bad: bool = False) -> str:
    return LOGIN_PAGE_HTML.format(error=LOGIN_ERROR_HTML if bad else "")
