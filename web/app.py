"""essaygrader2 web app — a thin FastAPI wrapper around the CLI pipeline.

Every route delegates to src/main.py: `preview()` in-process for the fast,
read-only classification, and a `python src/main.py --grade-scope ...`
subprocess (see jobs.py) for an actual run. Nothing here reimplements
grading, caching, or report-writing logic. See deploy/README.md for how this
is hosted (launchd + Tailscale Funnel).

Run in production via deploy/run_server.sh. For local dev:
    venv/bin/uvicorn web.app:app --port 8002 --reload
"""
from __future__ import annotations

import base64
import secrets
import sys
from pathlib import Path

WEB_DIR = Path(__file__).resolve().parent
SRC_DIR = WEB_DIR.parent / "src"
# Both main.py and this package's own siblings (auth, jobs, uploads, paths)
# use bare imports (main.py's own convention — see `from campaign import ...`
# at its top), which only resolve once their directory is on sys.path.
sys.path.insert(0, str(WEB_DIR))
sys.path.insert(0, str(SRC_DIR))

import os

from dotenv import load_dotenv

load_dotenv(WEB_DIR.parent / ".env")

import auth
import jobs
import uploads
from paths import OUTPUT_DIR
import main as pipeline  # the CLI pipeline module; only ever imported, never run as __main__

from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel

EG2_AUTH_PASSWORD = os.getenv("EG2_AUTH_PASSWORD", "")

app = FastAPI(title="essaygrader2")


def _check_basic_auth(header: str) -> bool:
    """HTTP Basic fallback for scripts/curl (username is ignored)."""
    if not header.startswith("Basic "):
        return False
    try:
        _, _, password = base64.b64decode(header[6:]).decode().partition(":")
        return secrets.compare_digest(password, EG2_AUTH_PASSWORD)
    except (ValueError, UnicodeDecodeError):
        return False


@app.middleware("http")
async def auth_gate(request: Request, call_next):
    """Cookie or HTTP Basic required everywhere except /healthz and /login.

    Disabled entirely (as a warning, not a crash) when EG2_AUTH_PASSWORD is
    unset — matches lessonplanner's own convention, so a local dev run still
    works without secrets configured.
    """
    if (
        EG2_AUTH_PASSWORD
        and request.method != "OPTIONS"
        and request.url.path not in ("/healthz", "/login")
    ):
        authorised = False
        cookie = request.cookies.get(auth.SESSION_COOKIE, "")
        if cookie and auth.verify_session(cookie, EG2_AUTH_PASSWORD):
            authorised = True
        if not authorised and _check_basic_auth(request.headers.get("Authorization", "")):
            authorised = True
        if not authorised:
            if request.url.path.startswith("/api/"):
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Authentication required"},
                    headers={"WWW-Authenticate": 'Basic realm="essaygrader2"'},
                )
            return RedirectResponse("/login", status_code=302)
    return await call_next(request)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Last line of defence: log the real error, return a clean 500 to the client."""
    print(f"Unhandled error on {request.method} {request.url.path}: {exc}", file=sys.stderr)
    return JSONResponse(status_code=500, content={"detail": "Internal server error."})


# --------------------------------------------------------------------------- #
# Auth pages
# --------------------------------------------------------------------------- #
@app.get("/healthz", include_in_schema=False)
def healthz():
    """Unauthenticated liveness probe — what the funnel watchdog checks."""
    return {"status": "ok"}


@app.get("/login", include_in_schema=False)
async def login_page(request: Request):
    if EG2_AUTH_PASSWORD:
        cookie = request.cookies.get(auth.SESSION_COOKIE, "")
        if cookie and auth.verify_session(cookie, EG2_AUTH_PASSWORD):
            return RedirectResponse("/", status_code=302)
    bad = request.query_params.get("bad") == "1"
    return HTMLResponse(auth.render_login_page(bad=bad))


@app.post("/login", include_in_schema=False)
async def login_submit(request: Request):
    if not EG2_AUTH_PASSWORD:
        return RedirectResponse("/", status_code=302)
    form = await request.form()
    password = str(form.get("password") or "").strip()
    if not secrets.compare_digest(password, EG2_AUTH_PASSWORD):
        return RedirectResponse("/login?bad=1", status_code=302)
    response = RedirectResponse("/", status_code=302)
    # Funnel terminates TLS and proxies plain HTTP to us — trust X-Forwarded-Proto.
    forwarded_proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    response.set_cookie(
        auth.SESSION_COOKIE,
        auth.mint_session(EG2_AUTH_PASSWORD),
        max_age=auth.SESSION_TTL_SECONDS,
        httponly=True,
        secure=forwarded_proto == "https",
        samesite="lax",
    )
    return response


@app.post("/logout", include_in_schema=False)
async def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie(auth.SESSION_COOKIE)
    return response


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(WEB_DIR / "static" / "index.html")


# --------------------------------------------------------------------------- #
# API
# --------------------------------------------------------------------------- #
def _campaign_status(fy: str | None = None) -> dict:
    """The banner every screen shows — which FY this run will file candidates under.

    `fy`, if given, is a per-action override (exactly like the CLI's --fy flag)
    — it is never written to config/campaign.txt. The staleness check still
    runs on whichever campaign this resolves to, override or not: that's the
    CLI's own behavior (run_pipeline warns even under an explicit --fy), so
    the web banner must not disagree with it. `default_campaign` is always the
    real, un-overridden config/campaign.txt value, for the dropdown to seed
    itself from once on load.
    """
    default_campaign = pipeline.active_campaign()
    override = (fy or "").strip().upper()
    campaign = override or default_campaign
    stale = pipeline.looks_stale(campaign)
    return {
        "campaign": campaign,
        "default_campaign": default_campaign,
        "looks_stale": stale,
        # Same wording as the CLI's own console warning (main.py run_pipeline),
        # so the web UI never disagrees with what `python src/main.py` says.
        "stale_warning": (
            f"Today falls in {pipeline.fy_for_date()}, but the campaign is set "
            f"to {campaign}. Edit config/campaign.txt when the new campaign "
            f"starts."
        ) if stale else None,
    }


@app.get("/api/status")
def api_status(fy: str | None = None):
    return {**_campaign_status(fy), "job": jobs.JOB.snapshot()}


@app.get("/api/preview")
def api_preview(roles: str | None = None, fy: str | None = None):
    """`roles` is a comma-separated string, same format as the CLI's --roles flag.

    Scopes a rubric-driven regrade to those roles only — new/changed essays are
    always graded regardless (see main.classify()). Omit for the unscoped
    default that matches today's `python src/main.py --dry-run`.

    `fy` is a per-action override of the campaign, same as the CLI's --fy —
    never written to config/campaign.txt.
    """
    try:
        return pipeline.preview(roles=pipeline.parse_roles(roles), fy=fy)
    except (FileNotFoundError, NotADirectoryError, ValueError) as exc:
        return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.post("/api/upload/essays")
async def api_upload_essays(files: list[UploadFile] = File(...)):
    if jobs.JOB.snapshot()["state"] == "running":
        return JSONResponse(
            status_code=409, content={"detail": "A grading run is in progress"}
        )
    try:
        return await uploads.replace_essays(files)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.post("/api/upload/recruitment-csv")
async def api_upload_csv(file: UploadFile = File(...)):
    if jobs.JOB.snapshot()["state"] == "running":
        return JSONResponse(
            status_code=409, content={"detail": "A grading run is in progress"}
        )
    return await uploads.replace_recruitment_csv(file)


class RunRequest(BaseModel):
    scope: str = "all"
    roles: list[str] = []
    fy: str | None = None
    report_only: bool = False


@app.post("/api/run")
async def api_run(payload: RunRequest):
    if not payload.report_only and payload.scope not in ("all", "new"):
        return JSONResponse(
            status_code=400, content={"detail": "scope must be 'all' or 'new'"}
        )
    roles = ",".join(payload.roles) or None
    if not jobs.start_run(payload.scope, roles, payload.fy, payload.report_only):
        return JSONResponse(
            status_code=409, content={"detail": "A grading run is already in progress"}
        )
    return JSONResponse(status_code=202, content={"state": "running"})


@app.get("/api/report")
def api_report(fy: str | None = None):
    campaign = (fy or "").strip().upper() or pipeline.active_campaign()
    report_path = OUTPUT_DIR / f"ai_essay_grading_report_{campaign}.xlsx"
    if not report_path.exists():
        return JSONResponse(
            status_code=404,
            content={"detail": f"No report for {campaign} yet — run a grading pass first."},
        )
    return FileResponse(
        report_path,
        filename=report_path.name,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
