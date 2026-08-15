# essaygrader2

Pipeline for grading instructor-candidate essays with Claude. Reads PDF essays
from a folder, sends each one to Claude for evaluation against a defined
rubric, and produces a structured Excel report with classification,
strengths, weaknesses, and per-question feedback.

This is the **active version** of the project. For the older CSV-based
version with anonymisation, see `../essaygrader/`.

---

## What it does

1. Reads every PDF in `input/essays/`, expecting filenames in the format
   `{candidate_number}_{role}_assignment.pdf`
2. Extracts essay text from each PDF
3. Sends each essay to Claude (Sonnet 5) along with the grading rubric —
   essays already graded in a previous run (and unchanged) are served from a
   local cache instead of being regraded (see *Incremental runs* below)
4. Screens every pair of essays for plagiarism (see below) — candidates in a
   high-risk pair are automatically downgraded to **Maybe**
5. Checks the re-application embargo against the recruitment export (see
   *The recruitment export* below)
6. Writes a four-sheet Excel report to
   `output/ai_essay_grading_report_<campaign>.xlsx` — one file per campaign,
   so running FY26 never overwrites FY27's report
   - **Summary**: one row per candidate — `Campaign`, `Submitted` (the real
     submission date), `Double Application`, `Embargo`, classification
     colour-coded, the cross-cutting scores, `File Format`, `Rubric Version`,
     and a colour-coded `Plagiarism Flag` column (empty when clean)
   - **Detailed**: strengths, weaknesses, rationale, AI risk indicators, and a
     summary column per question (Q1/Q2/Q3), keyed by canonical question number
     so a candidate who answered out of sequence still lines up with the rest
   - **Similarity**: one row per flagged essay pair with similarity scores,
     Claude's verdict and quoted shared evidence
   - **History**: every application by a candidate who appears in more than one
     campaign. The only sheet that crosses campaign boundaries, so a returning
     candidate's earlier grades sit beside the current one

The report includes three empty columns (`Human Override`, `Override Reason`,
`Reviewed`) for the reviewer to fill in. These are wired in early to support
the planned RAG/feedback phase in essaygrader3 — they are intentionally blank
on first generation.

---

## Project structure

```
essaygrader2/
├── input/                         # gitignored: holds personal data
│   ├── essays/                    # drop PDFs here
│   │   ├── 12345_LTC_assignment.pdf
│   │   └── ...
│   └── Recruitment_Export_2026-08-06.csv   # nightly List export
│
├── output/
│   ├── ai_essay_grading_report_FY26.xlsx    # one per campaign
│   ├── ai_essay_grading_report_FY27.xlsx
│   └── grading_cache.json         # incremental cache (auto-managed)
│
├── config/
│   ├── campaign.txt               # the current campaign, e.g. FY26
│   ├── essay_prompt.txt           # the grading rubric sent to Claude
│   └── plagiarism_prompt.txt      # the pair-review prompt for plagiarism verdicts
│
├── src/
│   ├── campaign.py                # which financial year a run belongs to
│   ├── pdf_loader.py              # filename parsing + PDF text extraction
│   ├── essay_grader.py            # Claude API calls, retry logic, JSON parsing
│   ├── grading_cache.py           # incremental cache: only grade new/changed essays
│   ├── plagiarism_checker.py      # pairwise similarity screen + Claude verdicts
│   ├── recruitment_list.py        # reads the List export: submission dates
│   ├── embargo.py                 # the six-month re-application rule
│   ├── report_writer.py           # Excel report generation
│   └── main.py                    # five-step orchestrator
│
├── tests/                         # unit tests, no API calls
│   ├── test_campaign.py
│   ├── test_embargo.py
│   ├── test_grading_cache.py
│   └── ...
│
├── .env                           # ANTHROPIC_API_KEY (gitignored)
├── .env.example                   # template for .env
├── requirements.txt
└── README.md                      # this file
```

---

## Prerequisites

- Python 3.13 (any 3.10+ should work, 3.13 is what the project was developed on)
- An Anthropic API key with billing configured
  (https://console.anthropic.com/settings/billing — pay-as-you-go from the
  first request, no free tier)
- A network path that doesn't intercept TLS to `api.anthropic.com` — see the
  Troubleshooting section below

---

## Setup

```powershell
# Clone or pull the project, then from the project root:

# 1. Create and activate a virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create your .env from the template, then edit it to add your API key
copy .env.example .env
notepad .env

# 4. (Optional) verify the API key works in isolation
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print('Key loaded:', bool(os.getenv('ANTHROPIC_API_KEY')))"
```

---

## Running

```powershell
# 1. Drop your candidate PDFs into input/essays/
#    Filenames must match: {candidate_number}_{role}_assignment.pdf
#    Example: 12345_LTC_assignment.pdf
#    Valid roles: LTC, TFO, TRI, TFO TRI

# 2. Drop the nightly recruitment export into input/
#    Example: Recruitment_Export_2026-08-06.csv

# 3. Check config/campaign.txt names the current campaign (e.g. FY26)

# 4. See what a run would cost before spending anything
python src\main.py --dry-run

# 5. Run the pipeline
python src\main.py

# 6. Open output/ai_essay_grading_report_FY26.xlsx
```

A clean run for ~5 essays takes 30 seconds to a couple of minutes,
depending on essay length and network latency.

---

## Command-line flags

| Flag | What it does |
|---|---|
| `--dry-run` | Reports what would be graded, and why, then exits **without calling the API**. Run it before any rubric, model or extractor change — those invalidate cached grades, and this is how you see the bill first. |
| `--report-only` | Rebuilds the Excel report from cached grades without grading anything. Use it to pick up a *reporting* change. Still runs the plagiarism screen, which the Similarity sheet needs, so it costs a couple of calls rather than nothing. |
| `--roles TRI` or `--roles TRI,TFO` | Scopes a rubric-driven **regrade** to particular roles. New and edited submissions are always graded whatever their role — scoping never suppresses new work, or the report would silently gain a hole. |
| `--fy FY26` | Reports on a specific campaign, overriding `config/campaign.txt` for one run. Earlier campaigns stay cached, so a past year's report can be regenerated at any time. |
| `--recruitment-list PATH` | Uses a specific export instead of the newest one found in `input/`. |

---

## Campaigns (financial years)

Recruitment runs in financial years, **1 October to 30 September**, each named
for the year it ends in: 1 Oct 2025 – 30 Sep 2026 is `FY26`. Every campaign
starts from scratch — the report contains only that campaign's candidates, and
plagiarism screening compares only within it.

The campaign lives in **`config/campaign.txt`**, one line, changed once when the
campaign turns over.

**It is declared, not detected**, for two reasons that both bite in practice:
submission files carry no usable date (copying PDFs resets their mtime), and the
calendar is not authoritative either, since FY27 applications arrive during
September while FY26 is still live. A date-derived rule would misfile them
silently. The date is used only for a default and for a warning when the setting
looks stale; it never overrides the file.

Nothing is ever deleted. Earlier campaigns stay in the grading cache and can be
reported on with `--fy FY26` at any time. A candidate who applied in FY26 and
re-applies in FY27 gets a **separate** cache entry, so last year's grade
survives — the History sheet is what shows you both.

### A run will not grade another campaign's essays

`input/essays/` is not campaign-aware — every PDF in it is loaded, whatever
campaign is being run. Without a check, running `--fy FY26` while last year's
folder still held the FY27 submissions would treat all ~150 of them as **new
FY26 candidates**: graded at full price, filed under the wrong campaign, and
appearing thereafter in History as returning applicants.

The export knows better, so each essay is checked against it **before anything is
graded**:

- the candidate has an application in the campaign being run → graded
- the candidate is in the export but only in *other* campaigns → **left out**,
  named individually, with the campaign they actually belong to
- the candidate is absent from the export → graded; absence is not evidence
- there is no export at all → everything is graded, as before

`--dry-run` reports the exclusions too, so the mistake surfaces while it is still
free. Running `--dry-run --fy FY27` against an FY26 folder reports **0 API
calls** rather than 154.

---

## The recruitment export

The second input, alongside the written assignments: a CSV export of the
Instructor Recruitment Master List (a Microsoft List), backed up nightly.

- Put it in **`input/`**, named `Recruitment_Export_<date>.csv`. The newest is
  chosen by **the date in the filename**, not the file's modification time —
  copying a file resets its mtime, so the name is the only reliable signal.
- `input/` is gitignored, which matters: the export contains names and email
  addresses and must not be committed.
- It is opened **read-only**. Every output of this pipeline goes to the Excel
  workbook; the master list is never written to.

Two columns are read from it.

**`Created`** — the submission date. It cannot be recovered from the PDFs:
filesystem mtimes are reset by copying, and a PDF's internal `CreationDate`
records when the document was *authored*, so a candidate reusing an old file
would look like an older applicant than they are.

**`FINANCIALYEAR`** — the campaign the application belongs to, declared as a
year (`2026` means FY26). This is **preferred over deriving the campaign from
the submission date**, because a campaign can open before 1 October: an FY27
application arriving in September is genuinely FY27, and a date-derived rule
would file it as FY26. That is the same reason `config/campaign.txt` is declared
rather than detected — the export simply lets the principle apply per
application as well as per run.

Exports predating the column fall back to the submission date, so older backups
still load. Everything else in the export is carried for reporting or ignored.

Both export shapes are handled: the friendly export (display headers with emoji,
UK `dd/mm/yyyy` dates) and SharePoint's raw OData export (escaped internal names
like `Staff_x0020_Number`, ISO 8601 dates, values wrapped in JSON).

### The re-application embargo

Unsuccessful candidates may not re-apply within **six months**. The pipeline
flags, in the Summary's `Embargo` column, any candidate whose application falls
within six months of one in an **earlier campaign**.

- **Any role counts** — the embargo attaches to the person, not the post.
- **Every prior applicant counts.** The pipeline has no reliable record of who
  was actually recruited and does not guess; a successful candidate is not
  re-applying anyway.
- Only an *earlier campaign* can trigger it. That is what stops the candidates
  who apply for two roles days apart in the same campaign flagging each other —
  that is the `Double Application` column's business.
- **Reported, never enforced.** An embargoed candidate is still graded and still
  appears in the report. A human decides.

The column has exactly four meanings, and a blank is one of them:

| Cell | Meaning |
|---|---|
| `⚠ Re-applied 76d (2.5 months) after FY26 application on …` | a measured breach |
| *(blank)* | **checked, and clear** |
| `? not in recruitment list` | candidate absent from the export |
| `? embargo not checked — recruitment list missing` | no export was found |

A blank never means "not checked" — that distinction is the whole point, since a
blank cell in a workbook that circulates would otherwise read as a clean bill of
health.

Note that in a campaign's **first** year the column is blank for everyone, which
is correct: there is no earlier campaign to measure six months against.

---

## Filename convention

The PDF loader is strict about filenames. Each PDF must be named:

```
{candidate_number}_{role}_assignment.pdf
```

Where:
- `candidate_number` is one or more digits (e.g. `12345`)
- `role` is exactly one of: `LTC`, `TFO`, `TRI`, `TFO TRI` (uppercase). `TFO TRI`
  sits the same written paper as `TRI`; `TFO_TRI` is accepted as an alternative
  spelling and normalised to `TFO TRI`
- The suffix is literally `_assignment.pdf`

**Note on the spelling**: "assesment" is missing an 's'. This was the
spelling in the original spec and the code matches it exactly. If you ever
want to fix it, change `FILENAME_SUFFIX` at the top of
`src/pdf_loader.py` and rename your PDFs to match.

The loader will refuse to process anything that doesn't match — no quiet
guessing. If you need to rename a batch in PowerShell:

```powershell
Get-ChildItem input\essays\*_assignment.pdf | Rename-Item -NewName { $_.Name -replace '_assignment\.pdf$', '_assignment.pdf' }
```

---

## How the code is organised

Each module has one job:

- **`pdf_loader.py`** — scans the essays folder, parses filenames, extracts
  text from each PDF. Refuses scanned/image-based PDFs (raises an error
  rather than sending nonsense to Claude).
- **`essay_grader.py`** — handles everything API-related: building the
  Claude client with custom HTTP settings, calling the API in streaming
  mode with retries, parsing the JSON response.
- **`plagiarism_checker.py`** — pairwise plagiarism screen. Cheap-first:
  every pair is compared locally (5-word shingle overlap + TF-IDF cosine,
  pure Python, no API cost); only pairs crossing a threshold are sent to
  Claude for a verdict (`confirmed` / `suspicious` / `likely_coincidence`)
  with quoted evidence. High-risk matches downgrade `Priority Interview`
  to `Maybe` (never upgrades — `Do Not Interview` stays).
- **`report_writer.py`** — writes the Excel report with three sheets and
  classification/risk-based colour coding.
- **`main.py`** — orchestrates the four steps in order. No business logic
  lives here.

### Why streaming + retries + custom HTTP client

Earlier iterations of this project failed in interesting ways on certain
home networks: WinError 10054 (connection forcibly closed),
mid-stream `httpx.ReadError`, `ReadTimeout`. The current code uses three
defensive measures stacked together:

1. **Streaming mode** — reads the response incrementally instead of
   waiting on a single long TCP read, which avoids the long-pending-read
   failure mode common on Windows networks with TLS-inspecting middleboxes.
2. **Retry loop** — catches transient connection errors (both Anthropic SDK
   wrapped exceptions and raw httpx exceptions) and retries with
   exponential backoff up to 4 attempts. Auth errors, JSON errors, and
   bad-request errors are NOT retried — those won't get better with time
   and would just hide real bugs.
3. **Custom httpx client** — HTTP/2 disabled (more forgiving on hostile
   networks), explicit longer timeouts (180s read timeout, since a
   4000-token grade can legitimately take 60+ seconds).

### JSON extraction

Claude sometimes wraps responses in markdown code fences. Sometimes it adds
preamble text. On borderline assessments it has been observed to produce
*two* JSON objects with markdown commentary between them (initial verdict,
then a self-corrected verdict). `_extract_first_json_object` in
`essay_grader.py` handles all of these by scanning for the first balanced
`{...}` pair, correctly tracking braces inside string literals.

We deliberately take the **first** JSON object. When Claude self-corrects,
the corrected verdict is sometimes the better one — but consistency matters
more than catching occasional improvements, and your manual review process
should catch borderline-looking grades anyway.

---

## Configuration knobs

In `src/essay_grader.py`, top of file:

- `DEFAULT_MODEL` — currently `claude-sonnet-5`. Changing it invalidates
  every cached grade, because the cache is keyed on the model as well as
  the rubric — run `--dry-run` first to see what a change would cost.
- `MAX_TOKENS` — currently 4000. Bump to 6000 or 8000 if you start seeing
  "Unterminated JSON object" errors (means the response was cut off).
- `MAX_RETRIES` — currently 4. Total backoff is 2+4+8+16 = 30 seconds
  before giving up.

In `src/plagiarism_checker.py`, top of file:

- `LEXICAL_SCREEN_THRESHOLD` / `SEMANTIC_SCREEN_THRESHOLD` — currently
  0.05 / 0.50. Pairs crossing either go to Claude for a verdict. Raise them
  if too many innocent pairs are being reviewed (each review is a small API
  call); lower them to cast a wider net.
- `LEXICAL_HIGH_THRESHOLD` — currently 0.20. Verbatim overlap above this is
  treated as High risk even if Claude equivocates.

Run the unit tests (no API calls, <1s) with:

```powershell
python -m unittest discover tests
```

### Incremental runs / grading cache

The pipeline keeps a cache at `output/grading_cache.json` so repeated runs only
grade **new or edited** essays instead of the whole folder — the expensive part
(one Claude call per essay) is skipped for anything unchanged. The plagiarism
screen still runs across the **full** corpus every time (it's local and free),
and the Excel report is always rebuilt from the complete set.

- The cache needs no setting up; `--roles`, `--fy` and `--report-only` only
  narrow what a run does, they are never required.
- Reuse is keyed on the **file bytes**, not the extracted text. If the PDF is
  unchanged the candidate did not resubmit, however our extraction code may have
  changed since. Re-reading files is opted into deliberately, by bumping
  `EXTRACTOR_VERSION` in `src/text_extractors.py`.
- It stores each essay's text as well as its grade, so a candidate whose PDF is
  later removed from `input/essays/` still appears in the report and is still
  compared for plagiarism.
- Entries are keyed on **campaign, candidate and role**, so a candidate
  re-applying in a later campaign gets a new entry rather than overwriting the
  earlier grade.
- **Editing `config/essay_prompt.txt` regrades everything automatically** — the
  cache stores a hash of the rubric and invalidates itself when it changes. The
  model is part of that hash too.
- To force a full regrade for any other reason, delete `output/grading_cache.json`.
  Prefer `--dry-run` first: on a 154-essay corpus that decision is worth
  a couple of hundred pounds.

A run that adds ~20 essays on top of 80 already cached costs roughly a fifth of a
full 100-essay run (only the 20 new essays hit the grading API).

---

## Troubleshooting

### `ANTHROPIC_API_KEY not found`

The `.env` file is missing or doesn't contain the key. Check:
- File is named exactly `.env` (not `.env.txt`)
- File is at the project root, not in `src/`
- Line is `ANTHROPIC_API_KEY=sk-ant-api03-...` with no quotes or spaces

### `401 Invalid authentication credentials`

The key is being rejected. Three causes, in order of likelihood:
1. **No billing on the account.** Check
   https://console.anthropic.com/settings/billing — needs a positive balance
   and a payment method.
2. **Network is corrupting the request.** Some home networks/routers do TLS
   inspection that strips or mangles the `x-api-key` header. If the same key
   works on a phone hotspot but not on home WiFi, this is the cause. Either
   identify and disable the interceptor (router admin panel → disable
   parental controls / web filtering / "smart security"), or run through
   Cloudflare WARP (free, https://1.1.1.1/) which encrypts traffic in a way
   the home network can't inspect.
3. **Key was revoked.** Check
   https://console.anthropic.com/settings/keys — generate a new one if
   needed.

### `WinError 10054` / `httpx.ReadError` / `ReadTimeout`

All variants of "the network killed the connection." The retry loop will
recover from occasional ones automatically. If they happen on every essay,
your network is hostile to long-streaming TLS connections. Same fixes as
the 401 case above: phone hotspot, Cloudflare WARP, or fix the router.

### `Could not parse JSON from Claude's response`

The response either had no JSON in it (Claude refused to grade for some
reason, usually content-policy related) or was truncated. The full raw
response is in the error message — read it. If truncated, bump `MAX_TOKENS`
in `essay_grader.py`. If Claude refused, look at the essay content.

### `PDF '...' yielded only X chars of text`

The PDF is scanned/image-based rather than text-based. The current pipeline
doesn't OCR. Either OCR the PDF separately first, or ask the candidate to
resubmit as a text PDF.

### Pipeline runs but produces wrong-looking grades

Almost certainly a prompt issue, not a code issue. Look at the rationale
column in the report — Claude usually explains its reasoning. If the
reasoning is consistently wrong (e.g. failing to distinguish competency
headings from observable behaviours), the rubric in
`config/essay_prompt.txt` needs tightening.

---

## What's next

Phase 2 of the project is RAG (Retrieval-Augmented Generation): feeding
human override decisions back into a vector database so future grading
prompts include calibration examples from past similar essays. That work
lives in **essaygrader3** when ready. The override columns in the report
exist precisely to support this — fill them in for any grades you disagree
with, and they'll feed the next iteration.

Don't start RAG work until you've used essaygrader2 for a few weeks and
accumulated 15+ override entries. Less than that and retrieval has nothing
useful to retrieve.

---

## Cost notes

Grading runs on Sonnet 5, at roughly 5x what the original Haiku 4.5 costing
assumed (~$0.02/essay). Budget on that basis and check current pricing before
any large batch.

**Run `--dry-run` before any rubric, model or extractor change.** It prints
exactly how many essays would be graded, and why, without spending anything —
which is the difference between a £3 run and a £150 one on a 154-essay corpus.

`--report-only` rebuilds the workbook from cached grades for the cost of the
plagiarism screen alone (a couple of calls), so picking up a *reporting* change
never means paying to regrade.

The plagiarism screen itself is free (local computation); each pair that
crosses a screening threshold adds one small Claude call (~$0.01). In a
clean batch that's usually zero to a handful of pairs, not all n².
