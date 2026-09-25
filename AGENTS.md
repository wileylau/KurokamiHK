# AGENTS.md

> Maintenance: If architectural changes, new tooling, or new constraints land after this file was written, update this file to stay accurate for the next agent.

## Overview & Stack
- Kurokami: a web scraper / live monitoring CLI for the Carousell marketplace (SG support, scrapes `carousell.com.hk`).
- Python package `kurokami/`: the core (URL building, blacklist, parsing, diffing) is importable and returns data; the CLI is a thin layer that owns prompts, CSV writing, and `sys.exit(1)`. `kurokami.py` is a shim that runs the same CLI (`python -m kurokami` works too).
- Pipeline: Selenium (Chrome) loads search pages, BeautifulSoup parses listings, `pandas` writes CSVs.
- Stack: Python 3.6+, asyncio, Selenium + Chrome WebDriver, BeautifulSoup4, pandas, argparse.
- `server.py` (aiohttp, used in-tree) + `daemon.py`: the live-monitor GUI (wayfinder tickets). Rich app layers like the CLI — `server.py` owns sockets/HTTP and validates browser input; `daemon.py` owns watches, scheduling, the politeness scan queue, storage, and the feed; both delegate scrapes to the pure core. Declared but unused in-tree: `discord`, `python-dotenv` (planned for a `bot.py` — currently absent). MIT license.

## Directory Mapping
- `kurokami/__init__.py` — Public API exports: `scrape`, `parse_items`, `new_rows`, `main`, exceptions, and the lower-level `request_page` / `parse_info` / `find_item_divs` / `load_blacklist` / snapshot helpers.
- `kurokami/core.py` — Pure library (keeps the Carousell HTML layout docstring): URL building, blacklist, `parse_info`, `find_item_divs`/`parse_items`, high-level `async scrape()` (returns a DataFrame, never writes output), `new_rows()` diff.
- `kurokami/browser.py` — `request_page()`: drives Chrome, clicks "Show more results" until the item target is met (min ~1.1x for ads/duplicates), returns the soup.
- `kurokami/cli.py` — `async def main(options)` preserving CLI behaviour: argparse/menu, interactive prompts, `utf-8-sig` CSV output, `sys.exit(1)` on no-results. The dict form is a server-side API used by the (future) bot, with args `i`, `n`, `o`, `t`, `s`, `c` (`ph`/`pl` optional).
- `kurokami/exceptions.py` — `KurokamiError` base plus `NoResultsError`, `NoValidItemsError`; core raises these and the CLI converts them to exit 1.
- `kurokami/__main__.py`, `kurokami.py` — entry points for `python -m kurokami` / `python kurokami.py`.
- `daemon.py` — app layer (wayfinder ticket 008): owns the watch catalog, the single politeness scan queue (one scrape in flight, ≥ `min_gap` s apart; adhoc search jobs ride the same queue), the state machine (baseline on add → one ~15m backoff retry → **stalled** → human `retry`), `right_only` diff vs the previous scan (baseline never notifies; reappearing uid = new again), and the storage model (wayfinder ticket 007): per-watch `output/watches/<id>/state.json` (rewriteable runtime state, the only restart-survivor) + `history.csv` (append-only, CLI shape, `utf-8-sig`), server-wide `output/feed.jsonl` (append-only); `queries.json` is a read-once seed applied only when the output catalog is empty (never rewritten afterwards). A runtime `queued` flag per watch guarantees exactly one enqueue per scan.
- `server.py` — aiohttp app (tickets 005 + 008): bound to 127.0.0.1, serves `static/`, validates browser inputs (SSRF note), and drives the daemon. Endpoints: `GET /api/watches`, `POST /api/watches`, `GET/DELETE /api/watches/{id}`, `POST /api/watches/{id}/retry`, `POST /api/watches/{id}/rescan`, `GET /api/watches/{id}/results`, `GET /api/feed`, `GET /api/status`, plus the shell seam `POST /api/search` + `GET /api/search/{job_id}` + `GET /api/jobs` + `GET /api/config`. `KUROKAMI_SERVER_TEST=1` makes scrapes read `utils/soup.pkl` instead of driving Chrome.
- `static/` — the runnable frontend surfaces (tickets 006 + 008): plain HTML/CSS/JS, no build step. `index.html` (roster), `results.html?watch=ID` (per-watch tail), `feed.html`, `settings.html`; `app.js` fetches the API and renders (each page has `data-page`); `styles.css` is the designed world. Empty states and the test-mode label read in the same grammar.
- `queries.example.json` — committed template for the gitignored `queries.json` config.
- `test_server.py` — standalone smoke script (aiohttp test utils, `kurokami.scrape` monkeypatched so no live fetch; no test framework needed; temp data dir; 46 checks).
- `utils/blacklist.txt` — Lowercased keyword blacklist; filtered out of results.
- `utils/soup.pkl` — Pickled BS4 snapshot for offline parse debugging (`-t` / `-s` modes).
- `output/` — Daemon storage (watches/ + feed.jsonl), gitignored. Timestamped CLI CSVs also land here.
- `requirements.txt` — Unpinned dependencies.
- `.gitignore`, `README.md`, `LICENSE` (MIT).

## Build / Test / Lint Commands
- No build system, test framework, lint/typecheck config, `setup.py`, CI, or `pyproject.toml` exists.
- Setup (README): create venv, then `pip install -r requirements.txt`. Requires local Chrome; Selenium must locate chromedriver on PATH.
- Run: `python kurokami.py -i "<item>" -n <count> [-o out.csv] [-ph H] [-pl L] [-t|-s] [-c prev.csv]` (or `python -m kurokami ...`).
- Run the server shell: `python server.py [--host 127.0.0.1] [--port 8080] [--data-dir output]` (aiohttp daemon; use `KUROKAMI_SERVER_TEST=1` to avoid firing a live scrape).
- Smoke-check the server: `python test_server.py` (in-process, patched scrape; 46 checks).
- Debug parsing against a snapshot: `python kurokami.py -t` (reads `utils/soup.pkl`) or `-s` (serializes a live fetch to `utils/soup.pkl`; `-t` and `-s` are mutually exclusive in intent).
- Import check: `python -c "import kurokami"` — the public API lives in `kurokami/__init__.py`.
- Frontend syntax check, if node is available: `node --check static/app.js`.

## Coding Standards
- Keep the importable core pure: library functions (`core.py`/`browser.py`) return data and raise `KurokamiError` subtypes instead of writing files, printing progress, or calling `sys.exit`; the CLI layer (`cli.py`) owns prompts, CSV output, and exit codes. Rich app layers (`daemon.py`, `server.py`) own scheduling/storage/HTTP but never build Carousell URLs from user data. The Carousell HTML layout docstring + code epoch stays in `core.py`.
- CSV output uses encoding `utf-8-sig` (BOM) so Excel opens it correctly.
- Preserve the blacklist guard (`is_blacklisted`) before appending items.
- Wrap fragile DOM-parsing in `try/except` of `(IndexError, ValueError, AttributeError)` to skip ads/malformed items; `sys.exit(1)` on fatal states.
- Confirm f-string compatibility (Python 3.6+); do not introduce newer syntax without bumping the floor.

## Key Constraints
- Scraping is discouraged on Carousell: scrape minimally (README guidance ~ once per 10 minutes). Treat the site as live/volatile — parsers break when markup changes.
- Headless mode is disabled (disabled in code via `--headless` note); opening Chrome GUI is required.
- No user-facing server inputs may reach `main(options)` (SSRF note in code) or the server's scrape path unvalidated; the browser never hands the server a URL. `-c` (compare) validates the CSV filename against `file_reg` and existence, then returns only `right_only` (new) rows.
- The aiohttp daemon runs exactly one scrape at a time globally (politeness queue spaced ≥ ~15s; env `KUROKAMI_SERVER_MIN_GAP` and `KUROKAMI_SERVER_BACKOFF` override for tests); `KUROKAMI_SERVER_TEST=1` swaps its scrape for the offline snapshot path.
- `output/`, `*.csv`, `.env`, `venv/`, `queries.json` are gitignored; `utils/soup.pkl` and `utils/blacklist.txt` are committed.