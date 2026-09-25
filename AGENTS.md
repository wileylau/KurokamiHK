# AGENTS.md

> Maintenance: If architectural changes, new tooling, or new constraints land after this file was written, update this file to stay accurate for the next agent.

## Overview & Stack
- Kurokami: a web scraper / live monitoring CLI for the Carousell marketplace (SG support, scrapes `carousell.com.hk`).
- Single-file Python CLI: `kurokami.py` drives Selenium (Chrome) to load search pages, BeautifulSoup to parse listings, and `pandas` to write CSVs.
- Stack: Python 3.6+, asyncio, Selenium + Chrome WebDriver, BeautifulSoup4, pandas, argparse.
- Declared but unused in-tree: `discord`, `aiohttp`, `python-dotenv` (planned for a `bot.py` — currently absent). MIT license.

## Directory Mapping
- `kurokami.py` — Main entry point. CLI parser + `async def main(options)`; the dict form is a server-side API used by the (future) bot, with args `i`, `n`, `o`, `t`, `s`, `c`.
- `utils/blacklist.txt` — Lowercased keyword blacklist; filtered out of results.
- `utils/soup.pkl` — Pickled BS4 snapshot for offline parse debugging (`-t` / `-s` modes).
- `output/` — Timestamped CSV results, gitignored.
- `requirements.txt` — Unpinned dependencies.
- `.gitignore`, `README.md`, `LICENSE` (MIT).

## Build / Test / Lint Commands
- No build system, test framework, lint/typecheck config, `setup.py`, CI, or `pyproject.toml` exists.
- Setup (README): create venv, then `pip install -r requirements.txt`. Requires local Chrome; Selenium must locate chromedriver on PATH.
- Run: `python kurokami.py -i "<item>" -n <count> [-o out.csv] [-ph H] [-pl L] [-t|-s] [-c prev.csv]`.
- Debug parsing against a snapshot: `python kurokami.py -t` (reads `utils/soup.pkl`) or `-s` (serializes a live fetch to `utils/soup.pkl`; `-t` and `-s` are mutually exclusive in intent).

## Coding Standards
- Keep the project single-file (`kurokami.py`); follow existing structure: module docstring documenting Carousell HTML layout + code epoch, argparse/menu CLI, async `main()`.
- CSV output uses encoding `utf-8-sig` (BOM) so Excel opens it correctly.
- Preserve the blacklist guard (`is_blacklisted`) before appending items.
- Wrap fragile DOM-parsing in `try/except` of `(IndexError, ValueError, AttributeError)` to skip ads/malformed items; `sys.exit(1)` on fatal states.
- Confirm f-string compatibility (Python 3.6+); do not introduce newer syntax without bumping the floor.

## Key Constraints
- Scraping is discouraged on Carousell: scrape minimally (README guidance ~ once per 10 minutes). Treat the site as live/volatile — parsers break when markup changes.
- Headless mode is disabled (disabled in code via `--headless` note); opening Chrome GUI is required.
- No user-facing server inputs may reach `main(options)` unvalidated (SSRF note in code). `-c` (compare) validates the CSV filename against `file_reg` and existence, then returns only `right_only` (new) rows.
- `output/`, `*.csv`, `.env`, `venv/`, `queries.json` are gitignored; `utils/soup.pkl` and `utils/blacklist.txt` are committed.