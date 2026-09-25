# Kurokami

Kurokami is a web scraper and live monitoring tool for the marketplace Carousell. Includes options such as price range
> [!NOTE]
> Support only for SG

> [!WARNING]
> Use this ethically as web scraping is heavily discouraged on this platform. Do not scrape more than needed.
> Once per 10 minutes is good enough

# Setup

Require python > 3.6 or you will be removing all the f strings in there

### Create the venv and install requirements in it
It is recommended that a venv is used in order to avoid conflict
```bash
python setup.py
```

### Activate the venv
On Linux:
```bash
source venv/bin/activate
```
to deactivate
```bash
deactivate
```

On Windows:
lazy to write this im doing it later

# Usage

### Kurokami Command line
```
usage: kurokami.py [-h] [-i ITEM] [-n NUMBER] [-o OUTPUT] [-t] [-s] [-c COMPARE] [-ph PRICE_HIGH] [-pl PRICE_LOW]

options:
  -h, --help            show this help message and exit
  -i ITEM, --item ITEM  Name of the item to scrape
  -n NUMBER, --number NUMBER Number of items to scrape
  -o OUTPUT, --output OUTPUT CSV file to write out to (defaults to timestamped)
  -t, --test            For debugging of parsers which could break often due to the changing structure, using a snapshot 
                        of a bs4 object while overriding these flags with the respective values: -i shirakami fubuki -n 10
  -s, --serialize       For debugging of parsers which could break often due to the changing structure, the BS4 object is serialised for fast access, must not have -t
  -c COMPARE, --compare Name of a .csv file output from this program to compare with
  -ph PRICE_HIGH             Upper price limit
  -pl PRICE_LOW              Lower price limit
```
`python -m kurokami ...` works the same as `python kurokami.py ...`.

### Kurokami as a library
`kurokami` is an importable package: the core returns data instead of writing output.
```python
import asyncio
import kurokami

df = asyncio.run(kurokami.scrape("shirakami fubuki", count=25, price_low=100))
# df is a pandas.DataFrame of parsed listings (uid, seller_name, price, time_posted, ...)

# Diff against a previous scrape for new listings:
new = kurokami.new_rows(prev_df, df)  # list of right_only (new) rows
```
`kurokami.main(options)` is the CLI/server entry point: it writes a `utf-8-sig` CSV and returns rows as lists; the dict form takes keys `i`, `n`, `o`, `t`, `s`, `c` (`ph`/`pl` optional) and is safe for server-side use. Core functions raise `NoResultsError` / `NoValidItemsError` (subclasses of `KurokamiError`) on failure rather than exiting.

### Kurokami live-monitor GUI (daemon + web frontend)

A local web GUI watches saved searches on a schedule, diffs each scan against the previous one (`right_only`), and surfaces genuinely new listings in an in-app feed. See `wayfinder/map.md` for the design story.

Start the daemon (bound to 127.0.0.1):
```bash
python server.py            # serves static/ + /api, schedules rescans
python server.py --port 8080 --data-dir output
```
Open `http://127.0.0.1:8080/` in Chrome. Add watches from the `settings` surface; continuous watches rescan on their interval, one-shot watches baseline once. A scraped watch goes through one ~15&nbsp;m backoff retry before it **stalls** and waits for "retry now" — or you can fire a manual `[rescan]` from the results surface immediately.

- Watch data and history live in `output/watches/<id>/` (`state.json` runtime state + `history.csv` appends); the in-app feed is `output/feed.jsonl` (append-only). Both are gitignored.
- `queries.json` (template: `queries.example.json`) seeds the catalog once on first boot.
- Politeness is built-in: one scrape in flight, spaced ≥ 15 s, with a soft ~10&nbsp;min-per-watch floor surfaced as a banner.
- Dry-run without firing Chrome (reads `utils/soup.pkl` for every scrape):
  `KUROKAMI_SERVER_TEST=1 python server.py`
- Smoke-check: `python test_server.py` (46 checks, patched scrape, temp data dir).
