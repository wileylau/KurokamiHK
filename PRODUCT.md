# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

Static HTML/CSS/JS with no build step — confirmed decision (ticket 002, user-approved). Served locally by the kurokami aiohttp server bound to 127.0.0.1; dev iteration runs on any plain static server.

## Users

The operator is a deal-hunter watching Carousell Hong Kong listings: they add search watches (item query + count + price bounds), rely on the monitor to rescan on schedule and surface new listings, and check the dashboard often and briefly. Success = a new listing is noticed within minutes of appearing, and acted on before it sells. Single operator, local tool, no auth, no multiuser.

## Product Purpose

A local live-monitor web GUI for kurokami: scheduled background rescans of Carousell HKG searches, new-listing detection, and an in-app notification feed — replacing the manual run-the-CLI-and-diff loop with a glanceable always-on surface.

## Positioning

A polite, local-first Carousell HKG monitor: one throttled background Chrome session rescans saved searches on schedule, diffs each scan against the previous one (`right_only` semantics), and feeds the operator only genuinely new listings — keeping the CLI's parsing, blacklist, and CSV behavior intact, and leaving the future Discord bot path open.

## Operating Context

- Bound to 127.0.0.1; the operator opens it in Chrome (already a repo dependency) and glances at it repeatedly across short sessions. Modest watch count: a handful to dozens.
- Runs on the repo's existing machinery: Carousell HKG search URLs, blacklist (`utils/blacklist.txt`), parser, uid `right_only` diff semantics (CLI `-c`), and `utf-8-sig` CSV history in `output/`.
- The politeness floor (~1 scrape / 10 min) is a **soft** limit: sub-floor intervals are permitted but must surface a warning banner.
- Python 3.6+ syntax floor for the server side; CLI flows, exit codes, and CSV output are untouched outside daemon mode.

## Capabilities and Constraints

Capabilities (v1 contract; seeds the surfaces):

- A **watch** = item query + item count + price bounds (low/high), inheriting the global blacklist. One-shot by default; continuous rescan opt-in per watch. Watch count unbounded; duplicates allowed, each independent.
- Per-watch **interval** in minutes (default 10, configurable), soft politeness floor with a warning banner. Baseline scan fires immediately on add; then every interval while continuous.
- On **failure**: one ~15 m backoff retry (capped); a second consecutive failure escalates the watch to **stalled** — auto-retries stop, the failure surfaces in the feed/UI, the persistent Chrome session is left open for human verification, recovery is a user "retry now" action.
- Global **politeness queue**: one scrape in flight, spaced ≥ ~15 s, across baselines, retries, and rescans.
- **New-listing detection**: per-watch uid `right_only` diff against the previous scan; a watch's first scan is its baseline (recorded, never notified); a reappearing uid counts as new again.
- **Notification** (v1): in-app feed only, server-persisted and unbounded (append-only), driving the UI. One entry per new uid per scan.
- **History**: append-only per-watch CSV in `output/`, existing format (`utf-8-sig`, CLI column shape); diff baseline is an in-memory uid snapshot per watch.

Constraints:

- What a graph endpoint or UI surface shows must map to the CLI's semantics exactly. No second data model.
- Not in scope for v1: browser `Notification`, Discord webhook (bot path stays architecturally open), price-drop / disappeared-listing events.
- The GUI surfaces the politeness warning; it never hard-blocks, per the contract.

## Brand Commitments

Name: **kurokami**. No other identity assets exist in-tree; no voice, palette, or style commitments beyond the name. Human-facing copy in the surfaces should stay plain and technical (a tool for operators), but that is a working default, not a binding tone document.

## Evidence on Hand

- `README.md` (usage, politeness guidance), `AGENTS.md` (architecture and constraints) at repo root.
- CLI reference implementation: `kurokami/` (core library), `kurokami.py`, `utils/blacklist.txt`, `utils/soup.pkl`.
- Product contract: `wayfinder/tickets/001-define-the-v1-live-monitor-contract.md`; architecture map: `wayfinder/map.md`.
- No brand assets, logos, screenshots, marketing copy, or real dashboard imagery exist. Nothing to fabricate: dummy data in mocks is explicitly synthetic.

## Product Principles

1. **Catch new listings first.** The newest `right_only` items on a watch are the reason the product exists; the dashboard must make them impossible to miss and cost about a second to read.
2. **Politeness is character.** Throttling and the soft politeness floor are respected and surfaced, never hidden — this is a scraping-respectful tool, by design.
3. **State is legible at a glance.** Watch status (idle / scheduled / queued / stalled / failure), last-scan time, and new-item counts must read from the first glance, without clicking.
4. **CLI semantics stay sovereign.** Everything the GUI shows (uids, `right_only` diff, CSV shape) is exactly what the CLI already produces; the GUI is a surface on the same engine, never a second implementation.
5. **Local-first and honest.** A local 127.0.0.1 tool with no cloud and no auth theater; when a watch stalls or a scrape failed, the UI says so plainly, in icon plus text, not color alone.

## Accessibility & Inclusion

Local desktop web app, single operator in Chrome, used daily. Color must not be the only channel for state (stalled / failure / new-item states carry icon + text, not just hue). Reasonable contrast and full keyboard operability are expected for a tool in daily use.