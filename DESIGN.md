---
name: kurokami
description: A polite daemon's terminal — local live-monitor GUI for Carousell watch scraping.
colors:
  ground: "#10140b"
  ground-raise: "#161b10"
  ground-sunken: "#0b0e07"
  bone: "#e9e4d1"
  bone-bright: "#f8f3df"
  dim: "#9aa38d"
  dim-2: "#8a9279"
  hairline: "rgba(233, 228, 209, 0.14)"
  hairline-strong: "rgba(233, 228, 209, 0.34)"
  green: "#38c76f"
  green-bright: "#83ec9f"
  red: "#e0544f"
  red-bright: "#ff9380"
  amber: "#e0b14d"
typography:
  mono:
    fontFamily: 'ui-monospace, "Cascadia Code", "SF Mono", "Consolas", "Menlo", monospace'
  heading:
    fontFamily: 'ui-monospace, "Cascadia Code", "SF Mono", "Consolas", "Menlo", monospace'
    fontSize: "1.45rem"
    fontWeight: 600
    lineHeight: 1.55
    letterSpacing: "-0.02em"
  body:
    fontFamily: 'ui-monospace, "Cascadia Code", "SF Mono", "Consolas", "Menlo", monospace'
    fontSize: "15px"
    fontWeight: 400
    lineHeight: 1.55
    letterSpacing: "normal"
spacing:
  step: "4px"
  space-1: "8px"
  space-2: "16px"
  space-3: "28px"
rounded:
  radius-0: "5px"
  radius-1: "6px"
  radius-lg: "10px"
components:
  window:
    backgroundColor: "{colors.ground-raise}"
    textColor: "{colors.bone}"
    rounded: "{rounded.radius-lg}"
  button-primary:
    backgroundColor: "{colors.green}"
    textColor: "{colors.ground}"
    rounded: "{rounded.radius-1}"
    padding: "5px 10px"
  button-primary-hover:
    backgroundColor: "{colors.green-bright}"
    textColor: "{colors.ground}"
    rounded: "{rounded.radius-1}"
    padding: "5px 10px"
  badge-new:
    backgroundColor: "{colors.green}"
    textColor: "{colors.ground}"
    rounded: "{rounded.radius-0}"
    typography: "{typography.mono}"
  badge-stalled:
    backgroundColor: "{colors.red}"
    textColor: "{colors.ground}"
    rounded: "{rounded.radius-0}"
    typography: "{typography.mono}"
  input:
    backgroundColor: "{colors.ground-sunken}"
    textColor: "{colors.bone}"
    rounded: "{rounded.radius-1}"
---

## Overview

kurokami is a local live-monitor for Carousell watch scraping, imagined as **the polite daemon's terminal**: a near-idle background process whose quiet screen is mostly dark ink, ruled by hairlines, and lit only where something actually happened. One signal pair — green meaning live/new, red meaning stalled/failure — carries every state that matters. Nothing decorative, nothing loud; every element is either a data cell, a state stamp, or a control a terminal operator would reach for.

The GUI is plain static HTML/CSS/JS (no build step) under `static/`. The world is deliberately anchored in the terminal genre: monospace only, tabular numerals, `--` / `·` separator grammar, immediate past-tense status lines, a `kurokami:~$` prompt glyph as the only flourish.

## Colors

Dark-ground inkwell palette; the ground is a deep green-black, the ink a warm bone-white, with exactly one functional signal pair plus a borrowed amber for the politeness warning.

| token | value | use |
|---|---|---|
| `--ground` | `#10140b` | page field; deep green-black |
| `--ground-raise` | `#161b10` | bar, hover, raised strips |
| `--ground-sunken` | `#0b0e07` | inputs, cut-through panel fill |
| `--bone` | `#e9e4d1` | body ink |
| `--bone-bright` | `#f8f3df` | headings / active ink |
| `--dim` | `#9aa38d` | secondary ink (≥ 4.5:1 on ground) |
| `--dim-2` | `#8a9279` | muted / placeholders (≥ 4.5:1 on ground) |
| `--hairline` / `--hairline-strong` | white at 14% / 34% | fine rules; strong for separators and borders |
| `--green` | `#38c76f` | new / live |
| `--green-bright` | `#83ec9f` | stamp ink on green ground |
| `--red` | `#e0544f` | stalled / failure |
| `--red-bright` | `#ff9380` | stamp ink on red ground |
| `--amber` | `#e0b14d` | politeness warning (borrowed signal) |

Rules: signal colors never decorate — they label a live state. Red ink on a stalled row is the row's whole message. The 4.5:1 floor is enforced for every dim tone against `--ground`.

## Typography

One system: a monospace stack throughout — `ui-monospace, "Cascadia Code", "SF Mono", "Consolas", "Menlo", monospace`. No display face; hierarchy is by weight, size, and ink rather than by face contrast.

- Base: 15px / 1.55 on the page (14px below 720px wide).
- Heading (`h1`): 1.45rem, weight 600, tight tracking −0.02em, `--bone-bright`.
- Tabular numerals on every data cell (`.num { font-variant-numeric: tabular-nums }`) so counts, times, and prices column-align.
- Micro-labels: 0.75–0.8rem with 0.03em tracking for roster heads and labels.
- The active nav item and the brand sit in `--green`; the `:~$` prompt glyph is `--dim-2` at weight 400.
- Copy grammar: `--` prefixes status/prose lines; `·` separates metadata fields; em-dashes banned in visible copy (middots instead).

## Layout

- Page: centered column, `max-width: 1120px`, full-height content rail, `color-scheme: dark`.
- Every surface is one terminal-style `.window`: a raised panel (hairline-strong border, 10px radius, sunken-gradient padding-box over ground) holding the terminal chrome (`bar` · content · `statusline`).
- `bar` (top): brand + prompt glyph · host `127.0.0.1:8080` · right-aligned nav (roster / feed / settings) · live clock. Hairline under.
- Content column per surface, each owning its grammar:
  - **roster** (index): header with rollup stats and add button; optional politeness banner; 7-column grid (`watch / query / int / last scan / next scan / count / state`), stalled rows pinned with a red-tinted strip.
  - **results** (per-watch): `cat watch::<id>` heading; review-kick link; NEW rows first with green stamp; thumbnail plates as dashed placeholder blocks; the listing price as its own tally-style cell on the row's right edge (`bone-bright`, weight 600, tabular) — the one number a buyer reads first; metadata line keeps condition · seen · uid only; babysteps footer.
  - **feed**: timestamped `tail -f` lines; `time · KIND · message · watch link · open`; KIND stamps `NEW` / `OK` / `WRN` / `QUEUED`; on NEW entries the price leads the message as the same tally-style token (the daemon message itself stays price-free; the frontend renders it from the listing row).
  - **settings**: `nano watches.conf` identity · schedule · flags; readonly global fieldset; watch list; save/cancel.
- 7-column roster grid collapses below 720px (`.cell-query` spans full row, query-first order).
- Footer `statusline`: two dim `--` lines; synthetic-data is always labeled here on pages carrying demo content.

## Elevation & Depth

Effectively flat. Depth is expressed by tonal steps between `ground-sunken` (inputs, panel cut-through), `ground` (page), and `ground-raise` (bar, hover, raised rows) — never by shadows. The only "depth" device is the `.window` sunken-gradient padding-box trick and 1px hairlines. Hover = one raise step + a green border-color nudge on clickables.

## Shapes

- Radius is small and consistent: `--radius` 10px for the window panel; 6px for inputs and buttons; 5px for badges and the retry pill; 8px circles for state pips.
- No hard offset shadows, no gradient fills beyond the window's own sunken trick, no decorative geometry. Shape carries only grouping (panel, pill, pip) and state (dashed border = pending/queued).

## Components

- **Terminal chrome**: `.window` / `.bar` / `.statusline` — the fixed frame every surface shares.
- **Badges**: uppercase micro-stamps (`.badge-new` green-on-ground, `.badge-stalled` red-on-ground, `.badge-queued` transparent with dashed hairline border) at 0.78rem / 700 / 0.06em tracking, 5px radius.
- **Pips**: 8px status dots — `--green` live, `--red` stalled, hairline-strong idle/queued.
- **Buttons**: flat outline pills (hairline-strong border, transparent fill); `.btn-primary` invert to green fill with ground ink; hover raises border/ink to green or bright green.
- **Retry pill**: red outline, red-bright ink; hover inverts to red fill / ground ink.
- **Inputs**: sunken ground, hairline-strong border, 6px radius, 15px mono; focus ring is a 1px green outline at +2px offset.
- **Links**: inherit ink; underline-offset 3px; hover `--bone-bright`. Nav active = green. Watch/feed links w32-green text where they identify a watch.
- **Politeness note**: amber one-liner, `--`-prefixed, shown only while a watch sits below the ~10m floor; validation errors are quiet red one-liners just under the offending field group.

## Do's and Don'ts

- Do let state speak through the signal pair: green for new/live, red for stalled/failure, and keep every other pixel near-neutral.
- Do honor the politeness fiction: the daemon is observable, unfussable, occasionally warns — it never nags.
- Do keep data in tabular numerals and columns aligned; numbers are the product.
- Do label synthetic/demo content wherever a visitor could mistake it for real listings.
- Do respect the "pinned, most recently scanned" roster order; never scramble states into decoration.
- Don't add shadows, gradients, textures, or dimensional material — this world is flat ink.
- Don't use em-dashes in visible copy (`·` and `--` instead); don't use glyph/emoji icons.
- Don't introduce second display faces, all-caps frills as body text, or decorative color outside the token set.
- Don't invent commercial claims — prices and listings in demos are placeholders marked synthetic.