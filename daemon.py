'''Kurokami daemon: scheduling, storage, and feed for the live-monitor GUI.

App layer like cli.py / server.py: it owns watches, the politeness scan
queue, and per-watch state on disk, and never touches sockets or HTTP. Every
scrape is delegated to the pure core (kurokami.scrape / kurokami.new_rows);
the CLI's CSV shapes and utf-8-sig encoding are kept as-is.

Storage model (wayfinder ticket 007):
  output/watches/<id>/history.csv   append-only, CLI column shape
  output/watches/<id>/state.json    rewriteable runtime state (restart survivor)
  output/feed.jsonl                 append-only, server-wide
queries.json is a read-once seed applied only when output/ is empty; the
daemon catalog is authoritative afterwards and never rewritten by hand.
'''

import asyncio
import collections
import json
import os
import time

import pandas as pd

import kurokami

DATA_DIR = "output"
WATCHES_DIR = "watches"
FEED_PATH = "feed.jsonl"
DEFAULT_INTERVAL_MIN = 10
POLITENESS_FLOOR_MIN = 10
DEFAULT_MIN_SCRAPE_GAP = 15.0  # seconds between scrapes
DEFAULT_BACKOFF_MIN = 15.0     # minutes; one auto-retry, then stalled
TICK = 0.5                     # scheduler wake seconds
FEED_RING = 200                # in-memory feed tail kept for serving

FEED_NEW, FEED_OK, FEED_WRN, FEED_QUEUED = "NEW", "OK", "WRN", "QUEUED"

HISTORY_COLUMNS = ["uid", "seller_name", "price", "time_posted",
                   "condition", "item_name", "item_url", "item_img", "seller_url"]


def _jsonable(value):
    """Recursively turn pandas/numpy scalars into native json types.
    NaN/NaT become None so the wire JSON never carries Python's bare `NaN`
    token, which is invalid JSON for browsers (a NaN row previously made
    /api/watches/{id}/results unparseable)."""
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, float):
        return None if value != value else value
    if value is not None and hasattr(value, "item"):  # numpy scalar / NaT
        return _jsonable(value.item())
    return value


def _fmt_state(state_path):
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["prev_scan_uids"] = set(data.get("prev_scan_uids") or [])
        data["last_new_uids"] = set(data.get("last_new_uids") or [])
        return data
    except (IOError, ValueError, KeyError):
        return None


class Watch:
    """One saved search + its schedule and diff state. Plain class (3.6)."""

    __slots__ = ("id", "item", "count", "price_low", "price_high",
                 "interval_min", "continuous", "sort_by", "status",
                 "last_scan", "next_scan", "consecutive_failures",
                 "baseline_done", "prev_scan_uids", "last_new_uids",
                 "last_rows", "queued")

    def __init__(self, wid, item, count, price_low=None, price_high=None,
                 interval_min=DEFAULT_INTERVAL_MIN, continuous=False,
                 sort_by=None):
        self.id = wid
        self.item = item
        self.count = count
        self.price_low = price_low
        self.price_high = price_high
        self.interval_min = interval_min
        self.continuous = continuous
        self.sort_by = kurokami.normalize_sort_name(sort_by)
        self.status = "scheduled"   # idle/scheduled/queued/running/stalled
        self.last_scan = None
        self.next_scan = time.time()  # baseline fires immediately
        self.consecutive_failures = 0
        self.baseline_done = False
        self.prev_scan_uids = set()
        self.last_new_uids = set()
        self.last_rows = []
        self.queued = False  # runtime: already on the scan queue (not persisted)


class Daemon:
    """Owns watches, the serialized politeness queue, and on-disk state."""

    def __init__(self, data_dir=DATA_DIR, test_mode=False,
                 seed_path="queries.json", min_gap=DEFAULT_MIN_SCRAPE_GAP,
                 backoff_min=DEFAULT_BACKOFF_MIN):
        self.data_dir = data_dir
        self.test_mode = test_mode
        self.seed_path = seed_path
        self.min_gap = min_gap
        self.backoff_seconds = backoff_min * 60
        self._watches = {}
        self._next_id = 1
        self._queue = asyncio.Queue()
        self._running = False
        self._last_scrape_at = 0
        self._feed_ring = collections.deque(maxlen=FEED_RING)

    # ---------- persistence paths ----------

    def _watch_dir(self, wid):
        return os.path.join(self.data_dir, WATCHES_DIR, str(wid))

    def _state_path(self, wid):
        return os.path.join(self._watch_dir(wid), "state.json")

    def _history_path(self, wid):
        return os.path.join(self._watch_dir(wid), "history.csv")

    def _feed_path(self):
        return os.path.join(self.data_dir, FEED_PATH)

    # ---------- feed ----------

    def _emit(self, kind, msg, watch=None, listing=None):
        entry = {"t": time.time(), "kind": kind, "msg": msg,
                 "watch": watch, "listing": listing}
        self._feed_ring.append(entry)
        path = self._feed_path()
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        return entry

    def feed(self, limit=FEED_RING):
        entries = list(self._feed_ring)
        entries.reverse()
        return entries[:limit]

    # ---------- load / save ----------

    def load(self):
        """Restore watches from output/watches/*; seed from queries.json when
        the catalog is empty."""
        watches_dir = os.path.join(self.data_dir, WATCHES_DIR)
        if os.path.isdir(watches_dir):
            for name in sorted(os.listdir(watches_dir)):
                state = _fmt_state(os.path.join(watches_dir, name, "state.json"))
                if state is None:
                    continue
                wid = state.get("id", name)
                w = Watch(wid, state["item"], state["count"],
                          state.get("price_low"), state.get("price_high"),
                          state.get("interval_min", DEFAULT_INTERVAL_MIN),
                          state.get("continuous", False),
                          state.get("sort_by"))
                w.status = state.get("status", "scheduled")
                w.last_scan = state.get("last_scan")
                w.next_scan = state.get("next_scan")
                w.consecutive_failures = state.get("consecutive_failures", 0)
                w.baseline_done = state.get("baseline_done", False)
                w.prev_scan_uids = state.get("prev_scan_uids") or set()
                w.last_new_uids = state.get("last_new_uids") or set()
                self._watches[wid] = w
        if self._watches:
            self._next_id = max(self._watches) + 1
        else:
            for query in self._load_seed():
                self.add_watch(
                    query.get("item"),
                    query.get("count", 25),
                    price_low=query.get("price_low"),
                    price_high=query.get("price_high"),
                    sort_by=query.get("sort_by"),
                )
        # restore the feed tail so /api/feed survives restarts
        if os.path.exists(self._feed_path()):
            tail = []
            try:
                with open(self._feed_path(), "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            tail.append(json.loads(line))
            except (IOError, ValueError):
                tail = []
            self._feed_ring = collections.deque(tail[-FEED_RING:], maxlen=FEED_RING)

    def _load_seed(self):
        if not os.path.exists(self.seed_path):
            return []
        try:
            with open(self.seed_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (IOError, ValueError):
            return []
        if isinstance(data, dict):
            data = data.get("queries", [])
        return data if isinstance(data, list) else []

    def _persist(self, w):
        directory = self._watch_dir(w.id)
        os.makedirs(directory, exist_ok=True)
        state = {
            "id": w.id, "item": w.item, "count": w.count,
            "price_low": w.price_low, "price_high": w.price_high,
            "interval_min": w.interval_min, "continuous": w.continuous,
            "sort_by": w.sort_by,
            "status": w.status, "last_scan": w.last_scan,
            "next_scan": w.next_scan,
            "consecutive_failures": w.consecutive_failures,
            "baseline_done": w.baseline_done,
            "prev_scan_uids": sorted(w.prev_scan_uids),
            "last_new_uids": sorted(w.last_new_uids),
        }
        with open(self._state_path(w.id), "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    def _append_history(self, w, rows):
        df = pd.DataFrame(rows, columns=HISTORY_COLUMNS)
        path = self._history_path(w.id)
        header = not os.path.exists(path)
        df.to_csv(path, index=False, encoding="utf-8-sig", mode="a",
                  header=header)

    # ---------- watch catalog API ----------

    def add_watch(self, item, count, price_low=None, price_high=None,
                  interval_min=DEFAULT_INTERVAL_MIN, continuous=False,
                  sort_by=None):
        w = Watch(self._next_id, item, count, price_low, price_high,
                  interval_min, continuous, sort_by)
        w.status = "queued"  # baseline scan is queued immediately on add
        w.queued = True
        self._watches[w.id] = w
        self._next_id += 1
        self._persist(w)
        self._queue.put_nowait(("watch", w))
        return w.id

    def remove_watch(self, wid):
        w = self._watches.pop(wid, None)
        if w is None:
            return False
        path = self._watch_dir(wid)
        for name in ("state.json", "history.csv"):
            candidate = os.path.join(path, name)
            if os.path.exists(candidate):
                os.remove(candidate)
        try:
            os.rmdir(path)
        except OSError:
            pass
        return True

    def submit_adhoc(self, coro):
        """Enqueue any coroutine (a one-off scrape) on the same politeness queue."""
        self._queue.put_nowait(("adhoc", coro))

    def update_watch(self, wid, item, count, price_low, price_high,
                     interval_min, continuous, sort_by):
        """Rewrite a watch's query/schedule and persist. Returns the watch or
        None. Changing the query identity resets the diff baseline and queues
        a fresh scan (a running or already-queued watch skips the requeue; the
        in-flight scrape keeps its snapshot, later scans use the new fields)."""
        w = self._watches.get(wid)
        if w is None:
            return None
        old_identity = (w.item, w.count, w.price_low, w.price_high, w.sort_by)
        new_identity = (item, count, price_low, price_high, sort_by)
        w.item = item
        w.count = count
        w.price_low = price_low
        w.price_high = price_high
        w.sort_by = kurokami.normalize_sort_name(sort_by)
        w.interval_min = interval_min
        w.continuous = continuous
        if new_identity != old_identity:
            w.baseline_done = False
            w.prev_scan_uids = set()
            w.last_new_uids = set()
            w.last_rows = []
            w.consecutive_failures = 0
            if not w.queued and w.status != "running":
                w.status = "queued"
                w.next_scan = time.time()
                w.queued = True
                self._queue.put_nowait(("watch", w))
                self._emit(FEED_QUEUED,
                           "watch w%d edited \u00b7 query changed, fresh scan queued"
                           % w.id, watch=w.id)
            else:
                self._emit(FEED_OK,
                           "watch w%d edited \u00b7 query changed, scan pending"
                           % w.id, watch=w.id)
        else:
            if w.status == "scheduled":
                w.next_scan = time.time() + w.interval_min * 60
            self._emit(FEED_OK, "watch w%d edited \u00b7 schedule updated"
                       % w.id, watch=w.id)
        self._persist(w)
        return w

    def retry(self, wid):
        w = self._watches.get(wid)
        if w is None or w.status != "stalled":
            return False
        w.status = "queued"
        w.next_scan = time.time()
        w.queued = True
        self._persist(w)
        self._queue.put_nowait(("watch", w))
        self._emit(FEED_QUEUED, "retry w%d triggered \u00b7 rerun queued" % w.id, watch=w.id)
        return True

    def trigger(self, wid):
        """Manual rescan request: requeue the watch now, whatever its state
        (idle/scheduled/stalled). Refused while already queued or running."""
        w = self._watches.get(wid)
        if w is None or w.queued or w.status == "running":
            return False
        w.status = "queued"
        w.next_scan = time.time()
        w.queued = True
        self._persist(w)
        self._queue.put_nowait(("watch", w))
        self._emit(FEED_QUEUED, "manual scan w%d triggered \u00b7 rerun queued" % w.id, watch=w.id)
        return True

    def get(self, wid):
        return self._watches.get(wid)

    def watches(self):
        return sorted(self._watches.values(), key=lambda w: w.id)

    def _row_msg(self, row):
        parts = [row.get("item_name") or ""]
        condition = row.get("condition")
        if condition and condition not in ("N/A", ""):
            parts.append(condition)
        return " \u00b7 ".join(parts)

    # ---------- scanning ----------

    def _dispatch(self, now):
        for wid, w in list(self._watches.items()):
            if w.queued:
                continue
            if w.status in ("scheduled", "queued") and w.next_scan is not None \
                    and w.next_scan <= now:
                w.queued = True
                self._queue.put_nowait(("watch", w))

    def _normalize_rows(self, df):
        rows = []
        for row in df.to_dict("records"):
            rows.append({k: _jsonable(v) for k, v in row.items()})
        return rows

    def _record_success(self, w, rows):
        uids = {str(r["uid"]) for r in rows}
        self._append_history(w, rows)
        w.last_rows = rows
        if w.baseline_done:
            new_uids = sorted(u for u in uids if u not in w.prev_scan_uids)
            w.last_new_uids = set(new_uids)
            for uid in new_uids:
                row = next((r for r in rows if str(r["uid"]) == uid), None)
                self._emit(FEED_NEW, self._row_msg(row or {}), watch=w.id, listing=row)
            self._emit(FEED_OK, "scan w%d complete \u00b7 %d rows, %d new, blacklist applied"
                       % (w.id, len(rows), len(new_uids)), watch=w.id)
        else:
            w.baseline_done = True
            w.last_new_uids = set()
            self._emit(FEED_OK, "first scan w%d complete \u00b7 baseline recorded, nothing notified"
                       % w.id, watch=w.id)
        w.prev_scan_uids = uids
        w.consecutive_failures = 0
        w.last_scan = time.time()
        w.next_scan = time.time() + w.interval_min * 60 if w.continuous else None
        w.status = "scheduled" if w.continuous else "idle"
        self._persist(w)

    def _record_failure(self, w, exc):
        w.consecutive_failures += 1
        w.last_scan = time.time()
        if w.consecutive_failures == 1:
            w.status = "scheduled"
            w.next_scan = time.time() + self.backoff_seconds
            self._emit(FEED_WRN, "scan w%d failed \u00b7 auto-retry in ~%dm"
                       % (w.id, int(round(self.backoff_seconds / 60))), watch=w.id)
        else:
            w.status = "stalled"
            w.next_scan = None
            self._emit(FEED_WRN, "watch w%d stalled on consecutive failure \u00b7 retry now; "
                       "Chrome left open at last page" % w.id, watch=w.id)
        self._persist(w)

    async def _scan(self, w):
        self._running = True
        w.status = "running"
        self._persist(w)
        try:
            df = await kurokami.scrape(
                w.item, count=w.count,
                price_low=w.price_low, price_high=w.price_high,
                test=self.test_mode, sort_by=w.sort_by,
            )
            self._record_success(w, self._normalize_rows(df))
        except Exception as exc:
            self._record_failure(w, exc)
        finally:
            self._running = False

    async def run(self):
        """Background scheduler + single politeness worker."""
        while True:
            self._dispatch(time.time())
            try:
                kind, obj = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(TICK)
                continue
            now = time.time()
            gap = max(0.0, self._last_scrape_at + self.min_gap - now)
            if gap:
                await asyncio.sleep(gap)
            self._last_scrape_at = time.time()
            if kind == "watch":
                w = self._watches.get(obj.id)
                if w is not None:
                    w.queued = False
                    await self._scan(w)
            else:
                await obj
            self._queue.task_done()

    # ---------- status ----------

    def status(self):
        now = time.time()
        last_ago = round(now - self._last_scrape_at, 1) if self._last_scrape_at else None
        below_floor = [w.id for w in self.watches()
                       if w.continuous and w.interval_min < POLITENESS_FLOOR_MIN]
        return {
            "test_mode": self.test_mode,
            "in_flight": self._running,
            "pending": self._queue.qsize(),
            "last_scrape_ago": last_ago,
            "min_gap": self.min_gap,
            "watch_count": len(self._watches),
            "below_floor": below_floor,
            "politeness_floor_min": POLITENESS_FLOOR_MIN,
        }

    def watch_dict(self, w):
        return {
            "id": w.id, "item": w.item, "count": w.count,
            "price_low": w.price_low, "price_high": w.price_high,
            "interval_min": w.interval_min, "continuous": w.continuous,
            "sort_by": w.sort_by,
            "status": w.status, "last_scan": w.last_scan,
            "next_scan": w.next_scan,
            "consecutive_failures": w.consecutive_failures,
            "baseline_done": w.baseline_done,
            "new_count": len(w.last_new_uids),
            "row_count": len(w.prev_scan_uids),
            "below_floor": w.continuous and w.interval_min < POLITENESS_FLOOR_MIN,
        }

    def results(self, wid, fallback_limit=None):
        w = self._watches.get(wid)
        if w is None:
            return None
        rows = list(w.last_rows)
        if not rows and os.path.exists(self._history_path(wid)):
            try:
                rows = pd.read_csv(self._history_path(wid), encoding="utf-8-sig")
                rows = self._normalize_rows(rows)
                rows = rows[-fallback_limit:] if fallback_limit else rows
            except (IOError, ValueError):
                rows = []
        out = []
        for raw in rows:
            row = {k: _jsonable(v) for k, v in raw.items()}
            row["new"] = str(row["uid"]) in w.last_new_uids
            out.append(row)
        return {"watch": self.watch_dict(w), "rows": out}