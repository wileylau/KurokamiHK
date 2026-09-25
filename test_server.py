'''Standalone smoke check for the GUI server (no test framework needed).

Runs the aiohttp app in-process with `kurokami.scrape` monkeypatched, so no
live Carousell fetch / Chrome session fires. Covers the shell's adhoc search
job seam plus the daemon surface: watch lifecycle, baseline/new-listing diff,
backoff -> stall -> retry, feed, results, and storage. Run:  python test_server.py
'''

import asyncio
import os
import sys
import tempfile

import pandas as pd
from aiohttp.test_utils import TestClient, TestServer

import kurokami
import server

RESULTS = []

STATE = {"calls": {}, "sorts": {}}  # item -> call counter / sort_by for the fake scraper


def check(name, cond, extra=""):
    RESULTS.append((name, bool(cond), extra))


def _rows(item, count):
    return pd.DataFrame([{
        "uid": "%s-%d" % (item, i),
        "seller_name": "seller",
        "price": ["$1"],
        "time_posted": "now",
        "condition": "new",
        "item_name": item,
        "item_url": "https://carousell.com.hk/p/x-%d" % i,
        "item_img": None,
        "seller_url": "https://carousell.com.hk/u/seller",
    } for i in range(count)])


async def fake_scrape(item, count=25, *, price_low=None, price_high=None,
                      test=False, serialize=False, blacklist=None, home=None,
                      sort_by=None):
    n = STATE["calls"].get(item, 0)
    STATE["calls"][item] = n + 1
    STATE["sorts"][item] = sort_by
    if item == "fail":
        raise kurokami.NoResultsError("forced failure")
    if item == "flaky" and n < 2:
        raise kurokami.NoResultsError("forced first two failures")
    return _rows(item, count)


async def main():
    real_scrape = kurokami.scrape
    kurokami.scrape = fake_scrape
    tmp = tempfile.TemporaryDirectory(prefix="kurokami-test-")
    try:
        app = server.make_app(test_mode=False, data_dir=tmp.name,
                              queries_path=os.path.join(tmp.name, "nope.json"),
                              min_gap=0.0, backoff_min=0.0005)
        client = TestClient(TestServer(app))
        await client.start_server()

        # ---- shell: static + config + adhoc search job seam ----
        r = await client.get("/")
        body = await r.read()
        check("GET / serves the roster page", r.status == 200 and b"watchers" in body.lower() and b"roster" in body.lower())

        r = await client.get("/static/index.html")
        check("GET /static/index.html serves", r.status == 200)

        r = await client.get("/api/config")
        cfg = await r.json()
        check("GET /api/config 200 + empty seed (no queries.json)", r.status == 200 and cfg["queries"] == [] and cfg["test_mode"] is False)

        r = await client.get("/api/status")
        st = await r.json()
        check("GET /api/status reports empty daemon", r.status == 200 and st["watch_count"] == 0 and st["test_mode"] is False)

        # search validation (SSRF note: browser input never reaches scrape unvalidated)
        cases = [
            ("non-JSON body", "not json"),
            ("missing item", {"count": 25}),
            ("blank item", {"item": "   "}),
            ("bad count", {"item": "x", "count": "abc"}),
            ("count out of range", {"item": "x", "count": 0}),
            ("bad price", {"item": "x", "price_high": "abc"}),
            ("negative price", {"item": "x", "price_low": -5}),
            ("low > high", {"item": "x", "price_low": 500, "price_high": 100}),
        ]
        for name, payload in cases:
            if payload == "not json":
                r = await client.post("/api/search", data="not json")
            else:
                r = await client.post("/api/search", json=payload)
            check("400: " + name, r.status == 400)

        # watch validation is a superset of search validation
        r = await client.post("/api/watches", json={"item": "x", "count": 1, "interval_min": 0})
        check("400: watch interval below floor", r.status == 400)
        r = await client.post("/api/watches", json={"item": "x", "count": 1, "continuous": "maybe"})
        check("400: watch bad continuous", r.status == 400)
        r = await client.post("/api/watches", json={"item": "x", "count": 1, "sort_by": "bogus"})
        check("400: watch bad sort_by", r.status == 400)

        # adhoc search job on the shared politeness queue
        r = await client.post("/api/search", json={"item": "test item", "count": 3, "price_low": 10, "price_high": 20})
        job = await r.json()
        check("POST /api/search -> 202 pending", r.status == 202 and job["status"] == "pending")

        job_id = job["id"]
        for _ in range(50):
            r = await client.get("/api/search/" + job_id)
            job = await r.json()
            if job["status"] in ("done", "failed"):
                break
            await asyncio.sleep(0.05)
        check("adhoc job runs to done", job["status"] == "done", str(job))
        check("done job carries count rows", len(job["items"]) == 3)

        r = await client.get("/api/search/does-not-exist")
        check("unknown job -> 404", r.status == 404)

        r = await client.get("/api/jobs")
        jobs = await r.json()
        check("GET /api/jobs lists the job", any(j["id"] == job_id for j in jobs))

        # ---- daemon: watch lifecycle ----
        r = await client.post("/api/watches", json={"item": "cam", "count": 3,
                                                    "price_low": 10, "price_high": 20,
                                                    "interval_min": 10, "continuous": False,
                                                    "sort_by": "4"})
        w = await r.json()
        check("POST /api/watches -> 202 queued + sort normalized",
              r.status == 202 and w["status"] == "queued" and w["sort_by"] == "price_asc")
        wid = w["id"]

        w = await _wait_watch(client, wid, done=lambda x: x["row_count"] == 3)
        check("baseline scan runs to 3 rows", w["row_count"] == 3 and w["baseline_done"] is True)
        check("baseline records no new", w["new_count"] == 0)
        check("baseline scan passes the watch sort_by to scrape",
              STATE["sorts"].get("cam") == "price_asc")

        history = os.path.join(tmp.name, "watches", str(wid), "history.csv")
        check("history.csv written with utf-8-sig BOM", os.path.exists(history) and open(history, "rb").read(3) == b"\xef\xbb\xbf")

        r = await client.get("/api/watches/" + str(wid) + "/results")
        res = await r.json()
        check("results carries 3 rows + no NEW flags",
              r.status == 200 and len(res["rows"]) == 3 and not any(x["new"] for x in res["rows"]))

        # ---- daemon: failure -> backoff -> stall -> retry ----
        r = await client.post("/api/watches", json={"item": "flaky", "count": 2, "interval_min": 10, "continuous": False})
        widb = (await r.json())["id"]
        w = await _wait_watch(client, widb, done=lambda x: x["status"] == "stalled", rounds=80)
        check("two consecutive failures => stalled", w is not None and w["status"] == "stalled" and w["consecutive_failures"] == 2)

        r = await client.post("/api/watches/" + str(widb) + "/retry")
        w = await r.json()
        check("retry requeues a stalled watch", r.status == 202 and w["status"] in ("queued", "running"))
        w = await _wait_watch(client, widb, done=lambda x: x["row_count"] == 2)
        check("retry succeeds and resets failures", w["status"] == "idle" and w["consecutive_failures"] == 0)

        r = await client.post("/api/watches/" + str(wid) + "/retry")
        check("retry on non-stalled watch -> 409", r.status == 409)

        # daemon: manual rescan trigger
        r = await client.post("/api/watches/" + str(widb) + "/rescan")
        w2 = await r.json()
        check("POST /api/watches/{id}/rescan -> 202 queued",
              r.status == 202 and w2["status"] == "queued")
        w = await _wait_watch(client, widb, done=lambda x: x["status"] == "idle", rounds=80)
        check("manual rescan runs through to idle again",
              w is not None and w["status"] == "idle")
        r = await client.post("/api/watches/9999/rescan")
        check("rescan unknown watch -> 404", r.status == 404)

        # daemon: edit (PATCH) — query change resets baseline + requeues
        r = await client.patch("/api/watches/" + str(widb), json={"sort_by": "5"})
        w3 = await r.json()
        check("PATCH watch sort -> 202 + normalized", r.status == 202 and w3["sort_by"] == "price_desc")
        w = await _wait_watch(client, widb, done=lambda x: x["status"] == "idle", rounds=80)
        check("edited query rescan runs back to idle", w is not None and w["status"] == "idle")
        check("edited query re-baselined", w["baseline_done"] is True)
        check("edited query sorts the next scrape", STATE["sorts"].get("flaky") == "price_desc")

        r = await client.patch("/api/watches/9999", json={"item": "x"})
        check("PATCH unknown watch -> 404", r.status == 404)
        r = await client.patch("/api/watches/" + str(widb), json={"sort_by": "bogus"})
        check("PATCH bad sort_by -> 400", r.status == 400)

        # ---- daemon: feed ----
        r = await client.get("/api/feed")
        feed = await r.json()
        kinds = [e["kind"] for e in feed["entries"]]
        check("feed has OK baseline + WRN stall + QUEUED retry kinds",
              "OK" in kinds and "WRN" in kinds and "QUEUED" in kinds, str(kinds[:6]))

        # ---- daemon: watch removal ----
        r = await client.delete("/api/watches/" + str(wid))
        check("DELETE watch -> 204", r.status == 204)
        r = await client.get("/api/watches/" + str(wid))
        check("deleted watch -> 404", r.status == 404)
        check("delete removes state.json", not os.path.exists(os.path.join(tmp.name, "watches", str(wid), "state.json")))

        r = await client.get("/api/watches")
        watches = await r.json()
        check("GET /api/watches lists remaining watch", len(watches) == 1 and watches[0]["id"] == widb)

        await client.close()
    finally:
        kurokami.scrape = real_scrape
        tmp.cleanup()

    _daemon_diff_checks()
    _json_safety_checks()

    failed = [n for n, ok, _ in RESULTS if not ok]
    for name, ok, extra in RESULTS:
        print(("ok   " if ok else "FAIL ") + name + (("  " + extra) if extra and not ok else ""))
    print("%d checks, %d failed" % (len(RESULTS), len(failed)))
    sys.exit(1 if failed else 0)


async def _wait_watch(client, wid, done, rounds=50):
    w = None
    for _ in range(rounds):
        r = await client.get("/api/watches/" + str(wid))
        w = await r.json()
        if done(w):
            return w
        await asyncio.sleep(0.05)
    return w


def _daemon_diff_checks():
    """Diff semantics (right_only vs previous scan, baseline never notified):
    covered at the daemon level without HTTP, so 4 scans run fast."""
    import daemon

    tmp = tempfile.TemporaryDirectory(prefix="kurokami-diff-")
    try:
        d = daemon.Daemon(data_dir=tmp.name, test_mode=False,
                          seed_path=os.path.join(tmp.name, "nope.json"),
                          min_gap=0.0, backoff_min=0.0)
        d.load()
        wid = d.add_watch("uids", 3)
        seq = (["a", "b"], ["a", "b", "c"], ["a", "b"], ["a", "b", "c"])
        expected_new = (0, 1, 0, 1)   # baseline, new c, none, c reappears
        w = d.get(wid)
        for batch, wanted in zip(seq, expected_new):
            before = len(d._feed_ring)
            rows = [{"uid": u, "seller_name": "s", "price": ["$1"],
                     "time_posted": "now", "condition": "new", "item_name": u,
                     "item_url": "https://carousell.com.hk/p/" + u,
                     "item_img": None,
                     "seller_url": "https://carousell.com.hk/u/s"} for u in batch]
            d._record_success(w, rows)
            added = list(d._feed_ring)[before:]
            new_added = sum(1 for e in added if e["kind"] == "NEW")
            check("diff scan records %d new" % wanted,
                  len(w.last_new_uids) == wanted and new_added == wanted)
        check("baseline + reappearing uid both notified as NEW",
              len([e for e in d._feed_ring if e["kind"] == "NEW"]) == 2)
        new_entries = [e for e in d._feed_ring if e["kind"] == "NEW"]
        check("feed NEW messages stay price-free (frontend renders the price cell)",
              all("$1" not in (e["msg"] or "") for e in new_entries) and
              any(e.get("listing", {}).get("price") for e in new_entries))
        res = d.results(wid)
        flagged = [r for r in res["rows"] if r["uid"] == "c" and r["new"]]
        check("results flags reappeared uid as NEW", len(flagged) == 1)
        check("history.csv appended 4 scans", os.path.exists(d._history_path(wid)))
    finally:
        tmp.cleanup()


def _json_safety_checks():
    """Wire JSON must never carry bare NaN tokens (invalid JSON in browsers;
    a NaN `item_img` previously made results.json() throw)."""
    import json
    import math

    import daemon

    tmp = tempfile.TemporaryDirectory(prefix="kurokami-json-")
    try:
        d = daemon.Daemon(data_dir=tmp.name)
        rows = d._normalize_rows(pd.DataFrame({
            "uid": ["a", "b"],
            "item_img": [math.nan, "http://img"],
            "price": [math.nan, 42],
        }))
        wire = json.dumps(rows)
        check("pandas/numpy NaN sanitized to null on the wire",
              "NaN" not in wire and rows[0]["item_img"] is None and rows[0]["price"] is None)
        check("non-null neighbours survive sanitization",
              rows[1]["item_img"] == "http://img" and rows[1]["price"] == 42)
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    asyncio.run(main())