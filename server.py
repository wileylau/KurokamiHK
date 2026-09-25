'''Kurokami daemon server: aiohttp app for the live-monitor GUI.

App layer that owns sockets/HTTP only: it validates browser input, hands
everything to the Daemon (scheduling, storage, feed), and never builds URLs
from user data or mirrors the data model (SSRF note: the browser never hands
the server a URL; item is a plain keyword quoted into the Carousell URL
server-side only). Bound to 127.0.0.1.

The Daemon owns the single politeness scan queue. Search jobs (the shell's
original one-off endpoint) and scheduled/scanned watches both ride that same
queue, so exactly one scrape is ever in flight, spaced >= min_gap seconds.

Usage:  python server.py [--host 127.0.0.1] [--port 8080]
Dry-run without firing a live scrape / Chrome session (reads utils/soup.pkl):
  KUROKAMI_SERVER_TEST=1 python server.py
'''

import argparse
import asyncio
import json
import os
import time
import uuid

import aiohttp.web as web

import daemon
import kurokami
from kurokami import KurokamiError

DATA_DIR = daemon.DATA_DIR
DEFAULT_QUERIES_PATH = "queries.json"
MAX_ITEM_LEN = 200
MAX_COUNT = 1000
DEFAULT_INTERVAL_MIN = daemon.DEFAULT_INTERVAL_MIN

_jobs = {}  # adhoc search job id -> Job (in-memory only, shell seam)


class Job:
    __slots__ = ("id", "item", "count", "price_low", "price_high",
                 "status", "started", "finished", "error", "items")

    def __init__(self, item, count, price_low, price_high):
        self.id = uuid.uuid4().hex
        self.item = item
        self.count = count
        self.price_low = price_low
        self.price_high = price_high
        self.status = "pending"  # pending -> running -> done | failed
        self.started = None
        self.finished = None
        self.error = None
        self.items = None

    def to_dict(self):
        data = {
            "id": self.id,
            "item": self.item,
            "count": self.count,
            "price_low": self.price_low,
            "price_high": self.price_high,
            "status": self.status,
            "started": self.started,
            "finished": self.finished,
            "error": self.error,
        }
        if self.status == "done":
            data["items"] = self.items
        return data


def _bad_request(message):
    return web.json_response({"error": message}, status=400)


def _validate_search(body):
    """Validate browser-supplied search input (SSRF note)."""
    if not isinstance(body, dict):
        return _bad_request("Expected a JSON object body"), None

    item = body.get("item")
    if not isinstance(item, str) or not item.strip():
        return _bad_request('"item" is required'), None
    item = item.strip()
    if len(item) > MAX_ITEM_LEN:
        return _bad_request('"item" too long (max %d chars)' % MAX_ITEM_LEN), None

    try:
        count = int(body.get("count", 25))
    except (TypeError, ValueError):
        return _bad_request('"count" must be an integer'), None
    if not 1 <= count <= MAX_COUNT:
        return _bad_request('"count" must be between 1 and %d' % MAX_COUNT), None

    price_low = body.get("price_low")
    price_high = body.get("price_high")
    for name in ("price_low", "price_high"):
        val = body.get(name)
        if val is None:
            continue
        try:
            val = int(val)
        except (TypeError, ValueError):
            return _bad_request('"%s" must be an integer' % name), None
        if val < 0:
            return _bad_request('"%s" must be >= 0' % name), None
        if name == "price_low":
            price_low = val
        else:
            price_high = val
    if price_low is not None and price_high is not None and price_low > price_high:
        return _bad_request('"price_low" cannot exceed "price_high"'), None

    return None, (item, count, price_low, price_high)


def _validate_watch(body):
    """Validate a watch payload: search fields plus interval + continuous."""
    error, payload = _validate_search(body)
    if error is not None:
        return error, None
    item, count, price_low, price_high = payload

    try:
        interval = int(body.get("interval_min", DEFAULT_INTERVAL_MIN))
    except (TypeError, ValueError):
        return _bad_request('"interval_min" must be an integer'), None
    if interval < 1:
        return _bad_request('"interval_min" must be >= 1'), None

    continuous_raw = body.get("continuous")
    if continuous_raw in (None, False, "no", "false", "0", ""):
        continuous = False
    elif continuous_raw in (True, "yes", "true", "1"):
        continuous = True
    else:
        return _bad_request('"continuous" must be yes|no'), None

    try:
        sort_by = kurokami.normalize_sort_name(body.get("sort_by"))
    except ValueError as exc:
        return _bad_request(str(exc)), None

    return None, (item, count, price_low, price_high, interval, continuous, sort_by)


async def _run_job(job, app):
    """Run an adhoc scrape through the daemon's politeness queue."""
    d = app["daemon"]
    job.started = time.time()
    job.status = "running"
    try:
        df = await kurokami.scrape(
            job.item, count=job.count,
            price_low=job.price_low, price_high=job.price_high,
            test=d.test_mode,
        )
        job.items = df.values.tolist()
        job.status = "done"
    except KurokamiError as exc:
        job.status = "failed"
        job.error = str(exc)
    except Exception as exc:
        job.status = "failed"
        job.error = "%s: %s" % (type(exc).__name__, exc)
    finally:
        job.finished = time.time()


async def handle_search(request):
    """POST /api/search  {item, count, price_low, price_high} -> 202 job."""
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return _bad_request("Body must be valid JSON")
    error, payload = _validate_search(body)
    if error is not None:
        return error
    item, count, price_low, price_high = payload
    job = Job(item, count, price_low, price_high)
    _jobs[job.id] = job

    async def _adhoc():
        await _run_job(job, request.app)

    request.app["daemon"].submit_adhoc(_adhoc())
    return web.json_response(job.to_dict(), status=202)


async def handle_job(request):
    """GET /api/search/{job_id} -> pollable job state (200/404)."""
    job = _jobs.get(request.match_info["job_id"])
    if job is None:
        return web.json_response({"error": "Unknown job id"}, status=404)
    return web.json_response(job.to_dict())


async def handle_jobs(request):
    """GET /api/jobs -> all adhoc jobs (in-memory only)."""
    return web.json_response([job.to_dict() for job in _jobs.values()])


async def handle_config(request):
    """GET /api/config -> read-once queries seed + test mode (shell seam)."""
    d = request.app["daemon"]
    return web.json_response({
        "queries": [{"id": w.id, "item": w.item, "count": w.count,
                     "price_low": w.price_low, "price_high": w.price_high,
                     "sort_by": w.sort_by}
                    for w in d.watches()],
        "test_mode": d.test_mode,
    })


async def handle_status(request):
    """GET /api/status -> daemon politeness + health surface."""
    return web.json_response(request.app["daemon"].status())


async def handle_watches(request):
    """GET /api/watches -> all watches with derived roster state."""
    d = request.app["daemon"]
    return web.json_response([d.watch_dict(w) for w in d.watches()])


async def handle_create_watch(request):
    """POST /api/watches -> create a watch; baseline scan is queued (202)."""
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return _bad_request("Body must be valid JSON")
    error, payload = _validate_watch(body)
    if error is not None:
        return error
    item, count, price_low, price_high, interval, continuous, sort_by = payload
    d = request.app["daemon"]
    wid = d.add_watch(item, count, price_low, price_high, interval, continuous,
                      sort_by)
    return web.json_response(d.watch_dict(d.get(wid)), status=202)


async def handle_watch(request):
    """GET /api/watches/{id} -> one watch."""
    d = request.app["daemon"]
    w = d.get(int(request.match_info["id"]))
    if w is None:
        return web.json_response({"error": "Unknown watch"}, status=404)
    return web.json_response(d.watch_dict(w))


async def handle_delete_watch(request):
    """DELETE /api/watches/{id} -> 204 (history + state removed)."""
    d = request.app["daemon"]
    if not d.remove_watch(int(request.match_info["id"])):
        return web.json_response({"error": "Unknown watch"}, status=404)
    return web.json_response({}, status=204)


async def handle_update_watch(request):
    """PATCH /api/watches/{id} -> edit query/schedule; missing fields keep
    their current values. Changing the query resets the diff baseline and
    queues a fresh scan."""
    d = request.app["daemon"]
    try:
        wid = int(request.match_info["id"])
    except ValueError:
        return _bad_request("watch id must be an integer")
    w = d.get(wid)
    if w is None:
        return web.json_response({"error": "Unknown watch"}, status=404)
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return _bad_request("Body must be valid JSON")
    if not isinstance(body, dict):
        return _bad_request("Expected a JSON object body")
    merged = {
        "item": body.get("item", w.item),
        "count": body.get("count", w.count),
        "price_low": body.get("price_low", w.price_low),
        "price_high": body.get("price_high", w.price_high),
        "interval_min": body.get("interval_min", w.interval_min),
        "continuous": body.get("continuous", w.continuous),
        "sort_by": body.get("sort_by", w.sort_by),
    }
    error, payload = _validate_watch(merged)
    if error is not None:
        return error
    item, count, price_low, price_high, interval, continuous, sort_by = payload
    updated = d.update_watch(wid, item, count, price_low, price_high,
                             interval, continuous, sort_by)
    return web.json_response(d.watch_dict(updated), status=202)


async def handle_retry_watch(request):
    """POST /api/watches/{id}/retry -> requeue a stalled watch (202)."""
    d = request.app["daemon"]
    if not d.retry(int(request.match_info["id"])):
        return web.json_response({"error": "Watch not found or not stalled"},
                                 status=409)
    return web.json_response(d.watch_dict(d.get(int(request.match_info["id"]))),
                             status=202)


async def handle_rescan_watch(request):
    """POST /api/watches/{id}/rescan -> requeue the watch now (202/404/409)."""
    d = request.app["daemon"]
    try:
        wid = int(request.match_info["id"])
    except ValueError:
        return _bad_request("watch id must be an integer")
    if d.get(wid) is None:
        return web.json_response({"error": "Unknown watch"}, status=404)
    if not d.trigger(wid):
        return web.json_response({"error": "Watch already queued or running"},
                                 status=409)
    return web.json_response(d.watch_dict(d.get(wid)), status=202)


async def handle_results(request):
    """GET /api/watches/{id}/results -> latest rows with NEW flags."""
    d = request.app["daemon"]
    try:
        wid = int(request.match_info["id"])
    except ValueError:
        return _bad_request("watch id must be an integer")
    w = d.get(wid)
    result = d.results(wid, fallback_limit=w.count if w else None)
    if result is None:
        return web.json_response({"error": "Unknown watch"}, status=404)
    return web.json_response(result)


async def handle_feed(request):
    """GET /api/feed -> feed entries, newest first."""
    d = request.app["daemon"]
    return web.json_response({"entries": d.feed()})


_PAGES = {
    "/": "index.html",
    "/index.html": "index.html",
    "/results.html": "results.html",
    "/feed.html": "feed.html",
    "/settings.html": "settings.html",
}


async def handle_page(request):
    return web.FileResponse(os.path.join("static", _PAGES[request.path]))


def make_app(test_mode=None, queries_path=DEFAULT_QUERIES_PATH,
             data_dir=DATA_DIR, min_gap=None, backoff_min=None):
    if test_mode is None:
        test_mode = os.environ.get("KUROKAMI_SERVER_TEST") == "1"
    if min_gap is None:
        min_gap = float(os.environ.get("KUROKAMI_SERVER_MIN_GAP",
                                        daemon.DEFAULT_MIN_SCRAPE_GAP))
    if backoff_min is None:
        backoff_min = float(os.environ.get("KUROKAMI_SERVER_BACKOFF",
                                           daemon.DEFAULT_BACKOFF_MIN))

    d = daemon.Daemon(data_dir=data_dir, test_mode=test_mode,
                      seed_path=queries_path, min_gap=min_gap,
                      backoff_min=backoff_min)
    d.load()

    app = web.Application()
    app["daemon"] = d
    app["test_mode"] = d.test_mode

    app.on_startup.append(_start_scheduler)
    app.on_cleanup.append(_stop_scheduler)

    app.router.add_get("/", handle_page)
    app.router.add_get("/index.html", handle_page)
    app.router.add_get("/results.html", handle_page)
    app.router.add_get("/feed.html", handle_page)
    app.router.add_get("/settings.html", handle_page)
    app.router.add_static("/static", "static", name="static")
    app.router.add_post("/api/search", handle_search)
    app.router.add_get("/api/search/{job_id}", handle_job)
    app.router.add_get("/api/jobs", handle_jobs)
    app.router.add_get("/api/config", handle_config)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/watches", handle_watches)
    app.router.add_post("/api/watches", handle_create_watch)
    app.router.add_get("/api/watches/{id}", handle_watch)
    app.router.add_delete("/api/watches/{id}", handle_delete_watch)
    app.router.add_patch("/api/watches/{id}", handle_update_watch)
    app.router.add_post("/api/watches/{id}/retry", handle_retry_watch)
    app.router.add_post("/api/watches/{id}/rescan", handle_rescan_watch)
    app.router.add_get("/api/watches/{id}/results", handle_results)
    app.router.add_get("/api/feed", handle_feed)
    return app


async def _start_scheduler(app):
    app["_scheduler"] = asyncio.ensure_future(app["daemon"].run())


async def _stop_scheduler(app):
    task = app.get("_scheduler")
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def main(args=None):
    parser = argparse.ArgumentParser(description="Kurokami live-monitor server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--data-dir", default=DATA_DIR)
    ns = parser.parse_args(args)
    app = make_app(data_dir=ns.data_dir)
    print("Kurokami server on http://%s:%d  (test mode: %s, data: %s)"
          % (ns.host, ns.port, app["test_mode"], ns.data_dir))
    web.run_app(app, host=ns.host, port=ns.port)


if __name__ == "__main__":
    main()