/* kurokami - live frontend for the daemon.
   Fetches the watch/feed/status API and renders the surfaces designed in the
   impeccable pass (the static design mock). Decorative-only mock wiring is
   replaced with real endpoints; pages fail soft with a quiet empty state. */

(function () {
  "use strict";

  var FLOOR_MIN = 10;

  function pad(n) { return String(n).padStart(2, "0"); }

  function setClock() {
    var el = document.getElementById("clock");
    if (!el) return;
    var d = new Date();
    el.textContent = pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
  }

  function markNav() {
    var path = location.pathname.split("/").pop() || "index.html";
    var page = path === "results.html" ? "index" : path.replace(".html", "");
    var links = document.querySelectorAll("nav a[data-nav]");
    for (var i = 0; i < links.length; i++) {
      links[i].setAttribute("data-active", links[i].getAttribute("data-nav") === page ? "true" : "false");
    }
  }

  function timeHm(epoch) {
    if (!epoch) return "--";
    var d = new Date(epoch * 1000);
    return pad(d.getHours()) + ":" + pad(d.getMinutes());
  }

  function timeHms(epoch) {
    if (!epoch) return "--:--:--";
    var d = new Date(epoch * 1000);
    return pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
  }

  function readJSON(r) {
    return r.json().catch(function () {
      return r.text().then(function (t) {
        throw new Error("non-JSON response: " + ((t && t.trim().slice(0, 80)) || ("HTTP " + r.status)));
      });
    });
  }

  function getJSON(url) {
    return fetch(url).then(function (r) {
      return readJSON(r).then(function (d) {
        if (!r.ok) throw new Error((d && d.error) || ("HTTP " + r.status));
        return d;
      });
    });
  }

  function postJSON(url, body) {
    return fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {})
    }).then(function (r) {
      if (!r.ok) {
        return readJSON(r).then(function (d) { throw new Error((d && d.error) || ("HTTP " + r.status)); });
      }
      return r.status === 204 ? null : r.json();
    });
  }

  function el(tag, cls, text) {
    var node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function money(row) {
    var p = row.price;
    if (Array.isArray(p)) p = p[0];
    return p ? String(p) : "--";
  }

  function watchLink(id, label) {
    var a = el("a", "w", label === undefined ? ("watch w" + id) : label);
    a.href = "results.html?watch=" + id;
    return a;
  }

  function openLink(url, label) {
    var a = el("a", "out-link", label || ("open \u2197"));
    a.href = url || "results.html";
    a.target = "_blank";
    a.rel = "noopener";
    return a;
  }

  function emptyRow(text, colSpan) {
    var row = el("div", "roster-row row-empty");
    var span = el("span", "faint", text);
    span.style.gridColumn = "1 / -1";
    row.appendChild(span);
    return row;
  }

  /* ------------------------------------------------------------------ */
  /* roster (index.html)                                                 */
  /* ------------------------------------------------------------------ */

  function stateCell(w) {
    var cell = el("span", "cell-state");
    cell.dataset.cell = "state";
    if (w.status === "stalled") {
      cell.appendChild(el("span", "pip pip-stalled", null));
      cell.appendChild(el("span", "badge badge-stalled", "STALLED"));
      var btn = el("button", "retry", "retry now");
      btn.type = "button";
      btn.addEventListener("click", function () {
        btn.disabled = true;
        btn.textContent = "queued \u2026";
        postJSON("/api/watches/" + w.id + "/retry")["catch"](function () {
          btn.disabled = false;
          btn.textContent = "retry now";
        });
      });
      cell.appendChild(btn);
    } else if (w.new_count > 0) {
      cell.appendChild(el("span", "pip pip-live", null));
      cell.appendChild(el("span", "badge badge-new", "NEW +" + w.new_count));
    } else if (!w.baseline_done) {
      cell.appendChild(el("span", "pip pip-idle", null));
      cell.appendChild(el("span", "faint", "first scan"));
    } else if (w.status === "queued") {
      cell.appendChild(el("span", "pip pip-idle", null));
      cell.appendChild(el("span", "badge badge-queued", "QUEUED"));
    } else if (w.status === "running") {
      cell.appendChild(el("span", "pip pip-idle", null));
      cell.appendChild(el("span", "faint", "running"));
    } else {
      cell.appendChild(el("span", "pip pip-live", null));
      cell.appendChild(el("span", "faint", "live"));
    }
    return cell;
  }

  function rosterRow(w) {
    var row = el("div", w.status === "stalled" ? "roster-row row-stalled" : "roster-row");

    var id = el("span", "cell-id num", "w" + w.id);
    id.dataset.cell = "watch";
    row.appendChild(id);

    var query = el("span", "cell-query num");
    query.dataset.cell = "query";
    var qa = el("a", null, w.item);
    qa.href = "results.html?watch=" + w.id;
    query.appendChild(qa);
    row.appendChild(query);

    var int = el("span", "num", w.continuous ? (w.interval_min + "m") : "1x");
    int.dataset.cell = "interval";
    row.appendChild(int);

    var last = el("span", "num", timeHm(w.last_scan));
    last.dataset.cell = "last scan";
    row.appendChild(last);

    var next = el("span", "num", (w.status === "stalled" || w.status === "queued" || w.status === "running")
      ? w.status : timeHm(w.next_scan));
    if (w.status === "stalled" || w.status === "queued" || w.status === "running") next.classList.add("dim");
    next.dataset.cell = "next scan";
    row.appendChild(next);

    var count = el("span", "num", w.row_count);
    count.dataset.cell = "count";
    row.appendChild(count);

    row.appendChild(stateCell(w));
    return row;
  }

  function renderRoster(watches) {
    var roster = document.getElementById("roster");
    if (!roster) return;
    roster.querySelectorAll(".roster-row").forEach ? null : null;
    var existing = roster.querySelectorAll(".roster-row");
    for (var i = 0; i < existing.length; i++) existing[i].remove();
    if (!watches.length) {
      roster.appendChild(emptyRow("\u2014 no watches yet \u2014 add one from settings", 7));
      return;
    }
    var sorted = watches.slice().sort(function (a, b) {
      if (a.status === "stalled" !== (b.status === "stalled")) return a.status === "stalled" ? -1 : 1;
      return (b.last_scan || 0) - (a.last_scan || 0);
    });
    for (var j = 0; j < sorted.length; j++) roster.appendChild(rosterRow(sorted[j]));
  }

  function renderPoliteness(watches) {
    var host = document.getElementById("politeness");
    if (!host || !host.parentNode) return;
    host.innerHTML = "";
    var below = 0;
    for (var i = 0; i < watches.length; i++) {
      var w = watches[i];
      if (w.below_floor || (w.continuous && w.interval_min < FLOOR_MIN)) {
        below++;
        var note = el("div", "politeness", null);
        note.setAttribute("data-visible", "true");
        note.setAttribute("role", "note");
        note.textContent = "watch w" + w.id + " runs at " + w.interval_min +
          "m, below the ~" + FLOOR_MIN + "m politeness floor. Permitted, never blocked; the banner persists while the interval stays there.";
        host.appendChild(note);
      }
    }
    if (!below) host.setAttribute("data-visible", "false");
  }

  function renderStatusline(st) {
    var main = document.getElementById("sline-main");
    var aux = document.getElementById("sline-aux");
    if (main) {
      main.textContent = "-- politeness queue: " + (st.in_flight ? "1 scrape in flight" : "idle") +
        " \u00b7 " + st.pending + " pending \u00b7 next scrape \u2265" + Math.round(st.min_gap) + "s --";
    }
    if (aux) {
      aux.textContent = st.test_mode
        ? "-- test mode: snapshot scraping (utils/soup.pkl), no live fetches --"
        : "-- last scrape " + (st.last_scrape_ago !== null ? (st.last_scrape_ago + "s ago") : "never") + " \u00b7 floor ~10m --";
    }
  }

  function initRoster() {
    var rollup = document.getElementById("rollup");
    Promise.all([getJSON("/api/watches"), getJSON("/api/status")]).then(function (pair) {
      var watches = pair[0];
      var st = pair[1];
      if (rollup) {
        var stalled = watches.filter(function (w) { return w.status === "stalled"; }).length;
        rollup.textContent = watches.length + " watch" + (watches.length === 1 ? "" : "es") +
          " \u00b7 " + stalled + " stalled" + " \u00b7 throttle " + Math.round(st.min_gap) + "s";
      }
      renderRoster(watches);
      renderPoliteness(watches);
      renderStatusline(st);
    })["catch"](function (err) {
      if (rollup) rollup.textContent = "daemon unreachable";
      renderStatusline({ test_mode: false, in_flight: false, pending: 0, min_gap: 15, last_scrape_ago: null });
      var roster = document.getElementById("roster");
      if (roster) roster.appendChild(emptyRow("\u2014 daemon unreachable (" + err.message + ") \u2014", 7));
    });
  }

  /* ------------------------------------------------------------------ */
  /* results (results.html?watch=ID)                                     */
  /* ------------------------------------------------------------------ */

  function thumbFor(row) {
    if (row.item_img) {
      var img = new Image();
      img.className = "thumb-img";
      img.loading = "lazy";
      img.alt = "";
      img.src = row.item_img;
      return img;
    }
    var plate = el("span", "thumb", "img_placeholder.png\n\u00d7");
    plate.setAttribute("aria-hidden", "true");
    return plate;
  }

  function listingRow(row) {
    var tr = el("div", "listing-row");
    tr.appendChild(thumbFor(row));
    var body = el("div");
    var title = el("div", "listing-title", null);
    if (row.new) {
      title.appendChild(el("span", "badge badge-new", "NEW"));
      title.appendChild(document.createTextNode("\u00a0"));
      title.appendChild(el("span", null, row.item_name));
    } else {
      title.textContent = row.item_name;
    }
    body.appendChild(title);
    var sub = el("div", "listing-sub num",
      [row.condition && row.condition !== "N/A" ? row.condition : null,
       "uid " + row.uid].filter(Boolean).join(" \u00b7 "));
    body.appendChild(sub);
    tr.appendChild(body);
    tr.appendChild(el("div", "listing-price num", money(row)));
    tr.appendChild(openLink(row.item_url));
    return tr;
  }

  var sortMode = "new";
  var resultsData = null;

  function priceVal(s) {
    if (s === null || s === undefined || s === "") return Infinity;
    if (typeof s !== "string") return isNaN(s) ? Infinity : s;
    var v = parseFloat(s.replace(/[^0-9.]/g, ""));
    if (!isNaN(v)) return v;
    return /free/i.test(s) ? 0 : Infinity;
  }

  function sortRows(rows, mode) {
    rows = rows.slice();
    if (mode === "price_asc") {
      rows.sort(function (a, b) { return priceVal(money(a)) - priceVal(money(b)); });
    } else if (mode === "price_desc") {
      rows.sort(function (a, b) { return priceVal(money(b)) - priceVal(money(a)); });
    } else {
      rows.sort(function (a, b) {
        if (a.new !== b.new) return a.new ? -1 : 1;
        return (b.seen || 0) - (a.seen || 0);
      });
    }
    return rows;
  }

  function scanLabel(w, mode) {
    var base = w.new_count + " rows new since last scan";
    if (mode === "price_asc") return base + " \u00b7 sorted: price low \u2192 high";
    if (mode === "price_desc") return base + " \u00b7 sorted: price high \u2192 low";
    return base + ", shown first";
  }

  function renderResultList() {
    var list = document.getElementById("listing");
    if (!list || !resultsData) return;
    while (list.firstChild) list.removeChild(list.firstChild);
    var w = resultsData.watch;
    var label = document.getElementById("scan-label");
    if (label) label.textContent = scanLabel(w, sortMode);
    var rows = sortRows(resultsData.rows, sortMode);
    if (!rows.length) {
      list.appendChild(emptyRow("\u2014 no rows yet \u2014 the first scan is the baseline; new listings land here --", 3));
    } else {
      for (var i = 0; i < rows.length; i++) list.appendChild(listingRow(rows[i]));
    }
    var s1 = document.getElementById("sline-main");
    if (s1) s1.textContent = "-- " + rows.length + " rows \u00b7 " + w.new_count +
      " new since last scan \u00b7 baseline never notified --";
  }

  function initSortMenu() {
    var host = document.getElementById("sort-bar");
    if (!host) return;
    var buttons = host.querySelectorAll(".sort-btn");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].addEventListener("click", function () {
        sortMode = this.getAttribute("data-sort");
        for (var j = 0; j < buttons.length; j++) {
          buttons[j].classList.toggle("is-active", buttons[j] === this);
        }
        renderResultList();
      });
    }
  }

  function initResults() {
    var params = new URLSearchParams(location.search);
    var id = params.get("watch");
    initSortMenu();
    var btn = document.getElementById("rescan");
    if (btn && id) {
      btn.addEventListener("click", function () {
        btn.disabled = true;
        btn.textContent = "queued \u2026";
        postJSON("/api/watches/" + id + "/rescan").then(function () {
          setMeta("meta-next", "next scan", "queued");
          pollRescan(id, btn, 60);
        })["catch"](function () {
          btn.disabled = false;
          btn.textContent = "[rescan]";
        });
      });
    }
    renderResults(id);
  }

  function renderResults(id) {
    var title = document.querySelector(".page-head h1");
    if (!id) {
      if (title) title.textContent = "cat watch::?";
      var box = document.getElementById("listing");
      if (box) box.appendChild(emptyRow("\u2014 no watch selected \u2014 pick one from the roster", 3));
      return;
    }
    var list = document.getElementById("listing");
    if (list) while (list.firstChild) list.removeChild(list.firstChild);
    getJSON("/api/watches/" + id + "/results").then(function (data) {
      var w = data.watch;
      resultsData = data;
      if (title) title.textContent = "cat watch::w" + w.id;
      var wl = document.getElementById("watch-link");
      if (wl) {
        var a = watchLink(w.id, "watch w" + w.id);
        a.className = "w";
        wl.textContent = "";
        wl.appendChild(a);
      }
      setMeta("meta-query", "query", w.item);
      setMeta("meta-interval", "interval", w.continuous ? (w.interval_min + "m") : "one-shot");
      setMeta("meta-last", "last scan", timeHm(w.last_scan));
      setMeta("meta-next", "next scan", w.status === "stalled" ? "stalled" : timeHm(w.next_scan));
      renderResultList();
      getJSON("/api/status").then(function (st) {
        var aux = document.getElementById("sline-aux");
        if (aux && st.test_mode) aux.textContent = "-- test mode: snapshot scraping, no live fetches --";
      });
    })["catch"](function (err) {
      if (title) title.textContent = "cat watch::w" + id;
      if (list) {
        var label = err && err.message === "Unknown watch"
          ? "\u2014 unknown or deleted watch (" + err.message + ") \u2014"
          : "\u2014 could not load w" + id + " (" + (err && err.message) + ") \u2014";
        list.appendChild(emptyRow(label, 3));
      }
    });
  }

  function pollRescan(id, btn, rounds) {
    getJSON("/api/watches/" + id).then(function (w) {
      if (w.status === "queued" || w.status === "running") {
        if (rounds > 1) setTimeout(function () { pollRescan(id, btn, rounds - 1); }, 2000);
        return;
      }
      btn.disabled = false;
      btn.textContent = "[rescan]";
      renderResults(id);
    })["catch"](function () {
      btn.disabled = false;
      btn.textContent = "[rescan]";
    });
  }

  function setMeta(id, label, value) {
    var node = document.getElementById(id);
    if (!node) return;
    node.textContent = "";
    var b = el("b", null, label);
    node.appendChild(b);
    node.appendChild(document.createTextNode("\u00a0 " + value));
  }

  /* ------------------------------------------------------------------ */
  /* feed (feed.html)                                                    */
  /* ------------------------------------------------------------------ */

  function feedEntry(entry) {
    var line = el("div", "feed-line");
    line.appendChild(el("span", "feed-time num", timeHms(entry.t)));
    line.appendChild(el("span", "feed-kind kind-" + entry.kind.toLowerCase(), entry.kind));
    var body = el("span");
    var listing = entry.listing;
    var priceText = listing ? money(listing) : "";
    if (listing && priceText && priceText !== "--") {
      body.appendChild(el("span", "feed-listing-price num", priceText));
      body.appendChild(document.createTextNode(" \u00b7 "));
    }
    body.appendChild(document.createTextNode(entry.msg));
    if (entry.watch) {
      body.appendChild(document.createTextNode(" \u00b7 "));
      body.appendChild(watchLink(entry.watch, "watch w" + entry.watch));
    }
    if (entry.listing && entry.listing.item_url) {
      body.appendChild(document.createTextNode(" \u00b7 "));
      var a = openLink(entry.listing.item_url, "open");
      a.className = "listing";
      body.appendChild(a);
    }
    line.appendChild(body);
    return line;
  }

  function initFeed() {
    getJSON("/api/feed").then(function (data) {
      var entries = data.entries || [];
      var feed = document.getElementById("feed");
      var roll = document.getElementById("rollup");
      if (feed) {
        if (!entries.length) {
          feed.appendChild(emptyRow("\u2014 feed is quiet \u2014 new listings and daemon events land here --", 3));
        } else {
          for (var i = 0; i < entries.length; i++) feed.appendChild(feedEntry(entries[i]));
        }
      }
      if (roll) roll.textContent = entries.length + " entries \u00b7 daemon events \u00b7 newest first";
      var s1 = document.getElementById("sline-main");
      if (s1) s1.textContent = "-- " + entries.length + " lines \u00b7 unbounded, append-only --";
      getJSON("/api/status").then(function (st) {
        var aux = document.getElementById("sline-aux");
        if (aux && st.test_mode) aux.textContent = "-- test mode: snapshot scraping, no live fetches --";
      });
    })["catch"](function (err) {
      var feed = document.getElementById("feed");
      if (feed) feed.appendChild(emptyRow("\u2014 daemon unreachable (" + err.message + ") \u2014", 3));
    });
  }

  /* ------------------------------------------------------------------ */
  /* settings (settings.html)                                            */
  /* ------------------------------------------------------------------ */

  function initSettings() {
    var form = document.getElementById("form");
    var note = document.getElementById("form-note");
    var save = document.getElementById("save");
    if (!form || !note || !save) return;

    var inputs = {
      item: document.getElementById("item"),
      count: document.getElementById("count"),
      pl: document.getElementById("pl"),
      ph: document.getElementById("ph"),
      int: document.getElementById("int"),
      cont: document.getElementById("cont")
    };

    function validate() {
      var msg = "";
      var item = inputs.item ? inputs.item.value.trim() : "";
      if (!item) msg = "-- item query is empty --";
      var c = inputs.count ? parseInt(inputs.count.value, 10) : 0;
      if (!msg && (!inputs.count.value || isNaN(c) || c < 1)) msg = "-- count must be >= 1 --";
      var intv = inputs.int ? parseInt(inputs.int.value, 10) : 0;
      if (!msg && (!inputs.int.value || isNaN(intv) || intv < 1)) msg = "-- interval must be >= 1 minute --";
      if (!msg && inputs.cont) {
        var v = String(inputs.cont.value).trim().toLowerCase();
        if (v !== "yes" && v !== "no") msg = "-- continuous must be yes or no --";
      }
      if (!msg && inputs.pl && inputs.ph && inputs.pl.value !== "" && inputs.ph.value !== "") {
        var lo = parseFloat(inputs.pl.value);
        var hi = parseFloat(inputs.ph.value);
        if (!isNaN(lo) && !isNaN(hi) && lo > hi) msg = "-- price low exceeds price high --";
      }
      note.textContent = msg;
      note.hidden = msg === "";
      return msg === "";
    }

    for (var key in inputs) {
      if (inputs[key]) inputs[key].addEventListener("input", validate);
    }
    validate();

    wirePoliteness(inputs.int, document.getElementById("politeness-note"));

    save.addEventListener("click", function () {
      if (!validate()) return;
      var payload = {
        item: inputs.item.value.trim(),
        count: parseInt(inputs.count.value, 10),
        price_low: inputs.pl.value === "" ? null : parseInt(inputs.pl.value, 10),
        price_high: inputs.ph.value === "" ? null : parseInt(inputs.ph.value, 10),
        interval_min: parseInt(inputs.int.value, 10),
        continuous: String(inputs.cont.value).trim().toLowerCase() === "yes"
      };
      save.disabled = true;
      var old = save.textContent;
      save.textContent = "[saving \u2026]";
      postJSON("/api/watches", payload).then(function (w) {
        save.textContent = "[saved \u2026 watch w" + w.id + " created]";
        setTimeout(function () { location.href = "results.html?watch=" + w.id; }, 900);
      })["catch"](function (err) {
        save.disabled = false;
        save.textContent = old;
        note.textContent = "-- " + err.message + " --";
        note.hidden = false;
      });
    });

    getJSON("/api/watches").then(function (watches) {
      var list = document.getElementById("watch-list");
      if (!list) return;
      if (!watches.length) {
        var li = el("li", null, "\u2014 no watches yet \u2014");
        li.classList.add("faint");
        list.appendChild(li);
        return;
      }
      for (var i = 0; i < watches.length; i++) {
        var w = watches[i];
        var li2 = el("li");
        var a = el("a", null, "w" + w.id);
        a.href = "results.html?watch=" + w.id;
        li2.appendChild(a);
        li2.appendChild(document.createTextNode(" " + w.item));
        var st = el("span", "w-state",
          (w.continuous ? ("continuous \u00b7 " + w.interval_min + "m") : "one-shot") +
          (w.status === "stalled" ? " \u2014 stalled \u2014 retry needed" : "") +
          (w.below_floor ? " \u00b7 below floor" : ""));
        li2.appendChild(st);
        list.appendChild(li2);
      }
    })["catch"](function () {});
  }

  function wirePoliteness(input, note) {
    if (!input || !note) return;
    function update() {
      var v = parseInt(input.value, 10);
      var below = !isNaN(v) && v > 0 && v < FLOOR_MIN;
      note.hidden = !below;
    }
    input.addEventListener("input", update);
    update();
  }

  /* ------------------------------------------------------------------ */

  setClock();
  setInterval(setClock, 1000);
  markNav();

  var page = document.body.getAttribute("data-page");
  if (page === "index") initRoster();
  else if (page === "results") initResults();
  else if (page === "feed") initFeed();
  else if (page === "settings") initSettings();
})();