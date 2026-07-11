"""
═══════════════════════════════════════════════════════════════════════
XRPRadar — Iteration 3
Version 60 — Major data-source repair + 29-item bug fix pass
Red Rio Ventures, LLC
═══════════════════════════════════════════════════════════════════════

Freshly written. No code copied from Iteration 1.

Version 4 changes:
  1. Status rectangles return to the compact horizontal layout (label on
     the left, value on the right) like the prior version.
  2. Fear & Greed is a horizontal color-coded line with a ball that shows
     the number, the ball tinted by zone color.
  3. XRP price is red or green based on 24h movement.
  4. Active Sources value uses the same blue as the headers.
  5. The three little label icons are larger.

Live data (background thread, refreshed every 60s):
  • XRP / USD      — CoinCap
  • Fear & Greed   — alternative.me
  • Active Sources — count of live data sources connected
ATH, CoinGecko, and access-limited feeds remain permanently excluded.
═══════════════════════════════════════════════════════════════════════
"""

import os
import time
import threading
from datetime import datetime, timezone, timedelta
import html
import json
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime
try:
    from zoneinfo import ZoneInfo
    CENTRAL = ZoneInfo("America/Chicago")
except Exception:
    CENTRAL = timezone(timedelta(hours=-6))  # CST fallback

import requests
from flask import Flask, Response, jsonify

# ─────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────
APP_VERSION = "99"
APP_NAME    = "XRPRadar"
TAGLINE     = "The NEW XRP Intelligence Standard"
COPYRIGHT   = "\u00A9\uFE0F Copyright 2026 Red Rio Ventures, LLC. All rights reserved globally."
BOOT_TIME   = datetime.now(timezone.utc)

app = Flask(__name__)

# ─────────────────────────────────────────────────────────────────────
# LIVE MARKET DATA (background refresh; page reads the cache)
# ─────────────────────────────────────────────────────────────────────
MARKET = {
    "xrp_price": None, "xrp_chg": None,
    "fng": None, "fng_label": None,
    "mcap": None, "vol24": None, "rank": None, "h24": None, "l24": None, "xrpbtc": None,
    "fng_history": [], "funding": None,
    "perf_1w": None, "perf_30d": None, "perf_90d": None, "perf_6m": None,
    "fx": {},
    "competitors": {},
    "ad_7d_delta": None, "ad_30d_delta": None,
    "corr_btc": None, "corr_eth": None,
    "ob_bids": [], "ob_asks": [], "ob_bid_total": None, "ob_ask_total": None,
    "sources_active": 0, "sources_total": 3,
    "updated": None,
    # technicals (Binance klines)
    "rsi_1h": None, "rsi_1d": None,
    "w52_low": None, "w52_high": None,
    "tm_1y": None, "tm_1m": None,
    "sr_support": None, "sr_resistance": None,
}


def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 1)


def _coinbase_candles(product_id, granularity=86400, limit=300):
    """Public Coinbase Exchange candles. Returns oldest->newest as
    [time, low, high, open, close, volume] floats. No key needed; not
    subject to Binance's cloud-IP geo-block."""
    hdr = {"User-Agent": "XRPRadar/4"}
    r = requests.get(f"https://api.exchange.coinbase.com/products/{product_id}/candles",
                      params={"granularity": granularity}, headers=hdr, timeout=8)
    data = r.json()
    if not isinstance(data, list):
        return []
    data.sort(key=lambda c: c[0])  # Coinbase returns newest-first; sort oldest->newest
    return data[-limit:]

def fetch_market():
    active = 0
    hdr = {"User-Agent": "XRPRadar/4"}

    # Price, 24h change, market cap, volume, rank — CoinPaprika (keyless; CoinCap v2 was
    # deprecated April 2025 and now requires a paid key, so it no longer works here)
    try:
        r = requests.get("https://api.coinpaprika.com/v1/tickers/xrp-xrp", headers=hdr, timeout=8)
        d = r.json()
        q = (d.get("quotes") or {}).get("USD") or {}
        p = float(q.get("price", 0) or 0)
        if p > 0:
            MARKET["xrp_price"] = p
            MARKET["xrp_chg"]   = float(q.get("percent_change_24h", 0) or 0)
            MARKET["mcap"]      = float(q.get("market_cap", 0) or 0)
            MARKET["vol24"]     = float(q.get("volume_24h", 0) or 0)
            MARKET["rank"]      = d.get("rank")
            active += 1
    except Exception:
        pass

    try:
        r = requests.get("https://api.alternative.me/fng/?limit=30", headers=hdr, timeout=5)
        arr = r.json().get("data", [])
        if arr:
            MARKET["fng"]       = int(arr[0].get("value", 0))
            MARKET["fng_label"] = arr[0].get("value_classification", "")
            MARKET["fng_history"] = [int(x.get("value", 0)) for x in reversed(arr)]  # oldest -> newest
            active += 1
    except Exception:
        pass

    # Note: funding rate needs a futures/perpetual exchange (Binance fapi was used before).
    # No safe cloud-reachable replacement is wired up, so this stays None. Smart Money Score
    # already rescales cleanly across whichever of its components are actually available.

    # Historical daily + hourly candles — Coinbase Exchange (public, no key, and not subject
    # to Binance.com's block on cloud-hosting IPs). Powers RSI, 52-week range, Price Time
    # Machine, Support & Resistance, longitudinal performance, and the A/D line.
    try:
        k1h = _coinbase_candles("XRP-USD", granularity=3600, limit=200)
        k1d = _coinbase_candles("XRP-USD", granularity=86400, limit=300)

        if k1h:
            closes_1h = [float(c[4]) for c in k1h]
            MARKET["rsi_1h"] = calc_rsi(closes_1h)
            last24 = k1h[-24:]
            MARKET["h24"] = max(float(c[2]) for c in last24)  # candle[2] = high
            MARKET["l24"] = min(float(c[1]) for c in last24)  # candle[1] = low

        if k1d:
            closes_1d = [float(c[4]) for c in k1d]
            highs_1d  = [float(c[2]) for c in k1d]
            lows_1d   = [float(c[1]) for c in k1d]
            MARKET["rsi_1d"]    = calc_rsi(closes_1d)
            MARKET["w52_low"]   = min(lows_1d)
            MARKET["w52_high"]  = max(highs_1d)
            # Price Time Machine (oldest available candle stands in for "~1 year ago" when
            # fewer than 365 days are available from a single 300-candle request)
            if len(closes_1d) >= 2:
                MARKET["tm_1y"] = closes_1d[0]
            if len(closes_1d) >= 31:
                MARKET["tm_1m"] = closes_1d[-31]
            # Support & Resistance from the last 90 days
            window = k1d[-90:] if len(k1d) >= 90 else k1d
            MARKET["sr_support"]    = min(float(c[1]) for c in window)
            MARKET["sr_resistance"] = max(float(c[2]) for c in window)
            # Longitudinal performance windows
            cur = closes_1d[-1]
            def _perf(days):
                if len(closes_1d) > days and closes_1d[-(days + 1)]:
                    old = closes_1d[-(days + 1)]
                    return (cur - old) / old * 100
                return None
            MARKET["perf_1w"]  = _perf(7)
            MARKET["perf_30d"] = _perf(30)
            MARKET["perf_90d"] = _perf(90)
            MARKET["perf_6m"]  = _perf(180)
            # Chaikin Accumulation/Distribution Line (pure price/volume TA indicator)
            ad = 0.0
            ad_series = []
            for c in k1d:
                lo, hi, cl, v = float(c[1]), float(c[2]), float(c[4]), float(c[5])
                mfm = ((cl - lo) - (hi - cl)) / (hi - lo) if hi != lo else 0.0
                ad += mfm * v
                ad_series.append(ad)
            if len(ad_series) >= 8:
                MARKET["ad_7d_delta"] = ad_series[-1] - ad_series[-8]
            if len(ad_series) >= 31:
                MARKET["ad_30d_delta"] = ad_series[-1] - ad_series[-31]
        if k1h or k1d:
            active += 1
    except Exception:
        pass

    # XRP/BTC cross-rate, computed from each asset's own USD close (Coinbase doesn't
    # need a direct XRP-BTC pair for this and it keeps one fewer network call in play)
    try:
        if MARKET.get("xrp_price"):
            btc_k = _coinbase_candles("BTC-USD", granularity=86400, limit=2)
            if btc_k:
                btc_price = float(btc_k[-1][4])
                if btc_price:
                    MARKET["xrpbtc"] = MARKET["xrp_price"] / btc_price
    except Exception:
        pass

    MARKET["sources_active"] = active
    MARKET["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


COMPETITORS = [
    {"id": "solana",   "symbol": "SOL", "emoji": "\u25CE", "paprika": "sol-solana",   "coinbase": "SOL-USD"},
    {"id": "ethereum", "symbol": "ETH", "emoji": "\u27E0", "paprika": "eth-ethereum", "coinbase": "ETH-USD"},
    {"id": "cardano",  "symbol": "ADA", "emoji": "\u20B3", "paprika": "ada-cardano",  "coinbase": "ADA-USD"},
    {"id": "stellar",  "symbol": "XLM", "emoji": "\u2726", "paprika": "xlm-stellar",  "coinbase": "XLM-USD"},
]
COMPETITOR_EDGE = {
    "SOL": "Payment rails vs. smart contract platform \u2014 XRP settles instantly for a near-zero fee.",
    "ETH": "XRP settles far cheaper per transaction with faster finality \u2014 purpose-built for payments.",
    "ADA": "XRP has live ODL corridors, bank partnerships and regulatory clarity vs. a research-first roadmap.",
    "XLM": "XRP carries deeper liquidity, more active corridors and broader institutional adoption.",
}

# ── CLARITY Act Tracker — top 10 most influential stories, hard-capped, oldest/lowest-ranked drop off ──
CLARITY_FEED = "https://news.google.com/rss/search?q=CLARITY+Act+crypto+Senate&hl=en-US&gl=US&ceid=US:en"
CLARITY_ACT_STORIES = []
_CLARITY_SEEN_KEYS = set()
_CLARITY_MAX = 10

def fetch_clarity_tracker():
    hdr = {"User-Agent": "XRPRadar/4"}
    now = datetime.now(timezone.utc)
    candidates = []

    # 1. Dedicated Google News RSS search
    try:
        r = requests.get(CLARITY_FEED, headers=hdr, timeout=8)
        for e in _parse_feed(r.content)[:12]:
            if not e["title"]:
                continue
            dt = _parse_date(e["date_str"]) or now
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            candidates.append({"key": "clarity:" + e["title"].lower()[:80], "title": e["title"][:160],
                               "link": e["link"] or "#", "source": "Google News", "dt": dt,
                               "influence": _influence(e["title"], "Google News")})
    except Exception:
        pass

    # 2. Scan the existing XRP news pool for CLARITY Act mentions (already-classified stories)
    for s in NEWS.get("pool", []):
        text = (s["title"] + " " + s.get("summary", "")).lower()
        if "clarity act" in text or "digital asset market clarity" in text:
            candidates.append({"key": "clarity:" + s["key"], "title": s["title"], "link": s["link"],
                               "source": s["source"], "dt": s["dt"], "influence": s["influence"]})

    for c in candidates:
        if c["key"] in _CLARITY_SEEN_KEYS:
            continue
        _CLARITY_SEEN_KEYS.add(c["key"])
        CLARITY_ACT_STORIES.append(c)

    # Keep only the 10 MOST RECENT stories — oldest drop off as fresh ones arrive
    CLARITY_ACT_STORIES.sort(key=lambda s: s["dt"], reverse=True)
    del CLARITY_ACT_STORIES[_CLARITY_MAX:]
    kept_keys = {s["key"] for s in CLARITY_ACT_STORIES}
    _CLARITY_SEEN_KEYS.intersection_update(kept_keys)


EXECUTIVES = [
    {"name": "Brad Garlinghouse", "title": "CEO, Ripple", "tab": "BRAD",
     "feed": "https://news.google.com/rss/search?q=Brad+Garlinghouse+XRP+Ripple&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Monica Long", "title": "President, Ripple", "tab": "MONICA",
     "feed": "https://news.google.com/rss/search?q=Monica+Long+Ripple+XRP&hl=en-US&gl=US&ceid=US:en"},
    {"name": "David Schwartz", "title": "CTO, Ripple", "tab": "DAVID",
     "feed": "https://news.google.com/rss/search?q=David+Schwartz+Ripple+XRPL&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Stuart Alderoty", "title": "Chief Legal Officer, Ripple", "tab": "STUART",
     "feed": "https://news.google.com/rss/search?q=Stuart+Alderoty+Ripple+SEC&hl=en-US&gl=US&ceid=US:en"},
]
EXEC_TRACKER = {"stories": [], "updated": None}

def fetch_exec_tracker():
    hdr = {"User-Agent": "XRPRadar/4"}
    now = datetime.now(timezone.utc)
    all_stories = []
    for ex in EXECUTIVES:
        try:
            r = requests.get(ex["feed"], headers=hdr, timeout=8)
            entries = _parse_feed(r.content)
            for e in entries[:4]:
                if not e["title"]:
                    continue
                dt = _parse_date(e["date_str"]) or now
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                all_stories.append({
                    "exec": ex["name"], "exec_title": ex["title"], "tab": ex["tab"],
                    "title": e["title"][:140], "link": e["link"] or "#", "dt": dt,
                })
        except Exception:
            continue
    all_stories.sort(key=lambda s: s["dt"], reverse=True)
    EXEC_TRACKER["stories"] = all_stories[:24]
    EXEC_TRACKER["updated"] = now.strftime("%H:%M UTC")


GITHUB_REPOS = [("XRPLF", "rippled"), ("XRPLF", "xrpl-dev-portal"), ("XRPLF", "xrpl.js")]
GITHUB_DEV = {"commits": [], "stars": 0, "issues": 0, "rippled_7d": 0, "other_7d": 0, "updated": None}

# ── Regulatory & Ledger Watch (V66) — XRPL amendments, SEC EDGAR, Federal Register ──
REG_WATCH = {"amendments": [], "edgar": [], "fedreg": [], "updated": None}

def fetch_reg_watch():
    """Fetch XRPL amendment voting, SEC EDGAR filings, and Federal Register crypto rules.
    All keyless public sources. Failures leave prior data intact."""
    # 1. XRPL Amendments — XRPScan public API
    try:
        r = requests.get("https://api.xrpscan.com/api/v1/amendments",
                         headers={"User-Agent": "Mozilla/5.0 XRPRadar/26"}, timeout=8)
        if r.status_code == 200:
            data = r.json()
            amendments = []
            for a in data:
                if not a.get("enabled", True):  # only pending/voting amendments
                    amendments.append({
                        "name": a.get("name", "Unknown"),
                        "threshold": a.get("threshold", ""),
                        "count": a.get("count", 0),
                        "eta": a.get("eta", ""),
                        "introduced": a.get("introduced", ""),
                    })
            if amendments:
                REG_WATCH["amendments"] = amendments[:8]
    except Exception:
        pass
    # 2. SEC EDGAR full-text search — official government RSS (Ripple mentions)
    try:
        r = requests.get(
            "https://efts.sec.gov/LATEST/search-index?q=%22Ripple%22%20%22XRP%22&dateRange=custom&forms=&output=atom",
            headers={"User-Agent": "XRPRadar admin@xrpradar.com"}, timeout=8)
        if r.status_code != 200:
            r = requests.get(
                "https://www.sec.gov/cgi-bin/srqsb?text=form-type%3D8-K+%22XRP%22&first=1&last=20&output=atom",
                headers={"User-Agent": "XRPRadar admin@xrpradar.com"}, timeout=8)
        if r.status_code == 200:
            entries = _parse_feed(r.content)
            edgar = []
            for e in entries[:6]:
                if e["title"]:
                    edgar.append({"title": e["title"][:140], "link": e["link"] or "#",
                                  "date": e["date_str"][:16] if e["date_str"] else ""})
            if edgar:
                REG_WATCH["edgar"] = edgar
    except Exception:
        pass
    # 3. Federal Register — official API, documents mentioning digital assets/crypto
    try:
        r = requests.get(
            "https://www.federalregister.gov/api/v1/documents.json?conditions%5Bterm%5D=digital+asset+cryptocurrency&per_page=6&order=newest",
            headers={"User-Agent": "Mozilla/5.0 XRPRadar/26"}, timeout=8)
        if r.status_code == 200:
            docs = r.json().get("results", [])
            fedreg = []
            for d in docs[:6]:
                fedreg.append({
                    "title": (d.get("title") or "")[:140],
                    "link": d.get("html_url") or "#",
                    "date": d.get("publication_date") or "",
                    "type": d.get("type") or "",
                    "agency": (d.get("agencies", [{}])[0].get("name", "") if d.get("agencies") else "")[:40],
                })
            if fedreg:
                REG_WATCH["fedreg"] = fedreg
    except Exception:
        pass
    REG_WATCH["updated"] = datetime.now(timezone.utc).strftime("%H:%M UTC")

def fetch_github_dev():
    hdr = {"Accept": "application/vnd.github.v3+json", "User-Agent": "XRPRadar/4"}
    all_commits = []
    stars = 0
    issues = 0
    for owner, repo in GITHUB_REPOS:
        try:
            r = requests.get(f"https://api.github.com/repos/{owner}/{repo}/commits?per_page=10",
                              headers=hdr, timeout=10)
            commits = r.json()
            if isinstance(commits, list):
                for c in commits[:6]:
                    cm = c.get("commit", {})
                    au = cm.get("author", {})
                    msg = (cm.get("message") or "")[:90]
                    nl = msg.find("\n")
                    if nl > 0:
                        msg = msg[:nl]
                    all_commits.append({
                        "repo": repo, "msg": msg, "author": (au.get("name") or "")[:30],
                        "date": (au.get("date") or "")[:10], "url": c.get("html_url", ""),
                    })
        except Exception:
            pass
        try:
            r2 = requests.get(f"https://api.github.com/repos/{owner}/{repo}", headers=hdr, timeout=8)
            meta = r2.json()
            stars += int(meta.get("stargazers_count", 0) or 0)
            issues += int(meta.get("open_issues_count", 0) or 0)
        except Exception:
            pass

    all_commits.sort(key=lambda c: c.get("date", ""), reverse=True)
    GITHUB_DEV["commits"] = all_commits[:15]
    GITHUB_DEV["stars"] = stars
    GITHUB_DEV["issues"] = issues
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    recent = [c for c in all_commits if c.get("date", "") >= cutoff]
    GITHUB_DEV["rippled_7d"] = len([c for c in recent if c["repo"] == "rippled"])
    GITHUB_DEV["other_7d"] = len([c for c in recent if c["repo"] != "rippled"])
    GITHUB_DEV["updated"] = datetime.now(timezone.utc).strftime("%H:%M UTC")


def fetch_competitors():
    hdr = {"User-Agent": "XRPRadar/4"}
    for c in COMPETITORS:
        entry = MARKET["competitors"].setdefault(c["id"], {})
        try:
            r = requests.get(f"https://api.coinpaprika.com/v1/tickers/{c['paprika']}", headers=hdr, timeout=8)
            d = r.json()
            q = (d.get("quotes") or {}).get("USD") or {}
            price = float(q.get("price", 0) or 0)
            if price:
                entry["price"] = price
                entry["change_24h"] = float(q.get("percent_change_24h", 0) or 0)
                entry["mcap"] = float(q.get("market_cap", 0) or 0)
        except Exception:
            pass
        try:
            closes = [float(x[4]) for x in _coinbase_candles(c["coinbase"], granularity=86400, limit=10)]
            if len(closes) > 7 and closes[-8]:
                entry["change_7d"] = (closes[-1] - closes[-8]) / closes[-8] * 100
        except Exception:
            pass


def _pearson(x, y):
    n = min(len(x), len(y))
    if n < 5:
        return None
    x, y = x[-n:], y[-n:]
    mx, my = sum(x) / n, sum(y) / n
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    vx = sum((v - mx) ** 2 for v in x)
    vy = sum((v - my) ** 2 for v in y)
    if vx == 0 or vy == 0:
        return None
    return cov / ((vx * vy) ** 0.5)

def _pct_returns(closes):
    return [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes)) if closes[i - 1]]

def fetch_correlation():
    def _closes(product_id):
        try:
            candles = _coinbase_candles(product_id, granularity=86400, limit=31)
            return [float(c[4]) for c in candles]
        except Exception:
            return []
    xrp_c = _closes("XRP-USD")
    btc_c = _closes("BTC-USD")
    eth_c = _closes("ETH-USD")
    xrp_r, btc_r, eth_r = _pct_returns(xrp_c), _pct_returns(btc_c), _pct_returns(eth_c)
    if xrp_r and btc_r:
        MARKET["corr_btc"] = _pearson(xrp_r, btc_r)
    if xrp_r and eth_r:
        MARKET["corr_eth"] = _pearson(xrp_r, eth_r)


def fetch_orderbook():
    hdr = {"User-Agent": "XRPRadar/4"}
    try:
        r = requests.get("https://api.exchange.coinbase.com/products/XRP-USD/book",
                          params={"level": 2}, headers=hdr, timeout=8)
        d = r.json()
        bids = [(float(p), float(q)) for p, q, *_ in d.get("bids", [])][:8]
        asks = [(float(p), float(q)) for p, q, *_ in d.get("asks", [])][:8]
        if bids and asks:
            MARKET["ob_bids"] = bids
            MARKET["ob_asks"] = asks
            MARKET["ob_bid_total"] = sum(p * q for p, q in bids)
            MARKET["ob_ask_total"] = sum(p * q for p, q in asks)
    except Exception:
        pass


def fetch_fx():
    hdr = {"User-Agent": "XRPRadar/4"}
    codes = ["EUR", "GBP", "JPY", "AUD", "CAD", "SGD", "INR", "BRL",
             "CHF", "CNY", "KRW", "MXN", "PHP", "NGN", "ZAR", "AED",
             "SAR", "HKD", "NZD", "SEK", "NOK", "TRY", "THB", "IDR",
             "VND", "PLN"]
    try:
        r = requests.get("https://api.exchangerate-api.com/v4/latest/USD", headers=hdr, timeout=8)
        rates = r.json().get("rates", {})
        if rates:
            MARKET["fx"] = {c: float(rates[c]) for c in codes if c in rates}
            return
    except Exception:
        pass
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", headers=hdr, timeout=8)
        rates = r.json().get("rates", {})
        if rates:
            MARKET["fx"] = {c: float(rates[c]) for c in codes if c in rates}
    except Exception:
        pass


def _bg_refresh():
    n = 0
    while True:
        try:
            fetch_market()
            if n % 5 == 0:
                fetch_fx()
                fetch_competitors()
                fetch_correlation()
            if n % 2 == 0:
                fetch_orderbook()
            if n % 60 == 0:  # check hourly whether the 3-day static directory refresh is due
                load_static_partner_directory()
        except Exception:
            pass
        n += 1
        time.sleep(60)

threading.Thread(target=_bg_refresh, daemon=True).start()

def _bg_news():
    n = 0
    while True:
        try:
            fetch_news()
            fetch_exec_tracker()
            fetch_clarity_tracker()
            if n % 2 == 0:
                fetch_github_dev()
            if n % 4 == 0:
                fetch_reg_watch()
        except Exception:
            pass
        n += 1
        time.sleep(300)

threading.Thread(target=_bg_news, daemon=True).start()

def _bg_brief():
    while True:
        try:
            slot_id, _ = _brief_slot(datetime.now(CENTRAL))
            if BRIEF["slot_id"] != slot_id:
                generate_brief()
        except Exception:
            pass
        time.sleep(60)

threading.Thread(target=_bg_brief, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────
# FEAR & GREED — horizontal color-coded line + tinted ball with number
# ─────────────────────────────────────────────────────────────────────
def fng_zone_color(v):
    if v < 25:   return "#ea3943"   # extreme fear  — red
    if v < 45:   return "#ea8c00"   # fear          — orange
    if v < 55:   return "#f3d42f"   # neutral       — yellow
    if v < 75:   return "#93d900"   # greed         — light green
    return "#16c784"                # extreme greed — green

def fng_bar_html(value):
    if value is None:
        return ('<div class="fng-wrap">'
                '<div class="fng-bar"></div>'
                '<div class="fng-ball" style="left:50%;background:#555">--</div>'
                '</div>')
    v = max(0, min(100, int(value)))
    col = fng_zone_color(v)
    return (f'<div class="fng-wrap">'
            f'<div class="fng-bar"></div>'
            f'<div class="fng-ball" style="left:{v}%;background:{col}">{v}</div>'
            f'</div>')


# ─────────────────────────────────────────────────────────────────────
# NEWS FEED (RSS/Atom via stdlib ElementTree — no feedparser dependency)
# ─────────────────────────────────────────────────────────────────────
NEWS_FEEDS = [
    # ── MAJOR CRYPTO NEWS (81 feeds) ─────────────────────────────────────────
    ("CoinDesk",                    "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Decrypt",                     "https://decrypt.co/feed"),
    ("The Block",                   "https://www.theblock.co/rss.xml"),
    ("Blockworks",                  "https://blockworks.co/feed"),
    ("Daily Hodl",                  "https://dailyhodl.com/feed/"),
    ("AMBCrypto",                   "https://ambcrypto.com/feed/"),
    ("BeInCrypto",                  "https://beincrypto.com/feed/"),
    ("NewsBTC",                     "https://www.newsbtc.com/feed/"),
    ("Finbold",                     "https://finbold.com/feed/"),
    ("CryptoSlate",                 "https://cryptoslate.com/feed/"),
    ("CryptoPotato",                "https://cryptopotato.com/feed/"),
    ("ZyCrypto",                    "https://zycrypto.com/feed/"),
    ("Bitcoinist",                  "https://bitcoinist.com/feed/"),
    ("Cryptonews",                  "https://cryptonews.com/news/feed/"),
    ("CoinGape",                    "https://coingape.com/feed/"),
    ("CryptoGlobe",                 "https://www.cryptoglobe.com/latest/feed/"),
    ("Crypto Daily",                "https://cryptodaily.co.uk/feed"),
    ("Invezz",                      "https://invezz.com/feed/"),
    ("InsideBitcoins",              "https://insidebitcoins.com/feed"),
    ("Crypto Briefing",             "https://cryptobriefing.com/feed/"),
    ("The Defiant",                 "https://thedefiant.io/feed"),
    ("Bitcoin Magazine",            "https://bitcoinmagazine.com/feed"),
    ("CoinGecko Blog",              "https://blog.coingecko.com/rss/"),
    ("CoinJournal XRP",             "https://news.google.com/rss/search?q=XRP+ripple+site:coinjournal.net&hl=en-US&gl=US&ceid=US:en"),
    ("99Bitcoins",                  "https://99bitcoins.com/feed/"),
    ("UseTheBitcoin",               "https://usethebitcoin.com/feed/"),
    ("BitcoinExchangeGuide",        "https://bitcoinexchangeguide.com/feed/"),
    ("GN: XRP Futures",             "https://news.google.com/rss/search?q=XRP+futures&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Coinbase2",           "https://news.google.com/rss/search?q=XRP+Coinbase&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Binance2",            "https://news.google.com/rss/search?q=XRP+Binance&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Price Target",        "https://news.google.com/rss/search?q=XRP+price+target&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Technical2",          "https://news.google.com/rss/search?q=XRP+technical+analysis&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Liquidity",           "https://news.google.com/rss/search?q=XRP+liquidity&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP CoinDesk2",           "https://news.google.com/rss/search?q=XRP+site:coindesk.com&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP TheBlock2",           "https://news.google.com/rss/search?q=XRP+site:theblock.co&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Decrypt2",            "https://news.google.com/rss/search?q=XRP+site:decrypt.co&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Market Cap",          "https://news.google.com/rss/search?q=XRP+market+cap&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Bitstamp",            "https://news.google.com/rss/search?q=XRP+Bitstamp&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP Blockworks EU",       "https://news.google.com/rss/search?q=XRP+Blockworks&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP Altcoin",             "https://news.google.com/rss/search?q=XRP+altcoin+season&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Halving",             "https://news.google.com/rss/search?q=XRP+crypto+halving&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Dominance",           "https://news.google.com/rss/search?q=XRP+dominance&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Volume",              "https://news.google.com/rss/search?q=XRP+trading+volume&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Chart",               "https://news.google.com/rss/search?q=XRP+chart+analysis&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Sentiment",           "https://news.google.com/rss/search?q=XRP+sentiment&hl=en-US&gl=US&ceid=US:en"),
    ("CryptoCompare Global",        "https://news.google.com/rss/search?q=XRP+cryptocompare&hl=en-US&gl=US&ceid=US:en"),
    ("Coinglass Derivatives",       "https://news.google.com/rss/search?q=XRP+coinglass+derivatives&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP LunarCrush",          "https://news.google.com/rss/search?q=XRP+LunarCrush&hl=en-US&gl=US&ceid=US:en"),
    ("Ledger Insights",             "https://ledgerinsights.com/feed/"),
    ("Finextra Finance",            "https://www.finextra.com/rss/headlines.aspx"),
    ("PYMNTS Blockchain",           "https://www.pymnts.com/feed/"),
    ("The Fintech Times",           "https://thefintechtimes.com/feed/"),
    ("GN: XRP Options",             "https://news.google.com/rss/search?q=XRP+options&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Evernode",            "https://news.google.com/rss/search?q=XRP+Evernode&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Sologenic",           "https://news.google.com/rss/search?q=XRP+Sologenic&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP XUMM",                "https://news.google.com/rss/search?q=XRP+XUMM+Xaman&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Hooks",               "https://news.google.com/rss/search?q=XRPL+Hooks&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRPL NFT",                "https://news.google.com/rss/search?q=XRPL+NFT&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRPL AMM",                "https://news.google.com/rss/search?q=XRPL+AMM&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRPL DeFi",               "https://news.google.com/rss/search?q=XRPL+DeFi&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Peersyst",            "https://news.google.com/rss/search?q=XRP+Peersyst&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP WSJ",                 "https://news.google.com/rss/search?q=XRP+site:wsj.com&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Bloomberg",           "https://news.google.com/rss/search?q=XRP+site:bloomberg.com&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Reuters",             "https://news.google.com/rss/search?q=XRP+site:reuters.com&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP FT",                  "https://news.google.com/rss/search?q=XRP+site:ft.com&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP CNBC",                "https://news.google.com/rss/search?q=XRP+site:cnbc.com&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Forbes",              "https://news.google.com/rss/search?q=XRP+site:forbes.com&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Fortune",             "https://news.google.com/rss/search?q=XRP+site:fortune.com&hl=en-US&gl=US&ceid=US:en"),
    ("CoinDesk XRP",                "https://news.google.com/rss/search?q=XRP+ripple+site:coindesk.com&hl=en-US&gl=US&ceid=US:en"),
    ("Ledger Insights Direct",      "https://ledgerinsights.com/category/blockchain/feed/"),
    ("Finextra Direct",             "https://www.finextra.com/rss/pressrelease.aspx"),
    ("PYMNTS Direct",               "https://www.pymnts.com/blockchain/feed/"),
    ("Fintech Times Direct",        "https://thefintechtimes.com/category/blockchain/feed/"),
    ("InsideBitcoins Direct",       "https://insidebitcoins.com/category/news/feed"),
    ("UseTheBitcoin Direct",        "https://usethebitcoin.com/category/news/feed/"),
    ("Invezz Crypto Direct",        "https://invezz.com/category/crypto/feed/"),
    ("Bitcoinist XRP Direct",       "https://bitcoinist.com/tag/xrp/feed/"),
    ("NewsBTC XRP Direct",          "https://www.newsbtc.com/tag/xrp/feed/"),
    ("CoinJournal XRP Direct",      "https://news.google.com/rss/search?q=XRP+ripple+coinjournal&hl=en&gl=GB&ceid=GB:en"),
    ("ZyCrypto XRP Direct",         "https://zycrypto.com/tag/xrp/feed/"),
    ("Crypto Daily Direct",         "https://cryptodaily.co.uk/tag/xrp/feed"),
    ("Cointelegraph",               "https://cointelegraph.com/rss"),
    # ── INSTITUTIONAL & BANKING (45 feeds) ───────────────────────────────────
    ("GN: XRP ETF",                 "https://news.google.com/rss/search?q=XRP+ETF&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Bank",                "https://news.google.com/rss/search?q=XRP+bank+partnership&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Custody",             "https://news.google.com/rss/search?q=XRP+crypto+custody&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP ETF Latest",          "https://news.google.com/rss/search?q=XRP+ETF+approval+2026&hl=en-US&gl=US&ceid=US:en"),
    ("Coinbase Blog",               "https://www.coinbase.com/blog/landing-page-data/rss"),
    ("GN: XRP Reserve",             "https://news.google.com/rss/search?q=XRP+strategic+reserve&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Custody Bank",        "https://news.google.com/rss/search?q=XRP+bank+custody+institutional&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Spot ETF",            "https://news.google.com/rss/search?q=XRP+spot+ETF&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Futures ETF",         "https://news.google.com/rss/search?q=XRP+futures+ETF&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP BlackRock",           "https://news.google.com/rss/search?q=XRP+BlackRock&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Seeking Alpha",       "https://news.google.com/rss/search?q=XRP+site:seekingalpha.com&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Messari",             "https://news.google.com/rss/search?q=XRP+Messari&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP EU Inst",             "https://news.google.com/rss/search?q=XRP+European+institutional&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP Grayscale",           "https://news.google.com/rss/search?q=XRP+Grayscale&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Galaxy",              "https://news.google.com/rss/search?q=XRP+Galaxy+Digital&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Pantera",             "https://news.google.com/rss/search?q=XRP+Pantera+Capital&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP a16z",                "https://news.google.com/rss/search?q=XRP+a16z+andreessen&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP ProShares",           "https://news.google.com/rss/search?q=XRP+ProShares&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Franklin",            "https://news.google.com/rss/search?q=XRP+Franklin+Templeton&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Ripple IPO",          "https://news.google.com/rss/search?q=Ripple+IPO&hl=en-US&gl=US&ceid=US:en"),
    ("Santiment Analytics",         "https://news.google.com/rss/search?q=XRP+Santiment+analytics&hl=en-US&gl=US&ceid=US:en"),
    ("Glassnode On-Chain",          "https://news.google.com/rss/search?q=XRP+Glassnode+on-chain&hl=en-US&gl=US&ceid=US:en"),
    ("Messari XRP",                 "https://news.google.com/rss/search?q=XRP+Messari+report&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP CryptoQuant",         "https://news.google.com/rss/search?q=XRP+CryptoQuant&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP IntoTheBlock",        "https://news.google.com/rss/search?q=XRP+IntoTheBlock&hl=en-US&gl=US&ceid=US:en"),
    ("GN: BIS XRP Research",        "https://news.google.com/rss/search?q=BIS+XRP+research&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP BIS Research",        "https://news.google.com/rss/search?q=XRP+Bank+International+Settlements&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP IMF",                 "https://news.google.com/rss/search?q=XRP+IMF&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP World Bank",          "https://news.google.com/rss/search?q=XRP+World+Bank&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP JPMorgan",            "https://news.google.com/rss/search?q=XRP+JPMorgan&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Goldman",             "https://news.google.com/rss/search?q=XRP+Goldman+Sachs&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP BlackRock ETF",       "https://news.google.com/rss/search?q=XRP+BlackRock+ETF&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Fidelity",            "https://news.google.com/rss/search?q=XRP+Fidelity&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Nasdaq",              "https://news.google.com/rss/search?q=XRP+Nasdaq&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Nansen",              "https://news.google.com/rss/search?q=XRP+Nansen&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Chainalysis",         "https://news.google.com/rss/search?q=XRP+Chainalysis&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Coin Metrics",        "https://news.google.com/rss/search?q=XRP+CoinMetrics&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Token Terminal",      "https://news.google.com/rss/search?q=XRP+Token+Terminal&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Dune Analytics",      "https://news.google.com/rss/search?q=XRP+Dune+Analytics&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP CME",                 "https://news.google.com/rss/search?q=XRP+CME+futures&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Wintermute",          "https://news.google.com/rss/search?q=XRP+Wintermute&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP Cumberland",          "https://news.google.com/rss/search?q=XRP+Cumberland+DRW&hl=en-US&gl=US&ceid=US:en"),
    ("Santiment Blog",              "https://santiment.net/blog/feed/"),
    ("GN: XRP Seeking Alpha 2",     "https://news.google.com/rss/search?q=Ripple+XRP+seekingalpha&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Motley Fool",         "https://news.google.com/rss/search?q=XRP+Motley+Fool&hl=en-US&gl=US&ceid=US:en"),
    # ── LEGAL & REGULATORY (36 feeds) ────────────────────────────────────────
    ("GN: XRP Legal",               "https://news.google.com/rss/search?q=XRP+legal+ruling&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Congress",            "https://news.google.com/rss/search?q=XRP+Congress+crypto+legislation&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP CFTC",                "https://news.google.com/rss/search?q=XRP+CFTC&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP OCC",                 "https://news.google.com/rss/search?q=XRP+OCC&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Treasury",            "https://news.google.com/rss/search?q=XRP+US+Treasury&hl=en-US&gl=US&ceid=US:en"),
    ("Crypto Slate SEC",            "https://cryptoslate.com/tag/sec/feed/"),
    ("GN: Crypto Act",              "https://news.google.com/rss/search?q=crypto+legislation+act+2026&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP SEC Update",          "https://news.google.com/rss/search?q=XRP+SEC+update&hl=en-US&gl=US&ceid=US:en"),
    ("GN: Crypto Tax US",           "https://news.google.com/rss/search?q=crypto+tax+IRS+2026&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP OCC Reg",             "https://news.google.com/rss/search?q=XRP+OCC+regulation&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Treasury 2",          "https://news.google.com/rss/search?q=Ripple+Treasury+crypto+policy&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP MiCA EU",             "https://news.google.com/rss/search?q=XRP+MiCA+Europe&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP UK FCA",              "https://news.google.com/rss/search?q=XRP+FCA+UK&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP Germany",             "https://news.google.com/rss/search?q=XRP+Germany+BaFin&hl=de&gl=DE&ceid=DE:de"),
    ("GN: XRP France",              "https://news.google.com/rss/search?q=XRP+France+AMF&hl=fr&gl=FR&ceid=FR:fr"),
    ("GN: XRP Netherlands",         "https://news.google.com/rss/search?q=XRP+Netherlands+DNB&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP Korea Reg",           "https://news.google.com/rss/search?q=XRP+Korea+FSC+regulation&hl=ko&gl=KR&ceid=KR:ko"),
    ("GN: XRP Japan FSA",           "https://news.google.com/rss/search?q=XRP+Japan+FSA&hl=ja&gl=JP&ceid=JP:ja"),
    ("GN: XRP Congress 2",          "https://news.google.com/rss/search?q=XRP+Senate+House+crypto+bill&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Gensler",             "https://news.google.com/rss/search?q=XRP+SEC+crypto+regulation&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP FDIC",                "https://news.google.com/rss/search?q=XRP+FDIC+crypto&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP White House",         "https://news.google.com/rss/search?q=XRP+White+House+crypto+policy&hl=en-US&gl=US&ceid=US:en"),
    ("VARA Dubai Reg",              "https://news.google.com/rss/search?q=VARA+Dubai+crypto+regulation&hl=en&gl=AE&ceid=AE:en"),
    ("ADGM Abu Dhabi",              "https://news.google.com/rss/search?q=ADGM+Abu+Dhabi+crypto&hl=en&gl=AE&ceid=AE:en"),
    ("SEC Press Releases",          "https://news.google.com/rss/search?q=SEC+crypto+press+release&hl=en-US&gl=US&ceid=US:en"),
    ("GN: SEC Crypto XRP",          "https://news.google.com/rss/search?q=SEC+XRP+crypto+enforcement&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Federal Reserve",     "https://news.google.com/rss/search?q=XRP+Federal+Reserve+CBDC&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP ECB Digital",         "https://news.google.com/rss/search?q=XRP+ECB+digital+euro&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP FinCEN",              "https://news.google.com/rss/search?q=XRP+FinCEN&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP CFTC Crypto",         "https://news.google.com/rss/search?q=CFTC+crypto+XRP+commodity&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP OCC Bank",            "https://news.google.com/rss/search?q=OCC+bank+crypto+XRP&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP UK FCA 2",            "https://news.google.com/rss/search?q=XRP+FCA+UK+crypto+regulation&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP MAS Singapore",       "https://news.google.com/rss/search?q=XRP+MAS+Singapore&hl=en&gl=SG&ceid=SG:en"),
    ("GN: XRP ASIC Australia",      "https://news.google.com/rss/search?q=XRP+ASIC+Australia&hl=en&gl=AU&ceid=AU:en"),
    ("GN: XRP FSA Japan Reg",       "https://news.google.com/rss/search?q=XRP+FSA+Japan+regulation&hl=ja&gl=JP&ceid=JP:ja"),
    ("GN: XRP FATF",                "https://news.google.com/rss/search?q=XRP+FATF+crypto&hl=en&gl=GB&ceid=GB:en"),
    # ── INTERNATIONAL & REGIONAL (82 feeds) ──────────────────────────────────
    ("GN: XRP SBI",                 "https://news.google.com/rss/search?q=XRP+SBI+Ripple&hl=ja&gl=JP&ceid=JP:ja"),
    ("CoinPost Japan",              "https://coinpost.jp/?feed=rss2"),
    ("CoinPost JP All",             "https://coinpost.jp/feed/"),
    ("Crypto Times JP",             "https://crypto-times.jp/feed/"),
    ("GN Japan XRP",                "https://news.google.com/rss/search?q=XRP+%E3%83%AA%E3%83%83%E3%83%97%E3%83%AB&hl=ja&gl=JP&ceid=JP:ja"),
    ("GN Japan XRP EN",             "https://news.google.com/rss/search?q=XRP+Japan+Ripple&hl=en-US&gl=US&ceid=US:en"),
    ("CoinPost JP XRP",             "https://coinpost.jp/tag/xrp/feed/"),
    ("CoinDesk Japan",              "https://news.google.com/rss/search?q=XRP+CoinDesk+Japan&hl=ja&gl=JP&ceid=JP:ja"),
    ("GN Korea XRP",                "https://news.google.com/rss/search?q=XRP+%EB%A6%AC%ED%94%8C&hl=ko&gl=KR&ceid=KR:ko"),
    ("GN Korea XRP EN",             "https://news.google.com/rss/search?q=XRP+Korea+Ripple&hl=en-US&gl=US&ceid=US:en"),
    ("Decenter KR",                 "https://news.google.com/rss/search?q=XRP+decenter+korea&hl=ko&gl=KR&ceid=KR:ko"),
    ("GN UAE XRP",                  "https://news.google.com/rss/search?q=XRP+UAE+Ripple&hl=en&gl=AE&ceid=AE:en"),
    ("GN ME Crypto",                "https://news.google.com/rss/search?q=XRP+Middle+East+crypto&hl=en&gl=AE&ceid=AE:en"),
    ("Rain Financial ME",           "https://news.google.com/rss/search?q=XRP+Rain+Financial+Bahrain&hl=en&gl=AE&ceid=AE:en"),
    ("VARA Dubai Reg 2",            "https://news.google.com/rss/search?q=VARA+Dubai+XRP+crypto&hl=en&gl=AE&ceid=AE:en"),
    ("ADGM Abu Dhabi 2",            "https://news.google.com/rss/search?q=ADGM+XRP+Abu+Dhabi&hl=en&gl=AE&ceid=AE:en"),
    ("GN Europe XRP",               "https://news.google.com/rss/search?q=XRP+Europe+Ripple&hl=en&gl=GB&ceid=GB:en"),
    ("GN UK XRP",                   "https://news.google.com/rss/search?q=XRP+UK+Ripple&hl=en&gl=GB&ceid=GB:en"),
    ("BTC Echo DE",                 "https://www.btc-echo.de/feed/"),
    ("CoinTelegraph DE",            "https://de.cointelegraph.com/rss"),
    ("CoinTelegraph IT",            "https://it.cointelegraph.com/rss"),
    ("CoinTelegraph FR",            "https://fr.cointelegraph.com/rss"),
    ("ForkLog Eastern EU",          "https://forklog.com/feed/"),
    ("GN India XRP",                "https://news.google.com/rss/search?q=XRP+India+Ripple&hl=en&gl=IN&ceid=IN:en"),
    ("WazirX Blog",                 "https://wazirx.com/blog/feed/"),
    ("Coinpedia",                   "https://coinpedia.org/feed/"),
    ("CoinDCX India",               "https://coindcx.com/blog/feed/"),
    ("GN LatAm XRP",                "https://news.google.com/rss/search?q=XRP+Ripple+Latin+America&hl=es&gl=MX&ceid=MX:es"),
    ("CriptoNoticias",              "https://www.criptonoticias.com/feed/"),
    ("Diario Bitcoin",              "https://www.diariobitcoin.com/feed/"),
    ("Bitso Blog LatAm",            "https://blog.bitso.com/feed/"),
    ("GN Africa XRP",               "https://news.google.com/rss/search?q=XRP+Africa+Ripple&hl=en&gl=ZA&ceid=ZA:en"),
    ("Bitmama Africa",              "https://news.google.com/rss/search?q=XRP+Bitmama+Africa+crypto&hl=en&gl=ZA&ceid=ZA:en"),
    ("Yellow Card Africa",          "https://news.google.com/rss/search?q=XRP+Yellow+Card+Africa&hl=en&gl=ZA&ceid=ZA:en"),
    ("GN SEA XRP",                  "https://news.google.com/rss/search?q=XRP+Southeast+Asia+Ripple&hl=en&gl=SG&ceid=SG:en"),
    ("Forkast Asia",                "https://forkast.news/feed/"),
    ("HashKey Exchange",            "https://news.google.com/rss/search?q=XRP+HashKey+Exchange&hl=en&gl=SG&ceid=SG:en"),
    ("Indodax Indonesia",           "https://news.google.com/rss/search?q=XRP+Indodax+Indonesia&hl=id&gl=ID&ceid=ID:id"),
    ("Tokocrypto Indonesia",        "https://news.google.com/rss/search?q=XRP+Tokocrypto+Indonesia&hl=id&gl=ID&ceid=ID:id"),
    ("CoinJar News",                "https://news.google.com/rss/search?q=XRP+CoinJar+Australia&hl=en&gl=AU&ceid=AU:en"),
    ("BTC Markets Australia",       "https://news.google.com/rss/search?q=XRP+BTC+Markets+Australia&hl=en&gl=AU&ceid=AU:en"),
    ("BlockTempo Taiwan",           "https://news.google.com/rss/search?q=XRP+BlockTempo+Taiwan&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"),
    ("GN: XRP Australia",           "https://news.google.com/rss/search?q=XRP+Australia+Ripple&hl=en&gl=AU&ceid=AU:en"),
    ("GN: XRP Hong Kong",           "https://news.google.com/rss/search?q=XRP+Hong+Kong&hl=en&gl=HK&ceid=HK:en"),
    ("GN: XRP Taiwan",              "https://news.google.com/rss/search?q=XRP+Taiwan+ripple&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"),
    ("GN: XRP Indonesia",           "https://news.google.com/rss/search?q=XRP+Indonesia+Ripple&hl=id&gl=ID&ceid=ID:id"),
    ("GN: XRP Malaysia",            "https://news.google.com/rss/search?q=XRP+Malaysia+Ripple&hl=en&gl=MY&ceid=MY:en"),
    ("GN: XRP Philippines",         "https://news.google.com/rss/search?q=XRP+Philippines+Ripple&hl=en&gl=PH&ceid=PH:en"),
    ("GN: XRP Thailand",            "https://news.google.com/rss/search?q=XRP+Thailand+Ripple&hl=th&gl=TH&ceid=TH:th"),
    ("GN: XRP Vietnam",             "https://news.google.com/rss/search?q=XRP+Vietnam+Ripple&hl=vi&gl=VN&ceid=VN:vi"),
    ("GN: XRP Brazil",              "https://news.google.com/rss/search?q=XRP+Brasil+Ripple&hl=pt-BR&gl=BR&ceid=BR:pt-419"),
    ("GN: XRP Mexico",              "https://news.google.com/rss/search?q=XRP+Mexico+Ripple&hl=es&gl=MX&ceid=MX:es"),
    ("GN: XRP Argentina 1",         "https://news.google.com/rss/search?q=XRP+Argentina+Ripple&hl=es&gl=AR&ceid=AR:es"),
    ("GN: XRP Colombia 1",          "https://news.google.com/rss/search?q=XRP+Colombia+Ripple&hl=es&gl=CO&ceid=CO:es"),
    ("GN: XRP Nigeria",             "https://news.google.com/rss/search?q=XRP+Nigeria+Ripple&hl=en&gl=NG&ceid=NG:en"),
    ("GN: XRP Kenya 1",             "https://news.google.com/rss/search?q=XRP+Kenya+Ripple&hl=en&gl=KE&ceid=KE:en"),
    ("GN: XRP South Africa 1",      "https://news.google.com/rss/search?q=XRP+South+Africa+Ripple&hl=en&gl=ZA&ceid=ZA:en"),
    ("GN: XRP Ghana 1",             "https://news.google.com/rss/search?q=XRP+Ghana+Ripple&hl=en&gl=GH&ceid=GH:en"),
    ("GN: XRP Ethiopia",            "https://news.google.com/rss/search?q=XRP+Ethiopia+crypto&hl=en&gl=ZA&ceid=ZA:en"),
    ("GN: XRP Morocco",             "https://news.google.com/rss/search?q=XRP+Morocco+crypto&hl=fr&gl=MA&ceid=MA:fr"),
    ("GN: XRP Saudi",               "https://news.google.com/rss/search?q=XRP+Saudi+Arabia+Ripple&hl=en&gl=SA&ceid=SA:en"),
    ("GN: XRP Bahrain 1",           "https://news.google.com/rss/search?q=XRP+Bahrain+crypto&hl=en&gl=AE&ceid=AE:en"),
    ("GN: XRP Israel 1",            "https://news.google.com/rss/search?q=XRP+Israel+crypto&hl=en&gl=IL&ceid=IL:en"),
    ("GN: XRP Pakistan 1",          "https://news.google.com/rss/search?q=XRP+Pakistan+Ripple&hl=en&gl=IN&ceid=IN:en"),
    ("GN: XRP Bangladesh 1",        "https://news.google.com/rss/search?q=XRP+Bangladesh+crypto&hl=en&gl=IN&ceid=IN:en"),
    ("GN: XRP Poland",              "https://news.google.com/rss/search?q=XRP+Poland+crypto&hl=pl&gl=PL&ceid=PL:pl"),
    ("GN: XRP Spain",               "https://news.google.com/rss/search?q=XRP+Spain+Ripple&hl=es&gl=ES&ceid=ES:es"),
    ("GN: XRP Switzerland",         "https://news.google.com/rss/search?q=XRP+Switzerland+FINMA&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP ECB",                 "https://news.google.com/rss/search?q=XRP+ECB+European+Central+Bank&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP Scandinavia",         "https://news.google.com/rss/search?q=XRP+Scandinavia+Nordic+crypto&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP UK Adoption",         "https://news.google.com/rss/search?q=XRP+UK+adoption+Ripple&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP Germany 2",           "https://news.google.com/rss/search?q=XRP+Deutschland+Krypto&hl=de&gl=DE&ceid=DE:de"),
    ("GN: XRP France 2",            "https://news.google.com/rss/search?q=XRP+France+crypto&hl=fr&gl=FR&ceid=FR:fr"),
    ("GN: XRP Netherlands 2",       "https://news.google.com/rss/search?q=XRP+Netherlands+crypto&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP Japan Bank",          "https://news.google.com/rss/search?q=XRP+Japan+bank+SBI&hl=ja&gl=JP&ceid=JP:ja"),
    ("GN: XRP Turkey 1",            "https://news.google.com/rss/search?q=XRP+Turkey+crypto&hl=tr&gl=TR&ceid=TR:tr"),
    ("GN: XRP Egypt",               "https://news.google.com/rss/search?q=XRP+Egypt+crypto&hl=en&gl=ZA&ceid=ZA:en"),
    ("GN: XRP Argentina 2",         "https://news.google.com/rss/search?q=XRP+Argentina+crypto+2026&hl=es&gl=AR&ceid=AR:es"),
    ("GN: XRP Colombia 2",          "https://news.google.com/rss/search?q=XRP+Colombia+crypto&hl=es&gl=CO&ceid=CO:es"),
    ("GN: XRP Chile",               "https://news.google.com/rss/search?q=XRP+Chile+crypto&hl=es&gl=CL&ceid=CL:es"),
    ("GN: XRP South Africa 2",      "https://news.google.com/rss/search?q=XRP+South+Africa+crypto&hl=en&gl=ZA&ceid=ZA:en"),
    ("GN: XRP Kenya 2",             "https://news.google.com/rss/search?q=XRP+Kenya+crypto&hl=en&gl=KE&ceid=KE:en"),
    ("GN: XRP Tanzania",            "https://news.google.com/rss/search?q=XRP+Tanzania+crypto&hl=en&gl=ZA&ceid=ZA:en"),
    ("GN: XRP Ghana 2",             "https://news.google.com/rss/search?q=XRP+Ghana+crypto&hl=en&gl=GH&ceid=GH:en"),
    ("GN: XRP Vietnam 2",           "https://news.google.com/rss/search?q=XRP+Vietnam+crypto&hl=vi&gl=VN&ceid=VN:vi"),
    ("GN: XRP Thailand 2",          "https://news.google.com/rss/search?q=XRP+Thailand+crypto&hl=th&gl=TH&ceid=TH:th"),
    ("GN: XRP Pakistan 2",          "https://news.google.com/rss/search?q=XRP+Pakistan+crypto&hl=en&gl=IN&ceid=IN:en"),
    ("GN: XRP Bangladesh 2",        "https://news.google.com/rss/search?q=XRP+Bangladesh+Ripple&hl=en&gl=IN&ceid=IN:en"),
    ("GN: XRP Bahrain 2",           "https://news.google.com/rss/search?q=XRP+Bahrain+Ripple&hl=en&gl=AE&ceid=AE:en"),
    ("GN: XRP Israel 2",            "https://news.google.com/rss/search?q=XRP+Israel+Ripple&hl=en&gl=IL&ceid=IL:en"),
    # ── ECOSYSTEM & TECHNICAL (22 feeds) ─────────────────────────────────────
    ("GN: XRP Adoption",            "https://news.google.com/rss/search?q=XRP+adoption+use+case&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP ODL",                 "https://news.google.com/rss/search?q=XRP+ODL+on-demand+liquidity&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP ISO 20022",           "https://news.google.com/rss/search?q=XRP+ISO+20022&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP CBDC",                "https://news.google.com/rss/search?q=XRP+CBDC+central+bank&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Partnership",         "https://news.google.com/rss/search?q=XRP+Ripple+partnership&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Payment",             "https://news.google.com/rss/search?q=XRP+payment+cross-border&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Fintech",             "https://news.google.com/rss/search?q=XRP+fintech+integration&hl=en-US&gl=US&ceid=US:en"),
    ("GN: Ripple CBDC",             "https://news.google.com/rss/search?q=Ripple+CBDC+platform&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP RippleNet",           "https://news.google.com/rss/search?q=RippleNet+XRP&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP ISO 20022 2",         "https://news.google.com/rss/search?q=XRP+ISO20022+payment+rails&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRPL Dev",                "https://news.google.com/rss/search?q=XRPL+developer+update&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRPL Tech",               "https://news.google.com/rss/search?q=XRPL+technical+upgrade&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP DeFi",                "https://news.google.com/rss/search?q=XRP+DeFi&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Web3 DeFi",           "https://news.google.com/rss/search?q=XRPL+Web3+DeFi&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP NFT Gaming",          "https://news.google.com/rss/search?q=XRPL+NFT+gaming&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Validator",           "https://news.google.com/rss/search?q=XRPL+validator+node&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP EU Banking",          "https://news.google.com/rss/search?q=XRP+European+banking+integration&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP UK Adoption 2",       "https://news.google.com/rss/search?q=XRP+UK+fintech+adoption&hl=en&gl=GB&ceid=GB:en"),
    ("GN: XRP EVM Sidechain",       "https://news.google.com/rss/search?q=XRPL+EVM+sidechain&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP AMM Liquidity",       "https://news.google.com/rss/search?q=XRPL+AMM+liquidity+DEX&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Stablecoin Tech",     "https://news.google.com/rss/search?q=RLUSD+stablecoin+XRPL&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Tokenization",        "https://news.google.com/rss/search?q=XRP+tokenization+RWA&hl=en-US&gl=US&ceid=US:en"),
    # ── OFFICIAL RIPPLE SOURCES (9 feeds) ────────────────────────────────────
    ("Ripple Insights",             "https://ripple.com/insights/feed/"),
    ("XRPL.org Blog",               "https://xrpl.org/blog/feed.xml"),
    ("GN: Garlinghouse",            "https://news.google.com/rss/search?q=Brad+Garlinghouse+XRP+Ripple&hl=en-US&gl=US&ceid=US:en"),
    ("GN: Ripple CEO",              "https://news.google.com/rss/search?q=Ripple+CEO+XRP&hl=en-US&gl=US&ceid=US:en"),
    ("GN: Brad Interview",          "https://news.google.com/rss/search?q=Brad+Garlinghouse+interview&hl=en-US&gl=US&ceid=US:en"),
    ("GN: David Schwartz",          "https://news.google.com/rss/search?q=David+Schwartz+Ripple+XRPL&hl=en-US&gl=US&ceid=US:en"),
    ("GN: Monica Long",             "https://news.google.com/rss/search?q=Monica+Long+Ripple+XRP&hl=en-US&gl=US&ceid=US:en"),
    ("GN: Ripple Labs",             "https://news.google.com/rss/search?q=Ripple+Labs+XRP&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRPLF",                   "https://news.google.com/rss/search?q=XRP+Ledger+Foundation&hl=en-US&gl=US&ceid=US:en"),
    # ── XRP PRICE & MARKET (9 feeds) ─────────────────────────────────────────
    ("U.Today XRP",                 "https://u.today/rss"),
    ("Crypto News Flash",           "https://www.crypto-news-flash.com/feed/"),
    ("XRP News CoinTele",           "https://cointelegraph.com/tags/xrp/feed"),
    ("CryptoSlate XRP",             "https://cryptoslate.com/tag/xrp/feed/"),
    ("GN: RLUSD",                   "https://news.google.com/rss/search?q=RLUSD+Ripple+stablecoin&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Price",               "https://news.google.com/rss/search?q=XRP+price+prediction&hl=en-US&gl=US&ceid=US:en"),
    ("Crypto Potato XRP",           "https://cryptopotato.com/tag/xrp/feed/"),
    ("Crypto Slate Ripple",         "https://cryptoslate.com/tag/ripple/feed/"),
    ("GN: XRP Stablecoin",          "https://news.google.com/rss/search?q=XRP+stablecoin+RLUSD&hl=en-US&gl=US&ceid=US:en"),
    # ── WHALE & AGGREGATOR (3 feeds) ─────────────────────────────────────────
    ("GN: XRP Whale",               "https://news.google.com/rss/search?q=XRP+whale+transaction&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Ripple Aggregator",   "https://news.google.com/rss/search?q=XRP+Ripple+news&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Breaking",            "https://news.google.com/rss/search?q=XRP+breaking+news&hl=en-US&gl=US&ceid=US:en"),
    # ── COMMUNITY (6 feeds) ──────────────────────────────────────────────────
    ("Reddit r/Ripple",             "https://www.reddit.com/r/Ripple/.rss"),
    ("Reddit r/XRP",                "https://www.reddit.com/r/XRP/.rss"),
    ("Reddit r/XRPTrader",          "https://www.reddit.com/r/XRPTrader/.rss"),
    ("Reddit r/CryptoCurr",         "https://www.reddit.com/r/CryptoCurrency/.rss"),
    ("Reddit r/XRPtrader 2",        "https://www.reddit.com/r/xrptrader/.rss"),
    ("Reddit r/Ripple 2",           "https://www.reddit.com/r/ripple/.rss"),
    # ── MAINSTREAM MEDIA XRP (12 feeds) ──────────────────────────────────────
    ("Forbes Crypto",               "https://www.forbes.com/crypto-blockchain/feed/"),
    ("Yahoo Finance Crypto",        "https://finance.yahoo.com/news/rssindex"),
    ("GN: XRP Reuters 2",           "https://news.google.com/rss/search?q=XRP+Reuters&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Bloomberg 2",         "https://news.google.com/rss/search?q=XRP+Bloomberg&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP CNBC 2",              "https://news.google.com/rss/search?q=XRP+CNBC&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP WSJ 2",               "https://news.google.com/rss/search?q=XRP+Wall+Street+Journal&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Forbes 2",            "https://news.google.com/rss/search?q=XRP+Forbes&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Fortune 2",           "https://news.google.com/rss/search?q=XRP+Fortune+magazine&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP AP News",             "https://news.google.com/rss/search?q=XRP+AP+News&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Nasdaq 2",            "https://news.google.com/rss/search?q=XRP+Nasdaq+listing&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Fed Policy",          "https://news.google.com/rss/search?q=XRP+Federal+Reserve+policy&hl=en-US&gl=US&ceid=US:en"),
    ("GN: XRP Inflation",           "https://news.google.com/rss/search?q=XRP+inflation+hedge&hl=en-US&gl=US&ceid=US:en"),
]

NEWS = {"current": [], "weekly": [], "pool": [], "feeds_active": 0, "feeds_total": len(NEWS_FEEDS), "updated": None}

# Regions (match Iteration-1) for Regional Discourse + Global Pulse signals
REGIONS = ["Japan", "Korea", "UAE", "Europe", "India", "LatAm", "Africa", "SEA"]
REGION_FLAGS = {"Japan": "\U0001F1EF\U0001F1F5", "Korea": "\U0001F1F0\U0001F1F7", "UAE": "\U0001F1E6\U0001F1EA",
                "Europe": "\U0001F1EA\U0001F1FA", "India": "\U0001F1EE\U0001F1F3", "LatAm": "\U0001F30E",
                "Africa": "\U0001F30D", "SEA": "\U0001F30F"}
REGION_KEYWORDS = {
    "Japan":  ["japan", "japanese", "sbi", "bitflyer", "coincheck", "jpn", "yen"],
    "Korea":  ["korea", "korean", "upbit", "bithumb", "coinone", "korbit", "krw"],
    "UAE":    ["uae", "dubai", "abu dhabi", "emirates", "difc", "vara", "middle east"],
    "Europe": ["europe", "european", " eu ", "mica", "ecb", " uk ", "britain", "germany", "france", "swiss", "spain"],
    "India":  ["india", "indian", "wazirx", "coinswitch", "coindcx", "inr", "sebi", "rbi"],
    "LatAm":  ["latin", "latam", "mexico", "brazil", "argentina", "colombia", "peru", "chile", "bitso"],
    "Africa": ["africa", "nigeria", "kenya", "south africa", "ghana", "ethiopia", "naira"],
    "SEA":    ["singapore", "thailand", "vietnam", "philippines", "indonesia", "malaysia", "tranglo"],
}
US_KEYWORDS = {"sec", "cftc", "etf", "congress", "senate", "white house", "united states",
               "nasdaq", "blackrock", "fidelity", "treasury", "washington", "u.s.", "american"}

def _classify_region(text_low):
    for region, kws in REGION_KEYWORDS.items():
        if any(kw in text_low for kw in kws):
            return region
    return None

_BULLISH = {"surge","surges","rally","rallies","soar","soars","jump","jumps","gain","gains",
            "bullish","approved","approval","win","wins","victory","adoption","partnership",
            "breakout","launch","launches","integration","etf","upgrade","record","high","boost"}
_BEARISH = {"crash","crashes","plunge","plunges","plummet","drop","drops","fall","falls","dump",
            "bearish","lawsuit","warning","hack","hacked","selloff","decline","declines","fud",
            "dip","fine","sued","delay","rejected","ban","risk","fear"}
_IMPORTANT = {"sec","etf","ruling","settlement","partnership","ripple","swift","billion",
              "approved","launch","lawsuit","court","bank","institutional","cbdc","blackrock",
              "nasdaq","fidelity","tokenization","rlusd","custody"}
_SOURCE_WEIGHT = {"CoinDesk":5,"Cointelegraph":5,"Decrypt":4,"The Daily Hodl":3,"U.Today":3,
                  "CryptoSlate":3,"Bitcoinist":2,"NewsBTC":2,"CryptoPotato":2,"AMBCrypto":2}


def _ln(tag):
    return tag.split('}')[-1] if '}' in tag else tag

def _parse_feed(content):
    root = ET.fromstring(content)
    out = []
    for node in root.iter():
        if _ln(node.tag) in ("item", "entry"):
            title = link = date_str = summary = ""
            for ch in node:
                c = _ln(ch.tag)
                if c == "title":
                    title = (ch.text or "").strip()
                elif c == "link":
                    if ch.text and ch.text.strip():
                        link = ch.text.strip()
                    elif ch.get("href"):
                        link = ch.get("href")
                elif c in ("pubDate", "published", "updated", "date") and not date_str:
                    date_str = (ch.text or "").strip()
                elif c in ("description", "summary", "content") and not summary:
                    summary = (ch.text or "")
            out.append({"title": title, "link": link, "date_str": date_str, "summary": summary})
    return out

def _parse_date(s):
    if not s:
        return None
    try:
        return parsedate_to_datetime(s)
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def _sentiment(text):
    t = text.lower()
    b = sum(1 for w in _BULLISH if w in t)
    r = sum(1 for w in _BEARISH if w in t)
    if b > r:
        return "bullish"
    if r > b:
        return "bearish"
    return "neutral"

def _influence(text, source):
    kw = sum(1 for w in _IMPORTANT if w in text.lower())
    return _SOURCE_WEIGHT.get(source, 1) * 2 + kw * 3

_BREAKING_KW = {"breaking", "just in", "urgent", "alert", "confirmed", "official"}

def _category(text):
    t = text.lower()
    if any(k in t for k in ["whale", "million xrp", "billion xrp", "large transfer", "moved xrp"]):
        return "Whale"
    if any(k in t for k in ["sec", "court", "lawsuit", "ruling", "settlement", "judge", "legal", "appeal"]):
        return "Legal"
    if any(k in t for k in ["regulat", "mica", "cftc", "policy", "license", "compliance", "sanction"]):
        return "Reg"
    if any(k in t for k in ["rlusd", "amm", "defi", "partnership", "tokeniz", "stablecoin", "adoption", "nft", "ecosystem"]):
        return "Ecosystem"
    if any(k in t for k in ["xrpl", "ledger", "upgrade", "hooks", "evm", "validator", "amendment"]):
        return "Tech"
    if any(k in t for k in ["price", "surge", "rally", "dump", "plunge", "target", "forecast", "breakout"]):
        return "Price"
    return "General"

def _is_foreign(text):
    if not text:
        return False
    non_ascii = sum(1 for c in text if ord(c) > 127)
    return (non_ascii / max(len(text), 1)) > 0.12

def _is_breaking(text, influence):
    return any(k in text.lower() for k in _BREAKING_KW) or influence >= 22

def _clean_summary(raw, limit=240):
    if not raw:
        return ""
    txt = re.sub(r"<[^>]+>", "", raw)          # strip HTML tags
    txt = re.sub(r"\s+", " ", txt).strip()      # collapse whitespace
    if len(txt) > limit:
        txt = txt[:limit].rsplit(" ", 1)[0] + "\u2026"
    return txt

def _translate_url(link):
    return "https://translate.google.com/translate?sl=auto&tl=en&u=" + html.escape(link, quote=True)

def _fetch_one_feed(name, url):
    """Fetch a single feed via network only. No shared state. Thread-safe."""
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 XRPRadar/26"}, timeout=6)
        if r.status_code != 200:
            return name, []
        return name, _parse_feed(r.content)
    except Exception:
        return name, []

def fetch_news():
    now = datetime.now(timezone.utc)
    active = 0
    seen = set()
    pool = []
    # Fetch all feeds in parallel — network I/O only, no shared state in threads
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_fetch_one_feed, name, url): name for name, url in NEWS_FEEDS}
        results = {}
        for future in as_completed(futures):
            name, entries = future.result()
            results[name] = entries
    # Process results serially — all state updates single-threaded
    for name, url in NEWS_FEEDS:
        entries = results.get(name, [])
        got = False
        for e in entries:
            title = e["title"]
            if not title:
                continue
            text = title + " " + e["summary"]
            low = text.lower()
            if "xrp" not in low and "ripple" not in low and "\u30ea\u30c3\u30d7\u30eb" not in text:
                continue
            key = title.lower()[:80]
            if key in seen:
                continue
            seen.add(key)
            dt = _parse_date(e["date_str"]) or now
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            infl = _influence(text, name)
            summary = _clean_summary(e["summary"])
            pool.append({
                "key": key, "title": title, "link": e["link"] or "#", "source": name, "dt": dt,
                "sentiment": _sentiment(text), "influence": infl,
                "region": _classify_region(low),
                "summary": summary,
                "category": _category(title + " " + summary),
                "foreign": _is_foreign(title),
                "breaking": _is_breaking(text, infl),
            })
            got = True
        if got:
            active += 1

    NEWS["pool"] = pool
    # Influential = the week's 20 most influential (takes priority so it always fills to 20)
    week_ago = now.timestamp() - 7 * 86400
    weekly_pool = [s for s in pool if s["dt"].timestamp() >= week_ago]
    NEWS["weekly"] = sorted(weekly_pool, key=lambda s: (s["influence"], s["dt"].timestamp()), reverse=True)[:20]
    weekly_keys = {s["key"] for s in NEWS["weekly"]}
    # Current = the 20 most recent, EXCLUDING anything already in Influential (no overlap)
    NEWS["current"] = [s for s in sorted(pool, key=lambda s: s["dt"], reverse=True)
                       if s["key"] not in weekly_keys][:20]
    NEWS["feeds_active"] = active
    NEWS["updated"] = now.strftime("%Y-%m-%d %H:%M:%S UTC")
    _track_sentiment_history(pool)
    _detect_partnership_deals(pool)
    _track_catalyst_clock(pool)
    _track_narrative_diffusion(pool)


# ── Narrative Diffusion Map — how fast a theme spreads from first mention to full regional coverage ──
# Reuses the existing theme keywords (Intelligence Brief) and region tags (news engine) already computed
# per story. Persistent accumulator, builds up honestly over time.
NARRATIVE_DIFFUSION = {}   # theme -> {"first_seen": dt, "regions": {region: dt_first_seen_in_region}}
_DIFFUSION_SEEN_KEYS = set()

def _track_narrative_diffusion(pool):
    for s in pool:
        key = s["key"]
        if key in _DIFFUSION_SEEN_KEYS:
            continue
        text = (s["title"] + " " + s.get("summary", "")).lower()
        matched = [name for name, kws in _BRIEF_THEMES.items() if any(kw in text for kw in kws)]
        if not matched:
            continue
        _DIFFUSION_SEEN_KEYS.add(key)
        dt = s["dt"]
        region = s.get("region")
        for theme in matched:
            entry = NARRATIVE_DIFFUSION.setdefault(theme, {"first_seen": dt, "regions": {}})
            if dt < entry["first_seen"]:
                entry["first_seen"] = dt
            if region:
                if region not in entry["regions"] or dt < entry["regions"][region]:
                    entry["regions"][region] = dt


# ── Catalyst Clock — when XRP-moving stories actually break (hour x weekday, UTC) ──
# Persistent accumulator, builds up honestly over time. Counts only stories already
# flagged "breaking" by the existing classifier -- no new definition of "significant" invented.
CATALYST_CLOCK = [[0] * 24 for _ in range(7)]   # [weekday 0=Mon..6=Sun][hour 0-23 UTC]
_CATALYST_SEEN_KEYS = set()
_CATALYST_TOTAL = 0

def _track_catalyst_clock(pool):
    global _CATALYST_TOTAL
    for s in pool:
        if not s.get("breaking"):
            continue
        key = s["key"]
        if key in _CATALYST_SEEN_KEYS:
            continue
        _CATALYST_SEEN_KEYS.add(key)
        try:
            dt = s["dt"].astimezone(timezone.utc)
        except Exception:
            continue
        CATALYST_CLOCK[dt.weekday()][dt.hour] += 1
        _CATALYST_TOTAL += 1


# ── Global XRP Enterprise & Partnership Ledger ──

# ── Static Global Partnership Directory (right rail) — refreshed every 3 days ──
# PLACEHOLDER data structure. Rich will supply the real 100+ entry list; this proves the mechanism.
STATIC_PARTNER_DIRECTORY = {"entries": [], "last_refreshed": None}
STATIC_PARTNER_REFRESH_DAYS = 3

def load_static_partner_directory(force=False):
    """(Re)loads the curated 100+ partnership list on a true 3-day elapsed-time cycle.
    Currently placeholder pending Rich's real list. Purely static/curated data — no external API call."""
    now = datetime.now(timezone.utc)
    last = STATIC_PARTNER_DIRECTORY.get("_last_dt")
    due = force or not last or (now - last).days >= STATIC_PARTNER_REFRESH_DAYS
    if not due and STATIC_PARTNER_DIRECTORY["entries"]:
        return
    STATIC_PARTNER_DIRECTORY["entries"] = [
        ("AMINA Bank", "FINMA-regulated digital asset institution with live native Ripple Payments", "ODL/XRP Live", "🚀", "🇨🇭"),
        ("Azimo", "International digital money transmitter processing enterprise payouts", "ODL/XRP Live", "🚀", "🇪🇺"),
        ("Bitso", "Core liquidity hub routing heavy institutional USD-to-MXN lanes", "ODL/XRP Live", "🚀", "🇲🇽"),
        ("BTC Markets", "Currency bridge managing the AUD leg of regional ODL clearing", "ODL/XRP Live", "🚀", "🇦🇺"),
        ("ChinaBank", "Clears Gulf-region corporate payments anchored to digital liquidity", "ODL/XRP Live", "🚀", "🇵🇭"),
        ("CIBC", "Settles institutional growth transfers via ODL infrastructure", "ODL/XRP Live", "🚀", "🇨🇦"),
        ("Coins.ph", "Digital consumer network handling incoming XRP liquid conversions", "ODL/XRP Live", "🚀", "🇵🇭"),
        ("Cuallix", "First fintech to pilot original xRapid/ODL settlement engines", "ODL/XRP Live", "🚀", "🇺🇸"),
        ("FlashFX", "Automated FX software routing transfers via on-chain token paths", "ODL/XRP Live", "🚀", "🇦🇺"),
        ("Independent Reserve", "Regional liquidity exchange partner providing settlement architecture", "ODL/XRP Live", "🚀", "🇦🇺"),
        ("iRemit", "Non-bank remittance giant using ledger for real-time treasury management", "ODL/XRP Live", "🚀", "🇵🇭"),
        ("Mercury FX", "Enterprise currency platform processing instant commercial payments via XRP", "ODL/XRP Live", "🚀", "🇬🇧"),
        ("MoneyMatch", "Digital conversion firm routing commercial payments to European endpoints", "ODL/XRP Live", "🚀", "🇲🇾"),
        ("Novatti", "Payments processor using XRP ledger routes for Southeast Asian corridors", "ODL/XRP Live", "🚀", "🇦🇺"),
        ("Pyypl", "Blockchain fintech offering consumer digital wallets via ODL", "ODL/XRP Live", "🚀", "🌍"),
        ("Qatar National Bank", "Cross-border pipeline targeting Philippine remittance partners", "ODL/XRP Live", "🚀", "🇶🇦"),
        ("SBI Remit / SBI Holdings", "Multi-corridor APAC retail & commercial remittance powered by XRP", "ODL/XRP Live", "🚀", "🇯🇵"),
        ("Siam Commercial Bank", "Active live ODL corridors for inbound Japanese capital", "ODL/XRP Live", "🚀", "🇹🇭"),
        ("Tranglo", "Regional processing giant fully integrated into ODL", "ODL/XRP Live", "🚀", "🇲🇾"),
        ("Travelex Bank", "First operational Latin American bank using XRP liquidity corridors", "ODL/XRP Live", "🚀", "🇧🇷"),
        ("UnionBank", "Automated processing for inbound domestic overseas worker remittances", "ODL/XRP Live", "🚀", "🇵🇭"),
        ("X Money", "Retail cross-border digital financial platform using decentralized settlement", "ODL/XRP Live", "🚀", "🌐"),
        ("Zand Bank", "Digital corporate bank processing payments via XRP and RLUSD", "ODL/XRP Live", "🚀", "🇦🇪"),
        ("Akbank", "Early regional banking partner conducting secure real-time automated tests", "Global Banks", "🏛️", "🇹🇷"),
        ("American Express", "Commercial B2B international payments clearing partner", "Global Banks", "🏛️", "🇺🇸"),
        ("ANZ Bank", "Historical testing partner of the underlying clearing protocol", "Global Banks", "🏛️", "🇦🇺"),
        ("Axis Bank", "Live infrastructure client managing real-time regional transaction tunnels", "Global Banks", "🏛️", "🇮🇳"),
        ("Banco Santander", "Powers international One Pay FX app via RippleNet messaging", "Global Banks", "🏛️", "🇪🇸"),
        ("Bank of America", "Infrastructure pilot participant holding patents referencing XRP settlement", "Global Banks", "🏛️", "🇺🇸"),
        ("BBVA", "Corporate banking implementing cross-border branch liquidity trials", "Global Banks", "🏛️", "🇪🇸"),
        ("BDO Unibank", "Major destination settlement point for international inbound money streams", "Global Banks", "🏛️", "🇵🇭"),
        ("BMO Financial Group", "North American commercial entity exploring cross-border clearing efficiency", "Global Banks", "🏛️", "🇨🇦"),
        ("CIMB Bank", "Deep integration node managing corridors across ASEAN borders", "Global Banks", "🏛️", "🇲🇾"),
        ("Commonwealth Bank (CBA)", "Major retail institution participating in pilot ecosystem networks", "Global Banks", "🏛️", "🇦🇺"),
        ("Deutsche Bank", "Combined Ripple blockchain architecture with legacy SWIFT mechanisms", "Global Banks", "🏛️", "🇩🇪"),
        ("Federal Bank", "Major localized retail bank utilizing automated routing systems", "Global Banks", "🏛️", "🇮🇳"),
        ("HSBC", "Multi-national banking network mapped via active system routing IDs", "Global Banks", "🏛️", "🇬🇧"),
        ("IndusInd Bank", "Captures inbound international money transfers using decentralized engines", "Global Banks", "🏛️", "🇮🇳"),
        ("ING Group", "Multi-national bank registered in regional backend messaging directories", "Global Banks", "🏛️", "🇳🇱"),
        ("Intesa Sanpaolo", "Enterprise participant tracking structural digital payment innovations", "Global Banks", "🏛️", "🇮🇹"),
        ("JPMorgan Chase", "Overlapping participant in multi-network settlement ledger groups", "Global Banks", "🏛️", "🌐"),
        ("Kotak Mahindra Bank", "Fintech clearing provider handling instant retail capital inflows", "Global Banks", "🏛️", "🇮🇳"),
        ("Krungsri (Bank of Ayudhya)", "Streamlines real-time corporate pipelines between Thailand and Japan", "Global Banks", "🏛️", "🇹🇭"),
        ("Macquarie Bank", "Financial and transaction group listed on official routing logs", "Global Banks", "🏛️", "🇦🇺"),
        ("MUFG Bank", "Tier-1 retail giant optimizing transaction messaging across APAC", "Global Banks", "🏛️", "🇯🇵"),
        ("National Australia Bank (NAB)", "Incorporated into the ledger settlement network indexing systems", "Global Banks", "🏛️", "🇦🇺"),
        ("PNC Bank", "First major domestic U.S. institutional network client", "Global Banks", "🏛️", "🇺🇸"),
        ("Royal Bank of Canada (RBC)", "Explored the decentralized rail protocol for automated settlement", "Global Banks", "🏛️", "🇨🇦"),
        ("SEB", "Operates high-volume corporate lines over Ripple software rails", "Global Banks", "🏛️", "🇸🇪"),
        ("Shinhan Bank", "Top South Korean network client maintaining active system access keys", "Global Banks", "🏛️", "🇰🇷"),
        ("Standard Chartered", "Core early corporate investor and active digital clearing hub collaborator", "Global Banks", "🏛️", "🇬🇧"),
        ("UBS", "Asset and investment firm evaluating high-speed distributed ledgers", "Global Banks", "🏛️", "🇨🇭"),
        ("Westpac", "Registered network member maintaining live backend communication IDs", "Global Banks", "🏛️", "🇦🇺"),
        ("Woori Bank", "Multi-channel asset institution utilizing programmatic payment lines", "Global Banks", "🏛️", "🇰🇷"),
        ("Yes Bank", "Commercial institution conducting high-velocity payment remittance operations", "Global Banks", "🏛️", "🇮🇳"),
        ("Accenture", "Consulting giant managing global deployment strategies for payment architecture", "Tech/Custody", "🛠️", "🌐"),
        ("Amazon Web Services (AWS)", "Hosts architecture allowing global nodes to run XRPL validation configurations", "Tech/Custody", "🛠️", "🌐"),
        ("BDACS", "Regulated secure vault platform for native ledger token storage", "Tech/Custody", "🛠️", "🇰🇷"),
        ("BeeTech", "Digital financial operator executing automated Latin American clearings", "Tech/Custody", "🛠️", "🇧🇷"),
        ("BNY Mellon", "Primary tier-1 institutional reserve custodian for stablecoin offerings", "Tech/Custody", "🛠️", "🇺🇸"),
        ("CGI Group", "IT consulting firm incorporating decentralized financial frameworks", "Tech/Custody", "🛠️", "🇨🇦"),
        ("Cross River Bank", "Financial tech enabler providing direct underlying banking backbone", "Tech/Custody", "🛠️", "🇺🇸"),
        ("Currencycloud", "B2B multi-currency platform streamlining automated foreign exchange", "Tech/Custody", "🛠️", "🇬🇧"),
        ("DBS Bank", "Southeast Asian institution utilizing bank-grade digital asset vaults", "Tech/Custody", "🛠️", "🇸🇬"),
        ("Deloitte", "Integrated distributed financial systems into client business models", "Tech/Custody", "🛠️", "🌐"),
        ("DZ Bank", "Leverages digital custody solutions for tokenized asset issuance", "Tech/Custody", "🛠️", "🇩🇪"),
        ("Fidor Bank", "Digital banking pioneer integrating alternative clearing protocol tools", "Tech/Custody", "🛠️", "🇩🇪"),
        ("Finastra", "Core banking software opening network access to 2,000+ regional banks", "Tech/Custody", "🛠️", "🇬🇧"),
        ("Frankenmuth Credit Union", "Local cooperative providing digital asset services to local consumers", "Tech/Custody", "🛠️", "🇺🇸"),
        ("GTreasury", "Corporate liquidity software suite managing modern capital balance sheets", "Tech/Custody", "🛠️", "🇺🇸"),
        ("Hidden Road", "Major institutional prime brokerage expanding liquidity paths for digital assets", "Tech/Custody", "🛠️", "🇺🇸"),
        ("InstaReM", "High-speed digital payment gateway connected via localized nodes", "Tech/Custody", "🛠️", "🇸🇬"),
        ("Kbank", "Digital platform implementing secure cryptographic wallet structures", "Tech/Custody", "🛠️", "🇰🇷"),
        ("Kyobo Life Insurance", "Utilizing token ledger blueprint for corporate structural bond settlement", "Tech/Custody", "🛠️", "🇰🇷"),
        ("Metaco", "Institutional crypto custody firm acquired by Ripple to secure bank assets globally", "Tech/Custody", "🛠️", "🇨🇭"),
        ("Modulr", "Payments provider optimizing massive local commercial transaction times", "Tech/Custody", "🛠️", "🇬🇧"),
        ("Nium", "Fintech provider optimizing massive outbound payment paths across global corridors", "Tech/Custody", "🛠️", "🇸🇬"),
        ("Sabadell", "Commercial infrastructure partner running real-time corporate data modules", "Tech/Custody", "🛠️", "🇪🇸"),
        ("Sentbe", "High-speed international remittance engine using the global banking network", "Tech/Custody", "🛠️", "🇰🇷"),
        ("Temenos", "Core banking software provider embedding automated accounting rails", "Tech/Custody", "🛠️", "🇨🇭"),
        ("Al Ansari Exchange", "High-volume Middle Eastern exchange network routing institutional transfers", "Regional", "🌍", "🇦🇪"),
        ("Banco Rendimento", "Foreign currency commercial bank using optimized digital payment tunnels", "Regional", "🌍", "🇧🇷"),
        ("Bank Alfalah", "Manages automated digital channels targeting the UAE-to-Pakistan corridor", "Regional", "🌍", "🇵🇰"),
        ("bKash", "Mobile financial giant plugged in to capture worker remittances", "Regional", "🌍", "🇧🇩"),
        ("Faysal Bank", "Specialized commercial banking provider processing inward retail cash flows", "Regional", "🌍", "🇵🇰"),
        ("Interbank", "Traditional retail banking destination tied to alternative clearing systems", "Regional", "🌍", "🇵🇪"),
        ("Intercorp", "Large conglomerate stabilizing localized payment legs for regional retail assets", "Regional", "🌍", "🇵🇪"),
        ("Itau Unibanco", "Giant South American banking provider utilizing alternative communication networks", "Regional", "🌍", "🇧🇷"),
        ("National Bank of Fujairah", "Trade finance group optimizing real-time B2B payment workflows", "Regional", "🌍", "🇦🇪"),
        ("National Bank of Kuwait (NBK)", "Runs international corporate transfer paths targeting the Gulf", "Regional", "🌍", "🇰🇼"),
        ("RAKBANK", "Integrates transaction routes to improve speed across enterprise pipelines", "Regional", "🌍", "🇦🇪"),
        ("Saudi Central Bank (SAMA)", "Central entity piloting distributed frameworks for commercial branches", "Regional", "🌍", "🇸🇦"),
        ("Vietcombank", "Explores modern asset frameworks under regional digital banking pilots", "Regional", "🌍", "🇻🇳"),
        ("Bitwise Asset Management", "Regulated Wall Street provider offering institutional XRP exposure", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Canary Capital Partners", "Asset management firm deploying institutional-grade XRP capital avenues", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Franklin Templeton", "Legacy asset firm filing for exchange-traded digital investment products", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Grayscale Investments", "Asset manager operating the regulated Grayscale XRP Trust and spot fund", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Hashdex Asset Management", "Global investment manager offering systemic access to ledger tokens", "ETF/Treasury", "🟡", "🌐"),
        ("Nature's Miracle Holding", "Agriculture Tech firm implementing a $20M Corporate Treasury on the XRPL", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Worksport Ltd.", "Clean automotive developer utilizing digital assets for inventory clearings", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Mastercard", "$9T payment network partnered with Ripple on settlement rails in 2026", "Global Banks", "🏛️", "🌐"),
        ("Banco Genial", "Ripple Payments for cross-border payouts, live 2026", "ODL/XRP Live", "🚀", "🇧🇷"),
        ("Thunes", "Brought stablecoin payouts to 11,500 SWIFT-connected banks via Ripple ODL routing", "Tech/Custody", "🛠️", "🇸🇬"),
        ("SendFriend", "ODL for international remittances", "ODL/XRP Live", "🚀", "🇺🇸"),
        ("Remitr", "RippleNet for cross-border business payments", "ODL/XRP Live", "🚀", "🌐"),
        ("Ondo Finance", "$323M+ tokenized US Treasury products on XRP Ledger", "Tech/Custody", "🛠️", "🇺🇸"),
        ("Archax", "UK-regulated exchange bringing $1B tokenized assets onto XRPL by mid-2026", "Tech/Custody", "🛠️", "🇬🇧"),
        ("Guggenheim Treasury Services", "Tokenized commercial paper / treasury products on XRPL", "Tech/Custody", "🛠️", "🇺🇸"),
        ("OpenEden", "Tokenized US Treasury products on the XRP Ledger", "Tech/Custody", "🛠️", "🇸🇬"),
        ("Zoniqx", "Prepared hundreds of millions in RWA for issuance on XRPL", "Tech/Custody", "🛠️", "🇺🇸"),
        ("abrdn", "£3.8B liquidity fund tokenized on XRPL via Archax (first tokenized MMF)", "ETF/Treasury", "🟡", "🇬🇧"),
        ("Aviva Investors", "Announced tokenization partnership with Ripple in 2026", "ETF/Treasury", "🟡", "🇬🇧"),
        ("Justoken", "Independent RWA tokenization project building on XRPL", "Tech/Custody", "🛠️", "🌐"),
        ("Ctrl Alt", "Partnered with Ripple + Dubai Land Department for real estate tokenization", "Tech/Custody", "🛠️", "🇦🇪"),
        ("Figment", "Staking infrastructure partnership for Ripple Custody (2026)", "Tech/Custody", "🛠️", "🌐"),
        ("Securosys", "HSM support partnership for Ripple Custody (2026)", "Tech/Custody", "🛠️", "🇨🇭"),
        ("Palisade", "Acquired by Ripple to expand custody stack", "Tech/Custody", "🛠️", "🇺🇸"),
        ("Chainalysis", "Compliance tools integrated into Ripple Custody", "Tech/Custody", "🛠️", "🇺🇸"),
        ("Doppler Finance", "Partnered with SBI Ripple Asia for XRP-based institutional yield products", "Tech/Custody", "🛠️", "🌏"),
        ("SBI Digital Markets", "Segregated custody for SBI Ripple Asia XRP yield products", "Tech/Custody", "🛠️", "🇸🇬"),
        ("Royal Monetary Authority of Bhutan", "National CBDC pilot on XRPL since 2021", "Regional", "🌍", "🇧🇹"),
        ("Central Bank of Montenegro", "CBDC pilot exploring blockchain national currency on XRPL", "Regional", "🌍", "🇲🇪"),
        ("Republic of Palau", "National stablecoin built with Ripple on XRPL", "Regional", "🌍", "🇵🇼"),
        ("Banco de la Republica", "Central bank exploring XRPL for digital peso settlement", "Regional", "🌍", "🇨🇴"),
        ("Reserve Bank of Australia", "Project Acacia deployed wholesale CBDC on XRPL in live tests with tokenized govt bonds", "Regional", "🌍", "🇦🇺"),
        ("Monetary Authority of Singapore", "MAS sandbox projects using RLUSD for programmable trade finance", "Regional", "🌍", "🇸🇬"),
        ("Hong Kong Monetary Authority", "e-HKD CBDC pilots involving XRPL infrastructure", "Regional", "🌍", "🇭🇰"),
        ("Dubai Land Department", "Real estate tokenization on XRPL with Ripple + Ctrl Alt (2025)", "Regional", "🌍", "🇦🇪"),
        ("21Shares", "Live XRP ETP issuer", "ETF/Treasury", "🟡", "🇨🇭"),
        ("CoinShares", "Live XRP exchange-traded product issuer", "ETF/Treasury", "🟡", "🇪🇺"),
        ("WisdomTree", "XRP ETF issuer", "ETF/Treasury", "🟡", "🇺🇸"),
        ("VanEck", "Live XRP ETF issuer", "ETF/Treasury", "🟡", "🇺🇸"),
        ("ProShares", "XRP futures/ETF product under review", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Volatility Shares", "XRP futures ETF issuer", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Teucrium", "Launched 2x leveraged XRP ETF", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Goldman Sachs", "Reported largest institutional XRP holder in the US", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Societe Generale (SG-FORGE)", "Launched EUR CoinVertible euro stablecoin on XRPL (Feb 2026)", "Global Banks", "🏛️", "🇫🇷"),
        ("WebBank", "Settles fiat card transactions using RLUSD on XRPL (with Gemini)", "Global Banks", "🏛️", "🇺🇸"),
        ("Gemini", "Card transaction settlement using RLUSD on the XRP Ledger", "Tech/Custody", "🛠️", "🇺🇸"),
        ("Mastercard (RLUSD cards)", "Fiat card settlement via RLUSD on XRPL with WebBank + Gemini", "ETF/Treasury", "🟡", "🌐"),
        ("BlackRock (BUIDL)", "BUIDL fund supported on Ripple Treasury platform routing via XRPL DEX", "ETF/Treasury", "🟡", "🇺🇸"),
        ("Alloy Networks", "Runs an XRPL validator node — signal of active XRP settlement usage", "ODL/XRP Live", "🚀", "🌐"),
        ("Onafriq", "Pan-African payments network using Ripple for cross-border corridors", "Regional", "🌍", "🌍"),
        ("Ripple National Trust Bank", "OCC conditionally approved Dec 2025 — federally chartered trust bank", "Global Banks", "🏛️", "🇺🇸"),
        ("Absa Group", "Major African bank exploring Ripple cross-border infrastructure", "Regional", "🌍", "🇿🇦"),
        ("Fenasbac", "Brazil central bank innovation arm partnered on Ripple pilots", "Regional", "🌍", "🇧🇷"),
        ("DZ Bank Digital", "Digital asset custody pilots involving XRPL infrastructure", "Global Banks", "🏛️", "🇩🇪"),
    ]
    STATIC_PARTNER_DIRECTORY["_last_dt"] = now
    STATIC_PARTNER_DIRECTORY["last_refreshed"] = now.strftime("%Y-%m-%d %H:%M UTC")

load_static_partner_directory()
# Ever-growing, never-trimmed. Seed = 100 known entities (undated baseline).
# New entries detected from the live news feed get real timestamps and always sort above the baseline.
PARTNERSHIP_LEDGER = []          # list of dicts: name, country, cat, status, detail, date(None or datetime), source, key
_PARTNERSHIP_SEEDED = False
_PARTNERSHIP_SEEN_KEYS = set()
_PARTNERSHIP_DEAL_KW = ["partner", "partnership", "collaborat", "agreement", "signs", "joins forces",
                        "integrat", "teams up", "merger", "acquisition", "acquires", "deal with",
                        "onboards", "adopts xrp", "adopts ripple"]

def seed_partnership_ledger():
    global _PARTNERSHIP_SEEDED
    if _PARTNERSHIP_SEEDED:
        return
    for name, country, cat, status, detail in ENTERPRISE_SEED:
        PARTNERSHIP_LEDGER.append({
            "key": f"seed:{name.lower()}", "name": name, "country": country, "cat": cat,
            "status": status, "detail": detail, "date": None, "source": "baseline", "link": None,
        })
    _PARTNERSHIP_SEEDED = True

def _detect_partnership_deals(pool):
    for s in pool:
        key = s["key"]
        if key in _PARTNERSHIP_SEEN_KEYS:
            continue
        text = (s["title"] + " " + s.get("summary", "")).lower()
        if s.get("category") != "Ecosystem":
            continue
        if not any(kw in text for kw in _PARTNERSHIP_DEAL_KW):
            continue
        _PARTNERSHIP_SEEN_KEYS.add(key)
        PARTNERSHIP_LEDGER.append({
            "key": f"news:{key}", "name": s["title"], "country": None, "cat": "N",
            "status": "NEW", "detail": s.get("summary", "") or s["source"], "date": s["dt"],
            "source": "detected", "link": s.get("link"),
        })

def partnership_ledger_html(limit=30):
    detected = sorted((e for e in PARTNERSHIP_LEDGER if e["source"] == "detected"),
                       key=lambda e: e["date"], reverse=True)
    baseline = [e for e in PARTNERSHIP_LEDGER if e["source"] == "baseline"]
    ordered = (detected + baseline)[:limit]
    if not ordered:
        return '<div class="empty">Directory loading\u2026</div>'
    out = ""
    for e in ordered:
        col = ENTERPRISE_CATEGORY_COLORS.get(e["cat"], "var(--tx)")
        if e["source"] == "detected":
            badge = '<span class="pl-new">\U0001F195 NEW</span>'
            when = _time_ago(e["date"])
            title_html = (f'<a href="{html.escape(e["link"] or "#", quote=True)}" target="_blank" rel="noopener">'
                          f'{html.escape(e["name"])}</a>')
            meta = html.escape(e["detail"][:140])
        else:
            badge = ""
            when = "Established"
            title_html = html.escape(e["name"])
            meta = f'{html.escape(e["country"] or "")} \u2014 {html.escape(e["detail"])}'
        out += (
            f'<div class="pl-row" data-cat="{e["cat"]}" data-text="{html.escape((e["name"] + " " + (e["country"] or "") + " " + e["detail"]).lower(), quote=True)}">'
            f'<div class="pl-top"><span class="pl-cat" style="color:{col}">{ENTERPRISE_CATEGORY_LABELS.get(e["cat"], "\U0001F195 New Deal")}</span>'
            f'{badge}<span class="pl-status" style="color:{col}">{html.escape(e["status"])}</span>'
            f'<span class="pl-when">{when}</span></div>'
            f'<div class="pl-name">{title_html}</div>'
            f'<div class="pl-meta">{meta}</div>'
            f'</div>'
        )
    return out


SENTIMENT_HISTORY = {}   # date_str -> {"bull","bear","neut","total","_keys"}
SENTIMENT_HISTORY_MAX = 30

def tech_specs_html():
    out = ""
    for metric, xrpl, eth, sol, btc in TECH_SPECS:
        out += (
            f'<tr><td style="padding:6px;color:var(--br)">{metric}</td>'
            f'<td style="text-align:center;padding:6px;color:var(--gr);font-weight:700">{xrpl}</td>'
            f'<td style="text-align:center;padding:6px;color:var(--tx)">{eth}</td>'
            f'<td style="text-align:center;padding:6px;color:var(--tx)">{sol}</td>'
            f'<td style="text-align:center;padding:6px;color:var(--tx)">{btc}</td></tr>'
        )
    return out

def use_case_html():
    out = ""
    for icon, title, col, detail in USE_CASES:
        out += (
            f'<div class="uc-card" style="border-left-color:{col}">'
            f'<div class="uc-title" style="color:{col}">{icon} {title}</div>'
            f'<div class="uc-detail">{detail}</div></div>'
        )
    return out

def ad_line_html():
    d7 = MARKET.get("ad_7d_delta")
    d30 = MARKET.get("ad_30d_delta")
    def _sig(delta):
        if delta is None:
            return "\u2014", "var(--tx)"
        return ("\U0001F7E2 Accumulation", "var(--gr)") if delta > 0 else ("\U0001F534 Distribution", "var(--rd)")
    s7, c7 = _sig(d7)
    s30, c30 = _sig(d30)
    return s7, c7, s30, c30

def correlation_html():
    def _row(label, val):
        if val is None:
            return f'<div class="corr-row"><span>{label}</span><span style="color:var(--tx)">\u2014</span></div>'
        col = "var(--gr)" if val >= 0 else "var(--rd)"
        sign = "+" if val >= 0 else ""
        lbl = "positive" if val >= 0 else "inverse"
        return (f'<div class="corr-row"><span>{label}</span>'
                f'<span style="color:{col}">{sign}{val:.2f} <small style="color:var(--tx)">({lbl})</small></span></div>')
    return _row("XRP vs BTC", MARKET.get("corr_btc")) + _row("XRP vs ETH", MARKET.get("corr_eth"))

def orderbook_html():
    bids = MARKET.get("ob_bids") or []
    asks = MARKET.get("ob_asks") or []
    if not bids or not asks:
        return ('<div class="home-base"><div class="home-base-icon">\U0001F4CA</div>'
                '<div class="home-base-title">Loading Order Book</div>'
                '<div class="home-base-sub">Live bid/ask depth from Binance populates on deploy.</div></div>', "", "\u2014", "\u2014")
    all_sizes = [q for _, q in bids] + [q for _, q in asks]
    mx = max(all_sizes) or 1
    bid_rows = "".join(
        f'<div class="ob-row"><span class="ob-price gr">${p:.4f}</span>'
        f'<div class="ob-bar-wrap"><div class="ob-bar gr" style="width:{q/mx*100:.0f}%"></div></div>'
        f'<span class="ob-qty">{q:,.0f}</span></div>' for p, q in bids)
    ask_rows = "".join(
        f'<div class="ob-row"><span class="ob-price rd">${p:.4f}</span>'
        f'<div class="ob-bar-wrap"><div class="ob-bar rd" style="width:{q/mx*100:.0f}%"></div></div>'
        f'<span class="ob-qty">{q:,.0f}</span></div>' for p, q in asks)
    bid_total = _fmt_usd(MARKET.get("ob_bid_total"))
    ask_total = _fmt_usd(MARKET.get("ob_ask_total"))
    return bid_rows, ask_rows, bid_total, ask_total

def liquidity_map_html():
    bids = MARKET.get("ob_bids") or []
    asks = MARKET.get("ob_asks") or []
    if not bids or not asks:
        return '<div class="empty">Liquidity data populates on deploy.</div>'
    bid_val = sum(p * q for p, q in bids)
    ask_val = sum(p * q for p, q in asks)
    total = bid_val + ask_val
    bid_pct = round(bid_val / total * 100) if total else 50
    ask_pct = 100 - bid_pct
    skew = "Buy-side heavier" if bid_pct > 55 else ("Sell-side heavier" if ask_pct > 55 else "Balanced")
    return (
        f'<div class="liq-bar"><div class="liq-fill" style="width:{bid_pct}%"></div></div>'
        f'<div class="liq-labels"><span style="color:var(--gr)">{bid_pct}% bids</span>'
        f'<span style="color:var(--rd)">{ask_pct}% asks</span></div>'
        f'<div class="liq-skew">{skew}</div>'
        f'<div class="liq-note">Top 8 levels each side \u00B7 Binance XRP/USDT</div>'
    )


def clarity_tracker_html():
    stories = sorted(CLARITY_ACT_STORIES, key=lambda s: s["dt"], reverse=True)
    if not stories:
        return ('<div class="home-base"><div class="home-base-icon">\U0001F3DB\uFE0F</div>'
                '<div class="home-base-title">Monitoring the CLARITY Act</div>'
                '<div class="home-base-sub">The 10 most recent stories on the bill\u2019s progress through the '
                'Senate will appear here automatically as they\u2019re published.</div></div>')
    out = ""
    for i, s in enumerate(stories, 1):
        out += (
            f'<div class="ca-row"><div class="ca-rank">#{i}</div><div class="ca-body">'
            f'<div class="ca-top"><span class="ca-src">{html.escape(s["source"])}</span>'
            f'<span class="ca-time">{_time_ago(s["dt"])}</span></div>'
            f'<a class="ca-hl" href="{html.escape(s["link"], quote=True)}" target="_blank" rel="noopener">'
            f'{html.escape(s["title"])}</a></div></div>'
        )
    return out


_WEEKDAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

def narrative_diffusion_html(limit=6):
    if not NARRATIVE_DIFFUSION:
        return ('<div class="home-base"><div class="home-base-icon">\U0001F30D</div>'
                '<div class="home-base-title">Monitoring Narrative Spread</div>'
                '<div class="home-base-sub">As themes emerge and reach multiple regions, their spread timeline '
                'will appear here automatically.</div></div>', "\u2014")

    themes = sorted(NARRATIVE_DIFFUSION.items(),
                     key=lambda kv: (len(kv[1]["regions"]), kv[1]["first_seen"]), reverse=True)[:limit]
    now = datetime.now(timezone.utc)
    cards = ""
    fastest_theme, fastest_span = None, None
    for theme, data in NARRATIVE_DIFFUSION.items():
        regs = data["regions"]
        if len(regs) >= 2:
            span = max(regs.values()) - min(regs.values())
            if fastest_span is None or span < fastest_span:
                fastest_span, fastest_theme = span, theme

    for theme, data in themes:
        age = _time_ago(data["first_seen"])
        regs_sorted = sorted(data["regions"].items(), key=lambda kv: kv[1])
        n_regs = len(regs_sorted)
        chips = ""
        for region, dt in regs_sorted:
            lag_sec = (dt - data["first_seen"]).total_seconds()
            lag_txt = "first" if lag_sec < 60 else f"+{int(lag_sec // 3600)}h" if lag_sec >= 3600 else f"+{int(lag_sec // 60)}m"
            chips += (f'<span class="nd-chip">{REGION_FLAGS.get(region, "")} {region} '
                      f'<span class="nd-lag">{lag_txt}</span></span>')
        spread_note = (f'Reached {n_regs} regions' if n_regs >= 2 else 'Still regional \u2014 1 region so far')
        cards += (
            f'<div class="nd-card"><div class="nd-top"><span class="nd-theme">{html.escape(theme)}</span>'
            f'<span class="nd-age">first seen {age}</span></div>'
            f'<div class="nd-chips">{chips}</div>'
            f'<div class="nd-note">{spread_note}</div></div>'
        )

    if fastest_theme and fastest_span:
        h = fastest_span.total_seconds() / 3600
        fastest_txt = f'"{fastest_theme}" reached multiple regions in {h:.1f}h' if h >= 1 else f'"{fastest_theme}" reached multiple regions in {int(fastest_span.total_seconds()//60)}m'
    else:
        fastest_txt = "\u2014 (building up)"
    return cards, fastest_txt


def catalyst_clock_html():
    mx = max(max(row) for row in CATALYST_CLOCK) or 1
    cells = ""
    for wd in range(7):
        cells += f'<div class="cc-row"><span class="cc-daylbl">{_WEEKDAY_LABELS[wd]}</span>'
        for hr in range(24):
            v = CATALYST_CLOCK[wd][hr]
            inten = v / mx if mx else 0
            if v == 0:
                bg = "var(--s2)"
            else:
                bg = f"rgba(255,153,0,{0.15 + inten * 0.75:.2f})"
            cells += f'<div class="cc-cell" style="background:{bg}" title="{_WEEKDAY_LABELS[wd]} {hr:02d}:00 UTC \u2014 {v} breaking stor{"y" if v == 1 else "ies"}"></div>'
        cells += '</div>'

    # Peak hour / weekday
    hour_totals = [sum(CATALYST_CLOCK[wd][hr] for wd in range(7)) for hr in range(24)]
    day_totals = [sum(CATALYST_CLOCK[wd]) for wd in range(7)]
    if _CATALYST_TOTAL:
        peak_hr = hour_totals.index(max(hour_totals))
        peak_day = _WEEKDAY_LABELS[day_totals.index(max(day_totals))]
        peak_txt = f"{peak_hr:02d}:00 UTC on {peak_day}s"
    else:
        peak_txt = "\u2014 (building up)"
    hour_lbls = "".join(f'<span class="cc-hourlbl">{h if h % 3 == 0 else ""}</span>' for h in range(24))
    return cells, peak_txt, hour_lbls


def ici_comps_html(comps):
    out = ""
    for name, detail, pts in comps:
        pct = round(pts / 20 * 100)
        out += (
            f'<div class="ici-comp-row"><span class="ici-comp-name">{html.escape(name)}</span>'
            f'<div class="ici-comp-track"><div class="ici-comp-fill" style="width:{pct}%"></div></div>'
            f'<span class="ici-comp-pts">{pts}/20</span></div>'
            f'<div class="ici-comp-detail">{html.escape(detail)}</div>'
        )
    return out


def partnership_momentum_html(weeks=10):
    """Deals detected per week, bucketed from our own Enterprise Ledger timestamps.
    Builds up honestly over time -- no fabricated history."""
    now = datetime.now(timezone.utc)
    detected = [e for e in PARTNERSHIP_LEDGER if e.get("source") == "detected" and e.get("date")]
    buckets = [0] * weeks
    for e in detected:
        age_days = (now - e["date"]).days
        week_idx = weeks - 1 - (age_days // 7)
        if 0 <= week_idx < weeks:
            buckets[week_idx] += 1
    mx = max(buckets) or 1
    bars = "".join(
        f'<div class="pm-bar" style="height:{max(6, v / mx * 100):.0f}%" title="{v} deal{"s" if v != 1 else ""}"></div>'
        for v in buckets
    )
    total = len(detected)
    this_week = buckets[-1]
    last_week = buckets[-2] if weeks >= 2 else 0
    if this_week > last_week:
        trend, tcol = f"\u25B2 up from {last_week} last week", "var(--gr)"
    elif this_week < last_week:
        trend, tcol = f"\u25BC down from {last_week} last week", "var(--rd)"
    else:
        trend, tcol = "\u2192 steady week over week", "var(--tx)"
    avg = round(total / weeks, 1) if total else 0.0
    return bars, total, this_week, trend, tcol, avg


def _track_sentiment_history(pool):
    for s in pool:
        try:
            day = s["dt"].astimezone(timezone.utc).date().isoformat()
        except Exception:
            continue
        bucket = SENTIMENT_HISTORY.setdefault(
            day, {"bull": 0, "bear": 0, "neut": 0, "total": 0, "_keys": set()})
        if s["key"] in bucket["_keys"]:
            continue
        bucket["_keys"].add(s["key"])
        bucket["total"] += 1
        if s["sentiment"] == "bullish":
            bucket["bull"] += 1
        elif s["sentiment"] == "bearish":
            bucket["bear"] += 1
        else:
            bucket["neut"] += 1
    if len(SENTIMENT_HISTORY) > SENTIMENT_HISTORY_MAX:
        for old_day in sorted(SENTIMENT_HISTORY.keys())[:len(SENTIMENT_HISTORY) - SENTIMENT_HISTORY_MAX]:
            del SENTIMENT_HISTORY[old_day]


def news_velocity_24h():
    """Stories per hour for the last 24h, oldest -> newest (24 buckets)."""
    now = datetime.now(timezone.utc)
    buckets = [0] * 24
    for s in NEWS.get("pool", []):
        try:
            hrs_ago = (now - s["dt"]).total_seconds() / 3600
            if 0 <= hrs_ago < 24:
                buckets[23 - int(hrs_ago)] += 1
        except Exception:
            continue
    return buckets


def interest_score():
    """XRP interest score (0-100), honestly derived from our own feed velocity
    (Iteration-1 used this exact approach as its fallback when Google Trends was unavailable)."""
    now = datetime.now(timezone.utc)
    pool = NEWS.get("pool", [])
    recent_6h = sum(1 for s in pool if (now - s["dt"]).total_seconds() < 21600)
    score = min(recent_6h * 6 + min(len(pool), 20), 100)
    if score > 70:
        label = "\U0001F525 Trending"
    elif score > 40:
        label = "\U0001F4C8 Rising"
    elif score > 15:
        label = "\U0001F634 Quiet"
    else:
        label = "\U0001F4A4 Minimal"
    return score, label


def sentiment_source_table(n=15):
    pool = NEWS.get("pool", [])
    agg = {}
    for s in pool:
        e = agg.setdefault(s["source"], {"name": s["source"], "total": 0, "bull": 0, "bear": 0, "breaking": 0})
        e["total"] += 1
        if s["sentiment"] == "bullish":
            e["bull"] += 1
        if s["sentiment"] == "bearish":
            e["bear"] += 1
        if s.get("breaking"):
            e["breaking"] += 1
    return sorted(agg.values(), key=lambda x: x["total"], reverse=True)[:n]


def _time_ago(dt):
    secs = (datetime.now(timezone.utc) - dt).total_seconds()
    if secs < 3600:
        return f"{int(secs // 60)}m ago"
    if secs < 86400:
        return f"{int(secs // 3600)}h ago"
    return f"{int(secs // 86400)}d ago"

def story_rows_html(stories):
    if not stories:
        return '<div class="empty">Connecting to news feeds\u2026 headlines populate on deploy.</div>'
    sent_col = {"bullish": "var(--gr)", "bearish": "var(--rd)", "neutral": "var(--tx)"}
    out = ""
    for i, s in enumerate(stories, 1):
        col = sent_col.get(s["sentiment"], "var(--tx)")
        out += (
            f'<a class="story" href="{html.escape(s["link"], quote=True)}" target="_blank" rel="noopener">'
            f'<span class="story-num">{i}</span>'
            f'<span class="story-body">'
            f'<span class="story-hl">{html.escape(s["title"])}</span>'
            f'<span class="story-meta"><span style="color:{col};font-weight:700">{s["sentiment"]}</span>'
            f' \u00B7 {html.escape(s["source"])} \u00B7 {_time_ago(s["dt"])}</span>'
            f'</span></a>'
        )
    return out


_GN_CAT_COLORS = {
    "ALL": "var(--br)", "PRICE": "var(--yl)", "LEGAL": "var(--rd)", "REG": "var(--or)",
    "ECOSYSTEM": "var(--gr)", "TECH": "var(--tq)", "WHALE": "var(--bl)", "GENERAL": "var(--tx)",
}

def global_feed_html(limit=60):
    pool = NEWS.get("pool", [])
    if not pool:
        return '<div class="empty">Connecting to news feeds\u2026 stories populate on deploy.</div>'
    sent_col = {"bullish": "var(--gr)", "bearish": "var(--rd)", "neutral": "#8099b3"}
    stories = sorted(pool, key=lambda s: s["dt"], reverse=True)[:limit]
    out = ""
    for s in stories:
        cat = s.get("category", "General")
        sent = s.get("sentiment", "neutral")
        col = sent_col.get(sent, "#8099b3")
        cat_col = _GN_CAT_COLORS.get(cat.upper(), "var(--tx)")
        title = html.escape(s["title"])
        summary = html.escape(s.get("summary", ""))
        data_text = html.escape((s["title"] + " " + s.get("summary", "")).lower(), quote=True)
        breaking = ('<span class="gn-break">\u26A1 BREAKING</span>' if s.get("breaking") else '')
        translate = ('' if not s.get("foreign") else
                     f'<a class="gn-tr" href="{_translate_url(s["link"])}" target="_blank" rel="noopener">\U0001F310 Translate</a>')
        summary_html = f'<div class="gn-sum">{summary}</div>' if summary else ''
        out += (
            f'<div class="gn-card" data-cat="{cat.upper()}" data-text="{data_text}">'
            f'<div class="gn-top"><span class="gn-src">{html.escape(s["source"])}</span>'
            f'<span class="gn-cat" style="color:{cat_col}">{cat}</span>{breaking}'
            f'<span class="gn-time">{_time_ago(s["dt"])}</span></div>'
            f'<a class="gn-hl" href="{html.escape(s["link"], quote=True)}" target="_blank" rel="noopener">{title}</a>'
            f'{translate}'
            f'{summary_html}'
            f'<div class="gn-foot"><span class="gn-dot" style="background:{col}"></span>'
            f'<span style="color:{col};text-transform:capitalize">{sent}</span></div>'
            f'</div>'
        )
    return out


def _matches(story, kws):
    t = (story["title"] + " " + story["source"]).lower()
    return any(k in t for k in kws)

def us_intelligence():
    """News-derived US briefing. (Upgrade point: swap internals for a Claude API call,
    keeping this computed version as the fallback.)"""
    pool = NEWS.get("pool", [])
    ts = NEWS.get("updated")
    us = [s for s in pool if _matches(s, US_KEYWORDS) or "ripple" in s["title"].lower()]
    if not us:
        return {"pulse": "Awaiting US market signals \u2014 the news feed is still loading.",
                "regulatory": "No US regulatory headlines in the current cycle.",
                "institutional": "No US institutional headlines in the current cycle.", "ts": ts}
    bulls = sum(1 for s in us if s["sentiment"] == "bullish")
    bears = sum(1 for s in us if s["sentiment"] == "bearish")
    lean = "bullish" if bulls > bears else "bearish" if bears > bulls else "balanced"
    n = len(us)
    pulse = (f"{n} US-focused XRP stor{'y' if n == 1 else 'ies'} this cycle; sentiment reads {lean} "
             f"({bulls} bullish, {bears} bearish), centered on regulatory clarity and institutional access.")
    reg = [s for s in us if _matches(s, {"sec", "cftc", "court", "ruling", "settlement", "legislation", "congress", "senate", "regulat"})]
    regulatory = (f"{len(reg)} stor{'y' if len(reg) == 1 else 'ies'} touch{'es' if len(reg) == 1 else ''} US regulation (SEC / CFTC / legislation)."
                  if reg else "Quiet on the US regulatory front this cycle.")
    inst = [s for s in us if _matches(s, {"etf", "bank", "custody", "blackrock", "fidelity", "nasdaq", "institutional", "fund"})]
    institutional = (f"{len(inst)} stor{'y' if len(inst) == 1 else 'ies'} cover{'s' if len(inst) == 1 else ''} US institutional activity (ETFs, banks, custody)."
                     if inst else "No notable US institutional moves this cycle.")
    return {"pulse": pulse, "regulatory": regulatory, "institutional": institutional, "ts": ts}

def _region_signals():
    pool = NEWS.get("pool", [])
    signals = {}
    for reg in REGIONS:
        rs = [s for s in pool if s.get("region") == reg]
        if rs:
            b = sum(1 for s in rs if s["sentiment"] == "bullish")
            r = sum(1 for s in rs if s["sentiment"] == "bearish")
            signals[reg] = "bullish" if b > r else "bearish" if r > b else "neutral"
        else:
            signals[reg] = "quiet"
    return signals

def global_pulse():
    """News-derived global synthesis (same upgrade point as US Intelligence)."""
    pool = NEWS.get("pool", [])
    ts = NEWS.get("updated")
    signals = _region_signals()
    if not pool:
        return {"pulse": "Awaiting global signals \u2014 the news feed is still loading.",
                "thesis": "Region signals populate as feeds report in.", "signals": signals, "ts": ts}
    bulls = sum(1 for s in pool if s["sentiment"] == "bullish")
    bears = sum(1 for s in pool if s["sentiment"] == "bearish")
    active = [r for r in REGIONS if signals[r] != "quiet"]
    lean = "risk-on" if bulls > bears else "risk-off" if bears > bulls else "balanced"
    pulse = (f"{len(pool)} XRP stories across {len(active)} active region{'s' if len(active) != 1 else ''}; "
             f"the global tape reads {lean} ({bulls} bullish, {bears} bearish).")
    bull_regions = [r for r in REGIONS if signals[r] == "bullish"]
    if bull_regions:
        thesis = f"Positive momentum is concentrated in {', '.join(bull_regions)}. "
    else:
        thesis = "No single region is clearly leading. "
    thesis += ("Broad positive flow supports continuation \u2014 watch US regulatory catalysts for confirmation."
               if bulls >= bears else
               "Mixed-to-cautious flow points to range-bound action until a clearer catalyst emerges.")
    return {"pulse": pulse, "thesis": thesis, "signals": signals, "ts": ts}

def _fmt_usd(v):
    if not v:
        return "\u2014"
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.2f}M"
    if v >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:.2f}"

def signal_stats():
    pool = NEWS.get("pool", [])
    total = len(pool)
    bull = sum(1 for s in pool if s["sentiment"] == "bullish")
    bear = sum(1 for s in pool if s["sentiment"] == "bearish")
    neut = total - bull - bear
    return total, bull, bear, neut

# ─────────────────────────────────────────────────────────────────────
# XRP INTELLIGENCE BRIEF — twice daily (AM 12:00 PM CST, PM 9:00 PM CST)
# News-derived; each edition is generated at its slot and cached until the next.
# ─────────────────────────────────────────────────────────────────────
BRIEF = {"slot_id": None, "edition": None, "generated": None, "next_run": None, "sections": {}}
BRIEF_ARCHIVE = {}   # slot_id -> {"edition","generated","sections"} — this week's editions live here
BRIEF_ARCHIVE_MAX = 1   # current edition only — next brief replaces it
BRIEF_ARCHIVE_FILE = "/tmp/xrpradar_brief_archive.json"  # survives simple restarts; wiped only on full redeploy

def _save_brief_archive():
    """Persist BRIEF_ARCHIVE to disk so a simple process restart doesn't lose it. Never raises."""
    try:
        with open(BRIEF_ARCHIVE_FILE, "w") as f:
            json.dump(BRIEF_ARCHIVE, f)
    except Exception:
        pass

def _load_brief_archive():
    """Load BRIEF_ARCHIVE from disk on startup, if present. Never raises."""
    try:
        with open(BRIEF_ARCHIVE_FILE, "r") as f:
            data = json.load(f)
            if isinstance(data, dict):
                BRIEF_ARCHIVE.update(data)
    except Exception:
        pass

_load_brief_archive()

_BRIEF_THEMES = {
    "Spot ETF": ["etf", "spot etf"],
    "SEC / Legal": ["sec", "lawsuit", "court", "ruling", "settlement", "appeal"],
    "RLUSD / Stablecoin": ["rlusd", "stablecoin"],
    "Bank Partnerships": ["partnership", "bank", "santander", "sbi", "custody"],
    "XRPL Tech": ["xrpl", "ledger", "amm", "evm", "amendment", "upgrade"],
    "Whale Flows": ["whale", "million xrp", "billion xrp", "transfer"],
    "CBDC / Sovereign": ["cbdc", "central bank", "sovereign", "digital currency"],
}

def _brief_slot(now_ct):
    d = now_ct.date()
    h = now_ct.hour
    if h >= 21:
        return f"{d.isoformat()}-PM", "PM"
    if h >= 12:
        return f"{d.isoformat()}-AM", "AM"
    yd = (now_ct - timedelta(days=1)).date()
    return f"{yd.isoformat()}-PM", "PM"

def _brief_next_run_dt(now_ct):
    h = now_ct.hour
    if h < 12:
        return now_ct.replace(hour=12, minute=0, second=0, microsecond=0)
    elif h < 21:
        return now_ct.replace(hour=21, minute=0, second=0, microsecond=0)
    else:
        return (now_ct + timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)

def _brief_next_run(now_ct):
    nxt = _brief_next_run_dt(now_ct)
    try:
        return nxt.strftime("%b %d, %-I:%M %p CST")
    except ValueError:
        return nxt.strftime("%b %d, %I:%M %p CST")

def _brief_sections(pool):
    total = len(pool)
    if not total:
        msg = "Awaiting the news feed \u2014 this edition publishes once stories are in."
        return {k: msg for k in ["pulse", "connections", "domino", "regional", "watchlist", "tradfi"]}
    bull = sum(1 for s in pool if s["sentiment"] == "bullish")
    bear = sum(1 for s in pool if s["sentiment"] == "bearish")
    lean = "bullish" if bull > bear else "bearish" if bear > bull else "balanced"
    chg = MARKET.get("xrp_chg")
    dir_txt = ("up" if (chg or 0) >= 0 else "down") + (f" {abs(chg):.2f}% over 24h" if chg is not None else "")
    fng = MARKET.get("fng")
    fng_txt = (f"Fear & Greed reads {fng} ({MARKET.get('fng_label', '')})" if fng is not None
               else "Fear & Greed is unavailable")

    pulse = (f"The tape carries {total} XRP stor{'y' if total == 1 else 'ies'} this edition, leaning {lean} "
             f"({bull} bullish, {bear} bearish). {fng_txt}; XRP is {dir_txt}.")

    theme_hits = []
    for name, kws in _BRIEF_THEMES.items():
        stories = [s for s in pool if any(k in (s["title"] + " " + s.get("summary", "")).lower() for k in kws)]
        if stories:
            srcs = len({s["source"] for s in stories})
            theme_hits.append((name, len(stories), srcs))
    theme_hits.sort(key=lambda t: (t[1], t[2]), reverse=True)
    if theme_hits:
        parts = [f"{n} ({c} stor{'y' if c == 1 else 'ies'} across {sc} outlet{'s' if sc != 1 else ''})"
                 for n, c, sc in theme_hits[:3]]
        connections = "The dominant thread is " + parts[0]
        if len(parts) > 1:
            connections += ", followed by " + " and ".join(parts[1:])
        connections += ". Cross-outlet convergence suggests the narrative is broadening, not isolated."
    else:
        connections = "Coverage is fragmented with no single dominant thread this edition."

    if theme_hits:
        lead = theme_hits[0][0]
        if lean == "bullish":
            domino = (f"If {lead} momentum holds, expect follow-through buying and secondary coverage from lagging "
                      f"outlets; watch for confirmation in price and volume.")
        elif lean == "bearish":
            domino = (f"With sentiment tilting bearish around {lead}, near-term downside headlines could compound; "
                      f"a single positive catalyst would be needed to reverse the tone.")
        else:
            domino = (f"{lead} is driving the cycle but sentiment is balanced \u2014 the next major headline likely "
                      f"sets direction; until then, expect a range-bound reaction.")
    else:
        domino = "No clear catalyst chain this edition; the market is between stories and likely to drift."

    reg_rows = _rank_counts([s["region"] for s in pool if s.get("region")])
    if reg_rows:
        parts = []
        for reg, cnt in reg_rows[:3]:
            rs = [s for s in pool if s.get("region") == reg]
            b = sum(1 for s in rs if s["sentiment"] == "bullish")
            r = sum(1 for s in rs if s["sentiment"] == "bearish")
            sig = "bullish" if b > r else "bearish" if r > b else "neutral"
            parts.append(f"{REGION_FLAGS.get(reg, '')} {reg} ({cnt}, {sig})")
        regional = "Regional activity concentrates in " + ", ".join(parts) + ". Other regions are quiet."
    else:
        regional = "No regional flashpoints \u2014 coverage is US and global-centric this edition."

    watch = sorted(pool, key=lambda s: s["influence"], reverse=True)[:4]
    if watch:
        items = "; ".join(f"({i}) {html.escape(s['title'])} \u2014 {html.escape(s['source'])}"
                          for i, s in enumerate(watch, 1))
        watchlist = "Highest-signal stories to watch: " + items + "."
    else:
        watchlist = "No standout stories to flag this edition."

    tradfi_kw = {"etf", "bank", "custody", "sec", "institutional", "nasdaq", "blackrock", "fidelity", "swift", "settlement"}
    tf = [s for s in pool if any(k in (s["title"] + " " + s.get("summary", "")).lower() for k in tradfi_kw)]
    if tf:
        tradfi = (f"{len(tf)} stor{'y' if len(tf) == 1 else 'ies'} touch traditional-finance integration "
                  f"(ETFs, banks, regulators, settlement rails). Institutional plumbing remains the structural story "
                  f"beneath the daily price noise.")
    else:
        tradfi = "Quiet on traditional-finance integration this edition; watch for ETF and banking headlines next cycle."

    return {"pulse": pulse, "connections": connections, "domino": domino,
            "regional": regional, "watchlist": watchlist, "tradfi": tradfi}

def generate_brief():
    now_ct = datetime.now(CENTRAL)
    slot_id, edition = _brief_slot(now_ct)
    BRIEF["slot_id"] = slot_id
    BRIEF["edition"] = edition
    try:
        BRIEF["generated"] = now_ct.strftime("%b %d, %Y \u00B7 %-I:%M %p CST")
    except ValueError:
        BRIEF["generated"] = now_ct.strftime("%b %d, %Y \u00B7 %I:%M %p CST")
    BRIEF["next_run"] = _brief_next_run(now_ct)
    BRIEF["sections"] = _brief_sections(NEWS.get("pool", []))

    BRIEF_ARCHIVE[slot_id] = {
        "edition": BRIEF["edition"],
        "generated": BRIEF["generated"],
        "sections": dict(BRIEF["sections"]),
    }
    if len(BRIEF_ARCHIVE) > BRIEF_ARCHIVE_MAX:
        for old_key in sorted(BRIEF_ARCHIVE.keys())[:len(BRIEF_ARCHIVE) - BRIEF_ARCHIVE_MAX]:
            del BRIEF_ARCHIVE[old_key]
    _save_brief_archive()


def brief_week_slots(now_ct, n=BRIEF_ARCHIVE_MAX):
    """Current + previous edition slots (n=BRIEF_ARCHIVE_MAX), most recent first."""
    cur_id, cur_edition = _brief_slot(now_ct)
    y, m, d, _ = cur_id.split("-")
    cur_date = datetime(int(y), int(m), int(d)).date()
    slots = []
    dd, ed = cur_date, cur_edition
    for _ in range(n):
        slot_id = f"{dd.isoformat()}-{ed}"
        slots.append({"slot_id": slot_id, "date": dd, "edition": ed})
        if ed == "PM":
            ed = "AM"
        else:
            ed = "PM"
            dd = dd - timedelta(days=1)
    return slots


# ── World briefing clocks: UTC + 7 major crypto-trading cities ──
WORLD_CITIES = [
    ("UTC",       "UTC"),
    ("New York",  "America/New_York"),
    ("London",    "Europe/London"),
    ("Dubai",     "Asia/Dubai"),
    ("Singapore", "Asia/Singapore"),
    ("Hong Kong", "Asia/Hong_Kong"),
    ("Tokyo",     "Asia/Tokyo"),
    ("Seoul",     "Asia/Seoul"),
]

def _tz(name):
    if name == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone.utc

def _fmt_local(dt, z):
    try:
        return dt.astimezone(z).strftime("%-I:%M %p")
    except ValueError:
        return dt.astimezone(z).strftime("%I:%M %p").lstrip("0")

def world_clocks_html():
    now_utc = datetime.now(timezone.utc)
    ct = datetime.now(CENTRAL)
    b1 = ct.replace(hour=12, minute=0, second=0, microsecond=0)   # 12:00 PM CST edition
    b2 = ct.replace(hour=21, minute=0, second=0, microsecond=0)   # 9:00 PM CST edition
    out = ""
    for city, tzname in WORLD_CITIES:
        z = _tz(tzname)
        off = now_utc.astimezone(z).utcoffset().total_seconds() / 3600
        hh = int(abs(off)); mm = int(round((abs(off) - hh) * 60))
        if tzname == "UTC":
            off_disp = "\u00B10"
        else:
            off_disp = ("+" if off >= 0 else "\u2212") + str(hh) + (f":{mm:02d}" if mm else "")
        out += (
            f'<div class="wc">'
            f'<div class="wc-city">{city}</div>'
            f'<div class="wc-clock" data-tz="{tzname}">'
            f'<span class="wc-hand wc-hr"></span>'
            f'<span class="wc-hand wc-min"></span>'
            f'<span class="wc-hand wc-sec"></span>'
            f'<span class="wc-center"></span>'
            f'</div>'
            f'<div class="wc-off">UTC {off_disp}</div>'
            f'<div class="wc-b">1st {_fmt_local(b1, z)}</div>'
            f'<div class="wc-b">2nd {_fmt_local(b2, z)}</div>'
            f'</div>'
        )
    return out


def institutional_confidence_index():
    """XRPRadar Institutional Confidence Index (ICI) — 0-100, rescaled from five real components
    unique to this site's own accumulated tracking. Every component is disclosed and computed
    from data already gathered elsewhere on the page; nothing here is invented or opaque."""
    comps = []

    # 1. Partnership Momentum — from our own growing Enterprise Ledger (detected deals only)
    detected_n = sum(1 for e in PARTNERSHIP_LEDGER if e["source"] == "detected")
    if detected_n >= 6:
        pm = 20
    elif detected_n >= 3:
        pm = 15
    elif detected_n >= 1:
        pm = 10
    else:
        pm = 5
    comps.append(("Partnership Momentum", f"{detected_n} new deals detected", pm))

    # 2. Developer Activity — from live XRPL GitHub tracking
    dev_commits = GITHUB_DEV.get("rippled_7d", 0) + GITHUB_DEV.get("other_7d", 0)
    if dev_commits >= 16:
        da = 20
    elif dev_commits >= 6:
        da = 15
    elif dev_commits >= 1:
        da = 10
    else:
        da = 5
    comps.append(("Developer Activity", f"{dev_commits} commits/7d", da))

    # 3. Smart Money Positioning — rescale the existing Smart Money Score (0-100 -> 0-20)
    sm = smart_money()
    smp = round(sm["score"] / 100 * 20)
    comps.append(("Smart Money Positioning", f'{sm["score"]}/100 \u2014 {sm["label"]}', smp))

    # 4. Executive Tone — sentiment across real statements in the Ripple Exec Tracker
    ex_stories = EXEC_TRACKER.get("stories", [])
    if ex_stories:
        ex_bull = sum(1 for s in ex_stories if _sentiment(s["title"]) == "bullish")
        ex_bear = sum(1 for s in ex_stories if _sentiment(s["title"]) == "bearish")
        ex_share = (ex_bull - ex_bear) / len(ex_stories)
        et = round(10 + ex_share * 10)
        et = max(0, min(20, et))
        et_disp = f"{ex_bull} positive / {ex_bear} negative of {len(ex_stories)}"
    else:
        et, et_disp = 10, "Awaiting statements"
    comps.append(("Executive Tone", et_disp, et))

    # 5. Regulatory Momentum — CLARITY Act tracker fill + net sentiment of Legal/Reg news
    ca_n = len(CLARITY_ACT_STORIES)
    pool = NEWS.get("pool", [])
    reg_stories = [s for s in pool if s.get("category") in ("Legal", "Reg")]
    if reg_stories:
        reg_bull = sum(1 for s in reg_stories if s["sentiment"] == "bullish")
        reg_bear = sum(1 for s in reg_stories if s["sentiment"] == "bearish")
        reg_share = (reg_bull - reg_bear) / len(reg_stories)
    else:
        reg_share = 0
    rm = round((ca_n / 10) * 10 + (reg_share * 10 + 10) / 2)
    rm = max(0, min(20, rm))
    comps.append(("Regulatory Momentum", f"{ca_n}/10 CLARITY Act stories tracked", rm))

    score = sum(c[2] for c in comps)
    if score >= 80:
        label, col = "Institutional Grade", "var(--gr)"
    elif score >= 65:
        label, col = "Strong Confidence", "var(--gr)"
    elif score >= 50:
        label, col = "Moderate Confidence", "var(--yl)"
    elif score >= 35:
        label, col = "Cautious", "var(--or)"
    else:
        label, col = "Low Confidence", "var(--rd)"
    return {"score": score, "label": label, "color": col, "comps": comps}


def signal_score():
    """Composite 0-100, rescaled from the 4 components we have real data for:
    Price Momentum (15), RSI (12), Sentiment (15), Fear & Greed (5) = 47 max."""
    chg = MARKET.get("xrp_chg")
    rsi = MARKET.get("rsi_1d")
    fng = MARKET.get("fng")
    total, bull, bear, _ = signal_stats()

    if chg is None:   pm = 5
    elif chg > 5:     pm = 15
    elif chg > 2:     pm = 12
    elif chg > 0:     pm = 8
    elif chg > -2:    pm = 5
    elif chg > -5:    pm = 3
    else:             pm = 0

    if not rsi:              rv = 8
    elif 30 <= rsi <= 40:    rv = 12
    elif 40 < rsi <= 50:     rv = 10
    elif 50 < rsi <= 60:     rv = 8
    elif 60 < rsi <= 70:     rv = 6
    elif rsi > 70:           rv = 3
    else:                    rv = 5

    ratio = (bull / total) if total else 0
    if not total:        se = 7
    elif ratio > 0.5:    se = 15
    elif ratio > 0.35:   se = 11
    elif ratio > 0.25:   se = 7
    elif ratio > 0.15:   se = 4
    else:                se = 1

    if fng is None:   fg = 2
    elif fng <= 20:   fg = 5
    elif fng <= 40:   fg = 4
    elif fng <= 60:   fg = 2
    elif fng <= 80:   fg = 1
    else:             fg = 0

    score = round((pm + rv + se + fg) / 47 * 100)
    if   score >= 75: label, col = "STRONG",   "var(--gr)"
    elif score >= 60: label, col = "BULLISH",  "var(--gr)"
    elif score >= 45: label, col = "NEUTRAL",  "var(--yl)"
    elif score >= 30: label, col = "CAUTIOUS", "var(--or)"
    else:             label, col = "BEARISH",  "var(--rd)"
    return {"score": score, "label": label, "color": col}

def smart_money():
    """Smart Money Score (0-100), rescaled from the components with real data:
    RSI 1D, Sentiment, Funding Rate. Higher = accumulation, lower = distribution."""
    rsi = MARKET.get("rsi_1d")
    total, bull, bear, _ = signal_stats()
    fund = MARKET.get("funding")
    comps = []

    if rsi:
        if rsi < 30:   rs = 85
        elif rsi < 45: rs = 70
        elif rsi < 55: rs = 55
        elif rsi < 70: rs = 40
        else:          rs = 25
        comps.append(("RSI 1D", f"{rsi:.1f}", rs))

    if total:
        share = bull / total * 100
        if share >= 60:   ss = 75
        elif share >= 45: ss = 62
        elif share >= 30: ss = 52
        elif share >= 15: ss = 42
        else:             ss = 32
        comps.append(("Sentiment", f"{round(share)}% bullish", ss))

    if fund is not None:
        fpct = fund * 100
        if fpct < -0.01:  fs = 80
        elif fpct < 0.01: fs = 62
        elif fpct < 0.05: fs = 46
        else:             fs = 30
        comps.append(("Funding Rate", f"{fpct:+.4f}%", fs))

    score = round(sum(c[2] for c in comps) / len(comps)) if comps else 50
    if   score < 35: label, col = "Distribution", "var(--rd)"
    elif score < 45: label, col = "Cautious", "var(--or)"
    elif score < 55: label, col = "Neutral / Mixed", "var(--yl)"
    elif score < 70: label, col = "Accumulation", "var(--gr)"
    else:            label, col = "Strong Accumulation", "var(--gr)"
    return {"score": score, "label": label, "color": col, "comps": comps}

def _fng_color(v):
    if v <= 25: return "var(--rd)"
    if v <= 45: return "var(--or)"
    if v <= 55: return "var(--yl)"
    if v <= 75: return "var(--gr)"
    return "var(--tq)"

def fng_history_html():
    hist = MARKET.get("fng_history") or []
    if not hist:
        return '<div class="empty">Fear &amp; Greed history populates on deploy.</div>'
    bars = ""
    n = len(hist)
    for i, v in enumerate(hist):
        col = _fng_color(v)
        h = max(6, min(100, v))
        last = " fg-today" if i == n - 1 else ""
        bars += f'<div class="fg-bar{last}" style="height:{h}%;background:{col}" title="{v}"></div>'
    return bars

REGION_DISPLAY = {"Japan": "Japan", "Korea": "Korea", "UAE": "UAE/Middle East", "Europe": "Europe",
                  "India": "India", "LatAm": "Latin America", "Africa": "Africa", "SEA": "SE Asia"}

def regional_heatmap_html():
    pool = NEWS.get("pool", [])
    counts = {r: 0 for r in REGIONS}
    for s in pool:
        r = s.get("region")
        if r in counts:
            counts[r] += 1
    mx = max(counts.values()) if counts else 0
    cards = ""
    for reg in REGIONS:
        c = counts[reg]
        if mx and c:
            inten = c / mx
            bg = f"rgba(72,255,130,{0.06 + inten * 0.22:.2f})"
            bd = f"rgba(72,255,130,{0.25 + inten * 0.45:.2f})"
            num_col = "var(--gr)"
        else:
            bg = "var(--s2)"
            bd = "var(--b)"
            num_col = "var(--tx)"
        cards += (
            f'<div class="rh-card" style="background:{bg};border-color:{bd}">'
            f'<div class="rh-flag">{REGION_FLAGS.get(reg, "")}</div>'
            f'<div class="rh-name">{REGION_DISPLAY.get(reg, reg)}</div>'
            f'<div class="rh-num" style="color:{num_col}">{c}</div>'
            f'<div class="rh-lbl">stories today</div>'
            f'</div>'
        )
    return cards


def velocity_chart_html():
    buckets = news_velocity_24h()
    mx = max(buckets) or 1
    return "".join(
        f'<div class="vel-bar" style="height:{max(6, v / mx * 100):.0f}%" title="{v} stories"></div>'
        for v in buckets
    )


def sentiment_trend_html():
    days = sorted(SENTIMENT_HISTORY.keys())
    if not days:
        return '<div class="empty">Sentiment history builds day by day as the server runs \u2014 check back soon.</div>'
    mx = max(SENTIMENT_HISTORY[d]["total"] for d in days) or 1
    bars = ""
    for d in days:
        b = SENTIMENT_HISTORY[d]
        h = max(6, b["total"] / mx * 100)
        if b["bull"] > b["bear"]:
            col = "var(--gr)"
        elif b["bear"] > b["bull"]:
            col = "var(--rd)"
        else:
            col = "var(--tx)"
        title = f'{d}: {b["bull"]} bull / {b["bear"]} bear / {b["neut"]} neutral'
        bars += f'<div class="sdt-bar" style="height:{h:.0f}%;background:{col}" title="{title}"></div>'
    return bars


def sentiment_leaderboard_html():
    rows = sentiment_source_table()
    if not rows:
        return '<tr><td colspan="6" class="empty">Feeds loading\u2026</td></tr>'
    out = ""
    for i, r in enumerate(rows, 1):
        t = max(r["total"], 1)
        bull_pct = r["bull"] / t * 100
        bear_pct = r["bear"] / t * 100
        out += (
            f'<tr><td>{i}</td><td style="color:var(--br);font-weight:700">{html.escape(r["name"])}</td>'
            f'<td style="text-align:center">{r["total"]}</td>'
            f'<td style="text-align:center;color:var(--gr)">{r["bull"]}</td>'
            f'<td style="text-align:center;color:var(--rd)">{r["bear"]}</td>'
            f'<td><div class="sent-bar-mini"><span style="width:{bull_pct:.0f}%;background:var(--gr)"></span>'
            f'<span style="width:{bear_pct:.0f}%;background:var(--rd)"></span></div></td>'
            f'<td style="text-align:center;color:var(--yl)">{r["breaking"] or "\u2014"}</td></tr>'
        )
    return out


def exec_tracker_html():
    stories = EXEC_TRACKER.get("stories", [])
    if not stories:
        return '<div class="home-base"><div class="home-base-icon">\U0001F3A4</div><div class="home-base-title">Monitoring Executive Statements</div><div class="home-base-sub">Public statements from Ripple\u2019s leadership team surface here automatically as they\u2019re published.</div></div>'
    out = ""
    for s in stories:
        out += (
            f'<div class="ex-row" data-tab="{s["tab"]}">'
            f'<div class="ex-top"><span class="ex-name">{html.escape(s["exec"])}</span>'
            f'<span class="ex-title">{html.escape(s["exec_title"])}</span>'
            f'<span class="ex-time">{_time_ago(s["dt"])}</span></div>'
            f'<a class="ex-hl" href="{html.escape(s["link"], quote=True)}" target="_blank" rel="noopener">{html.escape(s["title"])}</a>'
            f'</div>'
        )
    return out


def github_commits_html():
    commits = GITHUB_DEV.get("commits", [])
    if not commits:
        return '<div class="home-base"><div class="home-base-icon">\U0001F4BB</div><div class="home-base-title">Monitoring XRPL Development</div><div class="home-base-sub">Commits across rippled, xrpl-dev-portal and xrpl.js surface here automatically.</div></div>'
    out = ""
    for c in commits:
        out += (
            f'<div class="gh-row">'
            f'<span class="gh-repo">{html.escape(c["repo"])}</span>'
            f'<a class="gh-msg" href="{html.escape(c["url"], quote=True)}" target="_blank" rel="noopener">{html.escape(c["msg"] or "(no message)")}</a>'
            f'<span class="gh-meta">{html.escape(c["author"])} \u00B7 {html.escape(c["date"])}</span>'
            f'</div>'
        )
    return out


def competitor_table_html():
    xrp_price = MARKET.get("xrp_price")
    xrp_chg = MARKET.get("xrp_chg")
    xrp_7d = MARKET.get("perf_1w")
    xrp_mcap = MARKET.get("mcap")

    def _row(sym, emoji, price, chg24, chg7d, mcap, edge, is_self):
        px = f"${price:.4f}" if price and price < 1 else (f"${price:,.2f}" if price else "\u2014")
        c24 = f'{chg24:+.2f}%' if chg24 is not None else "\u2014"
        c24col = "var(--gr)" if (chg24 or 0) >= 0 else "var(--rd)"
        c7 = f'{chg7d:+.2f}%' if chg7d is not None else "\u2014"
        c7col = "var(--gr)" if (chg7d or 0) >= 0 else "var(--rd)"
        mc = _fmt_usd(mcap)
        rowbg = "background:rgba(117,188,255,.06);border-left:3px solid var(--bl)" if is_self else ""
        symcol = "var(--bl)" if is_self else "var(--br)"
        edgecol = "var(--bl)" if is_self else "var(--tx)"
        return (
            f'<tr style="{rowbg}"><td><span style="margin-right:6px">{emoji}</span>'
            f'<span style="font-weight:900;color:{symcol}">{sym}</span></td>'
            f'<td style="text-align:right">{px}</td>'
            f'<td style="text-align:right;color:{c24col}">{c24}</td>'
            f'<td style="text-align:right;color:{c7col}">{c7}</td>'
            f'<td style="text-align:right;color:var(--tx)">{mc}</td>'
            f'<td style="color:{edgecol};max-width:260px">{edge}</td></tr>'
        )

    rows = _row("XRP", "\U0001FA99", xrp_price, xrp_chg, xrp_7d, xrp_mcap, "\U0001F3AF Tracking live", True)
    for c in COMPETITORS:
        e = MARKET["competitors"].get(c["id"], {})
        rows += _row(c["symbol"], c["emoji"], e.get("price"), e.get("change_24h"), e.get("change_7d"),
                     e.get("mcap"), COMPETITOR_EDGE.get(c["symbol"], ""), False)
    return rows


def _rank_counts(items):
    counts = {}
    for it in items:
        counts[it] = counts.get(it, 0) + 1
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)

def lb_sources_html(n=6):
    rows = _rank_counts([s["source"] for s in NEWS.get("pool", [])])[:n]
    if not rows:
        return '<div class="lb-empty">Feeds loading\u2026</div>'
    out = ""
    for i, (src, cnt) in enumerate(rows, 1):
        out += (f'<div class="lb-row"><span class="lb-rank">{i}</span>'
                f'<span class="lb-name">{html.escape(src)}</span>'
                f'<span class="lb-cnt">{cnt}</span></div>')
    return out

def lb_regions_html(n=8):
    rows = _rank_counts([s["region"] for s in NEWS.get("pool", []) if s.get("region")])[:n]
    if not rows:
        return '<div class="lb-empty">Feeds loading\u2026</div>'
    out = ""
    for i, (reg, cnt) in enumerate(rows, 1):
        out += (f'<div class="lb-row"><span class="lb-rank">{i}</span>'
                f'<span class="lb-name">{REGION_FLAGS.get(reg, "")} {reg}</span>'
                f'<span class="lb-cnt">{cnt}</span></div>')
    return out

def regional_discourse_html():
    pool = NEWS.get("pool", [])
    sig_col = {"bullish": "var(--gr)", "bearish": "var(--rd)", "neutral": "var(--yl)", "quiet": "var(--tx)"}
    cards = ""
    for reg in REGIONS:
        rs = sorted([s for s in pool if s.get("region") == reg], key=lambda s: s["dt"], reverse=True)
        n = len(rs)
        if rs:
            b = sum(1 for s in rs if s["sentiment"] == "bullish")
            r = sum(1 for s in rs if s["sentiment"] == "bearish")
            sig = "bullish" if b > r else "bearish" if r > b else "neutral"
            top = html.escape(rs[0]["title"])
        else:
            sig = "quiet"
            top = "No regional stories yet \u2014 feeds are loading."
        col = sig_col[sig]
        cards += (
            f'<div class="rd-card">'
            f'<div class="rd-top"><span class="rd-name">{REGION_FLAGS[reg]} {reg}</span>'
            f'<span class="rd-sig" style="color:{col};border-color:{col}">{sig}</span></div>'
            f'<div class="rd-count">{n} stor{"y" if n == 1 else "ies"}</div>'
            f'<div class="rd-hl">{top}</div>'
            f'</div>'
        )
    return cards


def next_escrow_release():
    """Ripple releases 1B XRP from escrow on the 1st of each month (00:00 UTC)."""
    now = datetime.now(timezone.utc)
    if now.month == 12:
        nxt = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        nxt = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return nxt


ECOSYSTEM_CARDS = [
    {"ic": "\U0001F517", "name": "XRPL", "role": "The Foundation", "color": "var(--tq)",
     "bg": "rgba(0,229,204,.06)", "bd": "rgba(0,229,204,.3)",
     "desc": "Open-source, decentralised blockchain maintained by the independent XRPL Foundation. Consensus settles in 3-5 seconds. Native DEX, AMM pools, escrow, and payment channels built in at the protocol level.",
     "stats": [("Total Accounts", "6.4M+"), ("Settlement", "3-5 seconds"), ("Tx Fee", "~$0.0002")]},
    {"ic": "\U0001F3E2", "name": "Ripple Labs", "role": "The Company", "color": "var(--bl)",
     "bg": "rgba(117,188,255,.06)", "bd": "rgba(117,188,255,.3)",
     "desc": "Private San Francisco company that created XRP and builds enterprise blockchain solutions. NOT the same as XRPL. Revenue from ODL, software licensing, and XRP sales. Led by Brad Garlinghouse.",
     "stats": [("Founded", "2012"), ("HQ", "San Francisco + Dubai"), ("SEC Case", "\u2705 Settled 2025")]},
    {"ic": "\U0001F48E", "name": "XRP", "role": "The Asset", "color": "var(--gr)",
     "bg": "rgba(72,255,130,.06)", "bd": "rgba(72,255,130,.3)",
     "desc": "Native digital asset of the XRPL. Used as bridge currency in ODL, transaction gas, and wallet reserve. Fixed supply of 100 billion \u2014 no mining, no inflation. Burned slightly with every transaction.",
     "stats": [("Total Supply", "100B XRP"), ("Circulating", "~62B XRP"), ("In Escrow", "~43B XRP")]},
    {"ic": "\U0001F310", "name": "RippleNet", "role": "The Network", "color": "var(--or)",
     "bg": "rgba(255,153,0,.06)", "bd": "rgba(255,153,0,.3)",
     "desc": "Ripple's B2B payment network connecting 300+ financial institutions globally. Three tiers: Direct (messaging), Multi-hop (routing), and ODL (XRP bridge). Banks choose their level of XRP integration.",
     "stats": [("Partners", "300+ institutions"), ("Countries", "55+"), ("Type", "Enterprise B2B")]},
    {"ic": "\u26A1", "name": "ODL", "role": "On-Demand Liquidity", "color": "var(--rd)",
     "bg": "rgba(255,64,96,.06)", "bd": "rgba(255,64,96,.3)",
     "desc": "Instant cross-border settlement that converts fiat to XRP, moves it on the XRPL in seconds, then converts to the destination fiat \u2014 removing pre-funded accounts.",
     "stats": [("Active Corridors", "8+"), ("Settlement", "3-5 seconds"), ("Savings vs SWIFT", "Up to 60%")]},
    {"ic": "\U0001F4B5", "name": "RLUSD", "role": "The Stablecoin", "color": "var(--bl)",
     "bg": "rgba(117,188,255,.06)", "bd": "rgba(117,188,255,.3)",
     "desc": "Ripple's USD-pegged stablecoin launched December 2024. Runs natively on the XRPL and Ethereum, fully backed and regulated.",
     "stats": [("Peg", "1:1 USD"), ("Regulator", "NYDFS"), ("Networks", "XRPL + ETH")]},
    {"ic": "\U0001F6E0\uFE0F", "name": "XRPL Dev", "role": "Developer Layer", "color": "var(--tq)",
     "bg": "rgba(0,229,204,.06)", "bd": "rgba(0,229,204,.3)",
     "desc": "Tools, standards, and programmability: Hooks (lightweight smart contracts), AMM, native tokens, and multi-purpose tokens \u2014 expanding what builders can ship on the ledger.",
     "stats": [("Smart Contracts", "Hooks"), ("Native AMM", "Live"), ("Tokens", "IOU + MPT")]},
    {"ic": "\U0001F6E1\uFE0F", "name": "Validators", "role": "Consensus Layer", "color": "var(--yl)",
     "bg": "rgba(255,204,0,.06)", "bd": "rgba(255,204,0,.3)",
     "desc": "Independent validators worldwide run the consensus protocol, agreeing on ledger state every 3-5 seconds with no mining. A Unique Node List keeps the network decentralised, fast, and energy-efficient.",
     "stats": [("Validators", "150+"), ("Consensus", "RPCA"), ("Energy", "Carbon-neutral")]},
]


def ecosystem_cards_html():
    out = ""
    for c in ECOSYSTEM_CARDS:
        stats = "".join(
            f'<div class="eco-stat"><span class="k">{k}</span>'
            f'<span style="color:{c["color"]};font-weight:700">{v}</span></div>'
            for k, v in c["stats"]
        )
        out += (
            f'<div class="eco-card" style="background:{c["bg"]};border:1px solid {c["bd"]}">'
            f'<div class="eco-bar" style="background:linear-gradient(90deg,{c["color"]},transparent)"></div>'
            f'<div class="eco-ic">{c["ic"]}</div>'
            f'<div class="eco-name">{c["name"]}</div>'
            f'<div class="eco-role" style="color:{c["color"]}">{c["role"]}</div>'
            f'<div class="eco-desc">{c["desc"]}</div>'
            f'{stats}'
            f'</div>'
        )
    return out


# ─────────────────────────────────────────────────────────────────────
# MAINSTREAM INTEGRATION + INSTITUTIONAL PARTNERSHIPS (static reference)
# ─────────────────────────────────────────────────────────────────────
STATUS_COLORS = {
    "CONFIRMED": "var(--gr)",
    "LIVE":      "var(--gr)",
    "EXPLORING": "var(--bl)",
    "RUMORED":   "var(--yl)",
    "PILOT":     "var(--or)",
    "COMPETING": "var(--rd)",
}
STATUS_TINT = {
    "CONFIRMED": "rgba(72,255,130,.35)",
    "LIVE":      "rgba(72,255,130,.35)",
    "EXPLORING": "rgba(117,188,255,.35)",
    "RUMORED":   "rgba(255,204,0,.35)",
    "PILOT":     "rgba(255,153,0,.35)",
    "COMPETING": "rgba(255,64,96,.35)",
}
STATUS_EMOJI = {
    "CONFIRMED": "\u2705",
    "LIVE":      "\u2705",
    "EXPLORING": "\U0001F50D",
    "RUMORED":   "\U0001F4AC",
    "PILOT":     "\U0001F9EA",
    "COMPETING": "\u2694\uFE0F",
}

# Institutional Partnership Tracker — 20 institutions (screenshot order) = 5 rows of 4
# (name, type, flag, status, detail, source)
INSTITUTIONS = [
    ("Bank of America", "Bank", "\U0001F1FA\U0001F1F8", "RUMORED", "Multiple reports suggest BofA exploring Ripple ODL for cross-border settlement. Not officially confirmed.", "Industry reports 2025-2026"),
    ("JPMorgan Chase", "Bank", "\U0001F1FA\U0001F1F8", "EXPLORING", "JPM Coin runs on a private blockchain but JPMorgan has engaged with ISO 20022 standards compatible with XRPL. Watching closely.", "Bloomberg 2025"),
    ("SBI Holdings", "Bank", "\U0001F1EF\U0001F1F5", "CONFIRMED", "SBI Ripple Asia \u2014 joint venture fully operational. SBI VC Trade, SBI Remit, and MoneyTap all run on Ripple technology.", "SBI Holdings IR 2024"),
    ("Santander", "Bank", "\U0001F1EA\U0001F1F8", "CONFIRMED", "One Pay FX powered by Ripple since 2018. Expanded to multiple markets. One of the earliest major bank adopters.", "Santander Press Release"),
    ("Standard Chartered", "Bank", "\U0001F1EC\U0001F1E7", "CONFIRMED", "SC Ventures partnership with Ripple for cross-border payments in Asia-Pacific corridors.", "Standard Chartered 2023"),
    ("PNC Bank", "Bank", "\U0001F1FA\U0001F1F8", "CONFIRMED", "PNC joined RippleNet for cross-border payment capabilities. One of the largest US banks on the network.", "Ripple Press Release"),
    ("Ita\u00FA Unibanco", "Bank", "\U0001F1E7\U0001F1F7", "CONFIRMED", "Brazil's largest private bank partnered with Ripple for international transfers via RippleNet.", "Ripple Blog 2023"),
    ("Axis Bank", "Bank", "\U0001F1EE\U0001F1F3", "CONFIRMED", "Axis Bank uses RippleNet for inbound remittances into India. Major corridor from Gulf states.", "Ripple Partner Network"),
    ("Tranglo", "Payments", "\U0001F1F8\U0001F1EC", "CONFIRMED", "Ripple acquired 40% stake in Tranglo. Powers ODL across SE Asia including Philippines, Malaysia, Indonesia.", "Ripple Acquisition 2021"),
    ("Coins.ph", "Payments", "\U0001F1F5\U0001F1ED", "CONFIRMED", "Philippines-based wallet using ODL for the US-Philippines corridor. Millions of OFW remittances monthly.", "Ripple ODL Partner"),
    ("Bitso", "Exchange", "\U0001F1F2\U0001F1FD", "CONFIRMED", "Mexico's largest crypto exchange. Primary ODL partner for the USA-Mexico corridor \u2014 the largest ODL corridor globally.", "Bitso/Ripple 2021"),
    ("Western Union", "Payments", "\U0001F1FA\U0001F1F8", "EXPLORING", "WU tested Ripple technology in 2018 pilots. No full deployment but ongoing ISO 20022 alignment is notable.", "WU Annual Report 2023"),
    ("MoneyGram", "Payments", "\U0001F1FA\U0001F1F8", "EXPLORING", "Former deep Ripple partner (2019-2021). Regulatory pressure caused pause. Re-engagement rumored post-SEC settlement.", "Industry reports 2025"),
    ("Modulr", "Fintech", "\U0001F1EC\U0001F1E7", "CONFIRMED", "UK fintech using RippleNet for European payment infrastructure. Backed by PayPal Ventures.", "Ripple Partner 2023"),
    ("Bank of Bhutan", "Central Bank", "\U0001F1E7\U0001F1F9", "CONFIRMED", "National digital currency (Druk) built on XRPL. First sovereign digital currency on the XRP Ledger.", "Royal Monetary Authority 2023"),
    ("SWIFT", "Network", "\U0001F310", "COMPETING", "SWIFT gpi is ISO 20022 compliant \u2014 same standard as XRPL. Direct competitive overlap. SWIFT Connect explores DLT bridges.", "SWIFT 2024"),
    ("Nasdaq", "Exchange", "\U0001F1FA\U0001F1F8", "EXPLORING", "Nasdaq applied for XRP ETF custody services. Potential listing venue for spot XRP ETF products.", "SEC Filings 2025"),
    ("Fidelity", "Asset Manager", "\U0001F1FA\U0001F1F8", "EXPLORING", "Fidelity Digital Assets expanding custody. XRP support rumored post-SEC settlement clarity.", "Industry reports 2026"),
    ("BlackRock", "Asset Manager", "\U0001F1FA\U0001F1F8", "EXPLORING", "BlackRock BUIDL fund uses blockchain infrastructure. XRP Ledger compatibility being evaluated.", "BlackRock Digital 2025"),
    ("Ripple \u00D7 BIS", "Research", "\U0001F310", "CONFIRMED", "Bank for International Settlements Project Nexus exploring XRPL for multi-CBDC settlements between central banks.", "BIS Innovation Hub 2024"),
]

# Sovereign / CBDC projects (kept for a future dedicated section; not rendered here)
PARTNERSHIPS = [
    ("Bhutan", "\U0001F1E7\U0001F1F9", "Druk Digital", "LIVE", "National digital currency on XRPL. Royal Monetary Authority partnership."),
    ("Palau", "\U0001F1F5\U0001F1FC", "Palau Stablecoin", "LIVE", "PSC, a USD-backed digital currency on XRPL for government payments."),
    ("Montenegro", "\U0001F1F2\U0001F1EA", "Digital Euro Pilot", "PILOT", "Central Bank of Montenegro piloting digital euro infrastructure on XRPL."),
    ("Hong Kong", "\U0001F1ED\U0001F1F0", "HKD CBDC", "PILOT", "HKMA participating in Project mBridge. Ripple in discussion for the XRPL settlement layer."),
    ("Colombia", "\U0001F1E8\U0001F1F4", "Banco de la Rep\u00FAblica", "EXPLORING", "Colombia's central bank exploring XRPL for digital peso settlement infrastructure."),
    ("Georgia", "\U0001F1EC\U0001F1EA", "Digital GEL", "EXPLORING", "National Bank of Georgia exploring Ripple technology for a national digital currency."),
]

INTEGRATION_TIMELINE = [
    ("2012", "Ripple Founded", "OpenCoin (later Ripple) created with a mission to replace correspondent banking.", False),
    ("2018", "First Bank Partnerships", "Santander One Pay FX and American Express FX International Payments launch on RippleNet.", True),
    ("2019", "ODL Goes Live", "On-Demand Liquidity launches commercially. XRP used as a bridge currency at scale for the first time.", True),
    ("2020", "SEC Lawsuit", "SEC files suit \u2014 temporarily freezing institutional adoption in the US. Global expansion continues.", False),
    ("2021", "SBI + Tranglo", "SBI Holdings scales Japan operations. Ripple acquires 40% of Tranglo \u2014 an SE Asia ODL hub.", True),
    ("2022", "SWIFT ISO 20022", "SWIFT mandates ISO 20022 migration \u2014 the same standard XRPL natively supports. Alignment begins.", True),
    ("2023", "Bhutan CBDC Live", "Bank of Bhutan launches a national digital currency on XRPL. First sovereign CBDC on the ledger.", True),
    ("2023", "Partial Legal Victory", "Judge Torres: XRP is not a security in programmatic sales. US institutional adoption starts thawing.", True),
    ("2024", "XRPL EVM Sidechain", "An Ethereum-compatible sidechain launches on XRPL \u2014 opening DeFi and smart-contract integration.", True),
    ("2025", "SEC Settlement", "SEC drops the case. $50M settlement. Full US regulatory clarity arrives; institutional floodgates open.", True),
    ("2025", "ETF Filings Wave", "Bitwise, WisdomTree, and Canary Capital file US spot XRP ETF applications. European ETPs already live.", True),
    ("2026", "TradFi Integration Era", "Banks, asset managers, and payment networks actively building on XRPL. Post-lawsuit adoption accelerating.", True),
]

def timeline_html():
    out = ""
    for year, event, detail, major in INTEGRATION_TIMELINE:
        dot_col = "var(--gr)" if major else "var(--yl)"
        dot_sz  = "16px" if major else "11px"
        yr_col  = "var(--gr)" if major else "var(--yl)"
        out += (
            f'<div class="tl-node">'
            f'<div class="tl-top">'
            f'<div class="tl-year" style="color:{yr_col}">{year}</div>'
            f'<div class="tl-dot" style="width:{dot_sz};height:{dot_sz};background:{dot_col}"></div>'
            f'</div>'
            f'<div class="tl-event">{event}</div>'
            f'<div class="tl-detail">{detail}</div>'
            f'</div>'
        )
    return out


def institution_cards_html():
    out = ""
    for name, kind, flag, status, detail, source in INSTITUTIONS:
        col   = STATUS_COLORS.get(status, "var(--tx)")
        tint  = STATUS_TINT.get(status, "var(--b)")
        emoji = STATUS_EMOJI.get(status, "")
        out += (
            f'<div class="trk-card" data-status="{status}" style="border:1px solid {tint}">'
            f'<div class="trk-top">'
            f'<span class="trk-status">{flag} {emoji} <span style="color:{col}">{status}</span></span>'
            f'<span class="trk-type">{kind}</span>'
            f'</div>'
            f'<div class="trk-name">{name}</div>'
            f'<div class="trk-detail">{detail}</div>'
            f'<div class="trk-src">{source}</div>'
            f'</div>'
        )
    return out



# ─────────────────────────────────────────────────────────────────────
# PREFLIGHT
# ─────────────────────────────────────────────────────────────────────
def run_preflight():
    checks = []
    checks.append(("Flask app responding", True, "Server handled the request"))
    checks.append(("Version string present", bool(APP_VERSION), f"Reporting version {APP_VERSION}"))
    try:
        up = (datetime.now(timezone.utc) - BOOT_TIME).total_seconds()
        checks.append(("Uptime clock running", up >= 0, f"{int(up)} seconds since boot"))
    except Exception as e:
        checks.append(("Uptime clock running", False, str(e)))
    port = os.environ.get("PORT", "8080")
    checks.append(("Port configured", bool(port), f"PORT={port}"))

    passed = sum(1 for _, ok, _ in checks if ok)
    total  = len(checks)
    overall = "PASS" if passed == total else "FAIL"
    # informational (does not affect PASS/FAIL)
    checks.append(("Live data sources", True,
                   f"{MARKET['sources_active']}/{MARKET['sources_total']} connected"))
    return checks, passed, total, overall


# ─────────────────────────────────────────────────────────────────────
# PAGE
# ─────────────────────────────────────────────────────────────────────
TECH_SPECS = [
    ("Max TPS", "1,500", "~30", "65,000", "7"),
    ("Settlement", "3-5 sec", "12 sec", "0.4 sec", "60 min"),
    ("Tx Fee", "$0.0002", "$1-50", "$0.001", "$1-20"),
    ("Energy Use", "0.0079 kWh", "0.03 kWh", "0.00051 kWh", "1,173 kWh"),
    ("Consensus", "FBC", "PoS", "PoH+PoS", "PoW"),
    ("ISO 20022", "\u2705 Native", "\u274C No", "\u274C No", "\u274C No"),
    ("Supply Cap", "100B fixed", "Unlimited", "Fixed", "21M"),
]

USE_CASES = [
    ("\u26A1", "Cross-Border Payments (ODL)", "var(--gr)",
     "Banks use XRP as bridge currency to eliminate pre-funded nostro accounts. Saves up to 60% vs SWIFT. Active in 8+ corridors."),
    ("\U0001F4B5", "RLUSD Stablecoin Settlement", "var(--bl)",
     "NYDFS-regulated USD stablecoin on XRPL. Enables stable-value settlement while XRP handles liquidity bridge function."),
    ("\U0001F3DB\uFE0F", "Central Bank Digital Currency", "var(--yl)",
     "Bhutan (live), Montenegro (pilot), Palau (live), Colombia, Hong Kong exploring XRPL as CBDC settlement layer."),
    ("\U0001F3A8", "NFT Marketplace (XLS-20)", "var(--tq)",
     "Native NFT standard on XRPL. Low-fee minting ($0.0002), instant settlement. Multiple marketplaces active."),
    ("\U0001F4C8", "Tokenized Real-World Assets", "var(--or)",
     "Sologenic tokenizes stocks/ETFs on XRPL. Institutional-grade settlement infrastructure for the RWA market."),
    ("\u2697\uFE0F", "DeFi & AMM Protocols", "var(--rd)",
     "Native AMM live on XRPL mainnet. DEX built into protocol level. No smart contract risk \u2014 settlement at protocol layer."),
    ("\U0001F517", "ISO 20022 Payment Rails", "var(--gr)",
     "XRPL natively supports ISO 20022 data fields \u2014 the same standard SWIFT, Fedwire, CHAPS and TARGET2 are migrating to."),
    ("\U0001F310", "Micropayments & Streaming", "var(--bl)",
     "XRP enables sub-cent micropayments at $0.0002/tx \u2014 streaming money, API monetization, IoT payments."),
    ("\U0001F916", "AI Agent Payments", "var(--tq)",
     "Ripple integrating XRP/XRPL for AI agent-to-agent payments \u2014 instant, programmable, low-cost settlement."),
]

ENTERPRISE_CATEGORY_LABELS = {
    "A": "\U0001F680 ODL/XRP Live", "B": "\U0001F3DB\uFE0F Global Banks", "C": "\U0001F6E0\uFE0F Tech/Custody",
    "D": "\U0001F30D Regional", "E": "\U0001F7E1 ETF/Treasury",
}
ENTERPRISE_CATEGORY_COLORS = {"A": "var(--gr)", "B": "var(--bl)", "C": "var(--tq)", "D": "var(--or)", "E": "var(--yl)"}

ENTERPRISE_SEED = [
    # Category A: Live ODL / XRP Production Users (23)
    ("SBI Remit / SBI Holdings", "\U0001F1EF\U0001F1F5 Japan", "A", "LIVE ODL", "Multi-corridor APAC retail & commercial remittance powered by XRP"),
    ("Tranglo", "\U0001F1F2\U0001F1FE Malaysia/SE Asia", "A", "LIVE ODL", "Regional processing giant fully integrated into ODL"),
    ("Bitso", "\U0001F1F2\U0001F1FD Mexico/LatAm", "A", "LIVE ODL", "Core liquidity hub routing heavy institutional USD-to-MXN lanes"),
    ("Travelex Bank", "\U0001F1E7\U0001F1F7 Brazil", "A", "LIVE ODL", "First operational Latin American bank using XRP liquidity corridors"),
    ("Zand Bank", "\U0001F1E6\U0001F1EA UAE", "A", "LIVE", "Digital corporate bank processing payments via XRP and RLUSD"),
    ("AMINA Bank", "\U0001F1E8\U0001F1ED Switzerland", "A", "LIVE", "FINMA-regulated digital asset institution with live native Ripple Payments"),
    ("Siam Commercial Bank", "\U0001F1F9\U0001F1ED Thailand", "A", "LIVE ODL", "Active live ODL corridors for inbound Japanese capital"),
    ("UnionBank", "\U0001F1F5\U0001F1ED Philippines", "A", "LIVE ODL", "Automated processing for inbound domestic overseas worker remittances"),
    ("CIBC", "\U0001F1E8\U0001F1E6 Canada", "A", "LIVE ODL", "Settles institutional growth transfers via ODL infrastructure"),
    ("Qatar National Bank", "\U0001F1F6\U0001F1E6 Qatar", "A", "LIVE ODL", "Cross-border pipeline targeting Philippine remittance partners"),
    ("ChinaBank", "\U0001F1F5\U0001F1ED Philippines", "A", "LIVE", "Clears Gulf-region corporate payments anchored to digital liquidity"),
    ("Independent Reserve", "\U0001F1E6\U0001F1FA Australia", "A", "LIVE", "Regional liquidity exchange partner providing settlement architecture"),
    ("BTC Markets", "\U0001F1E6\U0001F1FA Australia", "A", "LIVE", "Currency bridge managing the AUD leg of regional ODL clearing"),
    ("Coins.ph", "\U0001F1F5\U0001F1ED Philippines", "A", "LIVE ODL", "Digital consumer network handling incoming XRP liquid conversions"),
    ("FlashFX", "\U0001F1E6\U0001F1FA Australia", "A", "LIVE ODL", "Automated FX software routing transfers via on-chain token paths"),
    ("Mercury FX", "\U0001F1EC\U0001F1E7 UK", "A", "LIVE ODL", "Enterprise currency platform processing instant commercial payments via XRP"),
    ("Cuallix", "\U0001F1FA\U0001F1F8/\U0001F1F2\U0001F1FD USA/Mexico", "A", "PIONEER", "First fintech to pilot original xRapid/ODL settlement engines"),
    ("X Money", "\U0001F310 Global", "A", "LIVE", "Retail cross-border digital financial platform using decentralized settlement"),
    ("Novatti", "\U0001F1E6\U0001F1FA Australia", "A", "LIVE ODL", "Payments processor using XRP ledger routes for Southeast Asian corridors"),
    ("iRemit", "\U0001F1F5\U0001F1ED Philippines", "A", "LIVE", "Non-bank remittance giant using ledger for real-time treasury management"),
    ("Azimo", "\U0001F1EA\U0001F1FA Europe", "A", "LIVE", "International digital money transmitter processing enterprise payouts"),
    ("Pyypl", "\U0001F30D Middle East/Africa", "A", "LIVE ODL", "Blockchain fintech offering consumer digital wallets via ODL"),
    ("MoneyMatch", "\U0001F1F2\U0001F1FE Malaysia", "A", "LIVE", "Digital conversion firm routing commercial payments to European endpoints"),
    # Category B: Global Banking Giants (32)
    ("Bank of America", "\U0001F1FA\U0001F1F8 USA", "B", "PILOT", "Infrastructure pilot participant holding patents referencing XRP settlement"),
    ("Banco Santander", "\U0001F1EA\U0001F1F8 Spain/UK", "B", "PRODUCTION", "Powers international One Pay FX app via RippleNet messaging"),
    ("PNC Bank", "\U0001F1FA\U0001F1F8 USA", "B", "PRODUCTION", "First major domestic U.S. institutional network client"),
    ("American Express", "\U0001F1FA\U0001F1F8 USA", "B", "PRODUCTION", "Commercial B2B international payments clearing partner"),
    ("Deutsche Bank", "\U0001F1E9\U0001F1EA Germany", "B", "PILOT", "Combined Ripple blockchain architecture with legacy SWIFT mechanisms"),
    ("Standard Chartered", "\U0001F1EC\U0001F1E7 UK", "B", "PRODUCTION", "Core early corporate investor and active digital clearing hub collaborator"),
    ("JPMorgan Chase", "\U0001F310 Global", "B", "PARTICIPANT", "Overlapping participant in multi-network settlement ledger groups"),
    ("HSBC", "\U0001F1EC\U0001F1E7 UK", "B", "PARTICIPANT", "Multi-national banking network mapped via active system routing IDs"),
    ("MUFG Bank", "\U0001F1EF\U0001F1F5 Japan", "B", "PRODUCTION", "Tier-1 retail giant optimizing transaction messaging across APAC"),
    ("ING Group", "\U0001F1F3\U0001F1F1 Netherlands", "B", "REGISTERED", "Multi-national bank registered in regional backend messaging directories"),
    ("BBVA", "\U0001F1EA\U0001F1F8 Spain", "B", "PILOT", "Corporate banking implementing cross-border branch liquidity trials"),
    ("Commonwealth Bank (CBA)", "\U0001F1E6\U0001F1FA Australia", "B", "PILOT", "Major retail institution participating in pilot ecosystem networks"),
    ("Westpac", "\U0001F1E6\U0001F1FA Australia", "B", "REGISTERED", "Registered network member maintaining live backend communication IDs"),
    ("ANZ Bank", "\U0001F1E6\U0001F1FA Australia", "B", "HISTORICAL", "Historical testing partner of the underlying clearing protocol"),
    ("National Australia Bank (NAB)", "\U0001F1E6\U0001F1FA Australia", "B", "REGISTERED", "Incorporated into the ledger settlement network indexing systems"),
    ("Macquarie Bank", "\U0001F1E6\U0001F1FA Australia", "B", "REGISTERED", "Financial and transaction group listed on official routing logs"),
    ("Royal Bank of Canada (RBC)", "\U0001F1E8\U0001F1E6 Canada", "B", "EXPLORING", "Explored the decentralized rail protocol for automated settlement"),
    ("SEB", "\U0001F1F8\U0001F1EA Sweden", "B", "PRODUCTION", "Operates high-volume corporate lines over Ripple software rails"),
    ("UBS", "\U0001F1E8\U0001F1ED Switzerland", "B", "EVALUATING", "Asset and investment firm evaluating high-speed distributed ledgers"),
    ("BMO Financial Group", "\U0001F1E8\U0001F1E6 Canada", "B", "EXPLORING", "North American commercial entity exploring cross-border clearing efficiency"),
    ("Intesa Sanpaolo", "\U0001F1EE\U0001F1F9 Italy", "B", "PARTICIPANT", "Enterprise participant tracking structural digital payment innovations"),
    ("Akbank", "\U0001F1F9\U0001F1F7 Turkey", "B", "PILOT", "Early regional banking partner conducting secure real-time automated tests"),
    ("Axis Bank", "\U0001F1EE\U0001F1F3 India", "B", "LIVE", "Live infrastructure client managing real-time regional transaction tunnels"),
    ("IndusInd Bank", "\U0001F1EE\U0001F1F3 India", "B", "LIVE", "Captures inbound international money transfers using decentralized engines"),
    ("Kotak Mahindra Bank", "\U0001F1EE\U0001F1F3 India", "B", "LIVE", "Fintech clearing provider handling instant retail capital inflows"),
    ("Yes Bank", "\U0001F1EE\U0001F1F3 India", "B", "LIVE", "Commercial institution conducting high-velocity payment remittance operations"),
    ("Federal Bank", "\U0001F1EE\U0001F1F3 India", "B", "LIVE", "Major localized retail bank utilizing automated routing systems"),
    ("Shinhan Bank", "\U0001F1F0\U0001F1F7 South Korea", "B", "LIVE", "Top South Korean network client maintaining active system access keys"),
    ("Woori Bank", "\U0001F1F0\U0001F1F7 South Korea", "B", "LIVE", "Multi-channel asset institution utilizing programmatic payment lines"),
    ("Krungsri (Bank of Ayudhya)", "\U0001F1F9\U0001F1ED Thailand", "B", "LIVE", "Streamlines real-time corporate pipelines between Thailand and Japan"),
    ("CIMB Bank", "\U0001F1F2\U0001F1FE Malaysia", "B", "LIVE", "Deep integration node managing corridors across ASEAN borders"),
    ("BDO Unibank", "\U0001F1F5\U0001F1ED Philippines", "B", "LIVE", "Major destination settlement point for international inbound money streams"),
    # Category C: Enterprise Tech, Custody & Infrastructure (25)
    ("Amazon Web Services (AWS)", "\U0001F310 Global", "C", "INFRASTRUCTURE", "Hosts architecture allowing global nodes to run XRPL validation configurations"),
    ("Finastra", "\U0001F1EC\U0001F1E7 UK", "C", "PRODUCTION", "Core banking software opening network access to 2,000+ regional banks"),
    ("Deloitte", "\U0001F310 Global", "C", "PRODUCTION", "Integrated distributed financial systems into client business models"),
    ("DZ Bank", "\U0001F1E9\U0001F1EA Germany", "C", "PRODUCTION", "Leverages digital custody solutions for tokenized asset issuance"),
    ("BNY Mellon", "\U0001F1FA\U0001F1F8 USA", "C", "PRODUCTION", "Primary tier-1 institutional reserve custodian for stablecoin offerings"),
    ("DBS Bank", "\U0001F1F8\U0001F1EC Singapore", "C", "LIVE", "Southeast Asian institution utilizing bank-grade digital asset vaults"),
    ("Kbank", "\U0001F1F0\U0001F1F7 South Korea", "C", "LIVE", "Digital platform implementing secure cryptographic wallet structures"),
    ("Kyobo Life Insurance", "\U0001F1F0\U0001F1F7 South Korea", "C", "LIVE", "Utilizing token ledger blueprint for corporate structural bond settlement"),
    ("BDACS", "\U0001F1F0\U0001F1F7 South Korea", "C", "LIVE", "Regulated secure vault platform for native ledger token storage"),
    ("Hidden Road", "\U0001F1FA\U0001F1F8 USA", "C", "EXPANDING", "Major institutional prime brokerage expanding liquidity paths for digital assets"),
    ("GTreasury", "\U0001F1FA\U0001F1F8 USA", "C", "LIVE", "Corporate liquidity software suite managing modern capital balance sheets"),
    ("Metaco", "\U0001F1E8\U0001F1ED Switzerland", "C", "ACQUIRED", "Institutional crypto custody firm acquired by Ripple to secure bank assets globally"),
    ("Temenos", "\U0001F1E8\U0001F1ED Switzerland", "C", "PRODUCTION", "Core banking software provider embedding automated accounting rails"),
    ("Accenture", "\U0001F310 Global", "C", "PRODUCTION", "Consulting giant managing global deployment strategies for payment architecture"),
    ("CGI Group", "\U0001F1E8\U0001F1E6 Canada", "C", "PRODUCTION", "IT consulting firm incorporating decentralized financial frameworks"),
    ("Modulr", "\U0001F1EC\U0001F1E7 UK/Europe", "C", "LIVE", "Payments provider optimizing massive local commercial transaction times"),
    ("Sentbe", "\U0001F1F0\U0001F1F7 South Korea", "C", "LIVE", "High-speed international remittance engine using the global banking network"),
    ("Currencycloud", "\U0001F1EC\U0001F1E7 UK", "C", "LIVE", "B2B multi-currency platform streamlining automated foreign exchange"),
    ("Nium", "\U0001F1F8\U0001F1EC Singapore", "C", "LIVE", "Fintech provider optimizing massive outbound payment paths across global corridors"),
    ("InstaReM", "\U0001F1F8\U0001F1EC Singapore", "C", "LIVE", "High-speed digital payment gateway connected via localized nodes"),
    ("BeeTech", "\U0001F1E7\U0001F1F7 Brazil", "C", "LIVE", "Digital financial operator executing automated Latin American clearings"),
    ("Fidor Bank", "\U0001F1E9\U0001F1EA Germany", "C", "PIONEER", "Digital banking pioneer integrating alternative clearing protocol tools"),
    ("Sabadell", "\U0001F1EA\U0001F1F8 Spain", "C", "LIVE", "Commercial infrastructure partner running real-time corporate data modules"),
    ("Cross River Bank", "\U0001F1FA\U0001F1F8 USA", "C", "LIVE", "Financial tech enabler providing direct underlying banking backbone"),
    ("Frankenmuth Credit Union", "\U0001F1FA\U0001F1F8 USA", "C", "LIVE", "Local cooperative providing digital asset services to local consumers"),
    # Category D: Regional / Middle East / LatAm (13)
    ("Al Ansari Exchange", "\U0001F1E6\U0001F1EA UAE", "D", "LIVE", "High-volume Middle Eastern exchange network routing institutional transfers"),
    ("National Bank of Fujairah", "\U0001F1E6\U0001F1EA UAE", "D", "LIVE", "Trade finance group optimizing real-time B2B payment workflows"),
    ("Saudi Central Bank (SAMA)", "\U0001F1F8\U0001F1E6 Saudi Arabia", "D", "PILOT", "Central entity piloting distributed frameworks for commercial branches"),
    ("National Bank of Kuwait (NBK)", "\U0001F1F0\U0001F1FC Kuwait", "D", "LIVE", "Runs international corporate transfer paths targeting the Gulf"),
    ("RAKBANK", "\U0001F1E6\U0001F1EA UAE", "D", "LIVE", "Integrates transaction routes to improve speed across enterprise pipelines"),
    ("Itau Unibanco", "\U0001F1E7\U0001F1F7 Brazil", "D", "LIVE", "Giant South American banking provider utilizing alternative communication networks"),
    ("Banco Rendimento", "\U0001F1E7\U0001F1F7 Brazil", "D", "LIVE", "Foreign currency commercial bank using optimized digital payment tunnels"),
    ("Intercorp", "\U0001F1F5\U0001F1EA Peru", "D", "LIVE", "Large conglomerate stabilizing localized payment legs for regional retail assets"),
    ("Faysal Bank", "\U0001F1F5\U0001F1F0 Pakistan", "D", "LIVE", "Specialized commercial banking provider processing inward retail cash flows"),
    ("Bank Alfalah", "\U0001F1F5\U0001F1F0 Pakistan", "D", "LIVE", "Manages automated digital channels targeting the UAE-to-Pakistan corridor"),
    ("bKash", "\U0001F1E7\U0001F1E9 Bangladesh", "D", "LIVE", "Mobile financial giant plugged in to capture worker remittances"),
    ("Vietcombank", "\U0001F1FB\U0001F1F3 Vietnam", "D", "PILOT", "Explores modern asset frameworks under regional digital banking pilots"),
    ("Interbank", "\U0001F1F5\U0001F1EA Peru", "D", "LIVE", "Traditional retail banking destination tied to alternative clearing systems"),
    # Category E: ETF Issuers & Corporate Treasury (7)
    ("Grayscale Investments", "\U0001F1FA\U0001F1F8 USA", "E", "LIVE ETF", "Asset manager operating the regulated Grayscale XRP Trust and spot fund"),
    ("Bitwise Asset Management", "\U0001F1FA\U0001F1F8 USA", "E", "LIVE ETF", "Regulated Wall Street provider offering institutional XRP exposure"),
    ("Franklin Templeton", "\U0001F1FA\U0001F1F8 USA", "E", "FILED", "Legacy asset firm filing for exchange-traded digital investment products"),
    ("Canary Capital Partners", "\U0001F1FA\U0001F1F8 USA", "E", "LIVE ETF", "Asset management firm deploying institutional-grade XRP capital avenues"),
    ("Hashdex Asset Management", "\U0001F310 Global", "E", "LIVE ETF", "Global investment manager offering systemic access to ledger tokens"),
    ("Worksport Ltd.", "\U0001F1FA\U0001F1F8 USA", "E", "TREASURY", "Clean automotive developer utilizing digital assets for inventory clearings"),
    ("Nature's Miracle Holding", "\U0001F1FA\U0001F1F8 USA", "E", "TREASURY", "Agriculture Tech firm implementing a $20M Corporate Treasury on the XRPL"),
]

COUNTRY_STATUS = [
    ("United States", "\U0001F1FA\U0001F1F8", "CONTESTED", "SEC lawsuit settled; XRP non-security ruling in programmatic sales. Evolving clarity."),
    ("European Union", "\U0001F1EA\U0001F1FA", "LEGAL", "MiCA regulation fully in force. XRP classified as crypto-asset, not security."),
    ("United Kingdom", "\U0001F1EC\U0001F1E7", "LEGAL", "FCA regulated. Crypto-asset promotion rules apply. No XRP-specific restrictions."),
    ("Japan", "\U0001F1EF\U0001F1F5", "LEGAL", "FSA regulated. XRP officially recognized as a crypto-asset. SBI Holdings major partner."),
    ("South Korea", "\U0001F1F0\U0001F1F7", "LEGAL", "FSC/FSS regulated. Major trading volume on Upbit and Bithumb."),
    ("Singapore", "\U0001F1F8\U0001F1EC", "LEGAL", "MAS regulated under PSA. Ripple holds a Major Payment Institution license."),
    ("UAE", "\U0001F1E6\U0001F1EA", "LEGAL", "VARA (Dubai) and ADGM (Abu Dhabi) regulated. Ripple has a regional HQ in Dubai."),
    ("Switzerland", "\U0001F1E8\U0001F1ED", "LEGAL", "FINMA regulated. Crypto Valley in Zug. Openly traded on licensed exchanges."),
    ("Australia", "\U0001F1E6\U0001F1FA", "LEGAL", "ASIC regulated. Crypto exchanges licensed. No XRP-specific restrictions."),
    ("Germany", "\U0001F1E9\U0001F1EA", "LEGAL", "BaFin regulated under MiCA. Deutsche B\u00F6rse-listed crypto products available."),
    ("Brazil", "\U0001F1E7\U0001F1F7", "LEGAL", "Banco Central do Brasil regulated. Bitso is a major corridor partner."),
    ("Canada", "\U0001F1E8\U0001F1E6", "LEGAL", "CSA regulated. Crypto ETPs listed on TSX. Active Canada-Mexico ODL corridor."),
    ("Mexico", "\U0001F1F2\U0001F1FD", "LEGAL", "CNBV regulated. Major Ripple ODL remittance corridor with the United States."),
    ("Philippines", "\U0001F1F5\U0001F1ED", "LEGAL", "BSP regulated. Major remittance corridor for OFW payments via Ripple partners."),
    ("India", "\U0001F1EE\U0001F1F3", "TAXED", "30% crypto tax + 1% TDS. Legal to hold and trade; framework still developing."),
    ("Thailand", "\U0001F1F9\U0001F1ED", "LEGAL", "SEC Thailand regulated. Listed on licensed exchanges with active Ripple partnerships."),
    ("Nigeria", "\U0001F1F3\U0001F1EC", "RESTRICTED", "CBN lifted crypto ban in 2023; regulated under SEC Nigeria, bank restrictions remain."),
    ("China", "\U0001F1E8\U0001F1F3", "BANNED", "All crypto trading banned since 2021. Citizens may not legally trade or hold XRP."),
    ("Russia", "\U0001F1F7\U0001F1FA", "RESTRICTED", "Limited legal use. Crypto as payment banned; trading tolerated but heavily restricted."),
    ("Saudi Arabia", "\U0001F1F8\U0001F1E6", "PENDING", "SAMA evaluating framework. Not officially prohibited but no clear legal status."),
]
COUNTRY_STATUS_COLORS = {
    "LEGAL": "var(--gr)", "CONTESTED": "var(--yl)", "TAXED": "var(--or)",
    "RESTRICTED": "var(--or)", "BANNED": "var(--rd)", "PENDING": "var(--bl)",
}

ETF_TRACKER = [
    {"applicant": "21Shares", "product": "XRP ETP", "market": "Europe", "status": "LIVE", "date": "2019",
     "note": "Actively trading on SIX Swiss Exchange. AUM growing."},
    {"applicant": "CoinShares", "product": "XRP ETP", "market": "Europe", "status": "LIVE", "date": "2020",
     "note": "Listed on multiple European exchanges. Institutional grade."},
    {"applicant": "WisdomTree", "product": "XRP ETP", "market": "Europe", "status": "LIVE", "date": "2021",
     "note": "FCA and EU regulated. Available in UK and Europe."},
    {"applicant": "VanEck", "product": "XRP ETP", "market": "Europe", "status": "LIVE", "date": "2021",
     "note": "Deutsche B\u00F6rse listed. Physically backed."},
    {"applicant": "Bitwise", "product": "XRP ETF", "market": "USA", "status": "FILED", "date": "2025",
     "note": "SEC review pending. Filed as a spot XRP ETF."},
    {"applicant": "WisdomTree", "product": "XRP ETF", "market": "USA", "status": "FILED", "date": "2025",
     "note": "US spot ETF filing submitted to the SEC."},
    {"applicant": "ProShares", "product": "XRP Futures ETF", "market": "USA", "status": "REVIEW", "date": "2025",
     "note": "Futures-based product under SEC consideration."},
    {"applicant": "Canary Capital", "product": "XRP ETF", "market": "USA", "status": "FILED", "date": "2024",
     "note": "First US spot XRP ETF filing. Pioneer application."},
]
ETF_STATUS_COLORS = {"LIVE": "var(--gr)", "FILED": "var(--yl)", "REVIEW": "var(--or)"}

SEC_TIMELINE = [
    ("Dec 2020", "SEC Files Lawsuit", "SEC sues Ripple Labs and its CEO for a $1.3B unregistered securities offering.", False),
    ("Nov 2022", "Judge Sides on Documents", "Court orders release of the Hinman speech documents.", False),
    ("Jul 2023", "Historic Partial Victory", "Judge Torres rules XRP is NOT a security in programmatic exchange sales.", True),
    ("Aug 2023", "SEC Appeals", "SEC files notice of appeal on the programmatic sales ruling.", False),
    ("Oct 2024", "SEC Drops Charges", "SEC drops charges against Ripple's leadership personally.", True),
    ("Mar 2025", "Settlement Reached", "Ripple and SEC settle. $50M fine paid vs. the original $2B demand.", True),
    ("2026", "Post-Settlement Era", "XRP operating in post-lawsuit clarity under a crypto-friendlier SEC.", True),
]

MICA_CALENDAR = [
    ("Jun 2023", "MiCA Published", "EU Markets in Crypto-Assets regulation officially published.", True),
    ("Dec 2024", "Stablecoin Rules Live", "Title III/IV provisions effective; RLUSD and issuers must comply.", True),
    ("Dec 2024", "Full MiCA in Force", "Complete framework operational across all 27 EU member states.", True),
    ("2025", "National Implementation", "Member states complete national regulatory adaptations.", False),
    ("2025-2026", "CASP Licensing Wave", "Crypto Asset Service Providers complete MiCA licensing.", False),
    ("2026+", "MiCA Review Clause", "European Commission reviews effectiveness and possible DeFi/NFT expansion.", False),
]

ODL_CORRIDORS = [
    {"from_c": "\U0001F1FA\U0001F1F8 USA", "to_c": "\U0001F1F2\U0001F1FD Mexico", "partner": "Bitso", "status": "ACTIVE",
     "note": "Largest ODL corridor globally \u2014 high daily volume via Bitso."},
    {"from_c": "\U0001F1FA\U0001F1F8 USA", "to_c": "\U0001F1F5\U0001F1ED Philippines", "partner": "Coins.ph", "status": "ACTIVE",
     "note": "Major OFW remittance route serving millions of Filipino workers."},
    {"from_c": "\U0001F1EA\U0001F1FA Europe", "to_c": "\U0001F1F2\U0001F1FD Mexico", "partner": "Bitso", "status": "ACTIVE",
     "note": "Cross-Atlantic corridor expanding with MiCA regulatory clarity."},
    {"from_c": "\U0001F1EF\U0001F1F5 Japan", "to_c": "\U0001F1F5\U0001F1ED Philippines", "partner": "SBI Remit", "status": "ACTIVE",
     "note": "SBI Holdings' flagship ODL corridor \u2014 high volume."},
    {"from_c": "\U0001F1E6\U0001F1FA Australia", "to_c": "\U0001F1F5\U0001F1ED Philippines", "partner": "FlashFX", "status": "ACTIVE",
     "note": "AUD to PHP remittance \u2014 major OFW corridor."},
    {"from_c": "\U0001F1EC\U0001F1E7 UK", "to_c": "\U0001F1F3\U0001F1EC Nigeria", "partner": "Ripple Partner", "status": "GROWING",
     "note": "Africa expansion focus with Flutterwave integration."},
    {"from_c": "\U0001F1FA\U0001F1F8 USA", "to_c": "\U0001F1EE\U0001F1F3 India", "partner": "Various", "status": "GROWING",
     "note": "Largest remittance market globally \u2014 $100B+ annual flows."},
    {"from_c": "\U0001F1F8\U0001F1EC Singapore", "to_c": "\U0001F30F SE Asia", "partner": "Various", "status": "GROWING",
     "note": "Regional hub \u2014 Ripple's Singapore MPI license is active."},
]

ISO20022_ADOPTERS = [
    {"name": "SWIFT gpi", "region": "Global", "note": "Fully ISO 20022 compliant since 2023."},
    {"name": "TARGET2", "region": "EU", "note": "ECB's large-value payment system, migrated Nov 2022."},
    {"name": "CHAPS", "region": "UK", "note": "Bank of England high-value payment system, migrated 2023."},
    {"name": "Fedwire", "region": "USA", "note": "US Federal Reserve system, migration completed 2024."},
    {"name": "CHIPS", "region": "USA", "note": "Clearing House Interbank Payments System, ISO 20022 compliant."},
    {"name": "SIC", "region": "Switzerland", "note": "Swiss Interbank Clearing system, migrated 2023."},
    {"name": "HVPS+", "region": "Canada", "note": "High Value Payment System Canada, completed 2023."},
    {"name": "RITS", "region": "Australia", "note": "Reserve Bank Information Transfer System, migrated."},
]


def country_grid_html():
    out = ""
    for name, flag, status, note in COUNTRY_STATUS:
        col = COUNTRY_STATUS_COLORS.get(status, "var(--tx)")
        out += (
            f'<div class="cg-card" style="border-color:{col}55">'
            f'<div class="cg-top"><span class="cg-flag">{flag}</span>'
            f'<span class="cg-name">{html.escape(name)}</span></div>'
            f'<span class="odl-status" style="background:{col}26;color:{col}">{status}</span>'
            f'<div class="cg-note">{html.escape(note)}</div>'
            f'</div>'
        )
    return out

def etf_tracker_html():
    out = ""
    for e in ETF_TRACKER:
        col = ETF_STATUS_COLORS.get(e["status"], "var(--tx)")
        out += (
            f'<tr><td style="font-weight:700;color:var(--br)">{html.escape(e["applicant"])}</td>'
            f'<td>{html.escape(e["product"])}</td><td style="color:var(--tx)">{html.escape(e["market"])}</td>'
            f'<td><span class="odl-status" style="background:{col}26;color:{col}">{e["status"]}</span></td>'
            f'<td style="color:var(--tx)">{html.escape(e["date"])}</td>'
            f'<td style="color:var(--tx);max-width:220px">{html.escape(e["note"])}</td></tr>'
        )
    return out

def sec_timeline_html():
    out = ""
    for date, event, detail, major in SEC_TIMELINE:
        dot_col = "var(--gr)" if major else "var(--yl)"
        dot_sz = "16px" if major else "11px"
        out += (
            f'<div class="tl-node" style="flex-basis:170px;min-width:170px">'
            f'<div class="tl-top">'
            f'<div class="tl-year" style="color:{dot_col};font-size:15px">{date}</div>'
            f'<div class="tl-dot" style="width:{dot_sz};height:{dot_sz};background:{dot_col}"></div>'
            f'</div>'
            f'<div class="tl-event" style="font-size:15px">{html.escape(event)}</div>'
            f'<div class="tl-detail" style="font-size:12px">{html.escape(detail)}</div>'
            f'</div>'
        )
    return out

def mica_calendar_html():
    out = ""
    for date, event, detail, done in MICA_CALENDAR:
        icon = "\u2705" if done else "\u25CB"
        col = "var(--gr)" if done else "var(--tx)"
        out += (
            f'<div class="mica-row">'
            f'<span class="mica-icon" style="color:{col}">{icon}</span>'
            f'<span class="mica-date">{html.escape(date)}</span>'
            f'<span class="mica-event" style="color:{col}">{html.escape(event)}</span>'
            f'<span class="mica-detail">{html.escape(detail)}</span>'
            f'</div>'
        )
    return out

def cbdc_grid_html():
    out = ""
    for name, flag, project, status, detail in PARTNERSHIPS:
        col = STATUS_COLORS.get(status, "var(--tx)")
        out += (
            f'<div class="cg-card" style="border-color:{col}55">'
            f'<div class="cg-top"><span class="cg-flag">{flag}</span>'
            f'<span class="cg-name">{html.escape(name)}</span></div>'
            f'<span class="odl-status" style="background:{col}26;color:{col}">{status}</span>'
            f'<div class="cg-note"><b style="color:var(--br)">{html.escape(project)}</b><br>{html.escape(detail)}</div>'
            f'</div>'
        )
    return out


def odl_corridors_html():
    out = ""
    for c in ODL_CORRIDORS:
        cls = c["status"].lower()
        out += (
            f'<div class="odl-item"><span class="odl-route">{c["from_c"]} \u2192 {c["to_c"]}</span>'
            f'<span class="odl-status {cls}">{c["status"]}</span>'
            f'<span style="color:var(--tx)">via {html.escape(c["partner"])}</span>'
            f'<span class="odl-note">{html.escape(c["note"])}</span></div>'
        )
    return out

def iso20022_html():
    out = ""
    for a in ISO20022_ADOPTERS:
        out += (
            f'<div class="iso-item"><span class="odl-status live">LIVE</span>'
            f'<span style="font-weight:700;color:var(--br)">{html.escape(a["name"])}</span>'
            f'<span style="color:var(--tx)">{html.escape(a["region"])}</span>'
            f'<span class="odl-note">{html.escape(a["note"])}</span></div>'
        )
    return out


def render_page():
    checks, passed, total, overall = run_preflight()
    overall_color = "#48ff82" if overall == "PASS" else "#ff4060"
    boot_str = BOOT_TIME.strftime("%Y-%m-%d %H:%M:%S UTC")
    hdr_feeds_active = NEWS["feeds_active"]
    hdr_feeds_total = NEWS["feeds_total"]

    # Breaking News bar — real breaking stories when present, home-base message otherwise
    _pool = NEWS.get("pool", [])
    _breaking_stories = sorted((s for s in _pool if s.get("breaking")), key=lambda s: s["dt"], reverse=True)
    if _breaking_stories:
        _top_break = _breaking_stories[0]
        bktext = f'{html.escape(_top_break["source"])}: {html.escape(_top_break["title"])}'
    elif _pool:
        bktext = "\U0001F6F0\uFE0F Monitoring live feeds \u2014 breaking alerts appear here automatically."
    else:
        bktext = "\U0001F6F0\uFE0F Connecting to news feeds \u2014 breaking alerts appear here automatically."

    # Whale Alert Feed — real whale-tagged stories when present, home-base placeholder otherwise
    _whale_stories = sorted((s for s in _pool if s.get("category") == "Whale"), key=lambda s: s["dt"], reverse=True)[:8]
    if _whale_stories:
        whale_feed_html = "".join(
            f'<div class="wa-row"><span class="wa-src">{html.escape(w["source"])}</span>'
            f'<a class="wa-hl" href="{html.escape(w["link"], quote=True)}" target="_blank" rel="noopener">{html.escape(w["title"])}</a>'
            f'<span class="wa-time">{_time_ago(w["dt"])}</span></div>'
            for w in _whale_stories
        )
        whale_ts_val = _time_ago(_whale_stories[0]["dt"])
    else:
        whale_feed_html = (
            '<div class="home-base"><div class="home-base-icon">\U0001F433</div>'
            '<div class="home-base-title">Monitoring On-Chain Movements</div>'
            '<div class="home-base-sub">Whale-sized transfers surface here automatically as soon as they appear in the live news feed \u2014 no action needed.</div></div>'
        )
        whale_ts_val = "\u2014"

    # XRP price — red or green by movement
    if MARKET["xrp_price"] is not None:
        chg = MARKET["xrp_chg"] or 0
        price_color = "#48ff82" if chg >= 0 else "#ff4060"
        arrow = "\u25B2" if chg >= 0 else "\u25BC"
        price_str = f"${MARKET['xrp_price']:.4f}"
        chg_str = f"{arrow} {abs(chg):.2f}%"
    else:
        price_color = "#8099b3"
        price_str = "\u2014"
        chg_str = ""

    sources_str = f"{MARKET['sources_active']} / {MARKET['sources_total']}"
    fng_label = MARKET["fng_label"] or ""
    fng_bar = fng_bar_html(MARKET["fng"])

    # ── Section 3 values ──
    def rsi_parts(v):
        if v is None:
            return "--", "--", "var(--tx)", 50
        if v >= 70:
            col, lbl = "#ff4060", "Overbought"
        elif v <= 30:
            col, lbl = "#48ff82", "Oversold"
        else:
            col, lbl = "#75bcff", "Neutral"
        return f"{v:.1f}", lbl, col, max(0, min(100, v))

    r1h_val, r1h_lbl, r1h_col, r1h_pct = rsi_parts(MARKET["rsi_1h"])
    r1d_val, r1d_lbl, r1d_col, r1d_pct = rsi_parts(MARKET["rsi_1d"])

    cur = MARKET["xrp_price"]
    lo, hi = MARKET["w52_low"], MARKET["w52_high"]
    if cur and lo and hi and hi > lo:
        w52_pos = (cur - lo) / (hi - lo) * 100
        w52_low_s  = f"${lo:.4f}"
        w52_high_s = f"${hi:.4f}"
        w52_cur_s  = f"${cur:.4f}"
        w52_from_low  = f"+{(cur-lo)/lo*100:.1f}%"
        w52_from_high = f"{(cur-hi)/hi*100:.1f}%"
        w52_pos_s = f"{w52_pos:.0f}%"
    else:
        w52_pos = 50
        w52_low_s = w52_high_s = w52_cur_s = "--"
        w52_from_low = w52_from_high = w52_pos_s = "--"

    sup, res = MARKET["sr_support"], MARKET["sr_resistance"]
    if sup and res:
        sr_html = (f'<div class="sr-line"><span style="color:var(--rd)">Resistance</span>'
                   f'<span style="color:var(--rd);font-weight:700">${res:.4f}</span></div>'
                   f'<div class="sr-line"><span style="color:var(--tx)">Current</span>'
                   f'<span style="color:var(--br);font-weight:700">${cur:.4f}</span></div>'
                   f'<div class="sr-line"><span style="color:var(--gr)">Support</span>'
                   f'<span style="color:var(--gr);font-weight:700">${sup:.4f}</span></div>') if cur else \
                  '<div class="empty">Calculating from 90-day price history...</div>'
    else:
        sr_html = '<div class="empty">Calculating from 90-day price history...</div>'

    def tm_box(price_then, label):
        if price_then and cur:
            chg = (cur - price_then) / price_then * 100
            col = "#48ff82" if chg >= 0 else "#ff4060"
            arrow = "\u25B2" if chg >= 0 else "\u25BC"
            return (f'<div class="albl">{label}</div>'
                    f'<div class="aval">${price_then:.4f}</div>'
                    f'<div class="asub" style="color:{col}">{arrow} {abs(chg):.1f}%</div>')
        return f'<div class="albl">{label}</div><div class="aval">--</div><div class="asub">--</div>'

    tm_1y_html = tm_box(MARKET["tm_1y"], "1 Year Ago")
    tm_1m_html = tm_box(MARKET["tm_1m"], "1 Month Ago")
    if MARKET["tm_1y"] and cur:
        chg1y = (cur - MARKET["tm_1y"]) / MARKET["tm_1y"] * 100
        updown = "up" if chg1y >= 0 else "down"
        tm_narr = f"XRP is {updown} {abs(chg1y):.1f}% versus one year ago (${MARKET['tm_1y']:.4f} then vs ${cur:.4f} now)."
    else:
        tm_narr = "Loading..."

    # Escrow release date + ecosystem cards
    esc = next_escrow_release()
    esc_date_str = esc.strftime("%b %d, %Y")
    esc_iso = esc.strftime("%Y-%m-%dT%H:%M:%SZ")
    eco_html = ecosystem_cards_html()
    inst_html = institution_cards_html()
    tl_html = timeline_html()
    stories_current = story_rows_html(NEWS["current"])
    stories_weekly = story_rows_html(NEWS["weekly"])

    us = us_intelligence()
    gl = global_pulse()
    _sig_col = {"bullish": "var(--gr)", "bearish": "var(--rd)", "neutral": "var(--yl)", "quiet": "var(--tx)"}
    gl_signals_html = "".join(
        f'<span class="sig-chip" style="color:{_sig_col[gl["signals"][r]]}">'
        f'<span class="sig-dot" style="background:{_sig_col[gl["signals"][r]]}"></span>'
        f'{REGION_FLAGS[r]} {r}: {gl["signals"][r]}</span>'
        for r in REGIONS
    )
    us_ts = us["ts"] or "\u2014"
    gl_ts = gl["ts"] or "\u2014"
    us_pulse = us["pulse"]
    us_regulatory = us["regulatory"]
    us_institutional = us["institutional"]
    gl_pulse = gl["pulse"]
    gl_thesis = gl["thesis"]
    rd_html = regional_discourse_html()

    # Signal Scoreboard
    sb_total, sb_bull, sb_bear, sb_neut = signal_stats()
    _t = sb_total or 1
    sb_bull_pct = round(sb_bull / _t * 100)
    sb_bear_pct = round(sb_bear / _t * 100)
    sb_net = sb_bull - sb_bear
    sb_net_col = "var(--gr)" if sb_net >= 0 else "var(--rd)"
    sb_net_str = f"+{sb_net}" if sb_net >= 0 else str(sb_net)
    sb_fng = MARKET["fng"] if MARKET["fng"] is not None else "\u2014"
    sb_fng_lbl = MARKET["fng_label"] or "\u2014"
    sb_rank = f'#{MARKET["rank"]}' if MARKET.get("rank") else "#\u2014"
    sb_mcap = _fmt_usd(MARKET.get("mcap"))
    sb_vol = _fmt_usd(MARKET.get("vol24"))
    sb_high = f'${MARKET["h24"]:.4f}' if MARKET.get("h24") else "\u2014"
    sb_low = f'${MARKET["l24"]:.4f}' if MARKET.get("l24") else "\u2014"
    sb_feeds = f'{NEWS["feeds_active"]}/{NEWS["feeds_total"]}'

    # On-Chain / Market Vitals — rebuilt to use reliably-populated MARKET data (V95)
    oc_mcap = _fmt_usd(MARKET.get("mcap"))
    oc_rank = f'Rank #{MARKET["rank"]}' if MARKET.get("rank") else "Rank \u2014"
    oc_vol = _fmt_usd(MARKET.get("vol24"))
    if MARKET.get("vol24") and MARKET.get("mcap"):
        oc_volmcap = f'{MARKET["vol24"] / MARKET["mcap"] * 100:.1f}% of mcap'
    else:
        oc_volmcap = "\u2014"
    oc_high = f'${MARKET["h24"]:.4f}' if MARKET.get("h24") else "\u2014"
    oc_low = f'${MARKET["l24"]:.4f}' if MARKET.get("l24") else "\u2014"
    oc_rsi = f'RSI {MARKET["rsi_1d"]:.0f}' if MARKET.get("rsi_1d") else "RSI \u2014"
    oc_52h = f'${MARKET["w52_high"]:.4f}' if MARKET.get("w52_high") else "\u2014"
    oc_52l = f'${MARKET["w52_low"]:.4f}' if MARKET.get("w52_low") else "\u2014"

    # Global News Feed + right rail
    gn_html = global_feed_html()
    gn_total = len(NEWS.get("pool", []))
    gn_shown = min(gn_total, 60)
    # Market Structure (excluded rows dropped: ATH, % Below ATH)
    ms_rank = f'#{MARKET["rank"]}' if MARKET.get("rank") else "\u2014"
    ms_price = f'${MARKET["xrp_price"]:.4f}' if MARKET.get("xrp_price") else "\u2014"
    if MARKET.get("xrp_chg") is not None:
        _c = MARKET["xrp_chg"]
        ms_chg = f'{_c:+.2f}%'
        ms_chg_col = "var(--gr)" if _c >= 0 else "var(--rd)"
    else:
        ms_chg = "\u2014"
        ms_chg_col = "var(--tx)"
    ms_mcap = _fmt_usd(MARKET.get("mcap"))
    ms_vol = _fmt_usd(MARKET.get("vol24"))
    if MARKET.get("vol24") and MARKET.get("mcap"):
        ms_volmcap = f'{MARKET["vol24"] / MARKET["mcap"] * 100:.2f}%'
    else:
        ms_volmcap = "\u2014"
    ms_high = f'${MARKET["h24"]:.4f}' if MARKET.get("h24") else "\u2014"
    ms_low = f'${MARKET["l24"]:.4f}' if MARKET.get("l24") else "\u2014"
    ms_xrpbtc = f'{MARKET["xrpbtc"]:.8f}' if MARKET.get("xrpbtc") else "\u2014"
    esc_next_str = esc_date_str

    # Analytics Lab
    al_ratio = (f'{(sb_bull / sb_bear):.2f}:1 bull/bear' if sb_bear else ('\u221E bull/bear' if sb_bull else '0:0'))
    al_fng = f'{MARKET["fng"]} \u2014 {MARKET["fng_label"]}' if MARKET.get("fng") is not None else "\u2014"
    al_foreign = sum(1 for s in NEWS.get("pool", []) if s.get("foreign"))

    # XRPRadar Leaderboard
    lb_ss = signal_score()
    lb_score = lb_ss["score"]
    lb_label = lb_ss["label"]
    lb_color = lb_ss["color"]
    lb_sources = lb_sources_html()
    lb_regions = lb_regions_html()

    # XRP Intelligence Brief — never show an empty box if a prior edition exists anywhere in the archive
    if not BRIEF.get("sections"):
        try:
            generate_brief()
        except Exception:
            pass
    if not BRIEF.get("sections") and BRIEF_ARCHIVE:
        # Fall back to the most recent archived edition instead of a placeholder
        _latest_key = sorted(BRIEF_ARCHIVE.keys())[-1]
        _latest = BRIEF_ARCHIVE[_latest_key]
        BRIEF["sections"] = dict(_latest.get("sections", {}))
        BRIEF["edition"] = _latest.get("edition")
        BRIEF["generated"] = _latest.get("generated")
        BRIEF["slot_id"] = _latest_key
    _bs = BRIEF.get("sections", {})
    brf_edition = BRIEF.get("edition") or "\u2014"
    brf_gen = BRIEF.get("generated") or "\u2014"
    brf_next = BRIEF.get("next_run") or "\u2014"
    brf_pulse = _bs.get("pulse", "\u2014")
    brf_conn = _bs.get("connections", "\u2014")
    brf_domino = _bs.get("domino", "\u2014")
    brf_regional = _bs.get("regional", "\u2014")
    brf_watch = _bs.get("watchlist", "\u2014")
    brf_tradfi = _bs.get("tradfi", "\u2014")
    wc_html = world_clocks_html()

    # Brief Home — designated schedule strip (this week's 14 editions)
    _now_ct = datetime.now(CENTRAL)
    _live_slot = BRIEF.get("slot_id")
    _next_run_dt = _brief_next_run_dt(_now_ct)
    brf_next_iso = _next_run_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # Single-edition mode: no edition strip. Only the current brief is kept and displayed.
    brf_strip_html = ""
    try:
        _archive_json = json.dumps(BRIEF_ARCHIVE).replace("</", "<\\/")
    except Exception:
        _archive_json = "{}"

    # Unique Displays — Smart Money Score + F&G history
    sm = smart_money()
    sm_score = sm["score"]
    sm_label = sm["label"]
    sm_color = sm["color"]
    sm_rows = "".join(
        f'<div class="sm-row"><span class="sm-k">{html.escape(name)}</span><span class="sm-v">{html.escape(val)}</span></div>'
        for name, val, _ in sm["comps"]
    ) or '<div class="sm-row"><span class="sm-k">Awaiting live signals\u2026</span><span class="sm-v">\u2014</span></div>'
    fng_hist_html = fng_history_html()

    # Longitudinal Value Markers
    def _perf_card(label, val):
        if val is None:
            return f'<div class="lvm-card"><div class="lvm-win">{label}</div><div class="lvm-val" style="color:var(--tx)">\u2014</div><div class="lvm-sub">price change</div></div>'
        col = "var(--gr)" if val >= 0 else "var(--rd)"
        arrow = "\u25B2" if val >= 0 else "\u25BC"
        return (f'<div class="lvm-card"><div class="lvm-win">{label}</div>'
                f'<div class="lvm-val" style="color:{col}">{arrow} {abs(val):.1f}%</div>'
                f'<div class="lvm-sub">price change</div></div>')
    lvm_html = (_perf_card("1 Week", MARKET.get("perf_1w")) + _perf_card("30 Day", MARKET.get("perf_30d")) +
                _perf_card("90 Day", MARKET.get("perf_90d")) + _perf_card("6 Month", MARKET.get("perf_6m")))

    # Regional News Activity Heatmap
    rh_html = regional_heatmap_html()

    # Sentiment Engine
    _isc_score, _isc_label = interest_score()
    vel_html = velocity_chart_html()
    sdt_html = sentiment_trend_html()
    sent_lb_rows = sentiment_leaderboard_html()

    # Competitive Briefing
    comp_rows = competitor_table_html()
    odl_html = odl_corridors_html()
    iso_html = iso20022_html()

    # Ripple Executive Tracker + XRPL Dev Activity
    ex_html = exec_tracker_html()
    ex_ts = EXEC_TRACKER.get("updated") or "\u2014"
    gh_commits_html = github_commits_html()
    gh_ts = GITHUB_DEV.get("updated") or "\u2014"
    gh_stars = f'{GITHUB_DEV.get("stars", 0):,}'
    gh_issues = f'{GITHUB_DEV.get("issues", 0):,}'
    gh_rippled_7d = GITHUB_DEV.get("rippled_7d", 0)
    gh_other_7d = GITHUB_DEV.get("other_7d", 0)
    _commits = GITHUB_DEV.get("commits", [])
    if _commits:
        gh_last_msg = html.escape(_commits[0]["msg"] or "(no message)")
        gh_last_meta = f'{html.escape(_commits[0]["author"])} \u00B7 {html.escape(_commits[0]["date"])}'
    else:
        gh_last_msg = "Awaiting first sync\u2026"
        gh_last_meta = "\u2014"

    # Regulatory Radar
    cg_html = country_grid_html()
    etf_html = etf_tracker_html()
    sec_tl_html = sec_timeline_html()
    mica_html = mica_calendar_html()
    cbdc_html = cbdc_grid_html()

    # Global XRP Enterprise & Partnership Ledger
    pl_html = partnership_ledger_html()
    pl_total = len(PARTNERSHIP_LEDGER)
    pl_detected = sum(1 for e in PARTNERSHIP_LEDGER if e["source"] == "detected")

    # Static Global Partnership Directory (right rail, refreshes every 3 days)
    sd_entries = STATIC_PARTNER_DIRECTORY.get("entries", [])
    sd_updated = STATIC_PARTNER_DIRECTORY.get("last_refreshed") or "\u2014"
    sd_count = len(sd_entries)
    sd_html = "".join(
        f'<div class="sd-item">'
        f'<div class="sd-item-top"><span class="sd-flag">{flag}</span>'
        f'<span class="sd-name">{html.escape(name)}</span></div>'
        f'<span class="sd-cat">{cat_emoji} {html.escape(cat_lbl)}</span>'
        f'<span class="sd-desc">{html.escape(desc)}</span></div>'
        for name, desc, cat_lbl, cat_emoji, flag in sd_entries
    ) or '<div class="sd-empty">Directory loading\u2026</div>'

    pl_by_cat = {}
    for e in PARTNERSHIP_LEDGER:
        pl_by_cat[e["cat"]] = pl_by_cat.get(e["cat"], 0) + 1

    # Advanced Metrics
    ts_html = tech_specs_html()
    uc_html = use_case_html()
    ad_s7, ad_c7, ad_s30, ad_c30 = ad_line_html()
    corr_html = correlation_html()
    ob_bid_html, ob_ask_html, ob_bid_total, ob_ask_total = orderbook_html()
    ob_has_data = bool(MARKET.get("ob_bids") and MARKET.get("ob_asks"))
    if ob_has_data:
        ob_body_html = (
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px">'
            f'<div><div style="font-size:15px;font-weight:700;color:var(--gr);font-family:var(--mn);margin-bottom:6px;text-align:center">\U0001F7E2 BUY WALLS (BIDS)</div>'
            f'{ob_bid_html}'
            f'<div style="margin-top:8px;padding:6px;background:rgba(72,255,130,.1);border:1px solid rgba(72,255,130,.2);border-radius:4px;text-align:center">'
            f'<span style="font-size:12px;color:var(--tx)">Total Bid Depth: </span>'
            f'<span style="font-size:15px;font-weight:700;color:var(--gr);font-family:var(--mn)">{ob_bid_total}</span></div></div>'
            f'<div><div style="font-size:15px;font-weight:700;color:var(--rd);font-family:var(--mn);margin-bottom:6px;text-align:center">\U0001F534 SELL WALLS (ASKS)</div>'
            f'{ob_ask_html}'
            f'<div style="margin-top:8px;padding:6px;background:rgba(255,64,96,.1);border:1px solid rgba(255,64,96,.2);border-radius:4px;text-align:center">'
            f'<span style="font-size:12px;color:var(--tx)">Total Ask Depth: </span>'
            f'<span style="font-size:15px;font-weight:700;color:var(--rd);font-family:var(--mn)">{ob_ask_total}</span></div></div>'
            f'</div>'
        )
    else:
        ob_body_html = ob_bid_html  # home-base placeholder
    liq_html = liquidity_map_html()

    # CLARITY Act Tracker
    ca_html = clarity_tracker_html()
    ca_count = len(CLARITY_ACT_STORIES)

    # XRPRadar Exclusive Intelligence — Institutional Confidence Index
    _ici = institutional_confidence_index()
    ici_score = _ici["score"]
    ici_label = _ici["label"]
    ici_color = _ici["color"]
    ici_comps_rendered = ici_comps_html(_ici["comps"])
    pm_bars, pm_total, pm_this_week, pm_trend, pm_tcol, pm_avg = partnership_momentum_html()
    cc_cells, cc_peak, cc_hourlbls = catalyst_clock_html()
    cc_total = _CATALYST_TOTAL
    nd_cards, nd_fastest = narrative_diffusion_html()
    flagship_ts = MARKET.get("updated") or NEWS.get("updated") or "\u2014"

    # Regulatory & Ledger Watch (V66)
    rw_amendments = ""
    for a in REG_WATCH["amendments"]:
        eta = f' \u00B7 ETA {html.escape(str(a["eta"])[:10])}' if a.get("eta") else ""
        rw_amendments += (f'<div class="rw-item"><span class="rw-name">{html.escape(a["name"])}</span>'
                          f'<span class="rw-meta">{a["count"]} validator votes{eta}</span></div>')
    if not rw_amendments:
        rw_amendments = '<div class="rw-empty">No pending amendments detected \u2014 all active amendments enabled, or data refreshing\u2026</div>'

    rw_edgar = ""
    for e in REG_WATCH["edgar"]:
        d = f'<span class="rw-meta">{html.escape(e["date"])}</span>' if e.get("date") else ""
        rw_edgar += (f'<div class="rw-item"><a href="{html.escape(e["link"])}" target="_blank" rel="noopener" '
                     f'class="rw-link">{html.escape(e["title"])}</a>{d}</div>')
    if not rw_edgar:
        rw_edgar = '<div class="rw-empty">No recent Ripple/XRP filings detected \u2014 data refreshing\u2026</div>'

    rw_fedreg = ""
    for f in REG_WATCH["fedreg"]:
        agency = f' \u00B7 {html.escape(f["agency"])}' if f.get("agency") else ""
        rw_fedreg += (f'<div class="rw-item"><a href="{html.escape(f["link"])}" target="_blank" rel="noopener" '
                      f'class="rw-link">{html.escape(f["title"])}</a>'
                      f'<span class="rw-meta">{html.escape(f["date"])}{agency}</span></div>')
    if not rw_fedreg:
        rw_fedreg = '<div class="rw-empty">No recent federal rulemaking detected \u2014 data refreshing\u2026</div>'

    rw_updated = REG_WATCH.get("updated") or "\u2014"

    # Practical Tools — multi-currency conversion (XRP price x FX rate)
    _fx = MARKET.get("fx") or {}
    _xp = MARKET.get("xrp_price") or 0
    def _fx_val(code, dec=4):
        rate = _fx.get(code)
        if rate is None or not _xp:
            return "\u2014"
        return f"{_xp * rate:,.{dec}f}"
    fx_eur = _fx_val("EUR"); fx_gbp = _fx_val("GBP"); fx_jpy = _fx_val("JPY", 2)
    fx_aud = _fx_val("AUD"); fx_cad = _fx_val("CAD"); fx_sgd = _fx_val("SGD")
    fx_inr = _fx_val("INR", 2); fx_brl = _fx_val("BRL")
    fx_chf = _fx_val("CHF"); fx_cny = _fx_val("CNY", 2); fx_krw = _fx_val("KRW", 0)
    fx_mxn = _fx_val("MXN", 2); fx_php = _fx_val("PHP", 2); fx_ngn = _fx_val("NGN", 2)
    fx_zar = _fx_val("ZAR", 2); fx_aed = _fx_val("AED", 2); fx_sar = _fx_val("SAR", 2)
    fx_hkd = _fx_val("HKD", 2); fx_nzd = _fx_val("NZD"); fx_sek = _fx_val("SEK", 2)
    fx_nok = _fx_val("NOK", 2); fx_try = _fx_val("TRY", 2); fx_thb = _fx_val("THB", 2)
    fx_idr = _fx_val("IDR", 0); fx_vnd = _fx_val("VND", 0); fx_pln = _fx_val("PLN", 2)
    fx_usd_disp = f"{_xp:.4f}" if _xp else "\u2014"
    fx_ts = MARKET.get("updated") or "\u2014"
    xrp_price_js = _xp or 0

    modal_rows = ""
    for label, ok, detail in checks:
        c = "#48ff82" if ok else "#ff4060"
        t = "PASS" if ok else "FAIL"
        modal_rows += (
            '<div class="pf-row">'
            f'<span class="pf-row-label">{label}</span>'
            f'<span class="pf-row-badge" style="color:{c}">{t}</span>'
            f'<span class="pf-row-detail">{detail}</span>'
            '</div>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{APP_NAME} \u2014 {TAGLINE}</title>
<style>
  :root{{
    --bg:#000; --s1:#0a0a0a; --s2:#111; --b:#1a2030;
    --gr:#48ff82; --grd:rgba(72,255,130,.1);
    --rd:#ff4060; --rdd:rgba(255,64,96,.1);
    --yl:#ffcc00; --yld:rgba(255,204,0,.1);
    --bl:#75bcff; --bld:rgba(117,188,255,.12);
    --tq:#00e5cc; --tqd:rgba(0,229,204,.15);
    --or:#ff9900; --tx:#8099b3; --br:#cce0ff; --hdr:#03b1fc;
    --mn:'Courier New',monospace;
  }}
  *{{ box-sizing:border-box; }}
  body{{ background:var(--bg); color:var(--br); font-family:system-ui,sans-serif; font-size:15px; min-height:100vh; -webkit-font-smoothing:antialiased; margin:0; }}
  .w{{ max-width:1400px; margin:0 auto; padding:10px 24px; }}
  @media(max-width:1440px){{ .w{{ max-width:1280px; padding:10px 18px; }} }}

  /* BREAKING NEWS BAR */
  #breaking{{ background:var(--s1); padding:8px 0; overflow:hidden; }}
  .bkinner{{ max-width:2400px; margin:0 auto; padding:0 28px; }}
  .bkrow{{ display:flex; align-items:center; width:100%; padding-bottom:8px; border-bottom:2px solid var(--hdr); }}
  .bklbl{{ color:var(--hdr); font-weight:900; font-size:17px; font-family:var(--mn); flex-shrink:0; padding-right:14px; margin-right:14px; border-right:2px solid rgba(3,177,252,.5); text-transform:uppercase; letter-spacing:.08em; display:inline-flex; align-items:center; gap:9px; }}
  .bk-bolt{{ font-size:22px; }}
  .bkscroll{{ flex:1; overflow:hidden; height:26px; position:relative; display:flex; align-items:center; }}
  .bktext{{ display:inline-block; animation:bkscroll 45s linear infinite; white-space:nowrap; will-change:transform; padding-left:100%; font-size:15px; color:var(--br); font-family:system-ui; font-weight:500; line-height:26px; }}
  .bkscroll:hover .bktext{{ animation-play-state:paused; }}
  @keyframes bkscroll{{ 0%{{transform:translateX(0)}} 100%{{transform:translateX(-100%)}} }}

  /* HEADER */
  .hdr{{ display:flex; align-items:center; justify-content:space-between; margin-bottom:10px; padding-top:36px; padding-bottom:40px; border-bottom:2px solid var(--hdr); flex-wrap:wrap; gap:6px; }}
  .logo{{ display:flex; align-items:center; gap:12px; }}
  .icon{{ width:110px; height:110px; border-radius:14px; background:linear-gradient(135deg,#001a3a,#0066cc,#75bcff); display:flex; align-items:center; justify-content:center; font-size:68px; box-shadow:0 0 16px rgba(117,188,255,.4); }}
  .title{{ font-size:22px; font-weight:900; color:var(--br); font-style:italic; }}
  .sub{{ font-size:15px; font-family:var(--mn); color:var(--tx); margin-top:2px; letter-spacing:1px; }}
  .hright{{ display:flex; align-items:center; gap:10px; flex-wrap:wrap; }}
  .dot{{ width:12px; height:12px; border-radius:50%; background:var(--gr); box-shadow:0 0 10px var(--gr); display:inline-block; animation:blink 2s infinite; }}
  @keyframes blink{{ 50%{{opacity:.1}} }}
  .run-lbl{{ font-size:15px; font-weight:800; font-family:var(--mn); color:var(--gr); letter-spacing:1px; }}
  .pill{{ padding:5px 14px; border-radius:20px; font-size:15px; font-family:var(--mn); font-weight:700; letter-spacing:1.5px; text-transform:uppercase; }}
  .plive{{ background:var(--grd); color:var(--gr); border:1px solid rgba(72,255,130,.4); }}
  .upd{{ font-family:var(--mn); font-size:15px; color:var(--tx); }}

  /* STATUS ROW — compact horizontal rectangles */
  .srow{{ display:grid; grid-template-columns:repeat(3,1fr); gap:8px; margin:10px 0; }}
  .si{{ background:var(--s1); border:1px solid var(--b); border-radius:8px; padding:14px 18px; display:flex; align-items:center; justify-content:space-between; gap:12px; min-height:64px; }}
  .si-lbl{{ color:#ffffff; font-size:17px; font-family:var(--mn); font-weight:700; letter-spacing:.5px; display:flex; align-items:center; gap:9px; white-space:nowrap; }}
  .si-lbl .ic{{ font-size:22px; }}
  .sv{{ font-weight:800; font-size:22px; font-family:var(--mn); line-height:1; text-align:right; }}
  .sv-sub{{ font-size:15px; font-family:var(--mn); margin-top:2px; }}

  /* FEAR & GREED horizontal line + ball */
  .fng-wrap{{ position:relative; width:180px; height:34px; display:flex; align-items:center; flex-shrink:0; }}
  .fng-bar{{ width:100%; height:10px; border-radius:6px;
    background:linear-gradient(90deg,#ea3943,#ea8c00,#f3d42f,#93d900,#16c784); }}
  .fng-ball{{ position:absolute; top:50%; transform:translate(-50%,-50%);
    width:32px; height:32px; border-radius:50%; border:2px solid #fff;
    display:flex; align-items:center; justify-content:center;
    font-family:var(--mn); font-weight:800; font-size:15px; color:#fff;
    text-shadow:0 1px 2px rgba(0,0,0,.7); box-shadow:0 0 6px rgba(0,0,0,.5); }}

  /* SECTION 3 — technical panels (RSI, S&R, Time Machine, 52-Week) */
  .grid2{{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin:10px 0; align-items:stretch; }}
  .col{{ display:flex; flex-direction:column; gap:10px; }}
  .acct{{ background:var(--s1); border:1px solid rgba(117,188,255,.25); border-radius:10px; padding:14px; }}
  .acct.grow{{ flex:1; }}   /* lets 52-week + time machine match column height */
  .sec-title{{ font-size:17px; text-transform:uppercase; letter-spacing:2px; font-family:var(--mn); color:#ffffff; margin-bottom:12px; font-weight:800; display:flex; align-items:center; gap:10px; }}
  .sec-title .sic{{ font-size:22px; }}   /* header icon = same size as status-row icons */
  .rsi-head{{ display:flex; justify-content:space-between; margin-bottom:6px; font-size:15px; font-family:var(--mn); }}
  .rsi-track{{ height:11px; background:var(--s2); border-radius:6px; overflow:hidden; border:1px solid var(--b); position:relative; }}
  .rsi-tick{{ position:absolute; top:0; bottom:0; width:1px; background:rgba(255,255,255,.12); }}
  .rsi-fill{{ height:100%; border-radius:6px; transition:all .6s; }}
  .rsi-scale{{ display:flex; justify-content:space-between; font-size:15px; font-family:var(--mn); color:var(--tx); margin-top:3px; }}
  .w52-row{{ display:flex; justify-content:space-between; font-family:var(--mn); font-size:15px; }}
  .w52-bar{{ height:15px; background:linear-gradient(90deg,var(--rd),var(--yl),var(--gr)); border-radius:7px; position:relative; border:1px solid var(--b); margin:10px 0; }}
  .w52-needle{{ position:absolute; top:-4px; width:6px; height:23px; background:var(--br); border-radius:3px; border:2px solid var(--bg); transform:translateX(-50%); transition:left .6s; }}
  .agrid2{{ display:grid; grid-template-columns:repeat(2,1fr); gap:8px; }}
  .abox{{ background:var(--s2); border:1px solid var(--b); border-radius:8px; padding:14px; text-align:center; }}
  .albl{{ font-size:15px; text-transform:uppercase; letter-spacing:1.5px; font-family:var(--mn); color:var(--tx); margin-bottom:6px; }}
  .aval{{ font-size:22px; font-weight:900; font-family:var(--mn); color:var(--br); line-height:1; }}
  .asub{{ font-size:15px; font-family:var(--mn); color:var(--tx); margin-top:5px; }}
  .sr-line{{ display:flex; justify-content:space-between; font-family:var(--mn); font-size:15px; padding:8px 0; border-bottom:1px solid var(--b); }}
  .sr-line:last-child{{ border-bottom:none; }}
  .empty{{ padding:16px; font-family:var(--mn); font-size:15px; color:var(--tx); text-align:center; }}

  /* Reusable "home base" — reserved space for upcoming/still-filling sections */
  .home-base{{ padding:26px 20px; text-align:center; border:1px dashed rgba(128,153,179,.3); border-radius:8px;
    background:rgba(128,153,179,.03); }}
  .home-base-icon{{ font-size:32px; line-height:1; margin-bottom:10px; opacity:.85; }}
  .home-base-title{{ font-size:15px; font-weight:800; font-family:var(--mn); letter-spacing:.5px; color:var(--br); margin-bottom:5px; }}
  .home-base-sub{{ font-size:12px; font-family:var(--mn); color:var(--tx); max-width:420px; margin:0 auto; line-height:1.6; }}
  .tvs{{ margin-top:12px; padding:10px 12px; background:var(--s2); border-radius:6px; border:1px solid var(--b); }}
  .tvs-lbl{{ font-size:15px; font-family:var(--mn); color:var(--tx); margin-bottom:4px; text-transform:uppercase; letter-spacing:1px; }}
  .tvs-txt{{ font-size:15px; color:var(--br); line-height:1.6; }}

  /* SECTION 5 — On-Chain Intelligence + Whale Alert Feed */
  .oc-grid{{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin:10px 0; align-items:stretch; }}
  .ocbox-grid{{ display:grid; grid-template-columns:repeat(3,1fr); gap:8px; }}
  .ocbox{{ background:var(--s2); border:1px solid var(--b); border-radius:8px; padding:14px; text-align:center; }}
  .ocbox.tq{{ border-color:rgba(0,229,204,.3); background:var(--tqd); }}
  .ocbox.esc{{ border-color:rgba(72,255,130,.3); background:var(--grd); grid-column:span 2; }}
  .oclbl{{ font-size:15px; text-transform:uppercase; letter-spacing:1.5px; font-family:var(--mn); color:var(--tx); margin-bottom:6px; }}
  .ocval{{ font-size:17px; font-weight:900; font-family:var(--mn); color:var(--br); line-height:1; }}
  .ocsub{{ font-size:15px; font-family:var(--mn); color:var(--tx); margin-top:5px; }}
  .esc-row{{ display:flex; align-items:baseline; gap:10px; justify-content:center; margin:6px 0; }}
  .esc-num{{ font-size:22px; font-weight:900; font-family:var(--mn); color:var(--gr); line-height:1; }}
  .esc-sep{{ color:var(--tx); font-size:17px; font-family:var(--mn); }}
  .panel{{ background:var(--s1); border:1px solid var(--b); border-radius:10px; overflow:hidden; }}
  .ph{{ padding:10px 14px; border-bottom:1px solid var(--b); display:flex; justify-content:space-between; align-items:center; background:var(--s2); }}
  .pt{{ font-size:17px; text-transform:uppercase; letter-spacing:2px; font-family:var(--mn); font-weight:800; display:flex; align-items:center; gap:10px; }}
  .pt .sic{{ font-size:22px; }}
  .whale-feed{{ padding:8px 14px; max-height:240px; overflow-y:auto; }}
  .wa-row{{ display:flex; align-items:center; gap:10px; padding:8px 0; border-bottom:1px solid rgba(26,32,48,.4); font-family:var(--mn); font-size:15px; }}
  .wa-row:last-child{{ border-bottom:none; }}
  .wa-src{{ color:var(--yl); font-weight:700; white-space:nowrap; }}
  .wa-hl{{ color:var(--br); text-decoration:none; flex:1; }}
  .wa-hl:hover{{ color:var(--hdr); text-decoration:underline; }}
  .wa-time{{ color:var(--tx); white-space:nowrap; font-size:12px; }}
  .whale-item{{ padding:10px 0; border-bottom:1px solid var(--b); }}
  .whale-item:last-child{{ border-bottom:none; }}
  .whale-hl{{ font-size:15px; font-weight:700; color:var(--yl); font-family:system-ui; line-height:1.4; margin-bottom:4px; }}
  .whale-meta{{ font-size:15px; font-family:var(--mn); color:var(--tx); }}

  /* SECTION 6 — XRP Ecosystem */
  .eco-wrap{{ background:linear-gradient(135deg,#06060f,#0a0a18); border:1px solid rgba(72,255,130,.35); border-radius:12px; overflow:hidden; margin:10px 0; }}
  .eco-head{{ padding:16px 18px; background:rgba(117,188,255,.06); border-bottom:1px solid rgba(117,188,255,.2); display:flex; align-items:center; gap:14px; }}
  .eco-head .gicon{{ font-size:22px; filter:drop-shadow(0 0 10px rgba(117,188,255,.6)); }}
  .eco-title{{ font-size:17px; font-weight:900; color:var(--hdr); font-family:var(--mn); text-transform:uppercase; letter-spacing:2px; }}
  .eco-sub{{ font-size:15px; font-family:system-ui; color:var(--bl); margin-top:3px; }}
  .eco-grid{{ display:grid; grid-template-columns:repeat(4,1fr); gap:8px; padding:14px 18px; }}
  .eco-card{{ border-radius:8px; padding:14px; position:relative; overflow:hidden; }}
  .eco-bar{{ position:absolute; top:0; left:0; right:0; height:2px; }}
  .eco-ic{{ font-size:22px; margin-bottom:6px; }}
  .eco-name{{ font-size:15px; font-weight:900; color:#fff; font-family:var(--mn); margin-bottom:4px; }}
  .eco-role{{ font-size:15px; font-weight:700; font-family:var(--mn); margin-bottom:8px; text-transform:uppercase; letter-spacing:1px; }}
  .eco-desc{{ font-size:15px; color:var(--tx); line-height:1.6; margin-bottom:10px; font-family:system-ui; }}
  .eco-stat{{ display:flex; justify-content:space-between; font-size:15px; font-family:var(--mn); padding:2px 0; }}
  .eco-stat .k{{ color:var(--tx); }}

  /* SECTION 6b — How the Layers Connect + Misconceptions (inside eco-wrap) */
  .eco-sub-h{{ font-size:15px; font-weight:700; color:var(--hdr); font-family:var(--mn); text-transform:uppercase; letter-spacing:1.5px; margin:6px 0 10px; padding:0 18px; display:flex; align-items:center; gap:8px; }}
  .flow{{ display:flex; align-items:center; justify-content:center; gap:0; overflow-x:auto; padding:6px 18px 18px; }}
  .flow-node{{ display:flex; flex-direction:column; align-items:center; min-width:120px; text-align:center; padding:8px; }}
  .flow-ic{{ font-size:22px; margin-bottom:8px; }}
  .flow-name{{ font-size:15px; font-weight:700; font-family:var(--mn); }}
  .flow-role{{ font-size:15px; color:var(--tx); font-family:var(--mn); margin-top:2px; }}
  .flow-arrow{{ color:var(--bl); font-size:22px; padding:0 8px; flex-shrink:0; font-weight:300; }}
  .myth-grid{{ display:grid; grid-template-columns:repeat(3,1fr); gap:8px; padding:0 18px 18px; }}
  .myth-card{{ background:var(--s2); border:1px solid var(--b); border-radius:8px; padding:14px; }}
  .myth-lbl{{ font-size:15px; font-weight:700; color:var(--rd); font-family:var(--mn); margin-bottom:5px; }}
  .myth-q{{ font-size:15px; color:var(--br); font-weight:700; margin-bottom:8px; }}
  .real-lbl{{ font-size:15px; font-weight:700; color:var(--gr); font-family:var(--mn); margin-bottom:5px; }}
  .real-txt{{ font-size:15px; color:var(--tx); line-height:1.55; font-family:system-ui; }}

  /* SECTION 7 — Mainstream Integration + Institutional Partnership trackers */
  .trk-tag{{ font-size:15px; font-style:italic; color:var(--yl); font-family:system-ui; margin:2px 0 12px; line-height:1.5; }}
  .trk-legend{{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:6px; }}
  .trk-btn{{ padding:6px 12px; border-radius:4px; font-size:15px; font-weight:700; font-family:var(--mn); letter-spacing:.5px; border:1px solid; cursor:pointer; background:transparent; opacity:.6; transition:opacity .15s; }}
  .trk-btn:hover{{ opacity:.9; }}
  .trk-btn.active{{ opacity:1; box-shadow:0 0 0 1px currentColor inset; }}
  .trk-grid{{ display:grid; grid-template-columns:repeat(4,1fr); gap:8px; }}
  .trk-card{{ background:var(--s2); border:1px solid var(--b); border-radius:8px; padding:12px 14px; display:flex; flex-direction:column; }}
  .trk-top{{ display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:6px; }}
  .trk-status{{ font-size:15px; font-weight:800; font-family:var(--mn); letter-spacing:1px; display:flex; align-items:center; gap:6px; }}
  .trk-type{{ font-size:15px; color:var(--tx); font-family:var(--mn); white-space:nowrap; }}
  .trk-name{{ font-size:17px; font-weight:800; color:var(--br); font-family:var(--mn); margin-bottom:6px; }}
  .trk-detail{{ font-size:15px; color:var(--tx); line-height:1.5; font-family:system-ui; margin-bottom:8px;
    display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; overflow:hidden; }}
  .trk-src{{ font-size:12px; font-style:italic; color:var(--tx); font-family:var(--mn); margin-top:auto; }}
  .trk-empty{{ padding:22px; text-align:center; color:var(--tx); font-family:var(--mn); font-size:15px; border:1px dashed var(--b); border-radius:8px; margin-top:8px; }}

  /* Integration Timeline (horizontal) */
  .tl-wrap{{ position:relative; padding:6px 0 4px; }}
  .tl-line{{ position:absolute; top:43px; left:0; right:0; height:2px; background:linear-gradient(90deg,transparent,var(--yl),var(--gr),transparent); }}
  .tl-track{{ display:flex; gap:0; overflow-x:auto; padding-bottom:10px; position:relative;
    scrollbar-width:thin; scrollbar-color:#33405e var(--s2); }}
  .tl-track::-webkit-scrollbar{{ height:6px; }}
  .tl-track::-webkit-scrollbar-track{{ background:var(--s2); border-radius:6px; }}
  .tl-track::-webkit-scrollbar-thumb{{ background:#33405e; border-radius:6px; }}
  .tl-node{{ flex:0 0 200px; min-width:200px; text-align:center; padding:0 10px; position:relative; }}
  .tl-top{{ height:44px; display:flex; flex-direction:column; align-items:center; justify-content:flex-end; gap:8px; margin-bottom:12px; }}
  .tl-year{{ font-size:17px; font-weight:900; font-family:var(--mn); line-height:1; }}
  .tl-dot{{ border-radius:50%; box-shadow:0 0 8px currentColor; border:2px solid var(--bg); flex-shrink:0; }}
  .tl-event{{ font-size:15px; font-weight:800; color:var(--br); font-family:var(--mn); margin-bottom:5px; }}
  .tl-detail{{ font-size:15px; color:var(--tx); line-height:1.5; font-family:system-ui; }}

  /* Top 20 XRP Stories */
  .story-list{{ display:flex; flex-direction:column; gap:2px; margin-bottom:14px; }}
  .story{{ display:flex; gap:12px; align-items:flex-start; padding:9px 8px; border-bottom:1px solid var(--b); text-decoration:none; border-radius:6px; }}
  .story:hover{{ background:var(--s2); }}
  .story:last-child{{ border-bottom:none; }}
  .story-num{{ flex:0 0 26px; text-align:center; font-family:var(--mn); font-weight:900; color:var(--hdr); font-size:15px; padding-top:1px; }}
  .story-body{{ display:flex; flex-direction:column; gap:3px; }}
  .story-hl{{ font-size:15px; font-weight:600; color:var(--br); font-family:system-ui; line-height:1.4; }}
  .story:hover .story-hl{{ color:#fff; }}
  .story-meta{{ font-size:15px; font-family:var(--mn); color:var(--tx); text-transform:capitalize; }}

  /* US Intelligence + Global Pulse (2-column) + Regional Discourse */
  .intel-grid{{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin:10px 0; align-items:stretch; }}
  .intel{{ background:var(--s1); border:1px solid var(--b); border-radius:10px; overflow:hidden; display:flex; flex-direction:column; }}
  .intel-h{{ padding:10px 14px; border-bottom:1px solid var(--b); background:var(--s2); display:flex; justify-content:space-between; align-items:center; }}
  .intel-t{{ font-size:17px; font-weight:800; font-family:var(--mn); letter-spacing:1.5px; text-transform:uppercase; display:flex; align-items:center; gap:8px; }}
  .intel-t .sic{{ font-size:22px; }}
  .intel-b{{ padding:12px 14px; display:flex; flex-direction:column; gap:10px; }}
  .intel-pulse{{ font-size:15px; color:var(--br); line-height:1.55; font-family:system-ui; }}
  .intel-row{{ font-size:15px; color:var(--tx); line-height:1.5; font-family:system-ui; }}
  .intel-row b{{ color:var(--label,#8099b3); font-family:var(--mn); text-transform:uppercase; letter-spacing:1px; font-size:12px; font-weight:800; }}
  .sig-row{{ display:flex; flex-wrap:wrap; gap:10px; margin-top:6px; }}
  .sig-chip{{ font-size:12px; font-family:var(--mn); font-weight:700; cursor:default; display:inline-flex; align-items:center; gap:5px; }}
  .sig-chip .sig-dot{{ width:7px; height:7px; border-radius:50%; display:inline-block; }}
  .rd-grid{{ display:grid; grid-template-columns:repeat(4,1fr); gap:8px; }}
  .rd-card{{ background:var(--s2); border:1px solid var(--b); border-radius:8px; padding:12px 14px; }}
  .rd-top{{ display:flex; justify-content:space-between; align-items:center; gap:8px; margin-bottom:6px; }}
  .rd-name{{ font-size:15px; font-weight:800; color:var(--br); font-family:var(--mn); }}
  .rd-sig{{ font-size:12px; font-weight:700; font-family:var(--mn); padding:1px 8px; border-radius:4px; border:1px solid; text-transform:uppercase; letter-spacing:1px; }}
  .rd-count{{ font-size:15px; color:var(--tx); font-family:var(--mn); margin-bottom:5px; }}
  .rd-hl{{ font-size:15px; color:var(--tx); line-height:1.5; font-family:system-ui;
    display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }}

  /* Signal Scoreboard */
  .sb-grid{{ display:grid; grid-template-columns:repeat(6,1fr); gap:8px; }}
  .sb-grid4{{ display:grid; grid-template-columns:repeat(4,1fr); gap:8px; margin-top:8px; }}
  .sb-box{{ background:var(--s2); border:1px solid var(--b); border-radius:8px; padding:12px 10px; text-align:center; }}
  .sb-num{{ font-size:22px; font-weight:900; font-family:var(--mn); line-height:1.1; color:var(--br); }}
  .sb-lbl{{ font-size:12px; text-transform:uppercase; letter-spacing:1px; color:var(--tx); font-family:var(--mn); margin-top:7px; }}
  .sb-sub{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-top:3px; }}
  .sb-bar{{ height:8px; background:var(--s2); border:1px solid var(--b); border-radius:4px; overflow:hidden; margin-top:10px; }}
  .sb-fill{{ height:100%; background:linear-gradient(90deg,var(--rd),var(--yl),var(--gr)); transition:width .4s; }}
  @media(max-width:900px){{ .sb-grid{{ grid-template-columns:repeat(3,1fr); }} .sb-grid4{{ grid-template-columns:repeat(2,1fr); }} }}

  /* Global News Feed + right rail */
  .feed-wrap{{ display:grid; grid-template-columns:2fr 1fr; gap:10px; margin:10px 0; align-items:start; }}
  .ledger-wrap{{ display:grid; grid-template-columns:2fr 1fr; gap:10px; margin:10px 0; align-items:stretch; }}
  @media(max-width:900px){{ .ledger-wrap{{ grid-template-columns:1fr; }} }}
  .gn-search{{ width:100%; box-sizing:border-box; background:#e9ecf1; border:1px solid #c3c8d1; border-radius:8px;
    color:#1a2a4a; font-family:var(--mn); font-size:15px; padding:12px 14px; margin-bottom:10px; }}
  .gn-search::placeholder{{ color:#6b7280; }}
  .gn-cats{{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:10px; }}
  .gn-btn{{ padding:6px 14px; border-radius:6px; font-size:15px; font-weight:700; font-family:var(--mn); letter-spacing:1px;
    border:1px solid var(--b); background:transparent; color:var(--tx); cursor:pointer; opacity:.75; }}
  .gn-btn:hover{{ opacity:1; }}
  .gn-btn.active{{ opacity:1; box-shadow:0 0 0 1px currentColor inset; }}
  .gn-stats{{ font-size:15px; font-family:var(--mn); color:var(--tx); margin-bottom:10px; }}
  .gn-stats b{{ color:var(--gr); }}
  .gn-list{{ display:flex; flex-direction:column; gap:8px; max-height:920px; overflow-y:scroll; padding-right:6px;
    scrollbar-width:thin; scrollbar-color:#33405e var(--s2); }}
  .gn-list::-webkit-scrollbar{{ width:8px; }}
  .gn-list::-webkit-scrollbar-track{{ background:var(--s2); border-radius:6px; }}
  .gn-list::-webkit-scrollbar-thumb{{ background:#33405e; border-radius:6px; }}
  .gn-list::-webkit-scrollbar-thumb:hover{{ background:var(--hdr); }}
  .gn-card{{ background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:14px; }}
  .gn-top{{ display:flex; align-items:center; gap:8px; margin-bottom:8px; flex-wrap:wrap; }}
  .gn-src{{ font-size:12px; font-weight:700; font-family:var(--mn); color:var(--tq); border:1px solid rgba(0,229,204,.4);
    border-radius:4px; padding:1px 8px; }}
  .gn-cat{{ font-size:12px; font-family:var(--mn); color:var(--tx); }}
  .gn-break{{ font-size:12px; font-weight:800; font-family:var(--mn); color:var(--yl); letter-spacing:1px; }}
  .gn-time{{ font-size:12px; font-family:var(--mn); color:var(--tx); margin-left:auto; }}
  .gn-hl{{ display:block; font-size:17px; font-weight:700; color:var(--hdr); font-family:system-ui; line-height:1.4;
    text-decoration:none; margin-bottom:4px; }}
  .gn-hl:hover{{ text-decoration:underline; }}
  .gn-tr{{ display:inline-block; font-size:15px; font-family:var(--mn); color:var(--tx); text-decoration:none; margin-bottom:6px; }}
  .gn-tr:hover{{ color:var(--hdr); text-decoration:underline; }}
  .gn-sum{{ font-size:15px; color:var(--tx); line-height:1.6; font-family:system-ui; margin-bottom:8px; }}
  .gn-foot{{ display:flex; align-items:center; gap:8px; font-size:15px; font-family:var(--mn); font-weight:700; }}
  .gn-dot{{ width:12px; height:12px; border-radius:50%; display:inline-block; }}
  .gn-empty{{ padding:22px; text-align:center; color:var(--tx); font-family:var(--mn); font-size:15px; }}
  .rail{{ display:flex; flex-direction:column; gap:10px; }}
  .rail-panel{{ background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:16px 18px; }}
  .rail-h{{ font-size:15px; font-weight:800; font-family:var(--mn); letter-spacing:1.5px; text-transform:uppercase;
    color:var(--hdr); display:flex; align-items:center; gap:10px; margin-bottom:6px; }}
  .rail-h .sic{{ font-size:22px; }}
  .rail-row{{ display:flex; justify-content:space-between; align-items:center; gap:10px; min-height:34px;
    font-family:var(--mn); font-size:15px; border-bottom:1px solid rgba(26,32,48,.35); }}
  .rail-row:last-child{{ border-bottom:none; }}
  .rail-k{{ color:var(--tx); }}
  .rail-v{{ font-weight:700; color:var(--br); text-align:right; white-space:nowrap; }}
  @media(max-width:900px){{ .feed-wrap{{ grid-template-columns:1fr; }} }}

  /* Analytics Lab */
  .lab3{{ display:grid; grid-template-columns:repeat(3,1fr); gap:10px; margin-bottom:10px; }}
  .labp{{ background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:14px 16px; }}
  .labt{{ font-size:15px; font-weight:800; font-family:var(--mn); color:var(--hdr); margin-bottom:8px; display:flex; align-items:center; gap:8px; }}
  .bstat{{ display:flex; justify-content:space-between; align-items:center; min-height:33px; font-family:var(--mn); font-size:15px; border-bottom:1px solid rgba(26,32,48,.35); }}
  .bstat:last-child{{ border-bottom:none; }}
  .bk{{ color:var(--tx); }}
  .bv{{ font-weight:700; color:var(--br); text-align:right; }}
  @media(max-width:900px){{ .lab3{{ grid-template-columns:1fr; }} }}

  /* XRPRadar Leaderboard */
  .lb-grid{{ display:grid; grid-template-columns:repeat(3,1fr); gap:10px; }}
  .lb-panel{{ background:var(--s2); border:1px solid var(--b); border-radius:8px; padding:14px 16px; }}
  .lb-t{{ font-size:15px; font-weight:800; font-family:var(--mn); letter-spacing:1.5px; margin-bottom:10px; text-transform:uppercase; }}
  .lb-row{{ display:flex; align-items:center; gap:12px; padding:7px 0; font-family:var(--mn); font-size:15px; border-bottom:1px solid rgba(26,32,48,.35); }}
  .lb-row:last-child{{ border-bottom:none; }}
  .lb-rank{{ color:var(--hdr); font-weight:900; width:18px; text-align:center; }}
  .lb-name{{ color:var(--br); flex:1; }}
  .lb-cnt{{ color:var(--tx); font-weight:700; }}
  .lb-empty{{ color:var(--tx); font-family:var(--mn); font-size:15px; padding:6px 0; }}
  .lb-score{{ text-align:center; padding:6px 0 10px; }}
  .lb-score-num{{ font-size:46px; font-weight:900; font-family:var(--mn); line-height:1; }}
  .lb-score-cap{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-top:4px; }}
  .lb-score-lbl{{ font-size:15px; font-weight:800; font-family:var(--mn); margin-top:6px; letter-spacing:1px; }}
  .lb-mini{{ border-top:1px solid var(--b); padding-top:8px; margin-top:4px; }}
  .lb-mini-row{{ display:flex; justify-content:space-between; font-size:15px; font-family:var(--mn); padding:3px 0; }}
  .lb-mini-row span:first-child{{ color:var(--tx); }}
  @media(max-width:900px){{ .lb-grid{{ grid-template-columns:1fr; }} }}

  /* XRP Intelligence Brief */
  .brf-head{{ display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:10px; margin-bottom:14px; }}
  .brf-sub{{ font-size:15px; color:var(--or); font-family:var(--mn); margin-top:3px; }}
  .brf-meta{{ text-align:right; font-family:var(--mn); }}
  .brf-badge{{ display:inline-block; font-size:15px; font-weight:800; letter-spacing:1px; padding:3px 12px; border-radius:5px;
    background:rgba(255,153,0,.12); color:var(--or); border:1px solid rgba(255,153,0,.45); }}
  .brf-when{{ font-size:15px; color:var(--br); font-family:var(--mn); margin-top:6px; font-weight:600; }}
  .brf-now-showing{{ font-size:15px; color:var(--hdr); font-family:var(--mn); font-weight:800; letter-spacing:0.5px;
    margin-bottom:8px; padding-bottom:8px; border-bottom:1px solid var(--b); display:flex; align-items:center;
    flex-wrap:wrap; gap:10px; }}
  .brf-now-spacer{{ color:var(--tx); font-weight:400; }}
  #brf-next-line{{ font-size:15px; color:var(--tx); font-weight:600; text-transform:none; letter-spacing:normal; }}
  .brf-ribbon-wrap{{ display:inline-flex; align-items:center; gap:6px; margin-right:4px; }}
  .brf-ribbon-icon{{ font-size:17px; }}
  .brf-ribbon{{ background:var(--or); color:#ffffff; font-family:var(--mn); font-weight:900; font-size:15px;
    letter-spacing:0.5px; padding:5px 16px 5px 12px; position:relative;
    clip-path:polygon(0 0, calc(100% - 8px) 0, 100% 50%, calc(100% - 8px) 100%, 0 100%); }}
  .brf-intro-line{{ font-size:15px; color:var(--tx); font-family:var(--mn); margin-bottom:10px; font-style:italic; }}
  .brf-grid{{ display:grid; grid-template-columns:1fr 1fr; gap:12px; }}
  .brf-block{{ background:rgba(117,188,255,.07); border:1px solid rgba(117,188,255,.25); border-radius:8px; padding:16px 18px; border-left:3px solid var(--or); min-height:140px; }}
  .brf-t{{ font-size:15px; font-weight:800; font-family:var(--mn); letter-spacing:1px; color:var(--hdr); text-transform:uppercase; margin-bottom:6px; display:flex; align-items:center; gap:8px; }}
  .brf-x{{ font-size:15px; color:var(--br); line-height:1.75; font-family:system-ui; }}
  .brf-note{{ font-size:12px; color:var(--tx); font-family:var(--mn); opacity:.7; margin-top:12px; }}
  @media(max-width:900px){{ .brf-grid{{ grid-template-columns:1fr; }} }}

  /* Brief Home — designated schedule strip */
  .brf-home{{ background:var(--s2); border:1px solid rgba(255,153,0,.3); border-radius:8px; padding:12px 14px; margin-bottom:14px; }}
  .brf-home-t{{ font-size:15px; font-weight:800; font-family:var(--mn); letter-spacing:1px; color:var(--or); text-transform:uppercase; margin-bottom:3px; display:flex; align-items:center; gap:8px; }}
  .brf-home-sub{{ font-size:15px; color:var(--br); font-family:var(--mn); margin-bottom:10px; font-weight:600; }}
  .brf-strip{{ display:flex; flex-wrap:wrap; gap:6px; }}
  .brf-slot{{ flex:1 1 60px; min-width:58px; text-align:center; padding:7px 4px; border-radius:6px; font-family:var(--mn);
    border:1px solid var(--b); background:var(--s1); cursor:default; display:block; text-decoration:none; }}
  .brf-slot-day{{ font-size:12px; color:var(--tx); }}
  .brf-slot-ed{{ font-size:15px; font-weight:800; margin-top:2px; }}
  .brf-slot.ready{{ cursor:pointer; border:2px solid var(--tq); background:rgba(0,229,204,.16); box-shadow:0 0 6px rgba(0,229,204,.25); }}
  .brf-slot.ready:hover{{ border-color:var(--tq); background:rgba(0,229,204,.28); box-shadow:0 0 10px rgba(0,229,204,.4); transform:translateY(-1px); }}
  .brf-slot.ready .brf-slot-ed{{ color:var(--tq); font-weight:900; }}
  .brf-slot.ready .brf-slot-day{{ color:var(--br); font-weight:700; }}
  .brf-slot.live{{ border-color:var(--or); background:rgba(255,153,0,.14); box-shadow:0 0 0 1px var(--or) inset; }}
  .brf-slot.live .brf-slot-ed{{ color:var(--or); }}
  .brf-slot.pending{{ opacity:.45; cursor:pointer; }}
  .brf-pending-msg{{ margin-top:8px; padding:8px 12px; background:rgba(255,153,0,.1); border:1px solid rgba(255,153,0,.35);
    border-radius:6px; font-size:12px; font-family:var(--mn); color:var(--or); }}
  .brf-slot.pending .brf-slot-ed{{ color:var(--tx); }}
  .brf-slot.active-view{{ outline:2px solid var(--br); outline-offset:1px; }}

  /* Next Briefing countdown teaser — same footprint as Brief Home, white fill */
  .brf-teaser{{ background:#3d7fc4; border:2px solid #2a5f96; border-radius:8px; padding:10px 14px; margin-bottom:14px; text-align:center; }}
  .brf-teaser-line{{ font-size:15px; font-weight:900; font-family:var(--mn); color:#ffffff; }}
  .brf-teaser-line span{{ color:var(--or); font-weight:900; }}
  .brf-teaser-sub{{ font-size:15px; font-family:var(--mn); color:#dcebfa; margin-top:4px; font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}

  /* World briefing clocks */
  .wc-row{{ display:flex; flex-wrap:wrap; gap:8px; justify-content:space-between; margin:14px 0; padding:12px;
    background:var(--s2); border:1px solid var(--b); border-radius:10px; }}
  .wc{{ flex:1 1 92px; min-width:84px; text-align:center; font-family:var(--mn); }}
  .wc-city{{ font-size:12px; font-weight:700; color:var(--br); margin-bottom:6px; white-space:nowrap; }}
  .wc-clock{{ position:relative; width:54px; height:54px; border-radius:50%; margin:0 auto 6px; border:2px solid #4a5878;
    background:radial-gradient(circle,rgba(128,153,179,.16),rgba(128,153,179,.04)); }}
  .wc-clock.wc-day{{ border-color:var(--or); background:radial-gradient(circle,rgba(255,153,0,.28),rgba(255,153,0,.07)); }}
  .wc-hand{{ position:absolute; left:50%; bottom:50%; transform-origin:bottom center; transform:rotate(0deg); background:var(--br); border-radius:2px; }}
  .wc-hr{{ width:3px; height:14px; margin-left:-1.5px; }}
  .wc-min{{ width:2px; height:20px; margin-left:-1px; }}
  .wc-sec{{ width:1px; height:21px; margin-left:-.5px; background:var(--rd); }}
  .wc-clock.wc-day .wc-hr, .wc-clock.wc-day .wc-min{{ background:#3a2200; }}
  .wc-center{{ position:absolute; left:50%; top:50%; width:5px; height:5px; border-radius:50%; background:var(--rd); transform:translate(-50%,-50%); }}
  .wc-off{{ font-size:12px; font-weight:700; color:var(--hdr); margin-bottom:2px; }}
  .wc-b{{ font-size:12px; color:var(--tx); line-height:1.5; white-space:nowrap; }}

  /* Unique Displays: Smart Money Score + F&G history */
  .ud-grid{{ display:grid; grid-template-columns:1fr 2fr; gap:12px; }}
  .ud-panel{{ background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:16px; }}
  .sm-score{{ font-size:52px; font-weight:900; font-family:var(--mn); line-height:1; }}
  .sm-cap{{ font-size:15px; color:var(--tx); font-family:var(--mn); }}
  .sm-label{{ font-size:17px; font-weight:800; font-family:var(--mn); margin:8px 0; }}
  .sm-bar{{ height:8px; background:var(--s2); border:1px solid var(--b); border-radius:4px; overflow:hidden; margin-bottom:14px; }}
  .sm-fill{{ height:100%; background:linear-gradient(90deg,var(--rd),var(--yl),var(--gr)); }}
  .sm-row{{ display:flex; justify-content:space-between; align-items:center; min-height:31px; font-family:var(--mn); font-size:15px; border-bottom:1px solid rgba(26,32,48,.35); }}
  .sm-row:last-child{{ border-bottom:none; }}
  .sm-k{{ color:var(--tx); }}
  .sm-v{{ color:var(--br); font-weight:700; }}
  .fg-title{{ font-size:15px; font-weight:800; font-family:var(--mn); letter-spacing:1px; color:var(--hdr); margin-bottom:12px; display:flex; align-items:center; gap:8px; }}
  .fg-chart{{ display:flex; align-items:flex-end; gap:3px; height:130px; padding:6px 0; }}
  .fg-bar{{ flex:1; min-width:4px; border-radius:2px 2px 0 0; }}
  .fg-bar.fg-today{{ outline:2px solid var(--br); outline-offset:1px; }}
  .fg-axis{{ display:flex; justify-content:space-between; font-size:12px; color:var(--tx); font-family:var(--mn); margin-top:4px; }}
  .fg-legend{{ display:flex; flex-wrap:wrap; gap:12px; margin-top:10px; font-size:12px; font-family:var(--mn); color:var(--tx); }}
  .fg-key{{ display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:5px; vertical-align:middle; }}
  @media(max-width:900px){{ .ud-grid{{ grid-template-columns:1fr; }} }}

  /* Longitudinal Value Markers */
  .lvm-grid{{ display:grid; grid-template-columns:repeat(4,1fr); gap:10px; }}
  .lvm-card{{ background:var(--s2); border:1px solid var(--b); border-radius:10px; padding:16px; text-align:center; }}
  .lvm-win{{ font-size:15px; color:var(--tx); font-family:var(--mn); text-transform:uppercase; letter-spacing:1px; margin-bottom:8px; }}
  .lvm-val{{ font-size:22px; font-weight:900; font-family:var(--mn); line-height:1; }}
  .lvm-sub{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-top:6px; }}
  @media(max-width:900px){{ .lvm-grid{{ grid-template-columns:repeat(2,1fr); }} }}

  /* Regional News Activity Heatmap */
  .rh-grid{{ display:grid; grid-template-columns:repeat(4,1fr); gap:10px; }}
  .rh-card{{ border:1px solid var(--b); border-radius:10px; padding:16px 12px; text-align:center; }}
  .rh-flag{{ font-size:22px; line-height:1; }}
  .rh-name{{ font-size:15px; font-weight:800; color:var(--br); font-family:var(--mn); margin:6px 0; }}
  .rh-num{{ font-size:22px; font-weight:900; font-family:var(--mn); line-height:1; }}
  .rh-lbl{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-top:5px; }}
  @media(max-width:900px){{ .rh-grid{{ grid-template-columns:repeat(2,1fr); }} }}

  /* Sentiment Engine */
  .sent-top{{ display:grid; grid-template-columns:200px 1fr; gap:10px; margin-bottom:14px; }}
  .vel-chart{{ display:flex; align-items:flex-end; gap:2px; height:60px; margin-top:8px; }}
  .vel-bar{{ flex:1; min-width:2px; background:var(--yl); border-radius:1px 1px 0 0; opacity:.85; }}
  .sdt-chart{{ display:flex; align-items:flex-end; gap:2px; height:80px; }}
  .sdt-bar{{ flex:1; min-width:3px; border-radius:2px 2px 0 0; }}
  .sent-bar-mini{{ display:flex; height:8px; border-radius:4px; overflow:hidden; width:80px; background:var(--s2); }}
  @media(max-width:900px){{ .sent-top{{ grid-template-columns:1fr; }} }}

  /* Competitive Briefing */
  .odl-item, .iso-item{{ background:var(--s2); border:1px solid var(--b); border-radius:6px; padding:9px 12px;
    margin-bottom:6px; font-family:var(--mn); font-size:15px; display:flex; align-items:center; gap:10px; flex-wrap:wrap; }}
  .odl-route{{ font-weight:700; color:var(--br); white-space:nowrap; }}
  .odl-status{{ font-size:12px; font-weight:800; padding:2px 8px; border-radius:4px; letter-spacing:.5px; white-space:nowrap; }}
  .odl-status.active{{ background:rgba(72,255,130,.15); color:var(--gr); }}
  .odl-status.growing{{ background:rgba(255,204,0,.15); color:var(--yl); }}
  .odl-status.live{{ background:rgba(0,229,204,.15); color:var(--tq); }}
  .odl-note{{ color:var(--tx); font-size:12px; flex:1; min-width:140px; }}
  .sw-grid{{ display:grid; grid-template-columns:repeat(5,1fr); gap:8px; }}
  @media(max-width:900px){{ .sw-grid{{ grid-template-columns:repeat(2,1fr); }} }}

  /* Ripple Executive Tracker + XRPL Dev Activity */
  .ed-grid{{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
  .ed-panel{{ background:var(--s1); border:1px solid var(--b); border-radius:10px; overflow:hidden; }}
  .ed-head{{ padding:10px 14px; background:var(--s2); border-bottom:1px solid var(--b); display:flex; justify-content:space-between; align-items:center; }}
  .ed-title{{ font-size:15px; font-weight:800; font-family:var(--mn); letter-spacing:1px; }}
  .ex-tabs{{ display:flex; gap:0; border-bottom:1px solid var(--b); overflow-x:auto; }}
  .ex-tab{{ padding:7px 14px; background:transparent; border:none; color:var(--tx); font-family:var(--mn);
    font-size:12px; font-weight:700; cursor:pointer; text-transform:uppercase; letter-spacing:1px; white-space:nowrap;
    border-bottom:2px solid transparent; }}
  .ex-tab.on{{ color:var(--or); border-bottom-color:var(--or); }}
  .ex-feed{{ max-height:340px; overflow-y:auto; padding:8px 12px; }}
  .ex-row{{ padding:9px 0; border-bottom:1px solid rgba(26,32,48,.4); }}
  .ex-row:last-child{{ border-bottom:none; }}
  .ex-top{{ display:flex; align-items:center; gap:8px; margin-bottom:4px; flex-wrap:wrap; }}
  .ex-name{{ font-size:15px; font-weight:800; color:var(--or); font-family:var(--mn); }}
  .ex-title{{ font-size:12px; color:var(--tx); font-family:var(--mn); }}
  .ex-time{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-left:auto; }}
  .ex-hl{{ display:block; font-size:15px; color:var(--br); text-decoration:none; line-height:1.5; font-family:system-ui; }}
  .ex-hl:hover{{ color:var(--hdr); text-decoration:underline; }}
  .gh-stats{{ display:grid; grid-template-columns:repeat(4,1fr); border-bottom:1px solid var(--b); background:var(--s2); }}
  .gh-stat{{ padding:9px 6px; text-align:center; border-right:1px solid var(--b); }}
  .gh-stat:last-child{{ border-right:none; }}
  .gh-stat-num{{ font-size:17px; font-weight:900; font-family:var(--mn); }}
  .gh-stat-lbl{{ font-size:12px; color:var(--tx); font-family:var(--mn); text-transform:uppercase; letter-spacing:.5px; line-height:1.4; margin-top:2px; }}
  .gh-latest{{ padding:9px 12px; border-bottom:1px solid var(--b); background:rgba(72,255,130,.04); }}
  .gh-latest-lbl{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-bottom:2px; }}
  .gh-latest-msg{{ font-size:15px; font-weight:700; color:var(--gr); font-family:system-ui; }}
  .gh-latest-meta{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-top:2px; }}
  .gh-feed{{ max-height:220px; overflow-y:auto; padding:8px 12px; }}
  .gh-row{{ padding:8px 0; border-bottom:1px solid rgba(26,32,48,.4); font-family:var(--mn); font-size:12px; }}
  .gh-row:last-child{{ border-bottom:none; }}
  .gh-repo{{ display:inline-block; color:var(--tq); font-weight:700; margin-right:6px; }}
  .gh-msg{{ color:var(--br); text-decoration:none; }}
  .gh-msg:hover{{ color:var(--hdr); text-decoration:underline; }}
  .gh-meta{{ display:block; color:var(--tx); margin-top:2px; }}
  @media(max-width:900px){{ .ed-grid{{ grid-template-columns:1fr; }} }}

  /* Regulatory Radar */
  .cg-grid{{ display:grid; grid-template-columns:repeat(5,1fr); gap:8px; }}
  .cg-card{{ background:var(--s2); border:1px solid var(--b); border-radius:8px; padding:10px; }}
  .cg-top{{ display:flex; align-items:center; gap:6px; margin-bottom:6px; }}
  .cg-flag{{ font-size:17px; }}
  .cg-name{{ font-size:12px; font-weight:700; color:var(--br); font-family:var(--mn); }}
  .cg-note{{ font-size:12px; color:var(--tx); line-height:1.5; font-family:system-ui; margin-top:6px; }}
  .mica-row{{ display:flex; align-items:center; gap:12px; padding:9px 4px; border-bottom:1px solid rgba(26,32,48,.4); font-family:var(--mn); }}
  .mica-row:last-child{{ border-bottom:none; }}
  .mica-icon{{ font-size:15px; flex:0 0 18px; text-align:center; }}
  .mica-date{{ font-size:12px; color:var(--tx); flex:0 0 78px; }}
  .mica-event{{ font-size:15px; font-weight:700; flex:0 0 190px; }}
  .mica-detail{{ font-size:12px; color:var(--tx); flex:1; font-family:system-ui; line-height:1.5; }}
  @media(max-width:700px){{ .mica-row{{ flex-wrap:wrap; }} .mica-event{{ flex-basis:100%; order:1; }} .mica-detail{{ flex-basis:100%; order:2; }} }}
  @media(max-width:900px){{ .cg-grid{{ grid-template-columns:repeat(2,1fr); }} }}

  /* Static Global Partnership Directory (right rail, V90) */
  .sd-panel{{ background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:14px; display:flex; flex-direction:column; }}
  .sd-head{{ display:flex; justify-content:space-between; align-items:center; margin-bottom:4px; }}
  .sd-title{{ font-size:15px; font-weight:800; font-family:var(--mn); color:var(--hdr); letter-spacing:0.5px; }}
  .sd-count{{ font-size:15px; font-weight:900; font-family:var(--mn); color:var(--yl); }}
  .sd-sub{{ font-size:12px; color:var(--tx); font-family:var(--mn); line-height:1.5; margin-bottom:10px; }}
  .sd-list{{ display:flex; flex-direction:column; gap:6px; flex:1 1 auto; min-height:0; max-height:820px; overflow-y:scroll; padding-right:6px;
    scrollbar-width:thin; scrollbar-color:#33405e var(--s2); }}
  .sd-list::-webkit-scrollbar{{ width:8px; }}
  .sd-list::-webkit-scrollbar-track{{ background:var(--s2); border-radius:6px; }}
  .sd-list::-webkit-scrollbar-thumb{{ background:#33405e; border-radius:6px; }}
  .sd-item{{ background:var(--s2); border:1px solid var(--b); border-radius:6px; padding:8px 10px; display:flex; flex-direction:column; gap:3px; }}
  .sd-item-top{{ display:flex; align-items:center; gap:7px; }}
  .sd-flag{{ font-size:15px; line-height:1; flex-shrink:0; }}
  .sd-name{{ font-size:15px; font-weight:700; color:var(--br); }}
  .sd-cat{{ font-size:12px; font-weight:700; font-family:var(--mn); color:var(--tq); letter-spacing:0.3px; }}
  .sd-desc{{ font-size:12px; color:var(--tx); line-height:1.4; }}
  .sd-empty{{ font-size:12px; color:var(--tx); font-style:italic; padding:10px 0; }}

  /* Global XRP Enterprise & Partnership Ledger */
  .pl-search{{ width:100%; box-sizing:border-box; background:#e9ecf1; border:1px solid #c3c8d1; border-radius:8px;
    color:#1a2a4a; font-family:var(--mn); font-size:15px; padding:11px 14px; margin-bottom:10px; }}
  .pl-search::placeholder{{ color:#6b7280; }}
  .pl-cats{{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:10px; }}
  .pl-btn{{ padding:6px 13px; border-radius:6px; font-size:12px; font-weight:700; font-family:var(--mn); letter-spacing:.5px;
    border:1px solid var(--b); background:transparent; color:var(--tx); cursor:pointer; opacity:.75; }}
  .pl-btn:hover{{ opacity:1; }}
  .pl-btn.active{{ opacity:1; box-shadow:0 0 0 1px currentColor inset; }}
  .pl-stats{{ font-size:15px; font-family:var(--mn); color:var(--tx); margin-bottom:10px; }}
  .pl-stats b{{ color:var(--yl); }}
  .pl-list{{ display:flex; flex-direction:column; gap:7px; max-height:600px; overflow-y:scroll; padding-right:6px;
    scrollbar-width:thin; scrollbar-color:#33405e var(--s2); }}
  .pl-list::-webkit-scrollbar{{ width:8px; }}
  .pl-list::-webkit-scrollbar-track{{ background:var(--s2); border-radius:6px; }}
  .pl-list::-webkit-scrollbar-thumb{{ background:#33405e; border-radius:6px; }}
  .pl-row{{ background:var(--s1); border:1px solid var(--b); border-radius:8px; padding:10px 14px; }}
  .pl-top{{ display:flex; align-items:center; gap:8px; margin-bottom:4px; flex-wrap:wrap; }}
  .pl-cat{{ font-size:12px; font-weight:800; font-family:var(--mn); letter-spacing:.5px; }}
  .pl-new{{ font-size:12px; font-weight:900; font-family:var(--mn); color:var(--bg); background:var(--yl);
    padding:1px 6px; border-radius:4px; letter-spacing:.5px; }}
  .pl-status{{ font-size:12px; font-weight:700; font-family:var(--mn); }}
  .pl-when{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-left:auto; }}
  .pl-name{{ font-size:15px; font-weight:700; color:var(--br); font-family:system-ui; margin-bottom:2px; }}
  .pl-name a{{ color:var(--hdr); text-decoration:none; }}
  .pl-name a:hover{{ text-decoration:underline; }}
  .pl-meta{{ font-size:12px; color:var(--tx); line-height:1.5; font-family:system-ui; }}
  .pl-counter{{ font-size:22px; font-weight:900; font-family:var(--mn); color:var(--yl); }}

  /* Advanced Metrics */
  .am-grid2{{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
  .am-panel{{ background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:16px; }}
  .am-title{{ font-size:15px; font-weight:800; font-family:var(--mn); margin-bottom:4px; display:flex; align-items:center; gap:8px; }}
  .am-sub{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-bottom:12px; }}
  .uc-list{{ display:flex; flex-direction:column; gap:6px; max-height:340px; overflow-y:auto; }}
  .uc-card{{ padding:9px 11px; background:var(--s2); border-radius:6px; border-left:3px solid; }}
  .uc-title{{ font-size:15px; font-weight:700; font-family:var(--mn); margin-bottom:2px; }}
  .uc-detail{{ font-size:12px; color:var(--tx); line-height:1.5; font-family:system-ui; }}
  .abox{{ padding:10px; background:var(--s2); border-radius:6px; border-left:3px solid var(--b); }}
  .abox-lbl{{ font-size:12px; color:var(--tx); font-family:var(--mn); text-transform:uppercase; letter-spacing:.5px; }}
  .abox-val{{ font-size:17px; font-weight:800; font-family:var(--mn); margin-top:4px; }}
  .corr-row{{ display:flex; justify-content:space-between; align-items:center; padding:9px 12px; background:var(--s2);
    border-radius:6px; border:1px solid var(--b); font-family:var(--mn); font-size:15px; font-weight:700; margin-bottom:8px; color:var(--br); }}
  .ob-row{{ display:grid; grid-template-columns:70px 1fr 60px; align-items:center; gap:8px; font-family:var(--mn); font-size:12px; padding:2px 0; }}
  .ob-price.gr{{ color:var(--gr); }}
  .ob-price.rd{{ color:var(--rd); }}
  .ob-bar-wrap{{ height:8px; background:var(--s2); border-radius:2px; overflow:hidden; }}
  .ob-bar{{ height:100%; }}
  .ob-bar.gr{{ background:rgba(72,255,130,.5); }}
  .ob-bar.rd{{ background:rgba(255,64,96,.5); }}
  .ob-qty{{ color:var(--tx); text-align:right; }}
  .liq-bar{{ height:22px; border-radius:6px; overflow:hidden; background:rgba(255,64,96,.35); margin-bottom:6px; }}
  .liq-fill{{ height:100%; background:rgba(72,255,130,.55); }}
  .liq-labels{{ display:flex; justify-content:space-between; font-size:15px; font-family:var(--mn); font-weight:700; margin-bottom:6px; }}
  .liq-skew{{ font-size:15px; font-weight:800; color:var(--br); font-family:var(--mn); margin-bottom:4px; }}
  .liq-note{{ font-size:12px; color:var(--tx); font-family:var(--mn); }}
  @media(max-width:900px){{ .am-grid2{{ grid-template-columns:1fr; }} }}

  /* CLARITY Act Tracker */
  .ca-list{{ display:flex; flex-direction:column; gap:7px; max-height:520px; overflow-y:auto; }}

  /* Visible thin scrollbars, applied consistently to every scrollable container on the site */
  .whale-feed, .ex-feed, .gh-feed, .uc-list, .ca-list {{
    scrollbar-width:thin; scrollbar-color:#33405e var(--s2);
  }}
  .whale-feed::-webkit-scrollbar, .ex-feed::-webkit-scrollbar, .gh-feed::-webkit-scrollbar,
  .uc-list::-webkit-scrollbar, .ca-list::-webkit-scrollbar {{ width:8px; }}
  .whale-feed::-webkit-scrollbar-track, .ex-feed::-webkit-scrollbar-track, .gh-feed::-webkit-scrollbar-track,
  .uc-list::-webkit-scrollbar-track, .ca-list::-webkit-scrollbar-track {{ background:var(--s2); border-radius:6px; }}
  .whale-feed::-webkit-scrollbar-thumb, .ex-feed::-webkit-scrollbar-thumb, .gh-feed::-webkit-scrollbar-thumb,
  .uc-list::-webkit-scrollbar-thumb, .ca-list::-webkit-scrollbar-thumb {{ background:#33405e; border-radius:6px; }}

  .flow, .ex-tabs, .cc-panel, .tbl-scroll {{
    scrollbar-width:thin; scrollbar-color:#33405e var(--s2); overflow-x:auto;
  }}
  .flow::-webkit-scrollbar, .ex-tabs::-webkit-scrollbar, .cc-panel::-webkit-scrollbar, .tbl-scroll::-webkit-scrollbar {{ height:8px; }}
  .flow::-webkit-scrollbar-track, .ex-tabs::-webkit-scrollbar-track, .cc-panel::-webkit-scrollbar-track, .tbl-scroll::-webkit-scrollbar-track {{ background:var(--s2); border-radius:6px; }}
  .flow::-webkit-scrollbar-thumb, .ex-tabs::-webkit-scrollbar-thumb, .cc-panel::-webkit-scrollbar-thumb, .tbl-scroll::-webkit-scrollbar-thumb {{ background:#33405e; border-radius:6px; }}
  .ca-row{{ display:flex; align-items:flex-start; gap:12px; background:var(--s1); border:1px solid var(--b);
    border-radius:8px; padding:10px 14px; }}
  .ca-rank{{ flex:0 0 26px; text-align:center; font-size:15px; font-weight:900; font-family:var(--mn); color:var(--yl); padding-top:2px; }}
  .ca-body{{ flex:1; min-width:0; }}
  .ca-top{{ display:flex; align-items:center; gap:8px; margin-bottom:3px; flex-wrap:wrap; }}
  .ca-src{{ font-size:12px; font-weight:700; color:var(--tq); font-family:var(--mn); }}
  .ca-time{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-left:auto; }}
  .ca-hl{{ font-size:15px; font-weight:700; color:var(--br); text-decoration:none; line-height:1.4; font-family:system-ui; }}
  .ca-hl:hover{{ color:var(--hdr); text-decoration:underline; }}

  /* Flagship: Institutional Confidence Index */
  .flagship-intro{{ font-size:15px; color:var(--tx); line-height:1.7; font-family:system-ui; margin-bottom:16px; max-width:920px; }}
  .flagship-list{{ margin:10px 0; padding-left:20px; }}
  .flagship-list li{{ margin-bottom:4px; }}
  .ici-wrap{{ display:grid; grid-template-columns:220px 1fr; gap:20px; background:linear-gradient(135deg,#0a0a14,#0d0d1a);
    border:1px solid rgba(255,204,0,.25); border-radius:14px; padding:22px; }}
  .ici-dial{{ display:flex; flex-direction:column; align-items:center; justify-content:center; text-align:center; }}
  .ici-score{{ font-size:64px; font-weight:900; font-family:var(--mn); line-height:1; }}
  .ici-cap{{ font-size:15px; color:var(--tx); font-family:var(--mn); margin-top:2px; }}
  .ici-label{{ font-size:15px; font-weight:800; font-family:var(--mn); margin-top:8px; letter-spacing:.5px; }}
  .ici-bar{{ width:100%; height:8px; background:var(--s2); border-radius:4px; overflow:hidden; margin-top:12px; }}
  .ici-fill{{ height:100%; background:linear-gradient(90deg,var(--rd),var(--or),var(--yl),var(--gr)); }}
  .ici-comps{{ display:flex; flex-direction:column; gap:8px; justify-content:center; }}
  .ici-comp-row{{ display:grid; grid-template-columns:170px 1fr 44px; align-items:center; gap:10px; }}
  .ici-comp-name{{ font-size:12px; font-weight:700; color:var(--br); font-family:var(--mn); }}
  .ici-comp-track{{ height:7px; background:var(--s2); border-radius:4px; overflow:hidden; position:relative; }}
  .ici-comp-fill{{ height:100%; background:var(--tq); border-radius:4px; }}
  .ici-comp-detail{{ font-size:12px; color:var(--tx); font-family:var(--mn); grid-column:1/3; margin-top:-4px; }}
  .ici-comp-pts{{ font-size:12px; font-weight:800; color:var(--yl); font-family:var(--mn); text-align:right; }}
  .ici-foot{{ margin-top:16px; padding-top:14px; border-top:1px solid rgba(255,255,255,.08); font-size:12px;
    color:var(--tx); font-family:var(--mn); line-height:1.6; }}
  @media(max-width:900px){{ .ici-wrap{{ grid-template-columns:1fr; }} }}

  /* Partnership Momentum Chart */
  .pm-panel{{ margin-top:16px; background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:16px; }}
  .pm-title{{ font-size:15px; font-weight:800; font-family:var(--mn); color:var(--yl); margin-bottom:4px; display:flex; align-items:center; gap:8px; }}
  .pm-sub{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-bottom:12px; }}
  .pm-stats{{ display:flex; gap:18px; flex-wrap:wrap; margin-bottom:12px; }}
  .pm-stat-num{{ font-size:22px; font-weight:900; font-family:var(--mn); }}
  .pm-stat-lbl{{ font-size:12px; color:var(--tx); font-family:var(--mn); text-transform:uppercase; letter-spacing:.5px; }}
  .pm-chart{{ display:flex; align-items:flex-end; gap:5px; height:70px; }}
  .pm-bar{{ flex:1; min-width:8px; background:var(--yl); border-radius:2px 2px 0 0; opacity:.85; }}
  .pm-axis{{ display:flex; justify-content:space-between; font-size:12px; color:var(--tx); font-family:var(--mn); margin-top:4px; }}

  /* Catalyst Clock */
  .cc-panel{{ margin-top:16px; background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:16px; overflow-x:auto; }}
  .cc-title{{ font-size:15px; font-weight:800; font-family:var(--mn); color:var(--or); margin-bottom:4px; display:flex; align-items:center; gap:8px; }}
  .cc-sub{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-bottom:12px; }}
  .cc-peak{{ font-size:15px; font-family:var(--mn); color:var(--br); margin-bottom:12px; }}
  .cc-peak b{{ color:var(--or); }}
  .cc-grid{{ min-width:640px; }}
  .cc-row{{ display:flex; align-items:center; gap:2px; margin-bottom:2px; }}
  .cc-daylbl{{ flex:0 0 30px; font-size:12px; color:var(--tx); font-family:var(--mn); }}
  .cc-cell{{ flex:1; height:14px; border-radius:2px; min-width:8px; }}
  .cc-hourlbls{{ display:flex; gap:2px; margin-top:2px; margin-left:32px; min-width:608px; }}
  .cc-hourlbl{{ flex:1; font-size:12px; color:var(--tx); font-family:var(--mn); text-align:center; min-width:8px; }}
  .cc-scrollnote{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-top:8px; }}

  /* Narrative Diffusion Map */
  .nd-panel{{ margin-top:16px; background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:16px; }}
  .nd-title{{ font-size:15px; font-weight:800; font-family:var(--mn); color:var(--tq); margin-bottom:4px; display:flex; align-items:center; gap:8px; }}
  .nd-sub{{ font-size:12px; color:var(--tx); font-family:var(--mn); margin-bottom:8px; }}
  .nd-fastest{{ font-size:15px; font-family:var(--mn); color:var(--br); margin-bottom:12px; }}
  .nd-fastest b{{ color:var(--tq); }}
  .nd-list{{ display:flex; flex-direction:column; gap:8px; }}
  .nd-card{{ background:var(--s2); border:1px solid var(--b); border-radius:8px; padding:10px 14px; }}
  .nd-top{{ display:flex; justify-content:space-between; align-items:center; margin-bottom:6px; flex-wrap:wrap; gap:6px; }}
  .nd-theme{{ font-size:15px; font-weight:800; color:var(--br); font-family:var(--mn); }}
  .nd-age{{ font-size:12px; color:var(--tx); font-family:var(--mn); }}
  .nd-chips{{ display:flex; flex-wrap:wrap; gap:6px; margin-bottom:6px; }}
  .nd-chip{{ font-size:12px; font-family:var(--mn); background:var(--s1); border:1px solid var(--b); border-radius:12px;
    padding:3px 10px; color:var(--br); }}
  .nd-lag{{ color:var(--tq); font-weight:700; margin-left:3px; }}
  .nd-note{{ font-size:12px; color:var(--tx); font-family:var(--mn); }}

  /* Practical Tools */
  .pt-cols{{ display:grid; grid-template-columns:1fr 1fr; gap:10px; align-items:start; }}
  .pt-col{{ display:flex; flex-direction:column; gap:10px; }}
  .pt-panel{{ background:var(--s1); border:1px solid var(--b); border-radius:10px; overflow:hidden; }}
  .pt-head{{ padding:10px 14px; background:var(--s2); border-bottom:1px solid var(--b); display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:6px; }}
  .pt-title{{ font-size:15px; font-weight:800; font-family:var(--mn); letter-spacing:1.2px; }}
  .pt-body{{ padding:14px; display:flex; flex-direction:column; gap:10px; }}
  .pt-lbl{{ font-size:12px; font-family:var(--mn); color:var(--tx); text-transform:uppercase; letter-spacing:1px; margin-bottom:4px; }}
  .pt-row2{{ display:grid; grid-template-columns:1fr 1fr; gap:8px; }}
  .pt-input, .pt-select{{ width:100%; box-sizing:border-box; background:var(--s2); border:1px solid var(--b); color:var(--br);
    padding:8px 10px; border-radius:5px; font-size:15px; font-family:var(--mn); outline:none; }}
  .pt-input::placeholder{{ color:var(--tx); }}
  .pt-use-live{{ color:var(--tq); cursor:pointer; margin-left:6px; font-size:12px; }}
  .pt-results{{ background:var(--s2); border:1px solid var(--b); border-radius:6px; padding:10px; font-family:var(--mn); font-size:15px; display:none; }}
  .pt-res-row{{ display:flex; justify-content:space-between; padding:4px 0; border-bottom:1px solid rgba(255,255,255,.05); }}
  .pt-res-row:last-child{{ border-bottom:none; }}
  .pt-note{{ font-size:12px; font-family:var(--mn); color:var(--tx); }}
  .pt-btn{{ background:rgba(117,188,255,.1); border:1px solid var(--bl); color:var(--bl); padding:8px 14px; border-radius:5px;
    cursor:pointer; font-family:var(--mn); font-size:15px; font-weight:700; text-transform:uppercase; white-space:nowrap; }}
  .pt-btn:hover{{ background:var(--bl); color:#000; }}
  .pt-btn-gr{{ background:rgba(72,255,130,.1); border:1px solid var(--gr); color:var(--gr); padding:6px 10px; border-radius:4px;
    cursor:pointer; font-family:var(--mn); font-size:15px; font-weight:700; }}
  .pt-btn-gr:hover{{ background:var(--gr); color:#000; }}
  .fx-grid{{ display:grid; grid-template-columns:repeat(3,1fr); gap:6px; padding:12px; }}
  .fx-box{{ background:var(--s2); border:1px solid var(--b); border-radius:6px; padding:8px; text-align:center; }}
  .fx-box.hi{{ border-color:var(--bl); }}
  .fx-lbl{{ font-size:12px; font-family:var(--mn); color:var(--tx); text-transform:uppercase; letter-spacing:1px; }}
  .fx-val{{ font-size:17px; font-weight:900; font-family:var(--mn); margin-top:4px; color:var(--br); }}
  .pt-tbl{{ width:100%; border-collapse:collapse; font-family:var(--mn); font-size:15px; margin-bottom:6px; }}
  .pt-tbl th{{ padding:4px 6px; text-align:right; color:var(--tx); font-size:12px; border-bottom:1px solid var(--b); }}
  .pt-tbl th:first-child{{ text-align:left; }}
  .pt-tbl td{{ padding:5px 6px; text-align:right; border-bottom:1px solid rgba(255,255,255,.03); }}
  .pt-tbl td:first-child{{ text-align:left; color:var(--br); font-weight:700; }}
  .pt-x{{ cursor:pointer; color:var(--rd); font-weight:900; }}
  .rm-fee-box{{ border-radius:6px; padding:10px; text-align:center; }}
  @media(max-width:900px){{ .pt-cols{{ grid-template-columns:1fr; }} .fx-grid{{ grid-template-columns:repeat(3,1fr); }} }}

  /* MAIN */
  main{{ max-width:1180px; margin:0 auto; padding:14px 28px 90px; min-height:46vh; }}
 
  .subtitle{{ color:var(--tx); font-size:15px; font-family:var(--mn); letter-spacing:1px; margin-bottom:22px; }}
  .note{{ border:1px solid var(--b); border-radius:8px; background:var(--s1); padding:16px 20px; color:var(--tx); font-size:15px; }}

  /* Regulatory & Ledger Watch (V66) */
  .rw-wrap {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:14px; }}
  .rw-panel {{ background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:16px; }}
  .rw-panel-title {{ font-size:15px; font-weight:700; color:var(--hdr); margin-bottom:4px; letter-spacing:0.5px; font-family:var(--mn); }}
  .rw-panel-sub {{ font-size:12px; color:var(--tx); margin-bottom:12px; line-height:1.5; }}
  .rw-item {{ padding:8px 0; border-bottom:1px solid var(--b); display:flex; flex-direction:column; gap:2px; }}
  .rw-item:last-child {{ border-bottom:none; }}
  .rw-name {{ font-size:15px; color:var(--br); font-weight:600; }}
  .rw-link {{ font-size:15px; color:var(--bl); text-decoration:none; line-height:1.4; }}
  .rw-link:hover {{ color:var(--tq); }}
  .rw-meta {{ font-size:12px; color:var(--tx); }}
  .rw-empty {{ font-size:12px; color:var(--tx); padding:12px 0; font-style:italic; }}

  /* XRP Community Hub (V67) */
  .cm-wrap {{ display:grid; grid-template-columns:1fr 1fr; gap:14px; }}
  @media(max-width:700px){{ .cm-wrap {{ grid-template-columns:1fr; }} }}
  .cm-panel {{ background:var(--s1); border:1px solid var(--b); border-radius:10px; padding:16px; }}
  .cm-panel-title {{ font-size:15px; font-weight:700; color:var(--hdr); margin-bottom:10px; letter-spacing:0.5px; font-family:var(--mn); }}
  .cm-item {{ padding:7px 0; border-bottom:1px solid var(--b); }}
  .cm-item:last-child {{ border-bottom:none; }}
  .cm-link {{ font-size:15px; color:var(--bl); text-decoration:none; font-weight:600; }}
  .cm-link:hover {{ color:var(--tq); }}
  .cm-desc {{ font-size:12px; color:var(--tx); margin-top:2px; line-height:1.4; }}

  /* FOOTER */
  footer{{ border-top:2px solid var(--bl); background:var(--bg); padding:16px 28px 16px; text-align:center; color:var(--tx); font-size:15px; font-family:var(--mn); }}
  footer .f-line{{ margin:5px 0; }}
  footer .brand-em{{ color:var(--bl); font-weight:700; font-style:normal; }}
  footer .val{{ color:var(--br); font-weight:700; }}
  .footer-btn{{ font-family:var(--mn); font-size:15px; font-weight:700; text-decoration:none; border-radius:3px; padding:1px 8px; cursor:pointer; margin-left:6px; }}
  .debug-btn{{ color:var(--or); border:1px solid var(--or); background:transparent; }}
  .debug-btn:hover{{ background:rgba(255,153,0,.12); }}
  .details-btn{{ color:var(--bl); border:1px solid var(--bl); background:transparent; }}
  .details-btn:hover{{ background:var(--bld); }}
  .notice{{ color:var(--yl); }}
  .copyright{{ font-size:12px; color:var(--tx); border-top:1px solid var(--b); padding-top:10px; margin-top:10px; }}

  /* PREFLIGHT MODAL */
  #pf-modal{{ display:none; position:fixed; inset:0; background:rgba(0,0,0,.92); z-index:9999; align-items:center; justify-content:center; padding:20px; }}
  #pf-box{{ background:var(--s1); border:1px solid var(--bl); border-radius:10px; max-width:580px; width:100%; overflow:hidden; }}
  #pf-box .pf-head{{ padding:12px 16px; background:var(--s2); border-bottom:1px solid var(--b); display:flex; justify-content:space-between; align-items:center; font-family:var(--mn); }}
  #pf-box .pf-head .t{{ color:var(--bl); font-weight:800; font-size:15px; text-transform:uppercase; letter-spacing:1px; }}
  #pf-box .pf-head .x{{ color:var(--bl); cursor:pointer; font-size:17px; font-weight:900; border:1px solid var(--bl); width:26px; height:26px; display:flex; align-items:center; justify-content:center; border-radius:4px; }}
  #pf-box .pf-head .x:hover{{ background:var(--bl); color:#000; }}
  #pf-box .pf-body{{ padding:14px 16px; font-family:var(--mn); font-size:15px; }}
  #pf-box .pf-overall{{ font-weight:800; color:{overall_color}; margin-bottom:10px; }}
  .pf-row{{ display:grid; grid-template-columns:1fr auto; grid-template-areas:"label badge" "detail detail"; gap:2px 10px; padding:8px 0; border-bottom:1px solid var(--b); }}
  .pf-row-label{{ grid-area:label; font-weight:700; color:var(--br); }}
  .pf-row-badge{{ grid-area:badge; font-weight:800; }}
  .pf-row-detail{{ grid-area:detail; color:var(--tx); font-size:12px; }}

  /* FLOATING RETURN / BACK-TO-TOP */
  #back-to-top{{ position:fixed; right:22px; bottom:22px; z-index:200; background:var(--bl); color:#000; border:none; border-radius:50%; width:46px; height:46px; font-size:17px; font-weight:900; cursor:pointer; box-shadow:0 0 14px rgba(117,188,255,.5); display:none; align-items:center; justify-content:center; line-height:1; }}
  #back-to-top:hover{{ background:#a6d4ff; }}
</style>
</head>
<body id="top">

  <!-- BREAKING NEWS BAR -->
  <div id="breaking">
    <div class="bkinner">
      <div class="bkrow">
        <span class="bklbl"><span class="bk-bolt">\u26A1</span>BREAKING NEWS</span>
        <div class="bkscroll">
          <div class="bktext" id="bktext">{bktext}</div>
        </div>
      </div>
    </div>
  </div>

  <div class="w">
    <!-- HEADER -->
    <div class="hdr">
      <div class="logo">
        <div class="icon">\U0001F6F0\uFE0F</div>
        <div>
          <div class="title">{APP_NAME}</div>
          <div class="sub" style="font-size:17px;color:var(--hdr);letter-spacing:1.5px">The <i>NEW</i> XRP Intelligence Standard</div>
          <div class="sub" style="font-size:15px;color:var(--br);letter-spacing:1.2px">Every Signal. Every Region. Every Hour.</div>
          <div class="sub" style="font-size:15px;color:var(--tx);letter-spacing:1px">306+ sources over 8 global regions signaling 24/7</div>
        </div>
      </div>
      <div class="hright" style="display:flex;flex-direction:column;align-items:flex-end;gap:6px">
        <div style="display:flex;align-items:center;gap:8px">
          <span class="dot"></span>
          <span class="run-lbl">LIVE</span>
          <span class="upd" id="uts">{boot_str}</span>
          <span style="font-size:12px;color:var(--tx);margin-left:8px;letter-spacing:0.5px">v{APP_VERSION}</span>
        </div>
        <div class="sub" style="font-size:15px;color:var(--gr);letter-spacing:1px">\u25CF {hdr_feeds_active}/{hdr_feeds_total} feeds scanned</div>
      </div>
    </div>

    <!-- SECTION 2: STATUS ROW (3 compact rectangles) -->
    <div class="srow">
      <div class="si">
        <span class="si-lbl"><span class="ic" style="color:var(--gr);font-weight:900">$</span> XRP / USD</span>
        <span>
          <span class="sv" id="st-price" style="color:{price_color};display:block">{price_str}</span>
          <span class="sv-sub" id="st-chg" style="color:{price_color};text-align:right;display:block">{chg_str}</span>
        </span>
      </div>
      <div class="si">
        <span class="si-lbl"><span class="ic">\U0001F4E1</span> Active Sources</span>
        <span class="sv" id="st-feeds" style="color:var(--bl)">{sources_str}</span>
      </div>
      <div class="si">
        <span class="si-lbl"><span class="ic">\U0001F630</span> Fear &amp; Greed</span>
        {fng_bar}
      </div>
    </div>

    <!-- SECTION 3: RSI / Support-Resistance / Time Machine / 52-Week -->
    <div class="grid2">
      <!-- LEFT COLUMN: RSI + 52-Week -->
      <div class="col">
        <div class="acct" style="border-color:rgba(3,177,252,.35)">
          <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F4D0</span> RSI Signals</div>
          <div style="margin-bottom:14px">
            <div class="rsi-head">
              <span style="color:var(--tx)">1H RSI</span>
              <span style="font-weight:700;color:{r1h_col}">{r1h_val}</span>
              <span style="color:{r1h_col}">{r1h_lbl}</span>
            </div>
            <div class="rsi-track">
              <div class="rsi-tick" style="left:30%"></div>
              <div class="rsi-tick" style="left:70%"></div>
              <div class="rsi-fill" style="width:{r1h_pct}%;background:{r1h_col}"></div>
            </div>
            <div class="rsi-scale"><span>0 \u2014 Oversold</span><span>30</span><span>50</span><span>70</span><span>Overbought \u2014 100</span></div>
          </div>
          <div>
            <div class="rsi-head">
              <span style="color:var(--tx)">1D RSI</span>
              <span style="font-weight:700;color:{r1d_col}">{r1d_val}</span>
              <span style="color:{r1d_col}">{r1d_lbl}</span>
            </div>
            <div class="rsi-track">
              <div class="rsi-tick" style="left:30%"></div>
              <div class="rsi-tick" style="left:70%"></div>
              <div class="rsi-fill" style="width:{r1d_pct}%;background:{r1d_col}"></div>
            </div>
            <div class="rsi-scale"><span>0 \u2014 Oversold</span><span>30</span><span>50</span><span>70</span><span>Overbought \u2014 100</span></div>
          </div>
        </div>

        <div class="acct grow" style="border-color:rgba(3,177,252,.35)">
          <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F4C5</span> 52-Week Range</div>
          <div class="w52-row">
            <span>Low: <strong style="color:var(--rd)">{w52_low_s}</strong></span>
            <span style="color:var(--tx)">Current: <strong style="color:var(--br)">{w52_cur_s}</strong></span>
            <span>High: <strong style="color:var(--gr)">{w52_high_s}</strong></span>
          </div>
          <div class="w52-bar">
            <div class="w52-needle" style="left:{w52_pos}%"></div>
          </div>
          <div class="w52-row">
            <span style="color:var(--tx)">From low: <strong style="color:var(--gr)">{w52_from_low}</strong></span>
            <span style="color:var(--tx)">Position: <strong style="color:var(--yl)">{w52_pos_s}</strong></span>
            <span style="color:var(--tx)">From high: <strong style="color:var(--rd)">{w52_from_high}</strong></span>
          </div>
        </div>
      </div>

      <!-- RIGHT COLUMN: Support/Resistance + Time Machine -->
      <div class="col">
        <div class="acct" style="border-color:rgba(255,64,96,.35)">
          <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F3AF</span> Support &amp; Resistance</div>
          {sr_html}
        </div>

        <div class="acct grow" style="border-color:rgba(3,177,252,.35)">
          <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F4C6</span> Price Time Machine</div>
          <div class="agrid2">
            <div class="abox">{tm_1y_html}</div>
            <div class="abox">{tm_1m_html}</div>
          </div>
          <div class="tvs">
            <div class="tvs-lbl">Today vs 1 Year Ago</div>
            <div class="tvs-txt" id="pt-narrative">{tm_narr}</div>
          </div>
        </div>
      </div>
    </div>

    <!-- SECTION 4: LIVE XRP/USD CHART -->
    <div class="acct" style="padding:10px;border-color:rgba(3,177,252,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F4CA</span> Live XRP/USD Chart</div>
      <div style="height:440px;border-radius:8px;overflow:hidden;border:1px solid var(--b)">
        <div class="tradingview-widget-container" style="width:100%;height:100%">
          <div class="tradingview-widget-container__widget" style="width:100%;height:100%"></div>
          <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js" async>
          {{"autosize":true,"symbol":"BITSTAMP:XRPUSD","interval":"60","timezone":"Etc/UTC","theme":"dark","style":"1","locale":"en","backgroundColor":"#000000","gridColor":"#0a0a0a","hide_top_toolbar":false,"allow_symbol_change":false,"save_image":false,"support_host":"https://www.tradingview.com"}}
          </script>
        </div>
      </div>
    </div>

    <!-- SECTION 5: ON-CHAIN INTELLIGENCE + WHALE ALERT FEED -->
    <div class="oc-grid">
      <div class="acct" style="border-color:rgba(0,229,204,.35)">
        <div class="sec-title" style="color:var(--hdr)"><span class="sic">\u26D3\uFE0F</span> On-Chain Intelligence</div>
        <div class="ocbox-grid">
          <div class="ocbox tq">
            <div class="oclbl">Market Cap</div>
            <div class="ocval" style="color:var(--tq)">{oc_mcap}</div>
            <div class="ocsub">{oc_rank}</div>
          </div>
          <div class="ocbox">
            <div class="oclbl">24h Volume</div>
            <div class="ocval" style="color:var(--bl)">{oc_vol}</div>
            <div class="ocsub">{oc_volmcap}</div>
          </div>
          <div class="ocbox">
            <div class="oclbl">24h Range</div>
            <div class="ocval" style="color:var(--tq);font-size:17px">{oc_low} \u2013 {oc_high}</div>
            <div class="ocsub">{oc_rsi}</div>
          </div>
          <div class="ocbox">
            <div class="oclbl">52-Week Range</div>
            <div class="ocval" style="color:var(--bl);font-size:17px">{oc_52l} \u2013 {oc_52h}</div>
            <div class="ocsub">XRP / USD</div>
          </div>
          <div class="ocbox esc">
            <div class="oclbl">\u23F3 Next Ripple Escrow Release</div>
            <div class="esc-row">
              <div><div class="esc-num" id="esc-days">--</div><div class="ocsub">days</div></div>
              <div class="esc-sep">:</div>
              <div><div class="esc-num" id="esc-hrs">--</div><div class="ocsub">hrs</div></div>
              <div class="esc-sep">:</div>
              <div><div class="esc-num" id="esc-min">--</div><div class="ocsub">min</div></div>
            </div>
            <div class="ocsub">1B XRP \u00B7 Next release: {esc_date_str}</div>
          </div>
        </div>
      </div>

      <div class="panel" style="border-color:rgba(255,204,0,.35)">
        <div class="ph">
          <span class="pt" style="color:var(--hdr)"><span class="sic">\U0001F433</span> Whale Alert Feed</span>
          <span style="font-size:15px;font-family:var(--mn);color:var(--tx)" id="whale-ts">{whale_ts_val}</span>
        </div>
        <div class="whale-feed" id="whale-feed">
          {whale_feed_html}
        </div>
      </div>
    </div>

    <!-- SECTION 6: XRP ECOSYSTEM -->
    <div class="eco-wrap">
      <div class="eco-head">
        <span class="gicon">\U0001F310</span>
        <div>
          <div class="eco-title">XRP Ecosystem</div>
          <div class="eco-sub">Eight interconnected layers powering the future of global finance</div>
        </div>
      </div>
      <div class="eco-grid">
        {eco_html}
      </div>

      <!-- How the Layers Connect -->
      <div class="eco-sub-h">\u26D3\uFE0F How the Layers Connect</div>
      <div class="flow">
        <div class="flow-node"><div class="flow-ic">\U0001F517</div><div class="flow-name" style="color:var(--tq)">XRPL</div><div class="flow-role">Foundation</div></div>
        <div class="flow-arrow">\u2192</div>
        <div class="flow-node"><div class="flow-ic">\U0001F48E</div><div class="flow-name" style="color:var(--gr)">XRP</div><div class="flow-role">Native Asset</div></div>
        <div class="flow-arrow">\u2192</div>
        <div class="flow-node"><div class="flow-ic">\U0001F3E2</div><div class="flow-name" style="color:var(--bl)">Ripple Labs</div><div class="flow-role">Builder</div></div>
        <div class="flow-arrow">\u2192</div>
        <div class="flow-node"><div class="flow-ic">\U0001F310</div><div class="flow-name" style="color:var(--or)">RippleNet</div><div class="flow-role">Network</div></div>
        <div class="flow-arrow">\u2192</div>
        <div class="flow-node"><div class="flow-ic">\u26A1</div><div class="flow-name" style="color:var(--rd)">ODL</div><div class="flow-role">Liquidity</div></div>
        <div class="flow-arrow">+</div>
        <div class="flow-node"><div class="flow-ic">\U0001F4B5</div><div class="flow-name" style="color:var(--bl)">RLUSD</div><div class="flow-role">Stablecoin</div></div>
        <div class="flow-arrow">\u2192</div>
        <div class="flow-node"><div class="flow-ic">\U0001F6E0\uFE0F</div><div class="flow-name" style="color:var(--yl)">Ecosystem</div><div class="flow-role">Builders</div></div>
      </div>

      <!-- Common Misconceptions -->
      <div class="eco-sub-h">\u26A0\uFE0F Common Misconceptions \u2014 Set the Record Straight</div>
      <div class="myth-grid">
        <div class="myth-card">
          <div class="myth-lbl">\u274C MYTH</div>
          <div class="myth-q">"Ripple controls XRP"</div>
          <div class="real-lbl">\u2705 REALITY</div>
          <div class="real-txt">XRP runs on the XRPL, which is decentralised and maintained by the independent XRPL Foundation. Ripple holds XRP but cannot create, destroy, or freeze it.</div>
        </div>
        <div class="myth-card">
          <div class="myth-lbl">\u274C MYTH</div>
          <div class="myth-q">"Ripple can print more XRP"</div>
          <div class="real-lbl">\u2705 REALITY</div>
          <div class="real-txt">XRP has a fixed maximum supply of 100 billion \u2014 hardcoded into the protocol. No mining, no inflation, no new XRP can ever be created. Supply only decreases as tiny amounts are burned per transaction.</div>
        </div>
        <div class="myth-card">
          <div class="myth-lbl">\u274C MYTH</div>
          <div class="myth-q">"XRP is a security"</div>
          <div class="real-lbl">\u2705 REALITY</div>
          <div class="real-txt">Judge Torres ruled in 2023 that XRP is NOT a security in programmatic sales. The SEC settled with Ripple in 2025. XRP now operates with full US regulatory clarity for the first time.</div>
        </div>
      </div>
    </div>

    <!-- SECTION 7: MAINSTREAM INTEGRATION MONITOR (title + tagline + legend key) -->
    <div class="acct" style="border-color:rgba(255,204,0,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001FA9A</span> Mainstream Integration Monitor</div>
      <div class="trk-tag">XRP is no longer knocking on the door of traditional finance \u2014 it's building new springboards for growth and utilization.</div>
      <div class="trk-legend">
        <button class="trk-btn active" data-filter="ALL" onclick="filterTracker('ALL',this)" style="color:#ffffff;border-color:rgba(255,255,255,.5)">ALL</button>
        <button class="trk-btn" data-filter="CONFIRMED" onclick="filterTracker('CONFIRMED',this)" style="color:var(--gr);border-color:rgba(72,255,130,.5)">\u2705 CONFIRMED</button>
        <button class="trk-btn" data-filter="EXPLORING" onclick="filterTracker('EXPLORING',this)" style="color:var(--bl);border-color:rgba(117,188,255,.5)">\U0001F50D EXPLORING</button>
        <button class="trk-btn" data-filter="RUMORED" onclick="filterTracker('RUMORED',this)" style="color:var(--yl);border-color:rgba(255,204,0,.5)">\U0001F4AC RUMORED</button>
        <button class="trk-btn" data-filter="PILOT" onclick="filterTracker('PILOT',this)" style="color:var(--or);border-color:rgba(255,153,0,.5)">\U0001F9EA PILOT</button>
        <button class="trk-btn" data-filter="COMPETING" onclick="filterTracker('COMPETING',this)" style="color:var(--rd);border-color:rgba(255,64,96,.5)">\u2694\uFE0F COMPETING</button>
      </div>
    </div>

    <!-- SECTION 8: INSTITUTIONAL PARTNERSHIP TRACKER (separate section: 20 institutions, 5 rows of 4) -->
    <div class="acct" style="border-color:rgba(255,204,0,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F3DB\uFE0F</span> Institutional Partnership Tracker</div>
      <div class="trk-grid">
        {inst_html}
      </div>
      <div id="trk-empty" class="trk-empty" style="display:none">No institutions in this category are currently available.</div>
    </div>

    <!-- SECTION 9: XRP × TRADITIONAL FINANCE — INTEGRATION TIMELINE -->
    <div class="acct" style="border-color:rgba(255,204,0,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F4C5</span> XRP \u00D7 Traditional Finance \u2014 Integration Timeline</div>
      <div class="tl-wrap">
        <div class="tl-line"></div>
        <div class="tl-track">
          {tl_html}
        </div>
      </div>
    </div>

    <!-- SECTION 10: TOP 20 XRP STORIES (two subsections) -->
    <div class="acct" style="border-color:rgba(255,204,0,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F3C6</span> Top 20 XRP Stories</div>
      <div class="eco-sub-h" style="padding:0"><span style="font-size:17px">\U0001F4F0</span> Top 20 Current Stories</div>
      <div class="story-list">
        {stories_current}
      </div>
      <div class="eco-sub-h" style="padding:0"><span style="font-size:17px">\U0001F525</span> Top 20 Most Influential Articles of the Week</div>
      <div class="story-list">
        {stories_weekly}
      </div>
    </div>

    <!-- SECTION 11: US INTELLIGENCE + GLOBAL PULSE (2-column, news-derived) -->
    <div class="intel-grid">
      <div class="intel" style="border-color:rgba(3,177,252,.35)">
        <div class="intel-h">
          <span class="intel-t" style="color:var(--hdr)"><span class="sic">\U0001F1FA\U0001F1F8</span> US Intelligence</span>
          <span style="font-size:15px;font-family:var(--mn);color:var(--tx)">{us_ts}</span>
        </div>
        <div class="intel-b">
          <div class="intel-pulse">{us_pulse}</div>
          <div class="intel-row"><b>Regulatory</b><br>{us_regulatory}</div>
          <div class="intel-row"><b>Institutional</b><br>{us_institutional}</div>
        </div>
      </div>
      <div class="intel" style="border-color:rgba(72,255,130,.35)">
        <div class="intel-h">
          <span class="intel-t" style="color:var(--hdr)"><span class="sic">\U0001F310</span> Global Pulse</span>
          <span style="font-size:15px;font-family:var(--mn);color:var(--tx)">{gl_ts}</span>
        </div>
        <div class="intel-b">
          <div class="intel-pulse">{gl_pulse}</div>
          <div class="intel-row"><b>Thesis</b><br>{gl_thesis}</div>
          <div class="sig-row">{gl_signals_html}</div>
        </div>
      </div>
    </div>

    <!-- SECTION 12: REGIONAL DISCOURSE (news-derived) -->
    <div class="acct" style="border-color:rgba(3,177,252,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F5FA\uFE0F</span> Regional Discourse</div>
      <div class="rd-grid">
        {rd_html}
      </div>
    </div>

    <!-- SECTION 13: SIGNAL SCOREBOARD -->
    <div class="acct" style="border-color:rgba(3,177,252,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F4E1</span> Signal Scoreboard</div>
      <div class="sb-grid">
        <div class="sb-box"><div class="sb-num" style="color:var(--bl)">{sb_total}</div><div class="sb-lbl">Stories Tracked</div><div class="sb-sub">{sb_feeds} sources</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--gr)">{sb_bull}</div><div class="sb-lbl">Bullish</div><div class="sb-sub">{sb_bull_pct}%</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--rd)">{sb_bear}</div><div class="sb-lbl">Bearish</div><div class="sb-sub">{sb_bear_pct}%</div></div>
        <div class="sb-box"><div class="sb-num">{sb_neut}</div><div class="sb-lbl">Neutral</div><div class="sb-sub" style="color:{sb_net_col}">Net: {sb_net_str}</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--yl)">{sb_fng}</div><div class="sb-lbl">Fear &amp; Greed</div><div class="sb-sub">{sb_fng_lbl}</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--bl)">{sb_rank}</div><div class="sb-lbl">Global Rank</div><div class="sb-sub">CoinCap</div></div>
      </div>
      <div class="sb-grid4">
        <div class="sb-box"><div class="sb-num" style="color:var(--bl)">{sb_mcap}</div><div class="sb-lbl">Market Cap</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--yl)">{sb_vol}</div><div class="sb-lbl">24h Volume</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--gr)">{sb_high}</div><div class="sb-lbl">24h High</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--rd)">{sb_low}</div><div class="sb-lbl">24h Low</div></div>
      </div>
      <div class="sb-bar"><div class="sb-fill" style="width:{sb_bull_pct}%"></div></div>
    </div>

    <!-- SECTION 14: GLOBAL NEWS FEED + RIGHT RAIL -->
    <div class="ledger-wrap">
      <div class="acct" style="border-color:rgba(3,177,252,.35);margin:0">
        <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F5DE\uFE0F</span> Global News Feed &amp; Search</div>
        <input class="gn-search" id="gn-search" type="text" placeholder="\U0001F50D Search XRP news..." oninput="filterFeed()">
        <div class="gn-cats" id="gn-cats">
          <button class="gn-btn active" data-cat="ALL" style="color:var(--br);border-color:var(--br)" onclick="feedCat('ALL',this)">ALL</button>
          <button class="gn-btn" data-cat="PRICE" style="color:var(--yl);border-color:var(--yl)" onclick="feedCat('PRICE',this)">PRICE</button>
          <button class="gn-btn" data-cat="LEGAL" style="color:var(--rd);border-color:var(--rd)" onclick="feedCat('LEGAL',this)">LEGAL</button>
          <button class="gn-btn" data-cat="REG" style="color:var(--or);border-color:var(--or)" onclick="feedCat('REG',this)">REG</button>
          <button class="gn-btn" data-cat="ECOSYSTEM" style="color:var(--gr);border-color:var(--gr)" onclick="feedCat('ECOSYSTEM',this)">ECOSYSTEM</button>
          <button class="gn-btn" data-cat="TECH" style="color:var(--tq);border-color:var(--tq)" onclick="feedCat('TECH',this)">TECH</button>
          <button class="gn-btn" data-cat="WHALE" style="color:var(--bl);border-color:var(--bl)" onclick="feedCat('WHALE',this)">WHALE</button>
        </div>
        <div class="gn-stats"><b id="gn-shown">{gn_shown}</b> stories shown &nbsp;|&nbsp; {gn_total} total &nbsp;|&nbsp; {sb_feeds} sources online</div>
        <div class="gn-list" id="gn-list">
          {gn_html}
        </div>
        <div class="gn-empty" id="gn-empty" style="display:none">No stories match your filter.</div>
      </div>

      <div class="rail">
        <div class="rail-panel">
          <div class="rail-h"><span class="sic">\U0001F517</span> XRPL Network</div>
          <div class="rail-row"><span class="rail-k">Network</span><span class="rail-v" style="color:var(--gr)">\u25CF Live</span></div>
          <div class="rail-row"><span class="rail-k">Consensus</span><span class="rail-v">Federated Byzantine</span></div>
          <div class="rail-row"><span class="rail-k">Ledger Close</span><span class="rail-v">~3-5 seconds</span></div>
          <div class="rail-row"><span class="rail-k">Tx Fee</span><span class="rail-v">~0.00001 XRP</span></div>
          <div class="rail-row"><span class="rail-k">Circulating</span><span class="rail-v" style="color:var(--gr)">62.2B XRP</span></div>
          <div class="rail-row"><span class="rail-k">Escrow Locked</span><span class="rail-v">~43B XRP</span></div>
          <div class="rail-row"><span class="rail-k">Total Supply</span><span class="rail-v">100B XRP</span></div>
        </div>
        <div class="rail-panel">
          <div class="rail-h"><span class="sic">\U0001F4CA</span> Market Structure</div>
          <div class="rail-row"><span class="rail-k">Price</span><span class="rail-v">{ms_price}</span></div>
          <div class="rail-row"><span class="rail-k">24h Change</span><span class="rail-v" style="color:{ms_chg_col}">{ms_chg}</span></div>
          <div class="rail-row"><span class="rail-k">Global Rank</span><span class="rail-v" style="color:var(--bl)">{ms_rank}</span></div>
          <div class="rail-row"><span class="rail-k">Market Cap</span><span class="rail-v">{ms_mcap}</span></div>
          <div class="rail-row"><span class="rail-k">24h Volume</span><span class="rail-v">{ms_vol}</span></div>
          <div class="rail-row"><span class="rail-k">Vol / MCap</span><span class="rail-v" style="color:var(--yl)">{ms_volmcap}</span></div>
          <div class="rail-row"><span class="rail-k">24h High</span><span class="rail-v" style="color:var(--gr)">{ms_high}</span></div>
          <div class="rail-row"><span class="rail-k">24h Low</span><span class="rail-v" style="color:var(--rd)">{ms_low}</span></div>
          <div class="rail-row"><span class="rail-k">XRP/BTC</span><span class="rail-v">{ms_xrpbtc}</span></div>
        </div>
        <div class="rail-panel">
          <div class="rail-h"><span class="sic">\u23F3</span> Ripple Escrow</div>
          <div class="rail-row"><span class="rail-k">Next Release</span><span class="rail-v" style="color:var(--yl)">{esc_next_str}</span></div>
          <div class="rail-row"><span class="rail-k">Amount</span><span class="rail-v">1B XRP</span></div>
        </div>
      </div>
    </div>

    <!-- SECTION 15: ANALYTICS LAB -->
    <div class="acct" style="border-color:rgba(3,177,252,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F52C</span> Analytics Lab</div>
      <div class="lab3">
        <div class="labp">
          <div class="labt"><span style="font-size:17px">\U0001F4C8</span> Signal Metrics</div>
          <div class="bstat"><span class="bk">Stories Today</span><span class="bv" style="color:var(--bl)">{sb_total}</span></div>
          <div class="bstat"><span class="bk">Bullish Signals</span><span class="bv" style="color:var(--gr)">{sb_bull}</span></div>
          <div class="bstat"><span class="bk">Bearish Signals</span><span class="bv" style="color:var(--rd)">{sb_bear}</span></div>
          <div class="bstat"><span class="bk">Neutral</span><span class="bv">{sb_neut}</span></div>
          <div class="bstat"><span class="bk">Net Sentiment</span><span class="bv" style="color:{sb_net_col}">{sb_net_str}</span></div>
          <div class="bstat"><span class="bk">Bull/Bear Ratio</span><span class="bv" style="color:var(--yl)">{al_ratio}</span></div>
        </div>
        <div class="labp">
          <div class="labt"><span style="font-size:17px">\U0001F4CA</span> Market Analytics</div>
          <div class="bstat"><span class="bk">Global Rank</span><span class="bv" style="color:var(--bl)">{ms_rank}</span></div>
          <div class="bstat"><span class="bk">Market Cap</span><span class="bv">{ms_mcap}</span></div>
          <div class="bstat"><span class="bk">24h Volume</span><span class="bv" style="color:var(--yl)">{ms_vol}</span></div>
          <div class="bstat"><span class="bk">Vol / MCap %</span><span class="bv" style="color:var(--bl)">{ms_volmcap}</span></div>
          <div class="bstat"><span class="bk">Fear &amp; Greed</span><span class="bv" style="color:var(--yl)">{al_fng}</span></div>
          <div class="bstat"><span class="bk">24h Change</span><span class="bv" style="color:{ms_chg_col}">{ms_chg}</span></div>
        </div>
        <div class="labp">
          <div class="labt"><span style="font-size:17px">\U0001F50D</span> Feed Intelligence</div>
          <div class="bstat"><span class="bk">Total Sources</span><span class="bv" style="color:var(--bl)">{NEWS["feeds_total"]}</span></div>
          <div class="bstat"><span class="bk">Active Feeds</span><span class="bv" style="color:var(--gr)">{NEWS["feeds_active"]}</span></div>
          <div class="bstat"><span class="bk">Foreign Feeds</span><span class="bv">{al_foreign} stories</span></div>
          <div class="bstat"><span class="bk">Refresh</span><span class="bv">5 min</span></div>
          <div class="bstat"><span class="bk">Regions Tracked</span><span class="bv" style="color:var(--yl)">8 regions</span></div>
          <div class="bstat"><span class="bk">Engine</span><span class="bv" style="color:var(--gr)">News-Derived</span></div>
        </div>
      </div>
      <div class="sb-grid4">
        <div class="sb-box"><div class="sb-num" style="color:var(--bl)">{sb_total}</div><div class="sb-lbl">Total Stories</div><div class="sb-sub">In memory</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--gr)">{sb_bull_pct}%</div><div class="sb-lbl">Bullish</div><div class="sb-sub">of tracked</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--rd)">{sb_bear_pct}%</div><div class="sb-lbl">Bearish</div><div class="sb-sub">of tracked</div></div>
        <div class="sb-box"><div class="sb-num" style="color:{sb_net_col}">{sb_net_str}</div><div class="sb-lbl">Net Sentiment</div><div class="sb-sub">bull \u2212 bear</div></div>
      </div>
    </div>

    <!-- SECTION 16: XRPRADAR LEADERBOARD -->
    <div class="acct" style="border-color:rgba(255,204,0,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F3C6</span> XRPRadar Leaderboard</div>
      <div class="trk-tag">Top sources, most active regions, and live intelligence \u2014 the XRPRadar rankings.</div>
      <div class="lb-grid">
        <div class="lb-panel">
          <div class="lb-t" style="color:var(--yl)">\U0001F4E1 Top Sources Today</div>
          {lb_sources}
        </div>
        <div class="lb-panel">
          <div class="lb-t" style="color:var(--bl)">\U0001F5FA\uFE0F Most Active Regions</div>
          {lb_regions}
        </div>
        <div class="lb-panel">
          <div class="lb-t" style="color:var(--gr)">\U0001F525 Live Intelligence</div>
          <div class="lb-score">
            <div class="lb-score-num" style="color:{lb_color}">{lb_score}</div>
            <div class="lb-score-cap">Signal Score / 100</div>
            <div class="lb-score-lbl" style="color:{lb_color}">{lb_label}</div>
          </div>
          <div class="lb-mini">
            <div class="lb-mini-row"><span>Feeds Active</span><span style="color:var(--gr)">{sb_feeds}</span></div>
            <div class="lb-mini-row"><span>Stories Today</span><span style="color:var(--bl)">{sb_total}</span></div>
            <div class="lb-mini-row"><span>Bullish Share</span><span style="color:var(--yl)">{sb_bull_pct}%</span></div>
          </div>
        </div>
      </div>
    </div>

    <!-- SECTION 17: XRP INTELLIGENCE BRIEF (twice daily — AM 12:00 PM CST, PM 9:00 PM CST) -->
    <div class="acct" style="border-color:rgba(255,204,0,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr);margin-bottom:10px"><span class="sic">\U0001F52E</span> XRP Intelligence Brief</div>

      <div class="brf-teaser">
        <div class="brf-teaser-line">\U0001F52E Next Proprietary Briefing in <span id="brf-countdown">\u2014</span></div>
        <div class="brf-teaser-sub">Twice daily \u2014 12:00 PM &amp; 9:00 PM CST \u2014 see World Clocks below</div>
      </div>

      <div class="brf-now-showing" id="brf-now-showing">
        <span class="brf-ribbon-wrap"><span class="brf-ribbon-icon">\U0001F52E</span><span class="brf-ribbon" id="brf-ribbon-label">CURRENT BRIEF</span></span>
        <span id="brf-now-edition">{brf_edition} EDITION</span>, {brf_gen}
        <span class="brf-now-spacer">\u00B7</span>
        <span id="brf-next-line">Next edition {brf_next}</span>
      </div>
      <div class="brf-intro-line">This edition's analysis, broken into 6 topics below \u2014 same briefing, organized by subject:</div>
      <div class="brf-grid" id="brief-{_live_slot}">
        <div class="brf-block"><div class="brf-t"><span style="font-size:17px">\U0001F4CA</span> Market Pulse</div><div class="brf-x" id="brf-pulse">{brf_pulse}</div></div>
        <div class="brf-block"><div class="brf-t"><span style="font-size:17px">\U0001F517</span> Story Connections</div><div class="brf-x" id="brf-connections">{brf_conn}</div></div>
        <div class="brf-block"><div class="brf-t"><span style="font-size:17px">\U0001F3B2</span> Domino Effect</div><div class="brf-x" id="brf-domino">{brf_domino}</div></div>
        <div class="brf-block"><div class="brf-t"><span style="font-size:17px">\U0001F30D</span> Regional Flashpoints</div><div class="brf-x" id="brf-regional">{brf_regional}</div></div>
        <div class="brf-block"><div class="brf-t"><span style="font-size:17px">\U0001F441\uFE0F</span> Watchlist</div><div class="brf-x" id="brf-watchlist">{brf_watch}</div></div>
        <div class="brf-block"><div class="brf-t"><span style="font-size:17px">\U0001F3DB\uFE0F</span> TradFi Integration Outlook</div><div class="brf-x" id="brf-tradfi">{brf_tradfi}</div></div>
      </div>
      <div class="brf-note">\u26A0\uFE0F Informational only \u2014 not financial advice. Editions publish at 12:00 PM and 9:00 PM CST and are derived from the live news feed.</div>
    </div>
    <script type="application/json" id="brief-archive-data">{_archive_json}</script>

    <!-- SECTION 18: WORLD BRIEFING CLOCKS -->
    <div class="acct" style="border-color:rgba(3,177,252,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F310</span> World Briefing Clocks</div>
      <div class="trk-tag" style="color:var(--tx)">Local time across major crypto hubs, with each city's 1st (12:00 PM CST) and 2nd (9:00 PM CST) briefing time \u2014 orange by day, gray by night.</div>
      <div class="wc-row">
        {wc_html}
      </div>
    </div>

    <!-- SECTION 19: UNIQUE DISPLAYS -->
    <div class="acct" style="border-color:rgba(3,177,252,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F3A8</span> Unique Displays</div>
      <div class="ud-grid">
        <div class="ud-panel">
          <div class="fg-title"><span style="font-size:17px">\U0001F9E0</span> Smart Money Score</div>
          <div><span class="sm-score" style="color:{sm_color}">{sm_score}</span><span class="sm-cap"> /100</span></div>
          <div class="sm-label" style="color:{sm_color}">{sm_label}</div>
          <div class="sm-bar"><div class="sm-fill" style="width:{sm_score}%"></div></div>
          {sm_rows}
        </div>
        <div class="ud-panel">
          <div class="fg-title"><span style="font-size:17px">\U0001F630</span> Fear &amp; Greed Index \u2014 30-Day History</div>
          <div class="fg-chart">{fng_hist_html}</div>
          <div class="fg-axis"><span>30 days ago</span><span>20 days ago</span><span>10 days ago</span><span>today</span></div>
          <div class="fg-legend">
            <span><span class="fg-key" style="background:var(--rd)"></span>Extreme Fear (0-25)</span>
            <span><span class="fg-key" style="background:var(--or)"></span>Fear (25-45)</span>
            <span><span class="fg-key" style="background:var(--yl)"></span>Neutral (45-55)</span>
            <span><span class="fg-key" style="background:var(--gr)"></span>Greed (55-75)</span>
            <span><span class="fg-key" style="background:var(--tq)"></span>Extreme Greed (75-100)</span>
          </div>
        </div>
      </div>
    </div>

    <!-- SECTION 20: LONGITUDINAL VALUE MARKERS -->
    <div class="acct" style="border-color:rgba(3,177,252,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F4C8</span> Longitudinal Value Markers</div>
      <div class="trk-tag" style="color:var(--tx)">XRP/USD price performance across key windows.</div>
      <div class="lvm-grid">
        {lvm_html}
      </div>
    </div>

    <!-- SECTION 21: REGIONAL NEWS ACTIVITY HEATMAP -->
    <div class="acct" style="border-color:rgba(3,177,252,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F5FA\uFE0F</span> Regional News Activity Heatmap</div>
      <div class="trk-tag" style="color:var(--tx)">XRP stories by region today \u2014 brighter means more coverage.</div>
      <div class="rh-grid">
        {rh_html}
      </div>
    </div>

    <!-- SECTION 22: SENTIMENT ENGINE -->
    <div class="acct" style="border-color:rgba(255,204,0,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F9E0</span> Sentiment Engine</div>

      <div class="sent-top">
        <div class="ud-panel" style="text-align:center">
          <div class="fg-title" style="justify-content:center"><span style="font-size:17px">\U0001F4E1</span> XRP Interest Score</div>
          <div class="sm-score" style="color:var(--yl)">{_isc_score}</div>
          <div class="sm-label" style="color:var(--yl)">{_isc_label}</div>
          <div class="sm-bar"><div class="sm-fill" style="width:{_isc_score}%"></div></div>
          <div class="pt-note" style="margin-top:8px">Derived from live feed velocity</div>
        </div>
        <div class="ud-panel">
          <div class="fg-title"><span style="font-size:17px">\U0001F4F0</span> News Velocity \u2014 Stories per Hour (24h)</div>
          <div class="vel-chart">{vel_html}</div>
          <div class="fg-axis"><span>24h ago</span><span>12h ago</span><span>now</span></div>
        </div>
      </div>

      <div class="ud-panel" style="margin-bottom:14px">
        <div class="fg-title"><span style="font-size:17px">\U0001F4C8</span> Sentiment Trend \u2014 Since Deploy (up to 30 days)</div>
        <div class="sdt-chart">{sdt_html}</div>
        <div class="fg-legend" style="margin-top:8px">
          <span><span class="fg-key" style="background:var(--gr)"></span>Bullish day</span>
          <span><span class="fg-key" style="background:var(--rd)"></span>Bearish day</span>
          <span><span class="fg-key" style="background:var(--tx)"></span>Balanced day</span>
        </div>
      </div>

      <div class="ud-panel">
        <div class="fg-title"><span style="font-size:17px">\U0001F3C6</span> Source Leaderboard \u2014 Most Active (Today)</div>
        <table class="pt-tbl">
          <thead><tr><th>#</th><th>Source</th><th style="text-align:center">Stories</th>
            <th style="text-align:center">Bull</th><th style="text-align:center">Bear</th>
            <th>Sentiment</th><th style="text-align:center">Breaking</th></tr></thead>
          <tbody>{sent_lb_rows}</tbody>
        </table>
      </div>
    </div>

    <!-- SECTION 23: COMPETITIVE BRIEFING -->
    <div class="acct" style="border-color:rgba(117,188,255,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\u2694\uFE0F</span> Competitive Briefing</div>

      <div class="trk-tag" style="color:var(--tx)">XRP vs major competitors \u2014 live performance.</div>
      <div class="tbl-scroll" style="margin-bottom:14px">
        <table class="pt-tbl">
          <thead><tr><th>Asset</th><th style="text-align:right">Price</th><th style="text-align:right">24h %</th>
            <th style="text-align:right">7d %</th><th style="text-align:right">Market Cap</th><th>XRP Edge</th></tr></thead>
          <tbody>{comp_rows}</tbody>
        </table>
      </div>

      <div class="pt-cols" style="margin-bottom:14px">
        <div class="pt-col">
          <div class="trk-tag" style="color:var(--tx);margin-bottom:8px">\U0001F310 Active ODL Corridors</div>
          {odl_html}
        </div>
        <div class="pt-col">
          <div class="trk-tag" style="color:var(--tx);margin-bottom:8px">\U0001F4CB ISO 20022 Adoption</div>
          <div style="background:var(--s2);border:1px solid rgba(72,255,130,.25);border-radius:8px;padding:10px;margin-bottom:8px">
            <div style="font-size:15px;color:var(--gr);line-height:1.7;font-family:system-ui">XRP and the XRPL natively support ISO 20022 data fields, positioning Ripple as infrastructure for the new global payment standard.</div>
          </div>
          {iso_html}
          <div style="margin-top:8px;padding:6px 10px;background:var(--s2);border-radius:5px;border:1px solid var(--b);font-size:15px;font-family:var(--mn)">
            Banks exploring ISO 20022 + Ripple: <span style="color:var(--yl);font-weight:700">200+</span>
          </div>
        </div>
      </div>

      <div class="trk-tag" style="color:var(--tx);margin-bottom:8px">\u26A1 XRP vs SWIFT \u2014 The Case for ODL</div>
      <div class="sw-grid">
        <div class="sb-box"><div class="sb-num" style="color:var(--rd)">$5T</div><div class="sb-lbl">SWIFT Daily Volume</div><div class="sb-sub">Traditional rails</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--rd)">1-5 days</div><div class="sb-lbl">SWIFT Settlement</div><div class="sb-sub">Avg. cross-border</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--rd)">2-10%</div><div class="sb-lbl">SWIFT Avg Cost</div><div class="sb-sub">Remittance fees</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--gr)">3-5 sec</div><div class="sb-lbl">XRPL Settlement</div><div class="sb-sub">Any corridor, 24/7</div></div>
        <div class="sb-box"><div class="sb-num" style="color:var(--gr)">$0.0002</div><div class="sb-lbl">XRPL Cost</div><div class="sb-sub">Per transaction</div></div>
      </div>
      <div style="margin-top:8px;padding:10px 14px;background:rgba(72,255,130,.04);border:1px solid rgba(72,255,130,.2);border-radius:6px;font-size:15px;color:var(--br);line-height:1.7;font-family:system-ui">
        XRPL settles in seconds for fractions of a cent, 24/7/365 \u2014 no correspondent banking chain, no cut-off times.
      </div>
    </div>

    <!-- SECTION 24: RIPPLE EXECUTIVE TRACKER + XRPL DEV ACTIVITY -->
    <div class="ed-grid" style="margin:10px 0">
      <div class="ed-panel" style="border-color:rgba(255,153,0,.25)">
        <div class="ed-head">
          <span class="ed-title" style="color:var(--or)">\U0001F3A4 Ripple Exec Tracker</span>
          <span style="font-size:12px;font-family:var(--mn);color:var(--tx)">{ex_ts}</span>
        </div>
        <div class="ex-tabs" id="ex-tabs">
          <button class="ex-tab on" data-tab="ALL" onclick="execTab('ALL',this)">ALL</button>
          <button class="ex-tab" data-tab="BRAD" onclick="execTab('BRAD',this)">BRAD</button>
          <button class="ex-tab" data-tab="MONICA" onclick="execTab('MONICA',this)">MONICA</button>
          <button class="ex-tab" data-tab="DAVID" onclick="execTab('DAVID',this)">DAVID</button>
          <button class="ex-tab" data-tab="STUART" onclick="execTab('STUART',this)">STUART</button>
        </div>
        <div class="ex-feed" id="ex-feed">
          {ex_html}
        </div>
      </div>

      <div class="ed-panel" style="border-color:rgba(72,255,130,.2)">
        <div class="ed-head">
          <span class="ed-title" style="color:var(--gr)">\U0001F4BB XRPL Dev Activity</span>
          <span style="font-size:12px;font-family:var(--mn);color:var(--tx)">{gh_ts}</span>
        </div>
        <div class="gh-stats">
          <div class="gh-stat"><div class="gh-stat-num" style="color:var(--gr)">{gh_rippled_7d}</div><div class="gh-stat-lbl">rippled commits<br>7 days</div></div>
          <div class="gh-stat"><div class="gh-stat-num" style="color:var(--bl)">{gh_other_7d}</div><div class="gh-stat-lbl">other repos<br>7 days</div></div>
          <div class="gh-stat"><div class="gh-stat-num" style="color:var(--yl)">{gh_stars}</div><div class="gh-stat-lbl">GitHub stars<br>3 repos</div></div>
          <div class="gh-stat"><div class="gh-stat-num" style="color:var(--or)">{gh_issues}</div><div class="gh-stat-lbl">open issues<br>3 repos</div></div>
        </div>
        <div class="gh-latest">
          <div class="gh-latest-lbl">Latest commit</div>
          <div class="gh-latest-msg">{gh_last_msg}</div>
          <div class="gh-latest-meta">{gh_last_meta}</div>
        </div>
        <div class="gh-feed" id="gh-feed">
          {gh_commits_html}
        </div>
      </div>
    </div>

    <!-- SECTION 25: REGULATORY RADAR -->
    <div class="acct" style="border-color:rgba(255,153,0,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F3DB\uFE0F</span> Regulatory Radar</div>

      <div class="trk-tag" style="color:var(--tx);display:flex;justify-content:space-between">
        <span>\U0001F30D Global XRP Legal Status</span><span>Reference \u2014 verify locally before acting</span>
      </div>
      <div class="cg-grid" style="margin-bottom:16px">
        {cg_html}
      </div>

      <div class="trk-tag" style="color:var(--tx);margin-bottom:8px">\U0001F4CA XRP ETF / ETP Tracker</div>
      <div class="tbl-scroll" style="margin-bottom:16px">
        <table class="pt-tbl">
          <thead><tr><th>Applicant</th><th>Product</th><th>Market</th><th>Status</th><th>Filed</th><th>Note</th></tr></thead>
          <tbody>{etf_html}</tbody>
        </table>
      </div>

      <div class="trk-tag" style="color:var(--tx);margin-bottom:8px">\u2696\uFE0F SEC Case Timeline</div>
      <div class="tl-wrap" style="margin-bottom:16px"><div class="tl-line"></div><div class="tl-track">{sec_tl_html}</div></div>

      <div class="trk-tag" style="color:var(--tx);margin-bottom:8px">\U0001F1EA\U0001F1FA MiCA Implementation</div>
      <div style="font-size:15px;color:var(--tx);line-height:1.7;font-family:system-ui;margin-bottom:10px;max-width:820px">
        MiCA (Markets in Crypto-Assets) is the EU's comprehensive crypto regulatory framework \u2014 the closest thing Europe has to
        a single rulebook for digital assets. It gives XRP formal status as a crypto-asset, not a security, across all 27
        member states. Here's how the rollout has progressed:
      </div>
      <div class="ud-panel" style="margin-bottom:16px">{mica_html}</div>

      <div class="trk-tag" style="color:var(--tx);margin-bottom:8px">\U0001F3E6 Central Bank / CBDC Projects on XRPL</div>
      <div class="cg-grid" style="grid-template-columns:repeat(3,1fr)">
        {cbdc_html}
      </div>
    </div>

    <!-- SECTION 26: CLARITY ACT TRACKER -->
    <div class="acct" style="border-color:rgba(255,153,0,.35);margin:10px 0">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;margin-bottom:6px">
        <div class="sec-title" style="color:var(--hdr);margin:0"><span class="sic">\U0001F3DB\uFE0F</span> CLARITY Act Tracker</div>
        <div style="text-align:right"><div class="pl-counter" style="color:var(--or)">{ca_count}/10</div>
          <div style="font-size:12px;color:var(--tx);font-family:var(--mn)">most recent stories</div></div>
      </div>
      <div style="font-size:15px;color:var(--tx);line-height:1.7;font-family:system-ui;margin-bottom:12px;max-width:900px">
        The Digital Asset Market Clarity Act (CLARITY Act) would split crypto oversight between the SEC and CFTC and is
        currently on the Senate calendar awaiting a floor vote. This tracker shows the 10 most recent stories on its
        progress \u2014 newest first, with the oldest dropping off automatically as fresh news breaks. Always current.
      </div>
      <div class="ca-list">
        {ca_html}
      </div>
    </div>

    <!-- SECTION 27: GLOBAL XRP ENTERPRISE & PARTNERSHIP LEDGER -->
    <div class="acct" style="border-color:rgba(255,204,0,.35);margin:10px 0">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;margin-bottom:6px">
        <div class="sec-title" style="color:var(--hdr);margin:0"><span class="sic">\U0001F310</span> Global XRP Enterprise &amp; Partnership Ledger</div>
        <div style="text-align:right"><div class="pl-counter">{pl_total}+</div><div style="font-size:12px;color:var(--tx);font-family:var(--mn)">institutions &amp; deals</div></div>
      </div>
      <div style="font-size:15px;color:var(--tx);line-height:1.7;font-family:system-ui;margin-bottom:12px;max-width:900px">
        An ever-growing record of banks, institutions, and enterprises using XRP, XRPL, or Ripple technology \u2014 from
        foundational partnerships to newly announced deals. New entries are detected automatically from the live news feed
        and added here permanently; nothing is ever removed. Newest announcements shown first.
      </div>
      <div class="feed-wrap">
        <div>
          <input class="pl-search" id="pl-search" type="text" placeholder="\U0001F50D Search institution, country, category..." oninput="filterPartnerships()">
      <div class="pl-cats" id="pl-cats">
        <button class="pl-btn active" data-cat="ALL" style="color:var(--br);border-color:var(--br)" onclick="plCat('ALL',this)">ALL</button>
        <button class="pl-btn" data-cat="A" style="color:var(--gr);border-color:var(--gr)" onclick="plCat('A',this)">\U0001F680 ODL/XRP Live</button>
        <button class="pl-btn" data-cat="B" style="color:var(--bl);border-color:var(--bl)" onclick="plCat('B',this)">\U0001F3DB\uFE0F Global Banks</button>
        <button class="pl-btn" data-cat="C" style="color:var(--tq);border-color:var(--tq)" onclick="plCat('C',this)">\U0001F6E0\uFE0F Tech/Custody</button>
        <button class="pl-btn" data-cat="D" style="color:var(--or);border-color:var(--or)" onclick="plCat('D',this)">\U0001F30D Regional</button>
        <button class="pl-btn" data-cat="E" style="color:var(--yl);border-color:var(--yl)" onclick="plCat('E',this)">\U0001F7E1 ETF/Treasury</button>
        <button class="pl-btn" data-cat="N" style="color:var(--yl);border-color:var(--yl)" onclick="plCat('N',this)">\U0001F195 New Deals</button>
      </div>
      <div class="pl-stats">
        <b id="pl-shown">{min(pl_total, 30)}</b> shown &nbsp;|&nbsp; <b>{pl_total}</b> total &nbsp;|&nbsp;
        <span style="color:var(--gr)">{pl_detected} newly detected</span>
      </div>
      <div class="pl-list" id="pl-list">
        {pl_html}
      </div>
      <div style="margin-top:10px;font-size:12px;color:var(--tx);font-family:var(--mn);opacity:.7">
        Baseline sources: Ripple.com partner listings, SEC filings, central bank announcements, verified corporate press
        releases. New entries are detected from the live news feed. Directory is for informational purposes; some
        partnerships may be pilots or historical integrations.
      </div>
        </div>
        <div class="sd-panel">
          <div class="sd-head">
            <span class="sd-title">\U0001F4D1 Global Partnership Directory</span>
            <span class="sd-count">{sd_count}+</span>
          </div>
          <div class="sd-sub">Curated master list of confirmed global partnerships &amp; contracts. Refreshes every 3 days. Updated {sd_updated}.</div>
          <div class="sd-list">
            {sd_html}
          </div>
        </div>
      </div>
    </div>

    <!-- SECTION 28: ADVANCED METRICS -->
    <div class="acct" style="border-color:rgba(0,229,204,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F52C</span> Advanced Metrics</div>
      <div class="trk-tag" style="color:var(--tx)">Technical indicators, order book depth, and reference specs \u2014 all computed from live, verifiable market data.</div>

      <div class="am-grid2" style="margin-bottom:10px">
        <div class="am-panel">
          <div class="am-title" style="color:var(--tq)">\u2699\uFE0F XRPL Technical Specs</div>
          <div class="am-sub">How XRPL compares on the metrics that matter for payments</div>
          <table class="pt-tbl">
            <thead><tr><th>Metric</th><th style="text-align:center;color:var(--gr)">XRPL</th>
              <th style="text-align:center;color:var(--bl)">ETH</th><th style="text-align:center;color:var(--or)">SOL</th>
              <th style="text-align:center;color:var(--tx)">BTC</th></tr></thead>
            <tbody>{ts_html}</tbody>
          </table>
        </div>
        <div class="am-panel">
          <div class="am-title" style="color:var(--or)">\U0001F4DA XRP Use Case Library</div>
          <div class="am-sub">Where XRP and XRPL are actually being used today</div>
          <div class="uc-list">{uc_html}</div>
        </div>
      </div>

      <div class="am-grid2" style="margin-bottom:10px">
        <div class="am-panel">
          <div class="am-title" style="color:var(--tq)">\U0001F4E6 Accumulation / Distribution</div>
          <div class="am-sub">Chaikin A/D Line \u2014 computed from price, volume, and daily range (no wallet tracking involved)</div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
            <div class="abox" style="border-left-color:var(--tq)"><div class="abox-lbl">7-Day Signal</div>
              <div class="abox-val" style="color:{ad_c7}">{ad_s7}</div></div>
            <div class="abox" style="border-left-color:var(--bl)"><div class="abox-lbl">30-Day Signal</div>
              <div class="abox-val" style="color:{ad_c30}">{ad_s30}</div></div>
          </div>
        </div>
        <div class="am-panel">
          <div class="am-title" style="color:var(--tq)">\U0001F522 XRP Correlation Matrix</div>
          <div class="am-sub">30-day return correlation (Pearson) \u2014 how closely XRP tracks each asset</div>
          {corr_html}
          <div style="margin-top:4px;font-size:12px;color:var(--tx);font-family:var(--mn)">
            +1.0 = moves identically \u00B7 0 = unrelated \u00B7 -1.0 = moves opposite
          </div>
        </div>
      </div>

      <div class="am-grid2">
        <div class="am-panel" style="grid-column:1/3">
          <div class="am-title" style="color:var(--gr)">\U0001F4CA XRP Order Book Depth</div>
          <div class="am-sub">Live bid/ask walls on Binance XRP/USDT \u2014 top 8 levels each side</div>
          {ob_body_html}
        </div>
      </div>
      <div class="am-panel" style="margin-top:10px">
        <div class="am-title" style="color:var(--tq)">\U0001F4A7 Liquidity Map</div>
        <div class="am-sub">Bid vs. ask value in the visible order book</div>
        {liq_html}
      </div>
    </div>

    <!-- SECTION 29: PRACTICAL TOOLS -->
    <div class="acct" style="border-color:rgba(0,229,204,.35);margin:10px 0">
      <div class="sec-title" style="color:var(--hdr)"><span class="sic">\U0001F6E0\uFE0F</span> Practical Tools</div>
      <div class="pt-cols">
        <div class="pt-col">
          <!-- P&L Calculator -->
          <div class="pt-panel" style="border-color:rgba(0,229,204,.25)">
            <div class="pt-head"><span class="pt-title" style="color:var(--tq)">\U0001F4B0 XRP P&amp;L Calculator</span></div>
            <div class="pt-body">
              <div class="pt-row2">
                <div><div class="pt-lbl">Buy Price (USD)</div>
                  <input id="pl-buy" class="pt-input" type="number" step="0.0001" placeholder="e.g. 0.50" oninput="calcPL()"></div>
                <div><div class="pt-lbl">Quantity (XRP)</div>
                  <input id="pl-qty" class="pt-input" type="number" step="1" placeholder="e.g. 10000" oninput="calcPL()"></div>
              </div>
              <div>
                <div class="pt-lbl">Sell / Target Price (USD)
                  <span class="pt-use-live" onclick="document.getElementById('pl-sell').value=currentXRPPrice.toFixed(4);calcPL()">[use live price]</span>
                </div>
                <input id="pl-sell" class="pt-input" type="number" step="0.0001" placeholder="e.g. 2.00" oninput="calcPL()">
              </div>
              <div id="pl-results" class="pt-results">
                <div class="pt-res-row"><span class="sm-k">Cost Basis</span><span class="sm-v" id="pl-cost">\u2014</span></div>
                <div class="pt-res-row"><span class="sm-k">Current / Target Value</span><span class="sm-v" id="pl-value">\u2014</span></div>
                <div class="pt-res-row"><span class="sm-k">P&amp;L (USD)</span><span id="pl-usd" style="font-weight:700;font-size:17px">\u2014</span></div>
                <div class="pt-res-row"><span class="sm-k">P&amp;L (%)</span><span id="pl-pct" style="font-weight:700;font-size:17px">\u2014</span></div>
              </div>
              <div class="pt-note">\u26A0\uFE0F Not financial advice. For informational purposes only.</div>
            </div>
          </div>

          <!-- Multi-Currency -->
          <div class="pt-panel" style="border-color:rgba(0,229,204,.2)">
            <div class="pt-head"><span class="pt-title" style="color:var(--tq)">\U0001F4B1 XRP Price \u2014 Multi-Currency</span><span class="pt-note">{fx_ts}</span></div>
            <div class="fx-grid">
              <div class="fx-box hi"><div class="fx-lbl">USD \U0001F1FA\U0001F1F8</div><div class="fx-val">${fx_usd_disp}</div></div>
              <div class="fx-box"><div class="fx-lbl">EUR \U0001F1EA\U0001F1FA</div><div class="fx-val">\u20AC{fx_eur}</div></div>
              <div class="fx-box"><div class="fx-lbl">GBP \U0001F1EC\U0001F1E7</div><div class="fx-val">\u00A3{fx_gbp}</div></div>
              <div class="fx-box"><div class="fx-lbl">JPY \U0001F1EF\U0001F1F5</div><div class="fx-val" style="font-size:17px">\u00A5{fx_jpy}</div></div>
              <div class="fx-box"><div class="fx-lbl">AUD \U0001F1E6\U0001F1FA</div><div class="fx-val">A${fx_aud}</div></div>
              <div class="fx-box"><div class="fx-lbl">CAD \U0001F1E8\U0001F1E6</div><div class="fx-val">C${fx_cad}</div></div>
              <div class="fx-box"><div class="fx-lbl">SGD \U0001F1F8\U0001F1EC</div><div class="fx-val">S${fx_sgd}</div></div>
              <div class="fx-box"><div class="fx-lbl">INR \U0001F1EE\U0001F1F3</div><div class="fx-val" style="font-size:17px">\u20B9{fx_inr}</div></div>
              <div class="fx-box"><div class="fx-lbl">BRL \U0001F1E7\U0001F1F7</div><div class="fx-val">R${fx_brl}</div></div>
              <div class="fx-box"><div class="fx-lbl">CHF \U0001F1E8\U0001F1ED</div><div class="fx-val">Fr{fx_chf}</div></div>
              <div class="fx-box"><div class="fx-lbl">CNY \U0001F1E8\U0001F1F3</div><div class="fx-val" style="font-size:17px">\u00A5{fx_cny}</div></div>
              <div class="fx-box"><div class="fx-lbl">KRW \U0001F1F0\U0001F1F7</div><div class="fx-val" style="font-size:17px">\u20A9{fx_krw}</div></div>
              <div class="fx-box"><div class="fx-lbl">MXN \U0001F1F2\U0001F1FD</div><div class="fx-val" style="font-size:17px">$MX{fx_mxn}</div></div>
              <div class="fx-box"><div class="fx-lbl">PHP \U0001F1F5\U0001F1ED</div><div class="fx-val" style="font-size:17px">\u20B1{fx_php}</div></div>
              <div class="fx-box"><div class="fx-lbl">NGN \U0001F1F3\U0001F1EC</div><div class="fx-val" style="font-size:17px">\u20A6{fx_ngn}</div></div>
              <div class="fx-box"><div class="fx-lbl">ZAR \U0001F1FF\U0001F1E6</div><div class="fx-val" style="font-size:17px">R{fx_zar}</div></div>
              <div class="fx-box"><div class="fx-lbl">AED \U0001F1E6\U0001F1EA</div><div class="fx-val" style="font-size:17px">AED{fx_aed}</div></div>
              <div class="fx-box"><div class="fx-lbl">SAR \U0001F1F8\U0001F1E6</div><div class="fx-val" style="font-size:17px">SAR{fx_sar}</div></div>
              <div class="fx-box"><div class="fx-lbl">HKD \U0001F1ED\U0001F1F0</div><div class="fx-val" style="font-size:17px">HK${fx_hkd}</div></div>
              <div class="fx-box"><div class="fx-lbl">NZD \U0001F1F3\U0001F1FF</div><div class="fx-val">NZ${fx_nzd}</div></div>
              <div class="fx-box"><div class="fx-lbl">SEK \U0001F1F8\U0001F1EA</div><div class="fx-val" style="font-size:17px">{fx_sek}kr</div></div>
              <div class="fx-box"><div class="fx-lbl">NOK \U0001F1F3\U0001F1F4</div><div class="fx-val" style="font-size:17px">{fx_nok}kr</div></div>
              <div class="fx-box"><div class="fx-lbl">TRY \U0001F1F9\U0001F1F7</div><div class="fx-val" style="font-size:17px">\u20BA{fx_try}</div></div>
              <div class="fx-box"><div class="fx-lbl">THB \U0001F1F9\U0001F1ED</div><div class="fx-val" style="font-size:17px">\u0E3F{fx_thb}</div></div>
              <div class="fx-box"><div class="fx-lbl">IDR \U0001F1EE\U0001F1E9</div><div class="fx-val" style="font-size:17px">Rp{fx_idr}</div></div>
              <div class="fx-box"><div class="fx-lbl">VND \U0001F1FB\U0001F1F3</div><div class="fx-val" style="font-size:17px">\u20AB{fx_vnd}</div></div>
              <div class="fx-box"><div class="fx-lbl">PLN \U0001F1F5\U0001F1F1</div><div class="fx-val" style="font-size:17px">z\u0142{fx_pln}</div></div>
            </div>
          </div>
        </div>

        <div class="pt-col">
          <!-- Escrow & Ripple Holdings Tracker -->
          <div class="pt-panel" style="border-color:rgba(117,188,255,.25)">
            <div class="pt-head"><span class="pt-title" style="color:var(--bl)">\U0001F512 Escrow &amp; Ripple Holdings</span></div>
            <div class="pt-body">
              <div class="pt-lbl">Ripple's Own XRP \u2014 Publicly Verifiable</div>
              <div style="background:var(--s2);border:1px solid rgba(117,188,255,.3);border-radius:6px;padding:10px;margin-top:6px">
                <div style="font-size:12px;color:var(--tx);margin-bottom:8px">Next scheduled release (1B XRP, 1st of month 00:00 UTC):</div>
                <div id="esc-countdown" data-eta="{esc_iso}" style="font-size:22px;font-weight:900;font-family:var(--mn);color:var(--bl);margin-bottom:8px">\u2014</div>
                <div style="display:flex;justify-content:space-between;font-size:12px;color:var(--tx);border-top:1px solid var(--b);padding-top:8px">
                  <span>Total in escrow</span><span style="color:var(--br);font-weight:700">~43B XRP</span>
                </div>
                <div style="display:flex;justify-content:space-between;font-size:12px;color:var(--tx);margin-top:4px">
                  <span>Circulating supply</span><span style="color:var(--br);font-weight:700">~62B XRP</span>
                </div>
                <div style="font-size:12px;color:var(--tx);margin-top:8px;font-style:italic">Escrow addresses are public and independently verifiable on-chain \u2014 this is Ripple's own locked supply, not a personal wallet lookup.</div>
              </div>
            </div>
          </div>

          <!-- Portfolio Tracker -->
          <div class="pt-panel" style="border-color:rgba(72,255,130,.2)">
            <div class="pt-head"><span class="pt-title" style="color:var(--gr)">\U0001F4C8 Portfolio Tracker</span><span class="pt-note">Session only</span></div>
            <div class="pt-body">
              <div style="display:grid;grid-template-columns:1fr 1fr 1fr auto;gap:6px">
                <input id="pt-label" class="pt-input" type="text" placeholder="Label (e.g. Wallet 1)">
                <input id="pt-amount" class="pt-input" type="number" placeholder="XRP amount">
                <input id="pt-cost" class="pt-input" type="number" placeholder="Avg buy price">
                <button class="pt-btn-gr" onclick="addPortfolioEntry()">+ ADD</button>
              </div>
              <div id="portfolio-table"><div style="font-size:15px;font-family:var(--mn);color:var(--tx)">No entries yet. Add a position above.</div></div>
              <div id="portfolio-totals" class="pt-results">
                <div class="pt-res-row"><span class="sm-k">Total XRP</span><span class="sm-v" id="pt-total-xrp">\u2014</span></div>
                <div class="pt-res-row"><span class="sm-k">Total Value</span><span class="sm-v" id="pt-total-val">\u2014</span></div>
                <div class="pt-res-row"><span class="sm-k">Total P&amp;L</span><span id="pt-total-pl" style="font-weight:700;font-size:15px">\u2014</span></div>
              </div>
              <div class="pt-note">\u26A0\uFE0F Session only \u2014 entries clear on page refresh. Not financial advice.</div>
            </div>
          </div>

          <!-- Remittance Calculator -->
          <div class="pt-panel" style="border-color:rgba(0,229,204,.25)">
            <div class="pt-head"><span class="pt-title" style="color:var(--tq)">\U0001F4B8 Remittance Calculator</span><span class="pt-note">SWIFT vs XRP</span></div>
            <div class="pt-body">
              <div class="pt-row2">
                <div><div class="pt-lbl">Send Amount (USD)</div>
                  <input id="rm-amount" class="pt-input" type="number" placeholder="e.g. 1000" oninput="calcRemittance()"></div>
                <div><div class="pt-lbl">Corridor</div>
                  <select id="rm-corridor" class="pt-select" onchange="calcRemittance()">
                    <option value="6.0">\U0001F1FA\U0001F1F8\u2192\U0001F1F2\U0001F1FD USA to Mexico (6%)</option>
                    <option value="7.5">\U0001F1FA\U0001F1F8\u2192\U0001F1F5\U0001F1ED USA to Philippines (7.5%)</option>
                    <option value="8.0">\U0001F1EC\U0001F1E7\u2192\U0001F1F3\U0001F1EC UK to Nigeria (8%)</option>
                    <option value="5.5">\U0001F1EF\U0001F1F5\u2192\U0001F1F5\U0001F1ED Japan to Philippines (5.5%)</option>
                    <option value="6.5">\U0001F1E6\U0001F1FA\u2192\U0001F1F5\U0001F1ED Australia to Philippines (6.5%)</option>
                    <option value="9.0">\U0001F1FA\U0001F1F8\u2192\U0001F1EE\U0001F1F3 USA to India (9%)</option>
                    <option value="7.0">\U0001F1EA\U0001F1FA\u2192\U0001F1F2\U0001F1FD Europe to Mexico (7%)</option>
                    <option value="5.0">\U0001F1F8\U0001F1EC\u2192\U0001F30F Singapore to SE Asia (5%)</option>
                  </select>
                </div>
              </div>
              <div id="rm-results" style="display:none">
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
                  <div class="rm-fee-box" style="background:rgba(255,64,96,.08);border:1px solid rgba(255,64,96,.3)">
                    <div class="pt-lbl" style="color:var(--rd)">SWIFT / Traditional</div>
                    <div style="font-size:22px;font-weight:900;font-family:var(--mn);color:var(--rd)" id="rm-swift-fee">\u2014</div>
                    <div class="pt-note">fee lost</div>
                    <div style="font-size:15px;font-family:var(--mn);color:var(--br);margin-top:6px;font-weight:700" id="rm-swift-recv">\u2014 received</div>
                    <div class="pt-note">\u23F1 1-5 business days</div>
                  </div>
                  <div class="rm-fee-box" style="background:rgba(72,255,130,.08);border:1px solid rgba(72,255,130,.3)">
                    <div class="pt-lbl" style="color:var(--gr)">XRP / XRPL ODL</div>
                    <div style="font-size:22px;font-weight:900;font-family:var(--mn);color:var(--gr)">$0.0002</div>
                    <div class="pt-note">fee lost</div>
                    <div style="font-size:15px;font-family:var(--mn);color:var(--br);margin-top:6px;font-weight:700" id="rm-xrp-recv">\u2014 received</div>
                    <div class="pt-note">\u26A1 3-5 seconds</div>
                  </div>
                </div>
                <div style="background:rgba(0,229,204,.08);border:1px solid rgba(0,229,204,.3);border-radius:6px;padding:10px;text-align:center;margin-top:8px">
                  <div class="pt-lbl" style="color:var(--tq)">XRP Saves You</div>
                  <div style="font-size:22px;font-weight:900;font-family:var(--mn);color:var(--tq)" id="rm-savings">\u2014</div>
                  <div class="pt-note" id="rm-xrp-needed">\u2014 XRP needed \u00B7 at live price</div>
                </div>
              </div>
              <div class="pt-note">\u26A0\uFE0F Traditional fees are averages. Actual rates vary by provider.</div>
            </div>
          </div>

          <!-- Break-Even / Target Price Calculator -->
          <div class="pt-panel" style="border-color:rgba(255,204,0,.25)">
            <div class="pt-head"><span class="pt-title" style="color:var(--yl)">\U0001F3AF Break-Even / Target Price</span><span class="pt-note">Solve for price, not profit</span></div>
            <div class="pt-body">
              <div class="pt-row2">
                <div><div class="pt-lbl">Buy Price (USD)</div>
                  <input id="bt-buy" class="pt-input" type="number" step="0.0001" placeholder="e.g. 0.50" oninput="calcBreakeven()"></div>
                <div><div class="pt-lbl">Quantity (XRP)</div>
                  <input id="bt-qty" class="pt-input" type="number" step="1" placeholder="e.g. 10000" oninput="calcBreakeven()"></div>
              </div>
              <div class="pt-row2">
                <div><div class="pt-lbl">Round-Trip Fee (%)</div>
                  <input id="bt-fee" class="pt-input" type="number" step="0.1" placeholder="e.g. 0.5" oninput="calcBreakeven()"></div>
                <div><div class="pt-lbl">Desired Return (%)</div>
                  <input id="bt-target" class="pt-input" type="number" step="1" placeholder="e.g. 50" oninput="calcBreakeven()"></div>
              </div>
              <div id="bt-results" class="pt-results" style="display:block">
                <div class="pt-res-row"><span class="sm-k">Break-Even Price</span><span class="sm-v" id="bt-breakeven">\u2014</span></div>
                <div class="pt-res-row"><span class="sm-k">Target Price</span><span class="sm-v" id="bt-target-price">\u2014</span></div>
                <div class="pt-res-row"><span class="sm-k">Profit at Target</span><span id="bt-target-profit" style="font-weight:700;font-size:17px">\u2014</span></div>
              </div>
              <div class="pt-note">\u26A0\uFE0F Not financial advice. Fee % covers combined buy + sell exchange costs.</div>
            </div>
          </div>

        </div>
      </div>
    </div>

  <!-- MAIN -->
    <!-- SECTION 30: XRPRADAR EXCLUSIVE INTELLIGENCE (flagship) -->
    <div class="acct" style="border-color:rgba(255,204,0,.4);margin:10px 0">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px">
        <div class="sec-title" style="color:var(--hdr);margin:0"><span class="sic">\U0001F3C6</span> XRPRadar Exclusive Intelligence</div>
        <div style="font-size:12px;color:var(--tx);font-family:var(--mn);padding-top:4px">Live as of {flagship_ts}</div>
      </div>
      <div class="flagship-intro">
        Metrics built entirely from data we track ourselves \u2014 our own growing partnership ledger, our own executive
        statement archive, our own GitHub monitoring, our own news timing history. Nothing here is copied from another
        site's API; it exists because XRPRadar has been watching and recording since deploy.
        <ul class="flagship-list">
          <li><b style="color:var(--yl)">Institutional Confidence Index</b> \u2014 one flagship score from five disclosed components.</li>
          <li><b style="color:var(--yl)">Partnership Momentum</b> \u2014 deals-per-week velocity from our own ledger.</li>
          <li><b style="color:var(--or)">Catalyst Clock</b> \u2014 when XRP-moving stories actually break, by hour and weekday.</li>
          <li><b style="color:var(--tq)">Narrative Diffusion Map</b> \u2014 how fast a theme spreads across regions.</li>
        </ul>
        Every chart below started empty at deploy and has been filling in honestly ever since \u2014 the longer XRPRadar runs, the sharper this section gets.
      </div>

      <div class="ici-wrap">
        <div class="ici-dial">
          <div class="ici-score" style="color:{ici_color}">{ici_score}</div>
          <div class="ici-cap">/ 100</div>
          <div class="ici-label" style="color:{ici_color}">{ici_label}</div>
          <div class="ici-bar"><div class="ici-fill" style="width:{ici_score}%"></div></div>
        </div>
        <div class="ici-comps">
          {ici_comps_rendered}
        </div>
      </div>
      <div class="ici-foot">
        \U0001F3C6 XRPRadar Institutional Confidence Index (ICI) \u2014 rescaled from five disclosed components: Partnership
        Momentum (our Enterprise Ledger), Developer Activity (live GitHub tracking), Smart Money Positioning (RSI +
        sentiment + funding rate), Executive Tone (sentiment across real Ripple leadership statements), and Regulatory
        Momentum (CLARITY Act coverage + Legal/Reg news sentiment). Each component is shown above with its real
        underlying value \u2014 nothing is a black box. Informational only, not financial advice.
      </div>

      <div class="pm-panel">
        <div class="pm-title">\U0001F4C8 Partnership Momentum</div>
        <div class="pm-sub">New deals detected per week, straight from our own Enterprise Ledger \u2014 builds up day by day, nothing fabricated.</div>
        <div class="pm-stats">
          <div><div class="pm-stat-num" style="color:var(--yl)">{pm_total}</div><div class="pm-stat-lbl">Total Detected</div></div>
          <div><div class="pm-stat-num" style="color:var(--gr)">{pm_this_week}</div><div class="pm-stat-lbl">This Week</div></div>
          <div><div class="pm-stat-num" style="color:{pm_tcol};font-size:15px;padding-top:4px">{pm_trend}</div><div class="pm-stat-lbl">Trend</div></div>
          <div><div class="pm-stat-num" style="color:var(--bl)">{pm_avg}</div><div class="pm-stat-lbl">Avg / Week</div></div>
        </div>
        <div class="pm-chart">{pm_bars}</div>
        <div class="pm-axis"><span>10 weeks ago</span><span>5 weeks ago</span><span>this week</span></div>
      </div>

      <div class="cc-panel">
        <div class="cc-title">\u23F0 Catalyst Clock</div>
        <div class="cc-sub">When XRP-moving stories actually break \u2014 hour (UTC) \u00D7 weekday, built from our own breaking-story history since deploy.</div>
        <div class="cc-peak">Peak so far: <b>{cc_peak}</b> &nbsp;|&nbsp; {cc_total} breaking stories tracked</div>
        <div class="cc-grid">
          {cc_cells}
          <div class="cc-hourlbls">{cc_hourlbls}</div>
        </div>
        <div class="cc-scrollnote">Darker = more breaking stories at that hour \u00B7 scroll horizontally on small screens</div>
      </div>

      <div class="nd-panel">
        <div class="nd-title">\U0001F30D Narrative Diffusion Map</div>
        <div class="nd-sub">How fast a story theme spreads from its first mention to full regional coverage \u2014 tracked from our own news timing history.</div>
        <div class="nd-fastest">Fastest spread so far: <b>{nd_fastest}</b></div>
        <div class="nd-list">
          {nd_cards}
        </div>
      </div>
    </div>

  <!-- REGULATORY & LEDGER WATCH (V66) -->
    <div class="acct" style="border-color:rgba(0,229,204,.4);margin:10px 0">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px">
        <div class="sec-title" style="color:var(--hdr);margin:0"><span class="sic">\U0001F4E1</span> Regulatory &amp; Ledger Watch</div>
        <div style="font-size:12px;color:var(--tx);font-family:var(--mn);padding-top:4px">Updated: {rw_updated}</div>
      </div>
      <div style="font-size:12px;color:var(--tx);margin-bottom:14px;line-height:1.6">
        Direct-from-source monitoring: XRPL protocol amendments in validator voting, official SEC filings mentioning Ripple/XRP, and live US federal rulemaking on digital assets. Government and ledger-level sources only.
      </div>
      <div class="rw-wrap">
        <div class="rw-panel">
          <div class="rw-panel-title">\u2699\uFE0F XRPL Amendment Tracker</div>
          <div class="rw-panel-sub">Protocol changes currently in validator voting \u2014 the earliest possible signal of XRPL evolution. Source: XRPScan.</div>
          {rw_amendments}
        </div>
        <div class="rw-panel">
          <div class="rw-panel-title">\U0001F4C4 SEC EDGAR Filing Watch</div>
          <div class="rw-panel-sub">Official SEC filings mentioning Ripple or XRP \u2014 straight from the source, before the press writes about them.</div>
          {rw_edgar}
        </div>
        <div class="rw-panel">
          <div class="rw-panel-title">\U0001F3DB\uFE0F Federal Register Rule Watch</div>
          <div class="rw-panel-sub">Proposed and final US federal rules on digital assets \u2014 the regulatory pipeline, direct from the Federal Register.</div>
          {rw_fedreg}
        </div>
      </div>
    </div>

  <!-- XRP COMMUNITY HUB (V67) -->
    <div class="acct" style="border-color:rgba(0,229,204,.4);margin:10px 0 40px 0">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px">
        <div class="sec-title" style="color:var(--hdr);margin:0"><span class="sic">\U0001F465</span> XRP Community Hub</div>
      </div>
      <div style="font-size:12px;color:var(--tx);margin-bottom:14px;line-height:1.6">
        The top 20 XRP-dedicated blogs, newsletters, social accounts, and forums \u2014 curated for signal over noise. External links open in a new tab.
      </div>
      <div class="cm-wrap">
        <div class="cm-panel">
          <div class="cm-panel-title">\U0001F4DD Blogs &amp; News Sites</div>
          <div class="cm-item"><a class="cm-link" href="https://u.today/xrp-news" target="_blank" rel="noopener">U.Today XRP</a><div class="cm-desc">High-volume dedicated XRP news desk</div></div>
          <div class="cm-item"><a class="cm-link" href="https://www.thecryptobasic.com/category/xrp-news/" target="_blank" rel="noopener">The Crypto Basic \u2014 XRP</a><div class="cm-desc">XRP-heavy coverage, community favorite</div></div>
          <div class="cm-item"><a class="cm-link" href="https://xrpl.org/blog/" target="_blank" rel="noopener">XRPL.org Blog</a><div class="cm-desc">Official ledger development blog</div></div>
          <div class="cm-item"><a class="cm-link" href="https://ripple.com/insights/" target="_blank" rel="noopener">Ripple Insights</a><div class="cm-desc">Official Ripple company blog</div></div>
          <div class="cm-item"><a class="cm-link" href="https://coinpost.jp/?s=XRP" target="_blank" rel="noopener">CoinPost Japan (XRP)</a><div class="cm-desc">Japan\u2019s largest crypto outlet \u2014 XRP focus</div></div>
        </div>
        <div class="cm-panel">
          <div class="cm-panel-title">\U0001F4E7 Newsletters &amp; Research</div>
          <div class="cm-item"><a class="cm-link" href="https://xrplf.org/" target="_blank" rel="noopener">XRPL Foundation Updates</a><div class="cm-desc">Ledger foundation announcements</div></div>
          <div class="cm-item"><a class="cm-link" href="https://dev.to/t/xrpl" target="_blank" rel="noopener">XRPL Dev Community</a><div class="cm-desc">Developer tutorials and build logs</div></div>
          <div class="cm-item"><a class="cm-link" href="https://xrpscan.com/" target="_blank" rel="noopener">XRPScan</a><div class="cm-desc">Ledger explorer + weekly metrics</div></div>
          <div class="cm-item"><a class="cm-link" href="https://bithomp.com/" target="_blank" rel="noopener">Bithomp</a><div class="cm-desc">Explorer, rich lists, escrow tracking</div></div>
          <div class="cm-item"><a class="cm-link" href="https://xrpl.services/" target="_blank" rel="noopener">XRPL Services</a><div class="cm-desc">Community tools and ledger utilities</div></div>
        </div>
        <div class="cm-panel">
          <div class="cm-panel-title">\U0001F4F1 Social Accounts</div>
          <div class="cm-item"><a class="cm-link" href="https://x.com/Ripple" target="_blank" rel="noopener">@Ripple</a><div class="cm-desc">Official Ripple company account</div></div>
          <div class="cm-item"><a class="cm-link" href="https://x.com/bgarlinghouse" target="_blank" rel="noopener">@bgarlinghouse</a><div class="cm-desc">Brad Garlinghouse \u2014 Ripple CEO</div></div>
          <div class="cm-item"><a class="cm-link" href="https://x.com/JoelKatz" target="_blank" rel="noopener">@JoelKatz</a><div class="cm-desc">David Schwartz \u2014 Ripple CTO, XRPL architect</div></div>
          <div class="cm-item"><a class="cm-link" href="https://x.com/XRPLF" target="_blank" rel="noopener">@XRPLF</a><div class="cm-desc">XRP Ledger Foundation</div></div>
          <div class="cm-item"><a class="cm-link" href="https://x.com/WietseWind" target="_blank" rel="noopener">@WietseWind</a><div class="cm-desc">Xaman (XUMM) wallet founder, XRPL builder</div></div>
        </div>
        <div class="cm-panel">
          <div class="cm-panel-title">\U0001F4AC Forums &amp; Communities</div>
          <div class="cm-item"><a class="cm-link" href="https://www.reddit.com/r/XRP/" target="_blank" rel="noopener">r/XRP</a><div class="cm-desc">Largest XRP subreddit</div></div>
          <div class="cm-item"><a class="cm-link" href="https://www.reddit.com/r/Ripple/" target="_blank" rel="noopener">r/Ripple</a><div class="cm-desc">Ripple company + ecosystem discussion</div></div>
          <div class="cm-item"><a class="cm-link" href="https://www.xrpchat.com/" target="_blank" rel="noopener">XRPChat</a><div class="cm-desc">Longest-running dedicated XRP forum</div></div>
          <div class="cm-item"><a class="cm-link" href="https://discord.com/invite/xrpl" target="_blank" rel="noopener">XRPL Developers Discord</a><div class="cm-desc">Official developer community chat</div></div>
          <div class="cm-item"><a class="cm-link" href="https://stackoverflow.com/questions/tagged/xrp" target="_blank" rel="noopener">Stack Overflow \u2014 XRP</a><div class="cm-desc">Technical Q&amp;A for XRPL builders</div></div>
        </div>
      </div>
    </div>

  </div>

  <!-- FLOATING RETURN / BACK-TO-TOP -->
  <button id="back-to-top" title="Return to XRPRadar" aria-label="Return to XRPRadar">&#8679;</button>

  <!-- FOOTER -->
  <footer>
    <div class="f-line">
      \U0001F6F0\uFE0F <em class="brand-em">{APP_NAME}</em>
      &nbsp;|&nbsp; Version: <span class="val">{APP_VERSION}</span>
      &nbsp;|&nbsp; Updated: <span class="val" id="ft-last">{boot_str}</span>
      &nbsp;|&nbsp; Uptime: <span class="val" id="ft-uptime">0s</span>
      <a class="footer-btn debug-btn" href="/debug" target="_blank" rel="noopener">DEBUG</a>
    </div>
    <div class="f-line notice">
      \u26A0\uFE0F Not Financial Advice \u2014 XRPRadar is for informational purposes only. DYOR.
    </div>
    <div class="f-line">
      Feeds: <span class="val" id="ft-feeds">{NEWS["feeds_active"]}/{NEWS["feeds_total"]}</span>
      &nbsp;|&nbsp; Maintenance: <span class="val" id="ft-maint">None</span>
      &nbsp;|&nbsp; Preflight: <span style="color:{overall_color};font-weight:800" id="ft-qa">{overall}</span>
      <button class="footer-btn details-btn" onclick="openPFModal()">\U0001F50D DETAILS</button>
    </div>
    <div class="f-line copyright">{COPYRIGHT}</div>
  </footer>

  <!-- PREFLIGHT DETAILS MODAL -->
  <div id="pf-modal" onclick="closePFModal(event)">
    <div id="pf-box" onclick="event.stopPropagation()">
      <div class="pf-head">
        <span class="t">\U0001F50D Preflight / QA Details</span>
        <span class="x" onclick="closePFModal()">\u2715</span>
      </div>
      <div class="pf-body">
        <div class="pf-overall">OVERALL: {overall} &nbsp;({passed}/{total} checks passed)</div>
        {modal_rows}
        <div style="margin-top:10px;color:var(--tx);font-size:12px">Last run: {boot_str}</div>
      </div>
    </div>
  </div>

  <script>
    (function () {{
      var bootMs = {int(BOOT_TIME.timestamp() * 1000)};
      var el = document.getElementById('ft-uptime');
      function tick() {{
        if (!el) return;
        var s = Math.floor((Date.now() - bootMs) / 1000);
        var h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
        el.textContent = (h ? h + 'h ' : '') + (m ? m + 'm ' : '') + sec + 's';
      }}
      tick(); setInterval(tick, 1000);
    }})();

    function openPFModal() {{ var m = document.getElementById('pf-modal'); if (m) m.style.display = 'flex'; }}
    function closePFModal() {{ var m = document.getElementById('pf-modal'); if (m) m.style.display = 'none'; }}
    document.addEventListener('keydown', function (e) {{ if (e.key === 'Escape') closePFModal(); }});

    // XRP Intelligence Brief — This Week's Editions (client-side swap, never reloads)
    var briefLiveSlot = {json.dumps(_live_slot)};
    var brfNextGlobal = {json.dumps(brf_next)};
    var briefArchive = {{}};
    try {{
      var _bd = document.getElementById('brief-archive-data');
      briefArchive = _bd ? JSON.parse(_bd.textContent) : {{}};
    }} catch (e) {{ briefArchive = {{}}; }}

    // Single-edition mode: only the current brief is shown (rendered server-side).
    // No edition switching, so no client-side brief loader is needed.

    // Practical Tools — client-side calculators (never block the page load)
    var currentXRPPrice = {xrp_price_js};

    function calcPL() {{
      var buy = parseFloat((document.getElementById('pl-buy') || {{}}).value || 0);
      var qty = parseFloat((document.getElementById('pl-qty') || {{}}).value || 0);
      var sell = parseFloat((document.getElementById('pl-sell') || {{}}).value || 0);
      var res = document.getElementById('pl-results');
      if (!buy || !qty || !sell || !res) return;
      var cost = buy * qty, value = sell * qty, plUSD = value - cost;
      var plPct = ((sell - buy) / buy) * 100;
      var isPos = plUSD >= 0, col = isPos ? 'var(--gr)' : 'var(--rd)', sign = isPos ? '+' : '';
      res.style.display = 'block';
      document.getElementById('pl-cost').textContent = '$' + cost.toLocaleString('en-US', {{minimumFractionDigits:2,maximumFractionDigits:2}});
      document.getElementById('pl-value').textContent = '$' + value.toLocaleString('en-US', {{minimumFractionDigits:2,maximumFractionDigits:2}});
      var u = document.getElementById('pl-usd');
      u.textContent = sign + '$' + Math.abs(plUSD).toLocaleString('en-US', {{minimumFractionDigits:2,maximumFractionDigits:2}});
      u.style.color = col;
      var p = document.getElementById('pl-pct');
      p.textContent = sign + plPct.toFixed(2) + '%';
      p.style.color = col;
    }}

    var portfolioEntries = [];
    function addPortfolioEntry() {{
      var label = ((document.getElementById('pt-label') || {{}}).value || '').trim() || ('Entry ' + (portfolioEntries.length + 1));
      var amount = parseFloat((document.getElementById('pt-amount') || {{}}).value || 0);
      var cost = parseFloat((document.getElementById('pt-cost') || {{}}).value || 0);
      if (!amount || amount <= 0) {{ alert('Enter a valid XRP amount'); return; }}
      portfolioEntries.push({{label: label, amount: amount, cost: cost, id: Date.now()}});
      ['pt-label', 'pt-amount', 'pt-cost'].forEach(function(id) {{
        var el = document.getElementById(id); if (el) el.value = '';
      }});
      renderPortfolio();
    }}
    function removePortfolioEntry(id) {{
      portfolioEntries = portfolioEntries.filter(function(e) {{ return e.id !== id; }});
      renderPortfolio();
    }}
    function renderPortfolio() {{
      var tableEl = document.getElementById('portfolio-table');
      var totalsEl = document.getElementById('portfolio-totals');
      if (!tableEl) return;
      if (!portfolioEntries.length) {{
        tableEl.innerHTML = '<div style="font-size:15px;font-family:var(--mn);color:var(--tx)">No entries yet. Add a position above.</div>';
        if (totalsEl) totalsEl.style.display = 'none';
        return;
      }}
      var totalXRP = 0, totalVal = 0, totalCost = 0;
      var rows = '';
      for (var i = 0; i < portfolioEntries.length; i++) {{
        var e = portfolioEntries[i];
        var val = e.amount * currentXRPPrice, cost = e.cost * e.amount, pl = val - cost;
        var pct = e.cost > 0 ? ((currentXRPPrice - e.cost) / e.cost * 100) : 0;
        var col = pl >= 0 ? 'var(--gr)' : 'var(--rd)', sign = pl >= 0 ? '+' : '';
        totalXRP += e.amount; totalVal += val; totalCost += cost;
        rows += '<tr><td>' + e.label + '</td><td>' + e.amount.toLocaleString() + '</td>' +
          '<td>$' + e.cost.toFixed(4) + '</td>' +
          '<td style="color:var(--bl);font-weight:700">$' + val.toLocaleString(undefined,{{minimumFractionDigits:2,maximumFractionDigits:2}}) + '</td>' +
          '<td style="color:' + col + ';font-weight:700">' + sign + '$' + Math.abs(pl).toLocaleString(undefined,{{minimumFractionDigits:2,maximumFractionDigits:2}}) + '</td>' +
          '<td style="color:' + col + '">' + sign + pct.toFixed(1) + '%</td>' +
          '<td><span class="pt-x" onclick="removePortfolioEntry(' + e.id + ')">\u2715</span></td></tr>';
      }}
      tableEl.innerHTML = '<table class="pt-tbl"><thead><tr><th>Label</th><th>XRP</th><th>Buy $</th><th>Value</th><th>P&amp;L</th><th>%</th><th></th></tr></thead><tbody>' + rows + '</tbody></table>';
      var totalPL = totalVal - totalCost;
      var tCol = totalPL >= 0 ? 'var(--gr)' : 'var(--rd)', tSign = totalPL >= 0 ? '+' : '';
      document.getElementById('pt-total-xrp').textContent = totalXRP.toLocaleString();
      document.getElementById('pt-total-val').textContent = '$' + totalVal.toLocaleString(undefined,{{minimumFractionDigits:2,maximumFractionDigits:2}});
      var tplEl = document.getElementById('pt-total-pl');
      tplEl.textContent = tSign + '$' + Math.abs(totalPL).toLocaleString(undefined,{{minimumFractionDigits:2,maximumFractionDigits:2}});
      tplEl.style.color = tCol;
      if (totalsEl) totalsEl.style.display = 'block';
    }}

    function calcRemittance() {{
      var amount = parseFloat((document.getElementById('rm-amount') || {{}}).value || 0);
      var corridor = parseFloat((document.getElementById('rm-corridor') || {{}}).value || 6.0);
      var res = document.getElementById('rm-results');
      if (!amount || amount <= 0 || !res) return;
      var swiftFee = amount * (corridor / 100), swiftRecv = amount - swiftFee;
      var xrpFee = 0.0002, xrpRecv = amount - xrpFee, savings = swiftFee - xrpFee;
      var xrpNeeded = currentXRPPrice > 0 ? (amount / currentXRPPrice).toFixed(2) : '--';
      var fmt = function(v) {{ return '$' + v.toLocaleString('en-US', {{minimumFractionDigits:2,maximumFractionDigits:2}}); }};
      document.getElementById('rm-swift-fee').textContent = fmt(swiftFee);
      document.getElementById('rm-swift-recv').textContent = fmt(swiftRecv) + ' received';
      document.getElementById('rm-xrp-recv').textContent = fmt(xrpRecv) + ' received';
      document.getElementById('rm-savings').textContent = fmt(savings);
      document.getElementById('rm-xrp-needed').textContent = xrpNeeded + ' XRP needed \u00B7 at live price';
      res.style.display = 'block';
    }}

    // Break-Even / Target Price Calculator — pure client-side math, no network calls
    function calcBreakeven() {{
      var buy = parseFloat((document.getElementById('bt-buy') || {{}}).value || 0);
      var qty = parseFloat((document.getElementById('bt-qty') || {{}}).value || 0);
      var feePct = parseFloat((document.getElementById('bt-fee') || {{}}).value || 0);
      var targetPct = parseFloat((document.getElementById('bt-target') || {{}}).value || 0);
      var beEl = document.getElementById('bt-breakeven');
      var tpEl = document.getElementById('bt-target-price');
      var profEl = document.getElementById('bt-target-profit');
      if (!buy || buy <= 0) {{
        if (beEl) beEl.textContent = '\u2014';
        if (tpEl) tpEl.textContent = '\u2014';
        if (profEl) profEl.textContent = '\u2014';
        return;
      }}
      var fee = feePct / 100;
      var breakeven = (fee > 0 && fee < 1) ? (buy * (1 + fee)) / (1 - fee) : buy;
      var hasTarget = targetPct !== 0 && !isNaN(targetPct);
      var targetPrice = hasTarget ? buy * (1 + targetPct / 100) : null;
      if (beEl) beEl.textContent = '$' + breakeven.toFixed(4);
      if (tpEl) tpEl.textContent = targetPrice !== null ? '$' + targetPrice.toFixed(4) : '\u2014';
      if (profEl) {{
        if (targetPrice !== null && qty > 0) {{
          var profit = (targetPrice - buy) * qty;
          profEl.textContent = '$' + profit.toLocaleString('en-US', {{minimumFractionDigits:2,maximumFractionDigits:2}});
          profEl.style.color = profit >= 0 ? 'var(--gr)' : 'var(--rd)';
        }} else {{
          profEl.textContent = '\u2014';
          profEl.style.color = 'var(--br)';
        }}
      }}
    }}
    function wcTick() {{
      var now = new Date();
      var clocks = document.querySelectorAll('.wc-clock');
      for (var i = 0; i < clocks.length; i++) {{
        var el = clocks[i];
        var tz = el.getAttribute('data-tz');
        var hh = 0, mm = 0, ss = now.getSeconds();
        try {{
          var parts = new Intl.DateTimeFormat('en-GB', {{
            timeZone: tz, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
          }}).formatToParts(now);
          for (var j = 0; j < parts.length; j++) {{
            if (parts[j].type === 'hour') hh = parseInt(parts[j].value, 10);
            else if (parts[j].type === 'minute') mm = parseInt(parts[j].value, 10);
            else if (parts[j].type === 'second') ss = parseInt(parts[j].value, 10);
          }}
          if (hh === 24) hh = 0;
        }} catch (e) {{ hh = now.getUTCHours(); mm = now.getUTCMinutes(); }}
        var day = (hh >= 6 && hh < 18);
        el.classList.toggle('wc-day', day);
        var hr = el.querySelector('.wc-hr'), mn = el.querySelector('.wc-min'), sc = el.querySelector('.wc-sec');
        if (hr) hr.style.transform = 'rotate(' + (((hh % 12) * 30) + (mm * 0.5)) + 'deg)';
        if (mn) mn.style.transform = 'rotate(' + (mm * 6) + 'deg)';
        if (sc) sc.style.transform = 'rotate(' + (ss * 6) + 'deg)';
      }}
    }}
    setInterval(wcTick, 1000);
    wcTick();

    // Partnership Tracker status filter (Mainstream Integration Monitor buttons)
    function filterTracker(status, btn) {{
      var cards = document.querySelectorAll('.trk-card');
      var visible = 0;
      for (var i = 0; i < cards.length; i++) {{
        var show = (status === 'ALL' || cards[i].getAttribute('data-status') === status);
        cards[i].style.display = show ? '' : 'none';
        if (show) visible++;
      }}
      var empty = document.getElementById('trk-empty');
      if (empty) empty.style.display = (visible === 0) ? 'block' : 'none';
      var btns = document.querySelectorAll('.trk-btn');
      for (var j = 0; j < btns.length; j++) btns[j].classList.remove('active');
      if (btn) btn.classList.add('active');
    }}

    // Global News Feed — search + category filter (client-side, never blocks)
    var _feedCat = 'ALL';
    function _applyFeed() {{
      var q = (document.getElementById('gn-search') || {{}}).value || '';
      q = q.toLowerCase().trim();
      var cards = document.querySelectorAll('#gn-list .gn-card');
      var shown = 0;
      for (var i = 0; i < cards.length; i++) {{
        var okCat = (_feedCat === 'ALL') || (cards[i].getAttribute('data-cat') === _feedCat);
        var okQ = !q || (cards[i].getAttribute('data-text') || '').indexOf(q) !== -1;
        var vis = okCat && okQ;
        cards[i].style.display = vis ? '' : 'none';
        if (vis) shown++;
      }}
      var sh = document.getElementById('gn-shown'); if (sh) sh.textContent = shown;
      var em = document.getElementById('gn-empty'); if (em) em.style.display = shown === 0 ? 'block' : 'none';
    }}
    function filterFeed() {{ _applyFeed(); }}
    function feedCat(cat, btn) {{
      _feedCat = cat;
      var btns = document.querySelectorAll('#gn-cats .gn-btn');
      for (var j = 0; j < btns.length; j++) btns[j].classList.remove('active');
      if (btn) btn.classList.add('active');
      _applyFeed();
    }}

    // Ripple Exec Tracker — tab filter (client-side, never blocks)
    function execTab(tab, btn) {{
      var rows = document.querySelectorAll('#ex-feed .ex-row');
      for (var i = 0; i < rows.length; i++) {{
        rows[i].style.display = (tab === 'ALL' || rows[i].getAttribute('data-tab') === tab) ? '' : 'none';
      }}
      var tabs = document.querySelectorAll('#ex-tabs .ex-tab');
      for (var j = 0; j < tabs.length; j++) tabs[j].classList.remove('on');
      if (btn) btn.classList.add('on');
    }}

    // Global XRP Enterprise & Partnership Ledger — search + category filter
    var _plCat = 'ALL';
    function _applyPl() {{
      var q = ((document.getElementById('pl-search') || {{}}).value || '').toLowerCase().trim();
      var rows = document.querySelectorAll('#pl-list .pl-row');
      var shown = 0;
      for (var i = 0; i < rows.length; i++) {{
        var okCat = (_plCat === 'ALL') || (rows[i].getAttribute('data-cat') === _plCat);
        var okQ = !q || (rows[i].getAttribute('data-text') || '').indexOf(q) !== -1;
        var vis = okCat && okQ;
        rows[i].style.display = vis ? '' : 'none';
        if (vis) shown++;
      }}
      var sh = document.getElementById('pl-shown'); if (sh) sh.textContent = shown;
    }}
    function filterPartnerships() {{ _applyPl(); }}
    function plCat(cat, btn) {{
      _plCat = cat;
      var btns = document.querySelectorAll('#pl-cats .pl-btn');
      for (var j = 0; j < btns.length; j++) btns[j].classList.remove('active');
      if (btn) btn.classList.add('active');
      _applyPl();
    }}

    // Next Briefing countdown (ticks live, hours/minutes)
    (function () {{
      var target = new Date("{brf_next_iso}").getTime();
      var el = document.getElementById('brf-countdown');
      function tickBrf() {{
        if (!el) return;
        var diff = target - Date.now();
        if (diff < 0) diff = 0;
        var h = Math.floor(diff / 3600000);
        var m = Math.floor((diff % 3600000) / 60000);
        el.textContent = h + 'h ' + ('0' + m).slice(-2) + 'm';
      }}
      tickBrf(); setInterval(tickBrf, 1000 * 15);
    }})();

    // Escrow countdown (to next 1st-of-month, 00:00 UTC)
    (function () {{
      var target = new Date("{esc_iso}").getTime();
      function tickEsc() {{
        var diff = target - Date.now();
        if (diff < 0) diff = 0;
        var d = Math.floor(diff / 86400000);
        var h = Math.floor((diff % 86400000) / 3600000);
        var m = Math.floor((diff % 3600000) / 60000);
        var ds = document.getElementById('esc-days');
        var hs = document.getElementById('esc-hrs');
        var ms = document.getElementById('esc-min');
        if (ds) ds.textContent = d;
        if (hs) hs.textContent = ('0' + h).slice(-2);
        if (ms) ms.textContent = ('0' + m).slice(-2);
      }}
      tickEsc(); setInterval(tickEsc, 1000 * 30);
    }})();

    // Practical Tools escrow countdown (same target time, separate display)
    (function () {{
      var el = document.getElementById('esc-countdown');
      if (!el) return;
      var target = new Date(el.getAttribute('data-eta')).getTime();
      function tick() {{
        var diff = target - Date.now();
        if (diff < 0) diff = 0;
        var d = Math.floor(diff / 86400000);
        var h = Math.floor((diff % 86400000) / 3600000);
        var m = Math.floor((diff % 3600000) / 60000);
        el.textContent = d + 'd ' + ('0'+h).slice(-2) + 'h ' + ('0'+m).slice(-2) + 'm';
      }}
      tick(); setInterval(tick, 1000 * 30);
    }})();

    (function () {{
      var btn = document.getElementById('back-to-top'); if (!btn) return;
      function toggle() {{ btn.style.display = (window.scrollY > 120 || document.documentElement.scrollTop > 120) ? 'flex' : 'none'; }}
      window.addEventListener('scroll', toggle, {{ passive:true }});
      document.addEventListener('scroll', toggle, {{ passive:true }});
      window.addEventListener('pageshow', toggle);       // fires on back/forward-cache restore (mobile Safari, etc.)
      document.addEventListener('visibilitychange', function () {{ if (!document.hidden) toggle(); }});
      btn.addEventListener('click', function () {{ window.scrollTo({{ top:0, behavior:'smooth' }}); }});
      toggle();
      setTimeout(toggle, 400);   // safety re-check after late layout shifts (widgets, images loading)
      setInterval(toggle, 2000); // low-frequency safety net in case a scroll event is ever missed
    }})();
  </script>

</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────
@app.route("/")
def home():
    return Response(render_page(), mimetype="text/html")


@app.route("/ping")
def ping():
    return jsonify({"status": "ok", "version": APP_VERSION})


# ─────────────────────────────────────────────────────────────────────
# COPYRIGHT ARCHIVE — PERMANENT, DO NOT MODIFY OR REMOVE THIS ROUTE.
# Serves a static, pre-rendered HTML snapshot captured July 4, 2026 (V56)
# for copyright documentation. This route must NEVER call render_page()
# or reference any live MARKET/NEWS/etc. data. It reads one static file
# and returns it verbatim, unchanged, regardless of any future edits
# made elsewhere in this app. Not linked from any nav/footer. Hidden via
# noindex meta tag (baked into the file itself) and via robots.txt below.
# ─────────────────────────────────────────────────────────────────────
_COPYRIGHT_ARCHIVE_FILE = "copyright_archive_2026_07_04.html"
_COPYRIGHT_ARCHIVE_FILE_B = "copyright_archive_2026_07_07_b.html"

@app.route("/copyright7_26")
def copyright_archive_2026_07_04():
    try:
        with open(_COPYRIGHT_ARCHIVE_FILE, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return "Archive temporarily unavailable.", 503


@app.route("/copyright7_26_b")
def copyright_archive_2026_07_07_b():
    # Second, independent dated snapshot (captured 2026-07-07, V95). The original
    # /copyright7_26 snapshot (2026-07-04, V63) is untouched and remains the earliest
    # dated proof of authorship; this route adds a second, later dated proof point.
    try:
        with open(_COPYRIGHT_ARCHIVE_FILE_B, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return "Archive temporarily unavailable.", 503


@app.route("/robots.txt")
def robots_txt():
    return (
        "User-agent: *\n"
        "Disallow: /copyright7_26\n"
        "Disallow: /copyright7_26_b\n"
    ), 200, {"Content-Type": "text/plain"}


@app.route("/debug")
def debug():
    checks, passed, total, overall = run_preflight()
    uptime = int((datetime.now(timezone.utc) - BOOT_TIME).total_seconds())
    return jsonify({
        "app":           APP_NAME,
        "version":       APP_VERSION,
        "iteration":     3,
        "preflight":     overall,
        "checks_passed": f"{passed}/{total}",
        "market": {
            "xrp_price":      MARKET["xrp_price"],
            "xrp_chg":        MARKET["xrp_chg"],
            "fng":            MARKET["fng"],
            "fng_label":      MARKET["fng_label"],
            "sources_active": MARKET["sources_active"],
            "sources_total":  MARKET["sources_total"],
            "updated":        MARKET["updated"],
        },
        "checks": [
            {"label": label, "status": "PASS" if ok else "FAIL", "detail": detail}
            for label, ok, detail in checks
        ],
        "uptime_secs":   uptime,
        "booted_utc":    BOOT_TIME.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "now_utc":       datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    })


try:
    fetch_market()
except Exception:
    pass

try:
    seed_partnership_ledger()
except Exception:
    pass

try:
    fetch_fx()
except Exception:
    pass

try:
    fetch_competitors()
except Exception:
    pass

try:
    fetch_news()
except Exception:
    pass

try:
    fetch_exec_tracker()
except Exception:
    pass

try:
    fetch_github_dev()
except Exception:
    pass

try:
    fetch_clarity_tracker()
except Exception:
    pass

try:
    fetch_correlation()
except Exception:
    pass

try:
    fetch_orderbook()
except Exception:
    pass

try:
    generate_brief()
except Exception:
    pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
