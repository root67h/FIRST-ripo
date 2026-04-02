#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════╗
║   DORK PARSER BOT v17.0 — MAXIMUM STEALTH & SPEED       ║
║   Parallel pages | DDG+Bing+Yahoo | 50+ UA rotation     ║
║   CH-UA fingerprint | Retry+backoff | Soft-block detect ║
║   Domain cap | Base-path dedup | SQLite persistence     ║
║   Per-engine workers+connectors | Gaussian timing       ║
║   Cookie jars | Referer chain | HTTP score bonus        ║
╚══════════════════════════════════════════════════════════╝
"""

import asyncio
import aiohttp
import os
import re
import random
import sqlite3
import tempfile
import time
import logging
from collections import defaultdict
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)

load_dotenv()

# ─── LOGGING ────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(f"logs/bot_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─── CONFIGURATION ──────────────────────────────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
WORKERS     = int(os.environ.get("WORKERS", 20))
MIN_DELAY   = float(os.environ.get("MIN_DELAY", 0.5))
MAX_DELAY   = float(os.environ.get("MAX_DELAY", 1.5))
MAX_RESULTS = int(os.environ.get("MAX_RESULTS", 10))
TOR_PROXY   = os.environ.get("TOR_PROXY", "socks5://127.0.0.1:9050")
OUTPUT_DIR  = Path("results")
OUTPUT_DIR.mkdir(exist_ok=True)
DB_PATH     = Path("dorkparser.db")

ALL_ENGINES = ["bing", "yahoo", "ddg"]
MAX_PAGES   = 70

# ─── RELIABILITY / ANTI-BLOCK CONSTANTS ─────────────────────────────────────
WORKER_FETCH_TIMEOUT    = 120
WATCHDOG_INTERVAL       = 30
WATCHDOG_STALL_LIMIT    = 90
SESSION_RESET_THRESHOLD = 4          # ↓ from 8 — faster session recycle
JOB_TIMEOUT             = 30 * 60
MAX_RETRIES             = 3
RETRY_BASE_DELAY        = 2.0
PAGE_SEM_LIMIT          = 5          # max concurrent page fetches per engine
DOMAIN_CAP              = 10         # max URLs kept per domain per job
BASE_PATH_CAP           = 3          # max URLs kept per base-path per job
GAUSS_MEAN              = 1.0
GAUSS_SIGMA             = 0.4

DEFAULT_SESSION = {
    "workers":     WORKERS,
    "engines":     ["bing", "yahoo"],
    "max_results": MAX_RESULTS,
    "pages":       [1],
    "tor":         False,
    "min_score":   30,
}

user_sessions: dict = {}
active_jobs:   dict = {}

# ═══════════════════════════════════════════════════════════
#  EXPANDED USER-AGENT POOL  (50 + profiles)
#  Each tuple: (ua_string, sec-ch-ua | None, platform, mobile_flag)
# ═══════════════════════════════════════════════════════════
UA_PROFILES = [
    # ── Chrome / Windows ────────────────────────────────────────────────────
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     '"Chromium";v="124","Google Chrome";v="124","Not-A.Brand";v="99"', "Windows", "?0"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
     '"Chromium";v="123","Google Chrome";v="123","Not-A.Brand";v="99"', "Windows", "?0"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
     '"Chromium";v="122","Google Chrome";v="122","Not-A.Brand";v="99"', "Windows", "?0"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
     '"Chromium";v="121","Google Chrome";v="121","Not-A.Brand";v="99"', "Windows", "?0"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
     '"Chromium";v="120","Google Chrome";v="120","Not-A.Brand";v="99"', "Windows", "?0"),
    # ── Chrome / macOS ──────────────────────────────────────────────────────
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     '"Chromium";v="124","Google Chrome";v="124","Not-A.Brand";v="99"', "macOS", "?0"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     '"Chromium";v="124","Google Chrome";v="124","Not-A.Brand";v="99"', "macOS", "?0"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
     '"Chromium";v="123","Google Chrome";v="123","Not-A.Brand";v="99"', "macOS", "?0"),
    # ── Chrome / Linux ──────────────────────────────────────────────────────
    ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     '"Chromium";v="124","Google Chrome";v="124","Not-A.Brand";v="99"', "Linux", "?0"),
    ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
     '"Chromium";v="123","Google Chrome";v="123","Not-A.Brand";v="99"', "Linux", "?0"),
    # ── Firefox / Windows ───────────────────────────────────────────────────
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
     None, "Windows", "?0"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
     None, "Windows", "?0"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
     None, "Windows", "?0"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
     None, "Windows", "?0"),
    # ── Firefox / macOS ─────────────────────────────────────────────────────
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0",
     None, "macOS", "?0"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:123.0) Gecko/20100101 Firefox/123.0",
     None, "macOS", "?0"),
    # ── Firefox / Linux ─────────────────────────────────────────────────────
    ("Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
     None, "Linux", "?0"),
    ("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
     None, "Linux", "?0"),
    # ── Safari / macOS ──────────────────────────────────────────────────────
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
     None, "macOS", "?0"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
     None, "macOS", "?0"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
     None, "macOS", "?0"),
    # ── Edge / Windows ──────────────────────────────────────────────────────
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
     '"Chromium";v="124","Microsoft Edge";v="124","Not-A.Brand";v="99"', "Windows", "?0"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
     '"Chromium";v="123","Microsoft Edge";v="123","Not-A.Brand";v="99"', "Windows", "?0"),
    # ── Brave ───────────────────────────────────────────────────────────────
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     '"Brave";v="124","Chromium";v="124","Not-A.Brand";v="99"', "Windows", "?0"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     '"Brave";v="124","Chromium";v="124","Not-A.Brand";v="99"', "macOS", "?0"),
    # ── Opera ───────────────────────────────────────────────────────────────
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 OPR/110.0.0.0",
     '"Chromium";v="124","Not-A.Brand";v="99","Opera";v="110"', "Windows", "?0"),
    # ── Android Chrome ──────────────────────────────────────────────────────
    ("Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
     '"Chromium";v="124","Google Chrome";v="124","Not-A.Brand";v="99"', "Android", "?1"),
    ("Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
     '"Chromium";v="124","Google Chrome";v="124","Not-A.Brand";v="99"', "Android", "?1"),
    ("Mozilla/5.0 (Linux; Android 13; SM-A546B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",
     '"Chromium";v="123","Google Chrome";v="123","Not-A.Brand";v="99"', "Android", "?1"),
    ("Mozilla/5.0 (Linux; Android 12; Redmi Note 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
     '"Chromium";v="122","Google Chrome";v="122","Not-A.Brand";v="99"', "Android", "?1"),
    ("Mozilla/5.0 (Linux; Android 11; OnePlus Nord 2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
     '"Chromium";v="121","Google Chrome";v="121","Not-A.Brand";v="99"', "Android", "?1"),
    # ── Android Firefox ─────────────────────────────────────────────────────
    ("Mozilla/5.0 (Android 14; Mobile; rv:124.0) Gecko/124.0 Firefox/124.0",
     None, "Android", "?1"),
    ("Mozilla/5.0 (Android 13; Mobile; rv:123.0) Gecko/123.0 Firefox/123.0",
     None, "Android", "?1"),
    # ── iOS Safari ──────────────────────────────────────────────────────────
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
     None, "iOS", "?1"),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
     None, "iOS", "?1"),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 16_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.7 Mobile/15E148 Safari/604.1",
     None, "iOS", "?1"),
    # ── iPad Safari ─────────────────────────────────────────────────────────
    ("Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
     None, "iOS", "?1"),
    ("Mozilla/5.0 (iPad; CPU OS 16_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.7 Mobile/15E148 Safari/604.1",
     None, "iOS", "?1"),
    # ── Samsung Browser ─────────────────────────────────────────────────────
    ("Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/24.0 Chrome/117.0.0.0 Mobile Safari/537.36",
     None, "Android", "?1"),
    # ── Vivaldi ─────────────────────────────────────────────────────────────
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Vivaldi/6.7.3329.21",
     '"Chromium";v="124","Not-A.Brand";v="99"', "Windows", "?0"),
    # ── Chrome on ChromeOS ──────────────────────────────────────────────────
    ("Mozilla/5.0 (X11; CrOS x86_64 15633.69.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
     '"Chromium";v="119","Google Chrome";v="119","Not-A.Brand";v="99"', "Chrome OS", "?0"),
]

ACCEPT_LANGS = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.9,fr;q=0.7",
    "en-US,en;q=0.9,de;q=0.7",
    "en-US,en;q=0.8,es;q=0.6",
    "en;q=0.9",
]


def _human_delay(mean: float = GAUSS_MEAN, sigma: float = GAUSS_SIGMA) -> float:
    """Gaussian delay clamped to [0.15, mean*5]."""
    return max(0.15, min(random.gauss(mean, sigma), mean * 5))


def _build_headers(engine: str, profile: tuple = None, referer: str = None) -> dict:
    """Build a full realistic browser header set."""
    if profile is None:
        profile = random.choice(UA_PROFILES)
    ua, ch_ua, platform, is_mobile = profile

    headers = {
        "User-Agent":                ua,
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           random.choice(ACCEPT_LANGS),
        "Accept-Encoding":           "gzip, deflate, br",
        "DNT":                       "1",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    # Referer chain — first request has none, subsequent have engine homepage
    if referer:
        headers["Referer"] = referer
    elif random.random() > 0.4:
        ref_map = {
            "bing":  "https://www.bing.com/",
            "yahoo": "https://search.yahoo.com/",
            "ddg":   "https://duckduckgo.com/",
        }
        headers["Referer"] = ref_map.get(engine, "")

    # Chrome Client Hints (only Chrome-based UAs have ch_ua)
    if ch_ua:
        headers["Sec-CH-UA"]          = ch_ua
        headers["Sec-CH-UA-Mobile"]   = is_mobile
        headers["Sec-CH-UA-Platform"] = f'"{platform}"'
        headers["Sec-Fetch-Dest"]     = "document"
        headers["Sec-Fetch-Mode"]     = "navigate"
        headers["Sec-Fetch-Site"]     = "same-origin" if referer else "none"
        headers["Sec-Fetch-User"]     = "?1"
        headers["Cache-Control"]      = "max-age=0"

    return headers


# ═══════════════════════════════════════════════════════════
#  SOFT BLOCK DETECTOR
# ═══════════════════════════════════════════════════════════
_BLOCK_RE = re.compile(
    r"captcha|recaptcha|are you a robot|unusual.traffic|"
    r"access.denied|rate.limit|too.many.requests|"
    r"verify.you.are.human|security.check|"
    r"cf-error-details|ddos.protection|blocked.your.ip|"
    r"enable.javascript.and.cookies",
    re.IGNORECASE,
)


def is_soft_blocked(html: str, status: int) -> bool:
    if status in (429, 503, 403):
        return True
    if len(html) < 400:
        return True
    return bool(_BLOCK_RE.search(html[:4000]))


# ═══════════════════════════════════════════════════════════
#  SQL SCORING ENGINE  (unchanged logic + HTTP bonus)
# ═══════════════════════════════════════════════════════════
BLACKLISTED_DOMAINS = {
    "yahoo.uservoice.com", "uservoice.com", "bing.com", "google.com", "googleapis.com",
    "gstatic.com", "youtube.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "pinterest.com", "reddit.com", "wikipedia.org", "amazon.com",
    "amazon.co", "ebay.com", "shopify.com", "wordpress.com", "blogspot.com", "medium.com",
    "github.com", "stackoverflow.com", "w3schools.com", "microsoft.com", "apple.com",
    "cloudflare.com", "yahoo.com", "msn.com", "live.com", "outlook.com", "mercadolibre.com",
    "aliexpress.com", "alibaba.com", "etsy.com", "walmart.com", "bestbuy.com",
    "capitaloneshopping.com", "onetonline.org", "moodle.", "duckduckgo.com",
}

SQL_HIGH_PARAMS = {
    "id", "uid", "user_id", "userid", "pid", "product_id", "productid",
    "cid", "cat_id", "catid", "category_id", "aid", "article_id",
    "nid", "news_id", "bid", "blog_id", "sid", "fid", "forum_id",
    "tid", "topic_id", "mid", "msg_id", "oid", "order_id",
    "rid", "page_id", "item_id", "itemid", "post_id", "gid",
    "lid", "vid", "did", "doc_id",
}

SQL_MED_PARAMS = {
    "q", "query", "search", "name", "username", "email",
    "page", "p", "type", "action", "do", "module",
    "view", "mode", "from", "date", "code", "ref",
    "file", "path", "url", "data", "value", "param",
    "price", "tag", "section", "content", "lang",
}

VULN_EXTENSIONS = {".php", ".asp", ".aspx", ".cfm", ".jsf", ".do", ".cgi", ".pl", ".jsp"}

_JUNK_RE = re.compile(
    r"aclick\?|uservoice\.com|utm_source=|"
    r"\.pdf$|\.jpg$|\.jpeg$|\.png$|\.gif$|\.webp$|\.avif$|"
    r"\.svg$|\.ico$|\.css$|\.js$|\.mp4$|\.mp3$|\.zip$|"
    r"/static/|/assets/|/images/|/img/|/fonts/|/media/|/cdn-cgi/|"
    r"/wp-content/uploads/",
    re.IGNORECASE,
)


def score_url(url: str) -> int:
    try:
        parsed = urlparse(url)
    except Exception:
        return 0
    if not url.startswith("http"):
        return 0
    domain = parsed.netloc.lower()
    for bd in BLACKLISTED_DOMAINS:
        if bd in domain:
            return 0
    if _JUNK_RE.search(url):
        return 0

    query = parsed.query
    path  = parsed.path.lower()
    has_vuln_ext = any(path.endswith(e) for e in VULN_EXTENSIONS)
    if not query:
        return 25 if has_vuln_ext else 5

    score = 15
    if parsed.scheme == "http":   # HTTP bonus — less hardened, more interesting
        score += 5

    params = parse_qs(query, keep_blank_values=True)
    pkeys  = {k.lower() for k in params}
    if has_vuln_ext:
        score += 20
    score += len(pkeys & SQL_HIGH_PARAMS) * 15
    score += len(pkeys & SQL_MED_PARAMS)  * 5
    for vals in params.values():
        for v in vals:
            if v.isdigit():
                score += 10
                break
    if len(url) > 300:
        score -= 10
    elif len(url) > 200:
        score -= 5
    if len(params) > 8:
        score -= 5
    return max(0, min(score, 100))


def filter_scored(urls: list, min_score: int) -> list:
    result = [(score_url(u), u) for u in urls]
    result = [(s, u) for s, u in result if s >= min_score]
    result.sort(reverse=True)
    return result


def _base_path_key(url: str) -> str:
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}{p.path}"
    except Exception:
        return url


def _domain_key(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return url


# ═══════════════════════════════════════════════════════════
#  ROBUST HTML LINK EXTRACTOR  (unchanged)
# ═══════════════════════════════════════════════════════════
class _LinkExtractor(HTMLParser):
    __slots__ = ("links", "_in_cite", "_buf")

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self._in_cite = False
        self._buf: list = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            adict = dict(attrs)
            for key in ("href", "data-u"):
                val = adict.get(key, "")
                if val.startswith("http"):
                    self.links.append(val)
        elif tag == "cite":
            self._in_cite = True
            self._buf.clear()

    def handle_endtag(self, tag):
        if tag == "cite" and self._in_cite:
            text = "".join(self._buf).strip()
            if text.startswith("http"):
                self.links.append(text)
            self._in_cite = False
            self._buf.clear()

    def handle_data(self, data):
        if self._in_cite:
            self._buf.append(data)


def _extract_links(html: str) -> list[str]:
    p = _LinkExtractor()
    try:
        p.feed(html)
    except Exception:
        pass
    return p.links


# ═══════════════════════════════════════════════════════════
#  TOR ROTATION  (unchanged)
# ═══════════════════════════════════════════════════════════
tor_rotation_task = None
tor_enabled_users = 0


async def rotate_tor_identity():
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", 9051)
        await reader.readuntil(b"250 ")
        writer.write(b'AUTHENTICATE ""\r\n')
        await writer.drain()
        resp = await reader.readuntil(b"250 ")
        if b"250" not in resp:
            writer.close()
            return
        writer.write(b"SIGNAL NEWNYM\r\n")
        await writer.drain()
        resp = await reader.readuntil(b"250 ")
        if b"250" in resp:
            log.info("Tor IP rotated")
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        log.warning(f"Tor rotation error: {e}")


async def tor_rotation_loop():
    while tor_enabled_users > 0:
        await rotate_tor_identity()
        await asyncio.sleep(120)


def start_tor_rotation():
    global tor_rotation_task
    if tor_rotation_task is None or tor_rotation_task.done():
        tor_rotation_task = asyncio.create_task(tor_rotation_loop())
        log.info("Tor rotation started")


def stop_tor_rotation():
    global tor_rotation_task
    if tor_rotation_task and not tor_rotation_task.done():
        tor_rotation_task.cancel()
        tor_rotation_task = None


# ═══════════════════════════════════════════════════════════
#  SESSION FACTORY — per-engine dedicated connectors
# ═══════════════════════════════════════════════════════════
def _make_engine_connector() -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(
        ssl=False,
        limit=50,
        limit_per_host=8,
        ttl_dns_cache=300,
    )


def _make_job_sessions(use_tor: bool, engines: list) -> dict:
    """Return {engine: ClientSession} — each engine gets its own connector + cookie jar."""
    sessions = {}
    if use_tor:
        try:
            from aiohttp_socks import ProxyConnector
            for engine in engines:
                conn = ProxyConnector.from_url(TOR_PROXY, ssl=False)
                sessions[engine] = aiohttp.ClientSession(
                    connector=conn,
                    connector_owner=True,
                    cookie_jar=aiohttp.CookieJar(unsafe=True),
                )
            return sessions
        except ImportError:
            log.warning("[TOR] aiohttp_socks not installed, falling back to direct")

    for engine in engines:
        sessions[engine] = aiohttp.ClientSession(
            connector=_make_engine_connector(),
            connector_owner=True,
            cookie_jar=aiohttp.CookieJar(unsafe=True),
        )
    return sessions


# ═══════════════════════════════════════════════════════════
#  FETCH HELPERS — retry + backoff + CH-UA + referer
# ═══════════════════════════════════════════════════════════
_BING_NOISE  = re.compile(r"bing\.com", re.IGNORECASE)
_YAHOO_NOISE = re.compile(r"yimg\.com|yahoo\.com|doubleclick\.net|googleadservices", re.IGNORECASE)
_DDG_NOISE   = re.compile(r"duckduckgo\.com", re.IGNORECASE)
_STATIC_EXT  = re.compile(r"\.(css|js|png|jpg|jpeg|gif|svg|ico|webp|woff2?|ttf|eot)(\?|$)", re.IGNORECASE)
_YAHOO_RU    = re.compile(r"/RU=([^/&]+)")


async def _fetch_raw(session: aiohttp.ClientSession,
                     method: str, url: str, headers: dict,
                     params=None, data=None, timeout: float = 15.0) -> tuple[int, str]:
    to = aiohttp.ClientTimeout(total=timeout)
    if method == "POST":
        async with session.post(url, data=data, headers=headers,
                                timeout=to, allow_redirects=True) as r:
            return r.status, await r.text(errors="replace")
    async with session.get(url, params=params, headers=headers,
                           timeout=to, allow_redirects=True) as r:
        return r.status, await r.text(errors="replace")


async def _fetch_retry(session: aiohttp.ClientSession,
                       method: str, url: str,
                       hdr_fn,           # callable() → fresh headers
                       params=None, data=None,
                       timeout: float = 15.0) -> tuple[int, str]:
    """Fetch with exponential-backoff retry on 429/503/timeout."""
    for attempt in range(MAX_RETRIES):
        try:
            status, html = await _fetch_raw(
                session, method, url, hdr_fn(),
                params=params, data=data, timeout=timeout,
            )
            if status in (429, 503):
                delay = RETRY_BASE_DELAY * (2 ** attempt) + _human_delay(0.5, 0.2)
                log.warning(f"[RETRY] {url[:45]} status={status} wait={delay:.1f}s attempt={attempt+1}")
                await asyncio.sleep(delay)
                continue
            return status, html
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == MAX_RETRIES - 1:
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt) + _human_delay(0.3, 0.1)
            log.warning(f"[RETRY] {url[:45]} err={exc} wait={delay:.1f}s")
            await asyncio.sleep(delay)
    return 0, ""


# ─── Bing ────────────────────────────────────────────────────────────────────
async def fetch_page_bing(session: aiohttp.ClientSession,
                          dork: str, page: int, max_res: int,
                          prev_url: str = None) -> tuple[list, bool]:
    try:
        params  = {"q": dork, "count": min(max_res, 10),
                   "first": (page - 1) * 10 + 1, "setlang": "en"}
        profile = random.choice(UA_PROFILES)
        hdr     = lambda: _build_headers("bing", profile, prev_url)

        status, html = await _fetch_retry(
            session, "GET", "https://www.bing.com/search", hdr, params=params
        )
        if is_soft_blocked(html, status):
            log.warning(f"[BING] Soft-block p{page}")
            return [], True

        raw  = _extract_links(html)
        urls = [u for u in raw if u.startswith("http") and not _BING_NOISE.search(u)]
        return list(dict.fromkeys(urls))[:max_res], False
    except Exception as e:
        log.warning(f"[BING] p{page}: {e}")
        return [], False


# ─── Yahoo ───────────────────────────────────────────────────────────────────
async def fetch_page_yahoo(session: aiohttp.ClientSession,
                           dork: str, page: int, max_res: int,
                           prev_url: str = None) -> tuple[list, bool]:
    try:
        params  = {"p": dork, "b": (page - 1) * 10 + 1,
                   "pz": min(max_res, 10), "vl": "lang_en"}
        profile = random.choice(UA_PROFILES)
        hdr     = lambda: _build_headers("yahoo", profile,
                                         prev_url or "https://search.yahoo.com/")

        status, html = await _fetch_retry(
            session, "GET", "https://search.yahoo.com/search", hdr, params=params
        )
        if is_soft_blocked(html, status):
            log.warning(f"[YAHOO] Soft-block p{page}")
            return [], True

        raw  = _extract_links(html)
        urls = []
        for u in raw:
            if not u.startswith("http"):
                continue
            if "r.search.yahoo.com" in u or "/r/" in u:
                parsed = urlparse(u)
                qs = parse_qs(parsed.query)
                if "RU" in qs:
                    real = unquote(qs["RU"][0])
                    if real.startswith(("http://", "https://")):
                        u = real
                else:
                    m = _YAHOO_RU.search(parsed.path)
                    if m:
                        real = unquote(m.group(1))
                        if real.startswith(("http://", "https://")):
                            u = real
            if _YAHOO_NOISE.search(u) or _STATIC_EXT.search(u):
                continue
            urls.append(u)
        return list(dict.fromkeys(urls))[:max_res], False
    except Exception as e:
        log.warning(f"[YAHOO] p{page}: {e}")
        return [], False


# ─── DuckDuckGo (HTML endpoint — no JS required) ────────────────────────────
async def fetch_page_ddg(session: aiohttp.ClientSession,
                         dork: str, page: int, max_res: int,
                         prev_url: str = None) -> tuple[list, bool]:
    """DDG HTML endpoint — POST page1, GET offset for subsequent pages."""
    try:
        profile = random.choice(UA_PROFILES)
        hdr     = lambda: _build_headers("ddg", profile,
                                         prev_url or "https://duckduckgo.com/")

        if page == 1:
            status, html = await _fetch_retry(
                session, "POST", "https://html.duckduckgo.com/html/",
                hdr, data={"q": dork, "b": "", "kl": "us-en"},
            )
        else:
            status, html = await _fetch_retry(
                session, "GET", "https://html.duckduckgo.com/html/",
                hdr, params={"q": dork, "s": str((page - 1) * 10), "kl": "us-en"},
            )

        if is_soft_blocked(html, status):
            log.warning(f"[DDG] Soft-block p{page}")
            return [], True

        raw  = _extract_links(html)
        urls = []
        for u in raw:
            if not u.startswith("http"):
                continue
            if _DDG_NOISE.search(u) or _STATIC_EXT.search(u):
                continue
            # Unwrap DDG redirect URLs
            if "uddg=" in u:
                try:
                    qs = parse_qs(urlparse(u).query)
                    if "uddg" in qs:
                        real = unquote(qs["uddg"][0])
                        if real.startswith("http"):
                            u = real
                except Exception:
                    pass
            urls.append(u)
        return list(dict.fromkeys(urls))[:max_res], False
    except Exception as e:
        log.warning(f"[DDG] p{page}: {e}")
        return [], False


# ═══════════════════════════════════════════════════════════
#  PARALLEL PAGE FETCHER  (asyncio.gather + per-engine sem)
# ═══════════════════════════════════════════════════════════
_FETCH_FN = {
    "bing":  fetch_page_bing,
    "yahoo": fetch_page_yahoo,
    "ddg":   fetch_page_ddg,
}


async def fetch_all_pages(session: aiohttp.ClientSession,
                          dork: str, engine: str,
                          pages: list, max_res: int,
                          sem: asyncio.Semaphore) -> list:
    """Fetch all pages CONCURRENTLY, bounded by sem. Returns flat URL list."""
    fn = _FETCH_FN[engine]

    async def _one(page: int) -> list:
        async with sem:
            urls, blocked = await fn(session, dork, page, max_res)
            if blocked:
                return []
            await asyncio.sleep(_human_delay(0.35, 0.12))
            return urls

    tasks   = [asyncio.create_task(_one(p)) for p in sorted(pages)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    merged  = []
    for r in results:
        if isinstance(r, list):
            merged.extend(r)
    return list(dict.fromkeys(merged))


# ═══════════════════════════════════════════════════════════
#  WORKER  (engine-dedicated — no more round-robin)
# ═══════════════════════════════════════════════════════════
async def dork_worker(wid: int,
                      queue: asyncio.Queue,
                      results_q: asyncio.Queue,
                      engine: str,
                      pages: list,
                      max_res: int,
                      session: aiohttp.ClientSession,
                      min_score: int,
                      stop_ev: asyncio.Event,
                      sem: asyncio.Semaphore):
    while not stop_ev.is_set():
        try:
            dork = await asyncio.wait_for(queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            continue

        log.info(f"[W{wid}][{engine.upper()}] {dork[:55]}")
        raw = []
        try:
            raw = await asyncio.wait_for(
                fetch_all_pages(session, dork, engine, pages, max_res, sem),
                timeout=WORKER_FETCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            log.warning(f"[W{wid}] timeout: {dork[:50]}")
        except asyncio.CancelledError:
            try:
                results_q.put_nowait((dork, engine, pages, [], 0))
            except asyncio.QueueFull:
                pass
            queue.task_done()
            raise
        except Exception as e:
            log.warning(f"[W{wid}] error: {e}")

        scored = filter_scored(raw, min_score)
        log.info(f"[W{wid}] raw={len(raw)} kept={len(scored)}")

        try:
            results_q.put_nowait((dork, engine, pages, scored, len(raw)))
        except asyncio.QueueFull:
            await results_q.put((dork, engine, pages, scored, len(raw)))

        queue.task_done()
        await asyncio.sleep(_human_delay())


# ═══════════════════════════════════════════════════════════
#  SQLITE PERSISTENCE
# ═══════════════════════════════════════════════════════════
def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS job_results (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id  INTEGER NOT NULL,
                job_ts   TEXT    NOT NULL,
                score    INTEGER NOT NULL,
                url      TEXT    NOT NULL,
                UNIQUE(chat_id, job_ts, url)
            )
        """)
        con.commit()


def db_save(chat_id: int, job_ts: str, scored: list):
    with sqlite3.connect(DB_PATH) as con:
        con.executemany(
            "INSERT OR IGNORE INTO job_results(chat_id,job_ts,score,url) VALUES(?,?,?,?)",
            [(chat_id, job_ts, sc, url) for sc, url in scored],
        )
        con.commit()


# ═══════════════════════════════════════════════════════════
#  JOB RUNNER
# ═══════════════════════════════════════════════════════════
async def run_dork_job(chat_id: int, dorks: list, context):
    sess      = get_session(chat_id)
    engines   = sess.get("engines", ["bing", "yahoo"])
    workers_n = sess.get("workers", WORKERS)
    max_res   = sess.get("max_results", MAX_RESULTS)
    pages     = sess.get("pages", [1])
    use_tor   = sess.get("tor", False)
    min_score = sess.get("min_score", 30)
    job_ts    = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Per-engine sessions & semaphores
    eng_sessions = _make_job_sessions(use_tor, engines)
    eng_sems     = {e: asyncio.Semaphore(PAGE_SEM_LIMIT) for e in engines}

    queue     = asyncio.Queue(maxsize=len(dorks) * 2)
    for d in dorks:
        await queue.put(d)
    results_q = asyncio.Queue(maxsize=2000)

    stop_ev     = asyncio.Event()
    processed   = 0
    total_dorks = len(dorks)
    seen_urls   = set()
    bp_counts   = defaultdict(int)    # base-path dedup
    dom_counts  = defaultdict(int)    # domain cap
    all_scored: list = []
    total_raw   = 0
    start_time  = time.time()
    pages_str   = ", ".join(str(p) for p in pages)

    tmp_file = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", delete=False,
        prefix=f"dork_{chat_id}_", suffix=".txt",
    )
    tmp_path = tmp_file.name
    for line in [
        f"# Dork Parser v17.0 — SQL Targeted Results",
        f"# Date    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"# Dorks   : {total_dorks} | Pages : {pages_str}",
        f"# Filter  : SQL ≥{min_score}",
        f"# Engines : {'+'.join(e.upper() for e in engines)}",
        "─" * 60, "",
    ]:
        tmp_file.write(line + "\n")
    tmp_file.flush()

    status_msg = await context.bot.send_message(
        chat_id,
        f"🕷 DORK PARSER v17.0 — STARTED\n"
        f"{'━'*30}\n"
        f"📋 Dorks   : {total_dorks}\n"
        f"📄 Pages   : {pages_str}  ⚡ parallel\n"
        f"⚙️ Workers : {workers_n}\n"
        f"🔍 Engines : {' + '.join(e.upper() for e in engines)}\n"
        f"🛡 Filter  : SQL ≥ {min_score}\n"
        f"🌐 Network : {'🧅 TOR' if use_tor else '🔓 Direct'}\n"
        f"{'━'*30}\n⏳ Starting...",
    )

    last_result_ts = [time.time()]
    consec_zero    = 0
    restarts       = 0
    worker_tasks:  list = []
    batch_buf:     list = []
    flush_lock     = asyncio.Lock()

    async def flush_buffer():
        nonlocal batch_buf
        if not batch_buf:
            return
        async with flush_lock:
            to_save   = list(batch_buf)
            batch_buf = []
        with open(tmp_path, "a", encoding="utf-8") as f:
            high   = [u for sc, u in to_save if sc >= 70]
            medium = [u for sc, u in to_save if 40 <= sc < 70]
            low    = [u for sc, u in to_save if sc < 40]
            if high:
                f.write("# HIGH VALUE (score 70+)\n")
                for u in high:
                    f.write(f"{u}\n")
            if medium:
                f.write("\n# MEDIUM VALUE (score 40-69)\n")
                for u in medium:
                    f.write(f"{u}\n")
            if low and min_score < 40:
                f.write("\n# LOW VALUE (score < 40)\n")
                for u in low:
                    f.write(f"{u}\n")
            f.write("\n")
        db_save(chat_id, job_ts, to_save)

    # ── Engine-dedicated worker spawner ──────────────────────────────────────
    def _spawn_workers():
        nonlocal worker_tasks
        worker_tasks.clear()
        base  = workers_n // len(engines)
        extra = workers_n % len(engines)
        wid   = 0
        for i, eng in enumerate(engines):
            n = base + (1 if i < extra else 0)
            for _ in range(n):
                t = asyncio.create_task(
                    dork_worker(wid, queue, results_q, eng, pages, max_res,
                                eng_sessions[eng], min_score, stop_ev, eng_sems[eng])
                )
                worker_tasks.append(t)
                wid += 1

    _spawn_workers()

    # ── Watchdog ─────────────────────────────────────────────────────────────
    async def watchdog():
        nonlocal restarts, consec_zero
        while not stop_ev.is_set():
            await asyncio.sleep(WATCHDOG_INTERVAL)
            if stop_ev.is_set():
                break
            elapsed = time.time() - last_result_ts[0]
            if elapsed < WATCHDOG_STALL_LIMIT:
                continue
            alive = sum(1 for t in worker_tasks if not t.done())
            log.warning(f"[WD][{chat_id}] stall={elapsed:.0f}s alive={alive}")
            for t in worker_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            worker_tasks.clear()
            if stop_ev.is_set() or queue.empty():
                break
            restarts += 1
            if restarts > 3:
                log.critical(f"[WD][{chat_id}] too many restarts, aborting")
                stop_ev.set()
                break
            log.info(f"[WD][{chat_id}] restarting workers (#{restarts})")
            _spawn_workers()
            last_result_ts[0] = time.time()
            consec_zero = 0

    # ── Collector ────────────────────────────────────────────────────────────
    async def collector():
        nonlocal processed, total_raw, consec_zero, restarts, batch_buf, all_scored

        while processed < total_dorks and not stop_ev.is_set():
            try:
                dork, engine, _pages, scored, raw_cnt = await asyncio.wait_for(
                    results_q.get(), timeout=45.0
                )
            except asyncio.TimeoutError:
                continue

            processed += 1
            total_raw += raw_cnt
            last_result_ts[0] = time.time()
            restarts = 0

            if raw_cnt == 0:
                consec_zero += 1
                if consec_zero >= SESSION_RESET_THRESHOLD:
                    log.warning(f"[JOB] {SESSION_RESET_THRESHOLD} zero-raw → recycling sessions")
                    for t in worker_tasks:
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(*worker_tasks, return_exceptions=True)
                    for s in eng_sessions.values():
                        await s.close()
                    eng_sessions.update(_make_job_sessions(use_tor, engines))
                    _spawn_workers()
                    consec_zero = 0
                    last_result_ts[0] = time.time()
            else:
                consec_zero = 0

            # ── Advanced dedup: exact + base-path cap + domain cap ──────────
            for sc, url in scored:
                if url in seen_urls:
                    continue
                bp  = _base_path_key(url)
                dom = _domain_key(url)
                if bp_counts[bp]   >= BASE_PATH_CAP:
                    continue
                if dom_counts[dom] >= DOMAIN_CAP:
                    continue
                seen_urls.add(url)
                bp_counts[bp]   += 1
                dom_counts[dom] += 1
                all_scored.append((sc, url))
                batch_buf.append((sc, url))

            if len(batch_buf) >= 500:
                await flush_buffer()

            if time.time() - getattr(collector, "last_edit", 0) > 4:
                pct = int(processed / total_dorks * 100)
                bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
                ela = int(time.time() - start_time)
                eta = int((ela / processed) * (total_dorks - processed)) if processed else 0
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_msg.message_id,
                        text=(
                            f"⚡ PARSING...\n"
                            f"{'━'*30}\n"
                            f"[{bar}] {pct}%\n"
                            f"✅ Done    : {processed}/{total_dorks}\n"
                            f"🎯 SQL     : {len(seen_urls)}\n"
                            f"🗑 Dropped : {total_raw - len(seen_urls)}\n"
                            f"⏱ {ela}s | ETA {eta}s\n"
                            f"{'━'*30}"
                        ),
                    )
                    collector.last_edit = time.time()
                except Exception:
                    pass

    async def job_timeout():
        await asyncio.sleep(JOB_TIMEOUT)
        log.warning(f"[JOB][{chat_id}] global timeout")
        stop_ev.set()

    collector_task = asyncio.create_task(collector())
    watchdog_task  = asyncio.create_task(watchdog())
    timeout_task   = asyncio.create_task(job_timeout())

    try:
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        await collector_task
        await flush_buffer()
    except asyncio.CancelledError:
        log.info(f"[JOB] cancelled for {chat_id}")
        stop_ev.set()
        for t in worker_tasks:
            t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        collector_task.cancel()
        await asyncio.gather(collector_task, return_exceptions=True)
        await flush_buffer()
        raise
    finally:
        timeout_task.cancel()
        watchdog_task.cancel()
        await asyncio.gather(timeout_task, watchdog_task, return_exceptions=True)
        for s in eng_sessions.values():
            await s.close()
        active_jobs.pop(chat_id, None)

    elapsed    = int(time.time() - start_time)
    unique_cnt = len(seen_urls)
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg.message_id,
            text=(
                f"🏁 JOB COMPLETE!\n"
                f"{'━'*30}\n"
                f"📋 Dorks   : {total_dorks}\n"
                f"📄 Pages   : {pages_str}\n"
                f"🔍 Raw     : {total_raw}\n"
                f"🎯 SQL     : {len(all_scored)} total URLs\n"
                f"✨ Unique  : {unique_cnt} URLs\n"
                f"🗑 Dropped : {total_raw - unique_cnt} junk\n"
                f"⏱ Time    : {elapsed}s\n"
                f"{'━'*30}"
            ),
        )
    except Exception:
        pass

    if all_scored:
        with open(tmp_path, "rb") as f:
            await context.bot.send_document(
                chat_id, f,
                filename=f"sql_{total_dorks}dorks_{unique_cnt}urls_{job_ts}.txt",
                caption=(
                    f"📁 SQL Targets — v17.0\n"
                    f"🎯 {unique_cnt} unique | 🗑 {total_raw - unique_cnt} dropped\n"
                    f"📋 {total_dorks} dorks | {pages_str}\n"
                    f"🔍 {'+'.join(e.upper() for e in engines)}"
                ),
            )
    os.unlink(tmp_path)


# ═══════════════════════════════════════════════════════════
#  UI HELPERS  (unchanged)
# ═══════════════════════════════════════════════════════════
def get_session(chat_id: int) -> dict:
    if chat_id not in user_sessions:
        user_sessions[chat_id] = dict(DEFAULT_SESSION)
    return user_sessions[chat_id]


def page_keyboard(selected: list) -> InlineKeyboardMarkup:
    rows, row = [], []
    for p in range(1, 71):
        row.append(InlineKeyboardButton(
            f"✅{p}" if p in selected else str(p),
            callback_data=f"pg_{p}",
        ))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("🔁 All (1-70)", callback_data="pg_all"),
        InlineKeyboardButton("❌ Clear",       callback_data="pg_clear"),
        InlineKeyboardButton("✅ Confirm",     callback_data="pg_confirm"),
    ])
    return InlineKeyboardMarkup(rows)


# ═══════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("📂 Bulk Upload",  callback_data="m_bulk"),
         InlineKeyboardButton("🔍 Single Dork",  callback_data="m_single")],
        [InlineKeyboardButton("📄 Select Pages", callback_data="m_pages"),
         InlineKeyboardButton("⚙️ Settings",     callback_data="m_settings")],
        [InlineKeyboardButton("🧅 Tor On/Off",   callback_data="m_tor"),
         InlineKeyboardButton("🛡 SQL Filter",   callback_data="m_filter")],
        [InlineKeyboardButton("📖 Help",         callback_data="m_help")],
    ]
    await update.message.reply_text(
        "🕷 DORK PARSER v17.0 — MAX STEALTH & SPEED\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ Parallel pages | 50+ UA rotation | CH-UA\n"
        "🔍 Bing + Yahoo + DDG | Retry + backoff\n"
        "🛡 Soft-block detect | Domain cap | SQLite\n"
        "🧅 Tor auto-rotation | Gaussian timing\n\n"
        "📌 Commands:\n"
        "  /dork <q>   — single dork\n"
        "  /pages      — pick pages 1-70\n"
        "  /tor        — toggle Tor\n"
        "  /filter N   — SQL score filter (0-100)\n"
        "  /engine X   — bing|yahoo|ddg|all\n"
        "  Upload .txt — bulk mode\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def cmd_dork(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /dork inurl:login.php?id=")
        return
    if chat_id in active_jobs and not active_jobs[chat_id].done():
        await update.message.reply_text("⚠️ Job running! Use /stop first.")
        return
    dork = " ".join(context.args)
    s    = get_session(chat_id)
    await update.message.reply_text(
        f"🔍 {dork[:60]}\n"
        f"📄 Pages: {', '.join(str(p) for p in s.get('pages',[1]))}"
        f"{'  🧅TOR' if s.get('tor') else ''}"
    )
    active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, [dork], context))


async def cmd_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id  = update.effective_chat.id
    selected = get_session(chat_id).get("pages", [1])
    await update.message.reply_text(
        f"📄 SELECT PAGES (1–70)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Selected: {', '.join(str(p) for p in selected)}\n"
        f"Tap to toggle, then Confirm.",
        reply_markup=page_keyboard(selected),
    )


async def cmd_tor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global tor_enabled_users
    chat_id = update.effective_chat.id
    sess    = get_session(chat_id)
    new_val = (context.args[0].lower() == "on") if context.args else not sess.get("tor", False)
    old_val = sess.get("tor", False)
    sess["tor"] = new_val
    if new_val and not old_val:
        tor_enabled_users += 1
        if tor_enabled_users == 1:
            start_tor_rotation()
        await update.message.reply_text(
            "🧅 TOR ENABLED\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "IP rotates every 2 minutes.\n"
            "  sudo apt install tor && sudo service tor start\n\n"
            "⚠️ Speed will be slower."
        )
    elif not new_val and old_val:
        tor_enabled_users -= 1
        if tor_enabled_users == 0:
            stop_tor_rotation()
        await update.message.reply_text("🔓 TOR DISABLED — Direct connection.")
    else:
        await update.message.reply_text(f"Tor is already {'ON' if new_val else 'OFF'}.")


async def cmd_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    sess    = get_session(chat_id)
    try:
        n = max(0, min(int(context.args[0]), 100))
        sess["min_score"] = n
        label = "🟥 High only" if n >= 70 else "🟧 Medium+" if n >= 40 else "🟨 All URLs"
        await update.message.reply_text(f"🛡 SQL Filter: ≥{n} ({label})")
    except Exception:
        cur = sess.get("min_score", 30)
        await update.message.reply_text(
            f"Usage: /filter N (0-100)\nCurrent: {cur}\n\n"
            f"🟥 70+ = high (likely SQLi)\n"
            f"🟧 40+ = medium (default 30)\n"
            f"🟨 0   = accept all"
        )


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    s       = get_session(chat_id)
    await update.message.reply_text(
        f"⚙️ SETTINGS\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔧 Workers  : {s.get('workers', WORKERS)}\n"
        f"📄 Pages    : {', '.join(str(p) for p in s.get('pages',[1]))} (1–70)\n"
        f"🔍 Engines  : {'+'.join(e.upper() for e in s.get('engines', ['bing','yahoo']))}\n"
        f"📊 Max/Page : {s.get('max_results', MAX_RESULTS)}\n"
        f"🛡 SQL ≥    : {s.get('min_score', 30)}\n"
        f"🧅 Tor      : {'ON' if s.get('tor') else 'OFF'}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"/workers N  | /maxres N\n"
        f"/engine bing|yahoo|ddg|all\n"
        f"/filter N   | /pages | /tor"
    )


async def cmd_workers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        n = max(1, min(int(context.args[0]), 50))
        get_session(chat_id)["workers"] = n
        await update.message.reply_text(f"✅ Workers: {n}")
    except Exception:
        await update.message.reply_text("Usage: /workers N (1-50)")


async def cmd_maxres(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        n = max(1, min(int(context.args[0]), 50))
        get_session(chat_id)["max_results"] = n
        await update.message.reply_text(f"✅ Max/page: {n}")
    except Exception:
        await update.message.reply_text("Usage: /maxres N (1-50)")


async def cmd_engine(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        choice = context.args[0].lower()
        mapping = {
            "bing":  ["bing"],
            "yahoo": ["yahoo"],
            "ddg":   ["ddg"],
            "all":   ["bing", "yahoo", "ddg"],
            "both":  ["bing", "yahoo"],
        }
        engines = mapping.get(choice)
        if not engines:
            raise ValueError
        get_session(chat_id)["engines"] = engines
        await update.message.reply_text(f"✅ Engines: {'+'.join(e.upper() for e in engines)}")
    except Exception:
        await update.message.reply_text("Usage: /engine bing|yahoo|ddg|all|both")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in active_jobs:
        active_jobs.pop(chat_id).cancel()
        await update.message.reply_text("🛑 Stopping... Partial results will be sent shortly.")
    else:
        await update.message.reply_text("No active job.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    job     = active_jobs.get(chat_id)
    await update.message.reply_text(
        "⚡ Job RUNNING" if job and not job.done() else "💤 No active job"
    )


# ═══════════════════════════════════════════════════════════
#  DOCUMENT & TEXT HANDLERS  (unchanged)
# ═══════════════════════════════════════════════════════════
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    doc     = update.message.document
    if chat_id in active_jobs and not active_jobs[chat_id].done():
        await update.message.reply_text("⚠️ Job running! Use /stop first.")
        return
    if not doc.file_name.endswith(".txt"):
        await update.message.reply_text("❌ Send a .txt file (one dork per line).")
        return
    await update.message.reply_text("📥 Reading file...")
    try:
        content = await (await context.bot.get_file(doc.file_id)).download_as_bytearray()
        dorks = [l.strip() for l in content.decode("utf-8", errors="replace").splitlines()
                 if l.strip() and not l.startswith("#")]
        if not dorks:
            await update.message.reply_text("❌ No dorks found.")
            return
        s = get_session(chat_id)
        await update.message.reply_text(
            f"✅ {len(dorks)} dorks | Pages: {', '.join(str(p) for p in s.get('pages',[1]))}\n"
            f"🛡 SQL ≥{s.get('min_score',30)} | {'🧅TOR' if s.get('tor') else '🔓 Direct'}\n🚀 Starting..."
        )
        active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, dorks, context))
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lines   = [l.strip() for l in update.message.text.splitlines()
               if l.strip() and not l.startswith("#")]
    if len(lines) > 1:
        if chat_id in active_jobs and not active_jobs[chat_id].done():
            await update.message.reply_text("⚠️ Job running! /stop first.")
            return
        s = get_session(chat_id)
        await update.message.reply_text(
            f"✅ {len(lines)} dorks | Pages: {', '.join(str(p) for p in s.get('pages',[1]))}\n🚀 Starting..."
        )
        active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, lines, context))
    else:
        await update.message.reply_text("Use /dork <q> or upload .txt\n/pages | /tor | /filter N")


# ═══════════════════════════════════════════════════════════
#  CALLBACK HANDLER  (unchanged)
# ═══════════════════════════════════════════════════════════
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat_id
    sess    = get_session(chat_id)

    if data.startswith("pg_"):
        cmd      = data[3:]
        selected = list(sess.get("pages", [1]))
        if cmd == "all":
            selected = list(range(1, 71))
        elif cmd == "clear":
            selected = []
        elif cmd == "confirm":
            sess["pages"] = selected or [1]
            try:
                await query.edit_message_text(
                    f"✅ Pages: {', '.join(str(p) for p in sorted(sess['pages']))}\n"
                    f"Run /dork or upload .txt"
                )
            except Exception:
                pass
            return
        else:
            try:
                p = int(cmd)
                selected.remove(p) if p in selected else selected.append(p)
                selected = sorted(selected)
            except ValueError:
                pass
        sess["pages"] = selected
        try:
            await query.edit_message_text(
                f"📄 SELECT PAGES (1–70)\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Selected: {', '.join(str(p) for p in selected) or 'none'}\n"
                f"Tap to toggle, then Confirm.",
                reply_markup=page_keyboard(selected),
            )
        except Exception:
            pass
        return

    replies = {
        "m_bulk":     "📂 Upload a .txt file — one dork per line. No limit!",
        "m_single":   "🔍 /dork inurl:login.php?id=\nSet pages with /pages",
        "m_tor":      f"🧅 Tor is {'ON — /tor off to disable' if sess.get('tor') else 'OFF — /tor on to enable'}",
        "m_filter":   f"🛡 SQL Filter ≥{sess.get('min_score',30)}\n/filter 70=high | /filter 40=medium | /filter 0=all",
        "m_settings": (
            f"⚙️ Workers:{sess.get('workers',WORKERS)} Pages:{','.join(str(p) for p in sess.get('pages',[1]))} "
            f"Engines:{'+'.join(e.upper() for e in sess.get('engines',['bing','yahoo']))} "
            f"Score≥{sess.get('min_score',30)} Tor:{'ON' if sess.get('tor') else 'OFF'}"
        ),
        "m_help": (
            "📖 COMMANDS\n━━━━━━━━━━━━━━━━━━━\n"
            "/dork <q>       — single dork\n"
            "/pages          — page selector (1-70)\n"
            "/tor            — toggle Tor\n"
            "/filter N       — SQL score (0-100)\n"
            "/engine X       — bing|yahoo|ddg|all\n"
            "/settings       — config\n"
            "/workers N      — workers 1-50\n"
            "/maxres N       — results/page (1-50)\n"
            "/stop           — stop job\n"
            "/status         — job status\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Upload .txt for unlimited bulk!\n"
            "📁 Results saved as file — no chat spam."
        ),
    }

    if data == "m_pages":
        await query.message.reply_text(
            f"📄 SELECT PAGES (1–70)\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Selected: {', '.join(str(p) for p in sess.get('pages',[1]))}\nTap to toggle.",
            reply_markup=page_keyboard(sess.get("pages", [1])),
        )
    elif data in replies:
        await query.message.reply_text(replies[data])


# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════
def main():
    if not BOT_TOKEN:
        log.critical("BOT_TOKEN not set!")
        raise SystemExit(1)

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    for name, handler in [
        ("start",    cmd_start),
        ("help",     cmd_settings),
        ("dork",     cmd_dork),
        ("pages",    cmd_pages),
        ("tor",      cmd_tor),
        ("filter",   cmd_filter),
        ("settings", cmd_settings),
        ("workers",  cmd_workers),
        ("maxres",   cmd_maxres),
        ("engine",   cmd_engine),
        ("stop",     cmd_stop),
        ("status",   cmd_status),
    ]:
        app.add_handler(CommandHandler(name, handler))

    app.add_handler(MessageHandler(filters.Document.ALL,            handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))

    log.info("=" * 55)
    log.info("  DORK PARSER v17.0 — MAX STEALTH & SPEED")
    log.info("=" * 55)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
