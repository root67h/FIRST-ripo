"""
╔══════════════════════════════════════════════════════════╗
║   DORK PARSER BOT v17.0 — STREAMING + SQLITE            ║
║   Handles 200k+ dorks | Bounded queues | Disk dedup     ║
║   Producer/Worker/Writer architecture                   ║
║   Pages 1-70 | Tor auto-rotation                        ║
╚══════════════════════════════════════════════════════════╝
"""

import asyncio
import aiohttp
import random
import re
import os
import time
import logging
import tempfile
import sqlite3
import io
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote
from typing import AsyncIterator, Optional

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

load_dotenv()

# ─── LOGGING ────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
log_file = f"logs/bot_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
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

ENGINES   = ["bing", "yahoo"]
MAX_PAGES = 70

# ─── RELIABILITY CONSTANTS ──────────────────────────────────────────────────
WORKER_FETCH_TIMEOUT = 120          # seconds per multi-page fetch
WATCHDOG_INTERVAL    = 30           # seconds between watchdog checks
WATCHDOG_STALL_LIMIT = 90           # seconds without result before restart
SESSION_RESET_THRESHOLD = 8         # consecutive zero-raw dorks before session recycle
JOB_TIMEOUT          = 30 * 60      # 30 minutes total job runtime

DEFAULT_SESSION = {
    "workers": WORKERS,
    "engines": list(ENGINES),
    "max_results": MAX_RESULTS,
    "pages": [1],
    "tor": False,
    "min_score": 30,
}

user_sessions: dict = {}
active_jobs:   dict = {}

# ─── SHARED CONNECTOR ───────────────────────────────────────────────────────
SHARED_CONNECTOR = aiohttp.TCPConnector(
    ssl=False,
    limit=100,
    limit_per_host=10,
    ttl_dns_cache=300,
)

# ─── TOR ROTATION (unchanged) ───────────────────────────────────────────────
tor_rotation_task = None
tor_enabled_users = 0

async def rotate_tor_identity():
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', 9051)
        await reader.readuntil(b'250 ')
        writer.write(b'AUTHENTICATE ""\r\n')
        await writer.drain()
        resp = await reader.readuntil(b'250 ')
        if b'250' not in resp:
            log.warning("Tor authentication failed")
            writer.close()
            return
        writer.write(b'SIGNAL NEWNYM\r\n')
        await writer.drain()
        resp = await reader.readuntil(b'250 ')
        if b'250' in resp:
            log.info("Tor IP rotated successfully")
        else:
            log.warning("Tor rotation failed")
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        log.warning(f"Tor rotation error: {e}")

async def tor_rotation_loop():
    global tor_rotation_task
    while tor_enabled_users > 0:
        await rotate_tor_identity()
        await asyncio.sleep(120)

def start_tor_rotation():
    global tor_rotation_task
    if tor_rotation_task is None or tor_rotation_task.done():
        tor_rotation_task = asyncio.create_task(tor_rotation_loop())
        log.info("Tor rotation task started")

def stop_tor_rotation():
    global tor_rotation_task
    if tor_rotation_task and not tor_rotation_task.done():
        tor_rotation_task.cancel()
        tor_rotation_task = None
        log.info("Tor rotation task stopped")

# ─── SQL FILTER ENGINE (unchanged) ─────────────────────────────────────────
BLACKLISTED_DOMAINS = {
    "yahoo.uservoice.com", "uservoice.com", "bing.com", "google.com", "googleapis.com",
    "gstatic.com", "youtube.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "pinterest.com", "reddit.com", "wikipedia.org", "amazon.com",
    "amazon.co", "ebay.com", "shopify.com", "wordpress.com", "blogspot.com", "medium.com",
    "github.com", "stackoverflow.com", "w3schools.com", "microsoft.com", "apple.com",
    "cloudflare.com", "yahoo.com", "msn.com", "live.com", "outlook.com", "mercadolibre.com",
    "aliexpress.com", "alibaba.com", "etsy.com", "walmart.com", "bestbuy.com",
    "capitaloneshopping.com", "onetonline.org", "moodle.", "lyrics.fi", "verkkouutiset.fi",
    "iltalehti.fi", "sapo.pt", "iol.pt", "idealo.", "zalando.", "trovaprezzi.",
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
    re.IGNORECASE
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

    has_vuln_ext = any(path.endswith(ext) for ext in VULN_EXTENSIONS)
    if not query:
        return 25 if has_vuln_ext else 5

    score  = 15
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

# ─── ROBUST HTML LINK EXTRACTOR (unchanged) ─────────────────────────────────
class _LinkExtractor(HTMLParser):
    __slots__ = ("links", "_in_cite", "_buf")

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self._in_cite: bool  = False
        self._buf:     list  = []

    def handle_starttag(self, tag: str, attrs):
        if tag == "a":
            adict = dict(attrs)
            for key in ("href", "data-u"):
                val = adict.get(key, "")
                if val.startswith("http"):
                    self.links.append(val)
        elif tag == "cite":
            self._in_cite = True
            self._buf.clear()

    def handle_endtag(self, tag: str):
        if tag == "cite" and self._in_cite:
            text = "".join(self._buf).strip()
            if text.startswith("http"):
                self.links.append(text)
            self._in_cite = False
            self._buf.clear()

    def handle_data(self, data: str):
        if self._in_cite:
            self._buf.append(data)


def _extract_links(html: str) -> list[str]:
    p = _LinkExtractor()
    try:
        p.feed(html)
    except Exception:
        pass
    return p.links


# ─── SEARCH ENGINE FUNCTIONS (unchanged) ────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (iPad; CPU OS 17_3 like Mac OS X) AppleWebKit/605.1.15 Mobile Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/122.0 Mobile Safari/537.36",
]

_BING_NOISE    = re.compile(r"bing\.com", re.IGNORECASE)
_YAHOO_NOISE   = re.compile(r"yimg\.com|yahoo\.com|doubleclick\.net|googleadservices", re.IGNORECASE)
_STATIC_EXT    = re.compile(r"\.(css|js|png|jpg|jpeg|gif|svg|ico|webp|woff2?|ttf|eot)(\?|$)", re.IGNORECASE)
_YAHOO_RU_PATH = re.compile(r"/RU=([^/&]+)")


async def fetch_page_bing(session: aiohttp.ClientSession, dork: str, page: int, max_res: int) -> list:
    try:
        params = {
            "q": dork, "count": min(max_res, 10),
            "first": (page - 1) * 10 + 1, "setlang": "en",
        }
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
        }
        async with session.get(
            "https://www.bing.com/search", params=params,
            headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                return []
            html = await resp.text(errors="replace")

        raw  = _extract_links(html)
        urls = [u for u in raw if u.startswith("http") and not _BING_NOISE.search(u)]
        return list(dict.fromkeys(urls))[:max_res]

    except Exception as e:
        log.warning(f"[BING] page {page} error: {e}")
        return []


async def fetch_page_yahoo(session: aiohttp.ClientSession, dork: str, page: int, max_res: int) -> list:
    try:
        params = {
            "p": dork, "b": (page - 1) * 10 + 1,
            "pz": min(max_res, 10), "vl": "lang_en",
        }
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://search.yahoo.com/",
        }
        async with session.get(
            "https://search.yahoo.com/search", params=params,
            headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                return []
            html = await resp.text(errors="replace")

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
                    m = _YAHOO_RU_PATH.search(parsed.path)
                    if m:
                        real = unquote(m.group(1))
                        if real.startswith(("http://", "https://")):
                            u = real
            if _YAHOO_NOISE.search(u):
                continue
            if _STATIC_EXT.search(u):
                continue
            urls.append(u)

        return list(dict.fromkeys(urls))[:max_res]

    except Exception as e:
        log.warning(f"[YAHOO] page {page} error: {e}")
        return []


# ─── FETCH ALL PAGES (unchanged) ────────────────────────────────────────────
async def fetch_all_pages(session: aiohttp.ClientSession, dork: str, engine: str,
                          pages: list, max_res: int) -> list:
    all_urls: list = []
    empty_counter = 0
    sorted_pages = sorted(pages)

    for page in sorted_pages:
        if engine == "bing":
            urls = await fetch_page_bing(session, dork, page, max_res)
        else:
            urls = await fetch_page_yahoo(session, dork, page, max_res)

        if urls:
            all_urls.extend(urls)
            empty_counter = 0
        else:
            empty_counter += 1
            if empty_counter >= 3:
                log.info(f"[{engine.upper()}] Stopped after page {page} (3 empty pages)")
                break

        if len(sorted_pages) > 1 and page != sorted_pages[-1]:
            await asyncio.sleep(random.uniform(0.3, 0.8))

    return all_urls


# ─── SQLITE RESULT STORE (NEW) ─────────────────────────────────────────────
class SQLiteResultStore:
    """Thread‑safe SQLite store for URL deduplication and final output."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS urls (url TEXT PRIMARY KEY, score INTEGER)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_score ON urls(score DESC)")

    def insert(self, url: str, score: int) -> bool:
        """Insert URL if not exists. Returns True if inserted (new)."""
        try:
            self.conn.execute("INSERT INTO urls (url, score) VALUES (?, ?)", (url, score))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def insert_many(self, items: list[tuple[str, int]]) -> int:
        """Insert multiple (url, score). Returns number of new URLs inserted."""
        new_count = 0
        cur = self.conn.cursor()
        for url, score in items:
            try:
                cur.execute("INSERT INTO urls (url, score) VALUES (?, ?)", (url, score))
                new_count += 1
            except sqlite3.IntegrityError:
                pass
        self.conn.commit()
        return new_count

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0]

    def get_all_sorted(self) -> list[tuple[int, str]]:
        """Return all (score, url) sorted descending by score."""
        return self.conn.execute("SELECT score, url FROM urls ORDER BY score DESC").fetchall()

    def close(self):
        self.conn.close()


# ─── WORKER (UPDATED TO USE BATCH QUEUE) ────────────────────────────────────
async def dork_worker(
    wid: int,
    dork_queue: asyncio.Queue,
    results_queue: asyncio.Queue,
    engines: list,
    pages: list,
    max_res: int,
    session: aiohttp.ClientSession,
    min_score: int,
    stop_ev: asyncio.Event
):
    """
    Pull dork from queue, fetch results, push results (dork, engine, raw_count, scored_urls)
    into results_queue. Always calls queue.task_done() after processing.
    """
    eidx = wid % len(engines)
    while not stop_ev.is_set():
        try:
            dork = await asyncio.wait_for(dork_queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            continue

        engine = engines[eidx % len(engines)]
        eidx += 1
        log.info(f"[W{wid}][{engine.upper()}] {dork[:55]}")

        raw = []
        try:
            raw = await asyncio.wait_for(
                fetch_all_pages(session, dork, engine, pages, max_res),
                timeout=WORKER_FETCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            log.warning(f"[W{wid}] fetch_all_pages timeout after {WORKER_FETCH_TIMEOUT}s: {dork[:55]}")
        except asyncio.CancelledError:
            dork_queue.task_done()
            raise
        except Exception as e:
            log.warning(f"[W{wid}] fetch error: {e}")

        scored = filter_scored(raw, min_score)
        log.info(f"[W{wid}] raw={len(raw)} kept={len(scored)}")

        # Put result into results queue (dork, engine, raw_count, scored list)
        try:
            await results_queue.put((dork, engine, len(raw), scored))
        except asyncio.CancelledError:
            dork_queue.task_done()
            raise

        dork_queue.task_done()

        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        if not raw:
            delay *= 2
        await asyncio.sleep(delay)


# ─── PRODUCER TASK (NEW) ──────────────────────────────────────────────────
async def producer(
    dork_queue: asyncio.Queue,
    dork_stream: AsyncIterator[str],
    total_dorks: int,
    stop_ev: asyncio.Event
):
    """
    Reads dorks from an async iterator and puts them into the dork_queue.
    Stops when stop_ev is set or iterator exhausted.
    """
    dork_count = 0
    async for dork in dork_stream:
        if stop_ev.is_set():
            break
        await dork_queue.put(dork)
        dork_count += 1
    log.info(f"Producer finished: {dork_count} dorks enqueued (total expected: {total_dorks})")


# ─── RESULT WRITER TASK (NEW) ──────────────────────────────────────────────
async def result_writer(
    results_queue: asyncio.Queue,
    store: SQLiteResultStore,
    total_dorks: int,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    status_msg: any,
    start_time: float,
    pages_str: str,
    stop_ev: asyncio.Event,
    last_activity: list,
    consecutive_zero_raw: list,
    worker_tasks: list,
    session_ref: list,
    use_tor: bool,
    engines: list,
    pages: list,
    max_res: int,
    min_score: int
):
    """
    Consumes results from results_queue, inserts them into SQLite,
    updates progress, and sends status updates. Also handles session recycling.
    """
    processed = 0
    total_raw = 0
    new_urls_inserted = 0
    zero_raw_streak = 0
    last_edit = start_time

    while processed < total_dorks and not stop_ev.is_set():
        try:
            dork, engine, raw_count, scored = await asyncio.wait_for(results_queue.get(), timeout=45.0)
        except asyncio.TimeoutError:
            continue

        processed += 1
        total_raw += raw_count
        last_activity[0] = time.time()

        # Handle zero‑raw streak
        if raw_count == 0:
            consecutive_zero_raw[0] += 1
            zero_raw_streak += 1
        else:
            consecutive_zero_raw[0] = 0
            zero_raw_streak = 0

        # Insert URLs
        if scored:
            # Insert in batch (use executor to avoid blocking)
            new_count = await asyncio.to_thread(store.insert_many, scored)
            new_urls_inserted += new_count

        # Session recycling
        if zero_raw_streak >= SESSION_RESET_THRESHOLD:
            log.warning(f"[WRITER] {SESSION_RESET_THRESHOLD} zero‑raw results – recycling session")
            # Cancel workers
            for t in worker_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            worker_tasks.clear()
            # Close old session and create new
            await session_ref[0].close()
            new_session, _ = _make_job_session(use_tor)
            session_ref[0] = new_session
            # Restart workers
            for i in range(len(worker_tasks)):
                t = asyncio.create_task(
                    dork_worker(i, dork_queue, results_queue, engines, pages, max_res,
                                session_ref[0], min_score, stop_ev)
                )
                worker_tasks.append(t)
            zero_raw_streak = 0
            last_activity[0] = time.time()  # reset stall timer

        # Update status message every 4 seconds
        now = time.time()
        if now - last_edit >= 4:
            pct = int(processed / total_dorks * 100) if total_dorks else 0
            bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
            elapsed = int(now - start_time)
            eta = int((elapsed / processed) * (total_dorks - processed)) if processed else 0
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_msg.message_id,
                    text=(
                        f"⚡ PARSING...\n"
                        f"{'━'*30}\n"
                        f"[{bar}] {pct}%\n"
                        f"✅ Dorks   : {processed}/{total_dorks}\n"
                        f"🎯 SQL     : {new_urls_inserted} unique\n"
                        f"🗑 Dropped : {total_raw - new_urls_inserted}\n"
                        f"⏱ {elapsed}s | ETA {eta}s\n"
                        f"{'━'*30}"
                    )
                )
                last_edit = now
            except Exception:
                pass

        results_queue.task_done()

    log.info(f"Result writer finished: processed {processed} dorks, {new_urls_inserted} unique URLs")


# ─── JOB RUNNER (REDESIGNED) ───────────────────────────────────────────────
async def run_dork_job(chat_id: int, dorks_source: AsyncIterator[str], total_dorks: int, context):
    """
    Main job controller with streaming producer, bounded queues, and SQLite dedup.
    """
    sess = get_session(chat_id)
    engines = sess.get("engines", list(ENGINES))
    workers_n = sess.get("workers", WORKERS)
    max_res = sess.get("max_results", MAX_RESULTS)
    pages = sess.get("pages", [1])
    use_tor = sess.get("tor", False)
    min_score = sess.get("min_score", 30)

    # Per-job session
    job_session, _ = _make_job_session(use_tor)
    session_ref = [job_session]  # mutable for watchdog

    # Bounded queues
    dork_queue = asyncio.Queue(maxsize=2000)
    results_queue = asyncio.Queue(maxsize=2000)

    stop_ev = asyncio.Event()
    start_time = time.time()
    pages_str = ", ".join(str(p) for p in pages)

    # Temporary SQLite database
    tmp_db = tempfile.NamedTemporaryFile(prefix=f"dork_{chat_id}_", suffix=".db", delete=False)
    tmp_db.close()
    store = SQLiteResultStore(tmp_db.name)

    # Temporary text file for final output (to be created from SQLite)
    tmp_txt = tempfile.NamedTemporaryFile(prefix=f"dork_{chat_id}_", suffix=".txt", delete=False)
    tmp_txt.close()

    status_msg = await context.bot.send_message(
        chat_id,
        f"🕷 DORK PARSER v17.0 — STREAMING\n"
        f"{'━'*30}\n"
        f"📋 Dorks   : {total_dorks}\n"
        f"📄 Pages   : {pages_str}\n"
        f"⚙️ Workers : {workers_n}\n"
        f"🔍 Engines : {' + '.join(e.upper() for e in engines)}\n"
        f"🛡 Filter  : SQL ≥ {min_score}\n"
        f"🌐 Network : {'🧅 TOR' if use_tor else '🔓 Direct'}\n"
        f"{'━'*30}\n⏳ Starting..."
    )

    # Shared state for watchdog and session recycling
    last_activity = [time.time()]
    consecutive_zero_raw = [0]

    # Create worker tasks
    worker_tasks = []
    for i in range(workers_n):
        t = asyncio.create_task(
            dork_worker(i, dork_queue, results_queue, engines, pages, max_res,
                        session_ref[0], min_score, stop_ev)
        )
        worker_tasks.append(t)

    # Create producer task
    producer_task = asyncio.create_task(producer(dork_queue, dorks_source, total_dorks, stop_ev))

    # Create result writer task
    writer_task = asyncio.create_task(
        result_writer(
            results_queue, store, total_dorks, context, chat_id, status_msg,
            start_time, pages_str, stop_ev, last_activity, consecutive_zero_raw,
            worker_tasks, session_ref, use_tor, engines, pages, max_res, min_score
        )
    )

    # Watchdog task
    async def watchdog():
        while not stop_ev.is_set():
            await asyncio.sleep(WATCHDOG_INTERVAL)
            if stop_ev.is_set():
                break
            elapsed = time.time() - last_activity[0]
            if elapsed > WATCHDOG_STALL_LIMIT:
                log.warning(f"[WATCHDOG][{chat_id}] Stall: no result for {elapsed:.0f}s")
                # Cancel workers
                for t in worker_tasks:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*worker_tasks, return_exceptions=True)
                worker_tasks.clear()
                # Restart workers if queue not empty
                if not dork_queue.empty():
                    log.info(f"[WATCHDOG] Restarting workers after stall")
                    for i in range(workers_n):
                        t = asyncio.create_task(
                            dork_worker(i, dork_queue, results_queue, engines, pages, max_res,
                                        session_ref[0], min_score, stop_ev)
                        )
                        worker_tasks.append(t)
                    last_activity[0] = time.time()

    # Global timeout task
    async def job_timeout():
        await asyncio.sleep(JOB_TIMEOUT)
        log.warning(f"[JOB][{chat_id}] Global timeout ({JOB_TIMEOUT}s) reached")
        stop_ev.set()

    # Start watchdog and timeout
    watchdog_task = asyncio.create_task(watchdog())
    timeout_task = asyncio.create_task(job_timeout())

    try:
        # Wait for producer to finish (it will exit when iterator exhausted)
        await producer_task
        # Then wait for workers to finish (they will exit when queue empty and stop_ev not set)
        # Wait for worker tasks to finish (they will stop when queue empty)
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        # Signal writer to stop (queue will be empty)
        stop_ev.set()
        await writer_task
    except asyncio.CancelledError:
        log.info(f"[JOB] Cancelled for {chat_id}")
        stop_ev.set()
        # Cancel all tasks
        producer_task.cancel()
        for t in worker_tasks:
            t.cancel()
        writer_task.cancel()
        watchdog_task.cancel()
        timeout_task.cancel()
        await asyncio.gather(producer_task, *worker_tasks, writer_task,
                             watchdog_task, timeout_task, return_exceptions=True)
    finally:
        # Finalize: generate output file from SQLite
        unique_count = store.count()
        if unique_count > 0:
            # Write all URLs to temp file with sections
            all_urls = await asyncio.to_thread(store.get_all_sorted)
            with open(tmp_txt.name, 'w', encoding='utf-8') as f:
                f.write(f"# Dork Parser v17.0 — SQL Targeted Results\n")
                f.write(f"# Date  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Dorks : {total_dorks} | Pages : {pages_str}\n")
                f.write(f"# Filter: SQL ≥{min_score}\n")
                f.write("─" * 60 + "\n\n")
                high = [url for score, url in all_urls if score >= 70]
                med  = [url for score, url in all_urls if 40 <= score < 70]
                low  = [url for score, url in all_urls if score < 40]
                if high:
                    f.write("# HIGH VALUE (score 70+)\n")
                    f.write("\n".join(high) + "\n\n")
                if med:
                    f.write("# MEDIUM VALUE (score 40-69)\n")
                    f.write("\n".join(med) + "\n\n")
                if low and min_score < 40:
                    f.write("# LOW VALUE (score < 40)\n")
                    f.write("\n".join(low) + "\n\n")
            # Send file
            with open(tmp_txt.name, 'rb') as f:
                await context.bot.send_document(
                    chat_id, f,
                    filename=f"sql_{total_dorks}dorks_{unique_count}urls.txt",
                    caption=(
                        f"📁 SQL Targets\n"
                        f"🎯 {unique_count} unique URLs\n"
                        f"📋 {total_dorks} dorks | Pages: {pages_str}"
                    )
                )
        else:
            await context.bot.send_message(chat_id, "⚠️ No URLs found.")

        # Cleanup
        store.close()
        os.unlink(tmp_db.name)
        os.unlink(tmp_txt.name)
        await session_ref[0].close()
        active_jobs.pop(chat_id, None)

        # Final status update
        elapsed = int(time.time() - start_time)
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg.message_id,
                text=(
                    f"🏁 JOB COMPLETE!\n"
                    f"{'━'*30}\n"
                    f"📋 Dorks   : {total_dorks}\n"
                    f"📄 Pages   : {pages_str}\n"
                    f"🎯 Unique  : {unique_count}\n"
                    f"⏱ Time    : {elapsed}s\n"
                    f"{'━'*30}"
                )
            )
        except Exception:
            pass


# ─── SESSION FACTORY (unchanged) ────────────────────────────────────────────
def _make_job_session(use_tor: bool):
    """Return (session, connector_owned)."""
    if use_tor:
        try:
            from aiohttp_socks import ProxyConnector
            connector = ProxyConnector.from_url(TOR_PROXY, ssl=False)
            return aiohttp.ClientSession(connector=connector, connector_owner=True), True
        except ImportError:
            log.warning("[TOR] aiohttp_socks not installed, using direct")
    return aiohttp.ClientSession(connector=SHARED_CONNECTOR, connector_owner=False), False


# ─── UI HELPERS (unchanged) ────────────────────────────────────────────────
def get_session(chat_id: int) -> dict:
    if chat_id not in user_sessions:
        user_sessions[chat_id] = dict(DEFAULT_SESSION)
    return user_sessions[chat_id]

def page_keyboard(selected: list) -> InlineKeyboardMarkup:
    rows, row = [], []
    for p in range(1, 71):
        row.append(InlineKeyboardButton(
            f"✅{p}" if p in selected else str(p),
            callback_data=f"pg_{p}"
        ))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("🔁 All (1-70)", callback_data="pg_all"),
        InlineKeyboardButton("❌ Clear",      callback_data="pg_clear"),
        InlineKeyboardButton("✅ Confirm",    callback_data="pg_confirm"),
    ])
    return InlineKeyboardMarkup(rows)


# ─── COMMAND HANDLERS (most unchanged, adapted for streaming) ───────────────
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
        "🕷 DORK PARSER v17.0 — STREAMING SQLITE\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ Streams 200k+ dorks | Bounded queues\n"
        "🗄️ SQLite dedup | Disk‑efficient\n"
        "🛡 SQL filter (adjust with /filter)\n"
        "🧅 Tor auto‑rotation every 2 minutes\n"
        "⏱️ Global job timeout: 30 min\n\n"
        "📌 Commands:\n"
        "  /dork <q>   — single dork\n"
        "  /pages      — pick pages 1-70\n"
        "  /tor        — toggle Tor IP\n"
        "  /filter N   — SQL score filter (0-100)\n"
        "  Upload .txt — bulk mode\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        reply_markup=InlineKeyboardMarkup(kb)
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
    # Create an async iterator that yields this single dork
    async def single_dork_iter():
        yield dork
    total_dorks = 1
    active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, single_dork_iter(), total_dorks, context))

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    doc = update.message.document
    if chat_id in active_jobs and not active_jobs[chat_id].done():
        await update.message.reply_text("⚠️ Job running! Use /stop first.")
        return
    if not doc.file_name.endswith(".txt"):
        await update.message.reply_text("❌ Send a .txt file (one dork per line).")
        return
    await update.message.reply_text("📥 Reading file...")
    try:
        content = await (await context.bot.get_file(doc.file_id)).download_as_bytearray()
        # Count lines and create a streaming iterator
        total_dorks = content.count(b'\n')
        # Use BytesIO to stream lines
        stream = io.BytesIO(content)
        async def dork_stream():
            for line in stream:
                line = line.decode("utf-8", errors="replace").strip()
                if line and not line.startswith("#"):
                    yield line
        active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, dork_stream(), total_dorks, context))
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lines = [l.strip() for l in update.message.text.splitlines()
             if l.strip() and not l.startswith("#")]
    if len(lines) > 1:
        if chat_id in active_jobs and not active_jobs[chat_id].done():
            await update.message.reply_text("⚠️ Job running! /stop first.")
            return
        async def list_stream():
            for d in lines:
                yield d
        total_dorks = len(lines)
        active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, list_stream(), total_dorks, context))
    else:
        await update.message.reply_text("Use /dork <q> or upload .txt\n/pages | /tor | /filter N")

# Other command handlers remain unchanged (cmd_pages, cmd_tor, cmd_filter, etc.)
# They are identical to the original; we'll copy them here for completeness.

async def cmd_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    selected = get_session(chat_id).get("pages", [1])
    await update.message.reply_text(
        f"📄 SELECT PAGES (1–70)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Selected: {', '.join(str(p) for p in selected)}\n"
        f"Tap to toggle, then Confirm.",
        reply_markup=page_keyboard(selected)
    )

async def cmd_tor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global tor_enabled_users
    chat_id = update.effective_chat.id
    sess    = get_session(chat_id)

    if context.args and context.args[0].lower() in ("on", "off"):
        new_val = context.args[0].lower() == "on"
    else:
        new_val = not sess.get("tor", False)

    old_val     = sess.get("tor", False)
    sess["tor"] = new_val

    if new_val and not old_val:
        tor_enabled_users += 1
        if tor_enabled_users == 1:
            start_tor_rotation()
        await update.message.reply_text(
            "🧅 TOR ENABLED\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Tor IP will rotate every 2 minutes.\n"
            "Make sure Tor is running:\n"
            "  sudo apt install tor\n"
            "  sudo service tor start\n\n"
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
        f"🔍 Engines  : {'+'.join(e.upper() for e in s.get('engines', ENGINES))}\n"
        f"📊 Max/Page : {s.get('max_results', MAX_RESULTS)}\n"
        f"🛡 SQL ≥    : {s.get('min_score', 30)}\n"
        f"🧅 Tor      : {'ON' if s.get('tor') else 'OFF'}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"/workers N | /maxres N\n"
        f"/engine X  | /filter N\n"
        f"/pages     | /tor"
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
        choice  = context.args[0].lower()
        engines = {"bing": ["bing"], "yahoo": ["yahoo"]}.get(choice, list(ENGINES))
        get_session(chat_id)["engines"] = engines
        await update.message.reply_text(f"✅ Engines: {'+'.join(e.upper() for e in engines)}")
    except Exception:
        await update.message.reply_text("Usage: /engine bing|yahoo|both")

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in active_jobs:
        task = active_jobs.pop(chat_id)
        task.cancel()
        await update.message.reply_text("🛑 Stopping... Partial results will be sent shortly.")
    else:
        await update.message.reply_text("No active job.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    job     = active_jobs.get(chat_id)
    await update.message.reply_text(
        "⚡ Job RUNNING" if job and not job.done() else "💤 No active job"
    )

# ─── CALLBACK HANDLER (unchanged) ─────────────────────────────────────────
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
                reply_markup=page_keyboard(selected)
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
            f"Engines:{'+'.join(e.upper() for e in sess.get('engines',ENGINES))} "
            f"Score≥{sess.get('min_score',30)} Tor:{'ON' if sess.get('tor') else 'OFF'}"
        ),
        "m_help": (
            "📖 COMMANDS\n━━━━━━━━━━━━━━━━━━━\n"
            "/dork <q>   — single dork\n"
            "/pages      — page selector (1-70)\n"
            "/tor        — toggle Tor (auto-rotate every 2 min)\n"
            "/filter N   — SQL score (0-100)\n"
            "/settings   — config\n"
            "/workers N  — workers 1-50\n"
            "/maxres N   — results/page (1-50)\n"
            "/engine X   — bing|yahoo|both\n"
            "/stop       — stop job (sends partial results)\n"
            "/status     — job status\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Upload .txt for unlimited bulk!\n\n"
            "📁 All results are saved as a file – no chat spam."
        ),
    }

    if data == "m_pages":
        await query.message.reply_text(
            f"📄 SELECT PAGES (1–70)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Selected: {', '.join(str(p) for p in sess.get('pages',[1]))}\nTap to toggle.",
            reply_markup=page_keyboard(sess.get("pages", [1]))
        )
    elif data in replies:
        await query.message.reply_text(replies[data])

# ─── MAIN ───────────────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN:
        log.critical("BOT_TOKEN not set! Add to .env file or environment.")
        raise SystemExit(1)

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

    async def shutdown():
        stop_tor_rotation()
    app.shutdown_handler = shutdown

    log.info("=" * 55)
    log.info("  DORK PARSER v17.0 — STREAMING + SQLITE")
    log.info("=" * 55)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
