"""
╔══════════════════════════════════════════════════════════╗
║   DORK PARSER BOT v16.0 — ENHANCED RELIABILITY          ║
║   Robust HTML parsing | Per-job session | Early dedup   ║
║   Watchdog + auto-restart | Global job timeout          ║
║   Bounded queues | No deadlocks                         ║
║   Pages 1-70 | Tor auto-rotation                       ║
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
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote

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
WORKER_FETCH_TIMEOUT = 180          # seconds per multi-page fetch
WATCHDOG_INTERVAL    = 30           # seconds between watchdog checks
WATCHDOG_STALL_LIMIT = 120           # seconds without result before restart
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

# ─── TOR ROTATION ──────────────────────────────────────────────────────────
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

# ─── SQL FILTER ENGINE ─────────────────────────────────────────────────────
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

# ─── ROBUST HTML LINK EXTRACTOR ─────────────────────────────────────────────
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


# ─── SEARCH ENGINE FUNCTIONS ─────────────────────────────────────────────
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


# ─── FETCH ALL PAGES ─────────────────────────────────────────────────────────
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


# ─── WORKER ──────────────────────────────────────────────────────────────────
async def dork_worker(wid: int,
                      queue: asyncio.Queue,
                      results_q: asyncio.Queue,
                      engines: list,
                      pages: list,
                      max_res: int,
                      session: aiohttp.ClientSession,
                      min_score: int,
                      stop_ev: asyncio.Event):
    """
    Pull dork from queue, fetch results, push to results_q.
    Always calls queue.task_done() after processing.
    """
    eidx = wid % len(engines)
    while not stop_ev.is_set():
        try:
            dork = await asyncio.wait_for(queue.get(), timeout=2.0)
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
            # Ensure we mark task done and push a dummy result so collector doesn't hang
            try:
                results_q.put_nowait((dork, engine, pages, [], 0))
            except asyncio.QueueFull:
                pass
            queue.task_done()
            raise
        except Exception as e:
            log.warning(f"[W{wid}] fetch error: {e}")

        scored = filter_scored(raw, min_score)
        log.info(f"[W{wid}] raw={len(raw)} kept={len(scored)}")

        try:
            results_q.put_nowait((dork, engine, pages, scored, len(raw)))
        except asyncio.QueueFull:
            # Wait a bit if queue is full; shouldn't happen with bounded queue but be safe
            await results_q.put((dork, engine, pages, scored, len(raw)))

        queue.task_done()

        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        if not raw:
            delay *= 2
        await asyncio.sleep(delay)


# ─── JOB RUNNER ──────────────────────────────────────────────────────────────
async def run_dork_job(chat_id: int, dorks: list, context):
    """
    Main job controller with:
        - bounded input/output queues
        - worker tasks
        - result collector with incremental file write
        - watchdog that restarts workers on stall
        - session recycling on many zero‑raw results
        - global job timeout
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
    job_session_ref = [job_session]  # mutable for watchdog

    # Bounded queues to limit memory
    queue = asyncio.Queue(maxsize=len(dorks) * 2)
    for d in dorks:
        await queue.put(d)
    results_q = asyncio.Queue(maxsize=1000)  # cap results

    stop_ev = asyncio.Event()
    total_dorks = len(dorks)
    processed = 0
    seen_urls = set()
    all_scored = []          # (score, url) for final file
    total_raw = 0
    start_time = time.time()
    pages_str = ", ".join(str(p) for p in pages)

    # Temporary file for incremental writing
    tmp_file = tempfile.NamedTemporaryFile(
        mode='w', encoding='utf-8', delete=False,
        prefix=f"dork_{chat_id}_", suffix='.txt'
    )
    tmp_path = tmp_file.name
    tmp_file.write(f"# Dork Parser v16.0 — SQL Targeted Results\n")
    tmp_file.write(f"# Date  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    tmp_file.write(f"# Dorks : {total_dorks} | Pages : {pages_str}\n")
    tmp_file.write(f"# Filter: SQL ≥{min_score}\n")
    tmp_file.write("─" * 60 + "\n\n")
    tmp_file.flush()

    status_msg = await context.bot.send_message(
        chat_id,
        f"🕷 DORK PARSER v16.0 — STARTED\n"
        f"{'━'*30}\n"
        f"📋 Dorks   : {total_dorks}\n"
        f"📄 Pages   : {pages_str}\n"
        f"⚙️ Workers : {workers_n}\n"
        f"🔍 Engines : {' + '.join(e.upper() for e in engines)}\n"
        f"🛡 Filter  : SQL ≥ {min_score}\n"
        f"🌐 Network : {'🧅 TOR' if use_tor else '🔓 Direct'}\n"
        f"{'━'*30}\n⏳ Starting..."
    )

    # Shared state for watchdog
    last_result_ts = [time.time()]          # mutable list
    consecutive_zero_raw = 0
    restarts_without_progress = 0
    max_restarts = 3

    # Helper to write buffered results to file
    batch_buffer = []
    batch_size = 1000
    flush_lock = asyncio.Lock()

    async def flush_buffer():
        nonlocal batch_buffer
        if not batch_buffer:
            return
        async with flush_lock:
            with open(tmp_path, 'a', encoding='utf-8') as f:
                high = [u for sc, u in batch_buffer if sc >= 70]
                medium = [u for sc, u in batch_buffer if 40 <= sc < 70]
                low = [u for sc, u in batch_buffer if sc < 40]
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
            batch_buffer.clear()

    # Worker tasks (mutable list so watchdog can modify)
    worker_tasks = []
    for i in range(workers_n):
        t = asyncio.create_task(
            dork_worker(i, queue, results_q, engines, pages, max_res,
                        job_session_ref[0], min_score, stop_ev)
        )
        worker_tasks.append(t)

    # Watchdog
    async def watchdog():
        nonlocal restarts_without_progress, consecutive_zero_raw
        while not stop_ev.is_set():
            await asyncio.sleep(WATCHDOG_INTERVAL)
            if stop_ev.is_set():
                break

            elapsed = time.time() - last_result_ts[0]
            if elapsed < WATCHDOG_STALL_LIMIT:
                continue

            # Stall detected
            alive = sum(1 for t in worker_tasks if not t.done())
            log.warning(
                f"[WATCHDOG][{chat_id}] Stall: no result for {elapsed:.0f}s, "
                f"alive={alive}/{len(worker_tasks)}"
            )

            # Cancel all workers
            for t in worker_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            worker_tasks.clear()

            if stop_ev.is_set():
                break

            # If queue is empty, we're done
            if queue.empty():
                log.info(f"[WATCHDOG][{chat_id}] Queue empty, not restarting workers")
                break

            # Check if we are making any progress at all
            # If after max_restarts we still get stalls, abort job
            restarts_without_progress += 1
            if restarts_without_progress > max_restarts:
                log.critical(f"[WATCHDOG][{chat_id}] Too many restarts, aborting job")
                stop_ev.set()
                break

            # Restart workers
            log.info(f"[WATCHDOG][{chat_id}] Restarting {workers_n} workers after stall")
            for i in range(workers_n):
                t = asyncio.create_task(
                    dork_worker(i, queue, results_q, engines, pages, max_res,
                                job_session_ref[0], min_score, stop_ev)
                )
                worker_tasks.append(t)
            last_result_ts[0] = time.time()   # reset stall clock
            consecutive_zero_raw = 0          # reset zero counter

    # Result collector
    async def collector():
        nonlocal processed, total_raw, consecutive_zero_raw, restarts_without_progress
        nonlocal batch_buffer, all_scored
        while processed < total_dorks and not stop_ev.is_set():
            try:
                dork, engine, used_pages, scored, raw_count = await asyncio.wait_for(
                    results_q.get(), timeout=45.0
                )
            except asyncio.TimeoutError:
                # No result for 45s – workers might be dead. Watchdog will handle.
                continue

            # Update progress
            processed += 1
            total_raw += raw_count
            last_result_ts[0] = time.time()   # reset stall clock
            restarts_without_progress = 0     # we made progress

            if raw_count == 0:
                consecutive_zero_raw += 1
                if consecutive_zero_raw >= SESSION_RESET_THRESHOLD:
                    log.warning(f"[JOB][{chat_id}] {SESSION_RESET_THRESHOLD} zero‑raw results – recycling session")
                    # Cancel workers, swap session, restart
                    for t in worker_tasks:
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(*worker_tasks, return_exceptions=True)
                    worker_tasks.clear()
                    await job_session_ref[0].close()
                    new_session, _ = _make_job_session(use_tor)
                    job_session_ref[0] = new_session
                    for i in range(workers_n):
                        t = asyncio.create_task(
                            dork_worker(i, queue, results_q, engines, pages, max_res,
                                        job_session_ref[0], min_score, stop_ev)
                        )
                        worker_tasks.append(t)
                    consecutive_zero_raw = 0
                    last_result_ts[0] = time.time()
            else:
                consecutive_zero_raw = 0

            # Deduplicate and accumulate
            for sc, url in scored:
                if url not in seen_urls:
                    seen_urls.add(url)
                    all_scored.append((sc, url))
                    batch_buffer.append((sc, url))

            if len(batch_buffer) >= batch_size:
                await flush_buffer()

            # Update status message every few seconds
            if time.time() - getattr(collector, 'last_edit', 0) > 4:
                pct = int(processed / total_dorks * 100)
                bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
                elapsed = int(time.time() - start_time)
                eta = int((elapsed / processed) * (total_dorks - processed)) if processed else 0
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
                            f"⏱ {elapsed}s | ETA {eta}s\n"
                            f"{'━'*30}"
                        )
                    )
                    collector.last_edit = time.time()
                except Exception:
                    pass

    # Global timeout task
    async def job_timeout():
        await asyncio.sleep(JOB_TIMEOUT)
        log.warning(f"[JOB][{chat_id}] Global timeout ({JOB_TIMEOUT}s) reached")
        stop_ev.set()

    # Start tasks
    collector_task = asyncio.create_task(collector())
    watchdog_task = asyncio.create_task(watchdog())
    timeout_task = asyncio.create_task(job_timeout())

    try:
        # Wait for all workers to finish (they exit when queue empty and stop_ev not set)
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        # When workers are done, queue is empty, collector will finish
        await collector_task
        # Final flush
        await flush_buffer()
    except asyncio.CancelledError:
        log.info(f"[JOB] Cancelled for {chat_id}")
        stop_ev.set()
        # Cancel any remaining tasks
        for t in worker_tasks:
            t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        collector_task.cancel()
        await asyncio.gather(collector_task, return_exceptions=True)
        # Still try to flush what we have
        await flush_buffer()
        raise
    finally:
        # Cancel timeout and watchdog
        timeout_task.cancel()
        watchdog_task.cancel()
        await asyncio.gather(timeout_task, watchdog_task, return_exceptions=True)
        await job_session_ref[0].close()
        active_jobs.pop(chat_id, None)

    # Job finished normally
    elapsed = int(time.time() - start_time)
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
            )
        )
    except Exception:
        pass

    if all_scored:
        with open(tmp_path, 'rb') as f:
            await context.bot.send_document(
                chat_id, f,
                filename=f"sql_{total_dorks}dorks_{unique_cnt}urls.txt",
                caption=(
                    f"📁 SQL Targets\n"
                    f"🎯 {unique_cnt} unique URLs | 🗑 {total_raw - unique_cnt} junk\n"
                    f"📋 {total_dorks} dorks | Pages: {pages_str}"
                )
            )
    os.unlink(tmp_path)


# ─── SESSION FACTORY ─────────────────────────────────────────────────────────
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


# ─── UI HELPERS ────────────────────────────────────────────────────────────
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


# ─── COMMAND HANDLERS ───────────────────────────────────────────────────────
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
        "🕷 DORK PARSER v16.0 — ENHANCED RELIABILITY\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ Workers | Sequential pages | Stop on 3 empty pages\n"
        "🔁 Auto‑restart on stall | Session reset on zero results\n"
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
    s = get_session(chat_id)
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

# ─── DOCUMENT & TEXT HANDLERS ───────────────────────────────────────────────
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

# ─── CALLBACK HANDLER ───────────────────────────────────────────────────────
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

# ─── MAIN ────────────────────────────────────────────────────────────────────
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
    log.info("  DORK PARSER v16.0 — ENHANCED RELIABILITY")
    log.info("=" * 55)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
