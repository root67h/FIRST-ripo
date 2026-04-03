"""
╔══════════════════════════════════════════════════════════╗
║   DORK PARSER BOT v17.0 — PARALLEL CHUNK ARCHITECTURE   ║
║   N isolated sessions | Per-chunk worker pools          ║
║   Retry + exponential backoff | Degraded HTML detection  ║
║   Auto-slowdown on empty rate | Global dedup + merge     ║
║   Bounded queues | Staggered chunk starts                ║
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
BOT_TOKEN             = os.environ.get("BOT_TOKEN", "")
N_CHUNKS              = int(os.environ.get("N_CHUNKS", 2))            # parallel chunk tasks
WORKERS_PER_CHUNK     = int(os.environ.get("WORKERS_PER_CHUNK", 8))   # workers per chunk (default)
MAX_WORKERS_PER_CHUNK = 20                                             # hard cap
MIN_DELAY             = float(os.environ.get("MIN_DELAY", 1.5))       # min inter-request delay (s)
MAX_DELAY             = float(os.environ.get("MAX_DELAY", 3.0))       # max inter-request delay (s)
MAX_RESULTS           = int(os.environ.get("MAX_RESULTS", 10))
TOR_PROXY             = os.environ.get("TOR_PROXY", "socks5://127.0.0.1:9050")
OUTPUT_DIR            = Path("results")
OUTPUT_DIR.mkdir(exist_ok=True)

ENGINES   = ["bing", "yahoo"]
MAX_PAGES = 70

# ─── RELIABILITY CONSTANTS ──────────────────────────────────────────────────
WORKER_FETCH_TIMEOUT  = 120      # seconds per multi-page fetch
JOB_TIMEOUT           = 30 * 60  # 30-minute global job timeout
MAX_RETRIES           = 3        # retry attempts per page fetch
CHUNK_STALL_TIMEOUT   = 60.0     # seconds without a result before checking worker health
EMPTY_RATE_SLOWDOWN   = 0.50     # empty-result fraction that triggers auto-slowdown
EMPTY_RATE_RECOVER    = 0.30     # fraction below which slowdown is lifted
CHUNK_STAGGER_DELAY   = (0.8, 2.5)  # (min, max) seconds between chunk starts

DEFAULT_SESSION = {
    "workers":     WORKERS_PER_CHUNK,
    "chunks":      N_CHUNKS,
    "engines":     list(ENGINES),
    "max_results": MAX_RESULTS,
    "pages":       [1],
    "tor":         False,
    "min_score":   30,
}

user_sessions: dict = {}
active_jobs:   dict = {}

# ─── TOR ROTATION ──────────────────────────────────────────────────────────
_tor_rotation_task = None
tor_enabled_users  = 0


async def rotate_tor_identity() -> None:
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", 9051)
        await reader.readuntil(b"250 ")
        writer.write(b'AUTHENTICATE ""\r\n')
        await writer.drain()
        resp = await reader.readuntil(b"250 ")
        if b"250" not in resp:
            log.warning("Tor authentication failed")
            writer.close()
            return
        writer.write(b"SIGNAL NEWNYM\r\n")
        await writer.drain()
        resp = await reader.readuntil(b"250 ")
        log.info("Tor IP rotated") if b"250" in resp else log.warning("Tor rotation failed")
        writer.close()
        await writer.wait_closed()
    except Exception as exc:
        log.warning(f"Tor rotation error: {exc}")


async def _tor_rotation_loop() -> None:
    while tor_enabled_users > 0:
        await rotate_tor_identity()
        await asyncio.sleep(120)


def start_tor_rotation() -> None:
    global _tor_rotation_task
    if _tor_rotation_task is None or _tor_rotation_task.done():
        _tor_rotation_task = asyncio.create_task(_tor_rotation_loop())
        log.info("Tor rotation task started")


def stop_tor_rotation() -> None:
    global _tor_rotation_task
    if _tor_rotation_task and not _tor_rotation_task.done():
        _tor_rotation_task.cancel()
        _tor_rotation_task = None
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

    query        = parsed.query
    path         = parsed.path.lower()
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
        self._in_cite: bool   = False
        self._buf:     list   = []

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


# ─── DEGRADED RESPONSE DETECTION ────────────────────────────────────────────
_CAPTCHA_RE = re.compile(
    r"captcha|are you a robot|unusual traffic|access denied|"
    r"verify you are human|please verify|too many requests|"
    r"blocked|forbidden|rate limit|temporarily unavailable",
    re.IGNORECASE,
)


def _is_degraded(html: str, engine: str) -> bool:
    """Return True if the response is empty, CAPTCHA-blocked, or lacks result markers."""
    if len(html) < 400:
        return True
    if _CAPTCHA_RE.search(html[:4096]):
        return True
    if engine == "bing" and 'id="b_results"' not in html and "b_algo" not in html:
        return True
    if engine == "yahoo" and 'id="results"' not in html and "searchCenterMiddle" not in html:
        return True
    return False


# ─── SESSION FACTORY ─────────────────────────────────────────────────────────
def _make_isolated_session(use_tor: bool = False) -> aiohttp.ClientSession:
    """
    Always returns a fully isolated ClientSession with its own private TCPConnector.
    No connector is ever shared between chunks or sessions.
    """
    if use_tor:
        try:
            from aiohttp_socks import ProxyConnector
            connector = ProxyConnector.from_url(TOR_PROXY, ssl=False)
            return aiohttp.ClientSession(connector=connector, connector_owner=True)
        except ImportError:
            log.warning("[SESSION] aiohttp_socks not installed — falling back to direct")

    connector = aiohttp.TCPConnector(
        ssl=False,
        limit=20,            # per-session limit, never shared
        limit_per_host=5,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    return aiohttp.ClientSession(connector=connector, connector_owner=True)


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


async def fetch_page_bing(
    session: aiohttp.ClientSession,
    dork: str, page: int, max_res: int,
    chunk_id: int = 0,
) -> tuple[list, bool]:
    """
    Fetch one Bing result page with retry + exponential backoff.
    Returns (urls, degraded) where degraded=True means a soft failure.
    """
    params = {
        "q": dork, "count": min(max_res, 10),
        "first": (page - 1) * 10 + 1, "setlang": "en",
    }

    for attempt in range(MAX_RETRIES):
        headers = {
            "User-Agent":      random.choice(USER_AGENTS),
            "Accept":          "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
        }
        try:
            async with session.get(
                "https://www.bing.com/search",
                params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                status   = resp.status
                html     = await resp.text(errors="replace")
                size_kb  = len(html) / 1024

                log.debug(
                    f"[C{chunk_id}][BING] p{page} attempt={attempt+1} "
                    f"status={status} size={size_kb:.1f}KB"
                )

                if status == 429:
                    backoff = (2 ** attempt) * random.uniform(4.0, 8.0)
                    log.warning(
                        f"[C{chunk_id}][BING] p{page} rate-limited (429) — "
                        f"backoff {backoff:.1f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue

                if status != 200:
                    log.warning(f"[C{chunk_id}][BING] p{page} non-200 status={status}")
                    return [], False

                if _is_degraded(html, "bing"):
                    log.warning(
                        f"[C{chunk_id}][BING] p{page} degraded response "
                        f"(size={size_kb:.1f}KB)"
                    )
                    if attempt < MAX_RETRIES - 1:
                        backoff = (2 ** attempt) * random.uniform(2.0, 5.0)
                        await asyncio.sleep(backoff)
                        continue
                    return [], True  # exhausted retries — signal degraded

                raw  = _extract_links(html)
                urls = [u for u in raw if u.startswith("http") and not _BING_NOISE.search(u)]
                urls = list(dict.fromkeys(urls))[:max_res]
                log.info(
                    f"[C{chunk_id}][BING] p{page} → {len(urls)} URLs "
                    f"(size={size_kb:.1f}KB, attempt={attempt+1})"
                )
                return urls, False

        except asyncio.TimeoutError:
            backoff = (2 ** attempt) * random.uniform(2.0, 4.0)
            log.warning(
                f"[C{chunk_id}][BING] p{page} timeout attempt={attempt+1} "
                f"— retry in {backoff:.1f}s"
            )
            await asyncio.sleep(backoff)

        except aiohttp.ClientError as exc:
            backoff = (2 ** attempt) * random.uniform(2.0, 4.0)
            log.warning(
                f"[C{chunk_id}][BING] p{page} ClientError={exc} attempt={attempt+1} "
                f"— retry in {backoff:.1f}s"
            )
            await asyncio.sleep(backoff)

        except Exception as exc:
            log.error(f"[C{chunk_id}][BING] p{page} unexpected error: {exc}")
            return [], False

    log.warning(f"[C{chunk_id}][BING] p{page} all {MAX_RETRIES} attempts exhausted")
    return [], True


async def fetch_page_yahoo(
    session: aiohttp.ClientSession,
    dork: str, page: int, max_res: int,
    chunk_id: int = 0,
) -> tuple[list, bool]:
    """
    Fetch one Yahoo result page with retry + exponential backoff.
    Returns (urls, degraded).
    """
    params = {
        "p": dork, "b": (page - 1) * 10 + 1,
        "pz": min(max_res, 10), "vl": "lang_en",
    }

    for attempt in range(MAX_RETRIES):
        headers = {
            "User-Agent":      random.choice(USER_AGENTS),
            "Accept":          "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         "https://search.yahoo.com/",
        }
        try:
            async with session.get(
                "https://search.yahoo.com/search",
                params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                status  = resp.status
                html    = await resp.text(errors="replace")
                size_kb = len(html) / 1024

                log.debug(
                    f"[C{chunk_id}][YAHOO] p{page} attempt={attempt+1} "
                    f"status={status} size={size_kb:.1f}KB"
                )

                if status == 429:
                    backoff = (2 ** attempt) * random.uniform(4.0, 8.0)
                    log.warning(
                        f"[C{chunk_id}][YAHOO] p{page} rate-limited (429) — "
                        f"backoff {backoff:.1f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue

                if status != 200:
                    log.warning(f"[C{chunk_id}][YAHOO] p{page} non-200 status={status}")
                    return [], False

                if _is_degraded(html, "yahoo"):
                    log.warning(
                        f"[C{chunk_id}][YAHOO] p{page} degraded response "
                        f"(size={size_kb:.1f}KB)"
                    )
                    if attempt < MAX_RETRIES - 1:
                        backoff = (2 ** attempt) * random.uniform(2.0, 5.0)
                        await asyncio.sleep(backoff)
                        continue
                    return [], True

                raw  = _extract_links(html)
                urls = []
                for u in raw:
                    if not u.startswith("http"):
                        continue
                    if "r.search.yahoo.com" in u or "/r/" in u:
                        parsed = urlparse(u)
                        qs     = parse_qs(parsed.query)
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
                    if _YAHOO_NOISE.search(u) or _STATIC_EXT.search(u):
                        continue
                    urls.append(u)

                urls = list(dict.fromkeys(urls))[:max_res]
                log.info(
                    f"[C{chunk_id}][YAHOO] p{page} → {len(urls)} URLs "
                    f"(size={size_kb:.1f}KB, attempt={attempt+1})"
                )
                return urls, False

        except asyncio.TimeoutError:
            backoff = (2 ** attempt) * random.uniform(2.0, 4.0)
            log.warning(
                f"[C{chunk_id}][YAHOO] p{page} timeout attempt={attempt+1} "
                f"— retry in {backoff:.1f}s"
            )
            await asyncio.sleep(backoff)

        except aiohttp.ClientError as exc:
            backoff = (2 ** attempt) * random.uniform(2.0, 4.0)
            log.warning(
                f"[C{chunk_id}][YAHOO] p{page} ClientError={exc} attempt={attempt+1} "
                f"— retry in {backoff:.1f}s"
            )
            await asyncio.sleep(backoff)

        except Exception as exc:
            log.error(f"[C{chunk_id}][YAHOO] p{page} unexpected error: {exc}")
            return [], False

    log.warning(f"[C{chunk_id}][YAHOO] p{page} all {MAX_RETRIES} attempts exhausted")
    return [], True


# ─── FETCH ALL PAGES ─────────────────────────────────────────────────────────
async def fetch_all_pages(
    session: aiohttp.ClientSession,
    dork: str, engine: str,
    pages: list, max_res: int,
    chunk_id: int = 0,
) -> tuple[list, int]:
    """
    Fetch multiple pages for a single dork.
    Returns (all_urls, degraded_page_count).
    Stops early after 3 consecutive empty pages.
    """
    all_urls      = []
    empty_streak  = 0
    degraded_total = 0
    sorted_pages  = sorted(pages)

    for page in sorted_pages:
        if engine == "bing":
            urls, degraded = await fetch_page_bing(session, dork, page, max_res, chunk_id)
        else:
            urls, degraded = await fetch_page_yahoo(session, dork, page, max_res, chunk_id)

        if degraded:
            degraded_total += 1

        if urls:
            all_urls.extend(urls)
            empty_streak = 0
        else:
            empty_streak += 1
            if empty_streak >= 3:
                log.info(
                    f"[C{chunk_id}][{engine.upper()}] "
                    f"Stopping at p{page} — 3 consecutive empty pages"
                )
                break

        # Randomised inter-page delay (prevents synchronized bursts)
        if len(sorted_pages) > 1 and page != sorted_pages[-1]:
            await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    return all_urls, degraded_total


# ─── WORKER ──────────────────────────────────────────────────────────────────
async def dork_worker(
    wid: int,
    chunk_id: int,
    queue: asyncio.Queue,
    results_q: asyncio.Queue,
    engines: list,
    pages: list,
    max_res: int,
    session: aiohttp.ClientSession,
    min_score: int,
    stop_ev: asyncio.Event,
    slowdown_ev: asyncio.Event,
) -> None:
    """
    Pull dorks from queue, fetch results, push to results_q.
    Applies randomised delays (1.5–3 s base) and auto-slows on empty streaks.
    Always calls queue.task_done().
    """
    eidx        = wid % len(engines)
    empty_streak = 0

    while not stop_ev.is_set():
        try:
            dork = await asyncio.wait_for(queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            continue

        engine = engines[eidx % len(engines)]
        eidx  += 1
        log.info(f"[C{chunk_id}][W{wid}][{engine.upper()}] {dork[:55]}")

        raw           = []
        degraded_cnt  = 0

        try:
            raw, degraded_cnt = await asyncio.wait_for(
                fetch_all_pages(session, dork, engine, pages, max_res, chunk_id),
                timeout=WORKER_FETCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            log.warning(f"[C{chunk_id}][W{wid}] fetch_all_pages timeout: {dork[:55]}")
        except asyncio.CancelledError:
            try:
                results_q.put_nowait((dork, engine, [], 0, 0))
            except asyncio.QueueFull:
                pass
            queue.task_done()
            raise
        except Exception as exc:
            log.warning(f"[C{chunk_id}][W{wid}] fetch error: {exc}")

        scored = filter_scored(raw, min_score)
        log.info(
            f"[C{chunk_id}][W{wid}] "
            f"raw={len(raw)} kept={len(scored)} degraded={degraded_cnt}"
        )

        try:
            results_q.put_nowait((dork, engine, scored, len(raw), degraded_cnt))
        except asyncio.QueueFull:
            await results_q.put((dork, engine, scored, len(raw), degraded_cnt))

        queue.task_done()

        # ── Adaptive delay ───────────────────────────────────────────────────
        delay = random.uniform(MIN_DELAY, MAX_DELAY)

        if not raw:
            empty_streak += 1
            if empty_streak >= 3:
                # Auto-slowdown: adds 2s per empty in streak, capped at 15s
                extra = min(empty_streak * 2.0, 15.0)
                log.info(
                    f"[C{chunk_id}][W{wid}] Auto-slowdown +"
                    f"{extra:.1f}s (empty_streak={empty_streak})"
                )
                delay += extra
        else:
            empty_streak = 0

        # Chunk-level slowdown signal (triggered by high empty-rate)
        if slowdown_ev.is_set():
            delay += random.uniform(2.0, 5.0)

        await asyncio.sleep(delay)


# ─── CHUNK RUNNER ────────────────────────────────────────────────────────────
async def run_chunk(
    chunk_id: int,
    dorks: list,
    engines: list,
    pages: list,
    max_res: int,
    use_tor: bool,
    min_score: int,
    workers_n: int,
    progress_q: asyncio.Queue,
    global_stop_ev: asyncio.Event,
) -> dict:
    """
    Run one isolated chunk:
      - creates its own aiohttp session + TCPConnector
      - spawns workers_n workers on a bounded queue
      - tracks empty-rate and triggers slowdown_ev when needed
      - reports per-dork progress to the shared progress_q
      - returns a summary dict with scored urls and stats
    """
    session    = _make_isolated_session(use_tor)
    queue      = asyncio.Queue(maxsize=len(dorks) * 2)
    results_q  = asyncio.Queue(maxsize=500)
    stop_ev    = asyncio.Event()
    slowdown_ev = asyncio.Event()

    for d in dorks:
        await queue.put(d)

    total          = len(dorks)
    processed      = 0
    empty_count    = 0
    chunk_raw      = 0
    chunk_degraded = 0
    chunk_scored   = []

    log.info(
        f"[C{chunk_id}] Starting — "
        f"{total} dorks | {workers_n} workers | engines={engines}"
    )

    # Propagate global cancellation into this chunk
    async def _watch_global() -> None:
        while not stop_ev.is_set():
            if global_stop_ev.is_set():
                stop_ev.set()
            await asyncio.sleep(0.5)

    worker_tasks = [
        asyncio.create_task(
            dork_worker(
                i, chunk_id, queue, results_q, engines, pages,
                max_res, session, min_score, stop_ev, slowdown_ev,
            )
        )
        for i in range(workers_n)
    ]
    global_watcher = asyncio.create_task(_watch_global())

    try:
        while processed < total and not stop_ev.is_set():
            try:
                dork, engine, scored, raw_cnt, deg_cnt = await asyncio.wait_for(
                    results_q.get(), timeout=CHUNK_STALL_TIMEOUT
                )
            except asyncio.TimeoutError:
                if all(t.done() for t in worker_tasks):
                    log.warning(
                        f"[C{chunk_id}] All workers done with "
                        f"{total - processed} dorks unaccounted — exiting early"
                    )
                    break
                continue

            processed      += 1
            chunk_raw      += raw_cnt
            chunk_degraded += deg_cnt

            if raw_cnt == 0:
                empty_count += 1
            
            chunk_scored.extend(scored)

            # ── Empty-rate auto-slowdown toggle ──────────────────────────────
            empty_rate = empty_count / max(processed, 1)
            if empty_rate >= EMPTY_RATE_SLOWDOWN and not slowdown_ev.is_set():
                log.warning(
                    f"[C{chunk_id}] Empty rate {empty_rate:.0%} — "
                    f"enabling chunk slowdown"
                )
                slowdown_ev.set()
            elif empty_rate < EMPTY_RATE_RECOVER and slowdown_ev.is_set():
                log.info(
                    f"[C{chunk_id}] Empty rate recovered to "
                    f"{empty_rate:.0%} — disabling slowdown"
                )
                slowdown_ev.clear()

            # Report progress to main job for status updates
            try:
                progress_q.put_nowait({
                    "chunk_id":  chunk_id,
                    "processed": processed,
                    "total":     total,
                    "raw":       raw_cnt,
                    "kept":      len(scored),
                })
            except asyncio.QueueFull:
                pass

        # Drain remaining workers gracefully
        for t in worker_tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    except asyncio.CancelledError:
        stop_ev.set()
        for t in worker_tasks:
            t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        raise
    finally:
        global_watcher.cancel()
        await asyncio.gather(global_watcher, return_exceptions=True)
        await session.close()

    success_rate = (processed - empty_count) / max(processed, 1)
    log.info(
        f"[C{chunk_id}] Done — processed={processed}/{total} "
        f"raw={chunk_raw} kept={len(chunk_scored)} "
        f"degraded={chunk_degraded} success_rate={success_rate:.0%}"
    )

    return {
        "chunk_id":      chunk_id,
        "scored":        chunk_scored,
        "raw_count":     chunk_raw,
        "degraded_count": chunk_degraded,
        "processed":     processed,
        "empty_count":   empty_count,
    }


# ─── JOB RUNNER ──────────────────────────────────────────────────────────────
async def run_dork_job(chat_id: int, dorks: list, context) -> None:
    """
    Orchestrates the full job:
      1. Splits dorks into N isolated chunks.
      2. Stagger-starts each chunk to avoid synchronised bursts.
      3. Runs all chunks concurrently via asyncio.gather.
      4. Aggregates progress and updates Telegram status in real-time.
      5. Globally deduplicates and merges all chunk results.
      6. Writes a scored output file and sends it to the user.
    """
    sess      = get_session(chat_id)
    engines   = sess.get("engines", list(ENGINES))
    workers_n = min(sess.get("workers", WORKERS_PER_CHUNK), MAX_WORKERS_PER_CHUNK)
    max_res   = sess.get("max_results", MAX_RESULTS)
    pages     = sess.get("pages", [1])
    use_tor   = sess.get("tor", False)
    min_score = sess.get("min_score", 30)
    n_chunks  = max(1, sess.get("chunks", N_CHUNKS))

    total_dorks = len(dorks)
    pages_str   = ", ".join(str(p) for p in pages)
    start_time  = time.time()

    # ── Split dorks into balanced chunks ─────────────────────────────────────
    chunk_size    = max(1, -(-total_dorks // n_chunks))  # ceiling division
    chunks        = [dorks[i : i + chunk_size] for i in range(0, total_dorks, chunk_size)]
    actual_chunks = len(chunks)

    log.info(
        f"[JOB][{chat_id}] Starting: {total_dorks} dorks → "
        f"{actual_chunks} chunks × {workers_n} workers/chunk | "
        f"delay={MIN_DELAY}–{MAX_DELAY}s"
    )

    # ── Temporary output file ─────────────────────────────────────────────────
    tmp_file = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", delete=False,
        prefix=f"dork_{chat_id}_", suffix=".txt",
    )
    tmp_path = tmp_file.name
    tmp_file.write(f"# Dork Parser v17.0 — SQL Targeted Results\n")
    tmp_file.write(f"# Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    tmp_file.write(f"# Dorks  : {total_dorks} | Pages: {pages_str}\n")
    tmp_file.write(f"# Filter : SQL ≥{min_score} | Chunks: {actual_chunks}\n")
    tmp_file.write("─" * 60 + "\n\n")
    tmp_file.close()

    # ── Initial status message ────────────────────────────────────────────────
    status_msg = await context.bot.send_message(
        chat_id,
        f"🕷 DORK PARSER v17.0 — STARTED\n"
        f"{'━'*30}\n"
        f"📋 Dorks   : {total_dorks}\n"
        f"📄 Pages   : {pages_str}\n"
        f"⚡ Chunks  : {actual_chunks} (isolated sessions)\n"
        f"⚙️ Workers : {workers_n}/chunk\n"
        f"🔍 Engines : {' + '.join(e.upper() for e in engines)}\n"
        f"🛡 Filter  : SQL ≥{min_score}\n"
        f"🌐 Network : {'🧅 TOR' if use_tor else '🔓 Direct'}\n"
        f"{'━'*30}\n⏳ Starting chunks...",
    )

    # ── Shared state ──────────────────────────────────────────────────────────
    global_stop_ev = asyncio.Event()
    progress_q: asyncio.Queue = asyncio.Queue(maxsize=total_dorks * 2)

    # Per-chunk counters for the live status bar
    chunk_counters = {
        i: {"processed": 0, "total": len(chunks[i])}
        for i in range(actual_chunks)
    }
    agg_raw  = [0]
    agg_kept = [0]

    # ── Live status updater ───────────────────────────────────────────────────
    last_edit = [0.0]
    total_processed = [0]

    async def _status_updater() -> None:
        while not global_stop_ev.is_set():
            # Drain all pending progress events
            drained = False
            while True:
                try:
                    ev = progress_q.get_nowait()
                    cid = ev["chunk_id"]
                    chunk_counters[cid]["processed"] = ev["processed"]
                    agg_raw[0]  += ev["raw"]
                    agg_kept[0] += ev["kept"]
                    total_processed[0] += 1
                    drained = True
                except asyncio.QueueEmpty:
                    break

            if drained and time.time() - last_edit[0] > 4.0:
                proc = total_processed[0]
                pct  = int(proc / total_dorks * 100) if total_dorks else 100
                bar  = "█" * (pct // 10) + "░" * (10 - pct // 10)
                elapsed = int(time.time() - start_time)
                eta  = int((elapsed / proc) * (total_dorks - proc)) if proc else 0
                cinfo = " | ".join(
                    f"C{i}:{chunk_counters[i]['processed']}/{chunk_counters[i]['total']}"
                    for i in range(actual_chunks)
                )
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_msg.message_id,
                        text=(
                            f"⚡ PARSING... [{actual_chunks} parallel chunks]\n"
                            f"{'━'*30}\n"
                            f"[{bar}] {pct}%\n"
                            f"✅ Done    : {proc}/{total_dorks}\n"
                            f"🎯 SQL     : {agg_kept[0]}\n"
                            f"🗑 Raw drop: {agg_raw[0] - agg_kept[0]}\n"
                            f"⏱ {elapsed}s | ETA {eta}s\n"
                            f"📦 {cinfo}\n"
                            f"{'━'*30}"
                        ),
                    )
                    last_edit[0] = time.time()
                except Exception:
                    pass

            await asyncio.sleep(0.5)

    # ── Global timeout ────────────────────────────────────────────────────────
    async def _job_timeout() -> None:
        await asyncio.sleep(JOB_TIMEOUT)
        log.warning(f"[JOB][{chat_id}] Global timeout ({JOB_TIMEOUT}s) — aborting")
        global_stop_ev.set()

    status_task  = asyncio.create_task(_status_updater())
    timeout_task = asyncio.create_task(_job_timeout())

    # ── Launch all chunks (staggered to prevent synchronised bursts) ──────────
    chunk_results = []
    try:
        chunk_tasks = []
        for i, chunk_dorks in enumerate(chunks):
            if i > 0:
                stagger = random.uniform(*CHUNK_STAGGER_DELAY)
                log.info(f"[JOB][{chat_id}] Staggering chunk C{i} by {stagger:.1f}s")
                await asyncio.sleep(stagger)

            task = asyncio.create_task(
                run_chunk(
                    chunk_id=i,
                    dorks=chunk_dorks,
                    engines=engines,
                    pages=pages,
                    max_res=max_res,
                    use_tor=use_tor,
                    min_score=min_score,
                    workers_n=workers_n,
                    progress_q=progress_q,
                    global_stop_ev=global_stop_ev,
                )
            )
            chunk_tasks.append(task)

        chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

    except asyncio.CancelledError:
        log.info(f"[JOB][{chat_id}] Job cancelled")
        global_stop_ev.set()
        for t in chunk_tasks:
            t.cancel()
        await asyncio.gather(*chunk_tasks, return_exceptions=True)
        raise
    finally:
        global_stop_ev.set()
        timeout_task.cancel()
        status_task.cancel()
        await asyncio.gather(timeout_task, status_task, return_exceptions=True)
        active_jobs.pop(chat_id, None)

    # ── Merge + global deduplication ─────────────────────────────────────────
    seen_urls   : set  = set()
    all_scored  : list = []
    total_raw        = 0
    total_degraded   = 0
    failed_chunks    = 0

    for result in chunk_results:
        if isinstance(result, Exception):
            log.error(f"[JOB][{chat_id}] Chunk raised: {result}")
            failed_chunks += 1
            continue
        for sc, url in result["scored"]:
            if url not in seen_urls:
                seen_urls.add(url)
                all_scored.append((sc, url))
        total_raw      += result["raw_count"]
        total_degraded += result["degraded_count"]

    all_scored.sort(reverse=True)
    unique_cnt   = len(all_scored)
    elapsed      = int(time.time() - start_time)
    success_rate = (total_raw - (total_raw - unique_cnt)) / max(total_raw, 1)

    log.info(
        f"[JOB][{chat_id}] COMPLETE — dorks={total_dorks} raw={total_raw} "
        f"unique={unique_cnt} degraded={total_degraded} "
        f"failed_chunks={failed_chunks} elapsed={elapsed}s "
        f"success_rate={success_rate:.1%}"
    )

    # ── Write scored output ───────────────────────────────────────────────────
    high   = [(sc, u) for sc, u in all_scored if sc >= 70]
    medium = [(sc, u) for sc, u in all_scored if 40 <= sc < 70]
    low    = [(sc, u) for sc, u in all_scored if sc < 40]

    with open(tmp_path, "a", encoding="utf-8") as f:
        if high:
            f.write(f"# ── HIGH VALUE (score ≥70) — {len(high)} URLs\n")
            for sc, u in high:
                f.write(f"{u}\n")
        if medium:
            f.write(f"\n# ── MEDIUM VALUE (score 40–69) — {len(medium)} URLs\n")
            for sc, u in medium:
                f.write(f"{u}\n")
        if low and min_score < 40:
            f.write(f"\n# ── LOW VALUE (score <40) — {len(low)} URLs\n")
            for sc, u in low:
                f.write(f"{u}\n")

    # ── Final status message ──────────────────────────────────────────────────
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg.message_id,
            text=(
                f"🏁 JOB COMPLETE!\n"
                f"{'━'*30}\n"
                f"📋 Dorks    : {total_dorks}\n"
                f"📄 Pages    : {pages_str}\n"
                f"⚡ Chunks   : {actual_chunks}\n"
                f"🔍 Raw      : {total_raw}\n"
                f"🎯 SQL      : {unique_cnt} unique URLs\n"
                f"🗑 Dropped  : {total_raw - unique_cnt} junk\n"
                f"⚠️ Degraded : {total_degraded} pages\n"
                f"📊 Hit rate : {success_rate:.0%}\n"
                f"⏱ Time     : {elapsed}s\n"
                f"{'━'*30}"
            ),
        )
    except Exception:
        pass

    if all_scored:
        with open(tmp_path, "rb") as f:
            await context.bot.send_document(
                chat_id, f,
                filename=f"sql_{total_dorks}dorks_{unique_cnt}urls.txt",
                caption=(
                    f"📁 SQL Targets\n"
                    f"🎯 {unique_cnt} unique | 🗑 {total_raw - unique_cnt} junk\n"
                    f"📋 {total_dorks} dorks | Pages: {pages_str} | "
                    f"⚡ {actual_chunks} chunks"
                ),
            )
    else:
        await context.bot.send_message(
            chat_id,
            "⚠️ No URLs matched the filter criteria.\n"
            "Try lowering /filter or adding more pages.",
        )

    try:
        os.unlink(tmp_path)
    except OSError:
        pass


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
        "🕷 DORK PARSER v17.0 — PARALLEL CHUNKS\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ N isolated sessions | Per-chunk worker pools\n"
        "🔁 Retry + backoff | Degraded HTML detection\n"
        "🛡 SQL filter | Auto-slowdown on empty rate\n"
        "🧅 Tor auto-rotation every 2 minutes\n"
        "⏱️ Global job timeout: 30 min\n\n"
        "📌 Commands:\n"
        "  /dork <q>   — single dork\n"
        "  /pages      — pick pages 1-70\n"
        "  /workers N  — workers per chunk (1-20)\n"
        "  /chunks N   — parallel chunk count (1-8)\n"
        "  /tor        — toggle Tor IP\n"
        "  /filter N   — SQL score filter (0-100)\n"
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
        f"📄 Pages: {', '.join(str(p) for p in s.get('pages', [1]))}"
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
            "  sudo apt install tor && sudo service tor start\n\n"
            "⚠️ Speed will be slower."
        )
    elif not new_val and old_val:
        tor_enabled_users = max(0, tor_enabled_users - 1)
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
        f"⚡ Chunks   : {s.get('chunks', N_CHUNKS)} parallel sessions\n"
        f"🔧 Workers  : {s.get('workers', WORKERS_PER_CHUNK)}/chunk (max {MAX_WORKERS_PER_CHUNK})\n"
        f"📄 Pages    : {', '.join(str(p) for p in s.get('pages', [1]))} (1–70)\n"
        f"🔍 Engines  : {'+'.join(e.upper() for e in s.get('engines', ENGINES))}\n"
        f"📊 Max/Page : {s.get('max_results', MAX_RESULTS)}\n"
        f"🛡 SQL ≥    : {s.get('min_score', 30)}\n"
        f"🧅 Tor      : {'ON' if s.get('tor') else 'OFF'}\n"
        f"⏱ Delay    : {MIN_DELAY}–{MAX_DELAY}s\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"/workers N | /chunks N | /maxres N\n"
        f"/engine X  | /filter N\n"
        f"/pages     | /tor"
    )


async def cmd_workers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        n = max(1, min(int(context.args[0]), MAX_WORKERS_PER_CHUNK))
        get_session(chat_id)["workers"] = n
        await update.message.reply_text(f"✅ Workers per chunk: {n} (max {MAX_WORKERS_PER_CHUNK})")
    except Exception:
        await update.message.reply_text(f"Usage: /workers N (1-{MAX_WORKERS_PER_CHUNK})")


async def cmd_chunks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        n = max(1, min(int(context.args[0]), 8))
        get_session(chat_id)["chunks"] = n
        await update.message.reply_text(
            f"✅ Parallel chunks: {n}\n"
            f"Each chunk uses an isolated session + {get_session(chat_id).get('workers', WORKERS_PER_CHUNK)} workers."
        )
    except Exception:
        cur = get_session(chat_id).get("chunks", N_CHUNKS)
        await update.message.reply_text(f"Usage: /chunks N (1-8)\nCurrent: {cur}")


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
        dorks   = [
            l.strip()
            for l in content.decode("utf-8", errors="replace").splitlines()
            if l.strip() and not l.startswith("#")
        ]
        if not dorks:
            await update.message.reply_text("❌ No dorks found.")
            return
        s = get_session(chat_id)
        await update.message.reply_text(
            f"✅ {len(dorks)} dorks | Pages: {', '.join(str(p) for p in s.get('pages', [1]))}\n"
            f"🛡 SQL ≥{s.get('min_score', 30)} | "
            f"⚡ {s.get('chunks', N_CHUNKS)} chunks | "
            f"{'🧅TOR' if s.get('tor') else '🔓 Direct'}\n🚀 Starting..."
        )
        active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, dorks, context))
    except Exception as exc:
        await update.message.reply_text(f"❌ Error: {exc}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lines   = [
        l.strip()
        for l in update.message.text.splitlines()
        if l.strip() and not l.startswith("#")
    ]
    if len(lines) > 1:
        if chat_id in active_jobs and not active_jobs[chat_id].done():
            await update.message.reply_text("⚠️ Job running! /stop first.")
            return
        s = get_session(chat_id)
        await update.message.reply_text(
            f"✅ {len(lines)} dorks | Pages: {', '.join(str(p) for p in s.get('pages', [1]))}\n🚀 Starting..."
        )
        active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, lines, context))
    else:
        await update.message.reply_text(
            "Use /dork <q> or upload .txt\n/pages | /tor | /filter N | /chunks N"
        )


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
                reply_markup=page_keyboard(selected),
            )
        except Exception:
            pass
        return

    replies = {
        "m_bulk":     "📂 Upload a .txt file — one dork per line. No limit!",
        "m_single":   "🔍 /dork inurl:login.php?id=\nSet pages with /pages",
        "m_tor":      f"🧅 Tor is {'ON — /tor off to disable' if sess.get('tor') else 'OFF — /tor on to enable'}",
        "m_filter":   f"🛡 SQL Filter ≥{sess.get('min_score', 30)}\n/filter 70=high | /filter 40=medium | /filter 0=all",
        "m_settings": (
            f"⚙️ Chunks:{sess.get('chunks', N_CHUNKS)} "
            f"Workers:{sess.get('workers', WORKERS_PER_CHUNK)}/chunk "
            f"Pages:{','.join(str(p) for p in sess.get('pages', [1]))} "
            f"Engines:{'+'.join(e.upper() for e in sess.get('engines', ENGINES))} "
            f"Score≥{sess.get('min_score', 30)} Tor:{'ON' if sess.get('tor') else 'OFF'}"
        ),
        "m_help": (
            "📖 COMMANDS\n━━━━━━━━━━━━━━━━━━━\n"
            "/dork <q>   — single dork\n"
            "/pages      — page selector (1-70)\n"
            "/chunks N   — parallel isolated sessions (1-8)\n"
            "/workers N  — workers per chunk (1-20)\n"
            "/tor        — toggle Tor (auto-rotate every 2 min)\n"
            "/filter N   — SQL score (0-100)\n"
            "/settings   — full config\n"
            "/maxres N   — results/page (1-50)\n"
            "/engine X   — bing|yahoo|both\n"
            "/stop       — stop job\n"
            "/status     — job status\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Upload .txt for unlimited bulk!\n\n"
            "📁 All results saved as a file — no chat spam."
        ),
    }

    if data == "m_pages":
        await query.message.reply_text(
            f"📄 SELECT PAGES (1–70)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Selected: {', '.join(str(p) for p in sess.get('pages', [1]))}\nTap to toggle.",
            reply_markup=page_keyboard(sess.get("pages", [1])),
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
        ("chunks",   cmd_chunks),
        ("maxres",   cmd_maxres),
        ("engine",   cmd_engine),
        ("stop",     cmd_stop),
        ("status",   cmd_status),
    ]:
        app.add_handler(CommandHandler(name, handler))

    app.add_handler(MessageHandler(filters.Document.ALL,            handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))

    async def _shutdown():
        stop_tor_rotation()
    app.shutdown_handler = _shutdown

    log.info("=" * 55)
    log.info("  DORK PARSER v17.0 — PARALLEL CHUNK ARCHITECTURE")
    log.info(f"  Chunks: {N_CHUNKS} | Workers/chunk: {WORKERS_PER_CHUNK}")
    log.info(f"  Delay: {MIN_DELAY}–{MAX_DELAY}s | Retries: {MAX_RETRIES}")
    log.info("=" * 55)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

