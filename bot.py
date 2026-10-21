import os
import re
import json
import asyncio
import logging
import sqlite3
from html import escape, unescape

import httpx
from aiohttp import web
from pyrogram import Client
from pyrogram.errors import FloodWait

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bot")


# ---------------- ID CLEANING ----------------
def parse_chat_id(raw):
    """Clean a Telegram chat id string and return int (or @username str)."""
    s = str(raw).strip().strip("'\"").replace(" ", "")
    if s.startswith("@"):
        return s
    if re.fullmatch(r"-?\d+", s):
        if s.startswith("-100"):
            return int(s)
        digits = s.lstrip("-")
        if len(digits) >= 10:  # missing -100 prefix
            return int("-100" + digits)
        return int(s)
    raise ValueError(f"Invalid chat id: {raw!r}")


def clean_uk(raw) -> str:
    return re.sub(r"[^\d]", "", str(raw))


# ---------------- CONFIG ----------------
API_ID = int(str(os.environ["API_ID"]).strip())
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
MOVIES_CHANNEL = parse_chat_id(os.environ["MOVIES_CHANNEL"])
ADULT_CHANNEL = parse_chat_id(os.environ["ADULT_CHANNEL"])
LOG_CHANNEL = parse_chat_id(os.environ["LOG_CHANNEL"])
PORT = int(os.environ.get("PORT", 8080))

# Each entry in TARGET_CHANNELS is used as the Hive `bot_uk`
_raw = os.environ.get("TARGET_CHANNELS", "").strip()
TARGET_CHANNELS = [clean_uk(c) for c in _raw.split(",") if clean_uk(c)] or ["4402319592370"]

HIVE_URL = os.environ.get("HIVE_URL", "https://dm.nephobox.com/wap/hive/channelShare")
SHARE_LIST_URL = "https://www.terabox.com/share/list"
SHARE_BASE = "https://1024terabox.com/s/"
APP_ID = "250528"

CHECK_INTERVAL = 15 * 60
MAX_PAGES = int(os.environ.get("MAX_PAGES", 200))
MAX_DEPTH = 6
MAX_FILES = 300
DB_PATH = "processed.db"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
PAGE_HEADERS = {
    "User-Agent": UA,
    "Referer": "https://dm.nephobox.com/wap/hive/channelShare",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
API_HEADERS = {
    "User-Agent": UA,
    "Referer": "https://dm.nephobox.com/wap/hive/channelShare",
    "Accept": "application/json, text/plain, */*",
}

ADULT_KEYWORDS = [
    "18+", "ullu", "primeshots", "kooku", "hotshots", "voovi", "atrangii",
    "chawl house", "erotic", "wafa", "hotx", "nuefliks", "fugi", "rabbit movies",
    "dreams films", "feneo", "uncut", "nude", "adult", "xxx",
]
MOVIE_KEYWORDS = [
    "#theaters", "#theatres", "netflix", "primevideo", "prime video", "amazon prime",
    "hotstar", "zee5", "sonyliv", "sony liv", "jiocinema", "mx player", "season",
    "1080p", "720p", "480p", "2160p", "4k", "webrip", "web-dl", "bluray", "hdrip",
    "x264", "x265", "hevc", "dual audio", "hindi dubbed",
]
MOVIE_REGEXES = [re.compile(r"\bs\d{1,2}(e\d{1,2})?\b", re.I)]
VIDEO_EXT = (".mp4", ".mkv")

# ---- parsing patterns ----
LINK_RE = re.compile(
    r"https?://(?:[\w-]+\.)*(?:terabox|teraboxapp|nephobox|1024tera|1024terabox|4funbox|mirrobox|"
    r"momerybox|teraboxlink|terafileshare|terasharefile|terasharelink|tibibox|freeterabox)"
    r"\.[a-z]+/(?:s/|sharing/link\?surl=)([\w-]+)",
    re.I,
)
SPATH_RE = re.compile(r"/s/(1[\w-]{10,})")
KEYED_SURL_RE = re.compile(
    r"""(?:shorturl|surl|short_url)["']?\s*[:=]\s*["'](1?[\w-]{10,40})["']""", re.I
)
LOOSE_SURL_RE = re.compile(r"""["'](1[a-zA-Z0-9_\-]{20,30})["']""")
CODE_OK_RE = re.compile(r"^[\w-]{10,40}$")

SCRIPT_RE = re.compile(r"<script\b([^>]*)>(.*?)</script>", re.S | re.I)
ASSIGN_RE = re.compile(
    r"(?:window\.[\w$.]+|(?:var|let|const)\s+[\w$]*(?:state|data|initial|preload|store|channel|hive|msg)[\w$]*)"
    r"\s*=\s*",
    re.I,
)
JSON_PARSE_RE = re.compile(r"JSON\.parse\(\s*(['\"])((?:\\.|(?!\1).)*)\1\s*\)", re.S)
KEY_RE = re.compile(r'"(msg_list|channel_msg|shorturl|surl)"\s*:\s*')
KEY_PRESENT_RE = re.compile(r'"(?:msg_list|channel_msg|shorturl|surl)"')
TEXT_FIELD_RE = re.compile(
    r"""["'](?:caption|content|text|title|desc|description|msg|message)["']\s*:\s*"((?:[^"\\]|\\.){3,600})\"""",
    re.I,
)
TEXT_KEYS = ("caption", "content", "text", "title", "desc", "description", "msg", "message")
ID_KEYS = ("msgid", "msg_id", "id", "post_id", "feedid", "ctime")
SURL_KEYS = ("shorturl", "surl", "short_url")
NOT_FOUND_MARKERS = ("can’t find the page", "can't find the page", "page not found")

BOT_HTTP = None
PYRO_OK = set()  # chat ids Pyrogram could resolve


# ---------------- DATABASE ----------------
db = sqlite3.connect(DB_PATH)
db.execute("CREATE TABLE IF NOT EXISTS processed (key TEXT PRIMARY KEY)")
db.commit()


def seen(key: str) -> bool:
    return db.execute("SELECT 1 FROM processed WHERE key=?", (key,)).fetchone() is not None


def mark(key: str):
    db.execute("INSERT OR IGNORE INTO processed (key) VALUES (?)", (key,))
    db.commit()


# ---------------- WEB SERVER ----------------
async def health(_request):
    return web.Response(text="Bot is running healthy")


async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Health server on port %s", PORT)


# ---------------- PAGE FETCH ----------------
async def fetch_page(http: httpx.AsyncClient, bot_uk: str, page: int, cursor):
    """Fetch the Hive channel page (or its paginated variant). Returns text or None."""
    params = {"bot_uk": bot_uk}
    if page > 1:
        params["page"] = page
    if cursor:
        params["cursor"] = cursor
    headers = dict(PAGE_HEADERS)
    if page > 1:
        headers["X-Requested-With"] = "XMLHttpRequest"
    for attempt in range(3):
        try:
            res = await http.get(HIVE_URL, params=params, headers=headers)
            text = res.text or ""
            log.info("GET %s page=%s -> HTTP %s, %s chars", HIVE_URL, page, res.status_code, len(text))
            if res.status_code == 200 and text.strip():
                low = text[:3000].lower()
                if any(m in low for m in NOT_FOUND_MARKERS):
                    log.error("Hive returned 'page not found' for bot_uk=%s. "
                              "Check that bot_uk is correct and the channel page is public.", bot_uk)
                    return None
                return text
            if res.status_code in (403, 404):
                log.error("HTTP %s for bot_uk=%s", res.status_code, bot_uk)
                return None
        except Exception as e:
            log.warning("Page request failed (%s): %s", attempt + 1, e)
        await asyncio.sleep(2 * (attempt + 1))
    return None


# ---------------- STATE / POST EXTRACTION ----------------
def _own_text(d: dict) -> str:
    for k in TEXT_KEYS:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return ""


def _own_id(d: dict) -> str:
    for k in ID_KEYS:
        if d.get(k):
            return str(d[k])
    return ""


def _codes_in_dict(d: dict):
    """Share codes found directly in this dict (keys + string values)."""
    codes = []
    for k in SURL_KEYS:
        v = d.get(k)
        if isinstance(v, str) and CODE_OK_RE.match(v.strip()):
            codes.append(v.strip())
    for v in d.values():
        if isinstance(v, str) and len(v) < 5000:
            vv = v.replace("\\/", "/")
            codes.extend(m.group(1) for m in LINK_RE.finditer(vv))
            codes.extend(m.group(1) for m in SPATH_RE.finditer(vv))
    return codes


def _walk_posts(obj, out, depth=0, ctx_text="", ctx_id=""):
    """Collect posts; child dicts inherit the parent's caption/id."""
    if depth > 16:
        return
    if isinstance(obj, dict):
        text = _own_text(obj) or ctx_text
        pid = _own_id(obj) or ctx_id
        for code in _codes_in_dict(obj):
            out.append({"id": f"{pid}:{code}" if pid else f"s-{code}",
                        "shorturl": code, "caption": text})
        for v in obj.values():
            if isinstance(v, (dict, list)):
                _walk_posts(v, out, depth + 1, text, pid)
    elif isinstance(obj, list):
        for item in obj:
            _walk_posts(item, out, depth + 1, ctx_text, ctx_id)


def dedupe_posts(posts):
    best = {}
    for p in posts:
        code = p.get("shorturl")
        if not code:
            continue
        if code not in best or len(p.get("caption", "")) > len(best[code].get("caption", "")):
            best[code] = p
    return list(best.values())


def _find_cursor(obj, depth=0):
    """Return (cursor, has_more_flag)."""
    if depth > 8:
        return None, None
    cursor, more = None, None
    if isinstance(obj, dict):
        for k in ("hasMore", "has_more", "hasmore"):
            if k in obj:
                more = bool(obj[k])
        for k in ("cursor", "next_cursor", "nextCursor", "next", "offset"):
            if obj.get(k) and isinstance(obj[k], (str, int)):
                cursor = obj[k]
                break
        if cursor is None:
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    c, m = _find_cursor(v, depth + 1)
                    cursor = cursor or c
                    more = m if more is None else more
    elif isinstance(obj, list):
        for v in obj[:3]:
            c, m = _find_cursor(v, depth + 1)
            cursor = cursor or c
            more = m if more is None else more
    return cursor, more


def _objs_around_keys(body: str):
    """Find JSON objects/arrays around msg_list / channel_msg / shorturl / surl keys."""
    objs, spans = [], []
    dec = json.JSONDecoder()
    for m in KEY_RE.finditer(body):
        pos = m.start()
        if any(a <= pos < b for a, b in spans):
            continue
        key, i = m.group(1), m.end()
        if key in ("msg_list", "channel_msg"):
            if i < len(body) and body[i] in "[{":
                try:
                    obj, end = dec.raw_decode(body, i)
                    objs.append(obj)
                    spans.append((i, end))
                except ValueError:
                    pass
            continue
        depth, j, tries = 0, pos, 0
        while j > 0 and pos - j < 6000 and tries < 4:
            j -= 1
            c = body[j]
            if c == "}":
                depth += 1
            elif c == "{":
                if depth == 0:
                    try:
                        obj, end = dec.raw_decode(body, j)
                        objs.append(obj)
                        spans.append((j, end))
                        break
                    except ValueError:
                        tries += 1
                else:
                    depth -= 1
    return objs


def _decode_js_string(quote: str, body: str):
    try:
        if quote == '"':
            return json.loads('"' + body + '"')
        s = body.replace("\\'", "'")
        s = re.sub(r'(?<!\\)"', r'\\"', s)
        return json.loads('"' + s + '"')
    except ValueError:
        return None


def _objs_from_script(body: str):
    objs = []
    dec = json.JSONDecoder()
    s = body.strip()
    if s[:1] in ("{", "["):
        try:
            return [json.loads(s)]
        except ValueError:
            pass
    # window.__INITIAL_STATE__ = {...}; window.__DATA__ = {...}; var xState = {...}
    for m in ASSIGN_RE.finditer(body):
        i = m.end()
        if i < len(body) and body[i] in "{[":
            try:
                obj, _ = dec.raw_decode(body, i)
                objs.append(obj)
            except ValueError:
                pass
    # window.__X__ = JSON.parse('...')
    for m in JSON_PARSE_RE.finditer(body):
        decoded = _decode_js_string(m.group(1), m.group(2))
        if decoded:
            try:
                objs.append(json.loads(decoded))
            except ValueError:
                pass
    if not objs and KEY_PRESENT_RE.search(body):
        objs.extend(_objs_around_keys(body))
    return objs


def _state_objects(html: str):
    objs = []
    for _attrs, body in SCRIPT_RE.findall(html):
        if len(body.strip()) < 30:
            continue
        found = _objs_from_script(body)
        if not found and re.search(r'\\"(?:msg_list|channel_msg|shorturl|surl)\\"', body):
            found = _objs_from_script(body.replace('\\"', '"').replace("\\\\", "\\"))
        objs.extend(found)
    if not objs and KEY_PRESENT_RE.search(html):
        objs.extend(_objs_around_keys(html))
    return objs


# ---------------- FALLBACK LINK EXTRACTOR ----------------
def _clean_html(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    html = re.sub(r"<a\s[^>]*href=[\"']([^\"']+)[\"'][^>]*>", r" \1 ", html, flags=re.I)
    html = re.sub(r"<br\s*/?>|</(p|div|li|h\d|section)>", "\n", html, flags=re.I)
    html = re.sub(r"<[^>]+>", " ", html)
    return unescape(html).replace("\\/", "/")


def _posts_from_visible_html(html: str):
    cleaned = _clean_html(html)
    matches = list(LINK_RE.finditer(cleaned))
    posts, found = [], set()
    for i, m in enumerate(matches):
        code = m.group(1)
        if code in found:
            continue
        found.add(code)
        start = max(matches[i - 1].end() if i else 0, m.start() - 400)
        chunk = re.sub(r"https?://\S+", " ", cleaned[start:m.start()])
        chunk = "\n".join(l.strip() for l in chunk.splitlines() if l.strip())
        posts.append({"id": f"html-{code}", "shorturl": code, "caption": chunk[-300:]})
    return posts


def _context_text(blob: str, pos: int) -> str:
    lo = max(0, pos - 800)
    window = blob[lo:pos + 800]
    best, best_d = "", None
    for m in TEXT_FIELD_RE.finditer(window):
        d = abs((lo + m.start()) - pos)
        if best_d is None or d < best_d:
            raw = m.group(1)
            try:
                raw = json.loads('"' + raw + '"')
            except ValueError:
                pass
            best, best_d = raw, d
    return best


def _plausible_loose(code: str) -> bool:
    has_letter = any(c.isalpha() for c in code)
    mixed = any(c.isupper() for c in code) and any(c.islower() for c in code)
    return has_letter and (mixed or "-" in code or "_" in code)


def fallback_links(html: str):
    """Tiered extraction: visible links -> /s/ codes -> keyed surl -> loose quoted codes."""
    posts = _posts_from_visible_html(html)
    if posts:
        log.info("Fallback tier 'visible links': %s code(s)", len(posts))
        return posts

    blob = html.replace("\\/", "/")
    found = {}
    counts = {}
    for tier, rx in (("full-url", LINK_RE), ("/s/ path", SPATH_RE), ("keyed surl", KEYED_SURL_RE)):
        n = 0
        for m in rx.finditer(blob):
            code = m.group(1)
            if code not in found:
                found[code] = _context_text(blob, m.start())
                n += 1
        counts[tier] = n
    if not found:
        n = 0
        for m in LOOSE_SURL_RE.finditer(blob):
            code = m.group(1)
            if code not in found and _plausible_loose(code):
                found[code] = _context_text(blob, m.start())
                n += 1
        counts["loose quoted"] = n
    log.info("Fallback link extractor counts: %s", counts)
    return [{"id": f"html-{c}", "shorturl": c, "caption": t} for c, t in found.items()]


def debug_dump(html: str):
    scripts = SCRIPT_RE.findall(html)
    inline = [b for a, b in scripts if "src=" not in a.lower()]
    log.error("0 posts extracted. HTML first 500 chars: %r", html[:500])
    log.error("Script tags: total=%s, with src=%s, inline=%s",
              len(scripts), len(scripts) - len(inline), len(inline))
    for b in sorted(inline, key=len, reverse=True)[:5]:
        log.error("  inline script %s chars: %r", len(b), b.strip()[:150])
    keys = {k: html.count(k) for k in ("shorturl", "surl", "msg_list", "channel_msg", "__INITIAL_STATE__", "__DATA__")}
    log.error("Key counts in HTML: %s", keys)
    paths = re.findall(r"""["'](/(?:api|wap)/[\w/\-.]*(?:hive|channel|msg|share)[\w/\-.]*)["']""", html)
    uniq = list(dict.fromkeys(paths))[:20]
    log.error("Candidate API paths found in scripts: %s", uniq)


def parse_response(text: str):
    """Return (posts, cursor) from JSON, embedded script state, or link fallbacks."""
    t = text.strip()
    if t.startswith(("{", "[")):
        try:
            obj = json.loads(t)
            out = []
            _walk_posts(obj, out)
            cursor, more = _find_cursor(obj)
            posts = dedupe_posts(out)
            if posts:
                return posts, (cursor if more is not False else None)
        except ValueError:
            log.warning("JSON-looking response failed to parse; using fallbacks")

    objs = _state_objects(text)
    out, cursor = [], None
    for obj in objs:
        _walk_posts(obj, out)
        c, more = _find_cursor(obj)
        if cursor is None and c and more is not False:
            cursor = c
    posts = dedupe_posts(out)
    if posts:
        log.info("Extracted %s post(s) from script state JSON (%s object(s) parsed)", len(posts), len(objs))
        return posts, cursor

    posts = fallback_links(text)
    if not posts:
        debug_dump(text)
    else:
        log.info("Extracted %s post(s) via fallback link extractor", len(posts))
    return posts, None


def post_fields(post: dict):
    post_id = str(post.get("id") or post.get("post_id") or post.get("msgid")
                  or post.get("feedid") or post.get("ctime") or "")
    text = ""
    for k in ("caption",) + TEXT_KEYS:
        if isinstance(post.get(k), str) and post[k].strip():
            text = post[k]
            break
    shorturl = ""
    for k in SURL_KEYS:
        if isinstance(post.get(k), str) and post[k].strip():
            shorturl = post[k].strip()
            break
    if not shorturl:
        m = LINK_RE.search(str(post).replace("\\/", "/"))
        if m:
            shorturl = m.group(1)
    return post_id, text.strip(), shorturl


# ---------------- FOLDER TRAVERSAL ----------------
async def safe_get_json(http: httpx.AsyncClient, url: str, params: dict):
    for attempt in range(3):
        try:
            res = await http.get(url, params=params, headers=API_HEADERS)
            body = res.text or ""
            if res.status_code == 200 and body.strip().startswith(("{", "[")):
                return res.json()
            log.warning("Non-JSON/HTTP %s from %s: %r", res.status_code, url, body[:100])
            if res.status_code == 200:
                return None
        except Exception as e:
            log.warning("Request failed (%s): %s", attempt + 1, e)
        await asyncio.sleep(2 * (attempt + 1))
    return None


async def list_dir(http, shorturl: str, path):
    surl = shorturl[1:] if shorturl.startswith("1") else shorturl
    params = {
        "app_id": APP_ID, "web": "1", "channel": "dubox", "clienttype": "0",
        "shorturl": surl, "page": "1", "num": "1000", "order": "name", "by": "name",
    }
    if path:
        params.update({"dir": path, "root": "0"})
    else:
        params["root"] = "1"
    data = await safe_get_json(http, SHARE_LIST_URL, params)
    if not isinstance(data, dict) or data.get("errno", 0) != 0:
        return []
    return data.get("list") or []


async def collect_videos(http, shorturl: str):
    """Iteratively traverse folders (isdir == 1) and return video filenames."""
    files, stack = [], [(None, 0)]
    while stack and len(files) < MAX_FILES:
        path, depth = stack.pop()
        for item in await list_dir(http, shorturl, path):
            name = item.get("server_filename") or item.get("filename") or ""
            if str(item.get("isdir", "0")) == "1":
                if depth < MAX_DEPTH and item.get("path"):
                    stack.append((item["path"], depth + 1))
            elif name.lower().endswith(VIDEO_EXT):
                files.append(name)
        await asyncio.sleep(0.5)
    return files


# ---------------- CLASSIFICATION ----------------
def classify(text: str, files: list) -> str:
    blob = (text + " " + " ".join(files)).lower()
    if any(k in blob for k in ADULT_KEYWORDS):
        return "ADULT"
    if any(k in blob for k in MOVIE_KEYWORDS) or any(r.search(blob) for r in MOVIE_REGEXES):
        return "MOVIES"
    return "UNKNOWN"


def build_caption(text: str, files: list, shorturl: str, category: str) -> str:
    plain = re.sub(r"https?://\S+", "", text).strip()
    first = plain.splitlines()[0] if plain else (files[0] if files else "Untitled")
    title = escape(first[:200])
    preview = "\n".join(f"• <code>{escape(f[:90])}</code>" for f in files[:10]) or "• No video files found"
    more = f"\n… and {len(files) - 10} more" if len(files) > 10 else ""
    link = f"{SHARE_BASE}{shorturl}" if shorturl else "N/A"
    return (
        f"🎬 <b>{title}</b>\n\n"
        f"📁 <b>Files:</b>\n{preview}{more}\n\n"
        f"📊 <b>Total Files:</b> {len(files)}\n"
        f"🏷 <b>Category:</b> {category}\n"
        f"🔗 <b>Access Link:</b> {link}"
    )[:4000]


# ---------------- TELEGRAM SENDING ----------------
async def send_via_bot_api(chat_id, text: str) -> bool:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
               "disable_web_page_preview": True}
    for _ in range(3):
        try:
            r = await BOT_HTTP.post(url, json=payload)
            data = r.json()
            if data.get("ok"):
                return True
            if r.status_code == 429:
                wait = data.get("parameters", {}).get("retry_after", 5)
                log.warning("Bot API flood wait %ss", wait)
                await asyncio.sleep(wait + 1)
                continue
            log.error("Bot API send to %s failed: %s (bot must be admin there and the ID correct)",
                      chat_id, data.get("description"))
            return False
        except Exception as e:
            log.error("Bot API request error: %s", e)
            await asyncio.sleep(2)
    return False


async def send(app, chat_id, text: str) -> bool:
    """Pyrogram only if it resolved this chat; otherwise Bot API is the primary sender."""
    if app is not None and chat_id in PYRO_OK:
        for _ in range(3):
            try:
                await app.send_message(chat_id, text, disable_web_page_preview=True)
                return True
            except FloodWait as e:
                log.warning("FloodWait %ss", e.value)
                await asyncio.sleep(e.value + 1)
            except Exception as e:
                log.warning("Pyrogram send to %s failed (%s); using Bot API", chat_id, e)
                break
    return await send_via_bot_api(chat_id, text)


async def report(app, text: str):
    await send(app, LOG_CHANNEL, f"ℹ️ {escape(text)}")


# ---------------- PROCESSING ----------------
async def process_channel(app, http: httpx.AsyncClient, bot_uk: str):
    sent, cursor, prev_sig = 0, None, None
    await report(app, f"Scanning bot_uk {bot_uk}")
    for page in range(1, MAX_PAGES + 1):
        text = await fetch_page(http, bot_uk, page, cursor)
        if not text:
            break
        posts, next_cursor = parse_response(text)
        if not posts:
            log.info("No posts on page %s for bot_uk=%s", page, bot_uk)
            if page == 1:
                await report(app, f"bot_uk {bot_uk}: 0 posts extracted (see server logs for debug dump)")
            break

        sig = tuple(sorted(post_fields(p)[2] or post_fields(p)[0] for p in posts))
        if sig == prev_sig:  # pagination returned the same content again
            break
        prev_sig = sig

        new_on_page = 0
        for post in posts:
            post_id, ptext, shorturl = post_fields(post)
            if not post_id and not shorturl:
                continue
            post_key = f"post:{bot_uk}:{post_id}"
            surl_key = f"surl:{shorturl}"
            if (post_id and seen(post_key)) or (shorturl and seen(surl_key)):
                continue
            new_on_page += 1

            try:
                files = await collect_videos(http, shorturl) if shorturl else []
                category = classify(ptext, files)
                caption = build_caption(ptext, files, shorturl, category)
                target = {"ADULT": ADULT_CHANNEL, "MOVIES": MOVIES_CHANNEL}.get(category, LOG_CHANNEL)
                if await send(app, target, caption):
                    if post_id:
                        mark(post_key)
                    if shorturl:
                        mark(surl_key)
                    sent += 1
            except Exception as e:
                log.exception("Post failed")
                await report(app, f"Error on {bot_uk}/{post_id}: {e}")
            await asyncio.sleep(3)

        if new_on_page == 0:
            break
        cursor = next_cursor
    await report(app, f"bot_uk {bot_uk} done. Dispatched: {sent}")


async def worker(app, http: httpx.AsyncClient):
    while True:
        for uk in TARGET_CHANNELS:
            try:
                await process_channel(app, http, uk)
            except Exception as e:
                log.exception("Channel %s failed", uk)
                try:
                    await report(app, f"Channel {uk} crashed: {e}")
                except Exception:
                    pass
        log.info("Cycle complete. Sleeping %ss", CHECK_INTERVAL)
        await asyncio.sleep(CHECK_INTERVAL)


async def main():
    global BOT_HTTP
    await start_web()
    BOT_HTTP = httpx.AsyncClient(timeout=30)

    app = None
    try:
        app = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
        await app.start()
        for name, cid in (("MOVIES_CHANNEL", MOVIES_CHANNEL),
                          ("ADULT_CHANNEL", ADULT_CHANNEL),
                          ("LOG_CHANNEL", LOG_CHANNEL)):
            try:
                chat = await app.get_chat(cid)
                PYRO_OK.add(cid)
                log.info("%s resolved via Pyrogram: %s (%s)", name, chat.title, cid)
            except Exception as e:
                log.warning("%s (%s) not resolvable via Pyrogram (%s) -> using Bot API", name, cid, e)
    except Exception as e:
        log.error("Pyrogram start failed (%s). Running with Bot API sender only.", e)
        app = None

    await report(app, "Bot started")
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as http:
            await worker(app, http)
    finally:
        await BOT_HTTP.aclose()
        if app is not None:
            await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
