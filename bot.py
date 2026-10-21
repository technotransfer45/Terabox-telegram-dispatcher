import os
import re
import asyncio
import logging
import sqlite3
from html import escape

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


def clean_channel_id(raw) -> str:
    return re.sub(r"[^\d]", "", str(raw))


# ---------------- CONFIG ----------------
API_ID = int(str(os.environ["API_ID"]).strip())
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
MOVIES_CHANNEL = parse_chat_id(os.environ["MOVIES_CHANNEL"])
ADULT_CHANNEL = parse_chat_id(os.environ["ADULT_CHANNEL"])
LOG_CHANNEL = parse_chat_id(os.environ["LOG_CHANNEL"])
PORT = int(os.environ.get("PORT", 8080))

_raw = os.environ.get("TARGET_CHANNELS", "").strip()
TARGET_CHANNELS = [clean_channel_id(c) for c in _raw.split(",") if clean_channel_id(c)] or ["4402319592370"]

CHANNEL_API_URL = os.environ.get(
    "CHANNEL_API_URL", "https://dm.nephobox.com/api/channel/share/msglist"
)
BOT_UK = os.environ.get("BOT_UK", "").strip()  # optional; falls back to channel_id
SHARE_LIST_URL = "https://www.terabox.com/share/list"
APP_ID = "250528"

CHECK_INTERVAL = 15 * 60
MAX_PAGES = int(os.environ.get("MAX_PAGES", 500))
MAX_DEPTH = 6
MAX_FILES = 300
DB_PATH = "processed.db"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
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

LINK_RE = re.compile(
    r"https?://(?:www\.)?(?:terabox|teraboxapp|nephobox|1024tera|4funbox|mirrobox|momerybox|"
    r"teraboxlink|terafileshare|1024terabox|terasharelink)\.[a-z]+/(?:s/|sharing/link\?surl=)([\w-]+)",
    re.I,
)
VIDEO_EXT = (".mp4", ".mkv")

BOT_HTTP: httpx.AsyncClient | None = None


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


# ---------------- HTTP / SCRAPING ----------------
async def safe_get(http: httpx.AsyncClient, url: str, params: dict):
    """GET with retries. Returns (parsed_json_or_None, raw_text)."""
    text = ""
    for attempt in range(3):
        try:
            res = await http.get(url, params=params, headers=HEADERS)
            text = res.text or ""
            if res.status_code == 200 and text.strip().startswith(("{", "[")):
                try:
                    return res.json(), text
                except ValueError as e:
                    log.warning("JSON decode failed: %s", e)
            elif res.status_code == 200:
                log.warning("Non-JSON response (%s chars) from %s: %r",
                            len(text), url, text[:120])
                return None, text  # HTML/empty: no point retrying
            else:
                log.warning("HTTP %s from %s", res.status_code, url)
        except Exception as e:
            log.warning("Request failed (%s): %s", attempt + 1, e)
        await asyncio.sleep(2 * (attempt + 1))
    return None, text


def extract_posts(data):
    """Return (posts, next_cursor) from a flexible response shape."""
    if isinstance(data, list):
        return [p for p in data if isinstance(p, dict)], None
    if not isinstance(data, dict):
        return [], None
    payload = data.get("data") if isinstance(data.get("data"), dict) else data
    posts = None
    for k in ("list", "msglist", "msg_list", "posts", "items", "data"):
        if isinstance(payload.get(k), list):
            posts = payload[k]
            break
    if posts is None and isinstance(data.get("list"), list):
        posts = data["list"]
    cursor = None
    for k in ("cursor", "next_cursor", "next", "offset"):
        if payload.get(k):
            cursor = payload[k]
            break
    has_more = payload.get("has_more", payload.get("hasmore", 1 if cursor else 0))
    return [p for p in (posts or []) if isinstance(p, dict)], (cursor if has_more else None)


def posts_from_html(html: str):
    """Fallback: pull share links straight out of an HTML page."""
    posts, found = [], set()
    for m in LINK_RE.finditer(html or ""):
        surl = m.group(1)
        if surl in found:
            continue
        found.add(surl)
        posts.append({"id": f"html-{surl}", "shorturl": surl, "caption": ""})
    return posts


async def fetch_posts_page(http, channel: str, cursor):
    params = {"channel_id": channel, "bot_uk": BOT_UK or channel, "num": 20}
    if cursor:
        params["cursor"] = cursor
    data, raw = await safe_get(http, CHANNEL_API_URL, params)
    if data is not None:
        return extract_posts(data)
    if raw and "<" in raw[:500]:
        fallback = posts_from_html(raw)
        log.info("HTML response; fallback share inspection found %s link(s)", len(fallback))
        return fallback, None
    return [], None


def post_fields(post: dict):
    post_id = str(post.get("id") or post.get("post_id") or post.get("msgid")
                  or post.get("feedid") or post.get("ctime") or "")
    text = ""
    for k in ("caption", "content", "text", "title", "desc", "description"):
        if post.get(k):
            text = str(post[k])
            break
    shorturl = post.get("shorturl") or post.get("surl") or ""
    if not shorturl:
        m = LINK_RE.search(text) or LINK_RE.search(str(post))
        if m:
            shorturl = m.group(1)
    return post_id, text.strip(), str(shorturl).strip()


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
    data, _ = await safe_get(http, SHARE_LIST_URL, params)
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
    title = escape((text.splitlines()[0] if text else (files[0] if files else "Untitled"))[:200])
    preview = "\n".join(f"• <code>{escape(f[:90])}</code>" for f in files[:10]) or "• No video files found"
    more = f"\n… and {len(files) - 10} more" if len(files) > 10 else ""
    link = f"https://terabox.com/s/{shorturl}" if shorturl else "N/A"
    return (
        f"🎬 <b>{title}</b>\n\n"
        f"📁 <b>Files:</b>\n{preview}{more}\n\n"
        f"📊 <b>Total Files:</b> {len(files)}\n"
        f"🏷 <b>Category:</b> {category}\n"
        f"🔗 <b>Access Link:</b> {link}"
    )[:4000]


# ---------------- TELEGRAM ----------------
async def send_via_bot_api(chat_id, text: str) -> bool:
    """Fallback that needs no access_hash/peer cache: plain Bot API."""
    if BOT_HTTP is None:
        return False
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
            log.error("Bot API send to %s failed: %s (bot must be admin of the channel "
                      "and the ID must be correct)", chat_id, data.get("description"))
            return False
        except Exception as e:
            log.error("Bot API request error: %s", e)
            await asyncio.sleep(2)
    return False


async def send(app: Client, chat_id, text: str) -> bool:
    for _ in range(3):
        try:
            await app.send_message(chat_id, text, disable_web_page_preview=True)
            return True
        except FloodWait as e:
            log.warning("FloodWait %ss", e.value)
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            log.warning("Pyrogram send to %s failed (%s); trying Bot API fallback", chat_id, e)
            return await send_via_bot_api(chat_id, text)
    return False


async def report(app: Client, text: str):
    await send(app, LOG_CHANNEL, f"ℹ️ {escape(text)}")


# ---------------- PROCESSING ----------------
async def process_channel(app: Client, http: httpx.AsyncClient, channel: str):
    sent = 0
    cursor = None
    await report(app, f"Scanning channel {channel}")
    for _page in range(1, MAX_PAGES + 1):
        posts, cursor = await fetch_posts_page(http, channel, cursor)
        if not posts:
            break

        new_on_page = 0
        for post in posts:
            post_id, text, shorturl = post_fields(post)
            if not post_id and not shorturl:
                continue
            post_key = f"post:{channel}:{post_id}"
            surl_key = f"surl:{shorturl}"
            if (post_id and seen(post_key)) or (shorturl and seen(surl_key)):
                continue
            new_on_page += 1

            try:
                files = await collect_videos(http, shorturl) if shorturl else []
                category = classify(text, files)
                caption = build_caption(text, files, shorturl, category)
                target = {"ADULT": ADULT_CHANNEL, "MOVIES": MOVIES_CHANNEL}.get(category, LOG_CHANNEL)
                if await send(app, target, caption):
                    if post_id:
                        mark(post_key)
                    if shorturl:
                        mark(surl_key)
                    sent += 1
            except Exception as e:
                log.exception("Post failed")
                await report(app, f"Error on {channel}/{post_id}: {e}")
            await asyncio.sleep(3)

        if new_on_page == 0 or not cursor:
            break
    await report(app, f"Channel {channel} done. Dispatched: {sent}")


async def worker(app: Client, http: httpx.AsyncClient):
    while True:
        for ch in TARGET_CHANNELS:
            try:
                await process_channel(app, http, ch)
            except Exception as e:
                log.exception("Channel %s failed", ch)
                try:
                    await report(app, f"Channel {ch} crashed: {e}")
                except Exception:
                    pass
        log.info("Cycle complete. Sleeping %ss", CHECK_INTERVAL)
        await asyncio.sleep(CHECK_INTERVAL)


async def main():
    global BOT_HTTP
    await start_web()
    BOT_HTTP = httpx.AsyncClient(timeout=30)
    app = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
    await app.start()

    # Resolve each destination independently; one failure never stops the rest.
    for name, cid in (("MOVIES_CHANNEL", MOVIES_CHANNEL),
                      ("ADULT_CHANNEL", ADULT_CHANNEL),
                      ("LOG_CHANNEL", LOG_CHANNEL)):
        try:
            chat = await app.get_chat(cid)
            log.info("%s resolved: %s (%s)", name, chat.title, cid)
        except Exception as e:
            log.warning("%s (%s) not resolvable via Pyrogram: %s. Bot API fallback will be used; "
                        "make sure the bot is admin there.", name, cid, e)

    await report(app, "Bot started")
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as http:
            await worker(app, http)
    finally:
        await BOT_HTTP.aclose()
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
