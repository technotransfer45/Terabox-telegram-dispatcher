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

# ---------------- CONFIG ----------------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
MOVIES_CHANNEL = int(os.environ["MOVIES_CHANNEL"])
ADULT_CHANNEL = int(os.environ["ADULT_CHANNEL"])
LOG_CHANNEL = int(os.environ["LOG_CHANNEL"])
PORT = int(os.environ.get("PORT", 8080))

_raw = os.environ.get("TARGET_CHANNELS", "").strip()
TARGET_CHANNELS = [c.strip() for c in _raw.split(",") if c.strip()] or ["4402319592370"]

# Channel posts API (override via env if your endpoint differs)
CHANNEL_API_URL = os.environ.get("CHANNEL_API_URL", "https://www.terabox.com/api/channel/posts")
SHARE_LIST_URL = "https://www.terabox.com/share/list"
APP_ID = "250528"

CHECK_INTERVAL = 15 * 60
MAX_PAGES = int(os.environ.get("MAX_PAGES", 500))
MAX_DEPTH = 6
MAX_FILES = 300
DB_PATH = "processed.db"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
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


# ---------------- SCRAPING ----------------
async def fetch_json(http: httpx.AsyncClient, url: str, params: dict):
    for attempt in range(3):
        try:
            r = await http.get(url, params=params)
            if r.status_code == 200:
                return r.json()
            log.warning("HTTP %s from %s", r.status_code, url)
        except Exception as e:
            log.warning("Request failed (%s): %s", attempt + 1, e)
        await asyncio.sleep(2 * (attempt + 1))
    return None


def extract_posts(data):
    """Return (posts, next_cursor) from a flexible response shape."""
    if not isinstance(data, dict):
        return [], None
    payload = data.get("data") if isinstance(data.get("data"), dict) else data
    posts = None
    for k in ("list", "posts", "items", "data"):
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
    return (posts or []), (cursor if has_more else None)


def post_fields(post: dict):
    post_id = str(post.get("id") or post.get("post_id") or post.get("feedid") or post.get("ctime") or "")
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


async def list_dir(http, shorturl: str, path: str | None):
    surl = shorturl[1:] if shorturl.startswith("1") else shorturl
    params = {
        "app_id": APP_ID, "web": "1", "channel": "dubox", "clienttype": "0",
        "shorturl": surl, "page": "1", "num": "1000", "order": "name", "by": "name",
    }
    if path:
        params.update({"dir": path, "root": "0"})
    else:
        params["root"] = "1"
    data = await fetch_json(http, SHARE_LIST_URL, params)
    if not data or data.get("errno", 0) != 0:
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
def classify(text: str, files: list[str]) -> str:
    blob = (text + " " + " ".join(files)).lower()
    if any(k in blob for k in ADULT_KEYWORDS):
        return "ADULT"
    if any(k in blob for k in MOVIE_KEYWORDS) or any(r.search(blob) for r in MOVIE_REGEXES):
        return "MOVIES"
    return "UNKNOWN"


def build_caption(text: str, files: list[str], shorturl: str, category: str) -> str:
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
async def send(app: Client, chat_id: int, text: str):
    for _ in range(3):
        try:
            await app.send_message(chat_id, text, disable_web_page_preview=True)
            return True
        except FloodWait as e:
            log.warning("FloodWait %ss", e.value)
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            log.error("Send to %s failed: %s", chat_id, e)
            return False
    return False


async def report(app: Client, text: str):
    await send(app, LOG_CHANNEL, f"ℹ️ {escape(text)}")


# ---------------- PROCESSING ----------------
async def process_channel(app: Client, http: httpx.AsyncClient, channel: str):
    sent = 0
    cursor = None
    await report(app, f"Scanning channel {channel}")
    for page in range(1, MAX_PAGES + 1):
        params = {"channel_id": channel, "num": 20}
        if cursor:
            params["cursor"] = cursor
        data = await fetch_json(http, CHANNEL_API_URL, params)
        posts, cursor = extract_posts(data)
        if not posts:
            break

        new_on_page = 0
        for post in posts:
            if not isinstance(post, dict):
                continue
            post_id, text, shorturl = post_fields(post)
            post_key = f"post:{channel}:{post_id}"
            surl_key = f"surl:{shorturl}"
            if not post_id and not shorturl:
                continue
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


async def worker(app: Client):
    async with httpx.AsyncClient(headers=HEADERS, timeout=30, follow_redirects=True) as http:
        while True:
            for ch in TARGET_CHANNELS:
                try:
                    await process_channel(app, http, ch)
                except Exception as e:
                    log.exception("Channel failed")
                    await report(app, f"Channel {ch} crashed: {e}")
            log.info("Cycle complete. Sleeping %ss", CHECK_INTERVAL)
            await asyncio.sleep(CHECK_INTERVAL)


async def main():
    await start_web()
    app = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
    await app.start()
    for cid in (MOVIES_CHANNEL, ADULT_CHANNEL, LOG_CHANNEL):
        try:
            await app.get_chat(cid)
        except Exception as e:
            log.warning("Cannot resolve chat %s: %s", cid, e)
    await report(app, "Bot started")
    try:
        await worker(app)
    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
