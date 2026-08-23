"""
Telegram Channel Archive Bot (Telethon Userbot + Bot API)
=========================================================
- Sirf owner (tum) use kar sakte ho
- Premium channel se content download → apne private channel pe upload
- Safe rate limiting with FloodWait handling
- Live progress updates
- Telegram Bot se bhi control kar sakte ho
"""

import os
import asyncio
import json
import logging
import time
import io
import hashlib
import re
import csv
import threading
import tempfile
import uuid
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, Response, render_template, jsonify, request, stream_with_context
from telethon import TelegramClient, events
from telethon.network import ConnectionTcpAbridged
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument,
    MessageMediaWebPage, DocumentAttributeFilename
)
from telethon.tl.types import (
    InputMessagesFilterPhotos,
    InputMessagesFilterVideo,
    InputMessagesFilterDocument,
    InputMessagesFilterGif,
    InputMessagesFilterVoice,
    InputMessagesFilterUrl,
)
from telethon.errors import (
    FloodWaitError, ChatWriteForbiddenError, ChannelPrivateError,
    FileReferenceExpiredError, MediaInvalidError, FilePartMissingError,
    SlowModeWaitError, BadMessageError, TimeoutError as TgTimeoutError,
    ServerError,
)
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, ContextTypes
)
from telethon.sessions import StringSession


_dashboard_condition = threading.Condition()
_dashboard_revision = 0


def _dashboard_changed():
    """Wake dashboard SSE clients after a meaningful state/log change."""
    global _dashboard_revision
    with _dashboard_condition:
        _dashboard_revision += 1
        _dashboard_condition.notify_all()


# ─── CONFIG ───────────────────────────────────────────
def _require_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {key}. Set it in Replit Secrets.")
    return val

API_ID       = int(_require_env("API_ID"))
API_HASH     = _require_env("API_HASH")
PHONE        = _require_env("PHONE")
OWNER_ID     = int(_require_env("OWNER_ID"))
BOT_TOKEN    = _require_env("BOT_TOKEN")
SESSION_STRING_FILE = "session_string.txt"
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()
if not SESSION_STRING and Path(SESSION_STRING_FILE).exists():
    SESSION_STRING = Path(SESSION_STRING_FILE).read_text(encoding="utf-8").strip()


MSG_DELAY    = 3        # seconds between messages
BATCH_SIZE   = 10      # messages per batch
BATCH_DELAY  = 10      # seconds after each batch

LOG_FILE     = "sync.log"

SESSION_FILE = "archive_session"
STATE_FILE   = "sync_state.json"

# ─── LOGGER SETUP ─────────────────────────────────────
logger = logging.getLogger("SyncBot")
logger.setLevel(logging.DEBUG)
_fmt = logging.Formatter("[%(asctime)s] %(levelname)s — %(message)s", "%d-%b %H:%M:%S")

_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(_fmt)

_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
_ch.setFormatter(_fmt)

logger.addHandler(_fh)
logger.addHandler(_ch)

# ── Live log ring buffer (web dashboard reads this) ────
_live_log: deque = deque(maxlen=5000)

def _log_live(msg: str):
    """Append timestamped entry to the in-memory live log."""
    ts = datetime.now().strftime("%H:%M:%S")
    _live_log.append(f"[{ts}] {msg}")
    _dashboard_changed()

class _LiveLogHandler(logging.Handler):
    """Mirror every logger record into _live_log for the web dashboard."""
    def emit(self, record):
        _log_live(f"[{record.levelname}] {record.getMessage()}")

_llh = _LiveLogHandler()
_llh.setLevel(logging.INFO)
logger.addHandler(_llh)
# ──────────────────────────────────────────────────────

CHUNK_SIZE       = 512 * 1024       # kept for reference (disk download use karta hai)
PARALLEL_WORKERS = 8                # disk mode mein 1 worker kaafi hai
SMALL_FILE_LIMIT = 5 * 1024 * 1024  # unused in disk mode

client = TelegramClient(
    StringSession(SESSION_STRING), API_ID, API_HASH,
    connection         = ConnectionTcpAbridged,
    connection_retries = 5,
    retry_delay        = 2,
)


def persist_session_string():
    """Save the authorized Telethon session so future restarts do not ask OTP."""
    try:
        session_value = client.session.save()
        if session_value:
            path = Path(SESSION_STRING_FILE)
            path.write_text(session_value + "\n", encoding="utf-8")
            path.chmod(0o600)
            logger.info("Telegram session persisted for next restart")
    except Exception as exc:
        logger.warning("Could not persist Telegram session: %s", exc)

# ─── STATE MANAGEMENT ─────────────────────────────────
def load_state():
    if Path(STATE_FILE).exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
    _dashboard_changed()

state = load_state()
state.setdefault("auto_forward", False)
state.setdefault("tasks", [])
state.setdefault("auto_stats", {"sent": 0, "failed": 0})
if not state.get("pairs"):
    if state.get("source") and state.get("target"):
        state["pairs"] = [{
            "id": "default",
            "name": "Default pair",
            "source": state["source"],
            "target": state["target"],
            "source_title": state.get("source_title", str(state["source"])),
            "target_title": state.get("target_title", str(state["target"])),
            "allowed_types": ["text", "photo", "video", "doc", "other"],
            "include_keywords": [],
            "exclude_keywords": [],
            "caption_prefix": "",
            "caption_suffix": "",
            "remove_links": False,
            "remove_source_name": False,
            "rate_delay": MSG_DELAY,
        }]
    else:
        state["pairs"] = []
state.setdefault("dedupe", {})
state.setdefault("task_controls", {})

# Sync requests are queued instead of being rejected while another sync runs.
_task_queue = deque()
_task_worker_running = False
_auto_forward_lock = asyncio.Lock()


def _task_view(task):
    return {
        "id": task["id"],
        "mode": task.get("mode", "full"),
        "source": task.get("source_title", task.get("source")),
        "target": task.get("target_title", task.get("target")),
        "status": task.get("status", "queued"),
        "created_at": task.get("created_at"),
        "min_id": task.get("min_id", 0),
        "limit": task.get("limit"),
        "pair_id": task.get("pair_id"),
        "stats": task.get("stats", {}),
        "started_at": task.get("started_at"),
        "finished_at": task.get("finished_at"),
        "total": task.get("total", 0),
        "current": task.get("current", 0),
    }


def _pair_by_id(pair_id):
    return next((p for p in state.get("pairs", []) if p.get("id") == pair_id), None)


def _pair_config(pair):
    pair = pair or {}
    return {
        "allowed_types": pair.get("allowed_types") or ["text", "photo", "video", "doc", "other"],
        "include_keywords": [str(x).lower() for x in pair.get("include_keywords", []) if str(x).strip()],
        "exclude_keywords": [str(x).lower() for x in pair.get("exclude_keywords", []) if str(x).strip()],
        "caption_prefix": pair.get("caption_prefix", ""),
        "caption_suffix": pair.get("caption_suffix", ""),
        "remove_links": bool(pair.get("remove_links")),
        "remove_source_name": bool(pair.get("remove_source_name")),
        "rate_delay": max(0, min(float(pair.get("rate_delay", MSG_DELAY)), 300)),
    }


def _message_allowed(message, config):
    msg_type = get_msg_type(message)
    if msg_type not in config["allowed_types"]:
        return False
    text = (message.text or "").lower()
    if config["include_keywords"] and not any(word in text for word in config["include_keywords"]):
        return False
    if any(word in text for word in config["exclude_keywords"]):
        return False
    return True


def _edited_caption(message, config, source_title=""):
    text = message.text or ""
    if config["remove_links"]:
        text = re.sub(r"(https?://|www\.)\S+", "", text, flags=re.IGNORECASE)
    if config["remove_source_name"] and source_title:
        text = re.sub(re.escape(source_title), "", text, flags=re.IGNORECASE)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = text.strip()
    if text:
        text = f"{config['caption_prefix']}{text}{config['caption_suffix']}"
    else:
        text = f"{config['caption_prefix']}{config['caption_suffix']}".strip()
    return text


def _dedupe_key(pair_id, message):
    raw = f"{pair_id}:{message.id}:{message.text or ''}:{getattr(message, 'grouped_id', '')}"
    media = getattr(message, "media", None)
    doc = getattr(media, "document", None)
    if doc:
        raw += f":{getattr(doc, 'id', '')}:{getattr(doc, 'size', '')}"
    return hashlib.sha256(raw.encode("utf-8", "ignore")).hexdigest()


def _remember_mapping(pair_id, source_id, sent):
    target_id = getattr(sent, "id", None)
    if target_id:
        state.setdefault("message_map", {}).setdefault(str(pair_id), {})[str(source_id)] = target_id


async def _task_worker():
    global _task_worker_running
    if _task_worker_running:
        return
    _task_worker_running = True
    try:
        while _task_queue:
            task = _task_queue.popleft()
            task["status"] = "running"
            state["active_task_id"] = task["id"]
            state["running"] = True
            state["paused"] = False
            state["stats"] = reset_stats()
            state["current_id"] = 0
            state["total_msgs"] = 0
            state["tasks"] = [
                {**item, "status": "running"} if item.get("id") == task["id"] else item
                for item in state.get("tasks", [])
            ]
            save_state(state)
            _log_live(f"📋 Task {task['id']} started ({task['mode']})")
            try:
                await _run_sync(
                    task["progress_msg"], task["source"], task["target"],
                    task["reverse"], task["min_id"], task["limit"], task["is_bot"],
                    task.get("pair_id"), task.get("config"), task["id"],
                    task.get("source_title"), task.get("target_title")
                )
                task["status"] = "complete" if not state.get("running") else "complete"
            except Exception as exc:
                task["status"] = "failed"
                logger.exception("Queued task failed: %s", exc)
            finally:
                task["finished_at"] = datetime.now().isoformat(timespec="seconds")
                task["stats"] = dict(state.get("stats", {}))
                task["total"] = state.get("total_msgs", 0)
                task["current"] = state.get("current_id", 0)
                task_view = _task_view(task)
                state["tasks"] = [
                    task_view if item.get("id") == task["id"] else item
                    for item in state.get("tasks", [])
                ]
                state.pop("active_task_id", None)
                if _task_queue:
                    state["running"] = True
                save_state(state)
    finally:
        _task_worker_running = False
        state["running"] = bool(_task_queue)
        state.pop("active_task_id", None)
        save_state(state)


def _queue_sync(source, target, reverse=True, min_id=0, limit=None,
                progress_msg=None, is_bot=False, mode="full", pair_id=None,
                config=None):
    pair = _pair_by_id(pair_id)
    task = {
        "id": uuid.uuid4().hex[:8],
        "source": source,
        "target": target,
        "source_title": (pair or {}).get("source_title", state.get("source_title", str(source))),
        "target_title": (pair or {}).get("target_title", state.get("target_title", str(target))),
        "reverse": reverse,
        "min_id": min_id,
        "limit": limit,
        "mode": mode,
        "pair_id": pair_id or "default",
        "config": config or _pair_config(_pair_by_id(pair_id)),
        "progress_msg": progress_msg or WebEvent(),
        "is_bot": is_bot,
        "status": "queued",
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    _task_queue.append(task)
    state["tasks"] = state.get("tasks", []) + [_task_view(task)]
    save_state(state)
    # Web routes run in Flask's thread, while bot commands run on Telegram's
    # event-loop thread. Always schedule the worker on the shared loop.
    if _loop is not None and _loop.is_running():
        asyncio.run_coroutine_threadsafe(_task_worker(), _loop)
    else:
        asyncio.create_task(_task_worker())
    return task

# ─── DISK-BASED DOWNLOADER (RAM bachane ke liye) ──────
async def fast_download(media, progress_cb=None) -> str:
    """
    File ko RAM mein nahi, disk (/tmp) pe download karta hai.
    Returns: tmp file path (str). Caller ka zimma hai delete karna.
    """
    # Temp file banao
    suffix = ".tmp"
    if isinstance(media, MessageMediaDocument) and media.document:
        for attr in media.document.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                ext = Path(attr.file_name).suffix
                if ext:
                    suffix = ext
                break
        else:
            mime = getattr(media.document, "mime_type", "")
            if "video" in mime:   suffix = ".mp4"
            elif "audio" in mime: suffix = ".mp3"
    elif isinstance(media, MessageMediaPhoto):
        suffix = ".jpg"

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=suffix, dir="/tmp")
    os.close(tmp_fd)

    total_size = 0
    if isinstance(media, MessageMediaDocument) and media.document:
        total_size = media.document.size

    downloaded = [0]
    _last_cb   = [0.0]

    logger.debug(f"Disk download → {tmp_path} | size={total_size//1024}KB")

    async def _progress(current, total):
        downloaded[0] = current
        if progress_cb:
            now = time.time()
            if now - _last_cb[0] >= 0.5:
                _last_cb[0] = now
                await progress_cb(current, total or total_size)

    await client.download_media(media, file=tmp_path, progress_callback=_progress)
    return tmp_path
# ──────────────────────────────────────────────────────


# ─── HELPERS ──────────────────────────────────────────
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def reset_stats():
    return {"text": 0, "photo": 0, "video": 0, "doc": 0, "other": 0, "failed": 0}

def stats_text(stats: dict) -> str:
    total = sum(v for k, v in stats.items() if k != "failed")
    return (
        f"Text: {stats['text']}\n"
        f"Photo: {stats['photo']}\n"
        f"Video: {stats['video']}\n"
        f"Doc: {stats['doc']}\n"
        f"Other: {stats['other']}\n"
        f"Failed: {stats['failed']}\n"
        f"Total: {total}"
    )

async def safe_reply(event, text):
    if len(text) > 4000:
        chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for chunk in chunks:
            await event.reply(chunk)
    else:
        await event.reply(text)


# ════════════════════════════════════════════════════════
#  USERBOT COMMANDS (outgoing messages with dot prefix)
# ════════════════════════════════════════════════════════

@client.on(events.NewMessage(outgoing=True, pattern=r"^\.help$"))
async def cmd_help(event):
    if not is_owner(event.sender_id):
        return
    await event.edit(
        "🤖 **Archive Bot Commands**\n\n"
        "`.setsource @user / -100id / link` — Source set karo\n"
        "`.setsource` (forwarded msg pe reply) — Private channel source set\n"
        "`.settarget @user / -100id / link` — Target set karo\n"
        "`.settarget` (forwarded msg pe reply) — Private channel target set\n"
        "`.info` — Current config dekho\n"
        "`.sync` — Full sync start karo (sab messages)\n"
        "`.syncfrom <id>` — Specific message ID se sync karo\n"
        "`.synclast <n>` — Last N messages sync karo\n"
        "`.pause` — Sync pause karo\n"
        "`.resume` — Sync resume karo\n"
        "`.stop` — Sync stop karo\n"
        "`.status` — Live status dekho\n"
        "`.reset` — Config reset karo\n"
        "`.help` — Ye menu\n\n"
        "⚠️ Sirf owner (tum) use kar sakte ho"
    )


def parse_channel_input(text):
    """
    Channel input ko normalize karo — support:
    - @username
    - -100xxxxxxxxxx  (channel ID)
    - plain number like 1234567890 (auto -100 prefix lagao)
    - https://t.me/username  ya  t.me/username
    - https://t.me/+invitehash  ya  t.me/joinchat/hash  (private invite)
    """
    text = text.strip()
    # Pure numeric ya -100 wala ID
    if text.lstrip("-").isdigit():
        num = int(text)
        # Agar positive number diya to -100 prefix laga do
        if num > 0:
            num = int(f"-100{num}")
        return num
    # t.me ya telegram.me links
    import re as _re
    link_match = _re.match(
        r"(?:https?://)?(?:t(?:elegram)?\.me|telegram\.org)/(?:joinchat/)?(.+)",
        text, _re.IGNORECASE
    )
    if link_match:
        path = link_match.group(1).rstrip("/")
        # Private invite link (+hash)
        if path.startswith("+"):
            return text  # Telethon handles full invite URL
        return f"@{path}" if not path.startswith("@") else path
    return text


async def get_channel_from_event(event):
    """
    Forwarded message reply se channel ID nikalo (userbot ke liye).
    Returns (channel_identifier, entity) ya (None, None)
    """
    if event.is_reply:
        replied = await event.get_reply_message()
        if replied and replied.fwd_from:
            fwd = replied.fwd_from
            peer = getattr(fwd, "from_id", None) or getattr(fwd, "channel_id", None)
            if peer:
                try:
                    entity = await client.get_entity(peer)
                    cid = entity.id
                    channel_id = int(f"-100{cid}") if cid > 0 else cid
                    return channel_id, entity
                except Exception:
                    pass
    return None, None


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.setsource(.*)$"))
async def cmd_setsource(event):
    if not is_owner(event.sender_id):
        return
    arg = event.pattern_match.group(1).strip()

    if not arg:
        # Forwarded message se try karo
        channel, entity = await get_channel_from_event(event)
        if channel is None:
            await event.edit(
                "❌ Usage:\n"
                "`.setsource @username` — public channel\n"
                "`.setsource -100xxxxxxxxxx` — channel ID (private ke liye)\n"
                "Ya kisi bhi channel ka message forward karke us pe reply karo `.setsource`"
            )
            return
    else:
        channel = parse_channel_input(arg)
        try:
            entity = await client.get_entity(channel)
        except Exception as e:
            await event.edit(f"❌ Error: `{e}`")
            return

    state["source"] = channel
    state["source_title"] = getattr(entity, "title", str(channel))
    save_state(state)
    await event.edit(f"✅ Source set: **{state['source_title']}**\n`{channel}`")


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.settarget(.*)$"))
async def cmd_settarget(event):
    if not is_owner(event.sender_id):
        return
    arg = event.pattern_match.group(1).strip()

    if not arg:
        # Forwarded message se try karo
        channel, entity = await get_channel_from_event(event)
        if channel is None:
            await event.edit(
                "❌ Usage:\n"
                "`.settarget @username` — public channel\n"
                "`.settarget -100xxxxxxxxxx` — channel ID (private ke liye)\n"
                "Ya kisi bhi channel ka message forward karke us pe reply karo `.settarget`"
            )
            return
    else:
        channel = parse_channel_input(arg)
        try:
            entity = await client.get_entity(channel)
        except Exception as e:
            await event.edit(f"❌ Error: `{e}`")
            return

    state["target"] = channel
    state["target_title"] = getattr(entity, "title", str(channel))
    save_state(state)
    await event.edit(f"✅ Target set: **{state['target_title']}**\n`{channel}`")


async def _count_filter(entity, f):
    """Return exact message count for a given filter using limit=0."""
    try:
        result = await client.get_messages(entity, limit=0, filter=f)
        return result.total
    except Exception:
        return 0


async def _fetch_channel_info(channel_id):
    """Fetch exact message counts per type using Telegram's built-in filters."""
    if not channel_id:
        return None
    try:
        entity = await client.get_entity(channel_id)

        # All counts fetched in parallel — each is a single fast API call
        (
            total,
            photos,
            videos,
            docs,
            gifs,
            voice,
            links,
        ) = await asyncio.gather(
            _count_filter(entity, None),                    # all messages
            _count_filter(entity, InputMessagesFilterPhotos()),
            _count_filter(entity, InputMessagesFilterVideo()),
            _count_filter(entity, InputMessagesFilterDocument()),
            _count_filter(entity, InputMessagesFilterGif()),
            _count_filter(entity, InputMessagesFilterVoice()),
            _count_filter(entity, InputMessagesFilterUrl()),
        )

        members = getattr(entity, "participants_count", None)
        return {
            "title":    getattr(entity, "title", str(channel_id)),
            "username": getattr(entity, "username", None),
            "total":    total,
            "members":  members,
            "photos":   photos,
            "videos":   videos,
            "docs":     docs,
            "gifs":     gifs,
            "voice":    voice,
            "links":    links,
        }
    except Exception as e:
        logger.warning(f"_fetch_channel_info error: {e}")
        return None


def _format_channel_block(info, label="Channel"):
    if not info:
        return f"{label}: ❌ Not set / unreachable"
    uname   = f"@{info['username']}" if info.get("username") else ""
    members = f"👥 Members: `{info['members']:,}`\n" if info.get("members") else ""
    return (
        f"**{info['title']}** {uname}\n"
        f"📨 Total: `{info['total']:,}`\n"
        f"{members}"
        f"📷 Photos: `{info['photos']:,}`  🎬 Videos: `{info['videos']:,}`\n"
        f"📄 Files: `{info['docs']:,}`  🔗 Links: `{info['links']:,}`\n"
        f"🎞 GIFs: `{info['gifs']:,}`  🎙 Voice: `{info['voice']:,}`"
    )


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.info$"))
async def cmd_info(event):
    if not is_owner(event.sender_id):
        return
    await event.edit("🔍 Fetching channel info...")
    src_id  = state.get("source")
    tgt_id  = state.get("target")
    last    = state.get("last_synced_id", 0)
    running = "🟢 Running" if state.get("running") else "🔴 Stopped"
    paused  = " (⏸️ Paused)" if state.get("paused") else ""

    src_info, tgt_info = await asyncio.gather(
        _fetch_channel_info(src_id),
        _fetch_channel_info(tgt_id),
    )

    src_block = _format_channel_block(src_info, "Source") if src_id else "📥 Source: ❌ Not set"
    tgt_block = _format_channel_block(tgt_info, "Target") if tgt_id else "📤 Target: ❌ Not set"

    await event.edit(
        f"📋 **Current Config**\n\n"
        f"📥 **Source**\n{src_block}\n\n"
        f"📤 **Target**\n{tgt_block}\n\n"
        f"🔢 Last synced ID: `{last}`\n"
        f"⚙️ Status: {running}{paused}"
    )


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.status$"))
async def cmd_status_userbot(event):
    if not is_owner(event.sender_id):
        return
    if not state.get("running"):
        stats = state.get("stats", reset_stats())
        await event.edit(
            f"🔴 **Not Running**\n\nLast session stats:\n{stats_text(stats)}"
        )
        return
    stats = state.get("stats", reset_stats())
    current = state.get("current_id", 0)
    total = state.get("total_msgs", 0)
    paused = "⏸️ Paused" if state.get("paused") else "🟢 Running"
    pct = f"{(current/total*100):.1f}%" if total else "?"
    await event.edit(
        f"📊 **Live Status**\n\n"
        f"State: {paused}\n"
        f"Progress: `{current}/{total}` ({pct})\n\n"
        f"{stats_text(stats)}"
    )


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.pause$"))
async def cmd_pause(event):
    if not is_owner(event.sender_id):
        return
    if not state.get("running"):
        await event.edit("❌ Koi sync chal nahi raha")
        return
    state["paused"] = True
    save_state(state)
    await event.edit("⏸️ Sync paused. `.resume` se resume karo")


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.resume$"))
async def cmd_resume(event):
    if not is_owner(event.sender_id):
        return
    if not state.get("paused"):
        await event.edit("❌ Paused nahi hai")
        return
    state["paused"] = False
    save_state(state)
    await event.edit("▶️ Sync resumed!")


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.stop$"))
async def cmd_stop(event):
    if not is_owner(event.sender_id):
        return
    state["running"] = False
    state["paused"] = False
    save_state(state)
    await event.edit("🛑 Sync stopped.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.reset$"))
async def cmd_reset(event):
    if not is_owner(event.sender_id):
        return
    state.clear()
    save_state(state)
    if Path(STATE_FILE).exists():
        os.remove(STATE_FILE)
    await event.edit("🔄 Config reset!")


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.sync$"))
async def cmd_sync(event):
    if not is_owner(event.sender_id):
        return
    await start_sync_userbot(event, reverse=True, min_id=0)


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.syncfrom (\d+)$"))
async def cmd_syncfrom(event):
    if not is_owner(event.sender_id):
        return
    min_id = int(event.pattern_match.group(1))
    await start_sync_userbot(event, reverse=True, min_id=min_id)


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.synclast (\d+)$"))
async def cmd_synclast(event):
    if not is_owner(event.sender_id):
        return
    n = int(event.pattern_match.group(1))
    await start_sync_userbot(event, reverse=False, limit=n)


# ════════════════════════════════════════════════════════
#  TELEGRAM BOT COMMANDS (via @BotFather bot)
# ════════════════════════════════════════════════════════

def bot_is_owner(update: Update) -> bool:
    return update.effective_user.id == OWNER_ID

async def bot_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        await update.message.reply_text("❌ Unauthorized! Sirf owner use kar sakta hai.")
        return
    await update.message.reply_text(
        "🤖 *Channel Copy Bot Active!*\n\n"
        "Neeche commands use karo:\n\n"
        "/help — Sab commands dekho\n"
        "/info — Current config\n"
        "/status — Live sync status",
        parse_mode="Markdown"
    )

async def bot_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    await update.message.reply_text(
        "🤖 *Bot Commands:*\n\n"
        "/setsource `@user / -100id / link` — Source set karo\n"
        "/setsource (msg forward karke) — Private channel source set\n"
        "/settarget `@user / -100id / link` — Target set karo\n"
        "/settarget (msg forward karke) — Private channel target set\n"
        "/info — Current config dekho\n"
        "/sync — Full sync start karo\n"
        "/syncfrom `<id>` — Message ID se sync karo\n"
        "/synclast `<n>` — Last N messages sync karo\n"
        "/tasks — Queue ke tasks dekho\n"
        "/autoforward on|off — New posts automatically copy karo\n"
        "/pause — Sync pause karo\n"
        "/resume — Sync resume karo\n"
        "/stop — Sync stop karo\n"
        "/status — Live status dekho\n"
        "/reset — Config reset karo\n\n"
        "⚠️ Sirf owner use kar sakta hai",
        parse_mode="Markdown"
    )

async def bot_setsource(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    msg = update.message

    # Option 1: Forwarded message se channel detect karo (no args needed)
    if not context.args:
        fwd_chat = getattr(msg.forward_from_chat, "id", None) if msg.forward_from_chat else None
        if fwd_chat:
            try:
                entity = await client.get_entity(fwd_chat)
                channel = fwd_chat
                state["source"] = channel
                state["source_title"] = getattr(entity, "title", str(channel))
                save_state(state)
                await msg.reply_text(
                    f"✅ Source set: *{state['source_title']}*\n`{channel}`",
                    parse_mode="Markdown"
                )
                return
            except Exception as e:
                await msg.reply_text(f"❌ Error: `{e}`", parse_mode="Markdown")
                return
        await msg.reply_text(
            "❌ Usage:\n"
            "`/setsource @username` — public channel\n"
            "`/setsource -100xxxxxxxxxx` — channel ID (private ke liye)\n"
            "Ya private channel ka koi message is chat mein forward karo, phir `/setsource` bina argument ke bhejo",
            parse_mode="Markdown"
        )
        return

    # Option 2: Argument diya gaya
    channel = parse_channel_input(context.args[0])
    try:
        entity = await client.get_entity(channel)
        state["source"] = channel
        state["source_title"] = getattr(entity, "title", str(channel))
        save_state(state)
        await msg.reply_text(
            f"✅ Source set: *{state['source_title']}*\n`{channel}`",
            parse_mode="Markdown"
        )
    except Exception as e:
        await msg.reply_text(f"❌ Error: `{e}`", parse_mode="Markdown")


async def bot_settarget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    msg = update.message

    # Option 1: Forwarded message se channel detect karo (no args needed)
    if not context.args:
        fwd_chat = getattr(msg.forward_from_chat, "id", None) if msg.forward_from_chat else None
        if fwd_chat:
            try:
                entity = await client.get_entity(fwd_chat)
                channel = fwd_chat
                state["target"] = channel
                state["target_title"] = getattr(entity, "title", str(channel))
                save_state(state)
                await msg.reply_text(
                    f"✅ Target set: *{state['target_title']}*\n`{channel}`",
                    parse_mode="Markdown"
                )
                return
            except Exception as e:
                await msg.reply_text(f"❌ Error: `{e}`", parse_mode="Markdown")
                return
        await msg.reply_text(
            "❌ Usage:\n"
            "`/settarget @username` — public channel\n"
            "`/settarget -100xxxxxxxxxx` — channel ID (private ke liye)\n"
            "Ya private channel ka koi message is chat mein forward karo, phir `/settarget` bina argument ke bhejo",
            parse_mode="Markdown"
        )
        return

    # Option 2: Argument diya gaya
    channel = parse_channel_input(context.args[0])
    try:
        entity = await client.get_entity(channel)
        state["target"] = channel
        state["target_title"] = getattr(entity, "title", str(channel))
        save_state(state)
        await msg.reply_text(
            f"✅ Target set: *{state['target_title']}*\n`{channel}`",
            parse_mode="Markdown"
        )
    except Exception as e:
        await msg.reply_text(f"❌ Error: `{e}`", parse_mode="Markdown")

async def bot_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    await update.message.reply_text("🔍 Fetching channel info...", parse_mode="Markdown")
    src_id  = state.get("source")
    tgt_id  = state.get("target")
    last    = state.get("last_synced_id", 0)
    running = "🟢 Running" if state.get("running") else "🔴 Stopped"
    paused  = " (⏸️ Paused)" if state.get("paused") else ""

    src_info, tgt_info = await asyncio.gather(
        _fetch_channel_info(src_id),
        _fetch_channel_info(tgt_id),
    )

    src_block = _format_channel_block(src_info, "Source") if src_id else "❌ Not set"
    tgt_block = _format_channel_block(tgt_info, "Target") if tgt_id else "❌ Not set"

    # Markdown-safe version for Bot API (no bold via **)
    src_block_md = src_block.replace("**", "*")
    tgt_block_md = tgt_block.replace("**", "*")

    await update.message.reply_text(
        f"📋 *Current Config*\n\n"
        f"📥 *Source*\n{src_block_md}\n\n"
        f"📤 *Target*\n{tgt_block_md}\n\n"
        f"🔢 Last synced ID: `{last}`\n"
        f"⚙️ Status: {running}{paused}",
        parse_mode="Markdown"
    )

async def bot_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    if not state.get("running"):
        stats = state.get("stats", reset_stats())
        await update.message.reply_text(
            f"🔴 *Not Running*\n\nLast session stats:\n{stats_text(stats)}",
            parse_mode="Markdown"
        )
        return
    stats = state.get("stats", reset_stats())
    current = state.get("current_id", 0)
    total = state.get("total_msgs", 0)
    paused = "⏸️ Paused" if state.get("paused") else "🟢 Running"
    pct = f"{(current/total*100):.1f}%" if total else "?"
    await update.message.reply_text(
        f"📊 *Live Status*\n\n"
        f"State: {paused}\n"
        f"Progress: `{current}/{total}` ({pct})\n\n"
        f"{stats_text(stats)}",
        parse_mode="Markdown"
    )

async def bot_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    if not state.get("running"):
        await update.message.reply_text("❌ Koi sync chal nahi raha")
        return
    state["paused"] = True
    save_state(state)
    await update.message.reply_text("⏸️ Sync paused! /resume se resume karo.")

async def bot_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    if not state.get("paused"):
        await update.message.reply_text("❌ Sync paused nahi hai")
        return
    state["paused"] = False
    save_state(state)
    await update.message.reply_text("▶️ Sync resumed!")

async def bot_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    state["running"] = False
    state["paused"] = False
    save_state(state)
    await update.message.reply_text("🛑 Sync stopped.")

async def bot_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    state.clear()
    save_state(state)
    if Path(STATE_FILE).exists():
        os.remove(STATE_FILE)
    await update.message.reply_text("🔄 Config reset kar diya!")

async def bot_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    msg = await update.message.reply_text("⏳ Full sync shuru ho rahi hai...")
    asyncio.create_task(start_sync_bot(msg, reverse=True, min_id=0))

async def bot_syncfrom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    if not context.args:
        await update.message.reply_text("❌ Usage: /syncfrom <message_id>")
        return
    try:
        min_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Valid message ID do (number)")
        return
    msg = await update.message.reply_text(f"⏳ Message ID {min_id} se sync shuru ho rahi hai...")
    asyncio.create_task(start_sync_bot(msg, reverse=True, min_id=min_id))

async def bot_synclast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    if not context.args:
        await update.message.reply_text("❌ Usage: /synclast <number>")
        return
    try:
        n = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Valid number do")
        return
    msg = await update.message.reply_text(f"⏳ Last {n} messages sync ho rahi hai...")
    asyncio.create_task(start_sync_bot(msg, reverse=False, limit=n))


async def bot_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    tasks = state.get("tasks", [])
    if not tasks:
        await update.message.reply_text("📋 Queue empty hai.")
        return
    active = state.get("active_task_id")
    lines = ["📋 *Task Queue*"]
    for task in tasks[-15:]:
        marker = " 🔄" if task.get("id") == active else ""
        lines.append(
            f"`{task.get('id')}` — {task.get('mode', 'sync')} — "
            f"{task.get('status', 'queued')}{marker}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def bot_autoforward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    if not context.args or context.args[0].lower() not in {"on", "off"}:
        await update.message.reply_text(
            f"Auto-forward ab {'ON' if state.get('auto_forward') else 'OFF'} hai.\n"
            "Use: /autoforward on ya /autoforward off"
        )
        return
    enabled = context.args[0].lower() == "on"
    if enabled and (not state.get("source") or not state.get("target")):
        await update.message.reply_text("❌ Pehle /setsource aur /settarget set karo.")
        return
    state["auto_forward"] = enabled
    save_state(state)
    await update.message.reply_text(
        f"✅ Auto-forward {'ON' if enabled else 'OFF'} kar diya.\n"
        "New posts direct copy honge; restricted post par download/upload fallback hoga."
    )


# ════════════════════════════════════════════════════════
#  CORE SYNC ENGINE
# ════════════════════════════════════════════════════════

async def start_sync_userbot(event, reverse=True, min_id=0, limit=None):
    if not state.get("source") or not state.get("target"):
        await event.edit("❌ Pehle `.setsource` aur `.settarget` karo!")
        return
    source, target = state["source"], state["target"]
    task = _queue_sync(source, target, reverse, min_id, limit, event,
                       False, "full" if not limit and not min_id else "range")
    await event.edit(
        f"⏳ Task `{task['id']}` queued\n"
        f"📥 {task['source_title']} → 📤 {task['target_title']}\n"
        f"Queue mein {len(_task_queue)} task(s) hain."
    )


async def start_sync_bot(progress_msg, reverse=True, min_id=0, limit=None):
    if not state.get("source") or not state.get("target"):
        await progress_msg.edit_text("❌ Pehle /setsource aur /settarget karo!")
        return
    task = _queue_sync(
        state["source"], state["target"], reverse, min_id, limit,
        progress_msg, True, "full" if not limit and not min_id else "range"
    )
    await progress_msg.edit_text(
        f"⏳ Task {task['id']} queued\n"
        f"{task['source_title']} → {task['target_title']}\n"
        f"Queue mein {len(_task_queue)} task(s) hain."
    )


def _make_progress_bar(done, total, width=14):
    if total <= 0:
        return "░" * width
    filled = int(width * done / total)
    return "▓" * filled + "░" * (width - filled)


def _fmt_eta(seconds):
    if seconds <= 0:
        return "?"
    td = timedelta(seconds=int(seconds))
    h, rem = divmod(td.seconds, 3600)
    m, s = divmod(rem, 60)
    if td.days or h:
        return f"{td.days*24+h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


TYPE_ICON = {
    "text":  "📝",
    "photo": "📷",
    "video": "🎬",
    "doc":   "📄",
    "other": "📎",
}


async def _run_sync(progress_msg, source, target, reverse, min_id, limit,
                    is_bot=False, pair_id="default", config=None, task_id=None,
                    source_title=None, target_title=None):
    async def edit_msg(text, parse_mode=None):
        try:
            if is_bot:
                kwargs = {"parse_mode": parse_mode} if parse_mode else {}
                await progress_msg.edit_text(text, **kwargs)
            else:
                await progress_msg.edit(text, parse_mode=parse_mode)
        except Exception:
            pass

    src_title  = source_title or state.get("source_title", str(source))
    tgt_title  = target_title or state.get("target_title", str(target))
    config = config or _pair_config(_pair_by_id(pair_id))

    try:
        source_entity = await client.get_entity(source)
        target_entity = await client.get_entity(target)

        total = await client.get_messages(source_entity, limit=0)
        total_count = total.total if not limit else limit
        state["total_msgs"] = total_count
        save_state(state)

        start_time = time.time()
        logger.info(f"Sync started | src={src_title} | tgt={tgt_title} | total={total_count}")
        _log_live(f"🚀 Sync shuru | {src_title} → {tgt_title} | {total_count} messages")

        await edit_msg(
            f"⚡ Sync शुरू हो गया!\n\n"
            f"📥 Source: {src_title}\n"
            f"📤 Target: {tgt_title}\n"
            f"📊 Total: {total_count} messages\n\n"
            f"Pehla message bheja ja raha hai..."
        )

        count   = 0
        failed  = 0
        stats   = reset_stats()
        _last_edit = 0   # throttle edit calls (max 1 per sec)

        async for message in client.iter_messages(
            source_entity,
            reverse=reverse,
            min_id=min_id,
            limit=limit
        ):
            controls = state.setdefault("task_controls", {})
            control = controls.setdefault(task_id or "legacy", {"paused": False, "cancelled": False})
            if control.get("cancelled") or not state.get("running"):
                logger.info("Sync stopped by user command")
                break

            while control.get("paused") and not control.get("cancelled"):
                await asyncio.sleep(2)
                control = state.get("task_controls", {}).get(task_id or "legacy", control)
            if control.get("cancelled"):
                break

            if not _message_allowed(message, config):
                stats = state.get("stats", stats)
                stats["skipped"] = stats.get("skipped", 0) + 1
                _log_live(f"⏭️ Filter skipped ID={message.id}")
                continue
            dedupe = state.setdefault("dedupe", {})
            dkey = _dedupe_key(pair_id, message)
            if dkey in dedupe:
                stats["duplicates"] = stats.get("duplicates", 0) + 1
                _log_live(f"⏭️ Duplicate skipped ID={message.id}")
                continue

            # ── Progress callback (live log + bot preview) ────
            _prog_last_live  = [0.0]   # last _live_log update time
            _prog_last_edit  = [0.0]   # last bot-message edit time
            _prog_last_log10 = [-1]    # last 10% milestone logged to file
            _prog_spd_time   = [time.time()]  # speed window start
            _prog_spd_bytes  = [0]            # bytes at speed window start
            _prog_last_dashboard = [0.0]      # dashboard push throttle

            def _fname_from_msg(msg):
                if isinstance(msg.media, MessageMediaDocument) and msg.media.document:
                    for attr in msg.media.document.attributes:
                        if isinstance(attr, DocumentAttributeFilename):
                            return attr.file_name
                    mime = getattr(msg.media.document, "mime_type", "")
                    if "video" in mime:   return f"video_{msg.id}.mp4"
                    if "audio" in mime:   return f"audio_{msg.id}.mp3"
                    return f"file_{msg.id}"
                if isinstance(msg.media, MessageMediaPhoto):
                    return f"photo_{msg.id}.jpg"
                return f"file_{msg.id}"

            def _fmt_speed(bps: float) -> str:
                if bps >= 1_048_576:  return f"{bps/1_048_576:.2f} MB/s"
                if bps >= 1024:       return f"{bps/1024:.1f} KB/s"
                return f"{bps:.0f} B/s"

            def _mini_bar(pct, w=10):
                filled = int(w * pct / 100)
                return "█" * filled + "░" * (w - filled)

            async def on_progress(phase: str, current: int, total: int):
                if total <= 0:
                    return
                now      = time.time()
                pct      = int(current / total * 100)
                cur_mb   = current / 1_048_576
                tot_mb   = total   / 1_048_576
                icon     = "📥" if phase == "download" else "📤"
                phase_lbl = "Downloading" if phase == "download" else "Uploading"

                # ── Instant transfer speed (sliding window) ──
                dt = now - _prog_spd_time[0]
                if dt >= 0.5:
                    bps = (current - _prog_spd_bytes[0]) / dt
                    _prog_spd_time[0]  = now
                    _prog_spd_bytes[0] = current
                    speed_str = _fmt_speed(max(bps, 0))
                else:
                    speed_str = "…"

                # ── Update state["transfer"] for /api/status ─
                fname = _fname_from_msg(message)
                state["transfer"] = {
                    "phase":    phase_lbl,
                    "file":     fname,
                    "pct":      pct,
                    "cur_mb":   round(cur_mb, 2),
                    "tot_mb":   round(tot_mb, 2),
                    "speed":    speed_str,
                }
                if now - _prog_last_dashboard[0] >= 0.5:
                    _prog_last_dashboard[0] = now
                    _dashboard_changed()

                # ── Live log update every 2s ───────────────
                if now - _prog_last_live[0] >= 2:
                    _prog_last_live[0] = now
                    bar = _mini_bar(pct)
                    _log_live(
                        f"{icon} {phase_lbl}: {fname} "
                        f"[{bar}] {pct}% "
                        f"({cur_mb:.1f}/{tot_mb:.1f} MB) "
                        f"⚡ {speed_str}"
                    )

                # ── File log every 10% milestone ─────────────
                milestone = (pct // 10) * 10
                if milestone != _prog_last_log10[0] and pct >= milestone and milestone > 0:
                    _prog_last_log10[0] = milestone
                    logger.info(
                        f"{icon} {phase_lbl} {milestone}% "
                        f"({cur_mb:.1f}/{tot_mb:.1f} MB) {speed_str} "
                        f"msg_id={message.id}"
                    )

                # ── Bot message edit every 4s during transfer ─
                if now - _prog_last_edit[0] >= 4.0:
                    _prog_last_edit[0] = now
                    elapsed  = now - start_time
                    msg_spd  = count / (elapsed / 60) if elapsed > 0 else 0
                    await edit_msg(
                        f"⚡ *Syncing...*\n\n"
                        f"📥 `{src_title}`\n"
                        f"📤 `{tgt_title}`\n\n"
                        f"📊 Msgs: *{count}* / {total_count}\n\n"
                        f"{icon} *{phase_lbl}...*\n"
                        f"`{_make_progress_bar(pct, 100, 12)}` {pct}%\n"
                        f"📦 {cur_mb:.1f} / {tot_mb:.1f} MB  ⚡ {speed_str}\n\n"
                        f"🚀 {msg_spd:.1f} msg/min",
                        parse_mode="Markdown"
                    )
            # ─────────────────────────────────────────────────

            try:
                msg_type = get_msg_type(message)
                logger.debug(f"Sending msg_id={message.id} type={msg_type}")

                # Retry loop for transient errors (max 3 attempts)
                MAX_RETRIES = 3
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        sent = await send_message(
                            target_entity, message,
                            on_progress=on_progress if msg_type != "text" else None,
                            config=config, source_title=src_title
                        )
                        break   # success
                    except (FilePartMissingError, TgTimeoutError, ServerError) as retry_err:
                        if attempt == MAX_RETRIES:
                            raise
                        wait = 5 * attempt
                        logger.warning(
                            f"⚠️ Transient error (attempt {attempt}/{MAX_RETRIES}) "
                            f"msg_id={message.id}: {retry_err} — retry in {wait}s"
                        )
                        await asyncio.sleep(wait)

                if sent:
                    _remember_mapping(pair_id, message.id, sent)
                    dedupe[dkey] = datetime.now().isoformat(timespec="seconds")
                    # Keep the latest IDs only; this prevents unbounded state growth.
                    if len(dedupe) > 10000:
                        for old_key in list(dedupe)[:2000]:
                            dedupe.pop(old_key, None)
                    count += 1
                    stats[msg_type] = stats.get(msg_type, 0) + 1
                    state["last_synced_id"] = message.id
                    state["current_id"]     = count
                    state["stats"]          = stats
                    state.pop("transfer", None)   # clear transfer card
                    save_state(state)
                    _log_live(
                        f"✅ [{count}/{total_count}] ID={message.id} "
                        f"{TYPE_ICON.get(msg_type, '📎')} {msg_type}"
                    )
                    logger.info(
                        f"✅ Sent [{count}/{total_count}] id={message.id} type={msg_type}"
                    )

                    # Main progress update after each successful send
                    now = time.time()
                    if now - _last_edit >= 1.0:
                        _last_edit = now
                        elapsed   = now - start_time
                        speed     = count / (elapsed / 60) if elapsed > 0 else 0
                        remaining = (total_count - count) / (speed / 60) if speed > 0 else 0
                        pct       = count / total_count * 100 if total_count else 0
                        bar       = _make_progress_bar(count, total_count)

                        await edit_msg(
                            f"⚡ *Syncing...*\n\n"
                            f"📥 `{src_title}`\n"
                            f"📤 `{tgt_title}`\n\n"
                            f"`{bar}` {pct:.1f}%\n"
                            f"*{count}* / {total_count} msgs\n\n"
                            f"{TYPE_ICON[msg_type]} Last: `#{message.id}`\n\n"
                            f"📝 Text:  {stats['text']}   "
                            f"📷 Photo: {stats['photo']}\n"
                            f"🎬 Video: {stats['video']}   "
                            f"📄 Doc:   {stats['doc']}\n"
                            f"📎 Other: {stats['other']}   "
                            f"❌ Failed: {stats['failed']}\n\n"
                            f"🚀 Speed: {speed:.1f} msg/min\n"
                            f"⏳ ETA:   {_fmt_eta(remaining)}\n"
                            f"🕐 Started: {datetime.fromtimestamp(start_time).strftime('%I:%M %p')}",
                            parse_mode="Markdown"
                        )

                    if count % BATCH_SIZE == 0:
                        logger.info(f"Batch pause {BATCH_DELAY}s after {count} msgs")
                        await asyncio.sleep(max(BATCH_DELAY, config["rate_delay"]))
                    else:
                        await asyncio.sleep(config["rate_delay"])

            except FloodWaitError as e:
                wait = e.seconds + 10
                logger.warning(f"FloodWait {wait}s after msg_id={message.id}")
                await edit_msg(
                    f"⏸️ *FloodWait!*\n\n"
                    f"Telegram ne slow karne kaha\n"
                    f"⏱ Waiting: *{wait}s*\n\n"
                    f"Progress: {count}/{total_count}",
                    parse_mode="Markdown"
                )
                await asyncio.sleep(wait)

            except SlowModeWaitError as e:
                wait = e.seconds + 5
                logger.warning(f"SlowMode {wait}s after msg_id={message.id}")
                await edit_msg(
                    f"🐢 *SlowMode Active!*\n\nTarget channel ka slow mode on hai\n"
                    f"⏱ Wait: *{wait}s*",
                    parse_mode="Markdown"
                )
                await asyncio.sleep(wait)

            except ChatWriteForbiddenError:
                logger.error("ChatWriteForbiddenError — no write permission on target")
                await edit_msg("❌ Target channel mein write permission nahi hai!")
                state["running"] = False
                save_state(state)
                return

            except (FileReferenceExpiredError, MediaInvalidError) as e:
                failed += 1
                stats["failed"] = failed
                state["stats"]  = stats
                state.pop("transfer", None)
                save_state(state)
                _log_live(f"⏭️ Skipped ID={message.id} — media unavailable: {type(e).__name__}")
                logger.warning(
                    f"⏭️ Skipped msg_id={message.id} — media unavailable: {type(e).__name__}"
                )
                await asyncio.sleep(MSG_DELAY)
                continue

            except BadMessageError as e:
                failed += 1
                stats["failed"] = failed
                state["stats"]  = stats
                state.pop("transfer", None)
                save_state(state)
                _log_live(f"❌ BadMessage ID={message.id}: {e}")
                logger.error(f"BadMessageError msg_id={message.id}: {e}")
                await asyncio.sleep(MSG_DELAY)
                continue

            except Exception as e:
                failed += 1
                stats["failed"] = failed
                state["stats"]  = stats
                state.pop("transfer", None)
                save_state(state)
                _log_live(f"❌ Failed ID={message.id} [{type(e).__name__}]: {str(e)[:80]}")
                logger.error(
                    f"❌ msg_id={message.id} FAILED [{type(e).__name__}]: {e}"
                )
                await asyncio.sleep(MSG_DELAY)
                continue

        elapsed_total = time.time() - start_time
        state["running"] = False
        state["stats"]   = stats
        state.pop("transfer", None)
        save_state(state)
        _log_live(
            f"🏁 Sync complete! ✅ {count} sent  ❌ {failed} failed  "
            f"⏱ {_fmt_eta(elapsed_total)}"
        )
        logger.info(
            f"Sync complete | sent={count} failed={failed} "
            f"time={_fmt_eta(elapsed_total)}"
        )

        await edit_msg(
            f"✅ *Sync Complete!*\n\n"
            f"📥 `{src_title}`\n"
            f"📤 `{tgt_title}`\n\n"
            f"📝 Text:  {stats['text']}   "
            f"📷 Photo: {stats['photo']}\n"
            f"🎬 Video: {stats['video']}   "
            f"📄 Doc:   {stats['doc']}\n"
            f"📎 Other: {stats['other']}   "
            f"❌ Failed: {stats['failed']}\n"
            f"📊 Total:  {count}\n\n"
            f"⏱ Time: {_fmt_eta(elapsed_total)}\n"
            f"🕐 {datetime.now().strftime('%d %b %Y, %I:%M %p')}",
            parse_mode="Markdown"
        )

    except ChannelPrivateError:
        logger.error("ChannelPrivateError — source is private or no access")
        state["running"] = False
        save_state(state)
        await edit_msg("❌ Source channel private hai ya access nahi hai!")

    except Exception as e:
        logger.error(f"Fatal sync error: {e}")
        state["running"] = False
        save_state(state)
        await edit_msg(f"❌ Fatal error: {e}")


async def send_message(target, message, on_progress=None, config=None, source_title=""):
    # Telegram can often reuse the source media directly. This creates a new
    # message without a forward header and avoids download/upload round trips.
    # Restricted or otherwise non-copyable messages fall back to the existing
    # disk-based path below.
    config = config or _pair_config(None)
    needs_rewrite = any([
        config["caption_prefix"], config["caption_suffix"],
        config["remove_links"], config["remove_source_name"],
    ])
    try:
        if needs_rewrite:
            raise ValueError("caption rewrite requires upload/copy path")
        sent = await client.send_message(target, message, parse_mode="md", link_preview=False)
        logger.info(f"⚡ Copied directly msg_id={message.id} (no forward tag)")
        return sent or True
    except Exception as copy_error:
        logger.debug(f"Direct copy unavailable for msg_id={message.id}: {copy_error}")

    if message.media and not isinstance(message.media, MessageMediaWebPage):

        async def dl_cb(current, total):
            if on_progress:
                await on_progress("download", current, total)

        tmp_path = await fast_download(message.media, progress_cb=dl_cb if on_progress else None)
        if not tmp_path or not Path(tmp_path).exists():
            raise Exception("Media download failed")

        caption = _edited_caption(message, config, source_title)
        send_path = Path(tmp_path)

        # ── Type detect karo ──────────────────────────────
        original_filename = None
        mime = ""
        attributes = []

        if isinstance(message.media, MessageMediaDocument) and message.media.document:
            doc = message.media.document
            mime = getattr(doc, "mime_type", "")
            attributes = doc.attributes or []
            for attr in attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    original_filename = attr.file_name
                    break

        is_video = "video" in mime
        is_audio = "audio" in mime or "ogg" in mime
        is_photo = isinstance(message.media, MessageMediaPhoto)

        # Rename to original filename if available
        if original_filename:
            named_path = Path("/tmp") / original_filename
            send_path.rename(named_path)
            send_path = named_path

        logger.debug(f"Uploading {send_path.stat().st_size//1024}KB | mime={mime} | file={send_path.name}")

        async def ul_cb(current, total):
            if on_progress:
                await on_progress("upload", current, total)

        try:
            if is_photo:
                # Photo as photo
                sent = await client.send_file(
                    target, str(send_path),
                    caption=caption, parse_mode="md",
                    force_document=False,
                    part_size_kb=1024,
                    progress_callback=ul_cb,
                )
            elif is_video:
                # Video as streamable video (not document)
                sent = await client.send_file(
                    target, str(send_path),
                    caption=caption, parse_mode="md",
                    force_document=False,   # streamable video
                    supports_streaming=True,
                    attributes=attributes,  # original duration/dimensions preserve
                    part_size_kb=1024,
                    progress_callback=ul_cb,
                )
            elif is_audio:
                # Audio as audio player
                sent = await client.send_file(
                    target, str(send_path),
                    caption=caption, parse_mode="md",
                    force_document=False,
                    attributes=attributes,  # title/duration preserve
                    part_size_kb=1024,
                    progress_callback=ul_cb,
                )
            else:
                # PDF, CSV, ZIP, etc — document as document
                sent = await client.send_file(
                    target, str(send_path),
                    caption=caption, parse_mode="md",
                    force_document=True,
                    attributes=attributes,
                    part_size_kb=1024,
                    progress_callback=ul_cb,
                )
        finally:
            try:
                send_path.unlink(missing_ok=True)
            except Exception:
                pass

        return sent or True

    elif message.text:
        sent = await client.send_message(
            target, caption if "caption" in locals() else _edited_caption(message, config, source_title),
            parse_mode="md", link_preview=False
        )
        return sent or True

    return False


def _telegram_chat_id(entity):
    entity_id = getattr(entity, "id", entity if isinstance(entity, int) else None)
    if entity_id is None:
        return None
    return int(f"-100{entity_id}") if entity_id > 0 else entity_id


@client.on(events.NewMessage)
async def auto_forward_handler(event):
    """Mirror new source posts immediately, using the same copy/fallback path."""
    if not state.get("auto_forward") or not state.get("source") or not state.get("target"):
        return
    try:
        source_entity = await client.get_entity(state["source"])
        if event.chat_id != _telegram_chat_id(source_entity):
            return
        # A single handler invocation per Telegram update, even after reconnects.
        message = event.message
        if not message:
            return
        async with _auto_forward_lock:
            target = await client.get_entity(state["target"])
            sent = await send_message(target, message)
            auto_stats = state.setdefault("auto_stats", {"sent": 0, "failed": 0})
            if sent:
                auto_stats["sent"] += 1
                _log_live(f"⚡ Auto-forwarded ID={message.id} (no forward tag)")
            else:
                auto_stats["failed"] += 1
                _log_live(f"❌ Auto-forward failed ID={message.id}")
            state["auto_stats"] = auto_stats
            state["auto_last_id"] = message.id
            save_state(state)
    except Exception as exc:
        logger.exception("Auto-forward failed: %s", exc)
        auto_stats = state.setdefault("auto_stats", {"sent": 0, "failed": 0})
        auto_stats["failed"] += 1
        state["auto_stats"] = auto_stats
        save_state(state)


@client.on(events.MessageEdited)
async def edit_sync_handler(event):
    if not state.get("source") or event.chat_id != _telegram_chat_id(await client.get_entity(state["source"])):
        return
    message = event.message
    mapping = state.get("message_map", {}).get("default", {}).get(str(message.id))
    if not mapping or not state.get("target"):
        return
    try:
        await client.edit_message(state["target"], mapping, text=message.text or "")
        _log_live(f"✏️ Edited target message for source ID={message.id}")
    except Exception as exc:
        logger.warning("Edit sync failed for %s: %s", message.id, exc)


def get_msg_type(message) -> str:
    if not message.media or isinstance(message.media, MessageMediaWebPage):
        return "text"
    if isinstance(message.media, MessageMediaPhoto):
        return "photo"
    if isinstance(message.media, MessageMediaDocument):
        doc = message.media.document
        mime = getattr(doc, "mime_type", "")
        if "video" in mime:
            return "video"
        return "doc"
    return "other"


# ════════════════════════════════════════════════════════
#  FLASK WEB SERVER (keeps Replit alive + full dashboard)
# ════════════════════════════════════════════════════════

flask_app  = Flask(__name__)
_start_time = time.time()
_loop: asyncio.AbstractEventLoop = None   # set in main()
_bot_application = None
_health_snapshot = {}


async def _notify_owner(text):
    if _bot_application:
        try:
            await _bot_application.bot.send_message(chat_id=OWNER_ID, text=text)
        except Exception as exc:
            logger.warning("Owner alert failed: %s", exc)


async def health_monitor():
    while True:
        await asyncio.sleep(300)
        snapshot = {}
        for label, channel in (("source", state.get("source")), ("target", state.get("target"))):
            if not channel:
                snapshot[label] = "not configured"
                continue
            try:
                await client.get_entity(channel)
                snapshot[label] = "ok"
            except Exception as exc:
                snapshot[label] = type(exc).__name__
        connected = client.is_connected()
        snapshot["login"] = "ok" if connected else "offline"
        if snapshot != _health_snapshot:
            old = dict(_health_snapshot)
            _health_snapshot.update(snapshot)
            state["health"] = snapshot
            save_state(state)
            if old and snapshot != old:
                await _notify_owner("⚠️ Channel health changed:\n" + "\n".join(
                    f"{key}: {value}" for key, value in snapshot.items()
                ))


# ── Async helpers ──────────────────────────────────────

class WebEvent:
    """Dummy Telegram event for web-triggered sync operations."""
    async def edit(self, text):  logger.info(f"[WEB] {str(text)[:200]}")
    async def reply(self, text): logger.info(f"[WEB] {str(text)[:200]}")


def _run_async(coro, timeout=25):
    """Run a coroutine from Flask (sync thread) and return result."""
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result(timeout=timeout)


def _run_bg(coro):
    """Fire-and-forget a coroutine from Flask (no wait)."""
    asyncio.run_coroutine_threadsafe(coro, _loop)


async def _set_source(channel_input):
    ch = parse_channel_input(channel_input)
    entity = await client.get_entity(ch)
    state["source"] = ch
    state["source_title"] = getattr(entity, "title", str(ch))
    save_state(state)
    return {"ok": True, "title": state["source_title"]}


async def _set_target(channel_input):
    ch = parse_channel_input(channel_input)
    entity = await client.get_entity(ch)
    state["target"] = ch
    state["target_title"] = getattr(entity, "title", str(ch))
    save_state(state)
    return {"ok": True, "title": state["target_title"]}


async def _create_pair(payload):
    source = parse_channel_input(str(payload.get("source", "")))
    target = parse_channel_input(str(payload.get("target", "")))
    if not source or not target:
        raise ValueError("Source and target are required")
    src_entity, tgt_entity = await asyncio.gather(
        client.get_entity(source), client.get_entity(target)
    )
    pair = {
        "id": uuid.uuid4().hex[:8],
        "name": str(payload.get("name") or "Pair"),
        "source": source, "target": target,
        "source_title": getattr(src_entity, "title", str(source)),
        "target_title": getattr(tgt_entity, "title", str(target)),
        "allowed_types": payload.get("allowed_types") or ["text", "photo", "video", "doc", "other"],
        "include_keywords": [x.strip() for x in str(payload.get("include_keywords", "")).split(",") if x.strip()],
        "exclude_keywords": [x.strip() for x in str(payload.get("exclude_keywords", "")).split(",") if x.strip()],
        "caption_prefix": str(payload.get("caption_prefix", "")),
        "caption_suffix": str(payload.get("caption_suffix", "")),
        "remove_links": bool(payload.get("remove_links")),
        "remove_source_name": bool(payload.get("remove_source_name")),
        "rate_delay": max(0, min(float(payload.get("rate_delay", MSG_DELAY)), 300)),
    }
    state.setdefault("pairs", []).append(pair)
    save_state(state)
    return pair


# ── Routes ─────────────────────────────────────────────

@flask_app.route("/")
def index():
    return render_template("index.html")


@flask_app.route("/favicon.ico")
def favicon():
    return Response(status=204)


def _status_payload():
    running = state.get("running", False)
    paused  = state.get("paused", False)
    stats   = state.get("stats", {})
    cur     = state.get("current_id", 0)
    tot     = state.get("total_msgs", 0)
    elapsed = int(time.time() - _start_time)
    h, rem  = divmod(elapsed, 3600)
    m, s    = divmod(rem, 60)
    connected = client.is_connected()
    return {
        "connected": connected,
        "connection_label": "Connected" if connected else "Offline",
        "running": running,
        "paused":  paused,
        "source":  state.get("source_title", ""),
        "target":  state.get("target_title", ""),
        "pairs":   state.get("pairs", []),
        "last_id": state.get("last_synced_id", 0),
        "current": cur,
        "total":   tot,
        "pct":     round(cur / tot * 100, 1) if tot else 0,
        "stats":   stats,
        "auto_forward": bool(state.get("auto_forward")),
        "auto_stats": state.get("auto_stats", {"sent": 0, "failed": 0}),
        "tasks": state.get("tasks", []),
        "queue_size": len(_task_queue),
        "health": state.get("health", {}),
        "transfer": state.get("transfer"),
        "uptime_seconds": elapsed,
        "uptime": f"{h}h {m}m {s}s" if h else f"{m}m {s}s",
    }


def _dashboard_payload():
    return {
        "status": _status_payload(),
        "logs": list(_live_log),
    }


@flask_app.route("/api/status")
def api_status():
    return jsonify(_status_payload())


@flask_app.route("/api/bootstrap")
def api_bootstrap():
    """One initial dashboard snapshot; later changes arrive over SSE."""
    return jsonify(_dashboard_payload())


@flask_app.route("/api/events")
def api_events():
    """Push dashboard snapshots only when state or logs actually change."""
    @stream_with_context
    def stream():
        last_revision = -1
        while True:
            with _dashboard_condition:
                if last_revision == _dashboard_revision:
                    _dashboard_condition.wait(timeout=25)
                current_revision = _dashboard_revision

            if current_revision == last_revision:
                # Keep proxies from closing a healthy idle stream. This is
                # not a dashboard request and carries no data update.
                yield ": keep-alive\n\n"
                continue

            last_revision = current_revision
            payload = json.dumps(_dashboard_payload(), ensure_ascii=False)
            yield f"event: dashboard\ndata: {payload}\nid: {last_revision}\n\n"

    return Response(
        stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@flask_app.route("/api/setsource", methods=["POST"])
def api_setsource():
    ch = (request.json or {}).get("channel", "").strip()
    if not ch:
        return jsonify({"ok": False, "error": "Channel required"})
    try:
        return jsonify(_run_async(_set_source(ch)))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@flask_app.route("/api/settarget", methods=["POST"])
def api_settarget():
    ch = (request.json or {}).get("channel", "").strip()
    if not ch:
        return jsonify({"ok": False, "error": "Channel required"})
    try:
        return jsonify(_run_async(_set_target(ch)))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@flask_app.route("/api/pairs", methods=["GET"])
def api_pairs():
    return jsonify({"ok": True, "pairs": state.get("pairs", [])})


@flask_app.route("/api/pairs", methods=["POST"])
def api_add_pair():
    try:
        pair = _run_async(_create_pair(request.json or {}))
        return jsonify({"ok": True, "pair": pair})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@flask_app.route("/api/pairs/<pair_id>", methods=["DELETE"])
def api_delete_pair(pair_id):
    before = len(state.get("pairs", []))
    state["pairs"] = [p for p in state.get("pairs", []) if p.get("id") != pair_id]
    if len(state["pairs"]) == before:
        return jsonify({"ok": False, "error": "Pair not found"})
    save_state(state)
    return jsonify({"ok": True})


@flask_app.route("/api/sync", methods=["POST"])
def api_sync():
    if not state.get("source") or not state.get("target"):
        return jsonify({"ok": False, "error": "Set source and target first"})
    task = _queue_sync(state["source"], state["target"], True, 0, None,
                       WebEvent(), False, "full")
    return jsonify({"ok": True, "task": _task_view(task)})


@flask_app.route("/api/syncfrom", methods=["POST"])
def api_syncfrom():
    if not state.get("source") or not state.get("target"):
        return jsonify({"ok": False, "error": "Set source and target first"})
    try:
        mid = int((request.json or {}).get("min_id", 0))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Valid message ID required"})
    task = _queue_sync(state["source"], state["target"], True, mid, None,
                       WebEvent(), False, "from_id")
    return jsonify({"ok": True, "task": _task_view(task)})


@flask_app.route("/api/synclast", methods=["POST"])
def api_synclast():
    if not state.get("source") or not state.get("target"):
        return jsonify({"ok": False, "error": "Set source and target first"})
    try:
        n = int((request.json or {}).get("n", 10))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Valid message count required"})
    if n < 1:
        return jsonify({"ok": False, "error": "Message count must be positive"})
    task = _queue_sync(state["source"], state["target"], False, 0, n,
                       WebEvent(), False, "last")
    return jsonify({"ok": True, "task": _task_view(task)})


@flask_app.route("/api/tasks", methods=["GET"])
def api_tasks():
    return jsonify({"ok": True, "tasks": state.get("tasks", []),
                    "queue_size": len(_task_queue)})


@flask_app.route("/api/tasks", methods=["POST"])
def api_create_task():
    payload = request.json or {}
    pair = _pair_by_id(payload.get("pair_id"))
    if not pair:
        return jsonify({"ok": False, "error": "Select a valid source-target pair"})
    mode = payload.get("mode", "full")
    try:
        limit = int(payload.get("limit", 0)) or None
        min_id = int(payload.get("min_id", 0))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Message limits must be numbers"})
    task = _queue_sync(
        pair["source"], pair["target"], mode != "last", min_id, limit,
        WebEvent(), False, mode, pair["id"], _pair_config(pair)
    )
    return jsonify({"ok": True, "task": _task_view(task)})


@flask_app.route("/api/tasks/<task_id>", methods=["PATCH", "DELETE"])
def api_task_control(task_id):
    task = next((t for t in state.get("tasks", []) if t.get("id") == task_id), None)
    if not task:
        return jsonify({"ok": False, "error": "Task not found"})
    if request.method == "DELETE":
        state.setdefault("task_controls", {})[task_id] = {"cancelled": True, "paused": False}
        for queued in list(_task_queue):
            if queued["id"] == task_id:
                _task_queue.remove(queued)
        task["status"] = "cancelled"
    else:
        paused = bool((request.json or {}).get("paused"))
        state.setdefault("task_controls", {}).setdefault(task_id, {})["paused"] = paused
        task["status"] = "paused" if paused else ("running" if task_id == state.get("active_task_id") else "queued")
    save_state(state)
    return jsonify({"ok": True, "task": task})


@flask_app.route("/api/autoforward", methods=["POST"])
def api_autoforward():
    enabled = bool((request.json or {}).get("enabled"))
    if enabled and (not state.get("source") or not state.get("target")):
        return jsonify({"ok": False, "error": "Set source and target first"})
    state["auto_forward"] = enabled
    save_state(state)
    _log_live(f"🔁 Auto-forward {'enabled' if enabled else 'disabled'}")
    return jsonify({"ok": True, "enabled": enabled})


@flask_app.route("/api/pause", methods=["POST"])
def api_pause():
    state["paused"] = True
    save_state(state)
    return jsonify({"ok": True})


@flask_app.route("/api/resume", methods=["POST"])
def api_resume():
    state["paused"] = False
    save_state(state)
    return jsonify({"ok": True})


@flask_app.route("/api/stop", methods=["POST"])
def api_stop():
    state["running"] = False
    state["paused"]  = False
    save_state(state)
    return jsonify({"ok": True})


@flask_app.route("/api/reset", methods=["POST"])
def api_reset():
    state.clear()
    save_state(state)
    return jsonify({"ok": True})


@flask_app.route("/api/logs")
def api_logs():
    return jsonify({"logs": list(_live_log)})


@flask_app.route("/api/logs/search")
def api_logs_search():
    query = request.args.get("q", "").lower().strip()
    logs = list(_live_log)
    if query:
        logs = [line for line in logs if query in line.lower()]
    return jsonify({"logs": logs})


@flask_app.route("/api/tasks/<task_id>/report")
def api_task_report(task_id):
    task = next((t for t in state.get("tasks", []) if t.get("id") == task_id), None)
    if not task:
        return jsonify({"ok": False, "error": "Task not found"}), 404
    if request.args.get("format") == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "pair_id", "status", "current", "total", "created_at", "finished_at"])
        writer.writerow([task.get(k, "") for k in ("id", "pair_id", "status", "current", "total", "created_at", "finished_at")])
        return Response(output.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": f"attachment; filename=task-{task_id}.csv"})
    return jsonify({"ok": True, "task": task})


@flask_app.route("/health")
def health():
    return jsonify({"status": "ok", "running": state.get("running", False)})


def run_flask():
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# ════════════════════════════════════════════════════════
#  MAIN — Run both userbot + bot together
# ════════════════════════════════════════════════════════

async def main():
    global _loop
    _loop = asyncio.get_event_loop()

    # Start Flask web server in background thread
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    print("🌐 Web dashboard running on port 8080")

    # Start Telethon userbot
    await client.start(phone=PHONE)
    persist_session_string()
    me = await client.get_me()
    print(f"✅ Userbot logged in as: {me.first_name} (@{me.username})")
    print(f"🔐 Owner ID: {OWNER_ID}")

    # Crypto backend confirm karo
    try:
        import cryptg
        ver = getattr(cryptg, "__version__", "installed")
        print(f"⚡ Crypto: cryptg {ver} (AES-NI hardware — FAST)")
        logger.info(f"Crypto backend: cryptg {ver} (AES-NI)")
    except ImportError:
        print("⚠️  Crypto: pyaes (pure Python — slow, cryptg install karo)")
        logger.warning("Crypto backend: pyaes (slow)")

    print(f"🔧 Workers: {PARALLEL_WORKERS} | Chunk: {CHUNK_SIZE//1024}KB | Connection: TcpAbridged")

    # Build Telegram Bot
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", bot_start))
    app.add_handler(CommandHandler("help", bot_help))
    app.add_handler(CommandHandler("setsource", bot_setsource))
    app.add_handler(CommandHandler("settarget", bot_settarget))
    app.add_handler(CommandHandler("info", bot_info))
    app.add_handler(CommandHandler("status", bot_status))
    app.add_handler(CommandHandler("pause", bot_pause))
    app.add_handler(CommandHandler("resume", bot_resume))
    app.add_handler(CommandHandler("stop", bot_stop))
    app.add_handler(CommandHandler("reset", bot_reset))
    app.add_handler(CommandHandler("sync", bot_sync))
    app.add_handler(CommandHandler("syncfrom", bot_syncfrom))
    app.add_handler(CommandHandler("synclast", bot_synclast))
    app.add_handler(CommandHandler("tasks", bot_tasks))
    app.add_handler(CommandHandler("autoforward", bot_autoforward))

    print("🤖 Telegram Bot started! Commands available via bot.")
    print("⚡ Both userbot + bot running...")

    # Run both concurrently
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    await client.run_until_disconnected()

    await app.updater.stop()
    await app.stop()
    await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
