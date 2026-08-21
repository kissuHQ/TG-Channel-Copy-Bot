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
import threading
import tempfile
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
SESSION_STRING = os.getenv("SESSION_STRING", "")  # env var se lega


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


# ════════════════════════════════════════════════════════
#  CORE SYNC ENGINE
# ════════════════════════════════════════════════════════

async def start_sync_userbot(event, reverse=True, min_id=0, limit=None):
    if not state.get("source") or not state.get("target"):
        await event.edit("❌ Pehle `.setsource` aur `.settarget` karo!")
        return
    if state.get("running"):
        await event.edit("⚠️ Sync pehle se chal raha hai! `.stop` karo pehle.")
        return

    source = state["source"]
    target = state["target"]
    state["running"] = True
    state["paused"] = False
    state["stats"] = reset_stats()
    state["current_id"] = 0
    save_state(state)

    progress_msg = await event.edit(
        f"⏳ **Sync Starting...**\n\n"
        f"📥 Source: `{state.get('source_title', source)}`\n"
        f"📤 Target: `{state.get('target_title', target)}`\n"
        f"⚙️ Fetching messages..."
    )

    await _run_sync(progress_msg, source, target, reverse, min_id, limit, is_bot=False)


async def start_sync_bot(progress_msg, reverse=True, min_id=0, limit=None):
    if not state.get("source") or not state.get("target"):
        await progress_msg.edit_text("❌ Pehle /setsource aur /settarget karo!")
        return
    if state.get("running"):
        await progress_msg.edit_text("⚠️ Sync pehle se chal raha hai! /stop karo pehle.")
        return

    source = state["source"]
    target = state["target"]
    state["running"] = True
    state["paused"] = False
    state["stats"] = reset_stats()
    state["current_id"] = 0
    save_state(state)

    await progress_msg.edit_text(
        f"⏳ Sync Starting...\n\n"
        f"Source: {state.get('source_title', source)}\n"
        f"Target: {state.get('target_title', target)}\n"
        f"Fetching messages..."
    )

    await _run_sync(progress_msg, source, target, reverse, min_id, limit, is_bot=True)


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


async def _run_sync(progress_msg, source, target, reverse, min_id, limit, is_bot=False):
    async def edit_msg(text, parse_mode=None):
        try:
            if is_bot:
                kwargs = {"parse_mode": parse_mode} if parse_mode else {}
                await progress_msg.edit_text(text, **kwargs)
            else:
                await progress_msg.edit(text, parse_mode=parse_mode)
        except Exception:
            pass

    src_title  = state.get("source_title", str(source))
    tgt_title  = state.get("target_title", str(target))

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
            if not state.get("running"):
                logger.info("Sync stopped by user command")
                break

            while state.get("paused"):
                await asyncio.sleep(2)
                state.update(load_state())

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
                            on_progress=on_progress if msg_type != "text" else None
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
                        await asyncio.sleep(BATCH_DELAY)
                    else:
                        await asyncio.sleep(MSG_DELAY)

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


async def send_message(target, message, on_progress=None):
    if message.media and not isinstance(message.media, MessageMediaWebPage):

        async def dl_cb(current, total):
            if on_progress:
                await on_progress("download", current, total)

        tmp_path = await fast_download(message.media, progress_cb=dl_cb if on_progress else None)
        if not tmp_path or not Path(tmp_path).exists():
            raise Exception("Media download failed")

        caption = message.text or ""
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
                await client.send_file(
                    target, str(send_path),
                    caption=caption, parse_mode="md",
                    force_document=False,
                    part_size_kb=1024,
                    progress_callback=ul_cb,
                )
            elif is_video:
                # Video as streamable video (not document)
                await client.send_file(
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
                await client.send_file(
                    target, str(send_path),
                    caption=caption, parse_mode="md",
                    force_document=False,
                    attributes=attributes,  # title/duration preserve
                    part_size_kb=1024,
                    progress_callback=ul_cb,
                )
            else:
                # PDF, CSV, ZIP, etc — document as document
                await client.send_file(
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

        return True

    elif message.text:
        await client.send_message(
            target, message.text,
            parse_mode="md", link_preview=False
        )
        return True

    return False

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
    return {
        "running": running,
        "paused":  paused,
        "source":  state.get("source_title", ""),
        "target":  state.get("target_title", ""),
        "last_id": state.get("last_synced_id", 0),
        "current": cur,
        "total":   tot,
        "pct":     round(cur / tot * 100, 1) if tot else 0,
        "stats":   stats,
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


@flask_app.route("/api/sync", methods=["POST"])
def api_sync():
    if state.get("running"):
        return jsonify({"ok": False, "error": "Already running"})
    _run_bg(start_sync_userbot(WebEvent(), reverse=True, min_id=0))
    return jsonify({"ok": True})


@flask_app.route("/api/syncfrom", methods=["POST"])
def api_syncfrom():
    if state.get("running"):
        return jsonify({"ok": False, "error": "Already running"})
    mid = int((request.json or {}).get("min_id", 0))
    _run_bg(start_sync_userbot(WebEvent(), reverse=True, min_id=mid))
    return jsonify({"ok": True})


@flask_app.route("/api/synclast", methods=["POST"])
def api_synclast():
    if state.get("running"):
        return jsonify({"ok": False, "error": "Already running"})
    n = int((request.json or {}).get("n", 10))
    _run_bg(start_sync_userbot(WebEvent(), reverse=False, limit=n))
    return jsonify({"ok": True})


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
