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
import random
import shutil
import mimetypes
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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
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
MIN_RATE_DELAY = 3
MAX_BATCH_TASKS = 5
MAX_TASK_MESSAGES = 5000
TASK_PRIORITIES = {"low": 10, "normal": 20, "high": 30}
RATE_PROFILES = {"very_safe": 5, "balanced": 3, "slow": 10}
DEFAULT_DAILY_MESSAGES = MAX_TASK_MESSAGES
DEFAULT_DAILY_MEDIA_MB = 2048
# Keep a safety margin on the small Replit disk.  This is a hard temporary
# storage ceiling, not a promise that the host has this much free space.
TEMP_STORAGE_LIMIT_BYTES = 1_800 * 1024 * 1024
TEMP_DIR = Path("/tmp/archive_bot")
THUMBNAIL_DIR = Path("thumbnails")

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
state.setdefault("message_map", {})
state.setdefault("media_fingerprints", {})
state.setdefault("pair_health", {})
state.setdefault("oversized_messages", [])
state.setdefault("templates", {})
state.setdefault("notification_settings", {
    "task_complete": True, "task_failed": True, "flood_wait": True,
    "disconnect": True, "daily_summary": False,
})
for _pair in state.get("pairs", []):
    # Missing setting means automatic new-post forwarding is enabled.
    # An explicit False remains disabled.
    _pair.setdefault("auto_forward", True)

# Sync requests are queued instead of being rejected while another sync runs.
_task_queue = deque()
_task_worker_running = False
_auto_forward_lock = asyncio.Lock()


def _task_view(task):
    return {
        "id": task["id"],
        "mode": task.get("mode", "full"),
        "priority": task.get("priority", "normal"),
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
        "pause_reason": task.get("pause_reason"),
        "paused_at": task.get("paused_at"),
        "resume_min_id": task.get("resume_min_id", task.get("min_id", 0)),
        "resume_max_id": task.get("resume_max_id", task.get("max_id", 0)),
        "task_settings": task.get("task_settings", task.get("config", {})),
    }


def _pair_by_id(pair_id):
    return next((p for p in state.get("pairs", []) if p.get("id") == pair_id), None)


def _pair_config(pair):
    pair = pair or {}
    profile = str(pair.get("rate_profile", "balanced")).lower()
    profile_delay = RATE_PROFILES.get(profile, RATE_PROFILES["balanced"])
    return {
        "allowed_types": pair.get("allowed_types") or ["text", "photo", "video", "doc", "other"],
        "include_keywords": [str(x).lower() for x in pair.get("include_keywords", []) if str(x).strip()],
        "exclude_keywords": [str(x).lower() for x in pair.get("exclude_keywords", []) if str(x).strip()],
        "caption_prefix": pair.get("caption_prefix", ""),
        "caption_suffix": pair.get("caption_suffix", ""),
        "remove_links": bool(pair.get("remove_links")),
        "remove_source_name": bool(pair.get("remove_source_name")),
        "rate_profile": profile if profile in RATE_PROFILES else "balanced",
        "rate_delay": max(MIN_RATE_DELAY, min(float(pair.get("rate_delay", profile_delay)), 300)),
        "max_messages": MAX_TASK_MESSAGES,
        "daily_message_limit": max(
            MAX_TASK_MESSAGES,
            min(int(pair.get("daily_message_limit", DEFAULT_DAILY_MESSAGES)), MAX_TASK_MESSAGES),
        ),
        "daily_media_mb": max(1, min(int(pair.get("daily_media_mb", DEFAULT_DAILY_MEDIA_MB)), 102400)),
        "auto_forward": bool(pair.get("auto_forward", False)),
        "dedupe_mode": pair.get("dedupe_mode", "strong"),
        "max_posts_per_hour": max(0, min(int(pair.get("max_posts_per_hour", 0) or 0), 10000)),
        "schedule_start": str(pair.get("schedule_start", "")),
        "schedule_end": str(pair.get("schedule_end", "")),
        "quiet_start": str(pair.get("quiet_start", "")),
        "quiet_end": str(pair.get("quiet_end", "")),
        "protected_behavior": pair.get("protected_behavior", "download"),
        "caption_enabled": bool(pair.get("caption_enabled", False)),
        "caption_template": str(pair.get("caption_template", "")),
        "caption_types": pair.get("caption_types") or ["text", "photo", "video", "doc", "other"],
        "caption_parse_mode": str(pair.get("caption_parse_mode", "md")),
        "thumbnail_enabled": bool(pair.get("thumbnail_enabled", False)),
        "thumbnail_path": str(pair.get("thumbnail_path", "")),
    }


class StorageLimitError(RuntimeError):
    """Raised before a download can exceed the temporary disk budget."""
    def __init__(self, message, required=0, available=0):
        super().__init__(message)
        self.required = required
        self.available = available


def _temp_usage_bytes():
    try:
        return sum(path.stat().st_size for path in TEMP_DIR.glob("*") if path.is_file())
    except OSError:
        return 0


def _storage_snapshot():
    usage = _temp_usage_bytes()
    try:
        free = shutil.disk_usage("/tmp").free
    except OSError:
        free = 0
    return {
        "limit_bytes": TEMP_STORAGE_LIMIT_BYTES,
        "used_bytes": usage,
        "available_bytes": max(0, min(TEMP_STORAGE_LIMIT_BYTES - usage, free)),
        "used_mb": round(usage / 1048576, 2),
        "limit_mb": round(TEMP_STORAGE_LIMIT_BYTES / 1048576, 2),
        "available_mb": round(max(0, min(TEMP_STORAGE_LIMIT_BYTES - usage, free)) / 1048576, 2),
    }


def _message_link(source_entity, message):
    username = getattr(source_entity, "username", None)
    if username:
        return f"https://t.me/{username}/{getattr(message, 'id', '')}"
    channel_id = getattr(source_entity, "id", None)
    if channel_id:
        return f"https://t.me/c/{channel_id}/{getattr(message, 'id', '')}"
    return None


def _media_fingerprint(message):
    media = getattr(message, "media", None)
    doc = getattr(media, "document", None)
    if doc:
        name = next((a.file_name for a in (doc.attributes or [])
                     if isinstance(a, DocumentAttributeFilename)), "")
        return f"document:{getattr(doc, 'id', '')}:{getattr(doc, 'size', 0)}:{name}:{getattr(doc, 'mime_type', '')}"
    photo = getattr(media, "photo", None)
    if photo:
        return f"photo:{getattr(photo, 'id', '')}:{getattr(photo, 'access_hash', '')}"
    return ""


def _strong_dedupe_key(pair_id, message):
    """Stable identity across reruns, even if caption/message IDs differ."""
    fingerprint = _media_fingerprint(message)
    if fingerprint:
        return f"{pair_id}:media:{hashlib.sha256(fingerprint.encode()).hexdigest()}"
    text = re.sub(r"\s+", " ", (message.text or "").strip().lower())
    return f"{pair_id}:text:{hashlib.sha256(text.encode()).hexdigest()}"


def _time_in_window(now, start, end):
    if not start or not end:
        return False
    try:
        current = now.hour * 60 + now.minute
        a = sum(int(x) * (60 if i == 0 else 1) for i, x in enumerate(start.split(":")))
        b = sum(int(x) * (60 if i == 0 else 1) for i, x in enumerate(end.split(":")))
        return current >= a and current < b if a <= b else current >= a or current < b
    except (ValueError, TypeError):
        return False


def _within_schedule(config):
    now = datetime.now()
    if _time_in_window(now, config.get("quiet_start"), config.get("quiet_end")):
        return False
    start, end = config.get("schedule_start"), config.get("schedule_end")
    return not start or not end or _time_in_window(now, start, end)


def _hourly_budget(pair_id, config, commit=False):
    bucket = state.setdefault("hourly_usage", {}).setdefault(str(pair_id), {
        "hour": datetime.now().strftime("%Y-%m-%d-%H"), "count": 0
    })
    current_hour = datetime.now().strftime("%Y-%m-%d-%H")
    if bucket.get("hour") != current_hour:
        bucket.update({"hour": current_hour, "count": 0})
    allowed = not config.get("max_posts_per_hour") or bucket["count"] < config["max_posts_per_hour"]
    if allowed and commit:
        bucket["count"] += 1
    return allowed, bucket


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


def _media_size_mb(message):
    media = getattr(message, "media", None)
    document = getattr(media, "document", None)
    return (getattr(document, "size", 0) or 0) / (1024 * 1024)


def _daily_budget(pair_id, config, message, commit=False):
    today = datetime.now().date().isoformat()
    usage = state.setdefault("daily_usage", {})
    bucket = usage.setdefault(str(pair_id or "default"), {"date": today, "messages": 0, "media_mb": 0.0})
    if bucket.get("date") != today:
        bucket.update({"date": today, "messages": 0, "media_mb": 0.0})
    size_mb = _media_size_mb(message)
    allowed = (
        bucket["messages"] < config.get("daily_message_limit", DEFAULT_DAILY_MESSAGES)
        and bucket["media_mb"] + size_mb <= config.get("daily_media_mb", DEFAULT_DAILY_MEDIA_MB)
    )
    if allowed and commit:
        bucket["messages"] += 1
        bucket["media_mb"] = round(bucket["media_mb"] + size_mb, 2)
    return allowed, bucket


async def send_album(target, messages, on_progress=None, config=None,
                     source_title="", source_entity=None):
    """Copy a Telegram album as one grouped post when possible."""
    config = config or _pair_config(None)
    source_entity = source_entity or getattr(messages[0], "chat", None)
    needs_rewrite = any([
        config["caption_prefix"], config["caption_suffix"],
        config["remove_links"], config["remove_source_name"],
        any(config.get("caption_enabled") and get_msg_type(message) in config.get("caption_types", [])
            for message in messages),
        any(config.get("thumbnail_enabled") and get_msg_type(message) == "video"
            for message in messages),
    ])
    restricted = bool(
        getattr(source_entity, "noforwards", False)
        or any(getattr(message, "noforwards", False) for message in messages)
    )
    if not restricted and not needs_rewrite:
        return await client.send_file(
            target, [message.media for message in messages],
            caption=[message.text or "" for message in messages],
            parse_mode=_parse_mode(config)
        )

    paths = []
    try:
        for message in messages:
            path = await fast_download(message.media)
            if path and Path(path).exists():
                paths.append(path)
        if len(paths) != len(messages):
            raise RuntimeError("Album media download failed")
        captions = [_edited_caption(message, config, source_title) for message in messages]
        return await client.send_file(
            target, paths, caption=captions, parse_mode=_parse_mode(config),
            thumb=_thumbnail_path(config)
        )
    finally:
        for path in paths:
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass


def _edited_caption(message, config, source_title=""):
    msg_type = get_msg_type(message)
    text = message.text or ""
    if config["remove_links"]:
        text = re.sub(r"(https?://|www\.)\S+", "", text, flags=re.IGNORECASE)
    if config["remove_source_name"] and source_title:
        text = re.sub(re.escape(source_title), "", text, flags=re.IGNORECASE)
    text = re.sub(r"[ \t]{2,}", " ", text).strip()
    if config.get("caption_enabled") and msg_type in config.get("caption_types", []):
        document = getattr(getattr(message, "media", None), "document", None)
        filename = ""
        if document:
            filename = next((a.file_name for a in (document.attributes or [])
                             if isinstance(a, DocumentAttributeFilename)), "")
        size = getattr(document, "size", 0) or 0
        values = {
            "caption": text, "filename": filename, "filesize": _human_size(size),
            "filesize_mb": f"{size / 1048576:.2f}", "message_id": str(getattr(message, "id", "")),
            "source": source_title, "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M"), "mime": getattr(document, "mime_type", "") if document else "",
            "type": msg_type,
        }
        template = config.get("caption_template", "")
        if template:
            try:
                text = template.format_map(_SafeFormat(values))
            except (ValueError, KeyError):
                logger.warning("Invalid caption template; using original caption")
    if text:
        text = f"{config['caption_prefix']}{text}{config['caption_suffix']}"
    else:
        text = f"{config['caption_prefix']}{config['caption_suffix']}".strip()
    return text


def _human_size(size):
    size = float(size or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.1f} {unit}"
        size /= 1024


class _SafeFormat(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def _thumbnail_path(config):
    path = config.get("thumbnail_path", "")
    return path if config.get("thumbnail_enabled") and path and Path(path).is_file() else None


def _parse_mode(config):
    mode = str(config.get("caption_parse_mode", "md")).lower()
    return {"md": "md", "markdown": "md", "html": "html"}.get(mode)


def _dedupe_key(pair_id, message):
    raw = f"{pair_id}:{message.id}:{message.text or ''}:{getattr(message, 'grouped_id', '')}"
    media = getattr(message, "media", None)
    doc = getattr(media, "document", None)
    if doc:
        raw += f":{getattr(doc, 'id', '')}:{getattr(doc, 'size', '')}"
    return hashlib.sha256(raw.encode("utf-8", "ignore")).hexdigest()


def _is_duplicate(pair_id, message):
    """Check source ID mapping plus stable media/text identities."""
    dedupe = state.setdefault("dedupe", {})
    keys = {_dedupe_key(pair_id, message), _strong_dedupe_key(pair_id, message)}
    fingerprint = _media_fingerprint(message)
    if fingerprint:
        keys.add(fingerprint)
    return any(key in dedupe for key in keys)


def _record_dedupe(pair_id, message):
    stamp = datetime.now().isoformat(timespec="seconds")
    dedupe = state.setdefault("dedupe", {})
    dedupe[_dedupe_key(pair_id, message)] = stamp
    dedupe[_strong_dedupe_key(pair_id, message)] = stamp
    fingerprint = _media_fingerprint(message)
    if fingerprint:
        dedupe[fingerprint] = stamp


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
            ordered = sorted(
                enumerate(_task_queue),
                key=lambda item: -TASK_PRIORITIES.get(item[1].get("priority", "normal"), 20)
            )
            selected_index = ordered[0][0]
            task = _task_queue[selected_index]
            del _task_queue[selected_index]
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
                    task.get("pair_id"), task.get("task_settings", task.get("config")), task["id"],
                    task.get("source_title"), task.get("target_title"),
                    task.get("force_sync", False),
                    max_id=task.get("resume_max_id", task.get("max_id", 0))
                )
                pause_requested = state.pop("_task_pause_requested", False)
                task["status"] = (
                    "paused" if pause_requested
                    else ("partial" if state.pop("_task_partial", False) else "complete")
                )
                if pause_requested:
                    task["pause_reason"] = state.pop("_task_pause_reason", "A limit temporarily stopped this task")
                    task["resume_min_id"] = state.pop("_task_resume_min_id", task.get("min_id", 0))
                    task["resume_max_id"] = state.pop("_task_resume_max_id", task.get("max_id", 0))
            except Exception as exc:
                task["status"] = "failed"
                logger.exception("Queued task failed: %s", exc)
            finally:
                if task["status"] == "paused":
                    task["paused_at"] = datetime.now().isoformat(timespec="seconds")
                    task.pop("finished_at", None)
                else:
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
                notification_key = "task_failed" if task["status"] == "failed" else "task_complete"
                if state.get("notification_settings", {}).get(notification_key, True):
                    status_icon = {"complete": "✅", "partial": "⚠️", "failed": "❌"}.get(
                        task["status"], "ℹ️"
                    )
                    message = (
                        f"⏸️ Task {task['id']} paused\n\n"
                        f"Reason: {task.get('pause_reason', 'A temporary limit was reached')}\n"
                        f"Progress: {task.get('current', 0)} processed\n\n"
                        "Continue button dabakar isi task ko saved progress se aage chala sakte ho."
                        if task["status"] == "paused" else
                        f"{status_icon} Task {task['id']} "
                        f"{task['status']} — {task.get('current', 0)} processed, "
                        f"{task.get('stats', {}).get('failed', 0)} failed"
                    )
                    markup = (
                        InlineKeyboardMarkup([[
                            InlineKeyboardButton("▶️ Continue", callback_data=f"continue:{task['id']}")
                        ]]) if task["status"] == "paused" else None
                    )
                    await _notify_owner(message, reply_markup=markup)
    finally:
        _task_worker_running = False
        state["running"] = bool(_task_queue)
        state.pop("active_task_id", None)
        save_state(state)


def _resume_paused_task(task):
    """Put a paused task back in the queue without creating a new task ID."""
    if task.get("status") != "paused":
        return False, "Task is not paused"
    if any(item.get("id") == task["id"] for item in _task_queue):
        return False, "Task is already queued"
    pair = _pair_by_id(task.get("pair_id"))
    source = (pair or {}).get("source") or task.get("source")
    target = (pair or {}).get("target") or task.get("target")
    internal = {
        **task,
        "source": source,
        "target": target,
        "source_title": (pair or {}).get("source_title", task.get("source", source)),
        "target_title": (pair or {}).get("target_title", task.get("target", target)),
        "reverse": task.get("mode") != "last",
        "min_id": task.get("resume_min_id", task.get("min_id", 0)),
        "max_id": task.get("resume_max_id", task.get("max_id", 0)),
        "config": task.get("task_settings") or _pair_config(pair),
        "task_settings": task.get("task_settings") or _pair_config(pair),
        "progress_msg": WebEvent(),
        "is_bot": False,
        "status": "queued",
    }
    task.update({
        "status": "queued",
        "min_id": internal["min_id"],
        "pause_reason": None,
    })
    _task_queue.append(internal)
    save_state(state)
    if _loop is not None and _loop.is_running():
        asyncio.run_coroutine_threadsafe(_task_worker(), _loop)
    return True, "Task queued to continue"


def _queue_sync(source, target, reverse=True, min_id=0, limit=None,
                progress_msg=None, is_bot=False, mode="full", pair_id=None,
                config=None, priority="normal", force_sync=False):
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
        "priority": priority if priority in TASK_PRIORITIES else "normal",
        "pair_id": pair_id or "default",
        "config": config or _pair_config(_pair_by_id(pair_id)),
        "progress_msg": progress_msg or WebEvent(),
        "is_bot": is_bot,
        "force_sync": force_sync,
        "status": "queued",
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    _task_queue.append(task)
    ordered = sorted(
        _task_queue,
        key=lambda item: -TASK_PRIORITIES.get(item.get("priority", "normal"), 20)
    )
    _task_queue.clear()
    _task_queue.extend(ordered)
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
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    total_size = 0
    if isinstance(media, MessageMediaDocument) and media.document:
        total_size = media.document.size or 0
    snapshot = _storage_snapshot()
    if total_size and total_size > snapshot["available_bytes"]:
        raise StorageLimitError(
            f"Temporary storage limit reached: need {total_size / 1048576:.1f} MB, "
            f"available {snapshot['available_mb']:.1f} MB",
            total_size, snapshot["available_bytes"]
        )
    # Temp file banao inside managed directory so accounting/cleanup works.
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

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=suffix, dir=str(TEMP_DIR))
    os.close(tmp_fd)

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
        "`.refresh` — Source refresh karke naye posts copy karo\n"
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


def _forwarded_chat_id(message):
    """Support both legacy and modern Bot API forwarded-message fields."""
    forwarded = getattr(message, "forward_from_chat", None)
    if forwarded:
        return getattr(forwarded, "id", None)
    origin = getattr(message, "forward_origin", None)
    chat = getattr(origin, "chat", None)
    return getattr(chat, "id", None)


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


async def refresh_sources(progress_msg, is_bot=True):
    """Queue only messages newer than the last observed source message."""
    pairs = list(state.get("pairs", []))
    if not pairs and state.get("source") and state.get("target"):
        pairs = [{
            "id": "default",
            "source": state["source"],
            "target": state["target"],
            "source_title": state.get("source_title", str(state["source"])),
            "target_title": state.get("target_title", str(state["target"])),
        }]
    if not pairs:
        text = "❌ Pehle source aur target set karo."
        await (progress_msg.edit_text(text) if is_bot else progress_msg.edit(text))
        return

    queued = []
    skipped = []
    source_last_ids = state.setdefault("source_last_ids", {})
    for pair in pairs:
        try:
            source_entity = await client.get_entity(pair["source"])
            latest = await client.get_messages(source_entity, limit=1)
            latest_message = latest[0] if latest else None
            latest_id = getattr(latest_message, "id", 0)
            pair_id = str(pair.get("id", "default"))
            last_id = int(source_last_ids.get(pair_id, 0) or 0)
            if not last_id and pair["source"] == state.get("source"):
                last_id = int(state.get("last_synced_id", 0) or 0)
            if latest_id <= last_id:
                skipped.append(pair.get("name", pair_id))
                continue
            queued.append(_queue_sync(
                pair["source"], pair["target"], True, last_id, None,
                progress_msg, is_bot, "refresh", pair_id,
                _pair_config(pair)
            ))
        except Exception as exc:
            logger.warning("Refresh failed for %s: %s", pair.get("source"), exc)
            skipped.append(f"{pair.get('name', pair.get('id', 'pair'))}: {type(exc).__name__}")

    if queued:
        summary = f"🔄 {len(queued)} refresh task(s) queued"
        if skipped:
            summary += f"\n⏭️ No new posts/error: {len(skipped)}"
    else:
        summary = "✅ Source refreshed — koi naya post nahi mila."
    await (progress_msg.edit_text(summary) if is_bot else progress_msg.edit(summary))


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.refresh$"))
async def cmd_refresh(event):
    if not is_owner(event.sender_id):
        return
    await refresh_sources(event, is_bot=False)


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
        "/force_sync — Full sync, daily limits ke bina\n"
        "/syncfrom `<id>` — Message ID se sync karo\n"
        "/synclast `<n>` — Last N messages sync karo\n"
        "/tasks — Queue ke tasks dekho\n"
        "/autoforward on|off — New posts automatically copy karo\n"
        "/caption <pair_id> on|off [template] — Caption rules set karo\n"
        "/setthumbnail <pair_id> — Photo/image ko reply karke thumbnail set karo\n"
        "/pause — Sync pause karo\n"
        "/resume — Sync resume karo\n"
        "/stop — Sync stop karo\n"
        "/status — Live status dekho\n"
        "/refresh — Source refresh karke naye posts copy karo\n"
        "/reset — Config reset karo\n\n"
        "⚠️ Sirf owner use kar sakta hai",
        parse_mode="Markdown"
    )


async def bot_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /caption <pair_id> on|off [template]\n"
            "Placeholders: {caption} {filename} {filesize} {filesize_mb} "
            "{message_id} {source} {date} {time} {mime} {type}"
        )
        return
    pair = _pair_by_id(context.args[0])
    if not pair:
        await update.message.reply_text("❌ Pair ID nahi mila. /status ya dashboard se ID dekho.")
        return
    enabled = context.args[1].lower() in {"on", "enable", "enabled"}
    template_args = context.args[2:]
    if template_args and template_args[0].lower().startswith("types="):
        pair["caption_types"] = [
            value.strip() for value in template_args[0][6:].split(",")
            if value.strip() in {"text", "photo", "video", "doc", "other"}
        ]
        template_args = template_args[1:]
    pair["caption_enabled"] = enabled
    if template_args:
        pair["caption_template"] = " ".join(template_args)
    save_state(state)
    await update.message.reply_text(
        f"✅ Caption {'enabled' if enabled else 'disabled'} for {pair.get('name', pair['id'])}"
    )


async def bot_setthumbnail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    pair_id = context.args[0] if context.args else None
    if not pair_id and len(state.get("pairs", [])) == 1:
        pair_id = state["pairs"][0].get("id")
    if not pair_id:
        await update.message.reply_text(
            "Usage: kisi photo/image ko reply karke /setthumbnail <pair_id> bhejo\n"
            "Agar sirf ek pair hai to pair ID optional hai."
        )
        return
    pair = _pair_by_id(pair_id)
    if pair and len(context.args) > 1 and context.args[1].lower() in {"off", "disable"}:
        pair["thumbnail_enabled"] = False
        save_state(state)
        await update.message.reply_text(f"✅ Thumbnail disabled for {pair.get('name', pair['id'])}")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Thumbnail set karne ke liye photo ko reply karo.")
        return
    replied = update.message.reply_to_message
    photo = getattr(replied, "photo", None)
    document = getattr(replied, "document", None)
    is_image_document = (
        document
        and str(getattr(document, "mime_type", "")).lower().startswith("image/")
    )
    if not pair or (not photo and not is_image_document):
        await update.message.reply_text(
            "❌ Valid pair ID ke saath replied photo ya image file required hai."
        )
        return
    THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
    path = THUMBNAIL_DIR / f"{pair['id']}.jpg"
    file_id = photo[-1].file_id if photo else document.file_id
    tg_file = await context.bot.get_file(file_id)
    await tg_file.download_to_drive(custom_path=str(path))
    pair["thumbnail_path"] = str(path)
    pair["thumbnail_enabled"] = True
    save_state(state)
    await update.message.reply_text(f"✅ Thumbnail enabled for {pair.get('name', pair['id'])}")

async def bot_setsource(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    msg = update.message

    # Option 1: Forwarded message se channel detect karo (no args needed)
    if not context.args:
        fwd_chat = _forwarded_chat_id(msg)
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
        fwd_chat = _forwarded_chat_id(msg)
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

async def bot_force_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    msg = await update.message.reply_text(
        "🚀 Force sync shuru ho rahi hai (daily limits bypass)..."
    )
    asyncio.create_task(
        start_sync_bot(msg, reverse=True, min_id=0, force_sync=True)
    )


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


async def bot_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_is_owner(update):
        return
    msg = await update.message.reply_text("🔄 Source refresh ho raha hai...")
    asyncio.create_task(refresh_sources(msg, is_bot=True))


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


async def start_sync_bot(progress_msg, reverse=True, min_id=0, limit=None,
                         force_sync=False):
    if not state.get("source") or not state.get("target"):
        await progress_msg.edit_text("❌ Pehle /setsource aur /settarget karo!")
        return
    task = _queue_sync(
        state["source"], state["target"], reverse, min_id, limit,
        progress_msg, True, "force" if force_sync else ("full" if not limit and not min_id else "range"),
        force_sync=force_sync
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
                    source_title=None, target_title=None, force_sync=False,
                    max_id=0):
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
        pair_limit = config.get("max_messages", MAX_TASK_MESSAGES)
        effective_limit = min(limit, pair_limit) if limit else pair_limit
        total_count = min(total.total, effective_limit)
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
        handled_albums = set()

        async for message in client.iter_messages(
            source_entity,
            reverse=reverse,
            min_id=min_id,
            limit=effective_limit,
            max_id=max_id or None
        ):
            state.setdefault("source_last_ids", {})[str(pair_id)] = message.id
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

            grouped_id = getattr(message, "grouped_id", None)
            if grouped_id and grouped_id not in handled_albums:
                nearby = await client.get_messages(
                    source_entity, limit=20, offset_id=message.id + 10
                )
                album = sorted(
                    [item for item in nearby if getattr(item, "grouped_id", None) == grouped_id],
                    key=lambda item: item.id
                )
                if len(album) > 1:
                    first_id = album[0].id if reverse else album[-1].id
                    if message.id != first_id:
                        continue
                    handled_albums.add(grouped_id)
                    album = [item for item in album if _message_allowed(item, config)]
                    album = [
                        item for item in album
                        if not _is_duplicate(pair_id, item)
                    ]
                    if album and (
                        force_sync
                        or all(_daily_budget(pair_id, config, item)[0] for item in album)
                    ):
                        sent_album = await send_album(
                            target_entity, album, config=config,
                            source_title=src_title, source_entity=source_entity
                        )
                        sent_album = sent_album if isinstance(sent_album, list) else [sent_album]
                        for index, item in enumerate(album):
                            sent_item = sent_album[min(index, len(sent_album) - 1)] if sent_album else None
                            _remember_mapping(pair_id, item.id, sent_item)
                            _record_dedupe(pair_id, item)
                            stats[get_msg_type(item)] = stats.get(get_msg_type(item), 0) + 1
                            if not force_sync:
                                _daily_budget(pair_id, config, item, commit=True)
                        count += len(album)
                        state["current_id"] = count
                        state["stats"] = stats
                        save_state(state)
                        _log_live(f"🖼️ Album copied as grouped media ({len(album)} items)")
                        await asyncio.sleep(max(MIN_RATE_DELAY, config["rate_delay"] + random.uniform(-0.5, 0.5)))
                        continue

            if not _message_allowed(message, config):
                stats = state.get("stats", stats)
                stats["skipped"] = stats.get("skipped", 0) + 1
                _log_live(f"⏭️ Filter skipped ID={message.id}")
                continue
            dedupe = state.setdefault("dedupe", {})
            dkey = _dedupe_key(pair_id, message)
            if _is_duplicate(pair_id, message):
                stats["duplicates"] = stats.get("duplicates", 0) + 1
                _log_live(f"⏭️ Duplicate skipped ID={message.id}")
                continue
            budget_ok, budget_bucket = (
                (True, None)
                if force_sync
                else _daily_budget(pair_id, config, message)
            )
            if not budget_ok:
                state["_task_pause_requested"] = True
                state["_task_pause_reason"] = "Daily message/media limit reached"
                if reverse:
                    state["_task_resume_min_id"] = max(0, message.id - 1)
                else:
                    state["_task_resume_max_id"] = message.id + 1
                save_state(state)
                _log_live(
                    f"⏸️ Daily limit reached for pair {pair_id}: "
                    f"{budget_bucket['messages']} messages / {budget_bucket['media_mb']:.1f} MB"
                )
                break

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
                            config=config, source_title=src_title,
                            source_entity=source_entity
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
                    _record_dedupe(pair_id, message)
                    # Keep the latest IDs only; this prevents unbounded state growth.
                    if len(dedupe) > 10000:
                        for old_key in list(dedupe)[:2000]:
                            dedupe.pop(old_key, None)
                    count += 1
                    stats[msg_type] = stats.get(msg_type, 0) + 1
                    _daily_budget(pair_id, config, message, commit=True)
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
                        await asyncio.sleep(max(
                            MIN_RATE_DELAY,
                            config["rate_delay"] + random.uniform(-0.5, 0.5)
                        ))

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
                state["_task_pause_requested"] = True
                state["_task_pause_reason"] = f"Telegram FloodWait limit ({wait}s suggested wait)"
                if reverse:
                    state["_task_resume_min_id"] = max(0, message.id - 1)
                else:
                    state["_task_resume_max_id"] = message.id + 1
                state["running"] = False
                save_state(state)
                break

            except SlowModeWaitError as e:
                wait = e.seconds + 5
                logger.warning(f"SlowMode {wait}s after msg_id={message.id}")
                await edit_msg(
                    f"🐢 *SlowMode Active!*\n\nTarget channel ka slow mode on hai\n"
                    f"⏱ Wait: *{wait}s*",
                    parse_mode="Markdown"
                )
                state["_task_pause_requested"] = True
                state["_task_pause_reason"] = f"Target channel slow mode ({wait}s suggested wait)"
                if reverse:
                    state["_task_resume_min_id"] = max(0, message.id - 1)
                else:
                    state["_task_resume_max_id"] = message.id + 1
                state["running"] = False
                save_state(state)
                break

            except ChatWriteForbiddenError:
                logger.error("ChatWriteForbiddenError — no write permission on target")
                await edit_msg("❌ Target channel mein write permission nahi hai!")
                state["running"] = False
                save_state(state)
                return

            except StorageLimitError as e:
                failed += 1
                stats["failed"] = failed
                link = _message_link(source_entity, message)
                record = {
                    "task_id": task_id, "pair_id": pair_id, "message_id": message.id,
                    "reason": str(e), "link": link, "created_at": datetime.now().isoformat(timespec="seconds")
                }
                state.setdefault("oversized_messages", []).append(record)
                state["oversized_messages"] = state["oversized_messages"][-500:]
                state["stats"] = stats
                save_state(state)
                _log_live(f"🛑 Storage blocked ID={message.id}: {e}")
                alert = f"🛑 Storage limit: task {task_id or 'sync'}, message {message.id}\n{e}"
                alert += f"\nLink: {link}" if link else "\nLink unavailable (private channel permission)."
                await _notify_owner(alert)
                await asyncio.sleep(1)
                continue

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

        was_partial = bool(state.get("_task_partial"))
        was_paused = bool(state.get("_task_pause_requested"))
        completion_title = (
            "⏸️ *Task Paused — Continue Later*"
            if was_paused else
            ("⚠️ *Sync Stopped at Limit!*" if was_partial else "✅ *Sync Complete!*")
        )
        stop_note = (
            "\nTemporary limit ki wajah se task pause hua hai. Dashboard ya Telegram ke Continue button se isi task ko aage chalao.\n"
            if was_paused else
            ("\nDaily message/media limit reached. Limit badhakar ya kal dobara refresh/sync karein.\n"
             if completion_title.startswith("⚠️") else "")
        )
        await edit_msg(
            f"{completion_title}\n\n"
            f"📥 `{src_title}`\n"
            f"📤 `{tgt_title}`\n\n"
            f"📝 Text:  {stats['text']}   "
            f"📷 Photo: {stats['photo']}\n"
            f"🎬 Video: {stats['video']}   "
            f"📄 Doc:   {stats['doc']}\n"
            f"📎 Other: {stats['other']}   "
            f"❌ Failed: {stats['failed']}\n"
            f"📊 Total:  {count}\n\n"
            f"{stop_note}"
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


async def send_message(target, message, on_progress=None, config=None, source_title="",
                       source_entity=None):
    # Telegram can often reuse the source media directly. This creates a new
    # message without a forward header and avoids download/upload round trips.
    # Restricted or otherwise non-copyable messages fall back to the existing
    # disk-based path below.
    config = config or _pair_config(None)
    msg_type = get_msg_type(message)
    needs_rewrite = any([
        config["caption_prefix"], config["caption_suffix"],
        config["remove_links"], config["remove_source_name"],
        config.get("caption_enabled") and msg_type in config.get("caption_types", []),
        config.get("thumbnail_enabled") and msg_type == "video",
    ])
    # Telegram marks protected channels with noforwards. Do not probe a
    # protected message with a copy request: download and re-upload instead.
    # This also makes the dashboard behavior deterministic instead of relying
    # on a failed API call for every restricted post.
    source_entity = source_entity or getattr(message, "chat", None)
    source_restricted = bool(
        getattr(source_entity, "noforwards", False)
        or getattr(message, "noforwards", False)
    )
    if source_restricted and config.get("protected_behavior") == "skip":
        _log_live(f"⏭️ Protected-content skipped ID={message.id}")
        return False
    try:
        if source_restricted:
            raise ValueError("source channel has forwarding protection")
        if needs_rewrite:
            raise ValueError("caption rewrite requires upload/copy path")
        sent = await client.send_message(target, message, parse_mode=_parse_mode(config), link_preview=False)
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
            named_path = Path(tmp_path).with_name(f"{Path(tmp_path).stem}_{Path(original_filename).name}")
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
                    caption=caption, parse_mode=_parse_mode(config),
                    force_document=False,
                    part_size_kb=1024,
                    progress_callback=ul_cb,
                )
            elif is_video:
                # Video as streamable video (not document)
                sent = await client.send_file(
                    target, str(send_path),
                    caption=caption, parse_mode=_parse_mode(config),
                    force_document=False,   # streamable video
                    supports_streaming=True,
                    thumb=_thumbnail_path(config),
                    attributes=attributes,  # original duration/dimensions preserve
                    part_size_kb=1024,
                    progress_callback=ul_cb,
                )
            elif is_audio:
                # Audio as audio player
                sent = await client.send_file(
                    target, str(send_path),
                    caption=caption, parse_mode=_parse_mode(config),
                    force_document=False,
                    attributes=attributes,  # title/duration preserve
                    part_size_kb=1024,
                    progress_callback=ul_cb,
                )
            else:
                # PDF, CSV, ZIP, etc — document as document
                sent = await client.send_file(
                    target, str(send_path),
                    caption=caption, parse_mode=_parse_mode(config),
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
    """Mirror every new post for enabled pairs, one route at a time."""
    try:
        message = event.message
        if not message:
            return
        routes = []
        for pair in state.get("pairs", []):
            if not pair.get("auto_forward"):
                continue
            source_entity = await client.get_entity(pair["source"])
            if event.chat_id == _telegram_chat_id(source_entity):
                routes.append((pair, source_entity))

        # Preserve the older global toggle for the default source/target pair.
        if state.get("auto_forward") and state.get("source") and state.get("target"):
            source_entity = await client.get_entity(state["source"])
            if event.chat_id == _telegram_chat_id(source_entity):
                legacy = {
                    "source": state["source"], "target": state["target"],
                    "source_title": getattr(source_entity, "title", str(state["source"])),
                    "target_title": state.get("target_title", str(state["target"])),
                    "rate_delay": MSG_DELAY,
                }
                routes.append((legacy, source_entity))
        if not routes:
            return

        sent_count = 0
        async with _auto_forward_lock:
            handled_routes = set()
            for pair, source_entity in routes:
                route_key = (str(pair.get("source")), str(pair.get("target")))
                if route_key in handled_routes:
                    continue
                handled_routes.add(route_key)
                pair_config = _pair_config(pair)
                if not _within_schedule(pair_config):
                    _log_live(f"⏸️ Auto-forward quiet/schedule window skipped ID={message.id}")
                    continue
                hourly_ok, _ = _hourly_budget(pair.get("id"), pair_config)
                if not hourly_ok:
                    _log_live(f"⏸️ Auto-forward hourly limit reached for {pair.get('target')}")
                    continue
                if not _message_allowed(message, pair_config):
                    _log_live(f"⏭️ Auto-forward filter skipped ID={message.id}")
                    continue
                budget_ok, budget_bucket = _daily_budget(route_key[0], pair_config, message)
                if not budget_ok:
                    _log_live(f"🛑 Auto-forward daily limit reached for {pair.get('target')}")
                    continue
                target = await client.get_entity(pair["target"])
                sent = await send_message(
                    target, message,
                    config=pair_config,
                    source_title=pair.get("source_title", ""),
                    source_entity=source_entity
                )
                auto_stats = state.setdefault("auto_stats", {"sent": 0, "failed": 0})
                if sent:
                    sent_count += 1
                    _daily_budget(route_key[0], pair_config, message, commit=True)
                    _hourly_budget(pair.get("id"), pair_config, commit=True)
                    _record_dedupe(pair.get("id", "default"), message)
                    auto_stats["sent"] += 1
                    _log_live(f"⚡ Auto-forwarded ID={message.id} → {pair.get('target_title', pair['target'])}")
                else:
                    auto_stats["failed"] += 1
                    _log_live(f"❌ Auto-forward failed ID={message.id}")
                state["auto_stats"] = auto_stats
                await asyncio.sleep(max(
                    MIN_RATE_DELAY,
                    pair_config["rate_delay"] + random.uniform(-0.5, 0.5)
                ))
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


async def _notify_owner(text, reply_markup=None):
    if _bot_application:
        try:
            await _bot_application.bot.send_message(
                chat_id=OWNER_ID, text=text, reply_markup=reply_markup
            )
        except Exception as exc:
            logger.warning("Owner alert failed: %s", exc)


async def health_monitor():
    global _health_snapshot
    while True:
        await asyncio.sleep(60)
        snapshot = {}
        for pair in state.get("pairs", []):
            health = {"source_accessible": False, "target_writable": False,
                      "protected": False, "last_success": pair.get("last_success"),
                      "last_error": pair.get("last_error")}
            try:
                source = await client.get_entity(pair["source"])
                health["source_accessible"] = True
                health["protected"] = bool(getattr(source, "noforwards", False))
            except Exception as exc:
                health["last_error"] = f"source: {type(exc).__name__}"
            try:
                target = await client.get_entity(pair["target"])
                health["target_writable"] = not bool(getattr(target, "default_banned_rights", None)
                                                     and getattr(target.default_banned_rights, "send_messages", False))
            except Exception as exc:
                health["last_error"] = f"target: {type(exc).__name__}"
            snapshot[str(pair["id"])] = health
        connected = client.is_connected()
        snapshot["login"] = "ok" if connected else "offline"
        # Compare only actual health states. Volatile fields such as
        # last_success/last_error must not trigger a repeated "login: ok".
        signature = {
            key: (
                value if key == "login" else (
                    value.get("source_accessible"),
                    value.get("target_writable"),
                    value.get("protected"),
                )
            )
            for key, value in snapshot.items()
        }
        changed = [
            key for key in set(_health_snapshot) | set(signature)
            if _health_snapshot.get(key) != signature.get(key)
        ]
        first_check = not _health_snapshot
        _health_snapshot = signature
        state["health"] = snapshot
        save_state(state)
        if changed and not first_check:
            details = "\n".join(
                f"{key}: {snapshot.get(key, 'removed')}" for key in sorted(changed)
            )
            await _notify_owner("⚠️ Channel health changed:\n" + details)


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
        "rate_profile": str(payload.get("rate_profile", "balanced")).lower(),
        "rate_delay": max(MIN_RATE_DELAY, min(float(payload.get("rate_delay", MSG_DELAY)), 300)),
        "max_messages": MAX_TASK_MESSAGES,
        "daily_message_limit": MAX_TASK_MESSAGES,
        "daily_media_mb": max(1, min(int(payload.get("daily_media_mb", DEFAULT_DAILY_MEDIA_MB)), 102400)),
        "auto_forward": bool(payload.get("auto_forward", True)),
        "dedupe_mode": str(payload.get("dedupe_mode", "strong")),
        "max_posts_per_hour": max(0, min(int(payload.get("max_posts_per_hour", 0) or 0), 10000)),
        "schedule_start": str(payload.get("schedule_start", "")),
        "schedule_end": str(payload.get("schedule_end", "")),
        "quiet_start": str(payload.get("quiet_start", "")),
        "quiet_end": str(payload.get("quiet_end", "")),
        "protected_behavior": str(payload.get("protected_behavior", "download")),
        "caption_enabled": bool(payload.get("caption_enabled", False)),
        "caption_template": str(payload.get("caption_template", "")),
        "caption_types": payload.get("caption_types") or ["text", "photo", "video", "doc", "other"],
        "caption_parse_mode": str(payload.get("caption_parse_mode", "md")),
        "thumbnail_enabled": bool(payload.get("thumbnail_enabled", False)),
        "thumbnail_path": "",
    }
    state.setdefault("pairs", []).append(pair)
    save_state(state)
    return pair


async def _dry_run_pair(pair, mode="full", limit=None, min_id=0):
    config = _pair_config(pair)
    source_entity = await client.get_entity(pair["source"])
    scan_limit = min(limit or config["max_messages"], config["max_messages"])
    messages = []
    async for message in client.iter_messages(
        source_entity, reverse=mode != "last", min_id=min_id, limit=scan_limit
    ):
        messages.append(message)
    allowed = [message for message in messages if _message_allowed(message, config)]
    duplicates = [message for message in allowed if _is_duplicate(pair["id"], message)]
    media_mb = sum(_media_size_mb(message) for message in allowed)
    return {
        "pair": pair.get("name", pair["id"]),
        "total_messages": len(messages),
        "allowed_messages": len(allowed) - len(duplicates),
        "filtered_messages": len(messages) - len(allowed),
        "duplicate_messages": len(duplicates),
        "estimated_media_mb": round(media_mb, 2),
        "approximate_seconds": round(len(allowed) * config["rate_delay"] + (len(allowed) // BATCH_SIZE) * BATCH_DELAY),
    }


async def _dry_run_many(pairs, mode, value):
    return await asyncio.gather(*[
        _dry_run_pair(
            pair, mode,
            value if mode == "last" else None,
            value if mode == "from_id" else 0
        )
        for pair in pairs
    ])


# ── Routes ─────────────────────────────────────────────

@flask_app.route("/")
def index():
    return render_template("dashboard.html", active_page="dashboard")


@flask_app.route("/dashboard")
def dashboard_page():
    return render_template("dashboard.html", active_page="dashboard")


@flask_app.route("/tasks")
def tasks_page():
    return render_template("tasks.html", active_page="tasks")


@flask_app.route("/tasks/<task_id>")
def task_detail_page(task_id):
    return render_template("task_detail.html", active_page="tasks", task_id=task_id)


@flask_app.route("/pairs")
def pairs_page():
    return render_template("pairs.html", active_page="pairs")


@flask_app.route("/settings")
def settings_page():
    return render_template("settings.html", active_page="settings")


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
        "limits": {
            "max_batch_tasks": MAX_BATCH_TASKS,
            "max_task_messages": MAX_TASK_MESSAGES,
            "min_rate_delay": MIN_RATE_DELAY,
        },
        "health": state.get("health", {}),
        "transfer": state.get("transfer"),
        "storage": _storage_snapshot(),
        "pair_health": state.get("health", {}),
        "oversized_messages": state.get("oversized_messages", [])[-20:],
        "templates": state.get("templates", {}),
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


@flask_app.route("/api/settings", methods=["GET", "PATCH"])
def api_settings():
    settings = state.setdefault("notification_settings", {
        "task_complete": True, "task_failed": True, "flood_wait": True,
    })
    if request.method == "PATCH":
        payload = request.json or {}
        for key in ("task_complete", "task_failed", "flood_wait"):
            if key in payload:
                settings[key] = bool(payload[key])
        if "auto_forward" in payload:
            state["auto_forward"] = bool(payload["auto_forward"])
        save_state(state)
    return jsonify({
        "ok": True,
        "notification_settings": settings,
        "auto_forward": bool(state.get("auto_forward")),
        "storage_limit_mb": round(TEMP_STORAGE_LIMIT_BYTES / 1048576),
        "max_task_messages": MAX_TASK_MESSAGES,
    })


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


@flask_app.route("/api/pairs/<pair_id>", methods=["PATCH", "DELETE"])
def api_delete_pair(pair_id):
    if request.method == "PATCH":
        pair = _pair_by_id(pair_id)
        if not pair:
            return jsonify({"ok": False, "error": "Pair not found"})
        payload = request.json or {}
        for key in ("name", "rate_profile", "rate_delay", "max_messages",
                    "daily_message_limit", "daily_media_mb", "auto_forward",
                    "caption_prefix", "caption_suffix", "remove_links",
                    "remove_source_name", "include_keywords", "exclude_keywords",
                    "allowed_types", "dedupe_mode", "max_posts_per_hour",
                    "schedule_start", "schedule_end", "quiet_start", "quiet_end",
                    "protected_behavior", "caption_enabled", "caption_template",
                    "caption_types", "caption_parse_mode", "thumbnail_enabled"):
            if key in payload:
                if key in {"include_keywords", "exclude_keywords"}:
                    value = payload[key]
                    pair[key] = (
                        [item.strip() for item in str(value).split(",") if item.strip()]
                        if isinstance(value, str) else value
                    )
                else:
                    pair[key] = payload[key]
        save_state(state)
        return jsonify({"ok": True, "pair": pair})
    pair = _pair_by_id(pair_id)
    before = len(state.get("pairs", []))
    state["pairs"] = [p for p in state.get("pairs", []) if p.get("id") != pair_id]
    if len(state["pairs"]) == before:
        return jsonify({"ok": False, "error": "Pair not found"})
    if pair and pair.get("thumbnail_path"):
        Path(pair["thumbnail_path"]).unlink(missing_ok=True)
    save_state(state)
    return jsonify({"ok": True})


@flask_app.route("/api/pairs/<pair_id>/thumbnail", methods=["POST", "DELETE"])
def api_pair_thumbnail(pair_id):
    pair = _pair_by_id(pair_id)
    if not pair:
        return jsonify({"ok": False, "error": "Pair not found"}), 404
    THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
    old = pair.get("thumbnail_path")
    if request.method == "DELETE":
        if old:
            Path(old).unlink(missing_ok=True)
        pair["thumbnail_path"] = ""
        pair["thumbnail_enabled"] = False
        save_state(state)
        return jsonify({"ok": True, "enabled": False})
    upload = request.files.get("thumbnail")
    if not upload or not upload.filename:
        return jsonify({"ok": False, "error": "Upload a thumbnail image"}), 400
    if not (upload.mimetype or "").startswith("image/"):
        return jsonify({"ok": False, "error": "Thumbnail must be an image"}), 400
    upload.stream.seek(0, os.SEEK_END)
    size = upload.stream.tell()
    upload.stream.seek(0)
    header = upload.stream.read(16)
    upload.stream.seek(0)
    valid_image = (
        header.startswith(b"\xff\xd8\xff")
        or header.startswith(b"\x89PNG\r\n\x1a\n")
        or header[:4] == b"RIFF" and header[8:12] == b"WEBP"
    )
    if not valid_image:
        return jsonify({"ok": False, "error": "Unsupported or invalid image file"}), 400
    if size > 20 * 1024 * 1024:
        return jsonify({"ok": False, "error": "Thumbnail must be 20 MB or smaller"}), 400
    suffix = Path(upload.filename).suffix.lower() or ".jpg"
    path = THUMBNAIL_DIR / f"{pair_id}{suffix}"
    upload.save(path)
    if old and old != str(path):
        Path(old).unlink(missing_ok=True)
    pair["thumbnail_path"] = str(path)
    pair["thumbnail_enabled"] = True
    save_state(state)
    return jsonify({"ok": True, "enabled": True, "filename": path.name})


@flask_app.route("/api/pairs/<pair_id>/dedupe", methods=["POST"])
def api_pair_dedupe(pair_id):
    """Clear identities for an explicit 'Copy again' action."""
    if not _pair_by_id(pair_id):
        return jsonify({"ok": False, "error": "Pair not found"}), 404
    prefix = f"{pair_id}:"
    dedupe = state.setdefault("dedupe", {})
    removed = sum(1 for key in list(dedupe) if str(key).startswith(prefix))
    for key in list(dedupe):
        if str(key).startswith(prefix):
            dedupe.pop(key, None)
    state.setdefault("message_map", {}).pop(str(pair_id), None)
    save_state(state)
    _log_live(f"🔁 Copy again enabled for pair {pair_id}; cleared {removed} identities")
    return jsonify({"ok": True, "removed": removed})


@flask_app.route("/api/tasks/<task_id>/thumbnail", methods=["POST", "DELETE"])
def api_task_thumbnail(task_id):
    task = next((item for item in state.get("tasks", []) if item.get("id") == task_id), None)
    if not task:
        return jsonify({"ok": False, "error": "Task not found"}), 404
    settings = dict(task.get("task_settings") or _pair_config(_pair_by_id(task.get("pair_id"))))
    old = settings.get("thumbnail_path")
    if request.method == "DELETE":
        if old:
            Path(old).unlink(missing_ok=True)
        settings["thumbnail_path"] = ""
        settings["thumbnail_enabled"] = False
    else:
        upload = request.files.get("thumbnail")
        if not upload or not upload.filename:
            return jsonify({"ok": False, "error": "Upload a thumbnail image"}), 400
        if not (upload.mimetype or "").startswith("image/"):
            return jsonify({"ok": False, "error": "Thumbnail must be an image"}), 400
        upload.stream.seek(0, os.SEEK_END)
        size = upload.stream.tell()
        upload.stream.seek(0)
        header = upload.stream.read(16)
        upload.stream.seek(0)
        valid_image = (
            header.startswith(b"\xff\xd8\xff")
            or header.startswith(b"\x89PNG\r\n\x1a\n")
            or (header[:4] == b"RIFF" and header[8:12] == b"WEBP")
        )
        if not valid_image:
            return jsonify({"ok": False, "error": "Unsupported or invalid image file"}), 400
        if size > 20 * 1024 * 1024:
            return jsonify({"ok": False, "error": "Thumbnail must be 20 MB or smaller"}), 400
        THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
        path = THUMBNAIL_DIR / f"task_{task_id}{Path(upload.filename).suffix.lower() or '.jpg'}"
        upload.save(path)
        if old and old != str(path):
            Path(old).unlink(missing_ok=True)
        settings["thumbnail_path"] = str(path)
        settings["thumbnail_enabled"] = True
    task["task_settings"] = settings
    for queued in _task_queue:
        if queued.get("id") == task_id:
            queued["task_settings"] = settings
            queued["config"] = settings
    save_state(state)
    return jsonify({"ok": True, "enabled": settings["thumbnail_enabled"]})


@flask_app.route("/api/storage/cleanup", methods=["POST"])
def api_storage_cleanup():
    removed = 0
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    for path in TEMP_DIR.glob("*"):
        try:
            if path.is_file():
                path.unlink()
                removed += 1
        except OSError:
            pass
    _log_live(f"🧹 Temporary storage cleanup removed {removed} file(s)")
    return jsonify({"ok": True, "removed": removed, "storage": _storage_snapshot()})


@flask_app.route("/api/templates", methods=["GET", "POST", "DELETE"])
def api_templates():
    templates = state.setdefault("templates", {})
    if request.method == "GET":
        return jsonify({"ok": True, "templates": templates})
    payload = request.json or {}
    name = str(payload.get("name", "")).strip()
    if not name:
        return jsonify({"ok": False, "error": "Template name is required"}), 400
    if request.method == "DELETE":
        templates.pop(name, None)
    else:
        templates[name] = {key: value for key, value in payload.items() if key != "name"}
    save_state(state)
    return jsonify({"ok": True, "templates": templates})


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


@flask_app.route("/api/tasks/dry-run", methods=["POST"])
def api_tasks_dry_run():
    payload = request.json or {}
    pair_ids = payload.get("pair_ids") or ([payload.get("pair_id")] if payload.get("pair_id") else [])
    pairs = [_pair_by_id(str(pair_id)) for pair_id in pair_ids]
    if not pairs or any(pair is None for pair in pairs):
        return jsonify({"ok": False, "error": "Select valid pairs first"})
    mode = payload.get("mode", "full")
    try:
        value = int(payload.get("value", 0))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Dry-run value must be a number"})
    if mode == "last" and value < 1:
        return jsonify({"ok": False, "error": "Enter a positive Last N value"})
    if mode == "from_id" and value < 1:
        return jsonify({"ok": False, "error": "Enter a positive message ID"})
    try:
        reports = _run_async(_dry_run_many(pairs, mode, value), timeout=120)
        return jsonify({"ok": True, "reports": reports})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})


@flask_app.route("/api/tasks", methods=["POST"])
def api_create_task():
    payload = request.json or {}
    requested_ids = payload.get("pair_ids")
    if requested_ids is None:
        requested_ids = [payload.get("pair_id")]
    if not isinstance(requested_ids, list):
        requested_ids = [requested_ids]
    requested_ids = list(dict.fromkeys(str(value) for value in requested_ids if value))
    if not requested_ids:
        return jsonify({"ok": False, "error": "Select at least one source-target pair"})
    if len(requested_ids) > MAX_BATCH_TASKS:
        return jsonify({"ok": False, "error": f"Maximum {MAX_BATCH_TASKS} tasks per request allowed"})
    pairs = [_pair_by_id(pair_id) for pair_id in requested_ids]
    if any(pair is None for pair in pairs):
        return jsonify({"ok": False, "error": "One or more selected pairs are invalid"})
    mode = payload.get("mode", "full")
    if mode not in {"full", "last", "from_id"}:
        return jsonify({"ok": False, "error": "Unsupported task mode"})
    try:
        limit = int(payload.get("limit", 0)) or None
        min_id = int(payload.get("min_id", 0))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Message limits must be numbers"})
    if mode == "last" and (limit is None or limit < 1 or limit > MAX_TASK_MESSAGES):
        return jsonify({"ok": False, "error": f"Last N must be between 1 and {MAX_TASK_MESSAGES}"})
    if mode == "from_id" and min_id < 1:
        return jsonify({"ok": False, "error": "From ID must be a positive message ID"})
    priority = str(payload.get("priority", "normal")).lower()
    if priority not in TASK_PRIORITIES:
        return jsonify({"ok": False, "error": "Priority must be low, normal, or high"})
    if payload.get("allow_duplicate") is not True:
        duplicates = [
            task for task in state.get("tasks", [])
            if task.get("pair_id") in requested_ids
            and task.get("mode") == mode
            and task.get("status") in {"queued", "running", "paused"}
        ]
        if duplicates:
            return jsonify({
                "ok": False,
                "code": "duplicate",
                "error": "A similar task is already queued or running",
                "duplicates": [_task_view(task) for task in duplicates[:5]],
            })
    tasks = [
        _queue_sync(
            pair["source"], pair["target"], mode != "last", min_id, limit,
            WebEvent(), False, mode, pair["id"], _pair_config(pair), priority
        )
        for pair in pairs
    ]
    return jsonify({
        "ok": True,
        "tasks": [_task_view(task) for task in tasks],
        "task": _task_view(tasks[0]),
        "created_count": len(tasks),
    })


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
        payload = request.json or {}
        if payload.get("continue") is True:
            ok, message = _resume_paused_task(task)
            return jsonify({"ok": ok, "message": message, "task": _task_view(task)})
        if isinstance(payload.get("settings"), dict):
            settings = dict(task.get("task_settings") or _pair_config(_pair_by_id(task.get("pair_id"))))
            editable = {
                "include_keywords", "exclude_keywords", "caption_prefix", "caption_suffix",
                "remove_links", "remove_source_name", "caption_enabled", "caption_template",
                "thumbnail_enabled", "rate_delay", "max_messages", "daily_media_mb",
                "protected_behavior", "schedule_start", "schedule_end", "quiet_start",
                "quiet_end", "max_posts_per_hour", "caption_parse_mode",
            }
            for key, value in payload["settings"].items():
                if key not in editable:
                    continue
                if key in {"include_keywords", "exclude_keywords"} and isinstance(value, str):
                    value = [item.strip() for item in value.split(",") if item.strip()]
                settings[key] = value
            task["task_settings"] = settings
            for queued in _task_queue:
                if queued.get("id") == task_id:
                    queued["task_settings"] = settings
                    queued["config"] = settings
            save_state(state)
            return jsonify({"ok": True, "task": _task_view(task)})
        paused = bool(payload.get("paused"))
        state.setdefault("task_controls", {}).setdefault(task_id, {})["paused"] = paused
        task["status"] = "paused" if paused else ("running" if task_id == state.get("active_task_id") else "queued")
    save_state(state)
    return jsonify({"ok": True, "task": task})


async def bot_continue_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or query.from_user.id != OWNER_ID:
        return
    await query.answer()
    task_id = (query.data or "").split(":", 1)[-1]
    task = next((item for item in state.get("tasks", []) if item.get("id") == task_id), None)
    if not task:
        await query.edit_message_text("❌ Task nahi mila.")
        return
    ok, message = _resume_paused_task(task)
    if ok:
        await query.edit_message_text(f"▶️ Task {task_id} continue ke liye queue ho gaya.")
    else:
        await query.answer(message, show_alert=True)


@flask_app.route("/api/tasks/bulk", methods=["POST"])
def api_tasks_bulk():
    payload = request.json or {}
    task_ids = [str(value) for value in payload.get("task_ids", []) if value]
    action = payload.get("action")
    if not task_ids or action not in {"pause", "resume", "cancel"}:
        return jsonify({"ok": False, "error": "Choose tasks and a valid action"})
    changed = []
    for task in state.get("tasks", []):
        if task.get("id") not in task_ids:
            continue
        task_id = task["id"]
        if action == "cancel":
            state.setdefault("task_controls", {})[task_id] = {"cancelled": True, "paused": False}
            for queued in list(_task_queue):
                if queued["id"] == task_id:
                    _task_queue.remove(queued)
            task["status"] = "cancelled"
        else:
            paused = action == "pause"
            state.setdefault("task_controls", {}).setdefault(task_id, {})["paused"] = paused
            task["status"] = "paused" if paused else (
                "running" if task_id == state.get("active_task_id") else "queued"
            )
        changed.append(task)
    save_state(state)
    return jsonify({"ok": True, "changed": len(changed), "tasks": changed})


@flask_app.route("/api/tasks/reorder", methods=["POST"])
def api_tasks_reorder():
    ordered_ids = [str(value) for value in (request.json or {}).get("task_ids", []) if value]
    if not ordered_ids:
        return jsonify({"ok": False, "error": "Task order is required"})
    queued = {task["id"]: task for task in _task_queue}
    if set(ordered_ids) - set(queued):
        return jsonify({"ok": False, "error": "Only queued tasks can be reordered"})
    if set(ordered_ids) != set(queued):
        return jsonify({"ok": False, "error": "Include every queued task exactly once"})
    _task_queue.clear()
    _task_queue.extend(queued[task_id] for task_id in ordered_ids)
    save_state(state)
    return jsonify({"ok": True, "queue_size": len(_task_queue)})


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
    global _bot_application
    app = Application.builder().token(BOT_TOKEN).build()
    _bot_application = app

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
    app.add_handler(CommandHandler("force_sync", bot_force_sync))
    app.add_handler(CommandHandler("syncfrom", bot_syncfrom))
    app.add_handler(CommandHandler("synclast", bot_synclast))
    app.add_handler(CommandHandler("refresh", bot_refresh))
    app.add_handler(CommandHandler("tasks", bot_tasks))
    app.add_handler(CommandHandler("autoforward", bot_autoforward))
    app.add_handler(CommandHandler("caption", bot_caption))
    app.add_handler(CommandHandler("setthumbnail", bot_setthumbnail))
    app.add_handler(CallbackQueryHandler(bot_continue_callback, pattern=r"^continue:"))

    print("🤖 Telegram Bot started! Commands available via bot.")
    print("⚡ Both userbot + bot running...")

    # Run both concurrently
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    asyncio.create_task(health_monitor())

    await client.run_until_disconnected()

    await app.updater.stop()
    await app.stop()
    await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
