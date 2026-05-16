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
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from telethon import TelegramClient, events
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument,
    MessageMediaWebPage
)
from telethon.errors import (
    FloodWaitError, ChatWriteForbiddenError,
    ChannelPrivateError
)

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, ContextTypes
)

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────
API_ID       = int(os.getenv("API_ID"))
API_HASH     = os.getenv("API_HASH")
PHONE        = os.getenv("PHONE")
OWNER_ID     = int(os.getenv("OWNER_ID"))
BOT_TOKEN    = os.getenv("BOT_TOKEN")

MSG_DELAY    = 4
BATCH_SIZE   = 15
BATCH_DELAY  = 90
# ──────────────────────────────────────────────────────

SESSION_FILE = "archive_session"
STATE_FILE   = "sync_state.json"

client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

# ─── STATE MANAGEMENT ─────────────────────────────────
def load_state():
    if Path(STATE_FILE).exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

state = load_state()

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


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.info$"))
async def cmd_info(event):
    if not is_owner(event.sender_id):
        return
    src = state.get("source_title", "❌ Not set")
    tgt = state.get("target_title", "❌ Not set")
    last = state.get("last_synced_id", 0)
    running = "🟢 Running" if state.get("running") else "🔴 Stopped"
    paused = " (⏸️ Paused)" if state.get("paused") else ""
    await event.edit(
        f"📋 **Current Config**\n\n"
        f"📥 Source: `{src}`\n"
        f"📤 Target: `{tgt}`\n"
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
    src = state.get("source_title", "❌ Not set")
    tgt = state.get("target_title", "❌ Not set")
    last = state.get("last_synced_id", 0)
    running = "🟢 Running" if state.get("running") else "🔴 Stopped"
    paused = " (⏸️ Paused)" if state.get("paused") else ""
    await update.message.reply_text(
        f"📋 *Current Config*\n\n"
        f"📥 Source: `{src}`\n"
        f"📤 Target: `{tgt}`\n"
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


async def _run_sync(progress_msg, source, target, reverse, min_id, limit, is_bot=False):
    async def edit_msg(text):
        try:
            if is_bot:
                await progress_msg.edit_text(text)
            else:
                await progress_msg.edit(text)
        except:
            pass

    try:
        source_entity = await client.get_entity(source)
        target_entity = await client.get_entity(target)

        total = await client.get_messages(source_entity, limit=0)
        total_count = total.total if not limit else limit
        state["total_msgs"] = total_count
        save_state(state)

        await edit_msg(
            f"Sync Started\n\n"
            f"Source: {state.get('source_title', source)}\n"
            f"Target: {state.get('target_title', target)}\n"
            f"Total messages: {total_count}\n\nStarting..."
        )

        count = 0
        failed = 0
        stats = reset_stats()

        async for message in client.iter_messages(
            source_entity,
            reverse=reverse,
            min_id=min_id,
            limit=limit
        ):
            if not state.get("running"):
                break

            while state.get("paused"):
                await asyncio.sleep(2)
                state.update(load_state())

            try:
                sent = await send_message(target_entity, message)
                if sent:
                    count += 1
                    msg_type = get_msg_type(message)
                    stats[msg_type] = stats.get(msg_type, 0) + 1
                    state["last_synced_id"] = message.id
                    state["current_id"] = count
                    state["stats"] = stats
                    save_state(state)

                    if count % 10 == 0:
                        pct = f"{(count/total_count*100):.1f}%" if total_count else "?"
                        await edit_msg(
                            f"Syncing...\n\n"
                            f"Progress: {count}/{total_count} ({pct})\n\n"
                            f"{stats_text(stats)}"
                        )

                    if count % BATCH_SIZE == 0:
                        await asyncio.sleep(BATCH_DELAY)
                    else:
                        await asyncio.sleep(MSG_DELAY)

            except FloodWaitError as e:
                wait = e.seconds + 10
                await edit_msg(
                    f"FloodWait!\n\n"
                    f"Telegram ne slow down karne kaha\n"
                    f"Waiting: {wait} seconds\n\n"
                    f"Progress: {count}/{total_count}"
                )
                await asyncio.sleep(wait)

            except ChatWriteForbiddenError:
                await edit_msg("❌ Target channel mein write permission nahi hai!")
                state["running"] = False
                save_state(state)
                return

            except Exception as e:
                failed += 1
                stats["failed"] = failed
                state["stats"] = stats
                save_state(state)
                print(f"❌ Message {message.id} failed: {e}")
                await asyncio.sleep(MSG_DELAY)
                continue

        state["running"] = False
        state["stats"] = stats
        save_state(state)

        await edit_msg(
            f"✅ Sync Complete!\n\n"
            f"Source: {state.get('source_title', source)}\n"
            f"Target: {state.get('target_title', target)}\n\n"
            f"{stats_text(stats)}\n\n"
            f"{datetime.now().strftime('%d %b %Y, %I:%M %p')}"
        )

    except ChannelPrivateError:
        state["running"] = False
        save_state(state)
        await edit_msg("❌ Source channel private hai ya access nahi hai!")

    except Exception as e:
        state["running"] = False
        save_state(state)
        await edit_msg(f"❌ Fatal error: {e}")


async def send_message(target, message):
    try:
        if message.media and not isinstance(message.media, MessageMediaWebPage):
            await client.send_file(
                target,
                file=message.media,
                caption=message.text or "",
                parse_mode="md"
            )
            return True
        elif message.text:
            await client.send_message(
                target,
                message.text,
                parse_mode="md",
                link_preview=False
            )
            return True
        return False
    except Exception as e:
        raise e


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
#  MAIN — Run both userbot + bot together
# ════════════════════════════════════════════════════════

async def main():
    # Start Telethon userbot
    await client.start(phone=PHONE)
    me = await client.get_me()
    print(f"✅ Userbot logged in as: {me.first_name} (@{me.username})")
    print(f"🔐 Owner ID: {OWNER_ID}")

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
