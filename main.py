"""
Telegram Channel Archive Bot (Telethon Userbot)
================================================
- Sirf owner (tum) use kar sakte ho
- Premium channel se content download → apne private channel pe upload
- Safe rate limiting with FloodWait handling
- Live progress updates
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

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────
API_ID       = int(os.getenv("API_ID"))
API_HASH     = os.getenv("API_HASH")
PHONE        = os.getenv("PHONE")          # +91xxxxxxxxxx
OWNER_ID     = int(os.getenv("OWNER_ID"))  # tera user ID

# Rate limits (safe values - change nahi karna)
MSG_DELAY    = 4    # seconds between each message
BATCH_SIZE   = 15   # kitne messages ke baad lamba pause
BATCH_DELAY  = 90   # seconds (batch ke baad)
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
# state structure:
# {
#   "source": "@channelname",
#   "target": "@targetchannel",
#   "last_synced_id": 0,
#   "running": false,
#   "paused": false,
#   "stats": { "text":0, "photo":0, "video":0, "doc":0, "failed":0 }
# }

# ─── HELPERS ──────────────────────────────────────────
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def reset_stats():
    return {"text": 0, "photo": 0, "video": 0, "doc": 0, "other": 0, "failed": 0}

def stats_text(stats: dict) -> str:
    total = sum(v for k, v in stats.items() if k != "failed")
    return (
        f"📝 Text: `{stats['text']}`\n"
        f"📷 Photo: `{stats['photo']}`\n"
        f"🎥 Video: `{stats['video']}`\n"
        f"📄 Doc: `{stats['doc']}`\n"
        f"📦 Other: `{stats['other']}`\n"
        f"❌ Failed: `{stats['failed']}`\n"
        f"✅ Total: `{total}`"
    )

async def safe_reply(event, text):
    """Reply safely — split if message too long"""
    if len(text) > 4000:
        chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for chunk in chunks:
            await event.reply(chunk)
    else:
        await event.reply(text)

# ─── COMMANDS ─────────────────────────────────────────

@client.on(events.NewMessage(outgoing=True, pattern=r"^\.help$"))
async def cmd_help(event):
    """Show all commands"""
    if not is_owner(event.sender_id):
        return

    await event.edit(
        "🤖 **Archive Bot Commands**\n\n"
        "`.setsource <channel>` — Source channel set karo\n"
        "`.settarget <channel>` — Target channel set karo\n"
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


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.setsource (.+)$"))
async def cmd_setsource(event):
    if not is_owner(event.sender_id):
        return

    channel = event.pattern_match.group(1).strip()
    try:
        entity = await client.get_entity(channel)
        state["source"] = channel
        state["source_title"] = getattr(entity, "title", channel)
        save_state(state)
        await event.edit(f"✅ Source set: **{state['source_title']}**\n`{channel}`")
    except Exception as e:
        await event.edit(f"❌ Error: `{e}`")


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.settarget (.+)$"))
async def cmd_settarget(event):
    if not is_owner(event.sender_id):
        return

    channel = event.pattern_match.group(1).strip()
    try:
        entity = await client.get_entity(channel)
        state["target"] = channel
        state["target_title"] = getattr(entity, "title", channel)
        save_state(state)
        await event.edit(f"✅ Target set: **{state['target_title']}**\n`{channel}`")
    except Exception as e:
        await event.edit(f"❌ Error: `{e}`")


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
async def cmd_status(event):
    if not is_owner(event.sender_id):
        return

    if not state.get("running"):
        stats = state.get("stats", reset_stats())
        await event.edit(
            f"🔴 **Not Running**\n\n"
            f"Last session stats:\n"
            f"{stats_text(stats)}"
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


# ─── SYNC COMMANDS ────────────────────────────────────

@client.on(events.NewMessage(outgoing=True, pattern=r"^\.sync$"))
async def cmd_sync(event):
    """Full sync — sab messages"""
    if not is_owner(event.sender_id):
        return
    await start_sync(event, reverse=True, min_id=0)


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.syncfrom (\d+)$"))
async def cmd_syncfrom(event):
    """Specific ID se sync"""
    if not is_owner(event.sender_id):
        return
    min_id = int(event.pattern_match.group(1))
    await start_sync(event, reverse=True, min_id=min_id)


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.synclast (\d+)$"))
async def cmd_synclast(event):
    """Last N messages sync"""
    if not is_owner(event.sender_id):
        return
    n = int(event.pattern_match.group(1))
    await start_sync(event, reverse=False, limit=n)


# ─── CORE SYNC ENGINE ─────────────────────────────────

async def start_sync(event, reverse=True, min_id=0, limit=None):
    """Core sync logic"""
    if not state.get("source") or not state.get("target"):
        await event.edit("❌ Pehle `.setsource` aur `.settarget` karo!")
        return

    if state.get("running"):
        await event.edit("⚠️ Sync pehle se chal raha hai! `.stop` karo pehle.")
        return

    source = state["source"]
    target = state["target"]

    # Init state
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

    try:
        source_entity = await client.get_entity(source)
        target_entity = await client.get_entity(target)

        # Count total messages
        total = await client.get_messages(source_entity, limit=0)
        total_count = total.total if not limit else limit
        state["total_msgs"] = total_count
        save_state(state)

        await progress_msg.edit(
            f"⏳ **Sync Started**\n\n"
            f"📥 `{state.get('source_title', source)}`\n"
            f"📤 `{state.get('target_title', target)}`\n"
            f"📊 Total messages: `{total_count}`\n\n"
            f"Starting..."
        )

        count = 0
        failed = 0
        stats = reset_stats()

        # Iter messages
        async for message in client.iter_messages(
            source_entity,
            reverse=reverse,
            min_id=min_id,
            limit=limit
        ):
            # Stop check
            if not state.get("running"):
                break

            # Pause check
            while state.get("paused"):
                await asyncio.sleep(2)
                state.update(load_state())

            try:
                sent = await send_message(target_entity, message)
                if sent:
                    count += 1
                    msg_type = get_msg_type(message)
                    stats[msg_type] = stats.get(msg_type, 0) + 1

                    # Update last synced
                    state["last_synced_id"] = message.id
                    state["current_id"] = count
                    state["stats"] = stats
                    save_state(state)

                    # Progress update every 10 messages
                    if count % 10 == 0:
                        pct = f"{(count/total_count*100):.1f}%" if total_count else "?"
                        try:
                            await progress_msg.edit(
                                f"⏳ **Syncing...**\n\n"
                                f"Progress: `{count}/{total_count}` ({pct})\n\n"
                                f"{stats_text(stats)}"
                            )
                        except:
                            pass

                    # Batch delay
                    if count % BATCH_SIZE == 0:
                        await asyncio.sleep(BATCH_DELAY)
                    else:
                        await asyncio.sleep(MSG_DELAY)

            except FloodWaitError as e:
                wait = e.seconds + 10
                try:
                    await progress_msg.edit(
                        f"⚠️ **FloodWait!**\n\n"
                        f"Telegram ne slow down karne kaha\n"
                        f"Waiting: `{wait}` seconds\n\n"
                        f"Progress so far: `{count}/{total_count}`"
                    )
                except:
                    pass
                await asyncio.sleep(wait)

            except ChatWriteForbiddenError:
                await progress_msg.edit("❌ Target channel mein write permission nahi hai!")
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

        # Done!
        state["running"] = False
        state["stats"] = stats
        save_state(state)

        await progress_msg.edit(
            f"✅ **Sync Complete!**\n\n"
            f"📥 Source: `{state.get('source_title', source)}`\n"
            f"📤 Target: `{state.get('target_title', target)}`\n\n"
            f"{stats_text(stats)}\n\n"
            f"🕐 {datetime.now().strftime('%d %b %Y, %I:%M %p')}"
        )

    except ChannelPrivateError:
        state["running"] = False
        save_state(state)
        await progress_msg.edit("❌ Source channel private hai ya access nahi hai!")

    except Exception as e:
        state["running"] = False
        save_state(state)
        await progress_msg.edit(f"❌ Fatal error: `{e}`")


async def send_message(target, message):
    """Single message ko target pe send karo"""
    try:
        # Media with caption
        if message.media and not isinstance(message.media, MessageMediaWebPage):
            await client.send_file(
                target,
                file=message.media,
                caption=message.text or "",
                parse_mode="md"
            )
            return True

        # Pure text
        elif message.text:
            await client.send_message(
                target,
                message.text,
                parse_mode="md",
                link_preview=False
            )
            return True

        return False  # Empty message, skip

    except Exception as e:
        raise e


def get_msg_type(message) -> str:
    """Message type detect karo"""
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


# ─── MAIN ─────────────────────────────────────────────

async def main():
    await client.start(phone=PHONE)
    me = await client.get_me()
    print(f"✅ Logged in as: {me.first_name} (@{me.username})")
    print(f"🔐 Owner ID: {OWNER_ID}")
    print(f"📋 Commands: .help")
    print(f"⚡ Bot running... (Ctrl+C to stop)")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
    