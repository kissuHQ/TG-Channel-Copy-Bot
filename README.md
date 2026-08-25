# TG Channel Copy Bot — Archive Bot
## By PR

Telegram channels ke messages ko source channel se target channel mein safely copy
karne wala owner-only archive bot. Project ek hi Python process mein:

- **Telethon userbot** — channel access, message reading aur media transfer
- **Telegram Bot API controller** — remote commands aur notifications
- **Flask dashboard** — live monitoring, pair management aur task controls

Forward headers ke bina copy karne ki koshish hoti hai. Protected content ya
custom caption/thumbnail wale messages download karke target par upload hote hain.

## Features

### Channel copying

- Full sync, message ID se sync, ya last N messages sync
- Source aur target channel ke liye:
  - `@username`
  - Telegram channel ID (`-100...`)
  - numeric ID
  - `t.me` / `telegram.me` link
  - forwarded message se private channel detection
- Text, photos, videos, documents, audio aur other media support
- Albums ko grouped upload ke roop mein preserve karne ki koshish
- Original filenames, document attributes, video streaming aur audio metadata
  preserve kiye jaate hain
- Protected-content policy:
  - **Download and upload** — protected media ko re-upload karo
  - **Skip** — protected message ko skip karo aur log karo
- Failed transfers ke liye error details aur available Telegram message link

### Strong duplicate detection

Bot duplicate messages ko multiple identities ke basis par detect karta hai:

- Source message ID mapping
- Stable text identity
- Media fingerprint using Telegram document/photo metadata
- Pair-specific dedupe records
- Dashboard se **Copy again** action, jo selected pair ki dedupe identities
  clear karta hai

### Per-pair rules

Har source → target pair ki independent settings hoti hain:

- Pair name
- Allowed message types
- Include keywords
- Exclude keywords
- Caption prefix aur suffix
- Link removal
- Source-name removal
- Rate profile aur custom delay
- Per-run maximum messages
- Daily message limit
- Daily media limit
- Schedule window
- Quiet hours
- Maximum posts per hour
- Protected-content behavior
- Caption changer
- Video thumbnail changer

### Caption changer

Caption changer per pair enable/disable kiya ja sakta hai. Caption ko selected
message types par apply kiya ja sakta hai:

- Text
- Photo
- Video
- Document
- Other

Supported formatting modes:

- Markdown
- HTML
- Plain text

Template placeholders:

| Placeholder | Value |
|---|---|
| `{caption}` | Original message text/caption |
| `{filename}` | Document filename, if available |
| `{filesize}` | Human-readable file size |
| `{filesize_mb}` | File size in MB |
| `{message_id}` | Source Telegram message ID |
| `{source}` | Source channel title |
| `{date}` | Processing date (`YYYY-MM-DD`) |
| `{time}` | Processing time (`HH:MM`) |
| `{mime}` | Media MIME type |
| `{type}` | Detected message type |

Example:

```text
🎬 {filename}
Size: {filesize}
Source: {source}

{caption}
```

Invalid templates original caption par safely fall back karte hain. Caption
customization direct-copy path ko bypass karke upload path use karti hai.

### Video thumbnail changer

- Thumbnail per pair enable/disable
- Sirf videos par apply hota hai
- Dashboard se image upload
- Telegram Bot API se replied photo ko thumbnail set karna
- JPEG, PNG aur WebP signature validation
- Maximum thumbnail size: 20 MB
- Pair delete karne par saved thumbnail cleanup
- Temporary media aur thumbnails ko source channel se permanently link nahi kiya
  jaata

### Safety, limits aur resilience

- Temporary downloads `/tmp/archive_bot` mein store hote hain
- Hard temporary storage budget: **1.8 GB**
- Download se pehle size/quota check
- Upload ke baad temporary files cleanup
- Dashboard se leftover temporary files cleanup
- Safe rate limiting aur Telegram `FloodWait` handling
- FloodWait warnings aur task failure notifications
- Storage-limit failure alerts
- Daily message/media quotas
- Source accessibility health
- Target write permission health
- Forwarding-protection status
- Telegram session connection status
- Last successful sync aur last error details
- Owner-only userbot aur Bot API controls
- Live dashboard updates Server-Sent Events ke through

## Requirements

- Python 3.12 recommended
- Telegram account with access to the source channel
- Target channel mein posting permission
- Packages listed in `requirements.txt`

Install:

```bash
pip install -r requirements.txt
```

## Replit setup

Replit Secrets mein ye values add karein:

| Secret | Description |
|---|---|
| `API_ID` | Telegram API ID from my.telegram.org |
| `API_HASH` | Telegram API hash |
| `PHONE` | Telethon user account phone number |
| `OWNER_ID` | Authorized Telegram owner numeric user ID |
| `BOT_TOKEN` | BotFather se Telegram bot token |

Secrets ko source code, logs ya README mein paste na karein.

First successful login ke baad Telethon session `session_string.txt` mein
persist hota hai. Is file ko public repository mein commit na karein.

## Running

Primary application workflow:

```bash
python main.py
```

Application Flask dashboard ko `PORT` par start karta hai. Replit mein local
dashboard normally port `8080` par available hota hai. Workflow `Start
application` userbot, Bot API controller aur dashboard ko saath run karta hai.

## First-time usage

1. Required Replit Secrets configure karein.
2. `python main.py` run karein aur Telegram login complete hone dein.
3. Dashboard open karein.
4. Source aur target channels set karein.
5. Source → target pair create karein.
6. Pair rules configure karein.
7. Optional caption template ya thumbnail upload karein.
8. Pehle **Dry run** use karke filters aur limits verify karein.
9. Sync task start karein.
10. Channel Health, Live Progress, logs aur task report monitor karein.

## Userbot commands

Userbot account se bheje gaye commands dot prefix use karte hain. Sirf
`OWNER_ID` authorized hai.

| Command | Action |
|---|---|
| `.help` | Command list |
| `.setsource @username` | Source set |
| `.setsource -100123...` | Source ID set |
| `.setsource` | Replied forwarded message se source set |
| `.settarget @username` | Target set |
| `.settarget -100123...` | Target ID set |
| `.settarget` | Replied forwarded message se target set |
| `.info` | Channel information aur current config |
| `.sync` | Full sync |
| `.syncfrom <id>` | Given message ID se sync |
| `.synclast <n>` | Last N messages sync |
| `.pause` | Running sync pause |
| `.resume` | Paused sync resume |
| `.stop` | Sync stop |
| `.status` | Live/last sync status |
| `.reset` | Configuration reset |

## Telegram bot commands

BotFather bot ko message bhejte waqt slash commands use karein. Bot commands
owner-only hain.

| Command | Action |
|---|---|
| `/start` | Bot introduction |
| `/help` | Command list |
| `/setsource <channel>` | Source set |
| `/settarget <channel>` | Target set |
| `/info` | Current configuration |
| `/sync` | Full sync |
| `/syncfrom <id>` | Message ID se sync |
| `/synclast <n>` | Last N messages sync |
| `/tasks` | Task queue |
| `/autoforward on` | New source posts auto-copy enable |
| `/autoforward off` | Auto-forward disable |
| `/caption <pair_id> on\|off [template]` | Pair caption changer |
| `/setthumbnail <pair_id>` | Replied photo ko video thumbnail set karo |
| `/setthumbnail <pair_id> off` | Thumbnail disable |
| `/pause` | Sync pause |
| `/resume` | Sync resume |
| `/stop` | Sync stop |
| `/status` | Live status |
| `/reset` | Configuration reset |

Caption command ke optional template mein placeholders use kiye ja sakte hain.
Selected types command se set karne ke liye template se pehle example:

```text
/caption abc123 on types=video,doc 🎬 {filename} | {filesize}
```

## Dashboard capabilities

Dashboard mein ye panels/actions available hain:

- Source aur target channel configuration
- Source → target pair create/edit/delete
- Message-type filters
- Keyword filters
- Rate, quota aur schedule controls
- Caption template, parse mode aur caption-type controls
- Per-pair thumbnail image upload
- Auto-forward enable/disable
- Full sync, sync-from-ID aur last-N controls
- Dry run
- Pause, resume aur stop
- Task queue, priority, reorder, bulk actions aur reports
- Live progress aur media statistics
- Channel health
- Temporary storage usage aur cleanup
- Searchable live logs
- Duplicate identities clear karke copy-again

## HTTP API

Dashboard same-origin Flask API use karta hai. Important endpoints:

| Endpoint | Method | Purpose |
|---|---|---|
| `/health` | GET | Health check |
| `/api/status` | GET | Dashboard status |
| `/api/bootstrap` | GET | Initial dashboard data |
| `/api/events` | GET | Live Server-Sent Events stream |
| `/api/pairs` | GET/POST | Pair list/create |
| `/api/pairs/<pair_id>` | PATCH/DELETE | Pair update/delete |
| `/api/pairs/<pair_id>/thumbnail` | POST/DELETE | Thumbnail upload/remove |
| `/api/pairs/<pair_id>/dedupe` | POST | Pair dedupe identities clear |
| `/api/templates` | GET/POST/DELETE | Stored pair-rule templates |
| `/api/storage/cleanup` | POST | Temporary files cleanup |
| `/api/tasks` | GET/POST | List/create tasks |
| `/api/tasks/dry-run` | POST | Filter preview |
| `/api/tasks/<task_id>` | PATCH/DELETE | Task update/delete |
| `/api/tasks/<task_id>/report` | GET | Task report; `?format=csv` for CSV |
| `/api/tasks/bulk` | POST | Bulk task action |
| `/api/tasks/reorder` | POST | Queue reorder |
| `/api/sync` | POST | Full sync |
| `/api/syncfrom` | POST | Sync from message ID |
| `/api/synclast` | POST | Sync last N messages |
| `/api/autoforward` | POST | Global auto-forward control |
| `/api/pause` | POST | Pause |
| `/api/resume` | POST | Resume |
| `/api/stop` | POST | Stop |
| `/api/reset` | POST | Reset |
| `/api/logs` | GET | Live logs |
| `/api/logs/search` | GET | Filter logs with `?q=...` |

## Persistent files

| File/path | Purpose |
|---|---|
| `sync_state.json` | Pair configuration, tasks, stats and dedupe state |
| `sync.log` | Detailed application log |
| `session_string.txt` | Persisted Telethon session |
| `thumbnails/` | Per-pair thumbnail files |
| `/tmp/archive_bot/` | Temporary downloaded media |

Sensitive/runtime files ko version control mein commit na karein. `.gitignore`
mein local session, runtime state, logs aur temporary artifacts exclude karna
recommended hai.

## Troubleshooting

### Dashboard open nahi ho raha

Workflow `Start application` running hai ya nahi check karein. `PORT` ke
through configured port use karein; Replit setup mein default local port 8080
hai.

### Source inaccessible

Userbot account ko source channel mein access hona chahiye. Private channel ke
liye channel ID ya forwarded message se `.setsource` / `/setsource` use karein.

### Target write error

Userbot account ko target channel mein member/admin posting permission dein.
Dashboard ke Channel Health panel mein target write status check karein.

### FloodWait

Rate profile ko `very_safe` ya longer delay par set karein. Bot FloodWait ko
respect karta hai; repeated retries se pehle Telegram ka wait period complete
hone dein.

### Storage limit

Dashboard ke Temporary Storage panel se cleanup run karein. Large media,
parallel tasks aur daily media limits review karein.

### Thumbnail apply nahi ho raha

Confirm karein ki:

1. Pair mein thumbnail enabled hai.
2. Saved file valid JPEG, PNG ya WebP hai.
3. Message ka detected type video hai.
4. Pair custom thumbnail upload ke baad save hua hai.

## Security notes

- Telegram credentials aur bot token sirf Replit Secrets mein rakhein.
- Owner checks userbot aur Bot API dono paths par apply hote hain.
- `OWNER_ID` ko correct numeric Telegram user ID par set karein.
- `session_string.txt`, `sync_state.json`, `sync.log` aur `thumbnails/` ko
  untrusted users ke saath share na karein.
- Production deployment ke liye Flask development server ke bajay suitable
  production WSGI setup use karein.

## License

License Taken by PR BOT SERVICE'S 
