# Archive Bot

## Run

The application workflow runs:

```bash
python main.py
```

It starts the Flask dashboard on the configured `PORT` (8080 locally) and the
Telethon userbot plus Telegram Bot API controller in the same process.

## Required Replit Secrets

`API_ID`, `API_HASH`, `PHONE`, `OWNER_ID`, and `BOT_TOKEN` must be configured
in Replit Secrets. The Telethon session is persisted to `session_string.txt`
after a successful login.

## Storage safety

Temporary media is downloaded under `/tmp/archive_bot`. Downloads are
preflight-checked against a hard 1.8 GB temporary-storage budget and are
deleted after upload. The dashboard's Temporary Storage panel can clean
leftover failed downloads.