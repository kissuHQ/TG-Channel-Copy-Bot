---
name: Workflow entrypoint audit
description: Imported combined Telegram/dashboard apps may retain a second workflow pointing at a removed legacy script.
---

For imported combined Telegram applications, verify every configured workflow points to a present entrypoint before debugging application behavior; keep one primary workflow when Flask, the userbot, and the Bot API already run together.

**Why:** A stale secondary workflow can look like a bot failure even when the primary combined process is healthy, and duplicate service workflows can compete for the same port or Telegram session.

**How to apply:** Check configured workflow commands against the repository early in an audit, remove only clearly obsolete entries, then restart and inspect the primary workflow logs.