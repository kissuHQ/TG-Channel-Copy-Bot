---
name: Limit-induced task pausing
description: Temporary Telegram and quota limits must preserve task progress for continuation.
---

Temporary limits are a paused-task condition, not successful completion or permanent failure.

**Why:** Marking a limit-stopped sync complete loses the user's intended continuation point and sends misleading status.

**How to apply:** Persist a resume cursor, expose a paused state, notify the owner with a Continue action, and requeue the same task identity when continued.