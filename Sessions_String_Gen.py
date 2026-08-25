"""
Standalone Telethon session-string generator for main.py.

This utility is intentionally separate from the main bot. The generated value
matches Telethon's StringSession format used by main.py.
"""

from __future__ import annotations

import asyncio
import getpass
import os
import sys
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession


def env_or_input(name: str, prompt: str, *, secret: bool = False) -> str:
    """Read a setting from the environment, or ask for it interactively."""
    value = os.getenv(name, "").strip()
    if value:
        return value

    read_value = getpass.getpass if secret else input
    return read_value(prompt).strip()


def api_id_from_env_or_input() -> int:
    while True:
        value = env_or_input("API_ID", "Telegram API ID: ")
        try:
            return int(value)
        except ValueError:
            if os.getenv("API_ID"):
                raise RuntimeError("API_ID must be a number.")
            print("API ID must be a number. Please try again.")


async def generate_session() -> None:
    api_id = api_id_from_env_or_input()
    api_hash = env_or_input("API_HASH", "Telegram API hash: ", secret=True)
    phone = env_or_input(
        "PHONE",
        "Telegram phone number (with country code, e.g. +91...): ",
    )
    if not api_hash:
        raise RuntimeError("API hash is required.")
    if not phone:
        raise RuntimeError("Phone number is required.")

    client = TelegramClient(StringSession(), api_id, api_hash)

    try:
        await client.connect()
        if await client.is_user_authorized():
            print("Existing authorized session found; exporting it.")
        else:
            await client.send_code_request(phone)
            print("\nOTP Telegram app/SMS par bheja gaya hai.")
            code = input("OTP code: ").strip().replace(" ", "")
            try:
                await client.sign_in(phone=phone, code=code)
            except SessionPasswordNeededError:
                print("Telegram 2-step verification enabled hai.")
                await client.sign_in(password=getpass.getpass("Telegram 2FA password: "))
            except PhoneCodeInvalidError as exc:
                raise RuntimeError("OTP code galat hai. Workflow dobara run karein.") from exc
            except PhoneCodeExpiredError as exc:
                raise RuntimeError("OTP expire ho gaya. Workflow dobara run karein.") from exc

        session_string = client.session.save()
        me = await client.get_me()
        output_file = Path(
            os.getenv("SESSION_OUTPUT_FILE", "session_string.txt")
        ).expanduser()
        output_file.write_text(session_string + "\n", encoding="utf-8")
        output_file.chmod(0o600)

        print("\nTelethon login successful.")
        print(f"Account: {me.first_name or ''} {me.last_name or ''}".strip())
        print(f"Telethon session string saved to: {output_file}")
        print("\n--- SESSION STRING (copy only this value) ---")
        print(session_string)
        print("--- END SESSION STRING ---")
    except FloodWaitError as exc:
        raise RuntimeError(
            f"Telegram FloodWait: {exc.seconds} seconds wait karke dobara try karein."
        ) from exc
    finally:
        await client.disconnect()


def main() -> int:
    try:
        asyncio.run(generate_session())
        return 0
    except KeyboardInterrupt:
        print("\nSession generation cancelled.")
        return 130
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())