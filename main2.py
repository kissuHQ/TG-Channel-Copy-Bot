"""
Standalone Pyrogram session-string generator.

This file intentionally has no imports from the main application. Run it from
the separate "session" workflow, enter the Telegram OTP in the console, and
copy the generated string into the project that uses Pyrogram.
"""

from __future__ import annotations

import asyncio
import getpass
import os
import sys

from pyrogram import Client
from pyrogram.errors import (
    FloodWait,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    SessionPasswordNeeded,
)


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing {name}. Add it to Replit Secrets before starting this workflow."
        )
    return value


async def generate_session() -> None:
    try:
        api_id = int(required_env("API_ID"))
        api_hash = required_env("API_HASH")
    except ValueError as exc:
        raise RuntimeError("API_ID must be a number.") from exc

    phone = os.getenv("PHONE", "").strip() or input(
        "Telegram phone number (with country code, e.g. +91...): "
    ).strip()
    if not phone:
        raise RuntimeError("Phone number is required.")

    # in_memory=True means this utility never creates a local .session file.
    client = Client(
        "session_generator",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
        device_model="Replit Session Generator",
        app_version="1.0",
    )

    try:
        await client.connect()
        sent_code = await client.send_code(phone)
        print("\nOTP Telegram app/SMS par bheja gaya hai.")
        code = input("OTP code: ").strip().replace(" ", "")

        try:
            await client.sign_in(phone, sent_code.phone_code_hash, code)
        except SessionPasswordNeeded:
            print("Telegram 2-step verification enabled hai.")
            password = getpass.getpass("Telegram 2FA password: ")
            await client.check_password(password)
        except PhoneCodeInvalid as exc:
            raise RuntimeError("OTP code galat hai. Workflow dobara run karein.") from exc
        except PhoneCodeExpired as exc:
            raise RuntimeError("OTP expire ho gaya. Workflow dobara run karein.") from exc

        session_string = await client.export_session_string()
        me = await client.get_me()

        print("\nLogin successful.")
        print(f"Account: {me.first_name or ''} {me.last_name or ''}".strip())
        print("\n--- SESSION STRING (copy only this value) ---")
        print(session_string)
        print("--- END SESSION STRING ---")
        print("\nIs workflow ko stop kar sakte hain; ye session file disk par save nahi hui.")
    except FloodWait as exc:
        raise RuntimeError(
            f"Telegram FloodWait: {exc.value} seconds wait karke dobara try karein."
        ) from exc
    finally:
        if client.is_connected:
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