#!/usr/bin/env python3
"""Verify the locally configured Hardcover token without printing it."""
from pathlib import Path
import os
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.api.hardcover import HardcoverAPIError, HardcoverClient
from src.local_env import load_local_env


def main() -> int:
    load_local_env(PROJECT_ROOT / ".env.local")
    token = os.environ.get("HARDCOVER_API_TOKEN", "").strip()
    if not token:
        print("HARDCOVER_API_TOKEN is missing. Add it to .env.local.", file=sys.stderr)
        return 2

    try:
        user = HardcoverClient(token).get_current_user()
    except HardcoverAPIError as exc:
        print(f"Hardcover API test failed: {exc}", file=sys.stderr)
        return 1

    username = str((user or {}).get("username") or "").strip()
    if not username:
        print("Hardcover authenticated, but the me query returned no username.", file=sys.stderr)
        return 1
    print(f"Hardcover API authentication works for: {username}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
