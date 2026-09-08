from __future__ import annotations

import getpass
import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.auth import SupabaseAuthStore, hash_password


def main() -> int:
    parser = argparse.ArgumentParser(description="Create or update the single DeltaForge operator account.")
    parser.add_argument("--username", help="Operator username. Prompts when omitted.")
    parser.add_argument(
        "--password-env",
        default="DELTAFORGE_OPERATOR_PASSWORD",
        help="Environment variable containing the password for non-interactive setup.",
    )
    args = parser.parse_args()
    load_dotenv(ROOT / ".env")
    username = (args.username or input("DeltaForge username: ")).strip()
    if not username:
        print("Username is required.", file=sys.stderr)
        return 1
    password = os.getenv(args.password_env)
    if password is None:
        password = getpass.getpass("DeltaForge password: ")
        confirm = getpass.getpass("Confirm password: ")
        if password != confirm:
            print("Passwords do not match.", file=sys.stderr)
            return 1
    if len(password) < 12:
        print("Password must be at least 12 characters.", file=sys.stderr)
        return 1
    store = SupabaseAuthStore()
    store.create_or_update_user(username, hash_password(password), is_active=True)
    print(f"DeltaForge operator account is active for username: {username}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
