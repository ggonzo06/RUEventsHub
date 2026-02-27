#!/usr/bin/env python3
"""
RU Events Hub — main scraper runner.

Usage:
    python backend/run_scraper.py
    python backend/run_scraper.py --reset-kill-switch   # clears kill switch manually
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
import os

# Load .env from the project root (one level above this file's directory).
_ROOT = Path(__file__).parent.parent
load_dotenv(_ROOT / ".env")

from supabase import create_client  # noqa: E402 — must come after load_dotenv
from connectors import getinvolved  # noqa: E402

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("run_scraper")

STATE_FILE = Path(__file__).parent / "state.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_supabase_client():
    url = os.getenv("SUPABASE_URL")
    # Prefer the service role key (bypasses RLS) for backend scraping.
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        logger.error(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY (or SUPABASE_ANON_KEY) must be set in the .env file."
        )
        sys.exit(1)
    return create_client(url, key)


def _reset_kill_switch() -> None:
    if not STATE_FILE.exists():
        print("state.json does not exist — nothing to reset.")
        return
    state = json.loads(STATE_FILE.read_text())
    for source in state:
        state[source]["kill_switch"] = False
        state[source]["consecutive_failures"] = 0
    STATE_FILE.write_text(json.dumps(state, indent=2))
    print("Kill switch reset for all sources.")


def _print_summary(result: dict, elapsed_ms: int) -> None:
    print()
    print("=" * 55)
    print("  RU Events Hub — Scraper Summary")
    print("=" * 55)
    print(f"  Timestamp : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Source    : getINVOLVED ({result.get('source', 'n/a')})")
    print(f"  Fetched   : {result.get('fetched', 0):>6} events")
    print(f"  Inserted  : {result.get('inserted', 0):>6} new")
    print(f"  Updated   : {result.get('updated', 0):>6} existing")
    print(f"  Duration  : {elapsed_ms:>6} ms")
    if result.get("error"):
        print(f"  ERROR     : {result['error']}")
    print("=" * 55)
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="RU Events Hub scraper runner")
    parser.add_argument(
        "--reset-kill-switch",
        action="store_true",
        help="Clear the kill switch in state.json and exit.",
    )
    args = parser.parse_args()

    if args.reset_kill_switch:
        _reset_kill_switch()
        return

    logger.info("Starting RU Events Hub scraper …")

    client = _build_supabase_client()

    start = datetime.now(timezone.utc)
    try:
        result = getinvolved.run(client)
    except Exception as exc:
        logger.exception("Unexpected error during scrape: %s", exc)
        result = {"fetched": 0, "inserted": 0, "updated": 0,
                  "source": "none", "error": str(exc)}

    elapsed_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)

    if result.get("error"):
        logger.error("Scrape finished with error: %s", result["error"])
    else:
        logger.info(
            "Scrape complete — fetched=%d  inserted=%d  updated=%d",
            result["fetched"], result["inserted"], result["updated"],
        )

    _print_summary(result, elapsed_ms)

    # Exit with non-zero code on error so cron / CI can detect failures.
    if result.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
