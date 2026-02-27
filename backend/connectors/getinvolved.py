"""
getINVOLVED connector for RU Events Hub.

Tries the Campus Labs REST API first; falls back to the public iCal feed
if the API returns 403 / 429 or any other network error.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dateutil import parser as dateutil_parser
from icalendar import Calendar
from supabase import Client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE = "getinvolved"
USER_AGENT = "RUEventsHub/1.0 (student project; contact: ruventshub@gmail.com)"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}

API_URL = "https://rutgers.campuslabs.com/engage/api/discovery/event/search"
ICAL_URL = "https://rutgers.campuslabs.com/engage/events/ical"

PAGE_SIZE = 100          # events per API page
PAGE_DELAY = 2.0         # seconds between paginated requests
REQUEST_TIMEOUT = 30     # seconds per HTTP request
MAX_FAILURES = 3         # consecutive failures before kill switch activates

STATE_FILE = Path(__file__).parent.parent / "state.json"

# ---------------------------------------------------------------------------
# Campus inference
# ---------------------------------------------------------------------------

CAMPUS_KEYWORDS: dict[str, list[str]] = {
    "College Ave": [
        "Student Center", "Scott Hall", "College Ave", "Voorhees", "Old Queens",
        "Zimmerli", "Hillel", "Academic Building", "Murray Hall", "Demarest",
        "Paul Robeson", "Hardenbergh", "Clothier", "Seminary", "Eagleton",
        "CASC", "Silvers", "Catholic Center", "Linguistic", "Civic Square",
        "St. Peter", "Little Theatre", "Women's House",
        "Alexander Library", "Nicholas Music", "McCormick", "Bloustein",
        "SC&I", "RU Cinema", "Art History", "Loree", "French Seminar",
        "Student Activities Center",
    ],
    "Livingston": [
        "RAC", "Jersey Mike", "Tillett", "Tillet", "Livi", "Livingston",
        "RBS", "Ludwig", "Lucy Stone", "Hatchery", "The Yard",
        "Graduate Student Lounge", "The Cage", "Richardson", "DSC Lounge",
    ],
    "Busch": [
        "Werblin", "Busch", "Hill Center", "SERC",
        "BSC", "BME", "Biomedical Engineering", "Serin", "Richard Weeks",
        "Research Tower", "Proteomics", "Life Science", "CoRE",
        "Rutgers Golf",
    ],
    "Cook/Douglass": [
        "Cook", "Douglass", "Hickman", "Passion Puddle",
        "Ruth Adams", "Marine and Coastal Sciences", "ENR",
        "Rutgers Botanical", "ABE",
    ],
}

ONLINE_KEYWORDS = ["zoom", "online", "virtual", "teams", "webinar"]

# Locations that are explicitly TBD — keep as Unknown rather than Off-Campus.
TBD_PATTERNS = ["tbd", "to be determined", "to be announced", "tba"]

# Recognizable non-Rutgers venues → Off-Campus.
OFF_CAMPUS_KEYWORDS = [
    "Eastern Star Rehabilitation", "Mitsuwa", "Liberty Science Center",
    "United Nations", "Utica University", "Buccleuch", "Johnson Park",
    "New Orleans", "Jersey City", "New York City", "Newark",
]


def infer_campus(location: str | None) -> str:
    if not location:
        return "Unknown"
    loc_lower = location.lower()

    # Explicit TBD → Unknown (not Off-Campus)
    for pat in TBD_PATTERNS:
        if loc_lower.startswith(pat):
            return "Unknown"

    # Online check
    for kw in ONLINE_KEYWORDS:
        if kw in loc_lower:
            return "Online"

    # Rutgers campus keywords
    for campus, keywords in CAMPUS_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in loc_lower:
                return campus

    # Off-campus fallback
    for kw in OFF_CAMPUS_KEYWORDS:
        if kw.lower() in loc_lower:
            return "Off-Campus"

    return "Unknown"


# ---------------------------------------------------------------------------
# Stable event ID
# ---------------------------------------------------------------------------

def make_event_id(title: str, start_time: str, source: str) -> str:
    raw = f"{title}|{start_time}|{source}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# HTML stripping
# ---------------------------------------------------------------------------

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def strip_html(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = _HTML_TAG_RE.sub(" ", text)
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned or None


# ---------------------------------------------------------------------------
# State / kill-switch helpers
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {SOURCE: {"consecutive_failures": 0, "kill_switch": False,
                     "last_success": None, "last_attempt": None}}


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


def _record_failure(state: dict) -> dict:
    s = state.setdefault(SOURCE, {})
    s["consecutive_failures"] = s.get("consecutive_failures", 0) + 1
    s["last_attempt"] = datetime.now(timezone.utc).isoformat()
    if s["consecutive_failures"] >= MAX_FAILURES:
        s["kill_switch"] = True
        logger.error(
            "Kill switch ACTIVATED after %d consecutive failures. "
            "Reset backend/state.json manually to resume.",
            s["consecutive_failures"],
        )
    _save_state(state)
    return state


def _record_success(state: dict) -> dict:
    s = state.setdefault(SOURCE, {})
    s["consecutive_failures"] = 0
    s["kill_switch"] = False
    s["last_success"] = datetime.now(timezone.utc).isoformat()
    s["last_attempt"] = s["last_success"]
    _save_state(state)
    return state


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _to_utc_iso(dt_value: Any) -> str | None:
    """Return an ISO-8601 UTC string from a datetime, date, or string."""
    if dt_value is None:
        return None
    if isinstance(dt_value, str):
        try:
            dt_value = dateutil_parser.parse(dt_value)
        except Exception:
            return None
    # icalendar returns date or datetime
    if not isinstance(dt_value, datetime):
        # plain date → midnight UTC
        dt_value = datetime(dt_value.year, dt_value.month, dt_value.day,
                            tzinfo=timezone.utc)
    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=timezone.utc)
    return dt_value.astimezone(timezone.utc).isoformat()


def _normalize_api_event(raw: dict) -> dict | None:
    """Map a Campus Labs API event dict to our schema."""
    title = (raw.get("name") or "").strip()
    if not title:
        return None

    start_iso = _to_utc_iso(raw.get("startsOn"))
    if not start_iso:
        return None

    end_iso = _to_utc_iso(raw.get("endsOn"))
    location = (raw.get("location") or "").strip() or None

    # organization — various field names observed in the wild
    org = (
        raw.get("organizationName")
        or raw.get("organizationId")  # fallback numeric id as string
        or ""
    )
    if isinstance(org, int):
        org = str(org)
    org = org.strip() or None

    # category — API may return a list or a single string
    categories = raw.get("categoryNames") or raw.get("categoryName") or []
    if isinstance(categories, str):
        categories = [categories]
    category = ", ".join(c for c in categories if c).strip() or None

    # source URL — construct from numeric id when available
    numeric_id = raw.get("id")
    if numeric_id:
        source_url = f"https://rutgers.campuslabs.com/engage/event/{numeric_id}"
    else:
        source_url = API_URL

    event_id = make_event_id(title, start_iso, SOURCE)

    return {
        "event_id":    event_id,
        "source":      SOURCE,
        "title":       title,
        "description": strip_html(raw.get("description")),
        "start_time":  start_iso,
        "end_time":    end_iso,
        "location":    location,
        "campus":      infer_campus(location),
        "organization": org,
        "category":    category,
        "source_url":  source_url,
        "last_seen":   datetime.now(timezone.utc).isoformat(),
    }


def _normalize_ical_event(component: Any) -> dict | None:
    """Map an icalendar VEVENT component to our schema."""
    title = str(component.get("SUMMARY") or "").strip()
    if not title:
        return None

    dtstart = component.get("DTSTART")
    start_iso = _to_utc_iso(dtstart.dt if dtstart else None)
    if not start_iso:
        return None

    dtend = component.get("DTEND")
    end_iso = _to_utc_iso(dtend.dt if dtend else None)

    location = str(component.get("LOCATION") or "").strip() or None
    description = strip_html(str(component.get("DESCRIPTION") or "").strip() or None)
    url = str(component.get("URL") or "").strip() or ICAL_URL

    # iCal UID can serve as a more stable identifier, but we still hash for
    # consistency with the API path.
    event_id = make_event_id(title, start_iso, SOURCE)

    return {
        "event_id":    event_id,
        "source":      SOURCE,
        "title":       title,
        "description": description,
        "start_time":  start_iso,
        "end_time":    end_iso,
        "location":    location,
        "campus":      infer_campus(location),
        "organization": None,   # iCal feed doesn't carry org info
        "category":    None,
        "source_url":  url,
        "last_seen":   datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Fetching logic
# ---------------------------------------------------------------------------

def _fetch_via_api() -> list[dict]:
    """
    Fetch all approved events from the Campus Labs REST API,
    paginating with PAGE_SIZE / PAGE_DELAY.
    Raises requests.HTTPError on 4xx/5xx so the caller can fall back.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    params = {
        "endsAfter":        today,
        "orderByField":     "endsOn",
        "orderByDirection": "ascending",
        "status":           "Approved",
        "take":             PAGE_SIZE,
        "skip":             0,
    }

    events: list[dict] = []
    page = 0

    while True:
        params["skip"] = page * PAGE_SIZE
        if page > 0:
            time.sleep(PAGE_DELAY)

        logger.info("API page %d (skip=%d) …", page, params["skip"])
        resp = requests.get(API_URL, params=params, headers=HEADERS,
                            timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        data = resp.json()
        # Campus Labs API wraps results under "value"; fallback to list root.
        raw_list: list[dict] = data.get("value") or (data if isinstance(data, list) else [])
        total: int = data.get("totalItems") or data.get("@odata.count") or len(raw_list)

        for raw in raw_list:
            normalized = _normalize_api_event(raw)
            if normalized:
                events.append(normalized)

        page += 1
        if len(events) >= total or not raw_list:
            break

    return events


def _fetch_via_ical() -> list[dict]:
    """Fetch and parse the public iCal feed as a fallback."""
    logger.info("Fetching iCal feed from %s …", ICAL_URL)
    resp = requests.get(ICAL_URL, headers={**HEADERS, "Accept": "text/calendar"},
                        timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    cal = Calendar.from_ical(resp.content)
    events: list[dict] = []
    for component in cal.walk():
        if component.name == "VEVENT":
            normalized = _normalize_ical_event(component)
            if normalized:
                events.append(normalized)

    return events


# ---------------------------------------------------------------------------
# Supabase upsert
# ---------------------------------------------------------------------------

def _get_stored_count(client: Client) -> int:
    result = (
        client.table("events")
        .select("id", count="exact")
        .eq("source", SOURCE)
        .execute()
    )
    return result.count or 0


def _upsert_events(client: Client, events: list[dict]) -> tuple[int, int]:
    """
    Upsert events into Supabase.
    Returns (inserted, updated) counts (approximate — Supabase doesn't
    distinguish them cleanly, so we track by checking existing event_ids).
    """
    if not events:
        return 0, 0

    # Deduplicate by event_id — the API can return the same event on multiple
    # pages, which causes Postgres to reject the batch upsert.
    seen: set[str] = set()
    deduped: list[dict] = []
    for e in events:
        if e["event_id"] not in seen:
            seen.add(e["event_id"])
            deduped.append(e)
    if len(deduped) < len(events):
        logger.info("Deduplicated %d → %d events.", len(events), len(deduped))
    events = deduped

    # Fetch existing event_ids for this batch so we can classify ins vs upd.
    ids = [e["event_id"] for e in events]
    existing_result = (
        client.table("events")
        .select("event_id")
        .in_("event_id", ids)
        .execute()
    )
    existing_ids = {row["event_id"] for row in (existing_result.data or [])}

    inserted = sum(1 for e in events if e["event_id"] not in existing_ids)
    updated = len(events) - inserted

    client.table("events").upsert(events, on_conflict="event_id").execute()

    return inserted, updated


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(client: Client) -> dict:
    """
    Fetch getINVOLVED events and upsert into Supabase.

    Returns a summary dict:
      {
        "fetched":  int,
        "inserted": int,
        "updated":  int,
        "source":   str ("api" | "ical" | "none"),
        "error":    str | None,
      }
    """
    state = _load_state()
    source_state = state.get(SOURCE, {})

    # --- Kill-switch check ---------------------------------------------------
    if source_state.get("kill_switch"):
        msg = (
            f"Kill switch is active for '{SOURCE}' after "
            f"{source_state.get('consecutive_failures', 0)} consecutive "
            "failures. Reset backend/state.json manually to resume."
        )
        logger.error(msg)
        return {"fetched": 0, "inserted": 0, "updated": 0,
                "source": "none", "error": msg}

    # --- Fetch ---------------------------------------------------------------
    events: list[dict] = []
    fetch_source = "none"

    try:
        events = _fetch_via_api()
        fetch_source = "api"
        logger.info("API returned %d events.", len(events))
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 0
        if status in (403, 429):
            logger.warning("API returned %d — falling back to iCal.", status)
        else:
            logger.warning("API error %d — falling back to iCal.", status)
        try:
            events = _fetch_via_ical()
            fetch_source = "ical"
            logger.info("iCal returned %d events.", len(events))
        except Exception as ical_exc:
            logger.error("iCal fallback also failed: %s", ical_exc)
            state = _record_failure(state)
            return {"fetched": 0, "inserted": 0, "updated": 0,
                    "source": "none", "error": str(ical_exc)}
    except Exception as exc:
        logger.warning("API fetch failed (%s) — falling back to iCal.", exc)
        try:
            events = _fetch_via_ical()
            fetch_source = "ical"
            logger.info("iCal returned %d events.", len(events))
        except Exception as ical_exc:
            logger.error("iCal fallback also failed: %s", ical_exc)
            state = _record_failure(state)
            return {"fetched": 0, "inserted": 0, "updated": 0,
                    "source": "none", "error": str(ical_exc)}

    # --- Safety check --------------------------------------------------------
    if len(events) == 0:
        stored = _get_stored_count(client)
        if stored >= 50:
            msg = (
                f"Fetched 0 events but {stored} are stored. "
                "Refusing to wipe data — check the source manually."
            )
            logger.error(msg)
            state = _record_failure(state)
            return {"fetched": 0, "inserted": 0, "updated": 0,
                    "source": fetch_source, "error": msg}
        logger.warning("Fetched 0 events (stored count: %d). Skipping upsert.", stored)
        state = _record_success(state)
        return {"fetched": 0, "inserted": 0, "updated": 0,
                "source": fetch_source, "error": None}

    # --- Upsert --------------------------------------------------------------
    try:
        inserted, updated = _upsert_events(client, events)
    except Exception as exc:
        logger.error("Supabase upsert failed: %s", exc)
        state = _record_failure(state)
        return {"fetched": len(events), "inserted": 0, "updated": 0,
                "source": fetch_source, "error": str(exc)}

    state = _record_success(state)
    return {
        "fetched":  len(events),
        "inserted": inserted,
        "updated":  updated,
        "source":   fetch_source,
        "error":    None,
    }
