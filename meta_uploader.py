"""
meta_uploader.py — Oneboxx Meta Offline Events Uploader
Reads new orders from Google Sheet → uploads to Facebook Offline Events API daily.

ENV VARS NEEDED (add to Railway):
  META_ACCESS_TOKEN   — your Meta system user / page access token
  META_DATASET_ID     — Offline Event Set ID from Events Manager → Settings
  (reuses GOOGLE_CREDENTIALS_JSON and GOOGLE_SHEET_ID from ship bot)

USAGE:
  - Called automatically daily at 11:00 PM IST via scheduler
  - Or manually via /uploadfb Telegram command
  - Tracks uploaded orders in meta_uploaded.json to avoid duplicates
"""

import os
import re
import json
import logging
import pytz
import requests
from datetime import datetime, timezone

log = logging.getLogger("meta_uploader")

META_API_VERSION = "v19.0"
UPLOADED_FILE    = "meta_uploaded.json"
IST              = pytz.timezone("Asia/Kolkata")

# ─── TRACKING ─────────────────────────────────────────────────────────────────

def load_uploaded():
    """Load set of already-uploaded order numbers."""
    if not os.path.exists(UPLOADED_FILE):
        return set()
    try:
        with open(UPLOADED_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_uploaded(uploaded: set):
    with open(UPLOADED_FILE, "w") as f:
        json.dump(sorted(list(uploaded)), f)

# ─── DATA HELPERS ─────────────────────────────────────────────────────────────

# Kannada → English city/state map
KANNADA_MAP = {
    "ಚಿತ್ರದುರ್ಗ": "chitradurga",
    "ಕರ್ನಾಟಕ":    "karnataka",
    "ಗುರು":       "guru",
    "ಬೆಂಗಳೂರು":   "bengaluru",
    "ಮೈಸೂರು":     "mysuru",
    "ಹಾಸನ":       "hassan",
    "ತುಮಕೂರು":    "tumkur",
    "ಮಂಡ್ಯ":       "mandya",
    "ಶಿವಮೊಗ್ಗ":   "shimoga",
}

def fix_text(s: str) -> str:
    s = str(s).strip()
    return KANNADA_MAP.get(s, s).lower()

def is_ascii(s: str) -> bool:
    return all(ord(c) < 128 for c in str(s))

def clean_phone(p) -> str:
    p = re.sub(r"\D", "", str(p).replace(".0", "").strip())
    return p[-10:] if len(p) >= 10 else p

def clean_zip(z) -> str:
    try:
        return str(int(z)).zfill(6)
    except Exception:
        return ""

def split_name(name: str):
    parts = fix_text(name).split()
    fn = parts[0] if parts else ""
    ln = " ".join(parts[1:]) if len(parts) > 1 else ""
    return fn, ln

def parse_date_to_iso(val) -> str:
    """
    Handles both datetime objects (from Excel/Sheet) and strings.
    Returns UTC ISO8601 string: 2026-03-07T13:11:59Z
    """
    try:
        if isinstance(val, datetime):
            # Sheet may give datetime(2026, 9, 3) meaning month=9=Sep, day=3
            # BUT original data is DD-MM stored as MM-DD by Excel → swap
            # Check if month > 12 (impossible) or if it looks swapped
            # Safe approach: treat as-is if it's already a proper datetime from gspread
            if val.month > 12:
                # impossible — must be swapped
                actual = datetime(val.year, val.day, val.month, 12, 0, 0)
            else:
                actual = val.replace(hour=12) if val.hour == 0 else val
        elif isinstance(val, str):
            val = val.strip()
            if not val or val.lower() in ("nan", "none", ""):
                actual = datetime.now()
            elif "T" in val:
                # Already ISO
                actual = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return actual.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            elif "/" in val:
                # M/D/YYYY
                actual = datetime.strptime(val, "%m/%d/%Y").replace(hour=12)
            elif "-" in val:
                parts = val.split(" ")[0].split("-")
                a, b = int(parts[0]), int(parts[1])
                time_part = val.split(" ")[1] if " " in val else "12:00:00"
                if b > 12:
                    fmt = f"{parts[2]}-{a:02d}-{b:02d} {time_part}"
                    actual = datetime.strptime(fmt, "%Y-%m-%d %H:%M:%S") if len(parts[2]) == 4 else \
                             datetime.strptime(f"{b:02d}-{a:02d}-{parts[2]} {time_part}", "%d-%m-%Y %H:%M:%S")
                else:
                    actual = datetime.strptime(val, "%d-%m-%Y %H:%M:%S") if len(parts[0]) <= 2 else \
                             datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
            else:
                actual = datetime.now()
        else:
            actual = datetime.now()

        dt_ist = IST.localize(actual) if actual.tzinfo is None else actual
        return dt_ist.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    except Exception as e:
        log.warning(f"Date parse failed for '{val}': {e}")
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ─── SHEET READER ─────────────────────────────────────────────────────────────

def read_sheet_orders():
    """
    Read all rows from Google Sheet 'Orders' tab.
    Returns list of raw row dicts.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        raw = os.getenv("GOOGLE_CREDENTIALS_JSON")
        sid = os.getenv("GOOGLE_SHEET_ID")
        if not raw or not sid:
            log.error("Missing GOOGLE_CREDENTIALS_JSON or GOOGLE_SHEET_ID")
            return []

        creds = Credentials.from_service_account_info(
            json.loads(raw),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sid)
        ws = sh.worksheet("Orders")
        rows = ws.get_all_records()
        log.info(f"Sheet: {len(rows)} rows read")
        return rows

    except Exception as e:
        log.error(f"Sheet read error: {e}", exc_info=True)
        return []

# ─── FORMAT EVENTS ────────────────────────────────────────────────────────────

def row_to_event(row: dict, order_num: str) -> dict:
    """Convert a sheet row to a Meta offline event dict."""
    fn, ln = split_name(row.get("Name", ""))
    phone  = clean_phone(row.get("Phone", ""))
    city   = fix_text(row.get("City", ""))
    state  = fix_text(row.get("State", ""))
    zipc   = clean_zip(row.get("Pincode", ""))
    event_time = parse_date_to_iso(row.get("Date", ""))

    # Value — use Total column
    try:
        value = float(str(row.get("Total", 0) or 0))
    except Exception:
        value = 0.0

    # Only include non-empty ASCII fields in user_data
    user_data = {}
    if phone and len(phone) == 10:
        user_data["ph"] = [phone]
    if fn and is_ascii(fn):
        user_data["fn"] = [fn]
    if ln and is_ascii(ln):
        user_data["ln"] = [ln]
    if city and is_ascii(city):
        user_data["ct"] = [city]
    if state and is_ascii(state):
        user_data["st"] = [state]
    if zipc:
        user_data["zp"] = [zipc]
    user_data["country"] = ["in"]

    return {
        "event_name":  "Purchase",
        "event_time":  event_time,
        "event_id":    f"OBX_{order_num}",   # unique dedup key
        "currency":    "INR",
        "value":       value,
        "user_data":   user_data,
    }

def format_events_batch(rows: list, uploaded: set) -> tuple[list, list]:
    """
    Returns (events_to_upload, order_nums_to_mark).
    Skips already-uploaded orders.
    """
    events   = []
    new_nums = []

    for row in rows:
        num = str(row.get("Order#", "")).strip()
        if not num or num in uploaded:
            continue

        # Skip cancelled orders
        status = str(row.get("Status", "")).lower()
        if status in ("cancelled", "rto", "returned"):
            continue

        evt = row_to_event(row, num)
        events.append(evt)
        new_nums.append(num)

    return events, new_nums

# ─── META API UPLOAD ──────────────────────────────────────────────────────────

def upload_to_meta(events: list) -> dict:
    """
    Upload events to Meta Offline Events API.
    Batches in groups of 50 (Meta limit).
    Returns summary dict.
    """
    access_token = os.getenv("META_ACCESS_TOKEN")
    dataset_id   = os.getenv("META_DATASET_ID")

    if not access_token or not dataset_id:
        return {"error": "META_ACCESS_TOKEN or META_DATASET_ID not set in Railway env vars"}

    url     = f"https://graph.facebook.com/{META_API_VERSION}/{dataset_id}/events"
    total   = len(events)
    success = 0
    errors  = []

    BATCH = 50
    for i in range(0, total, BATCH):
        chunk = events[i:i + BATCH]
        payload = {
            "data":         json.dumps(chunk),
            "access_token": access_token,
        }
        try:
            r = requests.post(url, data=payload, timeout=30)
            resp = r.json()
            if r.status_code == 200 and "events_received" in resp:
                success += resp.get("events_received", 0)
                log.info(f"Batch {i//BATCH + 1}: {resp.get('events_received')} received")
            else:
                err_msg = resp.get("error", {}).get("message", str(resp))
                errors.append(err_msg)
                log.error(f"Batch {i//BATCH + 1} error: {err_msg}")
        except Exception as e:
            errors.append(str(e))
            log.error(f"Batch {i//BATCH + 1} exception: {e}")

    return {
        "total":   total,
        "success": success,
        "errors":  errors,
    }

# ─── MAIN ENTRY ───────────────────────────────────────────────────────────────

def run_upload() -> str:
    """
    Full pipeline: read sheet → filter new → upload → mark done.
    Returns a summary string (for Telegram message).
    """
    log.info("Meta upload: starting")

    rows     = read_sheet_orders()
    if not rows:
        return "❌ Could not read Google Sheet"

    uploaded = load_uploaded()
    events, new_nums = format_events_batch(rows, uploaded)

    if not events:
        return f"✅ No new orders to upload ({len(uploaded)} already uploaded)"

    log.info(f"Uploading {len(events)} new events to Meta")
    result = upload_to_meta(events)

    if "error" in result:
        return f"❌ Meta API error: {result['error']}"

    # Mark as uploaded only if API accepted them
    if result["success"] > 0:
        uploaded.update(new_nums[:result["success"]])
        save_uploaded(uploaded)

    lines = [
        f"📊 *Meta Upload Done — {datetime.now(IST).strftime('%d %b %Y %H:%M')} IST*",
        f"",
        f"📦 New orders: {result['total']}",
        f"✅ Uploaded: {result['success']}",
        f"🗂 Total ever uploaded: {len(uploaded)}",
    ]
    if result["errors"]:
        lines.append(f"⚠️ Errors: {len(result['errors'])}")
        for e in result["errors"][:3]:
            lines.append(f"  • {e[:80]}")

    return "\n".join(lines)
