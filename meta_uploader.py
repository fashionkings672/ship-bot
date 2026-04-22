"""
meta_uploader.py — Oneboxx Meta Offline Events Uploader
Auto-creates 'Events' tab, writes clean formatted data,
and uploads to Facebook Offline Events API.
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

PINCODE_STATE = {
    "560":"karnataka","577":"karnataka","576":"karnataka","563":"karnataka",
    "561":"karnataka","580":"karnataka","590":"karnataka","581":"karnataka",
    "582":"karnataka","583":"karnataka","584":"karnataka","585":"karnataka",
    "518":"telangana","500":"telangana","501":"telangana","502":"telangana",
    "600":"tamil nadu","601":"tamil nadu","602":"tamil nadu","603":"tamil nadu",
    "604":"tamil nadu","605":"tamil nadu","606":"tamil nadu","607":"tamil nadu",
    "608":"tamil nadu","625":"tamil nadu","626":"tamil nadu","627":"tamil nadu",
    "682":"kerala","683":"kerala","685":"kerala","690":"kerala","691":"kerala",
    "400":"maharashtra","410":"maharashtra","411":"maharashtra","412":"maharashtra",
    "110":"delhi","120":"haryana","201":"uttar pradesh","226":"uttar pradesh",
}

def is_ascii(s: str) -> bool:
    try:
        return all(ord(c) < 128 for c in str(s))
    except Exception:
        return False

def clean_phone(p) -> str:
    """Clean phone to exactly 10 digits."""
    p = re.sub(r"\D", "", str(p).replace(".0", "").strip())
    return p[-10:] if len(p) >= 10 else p

def clean_zip(z) -> str:
    """Clean pincode to 6 digits."""
    try:
        return str(int(float(z))).zfill(6)
    except Exception:
        return ""

def guess_state(pincode: str) -> str:
    """Auto-detect state from first 3 digits of pincode."""
    return PINCODE_STATE.get(str(pincode).strip()[:3], "karnataka")

def parse_date_to_iso(val) -> str:
    """Parse various date formats to UTC ISO string."""
    try:
        if isinstance(val, datetime):
            actual = val
        elif isinstance(val, str):
            val = val.strip()
            if not val or val.lower() in ("nan", "none"):
                actual = datetime.now()
            elif "T" in val:
                actual = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return actual.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            elif "/" in val:
                actual = datetime.strptime(val, "%m/%d/%Y").replace(hour=12)
            elif "-" in val:
                actual = datetime.strptime(val.split(" ")[0], "%d-%m-%Y").replace(hour=12)
            else:
                actual = datetime.now()
        else:
            actual = datetime.now()

        dt_ist = IST.localize(actual) if actual.tzinfo is None else actual
        return dt_ist.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        log.warning(f"Date parse failed: {e}")
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_date_to_str(val) -> str:
    """Parse date+time to YYYY-MM-DD HH:MM format for Events sheet."""
    fmt = "%Y-%m-%d %H:%M"
    try:
        if isinstance(val, datetime):
            return val.strftime(fmt)
        elif isinstance(val, str):
            val = val.strip()
            if not val or val.lower() in ("nan", "none"):
                return datetime.now(IST).strftime(fmt)
            elif "T" in val:
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return dt.astimezone(IST).strftime(fmt)
            elif "/" in val:
                try:
                    return datetime.strptime(val, "%m/%d/%Y %H:%M:%S").strftime(fmt)
                except Exception:
                    return datetime.strptime(val, "%m/%d/%Y").replace(
                        hour=datetime.now(IST).hour,
                        minute=datetime.now(IST).minute
                    ).strftime(fmt)
            elif "-" in val:
                try:
                    return datetime.strptime(val, "%d-%m-%Y %H:%M:%S").strftime(fmt)
                except Exception:
                    try:
                        return datetime.strptime(val, "%Y-%m-%d %H:%M:%S").strftime(fmt)
                    except Exception:
                        try:
                            return datetime.strptime(val.split(" ")[0], "%d-%m-%Y").replace(
                                hour=datetime.now(IST).hour,
                                minute=datetime.now(IST).minute
                            ).strftime(fmt)
                        except Exception:
                            return datetime.strptime(val.split(" ")[0], "%Y-%m-%d").replace(
                                hour=datetime.now(IST).hour,
                                minute=datetime.now(IST).minute
                            ).strftime(fmt)
            else:
                return datetime.now(IST).strftime(fmt)
        else:
            return datetime.now(IST).strftime(fmt)
    except Exception:
        return datetime.now(IST).strftime(fmt)

# ─── GOOGLE SHEET CLIENT ──────────────────────────────────────────────────────

def get_sheet():
    """Get Google Sheet object."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        raw = os.getenv("GOOGLE_CREDENTIALS_JSON")
        sid = os.getenv("GOOGLE_SHEET_ID")
        if not raw or not sid:
            log.error("Missing GOOGLE_CREDENTIALS_JSON or GOOGLE_SHEET_ID")
            return None

        creds = Credentials.from_service_account_info(
            json.loads(raw),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        gc = gspread.authorize(creds)
        return gc.open_by_key(sid)

    except Exception as e:
        log.error(f"Google auth error: {e}", exc_info=True)
        return None

# ─── EVENTS TAB MANAGEMENT ────────────────────────────────────────────────────

EVENTS_HEADERS = [
    "order_id", "event_name", "event_time", "value", "currency",
    "fn", "ln", "phone", "ct", "st", "zip", "country",
]

def ensure_events_tab(sh):
    """
    Create Events tab if it doesn't exist.
    Set headers and formatting.
    Returns worksheet object.
    """
    try:
        ws = sh.worksheet("Events")
    except Exception:
        # Tab doesn't exist — create it
        ws = sh.add_worksheet(title="Events", rows=1000, cols=12)
        log.info("Created 'Events' tab")

    # Check if headers already set
    try:
        first_cell = ws.cell(1, 1).value
    except Exception:
        first_cell = None

    if not first_cell or first_cell != "order_id":
        # Write headers
        ws.update('A1', [EVENTS_HEADERS])

        # Style header row
        ws.format('A1:L1', {
            "backgroundColor": {"red": 0.1, "green": 0.3, "blue": 0.7},
            "textFormat": {
                "bold": True,
                "foregroundColor": {"red": 1, "green": 1, "blue": 1}
            },
            "horizontalAlignment": "CENTER",
        })

        # Format phone column (H) as TEXT to prevent scientific notation
        ws.format('H2:H1000', {
            "numberFormat": {"type": "TEXT"}
        })

        log.info("Events headers written")

    return ws

# ─── FORMAT ORDER FOR EVENTS SHEET ────────────────────────────────────────────

def order_to_event_row(order: dict) -> list:
    """
    Convert an order dict to a clean Events row list.
    Phone stored as string (not number) to avoid 9.35E+09.
    """
    name = order.get("customer_name", "")
    parts = name.strip().split()
    fn = parts[0] if parts else ""
    ln = " ".join(parts[1:]) if len(parts) > 1 else ""

    phone   = clean_phone(order.get("phone", ""))
    city    = str(order.get("city", "")).strip().lower()
    state   = str(order.get("state", "")).strip().lower()
    pincode = clean_zip(order.get("pincode", ""))
    order_num = str(order.get("order_number", ""))

    if not state:
        state = guess_state(pincode)

    try:
        value = int(float(order.get("cod_amount", 0) or order.get("total", 0) or 0))
    except Exception:
        value = 0

    # Get date string
    created = order.get("created_at", "")
    date_str = parse_date_to_str(created)

    return [
        f"OBX_{order_num}",                              # order_id
        "Purchase",                                       # event_name
        date_str,                                         # event_time
        value,                                            # value
        "INR",                                            # currency
        fn.lower() if fn and is_ascii(fn) else "",        # fn
        ln.lower() if ln and is_ascii(ln) else "",        # ln
        phone,                                            # phone (string!)
        city,                                             # ct
        state,                                            # st
        pincode,                                          # zip
        "in",                                             # country
    ]

# ─── WRITE SINGLE ORDER TO EVENTS TAB ─────────────────────────────────────────

def write_event_to_sheet(order: dict) -> bool:
    """
    Write a single order to Events tab.
    Called after each order creation by bot_enhanced.py.
    """
    try:
        sh = get_sheet()
        if not sh:
            log.error("No Google Sheet connection")
            return False

        ws = ensure_events_tab(sh)

        # Check for duplicate
        order_num = str(order.get("order_number", ""))
        event_id = f"OBX_{order_num}"

        try:
            existing_ids = ws.col_values(1)  # column A
        except Exception:
            existing_ids = []

        if event_id in existing_ids:
            log.info(f"Event {event_id} already in Events tab, skipping")
            return True

        # Format the row
        row = order_to_event_row(order)

        # Phone as string with prefix to force text
        phone_str = str(row[7])
        row[7] = phone_str

        # Append row using RAW so phone stays as text
        ws.append_rows([row], value_input_option="RAW")

        # Get the new row number and force phone as text
        new_row_num = len(existing_ids) + 1
        ws.update(f'H{new_row_num}', f'"{phone_str}"')

        log.info(f"Wrote {event_id} to Events tab (row {new_row_num})")
        return True

    except Exception as e:
        log.error(f"Write event error: {e}", exc_info=True)
        return False

# ─── META API UPLOAD ──────────────────────────────────────────────────────────

def order_to_meta_event(order: dict) -> dict:
    """Convert an order dict to Meta offline event format."""
    name = order.get("customer_name", "")
    parts = name.strip().split()
    fn = parts[0] if parts else ""
    ln = " ".join(parts[1:]) if len(parts) > 1 else ""

    phone   = clean_phone(order.get("phone", ""))
    city    = str(order.get("city", "")).strip().lower()
    state   = str(order.get("state", "")).strip().lower()
    pincode = clean_zip(order.get("pincode", ""))
    order_num = str(order.get("order_number", ""))
    created = order.get("created_at", "")

    if not state:
        state = guess_state(pincode)

    try:
        value = int(float(order.get("cod_amount", 0) or order.get("total", 0) or 0))
    except Exception:
        value = 0

    event_time = parse_date_to_iso(created)

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
    if pincode:
        user_data["zp"] = [pincode]
    user_data["country"] = ["in"]

    return {
        "event_name": "Purchase",
        "event_time": event_time,
        "event_id":   f"OBX_{order_num}",
        "currency":   "INR",
        "value":      value,
        "user_data":  user_data,
    }

def upload_single_to_meta(order: dict) -> str:
    """Upload a single order to Meta API. Returns status message."""
    access_token = os.getenv("META_ACCESS_TOKEN")
    dataset_id   = os.getenv("META_DATASET_ID")

    if not access_token or not dataset_id:
        return "⚠️ Meta env vars not set"

    event = order_to_meta_event(order)
    url = f"https://graph.facebook.com/{META_API_VERSION}/{dataset_id}/events"

    payload = {
        "data":         json.dumps([event]),
        "access_token": access_token,
    }

    try:
        r = requests.post(url, data=payload, timeout=30)
        resp = r.json()
        if r.status_code == 200 and "events_received" in resp:
            count = resp.get("events_received", 0)
            log.info(f"Meta: {count} event uploaded for {event['event_id']}")
            return f"✅ Meta: {count} event uploaded"
        else:
            err = resp.get("error", {}).get("message", str(resp))
            log.error(f"Meta API error: {err}")
            return f"⚠️ Meta: {err[:100]}"
    except Exception as e:
        log.error(f"Meta upload exception: {e}")
        return f"❌ Meta upload failed"

def upload_batch_to_meta(events: list) -> dict:
    """Upload multiple events to Meta. Returns summary."""
    access_token = os.getenv("META_ACCESS_TOKEN")
    dataset_id   = os.getenv("META_DATASET_ID")

    if not access_token or not dataset_id:
        return {"error": "META_ACCESS_TOKEN or META_DATASET_ID not set", "total": len(events), "success": 0, "errors": ["env vars missing"]}

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
                batch_count = resp.get("events_received", 0)
                success += batch_count
                log.info(f"Meta batch {i//BATCH + 1}: {batch_count} received")
            else:
                err = resp.get("error", {}).get("message", str(resp))
                errors.append(err)
                log.error(f"Meta batch {i//BATCH + 1} error: {err}")
        except Exception as e:
            errors.append(str(e))
            log.error(f"Meta batch {i//BATCH + 1} exception: {e}")

    return {"total": total, "success": success, "errors": errors}

# ─── SINGLE ORDER PIPELINE ────────────────────────────────────────────────────

def process_new_order(order: dict) -> str:
    """
    Called right after a new order is created in the bot.
    1. Writes clean row to Events tab in Google Sheet
    2. Uploads to Meta Offline Events Dataset
    Returns a short status message for Telegram confirmation.
    """
    order_num = order.get("order_number", "???")
    name = order.get("customer_name", "")
    phone = order.get("phone", "")

    lines = [f"📦 #{order_num} {name} ({phone})"]

    # Step 1: Write to Events tab
    sheet_ok = write_event_to_sheet(order)
    if sheet_ok:
        lines.append("📋 Events tab: ✅")
    else:
        lines.append("📋 Events tab: ⚠️ skipped")

    # Step 2: Upload to Meta
    meta_msg = upload_single_to_meta(order)
    lines.append(f"📤 {meta_msg}")

    return "\n".join(lines)

# ─── FULL DAILY BACKUP ────────────────────────────────────────────────────────

def run_upload() -> str:
    """
    Full daily backup: read Orders tab → find any not in Events tab
    → write missing to Events tab → upload to Meta.
    This catches anything that was missed during the day.
    Runs automatically at 11 PM IST.
    """
    log.info("Daily backup upload: starting")
    uploaded = load_uploaded()

    try:
        sh = get_sheet()
        if not sh:
            return "❌ Could not connect to Google Sheet"

        # Read Orders tab
        try:
            orders_ws = sh.worksheet("Orders")
            orders_rows = orders_ws.get_all_records()
            log.info(f"Read {len(orders_rows)} rows from Orders tab")
        except Exception as e:
            return f"❌ Could not read Orders tab: {e}"

        # Get or create Events tab
        events_ws = ensure_events_tab(sh)

        # Get existing event IDs from Events tab (column A)
        try:
            existing_ids = set(events_ws.col_values(1))
            existing_ids.discard("")
        except Exception:
            existing_ids = set()

        log.info(f"Events tab has {len(existing_ids)} existing entries")

        # Find orders not yet in Events tab
        new_orders = []
        for row in orders_rows:
            num = str(row.get("Order#", "")).strip()
            if not num:
                continue
            event_id = f"OBX_{num}"
            if event_id in existing_ids:
                continue

            status = str(row.get("Status", "")).lower()
            if status in ("cancelled", "rto", "returned"):
                continue

            # Convert sheet row to order dict format
            order = {
                "order_number":  num,
                "customer_name": row.get("Name", ""),
                "phone":         str(row.get("Phone", "")),
                "city":          row.get("City", ""),
                "state":         row.get("State", ""),
                "pincode":       str(row.get("Pincode", "")),
                "product":       row.get("Product", ""),
                "total":         row.get("Total", 0),
                "cod_amount":    row.get("COD", row.get("Total", 0)),
                "created_at":    str(row.get("Date", "")),
            }
            new_orders.append(order)

        if not new_orders:
            return f"✅ No new orders to upload ({len(uploaded)} already tracked)"

        log.info(f"Backup: {len(new_orders)} orders to process")

        # Write to Events tab and collect Meta events
        meta_events = []
        written = 0
        for order in new_orders:
            if write_event_to_sheet(order):
                written += 1
            meta_events.append(order_to_meta_event(order))

        # Batch upload to Meta
        result = upload_batch_to_meta(meta_events)

        # Mark as uploaded
        if result.get("success", 0) > 0:
            for order in new_orders[:result["success"]]:
                uploaded.add(str(order.get("order_number", "")))
            save_uploaded(uploaded)

        # Build summary
        now_str = datetime.now(IST).strftime("%d %b %Y %H:%M")
        lines = [
            f"📊 *Daily Meta Backup — {now_str} IST*",
            f"",
            f"📋 Events written: {written}",
            f"📤 Meta uploaded: {result.get('success', 0)}/{result.get('total', 0)}",
            f"🗂 Total tracked: {len(uploaded)}",
        ]
        if result.get("errors"):
            lines.append(f"⚠️ Errors: {len(result['errors'])}")
            for e in result["errors"][:3]:
                lines.append(f"  • {e[:80]}")

        return "\n".join(lines)

    except Exception as e:
        log.error(f"Daily backup error: {e}", exc_info=True)
        return f"❌ Backup failed: {e}"
