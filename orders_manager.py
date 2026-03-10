"""
orders_manager.py — Oneboxx Ship Bot (Final v3)
Changes:
  - calc_cod uses order total (not hardcoded 3300)
  - get_label_queue: advance_paid > 0 only, permanent flag
  - get_all_vendors / get_label_queue_by_vendor: uses pickup_location for SR orders
"""

import os
import json
import logging
from datetime import datetime, date, timedelta

log = logging.getLogger("orders_manager")
ORDERS_FILE = "orders.json"
ADS_FILE    = "ads_data.json"
COUNT_FILE  = "order_count.json"

# ─── LOCAL DB ─────────────────────────────

def load_orders():
    if not os.path.exists(ORDERS_FILE):
        return []
    try:
        with open(ORDERS_FILE) as f:
            return json.load(f)
    except Exception:
        return []

def save_orders(orders):
    with open(ORDERS_FILE, "w") as f:
        json.dump(orders, f, indent=2, default=str)

def next_order_number():
    data = {}
    if os.path.exists(COUNT_FILE):
        with open(COUNT_FILE) as f:
            data = json.load(f)
    n = data.get("count", 0) + 1
    data["count"] = n
    with open(COUNT_FILE, "w") as f:
        json.dump(data, f)
    return n

def save_order(order):
    orders = load_orders()
    orders.append(order)
    save_orders(orders)
    _sync_to_sheets(order)

def find_by_phone(phone):
    phone = str(phone).strip()
    matches = [o for o in load_orders() if str(o.get("phone","")).strip() == phone]
    return matches[-1] if matches else None

def find_by_awb(awb):
    awb = str(awb).strip().upper()
    for o in reversed(load_orders()):
        if str((o.get("shiprocket") or {}).get("awb","")).upper() == awb:
            return o
        if str((o.get("manual") or {}).get("awb","")).upper() == awb:
            return o
    return None

def update_order(phone, **fields):
    orders = load_orders()
    for o in reversed(orders):
        if str(o.get("phone","")).strip() == str(phone).strip():
            o.update(fields)
            save_orders(orders)
            _sync_update(o)
            return o
    return None

def update_order_by_id(order_id, **fields):
    orders = load_orders()
    for o in orders:
        if o.get("order_id") == order_id:
            o.update(fields)
            save_orders(orders)
            _sync_update(o)
            return o
    return None

# ─── PAYMENT ──────────────────────────────

def calc_cod(order, courier_paid, advance_paid):
    """Use order total — not hardcoded 3300."""
    total = order.get("total", 3300)
    return int(total) - int(courier_paid or 0) - int(advance_paid or 0)

def payment_status(order):
    c = order.get("courier_paid") or 0
    a = order.get("advance_paid")
    if a is None and c > 0:
        return "courier_only"
    if a is None:
        return "nothing"
    if a == 0:
        return "full_cod"
    return "advance_paid"

# ─── LABEL QUEUE ──────────────────────────

def get_label_queue():
    """Advance paid > 0 only. Permanent flag — never resets."""
    result = []
    for o in load_orders():
        a = o.get("advance_paid")
        if a is None or int(a) <= 0:
            continue
        if o.get("label_downloaded") is True:
            continue
        result.append(o)
    return result

def get_label_queue_by_vendor(vendor):
    vendor_lower = vendor.lower()
    result = []
    for o in get_label_queue():
        vm = o.get("manual") or {}
        v  = (vm.get("vendor") or o.get("pickup_location") or "shiprocket").lower()
        if v == vendor_lower:
            result.append(o)
    return result

def get_all_vendors():
    vendors = set()
    for o in load_orders():
        vm = o.get("manual") or {}
        vendors.add(vm.get("vendor") or o.get("pickup_location") or "Shiprocket")
    return sorted(vendors)

def mark_label_downloaded(order_id):
    orders = load_orders()
    for o in orders:
        if o.get("order_id") == order_id:
            o["label_downloaded"]      = True
            o["label_downloaded_date"] = date.today().isoformat()
            save_orders(orders)
            _sync_update(o)
            return True
    return False

# ─── PAYMENT REPORT ───────────────────────

def get_payment_report():
    pending = []; advance = []; full_cod = []; nothing = []
    for o in load_orders():
        s = payment_status(o)
        if s == "courier_only":  pending.append(o)
        elif s == "advance_paid": advance.append(o)
        elif s == "full_cod":     full_cod.append(o)
        else:                     nothing.append(o)
    return {"pending": pending, "advance": advance,
            "full_cod": full_cod, "nothing": nothing}

# ─── CREATIVE ─────────────────────────────

def get_missing_creative(period="today"):
    today     = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    orders    = load_orders()
    if period == "today":
        orders = [o for o in orders if o.get("created_at","").startswith(today)]
    elif period == "yesterday":
        orders = [o for o in orders if o.get("created_at","").startswith(yesterday)]
    return [o for o in orders if not o.get("creative")]

def set_creative(phone, creative):
    return update_order(phone, creative=creative.upper())

def get_creative_stats(days=7):
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    orders = [o for o in load_orders() if o.get("created_at","") >= cutoff]
    stats  = {}
    for o in orders:
        c = o.get("creative") or "—"
        if c not in stats:
            stats[c] = {"orders": 0, "revenue": 0}
        stats[c]["orders"]  += 1
        stats[c]["revenue"] += o.get("total", 0)
    return dict(sorted(stats.items(), key=lambda x: x[1]["orders"], reverse=True))

# ─── STATS ────────────────────────────────

def get_today_stats():
    today  = date.today().isoformat()
    orders = [o for o in load_orders() if o.get("created_at","").startswith(today)]
    return {
        "total":             len(orders),
        "advance_paid":      len([o for o in orders if (o.get("advance_paid") or -1) > 0]),
        "full_cod":          len([o for o in orders if o.get("advance_paid") == 0]),
        "courier_only":      len([o for o in orders if payment_status(o) == "courier_only"]),
        "nothing":           len([o for o in orders if payment_status(o) == "nothing"]),
        "revenue":           sum(o.get("total",0) for o in orders),
        "advance_collected": sum((o.get("advance_paid") or 0) for o in orders),
        "courier_collected": sum((o.get("courier_paid") or 0) for o in orders),
    }

def get_week_stats():
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    orders = [o for o in load_orders() if o.get("created_at","") >= cutoff]
    paid   = [o for o in orders if payment_status(o) in ("advance_paid","full_cod")]
    rate   = round(len(paid)/len(orders)*100,1) if orders else 0
    return {
        "total":     len(orders),
        "revenue":   sum(o.get("total",0) for o in orders),
        "conv_rate": rate,
    }

# ─── ADS ──────────────────────────────────

def load_ads():
    if not os.path.exists(ADS_FILE):
        return {}
    with open(ADS_FILE) as f:
        return json.load(f)

def save_ads(data):
    with open(ADS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def log_adsspend(total=None, breakdown=None):
    ads   = load_ads()
    today = date.today().isoformat()
    ads.setdefault(today, {})
    if total:
        ads[today]["total_spend"] = total
    if breakdown:
        for k, v in breakdown.items():
            ads[today][f"spend_{k}"] = v
        ads[today]["total_spend"] = sum(
            v for k, v in ads[today].items() if k.startswith("spend_"))
    save_ads(ads)
    return ads[today]

def log_campaign_orders(breakdown):
    ads   = load_ads()
    today = date.today().isoformat()
    ads.setdefault(today, {})
    total = 0
    for k, v in breakdown.items():
        ads[today][f"orders_{k}"] = v
        total += v
    ads[today]["total_campaign_orders"] = total
    spend = ads[today].get("total_spend", 0)
    if spend and total:
        ads[today]["cpo"] = round(spend / total)
    save_ads(ads)
    return ads[today]

def get_today_ads():
    return load_ads().get(date.today().isoformat(), {})

# ─── FORMAT ───────────────────────────────

def format_order(order):
    sr     = order.get("shiprocket") or {}
    vm     = order.get("manual") or {}
    c      = order.get("courier_paid") or 0
    a      = order.get("advance_paid")
    cod    = order.get("cod_amount", 0)
    s      = payment_status(order)
    vendor = vm.get("vendor") or order.get("pickup_location") or "Shiprocket"

    if s == "nothing":
        pay = "❌ Nothing paid"
    elif s == "courier_only":
        pay = f"Courier ₹{c} paid | Advance ⏳ pending"
    elif s == "full_cod":
        pay = f"Courier ₹{c} | Full COD | Delivery ₹{cod}"
    else:
        pay = f"Courier ₹{c} | Advance ₹{a} | Delivery ₹{cod}"

    lines = [
        "————————————————————",
        f"📦 ORDER #{order.get('order_number')}",
        "————————————————————",
        f"📅 {order.get('created_at','')[:16].replace('T',' ')}",
        "",
        f"👤 {order.get('customer_name','')}",
        f"📞 {order.get('phone','')}",
        f"📍 {order.get('city','')}, {order.get('state','')}, {order.get('pincode','')}",
        "",
        f"📦 {order.get('product','')} | ₹{order.get('total',0):,}",
        f"🎨 {order.get('creative','—')}",
        f"🏪 {vendor}",
        "",
        f"💰 {pay}",
    ]
    if sr.get("awb"):
        lines += ["", f"🚚 {sr.get('courier','')} | {sr.get('awb','')}", f"🔗 {sr.get('tracking','')}"]
    if vm.get("awb"):
        lines += ["", f"🏪 {vm.get('vendor','')} | {vm.get('courier','')} | {vm.get('awb','')}"]
    if order.get("label_downloaded_date"):
        lines.append(f"📥 Label: {order['label_downloaded_date']}")
    lines.append("————————————————————")
    return "\n".join(lines)

# ─── GOOGLE SHEETS ────────────────────────

_gc = None

def get_sheets_client():
    global _gc
    if _gc:
        return _gc
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        raw = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not raw:
            return None
        creds = Credentials.from_service_account_info(
            json.loads(raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
        _gc = gspread.authorize(creds)
        return _gc
    except Exception as e:
        log.error(f"Sheets: {e}")
        return None

def _order_to_row(o):
    sr     = o.get("shiprocket") or {}
    vm     = o.get("manual") or {}
    vendor = vm.get("vendor") or o.get("pickup_location") or "Shiprocket"
    return [
        o.get("order_number",""),
        o.get("created_at","")[:16].replace("T"," "),
        o.get("customer_name",""),
        o.get("phone",""),
        o.get("city",""),
        o.get("state",""),
        o.get("pincode",""),
        o.get("product",""),
        o.get("creative",""),
        o.get("total",0),
        o.get("courier_paid",0) or 0,
        o.get("advance_paid",""),
        o.get("cod_amount",0),
        payment_status(o),
        vendor,
        vm.get("courier","") or sr.get("courier",""),
        vm.get("awb","") or sr.get("awb",""),
        sr.get("tracking",""),
        o.get("status","active"),
        o.get("pickup_location",""),
        o.get("label_downloaded_date",""),
    ]

SHEET_HEADERS = [
    "Order#","Date","Name","Phone","City","State","Pincode",
    "Product","Creative","Total","Courier Paid","Advance",
    "COD","Payment Status","Vendor","Courier","AWB",
    "Tracking","Status","Pickup","Label Downloaded"
]

def _sync_to_sheets(order):
    try:
        gc  = get_sheets_client()
        sid = os.getenv("GOOGLE_SHEET_ID")
        if not gc or not sid:
            return
        sh = gc.open_by_key(sid)
        try:
            ws = sh.worksheet("Orders")
        except Exception:
            ws = sh.add_worksheet("Orders", rows=2000, cols=25)
            ws.append_row(SHEET_HEADERS)
        ws.append_row(_order_to_row(order))
    except Exception as e:
        log.error(f"Sheet sync: {e}")

def _sync_update(order):
    try:
        gc  = get_sheets_client()
        sid = os.getenv("GOOGLE_SHEET_ID")
        if not gc or not sid:
            return
        sh   = gc.open_by_key(sid)
        ws   = sh.worksheet("Orders")
        cell = ws.find(str(order.get("order_number","")))
        if not cell:
            _sync_to_sheets(order); return
        row = _order_to_row(order)
        for i, v in enumerate(row, 1):
            ws.update_cell(cell.row, i, v)
    except Exception as e:
        log.error(f"Sheet update: {e}")