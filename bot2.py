"""
bot2.py — Backbenchers Hub Ship Bot
Separate bot for Backbenchers Hub Shiprocket account.
Uses BOT_TOKEN_3, SR_EMAIL_BB, SR_PASS_BB, SHEET_ID_BB
Separate orders file: orders_bb.json
"""
import os, re, json, uuid, time, logging, asyncio, aiohttp
import requests
import pytz
from datetime import datetime, date, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, CallbackQueryHandler, filters
import openai
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── CONFIG ───────────────────────────────
BOT_TOKEN        = os.getenv("BOT_TOKEN_3")
SHIPROCKET_EMAIL = os.getenv("SR_EMAIL_BB")
SHIPROCKET_PASS  = os.getenv("SR_PASS_BB")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")
GOOGLE_SHEET_ID  = os.getenv("SHEET_ID_BB")

for k, v in [("BOT_TOKEN_3", BOT_TOKEN), ("SR_EMAIL_BB", SHIPROCKET_EMAIL), ("OPENAI", OPENAI_API_KEY)]:
    print(f"  BB {k}: {'OK' if v else 'MISSING'}")

if not BOT_TOKEN:        raise ValueError("BOT_TOKEN_3 not set")
if not SHIPROCKET_EMAIL: raise ValueError("SR_EMAIL_BB not set")
if not SHIPROCKET_PASS:  raise ValueError("SR_PASS_BB not set")
if not OPENAI_API_KEY:   raise ValueError("OPENAI_API_KEY not set")

openai.api_key = OPENAI_API_KEY
log = logging.getLogger("bot2")

COURIER_CHARGES   = 300
ORDERS_FILE       = "orders_bb.json"
COUNT_FILE        = "order_count_bb.json"
PRODUCTS_FILE     = "products_bb.json"
COURIER_PRIORITY_FILE = "courier_priority.json"

# ─── SHIPROCKET ───────────────────────────
SR_BASE    = "https://apiv2.shiprocket.in/v1/external"
session    = requests.Session()
_token     = None
_token_exp = 0
_pickups   = {}

def get_token(force=False):
    global _token, _token_exp
    if not force and _token and time.time() < _token_exp:
        return _token
    r = session.post(f"{SR_BASE}/auth/login",
                     json={"email": SHIPROCKET_EMAIL, "password": SHIPROCKET_PASS}, timeout=60)
    data = r.json()
    if "token" not in data:
        raise Exception(f"SR login failed: {data}")
    _token = data["token"]
    _token_exp = time.time() + 23 * 3600
    session.headers.update({"Authorization": f"Bearer {_token}"})
    return _token

def ensure_token():
    try: get_token()
    except: get_token(force=True)

def refresh_pickups():
    global _pickups
    ensure_token()
    r = session.get(f"{SR_BASE}/settings/company/pickup", timeout=60)
    lst = r.json().get("data", {}).get("shipping_address", [])
    _pickups = {p["pickup_location"].lower(): p for p in lst if p.get("pickup_location")}
    log.info(f"BB Pickups: {list(_pickups.keys())}")

def resolve_pickup(name):
    if not name: return next(iter(_pickups.values()), None)
    key = re.sub(r"\W", "", str(name).lower())
    for k, v in _pickups.items():
        if key in re.sub(r"\W", "", k) or re.sub(r"\W", "", k) in key:
            return v
    return next(iter(_pickups.values()), None)

def sr_post(ep, payload):
    ensure_token()
    r = session.post(f"{SR_BASE}{ep}", json=payload, timeout=45)
    return r.json() if r.content else {}

def sr_get(ep, params=None):
    ensure_token()
    r = session.get(f"{SR_BASE}{ep}", params=params, timeout=45)
    return r.json() if r.content else {}

def get_couriers(pp, dp, weight, cod):
    try:
        r = sr_get("/courier/serviceability/", {
            "pickup_postcode": pp,
            "delivery_postcode": dp,
            "cod": int(bool(cod)),
            "weight": weight
        })
        all_c = r.get("data", {}).get("available_courier_companies", []) or []
        filtered = []
        for c in all_c:
            charge_w = float(c.get("charge_weight") or c.get("min_weight") or weight)
            if charge_w <= weight:
                filtered.append(c)
            else:
                log.info(f"Skipped {c.get('courier_name', '')} — slab {charge_w}kg > {weight}kg")
        return filtered
    except:
        return []

def is_surface(c):
    mode = str(c.get("mode", "")).lower()
    name = str(c.get("courier_name", "")).lower()
    if "air" in mode or "air" in name:
        return False
    return "surface" in mode or "surface" in name

def priority_rank(name):
    if not os.path.exists(COURIER_PRIORITY_FILE): return 999
    prio = json.load(open(COURIER_PRIORITY_FILE))
    n = name.lower().strip()
    for k, v in prio.items():
        if k.lower().strip() == n: return v
    for k, v in prio.items():
        if k.lower().strip() in n: return v
    return 999

def courier_auto_rank(c):
    name = str(c.get("courier_name", ""))
    n = name.lower()
    if "air" in n: return 9999
    pr = priority_rank(name)
    if pr != 999: return pr
    if "bluedart" in n or "blue dart" in n: return 10
    if "delhivery" in n: return 20
    if "ekart" in n or "e-kart" in n: return 30
    if "dtdc" in n: return 40
    return 99

def assign_awb(shipment_id, courier_id=None):
    payload = {"shipment_id": shipment_id}
    if courier_id: payload["courier_id"] = courier_id
    r = sr_post("/courier/assign/awb", payload)
    log.info(f"assign_awb response: {r}")
    if r.get("awb_assign_status") == 1:
        return r["response"]["data"]["awb_code"]
    err = str(r).lower()
    if any(w in err for w in ["wallet", "balance", "recharge", "insufficient", "credit"]):
        return "WALLET_LOW"
    return None

def select_courier(couriers, shipment_id):
    surface = [c for c in couriers if is_surface(c)]
    if not surface: surface = couriers
    ranked = sorted(surface, key=courier_auto_rank)
    awb = None
    chosen = None
    for c in ranked:
        rank = courier_auto_rank(c)
        if rank >= 99: continue
        cid = c.get("courier_company_id") or c.get("courier_id")
        result = assign_awb(shipment_id, cid)
        if result == "WALLET_LOW":
            return "WALLET_LOW", None, False, surface
        if result:
            awb = result
            chosen = c
            break
    if awb:
        return awb, chosen, False, surface
    return None, None, True, [c for c in surface if courier_auto_rank(c) < 9999]

def generate_label(shipment_id):
    try:
        r = sr_post("/courier/generate/label", {"shipment_id": [shipment_id]})
        if r.get("label_created") == 1: return r.get("label_url")
    except: pass
    return None

def schedule_pickup(shipment_ids):
    try:
        r = sr_post("/courier/generate/pickup", {"shipment_id": shipment_ids})
        if r.get("pickup_scheduled") or r.get("status") == 1: return True, "✅ Pickup scheduled"
        return False, str(r)
    except Exception as e: return False, str(e)

def cancel_sr_order(sr_order_id):
    try:
        ensure_token()
        r = session.post(f"{SR_BASE}/orders/cancel", json={"ids": [str(sr_order_id)]}, timeout=30)
        resp = r.json()
        if r.status_code == 200 or "success" in str(resp).lower(): return True, "Cancelled"
        return False, str(resp)
    except Exception as e: return False, str(e)

# ─── LOCAL DB ─────────────────────────────
def load_orders():
    if not os.path.exists(ORDERS_FILE): return []
    try:
        with open(ORDERS_FILE) as f: return json.load(f)
    except: return []

def save_orders(orders):
    with open(ORDERS_FILE, "w") as f:
        json.dump(orders, f, indent=2, default=str)

def next_order_number():
    data = {}
    if os.path.exists(COUNT_FILE):
        with open(COUNT_FILE) as f: data = json.load(f)
    orders = load_orders()
    max_existing = max((int(o.get("order_number", 0)) for o in orders), default=0)
    current = max(data.get("count", 0), max_existing)
    n = current + 1
    data["count"] = n
    with open(COUNT_FILE, "w") as f: json.dump(data, f)
    return n

def save_order(order):
    orders = load_orders()
    orders.append(order)
    save_orders(orders)
    _sync_to_sheets(order)

def _norm_phone(phone):
    p = re.sub(r"[^\d]", "", str(phone).strip())
    if p.startswith("91") and len(p) == 12: p = p[2:]
    return p[-10:] if len(p) >= 10 else p

def find_by_phone(phone):
    phone = _norm_phone(phone)
    matches = [o for o in load_orders() if _norm_phone(o.get("phone", "")) == phone]
    return matches[-1] if matches else None

def find_by_awb(awb):
    awb = str(awb).strip().upper()
    for o in reversed(load_orders()):
        if str((o.get("shiprocket") or {}).get("awb", "")).upper() == awb: return o
        if str((o.get("manual") or {}).get("awb", "")).upper() == awb: return o
    return None

def update_order(phone, **fields):
    orders = load_orders()
    for o in reversed(orders):
        if _norm_phone(o.get("phone", "")) == _norm_phone(phone):
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

def payment_status(order):
    c = order.get("courier_paid") or 0
    a = order.get("advance_paid")
    if a is None and c > 0: return "courier_only"
    if a is None: return "nothing"
    if a == 0: return "full_cod"
    return "advance_paid"

def format_order(order):
    sr = order.get("shiprocket") or {}
    vm = order.get("manual") or {}
    c = order.get("courier_paid") or 0
    a = order.get("advance_paid")
    cod = order.get("cod_amount", 0)
    s = payment_status(order)
    vendor = vm.get("vendor") or order.get("pickup_location") or "Shiprocket"
    if s == "nothing": pay = "❌ Nothing paid"
    elif s == "courier_only": pay = f"Courier ₹{c} paid | Advance ⏳ pending"
    elif s == "full_cod": pay = f"Courier ₹{c} | Full COD | Delivery ₹{cod}"
    else: pay = f"Courier ₹{c} | Advance ₹{a} | Delivery ₹{cod}"
    lines = [
        "————————————————————",
        f"📦 BB ORDER #{order.get('order_number')}",
        "————————————————————",
        f"📅 {order.get('created_at', '')[:16].replace('T', ' ')}",
        f"👤 {order.get('customer_name', '')}",
        f"📞 {order.get('phone', '')}",
        f"📍 {order.get('city', '')}, {order.get('state', '')}, {order.get('pincode', '')}",
        f"📦 {order.get('product', '')} | ₹{order.get('total', 0):,}",
        f"🏪 {vendor}",
        f"💰 {pay}",
    ]
    if sr.get("awb"):
        lines += [f"🚚 {sr.get('courier', '')} | {sr.get('awb', '')}", f"🔗 {sr.get('tracking', '')}"]
    lines.append("————————————————————")
    return "\n".join(lines)

# ─── GOOGLE SHEETS ────────────────────────
_gc = None

def get_sheets_client():
    global _gc
    if _gc: return _gc
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        raw = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not raw: return None
        creds = Credentials.from_service_account_info(
            json.loads(raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"])
        _gc = gspread.authorize(creds)
        return _gc
    except Exception as e:
        log.error(f"Sheets: {e}")
        return None

SHEET_HEADERS = [
    "Order#", "Date", "Name", "Phone", "City", "State", "Pincode",
    "Product", "Total", "Courier Paid", "Advance", "COD",
    "Payment Status", "Vendor", "Courier", "AWB", "Tracking", "Status"
]

def _order_to_row(o):
    sr = o.get("shiprocket") or {}
    vm = o.get("manual") or {}
    vendor = vm.get("vendor") or o.get("pickup_location") or "Shiprocket"
    return [
        o.get("order_number", ""),
        o.get("created_at", "")[:16].replace("T", " "),
        o.get("customer_name", ""),
        o.get("phone", ""),
        o.get("city", ""),
        o.get("state", ""),
        o.get("pincode", ""),
        o.get("product", ""),
        o.get("total", 0),
        o.get("courier_paid", 0) or 0,
        o.get("advance_paid", ""),
        o.get("cod_amount", 0),
        payment_status(o),
        vendor,
        vm.get("courier", "") or sr.get("courier", ""),
        vm.get("awb", "") or sr.get("awb", ""),
        sr.get("tracking", ""),
        o.get("status", "active"),
    ]

def _sync_to_sheets(order):
    try:
        gc = get_sheets_client()
        sid = GOOGLE_SHEET_ID
        if not gc or not sid: return
        sh = gc.open_by_key(sid)
        try:
            ws = sh.worksheet("Orders")
        except:
            ws = sh.add_worksheet("Orders", rows=2000, cols=20)
            ws.append_row(SHEET_HEADERS)
        col_a = ws.col_values(1)
        order_num = str(order.get("order_number", ""))
        if order_num in col_a:
            row_idx = col_a.index(order_num) + 1
            ws.update(f"A{row_idx}", [_order_to_row(order)])
        else:
            ws.append_row(_order_to_row(order))
    except Exception as e:
        log.error(f"BB Sheet sync: {e}")

def _sync_update(order):
    try:
        gc = get_sheets_client()
        sid = GOOGLE_SHEET_ID
        if not gc or not sid: return
        sh = gc.open_by_key(sid)
        ws = sh.worksheet("Orders")
        col_a = ws.col_values(1)
        order_num = str(order.get("order_number", ""))
        if order_num in col_a:
            row_idx = col_a.index(order_num) + 1
            ws.update(f"A{row_idx}", [_order_to_row(order)])
        else:
            _sync_to_sheets(order)
    except Exception as e:
        log.error(f"BB Sheet update: {e}")

# ─── AI PARSER ────────────────────────────
def ai_parse(text):
    prompt = f"""Extract from this order text. Output EXACTLY this format:
Pickup: <pickup_location>
Product: <product_name>
Name: <full_name>
Address: <house_no + village/area/locality>
Address2: <landmark or NA>
City: <district or city name only>
State: <state>
Pincode: <6digit>
Phone: <10digit>
Alt_Phone: <10digit_or_NA>
Payment_Mode: <COD_or_PREPAID>
Amount: <number_only_or_MISSING>

Rules:
- Address = house/door number + street/village/locality. Keep all local identifiers exactly as written.
- Address2 = landmark only (near/opposite/behind). NA if none.
- City = district or city name ONLY. Never put village in City.
- State: derive from Pincode if not mentioned.
- Phone: exactly 10 digits, strip +91 prefix.
- Alt_Phone: second number if present, else NA.
- Payment_Mode: COD or PREPAID only.
- Amount: digits only, no ₹. MISSING if not found.

Text:
{text}"""
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )
    return resp.choices[0].message.content.strip()

def parse_fields(text):
    data = {}
    for line in text.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            data[k.strip().lower()] = v.strip()
    return data

# ─── KEYBOARDS ────────────────────────────
MAIN_KB = ReplyKeyboardMarkup([
    ["➕ Create Shipment", "🔍 Search Order"],
    ["📥 Download Labels", "📦 Products"],
], resize_keyboard=True)

def order_action_kb(order_id, phone):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("💰 Advance", callback_data=f"adv_start_{phone}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"action_cancel_{order_id}"),
    ]])

# ─── /start ───────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "🚀 *Backbenchers Hub Ship Bot*\n\n"
        "/report /uploadfb",
        parse_mode="Markdown", reply_markup=MAIN_KB)

# ─── COMMANDS ─────────────────────────────
async def cmd_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    today = date.today().isoformat()
    orders = load_orders()
    today_orders = [o for o in orders if o.get("created_at", "").startswith(today)]
    week_cut = (date.today() - timedelta(days=7)).isoformat()
    week_orders = [o for o in orders if o.get("created_at", "") >= week_cut]
    lines = [
        f"📊 *BB REPORT — {date.today()}*",
        f"📦 Today: {len(today_orders)}",
        f"📅 Week: {len(week_orders)}",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ─── MESSAGE HANDLER ──────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    ud = ctx.user_data
    state = ud.get("state")

    if text == "➕ Create Shipment":
        ud.clear(); ud["state"] = "create"
        await update.message.reply_text("Send order details:", reply_markup=MAIN_KB); return

    if text == "🔍 Search Order":
        ud.clear(); ud["state"] = "search"
        await update.message.reply_text("Enter phone or AWB:", reply_markup=MAIN_KB); return

    if text == "📥 Download Labels":
        ud.clear(); await show_label_menu(update, ctx); return

    if text == "📦 Products":
        ud.clear(); await show_products(update, ctx); return

    if state == "create":
        await do_create(update, ctx, text); return

    if state == "create_cod_missing":
        try:
            cod_amount = float(re.sub(r"[^\d.]", "", text))
            if cod_amount <= 0:
                await update.message.reply_text("❌ COD must be > 0. Enter again:"); return
        except:
            await update.message.reply_text("❌ Invalid. Number only:"); return
        ud["create_parsed"]["cod"] = str(int(cod_amount))
        ud["state"] = "create_creative"
        await update.message.reply_text(f"✅ COD: ₹{int(cod_amount):,}\n\nEnter creative code (or 'skip'):")
        return

    if state == "create_creative":
        ud["create_creative"] = "" if text.lower() == "skip" else text.upper()
        await do_create_shipment(update, ctx); return

    if state == "search":
        await do_search(update, ctx, text); return

    if state == "adv_custom":
        try: amt = int(text)
        except: await update.message.reply_text("Enter number only"); return
        await do_save_advance(update, ctx, amt); return

    if state == "manual_courier_pick":
        try:
            idx = int(text.strip()) - 1
            surface = ud.get("pending_surface_couriers", [])
            if 0 <= idx < len(surface):
                chosen = surface[idx]
                shipment_id = ud.get("pending_shipment_id")
                cid = chosen.get("courier_company_id") or chosen.get("courier_id")
                awb = assign_awb(shipment_id, cid)
                if awb:
                    await _finish_shipment_after_awb(update.message, ctx, awb, chosen)
                else:
                    await update.message.reply_text(f"❌ AWB failed. Try another number:")
            else:
                await update.message.reply_text("Invalid number. Try again:")
        except:
            await update.message.reply_text("Enter number only (e.g. 1)")
        return

    if state == "prod_add":
        await do_add_product(update, ctx, text); return

    await update.message.reply_text("Use the buttons ⬇️", reply_markup=MAIN_KB)

# ─── CREATE ───────────────────────────────
async def do_create(update, ctx, text):
    msg = await update.message.reply_text("⏳ Processing with AI...")
    try:
        parsed = ai_parse(text)
        d = parse_fields(parsed)

        if not d.get("phone") or not d.get("pincode"):
            await msg.edit_text("❌ Missing phone or pincode.")
            ctx.user_data.clear(); return

        cod_value = d.get("amount", "").strip().upper()
        if cod_value in ("MISSING", "", "NA"):
            ctx.user_data["create_parsed"] = d
            ctx.user_data["state"] = "create_cod_missing"
            await msg.edit_text(
                f"✅ Parsed:\nName: {d.get('name', '')}\nPhone: {d.get('phone', '')}\n"
                f"City: {d.get('city', '')}, {d.get('pincode', '')}\n\n⚠️ COD missing — enter amount:")
            return

        try:
            cod_amount = float(re.sub(r"[^\d.]", "", cod_value))
            if cod_amount <= 0: raise ValueError
            d["cod"] = str(int(cod_amount))
        except:
            ctx.user_data["create_parsed"] = d
            ctx.user_data["state"] = "create_cod_missing"
            await msg.edit_text(f"⚠️ Invalid COD. Enter valid amount:"); return

        existing = find_by_phone(d.get("phone", ""))
        if existing:
            ctx.user_data["create_parsed"] = d
            ctx.user_data["state"] = "create_dup_check"
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Create new order", callback_data="dup_yes"),
                InlineKeyboardButton("❌ Cancel", callback_data="dup_no"),
            ]])
            await msg.edit_text(
                f"⚠️ *Existing order found!*\n#{existing.get('order_number')} {existing.get('customer_name', '')}\n"
                f"AWB: {(existing.get('shiprocket') or {}).get('awb', '—')}\n\nCreate new anyway?",
                parse_mode="Markdown", reply_markup=kb)
            return

        ctx.user_data["create_parsed"] = d
        ctx.user_data["state"] = "create_creative"
        await msg.edit_text(
            f"✅ Parsed:\nName: {d.get('name', '')}\nPhone: {d.get('phone', '')}\n"
            f"Address: {d.get('address', '')}\nLandmark: {d.get('address2', 'NA')}\n"
            f"City: {d.get('city', '')}, {d.get('pincode', '')}\nState: {d.get('state', '')}\n"
            f"Product: {d.get('product', '')}\nCOD: ₹{int(float(d.get('cod', 0))):,}\n\n"
            f"Enter creative code (or 'skip'):")
    except Exception as e:
        log.error(f"Parse error: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")
        ctx.user_data.clear()

async def do_create_shipment(update_or_q, ctx):
    ud = ctx.user_data
    d = ud.get("create_parsed", {})
    creative = ud.get("create_creative", "")
    reply = getattr(update_or_q, 'message', None) or update_or_q.message
    msg = await reply.reply_text("⏳ Creating on Shiprocket...")

    try:
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        prod_name = d.get("product", "Projector")
        prod = products.get(prod_name, {"length": 20, "breadth": 15, "height": 10, "weight": 0.5})

        try:
            cod_amount = float(re.sub(r"[^\d.]", "", d.get("cod", "0")))
            if cod_amount <= 0: raise ValueError
        except:
            await msg.edit_text("❌ Invalid COD.")
            ctx.user_data.clear(); return

        pickup_obj = resolve_pickup(d.get("pickup", ""))
        if not pickup_obj:
            await msg.edit_text("❌ Pickup not found.")
            ctx.user_data.clear(); return

        pickup_display = pickup_obj.get("pickup_location", "")
        pickup_pin = str(pickup_obj.get("pin_code", "560001"))
        delivery_pin = str(d.get("pincode", "560001"))
        order_id = f"BB{int(time.time())}_{uuid.uuid4().hex[:5]}"
        weight = float(prod["weight"])
        is_prepaid = d.get("payment_mode", "").strip().upper() == "PREPAID"
        sr_payment = "Prepaid" if is_prepaid else "COD"

        payload = {
            "order_id": order_id,
            "order_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "pickup_location": pickup_display,
            "billing_customer_name": d.get("name", "Customer"),
            "billing_last_name": ".",
            "billing_address": d.get("address", ""),
            "billing_address_2": d.get("address2", ""),
            "billing_city": d.get("city", ""),
            "billing_state": d.get("state", "Karnataka"),
            "billing_country": "India",
            "billing_pincode": delivery_pin,
            "billing_email": "orders@backbenchershub.in",
            "billing_isd_code": "91",
            "billing_phone": d.get("phone", ""),
            "shipping_is_billing": True,
            "order_items": [{"name": prod_name, "sku": prod_name, "units": 1,
                             "selling_price": cod_amount, "discount": "0", "tax": "0", "hsn": ""}],
            "payment_method": sr_payment,
            "sub_total": cod_amount,
            "length": float(prod["length"]), "breadth": float(prod["breadth"]),
            "height": float(prod["height"]), "weight": weight,
        }
        if not is_prepaid:
            payload["cod_amount"] = cod_amount

        ensure_token()
        r = session.post(f"{SR_BASE}/orders/create/adhoc", json=payload, timeout=45)
        if r.status_code != 200:
            body = r.text
            if "wallet" in body.lower(): await msg.edit_text("❌ Insufficient wallet balance")
            elif "pincode" in body.lower(): await msg.edit_text(f"❌ Invalid pincode: {delivery_pin}")
            else: await msg.edit_text(f"❌ Failed: {body[:200]}")
            ctx.user_data.clear(); return

        resp = r.json()
        shipment_id = resp.get("shipment_id")
        await msg.edit_text("⏳ Assigning courier...")

        couriers = get_couriers(pickup_pin, delivery_pin, weight, True)
        if not couriers:
            await msg.edit_text(f"❌ No courier for {delivery_pin}")
            ctx.user_data.clear(); return

        awb, chosen, need_manual, surface_couriers = select_courier(couriers, shipment_id)

        if awb == "WALLET_LOW":
            await msg.edit_text("❌ Shiprocket wallet low — recharge and retry.")
            ctx.user_data.clear(); return

        if need_manual:
            ud.update({
                "state": "manual_courier_pick",
                "pending_shipment_id": shipment_id,
                "pending_order_id": order_id,
                "pending_sr_resp": resp,
                "pending_d": d,
                "pending_prod_name": prod_name,
                "pending_cod": cod_amount,
                "pending_pickup_display": pickup_display,
                "pending_creative": creative,
                "pending_surface_couriers": surface_couriers,
                "pending_sr_payment": sr_payment,
            })
            lines = ["⚠️ *Auto courier failed.*\nPick a courier:\n"]
            for i, c in enumerate(surface_couriers[:10], 1):
                lines.append(f"{i}. {c.get('courier_name', '')} — ₹{c.get('rate', 0)}")
            await reply.reply_text("\n".join(lines), parse_mode="Markdown")
            return

        await _finish_shipment_after_awb(reply, ctx, awb, chosen,
            order_id=order_id, resp=resp, d=d,
            prod_name=prod_name, cod_amount=cod_amount,
            pickup_display=pickup_display, delivery_pin=delivery_pin,
            weight=weight, shipment_id=shipment_id, creative=creative,
            sr_payment=sr_payment)

    except Exception as e:
        log.error(f"BB Create: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")
    finally:
        ctx.user_data.clear()

async def _finish_shipment_after_awb(reply, ctx, awb, chosen,
    order_id=None, resp=None, d=None,
    prod_name=None, cod_amount=None,
    pickup_display=None, delivery_pin=None,
    weight=None, shipment_id=None, creative=None,
    sr_payment=None):

    ud = ctx.user_data
    if order_id is None:
        order_id = ud.get("pending_order_id")
        resp = ud.get("pending_sr_resp", {})
        d = ud.get("pending_d", {})
        prod_name = ud.get("pending_prod_name", "Projector")
        cod_amount = ud.get("pending_cod", 0)
        pickup_display = ud.get("pending_pickup_display", "")
        creative = ud.get("pending_creative", "")
        shipment_id = ud.get("pending_shipment_id")
        delivery_pin = str(d.get("pincode", "560001")) if d else "560001"
        sr_payment = ud.get("pending_sr_payment", "COD")

    if sr_payment is None:
        is_prepaid = (d or {}).get("payment_mode", "").strip().upper() == "PREPAID"
        sr_payment = "Prepaid" if is_prepaid else "COD"

    tracking = f"https://shiprocket.co/tracking/{awb}"
    order_num = next_order_number()

    order_record = {
        "order_id": order_id,
        "order_number": order_num,
        "created_at": datetime.now().isoformat(),
        "phone": d.get("phone", ""),
        "customer_name": d.get("name", ""),
        "address": d.get("address", ""),
        "address2": d.get("address2", ""),
        "city": d.get("city", ""),
        "state": d.get("state", "Karnataka"),
        "pincode": delivery_pin,
        "product": prod_name,
        "creative": creative,
        "total": cod_amount,
        "cod_amount": cod_amount,
        "payment_method": sr_payment,
        "courier_paid": COURIER_CHARGES,
        "advance_paid": None,
        "status": "active",
        "pickup_location": pickup_display,
        "shiprocket": {
            "order_id": resp.get("order_id", ""),
            "shipment_id": shipment_id,
            "awb": awb,
            "courier": chosen.get("courier_name", ""),
            "rate": chosen.get("rate", 0),
            "tracking": tracking
        },
        "manual": None,
        "label_downloaded": False,
        "label_downloaded_date": "",
    }

    save_order(order_record)

    await reply.reply_text(
        f"✅ *BB Shipment Created!*\n"
        f"Order: #{order_num} | {d.get('name', '')} | {d.get('phone', '')}\n"
        f"Address: {d.get('address', '')}\nLandmark: {d.get('address2', '—')}\n"
        f"City: {d.get('city', '')}, {delivery_pin} | {d.get('state', 'Karnataka')}\n"
        f"Product: {prod_name} | Creative: {creative or '—'}\n"
        f"COD: ₹{int(cod_amount):,} | Payment: {sr_payment}\n"
        f"Vendor: {pickup_display} | {chosen.get('courier_name', '')}\n"
        f"AWB: `{awb}`\n"
        f"Tracking: {tracking}",
        parse_mode="Markdown")

    label_url = generate_label(shipment_id)
    if label_url:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(label_url) as r2:
                    if r2.status == 200:
                        await reply.reply_document(
                            document=await r2.read(),
                            filename=f"{awb}.pdf",
                            caption=f"📄 {d.get('name', '')} | {awb}")
        except Exception as e:
            log.error(f"Label: {e}")

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Schedule Pickup", callback_data=f"pickup_yes_{shipment_id}_{order_id}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"action_cancel_{order_id}"),
    ]])
    await reply.reply_text("Shipment action:", reply_markup=kb)
    ctx.user_data.clear()

# ─── SEARCH ───────────────────────────────
async def do_search(update, ctx, text):
    o = find_by_phone(text) if re.match(r"^\d{10}$", text.strip()) else find_by_awb(text)
    if o:
        await update.message.reply_text(format_order(o), reply_markup=order_action_kb(o.get("order_id", ""), o.get("phone", "")))
    else:
        await update.message.reply_text("❌ No order found", reply_markup=MAIN_KB)
    ctx.user_data.clear()

# ─── ADVANCE ──────────────────────────────
async def show_advance(q, ctx, phone):
    o = find_by_phone(phone)
    if not o:
        await q.message.reply_text("❌ Order not found", reply_markup=MAIN_KB); return
    ctx.user_data.update({"adv_phone": phone, "adv_order": o, "state": "adv_picking"})
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("₹400", callback_data="adv_400"),
         InlineKeyboardButton("₹500", callback_data="adv_500"),
         InlineKeyboardButton("₹600", callback_data="adv_600"),
         InlineKeyboardButton("₹700", callback_data="adv_700")],
        [InlineKeyboardButton("Custom", callback_data="adv_custom"),
         InlineKeyboardButton("₹0 Full COD", callback_data="adv_0")],
    ])
    await q.message.reply_text(
        f"📦 #{o.get('order_number')} — {o.get('customer_name', '')}\n"
        f"COD: ₹{int(o.get('cod_amount', 0)):,}\nAdvance paid?",
        reply_markup=kb)

async def do_save_advance(update_or_q, ctx, advance_amt):
    ud = ctx.user_data
    update_order(ud.get("adv_phone", ""), advance_paid=advance_amt)
    reply = getattr(update_or_q, 'message', None) or update_or_q
    await reply.reply_text(f"✅ Advance ₹{advance_amt} saved.", reply_markup=MAIN_KB)
    ud.clear()

# ─── LABELS ───────────────────────────────
async def show_label_menu(update, ctx):
    orders = [o for o in load_orders() if not o.get("label_downloaded")]
    if not orders:
        await update.message.reply_text("📥 No labels pending", reply_markup=MAIN_KB); return
    await update.message.reply_text(f"📥 {len(orders)} labels pending\n\nDownload all?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"📥 Download All ({len(orders)})", callback_data="dl_all_labels")]]))

# ─── PRODUCTS ─────────────────────────────
async def show_products(update, ctx):
    products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
    if not products:
        ctx.user_data["state"] = "prod_add"
        await update.message.reply_text("No products.\nSend: Name length breadth height weight"); return
    for name, p in products.items():
        await update.message.reply_text(
            f"*{name}*\n{p['length']}×{p['breadth']}×{p['height']}cm | {p['weight']}kg",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑 Delete", callback_data=f"prod_del_{name}")]]))
    await update.message.reply_text("Products ↑",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add", callback_data="prod_add")]]))

async def do_add_product(update, ctx, text):
    parts = text.strip().split()
    if len(parts) < 5:
        await update.message.reply_text("Format: Name length breadth height weight"); return
    try:
        l, b, h, w = float(parts[-4]), float(parts[-3]), float(parts[-2]), float(parts[-1])
        name = " ".join(parts[:-4])
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        products[name] = {"length": l, "breadth": b, "height": h, "weight": w}
        json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)
        await update.message.reply_text(f"✅ Saved: {name}", reply_markup=MAIN_KB)
    except:
        await update.message.reply_text("Invalid format.", reply_markup=MAIN_KB)
    ctx.user_data.clear()

# ─── CALLBACKS ────────────────────────────
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    ud = ctx.user_data

    if data == "dup_yes":
        ud["state"] = "create_creative"
        d = ud.get("create_parsed", {})
        await q.message.reply_text(
            f"✅ Name: {d.get('name', '')} | Phone: {d.get('phone', '')}\n"
            f"COD: ₹{int(float(d.get('cod', 0))):,}\n\nEnter creative code (or 'skip'):")
        return

    if data == "dup_no":
        await q.message.reply_text("Cancelled", reply_markup=MAIN_KB)
        ud.clear(); return

    if data.startswith("adv_start_"):
        await show_advance(q, ctx, data.replace("adv_start_", "")); return

    if data.startswith("adv_") and data not in ("adv_save", "adv_custom"):
        try: await do_save_advance(q, ctx, int(data.replace("adv", "")))
        except: pass
        return

    if data == "adv_custom":
        ud["state"] = "adv_custom"
        await q.message.reply_text("Enter advance amount:"); return

    if data.startswith("pickup_yes_"):
        parts = data.replace("pickup_yes_", "").split("_", 1)
        ok, msg = schedule_pickup([parts[0]])
        if ok and len(parts) > 1:
            update_order_by_id(parts[1], pickup_scheduled=True)
        await q.edit_message_text(msg); return

    if data.startswith("action_cancel_"):
        order_id = data.replace("action_cancel_", "")
        orders = load_orders()
        o = next((x for x in orders if x.get("order_id") == order_id), None)
        if o:
            sr = o.get("shiprocket") or {}
            sr_order_id = sr.get("order_id") or sr.get("shipment_id")
            if sr_order_id:
                ok, msg = cancel_sr_order(sr_order_id)
                if ok: update_order_by_id(order_id, status="cancelled")
                await q.message.reply_text(f"{'✅ Cancelled' if ok else '❌ ' + msg}", reply_markup=MAIN_KB)
        return

    if data == "dl_all_labels":
        orders = [o for o in load_orders() if not o.get("label_downloaded")]
        await q.message.reply_text(f"⏳ Generating {len(orders)} labels...")
        downloaded = 0
        for o in orders:
            sr = o.get("shiprocket") or {}
            sid = sr.get("shipment_id")
            if not sid: continue
            url = generate_label(sid)
            if url:
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(url) as r:
                            if r.status == 200:
                                await q.message.reply_document(
                                    document=await r.read(),
                                    filename=f"{sr.get('awb', 'label')}.pdf",
                                    caption=f"#{o.get('order_number')} — {o.get('customer_name', '')}")
                                update_order_by_id(o.get("order_id", ""),
                                    label_downloaded=True,
                                    label_downloaded_date=date.today().isoformat())
                                downloaded += 1
                except Exception as e:
                    log.error(f"Label DL: {e}")
        await q.message.reply_text(f"✅ {downloaded}/{len(orders)} downloaded.", reply_markup=MAIN_KB)
        return

    if data == "prod_add":
        ud["state"] = "prod_add"
        await q.message.reply_text("Send: Name length breadth height weight"); return

    if data.startswith("prod_del_"):
        name = data.replace("prod_del_", "")
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        products.pop(name, None)
        json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)
        await q.edit_message_text(f"🗑 Deleted: {name}"); return

# ─── MAIN ─────────────────────────────────
async def main():
    log.info("Starting BB bot...")
    get_token()
    log.info("BB Shiprocket OK")
    refresh_pickups()
    log.info("BB bot ready")
 
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
 
    log.info("BB bot running...")
    await app.run_polling()
 
 
if __name__ == "__main__":
    asyncio.run(main())
 
