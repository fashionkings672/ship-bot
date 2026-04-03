"""
bot_enhanced.py — Oneboxx Ship Bot Final
Changes:
- State auto-fill from pincode if missing
- Bulk rebook: direct execute if all found, confirm only if some missing
- Duplicate customer check on create shipment
- Monthly sheet tabs
- Order counter from sheet max
- Phone number normalization
"""
import os, re, json, uuid, time, logging, asyncio, aiohttp, io
import requests
from datetime import datetime, date, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, CallbackQueryHandler, filters
import openai
from orders_manager import (
    save_order, find_by_phone, find_by_awb, update_order, update_order_by_id,
    next_order_number, calc_cod, payment_status,
    get_label_queue, get_all_label_vendors, get_products_for_vendor,
    get_label_queue_by_vendor_product, get_label_counts, mark_label_downloaded,
    get_payment_report, get_missing_creative, set_creative,
    get_today_stats, get_week_stats, log_adsspend, log_campaign_orders, get_today_ads,
    format_order, load_orders, save_orders, sync_from_sheets
)

# ─── CONFIG ───────────────────────────────
BOT_TOKEN        = os.getenv("BOT_TOKEN_2")
SHIPROCKET_EMAIL = os.getenv("SHIPROCKET_EMAIL")
SHIPROCKET_PASS  = os.getenv("SHIPROCKET_PASSWORD")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")

for k,v in [("BOT_TOKEN_2",BOT_TOKEN),("SHIPROCKET",SHIPROCKET_EMAIL),("OPENAI",OPENAI_API_KEY)]:
    print(f"  {k}: {'OK' if v else 'MISSING'}")

if not BOT_TOKEN:        raise ValueError("BOT_TOKEN_2 not set")
if not SHIPROCKET_EMAIL: raise ValueError("SHIPROCKET_EMAIL not set")
if not SHIPROCKET_PASS:  raise ValueError("SHIPROCKET_PASSWORD not set")
if not OPENAI_API_KEY:   raise ValueError("OPENAI_API_KEY not set")

openai.api_key = OPENAI_API_KEY
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")

# ─── SHIPROCKET ───────────────────────────
SR_BASE    = "https://apiv2.shiprocket.in/v1/external"
session    = requests.Session()
_token     = None
_token_exp = 0
_pickups   = {}
PRODUCTS_FILE         = "products.json"
COURIER_PRIORITY_FILE = "courier_priority.json"

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
    _token_exp = time.time() + 23*3600
    session.headers.update({"Authorization": f"Bearer {_token}"})
    return _token

def ensure_token():
    try: get_token()
    except: get_token(force=True)

def refresh_pickups():
    global _pickups
    ensure_token()
    r = session.get(f"{SR_BASE}/settings/company/pickup", timeout=60)
    lst = r.json().get("data",{}).get("shipping_address",[])
    _pickups = {p["pickup_location"].lower(): p for p in lst if p.get("pickup_location")}
    log.info(f"Pickups: {list(_pickups.keys())}")

def resolve_pickup(name):
    if not name: return next(iter(_pickups.values()), None)
    key = re.sub(r"\W","",str(name).lower())
    for k,v in _pickups.items():
        if key in re.sub(r"\W","",k) or re.sub(r"\W","",k) in key:
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
        r = sr_get("/courier/serviceability/", {"pickup_postcode":pp,"delivery_postcode":dp,"cod":int(bool(cod)),"weight":weight})
        return r.get("data",{}).get("available_courier_companies",[]) or []
    except: return []

def priority_rank(name):
    if not os.path.exists(COURIER_PRIORITY_FILE): return 999
    prio = json.load(open(COURIER_PRIORITY_FILE))
    n = name.lower()
    for k,v in prio.items():
        if k.lower() in n or n in k.lower(): return v
    return 999

def assign_awb(shipment_id, courier_id=None):
    payload = {"shipment_id": shipment_id}
    if courier_id: payload["courier_id"] = courier_id
    r = sr_post("/courier/assign/awb", payload)
    if r.get("awb_assign_status") == 1:
        return r["response"]["data"]["awb_code"]
    return None

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
        r = session.post(f"{SR_BASE}/orders/cancel", json={"ids":[str(sr_order_id)]}, timeout=30)
        resp = r.json()
        if r.status_code == 200 or "success" in str(resp).lower(): return True, "Cancelled"
        return False, str(resp)
    except Exception as e: return False, str(e)

def get_real_sr_order_id(o):
    sr = o.get("shiprocket") or {}
    sr_order_id = sr.get("order_id","") or sr.get("shipment_id","")
    if not sr_order_id:
        awb = sr.get("awb","")
        if awb:
            try:
                ensure_token()
                r = sr_get(f"/orders/show/{awb}")
                sr_order_id = str(r.get("data",{}).get("id","") or "")
            except: pass
    return sr_order_id

def get_available_couriers_for_order(order):
    pickup_obj = resolve_pickup(order.get("pickup_location",""))
    if not pickup_obj: return []
    pickup_pin   = str(pickup_obj.get("pin_code","560001"))
    delivery_pin = str(order.get("pincode","560001"))
    products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
    prod = products.get(order.get("product","Projector"), {"weight":0.5})
    return get_couriers(pickup_pin, delivery_pin, prod["weight"], True)

def do_rebook_shipment(o, new_cod):
    sr_order_id = get_real_sr_order_id(o)
    if not sr_order_id:
        return False, "No Shiprocket order ID — cancel manually", None
    ok, err = cancel_sr_order(sr_order_id)
    if not ok: return False, err, None

    products  = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
    prod_name = o.get("product","Projector")
    prod      = products.get(prod_name, {"length":20,"breadth":15,"height":10,"weight":0.5})
    pickup_obj = resolve_pickup(o.get("pickup_location",""))
    if not pickup_obj: return False, "Pickup not found", None

    delivery_pin = str(o.get("pincode","560001"))
    pickup_pin   = str(pickup_obj.get("pin_code","560001"))
    new_order_id = f"OBX{int(time.time())}_{uuid.uuid4().hex[:5]}"

    payload = {
        "order_id": new_order_id,
        "order_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "pickup_location": pickup_obj.get("pickup_location"),
        "billing_customer_name": o.get("customer_name",""),
        "billing_last_name": ".",
        "billing_address": o.get("address",""),
        "billing_city": o.get("city",""),
        "billing_state": o.get("state","Karnataka"),
        "billing_country": "India",
        "billing_pincode": delivery_pin,
        "billing_email": "orders@oneboxx.in",
        "billing_isd_code": "91",
        "billing_phone": o.get("phone",""),
        "shipping_is_billing": True,
        "order_items": [{"name":prod_name,"sku":prod_name,"units":1,"selling_price":new_cod,"discount":"0","tax":"0","hsn":""}],
        "payment_method": "COD",
        "sub_total": new_cod, "cod_amount": new_cod,
        "length": float(prod["length"]), "breadth": float(prod["breadth"]),
        "height": float(prod["height"]), "weight": float(prod["weight"]),
    }

    ensure_token()
    r = session.post(f"{SR_BASE}/orders/create/adhoc", json=payload, timeout=45)
    if r.status_code != 200: return False, r.text[:100], None

    resp        = r.json()
    shipment_id = resp.get("shipment_id")
    couriers    = get_couriers(pickup_pin, delivery_pin, prod["weight"], True)
    sorted_c    = sorted(couriers, key=lambda c: priority_rank(c.get("courier_name","")))
    awb = None; chosen = None
    for c in sorted_c:
        cid = c.get("courier_company_id") or c.get("courier_id")
        awb = assign_awb(shipment_id, cid)
        if awb: chosen = c; break

    if not awb: return False, "AWB failed", None

    tracking = f"https://shiprocket.co/tracking/{awb}"
    update_order(o.get("phone",""),
        order_id=new_order_id, cod_amount=new_cod, status="active",
        shiprocket={
            "order_id": resp.get("order_id",""), "shipment_id": shipment_id,
            "awb": awb, "courier": chosen.get("courier_name",""),
            "rate": chosen.get("rate",0), "tracking": tracking,
        }
    )
    return True, awb, shipment_id

# ─── AI PARSER ────────────────────────────
def ai_format_address(raw_text):
    prompt = f"""
You are a shipping assistant for Shiprocket.
A customer has pasted a messy order.
Your job is to carefully extract the required details and output them in the exact format:

Input:
{raw_text}

Output format:
Pickup: <pickup_location>
Product: <product_name>
Name: <customer_name>
Address: <full_address_line_1>, <full_address_line_2>
City: <city>
District: <district>
State: <state>
Pincode: <pincode>
Phone: <10_digit_phone_number>
Alternate Phone: <10_digit_alt_phone_or_leave_blank>
Prepaid/COD: <payment_type> <amount>
Quantity: <number_of_units>
"""
    try:
        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        formatted_text = response.choices[0].message.content.strip()
        return formatted_text
    except Exception as e:
        log.error(f"OpenAI API error: {e}")
        raise
# ─── KEYBOARDS ────────────────────────────
MAIN_KB = ReplyKeyboardMarkup([
    ["➕ Create Shipment", "🔍 Search Order"],
    ["📥 Download Labels", "📊 Payment Report"],
    ["🎨 Creative",        "📦 Products"],
    ["⚡ Bulk Actions"],
], resize_keyboard=True)

BULK_KB = ReplyKeyboardMarkup([
    ["💰 Mark Advance"],
    ["🔄 Convert COD"],
    ["🔙 Back"],
], resize_keyboard=True)

def order_action_kb(order_id, phone):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("💰 Advance",    callback_data=f"adv_start_{phone}"),
        InlineKeyboardButton("🚚 Manual AWB", callback_data=f"manual_start_{phone}"),
        InlineKeyboardButton("❌ Cancel",      callback_data=f"action_cancel_{order_id}"),
    ]])

# ─── /start ───────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "🚀 *Oneboxx Ship Bot*\n\n/adsspend /orders /report /setcreative",
        parse_mode="Markdown", reply_markup=MAIN_KB)

# ─── COMMANDS ─────────────────────────────
async def cmd_adsspend(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = " ".join(ctx.args).strip()
    if not args:
        await update.message.reply_text("Usage:\n/adsspend 3300\n/adsspend BANG:500 KOLAR:400"); return
    if ":" in args:
        breakdown = {}
        for p in args.split():
            if ":" in p:
                k,v = p.split(":",1)
                try: breakdown[k.upper()] = float(v)
                except: pass
        data = log_adsspend(breakdown=breakdown)
        lines = ["✅ Spend logged"]
        for k,v in data.items():
            if k.startswith("spend_"): lines.append(f"  {k.replace('spend_','')}: ₹{v}")
        lines.append(f"  Total: ₹{data.get('total_spend',0)}")
        await update.message.reply_text("\n".join(lines))
    else:
        try:
            data = log_adsspend(total=float(args))
            await update.message.reply_text(f"✅ Spend: ₹{data['total_spend']}")
        except: await update.message.reply_text("Invalid. Use /adsspend 3300")

async def cmd_orders(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = " ".join(ctx.args).strip()
    if not args:
        await update.message.reply_text("Usage:\n/orders BANG:4 KOLAR:2 TUM:4"); return
    breakdown = {}
    for p in args.split():
        if ":" in p:
            k,v = p.split(":",1)
            try: breakdown[k.upper()] = int(v)
            except: pass
    data  = log_campaign_orders(breakdown)
    total = data.get("total_campaign_orders",0)
    cpo   = data.get("cpo",0)
    lines = [f"✅ Orders logged — {date.today()}"]
    for k,v in data.items():
        if k.startswith("orders_"): lines.append(f"  {k.replace('orders_','')}: {v}")
    lines.append(f"\nTotal: {total}")
    if cpo:
        lines.append(f"CPO: ₹{cpo}")
        if cpo < 150: lines.append("⭐ Excellent")
        elif cpo > 350: lines.append("⚠️ High CPO")
    await update.message.reply_text("\n".join(lines))

async def cmd_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = get_today_stats(); w = get_week_stats(); ads = get_today_ads()
    lines = [
        f"📊 *REPORT — {date.today()}*",
        f"📦 Orders: {t['total']}",
        f"💰 Advance: {t['advance_paid']}",
        f"💵 Full COD: {t['full_cod']}",
        f"⏳ Courier only: {t['courier_only']}",
        f"❌ Nothing: {t['nothing']}",
        f"",
        f"📢 Spend: ₹{ads.get('total_spend',0)}",
        f"🎯 CPO: {'₹'+str(ads.get('cpo',0)) if ads.get('cpo') else '—'}",
        f"📅 Week: {w['total']} orders | {w['conv_rate']}% paid",
    ]
    camps = {k.replace("orders_",""):v for k,v in ads.items() if k.startswith("orders_")}
    if camps:
        lines.append("\n🏙 Campaigns:")
        for c,o in sorted(camps.items(), key=lambda x:-x[1]): lines.append(f"  {c}: {o}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_setcreative(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) == 0:
        missing = get_missing_creative("all")
        if not missing: await update.message.reply_text("✅ All orders have creative set"); return
        lines = [f"🎨 No creative ({len(missing)}):\n"]
        for o in missing[:20]: lines.append(f"#{o.get('order_number')} {o.get('customer_name','')} — {o.get('phone','')}")
        lines.append("\nUse: /setcreative <phone> <code>")
        await update.message.reply_text("\n".join(lines))
    elif len(ctx.args) == 2:
        phone, code = ctx.args
        o = set_creative(phone, code)
        if o: await update.message.reply_text(f"✅ Creative {code.upper()} set for #{o.get('order_number')}")
        else: await update.message.reply_text("❌ Order not found")
    else:
        await update.message.reply_text("Usage: /setcreative <phone> <code>")

# ─── MESSAGE HANDLER ──────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text  = update.message.text.strip()
    ud    = ctx.user_data
    state = ud.get("state")

    log.info(f"MSG: '{text}' | state: '{state}'")

    if text == "➕ Create Shipment":
        ud.clear(); ud["state"] = "create"
        await update.message.reply_text("Send order details:", reply_markup=MAIN_KB); return

    if text == "🔍 Search Order":
        ud.clear(); ud["state"] = "search"
        await update.message.reply_text("Enter phone or AWB:", reply_markup=MAIN_KB); return

    if text == "📥 Download Labels":
        ud.clear(); await show_label_menu(update, ctx); return

    if text == "📊 Payment Report":
        ud.clear(); await show_payment_report(update, ctx); return

    if text == "🎨 Creative":
        ud.clear(); await show_creative_menu(update, ctx); return

    if text == "📦 Products":
        ud.clear(); await show_products(update, ctx); return

    if text == "⚡ Bulk Actions":
        ud.clear(); ud["state"] = "bulk_menu_open"
        await update.message.reply_text("⚡ *Bulk Actions* — choose:", parse_mode="Markdown", reply_markup=BULK_KB); return

    if text == "💰 Mark Advance":
        ud["bulk_mode"] = "advance"
        ud["state"]     = "bulk_input"
        await update.message.reply_text(
            "💰 Send phone numbers + advance amount on last line:\n\n9845123456\n9876543210\n600",
            reply_markup=BULK_KB); return

    if text == "🔄 Convert COD":
        ud["bulk_mode"] = "rebook"
        ud["state"]     = "bulk_input"
        await update.message.reply_text(
            "🔄 Send phone numbers + new COD on last line:\n\n9845123456\n9876543210\n3000",
            reply_markup=BULK_KB); return

    if text == "🔙 Back":
        ud.clear()
        await update.message.reply_text("Main menu:", reply_markup=MAIN_KB); return

    # ── State machine ──
    if state == "bulk_input":
        await do_bulk_parse(update, ctx, text); return

    if state == "create":
        await do_create(update, ctx, text); return

    if state == "create_creative":
        ud["create_creative"] = "" if text.lower() == "skip" else text.upper()
        ud["state"] = "create_courier"
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("₹200", callback_data="create_cour_200"),
            InlineKeyboardButton("₹300", callback_data="create_cour_300"),
            InlineKeyboardButton("₹0",   callback_data="create_cour_0"),
            InlineKeyboardButton("Custom", callback_data="create_cour_custom"),
        ]])
        await update.message.reply_text(
            f"Creative: {ud['create_creative'] or '—'} ✅\n\nCourier amount charged?", reply_markup=kb); return

    if state == "create_courier_custom":
        try: ud["create_courier_amount"] = int(text)
        except: await update.message.reply_text("Enter number only"); return
        await do_create_shipment(update, ctx); return

    if state == "search":
        await do_search(update, ctx, text); return

    if state == "adv_custom":
        try: amt = int(text)
        except: await update.message.reply_text("Enter number only"); return
        await do_save_advance(update, ctx, amt); return

    if state == "adv_new_cod":
        try: new_cod = int(text)
        except: await update.message.reply_text("Enter number only"); return
        await do_rebook_new_cod(update, ctx, new_cod); return

    if state == "manual_vendor":
        ud["manual_vendor"] = text; ud["state"] = "manual_courier"
        await update.message.reply_text("Enter courier name:"); return

    if state == "manual_courier":
        ud["manual_courier_name"] = text; ud["state"] = "manual_awb"
        await update.message.reply_text("Enter AWB number:"); return

    if state == "manual_awb":
        phone = ud.get("manual_phone")
        o = update_order(phone,
            manual={"vendor":ud.get("manual_vendor",""),"courier":ud.get("manual_courier_name",""),
                    "awb":text,"added_at":datetime.now().isoformat()}, status="manual")
        await update.message.reply_text(
            f"✅ Manual saved\nAWB: {text}" if o else "❌ Failed", reply_markup=MAIN_KB)
        ud.clear(); return

    if state == "prod_add":
        await do_add_product(update, ctx, text); return

    if state == "reassign_select":
        try:
            idx = int(text.strip()) - 1
            couriers = ud.get("reassign_couriers",[])
            if 0 <= idx < len(couriers): await do_reassign_courier(update, ctx, couriers[idx])
            else: await update.message.reply_text("Invalid number. Try again:")
        except: await update.message.reply_text("Enter the number only (e.g. 1)")
        return

    await update.message.reply_text("Use the buttons ⬇️", reply_markup=MAIN_KB)

# ─── CREATE SHIPMENT ──────────────────────
async def do_create(update, ctx, text):
    msg = await update.message.reply_text("⏳ Processing with AI...")
    try:
        parsed = ai_parse(text); d = parse_fields(parsed)
        if not d.get("phone") or not d.get("pincode"):
            await msg.edit_text("❌ Missing phone or pincode.\n\nFormat:\nName\nPhone\nAddress, City\nPincode\nProduct\nCOD amount")
            ctx.user_data.clear(); return

        # Duplicate check
        existing = find_by_phone(d.get("phone",""))
        if existing:
            ctx.user_data["create_parsed"] = d
            ctx.user_data["state"] = "create_dup_check"
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Create new order", callback_data="dup_yes"),
                InlineKeyboardButton("❌ Cancel",           callback_data="dup_no"),
            ]])
            await msg.edit_text(
                f"⚠️ *Existing order found!*\n"
                f"#{existing.get('order_number')} {existing.get('customer_name','')}\n"
                f"📅 {str(existing.get('created_at',''))[:10]}\n"
                f"COD: ₹{int(existing.get('cod_amount',0)):,} | Status: {existing.get('status','')}\n"
                f"AWB: {(existing.get('shiprocket') or {}).get('awb','—')}\n\n"
                f"Create new order anyway?",
                parse_mode="Markdown", reply_markup=kb)
            return

        ctx.user_data["create_parsed"] = d
        ctx.user_data["state"] = "create_creative"
        await msg.edit_text(
            f"✅ Parsed:\nName: {d.get('name','')}\nPhone: {d.get('phone','')}\n"
            f"City: {d.get('city','')}, {d.get('pincode','')}\n"
            f"State: {d.get('state','')}\nProduct: {d.get('product','')}\n"
            f"COD: ₹{d.get('cod','')}\n\nEnter creative code:")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {e}"); ctx.user_data.clear()

async def do_create_shipment(update_or_q, ctx):
    ud = ctx.user_data
    d  = ud.get("create_parsed",{})
    creative        = ud.get("create_creative","")
    courier_charged = ud.get("create_courier_amount",0)
    reply = getattr(update_or_q, 'message', None) or update_or_q.callback_query.message
    msg   = await reply.reply_text("⏳ Creating on Shiprocket...")
    try:
        products  = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        prod_name = d.get("product","Projector")
        prod      = products.get(prod_name, {"length":20,"breadth":15,"height":10,"weight":0.5})
        try: cod_amount = float(re.sub(r"[^\d.]","", d.get("cod","2400") or "2400"))
        except: cod_amount = 2400.0

        pickup_obj = resolve_pickup(d.get("pickup",""))
        if not pickup_obj:
            await msg.edit_text("❌ Pickup location not found."); ctx.user_data.clear(); return

        pickup_display = pickup_obj.get("pickup_location","")
        pickup_pin     = str(pickup_obj.get("pin_code","560001"))
        delivery_pin   = str(d.get("pincode","560001"))
        order_id       = f"OBX{int(time.time())}_{uuid.uuid4().hex[:5]}"

        payload = {
            "order_id": order_id,
            "order_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "pickup_location": pickup_display,
            "billing_customer_name": d.get("name","Customer"),
            "billing_last_name": ".",
            "billing_address": d.get("address",""),
            "billing_city": d.get("city",""),
            "billing_state": d.get("state","Karnataka"),
            "billing_country": "India",
            "billing_pincode": delivery_pin,
            "billing_email": "orders@oneboxx.in",
            "billing_isd_code": "91",
            "billing_phone": d.get("phone",""),
            "shipping_is_billing": True,
            "order_items": [{"name":prod_name,"sku":prod_name,"units":1,"selling_price":cod_amount,"discount":"0","tax":"0","hsn":""}],
            "payment_method": "COD", "sub_total": cod_amount, "cod_amount": cod_amount,
            "length": float(prod["length"]), "breadth": float(prod["breadth"]),
            "height": float(prod["height"]), "weight": float(prod["weight"]),
        }
        ensure_token()
        r = session.post(f"{SR_BASE}/orders/create/adhoc", json=payload, timeout=45)
        if r.status_code != 200:
            body = r.text
            if "wallet" in body.lower(): await msg.edit_text("❌ Insufficient wallet balance")
            elif "pincode" in body.lower(): await msg.edit_text(f"❌ Invalid pincode: {delivery_pin}")
            else: await msg.edit_text(f"❌ Failed: {body[:200]}")
            ctx.user_data.clear(); return

        resp        = r.json(); shipment_id = resp.get("shipment_id")
        await msg.edit_text("⏳ Assigning courier...")
        couriers = get_couriers(pickup_pin, delivery_pin, prod["weight"], True)
        if not couriers:
            await msg.edit_text(f"❌ No courier for {delivery_pin}"); ctx.user_data.clear(); return

        sorted_c = sorted(couriers, key=lambda c: priority_rank(c.get("courier_name","")))
        awb = None; chosen = None; fallback = False
        for c in sorted_c:
            cid = c.get("courier_company_id") or c.get("courier_id")
            if priority_rank(c.get("courier_name","")) == 999: fallback = True
            awb = assign_awb(shipment_id, cid)
            if awb: chosen = c; break

        if not awb:
            await msg.edit_text("❌ Courier assignment failed."); ctx.user_data.clear(); return

        tracking  = f"https://shiprocket.co/tracking/{awb}"
        order_num = next_order_number()
        order_record = {
            "order_id": order_id, "order_number": order_num,
            "created_at": datetime.now().isoformat(),
            "phone": d.get("phone",""), "customer_name": d.get("name",""),
            "address": d.get("address",""), "city": d.get("city",""),
            "state": d.get("state","Karnataka"), "pincode": delivery_pin,
            "product": prod_name, "creative": creative, "total": cod_amount,
            "cod_amount": cod_amount, "courier_paid": courier_charged, "advance_paid": None,
            "status": "active", "pickup_location": pickup_display,
            "shiprocket": {"order_id":resp.get("order_id",""),"shipment_id":shipment_id,
                           "awb":awb,"courier":chosen.get("courier_name",""),
                           "rate":chosen.get("rate",0),"tracking":tracking},
            "manual": None, "label_downloaded": False, "label_downloaded_date": "",
        }
        save_order(order_record)
        courier_note = "⚠️ Fallback" if fallback else "✅ Priority"
        await msg.edit_text(
            f"✅ *Shipment Created!*\n"
            f"Order: #{order_num} | {d.get('name','')} | {d.get('phone','')}\n"
            f"City: {d.get('city','')}, {delivery_pin}\n"
            f"State: {d.get('state','Karnataka')}\n"
            f"Product: {prod_name} | Creative: {creative or '—'}\n"
            f"COD: ₹{int(cod_amount):,} | Courier: ₹{courier_charged}\n"
            f"Vendor: {pickup_display} | {chosen.get('courier_name','')} {courier_note}\n"
            f"AWB: `{awb}`\n"
            f"Tracking: {tracking}",
            parse_mode="Markdown")

        label_url = generate_label(shipment_id)
        if label_url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(label_url) as r2:
                        if r2.status == 200:
                            await reply.reply_document(document=await r2.read(),
                                filename=f"{awb}.pdf", caption=f"📄 {d.get('name','')} | {awb}")
            except Exception as e: log.error(f"Label: {e}")

        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Schedule Pickup", callback_data=f"pickup_yes_{shipment_id}_{order_id}"),
            InlineKeyboardButton("🔄 Reassign", callback_data=f"action_reassign_{order_id}"),
            InlineKeyboardButton("❌ Cancel",    callback_data=f"action_cancel_{order_id}"),
        ]])
        await reply.reply_text("Shipment action:", reply_markup=kb)
    except Exception as e:
        log.error(f"Create: {e}", exc_info=True); await msg.edit_text(f"❌ Error: {e}")
    finally: ctx.user_data.clear()

# ─── SEARCH ───────────────────────────────
async def do_search(update, ctx, text):
    o = find_by_phone(text) if re.match(r"^\d{10}$", text.strip()) else find_by_awb(text)
    if o: await update.message.reply_text(format_order(o), reply_markup=order_action_kb(o.get("order_id",""), o.get("phone","")))
    else: await update.message.reply_text("❌ No order found", reply_markup=MAIN_KB)
    ctx.user_data.clear()

# ─── ADVANCE ──────────────────────────────
async def show_advance(q, ctx, phone):
    o = find_by_phone(phone)
    if not o: await q.message.reply_text("❌ Order not found", reply_markup=MAIN_KB); return
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
        f"📦 #{o.get('order_number')} — {o.get('customer_name','')}\n"
        f"Product: {o.get('product','')}\nCOD: ₹{int(o.get('cod_amount',0)):,}\n"
        f"Courier: ₹{o.get('courier_paid',0)} ✅\n\nAdvance paid?", reply_markup=kb)

async def do_save_advance(update_or_q, ctx, advance_amt):
    ud = ctx.user_data
    update_order(ud.get("adv_phone",""), advance_paid=advance_amt)
    ud["adv_advance"] = advance_amt
    reply = getattr(update_or_q, 'message', None) or update_or_q
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Save", callback_data="adv_save"),
        InlineKeyboardButton("🔄 Cancel + Rebook new COD", callback_data="adv_rebook"),
    ]])
    await reply.reply_text(f"Advance: ₹{advance_amt} saved.\n\nNeed to change COD and rebook?", reply_markup=kb)

async def do_rebook_new_cod(update, ctx, new_cod):
    ud = ctx.user_data; o = ud.get("adv_order")
    msg = await update.message.reply_text("⏳ Cancelling and rebooking...")
    ok, awb_or_err, shipment_id = do_rebook_shipment(o, new_cod)
    if not ok:
        await msg.edit_text(f"❌ Failed: {awb_or_err}"); ud.clear(); return
    await msg.edit_text(
        f"✅ Rebooked!\nNew AWB: {awb_or_err}\nNew COD: ₹{new_cod:,}\nAdvance: ₹{ud.get('adv_advance',0)} ✅",
        reply_markup=MAIN_KB)
    label_url = generate_label(shipment_id)
    if label_url:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(label_url) as r:
                    if r.status == 200:
                        await update.message.reply_document(document=await r.read(),
                            filename=f"{awb_or_err}.pdf", caption=f"📄 New label | {awb_or_err}")
        except Exception as e: log.error(f"Label rebook: {e}")
    ud.clear()

# ─── BULK ACTIONS ─────────────────────────
async def do_bulk_parse(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    ud   = ctx.user_data
    mode = ud.get("bulk_mode", "advance")
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]

    if len(lines) < 2:
        await update.message.reply_text(
            "❌ Need phone numbers + amount on last line.\n\nExample:\n9845123456\n9876543210\n600"); return

    try:
        amount = int(re.sub(r"[^\d]", "", lines[-1]))
        if amount == 0: raise ValueError
    except:
        await update.message.reply_text("❌ Last line must be amount (e.g. 600 or 3000)"); return

    phones = []
    for l in lines[:-1]:
        phones.extend(re.findall(r"\d{10}", l))
    phones = list(dict.fromkeys(phones))

    if not phones:
        await update.message.reply_text("❌ No valid 10-digit phone numbers found"); return

    found = []; missing = []
    for p in phones:
        o = find_by_phone(p)
        if o: found.append(o)
        else: missing.append(p)

    ud["bulk_found"]  = found
    ud["bulk_amount"] = amount
    miss_txt = "\n".join(f"  ❌ {p}" for p in missing) if missing else ""

    if not found:
        await update.message.reply_text(f"❌ No orders found:\n{miss_txt}\n\nCheck and resend."); return

    if mode == "advance":
        # Advance — always show confirm
        found_list = "\n".join(
            f"#{o.get('order_number')} {o.get('customer_name','')} | {o.get('phone','')}"
            for o in found)
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"✅ Mark ₹{amount} on {len(found)} orders", callback_data="bulk_confirm_go"),
            InlineKeyboardButton("❌ Cancel", callback_data="bulk_cancel"),
        ]])
        msg = f"💰 Mark ₹{amount} advance on {len(found)} orders?\n\n{found_list}"
        if miss_txt: msg += f"\n\n⚠️ Not found:\n{miss_txt}"
        await update.message.reply_text(msg, reply_markup=kb)

    else:
        # Rebook — show order details
        order_lines = []
        for o in found:
            sr = o.get("shiprocket") or {}
            order_lines.append(
                f"#{o.get('order_number')} {o.get('customer_name','')} | "
                f"{o.get('phone','')} | {sr.get('courier','—')} | COD: ₹{int(o.get('cod_amount',0)):,}")
        msg = f"🔄 Cancel + Rebook {len(found)} orders at ₹{amount:,}\n\n" + "\n".join(order_lines)
        if miss_txt: msg += f"\n\n⚠️ Not found:\n{miss_txt}"

        if missing:
            # Some missing — show confirm button
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(f"✅ Continue with {len(found)}", callback_data="bulk_confirm_go"),
                InlineKeyboardButton("❌ Cancel", callback_data="bulk_cancel"),
            ]])
            await update.message.reply_text(msg, reply_markup=kb)
        else:
            # All found — execute directly
            await update.message.reply_text(msg)
            await do_bulk_execute(update, ctx)

async def do_bulk_execute(update_or_q, ctx: ContextTypes.DEFAULT_TYPE):
    ud     = ctx.user_data
    mode   = ud.get("bulk_mode", "advance")
    found  = ud.get("bulk_found", [])
    amount = ud.get("bulk_amount", 0)

    # Handle both message and callback query
    if hasattr(update_or_q, "callback_query") and update_or_q.callback_query:
        reply = update_or_q.callback_query.message
    elif hasattr(update_or_q, "message") and update_or_q.message:
        reply = update_or_q.message
    else:
        reply = update_or_q

    if mode == "advance":
        await reply.reply_text(f"⏳ Marking ₹{amount} on {len(found)} orders...")
        done = []
        for o in found:
            update_order(o.get("phone",""), advance_paid=amount)
            done.append(f"#{o.get('order_number')} {o.get('customer_name','')} ✅")
        await reply.reply_text(
            f"✅ Advance ₹{amount} marked on {len(done)} orders\n\n" + "\n".join(done),
            reply_markup=MAIN_KB)
        ud.clear()

    elif mode == "rebook":
        await reply.reply_text(f"⏳ Processing {len(found)} orders...")
        label_pdfs = []; results = []

        for o in found:
            ok, awb_or_err, shipment_id = do_rebook_shipment(o, amount)
            if not ok:
                results.append(f"#{o.get('order_number')} {o.get('customer_name','')} ❌ {awb_or_err}")
                continue
            label_url = generate_label(shipment_id)
            if label_url:
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(label_url) as r:
                            if r.status == 200: label_pdfs.append(await r.read())
                except Exception as e: log.error(f"Label bulk: {e}")
            results.append(f"#{o.get('order_number')} {o.get('customer_name','')} ✅ AWB: {awb_or_err}")

        await reply.reply_text(
            f"✅ Done ({len([r for r in results if '✅' in r])}/{len(found)})\n\n" + "\n".join(results),
            reply_markup=MAIN_KB)

        if label_pdfs:
            await reply.reply_text(f"⏳ Merging {len(label_pdfs)} labels...")
            try:
                from pypdf import PdfWriter, PdfReader
                writer = PdfWriter()
                for pdf_bytes in label_pdfs:
                    reader = PdfReader(io.BytesIO(pdf_bytes))
                    for page in reader.pages: writer.add_page(page)
                out = io.BytesIO(); writer.write(out); out.seek(0)
                await reply.reply_document(document=out.read(),
                    filename=f"Bulk_{date.today().strftime('%d%b')}.pdf",
                    caption=f"📄 {len(label_pdfs)} labels | COD ₹{amount:,}")
            except Exception as e:
                log.error(f"PDF merge: {e}")
                await reply.reply_text("⚠️ Labels done but PDF merge failed")
        ud.clear()

# ─── LABELS ───────────────────────────────
async def show_label_menu(update, ctx):
    vendors = get_all_label_vendors()
    if not vendors:
        await update.message.reply_text("📥 No labels pending", reply_markup=MAIN_KB); return
    kb_rows = []
    for v in vendors:
        products = get_products_for_vendor(v)
        total = sum(get_label_counts(v,p)[0] for p in products)
        kb_rows.append([InlineKeyboardButton(f"🏪 {v} ({total})", callback_data=f"lv1_{v}")])
    await update.message.reply_text("📥 *Download Labels*\nSelect vendor:",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb_rows))

async def show_label_products(q, vendor):
    products = get_products_for_vendor(vendor)
    if not products: await q.message.reply_text("No labels for this vendor"); return
    kb_rows = []
    for p in products:
        total, adv = get_label_counts(vendor, p)
        kb_rows.append([InlineKeyboardButton(f"📦 {p} ({total})", callback_data=f"lv2_{vendor}|{p}")])
    await q.message.reply_text(f"🏪 *{vendor}* — Select product:",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb_rows))

async def show_label_filter(q, vendor, product):
    total, adv = get_label_counts(vendor, product)
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(f"📥 All ({total})",        callback_data=f"lv3_{vendor}|{product}|all"),
        InlineKeyboardButton(f"💰 Advance Paid ({adv})", callback_data=f"lv3_{vendor}|{product}|adv"),
    ]])
    await q.message.reply_text(f"🏪 {vendor} — 📦 {product}\n\nDownload which?", reply_markup=kb)

async def do_download_labels(update, orders):
    if not orders: await update.callback_query.message.reply_text("No labels"); return
    await update.callback_query.message.reply_text(f"⏳ Generating {len(orders)} labels...")
    downloaded = 0
    for o in orders:
        sr = o.get("shiprocket") or {}; sid = sr.get("shipment_id")
        if not sid: continue
        url = generate_label(sid)
        if url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(url) as r:
                        if r.status == 200:
                            vm = o.get("manual") or {}
                            vendor = vm.get("vendor") or o.get("pickup_location") or "SR"
                            await update.callback_query.message.reply_document(
                                document=await r.read(), filename=f"{sr.get('awb','label')}.pdf",
                                caption=f"#{o.get('order_number')} — {o.get('customer_name','')} | {vendor}")
                            mark_label_downloaded(o.get("order_id","")); downloaded += 1
            except Exception as e: log.error(f"Label DL: {e}")
    await update.callback_query.message.reply_text(
        f"✅ {downloaded}/{len(orders)} downloaded. These will not appear again.", reply_markup=MAIN_KB)

# ─── PAYMENT REPORT ───────────────────────
async def show_payment_report(update, ctx):
    r = get_payment_report()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏳ Pending", callback_data="rep_pending"),
         InlineKeyboardButton("✅ Advance", callback_data="rep_advance")],
        [InlineKeyboardButton("💵 Full COD", callback_data="rep_fullcod"),
         InlineKeyboardButton("📋 All",      callback_data="rep_all")],
    ])
    await update.message.reply_text(
        f"📊 *Payment Report*\n\n⏳ Pending: {len(r['pending'])}\n✅ Advance: {len(r['advance'])}\n"
        f"💵 Full COD: {len(r['full_cod'])}\n❌ Nothing: {len(r['nothing'])}",
        parse_mode="Markdown", reply_markup=kb)

# ─── CREATIVE ─────────────────────────────
async def show_creative_menu(update, ctx):
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Today",     callback_data="cr_today"),
        InlineKeyboardButton("Yesterday", callback_data="cr_yesterday"),
        InlineKeyboardButton("All time",  callback_data="cr_all"),
    ]])
    await update.message.reply_text("🎨 Creative — missing orders:", reply_markup=kb)

# ─── PRODUCTS ─────────────────────────────
async def show_products(update, ctx):
    products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
    if not products:
        ctx.user_data["state"] = "prod_add"
        await update.message.reply_text("No products.\nSend: Name length breadth height weight"); return
    for name, p in products.items():
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✏️ Edit",   callback_data=f"prod_edit_{name}"),
            InlineKeyboardButton("🗑 Delete", callback_data=f"prod_del_{name}"),
        ]])
        await update.message.reply_text(f"*{name}*\n{p['length']}×{p['breadth']}×{p['height']}cm | {p['weight']}kg",
            parse_mode="Markdown", reply_markup=kb)
    await update.message.reply_text("Products ↑", reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("➕ Add Product", callback_data="prod_add")]]))

async def do_add_product(update, ctx, text):
    parts = text.strip().split()
    if len(parts) < 5: await update.message.reply_text("Format: Name length breadth height weight"); return
    try:
        l,b,h,w = float(parts[-4]),float(parts[-3]),float(parts[-2]),float(parts[-1])
        name = " ".join(parts[:-4])
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        products[name] = {"length":l,"breadth":b,"height":h,"weight":w}
        json.dump(products, open(PRODUCTS_FILE,"w"), indent=2)
        await update.message.reply_text(f"✅ Saved: {name}", reply_markup=MAIN_KB)
    except: await update.message.reply_text("Invalid. Format: Name l b h w", reply_markup=MAIN_KB)
    ctx.user_data.clear()

# ─── REASSIGN ─────────────────────────────
async def do_reassign_courier(update, ctx, chosen_courier):
    ud = ctx.user_data; o = ud.get("reassign_order"); sr = o.get("shiprocket") or {}
    await update.message.reply_text(f"⏳ Reassigning to {chosen_courier.get('courier_name','')}...")

    sr_order_id = get_real_sr_order_id(o)
    if not sr_order_id:
        await update.message.reply_text("❌ No Shiprocket order ID — cancel manually", reply_markup=MAIN_KB)
        ud.clear(); return

    ok, err = cancel_sr_order(sr_order_id)
    if not ok:
        await update.message.reply_text(f"❌ Cancel failed: {err}", reply_markup=MAIN_KB); ud.clear(); return

    products  = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
    prod_name = o.get("product","Projector")
    prod      = products.get(prod_name, {"length":20,"breadth":15,"height":10,"weight":0.5})
    pickup_obj = resolve_pickup(o.get("pickup_location",""))
    if not pickup_obj:
        await update.message.reply_text("❌ Pickup not found", reply_markup=MAIN_KB); ud.clear(); return

    delivery_pin = str(o.get("pincode","560001"))
    new_order_id = f"OBX{int(time.time())}_{uuid.uuid4().hex[:5]}"
    cod_amount   = o.get("cod_amount",2400)

    payload = {
        "order_id": new_order_id,
        "order_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "pickup_location": pickup_obj.get("pickup_location"),
        "billing_customer_name": o.get("customer_name",""), "billing_last_name": ".",
        "billing_address": o.get("address",""), "billing_city": o.get("city",""),
        "billing_state": o.get("state","Karnataka"), "billing_country": "India",
        "billing_pincode": delivery_pin, "billing_email": "orders@oneboxx.in",
        "billing_isd_code": "91", "billing_phone": o.get("phone",""),
        "shipping_is_billing": True,
        "order_items": [{"name":prod_name,"sku":prod_name,"units":1,"selling_price":cod_amount,"discount":"0","tax":"0","hsn":""}],
        "payment_method": "COD", "sub_total": cod_amount, "cod_amount": cod_amount,
        "length": float(prod["length"]), "breadth": float(prod["breadth"]),
        "height": float(prod["height"]), "weight": float(prod["weight"]),
    }
    ensure_token()
    r = session.post(f"{SR_BASE}/orders/create/adhoc", json=payload, timeout=45)
    if r.status_code != 200:
        await update.message.reply_text(f"❌ Recreate failed", reply_markup=MAIN_KB); ud.clear(); return

    resp        = r.json(); shipment_id = resp.get("shipment_id")
    cid = chosen_courier.get("courier_company_id") or chosen_courier.get("courier_id")
    awb = assign_awb(shipment_id, cid)
    if not awb:
        await update.message.reply_text("❌ AWB failed", reply_markup=MAIN_KB); ud.clear(); return

    tracking = f"https://shiprocket.co/tracking/{awb}"
    update_order_by_id(ud.get("reassign_order_id"),
        order_id=new_order_id, status="active",
        shiprocket={"order_id":resp.get("order_id",""),"shipment_id":shipment_id,
                    "awb":awb,"courier":chosen_courier.get("courier_name",""),
                    "rate":chosen_courier.get("rate",0),"tracking":tracking})

    await update.message.reply_text(
        f"✅ Reassigned!\nNew: {chosen_courier.get('courier_name','')} | AWB: {awb}",
        reply_markup=MAIN_KB)

    label_url = generate_label(shipment_id)
    if label_url:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(label_url) as r2:
                    if r2.status == 200:
                        await update.message.reply_document(document=await r2.read(),
                            filename=f"{awb}.pdf", caption=f"📄 Reassigned | {awb}")
        except Exception as e: log.error(f"Label reassign: {e}")
    ud.clear()

# ─── CALLBACKS ────────────────────────────
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    await q.answer()
    data = q.data or ""
    ud   = ctx.user_data

    # Duplicate check — proceed or cancel
    if data == "dup_yes":
        ud["state"] = "create_creative"
        d = ud.get("create_parsed",{})
        await q.message.reply_text(
            f"✅ Parsed:\nName: {d.get('name','')}\nPhone: {d.get('phone','')}\n"
            f"City: {d.get('city','')}, {d.get('pincode','')}\n"
            f"State: {d.get('state','')}\nProduct: {d.get('product','')}\n"
            f"COD: ₹{d.get('cod','')}\n\nEnter creative code:")
        return

    if data == "dup_no":
        await q.message.reply_text("Cancelled", reply_markup=MAIN_KB); ud.clear(); return

    if data.startswith("create_cour_"):
        val = data.replace("create_cour_","")
        if val == "custom":
            ud["state"] = "create_courier_custom"
            await q.message.reply_text("Enter courier amount:"); return
        ud["create_courier_amount"] = int(val)
        await do_create_shipment(q, ctx); return

    if data.startswith("adv_start_"):
        await show_advance(q, ctx, data.replace("adv_start_","")); return

    if data.startswith("adv_") and data not in ("adv_save","adv_rebook","adv_custom"):
        await do_save_advance(q, ctx, int(data.replace("adv_",""))); return

    if data == "adv_custom":
        ud["state"] = "adv_custom"
        await q.message.reply_text("Enter advance amount:"); return

    if data == "adv_save":
        await q.message.reply_text("✅ Done!", reply_markup=MAIN_KB); ud.clear(); return

    if data == "adv_rebook":
        ud["state"] = "adv_new_cod"
        await q.message.reply_text("Enter new COD amount:"); return

    if data.startswith("pickup_yes_"):
        parts = data.replace("pickup_yes_","").split("_",1)
        ok, msg = schedule_pickup([parts[0]])
        if ok and len(parts)>1: update_order_by_id(parts[1], pickup_scheduled=True)
        await q.edit_message_text(msg); return

    if data.startswith("action_cancel_"):
        order_id = data.replace("action_cancel_","")
        orders = load_orders()
        o = next((x for x in orders if x.get("order_id")==order_id), None)
        if o:
            sr_order_id = get_real_sr_order_id(o)
            if sr_order_id:
                ok, msg = cancel_sr_order(sr_order_id)
                if ok: update_order_by_id(order_id, status="cancelled")
                await q.message.reply_text(f"{'✅ Cancelled' if ok else '❌ '+msg} #{o.get('order_number')}", reply_markup=MAIN_KB)
            else:
                await q.message.reply_text("❌ No Shiprocket order ID", reply_markup=MAIN_KB)
        return

    if data.startswith("action_reassign_"):
        order_id = data.replace("action_reassign_","")
        orders = load_orders()
        o = next((x for x in orders if x.get("order_id")==order_id), None)
        if not o: await q.message.reply_text("❌ Order not found"); return
        await q.message.reply_text("⏳ Fetching couriers...")
        couriers = get_available_couriers_for_order(o)
        if not couriers: await q.message.reply_text("❌ No couriers available"); return
        sorted_c = sorted(couriers, key=lambda c: priority_rank(c.get("courier_name","")))[:10]
        ud.update({"reassign_order_id":order_id,"reassign_order":o,"reassign_couriers":sorted_c,"state":"reassign_select"})
        lines = ["🔄 *Available Couriers:*\n"]
        for i,c in enumerate(sorted_c,1):
            rank = "Priority" if priority_rank(c.get("courier_name","")) < 999 else "Standard"
            lines.append(f"{i}. {c.get('courier_name','')} — ₹{c.get('rate',0)} ({rank})")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown"); return

    if data.startswith("manual_start_"):
        phone = data.replace("manual_start_",""); o = find_by_phone(phone)
        if o:
            ud.update({"manual_phone":phone,"manual_order":o})
            sr = o.get("shiprocket") or {}
            if sr.get("awb"):
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Yes cancel + manual", callback_data="manual_cancel_yes"),
                    InlineKeyboardButton("❌ No", callback_data="manual_cancel_no"),
                ]])
                await q.message.reply_text(f"AWB: {sr.get('awb')} — cancel + add manual?", reply_markup=kb)
            else:
                ud["state"] = "manual_vendor"
                await q.message.reply_text("Enter vendor name:")
        return

    if data == "manual_cancel_yes":
        o = ud.get("manual_order",{}); sr_order_id = get_real_sr_order_id(o)
        if sr_order_id: cancel_sr_order(sr_order_id)
        ud["state"] = "manual_vendor"
        await q.message.reply_text("✅ Cancelled\n\nEnter vendor name:"); return

    if data == "manual_cancel_no":
        await q.message.reply_text("Cancelled", reply_markup=MAIN_KB); ud.clear(); return

    if data.startswith("lv1_"):
        await show_label_products(q, data.replace("lv1_","")); return
    if data.startswith("lv2_"):
        parts = data.replace("lv2_","").split("|",1)
        await show_label_filter(q, parts[0], parts[1] if len(parts)>1 else ""); return
    if data.startswith("lv3_"):
        parts = data.replace("lv3_","").split("|")
        vendor=parts[0]; product=parts[1] if len(parts)>1 else ""; mode=parts[2] if len(parts)>2 else "all"
        await do_download_labels(update, get_label_queue_by_vendor_product(vendor, product, advance_only=(mode=="adv"))); return

    if data.startswith("rep_"):
        r = get_payment_report()
        map_ = {"rep_pending":("⏳ Pending",r["pending"]),"rep_advance":("✅ Advance",r["advance"]),
                "rep_fullcod":("💵 Full COD",r["full_cod"]),"rep_all":("📋 All",r["pending"]+r["advance"]+r["full_cod"]+r["nothing"])}
        title, orders = map_.get(data,("",""))
        if not orders: await q.message.reply_text(f"{title}: none"); return
        lines = [f"{title} ({len(orders)})\n"]
        for o in orders[:30]:
            vm = o.get("manual") or {}; vendor = vm.get("vendor") or o.get("pickup_location") or "SR"
            lines.append(f"#{o.get('order_number')} {o.get('customer_name','')} | {o.get('phone','')} | C:₹{o.get('courier_paid',0)} A:{'₹'+str(o.get('advance_paid')) if o.get('advance_paid') is not None else '—'} | {vendor}")
        await q.message.reply_text("\n".join(lines)); return

    if data.startswith("cr_"):
        period = {"cr_today":"today","cr_yesterday":"yesterday","cr_all":"all"}.get(data,"today")
        missing = get_missing_creative(period)
        if not missing: await q.message.reply_text(f"✅ No missing ({period})"); return
        lines = [f"🎨 Missing — {period} ({len(missing)})\n"]
        for o in missing[:25]: lines.append(f"#{o.get('order_number')} {o.get('customer_name','')} — {o.get('phone','')}")
        lines.append("\nUse: /setcreative <phone> <code>")
        await q.message.reply_text("\n".join(lines)); return

    if data == "bulk_confirm_go":
        await do_bulk_execute(update, ctx); return

    if data == "bulk_cancel":
        await q.message.reply_text("Cancelled", reply_markup=MAIN_KB); ud.clear(); return

    if data == "prod_add":
        ud["state"] = "prod_add"; await q.message.reply_text("Send: Name length breadth height weight"); return
    if data.startswith("prod_del_"):
        name = data.replace("prod_del_","")
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        products.pop(name,None); json.dump(products, open(PRODUCTS_FILE,"w"), indent=2)
        await q.edit_message_text(f"🗑 Deleted: {name}"); return
    if data.startswith("prod_edit_"):
        ud["state"] = "prod_add"; await q.message.reply_text("New details:\nName l b h w"); return

# ─── MAIN ─────────────────────────────────
async def main():
    log.info("Starting bot...")
    get_token(); log.info("Shiprocket OK")
    refresh_pickups()
    log.info("Syncing from sheets...")
    sync_from_sheets()
    log.info("Sync done")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("adsspend",    cmd_adsspend))
    app.add_handler(CommandHandler("orders",      cmd_orders))
    app.add_handler(CommandHandler("report",      cmd_report))
    app.add_handler(CommandHandler("setcreative", cmd_setcreative))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    log.info("Bot running...")
    await app.run_polling()

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())