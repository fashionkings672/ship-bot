"""
bot_enhanced.py — Oneboxx Ship Bot (Final)
"""

import os, re, json, uuid, time, logging, asyncio, aiohttp
import requests
from datetime import datetime, date, timedelta

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, CallbackQueryHandler, filters
)
import openai

from orders_manager import (
    save_order, find_by_phone, find_by_awb, update_order, update_order_by_id,
    next_order_number, calc_cod, is_standard_preset, payment_status,
    get_label_queue, get_label_queue_by_vendor, get_all_vendors, mark_label_downloaded,
    get_payment_report, get_missing_creative, set_creative, get_creative_stats,
    get_today_stats, get_week_stats, log_adsspend, log_campaign_orders, get_today_ads,
    format_order, load_orders, save_orders
)

# ─── CONFIG ───────────────────────────────
BOT_TOKEN        = os.getenv("BOT_TOKEN_2")
SHIPROCKET_EMAIL = os.getenv("SHIPROCKET_EMAIL")
SHIPROCKET_PASS  = os.getenv("SHIPROCKET_PASSWORD")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")

print("="*50)
for k,v in [("BOT_TOKEN_2",BOT_TOKEN),("SHIPROCKET",SHIPROCKET_EMAIL),
            ("OPENAI",OPENAI_API_KEY),("SHEETS",os.getenv("GOOGLE_SHEET_ID"))]:
    print(f"  {k}: {'✅' if v else '❌ MISSING'}")
print("="*50)

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
PRODUCTS_FILE        = "products.json"
COURIER_PRIORITY_FILE = "courier_priority.json"

def get_token(force=False):
    global _token, _token_exp
    if not force and _token and time.time() < _token_exp:
        return _token
    r = session.post(f"{SR_BASE}/auth/login",
                     json={"email": SHIPROCKET_EMAIL, "password": SHIPROCKET_PASS},
                     timeout=60)
    data = r.json()
    if "token" not in data:
        raise Exception(f"SR login failed: {data}")
    _token = data["token"]
    _token_exp = time.time() + 23*3600
    session.headers.update({"Authorization": f"Bearer {_token}"})
    return _token

def ensure_token():
    try:
        get_token()
    except Exception:
        get_token(force=True)

def refresh_pickups():
    global _pickups
    ensure_token()
    r = session.get(f"{SR_BASE}/settings/company/pickup", timeout=60)
    lst = r.json().get("data",{}).get("shipping_address",[])
    _pickups = {p["pickup_location"].lower(): p for p in lst if p.get("pickup_location")}
    log.info(f"Pickups loaded: {list(_pickups.keys())}")

def resolve_pickup(name):
    if not name:
        return next(iter(_pickups.values()), None)
    key = re.sub(r"\W","",str(name).lower())
    for k, v in _pickups.items():
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
        r = sr_get("/courier/serviceability/", {
            "pickup_postcode": pp, "delivery_postcode": dp,
            "cod": int(bool(cod)), "weight": weight
        })
        return r.get("data",{}).get("available_courier_companies",[]) or []
    except Exception:
        return []

def priority_rank(courier_name):
    if not os.path.exists(COURIER_PRIORITY_FILE):
        return 999
    prio = json.load(open(COURIER_PRIORITY_FILE))
    name = courier_name.lower()
    for k, v in prio.items():
        if k.lower() in name or name in k.lower():
            return v
    return 999

def assign_awb(shipment_id, courier_id=None):
    payload = {"shipment_id": shipment_id}
    if courier_id:
        payload["courier_id"] = courier_id
    r = sr_post("/courier/assign/awb", payload)
    if r.get("awb_assign_status") == 1:
        return r["response"]["data"]["awb_code"]
    return None

def generate_label(shipment_id):
    try:
        r = sr_post("/courier/generate/label", {"shipment_id": [shipment_id]})
        if r.get("label_created") == 1:
            return r.get("label_url")
    except Exception:
        pass
    return None

def schedule_pickup(shipment_ids):
    try:
        r = sr_post("/courier/generate/pickup", {"shipment_id": shipment_ids})
        if r.get("pickup_scheduled") or r.get("status") == 1:
            return True, "✅ Pickup scheduled"
        return False, str(r)
    except Exception as e:
        return False, str(e)

def cancel_sr_order(sr_order_id):
    """FIX: correct cancel endpoint"""
    try:
        ensure_token()
        r = session.post(f"{SR_BASE}/orders/cancel",
                         json={"ids": [str(sr_order_id)]}, timeout=30)
        resp = r.json()
        if r.status_code == 200 or "success" in str(resp).lower():
            return True, "Cancelled ✅"
        return False, str(resp)
    except Exception as e:
        return False, str(e)

# ─── AI PARSER ────────────────────────────

def ai_parse(text):
    prompt = f"""Extract from this order text. Output EXACTLY this format:

Pickup: <pickup_location>
Product: <product_name>
Name: <full_name>
Address: <street>
City: <city>
State: <state>
Pincode: <6digit>
Phone: <10digit>
COD: <amount_number_only>
Creative: <creative_code_or_blank>

Text:
{text}"""
    resp = openai.chat.completions.create(
        model="gpt-4",
        messages=[{"role":"user","content":prompt}],
        temperature=0.1
    )
    return resp.choices[0].message.content.strip()

def parse_fields(text):
    data = {}
    for line in text.splitlines():
        if ":" in line:
            k, v = line.split(":",1)
            data[k.strip().lower()] = v.strip()
    return data

# ─── KEYBOARDS ────────────────────────────

MAIN_KB = ReplyKeyboardMarkup([
    ["➕ Create Shipment",  "🔍 Search Order"],
    ["💰 Mark Advance",     "📥 Download Labels"],
    ["📝 Manual Entry",     "📊 Payment Report"],
    ["🎨 Creative",         "📦 Products"],
], resize_keyboard=True)

def order_action_kb(order_id, phone):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("💰 Advance",    callback_data=f"adv_start_{phone}"),
        InlineKeyboardButton("🚚 Manual AWB", callback_data=f"manual_start_{phone}"),
        InlineKeyboardButton("❌ Cancel",      callback_data=f"cancel_sr_{order_id}"),
    ]])

# ─── /start ───────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "🚀 *Oneboxx Ship Bot*\n\n"
        "Commands:\n"
        "/adsspend — log ad spend\n"
        "/orders — log campaign orders\n"
        "/report — daily report\n"
        "/setcreative — set creative on order",
        parse_mode="Markdown",
        reply_markup=MAIN_KB
    )

# ─── COMMANDS ─────────────────────────────

async def cmd_adsspend(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = " ".join(ctx.args).strip()
    if not args:
        await update.message.reply_text(
            "Usage:\n/adsspend 3300\n/adsspend BANG:500 KOLAR:400 TUM:300")
        return
    if ":" in args:
        breakdown = {}
        for p in args.split():
            if ":" in p:
                k, v = p.split(":",1)
                try: breakdown[k.upper()] = float(v)
                except: pass
        data = log_adsspend(breakdown=breakdown)
        lines = [f"✅ Spend logged\n"]
        for k,v in data.items():
            if k.startswith("spend_"):
                lines.append(f"  {k.replace('spend_','')}: ₹{v}")
        lines.append(f"  Total: ₹{data.get('total_spend',0)}")
        await update.message.reply_text("\n".join(lines))
    else:
        try:
            data = log_adsspend(total=float(args))
            await update.message.reply_text(f"✅ Spend: ₹{data['total_spend']}")
        except:
            await update.message.reply_text("Invalid. Use /adsspend 3300")

async def cmd_orders(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = " ".join(ctx.args).strip()
    if not args:
        await update.message.reply_text(
            "Usage:\n/orders BANG:4 KOLAR:2 TUM:4 RAM:3 INT:2 MYS:1 NKA:1")
        return
    breakdown = {}
    for p in args.split():
        if ":" in p:
            k,v = p.split(":",1)
            try: breakdown[k.upper()] = int(v)
            except: pass
    data  = log_campaign_orders(breakdown)
    total = data.get("total_campaign_orders",0)
    spend = data.get("total_spend",0)
    cpo   = data.get("cpo",0)
    lines = [f"✅ Orders logged — {date.today()}\n"]
    for k,v in data.items():
        if k.startswith("orders_"):
            lines.append(f"  {k.replace('orders_','')}: {v}")
    lines.append(f"\nTotal: {total}")
    if cpo:
        lines.append(f"Spend: ₹{spend}")
        lines.append(f"CPO: ₹{cpo}")
        if cpo < 150:   lines.append("⭐ Excellent — scale +20%")
        elif cpo > 350: lines.append("⚠️ High — review campaigns")
    await update.message.reply_text("\n".join(lines))

async def cmd_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t    = get_today_stats()
    w    = get_week_stats()
    ads  = get_today_ads()
    spend = ads.get("total_spend",0)
    camp_orders = ads.get("total_campaign_orders",0)
    cpo  = ads.get("cpo",0)
    lines = [
        f"📊 *DAILY REPORT — {date.today()}*",
        f"————————————————",
        f"📦 Orders: {t['total']}",
        f"💰 Advance paid: {t['advance_paid']}",
        f"💵 Full COD: {t['full_cod']}",
        f"⏳ Courier only: {t['courier_only']}",
        f"❌ Nothing: {t['nothing']}",
        f"",
        f"📢 Spend: ₹{spend}",
        f"🎯 CPO: {'₹'+str(cpo) if cpo else '—'}",
    ]
    camps = {k.replace("orders_",""):v for k,v in ads.items() if k.startswith("orders_")}
    if camps:
        lines.append("\n🏙 Campaigns:")
        for c,o in sorted(camps.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  {c}: {o}")
    lines += [f"\n📅 Week: {w['total']} orders | {w['conv_rate']}% paid"]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_setcreative(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) == 0:
        # Show all missing
        missing = get_missing_creative("all")
        if not missing:
            await update.message.reply_text("✅ All orders have creative set")
            return
        lines = [f"🎨 No creative set ({len(missing)}):\n"]
        for o in missing[:20]:
            lines.append(f"#{o.get('order_number')} {o.get('customer_name','')} — {o.get('phone','')}")
        lines.append("\nUse: /setcreative <phone> <code>")
        await update.message.reply_text("\n".join(lines))
    elif len(ctx.args) == 2:
        phone, code = ctx.args
        o = set_creative(phone, code)
        if o:
            await update.message.reply_text(f"✅ Creative set to {code.upper()} for #{o.get('order_number')}")
        else:
            await update.message.reply_text("❌ Order not found")
    else:
        await update.message.reply_text("Usage: /setcreative <phone> <code>")

# ─── MESSAGE HANDLER ──────────────────────

async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    ud   = ctx.user_data
    state = ud.get("state")

    # ── Button routing ──────────────────
    routes = {
        "➕ Create Shipment": "create",
        "🔍 Search Order":    "search",
        "💰 Mark Advance":    "adv_phone",
        "📥 Download Labels": "labels",
        "📝 Manual Entry":    "manual_menu",
        "📊 Payment Report":  "pay_report",
        "🎨 Creative":        "creative_menu",
        "📦 Products":        "products",
    }
    if text in routes:
        ud.clear()
        action = routes[text]

        if action == "labels":
            await show_label_menu(update, ctx)
            return
        if action == "pay_report":
            await show_payment_report(update, ctx)
            return
        if action == "creative_menu":
            await show_creative_menu(update, ctx)
            return
        if action == "products":
            await show_products(update, ctx)
            return
        if action == "manual_menu":
            ud["state"] = "manual_phone"
            await update.message.reply_text("Enter phone number:", reply_markup=MAIN_KB)
            return

        ud["state"] = action
        prompts = {
            "create":    "Send order details:",
            "search":    "Enter phone or AWB:",
            "adv_phone": "Enter phone number:",
        }
        await update.message.reply_text(prompts[action], reply_markup=MAIN_KB)
        return

    # ── State machine ────────────────────
    if state == "create":
        await do_create(update, ctx, text)
        return

    if state == "search":
        await do_search(update, ctx, text)
        return

    if state == "adv_phone":
        await do_adv_phone(update, ctx, text)
        return

    if state == "adv_courier":
        try:
            amt = int(text)
            ud["courier_paid"] = amt
        except:
            await update.message.reply_text("Enter number only (e.g. 300)")
            return
        await ask_advance_amount(update, ctx)
        return

    if state == "adv_custom_advance":
        try:
            amt = int(text)
        except:
            await update.message.reply_text("Enter number only")
            return
        await do_mark_advance(update, ctx, amt)
        return

    if state == "manual_phone":
        o = find_by_phone(text)
        if not o:
            await update.message.reply_text("❌ Order not found", reply_markup=MAIN_KB)
            ud.clear()
            return
        ud["manual_phone"] = text
        ud["manual_order"] = o
        sr = o.get("shiprocket") or {}
        if sr.get("awb"):
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes cancel + manual", callback_data="manual_cancel_yes"),
                InlineKeyboardButton("❌ No", callback_data="manual_cancel_no"),
            ]])
            await update.message.reply_text(
                f"Active Shiprocket shipment found:\nAWB: {sr.get('awb')} — {sr.get('courier','')}\n\nCancel and add manual?",
                reply_markup=kb
            )
        else:
            ud["state"] = "manual_vendor"
            await update.message.reply_text("Enter vendor name:")
        return

    if state == "manual_vendor":
        ud["manual_vendor"] = text
        ud["state"] = "manual_courier"
        await update.message.reply_text("Enter courier name:")
        return

    if state == "manual_courier":
        ud["manual_courier_name"] = text
        ud["state"] = "manual_awb"
        await update.message.reply_text("Enter AWB number:")
        return

    if state == "manual_awb":
        phone  = ud.get("manual_phone")
        vendor = ud.get("manual_vendor","")
        courier= ud.get("manual_courier_name","")
        o = update_order(phone,
            manual={"vendor": vendor, "courier": courier, "awb": text,
                    "added_at": datetime.now().isoformat()},
            status="manual"
        )
        if o:
            await update.message.reply_text(
                f"✅ Manual shipment saved\n"
                f"Vendor: {vendor}\nCourier: {courier}\nAWB: {text}",
                reply_markup=MAIN_KB
            )
        else:
            await update.message.reply_text("❌ Failed", reply_markup=MAIN_KB)
        ud.clear()
        return

    if state == "prod_add":
        await do_add_product(update, ctx, text)
        return

    await update.message.reply_text("Use the buttons ⬇️", reply_markup=MAIN_KB)

# ─── CREATE SHIPMENT ──────────────────────

async def do_create(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    msg = await update.message.reply_text("⏳ Processing with AI...")
    try:
        parsed = ai_parse(text)
        d      = parse_fields(parsed)

        # Product dimensions
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        prod_name = d.get("product","Projector")
        prod = products.get(prod_name, {"length":20,"breadth":15,"height":10,"weight":0.5})

        # COD from AI
        try:
            cod_amount = float(re.sub(r"[^\d.]","", d.get("cod","2400") or "2400"))
        except:
            cod_amount = 2400.0

        # Pickup
        pickup_obj = resolve_pickup(d.get("pickup",""))
        if not pickup_obj:
            await msg.edit_text("❌ Pickup location not found. Check Shiprocket pickup setup.")
            ctx.user_data.clear()
            return

        pickup_pin   = str(pickup_obj.get("pin_code","560001"))
        delivery_pin = str(d.get("pincode","560001"))

        order_id = f"OBX{int(time.time())}_{uuid.uuid4().hex[:5]}"
        payload  = {
            "order_id":               order_id,
            "order_date":             datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "pickup_location":        pickup_obj.get("pickup_location"),
            "billing_customer_name":  d.get("name","Customer"),
            "billing_last_name":      ".",
            "billing_address":        d.get("address",""),
            "billing_city":           d.get("city",""),
            "billing_state":          d.get("state","Karnataka"),
            "billing_country":        "India",
            "billing_pincode":        delivery_pin,
            "billing_email":          "orders@oneboxx.in",
            "billing_isd_code":       "91",
            "billing_phone":          d.get("phone",""),
            "shipping_is_billing":    True,
            "order_items": [{
                "name": prod_name, "sku": prod_name,
                "units": 1, "selling_price": cod_amount,
                "discount":"0","tax":"0","hsn":""
            }],
            "payment_method": "COD",
            "sub_total":      cod_amount,
            "cod_amount":     cod_amount,
            "length":  float(prod["length"]),
            "breadth": float(prod["breadth"]),
            "height":  float(prod["height"]),
            "weight":  float(prod["weight"]),
        }

        await msg.edit_text("⏳ Creating on Shiprocket...")
        ensure_token()
        r = session.post(f"{SR_BASE}/orders/create/adhoc", json=payload, timeout=45)
        if r.status_code != 200:
            body = r.text
            if "wallet" in body.lower() or "balance" in body.lower():
                await msg.edit_text("❌ Insufficient wallet balance\nRecharge: https://app.shiprocket.in/wallet")
            elif "pincode" in body.lower() or "invalid" in body.lower():
                await msg.edit_text(f"❌ Invalid pincode: {delivery_pin}\nCheck address and retry.")
            else:
                await msg.edit_text(f"❌ Order failed: {body[:200]}")
            ctx.user_data.clear()
            return

        resp        = r.json()
        shipment_id = resp.get("shipment_id")

        await msg.edit_text("⏳ Assigning courier...")
        couriers = get_couriers(pickup_pin, delivery_pin, prod["weight"], True)
        if not couriers:
            await msg.edit_text(
                f"❌ No courier serviceable for {delivery_pin}\n\n"
                f"Options:\n📝 Use Manual Entry\n❌ Cancel order"
            )
            ctx.user_data.clear()
            return

        sorted_couriers = sorted(couriers, key=lambda c: priority_rank(c.get("courier_name","")))
        awb = None
        chosen = None
        fallback = False

        for c in sorted_couriers:
            cid = c.get("courier_company_id") or c.get("courier_id")
            if priority_rank(c.get("courier_name","")) == 999:
                fallback = True
            awb = assign_awb(shipment_id, cid)
            if awb:
                chosen = c
                break

        if not awb:
            await msg.edit_text("❌ Courier assignment failed. Try again.")
            ctx.user_data.clear()
            return

        tracking    = f"https://shiprocket.co/tracking/{awb}"
        order_num   = next_order_number()
        courier_note= "⚠️ Fallback courier" if fallback else "✅ Priority"

        order_record = {
            "order_id":       order_id,
            "order_number":   order_num,
            "created_at":     datetime.now().isoformat(),
            "phone":          d.get("phone",""),
            "customer_name":  d.get("name",""),
            "address":        d.get("address",""),
            "city":           d.get("city",""),
            "state":          d.get("state","Karnataka"),
            "pincode":        delivery_pin,
            "product":        prod_name,
            "creative":       d.get("creative",""),
            "total":          cod_amount,
            "cod_amount":     cod_amount,
            "courier_paid":   None,
            "advance_paid":   None,
            "status":         "active",
            "pickup_location":pickup_obj.get("pickup_location",""),
            "shiprocket": {
                "order_id":   resp.get("order_id",""),
                "shipment_id":shipment_id,
                "awb":        awb,
                "courier":    chosen.get("courier_name",""),
                "rate":       chosen.get("rate",0),
                "tracking":   tracking,
            },
            "manual": None,
            "label_downloaded": False,
            "label_downloaded_date": "",
        }
        save_order(order_record)

        await msg.edit_text(
            f"✅ *Shipment Created!*\n"
            f"————————————————\n"
            f"Order:    #{order_num}\n"
            f"Name:     {d.get('name','')}\n"
            f"Phone:    {d.get('phone','')}\n"
            f"City:     {d.get('city','')}, {delivery_pin}\n"
            f"Product:  {prod_name}\n"
            f"Creative: {d.get('creative','—')}\n"
            f"COD:      ₹{int(cod_amount):,}\n"
            f"————————————————\n"
            f"Courier:  {chosen.get('courier_name','')} {courier_note}\n"
            f"Rate:     ₹{chosen.get('rate',0)}\n"
            f"AWB:      `{awb}`\n"
            f"Tracking: {tracking}",
            parse_mode="Markdown"
        )

        # Send label PDF
        label_url = generate_label(shipment_id)
        if label_url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(label_url) as resp2:
                        if resp2.status == 200:
                            pdf = await resp2.read()
                            await update.message.reply_document(
                                document=pdf, filename=f"{awb}.pdf",
                                caption=f"📄 Label — {d.get('name','')} | {awb}"
                            )
            except Exception as e:
                log.error(f"Label: {e}")

        # Pickup prompt
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes", callback_data=f"pickup_yes_{shipment_id}_{order_id}"),
            InlineKeyboardButton("❌ No",  callback_data="pickup_no"),
        ]])
        await update.message.reply_text("Schedule pickup?", reply_markup=kb)

    except Exception as e:
        log.error(f"Create: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")
    finally:
        ctx.user_data.clear()

# ─── SEARCH ───────────────────────────────

async def do_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    o = find_by_phone(text) if re.match(r"^\d{10}$",text.strip()) else find_by_awb(text)
    if o:
        await update.message.reply_text(
            format_order(o),
            reply_markup=order_action_kb(o.get("order_id",""), o.get("phone",""))
        )
    else:
        await update.message.reply_text("❌ No order found", reply_markup=MAIN_KB)
    ctx.user_data.clear()

# ─── ADVANCE PAYMENT ──────────────────────

async def do_adv_phone(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    o = find_by_phone(text)
    if not o:
        await update.message.reply_text("❌ Order not found", reply_markup=MAIN_KB)
        ctx.user_data.clear()
        return
    ctx.user_data["adv_phone"]  = text
    ctx.user_data["adv_order"]  = o
    ctx.user_data["state"]      = "adv_courier"
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("₹200", callback_data="cour_200"),
        InlineKeyboardButton("₹300", callback_data="cour_300"),
        InlineKeyboardButton("Custom", callback_data="cour_custom"),
    ]])
    await update.message.reply_text(
        f"📦 #{o.get('order_number')} — {o.get('customer_name','')}\n"
        f"COD: ₹{int(o.get('cod_amount',2400)):,}\n\n"
        f"Courier charge paid:",
        reply_markup=kb
    )

async def ask_advance_amount(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["state"] = "adv_picking_advance"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("₹400", callback_data="adv_400"),
         InlineKeyboardButton("₹500", callback_data="adv_500"),
         InlineKeyboardButton("₹600", callback_data="adv_600"),
         InlineKeyboardButton("₹700", callback_data="adv_700")],
        [InlineKeyboardButton("Custom", callback_data="adv_custom"),
         InlineKeyboardButton("₹0 Full COD", callback_data="adv_0")],
    ])
    c = ctx.user_data.get("courier_paid",0)
    await update.message.reply_text(f"Courier: ₹{c} ✅\n\nAdvance paid:", reply_markup=kb)

async def do_mark_advance(update: Update, ctx: ContextTypes.DEFAULT_TYPE, advance_amt: int):
    ud    = ctx.user_data
    phone = ud.get("adv_phone")
    o     = ud.get("adv_order")
    c     = ud.get("courier_paid", 0)
    a     = advance_amt
    new_cod = calc_cod(c, a)
    standard = is_standard_preset(c, a)

    if standard:
        # No rebook — just mark
        update_order(phone, courier_paid=c, advance_paid=a, cod_amount=new_cod)
        msg = (
            f"✅ Payment marked\n"
            f"Courier: ₹{c} | Advance: ₹{a}\n"
            f"COD on delivery: ₹{new_cod:,}\n"
            f"No rebook needed ✅"
        )
        await update.message.reply_text(msg, reply_markup=MAIN_KB)
        ud.clear()
    else:
        # Need rebook
        ud["new_cod"]    = new_cod
        ud["new_advance"]= a
        ud["new_courier"]= c
        ud["state"]      = "adv_confirm_rebook"
        sr = (o.get("shiprocket") or {})
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Confirm rebook", callback_data="rebook_yes"),
            InlineKeyboardButton("❌ Cancel",         callback_data="rebook_no"),
        ]])
        await update.message.reply_text(
            f"⚠️ Non-standard combo\n"
            f"Courier: ₹{c} | Advance: ₹{a}\n"
            f"New COD: ₹{new_cod:,}\n\n"
            f"Current AWB: {sr.get('awb','—')}\n"
            f"Cancel + rebook with new COD?",
            reply_markup=kb
        )

# ─── LABELS ───────────────────────────────

async def show_label_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    queue   = get_label_queue()
    vendors = get_all_vendors()
    if not queue:
        await update.message.reply_text("📥 No labels pending download", reply_markup=MAIN_KB)
        return
    kb_rows = [[
        InlineKeyboardButton(f"📥 All ({len(queue)})", callback_data="label_all"),
    ]]
    for v in vendors:
        vq = get_label_queue_by_vendor(v)
        if vq:
            kb_rows.append([InlineKeyboardButton(
                f"🏪 {v} ({len(vq)})", callback_data=f"label_vendor_{v}"
            )])
    await update.message.reply_text(
        f"📥 *Download Labels*\n{len(queue)} pending",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb_rows)
    )

async def do_download_labels(update: Update, orders: list):
    if not orders:
        await update.callback_query.message.reply_text("No labels in this group")
        return
    await update.callback_query.message.reply_text(f"⏳ Generating {len(orders)} labels...")
    downloaded = 0
    for o in orders:
        sr = o.get("shiprocket") or {}
        sid = sr.get("shipment_id")
        if not sid:
            continue
        url = generate_label(sid)
        if url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(url) as r:
                        if r.status == 200:
                            pdf = await r.read()
                            await update.callback_query.message.reply_document(
                                document=pdf,
                                filename=f"{sr.get('awb','label')}.pdf",
                                caption=f"#{o.get('order_number')} — {o.get('customer_name','')} | {o.get('city','')}"
                            )
                            mark_label_downloaded(o.get("order_id",""))
                            downloaded += 1
            except Exception as e:
                log.error(f"Label DL: {e}")
    await update.callback_query.message.reply_text(
        f"✅ {downloaded}/{len(orders)} labels downloaded\nWill not show tomorrow.",
        reply_markup=MAIN_KB
    )

# ─── PAYMENT REPORT ───────────────────────

async def show_payment_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏳ Pending Advance", callback_data="rep_pending"),
         InlineKeyboardButton("✅ Advance Paid",    callback_data="rep_advance")],
        [InlineKeyboardButton("💵 Full COD",        callback_data="rep_fullcod"),
         InlineKeyboardButton("📋 All Orders",      callback_data="rep_all")],
    ])
    r = get_payment_report()
    await update.message.reply_text(
        f"📊 *Payment Report*\n\n"
        f"⏳ Pending: {len(r['pending'])}\n"
        f"✅ Advance: {len(r['advance'])}\n"
        f"💵 Full COD: {len(r['full_cod'])}\n"
        f"❌ Nothing: {len(r['nothing'])}",
        parse_mode="Markdown",
        reply_markup=kb
    )

# ─── CREATIVE MENU ────────────────────────

async def show_creative_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Today",     callback_data="cr_today"),
        InlineKeyboardButton("Yesterday", callback_data="cr_yesterday"),
        InlineKeyboardButton("All time",  callback_data="cr_all"),
    ]])
    await update.message.reply_text("🎨 Creative — missing orders:", reply_markup=kb)

# ─── PRODUCTS ─────────────────────────────

async def show_products(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
    if not products:
        ctx.user_data["state"] = "prod_add"
        await update.message.reply_text(
            "No products yet.\nSend: Name length breadth height weight\nExample: Projector 20 15 10 0.5"
        )
        return
    for name, p in products.items():
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✏️ Edit",   callback_data=f"prod_edit_{name}"),
            InlineKeyboardButton("🗑 Delete", callback_data=f"prod_del_{name}"),
        ]])
        await update.message.reply_text(
            f"*{name}*\n{p['length']}×{p['breadth']}×{p['height']}cm | {p['weight']}kg",
            parse_mode="Markdown", reply_markup=kb
        )
    await update.message.reply_text(
        "Products ↑",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Add Product", callback_data="prod_add")
        ]])
    )

async def do_add_product(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    parts = text.strip().split()
    if len(parts) < 5:
        await update.message.reply_text("Format: Name length breadth height weight")
        return
    try:
        l,b,h,w = float(parts[-4]),float(parts[-3]),float(parts[-2]),float(parts[-1])
        name = " ".join(parts[:-4])
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        products[name] = {"length":l,"breadth":b,"height":h,"weight":w}
        json.dump(products, open(PRODUCTS_FILE,"w"), indent=2)
        await update.message.reply_text(f"✅ Saved: {name}", reply_markup=MAIN_KB)
    except:
        await update.message.reply_text("Invalid. Format: Name l b h w", reply_markup=MAIN_KB)
    ctx.user_data.clear()

# ─── CALLBACKS ────────────────────────────

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    await q.answer()
    data = q.data or ""
    ud   = ctx.user_data

    # Courier amount
    if data.startswith("cour_"):
        val = data.replace("cour_","")
        if val == "custom":
            ud["state"] = "adv_courier"
            await q.message.reply_text("Enter courier amount:")
            return
        ud["courier_paid"] = int(val)
        await ask_advance_amount(q, ctx)
        return

    # Advance amount
    if data.startswith("adv_"):
        val = data.replace("adv_","")
        if val == "custom":
            ud["state"] = "adv_custom_advance"
            await q.message.reply_text("Enter advance amount:")
            return
        await do_mark_advance(q, ctx, int(val))
        return

    # From search result — start advance
    if data.startswith("adv_start_"):
        phone = data.replace("adv_start_","")
        o = find_by_phone(phone)
        if o:
            ud["adv_phone"] = phone
            ud["adv_order"] = o
            ud["state"]     = "adv_courier"
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("₹200", callback_data="cour_200"),
                InlineKeyboardButton("₹300", callback_data="cour_300"),
                InlineKeyboardButton("Custom", callback_data="cour_custom"),
            ]])
            await q.message.reply_text(
                f"#{o.get('order_number')} — {o.get('customer_name','')}\nCourier paid:",
                reply_markup=kb
            )
        return

    # Rebook confirm
    if data == "rebook_yes":
        await do_rebook(q, ctx)
        return
    if data == "rebook_no":
        await q.message.reply_text("Cancelled", reply_markup=MAIN_KB)
        ud.clear()
        return

    # Pickup
    if data.startswith("pickup_yes_"):
        parts       = data.replace("pickup_yes_","").split("_",1)
        shipment_id = parts[0]
        order_id    = parts[1] if len(parts)>1 else ""
        ok, msg = schedule_pickup([shipment_id])
        if ok and order_id:
            update_order_by_id(order_id, pickup_scheduled=True)
        await q.edit_message_text(msg)
        return
    if data == "pickup_no":
        await q.edit_message_text("Pickup not scheduled")
        return

    # Cancel shipment from search
    if data.startswith("cancel_sr_"):
        order_id = data.replace("cancel_sr_","")
        orders = load_orders()
        o = next((x for x in orders if x.get("order_id")==order_id), None)
        if o:
            sr = o.get("shiprocket") or {}
            ok, msg = cancel_sr_order(sr.get("order_id","") or sr.get("shipment_id",""))
            if ok:
                update_order_by_id(order_id, status="cancelled")
                await q.message.reply_text(f"✅ Shipment cancelled\nOrder #{o.get('order_number')}", reply_markup=MAIN_KB)
            else:
                await q.message.reply_text(f"❌ Cancel failed: {msg}", reply_markup=MAIN_KB)
        return

    # Manual from search — cancel confirm
    if data == "manual_cancel_yes":
        o  = ud.get("manual_order",{})
        sr = (o.get("shiprocket") or {})
        cancel_sr_order(sr.get("order_id","") or sr.get("shipment_id",""))
        ud["state"] = "manual_vendor"
        await q.message.reply_text("✅ Shiprocket cancelled\n\nEnter vendor name:")
        return
    if data == "manual_cancel_no":
        await q.message.reply_text("Cancelled", reply_markup=MAIN_KB)
        ud.clear()
        return

    # From search — manual AWB
    if data.startswith("manual_start_"):
        phone = data.replace("manual_start_","")
        o = find_by_phone(phone)
        if o:
            ud["manual_phone"] = phone
            ud["manual_order"] = o
            sr = o.get("shiprocket") or {}
            if sr.get("awb"):
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Yes cancel + manual", callback_data="manual_cancel_yes"),
                    InlineKeyboardButton("❌ No", callback_data="manual_cancel_no"),
                ]])
                await q.message.reply_text(
                    f"AWB: {sr.get('awb')} — {sr.get('courier','')}\nCancel + add manual?",
                    reply_markup=kb
                )
            else:
                ud["state"] = "manual_vendor"
                await q.message.reply_text("Enter vendor name:")
        return

    # Labels
    if data == "label_all":
        await do_download_labels(update, get_label_queue())
        return
    if data.startswith("label_vendor_"):
        vendor = data.replace("label_vendor_","")
        await do_download_labels(update, get_label_queue_by_vendor(vendor))
        return

    # Payment report
    if data.startswith("rep_"):
        r = get_payment_report()
        map_ = {
            "rep_pending": ("⏳ Pending Advance", r["pending"]),
            "rep_advance": ("✅ Advance Paid",    r["advance"]),
            "rep_fullcod": ("💵 Full COD",        r["full_cod"]),
            "rep_all":     ("📋 All",             r["pending"]+r["advance"]+r["full_cod"]+r["nothing"]),
        }
        title, orders = map_.get(data, ("", []))
        if not orders:
            await q.message.reply_text(f"{title}: none")
            return
        lines = [f"{title} ({len(orders)})\n"]
        for o in orders[:30]:
            c = o.get("courier_paid") or 0
            a = o.get("advance_paid")
            a_str = f"₹{a}" if a is not None else "—"
            lines.append(f"#{o.get('order_number')} {o.get('customer_name','')} | {o.get('phone','')} | Cour:₹{c} Adv:{a_str}")
        await q.message.reply_text("\n".join(lines))
        return

    # Creative missing
    if data.startswith("cr_"):
        period = data.replace("cr_","")
        period = {"today":"today","yesterday":"yesterday","all":"all"}.get(period,"today")
        missing = get_missing_creative(period)
        if not missing:
            await q.message.reply_text(f"✅ No missing creative ({period})")
            return
        lines = [f"🎨 Missing creative — {period} ({len(missing)})\n"]
        for o in missing[:25]:
            lines.append(f"#{o.get('order_number')} {o.get('customer_name','')} — {o.get('phone','')}")
        lines.append("\nUse: /setcreative <phone> <code>")
        await q.message.reply_text("\n".join(lines))
        return

    # Products
    if data == "prod_add":
        ctx.user_data["state"] = "prod_add"
        await q.message.reply_text("Send: Name length breadth height weight\nExample: Projector 20 15 10 0.5")
        return
    if data.startswith("prod_del_"):
        name = data.replace("prod_del_","")
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        products.pop(name, None)
        json.dump(products, open(PRODUCTS_FILE,"w"), indent=2)
        await q.edit_message_text(f"🗑 Deleted: {name}")
        return
    if data.startswith("prod_edit_"):
        name = data.replace("prod_edit_","")
        ctx.user_data["state"] = "prod_add"
        await q.message.reply_text(f"New details for '{name}':\nName l b h w")
        return

# ─── REBOOK ───────────────────────────────

async def do_rebook(q, ctx):
    ud      = ctx.user_data
    phone   = ud.get("adv_phone")
    o       = ud.get("adv_order")
    new_cod = ud.get("new_cod")
    c       = ud.get("new_courier",0)
    a       = ud.get("new_advance",0)
    sr      = (o.get("shiprocket") or {})

    await q.message.reply_text("⏳ Cancelling old shipment...")
    ok, msg = cancel_sr_order(sr.get("order_id","") or sr.get("shipment_id",""))
    if not ok:
        await q.message.reply_text(f"❌ Cancel failed: {msg}\nDo it manually in Shiprocket.", reply_markup=MAIN_KB)
        ud.clear()
        return

    await q.message.reply_text("⏳ Creating new shipment with updated COD...")
    products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
    prod_name = o.get("product","Projector")
    prod = products.get(prod_name, {"length":20,"breadth":15,"height":10,"weight":0.5})
    pickup_obj = resolve_pickup(o.get("pickup_location",""))
    if not pickup_obj:
        await q.message.reply_text("❌ Pickup not found", reply_markup=MAIN_KB)
        ud.clear()
        return

    pickup_pin   = str(pickup_obj.get("pin_code","560001"))
    delivery_pin = str(o.get("pincode","560001"))
    new_order_id = f"OBX{int(time.time())}_{uuid.uuid4().hex[:5]}"

    payload = {
        "order_id":               new_order_id,
        "order_date":             datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "pickup_location":        pickup_obj.get("pickup_location"),
        "billing_customer_name":  o.get("customer_name",""),
        "billing_last_name":      ".",
        "billing_address":        o.get("address",""),
        "billing_city":           o.get("city",""),
        "billing_state":          o.get("state","Karnataka"),
        "billing_country":        "India",
        "billing_pincode":        delivery_pin,
        "billing_email":          "orders@oneboxx.in",
        "billing_isd_code":       "91",
        "billing_phone":          o.get("phone",""),
        "shipping_is_billing":    True,
        "order_items": [{
            "name": prod_name, "sku": prod_name,
            "units": 1, "selling_price": new_cod,
            "discount":"0","tax":"0","hsn":""
        }],
        "payment_method": "COD",
        "sub_total":      new_cod,
        "cod_amount":     new_cod,
        "length":  float(prod["length"]),
        "breadth": float(prod["breadth"]),
        "height":  float(prod["height"]),
        "weight":  float(prod["weight"]),
    }

    ensure_token()
    r = session.post(f"{SR_BASE}/orders/create/adhoc", json=payload, timeout=45)
    if r.status_code != 200:
        await q.message.reply_text(f"❌ Rebook failed: {r.text[:200]}", reply_markup=MAIN_KB)
        ud.clear()
        return

    resp        = r.json()
    shipment_id = resp.get("shipment_id")
    couriers    = get_couriers(pickup_pin, delivery_pin, prod["weight"], True)
    sorted_c    = sorted(couriers, key=lambda c: priority_rank(c.get("courier_name","")))
    awb = None
    chosen = None
    for c_opt in sorted_c:
        cid = c_opt.get("courier_company_id") or c_opt.get("courier_id")
        awb = assign_awb(shipment_id, cid)
        if awb:
            chosen = c_opt
            break

    if not awb:
        await q.message.reply_text("❌ Courier assignment failed after rebook", reply_markup=MAIN_KB)
        ud.clear()
        return

    tracking = f"https://shiprocket.co/tracking/{awb}"

    # Update order record
    update_order(phone,
        order_id=new_order_id,
        cod_amount=new_cod,
        courier_paid=ud.get("new_courier",0),
        advance_paid=ud.get("new_advance",0),
        status="active",
        shiprocket={
            "order_id":   resp.get("order_id",""),
            "shipment_id":shipment_id,
            "awb":        awb,
            "courier":    chosen.get("courier_name",""),
            "rate":       chosen.get("rate",0),
            "tracking":   tracking,
        }
    )

    await q.message.reply_text(
        f"✅ Rebooked!\n"
        f"Old AWB: {sr.get('awb','')} — Cancelled\n"
        f"New AWB: {awb}\n"
        f"Courier: {chosen.get('courier_name','')}\n"
        f"New COD: ₹{int(new_cod):,}",
        reply_markup=MAIN_KB
    )

    # New label
    label_url = generate_label(shipment_id)
    if label_url:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(label_url) as resp2:
                    if resp2.status == 200:
                        pdf = await resp2.read()
                        await q.message.reply_document(
                            document=pdf, filename=f"{awb}.pdf",
                            caption=f"📄 New label — {o.get('customer_name','')} | {awb}"
                        )
        except Exception as e:
            log.error(f"Label rebook: {e}")

    ud.clear()

# ─── MAIN ─────────────────────────────────

async def main():
    log.info("Starting Oneboxx Ship Bot...")
    get_token()
    log.info("Shiprocket auth OK")
    refresh_pickups()

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("adsspend",     cmd_adsspend))
    app.add_handler(CommandHandler("orders",       cmd_orders))
    app.add_handler(CommandHandler("report",       cmd_report))
    app.add_handler(CommandHandler("setcreative",  cmd_setcreative))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    log.info("Bot running...")
    await app.run_polling()

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())