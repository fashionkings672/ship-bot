"""
bot_enhanced.py — Oneboxx Ship Bot (Final v3)
Confirmed flow changes:
  1. Create Shipment: AI parse → ask creative → ask courier → create
  2. Mark Advance: from Search only, courier pre-filled, Save or Rebook
  3. Rebook: ask new COD only → cancel old → new shipment
  4. Search result: 3 buttons — Advance | Manual AWB | Cancel
  5. Main menu: 6 buttons only
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
    next_order_number, calc_cod, payment_status,
    get_label_queue, get_label_queue_by_vendor, get_all_vendors, mark_label_downloaded,
    get_payment_report, get_missing_creative, set_creative,
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
PRODUCTS_FILE         = "products.json"
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
    try: get_token()
    except Exception: get_token(force=True)

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

def get_available_couriers_for_order(order):
    pickup_obj = resolve_pickup(order.get("pickup_location",""))
    if not pickup_obj:
        return []
    pickup_pin   = str(pickup_obj.get("pin_code","560001"))
    delivery_pin = str(order.get("pincode","560001"))
    products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
    prod = products.get(order.get("product","Projector"), {"weight":0.5})
    return get_couriers(pickup_pin, delivery_pin, prod["weight"], True)

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
    ["➕ Create Shipment", "🔍 Search Order"],
    ["📥 Download Labels", "📊 Payment Report"],
    ["🎨 Creative",        "📦 Products"],
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
        await update.message.reply_text("Usage:\n/adsspend 3300\n/adsspend BANG:500 KOLAR:400")
        return
    if ":" in args:
        breakdown = {}
        for p in args.split():
            if ":" in p:
                k, v = p.split(":",1)
                try: breakdown[k.upper()] = float(v)
                except: pass
        data = log_adsspend(breakdown=breakdown)
        lines = ["✅ Spend logged\n"]
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
        await update.message.reply_text("Usage:\n/orders BANG:4 KOLAR:2 TUM:4 RAM:3")
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
        lines.append(f"Spend: ₹{spend}\nCPO: ₹{cpo}")
        if cpo < 150:   lines.append("⭐ Excellent — scale +20%")
        elif cpo > 350: lines.append("⚠️ High — review campaigns")
    await update.message.reply_text("\n".join(lines))

async def cmd_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t   = get_today_stats()
    w   = get_week_stats()
    ads = get_today_ads()
    spend = ads.get("total_spend",0)
    cpo   = ads.get("cpo",0)
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
    text  = update.message.text.strip()
    ud    = ctx.user_data
    state = ud.get("state")

    routes = {
        "➕ Create Shipment": "create",
        "🔍 Search Order":    "search",
        "📥 Download Labels": "labels",
        "📊 Payment Report":  "pay_report",
        "🎨 Creative":        "creative_menu",
        "📦 Products":        "products",
    }
    if text in routes:
        ud.clear()
        action = routes[text]
        if action == "labels":        await show_label_menu(update, ctx); return
        if action == "pay_report":    await show_payment_report(update, ctx); return
        if action == "creative_menu": await show_creative_menu(update, ctx); return
        if action == "products":      await show_products(update, ctx); return
        ud["state"] = action
        await update.message.reply_text(
            "Send order details:" if action == "create" else "Enter phone or AWB:",
            reply_markup=MAIN_KB
        )
        return

    if state == "create":
        await do_create(update, ctx, text); return

    if state == "create_creative":
        ud["create_creative"] = "" if text.lower() == "skip" else text.upper()
        ud["state"] = "create_courier"
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("₹200",   callback_data="create_cour_200"),
            InlineKeyboardButton("₹300",   callback_data="create_cour_300"),
            InlineKeyboardButton("₹0",     callback_data="create_cour_0"),
            InlineKeyboardButton("Custom", callback_data="create_cour_custom"),
        ]])
        await update.message.reply_text(
            f"Creative: {ud['create_creative'] or '—'} ✅\n\nCourier amount charged?",
            reply_markup=kb
        )
        return

    if state == "create_courier_custom":
        try: ud["create_courier_amount"] = int(text)
        except:
            await update.message.reply_text("Enter number only"); return
        await do_create_shipment(update, ctx); return

    if state == "search":
        await do_search(update, ctx, text); return

    if state == "adv_custom":
        try: amt = int(text)
        except:
            await update.message.reply_text("Enter number only"); return
        await do_save_advance(update, ctx, amt); return

    if state == "adv_new_cod":
        try: new_cod = int(text)
        except:
            await update.message.reply_text("Enter number only"); return
        await do_rebook_new_cod(update, ctx, new_cod); return

    if state == "manual_vendor":
        ud["manual_vendor"] = text
        ud["state"] = "manual_courier"
        await update.message.reply_text("Enter courier name:"); return

    if state == "manual_courier":
        ud["manual_courier_name"] = text
        ud["state"] = "manual_awb"
        await update.message.reply_text("Enter AWB number:"); return

    if state == "manual_awb":
        phone  = ud.get("manual_phone")
        vendor = ud.get("manual_vendor","")
        courier= ud.get("manual_courier_name","")
        o = update_order(phone,
            manual={"vendor": vendor, "courier": courier, "awb": text,
                    "added_at": datetime.now().isoformat()},
            status="manual"
        )
        msg = f"✅ Manual saved\nVendor: {vendor}\nCourier: {courier}\nAWB: {text}" if o else "❌ Failed"
        await update.message.reply_text(msg, reply_markup=MAIN_KB)
        ud.clear(); return

    if state == "prod_add":
        await do_add_product(update, ctx, text); return

    if state == "reassign_select":
        try:
            idx = int(text.strip()) - 1
            couriers = ud.get("reassign_couriers", [])
            if idx < 0 or idx >= len(couriers):
                await update.message.reply_text("Invalid number. Try again:"); return
            await do_reassign_courier(update, ctx, couriers[idx])
        except:
            await update.message.reply_text("Enter the number only (e.g. 1)")
        return

    await update.message.reply_text("Use the buttons ⬇️", reply_markup=MAIN_KB)

# ─── CREATE — STEP 1 ──────────────────────

async def do_create(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    msg = await update.message.reply_text("⏳ Processing with AI...")
    try:
        parsed = ai_parse(text)
        d      = parse_fields(parsed)
        if not d.get("phone") or not d.get("pincode"):
            await msg.edit_text(
                "❌ Missing phone or pincode. Format:\n\n"
                "Name\nPhone\nAddress, City\nPincode\nProduct\nCOD amount"
            )
            ctx.user_data.clear(); return
        ctx.user_data["create_parsed"] = d
        ctx.user_data["state"] = "create_creative"
        await msg.edit_text(
            f"✅ Parsed:\n"
            f"Name: {d.get('name','')}\n"
            f"Phone: {d.get('phone','')}\n"
            f"City: {d.get('city','')}, {d.get('pincode','')}\n"
            f"Product: {d.get('product','')}\n"
            f"COD: ₹{d.get('cod','')}\n\n"
            f"Enter creative code:"
        )
    except Exception as e:
        log.error(f"AI parse: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")
        ctx.user_data.clear()

# ─── CREATE — STEP 2 ──────────────────────

async def do_create_shipment(update, ctx):
    ud              = ctx.user_data
    d               = ud.get("create_parsed", {})
    creative        = ud.get("create_creative", "")
    courier_charged = ud.get("create_courier_amount", 0)

    # handle both message and callback query
    reply = update.message if hasattr(update, 'message') and update.message else update.callback_query.message
    msg = await reply.reply_text("⏳ Creating on Shiprocket...")

    try:
        products  = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        prod_name = d.get("product","Projector")
        prod = products.get(prod_name, {"length":20,"breadth":15,"height":10,"weight":0.5})

        try: cod_amount = float(re.sub(r"[^\d.]","", d.get("cod","2400") or "2400"))
        except: cod_amount = 2400.0

        pickup_obj = resolve_pickup(d.get("pickup",""))
        if not pickup_obj:
            await msg.edit_text("❌ Pickup location not found.")
            ctx.user_data.clear(); return

        pickup_display = pickup_obj.get("pickup_location","")
        pickup_pin     = str(pickup_obj.get("pin_code","560001"))
        delivery_pin   = str(d.get("pincode","560001"))
        order_id       = f"OBX{int(time.time())}_{uuid.uuid4().hex[:5]}"

        payload = {
            "order_id":               order_id,
            "order_date":             datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "pickup_location":        pickup_display,
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
            "order_items": [{"name": prod_name, "sku": prod_name, "units": 1,
                             "selling_price": cod_amount, "discount":"0","tax":"0","hsn":""}],
            "payment_method": "COD",
            "sub_total": cod_amount, "cod_amount": cod_amount,
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

        resp        = r.json()
        shipment_id = resp.get("shipment_id")
        await msg.edit_text("⏳ Assigning courier...")

        couriers = get_couriers(pickup_pin, delivery_pin, prod["weight"], True)
        if not couriers:
            await msg.edit_text(f"❌ No courier for {delivery_pin}. Search order → Manual AWB")
            ctx.user_data.clear(); return

        sorted_c = sorted(couriers, key=lambda c: priority_rank(c.get("courier_name","")))
        awb = None; chosen = None; fallback = False

        for c in sorted_c:
            cid = c.get("courier_company_id") or c.get("courier_id")
            if priority_rank(c.get("courier_name","")) == 999: fallback = True
            awb = assign_awb(shipment_id, cid)
            if awb: chosen = c; break

        if not awb:
            await msg.edit_text("❌ Courier assignment failed.")
            ctx.user_data.clear(); return

        tracking  = f"https://shiprocket.co/tracking/{awb}"
        order_num = next_order_number()

        order_record = {
            "order_id": order_id, "order_number": order_num,
            "created_at": datetime.now().isoformat(),
            "phone": d.get("phone",""), "customer_name": d.get("name",""),
            "address": d.get("address",""), "city": d.get("city",""),
            "state": d.get("state","Karnataka"), "pincode": delivery_pin,
            "product": prod_name, "creative": creative,
            "total": cod_amount, "cod_amount": cod_amount,
            "courier_paid": courier_charged, "advance_paid": None,
            "status": "active", "pickup_location": pickup_display,
            "shiprocket": {
                "order_id": resp.get("order_id",""), "shipment_id": shipment_id,
                "awb": awb, "courier": chosen.get("courier_name",""),
                "rate": chosen.get("rate",0), "tracking": tracking,
            },
            "manual": None, "label_downloaded": False, "label_downloaded_date": "",
        }
        save_order(order_record)

        courier_note = "⚠️ Fallback" if fallback else "✅ Priority"
        await msg.edit_text(
            f"✅ *Shipment Created!*\n"
            f"————————————————\n"
            f"Order:    #{order_num}\n"
            f"Name:     {d.get('name','')}\n"
            f"Phone:    {d.get('phone','')}\n"
            f"City:     {d.get('city','')}, {delivery_pin}\n"
            f"Product:  {prod_name}\n"
            f"Creative: {creative or '—'}\n"
            f"COD:      ₹{int(cod_amount):,}\n"
            f"Courier:  ₹{courier_charged}\n"
            f"————————————————\n"
            f"Vendor:   {pickup_display}\n"
            f"Courier:  {chosen.get('courier_name','')} {courier_note}\n"
            f"Rate:     ₹{chosen.get('rate',0)}\n"
            f"AWB:      `{awb}`\n"
            f"Tracking: {tracking}",
            parse_mode="Markdown"
        )

        label_url = generate_label(shipment_id)
        if label_url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(label_url) as resp2:
                        if resp2.status == 200:
                            pdf = await resp2.read()
                            await reply.reply_document(
                                document=pdf, filename=f"{awb}.pdf",
                                caption=f"📄 {d.get('name','')} | {awb} | {pickup_display}"
                            )
            except Exception as e:
                log.error(f"Label: {e}")

        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Schedule Pickup", callback_data=f"pickup_yes_{shipment_id}_{order_id}"),
            InlineKeyboardButton("🔄 Reassign",        callback_data=f"action_reassign_{order_id}"),
            InlineKeyboardButton("❌ Cancel",           callback_data=f"action_cancel_{order_id}"),
        ]])
        await reply.reply_text("Shipment action:", reply_markup=kb)

    except Exception as e:
        log.error(f"Create: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")
    finally:
        ctx.user_data.clear()

# ─── SEARCH ───────────────────────────────

async def do_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    o = find_by_phone(text) if re.match(r"^\d{10}$", text.strip()) else find_by_awb(text)
    if o:
        await update.message.reply_text(
            format_order(o),
            reply_markup=order_action_kb(o.get("order_id",""), o.get("phone",""))
        )
    else:
        await update.message.reply_text("❌ No order found", reply_markup=MAIN_KB)
    ctx.user_data.clear()

# ─── ADVANCE ──────────────────────────────

async def show_advance(q, ctx, phone):
    o = find_by_phone(phone)
    if not o:
        await q.message.reply_text("❌ Order not found", reply_markup=MAIN_KB); return
    ctx.user_data["adv_phone"] = phone
    ctx.user_data["adv_order"] = o
    ctx.user_data["state"]     = "adv_picking"
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
        f"Product: {o.get('product','')}\n"
        f"COD: ₹{int(o.get('cod_amount',0)):,}\n"
        f"Courier: ₹{o.get('courier_paid',0)} ✅\n\n"
        f"Advance paid?",
        reply_markup=kb
    )

async def do_save_advance(update_or_q, ctx, advance_amt):
    ud    = ctx.user_data
    phone = ud.get("adv_phone")
    update_order(phone, advance_paid=advance_amt)
    ud["adv_advance"] = advance_amt

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Save",                     callback_data="adv_save"),
        InlineKeyboardButton("🔄 Cancel + Rebook new COD", callback_data="adv_rebook"),
    ]])
    reply_fn = update_or_q.message if hasattr(update_or_q, 'message') else update_or_q
    await reply_fn.reply_text(
        f"Advance: ₹{advance_amt} saved.\n\nNeed to change COD and rebook?",
        reply_markup=kb
    )

async def do_rebook_new_cod(update: Update, ctx: ContextTypes.DEFAULT_TYPE, new_cod: int):
    ud    = ctx.user_data
    phone = ud.get("adv_phone")
    o     = ud.get("adv_order")
    sr    = o.get("shiprocket") or {}

    msg = await update.message.reply_text("⏳ Cancelling old shipment...")
    ok, err = cancel_sr_order(sr.get("order_id","") or sr.get("shipment_id",""))
    if not ok:
        await msg.edit_text(f"❌ Cancel failed: {err}")
        ud.clear(); return

    await msg.edit_text(f"⏳ Creating new shipment COD ₹{new_cod:,}...")
    products  = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
    prod_name = o.get("product","Projector")
    prod      = products.get(prod_name, {"length":20,"breadth":15,"height":10,"weight":0.5})
    pickup_obj = resolve_pickup(o.get("pickup_location",""))
    if not pickup_obj:
        await msg.edit_text("❌ Pickup not found")
        ud.clear(); return

    pickup_pin   = str(pickup_obj.get("pin_code","560001"))
    delivery_pin = str(o.get("pincode","560001"))
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
        "order_items": [{"name": prod_name, "sku": prod_name, "units": 1,
                         "selling_price": new_cod, "discount":"0","tax":"0","hsn":""}],
        "payment_method": "COD",
        "sub_total": new_cod, "cod_amount": new_cod,
        "length": float(prod["length"]), "breadth": float(prod["breadth"]),
        "height": float(prod["height"]), "weight": float(prod["weight"]),
    }

    ensure_token()
    r = session.post(f"{SR_BASE}/orders/create/adhoc", json=payload, timeout=45)
    if r.status_code != 200:
        await msg.edit_text(f"❌ Rebook failed: {r.text[:200]}")
        ud.clear(); return

    resp        = r.json()
    shipment_id = resp.get("shipment_id")
    couriers    = get_couriers(pickup_pin, delivery_pin, prod["weight"], True)
    sorted_c    = sorted(couriers, key=lambda c: priority_rank(c.get("courier_name","")))
    awb = None; chosen = None

    for c_opt in sorted_c:
        cid = c_opt.get("courier_company_id") or c_opt.get("courier_id")
        awb = assign_awb(shipment_id, cid)
        if awb: chosen = c_opt; break

    if not awb:
        await msg.edit_text("❌ Courier assignment failed")
        ud.clear(); return

    tracking = f"https://shiprocket.co/tracking/{awb}"
    update_order(phone,
        order_id=new_order_id, cod_amount=new_cod, status="active",
        shiprocket={
            "order_id": resp.get("order_id",""), "shipment_id": shipment_id,
            "awb": awb, "courier": chosen.get("courier_name",""),
            "rate": chosen.get("rate",0), "tracking": tracking,
        }
    )

    await msg.edit_text(
        f"✅ Rebooked!\n"
        f"Old AWB: {sr.get('awb','')} — Cancelled\n"
        f"New AWB: {awb}\n"
        f"Courier: {chosen.get('courier_name','')}\n"
        f"New COD: ₹{new_cod:,}\n"
        f"Advance: ₹{ud.get('adv_advance',0)} ✅",
        reply_markup=MAIN_KB
    )

    label_url = generate_label(shipment_id)
    if label_url:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(label_url) as resp2:
                    if resp2.status == 200:
                        pdf = await resp2.read()
                        await update.message.reply_document(
                            document=pdf, filename=f"{awb}.pdf",
                            caption=f"📄 New label — {o.get('customer_name','')} | {awb}"
                        )
        except Exception as e:
            log.error(f"Label rebook: {e}")
    ud.clear()

# ─── REASSIGN ─────────────────────────────

async def do_reassign_courier(update: Update, ctx: ContextTypes.DEFAULT_TYPE, chosen_courier: dict):
    ud       = ctx.user_data
    order_id = ud.get("reassign_order_id")
    o        = ud.get("reassign_order")
    sr       = o.get("shiprocket") or {}

    await update.message.reply_text(
        f"⏳ Cancelling {sr.get('courier','')} → {chosen_courier.get('courier_name','')}..."
    )
    ok, err = cancel_sr_order(sr.get("order_id","") or sr.get("shipment_id",""))
    if not ok:
        await update.message.reply_text(f"❌ Cancel failed: {err}", reply_markup=MAIN_KB)
        ud.clear(); return

    products  = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
    prod_name = o.get("product","Projector")
    prod      = products.get(prod_name, {"length":20,"breadth":15,"height":10,"weight":0.5})
    pickup_obj = resolve_pickup(o.get("pickup_location",""))
    if not pickup_obj:
        await update.message.reply_text("❌ Pickup not found", reply_markup=MAIN_KB)
        ud.clear(); return

    delivery_pin = str(o.get("pincode","560001"))
    new_order_id = f"OBX{int(time.time())}_{uuid.uuid4().hex[:5]}"
    cod_amount   = o.get("cod_amount", 2400)

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
        "order_items": [{"name": prod_name, "sku": prod_name, "units": 1,
                         "selling_price": cod_amount, "discount":"0","tax":"0","hsn":""}],
        "payment_method": "COD",
        "sub_total": cod_amount, "cod_amount": cod_amount,
        "length": float(prod["length"]), "breadth": float(prod["breadth"]),
        "height": float(prod["height"]), "weight": float(prod["weight"]),
    }

    ensure_token()
    r = session.post(f"{SR_BASE}/orders/create/adhoc", json=payload, timeout=45)
    if r.status_code != 200:
        await update.message.reply_text(f"❌ Recreate failed: {r.text[:200]}", reply_markup=MAIN_KB)
        ud.clear(); return

    resp        = r.json()
    shipment_id = resp.get("shipment_id")
    cid = chosen_courier.get("courier_company_id") or chosen_courier.get("courier_id")
    awb = assign_awb(shipment_id, cid)

    if not awb:
        await update.message.reply_text("❌ AWB failed", reply_markup=MAIN_KB)
        ud.clear(); return

    tracking = f"https://shiprocket.co/tracking/{awb}"
    update_order_by_id(order_id,
        order_id=new_order_id, status="active",
        shiprocket={
            "order_id": resp.get("order_id",""), "shipment_id": shipment_id,
            "awb": awb, "courier": chosen_courier.get("courier_name",""),
            "rate": chosen_courier.get("rate",0), "tracking": tracking,
        }
    )

    await update.message.reply_text(
        f"✅ Reassigned!\n"
        f"Old: {sr.get('courier','')} {sr.get('awb','')}\n"
        f"New: {chosen_courier.get('courier_name','')} | AWB: {awb}\n"
        f"Rate: ₹{chosen_courier.get('rate',0)}",
        reply_markup=MAIN_KB
    )

    label_url = generate_label(shipment_id)
    if label_url:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(label_url) as resp2:
                    if resp2.status == 200:
                        pdf = await resp2.read()
                        await update.message.reply_document(
                            document=pdf, filename=f"{awb}.pdf",
                            caption=f"📄 Reassigned — {o.get('customer_name','')} | {awb}"
                        )
        except Exception as e:
            log.error(f"Label reassign: {e}")
    ud.clear()

# ─── LABELS ───────────────────────────────

async def show_label_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    queue = get_label_queue()
    if not queue:
        await update.message.reply_text("📥 No labels pending", reply_markup=MAIN_KB); return

    vendor_counts = {}
    for o in queue:
        vm = o.get("manual") or {}
        v  = vm.get("vendor") or o.get("pickup_location") or "Shiprocket"
        vendor_counts[v] = vendor_counts.get(v, 0) + 1

    kb_rows = [[InlineKeyboardButton(f"📥 All ({len(queue)})", callback_data="label_all")]]
    for v, count in sorted(vendor_counts.items()):
        kb_rows.append([InlineKeyboardButton(f"🏪 {v} ({count})", callback_data=f"label_vendor_{v}")])

    lines = [f"📥 *Download Labels*\n{len(queue)} pending\n"]
    for v, count in sorted(vendor_counts.items()):
        lines.append(f"  {v}: {count}")

    await update.message.reply_text(
        "\n".join(lines), parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb_rows)
    )

async def do_download_labels(update, orders):
    if not orders:
        await update.callback_query.message.reply_text("No labels in this group"); return
    await update.callback_query.message.reply_text(f"⏳ Generating {len(orders)} labels...")
    downloaded = 0
    for o in orders:
        sr  = o.get("shiprocket") or {}
        sid = sr.get("shipment_id")
        if not sid: continue
        url = generate_label(sid)
        if url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(url) as r:
                        if r.status == 200:
                            pdf = await r.read()
                            vm     = o.get("manual") or {}
                            vendor = vm.get("vendor") or o.get("pickup_location") or "Shiprocket"
                            await update.callback_query.message.reply_document(
                                document=pdf, filename=f"{sr.get('awb','label')}.pdf",
                                caption=f"#{o.get('order_number')} — {o.get('customer_name','')} | {o.get('city','')} | {vendor}"
                            )
                            mark_label_downloaded(o.get("order_id",""))
                            downloaded += 1
            except Exception as e:
                log.error(f"Label DL: {e}")
    await update.callback_query.message.reply_text(
        f"✅ {downloaded}/{len(orders)} downloaded. These will not appear again.",
        reply_markup=MAIN_KB
    )

# ─── PAYMENT REPORT ───────────────────────

async def show_payment_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    r = get_payment_report()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏳ Pending Advance", callback_data="rep_pending"),
         InlineKeyboardButton("✅ Advance Paid",    callback_data="rep_advance")],
        [InlineKeyboardButton("💵 Full COD",        callback_data="rep_fullcod"),
         InlineKeyboardButton("📋 All Orders",      callback_data="rep_all")],
    ])
    await update.message.reply_text(
        f"📊 *Payment Report*\n\n"
        f"⏳ Pending: {len(r['pending'])}\n"
        f"✅ Advance: {len(r['advance'])}\n"
        f"💵 Full COD: {len(r['full_cod'])}\n"
        f"❌ Nothing: {len(r['nothing'])}",
        parse_mode="Markdown", reply_markup=kb
    )

# ─── CREATIVE ─────────────────────────────

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
        await update.message.reply_text("No products.\nSend: Name length breadth height weight"); return
    for name, p in products.items():
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✏️ Edit",   callback_data=f"prod_edit_{name}"),
            InlineKeyboardButton("🗑 Delete", callback_data=f"prod_del_{name}"),
        ]])
        await update.message.reply_text(
            f"*{name}*\n{p['length']}×{p['breadth']}×{p['height']}cm | {p['weight']}kg",
            parse_mode="Markdown", reply_markup=kb
        )
    await update.message.reply_text("Products ↑", reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("➕ Add Product", callback_data="prod_add")
    ]]))

async def do_add_product(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    parts = text.strip().split()
    if len(parts) < 5:
        await update.message.reply_text("Format: Name length breadth height weight"); return
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
        val = data.replace("adv_","")
        await do_save_advance(q, ctx, int(val)); return

    if data == "adv_custom":
        ud["state"] = "adv_custom"
        await q.message.reply_text("Enter advance amount:"); return

    if data == "adv_save":
        await q.message.reply_text("✅ Done!", reply_markup=MAIN_KB)
        ud.clear(); return

    if data == "adv_rebook":
        ud["state"] = "adv_new_cod"
        await q.message.reply_text("Enter new COD amount:"); return

    if data.startswith("pickup_yes_"):
        parts       = data.replace("pickup_yes_","").split("_",1)
        shipment_id = parts[0]
        order_id    = parts[1] if len(parts)>1 else ""
        ok, msg = schedule_pickup([shipment_id])
        if ok and order_id: update_order_by_id(order_id, pickup_scheduled=True)
        await q.edit_message_text(msg); return

    if data.startswith("action_cancel_"):
        order_id = data.replace("action_cancel_","")
        orders = load_orders()
        o = next((x for x in orders if x.get("order_id")==order_id), None)
        if o:
            sr = o.get("shiprocket") or {}
            ok, msg = cancel_sr_order(sr.get("order_id","") or sr.get("shipment_id",""))
            if ok:
                update_order_by_id(order_id, status="cancelled")
                await q.message.reply_text(f"✅ Cancelled #{o.get('order_number')}", reply_markup=MAIN_KB)
            else:
                await q.message.reply_text(f"❌ Cancel failed: {msg}", reply_markup=MAIN_KB)
        return

    if data.startswith("action_reassign_"):
        order_id = data.replace("action_reassign_","")
        orders = load_orders()
        o = next((x for x in orders if x.get("order_id")==order_id), None)
        if not o:
            await q.message.reply_text("❌ Order not found"); return
        await q.message.reply_text("⏳ Fetching couriers...")
        couriers = get_available_couriers_for_order(o)
        if not couriers:
            await q.message.reply_text("❌ No couriers available"); return
        sorted_c = sorted(couriers, key=lambda c: priority_rank(c.get("courier_name","")))[:10]
        ud["reassign_order_id"] = order_id
        ud["reassign_order"]    = o
        ud["reassign_couriers"] = sorted_c
        ud["state"]             = "reassign_select"
        lines = ["🔄 *Available Couriers — pick a number:*\n"]
        for i, c in enumerate(sorted_c, 1):
            rank = "Priority" if priority_rank(c.get("courier_name","")) < 999 else "Standard"
            lines.append(f"{i}. {c.get('courier_name','')} — ₹{c.get('rate',0)} ({rank})")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown"); return

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

    if data == "manual_cancel_yes":
        o  = ud.get("manual_order",{})
        sr = o.get("shiprocket") or {}
        cancel_sr_order(sr.get("order_id","") or sr.get("shipment_id",""))
        ud["state"] = "manual_vendor"
        await q.message.reply_text("✅ Cancelled\n\nEnter vendor name:"); return

    if data == "manual_cancel_no":
        await q.message.reply_text("Cancelled", reply_markup=MAIN_KB)
        ud.clear(); return

    if data == "label_all":
        await do_download_labels(update, get_label_queue()); return
    if data.startswith("label_vendor_"):
        await do_download_labels(update, get_label_queue_by_vendor(data.replace("label_vendor_",""))); return

    if data.startswith("rep_"):
        r = get_payment_report()
        map_ = {
            "rep_pending": ("⏳ Pending", r["pending"]),
            "rep_advance": ("✅ Advance", r["advance"]),
            "rep_fullcod": ("💵 Full COD", r["full_cod"]),
            "rep_all":     ("📋 All", r["pending"]+r["advance"]+r["full_cod"]+r["nothing"]),
        }
        title, orders = map_.get(data, ("", []))
        if not orders:
            await q.message.reply_text(f"{title}: none"); return
        lines = [f"{title} ({len(orders)})\n"]
        for o in orders[:30]:
            c = o.get("courier_paid") or 0
            a = o.get("advance_paid")
            vm = o.get("manual") or {}
            vendor = vm.get("vendor") or o.get("pickup_location") or "SR"
            lines.append(f"#{o.get('order_number')} {o.get('customer_name','')} | {o.get('phone','')} | C:₹{c} A:{'₹'+str(a) if a is not None else '—'} | {vendor}")
        await q.message.reply_text("\n".join(lines)); return

    if data.startswith("cr_"):
        period = {"cr_today":"today","cr_yesterday":"yesterday","cr_all":"all"}.get(data,"today")
        missing = get_missing_creative(period)
        if not missing:
            await q.message.reply_text(f"✅ No missing ({period})"); return
        lines = [f"🎨 Missing — {period} ({len(missing)})\n"]
        for o in missing[:25]:
            lines.append(f"#{o.get('order_number')} {o.get('customer_name','')} — {o.get('phone','')}")
        lines.append("\nUse: /setcreative <phone> <code>")
        await q.message.reply_text("\n".join(lines)); return

    if data == "prod_add":
        ctx.user_data["state"] = "prod_add"
        await q.message.reply_text("Send: Name length breadth height weight"); return
    if data.startswith("prod_del_"):
        name = data.replace("prod_del_","")
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        products.pop(name, None)
        json.dump(products, open(PRODUCTS_FILE,"w"), indent=2)
        await q.edit_message_text(f"🗑 Deleted: {name}"); return
    if data.startswith("prod_edit_"):
        ctx.user_data["state"] = "prod_add"
        await q.message.reply_text("New details:\nName l b h w"); return

# ─── MAIN ─────────────────────────────────

async def main():
    log.info("Starting Oneboxx Ship Bot...")
    get_token()
    log.info("Shiprocket auth OK")
    refresh_pickups()
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