# bot.py
import os
import re
import json
import uuid
import time
import logging
import requests
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, CallbackQueryHandler, filters
)
import asyncio
import openai
import aiohttp

# ---------------- CONFIG ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHIPROCKET_EMAIL = os.getenv("SHIPROCKET_EMAIL")
SHIPROCKET_PASSWORD = os.getenv("SHIPROCKET_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

CUSTOM_CHANNEL_ID = None
if os.path.exists("custom_channel.json"):
    try:
        CUSTOM_CHANNEL_ID = json.load(open("custom_channel.json")).get("id")
    except Exception:
        CUSTOM_CHANNEL_ID = None

SHIPROCKET_BASE = "https://apiv2.shiprocket.in/v1/external"
URLS = {
    "login": "/auth/login",
    "pickup": "/settings/company/pickup",
    "create_order": "/orders/create/adhoc",
    "courier_get": "/courier/serviceability/",
    "assign_awb": "/courier/assign/awb",
    "label": "/courier/generate/label",
    "get_quote": "/courier/charge/calculate",
    "generate_pickup": "/courier/generate/pickup",   # ✅ Correct endpoint
}
COURIER_PRIORITY = ["bluedart", "delhivery", "dtdc"]
PRODUCTS_FILE = "products.json"
DEFAULT_PRODUCT = {"length":10,"breadth":8,"height":5,"weight":0.5}

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("telegram_shipbot")
session = requests.Session()
pickup_map = {}
shipment_awb_map = {}  # Ensure AWB mapping

# ---------------- HELPERS ----------------
def strict_phone(ph):
    if not ph:
        return None
    ph = re.sub(r"\D", "", str(ph))
    return ph if len(ph) == 10 and ph[0] in "6789" else None

def parse_payment(payment_str):
    m = re.match(r"(prepaid|cod)\s+(\d+\.?\d*)", (payment_str or "").strip(), re.I)
    if not m:
        return "Prepaid", 0
    return m.group(1).capitalize(), float(m.group(2))

def normalize_pickup_obj(parsed):
    if parsed.get("pickup"):
        k = re.sub(r"\W","",parsed["pickup"].lower())
        for key, obj in pickup_map.items():
            norm_key = re.sub(r"\W","",key.lower())
            if k == norm_key or k in norm_key or norm_key in k:
                return obj
    return next(iter(pickup_map.values()), None)

# ---------------- SHIPROCKET LOGIN / PICKUP ----------------
def shiprocket_login():
    try:
        r = session.post(SHIPROCKET_BASE + URLS["login"], json={
            "email": SHIPROCKET_EMAIL,
            "password": SHIPROCKET_PASSWORD
        }, timeout=20)
        if r.status_code == 200 and r.json().get("token"):
            token = r.json()["token"]
            session.headers.update({"Authorization": f"Bearer {token}"})
            return True, "✅ Logged into Shiprocket"
        return False, f"❌ Login failed: {r.text}"
    except Exception as e:
        return False, f"❌ Login error: {e}"

def refresh_pickups():
    global pickup_map
    try:
        r = session.get(SHIPROCKET_BASE + URLS["pickup"], timeout=20)
        if r.status_code != 200:
            return False, f"❌ Pickup fetch failed: {r.status_code} {r.text}"
        lst = r.json().get("data", {}).get("shipping_address", [])
        pickup_map = {p["pickup_location"].lower(): p for p in lst if p.get("pickup_location")}
        return True, f"✅ Loaded {len(pickup_map)} pickups"
    except Exception as e:
        return False, f"❌ Pickup refresh error: {e}"

# ---------------- OPENAI ADDRESS FORMATTING ----------------
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
    response = openai.chat.completions.create(
        model="gpt-5",
        messages=[{"role":"user","content":prompt}],
        temperature=1
    )
    formatted_text = response.choices[0].message.content.strip()
    return formatted_text

# ---------------- SHIPROCKET API ----------------
def get_available_couriers(pickup_pin, delivery_pin, weight, cod):
    try:
        r = session.get(SHIPROCKET_BASE + URLS["courier_get"], params={
            "pickup_postcode": str(pickup_pin),
            "delivery_postcode": str(delivery_pin),
            "cod": int(bool(cod)),
            "weight": weight
        }, timeout=15)
        if r.status_code != 200: return []
        return r.json().get("data", {}).get("available_courier_companies", []) or []
    except Exception:
        return []

def pick_courier(couriers):
    if not couriers: return None
    for pr in COURIER_PRIORITY:
        options = [c for c in couriers if pr in (str(c.get("courier_name") or "").lower())]
        if options:
            return min(options, key=lambda x:x.get("rate",1e12))
    return min(couriers, key=lambda x:x.get("rate",1e12))

def get_shipping_quote(pickup_pin, delivery_pin, weight, cod):
    try:
        r = session.get(SHIPROCKET_BASE + URLS["get_quote"], params={
            "pickup_postcode": pickup_pin,
            "delivery_postcode": delivery_pin,
            "weight": weight,
            "cod": int(bool(cod))
        }, timeout=15)
        if r.status_code != 200: return None
        return r.json().get("data", {}).get("rate")
    except Exception:
        return None

def assign_awb(shipment_id, courier_id=None):
    try:
        payload = {"shipment_id": shipment_id}
        if courier_id:
            payload["courier_id"] = courier_id
        r = session.post(SHIPROCKET_BASE + URLS["assign_awb"], json=payload, timeout=20)
        resp_json = r.json()
        if resp_json.get("awb_assign_status") == 1:
            return resp_json["response"]["data"]["awb_code"]
        return None
    except Exception as e:
        log.error(f"AWB assignment error: {e}")
        return None

def generate_label(shipment_id):
    try:
        r = session.post(SHIPROCKET_BASE + URLS["label"], json={"shipment_id":[shipment_id]}, timeout=20)
        resp_json = r.json() if r else {}
        if not resp_json or resp_json.get("label_created") != 1:
            return None
        return resp_json.get("label_url")
    except Exception as e:
        log.error(f"Label generation error: {e}")
        return None

def create_order(payload):
    try:
        r = session.post(SHIPROCKET_BASE + URLS["create_order"], json=payload, timeout=20)
        resp_json = r.json() if r else None
        if r.status_code!=200 or (resp_json and resp_json.get("status_code") not in (1,200)):
            return None, r.text
        return resp_json, None
    except Exception as e:
        return None, str(e)

def schedule_pickup(shipment_ids, pickup_date=None, time_slot_id=None):
    try:
        payload = {"shipment_id": shipment_ids}
        if pickup_date:
            payload["pickup_date"] = pickup_date  # 'YYYY-MM-DD'
        if time_slot_id:
            payload["time_slot_id"] = time_slot_id  # optional

        r = session.post(SHIPROCKET_BASE + URLS["generate_pickup"], json=payload, timeout=20)

        # 🔎 Debug log — prints full response for troubleshooting
        print("🚚 Shiprocket Pickup Response:", r.status_code, r.text)

        try:
            resp_json = r.json()
        except Exception:
            return False, f"❌ Invalid response from Shiprocket: {r.text}"

        if r.status_code == 200:
            if resp_json.get("pickup_scheduled") or resp_json.get("status") == 1:
                pickup_id = (resp_json.get("pickup_id")
                             or resp_json.get("response", {}).get("pickup_id")
                             or resp_json.get("pickup_token_number"))
                return True, f"✅ Pickup scheduled! Pickup ID: {pickup_id or 'N/A'}"
            else:
                return False, f"❌ Pickup not scheduled: {resp_json.get('message') or resp_json.get('response')}"
        else:
            return False, f"❌ API Error {r.status_code}: {resp_json}"
    except Exception as e:
        return False, f"⚠️ Error scheduling pickup: {e}"
# ---------------- NEW: create_shipment_with_fallback ----------------
def create_shipment_with_fallback(shipment_id, pickup_pin, delivery_pin, weight, cod):
    couriers = get_available_couriers(pickup_pin, delivery_pin, weight, cod)
    if not couriers: return None, None, None

    def mode_pref(c):
        m = str(c.get("mode") or c.get("service_type") or "").lower()
        if "surface" in m: return 0
        if "air" in m: return 1
        return 2

    priority_json = None
    if os.path.exists("courier_priority.json"):
        try:
            priority_json = json.load(open("courier_priority.json"))
        except Exception:
            priority_json = None

    def priority_key(c):
        if priority_json:
            name = str(c.get("courier_name") or "").strip()
            mode = str(c.get("mode") or c.get("service_type") or "").strip()
            key = f"{name}{(' ' + mode.title()) if mode else ''}"
            val = priority_json.get(key)
            if isinstance(val, int):
                return (val, mode_pref(c), c.get("rate", 1e12))
        name_lower = str(c.get("courier_name") or "").lower()
        if "bluedart" in name_lower: base = 1
        elif "delhivery" in name_lower: base = 2
        elif "dtdc" in name_lower: base = 3
        else: base = 99
        return (base, mode_pref(c), c.get("rate",1e12))

    couriers_sorted = sorted(couriers, key=lambda c: priority_key(c))

    for courier in couriers_sorted:
        courier_id = (courier.get("courier_company_id") or
                      courier.get("courier_id") or
                      courier.get("courierId") or
                      courier.get("id"))
        if not courier_id:
            log.info(f"Skipping courier {courier.get('courier_name')} (no ID found)")
            continue
        try:
            awb = assign_awb(shipment_id, courier_id)
            log.info(f"Trying courier {courier.get('courier_name')} -> AWB: {awb}")
        except Exception as e:
            log.error(f"Error assigning AWB for courier {courier.get('courier_name')}: {e}")
            awb = None
        if awb:
            shipment_awb_map[shipment_id] = awb
            return courier, awb, courier.get("rate")

    return None, None, None
         
# ---------------- TELEGRAM BOT ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Add Product", callback_data="add_product")],
        [InlineKeyboardButton("Create Shipment", callback_data="create_shipment")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Welcome! Choose an action:", reply_markup=reply_markup)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if context.user_data.get("awaiting_product"):
        parts = text.strip().split()
        if len(parts) < 5:
            await update.message.reply_text("❌ Invalid format. Send: ProductName length breadth height weight")
            return
        try:
            length = float(parts[-4])
            breadth = float(parts[-3])
            height = float(parts[-2])
            weight = float(parts[-1])
        except ValueError:
            await update.message.reply_text("❌ Dimensions and weight must be numbers.")
            return
        product_name = " ".join(parts[:-4])
        products = {}
        if os.path.exists(PRODUCTS_FILE):
            products = json.load(open(PRODUCTS_FILE))
        products[product_name] = {"length": length,"breadth": breadth,"height": height,"weight": weight}
        json.dump(products, open(PRODUCTS_FILE,"w"), indent=2)
        context.user_data["awaiting_product"]=False
        await update.message.reply_text(f"✅ Product '{product_name}' saved successfully")
        return

    if context.user_data.get("awaiting_shipment"):
        try:
            formatted = ai_format_address(text)
            data = {}
            for line in formatted.splitlines():
                if ":" in line:
                    k,v=line.split(":",1)
                    data[k.strip().lower()]=v.strip()

            payment_method, sub_total = parse_payment(data.get("prepaid/cod","Prepaid 0"))
            sr_payment_method = "COD" if payment_method.lower()=="cod" else "Prepaid"
            cod_amount = sub_total if sr_payment_method=="COD" else 0
            qty = int(data.get("quantity","1"))
            product_data = json.load(open(PRODUCTS_FILE)).get(data.get("product",""), DEFAULT_PRODUCT)
            pickup_obj = normalize_pickup_obj({"pickup": data.get("pickup")})
            if not pickup_obj:
                await update.message.reply_text("❌ Pickup not found in Shiprocket account")
                return

            payload = {
                "order_id": f"ORDER{int(time.time())}_{uuid.uuid4().hex[:6]}",
                "order_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "pickup_location": pickup_obj.get("pickup_location"),
                "billing_customer_name": data.get("name","Customer"),
                "billing_last_name": ".",
                "billing_address": data.get("address",""),
                "billing_address_2": "",
                "billing_city": data.get("city", data.get("district","")),
                "billing_state": data.get("state",""),
                "billing_country": "India",
                "billing_pincode": data.get("pincode","110001"),
                "billing_email": "na@example.com",
                "billing_isd_code":"91",
                "billing_phone": data.get("phone",""),
                "billing_alternate_phone":data.get("alternate phone",""),
                "shipping_is_billing":True,
                "order_items":[{
                    "name": data.get("product",""),
                    "sku": data.get("product",""),
                    "units": qty,
                    "selling_price": sub_total,
                    "discount": "0",
                    "tax": "0",
                    "hsn": ""
                }],
                "payment_method": sr_payment_method,
                "shipping_charges": get_shipping_quote(
                    pickup_obj.get("pin_code","110001"),
                    data.get("pincode","110001"),
                    product_data.get("weight"),
                    sr_payment_method=="COD"
                ) or 0,
                "giftwrap_charges": 0,
                "transaction_charges":0,
                "total_discount":0,
                "sub_total": sub_total,
                "cod_amount": cod_amount,
                "length": float(product_data.get("length")),
                "breadth": float(product_data.get("breadth")),
                "height": float(product_data.get("height")),
                "weight": float(product_data.get("weight")),
            }
            if CUSTOM_CHANNEL_ID:
                payload["channel_id"]=CUSTOM_CHANNEL_ID

            resp, err = create_order(payload)
            if not resp:
                await update.message.reply_text(f"❌ Error creating shipment: {err}")
                return

            shipment_id = resp.get("shipment_id")

            courier, awb, rate = create_shipment_with_fallback(
                shipment_id,
                pickup_obj.get("pin_code","110001"),
                data.get("pincode","110001"),
                product_data.get("weight"),
                sr_payment_method=="COD"
            )

            if not courier or not awb:
                await update.message.reply_text("❌ No couriers available for this shipment")
                return

            shipment_awb_map[shipment_id] = awb  # ensure mapping

            label_url = generate_label(shipment_id)
            tracking_link = f"https://www.shiprocket.in/shipment-tracking/?awb={awb}" if awb else "N/A"

            await update.message.reply_text(
                f"✅ Shipment Created!\nCourier: {courier.get('courier_name')}\nRate: {rate}\nAWB: {awb}\nTracking: {tracking_link}"
            )

            if label_url:
                async with aiohttp.ClientSession() as session:
                    async with session.get(label_url) as resp_pdf:
                        if resp_pdf.status == 200:
                            data_pdf = await resp_pdf.read()
                            await update.message.reply_document(document=data_pdf, filename=f"{awb}.pdf")

            keyboard = [
                [InlineKeyboardButton("Schedule Pickup ✅", callback_data=f"schedule_yes_{shipment_id}")],
                [InlineKeyboardButton("Do Not Schedule ❌", callback_data=f"schedule_no_{shipment_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Do you want to schedule this shipment?", reply_markup=reply_markup)

        except Exception as e:
            await update.message.reply_text(f"⚠️ Error: {e}")
        finally:
            context.user_data["awaiting_shipment"]=False

# ---------------- CALLBACK HANDLER ----------------
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data=="add_product":
        context.user_data["awaiting_product"]=True
        await query.message.reply_text("Send product in format: ProductName length breadth height weight")
        return

    if data=="create_shipment":
        context.user_data["awaiting_shipment"]=True
        await query.message.reply_text("Send messy address/order to create shipment")
        return

    if data.startswith("schedule_yes_"):
        shipment_id = data.replace("schedule_yes_", "")
        ids = [int(shipment_id)] if shipment_id.isdigit() else [shipment_id]

        ok, msg = schedule_pickup(ids)
        await query.edit_message_text(("✅ " if ok else "❌ ") + msg)

    elif data.startswith("schedule_no_"):
        shipment_id = data.replace("schedule_no_", "")
        await query.edit_message_text(f"❌ Shipment {shipment_id} not scheduled")
# ---------------- MAIN ----------------
async def main():
    log.info("Bot starting...")
    ok,msg = shiprocket_login()
    log.info(msg)
    if not ok: return
    ok,msg = refresh_pickups()
    log.info(msg)

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button))
    await app.run_polling()

if __name__=="__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())
