# bot_enhanced.py
"""
Backbenchers Enhanced Bot - Standalone Version
Complete bot with order management, search, stats, and reporting
"""

import os
import re
import json
import uuid
import time
import logging
import requests
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, CallbackQueryHandler, filters
)
import asyncio
import openai
import aiohttp

# Import orders manager (same directory)
from orders_manager import (
    save_order, find_order_by_phone, find_order_by_awb,
    mark_advance_paid, convert_to_full_cod, add_manual_shipment,
    get_today_stats, get_week_stats, format_order_details
)

# ---------------- CONFIG ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN_2")  # ← CHANGED TO BOT_TOKEN_2
SHIPROCKET_EMAIL = os.getenv("SHIPROCKET_EMAIL")
SHIPROCKET_PASSWORD = os.getenv("SHIPROCKET_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

print("=" * 60)
print("BACKBENCHERS ENHANCED BOT - STARTING")
print("=" * 60)
print(f"BOT_TOKEN_2: {'Set' if BOT_TOKEN else 'Missing'}")
print(f"SHIPROCKET: {'Set' if SHIPROCKET_EMAIL else 'Missing'}")
print(f"OPENAI: {'Set' if OPENAI_API_KEY else 'Missing'}")
print("=" * 60)

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN_2 not set!")
if not SHIPROCKET_EMAIL or not SHIPROCKET_PASSWORD:
    raise ValueError("SHIPROCKET credentials not set!")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY not set!")

openai.api_key = OPENAI_API_KEY

SHIPROCKET_BASE = "https://apiv2.shiprocket.in/v1/external"
URLS = {
    "login": "/auth/login",
    "pickup": "/settings/company/pickup",
    "create_order": "/orders/create/adhoc",
    "courier_get": "/courier/serviceability/",
    "assign_awb": "/courier/assign/awb",
    "label": "/courier/generate/label",
    "get_quote": "/courier/charge/calculate",
    "generate_pickup": "/courier/generate/pickup",
    "cancel_shipment": "/orders/cancel/shipment/{}",
}
COURIER_PRIORITY = ["bluedart", "delhivery", "dtdc"]
PRODUCTS_FILE = "products.json"
DEFAULT_PRODUCT = {"length":10,"breadth":8,"height":5,"weight":0.5}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger("enhanced_bot")

session = requests.Session()
pickup_map = {}
shipment_awb_map = {}

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

# ---------------- SHIPROCKET ----------------
auth_token = None
token_expiry = 0

def get_token(force_refresh=False):
    global auth_token, token_expiry
    if not force_refresh and auth_token and time.time() < token_expiry:
        return auth_token
    try:
        log.info("Logging into Shiprocket...")
        r = session.post(
            SHIPROCKET_BASE + URLS["login"],
            json={"email": SHIPROCKET_EMAIL, "password": SHIPROCKET_PASSWORD},
            timeout=60
        )
        data = r.json() if r else {}
        if "token" not in data:
            raise Exception(f"Login failed: {data}")
        auth_token = data["token"]
        token_expiry = time.time() + (23 * 3600)
        session.headers.update({"Authorization": f"Bearer {auth_token}"})
        log.info("Shiprocket token obtained")
        return auth_token
    except Exception as e:
        log.error(f"Shiprocket login failed: {e}")
        raise

def ensure_valid_token():
    try:
        get_token()
    except Exception:
        get_token(force_refresh=True)

def refresh_pickups():
    global pickup_map
    try:
        ensure_valid_token()
        log.info("Fetching pickup locations...")
        r = session.get(SHIPROCKET_BASE + URLS["pickup"], timeout=60)
        if r.status_code != 200:
            return False, f"Failed: {r.status_code}"
        data = r.json()
        lst = data.get("data", {}).get("shipping_address", [])
        pickup_map = {p["pickup_location"].lower(): p for p in lst if p.get("pickup_location")}
        log.info(f"Loaded {len(pickup_map)} pickup locations")
        return True, f"Loaded {len(pickup_map)} pickups"
    except Exception as e:
        log.error(f"Pickup error: {e}")
        return False, str(e)

def ai_format_address(raw_text):
    prompt = f"""Extract shipping details from this text and output in exact format:

Input:
{raw_text}

Output format:
Pickup: <pickup_location>
Product: <product_name>
Name: <customer_name>
Address: <full_address>
City: <city>
State: <state>
Pincode: <pincode>
Phone: <10_digit_phone>
Alternate Phone: <10_digit_alt_phone_or_blank>
Prepaid/COD: <payment_type> <amount>
Quantity: <number>
Creative: <creative_code_if_present>
"""
    try:
        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[{"role":"user","content":prompt}],
            temperature=0.3
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        log.error(f"OpenAI error: {e}")
        raise

def get_available_couriers(pickup_pin, delivery_pin, weight, cod):
    try:
        r = session.get(SHIPROCKET_BASE + URLS["courier_get"], params={
            "pickup_postcode": str(pickup_pin),
            "delivery_postcode": str(delivery_pin),
            "cod": int(bool(cod)),
            "weight": weight
        }, timeout=60)
        if r.status_code != 200: return []
        return r.json().get("data", {}).get("available_courier_companies", []) or []
    except Exception as e:
        log.error(f"Courier error: {e}")
        return []

def get_shipping_quote(pickup_pin, delivery_pin, weight, cod):
    try:
        r = session.get(SHIPROCKET_BASE + URLS["get_quote"], params={
            "pickup_postcode": pickup_pin,
            "delivery_postcode": delivery_pin,
            "weight": weight,
            "cod": int(bool(cod))
        }, timeout=60)
        if r.status_code != 200: return None
        return r.json().get("data", {}).get("rate")
    except Exception:
        return None

def assign_awb(shipment_id, courier_id=None):
    try:
        payload = {"shipment_id": shipment_id}
        if courier_id:
            payload["courier_id"] = courier_id
        r = session.post(SHIPROCKET_BASE + URLS["assign_awb"], json=payload, timeout=40)
        resp_json = r.json()
        if resp_json.get("awb_assign_status") == 1:
            return resp_json["response"]["data"]["awb_code"]
        return None
    except Exception as e:
        log.error(f"AWB error: {e}")
        return None

def generate_label(shipment_id):
    try:
        r = session.post(SHIPROCKET_BASE + URLS["label"], json={"shipment_id":[shipment_id]}, timeout=40)
        resp_json = r.json() if r else {}
        if not resp_json or resp_json.get("label_created") != 1:
            return None
        return resp_json.get("label_url")
    except Exception as e:
        log.error(f"Label error: {e}")
        return None

def create_order(payload):
    try:
        ensure_valid_token()
        r = session.post(SHIPROCKET_BASE + URLS["create_order"], json=payload, timeout=40)
        resp_json = r.json() if r else None
        if r.status_code!=200 or (resp_json and resp_json.get("status_code") not in (1,200)):
            return None, r.text
        return resp_json, None
    except Exception as e:
        return None, str(e)

def cancel_shipment(shipment_id):
    try:
        ensure_valid_token()
        url = SHIPROCKET_BASE + URLS["cancel_shipment"].format(shipment_id)
        r = session.post(url, timeout=40)
        if r.status_code == 200:
            log.info(f"Cancelled: {shipment_id}")
            return True, "Cancelled"
        return False, str(r.json())
    except Exception as e:
        return False, str(e)

def schedule_pickup(shipment_ids, pickup_date=None):
    try:
        payload = {"shipment_id": shipment_ids}
        if pickup_date:
            payload["pickup_date"] = pickup_date
        r = session.post(SHIPROCKET_BASE + URLS["generate_pickup"], json=payload, timeout=40)
        resp_json = r.json()
        if r.status_code == 200 and (resp_json.get("pickup_scheduled") or resp_json.get("status") == 1):
            return True, "Pickup scheduled"
        return False, str(resp_json)
    except Exception as e:
        return False, str(e)

def create_shipment_with_fallback(shipment_id, pickup_pin, delivery_pin, weight, cod):
    couriers = get_available_couriers(pickup_pin, delivery_pin, weight, cod)
    if not couriers: return None, None, None
    
    for courier in sorted(couriers, key=lambda c: c.get("rate", 1e12)):
        courier_id = courier.get("courier_company_id") or courier.get("courier_id")
        if not courier_id:
            continue
        awb = assign_awb(shipment_id, courier_id)
        if awb:
            shipment_awb_map[shipment_id] = awb
            return courier, awb, courier.get("rate")
    return None, None, None

# ---------------- BOT HANDLERS ----------------
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["Add Product", "View Products"],
        ["Create Shipment", "Search Order"],
        ["Mark Advance", "Convert COD"],
        ["Manual Entry", "Stats"],
        ["Cancel"]
    ],
    resize_keyboard=True
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "Welcome to Backbenchers Enhanced Bot!\n\n"
        "Features:\n"
        "- Create and track shipments\n"
        "- Search orders\n"
        "- Mark advance payments\n"
        "- Convert to Full COD\n"
        "- Manual vendor entry\n"
        "- View statistics\n\n"
        "Use the buttons below!",
        reply_markup=MAIN_KEYBOARD
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    
    if context.user_data.get("editing_product"):
        try:
            parts = text.split()
            if len(parts) < 5:
                raise ValueError("bad format")
            name = " ".join(parts[:-4])
            l,b,h,w = float(parts[-4]), float(parts[-3]), float(parts[-2]), float(parts[-1])
            products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
            old_name = context.user_data.pop("editing_product", None)
            if old_name:
                products.pop(old_name, None)
            products[name] = {"length": l, "breadth": b, "height": h, "weight": w}
            json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)
            await update.message.reply_text(f"Product updated: {name}", reply_markup=MAIN_KEYBOARD)
        except:
            await update.message.reply_text("Format: Name length breadth height weight", reply_markup=MAIN_KEYBOARD)
        return
    
    if text == "Add Product":
        context.user_data["awaiting_product"] = True
        await update.message.reply_text("Send: ProductName length breadth height weight", reply_markup=MAIN_KEYBOARD)
        return
    
    if text == "View Products":
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        if not products:
            await update.message.reply_text("No products", reply_markup=MAIN_KEYBOARD)
            return
        for name, prod in products.items():
            kb = [[
                InlineKeyboardButton("Edit", callback_data=f"edit_{name}"),
                InlineKeyboardButton("Delete", callback_data=f"delete_{name}")
            ]]
            await update.message.reply_text(
                f"{name}: {prod['length']}x{prod['breadth']}x{prod['height']} | {prod['weight']}kg",
                reply_markup=InlineKeyboardMarkup(kb)
            )
        return
    
    if text == "Create Shipment":
        context.user_data["awaiting_shipment"] = True
        await update.message.reply_text("Send address/order details:", reply_markup=MAIN_KEYBOARD)
        return
    
    if text == "Search Order":
        context.user_data["awaiting_search"] = True
        await update.message.reply_text("Enter phone or AWB:", reply_markup=MAIN_KEYBOARD)
        return
    
    if text == "Mark Advance":
        context.user_data["awaiting_advance_phone"] = True
        await update.message.reply_text("Enter phone number:", reply_markup=MAIN_KEYBOARD)
        return
    
    if text == "Convert COD":
        context.user_data["awaiting_convert_phone"] = True
        await update.message.reply_text("Enter phone number:", reply_markup=MAIN_KEYBOARD)
        return
    
    if text == "Manual Entry":
        context.user_data["awaiting_manual_phone"] = True
        await update.message.reply_text("Enter phone number:", reply_markup=MAIN_KEYBOARD)
        return
    
    if text == "Stats":
        await show_stats(update, context)
        return
    
    if text == "Cancel":
        context.user_data.clear()
        await update.message.reply_text("Cancelled", reply_markup=MAIN_KEYBOARD)
        return
    
    if context.user_data.get("awaiting_search"):
        await handle_search(update, context, text)
        return
    
    if context.user_data.get("awaiting_advance_phone"):
        await handle_advance_phone(update, context, text)
        return
    
    if context.user_data.get("awaiting_advance_amount"):
        await handle_advance_amount(update, context, text)
        return
    
    if context.user_data.get("awaiting_convert_phone"):
        await handle_convert_phone(update, context, text)
        return
    
    if context.user_data.get("awaiting_manual_phone"):
        await handle_manual_phone(update, context, text)
        return
    
    if context.user_data.get("awaiting_manual_courier"):
        await handle_manual_courier(update, context, text)
        return
    
    if context.user_data.get("awaiting_manual_awb"):
        await handle_manual_awb(update, context, text)
        return
    
    if context.user_data.get("awaiting_product"):
        parts = text.strip().split()
        if len(parts) < 5:
            await update.message.reply_text("Format: ProductName length breadth height weight", reply_markup=MAIN_KEYBOARD)
            return
        try:
            l,b,h,w = float(parts[-4]), float(parts[-3]), float(parts[-2]), float(parts[-1])
            name = " ".join(parts[:-4])
            products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
            products[name] = {"length": l, "breadth": b, "height": h, "weight": w}
            json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)
            context.user_data.pop("awaiting_product")
            await update.message.reply_text(f"Product saved: {name}", reply_markup=MAIN_KEYBOARD)
        except:
            await update.message.reply_text("Invalid dimensions", reply_markup=MAIN_KEYBOARD)
        return
    
    if context.user_data.get("awaiting_shipment"):
        await handle_create_shipment(update, context, text)
        return
    
    await update.message.reply_text("Use the buttons below", reply_markup=MAIN_KEYBOARD)

async def handle_search(update, context, text):
    try:
        order = None
        if re.match(r'^\d{10}$', text.strip()):
            order = find_order_by_phone(text.strip())
        else:
            order = find_order_by_awb(text.strip())
        
        if not order:
            await update.message.reply_text("No order found", reply_markup=MAIN_KEYBOARD)
        else:
            await update.message.reply_text(format_order_details(order), reply_markup=MAIN_KEYBOARD)
        context.user_data.pop("awaiting_search", None)
    except Exception as e:
        await update.message.reply_text(f"Error: {e}", reply_markup=MAIN_KEYBOARD)
        context.user_data.pop("awaiting_search", None)

async def handle_advance_phone(update, context, text):
    phone = text.strip()
    order = find_order_by_phone(phone)
    if not order:
        await update.message.reply_text("Order not found", reply_markup=MAIN_KEYBOARD)
        context.user_data.pop("awaiting_advance_phone", None)
        return
    
    context.user_data["advance_phone"] = phone
    context.user_data.pop("awaiting_advance_phone", None)
    
    keyboard = [[
        InlineKeyboardButton("500", callback_data="advance_500"),
        InlineKeyboardButton("600", callback_data="advance_600"),
        InlineKeyboardButton("700", callback_data="advance_700")
    ]]
    await update.message.reply_text(f"Advance amount for {phone}?", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_advance_amount(update, context, text):
    try:
        amount = float(text.strip())
        phone = context.user_data.get("advance_phone")
        if mark_advance_paid(phone, amount):
            await update.message.reply_text(f"Advance recorded: Rs{amount}", reply_markup=MAIN_KEYBOARD)
        else:
            await update.message.reply_text("Failed", reply_markup=MAIN_KEYBOARD)
        context.user_data.clear()
    except:
        await update.message.reply_text("Invalid amount", reply_markup=MAIN_KEYBOARD)

async def handle_convert_phone(update, context, text):
    phone = text.strip()
    order = find_order_by_phone(phone)
    if not order or not order.get('shiprocket'):
        await update.message.reply_text("No active shipment", reply_markup=MAIN_KEYBOARD)
        context.user_data.pop("awaiting_convert_phone", None)
        return
    
    context.user_data["convert_order"] = order
    context.user_data.pop("awaiting_convert_phone", None)
    
    keyboard = [[
        InlineKeyboardButton("Yes", callback_data="convert_confirm"),
        InlineKeyboardButton("No", callback_data="convert_cancel")
    ]]
    await update.message.reply_text(
        f"Convert to Full COD?\nOrder #{order.get('order_number')}\nAWB: {order['shiprocket'].get('awb')}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_manual_phone(update, context, text):
    phone = text.strip()
    order = find_order_by_phone(phone)
    if not order:
        await update.message.reply_text("Order not found", reply_markup=MAIN_KEYBOARD)
        context.user_data.pop("awaiting_manual_phone", None)
        return
    
    context.user_data["manual_phone"] = phone
    context.user_data.pop("awaiting_manual_phone", None)
    context.user_data["awaiting_manual_courier"] = True
    
    if order.get('shiprocket', {}).get('status') == 'active':
        keyboard = [[
            InlineKeyboardButton("Yes, Cancel", callback_data="manual_cancel_yes"),
            InlineKeyboardButton("Keep Both", callback_data="manual_cancel_no")
        ]]
        await update.message.reply_text("Cancel Shiprocket shipment?", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text("Enter vendor courier name:", reply_markup=MAIN_KEYBOARD)

async def handle_manual_courier(update, context, text):
    context.user_data["manual_courier"] = text.strip()
    context.user_data.pop("awaiting_manual_courier", None)
    context.user_data["awaiting_manual_awb"] = True
    await update.message.reply_text(f"Courier: {text.strip()}\n\nEnter tracking/AWB:", reply_markup=MAIN_KEYBOARD)

async def handle_manual_awb(update, context, text):
    awb = text.strip()
    phone = context.user_data.get("manual_phone")
    courier = context.user_data.get("manual_courier")
    
    if add_manual_shipment(phone, courier, awb):
        await update.message.reply_text(
            f"Manual Entry Added!\nCourier: {courier}\nAWB: {awb}",
            reply_markup=MAIN_KEYBOARD
        )
    else:
        await update.message.reply_text("Failed", reply_markup=MAIN_KEYBOARD)
    context.user_data.clear()

async def show_stats(update, context):
    try:
        today = get_today_stats()
        week = get_week_stats()
        
        text = f"""TODAY'S STATS

Orders: {today.get('total_orders', 0)}
Revenue: Rs{today.get('total_revenue', 0):,}
Advances: Rs{today.get('total_advances', 0):,}

Payment:
- Advance: {today.get('advance_paid_count', 0)}
- Full COD: {today.get('full_cod_count', 0)}

Shipping:
- Shiprocket: {today.get('shiprocket_count', 0)}
- Manual: {today.get('manual_count', 0)}

THIS WEEK

Orders: {week.get('total_orders', 0)}
Revenue: Rs{week.get('total_revenue', 0):,}
Conversion: {week.get('advance_conversion', 0):.1f}%
"""
        await update.message.reply_text(text, reply_markup=MAIN_KEYBOARD)
    except Exception as e:
        await update.message.reply_text(f"Error: {e}", reply_markup=MAIN_KEYBOARD)

async def handle_create_shipment(update, context, text):
    try:
        formatted = ai_format_address(text)
        data = {}
        for line in formatted.splitlines():
            if ":" in line:
                k,v = line.split(":",1)
                data[k.strip().lower()] = v.strip()
        
        payment_method, sub_total = parse_payment(data.get("prepaid/cod","Prepaid 0"))
        sr_payment_method = "COD" if payment_method.lower()=="cod" else "Prepaid"
        cod_amount = sub_total if sr_payment_method=="COD" else 0
        qty = int(data.get("quantity","1"))
        
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        product_data = products.get(data.get("product",""), DEFAULT_PRODUCT)
        
        pickup_obj = normalize_pickup_obj({"pickup": data.get("pickup")})
        if not pickup_obj:
            await update.message.reply_text("Pickup not found", reply_markup=MAIN_KEYBOARD)
            return
        
        payload = {
            "order_id": f"ORDER{int(time.time())}_{uuid.uuid4().hex[:6]}",
            "order_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "pickup_location": pickup_obj.get("pickup_location"),
            "billing_customer_name": data.get("name","Customer"),
            "billing_last_name": ".",
            "billing_address": data.get("address",""),
            "billing_city": data.get("city",""),
            "billing_state": data.get("state",""),
            "billing_country": "India",
            "billing_pincode": data.get("pincode","110001"),
            "billing_email": "na@example.com",
            "billing_isd_code":"91",
            "billing_phone": data.get("phone",""),
            "billing_alternate_phone": data.get("alternate phone",""),
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
            "sub_total": sub_total,
            "cod_amount": cod_amount,
            "length": float(product_data.get("length")),
            "breadth": float(product_data.get("breadth")),
            "height": float(product_data.get("height")),
            "weight": float(product_data.get("weight")),
        }
        
        resp, err = create_order(payload)
        if not resp:
            await update.message.reply_text(f"Error: {err}", reply_markup=MAIN_KEYBOARD)
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
            await update.message.reply_text("No couriers available", reply_markup=MAIN_KEYBOARD)
            return
        
        tracking_link = f"https://shiprocket.co/tracking/{awb}"
        
        count_file = "order_count.json"
        count_data = json.load(open(count_file)) if os.path.exists(count_file) else {"count": 0}
        count_data["count"] = count_data.get("count",0) + 1
        json.dump(count_data, open(count_file,"w"), indent=2)
        order_number = count_data["count"]
        
        order_data = {
            "order_id": payload["order_id"],
            "order_number": order_number,
            "created_at": datetime.now().isoformat(),
            "phone": data.get("phone"),
            "customer_name": data.get("name"),
            "address": data.get("address"),
            "city": data.get("city"),
            "state": data.get("state"),
            "pincode": data.get("pincode"),
            "product": data.get("product"),
            "creative": data.get("creative", ""),
            "payment_300_paid": True,
            "payment_300_date": datetime.now().isoformat(),
            "advance_amount": 0,
            "advance_paid": False,
            "total": sub_total,
            "type": "advance_pending" if sr_payment_method != "COD" else "full_cod",
            "shiprocket": {
                "shipment_id": shipment_id,
                "awb": awb,
                "courier": courier.get("courier_name"),
                "rate": rate,
                "tracking": tracking_link,
                "status": "active",
                "pickup_scheduled": False
            },
            "vendor_shipment": None,
            "status": "active",
            "pickup_location": pickup_obj.get("pickup_location")
        }
        save_order(order_data)
        
        await update.message.reply_text(
            f"Shipment Created!\n"
            f"Order: #{order_number}\n"
            f"Courier: {courier.get('courier_name')}\n"
            f"Rate: Rs{rate}\n"
            f"AWB: {awb}\n"
            f"Tracking: {tracking_link}\n\n"
            f"Saved to database",
            reply_markup=MAIN_KEYBOARD
        )
        
        label_url = generate_label(shipment_id)
        if label_url:
            async with aiohttp.ClientSession() as session_http:
                async with session_http.get(label_url) as resp_pdf:
                    if resp_pdf.status == 200:
                        data_pdf = await resp_pdf.read()
                        await update.message.reply_document(document=data_pdf, filename=f"{awb}.pdf")
            
            keyboard = [[
                InlineKeyboardButton("Yes", callback_data=f"schedule_yes_{shipment_id}_{data.get('phone')}"),
                InlineKeyboardButton("No", callback_data=f"schedule_no_{shipment_id}")
            ]]
            await update.message.reply_text("Schedule pickup?", reply_markup=InlineKeyboardMarkup(keyboard))
    
    except Exception as e:
        log.error(f"Shipment error: {e}")
        await update.message.reply_text(f"Error: {e}", reply_markup=MAIN_KEYBOARD)
    finally:
        context.user_data.pop("awaiting_shipment", None)

async def handle_callback(update, context):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    
    context.user_data.pop("awaiting_product", None)
    context.user_data.pop("editing_product", None)
    context.user_data.pop("awaiting_shipment", None)
    
    if data.startswith("advance_"):
        if data == "advance_custom":
            context.user_data["awaiting_advance_amount"] = True
            await query.message.reply_text("Enter custom amount:", reply_markup=MAIN_KEYBOARD)
        else:
            amount = float(data.split("_")[1])
            phone = context.user_data.get("advance_phone")
            if mark_advance_paid(phone, amount):
                await query.message.reply_text(f"Advance: Rs{amount}", reply_markup=MAIN_KEYBOARD)
            else:
                await query.message.reply_text("Failed", reply_markup=MAIN_KEYBOARD)
            context.user_data.pop("advance_phone", None)
        return
    
    if data == "convert_confirm":
        order = context.user_data.get("convert_order")
        shipment_id = order['shiprocket']['shipment_id']
        success, msg = cancel_shipment(shipment_id)
        if success:
            await query.message.reply_text("Cancelled! Enter new COD amount:", reply_markup=MAIN_KEYBOARD)
            context.user_data["awaiting_convert_cod_amount"] = True
        else:
            await query.message.reply_text(f"Failed: {msg}", reply_markup=MAIN_KEYBOARD)
            context.user_data.pop("convert_order", None)
        return
    
    if data == "convert_cancel":
        await query.message.reply_text("Cancelled", reply_markup=MAIN_KEYBOARD)
        context.user_data.pop("convert_order", None)
        return
    
    if data.startswith("manual_cancel_"):
        phone = context.user_data.get("manual_phone")
        if data == "manual_cancel_yes":
            order = find_order_by_phone(phone)
            if order and order.get('shiprocket'):
                cancel_shipment(order['shiprocket']['shipment_id'])
            await query.message.reply_text("Cancelled! Enter courier name:", reply_markup=MAIN_KEYBOARD)
        else:
            await query.message.reply_text("Enter courier name:", reply_markup=MAIN_KEYBOARD)
        context.user_data["awaiting_manual_courier"] = True
        return
    
    if data.startswith("delete_"):
        name = data.split("delete_", 1)[1]
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        if name in products:
            products.pop(name)
            json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)
            await query.edit_message_text(f"Deleted: {name}")
        return
    
    if data.startswith("edit_"):
        name = data.split("edit_", 1)[1]
        products = json.load(open(PRODUCTS_FILE)) if os.path.exists(PRODUCTS_FILE) else {}
        if name in products:
            context.user_data["editing_product"] = name
            context.user_data["awaiting_product"] = True
            await query.message.reply_text(
                f"Send new details for '{name}':\nName length breadth height weight",
                reply_markup=MAIN_KEYBOARD
            )
        return
    
    if data.startswith("schedule_yes_"):
        parts = data.replace("schedule_yes_", "").split("_")
        shipment_id = parts[0]
        phone = parts[1] if len(parts) > 1 else None
        
        ok, msg = schedule_pickup([shipment_id])
        
        if ok and phone:
            from orders_manager import load_orders, save_orders
            orders = load_orders()
            for order in orders:
                if order.get('phone') == phone and order.get('shiprocket', {}).get('shipment_id') == shipment_id:
                    order['shiprocket']['pickup_scheduled'] = True
                    order['shiprocket']['pickup_scheduled_at'] = datetime.now().isoformat()
                    break
            save_orders(orders)
        
        await query.edit_message_text(msg)
        return
    
    if data.startswith("schedule_no_"):
        await query.edit_message_text("Pickup not scheduled")
        return

async def main():
    log.info("Enhanced Bot starting...")
    
    try:
        get_token()
        log.info("Shiprocket authenticated")
    except Exception as e:
        log.error(f"Shiprocket failed: {e}")
        raise
    
    ok, msg = refresh_pickups()
    log.info(msg)
    
    log.info(f"Building bot...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    log.info("Handlers registered")
    log.info("All features enabled")
    log.info("Starting polling...")
    
    await app.run_polling()

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())
    