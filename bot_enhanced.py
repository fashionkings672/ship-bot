# bot_enhanced.py

“””
Enhanced Backbenchers Telegram Bot - Phase 1
Includes: Search, Mark Advance, Convert COD, Manual Entry, Stats
“””

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

# Import our shared orders manager from shared/ folder

from shared.orders_manager import (
save_order, find_order_by_phone, find_order_by_awb,
mark_advance_paid, convert_to_full_cod, add_manual_shipment,
get_today_stats, get_week_stats, format_order_details
)

# –––––––– CONFIG ––––––––

BOT_TOKEN = os.getenv(“BOT_TOKEN”)
SHIPROCKET_EMAIL = os.getenv(“SHIPROCKET_EMAIL”)
SHIPROCKET_PASSWORD = os.getenv(“SHIPROCKET_PASSWORD”)
OPENAI_API_KEY = os.getenv(“OPENAI_API_KEY”)

# Debug logging

print(”=” * 60)
print(“🔍 ENVIRONMENT VARIABLES CHECK”)
print(”=” * 60)
print(f”BOT_TOKEN exists: {BOT_TOKEN is not None}”)
if BOT_TOKEN:
print(f”BOT_TOKEN preview: {BOT_TOKEN[:15]}…”)
else:
print(“BOT_TOKEN preview: NONE ❌”)
print(f”SHIPROCKET_EMAIL exists: {SHIPROCKET_EMAIL is not None}”)
print(f”SHIPROCKET_PASSWORD exists: {SHIPROCKET_PASSWORD is not None}”)
print(f”OPENAI_API_KEY exists: {OPENAI_API_KEY is not None}”)
print(”=” * 60)

# Safety checks

if not BOT_TOKEN:
raise ValueError(“❌ BOT_TOKEN is not set!”)
if not SHIPROCKET_EMAIL or not SHIPROCKET_PASSWORD:
raise ValueError(“❌ SHIPROCKET credentials not set!”)
if not OPENAI_API_KEY:
raise ValueError(“❌ OPENAI_API_KEY not set!”)

openai.api_key = OPENAI_API_KEY

CUSTOM_CHANNEL_ID = None
if os.path.exists(“custom_channel.json”):
try:
CUSTOM_CHANNEL_ID = json.load(open(“custom_channel.json”)).get(“id”)
except Exception:
CUSTOM_CHANNEL_ID = None

SHIPROCKET_BASE = “https://apiv2.shiprocket.in/v1/external”
URLS = {
“login”: “/auth/login”,
“pickup”: “/settings/company/pickup”,
“create_order”: “/orders/create/adhoc”,
“courier_get”: “/courier/serviceability/”,
“assign_awb”: “/courier/assign/awb”,
“label”: “/courier/generate/label”,
“get_quote”: “/courier/charge/calculate”,
“generate_pickup”: “/courier/generate/pickup”,
“cancel_shipment”: “/orders/cancel/shipment/{}”,  # NEW
}
COURIER_PRIORITY = [“bluedart”, “delhivery”, “dtdc”]
PRODUCTS_FILE = “products.json”
DEFAULT_PRODUCT = {“length”:10,“breadth”:8,“height”:5,“weight”:0.5}

logging.basicConfig(
level=logging.INFO,
format=’%(asctime)s - %(name)s - %(levelname)s - %(message)s’
)
log = logging.getLogger(“telegram_shipbot”)
session = requests.Session()
pickup_map = {}
shipment_awb_map = {}

# –––––––– HELPERS ––––––––

def strict_phone(ph):
if not ph:
return None
ph = re.sub(r”\D”, “”, str(ph))
return ph if len(ph) == 10 and ph[0] in “6789” else None

def parse_payment(payment_str):
m = re.match(r”(prepaid|cod)\s+(\d+.?\d*)”, (payment_str or “”).strip(), re.I)
if not m:
return “Prepaid”, 0
return m.group(1).capitalize(), float(m.group(2))

def normalize_pickup_obj(parsed):
if parsed.get(“pickup”):
k = re.sub(r”\W”,””,parsed[“pickup”].lower())
for key, obj in pickup_map.items():
norm_key = re.sub(r”\W”,””,key.lower())
if k == norm_key or k in norm_key or norm_key in k:
return obj
return next(iter(pickup_map.values()), None)

# –––––––– SHIPROCKET LOGIN / PICKUP ––––––––

auth_token = None
token_expiry = 0

def get_token(force_refresh=False):
global auth_token, token_expiry
if not force_refresh and auth_token and time.time() < token_expiry:
return auth_token
try:
log.info(“🔐 Logging into Shiprocket…”)
r = session.post(
SHIPROCKET_BASE + URLS[“login”],
json={“email”: SHIPROCKET_EMAIL, “password”: SHIPROCKET_PASSWORD},
timeout=60
)
data = r.json() if r else {}
if “token” not in data:
raise Exception(f”Login failed: {data}”)
auth_token = data[“token”]
token_expiry = time.time() + (23 * 3600)
session.headers.update({“Authorization”: f”Bearer {auth_token}”})
log.info(“✅ Shiprocket token obtained”)
return auth_token
except Exception as e:
log.error(f”❌ Shiprocket login failed: {e}”)
raise Exception(f”Shiprocket login failed: {e}”)

def ensure_valid_token():
try:
get_token()
except Exception:
get_token(force_refresh=True)

def refresh_pickups():
global pickup_map
try:
ensure_valid_token()
log.info(“📍 Fetching pickup locations…”)
r = session.get(SHIPROCKET_BASE + URLS[“pickup”], timeout=60)
if r.status_code != 200:
return False, f”❌ Pickup fetch failed: {r.status_code} {r.text}”
try:
data = r.json()
except Exception:
return False, f”❌ Invalid JSON response: {r.text}”
lst = data.get(“data”, {}).get(“shipping_address”, [])
pickup_map = {
p[“pickup_location”].lower(): p
for p in lst
if p.get(“pickup_location”)
}
log.info(f”✅ Loaded {len(pickup_map)} pickup locations”)
return True, f”✅ Loaded {len(pickup_map)} pickups”
except requests.exceptions.ConnectTimeout:
return False, “⚠️ Shiprocket pickup API timed out. Try again later.”
except Exception as e:
log.error(f”❌ Pickup refresh error: {e}”)
return False, f”❌ Pickup refresh error: {e}”

# –––––––– OPENAI ADDRESS FORMATTING ––––––––

def ai_format_address(raw_text):
prompt = f”””
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
Creative: <creative_code_if_present_else_leave_blank>
“””
try:
response = openai.chat.completions.create(
model=“gpt-4”,
messages=[{“role”:“user”,“content”:prompt}],
temperature=0.3
)
formatted_text = response.choices[0].message.content.strip()
return formatted_text
except Exception as e:
log.error(f”❌ OpenAI API error: {e}”)
raise

# –––––––– SHIPROCKET API ––––––––

def get_available_couriers(pickup_pin, delivery_pin, weight, cod):
try:
r = session.get(SHIPROCKET_BASE + URLS[“courier_get”], params={
“pickup_postcode”: str(pickup_pin),
“delivery_postcode”: str(delivery_pin),
“cod”: int(bool(cod)),
“weight”: weight
}, timeout=60)
if r.status_code != 200: return []
return r.json().get(“data”, {}).get(“available_courier_companies”, []) or []
except Exception as e:
log.error(f”❌ Error getting couriers: {e}”)
return []

def get_shipping_quote(pickup_pin, delivery_pin, weight, cod):
try:
r = session.get(SHIPROCKET_BASE + URLS[“get_quote”], params={
“pickup_postcode”: pickup_pin,
“delivery_postcode”: delivery_pin,
“weight”: weight,
“cod”: int(bool(cod))
}, timeout=60)
if r.status_code != 200: return None
return r.json().get(“data”, {}).get(“rate”)
except Exception:
return None

def assign_awb(shipment_id, courier_id=None):
try:
payload = {“shipment_id”: shipment_id}
if courier_id:
payload[“courier_id”] = courier_id
r = session.post(SHIPROCKET_BASE + URLS[“assign_awb”], json=payload, timeout=40)
resp_json = r.json()
if resp_json.get(“awb_assign_status”) == 1:
return resp_json[“response”][“data”][“awb_code”]
return None
except Exception as e:
log.error(f”AWB assignment error: {e}”)
return None

def generate_label(shipment_id):
try:
r = session.post(SHIPROCKET_BASE + URLS[“label”], json={“shipment_id”:[shipment_id]}, timeout=40)
resp_json = r.json() if r else {}
if not resp_json or resp_json.get(“label_created”) != 1:
return None
return resp_json.get(“label_url”)
except Exception as e:
log.error(f”Label generation error: {e}”)
return None

def create_order(payload):
try:
ensure_valid_token()
r = session.post(SHIPROCKET_BASE + URLS[“create_order”], json=payload, timeout=40)
resp_json = r.json() if r else None
if r.status_code!=200 or (resp_json and resp_json.get(“status_code”) not in (1,200)):
return None, r.text
return resp_json, None
except Exception as e:
return None, str(e)

def cancel_shipment(shipment_id):
“”“NEW: Cancel a shipment in Shiprocket”””
try:
ensure_valid_token()
url = SHIPROCKET_BASE + URLS[“cancel_shipment”].format(shipment_id)
r = session.post(url, timeout=40)
resp_json = r.json() if r else {}
if r.status_code == 200:
log.info(f”✅ Cancelled shipment: {shipment_id}”)
return True, “Shipment cancelled successfully”
else:
log.error(f”❌ Cancel failed: {resp_json}”)
return False, str(resp_json)
except Exception as e:
log.error(f”❌ Cancel error: {e}”)
return False, str(e)

def schedule_pickup(shipment_ids, pickup_date=None, time_slot_id=None):
try:
payload = {“shipment_id”: shipment_ids}
if pickup_date:
payload[“pickup_date”] = pickup_date
if time_slot_id:
payload[“time_slot_id”] = time_slot_id
r = session.post(SHIPROCKET_BASE + URLS[“generate_pickup”], json=payload, timeout=40)
try:
resp_json = r.json()
except Exception:
return False, f”❌ Invalid response: {r.text}”
response_data = resp_json.get(“response”, {})
status = resp_json.get(“status”) or response_data.get(“status”)
pickup_id = (
resp_json.get(“pickup_id”)
or response_data.get(“pickup_id”)
or resp_json.get(“pickup_token_number”)
or response_data.get(“pickup_token_number”)
)
pickup_date_str = response_data.get(“pickup_scheduled_date”)
if r.status_code == 200:
if resp_json.get(“pickup_scheduled”) or status == 1:
return True, f”✅ Pickup scheduled successfully! Pickup ID: {pickup_id or ‘N/A’}”
if status == 3:
return True, f”✅ Pickup already scheduled for {pickup_date_str}.”
if “already generated” in str(resp_json).lower():
return False, f”⚠️ Pickup already generated.”
return False, f”❌ Pickup not scheduled: {resp_json}”
else:
return False, f”❌ API Error {r.status_code}: {resp_json}”
except Exception as e:
return False, f”⚠️ Error scheduling pickup: {e}”

def create_shipment_with_fallback(shipment_id, pickup_pin, delivery_pin, weight, cod):
couriers = get_available_couriers(pickup_pin, delivery_pin, weight, cod)
if not couriers: return None, None, None
def mode_pref(c):
m = str(c.get(“mode”) or c.get(“service_type”) or “”).lower()
if “surface” in m: return 0
if “air” in m: return 1
return 2
priority_json = None
if os.path.exists(“courier_priority.json”):
try:
priority_json = json.load(open(“courier_priority.json”))
except Exception:
priority_json = None
def priority_key(c):
if priority_json:
name = str(c.get(“courier_name”) or “”).strip()
mode = str(c.get(“mode”) or c.get(“service_type”) or “”).strip()
key = f”{name}{(’ ’ + mode.title()) if mode else ‘’}”
val = priority_json.get(key)
if isinstance(val, int):
return (val, mode_pref(c), c.get(“rate”, 1e12))
name_lower = str(c.get(“courier_name”) or “”).lower()
if “bluedart” in name_lower: base = 1
elif “delhivery” in name_lower: base = 2
elif “dtdc” in name_lower: base = 3
else: base = 99
return (base, mode_pref(c), c.get(“rate”,1e12))
couriers_sorted = sorted(couriers, key=lambda c: priority_key(c))
for courier in couriers_sorted:
courier_id = (courier.get(“courier_company_id”) or
courier.get(“courier_id”) or
courier.get(“courierId”) or
courier.get(“id”))
if not courier_id:
log.info(f”Skipping courier {courier.get(‘courier_name’)} (no ID found)”)
continue
try:
awb = assign_awb(shipment_id, courier_id)
log.info(f”Trying courier {courier.get(‘courier_name’)} -> AWB: {awb}”)
except Exception as e:
log.error(f”Error assigning AWB for courier {courier.get(‘courier_name’)}: {e}”)
awb = None
if awb:
shipment_awb_map[shipment_id] = awb
return courier, awb, courier.get(“rate”)
return None, None, None

# –––––––– TELEGRAM BOT ––––––––

# NEW: Enhanced keyboard with all features

MAIN_KEYBOARD = ReplyKeyboardMarkup(
[
[“➕ Add Product”, “📋 View Products”],
[“📦 Create Shipment”, “🔍 Search Order”],
[“💰 Mark Advance”, “🔄 Convert COD”],
[“📝 Manual Entry”, “📊 Stats”],
[“🔙 Cancel”]
],
resize_keyboard=True
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
context.user_data.clear()
welcome_text = “”“👋 Welcome to Backbenchers Bot!

✅ Your Features:
• Create Shipment (AI-powered)
• Search Orders
• Mark Advance Payments
• Convert to Full COD
• Manual Vendor Entry
• View Statistics

Use the buttons below to get started!”””
await update.message.reply_text(welcome_text, reply_markup=MAIN_KEYBOARD)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
text = update.message.text.strip()

```
# --- Existing product editing ---
if context.user_data.get("editing_product"):
    try:
        parts = text.split()
        if len(parts) < 5:
            raise ValueError("bad format")
        name = " ".join(parts[:-4])
        l = float(parts[-4]); b = float(parts[-3]); h = float(parts[-2]); w = float(parts[-1])
        products = {}
        if os.path.exists(PRODUCTS_FILE):
            products = json.load(open(PRODUCTS_FILE))
        old_name = context.user_data.pop("editing_product", None)
        if old_name:
            products.pop(old_name, None)
        products[name] = {"length": l, "breadth": b, "height": h, "weight": w}
        json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)
        await update.message.reply_text(f"✅ Product updated: {name}", reply_markup=MAIN_KEYBOARD)
    except Exception:
        await update.message.reply_text("❌ Wrong format. Use:\nName length breadth height weight", reply_markup=MAIN_KEYBOARD)
    return

# --- Keyboard actions ---
if text == "➕ Add Product":
    context.user_data["awaiting_product"] = True
    context.user_data["awaiting_shipment"] = False
    await update.message.reply_text(
        "Send product in format:\nProductName length breadth height weight",
        reply_markup=MAIN_KEYBOARD
    )
    return

if text == "📋 View Products":
    products = {}
    if os.path.exists(PRODUCTS_FILE):
        products = json.load(open(PRODUCTS_FILE))
    if not products:
        await update.message.reply_text("⚠️ No products saved yet.", reply_markup=MAIN_KEYBOARD)
        return
    for name, prod in products.items():
        text_prod = f"{name}: {prod['length']}x{prod['breadth']}x{prod['height']} | {prod['weight']}kg"
        kb = [[
            InlineKeyboardButton("✏ Edit", callback_data=f"edit_{name}"),
            InlineKeyboardButton("❌ Delete", callback_data=f"delete_{name}")
        ]]
        await update.message.reply_text(text_prod, reply_markup=InlineKeyboardMarkup(kb))
    return

if text == "📦 Create Shipment":
    context.user_data["awaiting_shipment"] = True
    context.user_data["awaiting_product"] = False
    await update.message.reply_text("Send messy address/order to create shipment.", reply_markup=MAIN_KEYBOARD)
    return

# NEW: Search Order
if text == "🔍 Search Order":
    context.user_data["awaiting_search"] = True
    await update.message.reply_text(
        "🔍 Enter phone number or AWB to search:",
        reply_markup=MAIN_KEYBOARD
    )
    return

# NEW: Mark Advance
if text == "💰 Mark Advance":
    context.user_data["awaiting_advance_phone"] = True
    await update.message.reply_text(
        "💰 Enter phone number to mark advance:",
        reply_markup=MAIN_KEYBOARD
    )
    return

# NEW: Convert COD
if text == "🔄 Convert COD":
    context.user_data["awaiting_convert_phone"] = True
    await update.message.reply_text(
        "🔄 Enter phone number to convert to Full COD:",
        reply_markup=MAIN_KEYBOARD
    )
    return

# NEW: Manual Entry
if text == "📝 Manual Entry":
    context.user_data["awaiting_manual_phone"] = True
    await update.message.reply_text(
        "📝 Enter phone number for manual entry:",
        reply_markup=MAIN_KEYBOARD
    )
    return

# NEW: Stats
if text == "📊 Stats":
    await show_stats(update, context)
    return

if text == "🔙 Cancel":
    context.user_data.clear()
    await update.message.reply_text("✅ Cancelled. Back to main menu.", reply_markup=MAIN_KEYBOARD)
    return

# --- Handle awaiting states ---

# NEW: Handle search
if context.user_data.get("awaiting_search"):
    await handle_search(update, context, text)
    return

# NEW: Handle advance phone
if context.user_data.get("awaiting_advance_phone"):
    await handle_advance_phone(update, context, text)
    return

# NEW: Handle advance amount
if context.user_data.get("awaiting_advance_amount"):
    await handle_advance_amount(update, context, text)
    return

# NEW: Handle convert phone
if context.user_data.get("awaiting_convert_phone"):
    await handle_convert_phone(update, context, text)
    return

# NEW: Handle convert COD amount
if context.user_data.get("awaiting_convert_cod_amount"):
    await handle_convert_cod_amount(update, context, text)
    return

# NEW: Handle manual entry phone
if context.user_data.get("awaiting_manual_phone"):
    await handle_manual_phone(update, context, text)
    return

# NEW: Handle manual courier name
if context.user_data.get("awaiting_manual_courier"):
    await handle_manual_courier(update, context, text)
    return

# NEW: Handle manual AWB
if context.user_data.get("awaiting_manual_awb"):
    await handle_manual_awb(update, context, text)
    return

# Add product
if context.user_data.get("awaiting_product"):
    parts = text.strip().split()
    if len(parts) < 5:
        await update.message.reply_text("❌ Invalid format. Send: ProductName length breadth height weight", reply_markup=MAIN_KEYBOARD)
        return
    try:
        length = float(parts[-4])
        breadth = float(parts[-3])
        height = float(parts[-2])
        weight = float(parts[-1])
    except ValueError:
        await update.message.reply_text("❌ Dimensions and weight must be numbers.", reply_markup=MAIN_KEYBOARD)
        return
    product_name = " ".join(parts[:-4])
    products = {}
    if os.path.exists(PRODUCTS_FILE):
        products = json.load(open(PRODUCTS_FILE))
    products[product_name] = {"length": length,"breadth": breadth,"height": height,"weight": weight}
    json.dump(products, open(PRODUCTS_FILE,"w"), indent=2)
    context.user_data["awaiting_product"]=False
    await update.message.reply_text(f"✅ Product '{product_name}' saved successfully", reply_markup=MAIN_KEYBOARD)
    return

# Create shipment
if context.user_data.get("awaiting_shipment"):
    await handle_create_shipment(update, context, text)
    return

await update.message.reply_text("Please use the keyboard buttons.", reply_markup=MAIN_KEYBOARD)
```

# NEW: Handle search

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
try:
# Clean input
search_term = text.strip()

```
    # Try to find order
    order = None
    
    # Check if it's a phone number
    if re.match(r'^\d{10}$', search_term):
        order = find_order_by_phone(search_term)
    # Check if it's an AWB
    else:
        order = find_order_by_awb(search_term)
    
    if not order:
        await update.message.reply_text(
            "❌ No order found!\n\nTry:\n• 10-digit phone number\n• AWB number",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.pop("awaiting_search", None)
        return
    
    # Display order with action buttons
    order_text = format_order_details(order)
    
    keyboard = [
        [
            InlineKeyboardButton("💰 Mark Advance", callback_data=f"adv_{order['phone']}"),
            InlineKeyboardButton("🔄 Convert COD", callback_data=f"cod_{order['phone']}")
        ],
        [
            InlineKeyboardButton("📝 Manual Entry", callback_data=f"manual_{order['phone']}")
        ]
    ]
    
    await update.message.reply_text(
        order_text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    context.user_data.pop("awaiting_search", None)
    
except Exception as e:
    log.error(f"Search error: {e}")
    await update.message.reply_text(f"❌ Error: {e}", reply_markup=MAIN_KEYBOARD)
    context.user_data.pop("awaiting_search", None)
```

# NEW: Handle advance phone

async def handle_advance_phone(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
phone = text.strip()

```
order = find_order_by_phone(phone)
if not order:
    await update.message.reply_text("❌ Order not found!", reply_markup=MAIN_KEYBOARD)
    context.user_data.pop("awaiting_advance_phone", None)
    return

# Store phone and show amount buttons
context.user_data["advance_phone"] = phone
context.user_data.pop("awaiting_advance_phone", None)

keyboard = [
    [
        InlineKeyboardButton("₹500", callback_data="advance_500"),
        InlineKeyboardButton("₹600", callback_data="advance_600"),
        InlineKeyboardButton("₹700", callback_data="advance_700")
    ],
    [InlineKeyboardButton("Custom Amount", callback_data="advance_custom")]
]

await update.message.reply_text(
    f"💰 How much advance for {phone}?",
    reply_markup=InlineKeyboardMarkup(keyboard)
)
```

# NEW: Handle advance amount

async def handle_advance_amount(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
try:
amount = float(text.strip())
phone = context.user_data.get(“advance_phone”)

```
    if mark_advance_paid(phone, amount):
        await update.message.reply_text(
            f"✅ Advance Recorded!\n\nPhone: {phone}\nAdvance: ₹{amount:,.0f}\n\n✅ Saved to orders.json",
            reply_markup=MAIN_KEYBOARD
        )
    else:
        await update.message.reply_text("❌ Failed to update order", reply_markup=MAIN_KEYBOARD)
    
    context.user_data.pop("awaiting_advance_amount", None)
    context.user_data.pop("advance_phone", None)
    
except ValueError:
    await update.message.reply_text("❌ Please enter a valid number", reply_markup=MAIN_KEYBOARD)
```

# NEW: Handle convert phone

async def handle_convert_phone(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
phone = text.strip()

```
order = find_order_by_phone(phone)
if not order:
    await update.message.reply_text("❌ Order not found!", reply_markup=MAIN_KEYBOARD)
    context.user_data.pop("awaiting_convert_phone", None)
    return

# Check if has Shiprocket shipment
if not order.get('shiprocket'):
    await update.message.reply_text(
        "❌ No Shiprocket shipment found for this order!",
        reply_markup=MAIN_KEYBOARD
    )
    context.user_data.pop("awaiting_convert_phone", None)
    return

# Store order details
context.user_data["convert_order"] = order
context.user_data.pop("awaiting_convert_phone", None)

# Ask for confirmation
keyboard = [
    [
        InlineKeyboardButton("✅ Yes, Convert", callback_data="convert_confirm"),
        InlineKeyboardButton("❌ No, Cancel", callback_data="convert_cancel")
    ]
]

await update.message.reply_text(
    f"⚠️ Convert to Full COD?\n\n"
    f"Order #{order.get('order_number')}\n"
    f"Current AWB: {order['shiprocket'].get('awb')}\n\n"
    f"This will:\n"
    f"1. Cancel current shipment\n"
    f"2. Create new Full COD shipment",
    reply_markup=InlineKeyboardMarkup(keyboard)
)
```

# NEW: Handle convert COD amount

async def handle_convert_cod_amount(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
try:
cod_amount = float(text.strip())
order = context.user_data.get(“convert_order”)

```
    await update.message.reply_text("🔄 Creating new shipment...", reply_markup=MAIN_KEYBOARD)
    
    # Prepare payload for new order (same as original but COD)
    data = {
        "pickup": order.get("pickup_location", ""),
        "product": order.get("product", ""),
        "name": order.get("customer_name", ""),
        "address": order.get("address", ""),
        "city": order.get("city", ""),
        "state": order.get("state", ""),
        "pincode": order.get("pincode", ""),
        "phone": order.get("phone", ""),
        "creative": order.get("creative", "")
    }
    
    # Create new shipment with COD
    await create_full_cod_shipment(update, context, data, cod_amount)
    
    context.user_data.pop("awaiting_convert_cod_amount", None)
    context.user_data.pop("convert_order", None)
    
except ValueError:
    await update.message.reply_text("❌ Please enter a valid COD amount", reply_markup=MAIN_KEYBOARD)
```

# NEW: Handle manual phone

async def handle_manual_phone(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
phone = text.strip()

```
order = find_order_by_phone(phone)
if not order:
    await update.message.reply_text("❌ Order not found!", reply_markup=MAIN_KEYBOARD)
    context.user_data.pop("awaiting_manual_phone", None)
    return

# Store phone
context.user_data["manual_phone"] = phone
context.user_data.pop("awaiting_manual_phone", None)
context.user_data["awaiting_manual_courier"] = True

# Ask for courier confirmation if Shiprocket exists
if order.get('shiprocket', {}).get('status') == 'active':
    keyboard = [[
        InlineKeyboardButton("✅ Yes, Cancel", callback_data="manual_cancel_yes"),
        InlineKeyboardButton("❌ No, Keep Both", callback_data="manual_cancel_no")
    ]]
    await update.message.reply_text(
        f"⚠️ Active Shiprocket shipment found!\n"
        f"AWB: {order['shiprocket'].get('awb')}\n\n"
        f"Cancel it and add vendor tracking?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
else:
    await update.message.reply_text(
        "📝 Enter vendor courier name:\n(e.g., BlueDart, Delhivery, DTDC)",
        reply_markup=MAIN_KEYBOARD
    )
```

# NEW: Handle manual courier

async def handle_manual_courier(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
courier_name = text.strip()

```
context.user_data["manual_courier"] = courier_name
context.user_data.pop("awaiting_manual_courier", None)
context.user_data["awaiting_manual_awb"] = True

await update.message.reply_text(
    f"✅ Courier: {courier_name}\n\n📝 Enter tracking/AWB number:",
    reply_markup=MAIN_KEYBOARD
)
```

# NEW: Handle manual AWB

async def handle_manual_awb(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
awb = text.strip()
phone = context.user_data.get(“manual_phone”)
courier = context.user_data.get(“manual_courier”)

```
if add_manual_shipment(phone, courier, awb):
    await update.message.reply_text(
        f"✅ Manual Entry Added!\n\n"
        f"Phone: {phone}\n"
        f"Courier: {courier}\n"
        f"AWB: {awb}\n\n"
        f"✅ Saved to orders.json",
        reply_markup=MAIN_KEYBOARD
    )
else:
    await update.message.reply_text("❌ Failed to add manual entry", reply_markup=MAIN_KEYBOARD)

context.user_data.pop("awaiting_manual_awb", None)
context.user_data.pop("manual_phone", None)
context.user_data.pop("manual_courier", None)
```

# NEW: Show stats

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
try:
today_stats = get_today_stats()
week_stats = get_week_stats()

```
    text = f"""
```

📊 TODAY’S STATS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📦 Orders: {today_stats.get(‘total_orders’, 0)}
💰 Revenue: ₹{today_stats.get(‘total_revenue’, 0):,}
💵 Advances: ₹{today_stats.get(‘total_advances’, 0):,}

Payment Types:
• Advance Paid: {today_stats.get(‘advance_paid_count’, 0)}
• Full COD: {today_stats.get(‘full_cod_count’, 0)}

Shipping:
• Shiprocket: {today_stats.get(‘shiprocket_count’, 0)}
• Manual: {today_stats.get(‘manual_count’, 0)}

By Creative:
“””

```
    for creative, count in today_stats.get('creative_breakdown', {}).items():
        text += f"• {creative}: {count} orders\n"
    
    text += f"""
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📅 THIS WEEK
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Orders: {week_stats.get(‘total_orders’, 0)}
Revenue: ₹{week_stats.get(‘total_revenue’, 0):,}
Advances: ₹{week_stats.get(‘total_advances’, 0):,}
Conversion: {week_stats.get(‘advance_conversion’, 0):.1f}%
“””

```
    await update.message.reply_text(text, reply_markup=MAIN_KEYBOARD)
    
except Exception as e:
    log.error(f"Stats error: {e}")
    await update.message.reply_text(f"❌ Error: {e}", reply_markup=MAIN_KEYBOARD)
```

# EXISTING: Create shipment function (with NEW: save to orders.json)

async def handle_create_shipment(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
try:
formatted = ai_format_address(text)
data = {}
for line in formatted.splitlines():
if “:” in line:
k,v=line.split(”:”,1)
data[k.strip().lower()]=v.strip()

```
    payment_method, sub_total = parse_payment(data.get("prepaid/cod","Prepaid 0"))
    sr_payment_method = "COD" if payment_method.lower()=="cod" else "Prepaid"
    cod_amount = sub_total if sr_payment_method=="COD" else 0
    qty = int(data.get("quantity","1"))
    
    products = {}
    if os.path.exists(PRODUCTS_FILE):
        products = json.load(open(PRODUCTS_FILE))
    product_data = products.get(data.get("product",""), DEFAULT_PRODUCT)
    
    pickup_obj = normalize_pickup_obj({"pickup": data.get("pickup")})
    if not pickup_obj:
        await update.message.reply_text("❌ Pickup not found in Shiprocket account", reply_markup=MAIN_KEYBOARD)
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
        payload["channel_id"] = CUSTOM_CHANNEL_ID

    # Create order
    resp, err = create_order(payload)
    if not resp:
        if "insufficient balance" in str(err).lower():
            await update.message.reply_text("❌ Insufficient wallet balance in Shiprocket. Please recharge.", reply_markup=MAIN_KEYBOARD)
        else:
            await update.message.reply_text(f"❌ Error creating shipment: {err}", reply_markup=MAIN_KEYBOARD)
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
        await update.message.reply_text("❌ No couriers available for this shipment", reply_markup=MAIN_KEYBOARD)
        return

    shipment_awb_map[shipment_id] = awb

    label_url = generate_label(shipment_id)
    tracking_link = f"https://shiprocket.co/tracking/{awb}" if awb else "N/A"

    # Increment order counter
    count_file = "order_count.json"
    count_data = {"count": 0}
    if os.path.exists(count_file):
        try:
            count_data = json.load(open(count_file))
        except:
            pass
    count_data["count"] = count_data.get("count",0) + 1
    json.dump(count_data, open(count_file,"w"), indent=2)
    order_number = count_data["count"]

    # NEW: Save to orders.json
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
        "advance_date": None,
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
        f"✅ Shipment Created!\nOrder No: {order_number}\nCourier: {courier.get('courier_name')}\nRate: {rate}\nAWB: {awb}\nTracking: {tracking_link}\n\n✅ Saved to orders.json",
        reply_markup=MAIN_KEYBOARD
    )

    if label_url:
        async with aiohttp.ClientSession() as session_http:
            async with session_http.get(label_url) as resp_pdf:
                if resp_pdf.status == 200:
                    data_pdf = await resp_pdf.read()
                    await update.message.reply_document(document=data_pdf, filename=f"{awb}.pdf")

        keyboard = [[
            InlineKeyboardButton("✅ Yes", callback_data=f"schedule_yes_{shipment_id}_{data.get('phone')}"),
            InlineKeyboardButton("❌ No", callback_data=f"schedule_no_{shipment_id}")
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Do you want to schedule pickup?", reply_markup=reply_markup)

except Exception as e:
    log.error(f"❌ Shipment creation error: {e}")
    await update.message.reply_text(f"⚠️ Error: {e}", reply_markup=MAIN_KEYBOARD)
finally:
    context.user_data["awaiting_shipment"]=False
```

# NEW: Create Full COD shipment

async def create_full_cod_shipment(update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict, cod_amount: float):
# Similar to handle_create_shipment but with COD
# Implementation similar to above but with COD payment method
pass

# –––––––– CALLBACK HANDLER ––––––––

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
query = update.callback_query
await query.answer()
data = query.data or “”

```
# Clear awaiting states
context.user_data.pop("awaiting_product", None)
context.user_data.pop("editing_product", None)
context.user_data.pop("awaiting_shipment", None)

# NEW: Handle advance amount callbacks
if data.startswith("advance_"):
    if data == "advance_custom":
        context.user_data["awaiting_advance_amount"] = True
        await query.message.reply_text(
            "💰 Enter custom advance amount:",
            reply_markup=MAIN_KEYBOARD
        )
    else:
        amount = float(data.split("_")[1])
        phone = context.user_data.get("advance_phone")
        
        if mark_advance_paid(phone, amount):
            await query.message.reply_text(
                f"✅ Advance Recorded!\n\nPhone: {phone}\nAdvance: ₹{amount:,.0f}\n\n✅ Saved to orders.json",
                reply_markup=MAIN_KEYBOARD
            )
        else:
            await query.message.reply_text("❌ Failed to update order", reply_markup=MAIN_KEYBOARD)
        
        context.user_data.pop("advance_phone", None)
    return

# NEW: Handle convert callbacks
if data == "convert_confirm":
    order = context.user_data.get("convert_order")
    
    # Cancel old shipment
    shipment_id = order['shiprocket']['shipment_id']
    success, msg = cancel_shipment(shipment_id)
    
    if success:
        await query.message.reply_text(
            f"✅ Old shipment cancelled!\n\n💰 Enter new COD amount:\n(Suggested: ₹2700 for ₹2400 product)",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data["awaiting_convert_cod_amount"] = True
    else:
        await query.message.reply_text(f"❌ Cancel failed: {msg}", reply_markup=MAIN_KEYBOARD)
        context.user_data.pop("convert_order", None)
    return

if data == "convert_cancel":
    await query.message.reply_text("✅ Conversion cancelled", reply_markup=MAIN_KEYBOARD)
    context.user_data.pop("convert_order", None)
    return

# NEW: Handle manual entry callbacks
if data.startswith("manual_cancel_"):
    if data == "manual_cancel_yes":
        phone = context.user_data.get("manual_phone")
        order = find_order_by_phone(phone)
        
        if order and order.get('shiprocket'):
            shipment_id = order['shiprocket']['shipment_id']
            cancel_shipment(shipment_id)
        
        await query.message.reply_text(
            "✅ Shiprocket cancelled!\n\n📝 Enter vendor courier name:",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data["awaiting_manual_courier"] = True
    else:
        await query.message.reply_text(
            "📝 Enter vendor courier name:",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data["awaiting_manual_courier"] = True
    return

# NEW: Quick action callbacks from search results
if data.startswith("adv_"):
    phone = data.split("_")[1]
    context.user_data["advance_phone"] = phone
    
    keyboard = [
        [
            InlineKeyboardButton("₹500", callback_data="advance_500"),
            InlineKeyboardButton("₹600", callback_data="advance_600"),
            InlineKeyboardButton("₹700", callback_data="advance_700")
        ],
        [InlineKeyboardButton("Custom", callback_data="advance_custom")]
    ]
    
    await query.message.reply_text(
        f"💰 Advance amount for {phone}?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return

if data.startswith("cod_"):
    phone = data.split("_")[1]
    order = find_order_by_phone(phone)
    
    if order:
        context.user_data["convert_order"] = order
        
        keyboard = [[
            InlineKeyboardButton("✅ Yes", callback_data="convert_confirm"),
            InlineKeyboardButton("❌ No", callback_data="convert_cancel")
        ]]
        
        await query.message.reply_text(
            f"⚠️ Convert to Full COD?\n\nOrder #{order.get('order_number')}\n\nThis will cancel and rebook.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    return

if data.startswith("manual_"):
    phone = data.split("_")[1]
    context.user_data["manual_phone"] = phone
    
    order = find_order_by_phone(phone)
    
    if order and order.get('shiprocket', {}).get('status') == 'active':
        keyboard = [[
            InlineKeyboardButton("✅ Yes, Cancel", callback_data="manual_cancel_yes"),
            InlineKeyboardButton("❌ Keep Both", callback_data="manual_cancel_no")
        ]]
        await query.message.reply_text(
            "⚠️ Cancel Shiprocket shipment?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await query.message.reply_text(
            "📝 Enter vendor courier name:",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data["awaiting_manual_courier"] = True
    return

# EXISTING: Product edit/delete
if data.startswith("delete_"):
    name = data.split("delete_", 1)[1]
    products = {}
    if os.path.exists(PRODUCTS_FILE):
        products = json.load(open(PRODUCTS_FILE))
    if name in products:
        products.pop(name)
        json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)
        await query.edit_message_text(f"❌ Product '{name}' deleted.")
    else:
        await query.edit_message_text("❌ Product not found.")
    return

if data.startswith("edit_"):
    name = data.split("edit_", 1)[1]
    products = {}
    if os.path.exists(PRODUCTS_FILE):
        products = json.load(open(PRODUCTS_FILE))
    if name in products:
        context.user_data["editing_product"] = name
        context.user_data["awaiting_product"] = True
        await query.message.reply_text(
            f"✏ Send new details for '{name}' in format:\nName length breadth height weight",
            reply_markup=MAIN_KEYBOARD
        )
    else:
        await query.edit_message_text("❌ Product not found.")
    return

# EXISTING: Pickup schedule
if data.startswith("schedule_yes_"):
    parts = data.replace("schedule_yes_", "").split("_")
    shipment_id = parts[0]
    phone = parts[1] if len(parts) > 1 else None
    
    ids = [shipment_id]
    ok, msg = schedule_pickup(ids)
    
    # NEW: Update order with pickup status
    if ok and phone:
        from shared.orders_manager import load_orders, save_orders
        orders = load_orders()
        for order in orders:
            if order.get('phone') == phone and order.get('shiprocket', {}).get('shipment_id') == shipment_id:
                order['shiprocket']['pickup_scheduled'] = True
                order['shiprocket']['pickup_scheduled_at'] = datetime.now().isoformat()
                break
        save_orders(orders)
    
    await query.edit_message_text(("✅ " if ok else "❌ ") + msg)
    return

if data.startswith("schedule_no_"):
    shipment_id = data.replace("schedule_no_", "")
    await query.edit_message_text(f"❌ Shipment {shipment_id} not scheduled")
    return

await query.edit_message_text("⚠️ Unknown action")
```

# –––––––– MAIN ––––––––

async def main():
log.info(“🚀 Bot starting…”)

```
try:
    get_token()
    log.info("✅ Shiprocket token fetched")
except Exception as e:
    log.error(f"❌ Shiprocket login failed: {e}")
    raise

ok, msg = refresh_pickups()
log.info(msg)

log.info(f"🤖 Building Telegram bot with token: {BOT_TOKEN[:15]}...")
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
app.add_handler(CallbackQueryHandler(handle_callback))

log.info("✅ Bot handlers registered")
log.info("✅ Phase 1 features enabled:")
log.info("  • Search Order")
log.info("  • Mark Advance")
log.info("  • Convert Full COD")
log.info("  • Manual Entry")
log.info("  • Stats")
log.info("🔄 Starting polling...")
await app.run_polling()
```

if **name** == “**main**”:
import nest_asyncio
nest_asyncio.apply()
asyncio.run(main())