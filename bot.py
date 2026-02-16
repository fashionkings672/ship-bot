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

# — STEP 1: GLOBAL USER STATE —

user_state = {}

# –––– CONFIG ––––

BOT_TOKEN = os.getenv(“BOT_TOKEN”)
SHIPROCKET_EMAIL = os.getenv(“SHIPROCKET_EMAIL”)
SHIPROCKET_PASSWORD = os.getenv(“SHIPROCKET_PASSWORD”)
OPENAI_API_KEY = os.getenv(“OPENAI_API_KEY”)

# Debug logging

print(”=” * 60)
print(“ENVIRONMENT VARIABLES CHECK”)
print(”=” * 60)
print(f”BOT_TOKEN exists: {BOT_TOKEN is not None}”)
print(f”BOT_TOKEN length: {len(BOT_TOKEN) if BOT_TOKEN else 0}”)
if BOT_TOKEN:
print(f”BOT_TOKEN preview: {BOT_TOKEN[:15]}…”)
else:
print(“BOT_TOKEN preview: NONE”)
print(f”SHIPROCKET_EMAIL exists: {SHIPROCKET_EMAIL is not None}”)
print(f”SHIPROCKET_PASSWORD exists: {SHIPROCKET_PASSWORD is not None}”)
print(f”OPENAI_API_KEY exists: {OPENAI_API_KEY is not None}”)
print(”=” * 60)

# Safety checks

if not BOT_TOKEN:
raise ValueError(
“BOT_TOKEN is not set!\n”
“Go to Railway > Your Service > Variables tab\n”
“Add: BOT_TOKEN=your_token_from_botfather”
)

if not SHIPROCKET_EMAIL or not SHIPROCKET_PASSWORD:
raise ValueError(“SHIPROCKET credentials not set!”)

if not OPENAI_API_KEY:
raise ValueError(“OPENAI_API_KEY not set!”)

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
}

COURIER_PRIORITY = [“bluedart”, “delhivery”, “dtdc”]
PRODUCTS_FILE = “products.json”
DEFAULT_PRODUCT = {“length”: 10, “breadth”: 8, “height”: 5, “weight”: 0.5}

logging.basicConfig(
level=logging.INFO,
format=’%(asctime)s - %(name)s - %(levelname)s - %(message)s’
)
log = logging.getLogger(“telegram_shipbot”)

session = requests.Session()
pickup_map = {}
shipment_awb_map = {}

# –––– HELPERS ––––

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
k = re.sub(r”\W”, “”, parsed[“pickup”].lower())
for key, obj in pickup_map.items():
norm_key = re.sub(r”\W”, “”, key.lower())
if k == norm_key or k in norm_key or norm_key in k:
return obj
return next(iter(pickup_map.values()), None)

# –––– SHIPROCKET LOGIN / TOKEN ––––

auth_token = None
token_expiry = 0

def get_token(force_refresh=False):
“”“Get or refresh Shiprocket token safely.”””
global auth_token, token_expiry

```
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

    session.headers.update({
        "Authorization": f"Bearer {auth_token}"
    })

    log.info("Shiprocket token obtained successfully")
    return auth_token

except Exception as e:
    log.error(f"Shiprocket login failed: {e}")
    raise Exception(f"Shiprocket login failed: {e}")
```

def ensure_valid_token():
“”“Ensures token is valid, refreshes if expired.”””
try:
get_token()
except Exception:
get_token(force_refresh=True)

# –––– SHIPROCKET REQUEST WRAPPER (AUTO TOKEN REFRESH ON 401) ––––

def shiprocket_request(method, url, retry=True, **kwargs):
“””
Make Shiprocket API request with automatic token refresh on 401.
Replaces all direct session.get/session.post calls to Shiprocket.
On 401 response, refreshes the token and retries once.
“””
ensure_valid_token()
kwargs.setdefault(“timeout”, 60)

```
try:
    r = session.request(method, SHIPROCKET_BASE + url, **kwargs)
except requests.exceptions.ConnectTimeout:
    raise Exception(f"Shiprocket API timed out for {url}")
except requests.exceptions.RequestException as e:
    raise Exception(f"Shiprocket request error for {url}: {e}")

# Auto-refresh token on 401 and retry once
if r.status_code == 401 and retry:
    log.warning("Token expired (401), refreshing and retrying...")
    try:
        get_token(force_refresh=True)
    except Exception as e:
        raise Exception(f"Token refresh failed: {e}")

    try:
        r = session.request(method, SHIPROCKET_BASE + url, **kwargs)
    except requests.exceptions.RequestException as e:
        raise Exception(f"Shiprocket retry request error for {url}: {e}")

    if r.status_code == 401:
        raise Exception("Still getting 401 after token refresh. Check Shiprocket credentials.")

return r
```

# –––– PICKUP REFRESH ––––

def refresh_pickups():
global pickup_map
try:
log.info(“Fetching pickup locations…”)
r = shiprocket_request(“GET”, URLS[“pickup”])

```
    if r.status_code != 200:
        return False, f"Pickup fetch failed: {r.status_code} {r.text}"

    try:
        data = r.json()
    except Exception:
        return False, f"Invalid JSON response: {r.text}"

    lst = data.get("data", {}).get("shipping_address", [])

    pickup_map = {
        p["pickup_location"].lower(): p
        for p in lst
        if p.get("pickup_location")
    }

    log.info(f"Loaded {len(pickup_map)} pickup locations")
    return True, f"Loaded {len(pickup_map)} pickups"

except requests.exceptions.ConnectTimeout:
    return False, "Shiprocket pickup API timed out. Try again later."

except Exception as e:
    log.error(f"Pickup refresh error: {e}")
    return False, f"Pickup refresh error: {e}"
```

# –––– OPENAI ADDRESS FORMATTING ––––

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
“””
try:
response = openai.chat.completions.create(
model=“gpt-4”,
messages=[{“role”: “user”, “content”: prompt}],
temperature=0.3
)
formatted_text = response.choices[0].message.content.strip()
return formatted_text
except Exception as e:
log.error(f”OpenAI API error: {e}”)
raise

# –––– SHIPROCKET API FUNCTIONS ––––

def get_available_couriers(pickup_pin, delivery_pin, weight, cod):
try:
r = shiprocket_request(“GET”, URLS[“courier_get”], params={
“pickup_postcode”: str(pickup_pin),
“delivery_postcode”: str(delivery_pin),
“cod”: int(bool(cod)),
“weight”: weight
}, timeout=60)
if r.status_code != 200:
return []
return r.json().get(“data”, {}).get(“available_courier_companies”, []) or []
except Exception as e:
log.error(f”Error getting couriers: {e}”)
return []

def pick_courier(couriers):
if not couriers:
return None
for pr in COURIER_PRIORITY:
options = [c for c in couriers if pr in (str(c.get(“courier_name”) or “”).lower())]
if options:
return min(options, key=lambda x: x.get(“rate”, 1e12))
return min(couriers, key=lambda x: x.get(“rate”, 1e12))

def get_shipping_quote(pickup_pin, delivery_pin, weight, cod):
try:
r = shiprocket_request(“GET”, URLS[“get_quote”], params={
“pickup_postcode”: pickup_pin,
“delivery_postcode”: delivery_pin,
“weight”: weight,
“cod”: int(bool(cod))
}, timeout=60)
if r.status_code != 200:
return None
return r.json().get(“data”, {}).get(“rate”)
except Exception:
return None

def assign_awb(shipment_id, courier_id=None):
try:
payload = {“shipment_id”: shipment_id}
if courier_id:
payload[“courier_id”] = courier_id
r = shiprocket_request(“POST”, URLS[“assign_awb”], json=payload, timeout=40)
resp_json = r.json()
if resp_json.get(“awb_assign_status”) == 1:
return resp_json[“response”][“data”][“awb_code”]
return None
except Exception as e:
log.error(f”AWB assignment error: {e}”)
return None

def generate_label(shipment_id):
try:
r = shiprocket_request(“POST”, URLS[“label”], json={“shipment_id”: [shipment_id]}, timeout=40)
resp_json = r.json() if r else {}
if not resp_json or resp_json.get(“label_created”) != 1:
return None
return resp_json.get(“label_url”)
except Exception as e:
log.error(f”Label generation error: {e}”)
return None

def create_order(payload):
try:
r = shiprocket_request(“POST”, URLS[“create_order”], json=payload, timeout=40)
resp_json = r.json() if r else None
if r.status_code != 200 or (resp_json and resp_json.get(“status_code”) not in (1, 200)):
return None, r.text
return resp_json, None
except Exception as e:
return None, str(e)

def schedule_pickup(shipment_ids, pickup_date=None, time_slot_id=None):
try:
payload = {“shipment_id”: shipment_ids}
if pickup_date:
payload[“pickup_date”] = pickup_date
if time_slot_id:
payload[“time_slot_id”] = time_slot_id

```
    r = shiprocket_request("POST", URLS["generate_pickup"], json=payload, timeout=40)

    try:
        resp_json = r.json()
    except Exception:
        return False, f"Invalid response: {r.text}"

    response_data = resp_json.get("response", {})
    status = resp_json.get("status") or response_data.get("status")
    pickup_id = (
        resp_json.get("pickup_id")
        or response_data.get("pickup_id")
        or resp_json.get("pickup_token_number")
        or response_data.get("pickup_token_number")
    )
    pickup_date_str = response_data.get("pickup_scheduled_date")
    awb_info = response_data.get("data")

    if r.status_code == 200:
        if resp_json.get("pickup_scheduled") or status == 1:
            return True, f"Pickup scheduled successfully! Pickup ID: {pickup_id or 'N/A'}"

        if status == 3:
            return True, f"Pickup already scheduled for {pickup_date_str}.\n{awb_info}"

        if "already generated" in str(resp_json).lower():
            return False, f"Pickup already generated.\n{awb_info}"

        return False, f"Pickup not scheduled: {resp_json}"
    else:
        return False, f"API Error {r.status_code}: {resp_json}"

except Exception as e:
    return False, f"Error scheduling pickup: {e}"
```

# –––– create_shipment_with_fallback ––––

def create_shipment_with_fallback(shipment_id, pickup_pin, delivery_pin, weight, cod):
couriers = get_available_couriers(pickup_pin, delivery_pin, weight, cod)
if not couriers:
return None, None, None

```
def mode_pref(c):
    m = str(c.get("mode") or c.get("service_type") or "").lower()
    if "surface" in m:
        return 0
    if "air" in m:
        return 1
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
    if "bluedart" in name_lower:
        base = 1
    elif "delhivery" in name_lower:
        base = 2
    elif "dtdc" in name_lower:
        base = 3
    else:
        base = 99
    return (base, mode_pref(c), c.get("rate", 1e12))

couriers_sorted = sorted(couriers, key=lambda c: priority_key(c))

for courier in couriers_sorted:
    courier_id = (
        courier.get("courier_company_id")
        or courier.get("courier_id")
        or courier.get("courierId")
        or courier.get("id")
    )
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
```

# –––– TELEGRAM BOT ––––

MAIN_KEYBOARD = ReplyKeyboardMarkup(
[
[“➕ Add Product”, “📋 View Products”],
[“📦 Create Shipment”, “🔙 Cancel”]
],
resize_keyboard=True
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
context.user_data.clear()
await update.message.reply_text(“Welcome! Use the buttons below:”, reply_markup=MAIN_KEYBOARD)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
text = update.message.text.strip()

```
# --- 0) If user is currently editing a product, handle that first ---
if context.user_data.get("editing_product"):
    try:
        parts = text.split()
        if len(parts) < 5:
            raise ValueError("bad format")
        name = " ".join(parts[:-4])
        l = float(parts[-4])
        b = float(parts[-3])
        h = float(parts[-2])
        w = float(parts[-1])
        products = {}
        if os.path.exists(PRODUCTS_FILE):
            products = json.load(open(PRODUCTS_FILE))
        old_name = context.user_data.pop("editing_product", None)
        if old_name:
            products.pop(old_name, None)
        products[name] = {"length": l, "breadth": b, "height": h, "weight": w}
        json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)
        await update.message.reply_text(f"Product updated: {name}", reply_markup=MAIN_KEYBOARD)
    except Exception:
        await update.message.reply_text(
            "Wrong format. Use:\nName length breadth height weight",
            reply_markup=MAIN_KEYBOARD
        )
    return

# --- 1) Keyboard actions ---
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
        await update.message.reply_text("No products saved yet.", reply_markup=MAIN_KEYBOARD)
        return
    for name, prod in products.items():
        text_prod = f"{name}: {prod['length']}x{prod['breadth']}x{prod['height']} | {prod['weight']}kg"
        kb = [[
            InlineKeyboardButton("Edit", callback_data=f"edit_{name}"),
            InlineKeyboardButton("Delete", callback_data=f"delete_{name}")
        ]]
        await update.message.reply_text(text_prod, reply_markup=InlineKeyboardMarkup(kb))
    return

if text == "📦 Create Shipment":
    context.user_data["awaiting_shipment"] = True
    context.user_data["awaiting_product"] = False
    await update.message.reply_text(
        "Send messy address/order to create shipment.",
        reply_markup=MAIN_KEYBOARD
    )
    return

if text == "🔙 Cancel":
    context.user_data.pop("awaiting_product", None)
    context.user_data.pop("awaiting_shipment", None)
    context.user_data.pop("editing_product", None)
    await update.message.reply_text("Cancelled. Back to main menu.", reply_markup=MAIN_KEYBOARD)
    return

# --- 2) Add product ---
if context.user_data.get("awaiting_product"):
    parts = text.strip().split()
    if len(parts) < 5:
        await update.message.reply_text(
            "Invalid format. Send: ProductName length breadth height weight",
            reply_markup=MAIN_KEYBOARD
        )
        return
    try:
        length = float(parts[-4])
        breadth = float(parts[-3])
        height = float(parts[-2])
        weight = float(parts[-1])
    except ValueError:
        await update.message.reply_text(
            "Dimensions and weight must be numbers.",
            reply_markup=MAIN_KEYBOARD
        )
        return
    product_name = " ".join(parts[:-4])
    products = {}
    if os.path.exists(PRODUCTS_FILE):
        products = json.load(open(PRODUCTS_FILE))
    products[product_name] = {
        "length": length, "breadth": breadth, "height": height, "weight": weight
    }
    json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)
    context.user_data["awaiting_product"] = False
    await update.message.reply_text(
        f"Product '{product_name}' saved successfully",
        reply_markup=MAIN_KEYBOARD
    )
    return

# --- 3) Create shipment ---
if context.user_data.get("awaiting_shipment"):
    try:
        formatted = ai_format_address(text)
        data = {}
        for line in formatted.splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                data[k.strip().lower()] = v.strip()

        payment_method, sub_total = parse_payment(data.get("prepaid/cod", "Prepaid 0"))
        sr_payment_method = "COD" if payment_method.lower() == "cod" else "Prepaid"
        cod_amount = sub_total if sr_payment_method == "COD" else 0
        qty = int(data.get("quantity", "1"))
        product_data = json.load(open(PRODUCTS_FILE)).get(
            data.get("product", ""), DEFAULT_PRODUCT
        )
        pickup_obj = normalize_pickup_obj({"pickup": data.get("pickup")})
        if not pickup_obj:
            await update.message.reply_text(
                "Pickup not found in Shiprocket account",
                reply_markup=MAIN_KEYBOARD
            )
            return

        payload = {
            "order_id": f"ORDER{int(time.time())}_{uuid.uuid4().hex[:6]}",
            "order_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "pickup_location": pickup_obj.get("pickup_location"),
            "billing_customer_name": data.get("name", "Customer"),
            "billing_last_name": ".",
            "billing_address": data.get("address", ""),
            "billing_address_2": "",
            "billing_city": data.get("city", data.get("district", "")),
            "billing_state": data.get("state", ""),
            "billing_country": "India",
            "billing_pincode": data.get("pincode", "110001"),
            "billing_email": "na@example.com",
            "billing_isd_code": "91",
            "billing_phone": data.get("phone", ""),
            "billing_alternate_phone": data.get("alternate phone", ""),
            "shipping_is_billing": True,
            "order_items": [{
                "name": data.get("product", ""),
                "sku": data.get("product", ""),
                "units": qty,
                "selling_price": sub_total,
                "discount": "0",
                "tax": "0",
                "hsn": ""
            }],
            "payment_method": sr_payment_method,
            "shipping_charges": get_shipping_quote(
                pickup_obj.get("pin_code", "110001"),
                data.get("pincode", "110001"),
                product_data.get("weight"),
                sr_payment_method == "COD"
            ) or 0,
            "giftwrap_charges": 0,
            "transaction_charges": 0,
            "total_discount": 0,
            "sub_total": sub_total,
            "cod_amount": cod_amount,
            "length": float(product_data.get("length")),
            "breadth": float(product_data.get("breadth")),
            "height": float(product_data.get("height")),
            "weight": float(product_data.get("weight")),
        }

        if CUSTOM_CHANNEL_ID:
            payload["channel_id"] = CUSTOM_CHANNEL_ID

        # --- Duplicate order check ---
        recent_orders = []
        try:
            r = shiprocket_request(
                "GET", "/orders",
                params={"search": data.get("phone", "")},
                timeout=60
            )
            if r.status_code == 200:
                recent_orders = r.json().get("data", [])
        except Exception as e:
            log.error(f"Duplicate check failed: {e}")

        for order in recent_orders:
            try:
                order_date = datetime.strptime(
                    order.get("created_at", ""), "%Y-%m-%d %H:%M:%S"
                )
                if (
                    order.get("billing_phone") == data.get("phone")
                    and (datetime.now() - order_date).days < 7
                ):
                    keyboard = [
                        [
                            InlineKeyboardButton(
                                "Yes",
                                callback_data=f"dup_yes_{json.dumps(payload)}"
                            ),
                            InlineKeyboardButton("No", callback_data="dup_no")
                        ]
                    ]
                    await update.message.reply_text(
                        f"Duplicate order detected for {data.get('name')} within 7 days.\n"
                        f"Do you still want to create?",
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    return
            except Exception:
                pass

        # --- Create order ---
        resp, err = create_order(payload)
        if not resp:
            if "insufficient balance" in str(err).lower():
                await update.message.reply_text(
                    "Insufficient wallet balance in Shiprocket. Please recharge.",
                    reply_markup=MAIN_KEYBOARD
                )
            else:
                await update.message.reply_text(
                    f"Error creating shipment: {err}",
                    reply_markup=MAIN_KEYBOARD
                )
            return

        shipment_id = resp.get("shipment_id")

        courier, awb, rate = create_shipment_with_fallback(
            shipment_id,
            pickup_obj.get("pin_code", "110001"),
            data.get("pincode", "110001"),
            product_data.get("weight"),
            sr_payment_method == "COD"
        )

        if not courier or not awb:
            await update.message.reply_text(
                "No couriers available for this shipment",
                reply_markup=MAIN_KEYBOARD
            )
            return

        shipment_awb_map[shipment_id] = awb

        label_url = generate_label(shipment_id)
        tracking_link = f"https://shiprocket.co/tracking/{awb}" if awb else "N/A"

        # --- Increment order counter ---
        count_file = "order_count.json"
        count_data = {"count": 0}
        if os.path.exists(count_file):
            try:
                count_data = json.load(open(count_file))
            except Exception:
                pass
        count_data["count"] = count_data.get("count", 0) + 1
        json.dump(count_data, open(count_file, "w"), indent=2)
        order_number = count_data["count"]

        await update.message.reply_text(
            f"Shipment Created!\n"
            f"Order No: {order_number}\n"
            f"Courier: {courier.get('courier_name')}\n"
            f"Rate: {rate}\n"
            f"AWB: {awb}\n"
            f"Tracking: {tracking_link}",
            reply_markup=MAIN_KEYBOARD
        )

        if label_url:
            async with aiohttp.ClientSession() as session_http:
                async with session_http.get(label_url) as resp_pdf:
                    if resp_pdf.status == 200:
                        data_pdf = await resp_pdf.read()
                        await update.message.reply_document(
                            document=data_pdf,
                            filename=f"{awb}.pdf"
                        )

            keyboard = [
                [
                    InlineKeyboardButton(
                        "Yes",
                        callback_data=f"schedule_yes_{shipment_id}"
                    ),
                    InlineKeyboardButton(
                        "No",
                        callback_data=f"schedule_no_{shipment_id}"
                    )
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "Do you want to schedule pickup?",
                reply_markup=reply_markup
            )

    except Exception as e:
        log.error(f"Shipment creation error: {e}")
        await update.message.reply_text(f"Error: {e}", reply_markup=MAIN_KEYBOARD)
    finally:
        context.user_data["awaiting_shipment"] = False
    return

await update.message.reply_text(
    "Please use the keyboard buttons.",
    reply_markup=MAIN_KEYBOARD
)
```

# –––– CALLBACK HANDLER ––––

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
query = update.callback_query
await query.answer()
data = query.data or “”

```
context.user_data.pop("awaiting_product", None)
context.user_data.pop("editing_product", None)
context.user_data.pop("awaiting_shipment", None)

if data.startswith("delete_"):
    name = data.split("delete_", 1)[1]
    products = {}
    if os.path.exists(PRODUCTS_FILE):
        products = json.load(open(PRODUCTS_FILE))
    if name in products:
        products.pop(name)
        json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)
        await query.edit_message_text(f"Product '{name}' deleted.")
    else:
        await query.edit_message_text("Product not found.")
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
            f"Send new details for '{name}' in format:\nName length breadth height weight",
            reply_markup=MAIN_KEYBOARD
        )
    else:
        await query.edit_message_text("Product not found.")
    return

if data.startswith("dup_yes_"):
    try:
        payload = json.loads(data.split("dup_yes_", 1)[1])
        resp, err = create_order(payload)
        if not resp:
            await query.message.reply_text(
                f"Error creating shipment: {err}",
                reply_markup=MAIN_KEYBOARD
            )
            return

        shipment_id = resp.get("shipment_id")
        courier, awb, rate = create_shipment_with_fallback(
            shipment_id,
            payload.get("pickup_postcode", "110001"),
            payload.get("pincode", "110001"),
            payload.get("weight", "0.5"),
            payload.get("payment_method") == "COD"
        )
        if not courier or not awb:
            await query.message.reply_text(
                "No couriers available for this shipment",
                reply_markup=MAIN_KEYBOARD
            )
            return

        tracking_link = f"https://shiprocket.co/tracking/{awb}"

        count_file = "order_count.json"
        count_data = {"count": 0}
        if os.path.exists(count_file):
            try:
                count_data = json.load(open(count_file))
            except Exception:
                pass
        count_data["count"] = count_data.get("count", 0) + 1
        json.dump(count_data, open(count_file, "w"), indent=2)
        order_number = count_data["count"]

        await query.message.reply_text(
            f"Shipment Created!\n"
            f"Order No: {order_number}\n"
            f"Courier: {courier.get('courier_name')}\n"
            f"Rate: {rate}\n"
            f"AWB: {awb}\n"
            f"Tracking: {tracking_link}",
            reply_markup=MAIN_KEYBOARD
        )

    except Exception as e:
        await query.message.reply_text(
            f"Error confirming duplicate order: {e}",
            reply_markup=MAIN_KEYBOARD
        )
    return

if data == "dup_no":
    await query.message.reply_text(
        "Duplicate order cancelled.",
        reply_markup=MAIN_KEYBOARD
    )
    return

if data.startswith("schedule_yes_"):
    shipment_id = data.replace("schedule_yes_", "")
    ids = [shipment_id]
    ok, msg = schedule_pickup(ids)
    await query.edit_message_text(msg)
    return

if data.startswith("schedule_no_"):
    shipment_id = data.replace("schedule_no_", "")
    await query.edit_message_text(f"Shipment {shipment_id} not scheduled")
    return

await query.edit_message_text("Unknown action")
```

# –––– MAIN ––––

async def main():
log.info(“Bot starting…”)

```
try:
    get_token()
    log.info("Shiprocket token fetched")
except Exception as e:
    log.error(f"Shiprocket login failed: {e}")
    raise

ok, msg = refresh_pickups()
log.info(msg)

log.info(f"Building Telegram bot with token: {BOT_TOKEN[:15]}...")
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
app.add_handler(CallbackQueryHandler(handle_callback))

log.info("Bot handlers registered")
log.info("Starting polling...")
await app.run_polling()
```

if **name** == “**main**”:
import nest_asyncio
nest_asyncio.apply()
asyncio.run(main())