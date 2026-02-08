# control_bot.py - AI Conversational Agent (GPT-4 Powered)
import os
import re
import json
import uuid
import time
import logging
import requests
from datetime import datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
import asyncio
import openai
import aiohttp

# Import shared database
import shared_database as db

# ---------------- CONFIG ----------------
CONTROL_BOT_TOKEN = os.getenv("CONTROL_BOT_TOKEN") or os.getenv("BOT_TOKEN")
SHIPROCKET_EMAIL = os.getenv("SHIPROCKET_EMAIL")
SHIPROCKET_PASSWORD = os.getenv("SHIPROCKET_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not CONTROL_BOT_TOKEN:
    raise ValueError("❌ CONTROL_BOT_TOKEN not set!")
if not OPENAI_API_KEY:
    raise ValueError("❌ OPENAI_API_KEY not set!")

openai.api_key = OPENAI_API_KEY

# ---------------- SHIPROCKET CONFIG ----------------
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
}

DEFAULT_PRODUCT = {"length": 10, "breadth": 8, "height": 5, "weight": 0.5}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("control_bot")

session = requests.Session()
pickup_map = {}
auth_token = None
token_expiry = 0

# ---------------- SHIPROCKET FUNCTIONS ----------------
def get_token(force_refresh=False):
    global auth_token, token_expiry
    if not force_refresh and auth_token and time.time() < token_expiry:
        return auth_token
    
    try:
        r = session.post(
            SHIPROCKET_BASE + URLS["login"],
            json={"email": SHIPROCKET_EMAIL, "password": SHIPROCKET_PASSWORD},
            timeout=60
        )
        data = r.json()
        if "token" not in data:
            raise Exception("Login failed")
        
        auth_token = data["token"]
        token_expiry = time.time() + (23 * 3600)
        session.headers.update({"Authorization": f"Bearer {auth_token}"})
        return auth_token
    except Exception as e:
        log.error(f"Shiprocket login error: {e}")
        raise

def refresh_pickups():
    global pickup_map
    try:
        get_token()
        r = session.get(SHIPROCKET_BASE + URLS["pickup"], timeout=60)
        data = r.json()
        lst = data.get("data", {}).get("shipping_address", [])
        pickup_map = {p["pickup_location"].lower(): p for p in lst if p.get("pickup_location")}
        return True
    except Exception as e:
        log.error(f"Pickup refresh error: {e}")
        return False

def get_available_couriers(pickup_pin, delivery_pin, weight, cod):
    try:
        r = session.get(SHIPROCKET_BASE + URLS["courier_get"], params={
            "pickup_postcode": str(pickup_pin),
            "delivery_postcode": str(delivery_pin),
            "cod": int(bool(cod)),
            "weight": weight
        }, timeout=60)
        if r.status_code != 200:
            return []
        return r.json().get("data", {}).get("available_courier_companies", []) or []
    except:
        return []

def assign_awb(shipment_id, courier_id):
    try:
        r = session.post(SHIPROCKET_BASE + URLS["assign_awb"], 
                        json={"shipment_id": shipment_id, "courier_id": courier_id}, 
                        timeout=40)
        resp = r.json()
        if resp.get("awb_assign_status") == 1:
            return resp["response"]["data"]["awb_code"]
        return None
    except:
        return None

def generate_label(shipment_id):
    try:
        r = session.post(SHIPROCKET_BASE + URLS["label"], 
                        json={"shipment_id": [shipment_id]}, 
                        timeout=40)
        resp = r.json()
        if resp.get("label_created") == 1:
            return resp.get("label_url")
        return None
    except:
        return None

def create_order_shiprocket(payload):
    try:
        get_token()
        r = session.post(SHIPROCKET_BASE + URLS["create_order"], json=payload, timeout=40)
        if r.status_code == 200:
            return r.json(), None
        return None, r.text
    except Exception as e:
        return None, str(e)

# ---------------- AI FUNCTIONS ----------------
def call_gpt4(user_message, system_prompt):
    """Call GPT-4 to understand intent and extract data"""
    try:
        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            temperature=0.3
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        log.error(f"GPT-4 error: {e}")
        return None

def understand_intent(user_message):
    """Use GPT-4 to understand what user wants to do"""
    system_prompt = """You are an AI assistant for a shipment management system. 
Analyze the user's message and determine their intent. Return ONLY a JSON object with:
{
  "intent": "create_order|search_order|mark_advance|manual_shipment|convert_cod|show_stats|show_performance|general_chat",
  "confidence": 0.0-1.0,
  "extracted_data": {
    // relevant fields based on intent
  }
}

Intents:
- create_order: User wants to create shipment (look for: phone, address, product, city)
- search_order: User wants to find order (look for: phone, AWB, order number)
- mark_advance: User wants to record payment (look for: phone, amount)
- manual_shipment: User wants to add vendor tracking (look for: phone, courier, AWB)
- convert_cod: User wants to change payment method
- show_stats: User asks "today orders", "how many", "show pending", etc.
- show_performance: User asks about creative performance, ROAS, which ads
- general_chat: Casual conversation, greetings, questions

Examples:
"ship projector mysore 9110227567" → create_order
"find 9110227567" → search_order  
"mark 9110 advance 600" → mark_advance
"show today orders" → show_stats
"how is fb-may-001 doing" → show_performance
"""
    
    response = call_gpt4(user_message, system_prompt)
    if not response:
        return None
    
    try:
        # Extract JSON from response (GPT sometimes adds markdown)
        if "```json" in response:
            response = response.split("```json")[1].split("```")[0].strip()
        elif "```" in response:
            response = response.split("```")[1].split("```")[0].strip()
        
        return json.loads(response)
    except:
        return None

def extract_order_data(user_message):
    """Use GPT-4 to extract structured order data"""
    system_prompt = """Extract shipment details from user message and return ONLY JSON:
{
  "phone": "10_digit_phone",
  "name": "customer_name",
  "address": "full_address",
  "city": "city_name",
  "state": "state_name",
  "pincode": "6_digit_pincode",
  "product": "product_name",
  "creative_code": "FB-XXX-XXX or empty",
  "payment_method": "COD or Prepaid",
  "amount": 0
}

Extract what you can. Leave fields empty if not mentioned."""
    
    response = call_gpt4(user_message, system_prompt)
    if not response:
        return None
    
    try:
        if "```json" in response:
            response = response.split("```json")[1].split("```")[0].strip()
        elif "```" in response:
            response = response.split("```")[1].split("```")[0].strip()
        return json.loads(response)
    except:
        return None

# ---------------- TELEGRAM HANDLERS ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 **AI Control Bot Active!**\n\n"
        "I understand natural language. Just chat normally:\n\n"
        "💬 Examples:\n"
        "• \"ship projector mysore 9110227567\"\n"
        "• \"find order 9110227567\"\n"
        "• \"mark 9110 advance 600\"\n"
        "• \"show today orders\"\n"
        "• \"how is fb-may-001 performing\"\n"
        "• \"pending advances\"\n\n"
        "Talk to me like you're talking to a person! 💪",
        parse_mode="Markdown"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text.strip()
    
    # Show typing indicator
    await update.message.chat.send_action("typing")
    
    # Understand intent using GPT-4
    intent_data = understand_intent(user_message)
    
    if not intent_data:
        await update.message.reply_text("🤔 I didn't quite understand that. Can you rephrase?")
        return
    
    intent = intent_data.get("intent")
    confidence = intent_data.get("confidence", 0)
    
    log.info(f"Intent: {intent}, Confidence: {confidence}")
    
    # Route to appropriate handler
    if intent == "create_order":
        await handle_create_order(update, user_message)
    
    elif intent == "search_order":
        await handle_search_order(update, user_message, intent_data)
    
    elif intent == "mark_advance":
        await handle_mark_advance(update, user_message, intent_data)
    
    elif intent == "manual_shipment":
        await handle_manual_shipment(update, user_message, intent_data)
    
    elif intent == "show_stats":
        await handle_show_stats(update, user_message)
    
    elif intent == "show_performance":
        await handle_show_performance(update, user_message, intent_data)
    
    elif intent == "general_chat":
        await handle_general_chat(update, user_message)
    
    else:
        await update.message.reply_text("🤔 I'm not sure how to help with that yet.")

# ---------------- INTENT HANDLERS ----------------
async def handle_create_order(update: Update, user_message: str):
    """Handle order creation"""
    await update.message.reply_text("⏳ Creating shipment... (AI extracting details)")
    
    # Extract structured data
    order_data = extract_order_data(user_message)
    
    if not order_data or not order_data.get("phone"):
        await update.message.reply_text(
            "❌ I need at least:\n"
            "• Phone number\n"
            "• City or address\n"
            "• Product name\n\n"
            "Example: \"ship projector mysore 9110227567\""
        )
        return
    
    try:
        # Get product details
        product = db.get_product(order_data.get("product", ""))
        if not product:
            product = DEFAULT_PRODUCT
        
        # Create Shiprocket order
        payload = {
            "order_id": f"ORDER{int(time.time())}_{uuid.uuid4().hex[:6]}",
            "order_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "pickup_location": list(pickup_map.values())[0]["pickup_location"] if pickup_map else "Default",
            "billing_customer_name": order_data.get("name", "Customer"),
            "billing_last_name": ".",
            "billing_address": order_data.get("address", ""),
            "billing_city": order_data.get("city", ""),
            "billing_state": order_data.get("state", "Karnataka"),
            "billing_country": "India",
            "billing_pincode": order_data.get("pincode", "560001"),
            "billing_email": "na@example.com",
            "billing_phone": db.strict_phone(order_data.get("phone")),
            "shipping_is_billing": True,
            "order_items": [{
                "name": order_data.get("product", "Product"),
                "sku": order_data.get("product", "Product"),
                "units": 1,
                "selling_price": order_data.get("amount", 0),
            }],
            "payment_method": order_data.get("payment_method", "Prepaid"),
            "sub_total": order_data.get("amount", 0),
            "length": product["length"],
            "breadth": product["breadth"],
            "height": product["height"],
            "weight": product["weight"],
        }
        
        resp, err = create_order_shiprocket(payload)
        if not resp:
            await update.message.reply_text(f"❌ Shiprocket error: {err}")
            return
        
        shipment_id = resp.get("shipment_id")
        
        # Assign courier
        couriers = get_available_couriers(
            payload.get("billing_pincode", "560001"),
            payload.get("billing_pincode", "560001"),
            product["weight"],
            payload["payment_method"] == "COD"
        )
        
        if not couriers:
            await update.message.reply_text("❌ No couriers available")
            return
        
        best_courier = min(couriers, key=lambda x: x.get("rate", 1e12))
        courier_id = best_courier.get("courier_company_id")
        
        awb = assign_awb(shipment_id, courier_id)
        
        if not awb:
            await update.message.reply_text("❌ AWB assignment failed")
            return
        
        # Save to database
        db_order = {
            "phone": db.strict_phone(order_data.get("phone")),
            "name": order_data.get("name", "Customer"),
            "product": order_data.get("product", ""),
            "creative_code": order_data.get("creative_code", ""),
            "city": order_data.get("city", ""),
            "state": order_data.get("state", ""),
            "pincode": order_data.get("pincode", ""),
            "payment_method": payload["payment_method"],
            "amount": order_data.get("amount", 0),
            "awb": awb,
            "courier": best_courier.get("courier_name"),
            "rate": best_courier.get("rate"),
            "shipment_id": shipment_id,
            "shiprocket_order_id": resp.get("order_id"),
            "advance_paid": 0,
            "vendor_shipped": False
        }
        
        order_number = db.add_order(db_order)
        
        msg = f"✅ **Shipment Created!**\n\n"
        msg += f"📦 Order #{order_number}\n"
        msg += f"📱 {order_data.get('phone')}\n"
        msg += f"📍 {order_data.get('city')}\n"
        if order_data.get("creative_code"):
            msg += f"🎯 Creative: {order_data.get('creative_code')}\n"
        msg += f"\n🚚 {best_courier.get('courier_name')}\n"
        msg += f"📋 AWB: {awb}\n"
        msg += f"💰 Rate: ₹{best_courier.get('rate')}\n"
        msg += f"\n🔗 Track: https://shiprocket.co/tracking/{awb}"
        
        await update.message.reply_text(msg, parse_mode="Markdown")
        
        # Send label
        label_url = generate_label(shipment_id)
        if label_url:
            async with aiohttp.ClientSession() as http_session:
                async with http_session.get(label_url) as resp_pdf:
                    if resp_pdf.status == 200:
                        pdf_data = await resp_pdf.read()
                        await update.message.reply_document(
                            document=pdf_data,
                            filename=f"{awb}.pdf"
                        )
    
    except Exception as e:
        log.error(f"Order creation error: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def handle_search_order(update: Update, user_message: str, intent_data: dict):
    """Handle order search"""
    extracted = intent_data.get("extracted_data", {})
    
    # Try to extract search term
    phone = extracted.get("phone")
    awb = extracted.get("awb")
    order_num = extracted.get("order_number")
    creative = extracted.get("creative_code")
    
    # Smart search: try different methods
    search_term = re.sub(r"[^\d]", "", user_message)[:10]  # Extract digits
    
    results = []
    
    if search_term and len(search_term) == 10:
        # Phone search
        results = db.search_order_by_phone(search_term)
    elif "-" in user_message:
        # Likely creative code
        creative_code = user_message.split()[0].upper()
        results = db.search_order_by_creative(creative_code)
    else:
        # Try AWB
        order = db.search_order_by_awb(user_message.split()[0])
        if order:
            results = [order]
    
    if not results:
        await update.message.reply_text("❌ No orders found")
        return
    
    # Show results
    for order in results[:3]:  # Show first 3
        msg = f"📦 **Order #{order.get('order_number')}**\n\n"
        msg += f"👤 {order.get('name', 'Customer')}\n"
        msg += f"📱 {order.get('phone')}\n"
        msg += f"📦 {order.get('product')}\n"
        
        if order.get("creative_code"):
            msg += f"🎯 {order.get('creative_code')}\n"
        
        msg += f"\n💰 Payment: {order.get('payment_method')}\n"
        msg += f"Amount: ₹{order.get('amount', 0)}\n"
        
        if order.get("advance_paid"):
            msg += f"Advance: ✅ ₹{order.get('advance_paid')}\n"
        
        msg += f"\n🚚 Shipping:\n"
        
        if order.get("vendor_shipped"):
            msg += f"Vendor: {order.get('manual_courier')}\n"
            msg += f"AWB: {order.get('manual_awb')} ✅\n"
        else:
            msg += f"Courier: {order.get('courier')}\n"
            msg += f"AWB: {order.get('awb')}\n"
        
        msg += f"\n📅 {order.get('created_date')}"
        
        await update.message.reply_text(msg, parse_mode="Markdown")
    
    if len(results) > 3:
        await update.message.reply_text(f"... and {len(results) - 3} more orders")

async def handle_mark_advance(update: Update, user_message: str, intent_data: dict):
    """Handle advance payment marking"""
    # Parse message for phone and amount
    # Support: "mark 9110 600" or "9110 600"
    parts = user_message.split()
    phone_amount_pairs = []
    
    i = 0
    while i < len(parts) - 1:
        phone = db.strict_phone(parts[i])
        if phone:
            try:
                amount = float(parts[i + 1])
                phone_amount_pairs.append((phone, amount))
                i += 2
            except:
                i += 1
        else:
            i += 1
    
    if not phone_amount_pairs:
        await update.message.reply_text(
            "❌ Format: phone amount\n"
            "Example: \"mark 9110227567 600\"\n"
            "Or bulk: \"9110 600, 8197 500\""
        )
        return
    
    success_count, total_amount = db.mark_bulk_advances(phone_amount_pairs)
    
    if success_count > 0:
        await update.message.reply_text(
            f"✅ **{success_count} advance(s) recorded!**\n"
            f"💰 Total: ₹{total_amount}",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("❌ No orders found to update")

async def handle_manual_shipment(update: Update, user_message: str, intent_data: dict):
    """Handle manual shipment tracking"""
    # Parse: "phone courier awb"
    parts = user_message.split()
    shipments = []
    
    i = 0
    while i < len(parts) - 2:
        phone = db.strict_phone(parts[i])
        if phone:
            courier = parts[i + 1]
            awb = parts[i + 2]
            shipments.append((phone, courier, awb))
            i += 3
        else:
            i += 1
    
    if not shipments:
        await update.message.reply_text(
            "❌ Format: phone courier awb\n"
            "Example: \"manual 9110227567 BlueDart 888999\""
        )
        return
    
    success_count = db.add_bulk_manual_shipments(shipments)
    
    if success_count > 0:
        await update.message.reply_text(
            f"✅ **{success_count} manual shipment(s) added!**\n"
            f"📦 Vendor tracking started",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("❌ No orders found to update")

async def handle_show_stats(update: Update, user_message: str):
    """Show daily statistics"""
    summary = db.get_daily_summary()
    
    msg = f"📊 **TODAY'S STATS**\n\n"
    msg += f"📦 Orders: {summary['today_orders_count']}\n"
    msg += f"💰 Revenue: ₹{summary['today_revenue']}\n"
    msg += f"💵 Advances: ₹{summary['today_advances']}\n\n"
    msg += f"COD: {summary['cod_orders']} | Prepaid: {summary['prepaid_orders']}\n\n"
    
    if summary['pending_advances_count'] > 0:
        msg += f"⏳ Pending Advances: {summary['pending_advances_count']}\n"
    
    if summary['not_picked_count'] > 0:
        msg += f"🚨 Not Picked: {summary['not_picked_count']}\n"
    
    await update.message.reply_text(msg, parse_mode="Markdown")
    
    # Show details if requested
    if "pending" in user_message.lower():
        for order in summary['pending_advances'][:5]:
            await update.message.reply_text(
                f"⏳ Order #{order.get('order_number')}\n"
                f"📱 {order.get('phone')}\n"
                f"📦 {order.get('product')}\n"
                f"💰 Advance due: ₹{order.get('amount', 0) - order.get('advance_paid', 0)}"
            )

async def handle_show_performance(update: Update, user_message: str, intent_data: dict):
    """Show creative performance"""
    # Extract creative code
    creative_code = None
    for word in user_message.upper().split():
        if "-" in word:
            creative_code = word
            break
    
    if creative_code:
        # Show specific creative
        perf = db.get_creative_performance(creative_code)
        
        msg = f"🎯 **{creative_code} Performance**\n\n"
        msg += f"📦 Total Orders: {perf['total_orders']}\n"
        msg += f"💰 Revenue: ₹{perf['total_revenue']}\n"
        msg += f"💵 Advances: ₹{perf['total_advances']}\n\n"
        msg += f"COD: {perf['cod_orders']} | Prepaid: {perf['prepaid_orders']}"
        
        await update.message.reply_text(msg, parse_mode="Markdown")
    else:
        # Show all creatives
        creatives = db.get_all_creative_codes()
        
        if not creatives:
            await update.message.reply_text("❌ No creative codes tracked yet")
            return
        
        msg = "🎯 **All Creatives:**\n\n"
        for creative in creatives:
            perf = db.get_creative_performance(creative)
            msg += f"• {creative}: {perf['total_orders']} orders\n"
        
        await update.message.reply_text(msg, parse_mode="Markdown")

async def handle_general_chat(update: Update, user_message: str):
    """Handle casual conversation"""
    # Use GPT-4 for natural response
    system_prompt = """You are a helpful AI assistant for a shipment management system.
Respond naturally and friendly. Keep responses brief (2-3 sentences).
If user asks what you can do, mention: creating shipments, searching orders, marking advances, checking stats."""
    
    response = call_gpt4(user_message, system_prompt)
    
    if response:
        await update.message.reply_text(response)
    else:
        await update.message.reply_text("👋 Hey! How can I help you today?")

# ---------------- MAIN ----------------
async def main():
    log.info("🚀 AI Control Bot starting...")
    
    # Login to Shiprocket
    try:
        get_token()
        refresh_pickups()
        log.info("✅ Shiprocket ready")
    except Exception as e:
        log.error(f"❌ Shiprocket init failed: {e}")
    
    # Build bot
    app = ApplicationBuilder().token(CONTROL_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    log.info("✅ Control bot ready - accessible to everyone")
    await app.run_polling()

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())
