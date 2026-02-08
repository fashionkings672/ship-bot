# shared_database.py - Common database functions for both bots
import os
import json
import re
from datetime import datetime, timedelta

# ---------------- FILES ----------------
PRODUCTS_FILE = "products.json"
ORDERS_FILE = "orders.json"
ORDER_COUNT_FILE = "order_count.json"

# ---------------- HELPER FUNCTIONS ----------------
def strict_phone(ph):
    """Extract clean 10-digit phone number"""
    if not ph:
        return None
    ph = re.sub(r"\D", "", str(ph))
    return ph if len(ph) == 10 and ph[0] in "6789" else None

# ---------------- PRODUCTS ----------------
def load_products():
    """Load all products"""
    if os.path.exists(PRODUCTS_FILE):
        try:
            return json.load(open(PRODUCTS_FILE))
        except:
            return {}
    return {}

def save_products(products):
    """Save products to file"""
    json.dump(products, open(PRODUCTS_FILE, "w"), indent=2)

def add_product(name, length, breadth, height, weight):
    """Add or update a product"""
    products = load_products()
    products[name] = {
        "length": float(length),
        "breadth": float(breadth),
        "height": float(height),
        "weight": float(weight)
    }
    save_products(products)
    return True

def get_product(name):
    """Get product details"""
    products = load_products()
    return products.get(name)

# ---------------- ORDERS ----------------
def load_orders():
    """Load all orders"""
    if os.path.exists(ORDERS_FILE):
        try:
            return json.load(open(ORDERS_FILE))
        except:
            return {}
    return {}

def save_orders(orders):
    """Save orders to file"""
    json.dump(orders, open(ORDERS_FILE, "w"), indent=2)

def get_next_order_number():
    """Get next order number and increment counter"""
    count_data = {"count": 0}
    if os.path.exists(ORDER_COUNT_FILE):
        try:
            count_data = json.load(open(ORDER_COUNT_FILE))
        except:
            pass
    count_data["count"] = count_data.get("count", 0) + 1
    json.dump(count_data, open(ORDER_COUNT_FILE, "w"), indent=2)
    return count_data["count"]

def add_order(order_data):
    """Add new order to database"""
    orders = load_orders()
    order_number = get_next_order_number()
    order_data["order_number"] = order_number
    order_data["created_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    orders[f"order_{order_number}"] = order_data
    save_orders(orders)
    return order_number

def update_order(order_id, updates):
    """Update existing order"""
    orders = load_orders()
    if order_id in orders:
        orders[order_id].update(updates)
        save_orders(orders)
        return True
    return False

# ---------------- SEARCH FUNCTIONS ----------------
def search_order_by_phone(phone):
    """Search orders by phone number"""
    orders = load_orders()
    phone_clean = strict_phone(phone)
    if not phone_clean:
        return []
    
    results = []
    for order_id, order in orders.items():
        if order.get("phone") == phone_clean:
            results.append(order)
    return sorted(results, key=lambda x: x.get("created_date", ""), reverse=True)

def search_order_by_awb(awb):
    """Search order by AWB (Shiprocket or Manual)"""
    orders = load_orders()
    awb_clean = awb.strip().upper()
    
    for order_id, order in orders.items():
        if order.get("awb", "").upper() == awb_clean:
            return order
        if order.get("manual_awb", "").upper() == awb_clean:
            return order
    return None

def search_order_by_creative(creative_code):
    """Search orders by creative code"""
    orders = load_orders()
    creative_clean = creative_code.strip().upper()
    
    results = []
    for order_id, order in orders.items():
        if order.get("creative_code", "").upper() == creative_clean:
            results.append(order)
    return sorted(results, key=lambda x: x.get("created_date", ""), reverse=True)

def search_order_by_order_number(order_number):
    """Search order by order number"""
    orders = load_orders()
    order_id = f"order_{order_number}"
    return orders.get(order_id)

# ---------------- ANALYTICS ----------------
def get_today_orders():
    """Get orders created today"""
    orders = load_orders()
    today = datetime.now().strftime("%Y-%m-%d")
    
    today_orders = []
    for order_id, order in orders.items():
        created = order.get("created_date", "")
        if created.startswith(today):
            today_orders.append(order)
    return today_orders

def get_orders_by_date_range(days=7):
    """Get orders from last N days"""
    orders = load_orders()
    cutoff = datetime.now() - timedelta(days=days)
    
    results = []
    for order_id, order in orders.items():
        try:
            created = datetime.strptime(order.get("created_date", ""), "%Y-%m-%d %H:%M:%S")
            if created >= cutoff:
                results.append(order)
        except:
            pass
    return sorted(results, key=lambda x: x.get("created_date", ""), reverse=True)

def get_pending_advances():
    """Get orders with pending advances (3+ days old)"""
    orders = load_orders()
    cutoff = datetime.now() - timedelta(days=3)
    
    pending = []
    for order_id, order in orders.items():
        # Skip if full advance paid
        advance_paid = order.get("advance_paid", 0)
        payment_method = order.get("payment_method", "")
        
        if payment_method == "COD":
            continue  # COD orders don't need advance
        
        if advance_paid == 0:  # No advance paid yet
            try:
                created = datetime.strptime(order.get("created_date", ""), "%Y-%m-%d %H:%M:%S")
                if created <= cutoff:
                    pending.append(order)
            except:
                pass
    
    return sorted(pending, key=lambda x: x.get("created_date", ""))

def get_not_picked_orders(days=3):
    """Get orders not picked up after N days"""
    # This requires Shiprocket tracking status - placeholder for now
    # You can enhance this with actual Shiprocket API calls
    orders = load_orders()
    cutoff = datetime.now() - timedelta(days=days)
    
    not_picked = []
    for order_id, order in orders.items():
        # Skip vendor shipped orders
        if order.get("vendor_shipped"):
            continue
        
        # Check if old enough and not delivered
        try:
            created = datetime.strptime(order.get("created_date", ""), "%Y-%m-%d %H:%M:%S")
            if created <= cutoff:
                # In real implementation, check Shiprocket status here
                # For now, include all old orders
                not_picked.append(order)
        except:
            pass
    
    return sorted(not_picked, key=lambda x: x.get("created_date", ""))

def get_creative_performance(creative_code):
    """Get performance stats for a creative"""
    orders = search_order_by_creative(creative_code)
    
    total_orders = len(orders)
    total_revenue = sum(order.get("amount", 0) for order in orders)
    total_advances = sum(order.get("advance_paid", 0) for order in orders)
    
    # Count by payment method
    cod_orders = sum(1 for order in orders if order.get("payment_method") == "COD")
    prepaid_orders = total_orders - cod_orders
    
    return {
        "creative_code": creative_code,
        "total_orders": total_orders,
        "total_revenue": total_revenue,
        "total_advances": total_advances,
        "cod_orders": cod_orders,
        "prepaid_orders": prepaid_orders,
        "orders": orders
    }

def get_all_creative_codes():
    """Get list of all creative codes used"""
    orders = load_orders()
    creatives = set()
    
    for order_id, order in orders.items():
        creative = order.get("creative_code", "").strip().upper()
        if creative:
            creatives.add(creative)
    
    return sorted(list(creatives))

def get_product_wise_stats():
    """Get order counts by product"""
    orders = load_orders()
    product_counts = {}
    
    for order_id, order in orders.items():
        product = order.get("product", "Unknown")
        product_counts[product] = product_counts.get(product, 0) + 1
    
    return sorted(product_counts.items(), key=lambda x: x[1], reverse=True)

# ---------------- ADVANCE PAYMENT FUNCTIONS ----------------
def mark_advance(phone, amount):
    """Mark advance payment for an order"""
    orders = load_orders()
    phone_clean = strict_phone(phone)
    
    if not phone_clean:
        return False, "Invalid phone number"
    
    # Find most recent order for this phone
    found = False
    for order_id, order in sorted(orders.items(), reverse=True):
        if order.get("phone") == phone_clean:
            current_advance = order.get("advance_paid", 0)
            order["advance_paid"] = current_advance + float(amount)
            order["advance_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            found = True
            break
    
    if found:
        save_orders(orders)
        return True, f"Advance ₹{amount} recorded"
    else:
        return False, "Order not found"

def mark_bulk_advances(phone_amount_pairs):
    """Mark multiple advances at once
    Args:
        phone_amount_pairs: List of tuples [(phone, amount), ...]
    """
    success_count = 0
    total_amount = 0
    
    for phone, amount in phone_amount_pairs:
        success, msg = mark_advance(phone, amount)
        if success:
            success_count += 1
            total_amount += float(amount)
    
    return success_count, total_amount

# ---------------- MANUAL SHIPMENT FUNCTIONS ----------------
def add_manual_shipment(phone, courier_name, tracking_awb):
    """Add manual/vendor shipment tracking"""
    orders = load_orders()
    phone_clean = strict_phone(phone)
    
    if not phone_clean:
        return False, "Invalid phone number"
    
    # Find most recent order for this phone
    found = False
    for order_id, order in sorted(orders.items(), reverse=True):
        if order.get("phone") == phone_clean:
            order["manual_courier"] = courier_name
            order["manual_awb"] = tracking_awb
            order["manual_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            order["vendor_shipped"] = True
            found = True
            break
    
    if found:
        save_orders(orders)
        return True, f"Manual shipment added: {courier_name} - {tracking_awb}"
    else:
        return False, "Order not found"

def add_bulk_manual_shipments(shipment_data):
    """Add multiple manual shipments at once
    Args:
        shipment_data: List of tuples [(phone, courier, awb), ...]
    """
    success_count = 0
    
    for phone, courier, awb in shipment_data:
        success, msg = add_manual_shipment(phone, courier, awb)
        if success:
            success_count += 1
    
    return success_count

# ---------------- DAILY STATS ----------------
def get_daily_summary():
    """Get comprehensive daily summary"""
    today_orders = get_today_orders()
    pending_advances = get_pending_advances()
    not_picked = get_not_picked_orders()
    
    total_revenue = sum(order.get("amount", 0) for order in today_orders)
    total_advances_today = sum(order.get("advance_paid", 0) for order in today_orders if order.get("advance_date", "").startswith(datetime.now().strftime("%Y-%m-%d")))
    
    cod_count = sum(1 for order in today_orders if order.get("payment_method") == "COD")
    prepaid_count = len(today_orders) - cod_count
    
    return {
        "today_orders_count": len(today_orders),
        "today_revenue": total_revenue,
        "today_advances": total_advances_today,
        "cod_orders": cod_count,
        "prepaid_orders": prepaid_count,
        "pending_advances_count": len(pending_advances),
        "not_picked_count": len(not_picked),
        "today_orders": today_orders,
        "pending_advances": pending_advances,
        "not_picked": not_picked
    }

# ---------------- EXPORT FUNCTIONS ----------------
def export_orders_csv():
    """Export all orders to CSV format (for future Google Sheets sync)"""
    orders = load_orders()
    csv_lines = []
    
    # Header
    csv_lines.append("Order Number,Date,Phone,Name,Product,Creative,City,Payment Method,Amount,Advance,AWB,Manual AWB,Courier")
    
    # Data
    for order_id, order in sorted(orders.items()):
        line = ",".join([
            str(order.get("order_number", "")),
            order.get("created_date", ""),
            order.get("phone", ""),
            order.get("name", ""),
            order.get("product", ""),
            order.get("creative_code", ""),
            order.get("city", ""),
            order.get("payment_method", ""),
            str(order.get("amount", 0)),
            str(order.get("advance_paid", 0)),
            order.get("awb", ""),
            order.get("manual_awb", ""),
            order.get("courier", "")
        ])
        csv_lines.append(line)
    
    return "\n".join(csv_lines)
