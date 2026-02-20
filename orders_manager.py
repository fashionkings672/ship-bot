# orders_manager.py
"""
Orders Manager for Backbenchers Bot
Handles all order operations with orders.json
"""

import json
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import os
# Store orders.json in same directory as this file
ORDERS_FILE = "orders.json"

def init_orders_file():
    """Initialize orders.json if it doesn't exist"""
    if not os.path.exists(ORDERS_FILE):
        data = {
            "orders": [],
            "last_updated": datetime.now().isoformat()
        }
        with open(ORDERS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✅ Created {ORDERS_FILE}")

def load_orders() -> List[Dict]:
    """Load all orders from orders.json"""
    try:
        init_orders_file()
        with open(ORDERS_FILE, 'r') as f:
            data = json.load(f)
            return data.get('orders', [])
    except Exception as e:
        print(f"❌ Error loading orders: {e}")
        return []

def save_orders(orders: List[Dict]):
    """Save orders to orders.json"""
    try:
        data = {
            "orders": orders,
            "last_updated": datetime.now().isoformat()
        }
        with open(ORDERS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✅ Saved {len(orders)} orders to {ORDERS_FILE}")
    except Exception as e:
        print(f"❌ Error saving orders: {e}")

def save_order(order_data: Dict) -> bool:
    """
    Save a new order to orders.json
    
    Args:
        order_data: Dictionary containing order information
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        orders = load_orders()
        
        # Add timestamp if not present
        if 'created_at' not in order_data:
            order_data['created_at'] = datetime.now().isoformat()
        
        # Add to orders list
        orders.append(order_data)
        
        # Save
        save_orders(orders)
        
        print(f"✅ Saved order #{order_data.get('order_number', 'N/A')}")
        return True
        
    except Exception as e:
        print(f"❌ Error saving order: {e}")
        return False

def find_order_by_phone(phone: str) -> Optional[Dict]:
    """Find order by phone number (returns most recent)"""
    try:
        orders = load_orders()
        
        # Filter by phone
        matching = [o for o in orders if o.get('phone') == phone]
        
        if not matching:
            return None
        
        # Return most recent
        matching.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return matching[0]
        
    except Exception as e:
        print(f"❌ Error finding order: {e}")
        return None

def find_order_by_awb(awb: str) -> Optional[Dict]:
    """Find order by AWB number (Shiprocket or Vendor)"""
    try:
        orders = load_orders()
        
        for order in orders:
            # Check Shiprocket AWB
            if order.get('shiprocket', {}).get('awb') == awb:
                return order
            
            # Check Vendor AWB
            if order.get('vendor_shipment', {}).get('awb') == awb:
                return order
        
        return None
        
    except Exception as e:
        print(f"❌ Error finding order by AWB: {e}")
        return None

def find_order_by_order_number(order_number: int) -> Optional[Dict]:
    """Find order by order number"""
    try:
        orders = load_orders()
        
        for order in orders:
            if order.get('order_number') == order_number:
                return order
        
        return None
        
    except Exception as e:
        print(f"❌ Error finding order: {e}")
        return None

def update_order(phone: str, updates: Dict) -> bool:
    """
    Update an existing order
    
    Args:
        phone: Phone number to identify order
        updates: Dictionary of fields to update
        
    Returns:
        bool: True if successful
    """
    try:
        orders = load_orders()
        
        # Find order
        for i, order in enumerate(orders):
            if order.get('phone') == phone:
                # Update fields
                for key, value in updates.items():
                    order[key] = value
                
                # Update timestamp
                order['updated_at'] = datetime.now().isoformat()
                
                # Save
                save_orders(orders)
                
                print(f"✅ Updated order for {phone}")
                return True
        
        print(f"⚠️ Order not found for {phone}")
        return False
        
    except Exception as e:
        print(f"❌ Error updating order: {e}")
        return False

def mark_advance_paid(phone: str, amount: float) -> bool:
    """Mark advance as paid for an order"""
    try:
        updates = {
            'advance_amount': amount,
            'advance_paid': True,
            'advance_date': datetime.now().isoformat(),
            'type': 'advance_paid'
        }
        
        return update_order(phone, updates)
        
    except Exception as e:
        print(f"❌ Error marking advance: {e}")
        return False

def convert_to_full_cod(phone: str, new_shipment_data: Dict) -> bool:
    """
    Convert order to Full COD
    
    Args:
        phone: Phone number
        new_shipment_data: New shipment details after rebooking
    """
    try:
        orders = load_orders()
        
        for order in orders:
            if order.get('phone') == phone:
                # Mark old shipment as cancelled
                if order.get('shiprocket'):
                    order['shiprocket']['status'] = 'cancelled'
                    order['shiprocket']['cancelled_at'] = datetime.now().isoformat()
                
                # Add new shipment
                order['shiprocket'] = new_shipment_data
                
                # Update type
                order['type'] = 'full_cod'
                order['updated_at'] = datetime.now().isoformat()
                
                # Save
                save_orders(orders)
                
                print(f"✅ Converted to Full COD for {phone}")
                return True
        
        return False
        
    except Exception as e:
        print(f"❌ Error converting to COD: {e}")
        return False

def add_manual_shipment(phone: str, courier_name: str, awb: str, advance: float = 0) -> bool:
    """Add manual/vendor shipment details"""
    try:
        orders = load_orders()
        
        for order in orders:
            if order.get('phone') == phone:
                # Cancel Shiprocket if exists
                if order.get('shiprocket'):
                    order['shiprocket']['status'] = 'cancelled'
                    order['shiprocket']['cancelled_reason'] = 'Moved to vendor shipment'
                
                # Add vendor shipment
                order['vendor_shipment'] = {
                    'courier': courier_name,
                    'awb': awb,
                    'added_at': datetime.now().isoformat(),
                    'status': 'active',
                    'type': 'manual_entry'
                }
                
                # Update advance if provided
                if advance > 0:
                    order['advance_amount'] = advance
                    order['advance_paid'] = True
                    order['advance_date'] = datetime.now().isoformat()
                
                order['type'] = 'manual_shipment'
                order['updated_at'] = datetime.now().isoformat()
                
                # Save
                save_orders(orders)
                
                print(f"✅ Added manual shipment for {phone}")
                return True
        
        return False
        
    except Exception as e:
        print(f"❌ Error adding manual shipment: {e}")
        return False

def get_today_stats() -> Dict:
    """Get today's statistics"""
    try:
        orders = load_orders()
        today = datetime.now().date()
        
        today_orders = [
            o for o in orders
            if datetime.fromisoformat(o['created_at']).date() == today
        ]
        
        stats = {
            'total_orders': len(today_orders),
            'total_revenue': sum(o.get('total', 0) for o in today_orders),
            'total_advances': sum(o.get('advance_amount', 0) for o in today_orders if o.get('advance_paid')),
            'advance_paid_count': len([o for o in today_orders if o.get('advance_paid')]),
            'full_cod_count': len([o for o in today_orders if o.get('type') == 'full_cod']),
            'shiprocket_count': len([o for o in today_orders if o.get('shiprocket', {}).get('status') == 'active']),
            'manual_count': len([o for o in today_orders if o.get('vendor_shipment')]),
            'creative_breakdown': {}
        }
        
        # Creative breakdown
        for order in today_orders:
            creative = order.get('creative', 'Unknown')
            if creative not in stats['creative_breakdown']:
                stats['creative_breakdown'][creative] = 0
            stats['creative_breakdown'][creative] += 1
        
        return stats
        
    except Exception as e:
        print(f"❌ Error getting stats: {e}")
        return {}

def get_week_stats() -> Dict:
    """Get this week's statistics"""
    try:
        orders = load_orders()
        week_ago = datetime.now().date() - timedelta(days=7)
        
        week_orders = [
            o for o in orders
            if datetime.fromisoformat(o['created_at']).date() >= week_ago
        ]
        
        stats = {
            'total_orders': len(week_orders),
            'total_revenue': sum(o.get('total', 0) for o in week_orders),
            'total_advances': sum(o.get('advance_amount', 0) for o in week_orders if o.get('advance_paid')),
            'advance_conversion': len([o for o in week_orders if o.get('advance_paid')]) / len(week_orders) * 100 if week_orders else 0,
        }
        
        return stats
        
    except Exception as e:
        print(f"❌ Error getting week stats: {e}")
        return {}

def format_order_details(order: Dict) -> str:
    """Format order details for display in Telegram"""
    try:
        created = datetime.fromisoformat(order['created_at']).strftime('%d %b, %I:%M %p')
        
        text = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📦 ORDER #{order.get('order_number', 'N/A')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Created: {created}

👤 CUSTOMER:
Name: {order.get('customer_name', 'N/A')}
Phone: {order.get('phone', 'N/A')}
City: {order.get('city', 'N/A')}, {order.get('state', 'N/A')}

📦 PRODUCT:
Product: {order.get('product', 'N/A')}
Creative: {order.get('creative', 'N/A')}
Price: ₹{order.get('total', 0):,}

💰 PAYMENT:
₹300 Paid: {'✅' if order.get('payment_300_paid') else '❌'}
"""
        
        # Advance info
        if order.get('advance_paid'):
            text += f"Advance: ✅ ₹{order.get('advance_amount', 0)} paid\n"
            text += f"Balance COD: ₹{order.get('total', 0) - order.get('advance_amount', 0)}\n"
        else:
            text += f"Advance: ❌ Pending\n"
        
        # Shiprocket info
        if order.get('shiprocket'):
            sr = order['shiprocket']
            status_emoji = '✅' if sr.get('status') == 'active' else '❌'
            text += f"""
🚚 SHIPROCKET:
AWB: {sr.get('awb', 'N/A')}
Courier: {sr.get('courier', 'N/A')}
Status: {status_emoji} {sr.get('status', 'N/A').title()}
Tracking: {sr.get('tracking', 'N/A')}
"""
        
        # Vendor shipment info
        if order.get('vendor_shipment'):
            vs = order['vendor_shipment']
            text += f"""
📝 VENDOR SHIPMENT:
Courier: {vs.get('courier', 'N/A')}
AWB: {vs.get('awb', 'N/A')}
Type: Manual Entry
Status: ✅ Active
"""
        
        text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        
        return text
        
    except Exception as e:
        print(f"❌ Error formatting order: {e}")
        return "Error displaying order details"

# Initialize on import
init_orders_file()