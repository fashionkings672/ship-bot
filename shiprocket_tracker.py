"""
shiprocket_tracker.py — Shiprocket Status Checker (FINAL)
Only checks order status via AWB - does NOT create/modify orders
"""

import os, time, logging, asyncio
import requests
from datetime import datetime, timedelta

log = logging.getLogger("shiprocket_tracker")

SR_BASE = "https://apiv2.shiprocket.in/v1/external"
SHIPROCKET_EMAIL = os.getenv("SHIPROCKET_EMAIL")
SHIPROCKET_PASS  = os.getenv("SHIPROCKET_PASSWORD")

_token = None
_token_exp = 0

def get_token(force=False):
    global _token, _token_exp
    if not force and _token and time.time() < _token_exp:
        return _token
    
    r = requests.post(
        f"{SR_BASE}/auth/login",
        json={"email": SHIPROCKET_EMAIL, "password": SHIPROCKET_PASS},
        timeout=60
    )
    data = r.json()
    if "token" not in data:
        raise Exception(f"SR login failed: {data}")
    
    _token = data["token"]
    _token_exp = time.time() + 23 * 3600
    return _token

def check_order_status(awb):
    """Check single order status by AWB."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        
        r = requests.get(
            f"{SR_BASE}/courier/track/awb/{awb}",
            headers=headers,
            timeout=30
        )
        
        if r.status_code != 200:
            return None
        
        data = r.json()
        tracking_data = data.get("tracking_data", {})
        
        if not tracking_data:
            return None
        
        # Get latest status
        shipment_track = tracking_data.get("shipment_track", [])
        if shipment_track:
            latest = shipment_track[0]
            return {
                "awb": awb,
                "status": latest.get("current_status", ""),
                "date": latest.get("date", ""),
                "delivered": tracking_data.get("delivered_date") is not None,
                "cancelled": "cancel" in latest.get("current_status", "").lower(),
            }
        
        return None
    except Exception as e:
        log.error(f"Check status {awb}: {e}")
        return None

async def bulk_check_not_picked_up(orders):
    """
    Check multiple orders and return those NOT picked up.
    
    Args:
        orders: list of dicts with keys: order_number, name, phone, awb, date, vendor
    
    Returns:
        List of orders not picked up with days_pending and priority
    """
    not_picked = []
    
    for order in orders:
        awb = order.get('awb')
        if not awb:
            continue
        
        status_data = check_order_status(awb)
        
        if not status_data:
            continue
        
        current_status = status_data.get('status', '').lower()
        
        # Check if not picked up
        if 'pickup pending' in current_status or 'not picked' in current_status or current_status == '':
            # Calculate days pending
            try:
                order_date = datetime.fromisoformat(order['date'].replace(' ', 'T'))
                days_pending = (datetime.now() - order_date).days
            except:
                days_pending = 0
            
            # Set priority
            if days_pending >= 3:
                priority = "🔴 CRITICAL"
            elif days_pending == 2:
                priority = "🟡 WARNING"
            else:
                priority = "🟢 RECENT"
            
            not_picked.append({
                'order_number': order['order_number'],
                'name': order['name'],
                'phone': order['phone'],
                'awb': awb,
                'vendor': order.get('vendor', ''),
                'days_pending': days_pending,
                'priority': priority,
            })
        
        # Rate limit
        await asyncio.sleep(0.5)
    
    return not_picked

async def check_cancelled_orders(orders):
    """
    Check multiple orders and return those CANCELLED.
    
    Args:
        orders: list of dicts with keys: order_number, name, phone, awb, vendor
    
    Returns:
        List of cancelled orders
    """
    cancelled = []
    
    for order in orders:
        awb = order.get('awb')
        if not awb:
            continue
        
        status_data = check_order_status(awb)
        
        if not status_data:
            continue
        
        if status_data.get('cancelled'):
            cancelled.append({
                'order_number': order['order_number'],
                'name': order['name'],
                'phone': order['phone'],
                'awb': awb,
                'vendor': order.get('vendor', ''),
                'cancel_reason': status_data.get('status', 'Unknown'),
            })
        
        # Rate limit
        await asyncio.sleep(0.5)
    
    return cancelled
