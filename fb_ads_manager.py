"""
fb_ads_manager.py — Facebook Ads Manager Integration (FINAL)
Fetches ad performance data and merges with Google Sheets orders
"""

import os, logging
from datetime import datetime, date, timedelta

log = logging.getLogger("fb_ads_manager")

# ─── CONFIG ───────────────────────────────
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
FB_AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")  # Format: act_123456789

def init_fb_api():
    """Initialize Facebook Ads API."""
    if not FB_ACCESS_TOKEN or not FB_AD_ACCOUNT_ID:
        log.warning("Facebook Ads credentials not set")
        return None
    
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.adaccount import AdAccount
        
        FacebookAdsApi.init(access_token=FB_ACCESS_TOKEN)
        return AdAccount(FB_AD_ACCOUNT_ID)
    except Exception as e:
        log.error(f"FB API init error: {e}")
        return None

def get_adset_performance(days=7):
    """
    Fetch adset performance for last N days.
    
    Returns:
        dict: {adset_name: {spend, impressions, clicks, cpm, ctr, etc}}
    """
    account = init_fb_api()
    if not account:
        return {}
    
    try:
        from facebook_business.adobjects.adsinsights import AdsInsights
        
        # Date range
        date_from = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        date_to = date.today().strftime("%Y-%m-%d")
        
        params = {
            'time_range': {'since': date_from, 'until': date_to},
            'level': 'adset',
            'fields': [
                'adset_name',
                'spend',
                'impressions',
                'clicks',
                'cpm',
                'ctr',
                'reach',
                'frequency',
            ],
        }
        
        insights = account.get_insights(params=params)
        
        adset_data = {}
        for insight in insights:
            adset_name = insight.get('adset_name', '')
            
            adset_data[adset_name] = {
                'spend': float(insight.get('spend', 0)),
                'impressions': int(insight.get('impressions', 0)),
                'clicks': int(insight.get('clicks', 0)),
                'cpm': float(insight.get('cpm', 0)),
                'ctr': float(insight.get('ctr', 0)),
                'reach': int(insight.get('reach', 0)),
                'frequency': float(insight.get('frequency', 0)),
            }
        
        log.info(f"Fetched {len(adset_data)} adsets from Facebook")
        return adset_data
        
    except Exception as e:
        log.error(f"FB adset fetch error: {e}", exc_info=True)
        return {}

def match_creative_to_adset(creative_code, adset_name):
    """
    Match creative code (e.g., BANG1) to adset name.
    Checks if creative code appears in adset name.
    
    Args:
        creative_code: str (e.g., "BANG1")
        adset_name: str (e.g., "Bangalore - Projector - BANG1")
    
    Returns:
        bool: True if match found
    """
    if not creative_code or not adset_name:
        return False
    
    # Normalize
    code = str(creative_code).strip().upper()
    name = str(adset_name).strip().upper()
    
    # Check if code exists in adset name
    return code in name

def merge_orders_with_ads(orders, adset_data):
    """
    Merge order data with Facebook Ads data based on creative code.
    
    Args:
        orders: list of order dicts (from Google Sheets)
        adset_data: dict from get_adset_performance()
    
    Returns:
        dict: {creative_code: {orders, revenue, spend, cpo, roas, cpm, ctr, etc}}
    """
    from collections import defaultdict
    
    # Group orders by creative
    creative_stats = defaultdict(lambda: {
        'orders': 0,
        'revenue': 0,
        'advance_paid': 0,
        'full_cod': 0,
        'pending': 0,
    })
    
    for order in orders:
        creative = order.get('creative', '').strip().upper()
        if not creative:
            creative = "—"
        
        creative_stats[creative]['orders'] += 1
        creative_stats[creative]['revenue'] += order.get('total', 0)
        
        if order.get('advance_paid') and order['advance_paid'] > 0:
            creative_stats[creative]['advance_paid'] += 1
        elif order.get('advance_paid') == 0:
            creative_stats[creative]['full_cod'] += 1
        else:
            creative_stats[creative]['pending'] += 1
    
    # Merge with ad data
    merged_data = {}
    
    for creative, stats in creative_stats.items():
        merged_data[creative] = {
            'creative': creative,
            'orders': stats['orders'],
            'revenue': stats['revenue'],
            'advance_paid': stats['advance_paid'],
            'full_cod': stats['full_cod'],
            'pending': stats['pending'],
            'conversion_rate': round((stats['advance_paid'] + stats['full_cod']) / stats['orders'] * 100, 1) if stats['orders'] else 0,
            'spend': 0,
            'impressions': 0,
            'clicks': 0,
            'cpm': 0,
            'ctr': 0,
            'reach': 0,
            'cpo': 0,
            'roas': 0,
        }
        
        # Find matching adset
        matched_adset = None
        for adset_name, adset_stats in adset_data.items():
            if match_creative_to_adset(creative, adset_name):
                matched_adset = adset_name
                break
        
        if matched_adset:
            ad_stats = adset_data[matched_adset]
            merged_data[creative]['spend'] = ad_stats['spend']
            merged_data[creative]['impressions'] = ad_stats['impressions']
            merged_data[creative]['clicks'] = ad_stats['clicks']
            merged_data[creative]['cpm'] = ad_stats['cpm']
            merged_data[creative]['ctr'] = ad_stats['ctr']
            merged_data[creative]['reach'] = ad_stats['reach']
            merged_data[creative]['frequency'] = ad_stats.get('frequency', 0)
            
            # Calculate CPO and ROAS
            if stats['orders'] > 0:
                merged_data[creative]['cpo'] = round(ad_stats['spend'] / stats['orders'], 2)
            
            if ad_stats['spend'] > 0:
                merged_data[creative]['roas'] = round(stats['revenue'] / ad_stats['spend'], 2)
            
            merged_data[creative]['matched_adset'] = matched_adset
        else:
            merged_data[creative]['matched_adset'] = None
    
    return merged_data

def export_to_sheets(merged_data, sheet_client, sheet_id):
    """
    Export merged ad performance data to Google Sheets.
    
    Args:
        merged_data: dict from merge_orders_with_ads()
        sheet_client: gspread client
        sheet_id: Google Sheet ID
    """
    try:
        sh = sheet_client.open_by_key(sheet_id)
        
        # Get or create Ad Performance sheet
        try:
            ws = sh.worksheet("Ad Performance")
            ws.clear()
        except:
            ws = sh.add_worksheet("Ad Performance", rows=500, cols=20)
        
        # Headers
        headers = [
            "Creative",
            "Matched Adset",
            "Orders",
            "Revenue",
            "Advance Paid",
            "Full COD",
            "Pending",
            "Conv Rate %",
            "Spend",
            "CPO",
            "ROAS",
            "Impressions",
            "Clicks",
            "CPM",
            "CTR %",
            "Reach",
            "Frequency",
            "Status"
        ]
        
        rows = [headers]
        
        # Sort by orders descending
        for creative, data in sorted(merged_data.items(), key=lambda x: -x[1]['orders']):
            # Determine status
            cpo = data['cpo']
            conv_rate = data['conversion_rate']
            
            if cpo > 0 and cpo < 250 and conv_rate >= 60:
                status = "🟢 SCALE"
            elif cpo > 0 and cpo < 400 and conv_rate >= 40:
                status = "🟡 OPTIMIZE"
            elif cpo > 400 or conv_rate < 40:
                status = "🔴 KILL"
            else:
                status = "⚪ NEW"
            
            rows.append([
                data['creative'],
                data.get('matched_adset', 'Not Matched'),
                data['orders'],
                f"₹{int(data['revenue']):,}",
                data['advance_paid'],
                data['full_cod'],
                data['pending'],
                data['conversion_rate'],
                f"₹{int(data['spend']):,}",
                f"₹{int(data['cpo'])}" if data['cpo'] else "—",
                f"{data['roas']}x" if data['roas'] else "—",
                data['impressions'],
                data['clicks'],
                f"₹{data['cpm']:.2f}" if data['cpm'] else "—",
                f"{data['ctr']:.2f}%" if data['ctr'] else "—",
                data['reach'],
                f"{data.get('frequency', 0):.2f}",
                status
            ])
        
        ws.update("A1", rows)
        
        # Format headers
        ws.format("A1:R1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
            "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}}
        })
        
        # Auto-resize columns
        ws.columns_auto_resize(0, 17)
        
        log.info(f"✅ Exported {len(rows)-1} creatives to 'Ad Performance' sheet")
        return True
        
    except Exception as e:
        log.error(f"Export to sheets error: {e}", exc_info=True)
        return False
