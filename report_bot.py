"""
report_bot.py — Oneboxx Report Bot (FINAL)
- Reads ONLY from Google Sheets
- Facebook Ads integration with creative matching
- Daily automated checks (not picked up, cancelled)
- AI-powered analysis
- Complete performance reports
"""

import os, re, json, logging, asyncio
from datetime import datetime, date, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, CallbackQueryHandler, filters
import gspread
from google.oauth2.service_account import Credentials

from shiprocket_tracker import check_order_status, bulk_check_not_picked_up, check_cancelled_orders
from ai_analyst import analyze_creative_performance, generate_daily_briefing
from fb_ads_manager import get_adset_performance, merge_orders_with_ads, export_to_sheets

# ─── CONFIG ───────────────────────────────
REPORT_BOT_TOKEN = os.getenv("REPORT_BOT_TOKEN")
GOOGLE_SHEET_ID  = os.getenv("GOOGLE_SHEET_ID")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")

if not REPORT_BOT_TOKEN: raise ValueError("REPORT_BOT_TOKEN not set")
if not GOOGLE_SHEET_ID:  raise ValueError("GOOGLE_SHEET_ID not set")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("report_bot")

# ─── GOOGLE SHEETS ────────────────────────
_gc = None

def get_sheets_client():
    global _gc
    if _gc: return _gc
    try:
        raw = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not raw: raise ValueError("GOOGLE_CREDENTIALS_JSON not set")
        creds = Credentials.from_service_account_info(
            json.loads(raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
        _gc = gspread.authorize(creds)
        return _gc
    except Exception as e:
        log.error(f"Sheets auth: {e}")
        raise

def get_worksheet(sheet_name="Orders"):
    gc = get_sheets_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        return sh.worksheet(sheet_name)
    except:
        return None

def read_all_orders():
    """Read all orders from Google Sheets."""
    ws = get_worksheet("Orders")
    if not ws: return []
    rows = ws.get_all_records()
    
    orders = []
    for row in rows:
        if not row.get("Order#"): continue
        
        # Parse advance
        adv_raw = str(row.get("Advance","")).strip()
        if adv_raw in ("", "None", "none"):
            advance_paid = None
        else:
            try: advance_paid = int(float(adv_raw))
            except: advance_paid = None
        
        # Parse numeric fields
        try: courier_paid = int(float(str(row.get("Courier Paid",0) or 0)))
        except: courier_paid = 0
        
        try: total = float(str(row.get("Total",0) or 0))
        except: total = 0
        
        try: cod = float(str(row.get("COD",0) or 0))
        except: cod = 0
        
        orders.append({
            "order_number": str(row.get("Order#","")),
            "date": str(row.get("Date","")),
            "name": str(row.get("Name","")),
            "phone": str(row.get("Phone","")),
            "city": str(row.get("City","")),
            "state": str(row.get("State","")),
            "pincode": str(row.get("Pincode","")),
            "product": str(row.get("Product","")),
            "creative": str(row.get("Creative","")),
            "total": total,
            "courier_paid": courier_paid,
            "advance_paid": advance_paid,
            "cod": cod,
            "payment_status": str(row.get("Payment Status","")),
            "vendor": str(row.get("Vendor","")),
            "courier": str(row.get("Courier","")),
            "awb": str(row.get("AWB","")),
            "tracking": str(row.get("Tracking","")),
            "status": str(row.get("Status","active")),
            "pickup_location": str(row.get("Pickup","")),
            "label_downloaded": str(row.get("Label Downloaded","")),
        })
    
    return orders

def update_order_in_sheet(order_number, updates):
    """Update specific order in Google Sheets."""
    try:
        ws = get_worksheet("Orders")
        if not ws: return False
        
        col_a = ws.col_values(1)
        if str(order_number) in col_a:
            row_idx = col_a.index(str(order_number)) + 1
            
            # Update specific columns
            for col_name, value in updates.items():
                col_map = {
                    "Status": "S",
                    "Payment Status": "N",
                    "Advance": "L",
                    "Label Downloaded": "U",
                }
                if col_name in col_map:
                    ws.update(f"{col_map[col_name]}{row_idx}", [[value]])
            return True
        return False
    except Exception as e:
        log.error(f"Update sheet error: {e}")
        return False

def add_not_picked_sheet(data):
    """Create/update Not Picked Up sheet."""
    try:
        gc = get_sheets_client()
        sh = gc.open_by_key(GOOGLE_SHEET_ID)
        
        try:
            ws = sh.worksheet("Not Picked Up")
            ws.clear()
        except:
            ws = sh.add_worksheet("Not Picked Up", rows=500, cols=10)
        
        headers = ["Order#", "Name", "Phone", "AWB", "Days Pending", "Vendor", "Priority", "Last Checked"]
        rows = [headers]
        
        for item in data:
            rows.append([
                item['order_number'],
                item['name'],
                item['phone'],
                item['awb'],
                item['days_pending'],
                item['vendor'],
                item['priority'],
                datetime.now().strftime("%Y-%m-%d %H:%M")
            ])
        
        ws.update("A1", rows)
        ws.format("A1:H1", {"textFormat": {"bold": True}})
        return True
    except Exception as e:
        log.error(f"Add not picked sheet: {e}")
        return False

def add_cancelled_sheet(data):
    """Create/update Cancelled Orders sheet."""
    try:
        gc = get_sheets_client()
        sh = gc.open_by_key(GOOGLE_SHEET_ID)
        
        try:
            ws = sh.worksheet("Cancelled Orders")
        except:
            ws = sh.add_worksheet("Cancelled Orders", rows=500, cols=10)
            headers = ["Order#", "Name", "Phone", "AWB", "Vendor", "Reason", "Cancelled Date"]
            ws.append_row(headers)
            ws.format("A1:G1", {"textFormat": {"bold": True}})
        
        for item in data:
            ws.append_row([
                item['order_number'],
                item['name'],
                item['phone'],
                item['awb'],
                item['vendor'],
                item.get('cancel_reason', 'Unknown'),
                datetime.now().strftime("%Y-%m-%d %H:%M")
            ])
        
        return True
    except Exception as e:
        log.error(f"Add cancelled sheet: {e}")
        return False

# ─── KEYBOARDS ────────────────────────────
MAIN_KB = ReplyKeyboardMarkup([
    ["📦 Not Picked Up", "❌ Cancelled Orders"],
    ["🎨 Creative Report", "📊 Ads Performance"],
    ["💰 Financial Report", "🤖 AI Briefing"],
    ["🔄 Sync Data"],
], resize_keyboard=True)

# ─── /start ───────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📊 *Oneboxx Report Bot*\n\n"
        "Daily reports from Google Sheets + Facebook Ads\n\n"
        "Commands:\n"
        "/notpicked - Not picked up orders\n"
        "/cancelled - Cancelled orders\n"
        "/creative [days] - Creative performance\n"
        "/ads [days] - Facebook Ads performance\n"
        "/finance [days] - Financial report\n"
        "/ai - AI daily briefing\n"
        "/sync - Sync from Google Sheets",
        parse_mode="Markdown",
        reply_markup=MAIN_KB)

# ─── NOT PICKED UP ────────────────────────
async def cmd_not_picked(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Checking not picked up orders from Google Sheets...")
    
    try:
        orders = read_all_orders()
        active_orders = [o for o in orders if o['status'] == 'active' and o['awb']]
        
        if not active_orders:
            await msg.edit_text("✅ No active orders with AWB found")
            return
        
        # Check status via Shiprocket
        not_picked = await bulk_check_not_picked_up(active_orders)
        
        if not not_picked:
            await msg.edit_text("✅ All orders picked up!")
            return
        
        # Categorize by priority
        critical = [o for o in not_picked if o['days_pending'] >= 3]
        warning  = [o for o in not_picked if o['days_pending'] == 2]
        recent   = [o for o in not_picked if o['days_pending'] <= 1]
        
        # Update Google Sheets
        add_not_picked_sheet(not_picked)
        
        # Format report
        lines = [f"📦 *NOT PICKED UP ORDERS ({len(not_picked)})*\n"]
        
        if critical:
            lines.append(f"🔴 *CRITICAL ({len(critical)}) — 3+ days:*")
            for o in critical[:5]:
                lines.append(f"  #{o['order_number']} - {o['name']} - {o['days_pending']}d - {o['vendor']}")
            if len(critical) > 5:
                lines.append(f"  ... +{len(critical)-5} more")
            lines.append("")
        
        if warning:
            lines.append(f"🟡 *WARNING ({len(warning)}) — 2 days:*")
            for o in warning[:5]:
                lines.append(f"  #{o['order_number']} - {o['name']} - {o['vendor']}")
            if len(warning) > 5:
                lines.append(f"  ... +{len(warning)-5} more")
            lines.append("")
        
        if recent:
            lines.append(f"🟢 *RECENT ({len(recent)}) — 1 day:*")
            for o in recent[:3]:
                lines.append(f"  #{o['order_number']} - {o['name']} - {o['vendor']}")
            if len(recent) > 3:
                lines.append(f"  ... +{len(recent)-3} more")
        
        lines.append(f"\n✅ Updated 'Not Picked Up' sheet in Google Sheets")
        
        await msg.edit_text("\n".join(lines), parse_mode="Markdown")
        
    except Exception as e:
        log.error(f"Not picked error: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")

# ─── CANCELLED ORDERS ─────────────────────
async def cmd_cancelled(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Checking for cancelled orders...")
    
    try:
        orders = read_all_orders()
        active_orders = [o for o in orders if o['status'] == 'active' and o['awb']]
        
        # Check via Shiprocket API
        cancelled = await check_cancelled_orders(active_orders)
        
        if not cancelled:
            await msg.edit_text("✅ No cancelled orders found")
            return
        
        # Update Google Sheets
        for order in cancelled:
            update_order_in_sheet(order['order_number'], {"Status": "cancelled"})
        
        # Add to Cancelled Orders sheet
        add_cancelled_sheet(cancelled)
        
        lines = [f"❌ *CANCELLED ORDERS ({len(cancelled)})*\n"]
        
        for o in cancelled:
            lines.append(
                f"#{o['order_number']} - {o['name']}\n"
                f"  AWB: {o['awb']} | {o['vendor']}\n"
                f"  Reason: {o.get('cancel_reason', 'Unknown')}\n"
            )
        
        lines.append(f"\n✅ Updated {len(cancelled)} orders to 'cancelled' status")
        lines.append(f"✅ Added to 'Cancelled Orders' sheet")
        
        await msg.edit_text("\n".join(lines), parse_mode="Markdown")
        
    except Exception as e:
        log.error(f"Cancelled check error: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")

# ─── CREATIVE REPORT ──────────────────────
async def cmd_creative(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    days = 7
    if ctx.args:
        try: days = int(ctx.args[0])
        except: pass
    
    msg = await update.message.reply_text(f"⏳ Analyzing creative performance (last {days} days)...")
    
    try:
        orders = read_all_orders()
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        period_orders = [o for o in orders if o['date'] >= cutoff]
        
        if not period_orders:
            await msg.edit_text(f"No orders in last {days} days")
            return
        
        # Group by creative
        from collections import defaultdict
        creative_stats = defaultdict(lambda: {
            'orders': 0,
            'revenue': 0,
            'advance_paid': 0,
            'full_cod': 0,
            'pending': 0,
        })
        
        for o in period_orders:
            creative = o['creative'] or "—"
            creative_stats[creative]['orders'] += 1
            creative_stats[creative]['revenue'] += o['total']
            
            if o['advance_paid'] and o['advance_paid'] > 0:
                creative_stats[creative]['advance_paid'] += 1
            elif o['advance_paid'] == 0:
                creative_stats[creative]['full_cod'] += 1
            else:
                creative_stats[creative]['pending'] += 1
        
        # AI Analysis
        analysis = await analyze_creative_performance(creative_stats, days)
        
        lines = [
            f"🎨 *CREATIVE PERFORMANCE - Last {days} Days*\n",
            f"Total Orders: {len(period_orders)}\n"
        ]
        
        for creative, stats in sorted(creative_stats.items(), key=lambda x: -x[1]['orders']):
            conv_rate = round((stats['advance_paid'] + stats['full_cod']) / stats['orders'] * 100, 1)
            
            if conv_rate >= 60:
                emoji = "🟢"
            elif conv_rate >= 40:
                emoji = "🟡"
            else:
                emoji = "🔴"
            
            lines.append(
                f"{emoji} *{creative}*\n"
                f"  Orders: {stats['orders']} | Revenue: ₹{int(stats['revenue']):,}\n"
                f"  Advance: {stats['advance_paid']} ({round(stats['advance_paid']/stats['orders']*100,1)}%)\n"
                f"  Full COD: {stats['full_cod']} | Pending: {stats['pending']}\n"
                f"  Conv Rate: {conv_rate}%\n"
            )
        
        if analysis:
            lines.append(f"\n💡 *AI Recommendations:*\n{analysis}")
        
        await msg.edit_text("\n".join(lines), parse_mode="Markdown")
        
    except Exception as e:
        log.error(f"Creative report error: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")

# ─── ADS PERFORMANCE ──────────────────────
async def cmd_ads(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    days = 7
    if ctx.args:
        try: days = int(ctx.args[0])
        except: pass
    
    msg = await update.message.reply_text(f"⏳ Fetching ad performance from Facebook (last {days} days)...")
    
    try:
        # Fetch from Facebook Ads
        adset_data = get_adset_performance(days)
        
        if not adset_data:
            await msg.edit_text(
                "⚠️ No Facebook Ads data found.\n\n"
                "Check environment variables:\n"
                "- FB_ACCESS_TOKEN\n"
                "- FB_AD_ACCOUNT_ID"
            )
            return
        
        await msg.edit_text(f"⏳ Got {len(adset_data)} adsets. Merging with Google Sheets orders...")
        
        # Get orders from Google Sheets
        orders = read_all_orders()
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        period_orders = [o for o in orders if o['date'] >= cutoff]
        
        if not period_orders:
            await msg.edit_text(f"❌ No orders in last {days} days")
            return
        
        # Merge data
        merged_data = merge_orders_with_ads(period_orders, adset_data)
        
        # Export to Google Sheets
        gc = get_sheets_client()
        export_success = export_to_sheets(merged_data, gc, GOOGLE_SHEET_ID)
        
        # Format report
        lines = [
            f"📊 *AD PERFORMANCE REPORT - Last {days} Days*\n",
            f"📱 Facebook Adsets: {len(adset_data)}",
            f"📦 Total Orders: {len(period_orders)}",
            f"🎨 Creatives: {len(merged_data)}\n"
        ]
        
        # Top performers
        sorted_creatives = sorted(merged_data.items(), key=lambda x: -x[1]['orders'])
        
        lines.append("*🏆 TOP PERFORMERS:*")
        for creative, data in sorted_creatives[:5]:
            cpo = data['cpo']
            roas = data['roas']
            conv = data['conversion_rate']
            
            if cpo > 0 and cpo < 250 and conv >= 60:
                emoji = "🟢"
            elif cpo > 0 and cpo < 400:
                emoji = "🟡"
            else:
                emoji = "🔴"
            
            matched_adset = data.get('matched_adset', 'No match')
            if len(matched_adset) > 30:
                matched_adset = matched_adset[:27] + "..."
            
            lines.append(
                f"{emoji} *{creative}*\n"
                f"  Orders: {data['orders']} | CPO: ₹{int(cpo) if cpo else 0} | ROAS: {roas}x\n"
                f"  Spend: ₹{int(data['spend']):,} | Conv: {conv}%\n"
                f"  Matched: {matched_adset}\n"
            )
        
        # Summary stats
        total_spend = sum(d['spend'] for d in merged_data.values())
        total_revenue = sum(d['revenue'] for d in merged_data.values())
        total_orders = sum(d['orders'] for d in merged_data.values())
        
        avg_cpo = round(total_spend / total_orders, 2) if total_orders else 0
        avg_roas = round(total_revenue / total_spend, 2) if total_spend else 0
        
        lines.append(
            f"\n*📈 OVERALL:*\n"
            f"Total Spend: ₹{int(total_spend):,}\n"
            f"Total Revenue: ₹{int(total_revenue):,}\n"
            f"Avg CPO: ₹{int(avg_cpo)}\n"
            f"Avg ROAS: {avg_roas}x\n"
        )
        
        if export_success:
            lines.append("✅ Full report exported to 'Ad Performance' sheet")
        
        await msg.edit_text("\n".join(lines), parse_mode="Markdown")
        
    except Exception as e:
        log.error(f"Ads report error: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")

# ─── FINANCIAL REPORT ─────────────────────
async def cmd_finance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    days = 7
    if ctx.args:
        try: days = int(ctx.args[0])
        except: pass
    
    msg = await update.message.reply_text(f"⏳ Generating financial report ({days} days)...")
    
    try:
        orders = read_all_orders()
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        period_orders = [o for o in orders if o['date'] >= cutoff]
        
        total_orders = len(period_orders)
        total_revenue = sum(o['total'] for o in period_orders)
        
        advance_orders = [o for o in period_orders if o['advance_paid'] and o['advance_paid'] > 0]
        advance_collected = sum(o['advance_paid'] for o in advance_orders)
        
        cod_orders = [o for o in period_orders if o['advance_paid'] == 0]
        cod_amount = sum(o['total'] for o in cod_orders)
        
        pending = [o for o in period_orders if o['advance_paid'] is None]
        pending_amount = sum(o['total'] for o in pending)
        
        courier_charges = sum(o['courier_paid'] for o in period_orders)
        
        lines = [
            f"💰 *FINANCIAL REPORT - Last {days} Days*\n",
            f"📦 Total Orders: {total_orders}",
            f"💵 Total Revenue: ₹{int(total_revenue):,}\n",
            f"*Payment Collection:*",
            f"  💰 Advance Paid: ₹{int(advance_collected):,} ({len(advance_orders)} orders)",
            f"  💵 Full COD: ₹{int(cod_amount):,} ({len(cod_orders)} orders)",
            f"  ⏳ Pending: ₹{int(pending_amount):,} ({len(pending)} orders)\n",
            f"*Expenses:*",
            f"  🚚 Courier Charges: ₹{int(courier_charges):,}\n",
            f"*Net Revenue:*",
            f"  ✅ Collected: ₹{int(advance_collected + cod_amount):,}",
            f"  📊 Collection Rate: {round((advance_collected + cod_amount) / total_revenue * 100, 1)}%",
        ]
        
        await msg.edit_text("\n".join(lines), parse_mode="Markdown")
        
    except Exception as e:
        log.error(f"Finance report error: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")

# ─── AI BRIEFING ──────────────────────────
async def cmd_ai_briefing(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🤖 Generating AI daily briefing...")
    
    try:
        orders = read_all_orders()
        briefing = await generate_daily_briefing(orders)
        
        await msg.edit_text(briefing, parse_mode="Markdown")
        
    except Exception as e:
        log.error(f"AI briefing error: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")

# ─── SYNC DATA ────────────────────────────
async def cmd_sync(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Syncing all data from Google Sheets...")
    try:
        orders = read_all_orders()
        await msg.edit_text(f"✅ Loaded {len(orders)} orders from Google Sheets")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {e}")

# ─── MESSAGE HANDLER ──────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    
    if text == "📦 Not Picked Up":
        await cmd_not_picked(update, ctx)
    elif text == "❌ Cancelled Orders":
        await cmd_cancelled(update, ctx)
    elif text == "🎨 Creative Report":
        await cmd_creative(update, ctx)
    elif text == "📊 Ads Performance":
        await cmd_ads(update, ctx)
    elif text == "💰 Financial Report":
        await cmd_finance(update, ctx)
    elif text == "🤖 AI Briefing":
        await cmd_ai_briefing(update, ctx)
    elif text == "🔄 Sync Data":
        await cmd_sync(update, ctx)

# ─── SCHEDULED TASKS ──────────────────────
async def daily_not_picked_check():
    """Run at 9 AM daily - check not picked up orders and update sheet."""
    try:
        log.info("Running daily not picked up check...")
        orders = read_all_orders()
        active_orders = [o for o in orders if o['status'] == 'active' and o['awb']]
        
        not_picked = await bulk_check_not_picked_up(active_orders)
        
        if not_picked:
            add_not_picked_sheet(not_picked)
            log.info(f"Daily check: {len(not_picked)} orders not picked up - sheet updated")
        else:
            log.info("Daily check: All orders picked up")
            
    except Exception as e:
        log.error(f"Daily not picked check error: {e}", exc_info=True)

async def daily_cancelled_check():
    """Run every 6 hours - check cancelled orders and update sheet."""
    try:
        log.info("Running cancelled orders check...")
        orders = read_all_orders()
        active_orders = [o for o in orders if o['status'] == 'active' and o['awb']]
        
        cancelled = await check_cancelled_orders(active_orders)
        
        if cancelled:
            for order in cancelled:
                update_order_in_sheet(order['order_number'], {"Status": "cancelled"})
            
            add_cancelled_sheet(cancelled)
            log.info(f"Cancelled check: {len(cancelled)} orders cancelled - sheet updated")
        else:
            log.info("Cancelled check: No new cancellations")
            
    except Exception as e:
        log.error(f"Cancelled check error: {e}", exc_info=True)

# ─── SCHEDULER ────────────────────────────
async def schedule_tasks():
    """Schedule daily background tasks."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    
    scheduler = AsyncIOScheduler()
    
    # 9 AM daily - Not picked up check
    scheduler.add_job(
        daily_not_picked_check,
        CronTrigger(hour=9, minute=0)
    )
    
    # Every 6 hours - Cancelled check
    scheduler.add_job(
        daily_cancelled_check,
        CronTrigger(hour="*/6")
    )
    
    scheduler.start()
    log.info("✅ Scheduled tasks started (9 AM not picked up, every 6h cancelled check)")

# ─── MAIN ─────────────────────────────────
async def main():
    log.info("Starting Report Bot...")
    
    app = ApplicationBuilder().token(REPORT_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("notpicked", cmd_not_picked))
    app.add_handler(CommandHandler("cancelled", cmd_cancelled))
    app.add_handler(CommandHandler("creative", cmd_creative))
    app.add_handler(CommandHandler("ads", cmd_ads))
    app.add_handler(CommandHandler("finance", cmd_finance))
    app.add_handler(CommandHandler("ai", cmd_ai_briefing))
    app.add_handler(CommandHandler("sync", cmd_sync))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Start scheduler
    await schedule_tasks()
    
    log.info("Report Bot running...")
    await app.run_polling()

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())
