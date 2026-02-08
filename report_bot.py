# report_bot.py - Automated Report Bot (Read-Only, Safe for Employees)
import os
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
import asyncio

# Import shared database
import shared_database as db

# ---------------- CONFIG ----------------
REPORT_BOT_TOKEN = os.getenv("REPORT_BOT_TOKEN")

if not REPORT_BOT_TOKEN:
    raise ValueError("❌ REPORT_BOT_TOKEN not set!")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("report_bot")

# Store subscribers
SUBSCRIBERS_FILE = "report_subscribers.json"

def load_subscribers():
    """Load list of subscribed users"""
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            import json
            return json.load(open(SUBSCRIBERS_FILE))
        except:
            return []
    return []

def save_subscribers(subscribers):
    """Save subscribers list"""
    import json
    json.dump(subscribers, open(SUBSCRIBERS_FILE, "w"), indent=2)

def add_subscriber(user_id):
    """Add a new subscriber"""
    subscribers = load_subscribers()
    if user_id not in subscribers:
        subscribers.append(user_id)
        save_subscribers(subscribers)
        return True
    return False

def remove_subscriber(user_id):
    """Remove a subscriber"""
    subscribers = load_subscribers()
    if user_id in subscribers:
        subscribers.remove(user_id)
        save_subscribers(subscribers)
        return True
    return False

# ---------------- TELEGRAM HANDLERS ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📊 **Backbenchers Reports Bot**\n\n"
        "Get automated daily reports:\n"
        "🌅 Morning Alert (9 AM)\n"
        "🌙 Night Report (11 PM)\n\n"
        "Commands:\n"
        "/subscribe - Get daily reports\n"
        "/unsubscribe - Stop reports\n"
        "/stats - View current stats\n\n"
        "ℹ️ This is a read-only bot.\n"
        "No control commands available.",
        parse_mode="Markdown"
    )

async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if add_subscriber(user_id):
        await update.message.reply_text(
            "✅ **Subscribed!**\n\n"
            "You'll receive:\n"
            "🌅 Morning alerts at 9 AM\n"
            "🌙 Night reports at 11 PM",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("ℹ️ You're already subscribed!")

async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if remove_subscriber(user_id):
        await update.message.reply_text("✅ Unsubscribed successfully!")
    else:
        await update.message.reply_text("ℹ️ You weren't subscribed.")

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current statistics"""
    summary = db.get_daily_summary()
    
    msg = f"📊 **DAILY STATS**\n\n"
    msg += f"📦 Orders Today: {summary['today_orders_count']}\n"
    msg += f"💰 Revenue: ₹{summary['today_revenue']}\n\n"
    
    msg += f"Payment Methods:\n"
    msg += f"• COD: {summary['cod_orders']}\n"
    msg += f"• Prepaid: {summary['prepaid_orders']}\n\n"
    
    if summary['pending_advances_count'] > 0:
        msg += f"⏳ Pending Advances: {summary['pending_advances_count']}\n"
    
    if summary['not_picked_count'] > 0:
        msg += f"🚨 Not Picked: {summary['not_picked_count']}\n"
    
    msg += f"\n📅 Updated: {datetime.now().strftime('%I:%M %p')}"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle any other messages"""
    await update.message.reply_text(
        "ℹ️ This bot only sends automated reports.\n\n"
        "Available commands:\n"
        "/subscribe - Get daily reports\n"
        "/stats - View current stats\n"
        "/unsubscribe - Stop reports"
    )

# ---------------- SCHEDULED MESSAGES ----------------
async def send_morning_alert(bot):
    """Send morning alert to all subscribers"""
    log.info("📱 Sending morning alerts...")
    
    summary = db.get_daily_summary()
    subscribers = load_subscribers()
    
    # Create morning message
    msg = f"🌅 **GOOD MORNING!** ({datetime.now().strftime('%b %d, %Y')})\n\n"
    
    # Urgent items
    urgent_count = 0
    
    if summary['not_picked_count'] > 0:
        msg += f"🚨 **NOT PICKED UP**: {summary['not_picked_count']} orders\n"
        msg += "📞 Call vendors!\n\n"
        urgent_count += summary['not_picked_count']
    
    if summary['pending_advances_count'] > 0:
        msg += f"⏳ **PENDING ADVANCE**: {summary['pending_advances_count']} orders\n"
        msg += "💰 Call customers!\n\n"
        urgent_count += summary['pending_advances_count']
    
    if urgent_count == 0:
        msg += "✅ **All Good!**\n"
        msg += "No urgent actions needed.\n\n"
    
    msg += f"📊 **Quick Stats:**\n"
    msg += f"Total Orders: {len(db.load_orders())}\n"
    msg += f"Yesterday: {summary['today_orders_count']} orders\n"
    
    msg += f"\n💡 Have a great day!"
    
    # Send to all subscribers
    sent_count = 0
    for user_id in subscribers:
        try:
            await bot.send_message(chat_id=user_id, text=msg, parse_mode="Markdown")
            sent_count += 1
        except Exception as e:
            log.error(f"Failed to send to {user_id}: {e}")
    
    log.info(f"✅ Morning alerts sent to {sent_count} users")

async def send_night_report(bot):
    """Send night report to all subscribers"""
    log.info("📱 Sending night reports...")
    
    summary = db.get_daily_summary()
    subscribers = load_subscribers()
    
    # Create night report
    msg = f"🌙 **DAILY REPORT** ({datetime.now().strftime('%b %d, %Y')})\n\n"
    
    msg += f"📦 **Today's Performance:**\n"
    msg += f"Orders: {summary['today_orders_count']}\n"
    msg += f"Revenue: ₹{summary['today_revenue']}\n"
    msg += f"Advances: ₹{summary['today_advances']}\n\n"
    
    msg += f"Payment Split:\n"
    msg += f"• COD: {summary['cod_orders']} orders\n"
    msg += f"• Prepaid: {summary['prepaid_orders']} orders\n\n"
    
    # Product stats
    product_stats = db.get_product_wise_stats()
    if product_stats:
        msg += f"📦 **Top Products:**\n"
        for product, count in product_stats[:3]:
            msg += f"• {product}: {count} orders\n"
        msg += "\n"
    
    # Creative performance (if admin wants to show)
    creatives = db.get_all_creative_codes()
    if creatives and len(creatives) <= 5:  # Only show if not too many
        msg += f"🎯 **Creative Performance:**\n"
        for creative in creatives:
            perf = db.get_creative_performance(creative)
            msg += f"• {creative}: {perf['total_orders']} orders\n"
        msg += "\n"
    
    # Pending actions
    if summary['pending_advances_count'] > 0:
        msg += f"⏳ Action needed: {summary['pending_advances_count']} pending advances\n"
    
    msg += f"\n📊 Overall: "
    if summary['today_orders_count'] >= 10:
        msg += "Excellent! 🎉"
    elif summary['today_orders_count'] >= 5:
        msg += "Good progress! ✅"
    else:
        msg += "Keep going! 💪"
    
    # Send to all subscribers
    sent_count = 0
    for user_id in subscribers:
        try:
            await bot.send_message(chat_id=user_id, text=msg, parse_mode="Markdown")
            sent_count += 1
        except Exception as e:
            log.error(f"Failed to send to {user_id}: {e}")
    
    log.info(f"✅ Night reports sent to {sent_count} users")

# ---------------- SCHEDULER ----------------
async def scheduler_loop(bot):
    """Run scheduled tasks"""
    log.info("⏰ Scheduler started")
    
    while True:
        now = datetime.now()
        current_hour = now.hour
        current_minute = now.minute
        
        # Morning alert at 9:00 AM
        if current_hour == 9 and current_minute == 0:
            await send_morning_alert(bot)
            await asyncio.sleep(60)  # Wait 1 minute to avoid duplicate
        
        # Night report at 11:00 PM
        elif current_hour == 23 and current_minute == 0:
            await send_night_report(bot)
            await asyncio.sleep(60)  # Wait 1 minute to avoid duplicate
        
        # Check every 30 seconds
        await asyncio.sleep(30)

# ---------------- MAIN ----------------
async def main():
    log.info("🚀 Report Bot starting...")
    
    # Build bot
    app = ApplicationBuilder().token(REPORT_BOT_TOKEN).build()
    
    # Add handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("subscribe", subscribe))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe))
    app.add_handler(CommandHandler("stats", show_stats))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    log.info("✅ Report bot ready")
    
    # Start scheduler in background
    bot = app.bot
    asyncio.create_task(scheduler_loop(bot))
    
    # Start polling
    await app.run_polling()

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())
