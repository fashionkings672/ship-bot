"""
start_all_bots.py - Run both Oneboxx shipping bots together.
Handles:
- bot.py (first shipping bot)
- bot_enhanced.py (main shipping bot with Meta upload)
"""

import asyncio
import logging
from multiprocessing import Process, Manager
import os
import signal
import sys
import time

try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    print("❌ Missing nest_asyncio. Install with: pip install nest-asyncio")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("all_bots.log", mode='a', encoding='utf-8')
    ]
)
log = logging.getLogger("ALL_BOTS")

manager = Manager()
bot_status = manager.dict()

def check_environment():
    """Verify required environment variables exist."""
    log.info("🔍 Checking environment setup...")

    required_vars = [
        ("BOT_TOKEN",              "First shipping bot token"),
        ("BOT_TOKEN_2",            "Main shipping bot token"),
        ("GOOGLE_CREDENTIALS_JSON","Google Sheets credentials"),
        ("GOOGLE_SHEET_ID",        "Google Sheet ID"),
        ("SHIPROCKET_EMAIL",       "Shiprocket email"),
        ("SHIPROCKET_PASSWORD",    "Shiprocket password"),
        ("META_ACCESS_TOKEN",      "Meta access token (for uploader)"),
        ("META_DATASET_ID",        "Meta offline event set ID"),
    ]

    missing = []
    for var_name, description in required_vars:
        if not os.getenv(var_name):
            missing.append(f"{var_name} ({description})")
            log.error(f"❌ {var_name} not set!")
        else:
            log.info(f"✅ {var_name} OK")

    if missing:
        log.error(f"🚨 MISSING {len(missing)} REQUIRED VARIABLES:")
        for m in missing:
            log.error(f"  • {m}")
        return False

    log.info("✅ Environment check passed!")
    return True

def run_bot1():
    """Run first shipping bot (bot.py)"""
    name = "Bot1"
    try:
        log.info(f"🚀 Starting {name}...")
        bot_status[name] = "starting"
        import bot
        log.info(f"✅ {name} imported successfully")
        bot_status[name] = "running"
        asyncio.run(bot.main())
        bot_status[name] = "stopped"
        log.info(f"🛑 {name} stopped")
    except Exception as e:
        log.error(f"💥 {name} CRASHED: {e}", exc_info=True)
        bot_status[name] = f"error: {str(e)}"
        raise

def run_bot2():
    """Run enhanced shipping bot (bot_enhanced.py)"""
    name = "BotEnhanced"
    try:
        log.info(f"🚀 Starting {name}...")
        bot_status[name] = "starting"
        import bot_enhanced
        log.info(f"✅ {name} imported successfully")
        bot_status[name] = "running"
        asyncio.run(bot_enhanced.main())
        bot_status[name] = "stopped"
        log.info(f"🛑 {name} stopped")
    except Exception as e:
        log.error(f"💥 {name} CRASHED: {e}", exc_info=True)
        bot_status[name] = f"error: {str(e)}"
        raise

def monitor_bots(processes):
    """Monitor all bot processes and restart if needed."""
    log.info("👀 Starting bot monitor...")
    while True:
        time.sleep(5)
        for i, proc in enumerate(processes):
            if not proc.is_alive() and proc.exitcode is not None:
                log.warning(f"⚠️ Bot {i+1} ({proc.name}) died with exit code {proc.exitcode}")
                try:
                    log.info(f"🔄 Attempting to restart {proc.name}...")
                    new_proc = Process(target=proc._target, name=proc.name)
                    new_proc.start()
                    processes[i] = new_proc
                    log.info(f"✅ {proc.name} restarted successfully")
                except Exception as e:
                    log.error(f"❌ Failed to restart {proc.name}: {e}")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    log.info(f"🛑 Received signal {signum}, shutting down...")
    os._exit(0)

def main():
    """Main function to start both shipping bots."""
    log.info("=" * 60)
    log.info("🚀 STARTING ONEBOXX SHIPPING BOTS")
    log.info("=" * 60)

    if not check_environment():
        log.error("❌ Environment check failed. Exiting.")
        sys.exit(1)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    processes = []
    try:
        # Start Bot 1
        p1 = Process(target=run_bot1, name="Bot1")
        p1.start()
        processes.append(p1)
        log.info(f"✅ Bot1 started (PID: {p1.pid})")
        time.sleep(2)

        # Start Bot 2 (enhanced)
        p2 = Process(target=run_bot2, name="BotEnhanced")
        p2.start()
        processes.append(p2)
        log.info(f"✅ BotEnhanced started (PID: {p2.pid})")
        time.sleep(2)

        log.info("=" * 60)
        log.info("🎉 BOTH BOTS ARE RUNNING!")
        log.info("📊 BotEnhanced commands: /orders, /report, /uploadfb, /adsspend, /setcreative")
        log.info("⏰ Meta upload auto-runs daily at 11:00 PM IST")
        log.info("=" * 60)

        # Monitor
        monitor = Process(target=monitor_bots, args=(processes,))
        monitor.start()

        for p in processes:
            p.join()

    except KeyboardInterrupt:
        log.info("🛑 Manual shutdown requested")
    except Exception as e:
        log.error(f"💥 Main process error: {e}", exc_info=True)
    finally:
        log.info("👋 Shutting down all bots...")
        for p in processes:
            if p.is_alive():
                p.terminate()
        time.sleep(2)
        log.info("✅ All bots shut down")
        sys.exit(0)

if __name__ == "__main__":
    main()
s
