""""
start_all_bots.py - Run Oneboxx shipping bot.
Runs bot_enhanced.py only (uses Gemini 2.0 Flash for parsing).
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
    log.info("=" * 60)
    log.info("ENVIRONMENT VARIABLES CHECK")
    log.info("=" * 60)

    required_vars = [
        ("BOT_TOKEN_2",             "Telegram bot token"),
        ("GOOGLE_CREDENTIALS_JSON", "Google Sheets credentials"),
        ("GOOGLE_SHEET_ID",         "Google Sheet ID"),
        ("SHIPROCKET_EMAIL",        "Shiprocket email"),
        ("SHIPROCKET_PASSWORD",     "Shiprocket password"),
        ("GEMINI_API_KEY",          "Gemini API key (aistudio.google.com)"),
        ("META_ACCESS_TOKEN",       "Meta access token"),
        ("META_DATASET_ID",         "Meta offline event set ID"),
    ]

    missing = []
    for var_name, description in required_vars:
        if not os.getenv(var_name):
            missing.append(f"{var_name} ({description})")
            log.error(f"❌ {var_name} not set!")
        else:
            log.info(f"✅ {var_name} OK")

    if missing:
        log.error(f"🚨 MISSING {len(missing)} VARIABLES:")
        for m in missing:
            log.error(f"  • {m}")
        return False

    log.info("✅ All environment variables OK!")
    return True

def run_bot():
    """Run bot_enhanced.py"""
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

def is_proc_dead(proc):
    """Safe check — avoids AssertionError when called from non-parent process."""
    try:
        return not proc.is_alive() and proc.exitcode is not None
    except AssertionError:
        return proc.exitcode is not None

def monitor_bot(proc_holder):
    """Monitor bot process and restart if it dies."""
    log.info("👀 Bot monitor started...")
    while True:
        time.sleep(10)
        proc = proc_holder[0]
        try:
            if is_proc_dead(proc):
                log.warning(f"⚠️ BotEnhanced died (exit code {proc.exitcode}) — restarting...")
                try:
                    new_proc = Process(target=run_bot, name="BotEnhanced")
                    new_proc.start()
                    proc_holder[0] = new_proc
                    log.info(f"✅ BotEnhanced restarted (PID: {new_proc.pid})")
                except Exception as e:
                    log.error(f"❌ Restart failed: {e}")
        except Exception as e:
            log.error(f"❌ Monitor error: {e}")

def signal_handler(signum, frame):
    log.info(f"🛑 Signal {signum} received, shutting down...")
    os._exit(0)

def main():
    log.info("=" * 60)
    log.info("🚀 STARTING ONEBOXX BOT")
    log.info("=" * 60)

    if not check_environment():
        log.error("❌ Environment check failed. Exiting.")
        sys.exit(1)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        p = Process(target=run_bot, name="BotEnhanced")
        p.start()
        log.info(f"✅ BotEnhanced started (PID: {p.pid})")

        log.info("=" * 60)
        log.info("🎉 BOT IS RUNNING!")
        log.info("📊 Commands: /orders /report /uploadfb /adsspend /setcreative")
        log.info("⏰ Meta upload auto-runs daily at 11:00 PM IST")
        log.info("=" * 60)

        # Monitor in background
        proc_holder = [p]
        monitor = Process(target=monitor_bot, args=(proc_holder,), name="Monitor")
        monitor.start()

        p.join()

    except KeyboardInterrupt:
        log.info("🛑 Manual shutdown")
    except Exception as e:
        log.error(f"💥 Main error: {e}", exc_info=True)
    finally:
        log.info("👋 Shutting down...")
        try:
            if p.is_alive():
                p.terminate()
        except Exception:
            pass
        time.sleep(2)
        log.info("✅ Bot shut down")
        sys.exit(0)

if __name__ == "__main__":
    main()
