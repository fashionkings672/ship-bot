"""
start_all_bots.py - Run all Oneboxx bots together
Handles:
- bot.py
- bot_enhanced.py
- meta_bot.py
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
    print("❌ Missing nest_asyncio. pip install nest-asyncio")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("all_bots.log", mode='a', encoding='utf-8')]
)
log = logging.getLogger("ALL_BOTS")

manager = Manager()
bot_status = manager.dict()

def check_environment():
    log.info("🔍 Checking environment...")
    required = [
        ("BOT_TOKEN", "Shipping Bot 1"),
        ("BOT_TOKEN_2", "Main Shipping Bot"),
        ("META_BOT_TOKEN", "Meta Bot Token"),
        ("GOOGLE_CREDENTIALS_JSON", "Google Credentials"),
        ("GOOGLE_SHEET_ID", "Google Sheet ID"),
        ("SHIPROCKET_EMAIL", "Shiprocket Email"),
        ("SHIPROCKET_PASSWORD", "Shiprocket Password"),
        ("OPENAI_API_KEY", "OpenAI Key"),
        ("META_ACCESS_TOKEN", "Meta Access Token"),
        ("META_DATASET_ID", "Meta Dataset ID"),
        ("ADMIN_CHAT_ID", "Admin Chat ID"),
    ]
    missing = [f"{v[0]} ({v[1]})" for v in required if not os.getenv(v[0])]
    
    if missing:
        log.error(f"🚨 Missing variables: {missing}")
        return False
    log.info("✅ Environment OK")
    return True

def run_bot1():
    name = "Bot1"
    try:
        log.info(f"🚀 Starting {name}")
        bot_status[name] = "running"
        import bot
        asyncio.run(bot.main())
    except Exception as e:
        log.error(f"💥 {name} crashed", exc_info=True)

def run_bot2():
    name = "BotEnhanced"
    try:
        log.info(f"🚀 Starting {name}")
        bot_status[name] = "running"
        import bot_enhanced
        asyncio.run(bot_enhanced.main())
    except Exception as e:
        log.error(f"💥 {name} crashed", exc_info=True)

def run_meta_bot():
    name = "MetaBot"
    try:
        log.info(f"🚀 Starting {name}")
        bot_status[name] = "running"
        import meta_bot
        asyncio.run(meta_bot.main())
    except Exception as e:
        log.error(f"💥 {name} crashed", exc_info=True)

def monitor_bots(processes):
    while True:
        time.sleep(5)
        for i, proc in enumerate(processes):
            if not proc.is_alive():
                log.warning(f"⚠️ {proc.name} died. Restarting...")
                new_proc = Process(target=proc._target, name=proc.name)
                new_proc.start()
                processes[i] = new_proc

def signal_handler(signum, frame):
    log.info("🛑 Shutting down...")
    os._exit(0)

def main():
    log.info("=" * 70)
    log.info("🚀 STARTING ONEBOXX BOT SUITE (Report Bot Removed)")
    log.info("=" * 70)

    if not check_environment():
        sys.exit(1)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    processes = []

    for target, name in [(run_bot1, "Bot1"), (run_bot2, "BotEnhanced"), (run_meta_bot, "MetaBot")]:
        p = Process(target=target, name=name)
        p.start()
        processes.append(p)
        log.info(f"✅ {name} started (PID: {p.pid})")
        time.sleep(2)

    log.info("=" * 70)
    log.info("🎉 ALL BOTS RUNNING:")
    log.info("   • Bot1")
    log.info("   • BotEnhanced")
    log.info("   • MetaBot (Daily Meta Upload @ 11:00 PM IST)")
    log.info("=" * 70)

    monitor = Process(target=monitor_bots, args=(processes,))
    monitor.start()

    for p in processes:
        p.join()

if __name__ == "__main__":
    main()
