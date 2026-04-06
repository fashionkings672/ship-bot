"""
start_all_bots.py - Run all Oneboxx bots together
Handles:
- bot.py (first shipping bot)
- bot_enhanced.py (main shipping bot)
- report_bot.py (reporting bot)
"""

import asyncio
import logging
from multiprocessing import Process, Manager
import os
import signal
import sys
import time

# Apply nest_asyncio to handle nested event loops
try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    print("❌ Missing nest_asyncio. Install with: pip install nest-asyncio")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("all_bots.log", mode='a', encoding='utf-8')
    ]
)
log = logging.getLogger("ALL_BOTS")

# Shared manager for inter-process communication
manager = Manager()
bot_status = manager.dict()

def check_environment():
    """Verify required environment variables exist."""
    log.info("🔍 Checking environment setup...")
    
    # Required variables
    required_vars = [
        ("BOT_TOKEN", "First shipping bot token"),
        ("BOT_TOKEN_2", "Main shipping bot token"),
        ("REPORT_BOT_TOKEN", "Report bot token"),
        ("GOOGLE_CREDENTIALS_JSON", "Google Sheets credentials"),
        ("GOOGLE_SHEET_ID", "Google Sheet ID"),
        ("SHIPROCKET_EMAIL", "Shiprocket email"),
        ("SHIPROCKET_PASSWORD", "Shiprocket password"),
        ("OPENAI_API_KEY", "OpenAI API key")
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
        
        # Import here to catch any import errors
        import bot
        
        log.info(f"✅ {name} imported successfully")
        bot_status[name] = "running"
        
        # Run the bot
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
        
        # Import here to catch any import errors
        import bot_enhanced
        
        log.info(f"✅ {name} imported successfully")
        bot_status[name] = "running"
        
        # Run the bot
        asyncio.run(bot_enhanced.main())
        
        bot_status[name] = "stopped"
        log.info(f"🛑 {name} stopped")
        
    except Exception as e:
        log.error(f"💥 {name} CRASHED: {e}", exc_info=True)
        bot_status[name] = f"error: {str(e)}"
        raise

def run_bot3():
    """Run report bot (report_bot.py)"""
    name = "ReportBot"
    try:
        log.info(f"🚀 Starting {name}...")
        bot_status[name] = "starting"
        
        # Check critical dependencies before importing
        try:
            import apscheduler
            import gspread
            import facebook_business
            log.info("✅ Report Bot dependencies available")
        except ImportError as e:
            log.error(f"❌ Report Bot dependency missing: {e}")
            bot_status[name] = f"missing_dependency: {str(e)}"
            raise
            
        # Import after dependency check
        import report_bot
        
        log.info(f"✅ {name} imported successfully")
        bot_status[name] = "running"
        
        # Run the bot
        asyncio.run(report_bot.main())
        
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
        
        # Check each process
        for i, proc in enumerate(processes):
            if not proc.is_alive() and proc.exitcode is not None:
                log.warning(f"⚠️ Bot {i+1} ({proc.name}) died with exit code {proc.exitcode}")
                
                # Try to restart
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
    """Main function to start all bots."""
    log.info("=" * 60)
    log.info("🚀 STARTING ALL ONEBOXX BOTS")
    log.info("=" * 60)
    
    # Check environment
    if not check_environment():
        log.error("❌ Environment check failed. Exiting.")
        sys.exit(1)
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start bots
    processes = []
    try:
        # Start Bot 1
        p1 = Process(target=run_bot1, name="Bot1")
        p1.start()
        processes.append(p1)
        log.info(f"✅ Bot1 started (PID: {p1.pid})")
        time.sleep(2)
        
        # Start Bot 2
        p2 = Process(target=run_bot2, name="BotEnhanced")
        p2.start()
        processes.append(p2)
        log.info(f"✅ BotEnhanced started (PID: {p2.pid})")
        time.sleep(2)
        
        # Start Report Bot
        p3 = Process(target=run_bot3, name="ReportBot")
        p3.start()
        processes.append(p3)
        log.info(f"✅ ReportBot started (PID: {p3.pid})")
        
        log.info("=" * 60)
        log.info("🎉 ALL 3 BOTS ARE RUNNING!")
        log.info("📊 Report Bot commands:")
        log.info("/notpicked - Not picked up orders")
        log.info("/cancelled - Cancelled orders")
        log.info("/creative 7 - Creative performance")
        log.info("/ads 7 - Facebook Ads performance")
        log.info("/finance 7 - Financial report")
        log.info("/ai - AI daily briefing")
        log.info("=" * 60)
        
        # Start monitor
        monitor = Process(target=monitor_bots, args=(processes,))
        monitor.start()
        
        # Wait for all processes
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
