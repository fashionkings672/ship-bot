"""
main.py — Run Dappers (bot_enhanced) and Backbenchers (bot2) simultaneously
"""
import asyncio
import logging
import sys
import time
from multiprocessing import Process

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("MAIN")


def run_bot_enhanced():
    import bot_enhanced
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        log.info("▶ Dappers starting...")
        loop.run_until_complete(bot_enhanced.main())
    except Exception as e:
        log.error(f"💥 Dappers crashed: {e}", exc_info=True)
    finally:
        loop.close()


def run_bot2():
    import bot2
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        log.info("▶ Backbenchers starting...")
        loop.run_until_complete(bot2.main())
    except Exception as e:
        log.error(f"💥 Backbenchers crashed: {e}", exc_info=True)
    finally:
        loop.close()


if __name__ == "__main__":
    log.info("🚀 Starting both bots...")

    p1 = Process(target=run_bot_enhanced, name="Dappers", daemon=True)
    p2 = Process(target=run_bot2, name="Backbenchers", daemon=True)

    p1.start()
    log.info(f"✅ Dappers started (PID {p1.pid})")

    time.sleep(3)

    p2.start()
    log.info(f"✅ Backbenchers started (PID {p2.pid})")

    p1.join()
    p2.join()

    log.info("👋 Both bots exited.")
