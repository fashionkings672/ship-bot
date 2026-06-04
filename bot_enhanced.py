async def main():
    log.info("Starting bot...")
    get_token()
    log.info("Shiprocket OK")
    refresh_pickups()
    log.info("Syncing from sheets...")
    sync_from_sheets()
    log.info("Sync done")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("adsspend",    cmd_adsspend))
    app.add_handler(CommandHandler("orders",      cmd_orders))
    app.add_handler(CommandHandler("report",      cmd_report))
    app.add_handler(CommandHandler("setcreative", cmd_setcreative))
    app.add_handler(CommandHandler("uploadfb",    cmd_uploadfb))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # ── FIX: start scheduler INSIDE PTB's event loop via post_init ──────────
    scheduler = AsyncIOScheduler(timezone=pytz.timezone("Asia/Kolkata"))

    async def scheduled_upload():
        log.info("Scheduled Meta upload starting...")
        result = run_upload()
        log.info(f"Scheduled upload done: {result[:200]}")

    scheduler.add_job(scheduled_upload, "cron", hour=23, minute=0)

    async def on_startup(application):
        scheduler.start()
        log.info("Scheduler started — Meta upload daily at 11:00 PM IST")

    async def on_shutdown(application):
        if scheduler.running:
            scheduler.shutdown(wait=False)
        log.info("Scheduler stopped")

    app.post_init    = on_startup
    app.post_shutdown = on_shutdown
    # ────────────────────────────────────────────────────────────────────────

    log.info("Bot running...")
    await app.run_polling()


if __name__ == "__main__":
    asyncio.run(main())
