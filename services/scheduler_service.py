"""
Scheduler Service — Production APScheduler Configuration

Provides:
    - create_scheduler(db_path) → BackgroundScheduler with all jobs configured
    - Repo rate job: daily at 06:00
    - Gold rate job: daily at 06:05
    - Reload-safe: uses environment flag to prevent duplicate schedulers
"""

import os
import logging

logger = logging.getLogger(__name__)

# Environment flag to prevent duplicate schedulers in --reload mode
_SCHEDULER_ENV_FLAG = "SV_FINCLOUD_SCHEDULER_RUNNING"


def create_scheduler(db_path: str):
    """
    Create and configure the BackgroundScheduler with all daily jobs.

    Returns:
        BackgroundScheduler instance (not yet started), or None if APScheduler
        is not installed or scheduler is already running (reload guard).
    """

    # ── Reload safety: skip if scheduler already running in this process group ──
    if os.environ.get(_SCHEDULER_ENV_FLAG) == "1":
        logger.info("Scheduler already running (reload detected) — skipping")
        return None

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        logger.warning(
            "APScheduler not installed. Scheduled jobs will NOT run. "
            "Install with: pip install APScheduler"
        )
        return None

    # Import job functions
    from services.lending_engine_service import daily_repo_rate_job
    from services.gold_rate_service import daily_gold_rate_job

    try:
        scheduler = BackgroundScheduler(
            job_defaults={
                'coalesce': True,           # Merge missed runs into one
                'max_instances': 1,          # Prevent overlapping runs
                'misfire_grace_time': 3600,  # Allow 1 hour late execution
            }
        )

        # ── Job 1: Daily Repo Rate at 06:00 ──
        scheduler.add_job(
            daily_repo_rate_job,
            'cron',
            hour=6,
            minute=0,
            args=[str(db_path)],
            id='daily_repo_rate',
            name='Daily Repo Rate Fetch & Recalculate',
            replace_existing=True,
        )
        logger.info("Scheduled: daily_repo_rate at 06:00")

        # ── Job 2: Daily Gold Rate at 06:05 ──
        scheduler.add_job(
            daily_gold_rate_job,
            'cron',
            hour=6,
            minute=5,
            args=[str(db_path)],
            id='daily_gold_rate',
            name='Daily Gold Rate Auto-Update',
            replace_existing=True,
        )
        logger.info("Scheduled: daily_gold_rate at 06:05")

        # Set environment flag to prevent duplicates on reload
        os.environ[_SCHEDULER_ENV_FLAG] = "1"

        return scheduler

    except Exception as e:
        logger.error(f"Failed to create scheduler: {e}", exc_info=True)
        return None


def shutdown_scheduler(scheduler):
    """Gracefully shut down the scheduler and clear the environment flag."""
    if scheduler:
        try:
            scheduler.shutdown(wait=False)
            logger.info("Scheduler shut down successfully")
        except Exception as e:
            logger.warning(f"Scheduler shutdown error: {e}")

    # Clear the flag so next startup can schedule again
    os.environ.pop(_SCHEDULER_ENV_FLAG, None)
