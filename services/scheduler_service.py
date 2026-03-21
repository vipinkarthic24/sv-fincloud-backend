"""
Scheduler Service — Production APScheduler Configuration

Provides:
    - create_scheduler(db_path) → BackgroundScheduler with all jobs configured
    - Repo rate job: daily at 10:00 IST
    - Gold rate job: daily at 10:05 IST
    - Windows Task Scheduler compatible: robust initialization with comprehensive logging
"""
import os
import logging
import uuid
import pytz
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "aws-1-ap-southeast-1.pooler.supabase.com"),
        database=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres.roibmmoznlgtontiinns"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", 6543)),
        sslmode="require",
        cursor_factory=RealDictCursor,
    )
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from services.lending_engine_service import daily_repo_rate_job
from services.gold_rate_service import daily_gold_rate_job
from utils.ist_time import get_ist_now

logger = logging.getLogger(__name__)

# Environment flag to prevent duplicate schedulers in --reload mode
_SCHEDULER_ENV_FLAG = "SV_FINCLOUD_SCHEDULER_RUNNING"


def create_scheduler(db_path: str):
    """
    Create and configure the BackgroundScheduler with all daily jobs.
    
    Enhanced for Windows Task Scheduler compatibility with robust initialization
    and comprehensive logging.

    Returns:
        BackgroundScheduler instance (not yet started), or None if APScheduler
        is not installed or scheduler creation fails.
    """
    
    logger.debug("=== APScheduler Initialization Starting ===")

    # ── Enhanced Reload Safety: More flexible environment flag handling ──
    # Check if scheduler is already running, but allow override in certain contexts
    if os.environ.get(_SCHEDULER_ENV_FLAG) == "1":
        # In Windows Task Scheduler context, we may need to allow scheduler creation
        # even if the flag is set, as the process isolation may not work as expected
        session_name = os.environ.get('SESSIONNAME', '').lower()
        if session_name in ['services', 'console']:
            logger.warning(
                f"Scheduler environment flag is set, but running in {session_name} session. "
                f"Proceeding with scheduler creation for Windows Task Scheduler compatibility."
            )
        else:
            logger.info("Scheduler already running (reload detected) — skipping")
            return None

    # ── APScheduler Import with Enhanced Error Handling ──
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        logger.debug("APScheduler imported successfully")
    except ImportError as e:
        logger.error(
            f"APScheduler not installed: {e}. Scheduled jobs will NOT run. "
            f"Install with: pip install APScheduler"
        )
        return None

    # ── Import Job Functions with Error Handling ──
    try:
        from services.lending_engine_service import daily_repo_rate_job
        from services.gold_rate_service import daily_gold_rate_job
        logger.debug("Job functions imported successfully")
    except ImportError as e:
        logger.error(f"Failed to import job functions: {e}")
        return None

    # ── Enhanced Scheduler Creation with Comprehensive Configuration ──
    try:
        logger.debug("Creating BackgroundScheduler instance...")
        
        scheduler = BackgroundScheduler(
            job_defaults={
                'coalesce': True,           # Merge missed runs into one
                'max_instances': 1,          # Prevent overlapping runs
                'misfire_grace_time': 3600,  # Allow 1 hour late execution
            },
            # Enhanced configuration for Windows Task Scheduler compatibility
            timezone=pytz.timezone("Asia/Kolkata"),  # Explicit timezone to avoid Windows locale issues
        )
        
        logger.debug("BackgroundScheduler instance created successfully")

        # ── Job Registration ──
        logger.debug("Registering daily_repo_rate job...")
        scheduler.add_job(
            daily_repo_rate_job,
            'cron',
            hour=10,
            minute=0,
            args=[str(db_path)],
            id='daily_repo_rate',
            name='Daily Repo Rate Fetch & Recalculate',
            replace_existing=True,
        )
        logger.info("✓ Scheduled: daily_repo_rate at 10:00 IST")

        # Job 2: Daily Gold Rate at 09:05 and 18:05 IST
        logger.debug("Registering daily_gold_rate jobs...")
        
        # Morning update at 09:05
        scheduler.add_job(
            daily_gold_rate_job,
            'cron',
            hour=9,
            minute=5,
            id='daily_gold_rate_morning',
            name='Daily Gold Rate Auto-Update (Morning)',
            replace_existing=True,
        )
        logger.info("✓ Scheduled: daily_gold_rate_morning at 09:05 IST")
        
        # Evening update at 18:05
        scheduler.add_job(
            daily_gold_rate_job,
            'cron',
            hour=18,
            minute=5,
            id='daily_gold_rate_evening',
            name='Daily Gold Rate Auto-Update (Evening)',
            replace_existing=True,
        )
        logger.info("✓ Scheduled: daily_gold_rate_evening at 18:05 IST")

        # ── Job Registration Validation ──
        registered_jobs = scheduler.get_jobs()
        job_ids = [job.id for job in registered_jobs]
        logger.debug(f"Registered {len(registered_jobs)} jobs: {job_ids}")
        
        expected_jobs = ['daily_repo_rate', 'daily_gold_rate_morning', 'daily_gold_rate_evening']
        for expected_job in expected_jobs:
            if expected_job not in job_ids:
                logger.error(f"✗ Job '{expected_job}' NOT registered")
                return None

        os.environ[_SCHEDULER_ENV_FLAG] = "1"
        logger.info("=== APScheduler ready ===")
        return scheduler

    except Exception as e:
        logger.error(f"Failed to create scheduler: {e}", exc_info=True)
        logger.error("=== APScheduler Initialization FAILED ===")
        return None


def shutdown_scheduler(scheduler):
    """Gracefully shut down the scheduler and clear the environment flag."""
    if scheduler:
        try:
            scheduler.shutdown(wait=False)
            logger.info("Scheduler shut down")
        except Exception as e:
            logger.warning(f"Scheduler shutdown error: {e}")
    os.environ.pop(_SCHEDULER_ENV_FLAG, None)


def get_scheduler_status():
    """
    Get the current scheduler status for monitoring and debugging.
    
    Returns:
        dict: Scheduler status information
    """
    return {
        'environment_flag_set': os.environ.get(_SCHEDULER_ENV_FLAG) == "1",
        'environment_context': {
            'USERNAME': os.environ.get('USERNAME'),
            'COMPUTERNAME': os.environ.get('COMPUTERNAME'),
            'SESSIONNAME': os.environ.get('SESSIONNAME'),
            'USERDOMAIN': os.environ.get('USERDOMAIN'),
        },
        'current_working_directory': os.getcwd(),
    }


def validate_scheduler_jobs(scheduler):
    """
    Validate that all expected jobs are registered in the scheduler.
    
    Args:
        scheduler: APScheduler BackgroundScheduler instance
        
    Returns:
        dict: Validation results
    """
    if not scheduler:
        return {
            'valid': False,
            'error': 'Scheduler instance is None'
        }
    
    try:
        jobs = scheduler.get_jobs()
        job_ids = [job.id for job in jobs]
        
        expected_jobs = ['daily_repo_rate', 'daily_gold_rate_morning', 'daily_gold_rate_evening']
        missing_jobs = [job_id for job_id in expected_jobs if job_id not in job_ids]
        unexpected_jobs = [job_id for job_id in job_ids if job_id not in expected_jobs]
        
        validation_result = {
            'valid': len(missing_jobs) == 0,
            'total_jobs': len(jobs),
            'expected_jobs': expected_jobs,
            'registered_jobs': job_ids,
            'missing_jobs': missing_jobs,
            'unexpected_jobs': unexpected_jobs,
            'job_details': []
        }
        
        # Add detailed job information
        for job in jobs:
            job_detail = {
                'id': job.id,
                'name': job.name,
                'next_run_time': str(job.next_run_time) if job.next_run_time else None,
                'trigger': str(job.trigger),
                'func_name': job.func.__name__ if hasattr(job.func, '__name__') else str(job.func)
            }
            validation_result['job_details'].append(job_detail)
        
        return validation_result
        
    except Exception as e:
        return {
            'valid': False,
            'error': f'Job validation failed: {str(e)}'
        }


def log_job_execution_start(job_name: str, db_path: str):
    """
    Log the start of a scheduled job execution.
    
    Args:
        job_name: Name of the job being executed
        db_path: Database path for logging
    """
    logger.debug(f"=== JOB EXECUTION START: {job_name} ===")
    logger.debug(f"Execution time: {get_ist_now().isoformat()}")
    logger.debug("Database: PostgreSQL (Supabase)")
    
    # Log to database for history tracking
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        # Create job execution log table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_execution_log (
                id TEXT PRIMARY KEY,
                job_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                error_message TEXT,
                execution_time_seconds REAL
            )
        ''')

        # Insert start record
        log_id = str(uuid.uuid4())
        cursor.execute('''
            INSERT INTO job_execution_log (id, job_name, status, started_at)
            VALUES (%s, %s, 'started', %s)
        ''', (log_id, job_name, get_ist_now().isoformat()))

        conn.commit()
        cursor.close()
        conn.close()
        return log_id

    except psycopg2.Error as e:
        logger.error(f"Scheduler log insert failed: {e}")
        try:
            conn.rollback()
            cursor.close()
            conn.close()
        except Exception:
            pass
        return None
    except Exception as e:
        logger.error(f"Failed to log job execution start: {e}")
        return None


def log_job_execution_end(log_id: str, job_name: str, db_path: str, success: bool, error_message: str = None):
    """
    Log the end of a scheduled job execution.
    
    Args:
        log_id: ID from log_job_execution_start
        job_name: Name of the job
        db_path: Database path for logging
        success: Whether the job completed successfully
        error_message: Error message if job failed
    """
    status = 'completed' if success else 'failed'
    logger.info(f"=== JOB EXECUTION END: {job_name} - {status.upper()} ===")
    
    if not log_id:
        return
    
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        # Get start time to calculate execution duration
        cursor.execute('SELECT started_at FROM job_execution_log WHERE id = %s', (log_id,))
        start_record = cursor.fetchone()

        execution_time = None
        if start_record:
            start_time = datetime.fromisoformat(start_record['started_at'].replace('Z', '+00:00'))
            end_time = get_ist_now()
            execution_time = (end_time - start_time).total_seconds()

        # Update completion record
        cursor.execute('''
            UPDATE job_execution_log 
            SET status = %s, completed_at = %s, error_message = %s, execution_time_seconds = %s
            WHERE id = %s
        ''', (status, get_ist_now().isoformat(), error_message, execution_time, log_id))

        conn.commit()
        cursor.close()
        conn.close()

        if execution_time:
            logger.debug(f"Job execution time: {execution_time:.2f} seconds")

    except psycopg2.Error as e:
        logger.error(f"Scheduler log update failed: {e}")
        try:
            conn.rollback()
            cursor.close()
            conn.close()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Failed to log job execution end: {e}")

def run_missed_jobs_on_startup(db_path: str):
    """
    Run scheduled jobs if today's execution was missed.
    Updated: Only runs missed jobs if it's past 10:10 AM IST to ensure 
    fresh market data is available.
    """
    logger.debug("Checking for missed scheduled jobs...")

    # Get current time in IST
    tz = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(tz)
    today = now_ist.date()

    conn = get_pg_connection()
    cursor = conn.cursor()

    # ✅ Ensure log table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_execution_log (
            id TEXT PRIMARY KEY,
            job_name TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            error_message TEXT,
            execution_time_seconds REAL
        )
    """)

    # Check what has already run today
    cursor.execute("""
        SELECT job_name FROM job_execution_log WHERE started_at::date = %s
    """, (today.isoformat(),))

    executed_jobs = [r['job_name'] for r in cursor.fetchall()]

    # ONLY run missed jobs if it is currently PAST 10:10 AM IST
    if (now_ist.hour > 10) or (now_ist.hour == 10 and now_ist.minute >= 10):
        if "daily_repo_rate" not in executed_jobs:
            logger.info("Repo rate job missed today — running now")
            daily_repo_rate_job(db_path)

        # Check for missed gold rate jobs (morning or evening)
        gold_jobs_executed = any(job in executed_jobs for job in ["daily_gold_rate_morning", "daily_gold_rate_evening"])
        if not gold_jobs_executed:
            logger.info("Gold rate job missed today — running now")
            daily_gold_rate_job()
    else:
        logger.info(f"Current time {now_ist.strftime('%H:%M')} is before 10:10 AM. "
                    "Skipping missed job check; scheduler will handle it later.")

    cursor.close()
    conn.close()