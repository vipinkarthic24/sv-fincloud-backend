"""
Gold Rate Service — Market Rate Fetching & Auto-Update

Handles:
    - fetch_gold_market_rate()  → Fetch 22K gold rate from MetalPriceAPI
    - daily_gold_rate_job()     → Scheduled job for auto-updating gold rate
"""

import requests
import os
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import uuid
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from utils.ist_time import get_ist_now

load_dotenv()

logger = logging.getLogger(__name__)

# Lazy-initialized connection pool
_service_pool = None


def init_service_pool():
    global _service_pool
    if _service_pool is None:
        _service_pool = SimpleConnectionPool(
            1, 5,
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", 6543)),
            database=os.getenv("DB_NAME", "postgres"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            sslmode="require",
            cursor_factory=RealDictCursor,
        )
        logger.info("Gold rate service pool initialized")


def get_pg_connection():
    init_service_pool()
    return _service_pool.getconn()


def release_pg_connection(conn):
    if _service_pool is not None:
        _service_pool.putconn(conn)


# ==============================
# 1. FETCH GOLD MARKET RATE
# ==============================

def fetch_gold_market_rate(tenant_id: str | None = None) -> float | None:
    """
    Fetch the latest 22K gold rate (₹ per gram) from gold-api.com.
    API returns { "price": <24K gold price per gram in INR> }.
    Falls back to the most recent stored auto rate for the tenant on failure.
    Returns None only if no rate exists at all.
    """
    logger.debug("Fetching gold market rate from API...")
    try:
        url = "https://api.gold-api.com/price/XAU"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()

        if "price" not in data:
            raise ValueError("API response missing 'price' field")

        # API returns 24K gold price per gram in INR directly — no USD/ounce conversion needed
        gold_24k_per_gram = float(data["price"])
        gold_22k_per_gram = gold_24k_per_gram * (22 / 24) * 3.2785
        gold_rate = round(gold_22k_per_gram, 2)

        logger.info("Gold API returned 24K ₹%s → Converted 22K ₹%s", gold_24k_per_gram, gold_rate)
        return gold_rate

    except requests.exceptions.Timeout:
        logger.warning("Gold rate API timed out (5s)")
    except requests.exceptions.RequestException as e:
        logger.warning("Gold rate API request failed: %s", e)
    except (KeyError, ValueError, TypeError) as e:
        logger.warning("Gold rate API returned unexpected data: %s", e)

    # Fallback: latest stored auto rate for this tenant
    logger.debug("Falling back to latest stored auto rate from database...")
    conn = None
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        if tenant_id:
            cursor.execute("""
                SELECT rate_per_gram FROM gold_rate
                WHERE tenant_id = %s AND source = 'auto'
                ORDER BY updated_at DESC LIMIT 1
            """, (tenant_id,))
        else:
            cursor.execute("""
                SELECT rate_per_gram FROM gold_rate
                WHERE source = 'auto'
                ORDER BY updated_at DESC LIMIT 1
            """)

        result = cursor.fetchone()
        cursor.close()

        if result:
            fallback_rate = round(result['rate_per_gram'], 2)
            logger.debug("Using fallback auto rate from database: ₹%.2f/gram", fallback_rate)
            return fallback_rate
        else:
            logger.warning("No fallback auto rate found in database")
    except Exception as db_error:
        logger.error("Failed to fetch fallback rate from database: %s", db_error)
    finally:
        if conn:
            release_pg_connection(conn)

    logger.warning("Could not fetch gold rate from API or database")
    return None


# ==============================
# 2. BRANCH-SPECIFIC GOLD RATE RETRIEVAL
# ==============================

def get_gold_rate_for_branch(db_path: str, tenant_id: str, branch_id: str | None, source: str) -> float | None:
    """
    Fetch gold rate for a specific branch with automatic fallback to global rate.
    db_path is kept for signature compatibility but unused (PostgreSQL pool is used).
    """
    conn = None
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        if branch_id:
            cursor.execute("""
                SELECT rate_per_gram FROM gold_rate
                WHERE tenant_id = %s AND branch_id = %s AND source = %s
                ORDER BY updated_at DESC LIMIT 1
            """, (tenant_id, branch_id, source))
            result = cursor.fetchone()
            if result:
                rate = round(result["rate_per_gram"], 2)
                logger.debug("Gold rate resolved: tenant=%s branch=%s mode=%s rate=%.2f", tenant_id, branch_id, source, rate)
                cursor.close()
                return rate
            logger.debug("No branch-specific rate for branch '%s', falling back to global", branch_id)

        cursor.execute("""
            SELECT rate_per_gram FROM gold_rate
            WHERE tenant_id = %s AND branch_id IS NULL AND source = %s
            ORDER BY updated_at DESC LIMIT 1
        """, (tenant_id, source))
        result = cursor.fetchone()
        cursor.close()

        if result:
            rate = round(result["rate_per_gram"], 2)
            logger.debug("Gold rate resolved (global fallback): tenant=%s mode=%s rate=%.2f", tenant_id, source, rate)
            return rate

        logger.warning("No gold rate found for tenant '%s' with source '%s'", tenant_id, source)
        return None

    except psycopg2.Error as e:
        logger.error("Database error while fetching gold rate: %s", e)
        return None
    except Exception as e:
        logger.error("Unexpected error while fetching gold rate: %s", e)
        return None
    finally:
        if conn:
            release_pg_connection(conn)


# ==============================
# 3. DAILY GOLD RATE JOB
# ==============================

def daily_gold_rate_job():
    logger.info("=== Daily Gold Rate Job Starting ===")
    conn = None
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        gold_rate = fetch_gold_market_rate()
        if not gold_rate:
            logger.error("Gold API failed")
            return
        # Fetch only tenants that have gold_rate_settings configured (single query, no N+1)
        cursor.execute("""
            SELECT t.id AS tenant_id, grs.mode
            FROM tenants t
            JOIN gold_rate_settings grs ON grs.tenant_id = t.id
        """)
        tenant_rows = cursor.fetchall()
        if not tenant_rows:
            logger.info("No tenants with gold_rate_settings found — nothing to update")
            return
        current_time = get_ist_now()
        for row in tenant_rows:
            tenant_id = row["tenant_id"]
            mode = row["mode"]
            if mode != 'auto':
                logger.debug("Tenant %s is in manual mode — skipping auto rate insert", tenant_id)
                continue
            cursor.execute("""
                SELECT rate_per_gram
                FROM gold_rate
                WHERE tenant_id = %s AND source = 'auto'
                ORDER BY updated_at DESC
                LIMIT 1
            """, (tenant_id,))

            last = cursor.fetchone()
            last_rate = last["rate_per_gram"] if last else None
            if last_rate and abs(last_rate - gold_rate) < 0.1:
                logger.debug("Tenant %s rate unchanged — skipping", tenant_id)
                continue
            new_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO gold_rate (id, tenant_id, branch_id, rate_per_gram, updated_at, source)
                VALUES (%s, %s, NULL, %s, %s, 'auto')
            """, (new_id, tenant_id, gold_rate, current_time))
            logger.info("Inserted gold rate ₹%s for tenant %s", gold_rate, tenant_id)
        conn.commit()
        logger.info("=== Gold Rate Update Completed ===")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error("Gold rate job failed: %s", e)

    finally:
        if conn:
            release_pg_connection(conn)


# ==============================
# 4. DATABASE HEALTH CHECK
# ==============================

def verify_database_health() -> bool:
    """Verify database connectivity and required tables."""
    conn = None
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        for table in ['gold_rate', 'gold_rate_settings']:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables WHERE table_name = %s
                )
            """, (table,))
            if not cursor.fetchone()['exists']:
                logger.error("Required table '%s' not found", table)
                cursor.close()
                return False

        cursor.execute("SELECT COUNT(*) as cnt FROM gold_rate")
        logger.debug("Gold rate table: %d records", cursor.fetchone()['cnt'])
        cursor.close()
        logger.debug("Database health check passed")
        return True

    except Exception as e:
        logger.error("Database health check failed: %s", e)
        return False
    finally:
        if conn:
            release_pg_connection(conn)


# ==============================
# 5. STANDALONE ENTRY POINT
# ==============================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    daily_gold_rate_job()
