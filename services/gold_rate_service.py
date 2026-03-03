"""
Gold Rate Service — Market Rate Fetching & Auto-Update

Handles:
    - fetch_gold_market_rate()     → Fetch 22K gold rate from MetalPriceAPI
    - daily_gold_rate_job(db_path) → Scheduled job for auto-updating gold rate
"""

import requests
import sqlite3
import uuid
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# MetalPriceAPI key
GOLD_API_KEY = "317a568eff50a6ea3b384bb1364d5079"


# ==============================
# 1. FETCH GOLD MARKET RATE
# ==============================

def fetch_gold_market_rate() -> float | None:
    """
    Fetch the latest 22K gold rate (₹ per gram) from MetalPriceAPI.
    Returns None if the API call fails.
    """
    try:
        url = f"https://api.metalpriceapi.com/v1/latest?api_key={GOLD_API_KEY}&base=INR&currencies=XAU"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        gold_per_ounce = data["rates"]["INRXAU"]
        gold_24k = gold_per_ounce / 31.1035
        gold_22k = gold_24k * (22 / 24) * 1.05  # 22K with 5% premium

        logger.info(f"Fetched 22K gold rate: ₹{gold_22k:.2f}/gram")
        return round(gold_22k, 2)

    except requests.exceptions.Timeout:
        logger.warning("Gold rate API timed out")
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"Gold rate API request failed: {e}")
        return None
    except (KeyError, ValueError, TypeError) as e:
        logger.warning(f"Gold rate API returned unexpected data: {e}")
        return None


# ==============================
# 2. DAILY GOLD RATE JOB
# ==============================

def daily_gold_rate_job(db_path: str):
    """
    Daily scheduled job for gold rate auto-update:
    1. Check if mode is 'auto' — skip if manual
    2. Fetch market rate from API
    3. Compare with last auto rate — skip if unchanged (< ₹0.10 difference)
    4. Insert new rate with source='auto'

    Manual overrides (source='manual') are never touched.
    """
    logger.info("Running daily gold rate job...")

    conn = None
    try:
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        cursor = conn.cursor()

        # ── Step 1: Check mode ──
        cursor.execute("SELECT mode FROM gold_rate_settings WHERE id = 1")
        row = cursor.fetchone()
        mode = row[0] if row else "manual"

        if mode != "auto":
            logger.info("Gold rate mode is 'manual' — skipping auto update")
            return

        # ── Step 2: Fetch market rate ──
        gold_rate = fetch_gold_market_rate()
        if gold_rate is None:
            logger.warning("Could not fetch gold market rate — skipping update")
            return

        # ── Step 3: Compare with last auto rate ──
        cursor.execute("""
            SELECT rate_per_gram
            FROM gold_rate
            WHERE source='auto'
            ORDER BY updated_at DESC
            LIMIT 1
        """)
        last = cursor.fetchone()

        if last:
            difference = abs(round(last[0], 2) - round(gold_rate, 2))
        else:
            difference = None

        # ── Step 4: Insert only if changed significantly ──
        if not last or difference is None or difference > 0.10:
            cursor.execute("""
                INSERT INTO gold_rate (id, branch_id, rate_per_gram, updated_at, source)
                VALUES (?, NULL, ?, ?, 'auto')
            """, (
                str(uuid.uuid4()),
                gold_rate,
                datetime.now(timezone.utc).isoformat()
            ))
            conn.commit()
            logger.info(f"Gold rate auto-updated to ₹{gold_rate:.2f}/gram")
        else:
            logger.info(f"Gold rate unchanged (₹{gold_rate:.2f}) — skipping insert")

    except Exception as e:
        logger.error(f"Daily gold rate job failed: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


# ==============================
# 3. STANDALONE ENTRY POINT
# ==============================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    daily_gold_rate_job("sv_fincloud.db")
