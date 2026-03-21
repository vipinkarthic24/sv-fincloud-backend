"""
Lending Engine Service — Dynamic Bank-Style Interest & Tenure Calculation

Interest Formula:
    FINAL RATE = Repo Rate + Base Spread + Risk Premium

This module provides:
    - fetch_repo_rate()           → Fetch RBI repo rate (with fallback)
    - calculate_max_tenure()      → Risk-based max tenure enforcement
    - calculate_interest_rate()   → Dynamic interest calculation
    - recalculate_interest_rates()→ Batch update interest_rates table
"""

import requests
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from utils.ist_time import get_ist_now

load_dotenv()

logger = logging.getLogger(__name__)


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

# ==============================
# CONSTANTS
# ==============================

DEFAULT_REPO_RATE = 6.5  # Fallback if API unavailable

# Base Spread per loan type (added on top of repo rate)
BASE_SPREAD = {
    "gold_loan": 2.0,
    "vehicle_loan": 4.0,
    "personal_loan": 6.0,
}

# ==============================
# 1. FETCH REPO RATE
# ==============================

def fetch_repo_rate() -> float:
    """
    Fetch the latest RBI Repo Rate from an external API.
    Falls back to DEFAULT_REPO_RATE (6.5%) if API is unavailable.

    To use a real API, replace the URL and parsing logic below.
    Example paid APIs: TradingEconomics, RBI Data Portal, etc.
    """
    try:
        # --- STUB: Replace with your real API call ---
        # url = "https://api.tradingeconomics.com/country/india/indicator/interest-rate"
        # response = requests.get(url, params={"c": YOUR_API_KEY}, timeout=10)
        # data = response.json()
        # repo_rate = float(data[0]["Value"])
        # return repo_rate
        # --- END STUB ---

        # For now, return the current RBI repo rate (update manually or via API)
        logger.debug(f"Using default repo rate: {DEFAULT_REPO_RATE}%")
        return DEFAULT_REPO_RATE

    except Exception as e:
        logger.warning(f"Failed to fetch repo rate: {e}. Using fallback {DEFAULT_REPO_RATE}%")
        return DEFAULT_REPO_RATE


# ==============================
# 2. RISK-BASED MAX TENURE
# ==============================

def calculate_max_tenure(loan_type: str, inputs: dict) -> int:
    """
    Calculate the maximum allowed tenure (in months) based on risk factors.

    Args:
        loan_type: "personal_loan", "vehicle_loan", or "gold_loan"
        inputs: dict with keys depending on loan type:
            - personal_loan: {"cibil_score": int}
            - vehicle_loan:  {"vehicle_age": int}
            - gold_loan:     {"loan_amount": float}

    Returns:
        Maximum tenure in months, or -1 if application should be rejected.
    """

    if loan_type == "personal_loan":
        cibil = inputs.get("cibil_score", 0)
        if cibil >= 750:
            return 24
        elif cibil >= 700:
            return 18
        elif cibil >= 650:
            return 12
        else:
            return -1  # Reject

    elif loan_type == "vehicle_loan":
        vehicle_age = inputs.get("vehicle_age", 0)
        if vehicle_age <= 3:
            return 24
        elif vehicle_age <= 6:
            return 18
        else:
            return 12

    elif loan_type == "gold_loan":
        loan_amount = inputs.get("loan_amount", 0)
        if loan_amount <= 50000:
            return 24
        elif loan_amount <= 200000:
            return 18
        else:
            return 12

    # Unknown loan type — default
    return 24


# ==============================
# 3. DYNAMIC INTEREST CALCULATION
# ==============================

def _get_risk_premium(loan_type: str, inputs: dict) -> float:
    """
    Calculate the risk premium based on loan type and borrower risk factors.

    Returns:
        Risk premium as a percentage (e.g., 1.0 = 1%)
    """

    if loan_type == "personal_loan":
        cibil = inputs.get("cibil_score", 0)
        if cibil >= 750:
            return -0.5
        elif cibil >= 700:
            return 0.0
        elif cibil >= 650:
            return 1.0
        else:
            return 2.0  # Should already be rejected by tenure check

    elif loan_type == "vehicle_loan":
        vehicle_age = inputs.get("vehicle_age", 0)
        if vehicle_age <= 3:
            return 0.0
        elif vehicle_age <= 6:
            return 1.0
        else:
            return 2.5

    elif loan_type == "gold_loan":
        loan_amount = inputs.get("loan_amount", 0)
        if loan_amount <= 50000:
            return 0.0
        elif loan_amount <= 200000:
            return 0.5
        else:
            return 1.0

    return 0.0


def calculate_interest_rate(loan_type: str, inputs: dict, repo_rate: float = None) -> dict:
    """
    Calculate the dynamic interest rate using the formula:
        FINAL RATE = Repo Rate + Base Spread + Risk Premium

    Args:
        loan_type: "personal_loan", "vehicle_loan", or "gold_loan"
        inputs: Risk factor inputs (cibil_score, vehicle_age, loan_amount)
        repo_rate: Current repo rate (fetched externally). If None, uses default.

    Returns:
        dict with:
            - "rate": final interest rate (float, e.g. 12.5)
            - "repo_rate": repo rate used
            - "base_spread": base spread applied
            - "risk_premium": risk premium applied
            - "source": "dynamic"
    """
    if repo_rate is None:
        repo_rate = DEFAULT_REPO_RATE

    base_spread = BASE_SPREAD.get(loan_type, 6.0)
    risk_premium = _get_risk_premium(loan_type, inputs)

    final_rate = round(repo_rate + base_spread + risk_premium, 2)

    return {
        "rate": final_rate,
        "repo_rate": repo_rate,
        "base_spread": base_spread,
        "risk_premium": risk_premium,
        "source": "dynamic",
    }


# ==============================
# 4. RECALCULATE INTEREST RATES TABLE
# ==============================

def recalculate_interest_rates(db_path: str, repo_rate: float = None, tenant_id: str = None):
    """
    Recalculate all entries in the interest_rates table using the dynamic formula.
    This is triggered when the repo rate changes.

    Rows with is_overridden = 1 are SKIPPED (admin manual overrides are preserved).
    If tenant_id is provided, only recalculate for that tenant.
    """
    if repo_rate is None:
        repo_rate = fetch_repo_rate()

    # Define all category → inputs mappings
    rate_configs = [
        # Personal Loan
        ("personal_loan", "cibil_750_plus",  {"cibil_score": 750}),
        ("personal_loan", "cibil_700_749",   {"cibil_score": 725}),
        ("personal_loan", "cibil_650_699",   {"cibil_score": 675}),

        # Vehicle Loan
        ("vehicle_loan", "age_0_3",   {"vehicle_age": 1}),
        ("vehicle_loan", "age_4_6",   {"vehicle_age": 5}),
        ("vehicle_loan", "age_7_plus", {"vehicle_age": 8}),

        # Gold Loan
        ("gold_loan", "standard",     {"loan_amount": 50000}),
    ]

    conn = get_pg_connection()
    cursor = conn.cursor()

    # If tenant_id provided, recalculate only for that tenant
    # Otherwise recalculate for all tenants (backwards compat)
    if tenant_id:
        tenant_ids = [tenant_id]
    else:
        cursor.execute("SELECT id FROM tenants")
        tenant_ids = [r['id'] for r in cursor.fetchall()]
        if not tenant_ids:
            tenant_ids = [None]  # fallback for old data without tenant_id

    updated = 0
    skipped = 0

    for tid in tenant_ids:
        for loan_type, category, inputs in rate_configs:
            result = calculate_interest_rate(loan_type, inputs, repo_rate)
            new_rate = result["rate"]

            # Check if category exists for this tenant and whether it's overridden
            if tid:
                cursor.execute("SELECT id, is_overridden FROM interest_rates WHERE tenant_id = %s AND loan_type = %s AND category = %s",
                    (tid, loan_type, category)
                )
            else:
                cursor.execute("SELECT id, is_overridden FROM interest_rates WHERE loan_type = %s AND category = %s",
                    (loan_type, category)
                )
            existing = cursor.fetchone()

            if existing:
                if existing['is_overridden'] == 1:
                    skipped += 1
                    continue
                cursor.execute("UPDATE interest_rates SET rate = %s, source = 'auto' WHERE id = %s",
                    (new_rate, existing['id'])
                )
            else:
                cursor.execute("INSERT INTO interest_rates (id, tenant_id, loan_type, category, rate, is_overridden, source, created_at) VALUES (%s, %s, %s, %s, %s, 0, 'auto', %s)",
                    (str(uuid.uuid4()), tid, loan_type, category, new_rate, get_ist_now().isoformat())
                )

            updated += 1

    conn.commit()
    cursor.close()
    conn.close()

    logger.debug(f"Recalculated {updated} interest rates (skipped {skipped} overridden) with repo rate {repo_rate}%")
    return updated


# ==============================
# 5. SAVE REPO RATE HISTORY
# ==============================

def save_repo_rate(db_path: str, repo_rate: float, tenant_id: str = None):
    """Save a repo rate entry to the history table."""
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("INSERT INTO repo_rate_history (id, tenant_id, repo_rate, fetched_at) VALUES (%s, %s, %s, %s)",
        (str(uuid.uuid4()), tenant_id, repo_rate, get_ist_now().isoformat())
    )

    conn.commit()
    cursor.close()
    conn.close()
    logger.debug(f"Saved repo rate {repo_rate}% to history (tenant: {tenant_id})")


def get_latest_repo_rate(db_path: str, tenant_id: str = None) -> dict | None:
    """Get the latest repo rate from history table, optionally filtered by tenant."""
    conn = get_pg_connection()
    cursor = conn.cursor()

    if tenant_id:
        cursor.execute("""
            SELECT repo_rate, fetched_at
            FROM repo_rate_history
            WHERE tenant_id = %s
            ORDER BY fetched_at DESC
            LIMIT 1
        """, (tenant_id,))
    else:
        cursor.execute("""
            SELECT repo_rate, fetched_at
            FROM repo_rate_history
            ORDER BY fetched_at DESC
            LIMIT 1
        """)

    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row:
        return {"repo_rate": row["repo_rate"], "fetched_at": row["fetched_at"]}
    return None


# ==============================
# 6. DAILY SCHEDULER JOB
# ==============================

def daily_repo_rate_job(db_path: str):
    """
    Daily scheduled job:
    1. Fetch latest repo rate
    2. For each tenant: compare with last saved rate
    3. If changed → save + recalculate interest rates for that tenant
    4. If unchanged → save only (for audit trail)
    """
    logger.debug("Running daily repo rate job...")

    new_rate = fetch_repo_rate()

    # Get all tenants
    conn = get_pg_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM tenants")
    tenants = cursor.fetchall()
    cursor.close()
    conn.close()

    if not tenants:
        # Fallback: no tenants, save globally
        save_repo_rate(db_path, new_rate)
        return

    for tenant_row in tenants:
        tid = tenant_row['id']
        last = get_latest_repo_rate(db_path, tid)

        # Always save for audit trail
        save_repo_rate(db_path, new_rate, tid)

        # Recalculate only if rate changed
        if last is None or abs(last["repo_rate"] - new_rate) > 0.001:
            logger.info(f"Tenant {tid}: repo rate changed {last['repo_rate'] if last else 'N/A'} → {new_rate}. Recalculating...")
            recalculate_interest_rates(db_path, new_rate, tid)
        else:
            logger.debug(f"Tenant {tid}: repo rate unchanged at {new_rate}%")
