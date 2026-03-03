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
import sqlite3
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

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
        logger.info(f"Using default repo rate: {DEFAULT_REPO_RATE}%")
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

def recalculate_interest_rates(db_path: str, repo_rate: float = None):
    """
    Recalculate all entries in the interest_rates table using the dynamic formula.
    This is triggered when the repo rate changes.

    The interest_rates table serves as the source of truth for loan applications.
    When recalculated, each category gets the dynamic rate.

    Rows with is_overridden = 1 are SKIPPED (admin manual overrides are preserved).
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

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    cursor = conn.cursor()

    updated = 0
    skipped = 0

    for loan_type, category, inputs in rate_configs:
        result = calculate_interest_rate(loan_type, inputs, repo_rate)
        new_rate = result["rate"]

        # Check if category exists and whether it's overridden
        cursor.execute(
            "SELECT id, is_overridden FROM interest_rates WHERE loan_type = ? AND category = ?",
            (loan_type, category)
        )
        existing = cursor.fetchone()

        if existing:
            if existing[1] == 1:  # is_overridden
                skipped += 1
                continue
            cursor.execute(
                "UPDATE interest_rates SET rate = ? WHERE id = ?",
                (new_rate, existing[0])
            )
        else:
            cursor.execute(
                "INSERT INTO interest_rates (id, loan_type, category, rate, is_overridden, created_at) VALUES (?, ?, ?, ?, 0, ?)",
                (str(uuid.uuid4()), loan_type, category, new_rate, datetime.now(timezone.utc).isoformat())
            )

        updated += 1

    conn.commit()
    conn.close()

    logger.info(f"Recalculated {updated} interest rates (skipped {skipped} overridden) with repo rate {repo_rate}%")
    return updated


# ==============================
# 5. SAVE REPO RATE HISTORY
# ==============================

def save_repo_rate(db_path: str, repo_rate: float):
    """Save a repo rate entry to the history table."""
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO repo_rate_history (id, repo_rate, fetched_at) VALUES (?, ?, ?)",
        (str(uuid.uuid4()), repo_rate, datetime.now(timezone.utc).isoformat())
    )

    conn.commit()
    conn.close()
    logger.info(f"Saved repo rate {repo_rate}% to history")


def get_latest_repo_rate(db_path: str) -> dict | None:
    """Get the latest repo rate from history table."""
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT repo_rate, fetched_at
        FROM repo_rate_history
        ORDER BY fetched_at DESC
        LIMIT 1
    """)

    row = cursor.fetchone()
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
    2. Compare with last saved rate
    3. If changed → save + recalculate interest rates
    4. If unchanged → save only (for audit trail)
    """
    logger.info("Running daily repo rate job...")

    new_rate = fetch_repo_rate()
    last = get_latest_repo_rate(db_path)

    # Always save for audit trail
    save_repo_rate(db_path, new_rate)

    # Recalculate only if rate changed
    if last is None or abs(last["repo_rate"] - new_rate) > 0.001:
        logger.info(f"Repo rate changed: {last['repo_rate'] if last else 'N/A'} → {new_rate}. Recalculating...")
        recalculate_interest_rates(db_path, new_rate)
    else:
        logger.info(f"Repo rate unchanged at {new_rate}%. No recalculation needed.")
