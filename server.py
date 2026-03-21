from fastapi import FastAPI, APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from starlette.middleware.cors import CORSMiddleware
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional
import uuid
from datetime import datetime, timezone, timedelta
from utils.ist_time import get_ist_now, IST
import asyncio
import sqlite3
import threading
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
import jwt
import bcrypt
from contextlib import contextmanager, asynccontextmanager
import json
from fastapi import Request
from fastapi.responses import StreamingResponse
import io
import csv
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from fastapi.responses import StreamingResponse
from reportlab.lib.units import inch
from reportlab.platypus import HRFlowable
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
pdfmetrics.registerFont(TTFont("DejaVu", "dejavu-fonts-ttf-2.37/ttf/DejaVuSans.ttf"))
pdfmetrics.registerFont(TTFont("DejaVu-Bold", "dejavu-fonts-ttf-2.37/ttf/DejaVuSans-Bold.ttf"))
from reportlab.pdfbase.pdfmetrics import registerFontFamily
registerFontFamily("DejaVu", normal="DejaVu", bold="DejaVu-Bold", italic="DejaVu", boldItalic="DejaVu-Bold")
from utils.pdf_styles import (
    build_header, build_table_style, make_doc, make_footer_cb,
    fmt_currency, fmt_datetime, fmt_date,
    STYLE_INFO, STYLE_TITLE, STYLE_CELL, STYLE_CELL_BOLD, PAGE_MARGIN, PAGE_W,
)
from services.lending_engine_service import (
    fetch_repo_rate,
    calculate_max_tenure,
    calculate_interest_rate,
    recalculate_interest_rates,
    daily_repo_rate_job,
    save_repo_rate,
    get_latest_repo_rate,
    DEFAULT_REPO_RATE,
)
from services.gold_rate_service import get_gold_rate_for_branch



ROOT_DIR = Path(__file__).parent  # deploy: docker-build-v2
load_dotenv(ROOT_DIR / '.env')

_log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress noisy third-party loggers
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logging.getLogger("apscheduler.executors").setLevel(logging.WARNING)
logging.getLogger("apscheduler.jobstores").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.INFO)
logging.getLogger("uvicorn.access").propagate = False
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Suppress asyncio CancelledError noise on shutdown — it is not a bug
def _ignore_cancelled_error(loop, context):
    if isinstance(context.get("exception"), asyncio.CancelledError):
        return
    loop.default_exception_handler(context)

try:
    asyncio.get_event_loop().set_exception_handler(_ignore_cancelled_error)
except RuntimeError:
    pass  # No running loop at import time — handler set later if needed

logger.debug("DATABASE: %s", os.getenv("DB_HOST"))

# ── Simple TTL cache for expensive admin aggregate endpoints ─────────────────
# Stores: { cache_key: (result, expiry_timestamp) }
_CACHE: dict = {}
_CACHE_TTL = 300  # 5 minutes

def _cache_get(key: str):
    entry = _CACHE.get(key)
    if entry and datetime.now(timezone.utc).timestamp() < entry[1]:
        return entry[0]
    return None

def _cache_set(key: str, value, ttl: int = _CACHE_TTL):
    _CACHE[key] = (value, datetime.now(timezone.utc).timestamp() + ttl)

def _cache_invalidate_prefix(prefix: str):
    for k in list(_CACHE.keys()):
        if k.startswith(prefix):
            del _CACHE[k]

# JWT Configuration
SECRET_KEY = os.environ.get(
    'JWT_SECRET_KEY',
    'sv_fincloud_super_secure_secret_key_2026_project'
)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 480

# Database path
DB_PATH = ROOT_DIR / 'sv_fincloud.db'

security = HTTPBearer()

# Pool is initialized lazily on first use (or explicitly via init_db_pool)
db_pool = None


def init_db_pool():
    global db_pool
    if db_pool is None:
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            # psycopg2 requires postgres:// not postgresql://
            if db_url.startswith("postgresql://"):
                db_url = db_url.replace("postgresql://", "postgres://", 1)
            # Pass the DSN string directly — avoids any URL-parsing issues
            # with special characters (e.g. # in passwords)
            db_pool = SimpleConnectionPool(minconn=5, maxconn=20, dsn=db_url)
            logger.info("Connection pool initialized from DATABASE_URL")
        else:
            # Local / dev fallback — individual env vars
            db_pool = SimpleConnectionPool(
                5, 20,
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 6543)),
                database=os.getenv("DB_NAME", "postgres"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                sslmode="require",
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            logger.info("Connection pool initialized from individual env vars (host=%s)", os.getenv("DB_HOST"))


# Database connection helper — ensures pool exists before use
@contextmanager
def get_db():
    global db_pool
    if db_pool is None:
        init_db_pool()
    conn = None
    try:
        conn = db_pool.getconn()
        # Discard connections that are closed at the socket level
        if conn.closed != 0:
            try:
                db_pool.putconn(conn, close=True)
            except Exception:
                pass
            conn = db_pool.getconn()
        # Ping to detect SSL-broken connections that still appear open
        try:
            conn.cursor().execute("SELECT 1")
        except Exception:
            try:
                db_pool.putconn(conn, close=True)
            except Exception:
                pass
            conn = db_pool.getconn()
        conn.cursor_factory = psycopg2.extras.RealDictCursor
        yield conn
        conn.commit()
    except Exception as e:
        if conn and conn.closed == 0:
            try:
                conn.rollback()
            except Exception:
                pass
        raise e
    finally:
        if conn:
            try:
                db_pool.putconn(conn)
            except Exception:
                pass

# Initialize database
def _table_exists(cursor, table_name):
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = %s
        ) AS exists
    """, (table_name,))
    
    row = cursor.fetchone()
    return row["exists"]

def _alter_if_exists(cursor, table_name: str, column_def: str):
    if _table_exists(cursor, table_name):
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_def}")

def init_db():
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                tenant_id TEXT,
                branch_id TEXT,
                created_at TEXT NOT NULL,
                full_name TEXT,
                email TEXT,
                phone TEXT,
                employee_id TEXT,
                joining_date TEXT,
                status TEXT DEFAULT 'active',
                UNIQUE(username,tenant_id)
            )
        ''')
        
        # Customers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                cibil_score INTEGER,
                monthly_income REAL,
                address TEXT,
                tenant_id TEXT,
                branch_id TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            ''')
        
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS employees (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                address TEXT,
                designation TEXT,
                joining_date TEXT,
                salary REAL,
                tenant_id TEXT,
                branch_id TEXT,
                created_at TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
        ''')
        
        # Branches table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS branches (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                location TEXT,
                tenant_id TEXT,
                created_at TEXT NOT NULL
                
            )
        ''')
        
        # Loan types table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS loan_types (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                created_at TEXT NOT NULL
            )
        ''')
        
        # Interest rates table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS interest_rates (
                id TEXT PRIMARY KEY,
                loan_type TEXT NOT NULL,
                category TEXT,
                rate REAL NOT NULL,
                created_at TEXT NOT NULL
            )
        ''')
        
        # Gold rate table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gold_rate (
                id TEXT PRIMARY KEY,
                branch_id TEXT,
                rate_per_gram REAL NOT NULL,
                updated_at TEXT NOT NULL,
                tenant_id TEXT,
                source TEXT DEFAULT 'manual'
            )
        ''')

        # Gold rate settings table (Auto / Manual toggle) — per tenant
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gold_rate_settings (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL UNIQUE,
                mode TEXT DEFAULT 'manual'
            )
        ''')
        
        # Loans table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS loans (
                id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                loan_type TEXT NOT NULL,
                amount REAL NOT NULL,
                tenure INTEGER NOT NULL,
                interest_rate REAL NOT NULL,
                emi_amount REAL NOT NULL,
                processing_fee REAL NOT NULL,
                disbursed_amount REAL NOT NULL,
                outstanding_balance REAL NOT NULL,
                status TEXT NOT NULL,
                vehicle_age INTEGER,
                gold_weight REAL DEFAULT 0,
                vehicle_reg_no TEXT,
                approved_by TEXT,
                approved_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customers(id)
            )
        ''')
        
        # EMI schedule table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS emi_schedule (
                id TEXT PRIMARY KEY,
                loan_id TEXT NOT NULL,
                emi_number INTEGER NOT NULL,
                due_date TEXT NOT NULL,
                emi_amount REAL NOT NULL,
                principal_amount REAL NOT NULL,
                interest_amount REAL NOT NULL,
                penalty REAL DEFAULT 0,
                status TEXT NOT NULL,
                paid_at TEXT,
                FOREIGN KEY (loan_id) REFERENCES loans(id)
            )
        ''')
        
        # Payments table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id TEXT PRIMARY KEY,
                loan_id TEXT NOT NULL,
                emi_id TEXT NOT NULL,
                amount REAL NOT NULL,
                payment_date TEXT NOT NULL,
                collected_by TEXT NOT NULL,
                approved_by TEXT,
                approved_at TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (loan_id) REFERENCES loans(id),
                FOREIGN KEY (emi_id) REFERENCES emi_schedule(id)
            )
        ''')
        
        # Penalties table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS penalties (
                id TEXT PRIMARY KEY,
                loan_id TEXT NOT NULL,
                emi_id TEXT NOT NULL,
                amount REAL NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (loan_id) REFERENCES loans(id),
                FOREIGN KEY (emi_id) REFERENCES emi_schedule(id)
            )
        ''')
        
        # Audit logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_logs (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                action TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT,
                details TEXT,
                created_at TEXT NOT NULL,
                tenant_id TEXT
            )
        ''')
        # payments_receipt_counter
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments_receipt_counter (
                branch_id TEXT,
                tenant_id TEXT,
                last_seq INTEGER,
                PRIMARY KEY (branch_id, tenant_id)
            )
        ''')
        # Tenants table (COMPANIES)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tenants (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        ''')

    
        _alter_if_exists(cursor, "customers", "risk_score INTEGER DEFAULT 50")

        # 🔹 Multi-branch & Tenant Support
        for table, column_def in [
            ("users", "branch_id TEXT"),
            ("branches", "tenant_id TEXT"),
            ("customers", "branch_id TEXT"),
            ("customers", "tenant_id TEXT"),
            ("loans", "branch_id TEXT"),
            ("loans", "tenant_id TEXT"),
            ("payments", "branch_id TEXT"),
            ("payments", "tenant_id TEXT"),
            ("emi_schedule", "branch_id TEXT"),
            ("emi_schedule", "tenant_id TEXT"),
        ]:
            _alter_if_exists(cursor, table, column_def)

        _alter_if_exists(cursor, "payments", "balance_after_payment REAL")
        _alter_if_exists(cursor, "payments", "remaining_emi_after_payment INTEGER")
        _alter_if_exists(cursor, "payments", "receipt_no TEXT")
        _alter_if_exists(cursor, "payments", "receipt_seq INTEGER")
        _alter_if_exists(cursor, "audit_logs", "tenant_id TEXT")

        try:
            cursor.execute("DELETE FROM interest_rates WHERE category LIKE '% %'")
        except Exception:
            pass

        _alter_if_exists(cursor, "tenants", "logo_url TEXT")
        _alter_if_exists(cursor, "gold_rate", "source TEXT DEFAULT 'manual'")
        _alter_if_exists(cursor, "tenants", "primary_color TEXT DEFAULT '#2536eb'")
        _alter_if_exists(cursor, "tenants", "tagline TEXT")
        _alter_if_exists(cursor, "tenants", "stats_json TEXT")
        _alter_if_exists(cursor, "gold_rate", "branch_id TEXT")

        # Hybrid override flag for interest rates
        _alter_if_exists(cursor, "interest_rates", "is_overridden INTEGER DEFAULT 0")

        # Source tracking for interest rates ('auto' = repo-linked, 'manual' = admin override)
        _alter_if_exists(cursor, "interest_rates", "source TEXT DEFAULT 'auto'")

        # Backfill source for existing interest_rates rows
        try:
            cursor.execute("SAVEPOINT sp_ir_source_backfill")
            cursor.execute("""
                UPDATE interest_rates SET source = 'manual' WHERE is_overridden = 1 AND source IS NULL
            """)
            cursor.execute("""
                UPDATE interest_rates SET source = 'auto' WHERE (is_overridden = 0 OR is_overridden IS NULL) AND source IS NULL
            """)
            cursor.execute("RELEASE SAVEPOINT sp_ir_source_backfill")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_ir_source_backfill")
            logger.warning("interest_rates source backfill skipped: %s", e)

        # Add designation column to users table
        _alter_if_exists(cursor, "users", "designation TEXT")

        # Add vehicle_reg_no column to loans table for collateral locking
        _alter_if_exists(cursor, "loans", "vehicle_reg_no TEXT")

        # Add collection_remark column to loans for field agent notes
        _alter_if_exists(cursor, "loans", "collection_remark TEXT")

        # Add gold_weight column to loans for gold weight tracking
        _alter_if_exists(cursor, "loans", "gold_weight REAL DEFAULT 0")

        # Add gold_released_at column to loans for gold release tracking
        _alter_if_exists(cursor, "loans", "gold_released_at TEXT")

        # Add collection_remark and followup_date to emi_schedule for per-EMI agent notes
        _alter_if_exists(cursor, "emi_schedule", "collection_remark TEXT")
        _alter_if_exists(cursor, "emi_schedule", "followup_date DATE")

        # Add tenant_id column to gold_rate table for multi-tenancy support
        _alter_if_exists(cursor, "gold_rate", "tenant_id TEXT")

        # Add changed_by to gold_rate for direct user attribution
        _alter_if_exists(cursor, "gold_rate", "changed_by TEXT")

        # Backfill NULL changed_by on manual gold_rate rows with the tenant's super_admin
        try:
            cursor.execute("""
                UPDATE gold_rate gr
                SET changed_by = u.id
                FROM users u
                WHERE gr.source = 'manual'
                  AND gr.changed_by IS NULL
                  AND u.role = 'super_admin'
                  AND u.tenant_id = gr.tenant_id
            """)
        except Exception as e:
            logger.warning("gold_rate changed_by backfill skipped: %s", e)

        # Migrate gold_rate_settings from global (id=1) to per-tenant
        # PostgreSQL-safe: check information_schema instead of sqlite_master
        try:
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'gold_rate_settings' AND column_name = 'tenant_id'
            """)
            has_tenant_id = cursor.fetchone()
            if not has_tenant_id:
                # Old global schema detected — migrate
                cursor.execute("SELECT mode FROM gold_rate_settings WHERE id = 1")
                old_row = cursor.fetchone()
                old_mode = old_row['mode'] if old_row else 'manual'

                cursor.execute("DROP TABLE gold_rate_settings")
                cursor.execute('''
                    CREATE TABLE gold_rate_settings (
                        id TEXT PRIMARY KEY,
                        tenant_id TEXT NOT NULL UNIQUE,
                        mode TEXT DEFAULT 'manual'
                    )
                ''')

                # Seed a row for every existing tenant using the old mode
                cursor.execute("SELECT id FROM tenants")
                for t in cursor.fetchall():
                    cursor.execute("INSERT INTO gold_rate_settings (id, tenant_id, mode) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (str(uuid.uuid4()), t['id'], old_mode)
                    )
                conn.commit()
                logger.info("✓ Migrated gold_rate_settings to per-tenant (old mode: %s)", old_mode)
        except Exception as e:
            logger.warning("gold_rate_settings migration check: %s", e)

        # Add tenant_id to interest_rates for multi-tenancy
        _alter_if_exists(cursor, "interest_rates", "tenant_id TEXT")

        # Add tenant_id to repo_rate_history for multi-tenancy
        _alter_if_exists(cursor, "repo_rate_history", "tenant_id TEXT")

        # ===== VIEW-BASED ACCESS TABLES =====

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS roles (
                role_id SERIAL PRIMARY KEY,
                role_name TEXT UNIQUE NOT NULL
        )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS modules (
                module_id SERIAL PRIMARY KEY,
                module_name TEXT UNIQUE NOT NULL
        )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS permissions (
                permission_id SERIAL PRIMARY KEY,
                role_id INTEGER,
                module_id INTEGER,
                can_view INTEGER DEFAULT 0,
                can_insert INTEGER DEFAULT 0,
                can_update INTEGER DEFAULT 0,
                can_delete INTEGER DEFAULT 0,
                FOREIGN KEY(role_id) REFERENCES roles(role_id),
                FOREIGN KEY(module_id) REFERENCES modules(module_id),
                UNIQUE(role_id, module_id)
        )
        """)

        # ── One-time migration: deduplicate permissions & add UNIQUE constraint ──
        # PostgreSQL: permissions table is created with UNIQUE(role_id, module_id) above,
        # so this migration is a no-op on a fresh PostgreSQL database.
        try:
            cursor.execute("""
                INSERT INTO permissions (role_id, module_id, can_view, can_insert, can_update, can_delete)
                SELECT role_id, module_id,
                       MAX(can_view), MAX(can_insert), MAX(can_update), MAX(can_delete)
                FROM permissions
                GROUP BY role_id, module_id
                ON CONFLICT (role_id, module_id) DO NOTHING
            """)
        except Exception:
            pass  # Already migrated or table is clean
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_tenant_branch ON loans(tenant_id, branch_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_loan ON payments(loan_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_emi_loan ON emi_schedule(loan_id)")
        # Performance indexes for admin aggregate queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_tenant_status ON loans(tenant_id, status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_created_at ON loans(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_tenant_status ON payments(tenant_id, status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_branch_status ON payments(branch_id, status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_emi_status ON emi_schedule(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_rate_tenant ON gold_rate(tenant_id, updated_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_repo_rate_tenant ON repo_rate_history(tenant_id, fetched_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_tenant ON customers(tenant_id, branch_id)")

        # Partial unique index: prevent duplicate active vehicle loans across the entire system
        # Drop old index (narrower WHERE clause) and recreate with full active-status coverage
        try:
            cursor.execute("SAVEPOINT sp_vehicle_idx")
            cursor.execute("DROP INDEX IF EXISTS unique_active_vehicle_loan")
            cursor.execute("""
                CREATE UNIQUE INDEX unique_active_vehicle_loan
                ON loans (UPPER(REPLACE(COALESCE(vehicle_reg_no, ''), ' ', '')))
                WHERE status IN ('pending', 'pre-approved', 'active', 'approved', 'disbursed')
            """)
            cursor.execute("RELEASE SAVEPOINT sp_vehicle_idx")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_vehicle_idx")
            logger.warning("unique_active_vehicle_loan index skipped: %s", e)

        # Repo Rate History table (for dynamic lending engine)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS repo_rate_history (
                id TEXT PRIMARY KEY,
                repo_rate REAL NOT NULL,
                fetched_at TEXT NOT NULL,
                tenant_id TEXT
            )
        ''')

        conn.commit()

        # Backfill NULL tenant_ids for gold_rate, audit_logs, repo_rate_history
        # Only runs if there are rows with NULL tenant_id (idempotent)
        for table in ("gold_rate", "audit_logs", "repo_rate_history"):
            try:
                cursor.execute(f"SELECT COUNT(*) AS cnt FROM {table} WHERE tenant_id IS NULL")
                row = cursor.fetchone()
                null_count = row['cnt'] if row else 0
                if null_count > 0:
                    cursor.execute(f"""
                        UPDATE {table}
                        SET tenant_id = (SELECT id FROM tenants ORDER BY created_at ASC LIMIT 1)
                        WHERE tenant_id IS NULL
                    """)
                    conn.commit()
                    logger.debug("Backfilled %d NULL tenant_id rows in %s", null_count, table)
            except Exception as e:
                logger.warning("tenant_id backfill skipped for %s: %s", table, e)

        # Create sample users
        # create_sample_data(conn)

        # ── Loan / Payment / Receipt number sequences ────────────────────────
        # loan_sequences: per-tenant, per-loan-type
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS loan_sequences (
                tenant_id TEXT NOT NULL,
                loan_type TEXT NOT NULL,
                current_value INT DEFAULT 0,
                PRIMARY KEY (tenant_id, loan_type)
            )
        """)
        # Migrate old schema (loan_type-only PK) to new (tenant_id, loan_type) PK
        try:
            cursor.execute("SAVEPOINT sp_ls_migrate")
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'loan_sequences' AND column_name = 'tenant_id'
            """)
            if not cursor.fetchone():
                # Old schema — read existing values, drop, recreate, re-seed
                cursor.execute("SELECT loan_type, current_value FROM loan_sequences")
                old_rows = cursor.fetchall()
                cursor.execute("DROP TABLE loan_sequences")
                cursor.execute("""
                    CREATE TABLE loan_sequences (
                        tenant_id TEXT NOT NULL,
                        loan_type TEXT NOT NULL,
                        current_value INT DEFAULT 0,
                        PRIMARY KEY (tenant_id, loan_type)
                    )
                """)
                # Seed each existing tenant with the old global values
                cursor.execute("SELECT id FROM tenants")
                tenant_ids = [r["id"] for r in cursor.fetchall()]
                for tid in tenant_ids:
                    for row in old_rows:
                        cursor.execute("""
                            INSERT INTO loan_sequences (tenant_id, loan_type, current_value)
                            VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                        """, (tid, row["loan_type"], row["current_value"]))
                conn.commit()
                logger.info("✓ Migrated loan_sequences to per-tenant schema")
            cursor.execute("RELEASE SAVEPOINT sp_ls_migrate")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_ls_migrate")
            logger.warning("loan_sequences migration skipped: %s", e)

        # payment_sequence: per-tenant
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS payment_sequence (
                tenant_id TEXT PRIMARY KEY,
                current_value INT DEFAULT 0
            )
        """)
        # Migrate old schema (id INT PK) to new (tenant_id TEXT PK)
        try:
            cursor.execute("SAVEPOINT sp_ps_migrate")
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'payment_sequence' AND column_name = 'tenant_id'
            """)
            if not cursor.fetchone():
                cursor.execute("SELECT current_value FROM payment_sequence WHERE id = 1")
                old_val = cursor.fetchone()
                old_current = old_val["current_value"] if old_val else 0
                cursor.execute("DROP TABLE payment_sequence")
                cursor.execute("""
                    CREATE TABLE payment_sequence (
                        tenant_id TEXT PRIMARY KEY,
                        current_value INT DEFAULT 0
                    )
                """)
                cursor.execute("SELECT id FROM tenants")
                for t in cursor.fetchall():
                    cursor.execute("""
                        INSERT INTO payment_sequence (tenant_id, current_value)
                        VALUES (%s, %s) ON CONFLICT DO NOTHING
                    """, (t["id"], old_current))
                conn.commit()
                logger.info("✓ Migrated payment_sequence to per-tenant schema")
            cursor.execute("RELEASE SAVEPOINT sp_ps_migrate")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_ps_migrate")
            logger.warning("payment_sequence migration skipped: %s", e)

        # receipt_sequence: per-tenant
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS receipt_sequence (
                tenant_id TEXT PRIMARY KEY,
                current_value INT DEFAULT 0
            )
        """)
        # Migrate old schema (id INT PK) to new (tenant_id TEXT PK)
        try:
            cursor.execute("SAVEPOINT sp_rs_migrate")
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'receipt_sequence' AND column_name = 'tenant_id'
            """)
            if not cursor.fetchone():
                cursor.execute("SELECT current_value FROM receipt_sequence WHERE id = 1")
                old_val = cursor.fetchone()
                old_current = old_val["current_value"] if old_val else 0
                cursor.execute("DROP TABLE receipt_sequence")
                cursor.execute("""
                    CREATE TABLE receipt_sequence (
                        tenant_id TEXT PRIMARY KEY,
                        current_value INT DEFAULT 0
                    )
                """)
                cursor.execute("SELECT id FROM tenants")
                for t in cursor.fetchall():
                    cursor.execute("""
                        INSERT INTO receipt_sequence (tenant_id, current_value)
                        VALUES (%s, %s) ON CONFLICT DO NOTHING
                    """, (t["id"], old_current))
                conn.commit()
                logger.info("✓ Migrated receipt_sequence to per-tenant schema")
            cursor.execute("RELEASE SAVEPOINT sp_rs_migrate")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_rs_migrate")
            logger.warning("receipt_sequence migration skipped: %s", e)
        _alter_if_exists(cursor, "loans", "loan_number TEXT")
        _alter_if_exists(cursor, "payments", "payment_number TEXT")
        _alter_if_exists(cursor, "payments", "receipt_number TEXT")

        # Add UNIQUE constraints — each in its own savepoint so a duplicate-constraint
        # error doesn't abort the whole transaction
        for ddl in [
            "ALTER TABLE loans ADD CONSTRAINT unique_loan_number UNIQUE (loan_number)",
            "ALTER TABLE payments ADD CONSTRAINT unique_payment_number UNIQUE (payment_number)",
            "ALTER TABLE payments ADD CONSTRAINT unique_receipt_number UNIQUE (receipt_number)",
        ]:
            try:
                cursor.execute("SAVEPOINT sp_constraint")
                cursor.execute(ddl)
                cursor.execute("RELEASE SAVEPOINT sp_constraint")
            except Exception:
                cursor.execute("ROLLBACK TO SAVEPOINT sp_constraint")

        # Backfill loan_number for loans missing one (NULL only — never overwrite)
        try:
            cursor.execute("SAVEPOINT sp_loan_backfill")
            for lt, prefix in [('gold_loan', 'GL'), ('personal_loan', 'PL'), ('vehicle_loan', 'VL')]:
                cursor.execute("""
                    WITH missing AS (
                        SELECT id, tenant_id, ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY created_at) AS rn
                        FROM loans
                        WHERE loan_type = %s AND loan_number IS NULL
                    ),
                    max_seq AS (
                        SELECT tenant_id, COALESCE(MAX(
                            CASE WHEN loan_number ~ ('^' || %s || '-[0-9]+$')
                                 THEN CAST(SPLIT_PART(loan_number, '-', 2) AS INT)
                                 ELSE 0 END
                        ), 0) AS val FROM loans WHERE loan_type = %s GROUP BY tenant_id
                    )
                    UPDATE loans l
                    SET loan_number = %s || '-' || (max_seq.val + missing.rn)
                    FROM missing
                    JOIN max_seq ON max_seq.tenant_id = missing.tenant_id
                    WHERE l.id = missing.id
                """, (lt, prefix, lt, prefix))
            # Sync loan_sequences to max existing values per tenant
            for lt in ['gold_loan', 'personal_loan', 'vehicle_loan']:
                cursor.execute("""
                    INSERT INTO loan_sequences (tenant_id, loan_type, current_value)
                    SELECT tenant_id, %s,
                        COALESCE(MAX(
                            CASE WHEN loan_number ~ '^[A-Z]+-[0-9]+$'
                                 THEN CAST(SPLIT_PART(loan_number, '-', 2) AS INT)
                                 ELSE 0 END
                        ), 0)
                    FROM loans
                    WHERE loan_type = %s AND loan_number IS NOT NULL
                    GROUP BY tenant_id
                    ON CONFLICT (tenant_id, loan_type)
                    DO UPDATE SET current_value = GREATEST(
                        loan_sequences.current_value, excluded.current_value
                    )
                """, (lt, lt))
            cursor.execute("RELEASE SAVEPOINT sp_loan_backfill")
            logger.debug("Loan number backfill complete")
        except Exception as e:
            logger.debug("Loan backfill skipped (already exists): %s", e)
            cursor.execute("ROLLBACK TO SAVEPOINT sp_loan_backfill")

        # Backfill payment_number for payments missing one (NULL only — never overwrite)
        try:
            cursor.execute("SAVEPOINT sp_pay_backfill")
            cursor.execute("""
                WITH missing AS (
                    SELECT id, tenant_id, ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY created_at) AS rn
                    FROM payments
                    WHERE payment_number IS NULL
                ),
                max_seq AS (
                    SELECT tenant_id, COALESCE(MAX(
                        CASE WHEN payment_number ~ '^PAY-[0-9]+$'
                             THEN CAST(SPLIT_PART(payment_number, '-', 2) AS INT)
                             ELSE 0 END
                    ), 0) AS val FROM payments GROUP BY tenant_id
                )
                UPDATE payments p
                SET payment_number = 'PAY-' || (max_seq.val + missing.rn)
                FROM missing
                JOIN max_seq ON max_seq.tenant_id = missing.tenant_id
                WHERE p.id = missing.id
            """)
            cursor.execute("""
                INSERT INTO payment_sequence (tenant_id, current_value)
                SELECT tenant_id, COALESCE(MAX(
                    CASE WHEN payment_number ~ '^PAY-[0-9]+$'
                         THEN CAST(SPLIT_PART(payment_number, '-', 2) AS INT)
                         ELSE 0 END
                ), 0)
                FROM payments WHERE payment_number IS NOT NULL
                GROUP BY tenant_id
                ON CONFLICT (tenant_id)
                DO UPDATE SET current_value = GREATEST(
                    payment_sequence.current_value, excluded.current_value
                )
            """)
            cursor.execute("RELEASE SAVEPOINT sp_pay_backfill")
            logger.debug("Payment number backfill complete")
        except Exception as e:
            logger.debug("Payment backfill skipped (already exists): %s", e)
            cursor.execute("ROLLBACK TO SAVEPOINT sp_pay_backfill")

        # Backfill receipt_number for payments missing one (NULL only — never overwrite)
        try:
            cursor.execute("SAVEPOINT sp_rcpt_backfill")
            cursor.execute("""
                WITH missing AS (
                    SELECT id, tenant_id, ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY created_at) AS rn
                    FROM payments
                    WHERE receipt_number IS NULL
                ),
                max_seq AS (
                    SELECT tenant_id, COALESCE(MAX(
                        CASE WHEN receipt_number ~ '^RCPT-[0-9]+$'
                             THEN CAST(SPLIT_PART(receipt_number, '-', 2) AS INT)
                             ELSE 0 END
                    ), 0) AS val FROM payments GROUP BY tenant_id
                )
                UPDATE payments p
                SET receipt_number = 'RCPT-' || (max_seq.val + missing.rn)
                FROM missing
                JOIN max_seq ON max_seq.tenant_id = missing.tenant_id
                WHERE p.id = missing.id
            """)
            cursor.execute("""
                INSERT INTO receipt_sequence (tenant_id, current_value)
                SELECT tenant_id, COALESCE(MAX(
                    CASE WHEN receipt_number ~ '^RCPT-[0-9]+$'
                         THEN CAST(SPLIT_PART(receipt_number, '-', 2) AS INT)
                         ELSE 0 END
                ), 0)
                FROM payments WHERE receipt_number IS NOT NULL
                GROUP BY tenant_id
                ON CONFLICT (tenant_id)
                DO UPDATE SET current_value = GREATEST(
                    receipt_sequence.current_value, excluded.current_value
                )
            """)
            cursor.execute("RELEASE SAVEPOINT sp_rcpt_backfill")
            logger.debug("Receipt number backfill complete")
        except Exception as e:
            logger.debug("Receipt backfill skipped (already exists): %s", e)
            cursor.execute("ROLLBACK TO SAVEPOINT sp_rcpt_backfill")
            # Sync sequences to max existing values
            for lt in ['gold_loan', 'personal_loan', 'vehicle_loan']:
                cursor.execute("""
                    UPDATE loan_sequences ls
                    SET current_value = GREATEST(ls.current_value, COALESCE((
                        SELECT MAX(CAST(SPLIT_PART(loan_number, '-', 2) AS INT))
                        FROM loans
                        WHERE loan_type = %s
                          AND tenant_id = ls.tenant_id
                          AND loan_number IS NOT NULL
                          AND loan_number ~ '^[A-Z]+-[0-9]+$'
                    ), 0))
                    WHERE loan_type = %s
                """, (lt, lt))
            cursor.execute("RELEASE SAVEPOINT sp_loan_backfill")
            logger.debug("Loan number backfill complete")
        except Exception as e:
            logger.debug("Loan backfill skipped (already exists): %s", e)
            cursor.execute("ROLLBACK TO SAVEPOINT sp_loan_backfill")

        # ── Unconditional sequence sync: ensure sequences match actual MAX values ──
        # Runs on every startup to fix any stale/out-of-sync counters
        try:
            # Sync loan_sequences per tenant per loan_type
            for lt in ['gold_loan', 'personal_loan', 'vehicle_loan']:
                cursor.execute("""
                    UPDATE loan_sequences ls
                    SET current_value = GREATEST(ls.current_value, COALESCE((
                        SELECT MAX(CAST(SPLIT_PART(loan_number, '-', 2) AS INT))
                        FROM loans
                        WHERE loan_type = %s
                          AND tenant_id = ls.tenant_id
                          AND loan_number IS NOT NULL
                          AND loan_number ~ '^[A-Z]+-[0-9]+$'
                    ), 0))
                    WHERE loan_type = %s
                """, (lt, lt))

            # Sync payment_sequence per tenant
            cursor.execute("""
                UPDATE payment_sequence ps
                SET current_value = GREATEST(ps.current_value, COALESCE((
                    SELECT MAX(CAST(SPLIT_PART(payment_number, '-', 2) AS INT))
                    FROM payments
                    WHERE tenant_id = ps.tenant_id
                      AND payment_number IS NOT NULL
                      AND payment_number ~ '^PAY-[0-9]+$'
                ), 0))
            """)

            # Sync receipt_sequence per tenant
            cursor.execute("""
                UPDATE receipt_sequence rs
                SET current_value = GREATEST(rs.current_value, COALESCE((
                    SELECT MAX(CAST(SPLIT_PART(receipt_number, '-', 2) AS INT))
                    FROM payments
                    WHERE tenant_id = rs.tenant_id
                      AND receipt_number IS NOT NULL
                      AND receipt_number ~ '^RCPT-[0-9]+$'
                ), 0))
            """)
            logger.info("✓ Sequence sync complete (loan/payment/receipt)")
        except Exception as e:
            logger.warning("Sequence sync skipped: %s", e)

        # Backfill payment_number for payments missing one (NULL only — never overwrite)
        try:
            cursor.execute("SAVEPOINT sp_pay_backfill")
            cursor.execute("""
                WITH missing AS (
                    SELECT id, tenant_id, ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY created_at) AS rn
                    FROM payments
                    WHERE payment_number IS NULL
                ),
                max_seq AS (
                    SELECT tenant_id, COALESCE(MAX(
                        CASE WHEN payment_number ~ '^PAY-[0-9]+$'
                             THEN CAST(SPLIT_PART(payment_number, '-', 2) AS INT)
                             ELSE 0 END
                    ), 0) AS val FROM payments GROUP BY tenant_id
                )
                UPDATE payments p
                SET payment_number = 'PAY-' || (max_seq.val + missing.rn)
                FROM missing
                JOIN max_seq ON max_seq.tenant_id = missing.tenant_id
                WHERE p.id = missing.id
            """)
            cursor.execute("RELEASE SAVEPOINT sp_pay_backfill")
            logger.debug("Payment number backfill complete")
        except Exception as e:
            logger.debug("Payment backfill skipped (already exists): %s", e)
            cursor.execute("ROLLBACK TO SAVEPOINT sp_pay_backfill")

        # Backfill receipt_number for payments missing one (NULL only — never overwrite)
        try:
            cursor.execute("SAVEPOINT sp_rcpt_backfill")
            cursor.execute("""
                WITH missing AS (
                    SELECT id, tenant_id, ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY created_at) AS rn
                    FROM payments
                    WHERE receipt_number IS NULL
                ),
                max_seq AS (
                    SELECT tenant_id, COALESCE(MAX(
                        CASE WHEN receipt_number ~ '^RCPT-[0-9]+$'
                             THEN CAST(SPLIT_PART(receipt_number, '-', 2) AS INT)
                             ELSE 0 END
                    ), 0) AS val FROM payments GROUP BY tenant_id
                )
                UPDATE payments p
                SET receipt_number = 'RCPT-' || (max_seq.val + missing.rn)
                FROM missing
                JOIN max_seq ON max_seq.tenant_id = missing.tenant_id
                WHERE p.id = missing.id
            """)
            cursor.execute("""
                INSERT INTO receipt_sequence (tenant_id, current_value)
                SELECT tenant_id, COALESCE(MAX(
                    CASE WHEN receipt_number ~ '^RCPT-[0-9]+$'
                         THEN CAST(SPLIT_PART(receipt_number, '-', 2) AS INT)
                         ELSE 0 END
                ), 0)
                FROM payments WHERE receipt_number IS NOT NULL
                GROUP BY tenant_id
                ON CONFLICT (tenant_id)
                DO UPDATE SET current_value = GREATEST(
                    receipt_sequence.current_value, excluded.current_value
                )
            """)
            cursor.execute("RELEASE SAVEPOINT sp_rcpt_backfill")
            logger.debug("Receipt number backfill complete")
        except Exception as e:
            logger.debug("Receipt backfill skipped (already exists): %s", e)
            cursor.execute("ROLLBACK TO SAVEPOINT sp_rcpt_backfill")

        conn.commit()

def get_branch_filter(token_data, table_alias=""):
    role = token_data["role"]

    # Company-level roles → ALL branches
    if role in ["super_admin", "auditor"]:
        return "", []

    # Branch-level roles → restrict
    column = f"{table_alias}.branch_id" if table_alias else "branch_id"
    return f" AND {column} = %s", [token_data["branch_id"]]

def write_csv_summary(writer, report_name, branch_name, totals):
    """
    Write summary header rows to CSV export.
    
    Args:
        writer: CSV writer object
        report_name: Name of the report (e.g., "Pending Loans Report")
        branch_name: Branch name or "All Branches"
        totals: Dictionary of total labels and raw numeric values
                Values should be plain numbers (not formatted strings)
    
    Example:
        totals = {
            "Total Records": 10,
            "Total Loan Amount": 500000,
            "Average Loan Amount": 50000.50
        }
    """
    # Write report name
    writer.writerow(["Report", report_name])
    
    # Write branch name
    writer.writerow(["Branch", branch_name])
    
    # Write each total
    for label, value in totals.items():
        writer.writerow([label, str(value)])
    
    # Write blank separator row
    writer.writerow([])


def _make_csv_response(content: str, filename: str) -> StreamingResponse:
    """
    Return a StreamingResponse for CSV with UTF-8 BOM so Excel renders
    the ₹ symbol and other Unicode characters correctly.
    """
    encoded = content.encode("utf-8-sig")   # utf-8-sig prepends the BOM
    return StreamingResponse(
        io.BytesIO(encoded),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )


def generate_report_pdf(
    title,
    headers,
    rows,
    totals_info,
    company_name,
    branch_name,
    from_date=None,
    to_date=None,
    col_widths=None
):
    """Generate a PDF report using the shared pdf_styles utilities."""
    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []

    period = f"{from_date} \u2013 {to_date}" if from_date and to_date else None
    elements.extend(build_header(company_name, branch_name, period))

    if title:
        elements.append(Paragraph(title, STYLE_TITLE))
        elements.append(Spacer(1, 4))

    for key, value in totals_info.items():
        elements.append(Paragraph(f"<b>{key}:</b>  {value}", STYLE_INFO))
    if totals_info:
        elements.append(Spacer(1, 14))

    usable_w = PAGE_W - 2 * PAGE_MARGIN
    if col_widths is None:
        n = len(headers)
        col_widths = [usable_w / n] * n

    def _wrap(cell):
        """Wrap plain strings as Paragraph so DejaVu font renders ₹ correctly."""
        if isinstance(cell, str):
            return Paragraph(cell, STYLE_CELL)
        return cell  # already a Paragraph or other flowable

    wrapped_headers = [Paragraph(h, STYLE_CELL_BOLD) for h in headers]
    wrapped_rows = [[_wrap(cell) for cell in row] for row in rows]

    table_data = [wrapped_headers] + wrapped_rows
    tbl = Table(table_data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style())
    elements.append(tbl)

    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return buffer


# ── Paragraph helpers for table cells — ensures DejaVu renders ₹ correctly ──
def _p(text: str) -> "Paragraph":
    """Wrap a plain string as a Paragraph using the DejaVu cell style."""
    return Paragraph(str(text) if text is not None else "-", STYLE_CELL)

def _pb(text: str) -> "Paragraph":
    """Bold variant for header / total cells."""
    return Paragraph(str(text) if text is not None else "-", STYLE_CELL_BOLD)


def init_permissions():
    with get_db() as conn:
        cursor = conn.cursor()

        # ROLES
        cursor.execute("""
            INSERT INTO roles (role_name) VALUES
            ('super_admin'),
            ('admin'),
            ('finance_officer'),
            ('collection_agent'),
            ('auditor'),
            ('customer')
            ON CONFLICT (role_name) DO NOTHING
        """)

        # MODULES
        cursor.execute("""
            INSERT INTO modules (module_name) VALUES
            ('users'),
            ('customers'),
            ('loans'),
            ('payments'),
            ('emi_schedule'),
            ('reports'),
            ('audit_logs'),
            ('branches')
            ON CONFLICT (module_name) DO NOTHING
        """)

        # 🟢 SUPER ADMIN → FULL ACCESS
        cursor.execute("""
            INSERT INTO permissions
            (role_id, module_id, can_view, can_insert, can_update, can_delete)
            SELECT r.role_id, m.module_id, 1,1,1,1
            FROM roles r, modules m
            WHERE r.role_name = 'super_admin'
            ON CONFLICT(role_id, module_id) DO UPDATE SET
              can_view=excluded.can_view,
              can_insert=excluded.can_insert,
              can_update=excluded.can_update,
              can_delete=excluded.can_delete
        """)

        # 🟢 ADMIN → FULL EXCEPT DELETE AUDIT
        cursor.execute("""
            INSERT INTO permissions
            (role_id, module_id, can_view, can_insert, can_update, can_delete)
            SELECT r.role_id, m.module_id, 1,1,1,0
            FROM roles r, modules m
            WHERE r.role_name = 'admin'
            ON CONFLICT(role_id, module_id) DO UPDATE SET
              can_view=excluded.can_view,
              can_insert=excluded.can_insert,
              can_update=excluded.can_update,
              can_delete=excluded.can_delete
        """)

        # 🟡 FINANCE OFFICER
        cursor.execute("""
            INSERT INTO permissions
            (role_id, module_id, can_view, can_insert, can_update, can_delete)
            SELECT r.role_id, m.module_id,
                CASE WHEN m.module_name IN ('loans','payments','reports') THEN 1 ELSE 0 END,
                CASE WHEN m.module_name IN ('payments') THEN 1 ELSE 0 END,
                CASE WHEN m.module_name IN ('loans','payments') THEN 1 ELSE 0 END,
                0
            FROM roles r, modules m
            WHERE r.role_name = 'finance_officer'
            ON CONFLICT(role_id, module_id) DO UPDATE SET
              can_view=excluded.can_view,
              can_insert=excluded.can_insert,
              can_update=excluded.can_update,
              can_delete=excluded.can_delete
        """)

        # 🟡 COLLECTION AGENT
        cursor.execute("""
            INSERT INTO permissions
            (role_id, module_id, can_view, can_insert, can_update, can_delete)
            SELECT r.role_id, m.module_id,
                CASE WHEN m.module_name IN ('customers','payments','emi_schedule') THEN 1 ELSE 0 END,
                CASE WHEN m.module_name = 'payments' THEN 1 ELSE 0 END,
                0,0
            FROM roles r, modules m
            WHERE r.role_name = 'collection_agent'
            ON CONFLICT(role_id, module_id) DO UPDATE SET
              can_view=excluded.can_view,
              can_insert=excluded.can_insert,
              can_update=excluded.can_update,
              can_delete=excluded.can_delete
        """)

        # 🟣 AUDITOR → VIEW ONLY
        cursor.execute("""
            INSERT INTO permissions
            (role_id, module_id, can_view, can_insert, can_update, can_delete)
            SELECT r.role_id, m.module_id, 1,0,0,0
            FROM roles r, modules m
            WHERE r.role_name = 'auditor'
            ON CONFLICT(role_id, module_id) DO UPDATE SET
              can_view=excluded.can_view,
              can_insert=excluded.can_insert,
              can_update=excluded.can_update,
              can_delete=excluded.can_delete
        """)

        # 🔵 CUSTOMER → LIMITED
        cursor.execute("""
            INSERT INTO permissions
            (role_id, module_id, can_view, can_insert, can_update, can_delete)
            SELECT r.role_id, m.module_id,
                CASE WHEN m.module_name IN ('loans','payments','emi_schedule') THEN 1 ELSE 0 END,
                CASE WHEN m.module_name = 'loans' THEN 1 ELSE 0 END,
                0,0
            FROM roles r, modules m
            WHERE r.role_name = 'customer'
            ON CONFLICT(role_id, module_id) DO UPDATE SET
              can_view=excluded.can_view,
              can_insert=excluded.can_insert,
              can_update=excluded.can_update,
              can_delete=excluded.can_delete
        """)

        conn.commit()

def create_sample_data(conn):
    cursor = conn.cursor()
    
    # Check if users already exist
    cursor.execute("SELECT COUNT(*) FROM users")
    if cursor.fetchone()[0] > 0:
        return
    tenant_id = str(uuid.uuid4())
    cursor.execute("INSERT INTO tenants (id, name, created_at) VALUES (%s, %s, %s)",
        (tenant_id, "SV Fincloud", get_ist_now().isoformat())
    )
    # Hash passwords
    users_data = [
        ('superadmin', 'super123', 'super_admin'),
        ('admin', 'admin123', 'admin'),
        ('finance_officer', 'officer123', 'finance_officer'),
        ('collection_agent', 'agent123', 'collection_agent'),
        ('customer', 'customer123', 'customer'),
        ('auditor', 'auditor123', 'auditor')
    ]
    
    branch_id = str(uuid.uuid4())
    cursor.execute("INSERT INTO branches (id, name, location, tenant_id, created_at) VALUES (%s, %s, %s, %s, %s)",
            (branch_id, 'SV Fincloud Main Branch', 'Chennai', tenant_id, get_ist_now().isoformat())
    )
    for username, password, role in users_data:
        user_id = str(uuid.uuid4())
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode()

        branch_for_user = None if role in ["super_admin", "auditor"] else branch_id

        cursor.execute("""
            INSERT INTO users (id, username, password, role, tenant_id, branch_id, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                user_id,
                username,
                hashed.decode('utf-8'),
                role,
                tenant_id,
                branch_for_user,
                get_ist_now().isoformat()
            )
        )
        
        # Create customer profile for customer user
        if role == 'customer':
            cursor.execute("INSERT INTO customers (id, user_id, name, email, phone, cibil_score, branch_id, tenant_id, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (str(uuid.uuid4()),user_id,'Demo Customer','customer@svfincloud.com','9876543210',780,branch_id,tenant_id,get_ist_now().isoformat())
)
    
    # Create loan types
    loan_types = [
        ('personal_loan', 'Personal Loan'),
        ('vehicle_loan', 'Vehicle Loan'),
        ('gold_loan', 'Gold Loan')
    ]
    
    for lt_id, lt_name in loan_types:
        cursor.execute("INSERT INTO loan_types (id, name, description, created_at) VALUES (%s, %s, %s, %s)",
            (lt_id, lt_name, f'{lt_name} for customers', get_ist_now().isoformat())
        )
    
    # Create interest rates
    interest_rates = [
        ('personal_loan', 'cibil_750_plus', 12.0),
        ('personal_loan', 'cibil_700_749', 15.0),
        ('vehicle_loan', 'age_0_3', 11.0),
        ('vehicle_loan', 'age_4_6', 13.0),
        ('vehicle_loan', 'age_7_plus', 15.0),
        ('gold_loan', 'standard', 10.0)
    ]
    
    for loan_type, category, rate in interest_rates:
        cursor.execute("INSERT INTO interest_rates (id, tenant_id, loan_type, category, rate, created_at) VALUES (%s, %s, %s, %s, %s, %s)",
            (str(uuid.uuid4()), tenant_id, loan_type, category, rate, get_ist_now().isoformat())
        )
    
    # Set default gold rate for the tenant
    cursor.execute("INSERT INTO gold_rate (id, tenant_id, branch_id, rate_per_gram, updated_at, source) VALUES (%s, %s, NULL, %s, %s, 'manual')",
        (str(uuid.uuid4()), tenant_id, 6500.0, get_ist_now().isoformat())
    )

    # Seed gold rate settings for this tenant
    cursor.execute("INSERT INTO gold_rate_settings (id, tenant_id, mode) VALUES (%s, %s, 'manual') ON CONFLICT (tenant_id) DO NOTHING",
        (str(uuid.uuid4()), tenant_id)
    )

    # Seed per-tenant sequences
    for lt in ('gold_loan', 'personal_loan', 'vehicle_loan'):
        cursor.execute("""
            INSERT INTO loan_sequences (tenant_id, loan_type, current_value)
            VALUES (%s, %s, 0) ON CONFLICT DO NOTHING
        """, (tenant_id, lt))
    cursor.execute("""
        INSERT INTO payment_sequence (tenant_id, current_value) VALUES (%s, 0) ON CONFLICT DO NOTHING
    """, (tenant_id,))
    cursor.execute("""
        INSERT INTO receipt_sequence (tenant_id, current_value) VALUES (%s, 0) ON CONFLICT DO NOTHING
    """, (tenant_id,))

    conn.commit()

# NOTE: init_db(), init_permissions(), fix_null_branches() moved into lifespan()
# to avoid blocking module import on every --reload

def fix_null_branches():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT id, tenant_id FROM branches LIMIT 1")
        default_branch = cursor.fetchone()

        if not default_branch:
            return

        branch_id = default_branch["id"]
        tenant_id = default_branch["tenant_id"]

        cursor.execute("""
            UPDATE users
            SET branch_id = %s
            WHERE branch_id IS NULL
            AND tenant_id = %s
            AND role NOT IN ('super_admin','auditor')
            """,
            (branch_id, tenant_id)
        )

        cursor.execute("UPDATE customers SET branch_id = %s WHERE branch_id IS NULL AND tenant_id = %s",
            (branch_id, tenant_id)
        )

        cursor.execute("UPDATE loans SET branch_id = %s WHERE branch_id IS NULL AND tenant_id = %s",
            (branch_id, tenant_id)
        )

        cursor.execute("UPDATE payments SET branch_id = %s WHERE branch_id IS NULL AND tenant_id = %s",
            (branch_id, tenant_id)
        )

        cursor.execute("UPDATE emi_schedule SET branch_id = %s WHERE branch_id IS NULL AND tenant_id = %s",
            (branch_id, tenant_id)
        )

        conn.commit()

# fix_null_branches() moved into lifespan() — see above

# ── Background seeding helpers ──────────────────────────────────────

async def _seed_initial_data():
    """Non-blocking background task for first-run data seeding."""
    try:
        await asyncio.to_thread(_sync_seed_initial_data)
    except Exception as e:
        logger.warning("Background seeding failed: %s", e)


def _sync_seed_initial_data():
    """Synchronous seeding logic — runs in a thread pool."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()

            # Seed initial repo rate per tenant if needed
            cursor.execute("SELECT id FROM tenants")
            all_tenants = [r['id'] for r in cursor.fetchall()]

            for tid in all_tenants:
                try:
                    if get_latest_repo_rate(str(DB_PATH), tid) is None:
                        initial_rate = fetch_repo_rate()
                        save_repo_rate(str(DB_PATH), initial_rate, tid)
                        logger.debug("Seeded initial repo rate %s%% for tenant %s", initial_rate, tid)
                except Exception as e:
                    logger.warning("Repo rate seeding failed for tenant %s: %s", tid, e)

            # Seed interest rates per tenant if empty (first-run only)
            for tid in all_tenants:
                try:
                    cursor.execute("SELECT COUNT(*) as cnt FROM interest_rates WHERE tenant_id = %s", (tid,))
                    count = cursor.fetchone()['cnt']
                    if count == 0:
                        repo_data = get_latest_repo_rate(str(DB_PATH), tid)
                        repo_rate = repo_data["repo_rate"] if repo_data else DEFAULT_REPO_RATE
                        recalculate_interest_rates(str(DB_PATH), repo_rate, tid)
                        logger.debug("Interest rates seeded for tenant %s with repo rate %s%%", tid, repo_rate)
                    else:
                        logger.debug("Interest rates already initialized for tenant %s (%s records)", tid, count)
                except Exception as e:
                    logger.warning("Interest rate seeding failed for tenant %s: %s", tid, e)
    except Exception as e:
        logger.warning("Data seeding failed: %s", e)


def _check_db_health() -> bool:
    """Synchronous DB health check — used via asyncio.to_thread."""
    try:
        with get_db() as conn:
            conn.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False


# ── Gold Rate Catch-up ───────────────────────────────────────────────
# Runs in a daemon thread after startup so it never blocks the server.
# If the 09:05 scheduler job was missed (e.g. server started late),
# this fetches today's rate automatically.

def run_gold_rate_catchup():
    try:
        from services.gold_rate_service import daily_gold_rate_job

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT 1 FROM gold_rate
                WHERE source = 'auto'
                AND DATE(updated_at) = CURRENT_DATE
                LIMIT 1
            """)
            exists = cur.fetchone()

        if not exists:
            logger.info("Gold rate missing for today — running catch-up job")
            daily_gold_rate_job()
            logger.info("Gold rate catch-up completed")
        else:
            logger.debug("Gold rate already exists for today — skipping catch-up")
    except Exception as e:
        logger.error("Gold rate catch-up failed: %s", e)


def run_repo_rate_catchup():
    """
    Runs in a daemon thread after startup.
    If the 10:00 scheduler job was missed (server started late),
    this fetches today's repo rate automatically for all tenants.
    """
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM tenants")
            tenants = cur.fetchall()

        if not tenants:
            logger.debug("No tenants found — skipping repo rate catch-up")
            return

        for tenant_row in tenants:
            tid = tenant_row["id"]
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT 1 FROM repo_rate_history
                    WHERE tenant_id = %s
                    AND DATE(fetched_at) = CURRENT_DATE
                    LIMIT 1
                """, (tid,))
                exists = cur.fetchone()

            if not exists:
                logger.info("Repo rate missing for today (tenant %s) — running catch-up", tid)
                daily_repo_rate_job(str(DB_PATH))
                logger.info("Repo rate catch-up completed for tenant %s", tid)
                break  # daily_repo_rate_job iterates all tenants internally
            else:
                logger.debug("Repo rate already exists for today (tenant %s) — skipping", tid)
    except Exception as e:
        logger.error("Repo rate catch-up failed: %s", e)


# ── Lifespan ────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = None
    logger.info("SV Fincloud Server is starting up...")

    try:
        if not os.getenv("TESTING"):
            # 1. Pool init (required — fast with connect_timeout=5)
            init_db_pool()

            # 1b. Data migration from Supabase → Railway (one-shot, env-gated)
            if os.getenv("RUN_MIGRATION", "").lower() == "true":
                logger.info("RUN_MIGRATION=true detected — starting data migration...")
                try:
                    from migrate import main as run_migration
                    run_migration()
                    logger.info("Data migration completed successfully.")
                except Exception as _mig_err:
                    logger.error("Data migration FAILED: %s", _mig_err)
                    # Do not abort startup — app still runs after migration attempt

            # 2. Schema migrations (idempotent — ADD COLUMN IF NOT EXISTS)
            init_db()

            # 3. Permissions + branch fixes (fast, no schema changes)
            init_permissions()
            fix_null_branches()

            # 4. Ensure critical columns exist (belt-and-suspenders for live DBs)
            try:
                with get_db() as conn:
                    cur = conn.cursor()
                    cur.execute("ALTER TABLE interest_rates ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'auto'")
                    cur.execute("UPDATE interest_rates SET source = 'manual' WHERE is_overridden = 1 AND source IS NULL")
                    cur.execute("UPDATE interest_rates SET source = 'auto' WHERE (is_overridden = 0 OR is_overridden IS NULL) AND source IS NULL")
                    conn.commit()
                logger.debug("interest_rates.source column ensured")
            except Exception as e:
                logger.warning("interest_rates.source migration warning: %s", e)

            # 4b. Backfill audit_logs: correct wrongly stored rate_source='manual'
            #     Only fixes LOAN_APPLICATION logs where no actual manual rate override
            #     existed at the time the loan was created.
            try:
                with get_db() as conn:
                    cur = conn.cursor()
                    cur.execute("""
                        UPDATE audit_logs al
                        SET details = jsonb_set(
                            al.details::jsonb,
                            '{rate_source}',
                            '"auto"'
                        )
                        WHERE al.action = 'LOAN_APPLICATION'
                          AND al.details IS NOT NULL
                          AND al.details::jsonb->>'rate_source' = 'manual'
                          AND NOT EXISTS (
                              SELECT 1 FROM interest_rates ir
                              WHERE ir.source = 'manual'
                                AND ir.tenant_id = al.tenant_id
                          )
                    """)
                    conn.commit()
                logger.debug("audit_logs rate_source backfill complete")
            except Exception as e:
                logger.warning("audit_logs rate_source backfill skipped: %s", e)

            # 5. Start scheduler (BackgroundScheduler uses its own thread)
            try:
                from services.scheduler_service import create_scheduler
                scheduler = create_scheduler(str(DB_PATH))
                if scheduler:
                    scheduler.start()
                    logger.info("APScheduler started — repo rate (10:00) + gold rate (09:05 / 18:05)")
                    threading.Thread(target=run_gold_rate_catchup, daemon=True).start()
                    threading.Thread(target=run_repo_rate_catchup, daemon=True).start()
            except Exception as e:
                logger.warning("Scheduler failed to start: %s", e)

            # 6. Fire-and-forget: seed data in background (non-blocking)
            asyncio.create_task(_seed_initial_data())
        else:
            logger.info("TESTING mode — skipping scheduler and DB seeding")

        logger.info("SV Fincloud Server is ready")
        yield

    except asyncio.CancelledError:
        # Normal during Ctrl+C / --reload — not an error
        logger.debug("Lifespan cancelled during shutdown")
    except Exception as e:
        logger.error("Lifespan startup error: %s", e)
        raise
    finally:
        logger.info("SV Fincloud Server is shutting down safely...")
        if scheduler:
            try:
                from services.scheduler_service import shutdown_scheduler
                shutdown_scheduler(scheduler)
            except Exception as e:
                logger.warning("Scheduler shutdown error: %s", e)

app = FastAPI(lifespan=lifespan)
api_router = APIRouter(prefix="/api")

# Register CORS before routes so it wraps all requests correctly
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic Models
class LoginRequest(BaseModel):
    username: str
    password: str
    tenant_id: str

class UserResponse(BaseModel):
    id: str
    username: str
    role: str
    tenant_id: str
    branch_id: Optional[str] = None
    branch_name: Optional[str] = None
    tenant_name: str
    logo_url: Optional[str] = None
    primary_color: Optional[str] = "#2536eb"

class RegisterBusinessRequest(BaseModel):
    username: str
    password: str
    company_name: str
    location: str

class LoginResponse(BaseModel):
    token: str
    user: UserResponse

class LoanUpdateRequest(BaseModel):
    interest_rate: Optional[float] = None

class LoanApplicationRequest(BaseModel):
    loan_type: str
    amount: float
    tenure: int
    monthly_income: float
    cibil_score:Optional[int]=None
    vehicle_age: Optional[int] = None
    vehicle_reg_no: Optional[str] = None
    gold_weight: Optional[float] = None

class PaymentRequest(BaseModel):
    emi_id: str
    amount: float
    otp: str

class ApprovalRequest(BaseModel):
    entity_id: str
    action: str

class UserCreateRequest(BaseModel):
    username: str
    password: str
    role: str
    branch_id: Optional[str] = None

    # Common fields (for both employee & customer)
    name: Optional[str] = None  # Maps to full_name in users table
    email: Optional[str] = None
    phone: Optional[str] = None

    # Customer fields
    cibil_score: Optional[int] = None
    monthly_income: Optional[float] = None

    # Employee fields
    designation: Optional[str] = None
    joining_date: Optional[str] = None

    model_config = {"extra": "ignore"}

class BranchCreateRequest(BaseModel):
    name: str
    location: str

class InterestRateUpdateRequest(BaseModel):
    loan_type: str
    category: str
    rate: float

class InterestRateResetRequest(BaseModel):
    loan_type: str
    category: str

class GoldRateUpdateRequest(BaseModel):
    branch_id: str = None
    rate_per_gram: float

class RepoRateUpdateRequest(BaseModel):
    repo_rate: float

class GoldRateModeRequest(BaseModel):
    mode: str

class ConfirmReleaseRequest(BaseModel):
    loan_id: str

# Helper functions
def create_access_token(data: dict):
    to_encode = data.copy()
    expire = get_ist_now() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        logger.debug(f"verify_token called with token: {token[:20]}...")
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        logger.debug(f"Token decoded successfully: {payload}")
        return payload
    except jwt.ExpiredSignatureError:
        logger.debug("Token expired")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.PyJWTError as e:
        logger.debug(f"Invalid token: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")
    

def verify_token_query(token: str = None):
    """Accept JWT via ?token= query param (used for direct download links)."""
    if not token:
        raise HTTPException(status_code=401, detail="Token required")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


def require_role(allowed_roles: List[str]):
    def role_checker(token_data: dict = Depends(verify_token)):
        logger.debug(f"require_role check: allowed={allowed_roles}, role={token_data.get('role')}")
        
        if token_data.get('role') not in allowed_roles:
            logger.debug(f"Role check failed - {token_data.get('role')} not in {allowed_roles}")
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
        
        logger.debug("Role check passed")
        return token_data
    return role_checker

def check_permission(role, module, action):
    """
    Check if a role has permission to perform an action on a module.
    Uses its own isolated DB connection to avoid nesting inside a caller's transaction.
    """
    logger.debug("check_permission called: role=%s, module=%s, action=%s", role, module, action)

    # Super admin bypass — always allow
    if role == 'super_admin':
        return True

    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT p.*
                FROM permissions p
                JOIN roles r ON p.role_id = r.role_id
                JOIN modules m ON p.module_id = m.module_id
                WHERE r.role_name = %s AND m.module_name = %s
            """, (role, module))
            perm = cursor.fetchone()

            if not perm:
                logger.debug("No permission row found for role=%s module=%s — access denied", role, module)
                return False

            column_name = f'can_{action}'
            has_permission = bool(perm.get(column_name, 0))
            logger.debug("Permission check: %s.%s → %s", module, action, has_permission)
            return has_permission
    except Exception as e:
        logger.warning("check_permission failed (role=%s module=%s action=%s): %s — defaulting to False", role, module, action, e)
        return False

def log_audit(conn, user_id: str, tenant_id: str, action: str, entity_type: str, entity_id: str = None, details: str = None):
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO audit_logs 
        (id, user_id, tenant_id, action, entity_type, entity_id, details, created_at) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            str(uuid.uuid4()),
            user_id,
            tenant_id,
            action,
            entity_type,
            entity_id,
            details,
            get_ist_now().isoformat()
        )
    )
def apply_penalty_if_overdue(conn,branch_id,tenant_id):
    cursor = conn.cursor()
    today = get_ist_now().date()

    # Fetch all pending EMIs
    cursor.execute("""
        SELECT id, loan_id, emi_amount, due_date
        FROM emi_schedule
        WHERE status = 'pending'
        AND branch_id = %s
        AND tenant_id = %s
    """, (branch_id, tenant_id))
    emis = cursor.fetchall()

    for emi in emis:
        due_date = datetime.fromisoformat(emi['due_date']).date()

        # Check if EMI is overdue
        if today > due_date:
            # Check if penalty already applied for this EMI
            cursor.execute("SELECT 1 FROM penalties WHERE emi_id = %s",
                (emi['id'],)
            )
            if cursor.fetchone():
                continue  # Penalty already exists, skip

            # Calculate penalty (2% of EMI)
            penalty_amount = round(emi['emi_amount'] * 0.02, 2)

            # Insert penalty record
            cursor.execute("""
                INSERT INTO penalties (
                    id, loan_id, emi_id, amount, reason, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                str(uuid.uuid4()),
                emi['loan_id'],
                emi['id'],
                penalty_amount,
                "Overdue EMI Penalty (2%)",
                get_ist_now().isoformat()
            ))

            # OPTIONAL: Update EMI table penalty column (for quick UI display)
            cursor.execute("""
                UPDATE emi_schedule
                SET penalty = %s
                WHERE id = %s
            """, (penalty_amount, emi['id']))
def generate_company_prefix(company_name: str):
    words = company_name.strip().split()

    first_word = words[0].lower()

    # If first word is already short like SV, JSR → use it
    if len(first_word) <= 4:
        return first_word

    # Else take first letters of words
    return "".join(word[0] for word in words).lower()

# ── Health Check ────────────────────────────────────────────────────

@api_router.get("/health")
async def health_check():
    """Non-blocking health check with DB connectivity test (3s timeout)."""
    db_ok = False
    try:
        db_ok = await asyncio.wait_for(
            asyncio.to_thread(_check_db_health), timeout=3.0
        )
    except asyncio.TimeoutError:
        logger.warning("Health check: DB connection timed out (3s)")
    except Exception as e:
        logger.warning("Health check: DB connection failed: %s", e)

    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "timeout",
    }


# Authentication Routes
@api_router.post("/auth/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    with get_db() as conn:
        cursor = conn.cursor()

        # 1️⃣ FIND USER
        cursor.execute("""
            SELECT * FROM users
            WHERE username = %s AND tenant_id = %s
        """, (request.username, request.tenant_id))

        user = cursor.fetchone()

        # ❌ IF USER NOT FOUND → STOP HERE
        if not user:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        # 2️⃣ CHECK PASSWORD
        stored_password = user['password']

        # Convert to bytes if stored as string
        if isinstance(stored_password, str):
            stored_password = stored_password.encode('utf-8')

        if not bcrypt.checkpw(request.password.encode('utf-8'), stored_password):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        # 3️⃣ CREATE TOKEN
        token = create_access_token({
            "user_id": user['id'],
            "username": user['username'],
            "role": user['role'],
            "tenant_id": user['tenant_id'],
            "branch_id": user['branch_id']
        })

        # 4️⃣ GET TENANT INFO
        cursor.execute("SELECT name, logo_url, primary_color FROM tenants WHERE id = %s",
            (user['tenant_id'],)
        )
        tenant = cursor.fetchone()

        # 5️⃣ ⭐ GET BRANCH INFO (ADD HERE ONLY)
        branch = None
        if user['branch_id']:
            cursor.execute("SELECT name FROM branches WHERE id = %s",
                (user['branch_id'],)
            )
            branch = cursor.fetchone()

        # 6️⃣ RETURN USER DATA
        user_data = {
            "id": user['id'],
            "username": user['username'],
            "role": user['role'],
            "tenant_id": user['tenant_id'],
            "branch_id": user['branch_id'],
            "branch_name": branch["name"] if branch else None,  # ✅ FIXED

            "tenant_name": tenant["name"] if tenant else "Unknown",
            "logo_url": tenant["logo_url"] if tenant else None,
            "primary_color": tenant["primary_color"] if tenant else "#2536eb"
        }

        log_audit(conn, user['id'], user['tenant_id'], 'LOGIN', 'user', user['id'])

        return LoginResponse(token=token, user=user_data)

@api_router.get("/auth/me")
async def get_current_user(token_data: dict = Depends(verify_token)):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, role, tenant_id FROM users WHERE id = %s", (token_data['user_id'],))
        user = cursor.fetchone()
        
        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
        
        return dict(user)
# NEW: Customer self-registration
@api_router.post("/auth/signup")
async def signup(request: UserCreateRequest):
    with get_db() as conn:
        cursor = conn.cursor()        
        # ✅ Get tenant_id from selected branch
        cursor.execute("SELECT tenant_id FROM branches WHERE id = %s",
            (request.branch_id,)
        )
        # 2. Get tenant_id from selected branch ✅
        tenant_row = cursor.fetchone()

        if not tenant_row:
            raise HTTPException(status_code=400, detail="Invalid branch")

        tenant_id = tenant_row["tenant_id"]
        # NOW check username
        cursor.execute("SELECT id FROM users WHERE username = %s AND tenant_id = %s",
            (request.username, tenant_id)
        )
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Username already taken")

        # 3. Create user record
        user_id = str(uuid.uuid4())
        hashed = bcrypt.hashpw(request.password.encode('utf-8'), bcrypt.gensalt()).decode()
        
        cursor.execute("""
            INSERT INTO users (id, username, password, role, tenant_id, branch_id, full_name, phone, email, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (user_id, request.username, hashed, 'customer', tenant_id, request.branch_id,
             request.name, request.phone, request.email, get_ist_now().isoformat())
        )

        # 4. Create customer profile
        cursor.execute("INSERT INTO customers (id, user_id, name, cibil_score, branch_id, tenant_id, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (str(uuid.uuid4()), user_id, request.name, request.cibil_score, request.branch_id, tenant_id, get_ist_now().isoformat())
        )
        
        conn.commit()
        return {"message": "Registration successful! Please login."}

@api_router.post("/auth/register-business")
async def register_business(request: RegisterBusinessRequest):
    with get_db() as conn:
        cursor = conn.cursor()

        # 🏢 Create Tenant
        tenant_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO tenants (id, name, created_at)
            VALUES (%s, %s, %s)
        """, (
            tenant_id,
            request.company_name,
            get_ist_now().isoformat()
        ))

        # 🏬 Create Main Branch
        branch_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO branches (id, name, location, tenant_id, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            branch_id,
            f"{request.company_name} Main Branch",
            request.location,
            tenant_id,
            get_ist_now().isoformat()
        ))
        # Check username inside this company
        cursor.execute("""
            SELECT id FROM users
            WHERE username = %s AND tenant_id = %s
        """, (request.username,tenant_id))

        if cursor.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Username already exists in this company"
            )
        # 👑 CREATE SUPER ADMIN (NO BRANCH)
        super_admin_id = str(uuid.uuid4())
        hashed = bcrypt.hashpw(request.password.encode(), bcrypt.gensalt()).decode()

        cursor.execute("""
            INSERT INTO users (
                id, username, password, role,
                tenant_id, branch_id, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            super_admin_id,
            request.username,
            hashed,
            "super_admin",
            tenant_id,
            None,  # ❗ No branch
            get_ist_now().isoformat()
        ))

        # 🕵️ CREATE AUDITOR (ONE PER COMPANY)
       
        cursor.execute("""
            SELECT id FROM users
            WHERE role = 'auditor' AND tenant_id = %s
        """, (tenant_id,))

        if cursor.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Auditor already exists for this company"
            )
        auditor_id = str(uuid.uuid4())

        prefix = generate_company_prefix(request.company_name)

        auditor_username = f"{prefix}_auditor"

        auditor_pass = bcrypt.hashpw("auditor123".encode(), bcrypt.gensalt()).decode()
        cursor.execute("""
            INSERT INTO users (
                id, username, password, role,
                tenant_id, branch_id, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            auditor_id,
            auditor_username,
            auditor_pass,
            "auditor",
            tenant_id,
            None,
            get_ist_now().isoformat()
        ))

        # Seed per-tenant sequences
        for lt in ('gold_loan', 'personal_loan', 'vehicle_loan'):
            cursor.execute("""
                INSERT INTO loan_sequences (tenant_id, loan_type, current_value)
                VALUES (%s, %s, 0) ON CONFLICT DO NOTHING
            """, (tenant_id, lt))
        cursor.execute("""
            INSERT INTO payment_sequence (tenant_id, current_value) VALUES (%s, 0) ON CONFLICT DO NOTHING
        """, (tenant_id,))
        cursor.execute("""
            INSERT INTO receipt_sequence (tenant_id, current_value) VALUES (%s, 0) ON CONFLICT DO NOTHING
        """, (tenant_id,))

        conn.commit()

        return {
            "message": "Business registered successfully",
            "tenant_id": tenant_id,
            "super_admin_username": request.username,
            "auditor_username": auditor_username,
            "auditor_password": "auditor123"
        }

@api_router.get("/auth/public-tenants")
async def get_public_tenants():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""SELECT id, name, primary_color, tagline, stats_json FROM tenants""")
        return cursor.fetchall()

    
@api_router.get("/auth/public-branches/{tenant_id}")
async def get_public_branches(tenant_id: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, location FROM branches WHERE tenant_id = %s", (tenant_id,))
        return cursor.fetchall()
    
# Customer Routes
@api_router.post("/customer/loan-application")
async def apply_for_loan(
    request: LoanApplicationRequest,
    token_data: dict = Depends(require_role(['customer']))
):
    if not check_permission(token_data['role'], 'loans', 'insert'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        
        # 1️⃣ Fetch customer
        cursor.execute("""
            SELECT id, cibil_score, name 
            FROM customers 
            WHERE user_id = %s 
            AND tenant_id = %s
            """,
            (token_data['user_id'], token_data['tenant_id'])
        )
        customer = cursor.fetchone()
        if not customer:
            raise HTTPException(status_code=404, detail="Customer profile not found")
        branch_id = token_data["branch_id"]
        tenant_id = token_data["tenant_id"]

        # 2️⃣ Tenure validation
        if request.tenure > 24:
            raise HTTPException(
                status_code=400,
                detail="Maximum loan tenure allowed is 24 months"
            )

        # 3️⃣ Init
        loan_status = "pending"
        log_details = "Standard application"
        interest_rate = 0.0
        rate_source = "auto"  # Track whether rate is manual or dynamic

        cibil = (
            request.cibil_score
            if hasattr(request, "cibil_score") and request.cibil_score is not None
            else customer["cibil_score"] or 0
        )

        # ═══════════════════════════════════════════════
        # 3.5️⃣ RISK-BASED MAX TENURE ENFORCEMENT
        # ═══════════════════════════════════════════════
        risk_inputs = {}

        if request.loan_type == "personal_loan":
            risk_inputs["cibil_score"] = cibil
        elif request.loan_type == "vehicle_loan":
            risk_inputs["vehicle_age"] = request.vehicle_age or 0
        elif request.loan_type == "gold_loan":
            risk_inputs["loan_amount"] = request.amount

        max_tenure = calculate_max_tenure(request.loan_type, risk_inputs)

        if max_tenure == -1:
            return {
                "status": "rejected",
                "message": f"Loan rejected due to risk assessment (CIBIL: {cibil})"
            }

        if request.tenure > max_tenure:
            raise HTTPException(
                status_code=400,
                detail=f"Requested tenure ({request.tenure} months) exceeds maximum allowed ({max_tenure} months) based on risk assessment."
            )

        # ================= PERSONAL LOAN =================
        if request.loan_type == "personal_loan":
            if cibil < 650:
                return {
                    "status": "rejected",
                    "message": f"Loan rejected due to low CIBIL score ({cibil}). Minimum required: 650"
                }

            category = "cibil_750_plus" if cibil >= 750 else ("cibil_700_749" if cibil >= 700 else "cibil_650_699")
            loan_status = "pre-approved" if cibil >= 750 else "pending"

            cursor.execute("SELECT rate, COALESCE(source, 'auto') AS source, COALESCE(is_overridden, 0) AS is_overridden FROM interest_rates WHERE tenant_id = %s AND loan_type = %s AND category = %s",
                (token_data["tenant_id"], "personal_loan", category)
            )

        # ================= VEHICLE LOAN =================
        elif request.loan_type == "vehicle_loan":
            if request.vehicle_age is None:
                raise HTTPException(status_code=400, detail="Vehicle age required")

            # Vehicle collateral locking — global check across all tenants
            if request.vehicle_reg_no:
                _vno = request.vehicle_reg_no.strip().upper().replace(" ", "")
                cursor.execute("""
                    SELECT id FROM loans
                    WHERE UPPER(REPLACE(COALESCE(vehicle_reg_no, ''), ' ', '')) = %s
                      AND status IN ('pending', 'pre-approved', 'active', 'approved', 'disbursed')
                    """,
                    (_vno,)
                )
                if cursor.fetchone():
                    raise HTTPException(
                        status_code=400,
                        detail="This vehicle is already associated with an active or pending loan in the FinCloud network."
                    )

            # Vehicle owner must match applicant
            if request.vehicle_reg_no:
                from services.vehicle_service import fetch_vehicle_details
                vehicle_info = fetch_vehicle_details(request.vehicle_reg_no)
                if vehicle_info and vehicle_info.get("owner_name", "").strip().lower() != (customer["name"] or "").strip().lower():
                    raise HTTPException(
                        status_code=400,
                        detail="Vehicle owner must match the loan applicant."
                    )

            if request.vehicle_age > 15:
                return {
                    "status": "rejected",
                    "message": f"Loan rejected: Vehicle age {request.vehicle_age} years exceeds limit"
                }

            if request.vehicle_age <= 3:
                category = "age_0_3"
            elif request.vehicle_age <= 6:
                category = "age_4_6"
            else:
                category = "age_7_plus"

            cursor.execute("SELECT rate, COALESCE(source, 'auto') AS source, COALESCE(is_overridden, 0) AS is_overridden FROM interest_rates WHERE tenant_id = %s AND loan_type = %s AND category = %s",
                (token_data["tenant_id"], "vehicle_loan", category)
            )

        # ================= GOLD LOAN =================
        elif request.loan_type == "gold_loan":

            if not request.gold_weight:
                raise HTTPException(status_code=400, detail="Gold weight required")

            # Resolve gold rate using single source of truth (tenant-level, mode-based)
            rate_result = resolve_gold_rate(cursor, token_data["tenant_id"])
            gold_rate = rate_result["rate_per_gram"]
            mode = rate_result["mode"]

            # Fallback to default if no rate found
            if not gold_rate:
                gold_rate = 6500.0

            max_loan = request.gold_weight * gold_rate * 0.75

            if request.amount > max_loan:
                raise HTTPException(
                    status_code=400,
                    detail=f"Loan amount exceeds 75% of gold value. Max: ₹{round(max_loan,2)}"
                )

            cursor.execute("SELECT rate, COALESCE(source, 'auto') AS source, COALESCE(is_overridden, 0) AS is_overridden FROM interest_rates WHERE tenant_id = %s AND loan_type = %s AND category = %s",
                (token_data["tenant_id"], "gold_loan", "standard")
            )

        else:
            raise HTTPException(status_code=400, detail="Invalid loan type")

        # ═══════════════════════════════════════════════
        # 4️⃣ INTEREST RATE: Manual Override → Dynamic Fallback
        # ═══════════════════════════════════════════════
        rate_row = cursor.fetchone()

        if rate_row and rate_row["rate"]:
            interest_rate = rate_row["rate"]
            rate_source = rate_row["source"]  # 'auto' or 'manual' from DB
        else:
            # No rate in table → calculate dynamically
            repo_data = get_latest_repo_rate(str(DB_PATH))
            current_repo = repo_data["repo_rate"] if repo_data else 6.5

            dynamic_result = calculate_interest_rate(
                request.loan_type, risk_inputs, current_repo
            )
            interest_rate = dynamic_result["rate"]
            rate_source = "auto"

        # 5️⃣ EMI Calculation
        total_interest = (request.amount * interest_rate * request.tenure) / (100 * 12)
        emi_amount = (request.amount + total_interest) / request.tenure
        processing_fee = request.amount * 0.05
        disbursed_amount = request.amount - processing_fee

        # 6️⃣ EMI eligibility (30%)
        if emi_amount > request.monthly_income * 0.30:
            return {
                "status": "recommendation",
                "message": "EMI exceeds 30% of monthly income",
                "recommended_amount": request.monthly_income * 10
            }
        # Update customer's monthly income
        cursor.execute("""UPDATE customers SET monthly_income = %s WHERE id = %s""", (request.monthly_income, customer["id"]))
        # 7️⃣ Insert loan
        loan_id = str(uuid.uuid4())

        # Generate loan_number (GL-N / PL-N / VL-N) — gap-filling: lowest unused N per tenant
        prefix_map = {'gold_loan': 'GL', 'personal_loan': 'PL', 'vehicle_loan': 'VL'}
        ln_prefix = prefix_map.get(request.loan_type, 'LN')
        cursor.execute("""
            SELECT COALESCE(
                (
                    -- Find the lowest positive integer not already used
                    SELECT s.n
                    FROM generate_series(1, (
                        SELECT COUNT(*) + 1
                        FROM loans
                        WHERE loan_type = %s AND tenant_id = %s
                          AND loan_number ~ ('^' || %s || '-[0-9]+$')
                    )) AS s(n)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM loans
                        WHERE loan_type = %s AND tenant_id = %s
                          AND loan_number = %s || '-' || s.n
                    )
                    ORDER BY s.n
                    LIMIT 1
                ),
                1
            ) AS next_n
        """, (request.loan_type, tenant_id, ln_prefix,
              request.loan_type, tenant_id, ln_prefix))
        next_n = cursor.fetchone()['next_n']
        loan_number = f"{ln_prefix}-{next_n}"

        cursor.execute("""
            INSERT INTO loans (
                id, customer_id, loan_type, amount, tenure,
                interest_rate, emi_amount, processing_fee,
                disbursed_amount, outstanding_balance,
                status, vehicle_age, gold_weight,
                vehicle_reg_no, loan_number,
                branch_id, tenant_id, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                loan_id, customer["id"], request.loan_type, request.amount, request.tenure,
                interest_rate, emi_amount, processing_fee, disbursed_amount,
                request.amount, loan_status, request.vehicle_age, request.gold_weight,
                request.vehicle_reg_no if request.loan_type == "vehicle_loan" else None,
                loan_number,
                branch_id, tenant_id, get_ist_now().isoformat()
            )
        )
        logger.info("Loan inserted: id=%s type=%s amount=%.2f tenant=%s status=%s", loan_id, request.loan_type, request.amount, tenant_id, loan_status)
        # 8️⃣ Audit
        log_audit(conn, token_data["user_id"], token_data['tenant_id'], "LOAN_APPLICATION", "loan", loan_id,
                  json.dumps({"status": loan_status, "rate_source": rate_source, "interest_rate": interest_rate}))

        return {
            "status": loan_status,
            "message": f"Loan {loan_status} successfully",
            "loan_id": loan_id,
            "loan_number": loan_number,
            "max_tenure": max_tenure,
            "applied_interest_rate": interest_rate,
            "rate_source": rate_source
        }

@api_router.get("/customer/loans")
async def get_customer_loans(token_data: dict = Depends(require_role(['customer']))):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM customers WHERE user_id = %s", (token_data['user_id'],))
        customer = cursor.fetchone()
        
        if not customer:
            return []
        branch_filter, params = get_branch_filter(token_data)

        query = f"""
        SELECT * FROM loans
        WHERE customer_id = %s
        AND tenant_id = %s
        {branch_filter}
        ORDER BY created_at DESC
        """

        cursor.execute(query, (customer['id'], token_data["tenant_id"], *params))
        loans = cursor.fetchall()
        
        return loans

@api_router.get("/customer/emi-schedule/{loan_id}/download")
async def download_emi_schedule_pdf(
    loan_id: str,
    token_data: dict = Depends(require_role(['customer']))
):
    if not check_permission(token_data['role'], 'emi_schedule', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        # Verify ownership
        cursor.execute("""
            SELECT l.loan_number, l.amount, l.tenure, c.name AS customer_name
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            WHERE l.id = %s AND c.user_id = %s AND l.tenant_id = %s
        """, (loan_id, token_data['user_id'], token_data['tenant_id']))
        loan = cursor.fetchone()
        if not loan:
            raise HTTPException(404, "Loan not found")

        cursor.execute("""
            SELECT e.emi_number, e.due_date, e.emi_amount,
                   e.principal_amount, e.interest_amount,
                   COALESCE(p.penalty, 0) AS penalty, e.status
            FROM emi_schedule e
            LEFT JOIN (
                SELECT emi_id, SUM(amount) AS penalty FROM penalties GROUP BY emi_id
            ) p ON e.id = p.emi_id
            WHERE e.loan_id = %s AND e.tenant_id = %s
            ORDER BY e.emi_number
        """, (loan_id, token_data['tenant_id']))
        rows = cursor.fetchall()

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (token_data['tenant_id'],))
        t = cursor.fetchone()
        company = t['name'] if t else "SV Fincloud"

    loan_no = loan['loan_number'] or loan_id[:8].upper()
    customer_name = loan['customer_name'] or ''
    headers = ["EMI No", "Due Date", "EMI Amount", "Principal", "Interest", "Penalty", "Status"]
    table_data = [[_pb(h) for h in headers]]
    for r in rows:
        table_data.append([
            _p(str(r['emi_number'])),
            _p(fmt_date(r['due_date'])),
            _p(fmt_currency(r['emi_amount'])),
            _p(fmt_currency(r['principal_amount'])),
            _p(fmt_currency(r['interest_amount'])),
            _p(fmt_currency(r['penalty'])),
            _p((r['status'] or '').upper()),
        ])

    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * w for w in [0.08, 0.14, 0.16, 0.16, 0.14, 0.14, 0.18]]
    tbl = Table(table_data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(build_table_style())

    buf = io.BytesIO()
    doc = make_doc(buf)
    footer_cb = make_footer_cb(company)
    elements = build_header(company, customer_name, period=None)
    elements.append(Paragraph(f"Loan No: {loan_no}", STYLE_INFO))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph("EMI Schedule", STYLE_TITLE))
    elements.append(Spacer(1, 8))
    elements.append(tbl)
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename=emi_schedule_{loan_no}.pdf",
            "Access-Control-Expose-Headers": "Content-Disposition",
        })


class LoanEstimateRequest(BaseModel):
    loan_type: str
    amount: float
    tenure: float
    rate: float
    emi: float
    interest: float
    total: float
    eligible: Optional[float] = None


@api_router.post("/customer/loan-estimate/download")
async def download_loan_estimate(
    data: LoanEstimateRequest,
    token_data: dict = Depends(require_role(['customer']))
):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (token_data['tenant_id'],))
        t = cursor.fetchone()
        company = t['name'] if t else "SV Fincloud"

        cursor.execute("SELECT name FROM customers WHERE user_id = %s", (token_data['user_id'],))
        c = cursor.fetchone()
        customer_name = c['name'] if c else ''

    loan_type_map = {"gold_loan": "Gold Loan", "personal_loan": "Personal Loan", "vehicle_loan": "Vehicle Loan"}
    loan_label = loan_type_map.get(data.loan_type, data.loan_type.replace('_', ' ').title())

    rows = [
        [_pb("Loan Type"),      _p(loan_label)],
        [_pb("Loan Amount"),    _p(fmt_currency(data.amount))],
        [_pb("Tenure"),         _p(f"{int(data.tenure)} months")],
        [_pb("Interest Rate"),  _p(f"{data.rate}%")],
        [_pb("Monthly EMI"),    _p(fmt_currency(data.emi))],
        [_pb("Total Interest"), _p(fmt_currency(data.interest))],
        [_pb("Total Payable"),  _p(fmt_currency(data.total))],
    ]
    if data.eligible:
        rows.insert(2, [_pb("Eligible Amount"), _p(fmt_currency(data.eligible))])

    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.5, usable_w * 0.5]
    tbl = Table(rows, colWidths=col_widths)
    from reportlab.platypus import TableStyle as TS
    from reportlab.lib import colors as rl_colors
    tbl.setStyle(TS([
        ("FONTNAME",      (0, 0), (0, -1), "DejaVu-Bold"),
        ("FONTNAME",      (1, 0), (1, -1), "DejaVu"),
        ("FONTSIZE",      (0, 0), (-1, -1), 10),
        ("TEXTCOLOR",     (0, 0), (0, -1), rl_colors.HexColor("#0f172a")),
        ("TEXTCOLOR",     (1, 0), (1, -1), rl_colors.HexColor("#334155")),
        ("ROWBACKGROUNDS",(0, 0), (-1, -1), [rl_colors.white, rl_colors.HexColor("#f8fafc")]),
        ("GRID",          (0, 0), (-1, -1), 0.4, rl_colors.HexColor("#e2e8f0")),
        ("TOPPADDING",    (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
    ]))

    buf = io.BytesIO()
    doc = make_doc(buf)
    footer_cb = make_footer_cb(company)
    elements = build_header(company, customer_name, period=None)
    elements.append(Paragraph("Loan Estimate", STYLE_TITLE))
    elements.append(Spacer(1, 10))
    elements.append(tbl)
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={
            "Content-Disposition": "attachment; filename=loan_estimate.pdf",
            "Access-Control-Expose-Headers": "Content-Disposition",
        })


@api_router.get("/customer/financial-report/download")
async def download_financial_report(
    token_data: dict = Depends(require_role(['customer']))
):
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (token_data['tenant_id'],))
        t = cursor.fetchone()
        company = t['name'] if t else "SV Fincloud"

        cursor.execute("SELECT id, name FROM customers WHERE user_id = %s", (token_data['user_id'],))
        customer = cursor.fetchone()
        customer_name = customer['name'] if customer else ''

        cursor.execute("""
            SELECT loan_type, category, rate FROM interest_rates
            WHERE tenant_id = %s ORDER BY loan_type, category
        """, (token_data['tenant_id'],))
        rates = cursor.fetchall()

        cursor.execute("""
            SELECT rate_per_gram, updated_at, source FROM gold_rate
            WHERE tenant_id = %s ORDER BY updated_at DESC LIMIT 1
        """, (token_data['tenant_id'],))
        gold = cursor.fetchone()

    loan_type_map = {"gold_loan": "Gold Loan", "personal_loan": "Personal Loan", "vehicle_loan": "Vehicle Loan"}

    rate_headers = ["Loan Type", "Category", "Interest Rate"]
    rate_rows = []
    for r in rates:
        rate_rows.append([
            _p(loan_type_map.get(r['loan_type'], r['loan_type'])),
            _p((r['category'] or '').replace('_', ' ').title()),
            _p(f"{r['rate']}%"),
        ])

    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * w for w in [0.35, 0.40, 0.25]]
    rate_tbl = Table([[_pb(h) for h in rate_headers]] + rate_rows, colWidths=col_widths, repeatRows=1)
    rate_tbl.setStyle(build_table_style())

    buf = io.BytesIO()
    doc = make_doc(buf)
    footer_cb = make_footer_cb(company)
    elements = build_header(company, customer_name, period=None)
    elements.append(Paragraph("Financial Information Report", STYLE_TITLE))
    elements.append(Spacer(1, 6))

    if gold:
        elements.append(Paragraph(
            f"<b>Gold Rate:</b>  {fmt_currency(gold['rate_per_gram'])} / gram  "
            f"({(gold['source'] or 'manual').title()} — {fmt_date(gold['updated_at'])})",
            STYLE_INFO
        ))
        elements.append(Spacer(1, 10))

    elements.append(Paragraph("Interest Rates", STYLE_TITLE))
    elements.append(Spacer(1, 6))
    elements.append(rate_tbl)
    elements.append(Spacer(1, 14))

    policy_rows = [
        [_pb("Minimum CIBIL Score"),              _p("650")],
        [_pb("Max Tenure (Personal / Gold Loan)"), _p("24 months")],
        [_pb("Vehicle Loan Tenure"),               _p("Depends on vehicle age")],
        [_pb("Gold Loan Maximum"),                 _p("75% of gold value")],
        [_pb("EMI Cap"),                           _p("30% of monthly income")],
    ]
    policy_col_widths = [usable_w * 0.6, usable_w * 0.4]
    policy_tbl = Table([[_pb("Policy"), _pb("Value")]] + policy_rows, colWidths=policy_col_widths, repeatRows=1)
    policy_tbl.setStyle(build_table_style())
    elements.append(Paragraph("Loan Policies", STYLE_TITLE))
    elements.append(Spacer(1, 6))
    elements.append(policy_tbl)

    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={
            "Content-Disposition": "attachment; filename=financial_report.pdf",
            "Access-Control-Expose-Headers": "Content-Disposition",
        })


@api_router.get("/customer/my-loans/download")
async def download_my_loans_pdf(    token_data: dict = Depends(require_role(['customer']))
):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM customers WHERE user_id = %s", (token_data['user_id'],))
        customer = cursor.fetchone()
        if not customer:
            raise HTTPException(404, "Customer not found")

        branch_filter, params = get_branch_filter(token_data)
        cursor.execute(f"""
            SELECT loan_number, loan_type, amount, tenure, emi_amount,
                   outstanding_balance, status
            FROM loans
            WHERE customer_id = %s AND tenant_id = %s {branch_filter}
            ORDER BY created_at DESC
        """, (customer['id'], token_data['tenant_id'], *params))
        rows = cursor.fetchall()

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (token_data['tenant_id'],))
        t = cursor.fetchone()
        company = t['name'] if t else "SV Fincloud"

    loan_type_map = {"gold_loan": "Gold Loan", "personal_loan": "Personal Loan", "vehicle_loan": "Vehicle Loan"}
    headers = ["Loan No", "Type", "Amount", "Tenure", "EMI", "Outstanding", "Status"]
    table_data = [[_pb(h) for h in headers]]
    for r in rows:
        table_data.append([
            _p(r['loan_number'] or '-'),
            _p(loan_type_map.get(r['loan_type'], r['loan_type'] or '-')),
            _p(fmt_currency(r['amount'])),
            _p(f"{r['tenure']} mo"),
            _p(fmt_currency(r['emi_amount'])),
            _p(fmt_currency(r['outstanding_balance'])),
            _p((r['status'] or '').upper()),
        ])

    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * w for w in [0.16, 0.14, 0.14, 0.10, 0.14, 0.16, 0.16]]
    tbl = Table(table_data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(build_table_style())

    buf = io.BytesIO()
    doc = make_doc(buf)
    footer_cb = make_footer_cb(company)
    elements = build_header(company, customer['name'], period=None)
    elements.append(Paragraph("My Loans", STYLE_TITLE))
    elements.append(Spacer(1, 8))
    elements.append(tbl)
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={
            "Content-Disposition": "attachment; filename=my_loans.pdf",
            "Access-Control-Expose-Headers": "Content-Disposition",
        })


@api_router.get("/customer/emi-schedule/{loan_id}")
async def get_emi_schedule(loan_id: str, token_data: dict = Depends(require_role(['customer', 'collection_agent', 'finance_officer', 'auditor']))):
    if not check_permission(token_data['role'], 'emi_schedule', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter, params = get_branch_filter(token_data, "e")

        query = f"""
        SELECT e.*, COALESCE(p.penalty, 0) as penalty
        FROM emi_schedule e
        LEFT JOIN (
            SELECT emi_id, SUM(amount) as penalty
            FROM penalties
            GROUP BY emi_id
        ) p ON e.id = p.emi_id
        WHERE e.loan_id = %s
        AND e.tenant_id = %s
        {branch_filter}
        ORDER BY e.emi_number
        """

        cursor.execute(query, (loan_id, token_data["tenant_id"], *params))

        schedule = cursor.fetchall()
        return schedule


@api_router.get("/customer/payment-history/{loan_id}")
async def get_payment_history(loan_id: str, token_data: dict = Depends(require_role(['customer', 'collection_agent', 'finance_officer', 'auditor']))):
    if not check_permission(token_data['role'], 'payments', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter, params = get_branch_filter(token_data, "l")

        query = f"""
        SELECT p.*
        FROM payments p
        JOIN loans l ON p.loan_id = l.id
        WHERE p.loan_id = %s
        AND l.tenant_id = %s
        {branch_filter}
        ORDER BY p.created_at DESC
        """

        cursor.execute(query, (loan_id, token_data["tenant_id"], *params))
        payments = cursor.fetchall()
        return payments
        
@api_router.delete("/customer/loans/{loan_id}")
async def delete_loan(loan_id: str, token_data: dict = Depends(require_role(['customer']))):
    with get_db() as conn:
        cursor = conn.cursor()
        
        # 1. First, check if the loan belongs to this customer AND is still pending
        cursor.execute("""
            SELECT l.id, l.status
            FROM loans l
            INNER JOIN customers c ON l.customer_id = c.id
            WHERE l.id = %s
            AND c.user_id = %s
            AND l.branch_id = %s
            AND l.tenant_id = %s
        """, (loan_id, token_data['user_id'], token_data["branch_id"], token_data["tenant_id"]))
        
        loan = cursor.fetchone()
        
        if not loan:
            raise HTTPException(status_code=404, detail="Loan application not found")
        
        if loan['status'].lower() != 'pending':
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot delete a loan with status: {loan['status']}. Only 'pending' loans can be removed."
            )
        
        # 2. Perform the deletion
        cursor.execute("DELETE FROM payments WHERE loan_id = %s", (loan_id,))
        cursor.execute("DELETE FROM emi_schedule WHERE loan_id = %s", (loan_id,))
        cursor.execute("DELETE FROM loans WHERE id = %s", (loan_id,))
        
        # 3. Log the action
        log_audit(conn, token_data['user_id'], token_data['tenant_id'],'LOAN_DELETED', 'loan', loan_id)
        
        return {"message": "Loan application deleted successfully"}
    
def _fetch_receipt_data(emi_id: str, token_data: dict) -> dict:
    """Shared helper: fetch structured receipt data for an EMI."""
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT e.id AS emi_id, e.emi_number, l.id AS loan_id, l.loan_type
            FROM emi_schedule e
            JOIN loans l ON e.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            WHERE e.id = %s
            AND c.user_id = %s
            AND l.branch_id = %s
            AND l.tenant_id = %s
        """, (emi_id, token_data['user_id'], token_data["branch_id"], token_data["tenant_id"]))
        emi = cursor.fetchone()
        if not emi:
            raise HTTPException(status_code=404, detail="EMI not found")

        branch_filter, params = get_branch_filter(token_data, "l")
        query = f"""
        SELECT
            p.receipt_no,
            p.id AS payment_id,
            p.amount,
            p.payment_date,
            p.remaining_emi_after_payment,
            p.balance_after_payment,
            e.emi_number,
            e.principal_amount,
            e.interest_amount,
            l.id AS loan_id,
            l.loan_number,
            c.name AS customer_name
        FROM payments p
        JOIN emi_schedule e ON p.emi_id = e.id
        JOIN loans l ON p.loan_id = l.id
        JOIN customers c ON l.customer_id = c.id
        WHERE p.emi_id = %s
        AND c.user_id = %s
        AND p.status = 'approved'
        AND l.tenant_id = %s
        {branch_filter}
        ORDER BY p.created_at DESC
        LIMIT 1
        """
        cursor.execute(query, (emi_id, token_data['user_id'], token_data["tenant_id"], *params))
        payment = cursor.fetchone()
        if not payment:
            raise HTTPException(status_code=400, detail="Receipt not available")

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (token_data["tenant_id"],))
        tenant = cursor.fetchone()
        tenant_name = tenant["name"] if tenant else "SV Fincloud"

        principal = float(payment['principal_amount'] or 0)
        interest = float(payment['interest_amount'] or 0)
        total_paid = float(payment['amount'] or 0)
        # Fallback: if principal/interest not stored, split proportionally
        if principal == 0 and interest == 0 and total_paid > 0:
            principal = round(total_paid * 0.85, 2)
            interest = round(total_paid - principal, 2)

        loan_type_map = {
            "gold_loan": "Gold Loan",
            "personal_loan": "Personal Loan",
            "vehicle_loan": "Vehicle Loan",
        }
        loan_type_label = loan_type_map.get(emi['loan_type'], emi['loan_type'].replace("_", " ").title())

        return {
            "company_name": tenant_name,
            "receipt_no": payment['receipt_no'] or f"REC-{payment['payment_id'][:8].upper()}",
            "date_time": payment['payment_date'],
            "customer": {
                "name": payment['customer_name'],
            },
            "loan": {
                "loan_no": payment.get('loan_number') or emi['loan_id'],
                "loan_type": loan_type_label,
                "emi_number": emi['emi_number'],
            },
            "payment": {
                "amount_paid": total_paid,
                "transaction_id": f"TXN-{payment['payment_id'][:10].upper()}",
            },
            "emi_breakdown": {
                "principal": principal,
                "interest": interest,
            },
            "summary": {
                "remaining_emi": payment['remaining_emi_after_payment'],
                "remaining_balance": float(payment['balance_after_payment'] or 0),
            },
            "status": "SUCCESSFUL",
        }


@api_router.get("/customer/receipt/{emi_id}")
async def get_emi_receipt(
    emi_id: str,
    token_data: dict = Depends(require_role(['customer']))
):
    if not check_permission(token_data['role'], 'payments', 'view'):
        raise HTTPException(403, "Access denied")
    return _fetch_receipt_data(emi_id, token_data)


@api_router.get("/customer/receipt/{emi_id}/download")
async def download_emi_receipt_pdf(
    emi_id: str,
    token_data: dict = Depends(require_role(['customer']))
):
    if not check_permission(token_data['role'], 'payments', 'view'):
        raise HTTPException(403, "Access denied")

    r = _fetch_receipt_data(emi_id, token_data)

    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib import colors as rl_colors

    BRAND = rl_colors.HexColor("#4f46e5")
    LIGHT = rl_colors.HexColor("#f1f5f9")
    GRID  = rl_colors.HexColor("#e2e8f0")
    MUTED = rl_colors.HexColor("#64748b")
    SUCCESS = rl_colors.HexColor("#16a34a")

    def ps(name, **kw):
        defaults = dict(fontName="DejaVu", fontSize=10, spaceAfter=3, leading=14)
        defaults.update(kw)
        return ParagraphStyle(name, **defaults)

    sTitle   = ps("rTitle",   fontName="DejaVu-Bold", fontSize=18, alignment=1, textColor=BRAND, spaceAfter=2)
    sSub     = ps("rSub",     fontSize=10, alignment=1, textColor=MUTED, spaceAfter=10)
    sSection = ps("rSection", fontName="DejaVu-Bold", fontSize=10, textColor=BRAND, spaceAfter=4)
    sNormal  = ps("rNormal",  fontSize=9, textColor=rl_colors.HexColor("#334155"))
    sFooter  = ps("rFooter",  fontSize=8, alignment=1, textColor=MUTED, spaceAfter=0)
    sSuccess = ps("rSuccess", fontName="DejaVu-Bold", fontSize=11, alignment=1, textColor=SUCCESS, spaceAfter=6)

    def kv_table(rows):
        tbl = Table(rows, colWidths=[200, 220])
        tbl.setStyle(TableStyle([
            ("FONTNAME",      (0, 0), (0, -1), "DejaVu-Bold"),
            ("FONTNAME",      (1, 0), (1, -1), "DejaVu"),
            ("FONTSIZE",      (0, 0), (-1, -1), 9),
            ("TEXTCOLOR",     (0, 0), (0, -1), rl_colors.HexColor("#0f172a")),
            ("TEXTCOLOR",     (1, 0), (1, -1), rl_colors.HexColor("#334155")),
            ("ROWBACKGROUNDS",(0, 0), (-1, -1), [rl_colors.white, LIGHT]),
            ("GRID",          (0, 0), (-1, -1), 0.4, GRID),
            ("TOPPADDING",    (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
        ]))
        return tbl

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        rightMargin=60, leftMargin=60,
        topMargin=50, bottomMargin=50,
    )

    date_str = fmt_datetime(r["date_time"])
    elems = [
        Paragraph(r["company_name"].upper(), sTitle),
        Paragraph("Payment Receipt", sSub),
        HRFlowable(width="100%", thickness=1.5, color=BRAND),
        Spacer(1, 10),
        Paragraph(f"\u2714 {r['status']}", sSuccess),
        Spacer(1, 6),

        Paragraph("Receipt Details", sSection),
        kv_table([
            ["Receipt No",      r["receipt_no"]],
            ["Transaction ID",  r["payment"]["transaction_id"]],
            ["Date & Time",     date_str],
        ]),
        Spacer(1, 10),

        Paragraph("Customer", sSection),
        kv_table([
            ["Name", r["customer"]["name"]],
        ]),
        Spacer(1, 10),

        Paragraph("Loan Details", sSection),
        kv_table([
            ["Loan No",   r["loan"]["loan_no"]],
            ["Loan Type", r["loan"]["loan_type"]],
            ["EMI No",    str(r["loan"]["emi_number"])],
        ]),
        Spacer(1, 10),

        Paragraph("Payment Breakdown", sSection),
        kv_table([
            ["Principal Paid",  fmt_currency(r["emi_breakdown"]["principal"])],
            ["Interest Paid",   fmt_currency(r["emi_breakdown"]["interest"])],
            ["Total Paid",      fmt_currency(r["payment"]["amount_paid"])],
        ]),
        Spacer(1, 10),

        Paragraph("Loan Summary", sSection),
        kv_table([
            ["Remaining EMIs",    str(r["summary"]["remaining_emi"])],
            ["Remaining Balance", fmt_currency(r["summary"]["remaining_balance"])],
        ]),
        Spacer(1, 16),
        HRFlowable(width="100%", thickness=0.5, color=GRID),
        Spacer(1, 6),
        Paragraph("Thank you for your payment. This is a system-generated receipt.", sFooter),
    ]

    doc.build(elems)
    buf.seek(0)
    receipt_no = r["receipt_no"].replace("/", "-")
    return StreamingResponse(
        buf, media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename=receipt_{receipt_no}.pdf",
            "Access-Control-Expose-Headers": "Content-Disposition",
        }
    )



# Collection Agent Routes
@api_router.get("/agent/customers")
async def get_assigned_customers(token_data: dict = Depends(require_role(['collection_agent']))):
    if not check_permission(token_data['role'], 'customers', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter, params = get_branch_filter(token_data, "l")

        query = f"""
        SELECT DISTINCT 
            c.id, c.name, c.cibil_score, c.monthly_income, c.address,
            c.branch_id, c.tenant_id, c.created_at,
            COALESCE(u.phone, '') AS phone,
            l.id AS loan_id, 
            l.loan_type, 
            l.amount, 
            l.outstanding_balance, 
            l.status,
            l.loan_number
        FROM customers c
        INNER JOIN loans l ON c.id = l.customer_id
        LEFT JOIN users u ON c.user_id = u.id
        WHERE l.status = 'active'
        AND l.tenant_id = %s
        {branch_filter}
        ORDER BY c.name
        """

        cursor.execute(query, (token_data["tenant_id"], *params))
        return cursor.fetchall()

@api_router.post("/agent/enter-payment")
async def enter_payment(
    request: PaymentRequest,
    token_data: dict = Depends(require_role(['collection_agent']))
):
    if not check_permission(token_data['role'], 'payments', 'insert'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_id = token_data["branch_id"]
        tenant_id = token_data["tenant_id"]
        
        # 1️⃣ Validate EMI
        branch_filter, params = get_branch_filter(token_data, "l")

        query = f"""
        SELECT e.id, e.loan_id, e.status
        FROM emi_schedule e
        JOIN loans l ON e.loan_id = l.id
        WHERE e.id = %s
        AND l.tenant_id = %s
        {branch_filter}
"""

        cursor.execute(query, (request.emi_id, token_data["tenant_id"], *params))       
        emi = cursor.fetchone()

        if not emi:
            raise HTTPException(status_code=404, detail="EMI not found")

        if emi["status"] != "pending":
            raise HTTPException(
                status_code=400,
                detail="EMI already paid or payment in progress"
            )

        # OTP verification
        DEMO_OTP = "1234"
        if request.otp != DEMO_OTP:
            raise HTTPException(status_code=400, detail="Invalid OTP. Customer verification failed.")

        payment_id = str(uuid.uuid4())

        # 2️⃣ ATOMIC INSERT (DB WILL BLOCK DUPLICATES)
        try:
            # Generate payment_number — global per tenant
            cursor.execute("""
                INSERT INTO payment_sequence (tenant_id, current_value)
                VALUES (%s, 1)
                ON CONFLICT (tenant_id)
                DO UPDATE SET current_value = payment_sequence.current_value + 1
                RETURNING current_value
            """, (tenant_id,))
            pn_row = cursor.fetchone()
            payment_number = f"PAY-{pn_row['current_value']}" if pn_row else None

            cursor.execute("""
                INSERT INTO payments (
                    id, loan_id, emi_id, amount,
                    status, collected_by, branch_id, tenant_id,
                    created_at, payment_date, payment_number
                )
                VALUES (%s, %s, %s, %s, 'pending', %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s)
            """, (
                    payment_id,
                    emi["loan_id"],
                    request.emi_id,
                    request.amount,
                    token_data["user_id"],
                    branch_id,
                    tenant_id,
                    payment_number,
            ))

            # 3️⃣ LOCK EMI AFTER INSERT
            cursor.execute("""
                UPDATE emi_schedule
                SET status = 'pending_payment'
                WHERE id = %s
            """, (request.emi_id,))

            conn.commit()

        except Exception:
            conn.rollback()
            raise HTTPException(
                status_code=400,
                detail="Payment already submitted for this EMI"
            )

        return {
            "message": "Payment submitted for approval",
            "payment_id": payment_id
        }

@api_router.get("/agent/daily-collection-list")
async def get_daily_collection_list(token_data: dict = Depends(require_role(['collection_agent']))):
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        logger.debug("Fetching daily collections for branch=%s tenant=%s", branch_id, tenant_id)
        cursor.execute("""
            SELECT c.name AS customer_name,
                   COALESCE(u.phone, '') AS phone,
                   l.id AS loan_id, l.loan_number, l.outstanding_balance,
                   e.id AS emi_id, e.due_date, e.emi_amount,
                   e.status,
                   COALESCE(e.collection_remark, '-') AS collection_remark,
                   e.followup_date,
                   p.payment_date AS today_payment_date
            FROM emi_schedule e
            JOIN loans l ON e.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            JOIN users u ON c.user_id = u.id
            LEFT JOIN payments p ON p.emi_id = e.id AND DATE(p.payment_date) = CURRENT_DATE
            WHERE l.tenant_id = %s
              AND l.branch_id = %s
              AND (
                  CAST(e.due_date AS DATE) BETWEEN CURRENT_DATE - INTERVAL '5 days' AND CURRENT_DATE + INTERVAL '2 days'
                  OR DATE(p.payment_date) = CURRENT_DATE
              )
            ORDER BY
                CASE WHEN CAST(e.due_date AS DATE) < CURRENT_DATE THEN 0 ELSE 1 END,
                CAST(e.due_date AS DATE) ASC
        """, (tenant_id, branch_id))
        return [dict(row) for row in cursor.fetchall()]


@api_router.get("/agent/my-collection-stats")
async def get_my_collection_stats(token_data: dict = Depends(require_role(['collection_agent']))):
    user_id = token_data["user_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0) AS total_collected_today
            FROM payments
            WHERE collected_by = %s AND DATE(payment_date) = CURRENT_DATE
        """, (user_id,))
        total_collected_today = cursor.fetchone()["total_collected_today"]

        cursor.execute("""
            SELECT COUNT(*) AS pending_approvals
            FROM payments
            WHERE collected_by = %s AND status = 'pending'
        """, (user_id,))
        pending_approvals = cursor.fetchone()["pending_approvals"]

        return {
            "total_collected_today": total_collected_today,
            "pending_approvals": pending_approvals,
        }


@api_router.get("/agent/collection-history")
async def get_collection_history(
    token_data: dict = Depends(require_role(['collection_agent']))
):
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                p.id          AS payment_id,
                p.payment_number,
                c.name        AS customer_name,
                COALESCE(u.phone, '')  AS phone,
                l.id          AS loan_id,
                l.loan_number,
                p.amount,
                p.status,
                COALESCE(p.payment_date, p.created_at) AS payment_date
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN users u ON c.user_id = u.id
            WHERE p.tenant_id = %s
              AND p.branch_id = %s
            ORDER BY COALESCE(p.payment_date, p.created_at) DESC
            LIMIT 200
        """, (token_data["tenant_id"], token_data["branch_id"]))

        rows = cursor.fetchall()
        return [dict(row) for row in rows]


@api_router.patch("/agent/update-remarks/{loan_id}")
async def update_collection_remark(
    loan_id: str,
    request: dict,
    token_data: dict = Depends(require_role(['collection_agent']))
):
    remark = request.get("remark", "").strip()
    if not remark:
        raise HTTPException(status_code=400, detail="remark is required")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE loans SET collection_remark = %s
            WHERE id = %s AND branch_id = %s AND tenant_id = %s
        """, (remark, loan_id, branch_id, tenant_id))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Loan not found or access denied")
        conn.commit()
    return {"message": "Remark updated successfully"}


@api_router.patch("/agent/add-remark/{emi_id}")
async def add_emi_remark(
    emi_id: str,
    request: dict,
    token_data: dict = Depends(require_role(['collection_agent']))
):
    remark = request.get("remark", "").strip()
    if not remark:
        raise HTTPException(status_code=400, detail="Remark is required")
    followup_date = request.get("followup_date") or None  # YYYY-MM-DD or None
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE emi_schedule
            SET collection_remark = %s,
                followup_date = %s,
                due_date = CASE WHEN %s IS NOT NULL THEN %s ELSE due_date END
            WHERE id = %s
              AND loan_id IN (
                  SELECT id FROM loans WHERE tenant_id = %s AND branch_id = %s
              )
        """, (remark, followup_date, followup_date, followup_date, emi_id, tenant_id, branch_id))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="EMI not found or access denied")
        conn.commit()
    return {"message": "Remark saved successfully"}


# ── Collection Agent Export Helpers ─────────────────────────────────────────

def _agent_export_token(token: str):
    """Decode JWT and verify caller is a collection_agent."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("role") != "collection_agent":
            raise HTTPException(403, "Access denied")
        return payload
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(401, "Invalid token")


@api_router.get("/agent/daily-collection-list/pdf")
async def export_agent_daily_pdf(token: str = None):
    td = _agent_export_token(token)
    tenant_id = td["tenant_id"]
    branch_id = td["branch_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        row = cursor.fetchone()
        company_name = row["name"] if row else "SV Fincloud"
        cursor.execute("SELECT name FROM branches WHERE id = %s", (branch_id,))
        brow = cursor.fetchone()
        branch_name = brow["name"] if brow else "Branch"
        cursor.execute("""
            SELECT c.name AS customer_name,
                   COALESCE(u.phone, '') AS phone,
                   l.id AS loan_id, l.loan_number, l.outstanding_balance,
                   e.emi_amount, e.due_date, e.status,
                   COALESCE(e.collection_remark, '-') AS collection_remark,
                   e.followup_date
            FROM emi_schedule e
            JOIN loans l ON e.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            JOIN users u ON c.user_id = u.id
            WHERE l.tenant_id = %s AND l.branch_id = %s
              AND (
                  CAST(e.due_date AS DATE) BETWEEN CURRENT_DATE - INTERVAL '5 days' AND CURRENT_DATE + INTERVAL '2 days'
                  OR DATE((SELECT payment_date FROM payments WHERE emi_id = e.id ORDER BY created_at DESC LIMIT 1)) = CURRENT_DATE
              )
            ORDER BY CAST(e.due_date AS DATE) ASC
        """, (tenant_id, branch_id))
        rows = cursor.fetchall()

    # Landscape A4: 842 × 595 — gives ~742 usable points (842 - 2×50)
    LAND_W = 842
    usable_w = LAND_W - 2 * PAGE_MARGIN
    headers = ["Customer", "Phone", "Loan No", "EMI Amount", "Due Date", "Outstanding", "Status"]
    col_widths = [usable_w * w for w in [0.22, 0.14, 0.14, 0.12, 0.12, 0.14, 0.12]]
    table_rows = []
    for r in rows:
        r = dict(r)
        due = r["due_date"]
        due_str = due.strftime("%d/%m/%Y") if hasattr(due, "strftime") else str(due or "-")
        table_rows.append([
            _p(r["customer_name"] or "-"),
            _p(r["phone"] or "-"),
            _p(r["loan_number"] or r["loan_id"] or "-"),
            _p(fmt_currency(r["emi_amount"])),
            _p(due_str),
            _p(fmt_currency(r["outstanding_balance"])),
            _p((r["status"] or "-").upper()),
        ])

    from reportlab.lib.pagesizes import landscape, A4 as _A4
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=landscape(_A4),
        rightMargin=PAGE_MARGIN, leftMargin=PAGE_MARGIN,
        topMargin=40, bottomMargin=52,
    )
    elements = []
    elements.extend(build_header(company_name, branch_name, None))
    elements.append(Paragraph("Daily Collection List", STYLE_TITLE))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(f"<b>Total Records:</b>  {len(rows)}", STYLE_INFO))
    elements.append(Spacer(1, 14))
    tbl = Table([[_pb(h) for h in headers]] + table_rows, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style())
    elements.append(tbl)
    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=daily_collection_list.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/agent/daily-collection-list/csv")
async def export_agent_daily_csv(token: str = None):
    td = _agent_export_token(token)
    tenant_id = td["tenant_id"]
    branch_id = td["branch_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM branches WHERE id = %s", (branch_id,))
        brow = cursor.fetchone()
        branch_name = brow["name"] if brow else "Branch"
        cursor.execute("""
            SELECT c.name AS customer_name,
                   COALESCE(u.phone, '') AS phone,
                   l.id AS loan_id, l.loan_number, l.outstanding_balance,
                   e.emi_amount, e.due_date, e.status,
                   COALESCE(e.collection_remark, '-') AS collection_remark,
                   e.followup_date
            FROM emi_schedule e
            JOIN loans l ON e.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            JOIN users u ON c.user_id = u.id
            WHERE l.tenant_id = %s AND l.branch_id = %s
              AND (
                  CAST(e.due_date AS DATE) BETWEEN CURRENT_DATE - INTERVAL '5 days' AND CURRENT_DATE + INTERVAL '2 days'
                  OR DATE((SELECT payment_date FROM payments WHERE emi_id = e.id ORDER BY created_at DESC LIMIT 1)) = CURRENT_DATE
              )
            ORDER BY CAST(e.due_date AS DATE) ASC
        """, (tenant_id, branch_id))
        rows = cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    write_csv_summary(writer, "Daily Collection List", branch_name, {"Total Records": len(rows)})
    writer.writerow(["Customer", "Phone", "Loan No", "EMI Amount", "Due Date", "Outstanding", "Status", "Remark", "Follow-up"])
    for r in rows:
        r = dict(r)
        due = r["due_date"]
        due_str = due.strftime("%d/%m/%Y") if hasattr(due, "strftime") else str(due or "-")
        fu = r["followup_date"]
        fu_str = fu.strftime("%d/%m/%Y") if hasattr(fu, "strftime") else str(fu or "-")
        writer.writerow([
            f'="{r["customer_name"] or "-"}"',
            f'="{r["phone"] or "-"}"',
            f'="{r["loan_number"] or r["loan_id"] or "-"}"',
            r["emi_amount"] or 0,
            f'="{due_str}"',
            r["outstanding_balance"] or 0,
            (r["status"] or "-").upper(),
            r["collection_remark"] or "-",
            f'="{fu_str}"',
        ])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "daily_collection_list.csv")


@api_router.get("/agent/customers/pdf")
async def export_agent_customers_pdf(token: str = None):
    td = _agent_export_token(token)
    tenant_id = td["tenant_id"]
    branch_id = td["branch_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        row = cursor.fetchone()
        company_name = row["name"] if row else "SV Fincloud"
        cursor.execute("SELECT name FROM branches WHERE id = %s", (branch_id,))
        brow = cursor.fetchone()
        branch_name = brow["name"] if brow else "Branch"
        cursor.execute("""
            SELECT DISTINCT c.name, COALESCE(u.phone, '') AS phone,
                   l.id AS loan_id, l.loan_number, l.loan_type, l.amount, l.outstanding_balance
            FROM customers c
            JOIN loans l ON c.id = l.customer_id
            LEFT JOIN users u ON c.user_id = u.id
            WHERE l.status = 'active' AND l.tenant_id = %s AND l.branch_id = %s
            ORDER BY c.name
        """, (tenant_id, branch_id))
        rows = cursor.fetchall()

    headers = ["Customer", "Phone", "Loan No", "Loan Type", "Amount", "Outstanding"]
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * w for w in [0.20, 0.13, 0.13, 0.14, 0.18, 0.18]]
    table_rows = []
    for r in rows:
        r = dict(r)
        table_rows.append([
            r["name"] or "-",
            r["phone"] or "-",
            r["loan_number"] or r["loan_id"] or "-",
            (r["loan_type"] or "-").replace("_", " ").title(),
            fmt_currency(r["amount"]),
            fmt_currency(r["outstanding_balance"]),
        ])
    buf = generate_report_pdf(
        "Customer List", headers, table_rows,
        {"Total Records": len(rows)}, company_name, branch_name,
        col_widths=col_widths,
    )
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=customers.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/agent/customers/csv")
async def export_agent_customers_csv(token: str = None):
    td = _agent_export_token(token)
    tenant_id = td["tenant_id"]
    branch_id = td["branch_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM branches WHERE id = %s", (branch_id,))
        brow = cursor.fetchone()
        branch_name = brow["name"] if brow else "Branch"
        cursor.execute("""
            SELECT DISTINCT c.name, COALESCE(u.phone, '') AS phone,
                   l.id AS loan_id, l.loan_number, l.loan_type, l.amount, l.outstanding_balance
            FROM customers c
            JOIN loans l ON c.id = l.customer_id
            LEFT JOIN users u ON c.user_id = u.id
            WHERE l.status = 'active' AND l.tenant_id = %s AND l.branch_id = %s
            ORDER BY c.name
        """, (tenant_id, branch_id))
        rows = cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    write_csv_summary(writer, "Customer List", branch_name, {"Total Records": len(rows)})
    writer.writerow(["Customer", "Phone", "Loan No", "Loan Type", "Amount", "Outstanding"])
    for r in rows:
        r = dict(r)
        writer.writerow([
            f'="{r["name"] or "-"}"',
            f'="{r["phone"] or "-"}"',
            f'="{r["loan_number"] or r["loan_id"] or "-"}"',
            (r["loan_type"] or "-").replace("_", " ").title(),
            r["amount"] or 0,
            r["outstanding_balance"] or 0,
        ])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "customers.csv")


@api_router.get("/agent/collection-history/pdf")
async def export_agent_history_pdf(token: str = None):
    td = _agent_export_token(token)
    tenant_id = td["tenant_id"]
    branch_id = td["branch_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        row = cursor.fetchone()
        company_name = row["name"] if row else "SV Fincloud"
        cursor.execute("SELECT name FROM branches WHERE id = %s", (branch_id,))
        brow = cursor.fetchone()
        branch_name = brow["name"] if brow else "Branch"
        cursor.execute("""
            SELECT p.id AS payment_id, p.payment_number, c.name AS customer_name,
                   COALESCE(u.phone, '') AS phone,
                   l.id AS loan_id, l.loan_number, p.amount, p.status,
                   COALESCE(p.payment_date, p.created_at) AS payment_date
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN users u ON c.user_id = u.id
            WHERE p.tenant_id = %s AND p.branch_id = %s
            ORDER BY COALESCE(p.payment_date, p.created_at) DESC
            LIMIT 200
        """, (tenant_id, branch_id))
        rows = cursor.fetchall()

    headers = ["Customer", "Phone", "Loan No", "Amount", "Status", "Date"]
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * w for w in [0.25, 0.16, 0.18, 0.14, 0.13, 0.14]]
    table_rows = []
    for r in rows:
        r = dict(r)
        pd_val = r["payment_date"]
        pd_str = pd_val.strftime("%d/%m/%Y") if hasattr(pd_val, "strftime") else str(pd_val or "-")[:10]
        table_rows.append([
            r["customer_name"] or "-",
            r["phone"] or "-",
            r["loan_number"] or r["loan_id"] or "-",
            fmt_currency(r["amount"]),
            (r["status"] or "-").upper(),
            pd_str,
        ])
    buf = generate_report_pdf(
        "Collection History", headers, table_rows,
        {"Total Records": len(rows)}, company_name, branch_name,
        col_widths=col_widths,
    )
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=collection_history.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/agent/collection-history/csv")
async def export_agent_history_csv(token: str = None):
    td = _agent_export_token(token)
    tenant_id = td["tenant_id"]
    branch_id = td["branch_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM branches WHERE id = %s", (branch_id,))
        brow = cursor.fetchone()
        branch_name = brow["name"] if brow else "Branch"
        cursor.execute("""
            SELECT p.id AS payment_id, p.payment_number, c.name AS customer_name,
                   COALESCE(u.phone, '') AS phone,
                   l.id AS loan_id, l.loan_number, p.amount, p.status,
                   COALESCE(p.payment_date, p.created_at) AS payment_date
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN users u ON c.user_id = u.id
            WHERE p.tenant_id = %s AND p.branch_id = %s
            ORDER BY COALESCE(p.payment_date, p.created_at) DESC
            LIMIT 200
        """, (tenant_id, branch_id))
        rows = cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    write_csv_summary(writer, "Collection History", branch_name, {"Total Records": len(rows)})
    writer.writerow(["Payment No", "Customer", "Phone", "Loan No", "Amount", "Status", "Date"])
    for r in rows:
        r = dict(r)
        pd_val = r["payment_date"]
        pd_str = pd_val.strftime("%d/%m/%Y") if hasattr(pd_val, "strftime") else str(pd_val or "-")[:10]
        writer.writerow([
            f'="{r["payment_number"] or r["payment_id"] or "-"}"',
            f'="{r["customer_name"] or "-"}"',
            f'="{r["phone"] or "-"}"',
            f'="{r["loan_number"] or r["loan_id"] or "-"}"',
            r["amount"] or 0,
            (r["status"] or "-").upper(),
            f'="{pd_str}"',
        ])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "collection_history.csv")


# Finance Officer Routes
@api_router.get("/officer/loan-applications")
async def get_loan_applications(token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]

    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter, params = get_branch_filter(token_data, "l")

        query = f"""
        SELECT l.*, c.name as customer_name, u.email, u.phone,
            c.cibil_score, c.monthly_income
        FROM loans l
        INNER JOIN customers c ON l.customer_id = c.id
        LEFT JOIN users u ON c.user_id = u.id
        WHERE l.status IN ('pending', 'pre-approved', 'submitted', 'applied')
        AND l.tenant_id = %s
        {branch_filter}
        ORDER BY l.created_at DESC
        """

        cursor.execute(query, (token_data["tenant_id"], *params))

        applications = cursor.fetchall()
        return applications
    
@api_router.patch("/officer/update-loan/{loan_id}")
async def update_loan_details(loan_id: str,data: LoanUpdateRequest,token_data: dict = Depends(require_role(['finance_officer', 'admin']))):
    if not check_permission(token_data['role'], 'loans', 'update'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter, params = get_branch_filter(token_data)

        query = f"""
        SELECT id FROM loans
        WHERE id = %s
        AND tenant_id = %s
        {branch_filter}
        """

        cursor.execute(query, (loan_id, token_data["tenant_id"], *params))

        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Loan not found")
        # Example: Updating interest rate or amount before approval
        if 'interest_rate' in data:
            branch_filter, params = get_branch_filter(token_data)

            query = f"""
            UPDATE loans
            SET interest_rate = %s
            WHERE id = %s
            AND tenant_id = %s
            {branch_filter}
            """

            cursor.execute(query, (data['interest_rate'], loan_id, token_data["tenant_id"], *params))
        log_audit(conn, token_data['user_id'], token_data['tenant_id'],'LOAN_UPDATED', 'loan', loan_id, json.dumps(data))
        return {"message": "Loan updated successfully"}
    
@api_router.post("/officer/approve-loan")
async def approve_loan(data: dict, token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'loans', 'update'):
        raise HTTPException(403, "Access denied")
    loan_id = data.get('entity_id')
    action = data.get('action')
    gold_weight = data.get('gold_weight')
    vehicle_reg_no = data.get('vehicle_reg_no', '').strip().upper() if data.get('vehicle_reg_no') else ''
    input_cibil = data.get('cibil_score')

    with get_db() as conn:
        cursor = conn.cursor()

        if action == 'approve':
            # Fetch loan + customer for validation
            branch_filter, params = get_branch_filter(token_data, "l")
            cursor.execute(f"""
                SELECT l.loan_type, l.gold_weight AS stored_gold_weight,
                       l.vehicle_reg_no AS stored_vehicle_reg_no,
                       c.cibil_score AS stored_cibil_score
                FROM loans l
                JOIN customers c ON l.customer_id = c.id
                WHERE l.id = %s AND l.tenant_id = %s {branch_filter}
            """, (loan_id, token_data["tenant_id"], *params))
            loan_row = cursor.fetchone()
            if not loan_row:
                raise HTTPException(status_code=404, detail="Loan not found")

            loan_type = (loan_row["loan_type"] or "").lower()

            # ── Loan-type validation ──────────────────────────────────────
            if loan_type == "gold_loan":
                if not gold_weight or float(gold_weight) <= 0:
                    raise HTTPException(400, "Gold weight is required for Gold Loan approval")
                stored = float(loan_row["stored_gold_weight"] or 0)
                if stored > 0 and abs(float(gold_weight) - stored) > 0.01:
                    raise HTTPException(400,
                        f"Gold weight mismatch: entered {gold_weight}g, expected {stored}g")

            elif loan_type == "vehicle_loan":
                if not vehicle_reg_no:
                    raise HTTPException(400, "Vehicle registration number is required for Vehicle Loan approval")
                stored = (loan_row["stored_vehicle_reg_no"] or "").strip().upper()
                if stored and vehicle_reg_no != stored:
                    raise HTTPException(400,
                        f"Vehicle number mismatch: entered '{vehicle_reg_no}', expected '{stored}'")

                # Global duplicate check — block if this vehicle already has an active loan anywhere
                cursor.execute("""
                    SELECT id FROM loans
                    WHERE UPPER(REPLACE(COALESCE(vehicle_reg_no, ''), ' ', '')) = %s
                      AND status IN ('pending', 'pre-approved', 'active', 'approved', 'disbursed')
                      AND id != %s
                """, (vehicle_reg_no.replace(' ', ''), loan_id))
                if cursor.fetchone():
                    raise HTTPException(400,
                        "This vehicle is already associated with an active or pending loan in the FinCloud network."
                    )

            elif loan_type == "personal_loan":
                if input_cibil is None or input_cibil == "":
                    raise HTTPException(400, "CIBIL score is required for Personal Loan approval")
                stored = int(loan_row["stored_cibil_score"] or 0)
                if stored and int(input_cibil) != stored:
                    raise HTTPException(400,
                        f"CIBIL score mismatch: entered {input_cibil}, expected {stored}")

            try:
                # 1. Activate loan
                branch_filter, params = get_branch_filter(token_data, "loans")
                update_extra = ""
                update_params = []
                if loan_type == "gold_loan" and gold_weight:
                    update_extra = ", gold_weight = %s"
                    update_params = [float(gold_weight)]
                elif loan_type == "vehicle_loan" and vehicle_reg_no:
                    update_extra = ", vehicle_reg_no = %s"
                    update_params = [vehicle_reg_no]

                cursor.execute(f"""
                    UPDATE loans
                    SET status = 'active', approved_by = %s, approved_at = CURRENT_TIMESTAMP,
                        outstanding_balance = amount {update_extra}
                    WHERE id = %s AND tenant_id = %s {branch_filter}
                """, (token_data["user_id"], *update_params, loan_id, token_data["tenant_id"], *params))
                # 2. Fetch loan
                branch_filter, params = get_branch_filter(token_data, "loans")

                query = f"""
                SELECT *
                FROM loans
                WHERE id = %s
                AND tenant_id = %s
                {branch_filter}
                """

                cursor.execute(query, (loan_id, token_data["tenant_id"], *params))
                loan = cursor.fetchone()
                if not loan:
                    raise HTTPException(status_code=404, detail="Loan not found")
                
                # 3. Generate EMI schedule with exact monthly dates
                tenure = loan['tenure']
                principal_per_month = loan['amount'] / tenure
                
                # Use current date as the starting reference point
                start_date = get_ist_now()
        
                for i in range(1, tenure + 1):
                    due_date_obj = start_date + relativedelta(months=i)
                    due_date_str = due_date_obj.strftime('%Y-%m-%d')
                    
                    cursor.execute('''
                        INSERT INTO emi_schedule (
                            id, loan_id, emi_number,
                            emi_amount, principal_amount,
                            interest_amount, due_date, status,
                            branch_id, tenant_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s)
                    ''', (
                        str(uuid.uuid4()),
                        loan_id,
                        i,
                        loan['emi_amount'],
                        principal_per_month,
                        loan['emi_amount'] - principal_per_month,
                        due_date_str,
                        token_data["branch_id"],
                        token_data["tenant_id"]
                    ))
                
                conn.commit()
                log_audit(conn, token_data['user_id'], token_data['tenant_id'], 'LOAN_APPROVED', 'loan', loan_id)
                return {"message": "Loan approved and EMI schedule created"}

            
            except Exception as e:
                conn.rollback() 
                logger.error("APPROVE ERROR: %s", e)
                raise HTTPException(status_code=500, detail=f"Database Error: {str(e)}")
        
        else:
            branch_filter, params = get_branch_filter(token_data)

            query = f"""
            UPDATE loans
            SET status = 'rejected'
            WHERE id = %s
            AND tenant_id = %s
            {branch_filter}
            """

            cursor.execute(query, (loan_id, token_data["tenant_id"], *params))
            log_audit(conn, token_data['user_id'], token_data['tenant_id'], 'LOAN_REJECTED', 'loan', loan_id)
            conn.commit()
            return {"message": "Loan rejected successfully"}

@api_router.post("/officer/approve-payment")
async def approve_payment(
    request: ApprovalRequest,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'payments', 'update'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_id = token_data["branch_id"]
        tenant_id = token_data["tenant_id"]

        # 1. Fetch Payment Details
        branch_filter, params = get_branch_filter(token_data, "p")

        query = f"""
        SELECT *
        FROM payments p
        WHERE p.id = %s
        AND p.tenant_id = %s
        {branch_filter}
        """

        cursor.execute(query, (request.entity_id, tenant_id, *params))
        payment = cursor.fetchone()
        
        if not payment:
            raise HTTPException(status_code=404, detail="Payment record not found")
        
        if request.action == 'approve':
            # 2. Approve payment
            cursor.execute("""
                UPDATE payments 
                SET status = 'approved', approved_by = %s, approved_at = %s
                WHERE id = %s AND branch_id=%s AND tenant_id=%s
                """,
                (token_data['user_id'], get_ist_now().isoformat(), request.entity_id,branch_id, tenant_id)
            )
            
            # 3. Mark EMI as paid
            branch_filter, params = get_branch_filter(token_data)

            query = f"""
            UPDATE emi_schedule
            SET status = 'paid', paid_at = %s
            WHERE id = %s
            AND tenant_id = %s
            {branch_filter}
            """


            cursor.execute(
                query,
                (get_ist_now().isoformat(), payment['emi_id'], tenant_id, *params)
            )
            
            # 4. Get principal amount of this EMI
            cursor.execute("SELECT principal_amount FROM emi_schedule WHERE id = %s AND branch_id=%s AND tenant_id=%s",
                (payment['emi_id'],branch_id, tenant_id))
            
            emi = cursor.fetchone()
            if not emi:
                raise HTTPException(status_code=404, detail="EMI not found")

            principal_paid = emi['principal_amount']

            # 5. Get current outstanding balance
            branch_filter, params = get_branch_filter(token_data)

            query = f"""
            SELECT outstanding_balance
            FROM loans
            WHERE id = %s
            AND tenant_id = %s
            {branch_filter}
            """

            cursor.execute(query, (payment['loan_id'], tenant_id, *params))

            current_balance = cursor.fetchone()['outstanding_balance']

            # 6. Calculate new outstanding balance
            new_balance = max(0, current_balance - principal_paid)
            branch_filter, params = get_branch_filter(token_data)

            query = f"""
            UPDATE loans
            SET outstanding_balance = %s
            WHERE id = %s
            AND tenant_id = %s
            {branch_filter}
            """

            cursor.execute(
                query,
                (new_balance, payment['loan_id'], tenant_id, *params)
            )
            
            year = get_ist_now().year

            # Receipt sequence — global per tenant (no branch dependency)
            cursor.execute("""
                INSERT INTO receipt_sequence (tenant_id, current_value)
                VALUES (%s, 1)
                ON CONFLICT (tenant_id)
                DO UPDATE SET current_value = receipt_sequence.current_value + 1
                RETURNING current_value
            """, (tenant_id,))
            next_seq = cursor.fetchone()['current_value']

            # 🔥 GET TENANT NAME
            cursor.execute("SELECT name FROM tenants WHERE id = %s",
                (tenant_id,)
            )
            tenant = cursor.fetchone()
            tenant_name = tenant["name"] if tenant else "SV Fincloud"

            # 🔥 CREATE PREFIX
            prefix = ''.join(word[0] for word in tenant_name.split()[:3]).upper()

            # 🔥 FINAL RECEIPT NUMBER
            receipt_no = f"{prefix}-REC-{year}-{str(next_seq).zfill(6)}"

            # Simple RCPT-N number (same sequence, no branch)
            receipt_number = f"RCPT-{next_seq}"

            # ✅ 8. Count remaining EMIs AFTER this payment
            cursor.execute("""
                SELECT COUNT(*) AS remaining
                FROM emi_schedule
                WHERE loan_id = %s
                  AND status = 'pending' AND branch_id=%s AND tenant_id=%s
                """,
                (payment['loan_id'],branch_id, tenant_id))
            
            remaining_emi = cursor.fetchone()['remaining']

            # ✅ 9. Store SNAPSHOT + RECEIPT DETAILS
            cursor.execute("""
                UPDATE payments
                SET 
                    balance_after_payment = %s,
                    remaining_emi_after_payment = %s,
                    receipt_no = %s,
                    receipt_seq = %s,
                    receipt_number = %s
                WHERE id = %s AND branch_id=%s AND tenant_id=%s
                """,
                (new_balance, remaining_emi, receipt_no, next_seq, receipt_number, request.entity_id, branch_id, tenant_id)
            )

            # 10. Auto-close loan if fully paid
            branch_filter, params = get_branch_filter(token_data)

            query = f"""
            UPDATE loans
            SET status = 'closed'
            WHERE id = %s
            AND outstanding_balance <= 0
            AND tenant_id = %s
            {branch_filter}
            """

            cursor.execute(query, (payment['loan_id'], tenant_id, *params))

            conn.commit()
            log_audit(conn, token_data['user_id'], token_data['tenant_id'], 'PAYMENT_APPROVED', 'payment', request.entity_id)
            return {"message": "Payment approved and balance updated"}

        elif request.action == 'reject':
            cursor.execute("UPDATE payments SET status = 'rejected' WHERE id = %s AND branch_id = %s AND tenant_id = %s",
                (request.entity_id, branch_id, tenant_id)
            )
            conn.commit()
            log_audit(conn, token_data['user_id'], token_data['tenant_id'], 'PAYMENT_REJECTED', 'payment', request.entity_id)
            return {"message": "Payment rejected"}
        
@api_router.get("/officer/analytics-summary")
async def get_analytics(token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        branch_id = token_data["branch_id"]
        tenant_id = token_data["tenant_id"]
        apply_penalty_if_overdue(conn,branch_id,tenant_id)
        cursor = conn.cursor()

        # Total Collected (approved payments)
        branch_filter, params = get_branch_filter(token_data)
        cursor.execute(f"""
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM payments
            WHERE status = 'approved' AND tenant_id = %s
            {branch_filter}
        """, (tenant_id, *params))
        collected = cursor.fetchone()['total'] or 0

        # Pending EMI (customer has not paid yet)
        branch_filter, params = get_branch_filter(token_data, "l")
        cursor.execute(f"""
            SELECT COALESCE(SUM(e.emi_amount), 0) AS total
            FROM emi_schedule e
            JOIN loans l ON e.loan_id = l.id
            WHERE e.status = 'pending' AND l.tenant_id = %s
            {branch_filter}
        """, (tenant_id, *params))
        pending_emi = cursor.fetchone()['total'] or 0

        # Pending Approval (collected by agent, not yet approved by officer)
        branch_filter, params = get_branch_filter(token_data)
        cursor.execute(f"""
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM payments
            WHERE status = 'pending' AND tenant_id = %s
            {branch_filter}
        """, (tenant_id, *params))
        pending_approval = cursor.fetchone()['total'] or 0

        # Active Loans
        branch_filter, params = get_branch_filter(token_data)
        cursor.execute(f"""
            SELECT COUNT(*) AS count
            FROM loans
            WHERE status = 'active' AND tenant_id = %s
            {branch_filter}
        """, (tenant_id, *params))
        active_loans = cursor.fetchone()['count'] or 0

        # Collection Efficiency = collected / total_emi_due * 100
        branch_filter, params = get_branch_filter(token_data, "l")
        cursor.execute(f"""
            SELECT COALESCE(SUM(e.emi_amount), 0) AS total_due
            FROM emi_schedule e
            JOIN loans l ON e.loan_id = l.id
            WHERE l.tenant_id = %s
            {branch_filter}
        """, (tenant_id, *params))
        total_due = cursor.fetchone()['total_due'] or 0
        efficiency = round((collected / total_due * 100), 2) if total_due > 0 else 0.0

        return {
            "kpis": {
                "total_collected": collected,
                "pending_emi": pending_emi,
                "pending_approval": pending_approval,
                "active_loans": active_loans,
                "efficiency": f"{efficiency:.2f}%"
            }
        }

@api_router.get("/officer/pending-payments")
async def get_pending_payments(token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'payments', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter, params = get_branch_filter(token_data, "p")

        query = f"""
        SELECT 
            p.id, p.amount, p.created_at as payment_date, p.status,
            p.loan_id, l.loan_type, c.id as customer_id, c.name as customer_name,
            COALESCE(e.emi_number::text, 'Manual') as emi_number
        FROM payments p
        INNER JOIN loans l ON p.loan_id = l.id
        INNER JOIN customers c ON l.customer_id = c.id
        LEFT JOIN emi_schedule e ON p.emi_id = e.id
        WHERE (p.status = 'pending' OR p.status = 'PENDING')
        AND p.tenant_id = %s
        {branch_filter}
        ORDER BY p.created_at DESC
        """

        cursor.execute(query, (token_data["tenant_id"], *params))
        payments = cursor.fetchall()
        return payments

@api_router.get("/officer/customer-details/{loan_id}")
async def get_officer_customer_details(
    loan_id: str,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.name AS customer_name, c.cibil_score, c.monthly_income,
                   l.id AS loan_id, l.loan_type, l.amount, l.status,
                   l.outstanding_balance, l.interest_rate, l.tenure, l.created_at,
                   b.name AS branch_name
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE l.id = %s AND l.tenant_id = %s
        """, (loan_id, tenant_id))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Loan not found")
        return dict(row)

@api_router.get("/officer/customer-profile/{customer_id}")
async def get_officer_customer_profile(
    customer_id: str,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT u.id AS user_id, u.username, u.role, u.created_at,
                   c.id AS customer_id, c.name, c.cibil_score, c.monthly_income,
                   b.name AS branch_name
            FROM customers c
            JOIN users u ON c.user_id = u.id
            LEFT JOIN branches b ON u.branch_id = b.id
            WHERE c.id = %s AND c.tenant_id = %s
        """, (customer_id, tenant_id))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Customer not found")
        return dict(row)

@api_router.get("/officer/branch-summary")
async def branch_summary(token_data: dict = Depends(require_role(['finance_officer','admin']))):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()

        branch_filter, params = get_branch_filter(token_data)

        query = f"""
        SELECT COUNT(*) as total_loans,
            SUM(amount) as total_disbursed,
            SUM(outstanding_balance) as total_outstanding
        FROM loans
        WHERE tenant_id = %s
        {branch_filter}
        """

        cursor.execute(query, (token_data["tenant_id"], *params))
        loans = cursor.fetchone()

        branch_filter, params = get_branch_filter(token_data)

        query = f"""
        SELECT SUM(amount) as total_collected
        FROM payments
        WHERE status = 'approved'
        AND tenant_id = %s
        {branch_filter}
        """

        cursor.execute(query, (token_data["tenant_id"], *params))
        payments = cursor.fetchone()

        return {
            "branch_id": token_data["branch_id"],
            "total_loans": loans["total_loans"] or 0,
            "total_disbursed": loans["total_disbursed"] or 0,
            "total_outstanding": loans["total_outstanding"] or 0,
            "total_collected": payments["total_collected"] or 0
        }   

@api_router.get("/officer/daily-reconciliation")
async def get_daily_reconciliation(token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]

    with get_db() as conn:
        cursor = conn.cursor()

        # Total cash collected today (approved payments)
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM payments
            WHERE status = 'approved'
              AND DATE(payment_date) = CURRENT_DATE
              AND branch_id = %s AND tenant_id = %s
            """,
            (branch_id, tenant_id)
        )
        total_cash_collected = cursor.fetchone()["total"]

        # Total loans disbursed today (active loans approved today)
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM loans
            WHERE status = 'active'
              AND DATE(approved_at) = CURRENT_DATE
              AND branch_id = %s AND tenant_id = %s
            """,
            (branch_id, tenant_id)
        )
        total_loans_disbursed = cursor.fetchone()["total"]

        net_branch_cash = total_cash_collected - total_loans_disbursed

        # Transactions list (payments + disbursements today, ordered by timestamp DESC)
        cursor.execute("""
            SELECT 'payment' AS type, loan_id AS reference_id, amount, payment_date AS timestamp
            FROM payments
            WHERE status = 'approved' AND DATE(payment_date) = CURRENT_DATE
              AND branch_id = %s AND tenant_id = %s
            UNION ALL
            SELECT 'disbursement' AS type, id AS reference_id, amount, approved_at AS timestamp
            FROM loans
            WHERE status = 'active' AND DATE(approved_at) = CURRENT_DATE
              AND branch_id = %s AND tenant_id = %s
            ORDER BY timestamp DESC
            """,
            (branch_id, tenant_id, branch_id, tenant_id)
        )
        transactions = [dict(row) for row in cursor.fetchall()]

    return {
        "summary": {
            "total_cash_collected": total_cash_collected,
            "total_loans_disbursed": total_loans_disbursed,
            "net_branch_cash": net_branch_cash,
        },
        "transactions": transactions,
    }


@api_router.get("/officer/locker-inventory")
async def get_locker_inventory(token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT l.id AS loan_id, l.loan_number, c.id AS customer_id, c.name AS customer_name, l.gold_weight,
                   'PKT-' || SUBSTR(l.id, 1, 6) AS locker_packet_id
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            WHERE l.status = 'active' AND l.gold_weight > 0
              AND l.branch_id = %s AND l.tenant_id = %s
            """,
            (branch_id, tenant_id)
        )
        return [dict(row) for row in cursor.fetchall()]


@api_router.get("/officer/gold-release-queue")
async def get_gold_release_queue(token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT l.id AS loan_id, l.loan_number, c.id AS customer_id, c.name AS customer_name,
                   l.gold_weight, l.approved_at AS closed_at
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            WHERE l.status = 'closed' AND l.outstanding_balance = 0
              AND l.gold_released_at IS NULL
              AND l.branch_id = %s AND l.tenant_id = %s
            """,
            (branch_id, tenant_id)
        )
        return [dict(row) for row in cursor.fetchall()]


@api_router.post("/officer/confirm-release")
async def confirm_release(
    request: ConfirmReleaseRequest,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'loans', 'update'):
        raise HTTPException(403, "Access denied")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE loans SET gold_released_at = CURRENT_TIMESTAMP WHERE id = %s AND branch_id = %s AND tenant_id = %s",
            (request.loan_id, branch_id, tenant_id)
        )
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Loan not found")
        return {"message": "Gold released successfully"}


@api_router.get("/officer/closed-loans")
async def get_closed_loans(token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT l.id AS loan_id, l.loan_number, c.id AS customer_id, c.name AS customer_name,
                   l.amount AS loan_amount,
                   COALESCE(SUM(p.amount), 0) AS total_paid, l.approved_at AS closed_at
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN payments p ON p.loan_id = l.id AND p.status = 'approved'
            WHERE l.status = 'closed' AND l.branch_id = %s AND l.tenant_id = %s
            GROUP BY l.id, l.loan_number, c.id, c.name, l.amount, l.approved_at
            ORDER BY l.approved_at DESC
            """,
            (branch_id, tenant_id)
        )
        return [dict(row) for row in cursor.fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# Closed Loans & Locker Inventory Export Endpoints
# ─────────────────────────────────────────────────────────────────────────────

def _get_closed_loans_export(tenant_id: str, branch_id: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT l.id AS loan_id, l.loan_number, c.name AS customer_name, l.amount AS loan_amount,
                   COALESCE(SUM(p.amount), 0) AS total_paid, l.approved_at AS closed_at,
                   b.name AS branch_name
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN payments p ON p.loan_id = l.id AND p.status = 'approved'
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE l.status = 'closed' AND l.branch_id = %s AND l.tenant_id = %s
            GROUP BY l.id, l.loan_number, c.name, l.amount, l.approved_at, b.name
            ORDER BY l.approved_at DESC
        """, (branch_id, tenant_id))
        rows = [dict(r) for r in cursor.fetchall()]
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        t = cursor.fetchone()
        company_name = t["name"] if t else "SV Fincloud"
        branch_name = rows[0]["branch_name"] if rows else "Branch"
    return rows, company_name, branch_name


def _get_locker_export(tenant_id: str, branch_id: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT l.id AS loan_id, l.loan_number, c.name AS customer_name, l.gold_weight,
                   'PKT-' || SUBSTR(l.id, 1, 6) AS locker_packet_id,
                   b.name AS branch_name
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE l.status = 'active' AND l.gold_weight > 0
              AND l.branch_id = %s AND l.tenant_id = %s
        """, (branch_id, tenant_id))
        rows = [dict(r) for r in cursor.fetchall()]
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        t = cursor.fetchone()
        company_name = t["name"] if t else "SV Fincloud"
        branch_name = rows[0]["branch_name"] if rows else "Branch"
    return rows, company_name, branch_name


@api_router.get("/officer/closed-loans/pdf")
async def export_closed_loans_pdf(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    rows, company_name, branch_name = _get_closed_loans_export(
        token_data["tenant_id"], token_data["branch_id"]
    )
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.28, usable_w * 0.26, usable_w * 0.18, usable_w * 0.14, usable_w * 0.14]
    table_data = [[_pb("Loan No"), _pb("Customer"), _pb("Loan Amount"), _pb("Total Paid"), _pb("Closed Date")]]
    for r in rows:
        table_data.append([
            _p(r["loan_number"] or str(r["loan_id"])),
            _p(r["customer_name"]),
            _p(fmt_currency(r["loan_amount"])),
            _p(fmt_currency(r["total_paid"])),
            _p(fmt_date(r["closed_at"])),
        ])
    total_row_idx = len(table_data)
    table_data.append([
        _pb("TOTAL"), _pb(f"{len(rows)} loans"),
        _pb(fmt_currency(sum(r["loan_amount"] for r in rows))),
        _pb(fmt_currency(sum(r["total_paid"] for r in rows))),
        _pb(""),
    ])
    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []
    elements.extend(build_header(company_name, branch_name))
    elements.append(Paragraph("Closed Loans Report", STYLE_TITLE))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(f"<b>Total Closed Loans:</b>  {len(rows)}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Disbursed:</b>  {fmt_currency(sum(r['loan_amount'] for r in rows))}", STYLE_INFO))
    elements.append(Spacer(1, 14))
    tbl = Table(table_data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style(total_row_idx))
    elements.append(tbl)
    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=closed_loans.pdf"})


@api_router.get("/officer/closed-loans/csv")
async def export_closed_loans_csv(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    rows, company_name, branch_name = _get_closed_loans_export(
        token_data["tenant_id"], token_data["branch_id"]
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Closed Loans Report"])
    writer.writerow(["Company", company_name])
    writer.writerow(["Branch", branch_name])
    writer.writerow([])
    writer.writerow(["Loan No", "Customer", "Loan Amount", "Total Paid", "Closed Date"])
    for r in rows:
        writer.writerow([
            r["loan_number"] or r["loan_id"],
            r["customer_name"],
            fmt_currency(r["loan_amount"]),
            fmt_currency(r["total_paid"]),
            f'="{fmt_date(r["closed_at"])}"' if r["closed_at"] else "",
        ])
    writer.writerow(["TOTAL", "", fmt_currency(sum(r["loan_amount"] for r in rows)),
                     fmt_currency(sum(r["total_paid"] for r in rows)), ""])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "closed_loans.csv")


@api_router.get("/officer/locker-inventory/pdf")
async def export_locker_pdf(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    rows, company_name, branch_name = _get_locker_export(
        token_data["tenant_id"], token_data["branch_id"]
    )
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.28, usable_w * 0.28, usable_w * 0.28, usable_w * 0.16]
    table_data = [[_pb("Packet ID"), _pb("Loan No"), _pb("Customer"), _pb("Gold Weight (g)")]]
    for r in rows:
        table_data.append([
            _p(r["locker_packet_id"]),
            _p(r["loan_number"] or str(r["loan_id"])),
            _p(r["customer_name"]),
            _p(f'{r["gold_weight"]:.2f} g'),
        ])
    total_row_idx = len(table_data)
    total_gold = sum(float(r["gold_weight"] or 0) for r in rows)
    table_data.append([_pb("TOTAL"), _pb(f"{len(rows)} items"), _pb(""), _pb(f"{total_gold:.2f} g")])
    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []
    elements.extend(build_header(company_name, branch_name))
    elements.append(Paragraph("Locker Inventory Report", STYLE_TITLE))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(f"<b>Total Items in Locker:</b>  {len(rows)}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Gold Weight:</b>  {total_gold:.2f} g", STYLE_INFO))
    elements.append(Spacer(1, 14))
    tbl = Table(table_data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style(total_row_idx))
    elements.append(tbl)
    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=locker_inventory.pdf"})


@api_router.get("/officer/locker-inventory/csv")
async def export_locker_csv(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    rows, company_name, branch_name = _get_locker_export(
        token_data["tenant_id"], token_data["branch_id"]
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Locker Inventory Report"])
    writer.writerow(["Company", company_name])
    writer.writerow(["Branch", branch_name])
    writer.writerow([])
    writer.writerow(["Packet ID", "Loan No", "Customer", "Gold Weight (g)"])
    for r in rows:
        writer.writerow([
            r["locker_packet_id"],
            r["loan_number"] or r["loan_id"],
            r["customer_name"],
            f'{r["gold_weight"]:.2f}',
        ])
    total_gold = sum(float(r["gold_weight"] or 0) for r in rows)
    writer.writerow(["TOTAL", "", "", f"{total_gold:.2f}"])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "locker_inventory.csv")


# ─────────────────────────────────────────────────────────────────────────────
# Pending Payments, Active Loans, Pending EMI, Agent Performance Export
# ─────────────────────────────────────────────────────────────────────────────

def _get_company_branch(cursor, tenant_id, branch_id):
    cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
    t = cursor.fetchone()
    company_name = t["name"] if t else "SV Fincloud"
    cursor.execute("SELECT name FROM branches WHERE id = %s", (branch_id,))
    b = cursor.fetchone()
    branch_name = b["name"] if b else "Branch"
    return company_name, branch_name


@api_router.get("/officer/pending-payments/pdf")
async def export_pending_payments_pdf(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    branch_id = token_data["branch_id"]
    branch_filter, params = get_branch_filter(token_data, "p")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT p.id, p.amount, p.created_at AS payment_date, p.status,
                   l.loan_type, c.name AS customer_name,
                   COALESCE(e.emi_number::text, 'Manual') AS emi_number
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN emi_schedule e ON p.emi_id = e.id
            WHERE (p.status = 'pending' OR p.status = 'PENDING')
              AND p.tenant_id = %s {branch_filter}
            ORDER BY p.created_at DESC
        """, (tenant_id, *params))
        rows = [dict(r) for r in cursor.fetchall()]
        company_name, branch_name = _get_company_branch(cursor, tenant_id, branch_id)
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.30, usable_w * 0.20, usable_w * 0.18, usable_w * 0.16, usable_w * 0.16]
    table_data = [[_pb("Customer"), _pb("Loan Type"), _pb("EMI No"), _pb("Amount"), _pb("Date")]]
    for r in rows:
        table_data.append([
            _p(r["customer_name"]), _p(r["loan_type"] or "-"), _p(r["emi_number"]),
            _p(fmt_currency(r["amount"])), _p(fmt_date(r["payment_date"])),
        ])
    total_row_idx = len(table_data)
    table_data.append([_pb("TOTAL"), _pb(f"{len(rows)} payments"), _pb(""), _pb(fmt_currency(sum(r["amount"] for r in rows))), _pb("")])
    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []
    elements.extend(build_header(company_name, branch_name))
    elements.append(Paragraph("Pending Payments Report", STYLE_TITLE))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(f"<b>Total Pending:</b>  {len(rows)}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Amount:</b>  {fmt_currency(sum(r['amount'] for r in rows))}", STYLE_INFO))
    elements.append(Spacer(1, 14))
    tbl = Table(table_data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style(total_row_idx))
    elements.append(tbl)
    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=pending_payments.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/officer/pending-payments/csv")
async def export_pending_payments_csv(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    branch_id = token_data["branch_id"]
    branch_filter, params = get_branch_filter(token_data, "p")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT p.id, p.amount, p.created_at AS payment_date, p.status,
                   l.loan_type, c.name AS customer_name,
                   COALESCE(e.emi_number::text, 'Manual') AS emi_number
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN emi_schedule e ON p.emi_id = e.id
            WHERE (p.status = 'pending' OR p.status = 'PENDING')
              AND p.tenant_id = %s {branch_filter}
            ORDER BY p.created_at DESC
        """, (tenant_id, *params))
        rows = [dict(r) for r in cursor.fetchall()]
        company_name, branch_name = _get_company_branch(cursor, tenant_id, branch_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Pending Payments Report"])
    writer.writerow(["Company", company_name])
    writer.writerow(["Branch", branch_name])
    writer.writerow([])
    writer.writerow(["Customer", "Loan Type", "EMI No", "Amount", "Date"])
    for r in rows:
        writer.writerow([r["customer_name"], r["loan_type"] or "-", r["emi_number"],
                         fmt_currency(r["amount"]), fmt_date(r["payment_date"])])
    writer.writerow(["TOTAL", "", "", fmt_currency(sum(r["amount"] for r in rows)), ""])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "pending_payments.csv")


@api_router.get("/officer/active-loans/pdf")
async def export_active_loans_pdf(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    branch_id = token_data["branch_id"]
    branch_filter, params = get_branch_filter(token_data, "l")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT l.loan_number, c.name AS customer_name, l.loan_type,
                   l.amount, l.tenure, l.created_at
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            WHERE l.status = 'active' AND l.tenant_id = %s {branch_filter}
            ORDER BY l.created_at DESC
        """, (tenant_id, *params))
        rows = [dict(r) for r in cursor.fetchall()]
        company_name, branch_name = _get_company_branch(cursor, tenant_id, branch_id)
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.22, usable_w * 0.28, usable_w * 0.18, usable_w * 0.16, usable_w * 0.16]
    table_data = [[_pb("Loan No"), _pb("Customer"), _pb("Type"), _pb("Amount"), _pb("Disbursed")]]
    for r in rows:
        table_data.append([
            _p(r["loan_number"]), _p(r["customer_name"]), _p(r["loan_type"] or "-"),
            _p(fmt_currency(r["amount"])), _p(fmt_date(r["created_at"])),
        ])
    total_row_idx = len(table_data)
    table_data.append([_pb("TOTAL"), _pb(f"{len(rows)} loans"), _pb(""), _pb(fmt_currency(sum(r["amount"] for r in rows))), _pb("")])
    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []
    elements.extend(build_header(company_name, branch_name))
    elements.append(Paragraph("Active Loans Report", STYLE_TITLE))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(f"<b>Total Active Loans:</b>  {len(rows)}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Disbursed:</b>  {fmt_currency(sum(r['amount'] for r in rows))}", STYLE_INFO))
    elements.append(Spacer(1, 14))
    tbl = Table(table_data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style(total_row_idx))
    elements.append(tbl)
    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=active_loans.pdf"})


@api_router.get("/officer/active-loans/csv")
async def export_active_loans_csv(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    branch_id = token_data["branch_id"]
    branch_filter, params = get_branch_filter(token_data, "l")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT l.loan_number, c.name AS customer_name, l.loan_type,
                   l.amount, l.tenure, l.created_at
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            WHERE l.status = 'active' AND l.tenant_id = %s {branch_filter}
            ORDER BY l.created_at DESC
        """, (tenant_id, *params))
        rows = [dict(r) for r in cursor.fetchall()]
        company_name, branch_name = _get_company_branch(cursor, tenant_id, branch_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Active Loans Report"])
    writer.writerow(["Company", company_name])
    writer.writerow(["Branch", branch_name])
    writer.writerow([])
    writer.writerow(["Loan No", "Customer", "Type", "Amount", "Disbursed"])
    for r in rows:
        writer.writerow([r["loan_number"], r["customer_name"], r["loan_type"] or "-",
                         fmt_currency(r["amount"]), fmt_date(r["created_at"])])
    writer.writerow(["TOTAL", "", "", fmt_currency(sum(r["amount"] for r in rows)), ""])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "active_loans.csv")


@api_router.get("/officer/pending-emi/pdf")
async def export_pending_emi_pdf(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    branch_id = token_data["branch_id"]
    branch_filter, params = get_branch_filter(token_data, "l")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT l.loan_number, c.name AS customer_name,
                   SUM(e.emi_amount) AS pending_amount,
                   COUNT(*) FILTER (WHERE e.status = 'pending') AS pending_emi_count
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            JOIN emi_schedule e ON e.loan_id = l.id
            WHERE e.status = 'pending' AND l.tenant_id = %s {branch_filter}
            GROUP BY l.loan_number, c.name
            ORDER BY pending_amount DESC
        """, (tenant_id, *params))
        rows = [dict(r) for r in cursor.fetchall()]
        company_name, branch_name = _get_company_branch(cursor, tenant_id, branch_id)
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.28, usable_w * 0.36, usable_w * 0.18, usable_w * 0.18]
    table_data = [[_pb("Loan No"), _pb("Customer"), _pb("Pending EMIs"), _pb("Pending Amount")]]
    for r in rows:
        table_data.append([_p(r["loan_number"]), _p(r["customer_name"]),
                           _p(str(r["pending_emi_count"])), _p(fmt_currency(r["pending_amount"]))])
    total_row_idx = len(table_data)
    table_data.append([_pb("TOTAL"), _pb(f"{len(rows)} loans"), _pb(""), _pb(fmt_currency(sum(r["pending_amount"] for r in rows)))])
    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []
    elements.extend(build_header(company_name, branch_name))
    elements.append(Paragraph("Pending EMI Report", STYLE_TITLE))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(f"<b>Total Loans with Pending EMI:</b>  {len(rows)}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Pending Amount:</b>  {fmt_currency(sum(r['pending_amount'] for r in rows))}", STYLE_INFO))
    elements.append(Spacer(1, 14))
    tbl = Table(table_data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style(total_row_idx))
    elements.append(tbl)
    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=pending_emi.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/officer/pending-emi/csv")
async def export_pending_emi_csv(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    branch_id = token_data["branch_id"]
    branch_filter, params = get_branch_filter(token_data, "l")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT l.loan_number, c.name AS customer_name,
                   SUM(e.emi_amount) AS pending_amount,
                   COUNT(*) FILTER (WHERE e.status = 'pending') AS pending_emi_count
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            JOIN emi_schedule e ON e.loan_id = l.id
            WHERE e.status = 'pending' AND l.tenant_id = %s {branch_filter}
            GROUP BY l.loan_number, c.name
            ORDER BY pending_amount DESC
        """, (tenant_id, *params))
        rows = [dict(r) for r in cursor.fetchall()]
        company_name, branch_name = _get_company_branch(cursor, tenant_id, branch_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Pending EMI Report"])
    writer.writerow(["Company", company_name])
    writer.writerow(["Branch", branch_name])
    writer.writerow([])
    writer.writerow(["Loan No", "Customer", "Pending EMIs", "Pending Amount"])
    for r in rows:
        writer.writerow([r["loan_number"], r["customer_name"],
                         r["pending_emi_count"], fmt_currency(r["pending_amount"])])
    writer.writerow(["TOTAL", "", "", fmt_currency(sum(r["pending_amount"] for r in rows))])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "pending_emi.csv")


@api_router.get("/officer/agent-performance/pdf")
async def export_agent_performance_pdf(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    branch_id = token_data["branch_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COALESCE(u.full_name, u.username) AS agent_name,
                   COUNT(p.id) AS total_collections,
                   COALESCE(SUM(p.amount), 0) AS total_amount
            FROM payments p
            JOIN users u ON p.collected_by = u.id
            WHERE p.status = 'approved' AND p.branch_id = %s AND p.tenant_id = %s
            GROUP BY u.id, u.username, u.full_name
            ORDER BY total_amount DESC
        """, (branch_id, tenant_id))
        rows = [dict(r) for r in cursor.fetchall()]
        company_name, branch_name = _get_company_branch(cursor, tenant_id, branch_id)
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.46, usable_w * 0.27, usable_w * 0.27]
    table_data = [[_pb("Agent"), _pb("Collections"), _pb("Total Amount")]]
    for r in rows:
        table_data.append([_p(r["agent_name"]), _p(str(r["total_collections"])), _p(fmt_currency(r["total_amount"]))])
    total_row_idx = len(table_data)
    table_data.append([_pb("TOTAL"), _pb(str(sum(r["total_collections"] for r in rows))),
                       _pb(fmt_currency(sum(r["total_amount"] for r in rows)))])
    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []
    elements.extend(build_header(company_name, branch_name))
    elements.append(Paragraph("Agent Performance Report", STYLE_TITLE))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(f"<b>Total Agents:</b>  {len(rows)}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Collected:</b>  {fmt_currency(sum(r['total_amount'] for r in rows))}", STYLE_INFO))
    elements.append(Spacer(1, 14))
    tbl = Table(table_data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style(total_row_idx))
    elements.append(tbl)
    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=agent_performance.pdf"})


@api_router.get("/officer/agent-performance/csv")
async def export_agent_performance_csv(token: str = None):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    branch_id = token_data["branch_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COALESCE(u.full_name, u.username) AS agent_name,
                   COUNT(p.id) AS total_collections,
                   COALESCE(SUM(p.amount), 0) AS total_amount
            FROM payments p
            JOIN users u ON p.collected_by = u.id
            WHERE p.status = 'approved' AND p.branch_id = %s AND p.tenant_id = %s
            GROUP BY u.id, u.username, u.full_name
            ORDER BY total_amount DESC
        """, (branch_id, tenant_id))
        rows = [dict(r) for r in cursor.fetchall()]
        company_name, branch_name = _get_company_branch(cursor, tenant_id, branch_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Agent Performance Report"])
    writer.writerow(["Company", company_name])
    writer.writerow(["Branch", branch_name])
    writer.writerow([])
    writer.writerow(["Agent", "Collections", "Total Amount"])
    for r in rows:
        writer.writerow([r["agent_name"], r["total_collections"], fmt_currency(r["total_amount"])])
    writer.writerow(["TOTAL", sum(r["total_collections"] for r in rows),
                     fmt_currency(sum(r["total_amount"] for r in rows))])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "agent_performance.csv")


@api_router.get("/officer/active-loans")
async def get_active_loans(token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    branch_filter, params = get_branch_filter(token_data, "l")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT l.id AS loan_id, l.loan_number, c.id AS customer_id, c.name AS customer_name,
                   l.loan_type, l.amount, l.tenure, l.created_at
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            WHERE l.status = 'active' AND l.tenant_id = %s
            {branch_filter}
            ORDER BY l.created_at DESC
        """, (tenant_id, *params))
        return [dict(row) for row in cursor.fetchall()]


@api_router.get("/officer/pending-emi")
async def get_pending_emi(token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    branch_filter, params = get_branch_filter(token_data, "l")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT l.id AS loan_id, l.loan_number, c.id AS customer_id, c.name AS customer_name,
                   SUM(e.emi_amount) AS pending_amount,
                   COUNT(*) FILTER (WHERE e.status = 'pending') AS pending_emi_count
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            JOIN emi_schedule e ON e.loan_id = l.id
            WHERE e.status = 'pending' AND l.tenant_id = %s
            {branch_filter}
            GROUP BY l.id, l.loan_number, c.id, c.name
            ORDER BY pending_amount DESC
        """, (tenant_id, *params))
        return [dict(row) for row in cursor.fetchall()]


@api_router.get("/officer/loan-details/{loan_id}")
async def get_loan_details(
    loan_id: str,
    pending_only: bool = False,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        # Loan summary with status and user mobile
        cursor.execute("""
            SELECT l.id, l.amount, l.loan_type, l.tenure, l.status AS loan_status,
                   c.name AS customer_name, u.phone AS mobile,
                   COALESCE(SUM(CASE WHEN e.status = 'paid' THEN e.emi_amount ELSE 0 END), 0) AS total_paid,
                   COALESCE(SUM(CASE WHEN e.status = 'pending' THEN e.emi_amount ELSE 0 END), 0) AS total_pending
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN users u ON c.user_id = u.id
            JOIN emi_schedule e ON e.loan_id = l.id
            WHERE l.id = %s AND l.tenant_id = %s
            GROUP BY l.id, c.name, u.phone
        """, (loan_id, tenant_id))
        summary = cursor.fetchone()
        if not summary:
            raise HTTPException(404, "Loan not found")
        # EMI schedule — optionally filter to pending only
        emi_filter = "AND status = 'pending'" if pending_only else ""
        cursor.execute(f"""
            SELECT emi_number, emi_amount, due_date, status
            FROM emi_schedule
            WHERE loan_id = %s {emi_filter}
            ORDER BY emi_number
        """, (loan_id,))
        emis = [dict(row) for row in cursor.fetchall()]
        return {**dict(summary), "emis": emis}


@api_router.get("/officer/agent-profile/{agent_id}")
async def get_officer_agent_profile(
    agent_id: str,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT u.id, u.username, u.role, u.created_at, u.full_name, u.email, u.phone,
                   b.name AS branch_name
            FROM users u
            LEFT JOIN branches b ON u.branch_id = b.id
            WHERE u.id = %s AND u.tenant_id = %s
        """, (agent_id, tenant_id))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Agent not found")
        return dict(row)


@api_router.get("/officer/agent-collections/{agent_id}")
async def get_officer_agent_collections(
    agent_id: str,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT p.loan_id, c.name AS customer_name, p.amount, p.created_at
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            WHERE p.collected_by = %s AND p.status = 'approved' AND l.tenant_id = %s
            ORDER BY p.created_at DESC
        """, (agent_id, tenant_id))
        rows = [dict(r) for r in cursor.fetchall()]
        total_amount = sum(r['amount'] for r in rows)
        return {
            "total_collections": len(rows),
            "total_amount": total_amount,
            "collections": rows
        }


@api_router.get("/officer/user-details/{customer_id}")
async def get_officer_user_details(
    customer_id: str,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT u.id, u.username, u.role, u.created_at,
                   b.name AS branch_name,
                   c.name, c.cibil_score, c.monthly_income,
                   u.email, u.phone
            FROM customers c
            JOIN users u ON c.user_id = u.id
            LEFT JOIN branches b ON u.branch_id = b.id
            WHERE c.id = %s AND c.tenant_id = %s
        """, (customer_id, tenant_id))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Customer not found")
        result = dict(row)
        for key, val in result.items():
            if hasattr(val, 'isoformat'):
                result[key] = val.isoformat()
        return result


@api_router.get("/officer/agent-performance")
async def get_agent_performance(
    agent_id: str = None,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]

    with get_db() as conn:
        cursor = conn.cursor()

        if agent_id:
            # Drill-down: individual agent collections
            cursor.execute("""
                SELECT u.id AS agent_id,
                       COALESCE(u.full_name, u.username) AS agent_name,
                       p.id AS payment_id,
                       p.loan_id,
                       p.amount,
                       p.payment_date,
                       p.status
                FROM payments p
                JOIN users u ON p.collected_by = u.id
                WHERE p.status = 'approved'
                  AND p.branch_id = %s AND p.tenant_id = %s
                  AND u.id = %s
                ORDER BY p.payment_date DESC
                """,
                (branch_id, tenant_id, agent_id)
            )
            rows = [dict(r) for r in cursor.fetchall()]
            if not rows:
                return {"agent_id": agent_id, "agent_name": "Unknown", "collections": [], "total_collections": 0, "total_amount": 0}
            return {
                "agent_id": agent_id,
                "agent_name": rows[0]["agent_name"],
                "total_collections": len(rows),
                "total_amount": sum(r["amount"] for r in rows),
                "collections": rows,
            }

        # Summary: all agents
        cursor.execute("""
            SELECT u.id AS agent_id,
                   COALESCE(u.full_name, u.username) AS agent_name,
                   COUNT(p.id) AS total_collections,
                   COALESCE(SUM(p.amount), 0) AS total_amount
            FROM payments p
            JOIN users u ON p.collected_by = u.id
            WHERE p.status = 'approved'
              AND p.branch_id = %s AND p.tenant_id = %s
            GROUP BY u.id, u.username, u.full_name
            ORDER BY total_amount DESC
            """,
            (branch_id, tenant_id)
        )
        return [dict(row) for row in cursor.fetchall()]


@api_router.get("/officer/customer/{customer_id}")
async def get_officer_customer_detail(
    customer_id: str,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]

    with get_db() as conn:
        cursor = conn.cursor()

        # Customer profile
        cursor.execute("""
            SELECT c.id, c.name, c.cibil_score, c.monthly_income,
                   u.email, u.phone, u.created_at
            FROM customers c
            JOIN users u ON c.user_id = u.id
            WHERE c.id = %s AND c.tenant_id = %s
            """,
            (customer_id, tenant_id)
        )
        customer = cursor.fetchone()
        if not customer:
            raise HTTPException(404, "Customer not found")
        result = dict(customer)

        # Loans for this customer in this branch
        cursor.execute("""
            SELECT id, loan_type, amount, status, outstanding_balance, created_at
            FROM loans
            WHERE customer_id = %s AND tenant_id = %s AND branch_id = %s
            ORDER BY created_at DESC
            """,
            (customer_id, tenant_id, branch_id)
        )
        result["loans"] = [dict(r) for r in cursor.fetchall()]

        # Recent payments
        cursor.execute("""
            SELECT p.payment_date, p.amount, p.status
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            WHERE l.customer_id = %s AND p.tenant_id = %s AND p.branch_id = %s
            ORDER BY p.payment_date DESC
            LIMIT 10
            """,
            (customer_id, tenant_id, branch_id)
        )
        result["payments"] = [dict(r) for r in cursor.fetchall()]

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Officer Export Endpoints — Loan EMI + Agent Collections
# ─────────────────────────────────────────────────────────────────────────────

def _get_loan_export_data(loan_id: str, tenant_id: str):
    """Shared data fetch for loan export endpoints."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT l.id, l.amount, l.loan_type, l.tenure, l.status AS loan_status,
                   c.name AS customer_name, u.phone AS mobile,
                   b.name AS branch_name,
                   COALESCE(SUM(CASE WHEN e.status = 'paid' THEN e.emi_amount ELSE 0 END), 0) AS total_paid,
                   COALESCE(SUM(CASE WHEN e.status = 'pending' THEN e.emi_amount ELSE 0 END), 0) AS total_pending
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN users u ON c.user_id = u.id
            LEFT JOIN branches b ON l.branch_id = b.id
            JOIN emi_schedule e ON e.loan_id = l.id
            WHERE l.id = %s AND l.tenant_id = %s
            GROUP BY l.id, c.name, u.phone, b.name
        """, (loan_id, tenant_id))
        summary = cursor.fetchone()
        if not summary:
            raise HTTPException(404, "Loan not found")
        cursor.execute("""
            SELECT emi_number, emi_amount, due_date, status
            FROM emi_schedule WHERE loan_id = %s ORDER BY emi_number
        """, (loan_id,))
        emis = [dict(r) for r in cursor.fetchall()]

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        t = cursor.fetchone()
        company_name = t["name"] if t else "SV Fincloud"

    return dict(summary), emis, company_name


def _get_agent_export_data(agent_id: str, tenant_id: str):
    """Shared data fetch for agent export endpoints."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT u.username, u.full_name, u.email, u.phone,
                   b.name AS branch_name
            FROM users u
            LEFT JOIN branches b ON u.branch_id = b.id
            WHERE u.id = %s AND u.tenant_id = %s
        """, (agent_id, tenant_id))
        agent = cursor.fetchone()
        if not agent:
            raise HTTPException(404, "Agent not found")

        cursor.execute("""
            SELECT p.loan_id, p.payment_number, l.loan_number, c.name AS customer_name, p.amount, p.created_at
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            WHERE p.collected_by = %s AND p.status = 'approved' AND l.tenant_id = %s
            ORDER BY p.created_at DESC
        """, (agent_id, tenant_id))
        collections = [dict(r) for r in cursor.fetchall()]

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        t = cursor.fetchone()
        company_name = t["name"] if t else "SV Fincloud"

    return dict(agent), collections, company_name


@api_router.get("/officer/loan-export/pdf/{loan_id}")
async def export_loan_pdf(
    loan_id: str,
    token: str = None,
):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    summary, emis, company_name = _get_loan_export_data(loan_id, token_data["tenant_id"])

    branch_name = summary.get("branch_name") or "Branch"
    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []

    elements.extend(build_header(company_name, branch_name))
    elements.append(Paragraph("Loan EMI Report", STYLE_TITLE))
    elements.append(Spacer(1, 4))

    # Summary
    elements.append(Paragraph(f"<b>Customer:</b>  {summary['customer_name']}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Mobile:</b>  {summary.get('mobile') or 'N/A'}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Loan Type:</b>  {(summary.get('loan_type') or '').replace('_', ' ').upper()}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Loan Amount:</b>  {fmt_currency(summary['amount'])}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Tenure:</b>  {summary['tenure']} months", STYLE_INFO))
    elements.append(Paragraph(f"<b>Status:</b>  {(summary.get('loan_status') or '').upper()}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Paid:</b>  {fmt_currency(summary['total_paid'])}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Pending:</b>  {fmt_currency(summary['total_pending'])}", STYLE_INFO))
    elements.append(Spacer(1, 14))

    # EMI table
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.12, usable_w * 0.28, usable_w * 0.32, usable_w * 0.28]
    table_data = [[_pb("EMI #"), _pb("Amount"), _pb("Due Date"), _pb("Status")]]
    for e in emis:
        due = fmt_date(e["due_date"]) if e.get("due_date") else "-"
        table_data.append([
            _p(str(e["emi_number"])),
            _p(fmt_currency(e["emi_amount"])),
            _p(due),
            _p((e["status"] or "").upper()),
        ])

    tbl = Table(table_data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style())
    elements.append(tbl)

    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return StreamingResponse(
        buffer, media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=loan_emi_{loan_id[:8]}.pdf"}
    )


@api_router.get("/officer/loan-export/csv/{loan_id}")
async def export_loan_csv(
    loan_id: str,
    token: str = None,
):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    summary, emis, company_name = _get_loan_export_data(loan_id, token_data["tenant_id"])

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Loan EMI Report"])
    writer.writerow(["Customer", summary["customer_name"]])
    writer.writerow(["Mobile", summary.get("mobile") or "N/A"])
    writer.writerow(["Loan Type", (summary.get("loan_type") or "").replace("_", " ").upper()])
    writer.writerow(["Loan Amount", fmt_currency(summary["amount"])])
    writer.writerow(["Tenure", f"{summary['tenure']} months"])
    writer.writerow(["Status", (summary.get("loan_status") or "").upper()])
    writer.writerow(["Total Paid", fmt_currency(summary["total_paid"])])
    writer.writerow(["Total Pending", fmt_currency(summary["total_pending"])])
    writer.writerow([])
    writer.writerow(["EMI #", "Amount", "Due Date", "Status"])
    for e in emis:
        due = fmt_date(e["due_date"]) if e.get("due_date") else "-"
        writer.writerow([e["emi_number"], fmt_currency(e["emi_amount"]), due, (e["status"] or "").upper()])
    output.seek(0)
    return _make_csv_response(output.getvalue(), f"loan_emi_{loan_id[:8]}.csv")


@api_router.get("/officer/agent-export/pdf/{agent_id}")
async def export_agent_pdf(
    agent_id: str,
    token: str = None,
):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    agent, collections, company_name = _get_agent_export_data(agent_id, token_data["tenant_id"])

    branch_name = agent.get("branch_name") or "Branch"
    total_amount = sum(float(c["amount"]) for c in collections)

    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []

    elements.extend(build_header(company_name, branch_name))
    elements.append(Paragraph("Agent Collection Report", STYLE_TITLE))
    elements.append(Spacer(1, 4))

    agent_name = agent.get("full_name") or agent.get("username") or "Agent"
    elements.append(Paragraph(f"<b>Agent:</b>  {agent_name}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Username:</b>  {agent.get('username') or 'N/A'}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Phone:</b>  {agent.get('phone') or 'N/A'}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Collections:</b>  {len(collections)}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Amount Collected:</b>  {fmt_currency(total_amount)}", STYLE_INFO))
    elements.append(Spacer(1, 14))

    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.32, usable_w * 0.24, usable_w * 0.22, usable_w * 0.22]
    table_data = [[_pb("Customer"), _pb("Loan No"), _pb("Amount"), _pb("Date")]]
    for c in collections:
        table_data.append([
            _p(c["customer_name"]),
            _p(c["loan_number"] or str(c["loan_id"])),
            _p(fmt_currency(c["amount"])),
            _p(fmt_date(c["created_at"])),
        ])

    tbl = Table(table_data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style())
    elements.append(tbl)

    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return StreamingResponse(
        buffer, media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=agent_collections_{agent_id[:8]}.pdf"}
    )


@api_router.get("/officer/agent-export/csv/{agent_id}")
async def export_agent_csv(
    agent_id: str,
    token: str = None,
):
    token_data = verify_token_query(token)
    if token_data.get('role') not in ['finance_officer']:
        raise HTTPException(403, "Access denied")
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    agent, collections, company_name = _get_agent_export_data(agent_id, token_data["tenant_id"])

    total_amount = sum(float(c["amount"]) for c in collections)
    agent_name = agent.get("full_name") or agent.get("username") or "Agent"

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Agent Collection Report"])
    writer.writerow(["Agent", agent_name])
    writer.writerow(["Username", agent.get("username") or "N/A"])
    writer.writerow(["Branch", agent.get("branch_name") or "N/A"])
    writer.writerow(["Total Collections", len(collections)])
    writer.writerow(["Total Amount", fmt_currency(total_amount)])
    writer.writerow([])
    writer.writerow(["Customer", "Loan No", "Amount", "Date"])
    for c in collections:
        writer.writerow([c["customer_name"], c["loan_number"] or c["loan_id"], fmt_currency(c["amount"]), fmt_date(c["created_at"])])
    output.seek(0)
    return _make_csv_response(output.getvalue(), f"agent_collections_{agent_id[:8]}.csv")


@api_router.get("/finance/reconciliation-report")
async def get_reconciliation_report(
    from_date: str,
    to_date: str,
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]

    with get_db() as conn:
        cursor = conn.cursor()

        # ── ALL-TIME transactions (no date filter) — single source of truth ──
        cursor.execute("""
            SELECT 'Payment' AS type, p.amount
            FROM payments p
            WHERE p.status = 'approved'
              AND p.branch_id = %s AND p.tenant_id = %s
            UNION ALL
            SELECT 'Disbursement' AS type, l.amount
            FROM loans l
            WHERE l.status IN ('active', 'closed')
              AND l.branch_id = %s AND l.tenant_id = %s
        """, (branch_id, tenant_id, branch_id, tenant_id))
        alltime_rows = cursor.fetchall()
        alltime_collected = sum(float(r["amount"]) for r in alltime_rows if r["type"] == "Payment")
        alltime_disbursed = sum(float(r["amount"]) for r in alltime_rows if r["type"] == "Disbursement")

        # ── DATE-FILTERED transactions — same UNION, drives both table and summary ──
        cursor.execute("""
            SELECT 'Payment' AS type, p.loan_id AS reference_id,
                   p.amount, p.payment_date AS timestamp
            FROM payments p
            WHERE p.status = 'approved'
              AND p.branch_id = %s AND p.tenant_id = %s
              AND DATE(p.payment_date) BETWEEN %s AND %s
            UNION ALL
            SELECT 'Disbursement' AS type, l.id AS reference_id,
                   l.amount, l.approved_at AS timestamp
            FROM loans l
            WHERE l.status IN ('active', 'closed')
              AND l.branch_id = %s AND l.tenant_id = %s
              AND DATE(l.approved_at) BETWEEN %s AND %s
            ORDER BY timestamp DESC
        """, (branch_id, tenant_id, from_date, to_date,
              branch_id, tenant_id, from_date, to_date))
        transactions = [dict(row) for row in cursor.fetchall()]

        # ── Totals derived from the same transaction list ─────────────────
        total_credit = sum(float(t["amount"]) for t in transactions if t["type"] == "Payment")
        total_debit  = sum(float(t["amount"]) for t in transactions if t["type"] == "Disbursement")

    return {
        "from_date": from_date,
        "to_date": to_date,
        # All-time stats — not affected by date filter
        "stats": {
            "total_cash_collected": alltime_collected,
            "total_loans_disbursed": alltime_disbursed,
            "net_cash": alltime_collected - alltime_disbursed,
        },
        # Date-filtered summary — derived from same UNION as transactions table
        "summary": {
            "total_cash_collected": total_credit,
            "total_loans_disbursed": total_debit,
        },
        # Totals row for the transactions table
        "totals": {
            "credit": total_credit,
            "debit": total_debit,
        },
        "transactions": transactions,
    }


@api_router.get("/finance/reconciliation-report/download")
async def download_reconciliation_report(
    from_date: str,
    to_date: str,
    format: str = "csv",
    token_data: dict = Depends(require_role(['finance_officer']))
):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    branch_id = token_data["branch_id"]
    tenant_id = token_data["tenant_id"]

    def fmt_currency(amount):
        """Format as ₹1,23,456"""
        return f"\u20b9{int(round(float(amount))):,}"

    def fmt_ts(ts):
        """Format timestamp as 16/03/2026 06:10 PM"""
        if ts is None:
            return "-"
        if hasattr(ts, "strftime"):
            return ts.strftime("%d/%m/%Y %I:%M %p")
        try:
            from dateutil import parser as dtparser
            return dtparser.parse(str(ts)).strftime("%d/%m/%Y %I:%M %p")
        except Exception:
            return str(ts)[:16]

    with get_db() as conn:
        cursor = conn.cursor()

        # All-time stats — same UNION source as transactions
        cursor.execute("""
            SELECT 'Payment' AS type, p.amount
            FROM payments p
            WHERE p.status = 'approved'
              AND p.branch_id = %s AND p.tenant_id = %s
            UNION ALL
            SELECT 'Disbursement' AS type, l.amount
            FROM loans l
            WHERE l.status IN ('active', 'closed')
              AND l.branch_id = %s AND l.tenant_id = %s
        """, (branch_id, tenant_id, branch_id, tenant_id))
        alltime_rows = cursor.fetchall()
        alltime_collected = sum(float(r["amount"]) for r in alltime_rows if r["type"] == "Payment")
        alltime_disbursed = sum(float(r["amount"]) for r in alltime_rows if r["type"] == "Disbursement")

        # Filtered transactions
        cursor.execute("""
            SELECT 'Payment' AS type, p.loan_id AS reference_id,
                   p.amount, p.payment_date AS timestamp
            FROM payments p
            WHERE p.status = 'approved'
              AND p.branch_id = %s AND p.tenant_id = %s
              AND DATE(p.payment_date) BETWEEN %s AND %s
            UNION ALL
            SELECT 'Disbursement' AS type, l.id AS reference_id,
                   l.amount, l.approved_at AS timestamp
            FROM loans l
            WHERE l.status IN ('active', 'closed')
              AND l.branch_id = %s AND l.tenant_id = %s
              AND DATE(l.approved_at) BETWEEN %s AND %s
            ORDER BY timestamp DESC
        """, (branch_id, tenant_id, from_date, to_date,
              branch_id, tenant_id, from_date, to_date))
        transactions = [dict(r) for r in cursor.fetchall()]

        # Company and branch names
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        tenant_row = cursor.fetchone()
        company_name = tenant_row["name"] if tenant_row else "SV Fincloud"

        cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s", (branch_id, tenant_id))
        branch_row = cursor.fetchone()
        branch_name = branch_row["name"] if branch_row else "Main Branch"

    total_credit = sum(float(t["amount"]) for t in transactions if t["type"] == "Payment")
    total_debit  = sum(float(t["amount"]) for t in transactions if t["type"] == "Disbursement")
    from_fmt = datetime.strptime(from_date, "%Y-%m-%d").strftime("%d/%m/%Y")
    to_fmt   = datetime.strptime(to_date,   "%Y-%m-%d").strftime("%d/%m/%Y")
    period_str = f"{from_fmt} \u2013 {to_fmt}"

    # ── CSV ──────────────────────────────────────────────────────────────
    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Reconciliation Report"])
        writer.writerow(["Company", company_name])
        writer.writerow(["Branch", branch_name])
        writer.writerow(["Period", period_str])
        writer.writerow([])
        writer.writerow(["Total Cash Collected (All Time)", fmt_currency(alltime_collected)])
        writer.writerow(["Total Loans Disbursed (All Time)", fmt_currency(alltime_disbursed)])
        writer.writerow(["Net Branch Cash (All Time)", fmt_currency(alltime_collected - alltime_disbursed)])
        writer.writerow([])
        writer.writerow(["Type", "Reference", "Credit (+)", "Debit (-)", "Timestamp"])
        for t in transactions:
            is_credit = t["type"] == "Payment"
            writer.writerow([
                t["type"],
                t["reference_id"],
                fmt_currency(t["amount"]) if is_credit else "",
                fmt_currency(t["amount"]) if not is_credit else "",
                fmt_ts(t["timestamp"]),
            ])
        writer.writerow(["TOTAL", "", fmt_currency(total_credit), fmt_currency(total_debit), ""])
        output.seek(0)
        return _make_csv_response(output.getvalue(), f"reconciliation_{from_date}_{to_date}.csv")

    # ── PDF ──────────────────────────────────────────────────────────────
    buffer = io.BytesIO()
    margin = PAGE_MARGIN
    doc = make_doc(buffer)
    elements = []

    elements.extend(build_header(company_name, branch_name, period_str))

    # Summary block
    net = alltime_collected - alltime_disbursed
    net_color = "#059669" if net >= 0 else "#dc2626"
    elements.append(Paragraph(f"<b>Total Cash Collected (All Time):</b>  {fmt_currency(alltime_collected)}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Total Loans Disbursed (All Time):</b>  {fmt_currency(alltime_disbursed)}", STYLE_INFO))
    elements.append(Paragraph(f"<b>Net Branch Cash (All Time):</b>  <font color='{net_color}'>{fmt_currency(net)}</font>", STYLE_INFO))
    elements.append(Spacer(1, 14))

    usable_width = PAGE_W - 2 * margin
    # col widths: Type, Reference, Credit, Debit, Timestamp
    col_widths = [
        usable_width * 0.14,
        usable_width * 0.22,
        usable_width * 0.18,
        usable_width * 0.18,
        usable_width * 0.28,
    ]

    header_row = [_pb("Type"), _pb("Reference"), _pb("Credit (+)"), _pb("Debit (-)"), _pb("Timestamp")]
    table_data = [header_row]

    for t in transactions:
        is_credit = t["type"] == "Payment"
        ref = str(t["reference_id"])
        ref_display = ref[:20] + "\u2026" if len(ref) > 20 else ref
        table_data.append([
            _p(t["type"]),
            _p(ref_display),
            _p(fmt_currency(t["amount"]) if is_credit else "\u2014"),
            _p(fmt_currency(t["amount"]) if not is_credit else "\u2014"),
            _p(fmt_ts(t["timestamp"])),
        ])

    total_row_idx = len(table_data)
    table_data.append([_pb("TOTAL"), _pb("\u2014"), _pb(fmt_currency(total_credit)), _pb(fmt_currency(total_debit)), _pb("\u2014")])

    tbl = Table(table_data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(TableStyle([
        # Header row
        ("BACKGROUND",    (0, 0), (-1, 0), colors.HexColor("#1e3a8a")),
        ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
        ("FONTNAME",      (0, 0), (-1, 0), "DejaVu-Bold"),
        ("FONTSIZE",      (0, 0), (-1, 0), 9),
        ("ALIGN",         (0, 0), (-1, 0), "CENTER"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("TOPPADDING",    (0, 0), (-1, 0), 8),
        # Data rows
        ("FONTNAME",      (0, 1), (-1, -1), "DejaVu"),
        ("FONTSIZE",      (0, 1), (-1, -1), 9),
        ("ALIGN",         (0, 1), (-1, -1), "CENTER"),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
        ("TOPPADDING",    (0, 1), (-1, -1), 6),
        ("ROWBACKGROUNDS",(0, 1), (-1, total_row_idx - 1), [colors.white, colors.HexColor("#f8fafc")]),
        ("GRID",          (0, 0), (-1, -1), 0.4, colors.HexColor("#e2e8f0")),
        # TOTAL row
        ("BACKGROUND",    (0, total_row_idx), (-1, total_row_idx), colors.HexColor("#f0f4ff")),
        ("FONTNAME",      (0, total_row_idx), (-1, total_row_idx), "DejaVu-Bold"),
        ("FONTSIZE",      (0, total_row_idx), (-1, total_row_idx), 10),
        ("LINEABOVE",     (0, total_row_idx), (-1, total_row_idx), 1.5, colors.HexColor("#2563eb")),
        ("BOTTOMPADDING", (0, total_row_idx), (-1, total_row_idx), 8),
        ("TOPPADDING",    (0, total_row_idx), (-1, total_row_idx), 8),
    ]))
    elements.append(tbl)

    def add_footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#e2e8f0"))
        canvas.line(margin, 42, PAGE_W - margin, 42)
        canvas.setFont("DejaVu", 8)
        canvas.setFillColor(colors.HexColor("#94a3b8"))
        canvas.drawRightString(PAGE_W - margin, 28, f"Page {canvas.getPageNumber()}")
        canvas.drawString(margin, 28, f"Generated: {get_ist_now().strftime('%d/%m/%Y %I:%M %p')}")
        canvas.restoreState()

    doc.build(elements, onFirstPage=add_footer, onLaterPages=add_footer)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=reconciliation_{from_date}_{to_date}.pdf"}
    )


# Admin Routes
@api_router.post("/admin/create-user")
async def create_user(request: UserCreateRequest, token_data: dict = Depends(require_role(['admin','super_admin']))):
    """
    Create a new user with proper role-based access control.
    Only admin and super_admin can create users.
    """
    
    # Debug logging
    logger.debug(f"create_user endpoint called - role={request.role}, username={request.username}")
    
    def validate_permissions(token_role: str, target_role: str, branch_id: str, token_branch_id: str):
        """Validate if the requesting user has permission to create the target role."""
        logger.debug(f"validate_permissions: token_role={token_role}, target_role={target_role}, branch_id={branch_id}")
        
        # Only super_admin and admin can create users
        if token_role not in ['super_admin', 'admin']:
            logger.debug(f"Permission denied - role {token_role} not in ['super_admin', 'admin']")
            raise HTTPException(status_code=403, detail="Only admin and super_admin can create users")
        
        # Admin can only create users in their own branch
        if token_role == "admin" and branch_id != token_branch_id:
            raise HTTPException(status_code=403, detail="Admin can create users only in their branch")
        
        # Only super_admin can create admin and auditor roles
        if target_role.lower() in ["admin", "auditor"] and token_role != "super_admin":
            raise HTTPException(status_code=403, detail=f"Only Super Admin can create {target_role}")
    
    def validate_branch(cursor, branch_id: str, tenant_id: str, role: str):
        """Validate that the branch exists and belongs to the tenant."""
        # Super admin and auditor don't need branch validation
        if role.lower() in ["super_admin", "auditor"]:
            return
        
        if not branch_id:
            raise HTTPException(status_code=400, detail="Branch ID is required for this role")
        
        cursor.execute("""
            SELECT id FROM branches
            WHERE id = %s AND tenant_id = %s
        """, (branch_id, tenant_id))
        
        if not cursor.fetchone():
            raise HTTPException(status_code=400, detail="Invalid branch for this company")
    
    def check_unique_role_constraints(cursor, role: str, branch_id: str, tenant_id: str):
        """Check if role-specific uniqueness constraints are satisfied."""
        # Only one admin per branch
        if role.lower() == "admin":
            cursor.execute("""
                SELECT id FROM users 
                WHERE role = 'admin' AND branch_id = %s AND tenant_id = %s
            """, (branch_id, tenant_id))
            if cursor.fetchone():
                
                raise HTTPException(status_code=400, detail="Admin already exists for this branch")
        
        # Only one auditor per company
        if role.lower() == "auditor":
            cursor.execute("""
                SELECT id FROM users
                WHERE role = 'auditor' AND tenant_id = %s
            """, (tenant_id,))
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="Auditor already exists for this company")
    
    def create_user_record(cursor, user_id: str, request: UserCreateRequest, tenant_id: str, hashed_password: str):
        """Create the main user record in the users table."""
        cursor.execute("""
            INSERT INTO users
            (id, username, password, role, tenant_id, branch_id, created_at,
            full_name, email, phone, designation, joining_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                user_id,
                request.username,
                hashed_password,
                request.role,
                tenant_id,
                request.branch_id,
                get_ist_now().isoformat(),
                request.name,  # Maps to full_name column
                request.email,
                request.phone,
                request.designation,
                request.joining_date
            )
        )
    
    def create_employee_profile(cursor, user_id: str, request: UserCreateRequest, tenant_id: str):
        """Create employee profile for staff roles."""
        if request.role.lower() in ["collection_agent", "finance_officer", "admin"]:
            cursor.execute("""
                INSERT INTO employees
                (id, user_id, name, email, phone, designation, joining_date, tenant_id, branch_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(uuid.uuid4()),
                    user_id,
                    request.name,
                    request.email,
                    request.phone,
                    request.designation,
                    request.joining_date,
                    tenant_id,
                    request.branch_id,
                    get_ist_now().isoformat()
                )
            )
    
    def create_customer_profile(cursor, user_id: str, request: UserCreateRequest, tenant_id: str):
        """Create customer profile for customer role."""
        if request.role.lower() == 'customer':
            cursor.execute("""
                INSERT INTO customers
                (id, user_id, name, cibil_score, monthly_income, branch_id, tenant_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(uuid.uuid4()),
                    user_id,
                    request.name,
                    request.cibil_score,
                    request.monthly_income,
                    request.branch_id,
                    tenant_id,
                    get_ist_now().isoformat()
                )
            )
    
    # Main execution flow
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get tenant_id from token
        tenant_id = token_data.get("tenant_id")
        if not tenant_id:
            # Fallback: look it up from DB
            cursor.execute("SELECT tenant_id FROM users WHERE id = %s", (token_data['user_id'],))
            admin = cursor.fetchone()
            tenant_id = admin['tenant_id'] if admin else None
        
        if not tenant_id:
            raise HTTPException(status_code=401, detail="Could not determine Tenant ID")
        
        # Auto assign branch for admin if not provided
        if token_data["role"] == "admin" and not request.branch_id:
            request.branch_id = token_data["branch_id"]

        # Validate permissions
        validate_permissions(
            token_data["role"],
            request.role,
            request.branch_id,
            token_data.get("branch_id")
        )
       
        
        # Check username uniqueness
        cursor.execute("SELECT id FROM users WHERE username = %s AND tenant_id = %s", (request.username, tenant_id))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Username already exists")
        
        # Validate branch
        validate_branch(cursor, request.branch_id, tenant_id, request.role)
        
        # Check unique role constraints
        check_unique_role_constraints(cursor, request.role, request.branch_id, tenant_id)
        
        # Generate user ID and hash password
        user_id = str(uuid.uuid4())
        hashed_password = bcrypt.hashpw(
            request.password.encode("utf-8"),
            bcrypt.gensalt()
        ).decode()
        
        # Create user record
        create_user_record(cursor, user_id, request, tenant_id, hashed_password)
        
        # Create employee profile if applicable
        create_employee_profile(cursor, user_id, request, tenant_id)
        
        # Create customer profile if applicable
        create_customer_profile(cursor, user_id, request, tenant_id)
        
        # Log audit trail
        log_audit(
            conn,
            token_data['user_id'],
            tenant_id,
            'USER_CREATED',
            'user',
            user_id,
            json.dumps({"username": request.username, "role": request.role})
        )
        
        # Commit the transaction
        conn.commit()
        
        logger.info("User created: username=%s role=%s tenant=%s", request.username, request.role, tenant_id)
        return {"message": "User created successfully", "user_id": user_id}
    
@api_router.patch("/admin/users/{user_id}/role")
async def update_user_role(
    user_id: str,
    data: dict,
    token_data: dict = Depends(require_role(['admin', 'super_admin']))
):
    new_role = data.get("role")
    if not new_role:
        raise HTTPException(status_code=400, detail="role is required")
    with get_db() as conn:
        cursor = conn.cursor()
        tenant_id = token_data["tenant_id"]
        cursor.execute("UPDATE users SET role = %s WHERE id = %s AND tenant_id = %s",
            (new_role, user_id, tenant_id)
        )
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="User not found")
        log_audit(conn, token_data['user_id'], tenant_id, 'USER_ROLE_UPDATED', 'user', user_id,
                  json.dumps({"new_role": new_role}))
        return {"message": "User role updated successfully"}


@api_router.get("/admin/users")
async def get_all_users(
    branch_ids: Optional[str] = None,  # comma separated (legacy)
    branch_id: Optional[str] = None,   # single branch filter
    token_data: dict = Depends(require_role(['admin','super_admin']))
):
    if not check_permission(token_data['role'], 'users', 'view'):
        raise HTTPException(status_code=403, detail="Access denied")
    with get_db() as conn:
        cursor = conn.cursor()

        tenant_id = token_data["tenant_id"]
        base_query = """
            SELECT 
                u.id,
                u.username,
                u.role,
                u.tenant_id,
                u.branch_id,
                b.name AS branch_name,
                u.created_at,
                COALESCE(c.name, e.name, u.full_name) AS name,
                COALESCE(e.phone, u.phone) AS phone,
                COALESCE(e.email, u.email) AS email,
                COALESCE(e.designation, u.designation) AS designation,
                COALESCE(e.joining_date, u.joining_date) AS joining_date,
                c.cibil_score
            FROM users u
            LEFT JOIN branches b ON u.branch_id = b.id
            LEFT JOIN customers c ON u.id = c.user_id
            LEFT JOIN employees e ON u.id = e.user_id
            WHERE u.tenant_id = %s
        """

        params = [tenant_id]

        if token_data["role"] == "admin":
            # admin is always scoped to their own branch
            base_query += " AND u.branch_id = %s"
            params.append(token_data["branch_id"])
        else:
            # super_admin: prefer single branch_id, fall back to comma-separated branch_ids
            effective_branch = branch_id or None
            if not effective_branch and branch_ids:
                branch_list = branch_ids.split(",")
                placeholders = ",".join(["%s"] * len(branch_list))
                base_query += f" AND u.branch_id IN ({placeholders})"
                params.extend(branch_list)
            elif effective_branch:
                base_query += " AND u.branch_id = %s"
                params.append(effective_branch)

        base_query += " ORDER BY u.created_at DESC"

        cursor.execute(base_query, params)

        users = []
        for row in cursor.fetchall():
            user = dict(row)

            # format created_at
            if user.get("created_at"):
                try:
                    user["created_at"] = datetime.fromisoformat(
                        user["created_at"].replace("Z", "+00:00")
                    ).strftime("%d/%m/%Y")
                except:
                    pass

            # format joining_date
            if user.get("joining_date"):
                try:
                    user["joining_date"] = datetime.fromisoformat(
                        user["joining_date"].replace("Z", "+00:00")
                    ).strftime("%d/%m/%Y")
                except:
                    pass

            users.append(user)

        return users

@api_router.get("/admin/user-details")
async def get_user_details(user_id:str,
token_data: dict = Depends(require_role(['admin','super_admin']))):

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT role FROM users WHERE id=%s AND tenant_id=%s", (user_id, token_data["tenant_id"]))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")
        
        role = row["role"]

        if role == "customer":

            cursor.execute("""
            SELECT u.username,u.role,b.name as branch,
                   c.name,c.email,c.phone,c.cibil_score
            FROM users u
            LEFT JOIN branches b ON u.branch_id=b.id
            LEFT JOIN customers c ON u.id=c.user_id
            WHERE u.id=%s AND u.tenant_id=%s
            """,(user_id, token_data["tenant_id"]))

        else:

            cursor.execute("""
            SELECT u.username,u.role,b.name as branch,
                   e.name,e.email,e.phone,e.designation
            FROM users u
            LEFT JOIN branches b ON u.branch_id=b.id
            LEFT JOIN employees e ON u.id=e.user_id
            WHERE u.id=%s AND u.tenant_id=%s
            """,(user_id, token_data["tenant_id"]))

        result = cursor.fetchone()

        if not result:
            raise HTTPException(404, "User details not found")

        result_dict = dict(result)
        for key, val in result_dict.items():
            if hasattr(val, 'isoformat'):
                result_dict[key] = val.isoformat()
        return result_dict

@api_router.get("/admin/user/{user_id}")
async def get_user_by_id(
    user_id: str,
    token_data: dict = Depends(require_role(['admin', 'super_admin']))
):
    """
    Get detailed information for a specific user by ID.
    Returns user details including username, role, branch info, contact details, etc.
    """
    if not check_permission(token_data['role'], 'users', 'view'):
        raise HTTPException(status_code=403, detail="Access denied")
    
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Fetch user details with branch name
        cursor.execute("""
            SELECT 
                u.id,
                u.username,
                u.role,
                u.branch_id,
                b.name AS branch_name,
                u.created_at,
                COALESCE(c.name, e.name, u.full_name) AS name,
                COALESCE(e.email, u.email) AS email,
                COALESCE(e.phone, u.phone) AS phone,
                c.cibil_score,
                COALESCE(e.designation, u.designation) AS designation,
                COALESCE(e.joining_date, u.joining_date) AS joining_date
            FROM users u
            LEFT JOIN branches b ON u.branch_id = b.id
            LEFT JOIN customers c ON u.id = c.user_id
            LEFT JOIN employees e ON u.id = e.user_id
            WHERE u.id = %s AND u.tenant_id = %s
        """, (user_id, token_data["tenant_id"]))
        
        user = cursor.fetchone()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_dict = dict(user)
        for key, val in user_dict.items():
            if hasattr(val, 'isoformat'):
                user_dict[key] = val.isoformat()
        return user_dict

def get_branch_name_for_report(cursor, token_data, branch_id=None):
    """
    Determine the appropriate branch name for report headers.
    
    Args:
        cursor: Database cursor
        token_data: JWT token data with user info
        branch_id: Optional branch filter parameter
    
    Returns:
        String: Branch name or "All Branches"
    """
    if token_data.get("role") == "super_admin":
        if branch_id:
            cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                (branch_id, token_data["tenant_id"])
            )
            branch_row = cursor.fetchone()
            return branch_row["name"] if branch_row else "Branch"
        else:
            return "All Branches"
    elif token_data.get("branch_id"):
        cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
            (token_data["branch_id"], token_data["tenant_id"])
        )
        branch_row = cursor.fetchone()
        return branch_row["name"] if branch_row else "Branch"
    else:
        return "Branch"

@api_router.get("/admin/user/{user_id}/report/pdf")
async def download_user_report_pdf(
    user_id: str,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin', 'super_admin']))
):
    """
    Generate and download a PDF report for a specific user.
    """
    if not check_permission(token_data['role'], 'users', 'view'):
        raise HTTPException(status_code=403, detail="Access denied")
    
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get company name
        cursor.execute("SELECT name FROM tenants WHERE id = %s",
            (token_data["tenant_id"],)
        )
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"
        
        # Determine branch name
        branch_name = get_branch_name_for_report(cursor, token_data, branch_id)
        
        # Fetch user details
        cursor.execute("""
            SELECT 
                u.username,
                u.role,
                b.name AS branch_name,
                COALESCE(c.name, e.name, u.full_name) AS name,
                COALESCE(e.email, u.email) AS email,
                COALESCE(e.phone, u.phone) AS phone,
                u.created_at,
                c.cibil_score,
                COALESCE(e.designation, u.designation) AS designation,
                COALESCE(e.joining_date, u.joining_date) AS joining_date
            FROM users u
            LEFT JOIN branches b ON u.branch_id = b.id
            LEFT JOIN customers c ON u.id = c.user_id
            LEFT JOIN employees e ON u.id = e.user_id
            WHERE u.id = %s AND u.tenant_id = %s
        """, (user_id, token_data["tenant_id"]))
        
        user = cursor.fetchone()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_dict = dict(user)
        
        # Convert phone to string to prevent scientific notation
        if user_dict.get('phone'):
            user_dict['phone'] = str(user_dict['phone'])
        
        # Format dates
        if user_dict.get("created_at"):
            try:
                user_dict["created_at"] = datetime.fromisoformat(
                    user_dict["created_at"].replace("Z", "+00:00")
                ).strftime("%d/%m/%Y")
            except:
                pass
        
        if user_dict.get("joining_date"):
            try:
                user_dict["joining_date"] = datetime.fromisoformat(
                    user_dict["joining_date"].replace("Z", "+00:00")
                ).strftime("%d/%m/%Y")
            except:
                pass
        
        # Prepare PDF data
        headers = ["Field", "Value"]
        rows = [
            ["Username", user_dict.get("username", "N/A")],
            ["Name", user_dict.get("name", "N/A")],
            ["Role", user_dict.get("role", "N/A").replace("_", " ").title()],
            ["Branch", user_dict.get("branch_name", "N/A")],
            ["Email", user_dict.get("email", "N/A")],
            ["Phone", user_dict.get("phone", "N/A")],
            ["CIBIL Score", str(user_dict.get("cibil_score", "N/A"))],
            ["Designation", user_dict.get("designation", "N/A")],
            ["Joining Date", user_dict.get("joining_date", "N/A")],
            ["Created At", user_dict.get("created_at", "N/A")]
        ]
        
        totals_info = {
            "Report Type": "User Details Report"
        }
        
        pdf_buffer = generate_report_pdf(
            title="User Details Report",
            headers=headers,
            rows=rows,
            totals_info=totals_info,
            company_name=company_name,
            branch_name=branch_name,
            col_widths=[2.5 * inch, 4.5 * inch]
        )
        
        pdf_buffer.seek(0)
        
        return StreamingResponse(
            pdf_buffer,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename=user_report_{user_dict['username']}.pdf",
                "Access-Control-Expose-Headers": "Content-Disposition",
            }
        )

@api_router.get("/admin/user/{user_id}/report/csv")
async def download_user_report_csv(
    user_id: str,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin', 'super_admin']))
):
    """
    Generate and download a CSV report for a specific user with Excel-safe phone number formatting.
    """
    if not check_permission(token_data['role'], 'users', 'view'):
        raise HTTPException(status_code=403, detail="Access denied")
    
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Determine branch name
        branch_name = get_branch_name_for_report(cursor, token_data, branch_id)
        
        # Fetch user details (same query as PDF endpoint)
        cursor.execute("""
            SELECT 
                u.username,
                u.role,
                b.name AS branch_name,
                COALESCE(c.name, e.name, u.full_name) AS name,
                COALESCE(e.email, u.email) AS email,
                COALESCE(e.phone, u.phone) AS phone,
                u.created_at,
                c.cibil_score,
                COALESCE(e.designation, u.designation) AS designation,
                COALESCE(e.joining_date, u.joining_date) AS joining_date
            FROM users u
            LEFT JOIN branches b ON u.branch_id = b.id
            LEFT JOIN customers c ON u.id = c.user_id
            LEFT JOIN employees e ON u.id = e.user_id
            WHERE u.id = %s AND u.tenant_id = %s
        """, (user_id, token_data["tenant_id"]))
        
        user = cursor.fetchone()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_dict = dict(user)
        
        # Format dates
        if user_dict.get("created_at"):
            try:
                user_dict["created_at"] = datetime.fromisoformat(
                    user_dict["created_at"].replace("Z", "+00:00")
                ).strftime("%d/%m/%Y")
            except:
                pass
        
        if user_dict.get("joining_date"):
            try:
                user_dict["joining_date"] = datetime.fromisoformat(
                    user_dict["joining_date"].replace("Z", "+00:00")
                ).strftime("%d/%m/%Y")
            except:
                pass
        
        # Generate CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write CSV summary header
        totals = {
            "Total Records": 1
        }
        write_csv_summary(writer, "User Details Report", branch_name, totals)
        
        # Write table headers
        writer.writerow(["Field", "Value"])
        
        # Write user data with Excel-safe phone number formatting
        phone_value = user_dict.get("phone", "N/A")
        if phone_value and phone_value != "N/A":
            phone_value = str(phone_value)
            phone_value = f'="{phone_value}"'  # Excel formula to prevent scientific notation
        
        writer.writerow(["Username", user_dict.get("username", "N/A")])
        writer.writerow(["Name", user_dict.get("name", "N/A")])
        writer.writerow(["Role", user_dict.get("role", "N/A").replace("_", " ").title()])
        writer.writerow(["Branch", user_dict.get("branch_name", "N/A")])
        writer.writerow(["Email", user_dict.get("email", "N/A")])
        writer.writerow(["Phone", phone_value])
        writer.writerow(["CIBIL Score", str(user_dict.get("cibil_score", "N/A"))])
        writer.writerow(["Designation", user_dict.get("designation", "N/A")])
        writer.writerow(["Joining Date", user_dict.get("joining_date", "N/A")])
        writer.writerow(["Created At", user_dict.get("created_at", "N/A")])
        
        output.seek(0)
        return _make_csv_response(output.getvalue(), f"user_report_{user_dict['username']}.csv")

@api_router.get("/admin/report/disbursed/download")
async def download_disbursed_report(
    format: str = "csv",
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (token_data["tenant_id"],))
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"

        if token_data.get("role") == "super_admin":
            effective_branch_id = branch_id or (branch_ids.split(",")[0] if branch_ids else None)
            if effective_branch_id:
                cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                    (effective_branch_id, token_data["tenant_id"]))
                branch_row = cursor.fetchone()
                branch_name = branch_row["name"] if branch_row else "Branch"
            else:
                branch_name = "All Branches"
        elif token_data.get("branch_id"):
            cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                (token_data["branch_id"], token_data["tenant_id"]))
            branch_row = cursor.fetchone()
            branch_name = branch_row["name"] if branch_row else "Branch"
        else:
            branch_name = "Branch"

        query = """
        SELECT
            c.name as customer,
            l.loan_type,
            l.amount as loan_amount,
            b.name as branch
        FROM loans l
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE l.status != 'pending'
        AND l.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)

        rows = [dict(r) for r in cursor.fetchall()]

    # =========================
    # CSV EXPORT (Excel)
    # =========================

    if format == "csv":

        output = io.StringIO()
        writer = csv.writer(output)

        # Write CSV summary header
        total_loan_amount = sum(r["loan_amount"] for r in rows)
        totals = {
            "Total Records": len(rows),
            "Total Loan Amount": total_loan_amount
        }
        write_csv_summary(writer, "Disbursed Loans Report", branch_name, totals)

        # Write table headers
        writer.writerow(["Customer","Loan Type","Loan Amount","Branch"])

        for r in rows:
            writer.writerow([
                r["customer"],
                r["loan_type"],
                fmt_currency(r["loan_amount"]),
                r["branch"]
            ])

        output.seek(0)
        return _make_csv_response(output.getvalue(), "disbursed_report.csv")

    # =========================
    # PDF EXPORT
    # =========================

    if format == "pdf":
        # Calculate totals
        total_records = len(rows)
        total_loan_amount = sum(r["loan_amount"] for r in rows)

        # Prepare table data
        table_rows = []
        for r in rows:
            table_rows.append([
                r["customer"],
                r["loan_type"],
                f"₹{r['loan_amount']:,.2f}",
                r["branch"]
            ])

        # Generate PDF using helper function
        totals_info = {
            "Total Records": total_records,
            "Total Loan Amount": f"₹{total_loan_amount:,.2f}"
        }

        buffer = generate_report_pdf(
            title="Disbursed Loans Report",
            headers=["Customer", "Loan Type", "Loan Amount", "Branch"],
            rows=table_rows,
            totals_info=totals_info,
            company_name=company_name,
            branch_name=branch_name,
            from_date=None,
            to_date=None,
            col_widths=[2*inch, 1.5*inch, 1.5*inch, 2*inch]
        )

        # Generate filename with current date
        current_date = get_ist_now().strftime("%Y-%m-%d")
        filename = f"disbursed_loans_{current_date}.pdf"

        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

@api_router.get("/admin/report/outstanding")
async def outstanding_report(
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        query = """
        SELECT
            c.name as customer,
            l.loan_type,
            l.amount as loan_amount,
            l.outstanding_balance as outstanding_amount,
            b.name as branch
        FROM loans l
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE l.status = 'active'
        AND l.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)

        rows = cursor.fetchall()

        return [dict(row) for row in rows]

@api_router.get("/admin/report/outstanding/download")
async def download_outstanding_report(
    format: str = "csv",
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        # Get company and branch info
        cursor.execute("SELECT name FROM tenants WHERE id = %s",
            (token_data["tenant_id"],)
        )
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"

        # Determine branch name based on role and filters
        if token_data.get("role") == "super_admin":
            effective_branch_id = branch_id or (branch_ids.split(",")[0] if branch_ids else None)
            if effective_branch_id:
                cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                    (effective_branch_id, token_data["tenant_id"])
                )
                branch_row = cursor.fetchone()
                branch_name = branch_row["name"] if branch_row else "Branch"
            else:
                branch_name = "All Branches"
        elif token_data.get("branch_id"):
            cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                (token_data["branch_id"], token_data["tenant_id"])
            )
            branch_row = cursor.fetchone()
            branch_name = branch_row["name"] if branch_row else "Branch"
        else:
            branch_name = "Branch"

        query = """
        SELECT
            c.name as customer,
            l.loan_type,
            l.amount as loan_amount,
            l.outstanding_balance as outstanding_amount,
            b.name as branch
        FROM loans l
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE l.status='active'
        AND l.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)
        rows = [dict(r) for r in cursor.fetchall()]

    # =========================
    # CSV EXPORT (Excel)
    # =========================

    if format == "csv":

        output = io.StringIO()
        writer = csv.writer(output)

        # Write CSV summary header
        total_outstanding = sum(r["outstanding_amount"] for r in rows)
        totals = {
            "Total Records": len(rows),
            "Total Outstanding Balance": total_outstanding
        }
        write_csv_summary(writer, "Outstanding Loans Report", branch_name, totals)

        # Write table headers
        writer.writerow(["Customer","Loan Type","Loan Amount","Outstanding","Branch"])

        for r in rows:
            writer.writerow([
                r["customer"],
                r["loan_type"],
                fmt_currency(r["loan_amount"]),
                fmt_currency(r["outstanding_amount"]),
                r["branch"]
            ])

        output.seek(0)
        return _make_csv_response(output.getvalue(), "outstanding_report.csv")

    # =========================
    # PDF EXPORT
    # =========================

    if format == "pdf":
        # Calculate totals
        total_records = len(rows)
        total_outstanding = sum(r["outstanding_amount"] for r in rows)

        # Prepare table data
        table_rows = []
        for r in rows:
            table_rows.append([
                r["customer"],
                r["loan_type"],
                f"₹{r['loan_amount']:,.2f}",
                f"₹{r['outstanding_amount']:,.2f}",
                r["branch"]
            ])

        # Generate PDF using helper function
        totals_info = {
            "Total Records": total_records,
            "Total Outstanding Balance": f"₹{total_outstanding:,.2f}"
        }

        buffer = generate_report_pdf(
            title="Outstanding Loans Report",
            headers=["Customer", "Loan Type", "Loan Amount", "Outstanding", "Branch"],
            rows=table_rows,
            totals_info=totals_info,
            company_name=company_name,
            branch_name=branch_name,
            from_date=None,
            to_date=None,
            col_widths=[1.5*inch, 1.5*inch, 1.5*inch, 1.5*inch, 1.5*inch]
        )

        # Generate filename with current date
        current_date = get_ist_now().strftime("%Y-%m-%d")
        filename = f"outstanding_loans_{current_date}.pdf"

        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

        # ===== HEADER STYLE =====
        company_style = ParagraphStyle(
            name='Company',
            fontSize=18,
            alignment=1,
            textColor=colors.darkblue
        )

        # ===== ELEMENTS =====
        elements = []

        elements.append(Paragraph("Outstanding Loan Report", company_style))
        elements.append(Spacer(1,10))

        # Add table after header
        elements.append(table)

        # Build PDF
        doc.build(elements)
        buffer.seek(0)

        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={
                "Content-Disposition":"attachment; filename=outstanding_report.pdf",
                "Access-Control-Expose-Headers": "Content-Disposition",
            }
        )

@api_router.get("/admin/report/interest/download")
async def download_interest_report(
    format: str = "csv",
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        # Get company and branch info
        cursor.execute("SELECT name FROM tenants WHERE id = %s",
            (token_data["tenant_id"],)
        )
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"

        # Determine branch name based on role and filters
        if token_data.get("role") == "super_admin":
            effective_branch_id = branch_id or (branch_ids.split(",")[0] if branch_ids else None)
            if effective_branch_id:
                cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                    (effective_branch_id, token_data["tenant_id"])
                )
                branch_row = cursor.fetchone()
                branch_name = branch_row["name"] if branch_row else "Branch"
            else:
                branch_name = "All Branches"
        elif token_data.get("branch_id"):
            cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                (token_data["branch_id"], token_data["tenant_id"])
            )
            branch_row = cursor.fetchone()
            branch_name = branch_row["name"] if branch_row else "Branch"
        else:
            branch_name = "Branch"

        query = """
        SELECT
            c.name as customer,
            SUM(e.interest_amount) as interest_earned,
            b.name as branch
        FROM emi_schedule e
        JOIN loans l ON e.loan_id = l.id
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE e.status='paid'
        AND l.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        query += " GROUP BY c.name, b.name"

        cursor.execute(query, params)

        rows = [dict(r) for r in cursor.fetchall()]

    # =========================
    # CSV EXPORT (Excel)
    # =========================

    if format == "csv":

        output = io.StringIO()
        writer = csv.writer(output)

        # Write CSV summary header
        total_interest = sum(r["interest_earned"] for r in rows)
        totals = {
            "Total Records": len(rows),
            "Total Interest Earned": total_interest
        }
        write_csv_summary(writer, "Interest Earned Report", branch_name, totals)

        # Write table headers
        writer.writerow(["Customer","Interest Earned","Branch"])

        for r in rows:
            writer.writerow([
                r["customer"],
                fmt_currency(r["interest_earned"]),
                r["branch"]
            ])

        output.seek(0)
        return _make_csv_response(output.getvalue(), "interest_report.csv")

    # =========================
    # PDF EXPORT
    # =========================

    if format == "pdf":
        # Calculate totals
        total_records = len(rows)
        total_interest = sum(r["interest_earned"] for r in rows)

        # Prepare table data
        table_rows = []
        for r in rows:
            table_rows.append([
                r["customer"],
                f"₹{r['interest_earned']:,.2f}",
                r["branch"]
            ])

        # Generate PDF using helper function
        totals_info = {
            "Total Records": total_records,
            "Total Interest Earned": f"₹{total_interest:,.2f}"
        }

        buffer = generate_report_pdf(
            title="Interest Earned Report",
            headers=["Customer", "Interest Earned", "Branch"],
            rows=table_rows,
            totals_info=totals_info,
            company_name=company_name,
            branch_name=branch_name,
            from_date=None,
            to_date=None,
            col_widths=[2.5*inch, 2.5*inch, 2.5*inch]
        )

        # Generate filename with current date
        current_date = get_ist_now().strftime("%Y-%m-%d")
        filename = f"interest_earned_{current_date}.pdf"

        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

@api_router.get("/admin/customer-by-name")
async def get_customer_by_name(
    name: str,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
        SELECT 
            u.id,
            u.username,
            u.role,
            u.created_at,
            b.name as branch_name,
            c.name,
            c.phone,
            c.cibil_score
        FROM customers c
        JOIN users u ON c.user_id = u.id
        LEFT JOIN branches b ON u.branch_id = b.id
        WHERE c.name = %s
        """,(name,))

        row = cursor.fetchone()

        if not row:
            raise HTTPException(404,"Customer not found")

        return dict(row)
        
@api_router.get("/admin/report/pending")
async def pending_report(
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):
    with get_db() as conn:
        cursor = conn.cursor()

        query = """
        SELECT
            c.name as customer,
            l.loan_type,
            l.amount as loan_amount,
            b.name as branch
        FROM loans l
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE LOWER(l.status) = 'pending'
        AND l.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        return [dict(r) for r in rows]

@api_router.get("/admin/report/approved")
async def approved_report(
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        query = """
        SELECT
            c.name as customer,
            l.loan_type,
            l.amount as loan_amount,
            b.name as branch
        FROM loans l
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE l.status = 'active'
        AND l.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        return [dict(r) for r in rows]

@api_router.get("/admin/report/customers")
async def customers_report(
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        query = """
        SELECT
            c.name,
            COALESCE(u.phone, '') AS phone,
            c.cibil_score,
            b.name as branch
        FROM customers c
        LEFT JOIN users u ON c.user_id = u.id
        LEFT JOIN branches b ON u.branch_id = b.id
        WHERE u.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND u.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND u.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND u.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        return [dict(r) for r in rows]

@api_router.get("/admin/report/avg-loan")
async def avg_loan_report(
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        query = """
        SELECT
            c.name as customer,
            l.loan_type,
            l.amount as loan_amount,
            b.name as branch
        FROM loans l
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE l.tenant_id = %s
        AND l.status = 'active'
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        return [dict(r) for r in rows]

@api_router.get("/admin/report/customers/download")
async def download_customers_report(
    format: str = "csv",
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (token_data["tenant_id"],))
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"

        if token_data.get("role") == "super_admin":
            effective_branch_id = branch_id or (branch_ids.split(",")[0] if branch_ids else None)
            if effective_branch_id:
                cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                    (effective_branch_id, token_data["tenant_id"]))
                branch_row = cursor.fetchone()
                branch_name = branch_row["name"] if branch_row else "Branch"
            else:
                branch_name = "All Branches"
        elif token_data.get("branch_id"):
            cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                (token_data["branch_id"], token_data["tenant_id"]))
            branch_row = cursor.fetchone()
            branch_name = branch_row["name"] if branch_row else "Branch"
        else:
            branch_name = "Branch"

        query = """
        SELECT
            c.name,
            COALESCE(u.phone, '') AS phone,
            c.cibil_score,
            b.name as branch
        FROM customers c
        LEFT JOIN users u ON c.user_id = u.id
        LEFT JOIN branches b ON u.branch_id = b.id
        WHERE u.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND u.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND u.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND u.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)
        rows = [dict(r) for r in cursor.fetchall()]

        for row in rows:
            if 'phone' in row and row['phone'] is not None:
                row['phone'] = str(row['phone'])

    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write CSV summary header
        totals = {
            "Total Customers": len(rows)
        }
        write_csv_summary(writer, "Customers Report", branch_name, totals)
        
        # Write table headers
        writer.writerow(["Name", "Phone", "CIBIL", "Branch"])

        for r in rows:
            # Force Excel to treat phone as text using ="value" format
            phone_value = f'="{r["phone"]}"' if r["phone"] else ""
            writer.writerow([
                r["name"],
                phone_value,
                r["cibil_score"],
                r["branch"]
            ])

        output.seek(0)
        return _make_csv_response(output.getvalue(), "customers_report.csv")

    if format == "pdf":
        # Calculate totals
        total_customers = len(rows)

        # Prepare table data
        table_rows = []
        for r in rows:
            table_rows.append([
                r["name"],
                r["phone"] or "",
                str(r["cibil_score"]) if r["cibil_score"] else "",
                r["branch"]
            ])

        # Generate PDF using helper function
        totals_info = {
            "Total Customers": total_customers
        }

        buffer = generate_report_pdf(
            title="Customers Report",
            headers=["Name", "Phone", "CIBIL", "Branch"],
            rows=table_rows,
            totals_info=totals_info,
            company_name=company_name,
            branch_name=branch_name,
            from_date=None,
            to_date=None,
            col_widths=[2*inch, 1.5*inch, 1.5*inch, 2.5*inch]
        )

        # Generate filename with current date
        current_date = get_ist_now().strftime("%Y-%m-%d")
        filename = f"customers_{current_date}.pdf"

        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

@api_router.get("/admin/report/users")
async def get_users_report(
    role: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin', 'super_admin']))
):
    """
    Get filtered list of users based on role and branch.
    Role parameter can be: admin, customer, collection_agent, finance_officer, auditor
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        query = """
            SELECT 
                u.id,
                u.username,
                u.role,
                u.branch_id,
                b.name AS branch_name,
                u.created_at
            FROM users u
            LEFT JOIN branches b ON u.branch_id = b.id
            WHERE u.tenant_id = %s
        """
        
        params = [token_data["tenant_id"]]
        
        # Role filtering
        if role:
            if role == 'admin':
                # Admin filter includes both admin and super_admin
                query += " AND (u.role = 'admin' OR u.role = 'super_admin')"
            else:
                query += " AND u.role = %s"
                params.append(role)
        
        # Branch filtering
        if token_data["role"] == "admin":
            # Admin users can only see their branch
            query += " AND u.branch_id = %s"
            params.append(token_data["branch_id"])
        elif branch_id:
            # Super admin with branch filter
            query += " AND u.branch_id = %s"
            params.append(branch_id)
        
        query += " ORDER BY u.created_at DESC"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        if not rows:
            raise HTTPException(status_code=404, detail="No users found")
        
        return [dict(row) for row in rows]


@api_router.get("/admin/report/users/download")
async def download_users_report(
    format: str,
    role: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin', 'super_admin']))
):
    """
    Download user report in PDF or CSV format.
    """
    if format not in ["pdf", "csv"]:
        raise HTTPException(status_code=400, detail="Invalid format. Use 'pdf' or 'csv'")
    
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get company name
        cursor.execute("SELECT name FROM tenants WHERE id = %s",
            (token_data["tenant_id"],)
        )
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"
        
        # Determine branch name
        branch_name = get_branch_name_for_report(cursor, token_data, branch_id)
        
        # Build query (same as GET /api/admin/report/users but with phone)
        query = """
            SELECT 
                u.username,
                u.role,
                b.name AS branch_name,
                u.created_at,
                COALESCE(u.phone) AS phone
            FROM users u
            LEFT JOIN branches b ON u.branch_id = b.id
            LEFT JOIN customers c ON u.id = c.user_id
            WHERE u.tenant_id = %s
        """
        
        params = [token_data["tenant_id"]]
        
        # Role filtering
        if role:
            if role == 'admin':
                query += " AND (u.role = 'admin' OR u.role = 'super_admin')"
            else:
                query += " AND u.role = %s"
                params.append(role)
        
        # Branch filtering
        if token_data["role"] == "admin":
            query += " AND u.branch_id = %s"
            params.append(token_data["branch_id"])
        elif branch_id:
            query += " AND u.branch_id = %s"
            params.append(branch_id)
        
        query += " ORDER BY u.created_at DESC"
        
        cursor.execute(query, params)
        rows = [dict(row) for row in cursor.fetchall()]
        
        if not rows:
            raise HTTPException(status_code=404, detail="No users found")
        
        # Convert phone numbers to strings and format dates
        for row in rows:
            if 'phone' in row and row['phone'] is not None:
                row['phone'] = str(row['phone'])
            
            # Format dates to DD/MM/YYYY
            if row.get("created_at"):
                try:
                    row["created_at"] = datetime.fromisoformat(
                        row["created_at"].replace("Z", "+00:00")
                    ).strftime("%d/%m/%Y")
                except:
                    pass
        
        # Determine report title
        role_labels = {
            'admin': 'Admin Users',
            'customer': 'Customer Users',
            'collection_agent': 'Collection Agents',
            'finance_officer': 'Finance Officers',
            'auditor': 'Auditors'
        }
        report_title = role_labels.get(role, 'All Users') + ' Report'
        
        # Generate report based on format
        if format == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write CSV summary header
            totals = {
                "Total Records": len(rows)
            }
            write_csv_summary(writer, report_title, branch_name, totals)
            
            # Write table headers
            writer.writerow(["Username", "Role", "Branch", "Phone", "Created At"])
            
            # Write user data with Excel-safe phone number formatting
            for row in rows:
                phone_value = row.get("phone", "N/A")
                if phone_value and phone_value != "N/A":
                    phone_value = f'="{phone_value}"'  # Excel formula to prevent scientific notation
                
                writer.writerow([
                    row.get("username", "N/A"),
                    row.get("role", "N/A").replace("_", " ").title(),
                    row.get("branch_name", "N/A"),
                    phone_value,
                    row.get("created_at", "N/A")
                ])
            
            output.seek(0)
            
            # Generate filename
            role_label = role.replace("_", "_").lower() if role else "all"
            filename = f"{role_label}_users_report.csv"
            return _make_csv_response(output.getvalue(), filename)
        
        elif format == "pdf":
            # Prepare table data
            headers = ["Username", "Role", "Branch", "Created At"]
            table_rows = []
            
            for row in rows:
                table_rows.append([
                    row.get("username", "N/A"),
                    row.get("role", "N/A").replace("_", " ").title(),
                    row.get("branch_name", "N/A"),
                    row.get("created_at", "N/A")
                ])
            
            # Generate PDF using helper function
            totals_info = {
                "Total Users": len(rows)
            }
            
            buffer = generate_report_pdf(
                title=report_title,
                headers=headers,
                rows=table_rows,
                totals_info=totals_info,
                company_name=company_name,
                branch_name=branch_name,
                col_widths=[2*inch, 2*inch, 2*inch, 1.5*inch]
            )
            
            # Generate filename
            role_label = role.replace("_", "_").lower() if role else "all"
            filename = f"{role_label}_users_report.pdf"
            
            return StreamingResponse(
                buffer,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": f"attachment; filename={filename}"
                }
            )


@api_router.get("/admin/report/pending/download")
async def download_pending_report(
    format: str = "csv",
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (token_data["tenant_id"],))
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"

        if token_data.get("role") == "super_admin":
            effective_branch_id = branch_id or (branch_ids.split(",")[0] if branch_ids else None)
            if effective_branch_id:
                cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                    (effective_branch_id, token_data["tenant_id"]))
                branch_row = cursor.fetchone()
                branch_name = branch_row["name"] if branch_row else "Branch"
            else:
                branch_name = "All Branches"
        elif token_data.get("branch_id"):
            cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                (token_data["branch_id"], token_data["tenant_id"]))
            branch_row = cursor.fetchone()
            branch_name = branch_row["name"] if branch_row else "Branch"
        else:
            branch_name = "Branch"

        query = """
        SELECT
            c.name as customer,
            l.loan_type,
            l.amount as loan_amount,
            b.name as branch
        FROM loans l
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE LOWER(l.status) = 'pending'
        AND l.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)
        rows = [dict(r) for r in cursor.fetchall()]

    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write CSV summary header
        total_loan_amount = sum(r["loan_amount"] for r in rows)
        totals = {
            "Total Records": len(rows),
            "Total Loan Amount": total_loan_amount
        }
        write_csv_summary(writer, "Pending Loans Report", branch_name, totals)
        
        # Write table headers
        writer.writerow(["Customer", "Loan Type", "Loan Amount", "Branch"])

        for r in rows:
            writer.writerow([
                r["customer"],
                r["loan_type"],
                fmt_currency(r["loan_amount"]),
                r["branch"]
            ])

        output.seek(0)
        return _make_csv_response(output.getvalue(), "pending_report.csv")

    if format == "pdf":
        # Calculate totals
        total_records = len(rows)
        total_loan_amount = sum(r["loan_amount"] for r in rows)

        # Prepare table data
        table_rows = []
        for r in rows:
            table_rows.append([
                r["customer"],
                r["loan_type"],
                f"₹{r['loan_amount']:,.2f}",
                r["branch"]
            ])

        # Generate PDF using helper function
        totals_info = {
            "Total Records": total_records,
            "Total Loan Amount": f"₹{total_loan_amount:,.2f}"
        }

        buffer = generate_report_pdf(
            title="Pending Loans Report",
            headers=["Customer", "Loan Type", "Loan Amount", "Branch"],
            rows=table_rows,
            totals_info=totals_info,
            company_name=company_name,
            branch_name=branch_name,
            from_date=None,
            to_date=None,
            col_widths=[2*inch, 1.5*inch, 1.5*inch, 2*inch]
        )

        # Generate filename with current date
        current_date = get_ist_now().strftime("%Y-%m-%d")
        filename = f"pending_loans_{current_date}.pdf"

        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

@api_router.get("/admin/report/approved/download")
async def download_approved_report(
    format: str = "csv",
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (token_data["tenant_id"],))
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"

        if token_data.get("role") == "super_admin":
            effective_branch_id = branch_id or (branch_ids.split(",")[0] if branch_ids else None)
            if effective_branch_id:
                cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                    (effective_branch_id, token_data["tenant_id"]))
                branch_row = cursor.fetchone()
                branch_name = branch_row["name"] if branch_row else "Branch"
            else:
                branch_name = "All Branches"
        elif token_data.get("branch_id"):
            cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                (token_data["branch_id"], token_data["tenant_id"]))
            branch_row = cursor.fetchone()
            branch_name = branch_row["name"] if branch_row else "Branch"
        else:
            branch_name = "Branch"

        query = """
        SELECT
            c.name as customer,
            l.loan_type,
            l.amount as loan_amount,
            b.name as branch
        FROM loans l
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE l.status = 'active'
        AND l.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)
        rows = [dict(r) for r in cursor.fetchall()]

    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write CSV summary header
        total_loan_amount = sum(r["loan_amount"] for r in rows)
        totals = {
            "Total Records": len(rows),
            "Total Loan Amount": total_loan_amount
        }
        write_csv_summary(writer, "Approved Loans Report", branch_name, totals)
        
        # Write table headers
        writer.writerow(["Customer", "Loan Type", "Loan Amount", "Branch"])

        for r in rows:
            writer.writerow([
                r["customer"],
                r["loan_type"],
                fmt_currency(r["loan_amount"]),
                r["branch"]
            ])

        output.seek(0)
        return _make_csv_response(output.getvalue(), "approved_report.csv")

    if format == "pdf":
        # Calculate totals
        total_records = len(rows)
        total_loan_amount = sum(r["loan_amount"] for r in rows)

        # Prepare table data
        table_rows = []
        for r in rows:
            table_rows.append([
                r["customer"],
                r["loan_type"],
                f"₹{r['loan_amount']:,.2f}",
                r["branch"]
            ])

        # Generate PDF using helper function
        totals_info = {
            "Total Records": total_records,
            "Total Loan Amount": f"₹{total_loan_amount:,.2f}"
        }

        buffer = generate_report_pdf(
            title="Approved Loans Report",
            headers=["Customer", "Loan Type", "Loan Amount", "Branch"],
            rows=table_rows,
            totals_info=totals_info,
            company_name=company_name,
            branch_name=branch_name,
            from_date=None,
            to_date=None,
            col_widths=[2*inch, 1.5*inch, 1.5*inch, 2*inch]
        )

        # Generate filename with current date
        current_date = get_ist_now().strftime("%Y-%m-%d")
        filename = f"approved_loans_{current_date}.pdf"

        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

@api_router.get("/admin/report/avg-loan/download")
async def download_avg_loan_report(
    format: str = "csv",
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (token_data["tenant_id"],))
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"

        if token_data.get("role") == "super_admin":
            effective_branch_id = branch_id or (branch_ids.split(",")[0] if branch_ids else None)
            if effective_branch_id:
                cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                    (effective_branch_id, token_data["tenant_id"]))
                branch_row = cursor.fetchone()
                branch_name = branch_row["name"] if branch_row else "Branch"
            else:
                branch_name = "All Branches"
        elif token_data.get("branch_id"):
            cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s",
                (token_data["branch_id"], token_data["tenant_id"]))
            branch_row = cursor.fetchone()
            branch_name = branch_row["name"] if branch_row else "Branch"
        else:
            branch_name = "Branch"

        query = """
        SELECT
            c.name as customer,
            l.loan_type,
            l.amount as loan_amount,
            b.name as branch
        FROM loans l
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE l.tenant_id = %s
        AND l.status = 'active'
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)
        rows = [dict(r) for r in cursor.fetchall()]

    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write CSV summary header
        total_loan_amount = sum(r["loan_amount"] for r in rows)
        avg_loan_amount = total_loan_amount / len(rows) if len(rows) > 0 else 0
        totals = {
            "Total Records": len(rows),
            "Total Loan Amount": total_loan_amount,
            "Average Loan Amount": avg_loan_amount
        }
        write_csv_summary(writer, "Average Loan Report", branch_name, totals)
        
        # Write table headers
        writer.writerow(["Customer", "Loan Type", "Loan Amount", "Branch"])

        for r in rows:
            writer.writerow([
                r["customer"],
                r["loan_type"],
                fmt_currency(r["loan_amount"]),
                r["branch"]
            ])

        output.seek(0)
        return _make_csv_response(output.getvalue(), "avg_loan_report.csv")

    if format == "pdf":
        # Calculate totals
        total_records = len(rows)
        total_loan_amount = sum(r["loan_amount"] for r in rows)
        avg_loan_amount = total_loan_amount / total_records if total_records > 0 else 0

        # Prepare table data
        table_rows = []
        for r in rows:
            table_rows.append([
                r["customer"],
                r["loan_type"],
                f"₹{r['loan_amount']:,.2f}",
                r["branch"]
            ])

        # Generate PDF using helper function
        totals_info = {
            "Total Records": total_records,
            "Total Loan Amount": f"₹{total_loan_amount:,.2f}",
            "Average Loan Amount": f"₹{avg_loan_amount:,.2f}"
        }

        buffer = generate_report_pdf(
            title="Average Loan Report",
            headers=["Customer", "Loan Type", "Loan Amount", "Branch"],
            rows=table_rows,
            totals_info=totals_info,
            company_name=company_name,
            branch_name=branch_name,
            from_date=None,
            to_date=None,
            col_widths=[2*inch, 1.5*inch, 1.5*inch, 2*inch]
        )

        # Generate filename with current date
        current_date = get_ist_now().strftime("%Y-%m-%d")
        filename = f"avg_loan_{current_date}.pdf"

        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )


@api_router.post("/admin/resync-sequences")
async def resync_sequences(token_data: dict = Depends(require_role(['super_admin']))):
    """Re-index all loans per tenant in created_at order, filling gaps. Active first, then pending."""
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        prefix_map = {'gold_loan': 'GL', 'personal_loan': 'PL', 'vehicle_loan': 'VL'}
        updated = {}
        for lt, prefix in prefix_map.items():
            # Renumber: active/approved/disbursed/closed first, then pending/pre-approved — all in created_at order
            cursor.execute("""
                WITH ranked AS (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               ORDER BY
                                 CASE WHEN status IN ('active','approved','disbursed','closed') THEN 0 ELSE 1 END,
                                 created_at ASC
                           ) AS rn
                    FROM loans
                    WHERE loan_type = %s AND tenant_id = %s
                )
                UPDATE loans l
                SET loan_number = %s || '-' || ranked.rn
                FROM ranked
                WHERE l.id = ranked.id
                RETURNING l.loan_number
            """, (lt, tenant_id, prefix))
            rows = cursor.fetchall()
            # Sync sequence to max so next gap-fill starts correctly
            cursor.execute("""
                INSERT INTO loan_sequences (tenant_id, loan_type, current_value)
                VALUES (%s, %s, %s)
                ON CONFLICT (tenant_id, loan_type)
                DO UPDATE SET current_value = EXCLUDED.current_value
            """, (tenant_id, lt, len(rows)))
            updated[lt] = len(rows)
        logger.info("resync-sequences: tenant=%s updated=%s", tenant_id, updated)
        return {"status": "ok", "updated": updated}


@api_router.post("/admin/create-branch")
async def create_branch(request: BranchCreateRequest, token_data: dict = Depends(require_role(['super_admin']))):
    if not check_permission(token_data['role'], 'branches', 'insert'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_id = str(uuid.uuid4())
        cursor.execute("INSERT INTO branches (id, name, location, tenant_id, created_at) VALUES (%s, %s, %s, %s, %s)",
            (branch_id, request.name, request.location, token_data["tenant_id"], get_ist_now().isoformat()))
                
        log_audit(conn, token_data['user_id'], token_data['tenant_id'],'BRANCH_CREATED', 'branch', branch_id)
        logger.info("Branch created: id=%s name=%s tenant=%s", branch_id, request.name, token_data["tenant_id"])
        return {"message": "Branch created successfully", "branch_id": branch_id}

@api_router.get("/admin/branches")
async def get_branches(token_data: dict = Depends(require_role(['admin','super_admin', 'auditor']))):
    if not check_permission(token_data['role'], 'branches', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        if token_data["role"] == "admin":
            cursor.execute("SELECT * FROM branches WHERE id = %s",
                (token_data["branch_id"],)
            )
        else:
            cursor.execute("SELECT * FROM branches WHERE tenant_id = %s ORDER BY created_at DESC",
                (token_data["tenant_id"],)
            )
        branches = [dict(row) for row in cursor.fetchall()]
        return branches

@api_router.post("/admin/update-interest-rate")
async def update_interest_rate(request: InterestRateUpdateRequest, token_data: dict = Depends(require_role(['admin','super_admin']))):
    if not check_permission(token_data['role'], 'reports', 'update'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        tenant_id = token_data["tenant_id"]
        
        cursor.execute("SELECT id FROM interest_rates WHERE tenant_id = %s AND loan_type = %s AND category = %s",
            (tenant_id, request.loan_type, request.category)
        )
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute("UPDATE interest_rates SET rate = %s, is_overridden = 1, source = 'manual' WHERE id = %s",
                (request.rate, existing['id'])
            )
        else:
            cursor.execute("INSERT INTO interest_rates (id, tenant_id, loan_type, category, rate, is_overridden, source, created_at) VALUES (%s, %s, %s, %s, %s, 1, 'manual', %s)",
                (str(uuid.uuid4()), tenant_id, request.loan_type, request.category, request.rate, get_ist_now().isoformat())
            )
        
        conn.commit()
        return {"message": "Interest rate updated successfully"}

@api_router.post("/admin/reset-interest-override")
async def reset_interest_override(request: InterestRateResetRequest, token_data: dict = Depends(require_role(['super_admin']))):
    """Reset an admin-overridden interest rate back to dynamic calculation."""
    if not check_permission(token_data['role'], 'reports', 'update'):
        raise HTTPException(403, "Access denied")

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM interest_rates WHERE tenant_id = %s AND loan_type = %s AND category = %s",
            (token_data["tenant_id"], request.loan_type, request.category)
        )
        existing = cursor.fetchone()

        if not existing:
            raise HTTPException(404, f"Interest rate not found for {request.loan_type}/{request.category}")

        # Get current repo rate for this tenant
        repo_data = get_latest_repo_rate(str(DB_PATH), token_data["tenant_id"])
        repo_rate = repo_data["repo_rate"] if repo_data else DEFAULT_REPO_RATE

        # Build inputs for this category
        category_inputs = {
            "cibil_750_plus": {"cibil_score": 750},
            "cibil_700_749": {"cibil_score": 725},
            "cibil_650_699": {"cibil_score": 675},
            "age_0_3": {"vehicle_age": 1},
            "age_4_6": {"vehicle_age": 5},
            "age_7_plus": {"vehicle_age": 8},
            "standard": {"loan_amount": 50000},
        }

        inputs = category_inputs.get(request.category)
        if not inputs:
            raise HTTPException(400, f"Unknown category: {request.category}")

        result = calculate_interest_rate(request.loan_type, inputs, repo_rate)
        new_rate = result["rate"]

        cursor.execute("UPDATE interest_rates SET rate = %s, is_overridden = 0, source = 'auto' WHERE id = %s",
            (new_rate, existing['id'])
        )

        conn.commit()
        logger.info("Interest rate override reset: %s/%s → %s%% (is_overridden=0)", request.loan_type, request.category, new_rate)
        return {
            "message": "Override reset successfully",
            "loan_type": request.loan_type,
            "category": request.category,
            "new_rate": new_rate,
            "repo_rate": repo_rate
        }

@api_router.get("/admin/interest-rates")
async def get_interest_rates(token_data: dict = Depends(require_role(['admin', 'super_admin', 'finance_officer', 'auditor']))):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")

    with get_db() as conn:
        cursor = conn.cursor()
        tenant_id = token_data["tenant_id"]
        cursor.execute("SELECT * FROM interest_rates WHERE tenant_id = %s ORDER BY loan_type, category", (tenant_id,))
        return [dict(row) for row in cursor.fetchall()]

@api_router.post("/admin/update-gold-rate")
async def update_gold_rate(
    request: GoldRateUpdateRequest,
    token_data: dict = Depends(require_role(['admin', 'super_admin']))
):

    if not check_permission(token_data['role'], 'reports', 'update'):
        raise HTTPException(403, "Access denied")

    with get_db() as conn:
        cursor = conn.cursor()

        tenant_id = token_data["tenant_id"]
        now_ts = get_ist_now().isoformat()

        # 🏢 Branch admin restriction
        if token_data["role"] == "admin":
            branch_id = token_data["branch_id"]

        else:
            # Super admin: validate branch_id
            if request.branch_id == "ALL":
                branch_id = "ALL"
            elif request.branch_id and request.branch_id.strip():
                branch_id = request.branch_id.strip()
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Branch must be selected when applying rate to a specific branch"
                )

        # 🏆 MASTER RATE LOGIC: 'All Branches' — single global record only
        if branch_id == "ALL":
            if token_data["role"] != "super_admin":
                raise HTTPException(403, "Only super admin can update all branches")

            # Insert ONE global record (branch_id = 'ALL') — no per-branch duplicates
            cursor.execute("""
            INSERT INTO gold_rate (id, tenant_id, branch_id, rate_per_gram, source, updated_at, changed_by)
            VALUES (%s, %s, 'ALL', %s, 'manual', %s, %s)
            """, (str(uuid.uuid4()), tenant_id, request.rate_per_gram, now_ts, token_data["user_id"]))

            branches_updated = 1
        else:
            # 🔹 Branch-specific update
            cursor.execute("""
            INSERT INTO gold_rate (id, tenant_id, branch_id, rate_per_gram, source, updated_at, changed_by)
            VALUES (%s, %s, %s, %s, 'manual', %s, %s)
            """, (str(uuid.uuid4()), tenant_id, branch_id, request.rate_per_gram, now_ts, token_data["user_id"]))
            branches_updated = 1

        # 🔒 CRITICAL: Automatically set mode to 'manual' when user manually updates rate
        # This prevents the scheduler from overwriting the custom rate
        cursor.execute("""
            SELECT id FROM gold_rate_settings WHERE tenant_id = %s
        """, (tenant_id,))
        
        existing_settings = cursor.fetchone()
        
        if existing_settings:
            # Update existing settings to 'manual'
            cursor.execute("""
                UPDATE gold_rate_settings
                SET mode = 'manual'
                WHERE tenant_id = %s
            """, (tenant_id,))
        else:
            # Create new settings record with 'manual' mode
            cursor.execute("""
                INSERT INTO gold_rate_settings (id, tenant_id, mode)
                VALUES (%s, %s, 'manual')
            """, (str(uuid.uuid4()), tenant_id))

        conn.commit()

        log_audit(conn, token_data['user_id'], tenant_id, 'GOLD_RATE_MANUAL_UPDATE', 'gold_rate',
                  details=json.dumps({"branch_id": branch_id, "rate_per_gram": request.rate_per_gram, "source": "manual"}))

        _cache_invalidate_prefix(f"gold_rate:{tenant_id}")

        return {
            "message": f"Gold rate updated successfully for {branches_updated} branch(es) (mode set to manual)",
            "branch_id": branch_id,
            "rate_per_gram": request.rate_per_gram,
            "mode": "manual",
            "branches_updated": branches_updated
        }
# --- Gold Rate Helpers ---

def resolve_gold_rate_mode(cursor, tenant_id):
    """Fetch the tenant's configured mode from gold_rate_settings. Defaults to 'manual'."""
    cursor.execute("SELECT mode FROM gold_rate_settings WHERE tenant_id = %s", (tenant_id,))
    row = cursor.fetchone()
    return row["mode"] if row else "manual"


def get_global_gold_rate(cursor, tenant_id, source):
    """Fetch the latest global gold rate (branch_id IS NULL or 'ALL')."""
    cursor.execute("""
        SELECT rate_per_gram, updated_at FROM gold_rate
        WHERE tenant_id = %s AND source = %s AND (branch_id IS NULL OR branch_id = 'ALL')
        ORDER BY updated_at DESC LIMIT 1
    """, (tenant_id, source))
    return cursor.fetchone()


def get_branch_gold_rate(cursor, tenant_id, branch_id, source):
    """Fetch the latest gold rate for a specific branch."""
    cursor.execute("""
        SELECT rate_per_gram, updated_at FROM gold_rate
        WHERE tenant_id = %s AND branch_id = %s AND source = %s
        ORDER BY updated_at DESC LIMIT 1
    """, (tenant_id, branch_id, source))
    return cursor.fetchone()


def fetch_default_market_rate(tenant_id: str = None):
    """Fetch market gold rate; falls back to latest stored auto rate, then 6500.0."""
    try:
        from services.gold_rate_service import fetch_gold_market_rate
        rate = fetch_gold_market_rate(tenant_id=tenant_id)
        return rate if rate else 6500.0
    except Exception:
        return 6500.0


def insert_gold_rate(cursor, tenant_id, branch_id, rate, source):
    """Insert a new gold rate row. Skips insertion if an auto rate already exists for today."""
    if source == "auto":
        cursor.execute("""
            SELECT id FROM gold_rate
            WHERE tenant_id = %s AND source = 'auto' AND DATE(updated_at) = CURRENT_DATE
        """, (tenant_id,))
        if cursor.fetchone():
            return
    now_ts = get_ist_now().isoformat()
    cursor.execute("""
        INSERT INTO gold_rate (id, tenant_id, branch_id, rate_per_gram, source, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (str(uuid.uuid4()), tenant_id, branch_id, rate, source, now_ts))
    return now_ts


def resolve_gold_rate(cursor, tenant_id):
    """
    Single source of truth for gold rate resolution.
    Rate depends ONLY on tenant_id + mode. Branch is irrelevant.
    1. Reads mode from gold_rate_settings (defaults to 'manual').
    2. Fetches latest rate matching that mode for the tenant.
    Returns dict: { mode, rate_per_gram, updated_at }
    """
    cursor.execute("SELECT mode FROM gold_rate_settings WHERE tenant_id = %s", (tenant_id,))
    mode_row = cursor.fetchone()
    mode = mode_row["mode"] if mode_row else "manual"

    cursor.execute("""
        SELECT rate_per_gram, updated_at FROM gold_rate
        WHERE tenant_id = %s AND source = %s
        ORDER BY updated_at DESC LIMIT 1
    """, (tenant_id, mode))
    row = cursor.fetchone()

    if not row:
        # Fallback: any rate for tenant regardless of source
        cursor.execute("""
            SELECT rate_per_gram, updated_at FROM gold_rate
            WHERE tenant_id = %s
            ORDER BY updated_at DESC LIMIT 1
        """, (tenant_id,))
        row = cursor.fetchone()

    return {
        "mode": mode,
        "rate_per_gram": float(row["rate_per_gram"]) if row else 0.0,
        "updated_at": row["updated_at"] if row else None,
    }


@api_router.get("/admin/gold-rate")
async def get_gold_rate(
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin', 'super_admin']))
):
    tenant_id = token_data["tenant_id"]

    cache_key = f"gold_rate:{tenant_id}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    with get_db() as conn:
        cursor = conn.cursor()
        result = resolve_gold_rate(cursor, tenant_id)

        if result["rate_per_gram"] == 0.0 and result["updated_at"] is None:
            mode = result["mode"]
            default_rate = fetch_default_market_rate(tenant_id) if mode == "auto" else 6500.0
            ts = insert_gold_rate(cursor, tenant_id, None, default_rate, mode)
            conn.commit()
            result = {"rate_per_gram": default_rate, "updated_at": ts, "mode": mode}

    _cache_set(cache_key, result, ttl=60)
    return result
        
@api_router.post("/admin/gold-rate-mode")
async def set_gold_rate_mode(
    request: GoldRateModeRequest,
    token_data: dict = Depends(require_role(['super_admin']))
):
    mode = request.mode

    if mode not in ["auto", "manual"]:
        raise HTTPException(400, "Invalid mode")

    with get_db() as conn:
        cursor = conn.cursor()

        tenant_id = token_data["tenant_id"]

        # Upsert: update if exists, insert if not
        cursor.execute("SELECT id FROM gold_rate_settings WHERE tenant_id = %s", (tenant_id,))
        existing = cursor.fetchone()

        if existing:
            cursor.execute("""
                UPDATE gold_rate_settings
                SET mode = %s
                WHERE tenant_id = %s
            """, (mode, tenant_id))
        else:
            cursor.execute("""
                INSERT INTO gold_rate_settings (id, tenant_id, mode)
                VALUES (%s, %s, %s)
            """, (str(uuid.uuid4()), tenant_id, mode))

        conn.commit()

        # Invalidate gold rate cache so next fetch reflects the new mode
        _cache_invalidate_prefix(f"gold_rate:{tenant_id}")

        # 🆕 When switching to auto, immediately fetch and insert the latest market rate
        if mode == "auto":
            try:
                from services.gold_rate_service import fetch_gold_market_rate
                market_rate = fetch_gold_market_rate(tenant_id=tenant_id)
                default_rate = market_rate if market_rate else 6500.0

                # Check if today's auto rate already exists for this tenant
                cursor.execute("""
                    SELECT rate_per_gram FROM gold_rate
                    WHERE tenant_id = %s AND source = 'auto'
                    AND DATE(updated_at) = CURRENT_DATE
                    ORDER BY updated_at DESC LIMIT 1
                """, (tenant_id,))
                today_row = cursor.fetchone()

                # Only insert if no today's rate or rate changed significantly
                if not today_row or abs(round(today_row["rate_per_gram"], 2) - round(default_rate, 2)) >= 0.10:
                    now_ts = get_ist_now().isoformat()
                    cursor.execute("""
                        INSERT INTO gold_rate (id, tenant_id, branch_id, rate_per_gram, source, updated_at)
                        VALUES (%s, %s, NULL, %s, 'auto', %s)
                    """, (str(uuid.uuid4()), tenant_id, default_rate, now_ts))
                    conn.commit()
            except Exception:
                pass

    return {"message": f"Gold rate mode set to {mode}"}

@api_router.get("/admin/gold-rate-mode")
async def get_gold_rate_mode(
    token_data: dict = Depends(require_role(['admin','super_admin']))
):
    with get_db() as conn:
        cursor = conn.cursor()

        tenant_id = token_data["tenant_id"]
        cursor.execute("SELECT mode FROM gold_rate_settings WHERE tenant_id = %s", (tenant_id,))
        row = cursor.fetchone()

        # If no settings exist for this tenant, create default and return
        if not row:
            cursor.execute("INSERT INTO gold_rate_settings (id, tenant_id, mode) VALUES (%s, %s, 'manual')",
                (str(uuid.uuid4()), tenant_id)
            )
            conn.commit()
            return {"mode": "manual"}

    return {"mode": row["mode"]}

@api_router.get("/admin/export-gold-rate-pdf")
async def export_gold_rate_pdf(
    from_date: str,
    to_date: str,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):
    # 🔄 Convert input dates to DB format
    from_date_db = datetime.strptime(from_date, "%d-%m-%Y").strftime("%Y-%m-%d")
    to_date_db   = datetime.strptime(to_date, "%d-%m-%Y").strftime("%Y-%m-%d")

    # 🔥 Display format dd/MM/yyyy
    display_from = datetime.strptime(from_date_db, "%Y-%m-%d").strftime("%d/%m/%Y")
    display_to   = datetime.strptime(to_date_db, "%Y-%m-%d").strftime("%d/%m/%Y")

    # 🏢 FETCH COMPANY + BRANCH
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM tenants WHERE id = %s",
            (token_data["tenant_id"],)
        )
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"

        branch_id = token_data.get("branch_id")

        if token_data.get("role") == "super_admin":
            branch_name = "All Branches"
        elif branch_id:
            cursor.execute("SELECT name FROM branches WHERE id = %s AND tenant_id = %s", (branch_id, token_data["tenant_id"]))
            branch_row = cursor.fetchone()
            branch_name = branch_row[0] if branch_row else "Branch"
        else:
            branch_name = "Branch"

        # ⭐ FETCH DATA
        if token_data["role"] == "super_admin":
            cursor.execute("""
                SELECT g.rate_per_gram, g.source, g.updated_at, g.branch_id, b.name AS branch_name
                FROM gold_rate g
                LEFT JOIN branches b ON g.branch_id = b.id AND b.tenant_id = g.tenant_id
                WHERE g.tenant_id = %s
                AND DATE(g.updated_at) BETWEEN DATE(%s) AND DATE(%s)
                ORDER BY g.updated_at ASC
                """, (
                    token_data["tenant_id"],
                    from_date_db,
                    to_date_db
                ))
        else:
            cursor.execute("""
                SELECT g.rate_per_gram, g.source, g.updated_at, g.branch_id, b.name AS branch_name
                FROM gold_rate g
                LEFT JOIN branches b ON g.branch_id = b.id AND b.tenant_id = g.tenant_id
                WHERE g.tenant_id = %s
                AND (g.branch_id = %s OR g.branch_id IS NULL OR g.branch_id = 'ALL')
                AND DATE(g.updated_at) BETWEEN DATE(%s) AND DATE(%s)
                ORDER BY g.updated_at ASC
                """, (
                    token_data["tenant_id"],
                    token_data["branch_id"],
                    from_date_db,
                    to_date_db
                ))

        rows = cursor.fetchall()

    # ✅ 404 CHECK — No data for selected date range
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No gold rate records found for the selected date range."
        )

    # 📊 TOTAL RECORDS
    total_records = len(rows)

    # CREATE PDF — using shared pdf_styles utilities
    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []

    period = f"{display_from} – {display_to}"
    elements.extend(build_header(company_name, branch_name, period))
    elements.append(Paragraph("Gold Rate Report", STYLE_TITLE))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(f"<b>Total Records:</b>  {total_records}", STYLE_INFO))
    elements.append(Spacer(1, 14))

    # TABLE DATA
    data = [[_pb("Date"), _pb("Branch"), _pb("Gold Rate (\u20b9)"), _pb("Mode")]]
    for row in rows:
        row = dict(row)
        ts = row["updated_at"]
        if ts is None:
            date_only = "-"
        elif hasattr(ts, "strftime"):
            date_only = ts.strftime("%d/%m/%Y")
        else:
            try:
                date_only = fmt_date(ts)
            except Exception:
                date_only = str(ts)[:10]

        b_id = row.get("branch_id")
        b_name = row.get("branch_name")
        if b_id == "ALL" or b_id is None:
            branch_label = "All Branches"
        elif b_name:
            branch_label = b_name
        else:
            branch_label = "Market Rate"

        rate = row["rate_per_gram"]
        source = row["source"]
        data.append([
            _p(date_only),
            _p(branch_label),
            _p(fmt_currency(rate)),
            _p(source.upper() if source else "-"),
        ])

    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.18, usable_w * 0.30, usable_w * 0.28, usable_w * 0.24]
    tbl = Table(data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style())
    elements.append(tbl)

    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)

    buffer.seek(0)

    filename = f"gold_rate_report_{from_date_db}_to_{to_date_db}.pdf"

    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )

@api_router.get("/admin/export-gold-rate-csv")
async def export_gold_rate_csv(
    from_date: str,
    to_date: str,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    from_date_db = datetime.strptime(from_date,"%d-%m-%Y").strftime("%Y-%m-%d")
    to_date_db = datetime.strptime(to_date,"%d-%m-%Y").strftime("%Y-%m-%d")

    with get_db() as conn:
        cursor = conn.cursor()

        if token_data["role"] == "super_admin":
           cursor.execute("""
                SELECT g.rate_per_gram, g.source, g.updated_at, g.branch_id, b.name AS branch_name
                FROM gold_rate g
                LEFT JOIN branches b ON g.branch_id = b.id AND b.tenant_id = g.tenant_id
                WHERE g.tenant_id = %s
                AND DATE(g.updated_at) BETWEEN DATE(%s) AND DATE(%s)
                ORDER BY g.updated_at ASC
                """, (
                    token_data["tenant_id"],
                    from_date_db,
                    to_date_db
                ))

        else:
            cursor.execute("""
                SELECT g.rate_per_gram, g.source, g.updated_at, g.branch_id, b.name AS branch_name
                FROM gold_rate g
                LEFT JOIN branches b ON g.branch_id = b.id AND b.tenant_id = g.tenant_id
                WHERE g.tenant_id = %s
                AND (g.branch_id = %s OR g.branch_id IS NULL OR g.branch_id = 'ALL')
                AND DATE(g.updated_at) BETWEEN DATE(%s) AND DATE(%s)
                ORDER BY g.updated_at ASC
                """, (
                    token_data["tenant_id"],
                    token_data["branch_id"],
                    from_date_db,
                    to_date_db
                ))
        rows = cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date","Branch Name","Gold Rate","Mode"])
    for rate, source, updated_at, branch_id, branch_name in rows:
        date = datetime.fromisoformat(updated_at).strftime("%d/%m/%Y")

        if branch_id == "ALL":
            branch = "All Branches"
        elif branch_name:
            branch = branch_name
        else:
            branch = "Market Rate"

        writer.writerow([
            f'="{date}"',
            branch,
            rate,
            source
        ])

    output.seek(0)
    return _make_csv_response(output.getvalue(), "gold_rate_report.csv")
@api_router.get("/admin/report/disbursed")
async def disbursed_report(
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        query = """
            SELECT
                c.name as customer,
                l.loan_type,
                l.amount as loan_amount,
                b.name as branch
            FROM loans l
            LEFT JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE l.status!='pending'
            AND l.tenant_id = %s
            """

        params=[token_data["tenant_id"]]

        if token_data["role"]=="admin":
            query+=" AND l.branch_id=%s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids=branch_ids.split(",")
            placeholders=",".join(["%s"]*len(ids))
            query+=f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query,params)

        return [dict(r) for r in cursor.fetchall()]
@api_router.get("/admin/report/interest")
async def interest_report(
    branch_ids: Optional[str] = None,
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):

    with get_db() as conn:
        cursor = conn.cursor()

        query = """
        SELECT
            c.name as customer,
            SUM(e.interest_amount) as interest_earned,
            b.name as branch
        FROM emi_schedule e
        JOIN loans l ON e.loan_id = l.id
        LEFT JOIN customers c ON l.customer_id = c.id
        LEFT JOIN branches b ON l.branch_id = b.id
        WHERE e.status='paid'
        AND l.tenant_id = %s
        """

        params = [token_data["tenant_id"]]

        if token_data["role"] == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        elif branch_id:
            query += " AND l.branch_id = %s"
            params.append(branch_id)

        elif branch_ids:
            ids = branch_ids.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        query += " GROUP BY c.name, b.name"

        cursor.execute(query, params)

        return [dict(r) for r in cursor.fetchall()]
@api_router.get("/admin/stats")
async def get_admin_stats(branch_id: Optional[str] = None, token_data: dict = Depends(require_role(['admin','super_admin']))):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")

    tenant_id = token_data["tenant_id"]
    role = token_data["role"]

    # Build branch filter
    branch_filter_loans = ""
    branch_filter_users = ""
    branch_filter_payments = ""
    params_loans = [tenant_id]
    params_users = [tenant_id]
    params_payments = [tenant_id]

    if role == "admin":
        bid = token_data["branch_id"]
        branch_filter_loans = " AND branch_id = %s"
        branch_filter_users = " AND branch_id = %s"
        branch_filter_payments = " AND branch_id = %s"
        params_loans.append(bid)
        params_users.append(bid)
        params_payments.append(bid)
    elif role == "super_admin" and branch_id:
        ids = branch_id.split(",")
        placeholders = ",".join(["%s"] * len(ids))
        branch_filter_loans = f" AND branch_id IN ({placeholders})"
        branch_filter_users = f" AND branch_id IN ({placeholders})"
        branch_filter_payments = f" AND branch_id IN ({placeholders})"
        params_loans.extend(ids)
        params_users.extend(ids)
        params_payments.extend(ids)

    cache_key = f"admin_stats:{tenant_id}:{role}:{branch_id}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    with get_db() as conn:
        cursor = conn.cursor()

        # Single query for all loan aggregates
        cursor.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending')                          AS pending_loans,
                COUNT(*) FILTER (WHERE status = 'active')                           AS active_loans,
                COUNT(*) FILTER (WHERE status != 'pending')                         AS total_loans,
                COALESCE(SUM(amount) FILTER (WHERE status != 'pending'), 0)         AS total_disbursed,
                COALESCE(SUM(outstanding_balance) FILTER (WHERE status = 'active'), 0) AS total_outstanding,
                COALESCE(SUM(CASE WHEN loan_type='gold_loan'     AND status='active' THEN 1 ELSE 0 END), 0) AS gold_loans,
                COALESCE(SUM(CASE WHEN loan_type='personal_loan' AND status='active' THEN 1 ELSE 0 END), 0) AS personal_loans,
                COALESCE(SUM(CASE WHEN loan_type='vehicle_loan'  AND status='active' THEN 1 ELSE 0 END), 0) AS vehicle_loans
            FROM loans
            WHERE tenant_id = %s {branch_filter_loans}
        """, params_loans)
        loan_row = cursor.fetchone()

        # Single query for payments aggregate
        cursor.execute(f"""
            SELECT COALESCE(SUM(amount), 0) AS total_collected
            FROM payments
            WHERE status = 'approved' AND tenant_id = %s {branch_filter_payments}
        """, params_payments)
        pay_row = cursor.fetchone()

        # Users and customers counts
        cursor.execute(f"SELECT COUNT(*) AS total FROM users WHERE tenant_id = %s {branch_filter_users}", params_users)
        total_users = cursor.fetchone()['total']

        cursor.execute(f"SELECT COUNT(*) AS total FROM customers WHERE tenant_id = %s {branch_filter_loans}", params_loans)
        total_customers = cursor.fetchone()['total']

        result = {
            "total_users": total_users,
            "total_customers": total_customers,
            "pending_loans": loan_row["pending_loans"] or 0,
            "approved_loans": loan_row["active_loans"] or 0,
            "total_loans": loan_row["total_loans"] or 0,
            "total_disbursed": float(loan_row["total_disbursed"] or 0),
            "total_outstanding": float(loan_row["total_outstanding"] or 0),
            "total_collected": float(pay_row["total_collected"] or 0),
            "gold_loans": loan_row["gold_loans"] or 0,
            "personal_loans": loan_row["personal_loans"] or 0,
            "vehicle_loans": loan_row["vehicle_loans"] or 0,
        }

    _cache_set(cache_key, result)
    return result

@api_router.get("/admin/interest-earned")
async def get_interest_earned(
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):
    tenant_id = token_data["tenant_id"]
    role = token_data["role"]

    cache_key = f"interest_earned:{tenant_id}:{role}:{branch_id}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    with get_db() as conn:
        cursor = conn.cursor()

        query = """
            SELECT COALESCE(SUM(e.interest_amount), 0) AS interest_earned
            FROM emi_schedule e
            JOIN loans l ON e.loan_id = l.id
            WHERE e.status = 'paid'
            AND l.tenant_id = %s
        """

        params = [tenant_id]

        if role == "admin":
            query += " AND l.branch_id = %s"
            params.append(token_data["branch_id"])
        elif role == "super_admin" and branch_id:
            ids = branch_id.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(query, params)
        result = {"interest_earned": round(cursor.fetchone()["interest_earned"], 2)}

    _cache_set(cache_key, result)
    return result

@api_router.get("/admin/branch-loan-stats")
async def branch_loan_stats(branch_id: Optional[str] = None,
                            token_data: dict = Depends(require_role(['admin','super_admin']))):

    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")

    with get_db() as conn:
        cursor = conn.cursor()

        tenant_id = token_data["tenant_id"]
        role = token_data["role"]

        branch_filter = ""
        params = [tenant_id]

        # 🔒 Admin → Only their branch
        if role == "admin":
            branch_filter = " AND l.branch_id = %s"
            params.append(token_data["branch_id"])

        # 👑 Super admin filter
        elif role == "super_admin" and branch_id:
            ids = branch_id.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            branch_filter = f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(f"""
            SELECT b.name, SUM(l.amount) as total
            FROM loans l
            JOIN branches b ON l.branch_id = b.id
            WHERE l.status='active'
            AND l.tenant_id=%s
            {branch_filter}
            GROUP BY b.name
        """, params)

        return [{"branch": row["name"], "amount": row["total"] or 0} for row in cursor.fetchall()]


@api_router.get("/admin/branch-performance")
async def branch_performance(
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")

    tenant_id = token_data["tenant_id"]
    role = token_data["role"]

    cache_key = f"branch_performance:{tenant_id}:{role}:{branch_id}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    with get_db() as conn:
        cursor = conn.cursor()

        branch_filter = ""
        params = [tenant_id]

        if role == "admin":
            branch_filter = " AND l.branch_id = %s"
            params.append(token_data["branch_id"])
        elif role == "super_admin" and branch_id:
            ids = branch_id.split(",")
            placeholders = ",".join(["%s"] * len(ids))
            branch_filter = f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        cursor.execute(f"""
            SELECT b.name,
                   SUM(p.amount) as total_collected
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN branches b ON l.branch_id = b.id
            WHERE p.status = 'approved'
            AND l.tenant_id = %s
            {branch_filter}
            GROUP BY b.name
            ORDER BY total_collected DESC
        """, params)

        result = [
            {"branch": row["name"], "collected": row["total_collected"] or 0}
            for row in cursor.fetchall()
        ]

    _cache_set(cache_key, result)
    return result
@api_router.get("/admin/monthly-collections")
async def monthly_collections(
    branch_id: Optional[str] = None,
    token_data: dict = Depends(require_role(['admin','super_admin']))
):
    tenant_id = token_data["tenant_id"]
    role = token_data["role"]

    cache_key = f"monthly_collections:{tenant_id}:{role}:{branch_id}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    with get_db() as conn:
        cursor = conn.cursor()

        branch_filter = ""
        params = [tenant_id]

        if role == "admin":
            branch_filter = " AND l.branch_id = %s"
            params.append(token_data["branch_id"])
        elif role == "super_admin" and branch_id:
            branch_filter = " AND l.branch_id = %s"
            params.append(branch_id)

        cursor.execute(f"""
            SELECT 
                TO_CHAR(p.payment_date::date, 'YYYY-MM') as month,
                SUM(p.amount) as amount
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            WHERE p.status='approved'
            AND l.tenant_id = %s
            {branch_filter}
            GROUP BY TO_CHAR(p.payment_date::date, 'YYYY-MM')
            ORDER BY month
        """, params)

        result = [
            {"month": row["month"], "amount": row["amount"] or 0}
            for row in cursor.fetchall()
        ]

    _cache_set(cache_key, result)
    return result

@api_router.get("/admin/repo-rate")
async def get_repo_rate(token_data: dict = Depends(require_role(['admin', 'super_admin']))):
    """Return the latest RBI repo rate from history for this tenant."""
    tenant_id = token_data["tenant_id"]

    cache_key = f"repo_rate:{tenant_id}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    repo_data = get_latest_repo_rate(str(DB_PATH), tenant_id)

    if not repo_data:
        current_rate = fetch_repo_rate()
        save_repo_rate(str(DB_PATH), current_rate, tenant_id)
        result = {
            "repo_rate": current_rate,
            "fetched_at": get_ist_now().isoformat(),
            "source": "freshly_fetched"
        }
        _cache_set(cache_key, result)
        return result

    result = {
        "repo_rate": repo_data["repo_rate"],
        "fetched_at": repo_data["fetched_at"],
        "source": "database"
    }
    _cache_set(cache_key, result)
    return result
@api_router.post("/admin/update-repo-rate")
async def update_repo_rate(
    request: RepoRateUpdateRequest,
    token_data: dict = Depends(require_role(['super_admin']))
):
    tenant_id = token_data["tenant_id"]
    save_repo_rate(str(DB_PATH), request.repo_rate, tenant_id)
    recalculate_interest_rates(str(DB_PATH), request.repo_rate, tenant_id)
    _cache_invalidate_prefix(f"repo_rate:{tenant_id}")

    return {
        "message": f"Repo rate updated to {request.repo_rate}% and interest rates recalculated."
    }

# Auditor Routes
@api_router.get("/auditor/loans")
async def get_all_loans(
    branch_id: str = None,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [token_data["tenant_id"]] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT l.id AS loan_id, l.loan_number, l.loan_type, l.amount AS loan_amount,
                   l.status, DATE(l.created_at) AS created_date,
                   COALESCE(b.name, '-') AS branch_name,
                   COALESCE(c.name, '-') AS customer_name
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE l.tenant_id = %s {branch_filter}
            ORDER BY l.created_at DESC
        """, params)
        return [dict(row) for row in cursor.fetchall()]

@api_router.get("/auditor/payments")
async def get_all_payments(
    branch_id: str = None,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    if not check_permission(token_data['role'], 'payments', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND p.branch_id = %s" if branch_id else ""
        params = [token_data["tenant_id"]] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT p.id AS payment_id, p.payment_number, p.receipt_number,
                   p.amount, p.status,
                   DATE(p.created_at) AS payment_date,
                   p.approved_at AS approved_date,
                   l.loan_number,
                   COALESCE(b.name, '-') AS branch_name,
                   COALESCE(c.name, '-') AS customer_name
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE p.tenant_id = %s {branch_filter}
            ORDER BY p.created_at DESC
        """, params)
        return [dict(row) for row in cursor.fetchall()]

@api_router.get("/auditor/audit-logs")
async def get_audit_logs(
    view: str = "employee",
    branch_id: str = None,
    token_data: dict = Depends(require_role(['auditor','super_admin', 'admin']))
):
    if not check_permission(token_data['role'], 'audit_logs', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        if view == "customer":
            role_filter = "AND u.role = 'customer'"
        else:
            role_filter = "AND u.role IN ('admin','auditor','finance_officer','collection_agent','super_admin')"
        branch_filter = "AND u.branch_id = %s" if branch_id else ""
        params = [token_data["tenant_id"]] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT a.action, a.details, a.details AS description, a.created_at,
                   COALESCE(u.username, '-') AS username,
                   COALESCE(u.role, '-') AS role,
                   COALESCE(b.name, '-') AS branch_name
            FROM audit_logs a
            LEFT JOIN users u ON a.user_id = u.id
            LEFT JOIN branches b ON u.branch_id = b.id
            WHERE a.tenant_id = %s {role_filter} {branch_filter}
            ORDER BY a.created_at DESC
            LIMIT 500
        """, params)
        return [dict(row) for row in cursor.fetchall()]

@api_router.get("/auditor/verification-logs")
async def get_verification_logs(
    branch_id: str = None,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT
                l.id AS loan_id,
                l.loan_number,
                l.amount AS loan_amount,
                l.loan_type,
                l.gold_weight,
                COALESCE(u.full_name, u.username) AS approved_by,
                COALESCE(b.name, '-') AS branch_name,
                l.approved_at
            FROM loans l
            LEFT JOIN users u ON l.approved_by = u.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE l.tenant_id = %s AND l.status = 'active' {branch_filter}
            ORDER BY l.approved_at DESC
        """, params)
        return [dict(row) for row in cursor.fetchall()]


@api_router.get("/auditor/gold-rate-history")
async def get_gold_rate_history(
    source: str = None,
    from_date: str = None,
    to_date: str = None,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        filters = ""
        params = [tenant_id]
        if source in ("auto", "manual"):
            filters += " AND gr.source = %s"
            params.append(source)
        if from_date:
            try:
                from_db = datetime.strptime(from_date, "%Y-%m-%d").strftime("%Y-%m-%d")
                filters += " AND DATE(gr.updated_at) >= %s"
                params.append(from_db)
            except ValueError:
                pass
        if to_date:
            try:
                to_db = datetime.strptime(to_date, "%Y-%m-%d").strftime("%Y-%m-%d")
                filters += " AND DATE(gr.updated_at) <= %s"
                params.append(to_db)
            except ValueError:
                pass
        cursor.execute(f"""
            SELECT
                gr.updated_at,
                CASE
                    WHEN gr.branch_id = 'ALL' OR gr.branch_id IS NULL THEN 'All Branches'
                    ELSE COALESCE(b.name, gr.branch_id)
                END AS branch_name,
                gr.rate_per_gram,
                gr.source,
                CASE
                    WHEN gr.source = 'auto' THEN 'Scheduler'
                    ELSE COALESCE(u.full_name, u.username, '-')
                END AS changed_by,
                CASE
                    WHEN gr.source = 'auto' THEN 'system'
                    ELSE COALESCE(u.role, '-')
                END AS changed_role
            FROM gold_rate gr
            LEFT JOIN branches b ON gr.branch_id = b.id AND b.tenant_id = gr.tenant_id
            LEFT JOIN users u ON gr.changed_by = u.id
            WHERE gr.tenant_id = %s {filters}
            ORDER BY gr.updated_at DESC
        """, params)
        return [dict(row) for row in cursor.fetchall()]


@api_router.get("/auditor/gold-rate-history/pdf")
async def export_auditor_gold_rate_pdf(
    token: str = None,
    source: str = None,
    from_date: str = None,
    to_date: str = None,
):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        row = cursor.fetchone()
        company_name = row["name"] if row else "SV Fincloud"

        filters = ""
        params = [tenant_id]
        if source in ("auto", "manual"):
            filters += " AND gr.source = %s"
            params.append(source)
        if from_date:
            try:
                filters += " AND DATE(gr.updated_at) >= %s"
                params.append(datetime.strptime(from_date, "%Y-%m-%d").strftime("%Y-%m-%d"))
            except ValueError:
                pass
        if to_date:
            try:
                filters += " AND DATE(gr.updated_at) <= %s"
                params.append(datetime.strptime(to_date, "%Y-%m-%d").strftime("%Y-%m-%d"))
            except ValueError:
                pass

        cursor.execute(f"""
            SELECT
                gr.updated_at,
                CASE
                    WHEN gr.branch_id = 'ALL' OR gr.branch_id IS NULL THEN 'All Branches'
                    ELSE COALESCE(b.name, gr.branch_id)
                END AS branch_name,
                gr.rate_per_gram,
                gr.source,
                CASE
                    WHEN gr.source = 'auto' THEN 'Scheduler'
                    ELSE COALESCE(u.full_name, u.username, '-')
                END AS changed_by
            FROM gold_rate gr
            LEFT JOIN branches b ON gr.branch_id = b.id AND b.tenant_id = gr.tenant_id
            LEFT JOIN users u ON gr.changed_by = u.id
            WHERE gr.tenant_id = %s {filters}
            ORDER BY gr.updated_at DESC
        """, params)
        rows = cursor.fetchall()

    period = f"{from_date} – {to_date}" if from_date and to_date else None
    headers = ["Date", "Branch", "Rate/g (₹)", "Source", "Changed By"]
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    col_widths = [usable_w * 0.18, usable_w * 0.25, usable_w * 0.20, usable_w * 0.15, usable_w * 0.22]
    table_rows = []
    for r in rows:
        r = dict(r)
        ts = r["updated_at"]
        date_str = ts.strftime("%d/%m/%Y %H:%M") if hasattr(ts, "strftime") else str(ts)[:16].replace("T", " ")
        table_rows.append([
            date_str,
            r["branch_name"] or "All Branches",
            fmt_currency(r["rate_per_gram"]),
            (r["source"] or "").upper(),
            r["changed_by"] or "-",
        ])
    buf = generate_report_pdf(
        "Gold Rate History",
        headers, table_rows,
        {"Total Records": len(rows)},
        company_name, "All Branches",
        from_date, to_date,
        col_widths=col_widths,
    )
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=gold_rate_history.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/auditor/gold-rate-history/csv")
async def export_auditor_gold_rate_csv(
    token: str = None,
    source: str = None,
    from_date: str = None,
    to_date: str = None,
):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        filters = ""
        params = [tenant_id]
        if source in ("auto", "manual"):
            filters += " AND gr.source = %s"
            params.append(source)
        if from_date:
            try:
                filters += " AND DATE(gr.updated_at) >= %s"
                params.append(datetime.strptime(from_date, "%Y-%m-%d").strftime("%Y-%m-%d"))
            except ValueError:
                pass
        if to_date:
            try:
                filters += " AND DATE(gr.updated_at) <= %s"
                params.append(datetime.strptime(to_date, "%Y-%m-%d").strftime("%Y-%m-%d"))
            except ValueError:
                pass
        cursor.execute(f"""
            SELECT
                gr.updated_at,
                CASE
                    WHEN gr.branch_id = 'ALL' OR gr.branch_id IS NULL THEN 'All Branches'
                    ELSE COALESCE(b.name, gr.branch_id)
                END AS branch_name,
                gr.rate_per_gram,
                gr.source,
                CASE
                    WHEN gr.source = 'auto' THEN 'Scheduler'
                    ELSE COALESCE(u.full_name, u.username, '-')
                END AS changed_by
            FROM gold_rate gr
            LEFT JOIN branches b ON gr.branch_id = b.id AND b.tenant_id = gr.tenant_id
            LEFT JOIN users u ON gr.changed_by = u.id
            WHERE gr.tenant_id = %s {filters}
            ORDER BY gr.updated_at DESC
        """, params)
        rows = cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    write_csv_summary(writer, "Gold Rate History", "All Branches", {"Total Records": len(rows)})
    writer.writerow(["Date", "Branch", "Rate Per Gram (₹)", "Source", "Changed By"])
    for r in rows:
        r = dict(r)
        ts = r["updated_at"]
        date_str = ts.strftime("%d/%m/%Y %H:%M") if hasattr(ts, "strftime") else str(ts)[:16].replace("T", " ")
        writer.writerow([
            f'="{date_str}"',
            r["branch_name"] or "All Branches",
            r["rate_per_gram"],
            r["source"] or "-",
            r["changed_by"] or "-",
        ])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "gold_rate_history.csv")


@api_router.get("/auditor/reports/branch-summary")
async def get_branch_summary(token_data: dict = Depends(require_role(['auditor', 'super_admin']))):
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                b.id AS branch_id,
                b.name AS branch_name,
                b.location,
                COALESCE(SUM(CASE WHEN l.status = 'active' THEN l.disbursed_amount ELSE 0 END), 0) AS total_disbursed,
                COALESCE(SUM(CASE WHEN l.loan_type = 'gold_loan' AND l.status = 'active'
                                  THEN l.gold_weight ELSE 0 END), 0) AS active_gold_weight,
                COUNT(CASE WHEN l.status = 'active' AND DATE(l.approved_at) = CURRENT_DATE
                           THEN 1 END) AS verifications_today
            FROM branches b
            LEFT JOIN loans l ON l.branch_id = b.id AND l.tenant_id = b.tenant_id
            WHERE b.tenant_id = %s
            GROUP BY b.id, b.name, b.location
        """, (tenant_id,))
        branches = {row["branch_id"]: dict(row) for row in cursor.fetchall()}

        cursor.execute("""
            SELECT branch_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved
            FROM payments WHERE tenant_id = %s
            GROUP BY branch_id
        """, (tenant_id,))
        for row in cursor.fetchall():
            bid = row["branch_id"]
            if bid in branches and row["total"] > 0:
                branches[bid]["collection_efficiency"] = round(row["approved"] * 100.0 / row["total"], 2)

        result = []
        for b in branches.values():
            b.setdefault("collection_efficiency", 0.0)
            result.append(b)
        return result


@api_router.get("/auditor/dashboard-summary")
async def get_dashboard_summary(
    branch_id: str = None,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    tenant_id = token_data["tenant_id"]
    bf_loans = "AND branch_id = %s" if branch_id else ""
    bf_pay   = "AND branch_id = %s" if branch_id else ""
    p_loans  = [tenant_id] + ([branch_id] if branch_id else [])
    p_pay    = [tenant_id] + ([branch_id] if branch_id else [])
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT COALESCE(SUM(disbursed_amount), 0) AS total_disbursed
            FROM loans WHERE tenant_id = %s AND status IN ('active', 'closed') {bf_loans}
        """, p_loans)
        total_disbursed = cursor.fetchone()["total_disbursed"]

        cursor.execute(f"""
            SELECT COALESCE(SUM(gold_weight), 0) AS total_gold_weight
            FROM loans WHERE tenant_id = %s AND status = 'active' {bf_loans}
        """, p_loans)
        total_gold_weight = cursor.fetchone()["total_gold_weight"]

        cursor.execute(f"""
            SELECT COUNT(*) AS todays_verifications
            FROM loans WHERE tenant_id = %s AND status = 'active' AND DATE(approved_at) = CURRENT_DATE {bf_loans}
        """, p_loans)
        todays_verifications = cursor.fetchone()["todays_verifications"]

        cursor.execute(f"""
            SELECT branch_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved
            FROM payments WHERE tenant_id = %s {bf_pay}
            GROUP BY branch_id
        """, p_pay)
        branch_rows = cursor.fetchall()
        efficiencies = [
            row["approved"] * 100.0 / row["total"]
            for row in branch_rows if row["total"] > 0
        ]
        avg_collection_efficiency = round(sum(efficiencies) / len(efficiencies), 2) if efficiencies else 0.0

        return {
            "total_disbursed": total_disbursed,
            "total_gold_weight": total_gold_weight,
            "avg_collection_efficiency": avg_collection_efficiency,
            "todays_verifications": todays_verifications,
        }


@api_router.get("/auditor/reports/loan-type-distribution")
async def get_loan_type_distribution(
    branch_id: str = None,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT loan_type, COUNT(*) AS count
            FROM loans WHERE tenant_id = %s AND status = 'active' {branch_filter}
            GROUP BY loan_type
        """, params)
        return {row["loan_type"]: row["count"] for row in cursor.fetchall()}


@api_router.get("/auditor/reports/monthly-disbursement")
async def get_monthly_disbursement(token_data: dict = Depends(require_role(['auditor', 'super_admin']))):
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TO_CHAR(approved_at, 'Mon') AS month,
                   TO_CHAR(approved_at, 'YYYY-MM') AS sort_key,
                   COALESCE(SUM(amount), 0) AS amount
            FROM loans
            WHERE tenant_id = %s AND status = 'active' AND approved_at IS NOT NULL
            GROUP BY TO_CHAR(approved_at, 'YYYY-MM')
            ORDER BY sort_key ASC
        """, (tenant_id,))
        rows = cursor.fetchall()
        if not rows:
            return []
        return [{"month": row["month"], "amount": row["amount"]} for row in rows]


@api_router.get("/auditor/gold-rate-summary")
async def get_gold_rate_summary(token_data: dict = Depends(require_role(['auditor', 'super_admin']))):
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        result = resolve_gold_rate(cursor, tenant_id)
        current_rate = result["rate_per_gram"]

        cursor.execute("""
            SELECT COUNT(*) AS cnt FROM gold_rate
            WHERE tenant_id = %s AND source = 'auto' AND DATE(updated_at) = CURRENT_DATE
        """, (tenant_id,))
        auto_updates_today = cursor.fetchone()["cnt"]

        cursor.execute("""
            SELECT COUNT(*) AS cnt FROM gold_rate
            WHERE tenant_id = %s AND source = 'manual' AND DATE(updated_at) = CURRENT_DATE
        """, (tenant_id,))
        manual_updates_today = cursor.fetchone()["cnt"]

        return {
            "current_rate": current_rate,
            "mode": result["mode"],
            "auto_updates_today": auto_updates_today,
            "manual_updates_today": manual_updates_today,
        }


@api_router.get("/auditor/reports/disbursement-detail")
async def get_disbursement_detail(
    branch_id: str = None,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT l.id AS loan_id, l.loan_number, c.name AS customer_name, b.name AS branch_name,
                   l.disbursed_amount, l.loan_type,
                   COALESCE(u.full_name, u.username) AS approved_by, l.approved_at
            FROM loans l
            LEFT JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            LEFT JOIN users u ON l.approved_by = u.id
            WHERE l.tenant_id = %s AND l.status IN ('active', 'closed') {branch_filter}
            ORDER BY l.approved_at DESC
        """, params)
        return [dict(row) for row in cursor.fetchall()]


@api_router.get("/auditor/reports/gold-detail")
async def get_gold_detail(
    branch_id: str = None,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT l.id AS loan_id, l.loan_number, c.name AS customer_name, b.name AS branch_name,
                   l.gold_weight, l.amount,
                   COALESCE(u.full_name, u.username) AS approved_by, l.approved_at
            FROM loans l
            LEFT JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            LEFT JOIN users u ON l.approved_by = u.id
            WHERE l.tenant_id = %s AND l.status = 'active' AND l.loan_type = 'gold_loan' {branch_filter}
            ORDER BY l.approved_at DESC
        """, params)
        return [dict(row) for row in cursor.fetchall()]


@api_router.get("/auditor/reports/collection-detail")
async def get_collection_detail(
    branch_id: str = None,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND p.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT p.id AS payment_id, p.payment_number, l.loan_number,
                   c.name AS customer_name, b.name AS branch_name,
                   p.amount, p.status, p.payment_date, p.approved_at
            FROM payments p
            LEFT JOIN loans l ON p.loan_id = l.id
            LEFT JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON p.branch_id = b.id
            WHERE p.tenant_id = %s {branch_filter}
            ORDER BY p.payment_date DESC
        """, params)
        return [dict(row) for row in cursor.fetchall()]


@api_router.get("/auditor/reports/verification-detail")
async def get_verification_detail(
    branch_id: str = None,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    tenant_id = token_data["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT l.id AS loan_id, l.loan_number, c.name AS customer_name, b.name AS branch_name,
                   l.amount, l.loan_type,
                   COALESCE(u.full_name, u.username) AS approved_by, l.approved_at
            FROM loans l
            LEFT JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            LEFT JOIN users u ON l.approved_by = u.id
            WHERE l.tenant_id = %s AND l.status = 'active' AND DATE(l.approved_at) = CURRENT_DATE {branch_filter}
            ORDER BY l.approved_at DESC
        """, params)
        return [dict(row) for row in cursor.fetchall()]


# ── Auditor Tab Export Endpoints ──────────────────────────────────────────────

def _auditor_export_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("role") not in ("auditor", "super_admin"):
            raise HTTPException(403, "Access denied")
        return payload
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(401, "Invalid token")


@api_router.get("/auditor/loans/pdf")
async def export_auditor_loans_pdf(token: str = None, branch_id: str = None):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        t = cursor.fetchone(); company_name = t["name"] if t else "SV Fincloud"
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT l.id AS loan_id, l.loan_number, l.loan_type, l.amount AS loan_amount,
                   l.status, DATE(l.created_at) AS created_date,
                   COALESCE(b.name, '-') AS branch_name,
                   COALESCE(c.name, '-') AS customer_name
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE l.tenant_id = %s {branch_filter} ORDER BY l.created_at DESC
        """, params)
        rows = cursor.fetchall()
    branch_label = rows[0]["branch_name"] if rows and branch_id else "All Branches"
    headers = ["Loan No", "Customer", "Branch", "Amount", "Type", "Status", "Date"]
    col_widths = [70, 100, 90, 80, 70, 55, 65]  # total=530 ≤ usable ~495+margin
    _cell = ParagraphStyle("LC", fontName="DejaVu", fontSize=8, leading=10, wordWrap="CJK")
    pdf_rows = [
        [Paragraph(r["loan_number"] or str(r["loan_id"]), _cell),
         Paragraph(r["customer_name"], _cell),
         Paragraph(r["branch_name"], _cell),
         Paragraph(f'\u20b9{float(r["loan_amount"] or 0):,.2f}', _cell),
         Paragraph((r["loan_type"] or "").replace("_", " ").title(), _cell),
         Paragraph((r["status"] or "").upper(), _cell),
         Paragraph(str(r["created_date"] or "-"), _cell)]
        for r in rows
    ]
    totals = {"Total Records": len(rows), "Total Amount": f'\u20b9{sum(float(r["loan_amount"] or 0) for r in rows):,.2f}'}
    buf = generate_report_pdf("All Loans", headers, pdf_rows, totals, company_name, branch_label, col_widths=col_widths)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=all_loans.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/auditor/loans/csv")
async def export_auditor_loans_csv(token: str = None, branch_id: str = None):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT l.id AS loan_id, l.loan_number, l.loan_type, l.amount AS loan_amount,
                   l.status, DATE(l.created_at) AS created_date,
                   COALESCE(b.name, '-') AS branch_name,
                   COALESCE(c.name, '-') AS customer_name
            FROM loans l
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE l.tenant_id = %s {branch_filter} ORDER BY l.created_at DESC
        """, params)
        rows = cursor.fetchall()
    branch_label = rows[0]["branch_name"] if rows and branch_id else "All Branches"
    output = io.StringIO()
    writer = csv.writer(output)
    write_csv_summary(writer, "All Loans", branch_label, {
        "Total Records": len(rows),
        "Total Amount": f'{sum(float(r["loan_amount"] or 0) for r in rows):.2f}'
    })
    writer.writerow(["Loan No", "Customer", "Branch", "Amount", "Type", "Status", "Date"])
    for r in rows:
        writer.writerow([
            f'="{r["loan_number"] or r["loan_id"]}"', r["customer_name"], r["branch_name"],
            float(r["loan_amount"] or 0),
            (r["loan_type"] or "").replace("_", " ").title(),
            (r["status"] or "").upper(),
            f'="{r["created_date"]}"' if r["created_date"] else "-"
        ])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "all_loans.csv")


@api_router.get("/auditor/payments/pdf")
async def export_auditor_payments_pdf(token: str = None, branch_id: str = None):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        t = cursor.fetchone(); company_name = t["name"] if t else "SV Fincloud"
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT p.id AS payment_id, p.payment_number, p.amount, p.status,
                   DATE(p.created_at) AS payment_date,
                   p.approved_at AS approved_date,
                   l.loan_number,
                   COALESCE(b.name, '-') AS branch_name,
                   COALESCE(c.name, '-') AS customer_name
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE p.tenant_id = %s {branch_filter} ORDER BY p.created_at DESC
        """, params)
        rows = cursor.fetchall()
    branch_label = rows[0]["branch_name"] if rows and branch_id else "All Branches"
    headers = ["Payment No", "Customer", "Branch", "Amount", "Status", "Date", "Approved Date"]
    col_widths = [75, 100, 90, 75, 55, 60, 75]  # total=530
    _cell = ParagraphStyle("PC", fontName="DejaVu", fontSize=8, leading=10, wordWrap="CJK")
    pdf_rows = [
        [Paragraph(r["payment_number"] or str(r["payment_id"]), _cell),
         Paragraph(r["customer_name"], _cell),
         Paragraph(r["branch_name"], _cell),
         Paragraph(f'\u20b9{float(r["amount"] or 0):,.2f}', _cell),
         Paragraph((r["status"] or "").upper(), _cell),
         Paragraph(str(r["payment_date"] or "-"), _cell),
         Paragraph(str(r["approved_date"])[:10] if r["status"] == "approved" and r["approved_date"] else "-", _cell)]
        for r in rows
    ]
    totals = {"Total Records": len(rows), "Total Amount": f'\u20b9{sum(float(r["amount"] or 0) for r in rows):,.2f}'}
    buf = generate_report_pdf("All Payments", headers, pdf_rows, totals, company_name, branch_label, col_widths=col_widths)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=all_payments.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/auditor/payments/csv")
async def export_auditor_payments_csv(token: str = None, branch_id: str = None):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT p.id AS payment_id, p.payment_number, p.amount, p.status,
                   DATE(p.created_at) AS payment_date,
                   p.approved_at AS approved_date,
                   l.loan_number,
                   COALESCE(b.name, '-') AS branch_name,
                   COALESCE(c.name, '-') AS customer_name
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE p.tenant_id = %s {branch_filter} ORDER BY p.created_at DESC
        """, params)
        rows = cursor.fetchall()
    branch_label = rows[0]["branch_name"] if rows and branch_id else "All Branches"
    output = io.StringIO()
    writer = csv.writer(output)
    write_csv_summary(writer, "All Payments", branch_label, {
        "Total Records": len(rows),
        "Total Amount": f'{sum(float(r["amount"] or 0) for r in rows):.2f}'
    })
    writer.writerow(["Payment No", "Customer", "Branch", "Amount", "Status", "Date", "Approved Date"])
    for r in rows:
        approved = f'="{str(r["approved_date"])[:10]}"' if r["status"] == "approved" and r["approved_date"] else "-"
        writer.writerow([
            f'="{r["payment_number"] or r["payment_id"]}"', r["customer_name"], r["branch_name"],
            float(r["amount"] or 0),
            (r["status"] or "").upper(),
            f'="{r["payment_date"]}"' if r["payment_date"] else "-",
            approved
        ])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "all_payments.csv")


@api_router.get("/auditor/audit-logs/pdf")
async def export_auditor_audit_logs_pdf(token: str = None, view: str = "employee", branch_id: str = None):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        t = cursor.fetchone(); company_name = t["name"] if t else "SV Fincloud"
        role_filter = "AND u.role = 'customer'" if view == "customer" else \
            "AND u.role IN ('admin','auditor','finance_officer','collection_agent','super_admin')"
        branch_filter = "AND b.id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT a.action, a.details, a.created_at,
                   COALESCE(u.username, '-') AS username,
                   COALESCE(u.role, '-') AS role,
                   COALESCE(b.name, '-') AS branch_name
            FROM audit_logs a
            LEFT JOIN users u ON a.user_id = u.id
            LEFT JOIN branches b ON u.branch_id = b.id
            WHERE a.tenant_id = %s {role_filter} {branch_filter}
            ORDER BY a.created_at DESC LIMIT 500
        """, params)
        rows = cursor.fetchall()
    headers = ["Action", "User", "Role", "Branch", "Details", "Timestamp"]
    col_widths = [70, 85, 85, 110, 145, 100]
    if view == "employee":
        headers = ["Action", "User", "Role", "Branch", "Timestamp"]
        col_widths = [80, 100, 100, 155, 160]
    _cell_style = ParagraphStyle("AuditCell", fontName="DejaVu", fontSize=8, leading=10, wordWrap="CJK")
    pdf_rows = []
    for r in rows:
        ts = r["created_at"]
        ts_str = ts.strftime("%d/%m/%Y %H:%M") if hasattr(ts, "strftime") else (str(ts)[:16].replace("T", " ") if ts else "-")
        row = [
            Paragraph(str(r["action"] or "-"), _cell_style),
            Paragraph(str(r["username"]), _cell_style),
            Paragraph(str(r["role"]), _cell_style),
            Paragraph(str(r["branch_name"]), _cell_style),
        ]
        if view != "employee":
            row.append(Paragraph(str(r["details"] or "-"), _cell_style))
        row.append(Paragraph(ts_str, _cell_style))
        pdf_rows.append(row)
    totals = {"Total Records": len(rows), "View": view.title()}
    buf = generate_report_pdf("Audit Logs", headers, pdf_rows, totals, company_name, "All Branches",
                              col_widths=col_widths)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=audit_logs.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/auditor/audit-logs/csv")
async def export_auditor_audit_logs_csv(token: str = None, view: str = "employee", branch_id: str = None):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        role_filter = "AND u.role = 'customer'" if view == "customer" else \
            "AND u.role IN ('admin','auditor','finance_officer','collection_agent','super_admin')"
        branch_filter = "AND b.id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT a.action, a.details, a.created_at,
                   COALESCE(u.username, '-') AS username,
                   COALESCE(u.role, '-') AS role,
                   COALESCE(b.name, '-') AS branch_name
            FROM audit_logs a
            LEFT JOIN users u ON a.user_id = u.id
            LEFT JOIN branches b ON u.branch_id = b.id
            WHERE a.tenant_id = %s {role_filter} {branch_filter}
            ORDER BY a.created_at DESC LIMIT 500
        """, params)
        rows = cursor.fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    write_csv_summary(writer, "Audit Logs", "All Branches", {"Total Records": len(rows), "View": view.title()})
    if view == "employee":
        writer.writerow(["Action", "User", "Role", "Branch", "Timestamp"])
    else:
        writer.writerow(["Action", "User", "Role", "Branch", "Details", "Timestamp"])
    for r in rows:
        ts = r["created_at"]
        ts_str = ts.strftime("%d/%m/%Y %H:%M") if hasattr(ts, "strftime") else (str(ts)[:16].replace("T", " ") if ts else "-")
        if view == "employee":
            writer.writerow([r["action"] or "-", r["username"], r["role"], r["branch_name"], f'="{ts_str}"'])
        else:
            writer.writerow([r["action"] or "-", r["username"], r["role"], r["branch_name"], r["details"] or "-", f'="{ts_str}"'])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "audit_logs.csv")


@api_router.get("/auditor/branch-summary/pdf")
async def export_auditor_branch_summary_pdf(token: str = None):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        t = cursor.fetchone(); company_name = t["name"] if t else "SV Fincloud"
        cursor.execute("""
            SELECT b.id AS branch_id, b.name AS branch_name, b.location,
                   COALESCE(SUM(CASE WHEN l.status = 'active' THEN l.disbursed_amount ELSE 0 END), 0) AS total_disbursed,
                   COALESCE(SUM(CASE WHEN l.loan_type = 'gold_loan' AND l.status = 'active' THEN l.gold_weight ELSE 0 END), 0) AS active_gold_weight,
                   COUNT(CASE WHEN l.status = 'active' AND DATE(l.approved_at) = CURRENT_DATE THEN 1 END) AS verifications_today
            FROM branches b
            LEFT JOIN loans l ON l.branch_id = b.id AND l.tenant_id = b.tenant_id
            WHERE b.tenant_id = %s GROUP BY b.id, b.name, b.location
        """, (tenant_id,))
        branches_raw = cursor.fetchall()
        cursor.execute("""
            SELECT branch_id, COUNT(*) AS total,
                   SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved
            FROM payments WHERE tenant_id = %s GROUP BY branch_id
        """, (tenant_id,))
        eff_map = {r["branch_id"]: round(r["approved"] * 100.0 / r["total"], 1) if r["total"] > 0 else 0.0
                   for r in cursor.fetchall()}
    headers = ["Branch", "Location", "Total Disbursed", "Collection %", "Gold Weight (g)", "Verifications Today"]
    _cell_style = ParagraphStyle("BranchCell", fontName="DejaVu", fontSize=9, leading=12, wordWrap="CJK")
    pdf_rows = [
        [Paragraph(r["branch_name"] or "-", _cell_style),
         Paragraph(r["location"] or "-", _cell_style),
         Paragraph(f'\u20b9{float(r["total_disbursed"] or 0):,.2f}', _cell_style),
         Paragraph(f'{eff_map.get(r["branch_id"], 0.0):.1f}%', _cell_style),
         Paragraph(f'{float(r["active_gold_weight"] or 0):.2f}', _cell_style),
         Paragraph(str(r["verifications_today"] or 0), _cell_style)]
        for r in branches_raw
    ]
    totals = {"Total Branches": len(branches_raw)}
    _col_widths = [88, 88, 88, 80, 80, 71]
    buf = generate_report_pdf("Branch Summary", headers, pdf_rows, totals, company_name, "All Branches", col_widths=_col_widths)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=branch_summary.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/auditor/branch-summary/csv")
async def export_auditor_branch_summary_csv(token: str = None):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT b.id AS branch_id, b.name AS branch_name, b.location,
                   COALESCE(SUM(CASE WHEN l.status = 'active' THEN l.disbursed_amount ELSE 0 END), 0) AS total_disbursed,
                   COALESCE(SUM(CASE WHEN l.loan_type = 'gold_loan' AND l.status = 'active' THEN l.gold_weight ELSE 0 END), 0) AS active_gold_weight,
                   COUNT(CASE WHEN l.status = 'active' AND DATE(l.approved_at) = CURRENT_DATE THEN 1 END) AS verifications_today
            FROM branches b
            LEFT JOIN loans l ON l.branch_id = b.id AND l.tenant_id = b.tenant_id
            WHERE b.tenant_id = %s GROUP BY b.id, b.name, b.location
        """, (tenant_id,))
        branches_raw = cursor.fetchall()
        cursor.execute("""
            SELECT branch_id, COUNT(*) AS total,
                   SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved
            FROM payments WHERE tenant_id = %s GROUP BY branch_id
        """, (tenant_id,))
        eff_map = {r["branch_id"]: round(r["approved"] * 100.0 / r["total"], 1) if r["total"] > 0 else 0.0
                   for r in cursor.fetchall()}
    output = io.StringIO()
    writer = csv.writer(output)
    write_csv_summary(writer, "Branch Summary", "All Branches", {"Total Branches": len(branches_raw)})
    writer.writerow(["Branch", "Location", "Total Disbursed", "Collection %", "Gold Weight (g)", "Verifications Today"])
    for r in branches_raw:
        writer.writerow([
            r["branch_name"], r["location"] or "-",
            float(r["total_disbursed"] or 0),
            f'{eff_map.get(r["branch_id"], 0.0):.1f}%',
            f'{float(r["active_gold_weight"] or 0):.2f}',
            r["verifications_today"] or 0
        ])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "branch_summary.csv")


@api_router.get("/auditor/verification-logs/pdf")
async def export_auditor_verification_logs_pdf(token: str = None, branch_id: str = None):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        t = cursor.fetchone(); company_name = t["name"] if t else "SV Fincloud"
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT l.id AS loan_id, l.loan_number, l.amount AS loan_amount, l.gold_weight,
                   COALESCE(u.full_name, u.username) AS approved_by,
                   COALESCE(b.name, '-') AS branch_name, l.approved_at
            FROM loans l
            LEFT JOIN users u ON l.approved_by = u.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE l.tenant_id = %s AND l.status = 'active' {branch_filter}
            ORDER BY l.approved_at DESC
        """, params)
        rows = cursor.fetchall()
    branch_label = rows[0]["branch_name"] if rows and branch_id else "All Branches"
    headers = ["Loan No", "Loan Amount", "Gold Weight (g)", "Approved By", "Branch", "Approval Date"]
    col_widths = [75, 90, 80, 110, 100, 75]  # total=530
    _cell = ParagraphStyle("VC", fontName="DejaVu", fontSize=8, leading=10, wordWrap="CJK")
    pdf_rows = [
        [Paragraph(r["loan_number"] or str(r["loan_id"]), _cell),
         Paragraph(f'\u20b9{float(r["loan_amount"] or 0):,.2f}', _cell),
         Paragraph(f'{float(r["gold_weight"] or 0):.2f}' if r["gold_weight"] else "-", _cell),
         Paragraph(str(r["approved_by"]) if r["approved_by"] else "Pending Approval", _cell),
         Paragraph(str(r["branch_name"]), _cell),
         Paragraph(str(r["approved_at"])[:10] if r["approved_at"] else "-", _cell)]
        for r in rows
    ]
    totals = {"Total Records": len(rows)}
    buf = generate_report_pdf("Loan Verification Logs", headers, pdf_rows, totals, company_name, branch_label, col_widths=col_widths)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=verification_logs.pdf",
                 "Access-Control-Expose-Headers": "Content-Disposition"})


@api_router.get("/auditor/verification-logs/csv")
async def export_auditor_verification_logs_csv(token: str = None, branch_id: str = None):
    td = _auditor_export_token(token)
    tenant_id = td["tenant_id"]
    with get_db() as conn:
        cursor = conn.cursor()
        branch_filter = "AND l.branch_id = %s" if branch_id else ""
        params = [tenant_id] + ([branch_id] if branch_id else [])
        cursor.execute(f"""
            SELECT l.id AS loan_id, l.loan_number, l.amount AS loan_amount, l.gold_weight,
                   COALESCE(u.full_name, u.username) AS approved_by,
                   COALESCE(b.name, '-') AS branch_name, l.approved_at
            FROM loans l
            LEFT JOIN users u ON l.approved_by = u.id
            LEFT JOIN branches b ON l.branch_id = b.id
            WHERE l.tenant_id = %s AND l.status = 'active' {branch_filter}
            ORDER BY l.approved_at DESC
        """, params)
        rows = cursor.fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    write_csv_summary(writer, "Loan Verification Logs", "All Branches", {"Total Records": len(rows)})
    writer.writerow(["Loan No", "Loan Amount", "Gold Weight (g)", "Approved By", "Branch", "Approval Date"])
    for r in rows:
        writer.writerow([
            f'="{r["loan_number"] or r["loan_id"]}"',
            float(r["loan_amount"] or 0),
            f'{float(r["gold_weight"] or 0):.2f}' if r["gold_weight"] else "-",
            r["approved_by"], r["branch_name"],
            f'="{str(r["approved_at"])[:10]}"' if r["approved_at"] else "-"
        ])
    output.seek(0)
    return _make_csv_response(output.getvalue(), "verification_logs.csv")


@api_router.get("/auditor/reports/export")
async def export_auditor_report(
    type: str,
    format: str,
    token_data: dict = Depends(require_role(['auditor', 'super_admin']))
):
    tenant_id = token_data["tenant_id"]
    if format not in ("csv", "pdf"):
        raise HTTPException(status_code=400, detail="format must be csv or pdf")

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM tenants WHERE id = %s", (tenant_id,))
        t = cursor.fetchone()
        company_name = t["name"] if t else "SV Fincloud"

        if type == "disbursement":
            cursor.execute("""
                SELECT l.id, l.loan_number, c.name AS customer_name, COALESCE(b.name, '-') AS branch_name,
                       l.amount, l.loan_type,
                       COALESCE(u.full_name, u.username) AS approved_by, l.approved_at
                FROM loans l
                LEFT JOIN customers c ON l.customer_id = c.id
                LEFT JOIN branches b ON l.branch_id = b.id
                LEFT JOIN users u ON l.approved_by = u.id
                WHERE l.tenant_id = %s AND l.status IN ('active','closed')
                ORDER BY l.approved_at DESC
            """, (tenant_id,))
            rows_raw = cursor.fetchall()
            title = "Loan Disbursement Report"
            headers = ["Loan No", "Customer", "Branch", "Loan Amount", "Loan Type", "Approved By", "Approved Date"]
            rows = [
                [f'="{r["loan_number"] or r["id"]}"', r["customer_name"] or "-", r["branch_name"] or "-",
                 float(r["amount"] or 0), r["loan_type"] or "-",
                 r["approved_by"] or "-",
                 f'="{r["approved_at"][:10]}"' if r["approved_at"] else "-"]
                for r in rows_raw
            ]
            pdf_rows = [
                [r["loan_number"] or str(r["id"]), r["customer_name"] or "-", r["branch_name"] or "-",
                 f'₹{r["amount"]:,.2f}', r["loan_type"] or "-",
                 r["approved_by"] or "-",
                 r["approved_at"][:10] if r["approved_at"] else "-"]
                for r in rows_raw
            ]
            total_amt = sum(r["amount"] or 0 for r in rows_raw)
            totals = {"Total Records": len(rows), "Total Disbursed": f"{total_amt:,.2f}"}

        elif type == "gold":
            cursor.execute("""
                SELECT l.id, l.loan_number, c.name AS customer_name, COALESCE(b.name, '-') AS branch_name,
                       l.gold_weight, l.amount,
                       COALESCE(u.full_name, u.username) AS approved_by, l.approved_at
                FROM loans l
                LEFT JOIN customers c ON l.customer_id = c.id
                LEFT JOIN branches b ON l.branch_id = b.id
                LEFT JOIN users u ON l.approved_by = u.id
                WHERE l.tenant_id = %s AND l.status = 'active' AND l.loan_type = 'gold_loan'
                ORDER BY l.approved_at DESC
            """, (tenant_id,))
            rows_raw = cursor.fetchall()
            title = "Gold in Safe Report"
            headers = ["Loan No", "Customer", "Branch", "Gold Weight (g)", "Loan Amount", "Approved By", "Approved Date"]
            rows = [
                [f'="{r["loan_number"] or r["id"]}"', r["customer_name"] or "-", r["branch_name"] or "-",
                 f'="{r["gold_weight"]:.2f}"' if r["gold_weight"] else "0",
                 float(r["amount"] or 0), r["approved_by"] or "-",
                 f'="{r["approved_at"][:10]}"' if r["approved_at"] else "-"]
                for r in rows_raw
            ]
            pdf_rows = [
                [r["loan_number"] or str(r["id"]), r["customer_name"] or "-", r["branch_name"] or "-",
                 f'{r["gold_weight"]:.2f}' if r["gold_weight"] else "0",
                 f'₹{r["amount"]:,.2f}', r["approved_by"] or "-",
                 r["approved_at"][:10] if r["approved_at"] else "-"]
                for r in rows_raw
            ]
            totals = {"Total Records": len(rows)}

        elif type == "collection":
            cursor.execute("""
                SELECT p.id, p.payment_number, c.name AS customer_name, COALESCE(b.name, '-') AS branch_name,
                       p.amount, p.status, DATE(p.created_at) AS payment_date, p.approved_at
                FROM payments p
                LEFT JOIN loans l ON p.loan_id = l.id
                LEFT JOIN customers c ON l.customer_id = c.id
                LEFT JOIN branches b ON l.branch_id = b.id
                WHERE p.tenant_id = %s
                ORDER BY p.created_at DESC
            """, (tenant_id,))
            rows_raw = cursor.fetchall()
            title = "Collection Efficiency Report"
            headers = ["Payment No", "Customer", "Branch", "Amount", "Status", "Payment Date", "Approved Date"]
            rows = [
                [f'="{r["payment_number"] or r["id"]}"', r["customer_name"] or "-", r["branch_name"] or "-",
                 float(r["amount"] or 0), r["status"],
                 f'="{r["payment_date"]}"' if r["payment_date"] else "-",
                 f'="{r["approved_at"][:10]}"' if r["approved_at"] else "-"]
                for r in rows_raw
            ]
            pdf_rows = [
                [r["payment_number"] or str(r["id"]), r["customer_name"] or "-", r["branch_name"] or "-",
                 f'₹{r["amount"]:,.2f}', r["status"],
                 str(r["payment_date"]) if r["payment_date"] else "-",
                 r["approved_at"][:10] if r["approved_at"] else "-"]
                for r in rows_raw
            ]
            totals = {"Total Records": len(rows)}

        elif type == "verifications":
            cursor.execute("""
                SELECT l.id, l.loan_number, c.name AS customer_name, COALESCE(b.name, '-') AS branch_name,
                       l.amount, l.loan_type,
                       COALESCE(u.full_name, u.username) AS approved_by, l.approved_at
                FROM loans l
                LEFT JOIN customers c ON l.customer_id = c.id
                LEFT JOIN branches b ON l.branch_id = b.id
                LEFT JOIN users u ON l.approved_by = u.id
                WHERE l.tenant_id = %s AND l.status = 'active' AND DATE(l.approved_at) = CURRENT_DATE
                ORDER BY l.approved_at DESC
            """, (tenant_id,))
            rows_raw = cursor.fetchall()
            title = "Today's Verifications Report"
            headers = ["Loan No", "Customer", "Branch", "Loan Amount", "Loan Type", "Approved By", "Approved Date"]
            rows = [
                [f'="{r["loan_number"] or r["id"]}"', r["customer_name"] or "-", r["branch_name"] or "-",
                 float(r["amount"] or 0), r["loan_type"] or "-",
                 r["approved_by"] or "-",
                 f'="{r["approved_at"][:10]}"' if r["approved_at"] else "-"]
                for r in rows_raw
            ]
            pdf_rows = [
                [r["loan_number"] or str(r["id"]), r["customer_name"] or "-", r["branch_name"] or "-",
                 f'₹{r["amount"]:,.2f}', r["loan_type"] or "-",
                 r["approved_by"] or "-",
                 r["approved_at"][:10] if r["approved_at"] else "-"]
                for r in rows_raw
            ]
            totals = {"Total Records": len(rows)}

        else:
            raise HTTPException(status_code=400, detail=f"Unknown report type: {type}")

        if format == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            write_csv_summary(writer, title, company_name, totals)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)
            output.seek(0)
            return _make_csv_response(output.getvalue(), f"{type}_report.csv")
        else:
            from reportlab.lib.units import inch
            from reportlab.platypus import Paragraph
            body_style = ParagraphStyle("ARBody", fontName="DejaVu", fontSize=8, leading=10, wordWrap="CJK")

            # Wrap all cell values in Paragraph for text wrapping
            wrapped_rows = [
                [Paragraph(str(cell), body_style) for cell in row]
                for row in pdf_rows
            ]
            wrapped_headers = [Paragraph(str(h), body_style) for h in headers]

            # Per-report explicit column widths (inches → points via inch)
            col_width_map = {
                "disbursement": [1.1*inch, 1.2*inch, 1.1*inch, 1.0*inch, 0.9*inch, 1.0*inch, 0.9*inch],
                "gold":         [1.1*inch, 1.2*inch, 1.1*inch, 0.9*inch, 1.0*inch, 1.0*inch, 0.9*inch],
                "collection":   [1.1*inch, 1.2*inch, 1.1*inch, 1.0*inch, 0.8*inch, 0.9*inch, 0.9*inch],
                "verifications":[1.1*inch, 1.2*inch, 1.1*inch, 1.0*inch, 0.9*inch, 1.0*inch, 0.9*inch],
            }
            col_widths = col_width_map.get(type, [1.0*inch] * len(headers))

            pdf_buffer = generate_report_pdf(
                title=title,
                headers=wrapped_headers,
                rows=wrapped_rows,
                totals_info=totals,
                company_name=company_name,
                branch_name="All Branches",
                col_widths=col_widths
            )
            return StreamingResponse(
                pdf_buffer,
                media_type="application/pdf",
                headers={"Content-Disposition": f"attachment; filename={type}_report.pdf",
                         "Access-Control-Expose-Headers": "Content-Disposition"}
            )


# Common Routes
@api_router.get("/loan-types")
async def get_loan_types(token_data: dict = Depends(verify_token)):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM loan_types")
        types = [dict(row) for row in cursor.fetchall()]
        return types

@api_router.get("/gold-rate")
async def get_public_gold_rate(token_data: dict = Depends(verify_token)):
    with get_db() as conn:
        cursor = conn.cursor()
        tenant_id = token_data["tenant_id"]
        result = resolve_gold_rate(cursor, tenant_id)
        logger.debug("MODE: %s RATE: %s", result["mode"], result["rate_per_gram"])
        return result



@api_router.get("/vehicle/details")
async def get_vehicle_details(reg_number: str):

    from services.vehicle_service import fetch_vehicle_details

    vehicle = fetch_vehicle_details(reg_number)

    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")

    return vehicle
# Scheduler Health Check and Monitoring
@app.get("/health/scheduler")
async def scheduler_health_check():
    """
    Health check endpoint for scheduler status and job registration.
    Useful for monitoring and debugging Windows Task Scheduler issues.
    """
    try:
        from services.scheduler_service import get_scheduler_status
        from services.gold_rate_service import verify_database_health
        
        # Get scheduler status
        scheduler_status = get_scheduler_status()
        
        # Verify database health
        db_healthy = verify_database_health(str(DB_PATH))
        
        # Try to create a test scheduler to verify functionality
        test_scheduler = None
        scheduler_creation_success = False
        registered_jobs = []
        
        try:
            from services.scheduler_service import create_scheduler
            
            # Temporarily clear environment flag for testing
            import os
            original_flag = os.environ.get("SV_FINCLOUD_SCHEDULER_RUNNING")
            if original_flag:
                del os.environ["SV_FINCLOUD_SCHEDULER_RUNNING"]
            
            test_scheduler = create_scheduler(str(DB_PATH))
            
            if test_scheduler:
                scheduler_creation_success = True
                jobs = test_scheduler.get_jobs()
                
                registered_jobs = []

                for job in jobs:
                    next_run = None
                    if hasattr(job, "next_run_time") and job.next_run_time:
                        next_run = str(job.next_run_time)

                    registered_jobs.append({
                        "id": job.id,
                        "name": job.name,
                        "next_run": next_run
                    })                
                # Clean up test scheduler
                test_scheduler.shutdown(wait=False)
            
            # Restore original flag
            if original_flag:
                os.environ["SV_FINCLOUD_SCHEDULER_RUNNING"] = original_flag
                
        except Exception as e:
            logging.error(f"Scheduler test creation failed: {e}")
        
        health_status = {
            "status": "healthy" if (scheduler_creation_success and db_healthy) else "unhealthy",
            "timestamp": get_ist_now().isoformat(),
            "scheduler": {
                "creation_success": scheduler_creation_success,
                "environment_status": scheduler_status,
                "registered_jobs": registered_jobs,
                "expected_jobs": ["daily_repo_rate", "daily_gold_rate"]
            },
            "database": {
                "healthy": db_healthy,
                "path": str(DB_PATH)
            }
        }
        
        return health_status
        
    except Exception as e:
        logging.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "timestamp": get_ist_now().isoformat(),
            "error": str(e)
        }
@app.get("/debug/run-gold-job")
async def run_gold_job():
    from services.gold_rate_service import daily_gold_rate_job

    daily_gold_rate_job(str(DB_PATH))

    return {"message": "Gold rate job executed"}

@api_router.get("/customer/interest-rates")
async def get_customer_interest_rates(
    token_data: dict = Depends(require_role(['customer']))
):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT loan_type, category, rate,
                   COALESCE(source, 'auto') AS source,
                   COALESCE(is_overridden, 0) AS is_overridden
            FROM interest_rates
            WHERE tenant_id = %s
            ORDER BY loan_type, category
        """, (token_data["tenant_id"],))
        return [dict(row) for row in cursor.fetchall()]

# Include router
app.include_router(api_router)

# ── Bare health check — must be registered before the catch-all ──────────────
# Railway healthcheck hits this before the DB is necessarily ready.
@app.get("/api/health", include_in_schema=False)
async def _health():
    return {"status": "ok"}

# ── Serve React frontend ──────────────────────────────────────────────────────
# In the Docker container, frontend/build is copied into /app/frontend/build
_FRONTEND_BUILD = ROOT_DIR / "frontend" / "build"
if not _FRONTEND_BUILD.is_dir():
    _FRONTEND_BUILD = ROOT_DIR.parent / "frontend" / "build"

if _FRONTEND_BUILD.is_dir():
    # Mount /static sub-folder (JS/CSS chunks)
    _static_dir = _FRONTEND_BUILD / "static"
    if _static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

    @app.get("/", include_in_schema=False)
    async def serve_root():
        return FileResponse(str(_FRONTEND_BUILD / "index.html"))

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_react(full_path: str):
        """Serve static files from build root if they exist, else fall back to index.html."""
        candidate = _FRONTEND_BUILD / full_path
        if candidate.is_file():
            return FileResponse(str(candidate))
        return FileResponse(str(_FRONTEND_BUILD / "index.html"))

