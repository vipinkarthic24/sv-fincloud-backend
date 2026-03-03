from fastapi import FastAPI, APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
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
import sqlite3
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
from datetime import datetime
from reportlab.lib.units import inch
from reportlab.platypus import HRFlowable
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



ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# JWT Configuration
SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'sv-fincloud-secret-key-2025')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 480

# Database path
DB_PATH = ROOT_DIR / 'sv_fincloud.db'

security = HTTPBearer()

# Database connection helper
@contextmanager
def get_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# Initialize database
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
                UNIQUE(username,tenant_id)
            )
        ''')
        
        # Customers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                address TEXT,
                cibil_score INTEGER,
                monthly_income REAL,
                tenant_id TEXT,
                branch_id TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
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
                updated_at TEXT NOT NULL
            )
        ''')

        # Gold rate settings table (Auto / Manual toggle)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gold_rate_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                mode TEXT DEFAULT 'manual'
            )
        ''')

        # Insert default mode (manual)
        cursor.execute("""
            INSERT OR IGNORE INTO gold_rate_settings (id, mode)
            VALUES (1, 'manual')
        """)
        
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
                gold_weight REAL,
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
                created_at TEXT NOT NULL
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

    
        try:
            cursor.execute("ALTER TABLE customers ADD COLUMN risk_score INTEGER DEFAULT 50")
        except sqlite3.OperationalError:
            pass  

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
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")
            except sqlite3.OperationalError:
                pass
        
        # Add balance_after_payment column if not exists
        try:
            cursor.execute(
                "ALTER TABLE payments ADD COLUMN balance_after_payment REAL"
            )
        except sqlite3.OperationalError:
            pass
        # Add remaining_emi_after_payment column if not exists
        try:
            cursor.execute(
                "ALTER TABLE payments ADD COLUMN remaining_emi_after_payment INTEGER"
            )
        except sqlite3.OperationalError:
            pass  
        try:
            cursor.execute("ALTER TABLE payments ADD COLUMN receipt_no TEXT")
        except sqlite3.OperationalError:
            pass

        try:
            cursor.execute("ALTER TABLE payments ADD COLUMN receipt_seq INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("ALTER TABLE audit_logs ADD COLUMN tenant_id TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("DELETE FROM interest_rates WHERE category LIKE '% %'")
        except sqlite3.OperationalError:
            pass  
        try:
            cursor.execute("ALTER TABLE tenants ADD COLUMN logo_url TEXT")
        except:
            pass

        try:
            cursor.execute("ALTER TABLE tenants ADD COLUMN primary_color TEXT DEFAULT '#2536eb'")
        except:
            pass

        try:
            cursor.execute("ALTER TABLE tenants ADD COLUMN tagline TEXT")
        except:
            pass

        try:
            cursor.execute("ALTER TABLE tenants ADD COLUMN stats_json TEXT")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE gold_rate ADD COLUMN branch_id TEXT")
        except sqlite3.OperationalError:
            pass

        # Hybrid override flag for interest rates
        try:
            cursor.execute("ALTER TABLE interest_rates ADD COLUMN is_overridden INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass

        # ===== VIEW-BASED ACCESS TABLES =====

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS roles (
                role_id INTEGER PRIMARY KEY AUTOINCREMENT,
                role_name TEXT UNIQUE NOT NULL
        )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS modules (
                module_id INTEGER PRIMARY KEY AUTOINCREMENT,
                module_name TEXT UNIQUE NOT NULL
        )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS permissions (
                permission_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        try:
            cursor.execute("""
                CREATE TABLE permissions_new (
                    permission_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            cursor.execute("""
                INSERT INTO permissions_new (role_id, module_id, can_view, can_insert, can_update, can_delete)
                SELECT role_id, module_id,
                       MAX(can_view), MAX(can_insert), MAX(can_update), MAX(can_delete)
                FROM permissions
                GROUP BY role_id, module_id
            """)
            cursor.execute("DROP TABLE permissions")
            cursor.execute("ALTER TABLE permissions_new RENAME TO permissions")
        except sqlite3.OperationalError:
            pass  # Already migrated
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_tenant_branch ON loans(tenant_id, branch_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_loan ON payments(loan_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_emi_loan ON emi_schedule(loan_id)")

        # Repo Rate History table (for dynamic lending engine)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS repo_rate_history (
                id TEXT PRIMARY KEY,
                repo_rate REAL NOT NULL,
                fetched_at TEXT NOT NULL
            )
        ''')

        conn.commit()
        
        # Create sample users
        # create_sample_data(conn)

def get_branch_filter(token_data, table_alias=""):
    role = token_data["role"]

    # Company-level roles → ALL branches
    if role in ["super_admin", "auditor"]:
        return "", []

    # Branch-level roles → restrict
    column = f"{table_alias}.branch_id" if table_alias else "branch_id"
    return f" AND {column} = ?", [token_data["branch_id"]]

def init_permissions():
    with get_db() as conn:
        cursor = conn.cursor()

        # ROLES
        cursor.execute("""
            INSERT OR IGNORE INTO roles (role_name) VALUES
            ('super_admin'),
            ('admin'),
            ('finance_officer'),
            ('collection_agent'),
            ('auditor'),
            ('customer')
        """)

        # MODULES
        cursor.execute("""
            INSERT OR IGNORE INTO modules (module_name) VALUES
            ('users'),
            ('customers'),
            ('loans'),
            ('payments'),
            ('emi_schedule'),
            ('reports'),
            ('audit_logs'),
            ('branches')
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
    cursor.execute(
        "INSERT INTO tenants (id, name, created_at) VALUES (?, ?, ?)",
        (tenant_id, "SV Fincloud", datetime.now(timezone.utc).isoformat())
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
    cursor.execute(
            "INSERT INTO branches (id, name, location, tenant_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (branch_id, 'SV Fincloud Main Branch', 'Chennai', tenant_id, datetime.now(timezone.utc).isoformat())
    )
    for username, password, role in users_data:
        user_id = str(uuid.uuid4())
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

        branch_for_user = None if role in ["super_admin", "auditor"] else branch_id

        cursor.execute(
            """
            INSERT INTO users (id, username, password, role, tenant_id, branch_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                username,
                hashed.decode('utf-8'),
                role,
                tenant_id,
                branch_for_user,
                datetime.now(timezone.utc).isoformat()
            )
        )
        
        # Create customer profile for customer user
        if role == 'customer':
            cursor.execute(
                "INSERT INTO customers (id, user_id, name, email, phone, cibil_score, branch_id, tenant_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (str(uuid.uuid4()),user_id,'Demo Customer','customer@svfincloud.com','9876543210',780,branch_id,tenant_id,datetime.now(timezone.utc).isoformat())
)
    
    # Create loan types
    loan_types = [
        ('personal_loan', 'Personal Loan'),
        ('vehicle_loan', 'Vehicle Loan'),
        ('gold_loan', 'Gold Loan')
    ]
    
    for lt_id, lt_name in loan_types:
        cursor.execute(
            "INSERT INTO loan_types (id, name, description, created_at) VALUES (?, ?, ?, ?)",
            (lt_id, lt_name, f'{lt_name} for customers', datetime.now(timezone.utc).isoformat())
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
        cursor.execute(
            "INSERT INTO interest_rates (id, loan_type, category, rate, created_at) VALUES (?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), loan_type, category, rate, datetime.now(timezone.utc).isoformat())
        )
    
    # Set default gold rate
    cursor.execute(
        "INSERT INTO gold_rate (id, rate_per_gram, updated_at) VALUES (?, ?, ?)",
        (str(uuid.uuid4()), 6500.0, datetime.now(timezone.utc).isoformat())
    )

    conn.commit()

init_db()
init_permissions()

def fix_null_branches():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT id, tenant_id FROM branches LIMIT 1")
        default_branch = cursor.fetchone()

        if not default_branch:
            return

        branch_id = default_branch["id"]
        tenant_id = default_branch["tenant_id"]

        cursor.execute(
            """
            UPDATE users
            SET branch_id = ?
            WHERE branch_id IS NULL
            AND tenant_id = ?
            AND role NOT IN ('super_admin','auditor')
            """,
            (branch_id, tenant_id)
        )

        cursor.execute(
            "UPDATE customers SET branch_id = ? WHERE branch_id IS NULL AND tenant_id = ?",
            (branch_id, tenant_id)
        )

        cursor.execute(
            "UPDATE loans SET branch_id = ? WHERE branch_id IS NULL AND tenant_id = ?",
            (branch_id, tenant_id)
        )

        cursor.execute(
            "UPDATE payments SET branch_id = ? WHERE branch_id IS NULL AND tenant_id = ?",
            (branch_id, tenant_id)
        )

        cursor.execute(
            "UPDATE emi_schedule SET branch_id = ? WHERE branch_id IS NULL AND tenant_id = ?",
            (branch_id, tenant_id)
        )

        conn.commit()
fix_null_branches()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("SV Fincloud Server is starting up...")

    # --- Unified Scheduler: Repo Rate + Gold Rate ---
    scheduler = None
    try:
        from services.scheduler_service import create_scheduler, shutdown_scheduler

        scheduler = create_scheduler(str(DB_PATH))
        if scheduler:
            scheduler.start()
            print("APScheduler started — repo rate (06:00) + gold rate (06:05)")

        # Seed initial repo rate if table is empty
        if get_latest_repo_rate(str(DB_PATH)) is None:
            initial_rate = fetch_repo_rate()
            save_repo_rate(str(DB_PATH), initial_rate)
            print(f"Seeded initial repo rate: {initial_rate}%")

        # Seed interest rates if table is empty (first-run only)
        try:
            conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM interest_rates")
            count = cursor.fetchone()[0]
            conn.close()

            if count == 0:
                repo_data = get_latest_repo_rate(str(DB_PATH))
                repo_rate = repo_data["repo_rate"] if repo_data else DEFAULT_REPO_RATE
                recalculate_interest_rates(str(DB_PATH), repo_rate)
                print(f"Interest rates seeded with repo rate {repo_rate}%")
            else:
                print(f"Interest rates already initialized ({count} records)")
        except Exception as e:
            print(f"WARNING: Interest rate seeding failed: {e}")

    except Exception as e:
        print(f"WARNING: Scheduler failed to start: {e}")

    yield

    # Graceful shutdown
    if scheduler:
        from services.scheduler_service import shutdown_scheduler
        shutdown_scheduler(scheduler)
    print("SV Fincloud Server is shutting down safely...")

app = FastAPI(lifespan=lifespan)
api_router = APIRouter(prefix="/api")


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
    gold_weight: Optional[float] = None

class PaymentRequest(BaseModel):
    emi_id: str
    amount: float

class ApprovalRequest(BaseModel):
    entity_id: str
    action: str

class UserCreateRequest(BaseModel):
    username: str
    password: str
    role: str
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    cibil_score: Optional[int] = None
    branch_id: str
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
    branch_id: str
    rate_per_gram: float

class RepoRateUpdateRequest(BaseModel):
    repo_rate: float

class GoldRateModeRequest(BaseModel):
    mode: str

# Helper functions
def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    

def require_role(allowed_roles: List[str]):
    def role_checker(token_data: dict = Depends(verify_token)):
        if token_data.get('role') not in allowed_roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
        return token_data
    return role_checker

def check_permission(role, module, action):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT p.*
            FROM permissions p
            JOIN roles r ON p.role_id = r.role_id
            JOIN modules m ON p.module_id = m.module_id
            WHERE r.role_name = ? AND m.module_name = ?
        """, (role, module))
        
        perm = cursor.fetchone()

        if not perm:
            return False

        return perm[f'can_{action}'] == 1

def log_audit(conn, user_id: str, tenant_id: str, action: str, entity_type: str, entity_id: str = None, details: str = None):
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO audit_logs 
        (id, user_id, tenant_id, action, entity_type, entity_id, details, created_at) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            str(uuid.uuid4()),
            user_id,
            tenant_id,
            action,
            entity_type,
            entity_id,
            details,
            datetime.now(timezone.utc).isoformat()
        )
    )
def apply_penalty_if_overdue(conn,branch_id,tenant_id):
    cursor = conn.cursor()
    today = datetime.now().date()

    # Fetch all pending EMIs
    cursor.execute("""
        SELECT id, loan_id, emi_amount, due_date
        FROM emi_schedule
        WHERE status = 'pending'
        AND branch_id = ?
        AND tenant_id = ?
    """, (branch_id, tenant_id))
    emis = cursor.fetchall()

    for emi in emis:
        due_date = datetime.fromisoformat(emi['due_date']).date()

        # Check if EMI is overdue
        if today > due_date:
            # Check if penalty already applied for this EMI
            cursor.execute(
                "SELECT 1 FROM penalties WHERE emi_id = ?",
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
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                str(uuid.uuid4()),
                emi['loan_id'],
                emi['id'],
                penalty_amount,
                "Overdue EMI Penalty (2%)",
                datetime.now(timezone.utc).isoformat()
            ))

            # OPTIONAL: Update EMI table penalty column (for quick UI display)
            cursor.execute("""
                UPDATE emi_schedule
                SET penalty = ?
                WHERE id = ?
            """, (penalty_amount, emi['id']))
def generate_company_prefix(company_name: str):
    words = company_name.strip().split()

    first_word = words[0].lower()

    # If first word is already short like SV, JSR → use it
    if len(first_word) <= 4:
        return first_word

    # Else take first letters of words
    return "".join(word[0] for word in words).lower()

# Authentication Routes
@api_router.post("/auth/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    with get_db() as conn:
        cursor = conn.cursor()

        # 1️⃣ FIND USER
        cursor.execute("""
            SELECT * FROM users
            WHERE username = ? AND tenant_id = ?
        """, (request.username, request.tenant_id))

        user = cursor.fetchone()

        # ❌ IF USER NOT FOUND → STOP HERE
        if not user:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        # 2️⃣ CHECK PASSWORD
        if not bcrypt.checkpw(request.password.encode('utf-8'), user['password'].encode('utf-8')):
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
        cursor.execute(
            "SELECT name, logo_url, primary_color FROM tenants WHERE id = ?",
            (user['tenant_id'],)
        )
        tenant = cursor.fetchone()

        # 5️⃣ ⭐ GET BRANCH INFO (ADD HERE ONLY)
        branch = None
        if user['branch_id']:
            cursor.execute(
                "SELECT name FROM branches WHERE id = ?",
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
        cursor.execute("SELECT id, username, role, tenant_id FROM users WHERE id = ?", (token_data['user_id'],))
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
        cursor.execute(
            "SELECT tenant_id FROM branches WHERE id = ?",
            (request.branch_id,)
        )
        # 2. Get tenant_id from selected branch ✅
        tenant_row = cursor.fetchone()

        if not tenant_row:
            raise HTTPException(status_code=400, detail="Invalid branch")

        tenant_id = tenant_row["tenant_id"]
        # NOW check username
        cursor.execute(
            "SELECT id FROM users WHERE username = ? AND tenant_id = ?",
            (request.username, tenant_id)
        )
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Username already taken")

        # 3. Create user record
        user_id = str(uuid.uuid4())
        hashed = bcrypt.hashpw(request.password.encode('utf-8'), bcrypt.gensalt())
        
        cursor.execute(
            "INSERT INTO users (id, username, password, role, tenant_id, branch_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, request.username, hashed.decode('utf-8'), 'customer', tenant_id, request.branch_id, datetime.now(timezone.utc).isoformat())
        )

        # 4. Create customer profile
        cursor.execute(
            "INSERT INTO customers (id, user_id, name, email, phone, cibil_score, branch_id, tenant_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), user_id, request.name, request.email, request.phone, request.cibil_score, request.branch_id, tenant_id, datetime.now(timezone.utc).isoformat())
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
            VALUES (?, ?, ?)
        """, (
            tenant_id,
            request.company_name,
            datetime.now(timezone.utc).isoformat()
        ))

        # 🏬 Create Main Branch
        branch_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO branches (id, name, location, tenant_id, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            branch_id,
            f"{request.company_name} Main Branch",
            request.location,
            tenant_id,
            datetime.now(timezone.utc).isoformat()
        ))
        # Check username inside this company
        cursor.execute("""
            SELECT id FROM users
            WHERE username = ? AND tenant_id = ?
        """, (request.username,tenant_id))

        if cursor.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Username already exists in this company"
            )
        # 👑 CREATE SUPER ADMIN (NO BRANCH)
        super_admin_id = str(uuid.uuid4())
        hashed = bcrypt.hashpw(request.password.encode(), bcrypt.gensalt())

        cursor.execute("""
            INSERT INTO users (
                id, username, password, role,
                tenant_id, branch_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            super_admin_id,
            request.username,
            hashed.decode(),
            "super_admin",
            tenant_id,
            None,  # ❗ No branch
            datetime.now(timezone.utc).isoformat()
        ))

        # 🕵️ CREATE AUDITOR (ONE PER COMPANY)
       
        cursor.execute("""
            SELECT id FROM users
            WHERE role = 'auditor' AND tenant_id = ?
        """, (tenant_id,))

        if cursor.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Auditor already exists for this company"
            )
        auditor_id = str(uuid.uuid4())

        prefix = generate_company_prefix(request.company_name)

        auditor_username = f"{prefix}_auditor"

        auditor_pass = bcrypt.hashpw("auditor123".encode(), bcrypt.gensalt())
        cursor.execute("""
            INSERT INTO users (
                id, username, password, role,
                tenant_id, branch_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            auditor_id,
            auditor_username,
            auditor_pass.decode(),
            "auditor",
            tenant_id,
            None,
            datetime.now(timezone.utc).isoformat()
        ))

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
        return [dict(row) for row in cursor.fetchall()]

    
@api_router.get("/auth/public-branches/{tenant_id}")
async def get_public_branches(tenant_id: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM branches WHERE tenant_id = ?", (tenant_id,))
        return [dict(row) for row in cursor.fetchall()]
    
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
        cursor.execute(
            """
            SELECT id, cibil_score 
            FROM customers 
            WHERE user_id = ? 
            AND tenant_id = ?
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
        rate_source = "manual"  # Track whether rate is manual or dynamic

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

            cursor.execute(
                "SELECT rate FROM interest_rates WHERE loan_type = ? AND category = ?",
                ("personal_loan", category)
            )

        # ================= VEHICLE LOAN =================
        elif request.loan_type == "vehicle_loan":
            if request.vehicle_age is None:
                raise HTTPException(status_code=400, detail="Vehicle age required")

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

            cursor.execute(
                "SELECT rate FROM interest_rates WHERE loan_type = ? AND category = ?",
                ("vehicle_loan", category)
            )

        # ================= GOLD LOAN =================
        elif request.loan_type == "gold_loan":
            if not request.gold_weight:
                raise HTTPException(status_code=400, detail="Gold weight required")

            # 🟢 CHECK MODE FIRST
            cursor.execute("SELECT mode FROM gold_rate_settings WHERE id = 1")
            mode = cursor.fetchone()["mode"]

            # 🟢 AUTO MODE → USE MARKET RATE
            if mode == "auto":
                gold_rate = 7250.0   # simulate market API

            # 🔴 MANUAL MODE → USE DATABASE
            else:
                cursor.execute("""
                    SELECT rate_per_gram
                    FROM gold_rate
                    WHERE branch_id = ? OR branch_id IS NULL
                    ORDER BY (branch_id = ?) DESC, updated_at DESC,rowid DESC
                    LIMIT 1
                """, (branch_id, branch_id))

                row = cursor.fetchone()
                gold_rate = row["rate_per_gram"] if row else 6500.0

            max_loan = request.gold_weight * gold_rate * 0.75  # 75% LTV per new spec

            if request.amount > max_loan:
                raise HTTPException(
                    status_code=400,
                    detail=f"Loan amount exceeds 75% of gold value. Max: ₹{round(max_loan,2)}"
                )

            cursor.execute(
                "SELECT rate FROM interest_rates WHERE loan_type = ? AND category = ?",
                ("gold_loan", "standard")
            )

        else:
            raise HTTPException(status_code=400, detail="Invalid loan type")

        # ═══════════════════════════════════════════════
        # 4️⃣ INTEREST RATE: Manual Override → Dynamic Fallback
        # ═══════════════════════════════════════════════
        rate_row = cursor.fetchone()

        if rate_row and rate_row["rate"]:
            # Manual rate exists in interest_rates table → use it
            interest_rate = rate_row["rate"]
            rate_source = "manual"
        else:
            # No manual rate → calculate dynamically
            repo_data = get_latest_repo_rate(str(DB_PATH))
            current_repo = repo_data["repo_rate"] if repo_data else 6.5

            dynamic_result = calculate_interest_rate(
                request.loan_type, risk_inputs, current_repo
            )
            interest_rate = dynamic_result["rate"]
            rate_source = "dynamic"

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
        cursor.execute("""UPDATE customers SET monthly_income = ? WHERE id = ?""", (request.monthly_income, customer["id"]))

        # 7️⃣ Insert loan
        loan_id = str(uuid.uuid4())
        cursor.execute(
            """
            INSERT INTO loans (
                id, customer_id, loan_type, amount, tenure,
                interest_rate, emi_amount, processing_fee,
                disbursed_amount, outstanding_balance,
                status, vehicle_age, gold_weight,
                branch_id, tenant_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                loan_id, customer["id"], request.loan_type, request.amount, request.tenure,
                interest_rate, emi_amount, processing_fee, disbursed_amount,
                request.amount, loan_status, request.vehicle_age, request.gold_weight,
                branch_id, tenant_id, datetime.now(timezone.utc).isoformat()
            )
        )
        # 8️⃣ Audit
        log_audit(conn, token_data["user_id"], token_data['tenant_id'], "LOAN_APPLICATION", "loan", loan_id,
                  json.dumps({"status": loan_status, "rate_source": rate_source, "interest_rate": interest_rate}))

        return {
            "status": loan_status,
            "message": f"Loan {loan_status} successfully",
            "loan_id": loan_id,
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
        
        cursor.execute("SELECT id FROM customers WHERE user_id = ?", (token_data['user_id'],))
        customer = cursor.fetchone()
        
        if not customer:
            return []
        branch_filter, params = get_branch_filter(token_data)

        query = f"""
        SELECT * FROM loans
        WHERE customer_id = ?
        AND tenant_id = ?
        {branch_filter}
        ORDER BY created_at DESC
        """

        cursor.execute(query, (customer['id'], token_data["tenant_id"], *params))
        loans = [dict(row) for row in cursor.fetchall()]
        
        return loans

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
        WHERE e.loan_id = ?
        AND e.tenant_id = ?
        {branch_filter}
        ORDER BY e.emi_number
        """

        cursor.execute(query, (loan_id, token_data["tenant_id"], *params))

        schedule = [dict(row) for row in cursor.fetchall()]
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
        WHERE p.loan_id = ?
        AND l.tenant_id = ?
        {branch_filter}
        ORDER BY p.created_at DESC
        """

        cursor.execute(query, (loan_id, token_data["tenant_id"], *params))
        payments = [dict(row) for row in cursor.fetchall()]
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
            WHERE l.id = ?
            AND c.user_id = ?
            AND l.branch_id = ?
            AND l.tenant_id = ?
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
        cursor.execute("DELETE FROM payments WHERE loan_id = ?", (loan_id,))
        cursor.execute("DELETE FROM emi_schedule WHERE loan_id = ?", (loan_id,))
        cursor.execute("DELETE FROM loans WHERE id = ?", (loan_id,))
        
        # 3. Log the action
        log_audit(conn, token_data['user_id'], token_data['tenant_id'],'LOAN_DELETED', 'loan', loan_id)
        
        return {"message": "Loan application deleted successfully"}
    
@api_router.get("/customer/receipt/{emi_id}")
async def get_emi_receipt(
    emi_id: str,
    token_data: dict = Depends(require_role(['customer']))
):
    if not check_permission(token_data['role'], 'payments', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()

        # 1. Verify EMI ownership
        cursor.execute("""
            SELECT 
                e.id AS emi_id,
                e.emi_number,
                l.id AS loan_id
            FROM emi_schedule e
            JOIN loans l ON e.loan_id = l.id
            JOIN customers c ON l.customer_id = c.id
            WHERE e.id = ?
            AND c.user_id = ?
            AND l.branch_id = ?
            AND l.tenant_id = ?
        """, (emi_id, token_data['user_id'], token_data["branch_id"], token_data["tenant_id"]))
        emi = cursor.fetchone()

        if not emi:
            raise HTTPException(status_code=404, detail="EMI not found")

        # 2. Fetch APPROVED PAYMENT SNAPSHOT (THIS EMI ONLY)
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
            l.id AS loan_id
        FROM payments p
        JOIN emi_schedule e ON p.emi_id = e.id
        JOIN loans l ON p.loan_id = l.id
        JOIN customers c ON l.customer_id = c.id
        WHERE p.emi_id = ?
        AND c.user_id = ?
        AND p.status = 'approved'
        AND l.tenant_id = ?
        {branch_filter}
        ORDER BY p.created_at DESC
        LIMIT 1
        """

        cursor.execute(query, (emi_id, token_data['user_id'], token_data["tenant_id"], *params))
        payment = cursor.fetchone()

        if not payment:
            raise HTTPException(status_code=400, detail="Receipt not available")
        # 🔥 Get tenant name for receipt
        cursor.execute(
            "SELECT name FROM tenants WHERE id = ?",
            (token_data["tenant_id"],)
        )
        tenant = cursor.fetchone()
        tenant_name = tenant["name"] if tenant else "SV Fincloud"

        # ✅ RETURN SNAPSHOT RECEIPT (NO LIVE CALCULATION)
        return {
            "company_name": tenant_name,
            "receipt_no": payment['receipt_no'],
            "transaction_ref": f"TXN-{payment['payment_id'][:10].upper()}",
            "loan_id": emi['loan_id'],
            "emi_number": emi['emi_number'],   
            "amount_paid": payment['amount'],
            "payment_date": payment['payment_date'],
            "remaining_emi_count": payment['remaining_emi_after_payment'],
            "remaining_amount": payment['balance_after_payment'],
            "status": "SUCCESSFUL"
        }



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
            c.*, 
            l.id AS loan_id, 
            l.loan_type, 
            l.amount, 
            l.outstanding_balance, 
            l.status
        FROM customers c
        INNER JOIN loans l ON c.id = l.customer_id
        WHERE l.status = 'active'
        AND l.tenant_id = ?
        {branch_filter}
        ORDER BY c.name
        """

        cursor.execute(query, (token_data["tenant_id"], *params))
        return [dict(row) for row in cursor.fetchall()]

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
        WHERE e.id = ?
        AND l.tenant_id = ?
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

        payment_id = str(uuid.uuid4())

        # 2️⃣ ATOMIC INSERT (DB WILL BLOCK DUPLICATES)
        try:
            cursor.execute("""
                INSERT INTO payments (
                    id, loan_id, emi_id, amount,
                    status, collected_by, branch_id, tenant_id,
                    created_at, payment_date
                )
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                    payment_id,
                    emi["loan_id"],
                    request.emi_id,
                    request.amount,
                    token_data["user_id"],
                    branch_id,
                    tenant_id    
            ))

            # 3️⃣ LOCK EMI AFTER INSERT
            cursor.execute("""
                UPDATE emi_schedule
                SET status = 'pending_payment'
                WHERE id = ?
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
        SELECT l.*, c.name as customer_name, c.email, c.phone,
            c.cibil_score, c.monthly_income
        FROM loans l
        INNER JOIN customers c ON l.customer_id = c.id
        WHERE l.status IN ('pending', 'pre-approved', 'submitted', 'applied')
        AND l.tenant_id = ?
        {branch_filter}
        ORDER BY l.created_at DESC
        """

        cursor.execute(query, (token_data["tenant_id"], *params))

        applications = [dict(row) for row in cursor.fetchall()]
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
        WHERE id = ?
        AND tenant_id = ?
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
            SET interest_rate = ?
            WHERE id = ?
            AND tenant_id = ?
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
    
    with get_db() as conn:
        cursor = conn.cursor()
        
        if action == 'approve':
            try:
                # 1. Activate loan
                branch_filter, params = get_branch_filter(token_data)

                query = f"""
                UPDATE loans
                SET status = 'active', approved_at = CURRENT_TIMESTAMP,
                    outstanding_balance = amount
                WHERE id = ?
                AND tenant_id = ?
                {branch_filter}
                """

                cursor.execute(query, (loan_id, token_data["tenant_id"], *params))
                # 2. Fetch loan
                branch_filter, params = get_branch_filter(token_data)

                query = f"""
                SELECT *
                FROM loans
                WHERE id = ?
                AND tenant_id = ?
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
                start_date = datetime.now()
        
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
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
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
                return {"message": "Loan approved and EMI schedule created"}

            
            except Exception as e:
                conn.rollback() 
                print(f"APPROVE ERROR: {e}")
                raise HTTPException(status_code=500, detail=f"Database Error: {str(e)}")
        
        else:
            branch_filter, params = get_branch_filter(token_data)

            query = f"""
            UPDATE loans
            SET status = 'rejected'
            WHERE id = ?
            AND tenant_id = ?
            {branch_filter}
            """

            cursor.execute(query, (loan_id, token_data["tenant_id"], *params))
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
        WHERE p.id = ?
        AND p.tenant_id = ?
        {branch_filter}
        """

        cursor.execute(query, (request.entity_id, tenant_id, *params))
        payment = cursor.fetchone()
        
        if not payment:
            raise HTTPException(status_code=404, detail="Payment record not found")
        
        if request.action == 'approve':
            # 2. Approve payment
            cursor.execute(
                """
                UPDATE payments 
                SET status = 'approved', approved_by = ?, approved_at = ?
                WHERE id = ? AND branch_id=? AND tenant_id=?
                """,
                (token_data['user_id'], datetime.now(timezone.utc).isoformat(), request.entity_id,branch_id, tenant_id)
            )
            
            # 3. Mark EMI as paid
            branch_filter, params = get_branch_filter(token_data, "e")

            query = f"""
            UPDATE emi_schedule e
            SET status = 'paid', paid_at = ?
            WHERE e.id = ?
            AND e.tenant_id = ?
            {branch_filter}
            """

            cursor.execute(
                query,
                (datetime.now(timezone.utc).isoformat(), payment['emi_id'], tenant_id, *params)
            )
            
            # 4. Get principal amount of this EMI
            cursor.execute(
                "SELECT principal_amount FROM emi_schedule WHERE id = ? AND branch_id=? AND tenant_id=?",
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
            WHERE id = ?
            AND tenant_id = ?
            {branch_filter}
            """

            cursor.execute(query, (payment['loan_id'], tenant_id, *params))

            current_balance = cursor.fetchone()['outstanding_balance']

            # 6. Calculate new outstanding balance
            new_balance = max(0, current_balance - principal_paid)
            branch_filter, params = get_branch_filter(token_data)

            query = f"""
            UPDATE loans
            SET outstanding_balance = ?
            WHERE id = ?
            AND tenant_id = ?
            {branch_filter}
            """

            cursor.execute(
                query,
                (new_balance, payment['loan_id'], tenant_id, *params)
            )
            
            year = datetime.now().year

            cursor.execute("""
                INSERT OR IGNORE INTO payments_receipt_counter(branch_id, tenant_id, last_seq)
                VALUES (?, ?, 1)
                ON CONFLICT(branch_id, tenant_id)
                DO UPDATE SET last_seq = last_seq + 1
                RETURNING last_seq
            """, (branch_id, tenant_id))

            next_seq = cursor.fetchone()['last_seq']

            # 🔥 GET TENANT NAME
            cursor.execute(
                "SELECT name FROM tenants WHERE id = ?",
                (tenant_id,)
            )
            tenant = cursor.fetchone()
            tenant_name = tenant["name"] if tenant else "SV Fincloud"

            # 🔥 CREATE PREFIX
            prefix = ''.join(word[0] for word in tenant_name.split()[:3]).upper()

            # 🔥 FINAL RECEIPT NUMBER
            receipt_no = f"{prefix}-REC-{year}-{str(next_seq).zfill(6)}"

            # ✅ 8. Count remaining EMIs AFTER this payment
            cursor.execute(
                """
                SELECT COUNT(*) AS remaining
                FROM emi_schedule
                WHERE loan_id = ?
                  AND status = 'pending' AND branch_id=? AND tenant_id=?
                """,
                (payment['loan_id'],branch_id, tenant_id))
            
            remaining_emi = cursor.fetchone()['remaining']

            # ✅ 9. Store SNAPSHOT + RECEIPT DETAILS
            cursor.execute(
                """
                UPDATE payments
                SET 
                    balance_after_payment = ?,
                    remaining_emi_after_payment = ?,
                    receipt_no = ?,
                    receipt_seq = ?
                WHERE id = ? AND branch_id=? AND tenant_id=?
                """,
                (new_balance,remaining_emi,receipt_no,next_seq,request.entity_id,branch_id, tenant_id)
            )

            # 10. Auto-close loan if fully paid
            branch_filter, params = get_branch_filter(token_data)

            query = f"""
            UPDATE loans
            SET status = 'closed'
            WHERE id = ?
            AND outstanding_balance <= 0
            AND tenant_id = ?
            {branch_filter}
            """

            cursor.execute(query, (payment['loan_id'], tenant_id, *params))

            conn.commit()
            return {"message": "Payment approved and balance updated"}
        
@api_router.get("/officer/analytics-summary")
async def get_analytics(token_data: dict = Depends(require_role(['finance_officer']))):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        branch_id = token_data["branch_id"]
        tenant_id = token_data["tenant_id"]
        apply_penalty_if_overdue(conn,branch_id,tenant_id)
        cursor = conn.cursor()

        # Total Collected (Approved Payments)
        branch_filter, params = get_branch_filter(token_data)

        query = f"""
        SELECT SUM(amount) as total
        FROM payments
        WHERE status='approved'
        AND tenant_id = ?
        {branch_filter}
        """

        cursor.execute(query, (token_data["tenant_id"], *params))
        collected = cursor.fetchone()['total'] or 0
        # Total Pending (EMIs due but not paid)
        branch_filter, params = get_branch_filter(token_data, "l")

        query = f"""
        SELECT SUM(e.emi_amount + COALESCE(e.penalty,0)) as total
        FROM emi_schedule e
        JOIN loans l ON e.loan_id = l.id
        WHERE e.status = 'pending'
        AND l.tenant_id = ?
        {branch_filter}
        """

        cursor.execute(query, (tenant_id, *params))
        pending = cursor.fetchone()['total'] or 0
        
        branch_filter, params = get_branch_filter(token_data)

        query = f"""
        SELECT COUNT(*) as count
        FROM loans
        WHERE status = 'active'
        AND tenant_id = ?
        {branch_filter}
        """

        cursor.execute(query, (tenant_id, *params))
        active_loans = cursor.fetchone()['count'] or 0
        # Collection Efficiency %
        efficiency = (collected / (collected + pending) * 100) if (collected + pending) > 0 else 0

        return {
            "kpis": {
                "total_collected": collected,
                "total_pending": pending,
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
            l.loan_type, c.name as customer_name,
            COALESCE(e.emi_number, 'Manual') as emi_number
        FROM payments p
        INNER JOIN loans l ON p.loan_id = l.id
        INNER JOIN customers c ON l.customer_id = c.id
        LEFT JOIN emi_schedule e ON p.emi_id = e.id
        WHERE (p.status = 'pending' OR p.status = 'PENDING')
        AND p.tenant_id = ?
        {branch_filter}
        ORDER BY p.created_at DESC
        """

        cursor.execute(query, (token_data["tenant_id"], *params))
        payments = [dict(row) for row in cursor.fetchall()]
        return payments
    
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
        WHERE tenant_id = ?
        {branch_filter}
        """

        cursor.execute(query, (token_data["tenant_id"], *params))
        loans = cursor.fetchone()

        branch_filter, params = get_branch_filter(token_data)

        query = f"""
        SELECT SUM(amount) as total_collected
        FROM payments
        WHERE status = 'approved'
        AND tenant_id = ?
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

# Admin Routes
@api_router.post("/admin/create-user")
async def create_user(request: UserCreateRequest, token_data: dict = Depends(require_role(['admin','super_admin']))):
    if not check_permission(token_data['role'], 'users', 'insert'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        
        # 1. DEFINE tenant_id FIRST
        # Get it directly from the token_data (which is already provided by your dependency)
        tenant_id = token_data.get("tenant_id")
        
        if not tenant_id:
             # Fallback: if not in token, look it up from DB
             cursor.execute("SELECT tenant_id FROM users WHERE id = ?", (token_data['user_id'],))
             admin = cursor.fetchone()
             tenant_id = admin['tenant_id'] if admin else None

        if not tenant_id:
            raise HTTPException(status_code=401, detail="Could not determine Tenant ID")
        # ===== ROLE + BRANCH VALIDATIONS =====

        # 1️⃣ ADMIN can create users ONLY in their branch
        if token_data["role"] == "admin":
            if request.branch_id != token_data["branch_id"]:
                raise HTTPException(
                    status_code=403,
                    detail="Admin can create users only in their branch"
                )

        # 2️⃣ Only SUPER ADMIN can create Admins
        if request.role.lower() == "admin" and token_data["role"] != "super_admin":
            raise HTTPException(
                status_code=403,
                detail="Only Super Admin can create branch admins"
            )

        # 3️⃣ Only ONE ADMIN per branch
        if request.role.lower() == "admin":
            cursor.execute("""
                SELECT id FROM users 
                WHERE role = 'admin' AND branch_id = ? AND tenant_id = ?
            """, (request.branch_id, tenant_id))
            if cursor.fetchone():
                raise HTTPException(
                    status_code=400,
                    detail="Admin already exists for this branch"
                )
        if request.role.lower() == "auditor" and token_data["role"] != "super_admin":
            raise HTTPException(
                status_code=403,
                detail="Only Super Admin can create Auditor"
            )
        # 4️⃣ Only ONE AUDITOR per company
        if request.role.lower() == "auditor":
            cursor.execute("""
                SELECT id FROM users
                WHERE role = 'auditor' AND tenant_id = ?
            """, (tenant_id,))
            if cursor.fetchone():
                raise HTTPException(
                    status_code=400,
                    detail="Auditor already exists for this company"
                )
        # 2. NOW you can safely use it in queries
        cursor.execute("SELECT id FROM users WHERE username = ? AND tenant_id = ?", (request.username, tenant_id))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Username already exists")
        # 🔒 VALIDATE BRANCH BELONGS TO 
        if request.role.lower() not in ["super_admin", "auditor"]:
            cursor.execute("""
                SELECT id FROM branches
                WHERE id = ? AND tenant_id = ?
            """, (request.branch_id, tenant_id))

            if not cursor.fetchone():
                raise HTTPException(
                    status_code=400,
                    detail="Invalid branch for this company"
                )
        # 3. Create user
        user_id = str(uuid.uuid4())
        hashed = bcrypt.hashpw(request.password.encode('utf-8'), bcrypt.gensalt())
        
        cursor.execute(
            "INSERT INTO users (id, username, password, role, tenant_id, branch_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, request.username, hashed.decode('utf-8'), request.role.lower(), tenant_id, request.branch_id, datetime.now(timezone.utc).isoformat())
        )

        # 4. If customer role, create customer profile
        if request.role.lower() == 'customer' and request.name:
            cursor.execute(
                "INSERT INTO customers (id, user_id, name, email, phone, cibil_score, branch_id, tenant_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    user_id,
                    request.name,
                    request.email,
                    request.phone,
                    request.cibil_score,
                    request.branch_id,
                    tenant_id,
                    datetime.now(timezone.utc).isoformat()
                )
            )
        
        # Fixed log_audit call to include tenant_id as expected by your function definition
        log_audit(conn, token_data['user_id'], tenant_id, 'USER_CREATED', 'user', user_id, 
                  json.dumps({"username": request.username, "role": request.role}))
        
        return {"message": "User created successfully", "user_id": user_id}
    
@api_router.get("/admin/users")
async def get_all_users(
    branch_ids: Optional[str] = None,  # comma separated
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
                u.created_at
            FROM users u
            LEFT JOIN branches b ON u.branch_id = b.id
            WHERE u.tenant_id = ?
        """

        params = [tenant_id]
        if token_data["role"] == "admin":
            branch_ids=None
            base_query += " AND u.branch_id = ?"
            params.append(token_data["branch_id"])

        # 🔹 Multi-branch filter support
        if branch_ids:
            branch_list = branch_ids.split(",")  # ["id1", "id2"]
            placeholders = ",".join(["?"] * len(branch_list))
            base_query += f" AND u.branch_id IN ({placeholders})"
            params.extend(branch_list)

        base_query += " ORDER BY u.created_at DESC"

        cursor.execute(base_query, params)
        users = [dict(row) for row in cursor.fetchall()]
        return users


@api_router.post("/admin/create-branch")
async def create_branch(request: BranchCreateRequest, token_data: dict = Depends(require_role(['super_admin']))):
    if not check_permission(token_data['role'], 'branches', 'insert'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        branch_id = str(uuid.uuid4())
        cursor.execute(
            "INSERT INTO branches (id, name, location, tenant_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (branch_id, request.name, request.location, token_data["tenant_id"], datetime.now(timezone.utc).isoformat()))
                
        log_audit(conn, token_data['user_id'], token_data['tenant_id'],'BRANCH_CREATED', 'branch', branch_id)
        return {"message": "Branch created successfully", "branch_id": branch_id}

@api_router.get("/admin/branches")
async def get_branches(token_data: dict = Depends(require_role(['admin','super_admin', 'auditor']))):
    if not check_permission(token_data['role'], 'branches', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        if token_data["role"] == "admin":
            cursor.execute(
                "SELECT * FROM branches WHERE id = ?",
                (token_data["branch_id"],)
            )
        else:
            cursor.execute(
                "SELECT * FROM branches WHERE tenant_id = ? ORDER BY created_at DESC",
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
        
        cursor.execute(
            "SELECT id FROM interest_rates WHERE loan_type = ? AND category = ?",
            (request.loan_type, request.category)
        )
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute(
                "UPDATE interest_rates SET rate = ?, is_overridden = 1 WHERE id = ?",
                (request.rate, existing['id'])
            )
        else:
            cursor.execute(
                "INSERT INTO interest_rates (id, loan_type, category, rate, is_overridden, created_at) VALUES (?, ?, ?, ?, 1, ?)",
                (str(uuid.uuid4()), request.loan_type, request.category, request.rate, datetime.now(timezone.utc).isoformat())
            )
        
        conn.commit()
        print(f"Interest rate manually updated: {request.loan_type}/{request.category} → {request.rate}% (is_overridden=1)")
        return {"message": "Interest rate updated successfully"}

@api_router.post("/admin/reset-interest-override")
async def reset_interest_override(request: InterestRateResetRequest, token_data: dict = Depends(require_role(['super_admin']))):
    """Reset an admin-overridden interest rate back to dynamic calculation."""
    if not check_permission(token_data['role'], 'reports', 'update'):
        raise HTTPException(403, "Access denied")

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id FROM interest_rates WHERE loan_type = ? AND category = ?",
            (request.loan_type, request.category)
        )
        existing = cursor.fetchone()

        if not existing:
            raise HTTPException(404, f"Interest rate not found for {request.loan_type}/{request.category}")

        # Get current repo rate
        repo_data = get_latest_repo_rate(str(DB_PATH))
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

        cursor.execute(
            "UPDATE interest_rates SET rate = ?, is_overridden = 0 WHERE id = ?",
            (new_rate, existing['id'])
        )

        conn.commit()
        print(f"Interest rate override reset: {request.loan_type}/{request.category} → {new_rate}% (is_overridden=0)")
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
        cursor.execute("SELECT * FROM interest_rates ORDER BY loan_type, category")
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

        # 🔒 Branch admin restriction
        if token_data["role"] == "admin":
            if request.branch_id != token_data["branch_id"]:
                raise HTTPException(
                    status_code=403,
                    detail="Admin can update only their branch rate"
                )

        # 🏆 APPLY TO ALL BRANCHES (SUPER ADMIN ONLY)
        if request.branch_id == "ALL":

            if token_data["role"] != "super_admin":
                raise HTTPException(403, "Only super admin can update all branches")

            # 🌍 ONLY GLOBAL INSERT — ONE ROW ONLY
            cursor.execute("""
                INSERT INTO gold_rate (id, branch_id, rate_per_gram, updated_at, source)
                VALUES (?, NULL, ?, ?, 'manual')
            """, (
                str(uuid.uuid4()),
                request.rate_per_gram,
                datetime.now(timezone.utc).isoformat()
            ))
            conn.commit()

            return {"message": "Gold rate updated globally"}
        # 🏢 SPECIFIC BRANCH
        if request.branch_id != "ALL":
            raise HTTPException(
                status_code=400,
                detail="Gold rate can be updated only globally"
            )

    return {
        "message": "Gold rate updated successfully",
        "rate_per_gram": request.rate_per_gram
    }

@api_router.get("/admin/gold-rate")
async def get_gold_rate(token_data: dict = Depends(require_role(['admin','super_admin']))):
    with get_db() as conn:
        cursor = conn.cursor()

        # 1️⃣ GET MODE
        cursor.execute("SELECT mode FROM gold_rate_settings WHERE id = 1")
        mode = cursor.fetchone()["mode"]

        # 🟢 AUTO MODE → MARKET RATE FROM DB
        if mode == "auto":
            cursor.execute("""
                SELECT rate_per_gram, updated_at
                FROM gold_rate
                WHERE source = 'auto'
                ORDER BY updated_at DESC
                LIMIT 1
            """)

            row = cursor.fetchone()

            if not row:
                raise HTTPException(404, "Market rate not found")

            return {
                "rate_per_gram": row["rate_per_gram"],
                "updated_at": row["updated_at"],
                "mode": "auto"
            }

        # 🔴 MANUAL MODE → ADMIN RATE
        if token_data["role"] == "super_admin":
            cursor.execute("""
                SELECT rate_per_gram, updated_at
                FROM gold_rate
                WHERE source = 'manual'
                AND branch_id IS NULL
                ORDER BY updated_at DESC, rowid DESC
                LIMIT 1
            """)
        else:
            cursor.execute("""
                SELECT rate_per_gram, updated_at
                FROM gold_rate
                WHERE source = 'manual'
                AND (branch_id = ? OR branch_id IS NULL)
                ORDER BY
                    (branch_id = ?) DESC,
                    updated_at DESC
                LIMIT 1
            """, (token_data["branch_id"], token_data["branch_id"]))
        row = cursor.fetchone()

    if not row:
        raise HTTPException(404, "Gold rate not found")

    return {
        "rate_per_gram": row["rate_per_gram"],
        "updated_at": row["updated_at"],
        "mode": "manual"
    }

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

        cursor.execute("""
            UPDATE gold_rate_settings
            SET mode = ?
            WHERE id = 1
        """, (mode,))
        conn.commit()

    return {"message": f"Gold rate mode set to {mode}"}

@api_router.get("/admin/gold-rate-mode")
async def get_gold_rate_mode(
    token_data: dict = Depends(require_role(['admin','super_admin']))
):
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT mode FROM gold_rate_settings WHERE id = 1")
        row = cursor.fetchone()

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

        cursor.execute(
            "SELECT name FROM tenants WHERE id = ?",
            (token_data["tenant_id"],)
        )
        company_row = cursor.fetchone()
        company_name = company_row["name"] if company_row else "SV FINCLOUD"

        branch_id = token_data.get("branch_id")

        if token_data.get("role") == "super_admin":
            branch_name = "All Branches"
        elif branch_id:
            cursor.execute("SELECT name FROM branches WHERE id = ?", (branch_id,))
            branch_row = cursor.fetchone()
            branch_name = branch_row[0] if branch_row else "Branch"
        else:
            branch_name = "Branch"

        # ⭐ FETCH DATA
        cursor.execute("""
            SELECT rate_per_gram, source, updated_at
            FROM gold_rate
            WHERE substr(updated_at,1,10) BETWEEN ? AND ?
            ORDER BY updated_at ASC
        """, (from_date_db, to_date_db))

        rows = cursor.fetchall()

    # ✅ 404 CHECK — No data for selected date range
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No gold rate records found for the selected date range."
        )

    # 📊 TOTAL RECORDS
    total_records = len(rows)

    # 📄 CREATE PDF
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=30,
        leftMargin=30,
        topMargin=30,
        bottomMargin=40
    )

    styles = getSampleStyleSheet()
    elements = []

    # ===== HEADER STYLES =====
    company_style = ParagraphStyle(
        name='Company',
        fontSize=18,
        alignment=1,
        spaceAfter=4,
        textColor=colors.darkblue,
        leading=20
    )

    branch_style = ParagraphStyle(
        name='Branch',
        fontSize=12,
        alignment=1,
        spaceAfter=12
    )

    # ===== HEADER =====
    elements.append(Paragraph(company_name, company_style))
    elements.append(Paragraph(branch_name, branch_style))

    # ===== STEP 2 — HEADER SEPARATOR LINE =====
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
    elements.append(Spacer(1, 10))
    info_style = ParagraphStyle(
        name='Info',
        fontSize=11,
        alignment=0,  # LEFT
        spaceAfter=4
    )

    elements.append(Paragraph(f"From {display_from} To {display_to}", info_style))
    elements.append(Paragraph(f"Total Records: {total_records}", info_style))

    elements.append(Spacer(1, 12))

    # ===== TABLE DATA =====
    data = [["Date", "Gold Rate (₹)", "Mode"]]

    for rate, source, updated_at in rows:
        date_only = datetime.fromisoformat(updated_at).strftime("%d-%m-%Y")
        data.append([date_only, f"{rate:.2f}", source.upper()])

    table = Table(
        data,
        colWidths=[2*inch, 2.5*inch, 1.5*inch],
        hAlign='CENTER'
    )

    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.darkblue),
        ("TEXTCOLOR", (0,0), (-1,0), colors.whitesmoke),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,0), 11),

        ("ALIGN", (0,0), (-1,-1), "CENTER"),
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),

        ("ROWBACKGROUNDS", (0,1), (-1,-1),
            [colors.whitesmoke, colors.lightgrey]),

        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("TOPPADDING", (0,0), (-1,-1), 8),
    ]))

    elements.append(table)

    # ===== FOOTER FUNCTION =====
    def add_footer(canvas, doc):
        page_num = canvas.getPageNumber()

        # Line above footer
        canvas.setStrokeColor(colors.grey)
        canvas.line(30, 40, 565, 40)

        # Page number
        canvas.setFont("Helvetica", 9)
        canvas.drawRightString(560, 25, f"Page {page_num}")

        # Timestamp
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        canvas.drawString(30, 25, f"Generated on: {now}")

    # ✅ BUILD THE PDF (this was the missing critical step!)
    doc.build(elements, onFirstPage=add_footer, onLaterPages=add_footer)

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

@api_router.get("/admin/stats")
async def get_admin_stats(branch_id: Optional[str] = None, token_data: dict = Depends(require_role(['admin','super_admin']))):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        tenant_id = token_data["tenant_id"]
        role = token_data["role"]

        # 1. Prepare the branch filter logic
        # If branch_id exists, we split it into a list and create placeholders like (?, ?)
        branch_filter = ""
        params = [tenant_id]
        
        if role == "admin":
            branch_filter = " AND branch_id = ?"
            params = [tenant_id, token_data["branch_id"]]

        elif branch_id:
            ids = branch_id.split(",")
            placeholders = ",".join(["?"] * len(ids))
            branch_filter = f" AND branch_id IN ({placeholders})"
            params.extend(ids)

        else:
            branch_filter = ""
            params = [tenant_id]

        # TOTAL USERS
        cursor.execute(f"SELECT COUNT(*) as total FROM users WHERE tenant_id = ? {branch_filter}", params)
        total_users = cursor.fetchone()['total']

        # TOTAL CUSTOMERS 
        cursor.execute(f"SELECT COUNT(*) as total FROM customers WHERE tenant_id = ? {branch_filter}", params)
        total_customers = cursor.fetchone()['total']

        # PENDING LOANS
        cursor.execute(f"SELECT COUNT(*) as total FROM loans WHERE status='pending' AND tenant_id = ? {branch_filter}", params)
        pending_loans = cursor.fetchone()['total']

        # APPROVED (ACTIVE) LOANS
        cursor.execute(f"SELECT COUNT(*) as total FROM loans WHERE status='active' AND tenant_id = ? {branch_filter}", params)
        active_loans = cursor.fetchone()['total']

        # TOTAL DISBURSED
        cursor.execute(f"SELECT SUM(amount) as total FROM loans WHERE status='active' AND tenant_id = ? {branch_filter}", params)
        total_disbursed = cursor.fetchone()['total'] or 0

        # TOTAL OUTSTANDING
        cursor.execute(f"SELECT SUM(outstanding_balance) as total FROM loans WHERE status='active' AND tenant_id = ? {branch_filter}", params)
        total_outstanding = cursor.fetchone()['total'] or 0

        return {
            "total_users": total_users,
            "total_customers": total_customers,
            "pending_loans": pending_loans,
            "approved_loans": active_loans,
            "total_disbursed": total_disbursed,
            "total_outstanding": total_outstanding
        }

@api_router.get("/admin/interest-earned")
async def get_interest_earned(branch_id: str = None, token_data: dict = Depends(require_role(['admin','super_admin']))):
    with get_db() as conn:
        cursor = conn.cursor()
        tenant_id = token_data["tenant_id"]

        # 1. Base query parts
        query = """
            SELECT COALESCE(SUM(e.interest_amount), 0) AS interest_earned
            FROM emi_schedule e
            JOIN loans l ON e.loan_id = l.id
            WHERE e.status = 'paid'
            AND l.tenant_id = ?
        """
        params = [tenant_id]

        # 2. Add multi-branch support if branch_id is provided
        if branch_id:
            # Split "id1,id2" into ['id1', 'id2']
            ids = branch_id.split(",")
            # Create placeholders: (?, ?)
            placeholders = ",".join(["?"] * len(ids))
            query += f" AND l.branch_id IN ({placeholders})"
            params.extend(ids)

        # 3. Execute and return
        cursor.execute(query, params)
        result = cursor.fetchone()

        return {
            "interest_earned": round(result["interest_earned"], 2)
        }
@api_router.get("/admin/branch-loan-stats")
async def branch_loan_stats(token_data: dict = Depends(require_role(['admin','super_admin']))):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT b.name, SUM(l.amount) as total
            FROM loans l
            JOIN branches b ON l.branch_id = b.id
            WHERE l.status = 'active' AND l.tenant_id = ?
            GROUP BY b.name
        """, (token_data["tenant_id"],))

        return [{"branch": row["name"], "amount": row["total"] or 0} for row in cursor.fetchall()]
    
@api_router.get("/admin/monthly-collections")
async def monthly_collections(token_data: dict = Depends(require_role(['admin','super_admin']))):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()

        branch_filter, params = get_branch_filter(token_data)

        query = f"""
        SELECT strftime('%Y-%m', payment_date) as month,
            SUM(amount) as total
        FROM payments
        WHERE status = 'approved'
        AND tenant_id = ?
        {branch_filter}
        GROUP BY month
        ORDER BY month
        """

        cursor.execute(query, (token_data["tenant_id"], *params))

        return [{"month": row["month"], "amount": row["total"] or 0} for row in cursor.fetchall()]
@api_router.get("/admin/branch-performance")
async def branch_performance(token_data: dict = Depends(require_role(['admin','super_admin']))):
    if not check_permission(token_data['role'], 'reports', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT b.name,
                   SUM(p.amount) as total_collected
            FROM payments p
            JOIN loans l ON p.loan_id = l.id
            JOIN branches b ON l.branch_id = b.id
            WHERE p.status = 'approved' AND l.tenant_id = ?
            GROUP BY b.name
            ORDER BY total_collected DESC
        """, (token_data["tenant_id"],))

        return [{"branch": row["name"], "collected": row["total_collected"] or 0} for row in cursor.fetchall()]
    
# Auditor Routes
@api_router.get("/auditor/loans")
async def get_all_loans(token_data: dict = Depends(require_role(['auditor', 'super_admin']))):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT l.*, c.name as customer_name, c.email, c.phone
            FROM loans l
            INNER JOIN customers c ON l.customer_id = c.id
            WHERE l.tenant_id = ?
            ORDER BY l.created_at DESC
        """, (token_data["tenant_id"],))
        loans = [dict(row) for row in cursor.fetchall()]
        return loans

@api_router.get("/auditor/payments")
async def get_all_payments(token_data: dict = Depends(require_role(['auditor', 'super_admin']))):
    if not check_permission(token_data['role'], 'payments', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT p.*, l.loan_type, c.name as customer_name
            FROM payments p
            INNER JOIN loans l ON p.loan_id = l.id
            INNER JOIN customers c ON l.customer_id = c.id
            WHERE p.tenant_id = ?
            ORDER BY p.created_at DESC
        """, (token_data["tenant_id"],))
        payments = [dict(row) for row in cursor.fetchall()]
        return payments

@api_router.get("/auditor/audit-logs")
async def get_audit_logs(token_data: dict = Depends(require_role(['auditor','super_admin', 'admin']))):
    if not check_permission(token_data['role'], 'audit_logs', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT a.*, u.username
            FROM audit_logs a
            INNER JOIN users u ON a.user_id = u.id
            WHERE u.tenant_id = ?
            ORDER BY a.created_at DESC
            LIMIT 500
        """, (token_data["tenant_id"],))
        logs = [dict(row) for row in cursor.fetchall()]
        return logs

# Common Routes
@api_router.get("/loan-types")
async def get_loan_types(token_data: dict = Depends(verify_token)):
    if not check_permission(token_data['role'], 'loans', 'view'):
        raise HTTPException(403, "Access denied")
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM loan_types")
        types = [dict(row) for row in cursor.fetchall()]
        return types


@api_router.get("/admin/repo-rate")
async def get_repo_rate(token_data: dict = Depends(require_role(['admin', 'super_admin']))):
    """Return the latest RBI repo rate from history."""
    repo_data = get_latest_repo_rate(str(DB_PATH))

    if not repo_data:
        # No history yet — fetch and seed
        current_rate = fetch_repo_rate()
        save_repo_rate(str(DB_PATH), current_rate)
        return {
            "repo_rate": current_rate,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source": "freshly_fetched"
        }

    return {
        "repo_rate": repo_data["repo_rate"],
        "fetched_at": repo_data["fetched_at"],
        "source": "database"
    }
@api_router.post("/admin/update-repo-rate")
async def update_repo_rate(
    request: RepoRateUpdateRequest,
    token_data: dict = Depends(require_role(['super_admin']))
):
    save_repo_rate(str(DB_PATH), request.repo_rate)

    recalculate_interest_rates(str(DB_PATH), request.repo_rate)

    return {
        "message": f"Repo rate updated to {request.repo_rate}% and interest rates recalculated."
    }

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Include router
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
