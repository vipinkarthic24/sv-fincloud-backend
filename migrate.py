"""
migrate.py — Full Supabase -> Railway data migration
Run this INSIDE Railway shell:
  python migrate.py

Set these env vars before running:
  export SOURCE_DB_URL="<supabase connection string>"
  export DEST_DB_URL="<railway internal connection string>"
"""

import os
import psycopg2
import psycopg2.extras
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)



def main():
    source_url = os.environ.get("SOURCE_DB_URL")
    dest_url   = os.environ.get("DEST_DB_URL")

    if not source_url or not dest_url:
        log.error("SOURCE_DB_URL and DEST_DB_URL must be set as environment variables.")
        return

# Migration order — respects FK dependencies
MIGRATION_GROUPS = [
    # Group 1: no FK dependencies
    ["tenants", "branches", "modules", "loan_types"],
    # Group 2: depends on Group 1
    ["roles", "users", "employees", "customers", "gold_rate_settings"],
    # Group 3: depends on Group 2
    ["loans", "interest_rates", "gold_rate", "repo_rate_history",
     "loan_sequences", "payment_sequence", "receipt_sequence",
     "payments_receipt_counter"],
    # Group 4: depends on Group 3
    ["emi_schedule", "payments", "penalties", "audit_logs", "permissions"],
]


def get_all_tables(cursor):
    cursor.execute("""
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
    """)
    return {row["tablename"] for row in cursor.fetchall()}


def get_columns(cursor, table):
    cursor.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    return [row["column_name"] for row in cursor.fetchall()]


def get_primary_keys(cursor, table):
    cursor.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = %s
        ORDER BY kcu.ordinal_position
    """, (table,))
    return [row["column_name"] for row in cursor.fetchall()]


def quote_cols(cols):
    return ", ".join('"' + c + '"' for c in cols)


def migrate_table(src_cur, dst_conn, dst_cur, table, src_tables, dst_tables):
    if table not in src_tables:
        log.info("  SKIP %-35s (not in source)", table)
        return 0
    if table not in dst_tables:
        log.warning("  SKIP %-35s (not in destination — run app once to init schema)", table)
        return 0

    src_col_list = get_columns(src_cur, table)
    dst_col_set  = set(get_columns(dst_cur, table))
    cols = [c for c in src_col_list if c in dst_col_set]

    if not cols:
        log.warning("  SKIP %-35s (no matching columns)", table)
        return 0

    pks = get_primary_keys(dst_cur, table)

    select_sql = 'SELECT ' + quote_cols(cols) + ' FROM "' + table + '"'
    src_cur.execute(select_sql)
    rows = src_cur.fetchall()

    if not rows:
        log.info("  OK   %-35s (0 rows — empty)", table)
        return 0

    col_list          = quote_cols(cols)
    val_placeholders  = ", ".join(["%s"] * len(cols))

    if pks:
        non_pk_cols = [c for c in cols if c not in pks]
        pk_conflict = "(" + quote_cols(pks) + ")"
        if non_pk_cols:
            update_set = ", ".join('"' + c + '" = EXCLUDED."' + c + '"' for c in non_pk_cols)
            conflict_clause = "ON CONFLICT " + pk_conflict + " DO UPDATE SET " + update_set
        else:
            conflict_clause = "ON CONFLICT " + pk_conflict + " DO NOTHING"
        sql = 'INSERT INTO "' + table + '" (' + col_list + ') VALUES (' + val_placeholders + ') ' + conflict_clause
    else:
        sql = 'INSERT INTO "' + table + '" (' + col_list + ') VALUES (' + val_placeholders + ')'

    inserted = 0
    errors   = 0
    for row in rows:
        values = [row[c] for c in cols]
        try:
            dst_cur.execute(sql, values)
            inserted += 1
        except Exception as e:
            dst_conn.rollback()
            errors += 1
            if errors <= 3:
                log.warning("    Row error in %s: %s", table, e)

    dst_conn.commit()
    log.info("  OK   %-35s (%d rows upserted, %d errors)", table, inserted, errors)
    return inserted


def main():
    source_url = os.environ.get("SOURCE_DB_URL")
    dest_url   = os.environ.get("DEST_DB_URL")

    if not source_url or not dest_url:
        log.error("SOURCE_DB_URL and DEST_DB_URL must be set as environment variables.")
        return

    log.info("Connecting to source (Supabase)...")
    src_conn = psycopg2.connect(source_url)
    src_conn.set_session(readonly=True, autocommit=True)
    src_cur = src_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    log.info("Connecting to destination (Railway)...")
    dst_conn = psycopg2.connect(dest_url)
    dst_cur  = dst_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    src_tables = get_all_tables(src_cur)
    dst_tables = get_all_tables(dst_cur)

    log.info("Source tables:      %s", sorted(src_tables))
    log.info("Destination tables: %s", sorted(dst_tables))

    log.info("Disabling FK checks on destination...")
    dst_cur.execute("SET session_replication_role = 'replica'")
    dst_conn.commit()

    total            = 0
    migrated_tables  = set()

    for group_idx, group in enumerate(MIGRATION_GROUPS, 1):
        log.info("--- Group %d ---", group_idx)
        for table in group:
            total += migrate_table(src_cur, dst_conn, dst_cur, table, src_tables, dst_tables)
            migrated_tables.add(table)

    remaining = src_tables - migrated_tables
    if remaining:
        log.info("--- Remaining tables ---")
        for table in sorted(remaining):
            total += migrate_table(src_cur, dst_conn, dst_cur, table, src_tables, dst_tables)

    log.info("Re-enabling FK checks on destination...")
    dst_cur.execute("SET session_replication_role = 'origin'")
    dst_conn.commit()

    src_cur.close();  src_conn.close()
    dst_cur.close();  dst_conn.close()

    log.info("Migration complete. Total rows upserted: %d", total)


if __name__ == "__main__":
    main()
