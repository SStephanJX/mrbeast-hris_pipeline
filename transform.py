# ──────────────────────────────────────────────────────────────────────────────
# transform.py — HRIS View Construction Script
# Defines SQLite views for analytical metrics, with error tracking
# ──────────────────────────────────────────────────────────────────────────────

import sqlite3
import pandas as pd
import os

DB_PATH = 'hris_project.db'
ERROR_LOG_PATH = 'logs/invalid_hires.csv'

# ───────────────────────────── Error Logger ─────────────────────────────
def log_invalid_hires(conn):
    """
    Identifies cases where hire_date precedes application_date,
    then logs the affected records for HR review.
    """
    query = """
        SELECT 
            a.name,
            a.role,
            a.application_date,
            e.hire_date,
            e.department,
            'Hire date precedes application date' AS error_reason
        FROM applicants a
        JOIN employees e ON LOWER(TRIM(a.name)) = LOWER(TRIM(e.name))
        WHERE e.hire_date IS NOT NULL
          AND a.application_date IS NOT NULL
          AND julianday(e.hire_date) < julianday(a.application_date)
    """
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        os.makedirs('logs', exist_ok=True)
        df.to_csv(ERROR_LOG_PATH, index=False)
        print(f"⚠️ Logged {len(df)} invalid hires to {ERROR_LOG_PATH}")

# ───────────────────────────── View Builder ─────────────────────────────
def create_views():
    """
    Creates SQL views for:
    - Time to hire: Includes only valid hire timelines
    - Status summary: Counts applicant statuses
    Also triggers error logger for auditing removed records.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Drop old views to ensure refresh
    cursor.execute("DROP VIEW IF EXISTS time_to_hire")
    cursor.execute("DROP VIEW IF EXISTS status_summary")

    # ───── View: time_to_hire ─────
    cursor.execute("""
        CREATE VIEW time_to_hire AS
        SELECT
            LOWER(TRIM(a.name)) AS name,
            a.role,
            a.application_date,
            e.hire_date,
            julianday(e.hire_date) - julianday(a.application_date) AS time_to_hire_days,
            e.department
        FROM applicants a
        JOIN employees e ON LOWER(TRIM(a.name)) = LOWER(TRIM(e.name))
        WHERE e.hire_date IS NOT NULL
          AND a.application_date IS NOT NULL
          AND julianday(e.hire_date) >= julianday(a.application_date)
    """)

    # ───── View: status_summary ─────
    cursor.execute("""
        CREATE VIEW status_summary AS
        SELECT status, COUNT(*) AS count
        FROM applicants
        GROUP BY status
    """)

    # Log temporal inconsistencies for HR audit trail
    log_invalid_hires(conn)

    conn.close()
    print("📐 Views created successfully in database.")

# ───────────────────────────── Execution Entry ─────────────────────────────
if __name__ == "__main__":
    print("🔧 Running transformation module...")
    create_views().`    `