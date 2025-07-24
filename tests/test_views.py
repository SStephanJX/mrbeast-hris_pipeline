# ──────────────────────────────────────────────────────────────────────────────
# test_views.py — Full Integrity Test Suite for HRIS SQLite Views & Pipeline
# Runs setup, validates view logic, and alerts if data looks off
# ──────────────────────────────────────────────────────────────────────────────

import sqlite3
import pandas as pd
import subprocess
import os

DB_PATH = 'hris_project.db'

# ───────────────────────────── Setup Module ─────────────────────────────
def setup_module(module):
    """Run data loader and transform scripts before tests."""
    print("\n🔁 Running data_loader.py...")
    subprocess.run(['python', 'data_loader.py'], check=True)

    print("\n🔁 Running transform.py...")
    subprocess.run(['python', 'transform.py'], check=True)

    assert os.path.exists(DB_PATH), "SQLite DB not found after setup"

# ───────────────────────────── View Loaders ─────────────────────────────
def load_view(view_name):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(f"SELECT * FROM {view_name}", conn)
    conn.close()
    return df

# ───────────────────────────── View Integrity Tests ─────────────────────────────
def test_time_to_hire_view_structure():
    df = load_view('time_to_hire')
    expected_cols = {'name', 'role', 'application_date', 'hire_date', 'time_to_hire_days', 'department'}

    assert expected_cols.issubset(df.columns), "Missing columns in time_to_hire"
    assert len(df) >= 10, f"Too few rows in time_to_hire ({len(df)} found)"
    assert df['time_to_hire_days'].dropna().ge(0).all(), "Negative values in time_to_hire_days"

def test_status_summary_view_structure():
    df = load_view('status_summary')

    assert {'status', 'count'}.issubset(df.columns), "Missing columns in status_summary"
    assert len(df) >= 3, f"Too few status categories in status_summary ({len(df)} found)"
    assert df['count'].dropna().ge(0).all(), "Invalid counts in status_summary"

def test_time_to_hire_null_flags():
    df = load_view('time_to_hire')
    nulls = df['time_to_hire_days'].isna().sum()

    assert nulls <= 5, f"Too many null time_to_hire values ({nulls}) — check join logic"