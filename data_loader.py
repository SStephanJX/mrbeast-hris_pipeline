# ──────────────────────────────────────────────────────────────────────────────
# data_loader.py — HRIS Data Ingestion Script
# Loads Excel worksheets into SQLite, normalizes structure
# ──────────────────────────────────────────────────────────────────────────────

import pandas as pd
import sqlite3
import os

# ───────────────────────────── Constants ─────────────────────────────
EXCEL_PATH = r'C:\Users\Stephen\Projects\MrBeastSeniorHRISEngineerTakeHomeProject\HRIS_TAKE_HOME_PROJECT_DATA.xlsx'
DB_PATH = 'hris_project.db'

# ───────────────────────────── Load Excel ─────────────────────────────
def load_excel_data():
    """Load all sheets from the Excel file into DataFrames."""
    if not os.path.exists(EXCEL_PATH):
        raise FileNotFoundError(f"Excel file not found at: {EXCEL_PATH}")
    
    xl = pd.ExcelFile(EXCEL_PATH)
    employees_df = xl.parse('Employees')
    applicants_df = xl.parse('Applicants')
    employment_type_df = xl.parse('EmploymentType')  # Sheet name renamed and trimmed

    # Normalize all column headers to snake_case
    employees_df.columns = employees_df.columns.str.strip().str.lower().str.replace(' ', '_')
    applicants_df.columns = applicants_df.columns.str.strip().str.lower().str.replace(' ', '_')
    employment_type_df.columns = employment_type_df.columns.str.strip().str.lower().str.replace(' ', '_')

    return employees_df, applicants_df, employment_type_df

# ───────────────────────────── Clean Data ─────────────────────────────
def clean_dataframes(employees_df, applicants_df, employment_type_df):
    """Clean and normalize datasets — date formatting, deduplication, missing values."""

    os.makedirs('logs', exist_ok=True)

    # ───────────────────────────── Employees ─────────────────────────────
    print("\n🔍 Cleaning Employees data...")
    employees_df['hire_date'] = pd.to_datetime(employees_df['start_date'], errors='coerce')
    employees_df['end_date'] = pd.to_datetime(employees_df['end_date'], errors='coerce')

    pre_emp = employees_df.copy()
    employees_df.drop_duplicates(subset=['name', 'hire_date', 'department'], inplace=True)
    dropped_emp = pre_emp[~pre_emp.index.isin(employees_df.index)]
    dropped_emp.to_csv('logs/dropped_employees.csv', index=False)
    print(f"🧹 Employees deduped: {len(pre_emp)} → {len(employees_df)} (Saved {len(dropped_emp)} to logs/dropped_employees.csv)")

    # ───────────────────────────── Applicants ─────────────────────────────
    print("\n🔍 Cleaning Applicants data...")
    applicants_df['application_date'] = pd.to_datetime(applicants_df['application_date'], errors='coerce')
    applicants_df['status'] = applicants_df['status'].fillna('unknown')

    pre_app = applicants_df.copy()
    applicants_df.drop_duplicates(subset=['name', 'role', 'application_date'], inplace=True)
    dropped_app = pre_app[~pre_app.index.isin(applicants_df.index)]
    dropped_app.to_csv('logs/dropped_applicants.csv', index=False)
    print(f"🧹 Applicants deduped: {len(pre_app)} → {len(applicants_df)} (Saved {len(dropped_app)} to logs/dropped_applicants.csv)")

    # ───────────────────────────── Employment Types ─────────────────────────────
    print("\n🔍 Cleaning Employment Types data...")
    before = len(employment_type_df)
    employment_type_df.drop_duplicates(subset=['employment_type'], inplace=True)
    print(f"🧹 Employment Types deduped: {before} → {len(employment_type_df)}")

    # ───────────────────────────── Null Summary ─────────────────────────────
    print("\n🗓️ Null Date Summary:")
    print(f"- Employees missing hire_date: {employees_df['hire_date'].isna().sum()}")
    print(f"- Applicants missing application_date: {applicants_df['application_date'].isna().sum()}")

    return employees_df, applicants_df, employment_type_df

# ───────────────────────────── Write to SQLite ─────────────────────────────
def write_to_sqlite(employees_df, applicants_df, employment_type_df):
    """Write cleaned dataframes to SQLite tables."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    employees_df.to_sql('employees', conn, if_exists='replace', index=False)
    applicants_df.to_sql('applicants', conn, if_exists='replace', index=False)
    employment_type_df.to_sql('employment_types', conn, if_exists='replace', index=False)
    conn.close()

    print(f"✅ Database created at {DB_PATH} with 3 tables.")

# ───────────────────────────── Main Execution ─────────────────────────────
if __name__ == "__main__":
    print("🔁 Loading Excel data...")
    employees_df, applicants_df, employment_type_df = load_excel_data()

    print("🧼 Cleaning dataframes...")
    employees_df, applicants_df, employment_type_df = clean_dataframes(
        employees_df, applicants_df, employment_type_df
    )

    print("💾 Writing to SQLite...")
    write_to_sqlite(employees_df, applicants_df, employment_type_df)

    print("🏁 ✅ Data ingestion completed.")