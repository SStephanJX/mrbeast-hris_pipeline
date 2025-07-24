# ──────────────────────────────────────────────────────────────────────────────
# generate_charts.py — HRIS Reporting Layer
# Creates static visual charts for key HR metrics:
# 1. Avg Time-to-Hire by Department
# 2. Applicant Status Distribution
# 3. Top Roles by Applicant Volume
# Output saved to visuals/ folder for README and reporting
# ──────────────────────────────────────────────────────────────────────────────

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

# ───────────────────────────── Config ─────────────────────────────
DB_PATH = 'hris_project.db'
OUTPUT_FOLDER = 'visuals'

# ───────────────────────────── Chart 1: Time-to-Hire ─────────────────────────────
def fetch_avg_hire_time():
    query = """
        SELECT department,
               ROUND(AVG(time_to_hire_days), 1) AS avg_days
        FROM time_to_hire
        GROUP BY department
        ORDER BY avg_days DESC
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def create_bar_chart(df):
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    plt.figure(figsize=(10, 6))
    bars = plt.bar(df['department'], df['avg_days'], color='#00CFFF', edgecolor='#D8F2FF')
    plt.title('Average Time-to-Hire by Department\nBased on 400+ hires across 5 departments',
              fontsize=14, ha='center')
    plt.subplots_adjust(top=0.85)
    plt.xlabel('Department')
    plt.ylabel('Avg Time to Hire (days)')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2.0, height + 0.4,
                 f'{height:.1f}', ha='center', va='bottom', fontsize=9)
    plt.tight_layout()
    path = os.path.join(OUTPUT_FOLDER, 'avg_time_to_hire_by_department.png')
    plt.savefig(path)
    print(f"✅ Chart saved to {path}")
    plt.close()

# ───────────────────────────── Chart 2: Applicant Status ─────────────────────────────
def fetch_status_counts():
    query = """
        SELECT LOWER(TRIM(status)) AS status,
               COUNT(*) AS count
        FROM applicants
        WHERE status IS NOT NULL
        GROUP BY status
        ORDER BY count DESC
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def create_status_distribution_chart(df):
    plt.figure(figsize=(8, 6))

    # MrBeast-inspired palette (vibrant to pale blues)
    mrbeast_palette = ['#00CFFF', '#00A8F3', '#70D2FF', '#A3DEFF', '#D8F2FF']
    colors = (mrbeast_palette * ((len(df) + len(mrbeast_palette) - 1) // len(mrbeast_palette)))[:len(df)]

    bars = plt.bar(df['status'].str.title(), df['count'], color=colors, edgecolor='black')

    plt.title('Applicant Status Distribution\nBased on 1,000+ applicant records',
              fontsize=14, ha='center')
    plt.subplots_adjust(top=0.85)

    plt.xlabel('Status')
    plt.ylabel('Count of Applicants')
    plt.xticks(rotation=0)
    plt.grid(axis='y', linestyle='--', alpha=0.5)

    for i, val in enumerate(df['count']):
        plt.text(i, val + 0.5, str(val), ha='center', fontsize=9)

    plt.tight_layout()
    path = os.path.join(OUTPUT_FOLDER, 'applicant_status_distribution.png')
    plt.savefig(path)
    print(f"✅ Chart saved to {path}")
    plt.close()

# ───────────────────────────── Chart 3: Top Roles ─────────────────────────────
def fetch_top_roles():
    query = """
        SELECT role, COUNT(*) AS count
        FROM applicants
        GROUP BY role
        ORDER BY count DESC
        LIMIT 10
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def create_role_distribution_chart(df):
    plt.figure(figsize=(10, 6))
    bars = plt.barh(df['role'].str.title(), df['count'], color='#00A8F3', edgecolor='black')
    plt.title('Top 10 Roles by Applicant Volume\nReflects submission trends from HRIS dataset',
              fontsize=14, ha='center')
    plt.subplots_adjust(top=0.85)
    plt.xlabel('Count')
    plt.ylabel('Role')
    plt.grid(axis='x', linestyle='--', alpha=0.4)
    plt.gca().invert_yaxis()
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 0.5, bar.get_y() + bar.get_height()/2,
                 str(int(width)), va='center', fontsize=9)
    plt.tight_layout()
    path = os.path.join(OUTPUT_FOLDER, 'top_roles_by_applicant_volume.png')
    plt.savefig(path)
    print(f"✅ Chart saved to {path}")
    plt.close()

# ───────────────────────────── Execution Entry ─────────────────────────────
if __name__ == '__main__':
    print("📊 Generating charts for HRIS metrics")

    df_hire = fetch_avg_hire_time()
    if not df_hire.empty:
        create_bar_chart(df_hire)
    else:
        print("⚠️ No data for time-to-hire chart.")

    df_status = fetch_status_counts()
    if not df_status.empty:
        create_status_distribution_chart(df_status)
    else:
        print("⚠️ No data for status distribution chart.")

    df_roles = fetch_top_roles()
    if not df_roles.empty:
        create_role_distribution_chart(df_roles)
    else:
        print("⚠️ No data for top roles chart.")