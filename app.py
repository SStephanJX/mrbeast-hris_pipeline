# STEP 1: Import Modules and Configure App
# ──────────────────────────────────────────────────────────────────────────────
# app.py — REST API Server for MrBeast HRIS Take-Home
# Serves endpoints from prebuilt SQLite views with resilience and clarity
# ──────────────────────────────────────────────────────────────────────────────

from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)
DB_PATH = 'hris_project.db'


# STEP 2: Shared Safe Query Function
# ──────────────────────────────────────────────────────────────────────────────
def safe_query(query, transform_fn):
    """Wraps DB access with structured error handling and metadata."""
    try:
        conn = sqlite3.connect(DB_PATH)
        results = conn.execute(query).fetchall()
        return jsonify({
            "count": len(results),
            "data": [transform_fn(row) for row in results]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# STEP 3: Endpoint — /hiring-metrics
# Returns average time-to-hire by department
# ──────────────────────────────────────────────────────────────────────────────
@app.route('/hiring-metrics')
def hiring_metrics():
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT department, 
               ROUND(AVG(time_to_hire_days), 1) AS avg_time_to_hire
        FROM time_to_hire
        GROUP BY department
    """
    results = conn.execute(query).fetchall()
    conn.close()
    data = [{"department": row[0], "avg_time_to_hire": row[1]} for row in results]
    return jsonify({"count": len(data), "data": data})

# STEP 4: Endpoint — /applicants/status-summary
# Returns count of applicants by status, with optional ?status=filter
# ──────────────────────────────────────────────────────────────────────────────
@app.route('/applicants/status-summary')
def status_summary():
    status_filter = request.args.get('status')
    conn = sqlite3.connect(DB_PATH)

    if status_filter:
        query = """
            SELECT status, COUNT(*) AS count
            FROM applicants
            WHERE LOWER(status) = ?
            GROUP BY status
        """
        results = conn.execute(query, (status_filter.lower(),)).fetchall()
    else:
        query = "SELECT status, count FROM status_summary"
        results = conn.execute(query).fetchall()

    conn.close()
    data = [{"status": row[0], "count": row[1]} for row in results]
    return jsonify({"count": len(data), "data": data})

# STEP 5: Start API Server
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True)
