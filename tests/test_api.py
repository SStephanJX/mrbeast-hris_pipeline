# STEP 1: Imports & Setup
# ──────────────────────────────────────────────────────────────────────────────
import pytest
import pandas as pd
from app import app  # Assumes a Flask app object named 'app'

# STEP 2: Test Client Fixture for Flask
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

# STEP 3: API Contract Tests — /hiring-metrics
# ──────────────────────────────────────────────────────────────────────────────
def test_hiring_metrics(client):
    """Verify structure and status of /hiring-metrics response."""
    response = client.get("/hiring-metrics")
    assert response.status_code == 200
    json_data = response.get_json()
    assert "count" in json_data
    assert isinstance(json_data["data"], list)
    for item in json_data["data"]:
        assert "department" in item
        assert "avg_time_to_hire" in item

# STEP 4: API Contract Tests — /status-summary
# ──────────────────────────────────────────────────────────────────────────────
def test_status_summary(client):
    """Verify structure and status of /applicants/status-summary response."""
    response = client.get("/applicants/status-summary")
    assert response.status_code == 200
    json_data = response.get_json()
    assert "count" in json_data
    assert isinstance(json_data["data"], list)
    for item in json_data["data"]:
        assert "status" in item
        assert "count" in item

# STEP 5: API Param Logic — Filter by status
# ──────────────────────────────────────────────────────────────────────────────
def test_status_filter(client):
    """Ensure /status-summary?status=hired filters correctly."""
    response = client.get("/applicants/status-summary?status=hired")
    assert response.status_code == 200
    json_data = response.get_json()
    for item in json_data["data"]:
        assert item["status"].lower() == "hired"

# STEP 6: Internal Logic Validation — Time to Hire
# ──────────────────────────────────────────────────────────────────────────────
def test_time_to_hire_calculation():
    """Verify time-to-hire calculation from applied_date to hire_date."""
    df = pd.DataFrame({
        'applied_date': ['2023-01-01'],
        'hire_date': ['2023-01-15']
    })
    df['applied_date'] = pd.to_datetime(df['applied_date'])
    df['hire_date'] = pd.to_datetime(df['hire_date'])
    df['time_to_hire_days'] = (df['hire_date'] - df['applied_date']).dt.days

    assert df['time_to_hire_days'][0] == 14