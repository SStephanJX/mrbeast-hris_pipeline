import pandas as pd

def test_time_to_hire_calculation():
    df = pd.DataFrame({
        'applied_date': ['2023-01-01'],
        'hire_date': ['2023-01-15']
    })
    df['applied_date'] = pd.to_datetime(df['applied_date'])
    df['hire_date'] = pd.to_datetime(df['hire_date'])
    df['time_to_hire_days'] = (df['hire_date'] - df['applied_date']).dt.days

    assert df['time_to_hire_days'][0] == 14