import pandas as pd

def normalize_response_time(df, column='response_time'):
    """Scale values between 0 and 1"""
    # Create a copy so we don't modify the original dataframe in-place unexpectadly, 
    # though in typical pandas pipelines returning it modified is fine
    df = df.copy()
    min_val = df[column].min()
    max_val = df[column].max()
    df[column + '_normalized'] = (df[column] - min_val) / (max_val - min_val)
    return df

def aggregate_by_zone(df):
    """Count incidents and avg response time per zone"""
    return df.groupby('zone').agg(
        total_incidents=('incident_id', 'count'),
        avg_response_time=('response_time', 'mean')
    ).reset_index()

def validate_severity(df):
    """Flag rows with invalid severity values"""
    df = df.copy()
    valid = ['low', 'medium', 'high', 'critical']
    df['valid_severity'] = df['severity'].str.lower().isin(valid)
    return df
