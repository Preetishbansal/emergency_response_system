"""
Task 29 - Data Quality Checks
Validate incident data, detect anomalies, and ensure schema integrity.
Fixed for Windows terminal encoding (ASCII-only).
"""

import pandas as pd
import numpy as np
import os
import sys

def run_quality_checks(df, name="Dataset"):
    print("\n" + "="*60)
    print(f"  DATA QUALITY REPORT: {name}")
    print("="*60)
    
    issues = []
    
    # 1. Schema validation
    required_cols = ['incident_id', 'timestamp', 'zone', 'severity', 'response_time']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        issues.append(f"MISSING COLUMNS: {missing}")
    
    # 2. Null checks
    cols_to_check = [c for c in required_cols if c in df.columns]
    null_counts = df[cols_to_check].isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            issues.append(f"NULLS in {col}: {count} rows")
            
    # 3. Range checks
    if 'response_time' in df.columns:
        invalid_time = df[df['response_time'] < 0].shape[0]
        too_slow = df[df['response_time'] > 7200].shape[0]  # > 2 hours
        if invalid_time: 
            issues.append(f"NEGATIVE response times: {invalid_time}")
        if too_slow: 
            issues.append(f"Suspiciously slow response (> 2 hrs): {too_slow}")
            
        # 4. Anomaly detection (z-score)
        if len(df) > 1:
            mean_val = df['response_time'].mean()
            std_val = df['response_time'].std()
            if std_val > 0:
                z_scores = np.abs((df['response_time'] - mean_val) / std_val)
                anomalies = df[z_scores > 3].shape[0]
                if anomalies: 
                    issues.append(f"STATISTICAL ANOMALIES (Z > 3): {anomalies}")

    # 5. Duplicate check
    if 'incident_id' in df.columns:
        dupes = df.duplicated(subset=['incident_id']).sum()
        if dupes: 
            issues.append(f"DUPLICATE incident IDs: {dupes}")

    # Final logic
    if issues:
        print("\nQUALITY ISSUES FOUND:")
        for i in issues: 
            print(f"  - {i}")
        print("\nStatus: FAILED")
    else:
        print("\nAll quality checks passed!")
        print("Status: PASSED")
        
    print("="*60 + "\n")
    return len(issues) == 0

if __name__ == "__main__":
    # Test on the cleaned data
    path = 'data/processed/incidents_clean.csv'
    if os.path.exists(path):
        df_test = pd.read_csv(path)
        run_quality_checks(df_test, name="Cleaned Incidents")
    else:
        # Fallback to raw data if cleaned doesn't exist
        path_raw = 'data/raw/incidents.csv'
        if os.path.exists(path_raw):
            df_test = pd.read_csv(path_raw)
            run_quality_checks(df_test, name="Raw Incidents")
        else:
            print("No data found to check.")
