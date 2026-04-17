import pandas as pd
import numpy as np
import os

def generate_large_csv():
    print("Generating 1.2 million rows mock dataset (this might take a few seconds)...")
    num_rows = 1_200_000
    df = pd.DataFrame({
        'incident_id': np.arange(num_rows),
        'timestamp': pd.date_range(start='2025-01-01', periods=num_rows, freq='S'),
        'severity': np.random.choice(['low', 'medium', 'high', 'critical'], num_rows),
        'zone_id': np.random.randint(1, 10, num_rows, dtype=np.int16),
        'response_time': np.random.uniform(5, 5000, num_rows) # some above 3600 to filter
    })
    os.makedirs('data/raw', exist_ok=True)
    df.to_csv('data/raw/incidents_large.csv', index=False)
    print("Large CSV generated at data/raw/incidents_large.csv!")

if __name__ == "__main__":
    generate_large_csv()
