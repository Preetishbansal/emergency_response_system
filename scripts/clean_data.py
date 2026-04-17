import pandas as pd
import os
from transformations import normalize_response_time, validate_severity, aggregate_by_zone

# Read a CSV file
df = pd.read_csv('data/raw/incidents.csv')

# See what's in it
print("Shape:", df.shape)          # rows x columns
print("Columns:", df.columns.tolist())
print("Missing values:\n", df.isnull().sum())

# Clean missing values
df['response_time'] = df['response_time'].fillna(df['response_time'].mean())
df.dropna(subset=['incident_id'], inplace=True)
# Apply Step 4 transformations
df = normalize_response_time(df)
df = validate_severity(df)

zone_stats = aggregate_by_zone(df)
print("\nZone Stats:\n", zone_stats)

# Save cleaned data
os.makedirs('data/processed', exist_ok=True)
df.to_csv('data/processed/incidents_clean.csv', index=False)
print("Done! Saved cleaned file.")