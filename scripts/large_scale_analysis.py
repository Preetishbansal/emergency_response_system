import pandas as pd
import numpy as np

# Read large file efficiently (in chunks)
chunk_size = 100_000
chunks = []

for chunk in pd.read_csv('data/raw/incidents_large.csv', chunksize=chunk_size):
    # Process each chunk
    chunk = chunk[chunk['response_time'] < 3600] # filter invalid
    chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)
print(f"Total rows: {len(df):,}")

# Memory optimization: use smaller data types
df['severity'] = df['severity'].astype('category')  # saves RAM
df['zone_id'] = df['zone_id'].astype('int16')        # saves RAM

print("Memory usage:", df.memory_usage(deep=True).sum() / 1024**2, "MB")
