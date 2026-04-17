import pandas as pd
import random
import uuid
import os
from datetime import datetime, timedelta

def generate_emergency_data(num_records=100):
    incidents = []
    incident_types = ['Fire', 'Medical', 'Police', 'Traffic', 'Hazardous Material']
    severities = ['Low', 'Medium', 'High', 'Critical']
    zones = ['North', 'South', 'East', 'West', 'Downtown']
    
    # Generate past 24 hours of data
    base_time = datetime.now() - timedelta(days=1)
    
    for _ in range(num_records):
        record = {
            'incident_id': str(uuid.uuid4())[:8],
            'timestamp': (base_time + timedelta(minutes=random.randint(1, 1440))).isoformat(),
            'incident_type': random.choice(incident_types),
            'severity': random.choice(severities),
            'zone': random.choice(zones),
            'latitude': round(random.uniform(40.5, 40.9), 4),
            'longitude': round(random.uniform(-74.3, -73.7), 4),
            'response_time': random.choice([None, random.uniform(2.0, 15.0)]) # Some missing values for cleaning
        }
        incidents.append(record)
        
    df = pd.DataFrame(incidents)
    
    # Ensure directory exists
    os.makedirs('data/raw', exist_ok=True)
    df.to_csv('data/raw/incidents.csv', index=False)
    print(f"[SUCCESS] Generated {num_records} incident records to data/raw/incidents.csv")

if __name__ == "__main__":
    generate_emergency_data()
