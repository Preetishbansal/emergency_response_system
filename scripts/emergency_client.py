import socket
import json
import random
import uuid
import time
from datetime import datetime

# Phase 1: Networking Basics - Client
def send_alert(host='127.0.0.1', port=9999):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        client.connect((host, port))
        
        # Format a live emergency alert
        alert = {
            'incident_id': str(uuid.uuid4())[:8],
            'timestamp': datetime.now().isoformat(),
            'incident_type': random.choice(['Fire', 'Medical', 'Police']),
            'priority': 'Critical'
        }
        
        payload = json.dumps(alert)
        client.send(payload.encode('utf-8'))
        
        # Wait for acknowledgment
        response = client.recv(4096).decode('utf-8')
        print(f"[SUCCESS] Server Response: {response}")
        
    except ConnectionRefusedError:
        print("[ERROR] Could not connect to the Emergency Server. Is it running?")
    finally:
        client.close()

if __name__ == "__main__":
    print("Simulating 3 live emergency alerts...")
    for i in range(3):
        send_alert()
        time.sleep(1)
