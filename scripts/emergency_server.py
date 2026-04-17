import socket
import json
import os

# Phase 1: Networking Basics - Server
def start_server(host='127.0.0.1', port=9999):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    
    print(f"🚨 Emergency Response Server listening on {host}:{port}")
    os.makedirs('../data/raw', exist_ok=True)
    
    with open('../data/raw/streaming.log', 'a') as log_file:
        while True:
            client_socket, addr = server.accept()
            print(f"[*] Accepted connection from {addr[0]}:{addr[1]}")
            
            request = client_socket.recv(1024).decode('utf-8')
            if request:
                print(f"[*] Received Alert: {request}")
                # Save the log
                log_file.write(f"{request}\n")
                log_file.flush()
                
                # Send ACK
                client_socket.send("ACK: Alert Received and Logged".encode('utf-8'))
            
            client_socket.close()

if __name__ == "__main__":
    try:
        start_server()
    except KeyboardInterrupt:
        print("\nShutting down server.")
