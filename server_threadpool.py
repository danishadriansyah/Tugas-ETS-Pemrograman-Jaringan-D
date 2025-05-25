import socket
import os
import base64
import concurrent.futures
import sys
import threading
import time

# Network configuration
NETWORK_HOST = '0.0.0.0'
NETWORK_PORT = 8995
DATA_CHUNK = 1048576  # 1MB per chunk
STORAGE_FOLDER = 'server_storage'
TIMEOUT_DURATION = 1000  # 1000 seconds timeout

# Ensure storage directory exists
os.makedirs(STORAGE_FOLDER, exist_ok=True)

# Operation counters
successful_ops = 0
failed_ops = 0
counter_lock = threading.Lock()

def process_client_request(connection, client_address):
    """Handle client requests in thread pool"""
    global successful_ops, failed_ops
    print(f"Processing connection from {client_address}")

    try:
        connection.settimeout(TIMEOUT_DURATION)
        # Disable Nagle's algorithm for immediate transmission
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        request_data = connection.recv(DATA_CHUNK).decode()
        if not request_data:
            with counter_lock:
                failed_ops += 1
            return

        command_parts = request_data.strip().split()
        if not command_parts:
            connection.send(b'Invalid request format')
            with counter_lock:
                failed_ops += 1
            return

        action = command_parts[0].upper()

        if action == 'DIRECTORY':
            file_list = os.listdir(STORAGE_FOLDER)
            response = '\n'.join(file_list) if file_list else 'Directory empty'
            connection.send(response.encode())
            with counter_lock:
                successful_ops += 1

        elif action == 'STORE':
            if len(command_parts) < 2:
                connection.send(b'Filename required')
                with counter_lock:
                    failed_ops += 1
                return
                
            target_filename = command_parts[1]
            connection.send(b'READY_FOR_DATA')

            received_data = b''
            while True:
                data_part = connection.recv(DATA_CHUNK)
                if b'__COMPLETE__' in data_part:
                    received_data += data_part.replace(b'__COMPLETE__', b'')
                    break
                received_data += data_part

            try:
                decoded_content = base64.b64decode(received_data)
                file_path = os.path.join(STORAGE_FOLDER, target_filename)
                with open(file_path, 'wb') as output_file:
                    output_file.write(decoded_content)
                connection.send(b'File stored successfully')
                with counter_lock:
                    successful_ops += 1
            except Exception as error:
                connection.send(f'Storage error: {str(error)}'.encode())
                with counter_lock:
                    failed_ops += 1

        elif action == 'FETCH':
            if len(command_parts) < 2:
                connection.send(b'Filename required')
                with counter_lock:
                    failed_ops += 1
                return
                
            target_filename = command_parts[1]
            file_path = os.path.join(STORAGE_FOLDER, target_filename)

            if not os.path.exists(file_path):
                connection.send(b'File unavailable')
                with counter_lock:
                    failed_ops += 1
                return

            try:
                with open(file_path, 'rb') as input_file:
                    while True:
                        data_chunk = input_file.read(DATA_CHUNK)
                        if not data_chunk:
                            break
                        encoded_chunk = base64.b64encode(data_chunk)
                        connection.sendall(encoded_chunk)
                connection.send(b'__COMPLETE__')
                with counter_lock:
                    successful_ops += 1
            except Exception as error:
                connection.send(f'Retrieval error: {str(error)}'.encode())
                with counter_lock:
                    failed_ops += 1

        else:
            connection.send(b'Unrecognized command')
            with counter_lock:
                failed_ops += 1

    except Exception as error:
        print(f"Client handling error: {error}")
        try:
            connection.send(f'Error: {error}'.encode())
        except:
            pass
        with counter_lock:
            failed_ops += 1
    finally:
        connection.close()

def monitor_performance():
    """Display server metrics periodically"""
    global successful_ops, failed_ops
    while True:
        time.sleep(10)
        with counter_lock:
            print(f"[PERFORMANCE] Success: {successful_ops}, Failures: {failed_ops}")

def launch_server(worker_count=5):
    """Initialize server with thread pool"""
    monitor_thread = threading.Thread(target=monitor_performance, daemon=True)
    monitor_thread.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((NETWORK_HOST, NETWORK_PORT))
        server.listen(100)
        print(f"Server ready at {NETWORK_HOST}:{NETWORK_PORT} with {worker_count} workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            while True:
                client_conn, client_addr = server.accept()
                executor.submit(process_client_request, client_conn, client_addr)

if __name__ == '__main__':
    workers = int(sys.argv[1]) if len(sys.argv) >= 2 else 5
    launch_server(workers)