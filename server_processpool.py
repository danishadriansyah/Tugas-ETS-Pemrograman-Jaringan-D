import socket
import os
import base64
import concurrent.futures
import threading
import sys
import time
import uuid

# Server configuration
HOST = '0.0.0.0'
PORT = 8995
CHUNK_SIZE = 1048576  # 1MB chunk size
STORAGE_DIR = 'server_storage'
CONNECTION_TIMEOUT = 60

# Ensure storage directory exists
os.makedirs(STORAGE_DIR, exist_ok=True)

# Performance tracking
completed_operations = 0
failed_operations = 0
stats_lock = threading.Lock()

def execute_command(command, parameters):
    """Process different file operations"""
    try:
        if command == 'LIST_FILES':
            files = os.listdir(STORAGE_DIR)
            return '\n'.join(files).encode() if files else 'Empty directory'.encode()

        elif command == 'STORE_FILE':
            filename, temp_path = parameters
            if not os.path.exists(temp_path):
                return f'Temp file missing: {temp_path}'.encode()

            with open(temp_path, 'rb') as temp_file:
                encoded_content = temp_file.read()
            
            file_content = base64.b64decode(encoded_content)
            destination = os.path.join(STORAGE_DIR, filename)
            
            with open(destination, 'wb') as dest_file:
                dest_file.write(file_content)
            
            os.remove(temp_path)
            return b'File stored successfully'

        elif command == 'RETRIEVE_FILE':
            filename = parameters
            file_path = os.path.join(STORAGE_DIR, filename)
            if not os.path.exists(file_path):
                return b'File unavailable'
            
            with open(file_path, 'rb') as file:
                content = file.read()
            
            return base64.b64encode(content) + b'__EOF__'

        else:
            return b'Invalid operation'

    except Exception as error:
        return f'Operation failed: {str(error)}'.encode()

def display_stats():
    """Periodically show server performance metrics"""
    global completed_operations, failed_operations
    while True:
        time.sleep(10)
        with stats_lock:
            print(f"[STATS] Successful: {completed_operations}, Failed: {failed_operations}")

def manage_connection(connection, client_address, executor):
    """Handle client connection and requests"""
    global completed_operations, failed_operations
    
    print(f"New connection from {client_address}")
    
    try:
        connection.settimeout(CONNECTION_TIMEOUT)
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        request = connection.recv(CHUNK_SIZE).decode().strip()
        if not request:
            with stats_lock:
                failed_operations += 1
            return

        parts = request.split()
        if not parts:
            connection.send(b'Invalid request')
            with stats_lock:
                failed_operations += 1
            return

        operation = parts[0].upper()

        if operation == 'LIST':
            task = executor.submit(execute_command, 'LIST_FILES', None)
            response = task.result()
            connection.sendall(response)
            with stats_lock:
                completed_operations += 1

        elif operation == 'UPLOAD':
            if len(parts) < 2:
                connection.send(b'Missing filename')
                with stats_lock:
                    failed_operations += 1
                return
                
            filename = parts[1]
            connection.send(b'PROCEED')

            temp_filename = f"{filename}_{uuid.uuid4()}.tmp"
            temp_path = os.path.join(STORAGE_DIR, temp_filename)

            with open(temp_path, 'wb') as temp_file:
                while True:
                    data_chunk = connection.recv(CHUNK_SIZE)
                    if b'__EOF__' in data_chunk:
                        temp_file.write(data_chunk.replace(b'__EOF__', b''))
                        break
                    temp_file.write(data_chunk)

            task = executor.submit(execute_command, 'STORE_FILE', (filename, temp_path))
            response = task.result()
            connection.sendall(response)

            if b'success' in response.lower():
                with stats_lock:
                    completed_operations += 1
            else:
                with stats_lock:
                    failed_operations += 1

        elif operation == 'DOWNLOAD':
            if len(parts) < 2:
                connection.send(b'Missing filename')
                with stats_lock:
                    failed_operations += 1
                return
                
            filename = parts[1]
            task = executor.submit(execute_command, 'RETRIEVE_FILE', filename)
            response = task.result()

            if b'unavailable' in response.lower():
                connection.sendall(response)
                with stats_lock:
                    failed_operations += 1
                return

            connection.sendall(response)
            with stats_lock:
                completed_operations += 1

        else:
            connection.send(b'Unknown operation')
            with stats_lock:
                failed_operations += 1

    except Exception as error:
        print(f"Connection error: {error}")
        try:
            connection.send(f'Error: {error}'.encode())
        except:
            pass
        with stats_lock:
            failed_operations += 1
    finally:
        connection.close()

def initialize_server(worker_count=5):
    """Start the server with specified worker pool"""
    stats_thread = threading.Thread(target=display_stats, daemon=True)
    stats_thread.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, PORT))
        server.listen(100)
        print(f"Server active on {HOST}:{PORT} with {worker_count} workers")

        with concurrent.futures.ProcessPoolExecutor(max_workers=worker_count) as pool:
            while True:
                conn, addr = server.accept()
                client_thread = threading.Thread(
                    target=manage_connection, 
                    args=(conn, addr, pool)
                )
                client_thread.start()

if __name__ == '__main__':
    workers = int(sys.argv[1]) if len(sys.argv) >= 2 else 5
    initialize_server(workers)