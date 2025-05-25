import socket
import os
import base64
import concurrent.futures
import time
import sys
from tqdm import tqdm

# Connection settings
SERVER_IP = '172.16.16.101'  # Update this with your server's actual IP
SERVER_PORT = 8995
TRANSFER_BLOCK = 1048576    # 1MB
LOCAL_FOLDER = 'client_data'
RECEIVED_FOLDER = 'client_downloads'

# Ensure download directory exists
os.makedirs(RECEIVED_FOLDER, exist_ok=True)

def perform_upload(file_name):
    """Handle file upload operation (fixed to match server protocol)"""
    try:
        file_path = os.path.join(LOCAL_FOLDER, file_name)
        file_size = os.path.getsize(file_path)
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((SERVER_IP, SERVER_PORT))
            sock.send(f'UPLOAD {file_name}'.encode())

            server_response = sock.recv(TRANSFER_BLOCK)
            if server_response != b'PROCEED':
                return False, f"Server response: {server_response.decode()}"

            with open(file_path, 'rb') as file, \
                 tqdm(total=file_size, unit='B', unit_scale=True, 
                      desc=f'Uploading {file_name}', leave=False) as progress:
                
                while True:
                    data_block = file.read(TRANSFER_BLOCK)
                    if not data_block:
                        break
                    encoded_block = base64.b64encode(data_block)
                    sock.send(encoded_block)
                    progress.update(len(data_block))

            sock.send(b'__EOF__')
            result = sock.recv(TRANSFER_BLOCK).decode()
            return True, result
            
    except Exception as e:
        return False, str(e)

def perform_download(file_name):
    """Handle file download operation"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((SERVER_IP, SERVER_PORT))
            sock.send(f'DOWNLOAD {file_name}'.encode())

            received_data = b''
            with tqdm(unit='B', unit_scale=True, 
                     desc=f'Downloading {file_name}', leave=False) as progress:
                
                while True:
                    data_part = sock.recv(TRANSFER_BLOCK)
                    if b'__EOF__' in data_part:
                        received_data += data_part.replace(b'__EOF__', b'')
                        progress.update(len(data_part) - len(b'__EOF__'))
                        break
                    received_data += data_part
                    progress.update(len(data_part))

            decoded_content = base64.b64decode(received_data)
            save_path = os.path.join(RECEIVED_FOLDER, file_name)
            
            with open(save_path, 'wb') as file:
                file.write(decoded_content)

            return True, "Download completed"
            
    except Exception as e:
        return False, str(e)

def execute_operation(op_type, file_name):
    """Execute and time an operation"""
    start_time = time.time()
    
    if op_type == 'upload':
        success, message = perform_upload(file_name)
    elif op_type == 'download':
        success, message = perform_download(file_name)
    else:
        return False, 0, 0, f"Invalid operation: {op_type}"
    
    end_time = time.time()
    duration = end_time - start_time
    
    file_path = os.path.join(LOCAL_FOLDER if op_type == 'upload' else RECEIVED_FOLDER, file_name)
    size = os.path.getsize(file_path) if success and os.path.exists(file_path) else 0
    speed = size / duration if duration > 0 else 0
    
    return success, duration, speed, message

def run_client():
    """Main client function"""
    if len(sys.argv) != 5:
        print("Usage: python client.py [upload|download] [10MB|50MB|100MB] [workers] [thread|process]")
        return

    operation = sys.argv[1].lower()
    size_option = sys.argv[2]
    worker_count = int(sys.argv[3])
    execution_mode = sys.argv[4].lower()

    file_mapping = {
        "10MB": "10mb.txt",
        "50MB": "50mb.txt", 
        "100MB": "100mb.txt"
    }

    target_file = file_mapping.get(size_option)
    if not target_file:
        print(f"Invalid size option: {size_option}")
        return

    if execution_mode == 'thread':
        ExecutorClass = concurrent.futures.ThreadPoolExecutor
    elif execution_mode == 'process':
        ExecutorClass = concurrent.futures.ProcessPoolExecutor
    else:
        print("Execution mode must be 'thread' or 'process'")
        return

    print(f"Starting test: {operation} {target_file} | Workers: {worker_count} | Mode: {execution_mode}")

    success_total = 0
    failure_total = 0
    time_accumulated = 0
    speed_accumulated = 0

    with ExecutorClass(max_workers=worker_count) as executor:
        tasks = [executor.submit(execute_operation, operation, target_file) 
                for _ in range(worker_count)]
        
        for task in concurrent.futures.as_completed(tasks):
            status, elapsed, throughput, msg = task.result()
            
            if status:
                success_total += 1
                time_accumulated += elapsed
                speed_accumulated += throughput
            else:
                failure_total += 1
                
            print(f"[{'SUCCESS' if status else 'FAILURE'}] Time: {elapsed:.2f}s | Speed: {throughput:.2f} B/s | Message: {msg}")

    avg_time = time_accumulated / success_total if success_total else 0
    avg_speed = speed_accumulated / success_total if success_total else 0

    print("\n=== Test Summary ===")
    print(f"Operation type: {operation.upper()}")
    print(f"Test file: {target_file}")
    print(f"Worker count: {worker_count}")
    print(f"Execution mode: {execution_mode}")
    print(f"Average time: {avg_time:.2f} seconds")
    print(f"Average speed: {avg_speed:.2f} bytes/second")
    print(f"Successful operations: {success_total}")
    print(f"Failed operations: {failure_total}")

if __name__ == '__main__':
    run_client()
