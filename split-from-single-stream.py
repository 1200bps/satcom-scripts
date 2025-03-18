import socket
import re
import os
import argparse
import sys
import time

def parse_acars_message(message):
    """
    Parse an ACARS message and extract its label.
    
    Args:
        message (str): A complete ACARS message
        
    Returns:
        tuple: (label, message) or (None, message) if no label found
    """
    # Extract the first line of the message (header line)
    first_line = message.split('\n')[0].strip()
    
    # The format appears to be consistent with fields separated by spaces
    # After the timestamp, metadata, and aircraft ID, we have delimiter, label, etc.
    # Example: 00:16:25 18-03-25 UTC AES:E4920F GES:D0 2 ..PTZNG 2 52 A
    parts = first_line.split()
    
    # We need at least 9 parts to have a label
    if len(parts) >= 9:
        # The label should be at a specific position (9th item)
        label_position = 8  # 0-indexed, so this is the 9th item
        if label_position < len(parts):
            label = parts[label_position]
            # Verify it's a valid label (alphanumeric)
            if re.match(r'^[A-Za-z0-9]+$', label):
                return label, message
    
    return None, message

def append_message_to_file(output_dir, label, message):
    """
    Append a message to the appropriate file based on its label.
    
    Args:
        output_dir (str): Directory to store output files
        label (str or None): Message label or None for unlabeled messages
        message (str): The ACARS message to append
    """
    if label:
        output_file = os.path.join(output_dir, f"acars_{label}.txt")
    else:
        output_file = os.path.join(output_dir, "acars_unlabeled.txt")
    
    # Check if the file exists and has content
    file_exists = os.path.exists(output_file)
    file_has_content = file_exists and os.path.getsize(output_file) > 0
    
    # Append the message to the file
    with open(output_file, 'a') as file:
        if file_has_content:
            # Add two newlines between messages
            file.write('\n\n')
        file.write(message)

def process_buffer(buffer, output_dir):
    """
    Process the buffer to extract and handle complete messages.
    
    Args:
        buffer (str): The current buffer containing ACARS messages
        output_dir (str): Directory to store output files
        
    Returns:
        tuple: (remaining_buffer, messages_processed)
    """
    timestamp_pattern = r'^\d{2}:\d{2}:\d{2} \d{2}-\d{2}-\d{2} UTC'
    matches = list(re.finditer(timestamp_pattern, buffer, re.MULTILINE))
    
    if len(matches) < 2:
        # Not enough timestamps to identify a complete message
        return buffer, 0
    
    messages_processed = 0
    
    # Process all complete messages (except possibly the last one)
    for i in range(len(matches) - 1):
        start_pos = matches[i].start()
        end_pos = matches[i+1].start()
        
        # Extract the complete message
        message = buffer[start_pos:end_pos].strip()
        
        # Process the message
        label, full_message = parse_acars_message(message)
        append_message_to_file(output_dir, label, full_message)
        print(f"Processed message with label: {label or 'None'}")
        messages_processed += 1
    
    # Keep the last (potentially incomplete) message in the buffer
    remaining_buffer = buffer[matches[-1].start():]
    
    return remaining_buffer, messages_processed

def listen_and_process_acars(host, port, output_dir, buffer_timeout=60):
    """
    Listen for ACARS messages on a UDP socket and process them.
    
    Args:
        host (str): Host address to listen on
        port (int): UDP port to listen on
        output_dir (str): Directory to store output files
        buffer_timeout (int): Time in seconds to wait before processing an incomplete buffer
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    
    # Set a timeout for the socket to periodically check the buffer
    sock.settimeout(buffer_timeout)
    
    print(f"Listening for ACARS messages on {host}:{port}...")
    
    # Buffer for incoming data
    buffer = ""
    last_process_time = time.time()
    
    try:
        while True:
            try:
                # Receive data from the socket
                data, addr = sock.recvfrom(4096)
                
                # Decode the data and add it to our buffer
                try:
                    decoded_data = data.decode('utf-8')
                    buffer += decoded_data
                except UnicodeDecodeError:
                    print(f"Warning: Received data that could not be decoded as UTF-8", file=sys.stderr)
                    continue
                
                # Process the buffer if we have enough data
                buffer, messages_processed = process_buffer(buffer, output_dir)
                
                if messages_processed > 0:
                    last_process_time = time.time()
                
            except socket.timeout:
                # No data received before timeout
                current_time = time.time()
                
                # If it's been a while since we processed anything and we have data,
                # try to process what we have even if it might be incomplete
                if buffer and (current_time - last_process_time > buffer_timeout * 2):
                    timestamp_pattern = r'^\d{2}:\d{2}:\d{2} \d{2}-\d{2}-\d{2} UTC'
                    match = re.search(timestamp_pattern, buffer, re.MULTILINE)
                    
                    if match:
                        # We have at least one timestamp, try to process it as a message
                        message = buffer[match.start():].strip()
                        label, full_message = parse_acars_message(message)
                        append_message_to_file(output_dir, label, full_message)
                        print(f"Processed timeout message with label: {label or 'None'}")
                        buffer = ""
                        last_process_time = current_time
                
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        sock.close()

def main():
    parser = argparse.ArgumentParser(description='Listen for ACARS messages via UDP and split by label.')
    parser.add_argument('-a', '--address', default='127.0.0.1',
                      help='IP address to listen on (default: 127.0.0.1)')
    parser.add_argument('-p', '--port', type=int, required=True,
                      help='UDP port to listen on')
    parser.add_argument('-o', '--output-dir', default='acars_by_label',
                      help='Directory to store output files (default: acars_by_label)')
    parser.add_argument('-t', '--timeout', type=int, default=60,
                      help='Buffer timeout in seconds (default: 60)')
    
    args = parser.parse_args()
    listen_and_process_acars(args.address, args.port, args.output_dir, args.timeout)

if __name__ == "__main__":
    main()
